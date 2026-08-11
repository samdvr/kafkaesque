//! Zombie-mode exit verification and partition re-open.

use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::ownership::{OwnershipContext, apply_store_tuning};
use crate::cluster::error::SlateDBResult;
use crate::cluster::partition_state::PartitionState;
use crate::cluster::partition_store::PartitionStore;
use crate::cluster::traits::ClusterCoordinator;

/// Helper macro to check if zombie mode was re-entered during verification.
/// Returns early with Ok(false) if re-entry detected.
macro_rules! check_zombie_reentry {
    ($ctx:expr, $entered_at_start:expr, $operation:expr) => {
        if $ctx.zombie_state.entered_at() != $entered_at_start {
            warn!(
                "Zombie mode re-entered during verification (heartbeat failed again) - aborting {} and staying in zombie mode (broker_id={}, operation={})",
                $operation,
                $ctx.broker_id,
                $operation
            );
            return Ok(false);
        }
    };
}

pub(super) async fn try_exit_zombie_mode<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
) -> SlateDBResult<bool> {
    if !ctx.zombie_state.is_active() {
        return Ok(true);
    }

    // CRITICAL: Capture the zombie mode entry timestamp at the start of verification.
    // If the heartbeat loop enters zombie mode again during our verification,
    // the timestamp will change and we MUST NOT exit zombie mode.
    //
    // We now check this timestamp after EACH async operation, not just at the end.
    // This prevents the race condition where:
    //   1. We start verification (zombie_mode = true, entered_at = T1)
    //   2. We verify partition A (OK)
    //   3. Heartbeat fails again, re-enters zombie mode (entered_at = T2)
    //   4. We continue verifying partitions B, C, D with STALE assumptions
    //   5. We try to exit - BAD: partition A's verification is now invalid
    let entered_at_start = ctx.zombie_state.entered_at();

    info!(
        ctx.broker_id,
        "Attempting to exit zombie mode - verifying partition leases and SlateDB fencing"
    );

    let owned_partitions: Vec<_> = ctx
        .partition_states
        .iter()
        .filter_map(|e| {
            if e.value().is_owned() {
                Some(e.key().clone())
            } else {
                None
            }
        })
        .collect();

    let mut lost_partitions = Vec::new();
    let mut need_reopen = Vec::new();

    for (topic, partition) in &owned_partitions {
        // Check for re-entry before each partition verification
        check_zombie_reentry!(ctx, entered_at_start, "partition verification");

        // Step 1: Verify coordinator ownership
        match ctx
            .coordinator
            .owns_partition_for_read(topic, *partition)
            .await
        {
            Ok(true) => {
                debug!(
                    topic = &**topic,
                    partition, "Verified partition ownership in coordinator"
                );

                // Check for re-entry after coordinator call
                check_zombie_reentry!(ctx, entered_at_start, "ownership verification");

                // Step 2: Verify SlateDB handle is not fenced
                // During zombie mode, another broker may have acquired the partition,
                // opened SlateDB (getting a new fencing token), and fenced our handle.
                // We need to verify our SlateDB handle is still valid.
                if let Some(state) = ctx.partition_states.get(&(Arc::clone(topic), *partition))
                    && let Some(store) = state.store()
                {
                    // Try to read HWM to verify SlateDB access
                    // This will fail if we've been fenced
                    match store.high_watermark_check().await {
                        Ok(_) => {
                            debug!(
                                topic = &**topic,
                                partition, "Verified SlateDB handle is not fenced"
                            );
                            // Check for re-entry after SlateDB check
                            check_zombie_reentry!(ctx, entered_at_start, "SlateDB verification");
                        }
                        Err(e) => {
                            if e.is_fenced() {
                                warn!(
                                    topic = &**topic,
                                    partition,
                                    "SlateDB handle fenced - will close and re-acquire partition"
                                );
                                // Mark for re-opening with fresh SlateDB handle
                                need_reopen.push((Arc::clone(topic), *partition));
                            } else {
                                error!(
                                    topic = &**topic, partition, error = %e,
                                    "Failed to verify SlateDB handle - staying in zombie mode"
                                );
                                return Err(e);
                            }
                        }
                    }
                }
            }
            Ok(false) => {
                warn!(
                    topic = &**topic,
                    partition, "Lost partition ownership during zombie mode"
                );
                lost_partitions.push((Arc::clone(topic), *partition));
            }
            Err(e) => {
                error!(topic = &**topic, partition, error = %e, "Failed to verify partition ownership - staying in zombie mode");
                return Err(e);
            }
        }
    }

    // Check for re-entry before cleanup phase
    check_zombie_reentry!(ctx, entered_at_start, "cleanup phase");

    // Close and remove lost partitions
    for (topic, partition) in lost_partitions {
        if let Some(store) = ctx
            .partition_states
            .remove(&(Arc::clone(&topic), partition))
            .and_then(|(_, s)| s.store())
        {
            // close() takes &self so this always works
            let _ = store.close().await;
        }
        crate::cluster::metrics::OWNED_PARTITIONS
            .with_label_values(&[&topic])
            .dec();
    }

    // Check for re-entry before re-opening phase
    check_zombie_reentry!(ctx, entered_at_start, "re-open phase");

    // Re-open partitions that have fenced SlateDB handles
    for (topic, partition) in need_reopen {
        // Check for re-entry before each re-open operation
        check_zombie_reentry!(ctx, entered_at_start, "partition re-open");

        info!(
            topic = &*topic,
            partition, "Re-opening partition with fresh SlateDB handle after zombie mode"
        );

        // Close existing store
        if let Some(store) = ctx
            .partition_states
            .remove(&(Arc::clone(&topic), partition))
            .and_then(|(_, s)| s.store())
        {
            // close() takes &self so this always works
            let _ = store.close().await;
        }

        // Check for re-entry after close
        check_zombie_reentry!(ctx, entered_at_start, "store close");

        // CRITICAL: Verify we still own the partition AND extend the lease before re-opening SlateDB.
        // This addresses the race condition where:
        // 1. We entered zombie mode
        // 2. Our lease expired during zombie mode
        // 3. Another broker acquired the partition
        // 4. We try to re-open, thinking we still own it
        //
        // Using verify_and_extend_lease ensures:
        // - We still own the partition (fresh check, no cache)
        // - The lease is extended to ensure it won't expire during SlateDB open
        match ctx
            .coordinator
            .verify_and_extend_lease(&topic, partition, ctx.lease_secs)
            .await
        {
            Ok(remaining_ttl) => {
                debug!(
                    topic = &*topic,
                    partition,
                    remaining_ttl,
                    "Verified lease ownership and extended before re-opening SlateDB"
                );
                // Check for re-entry after lease extension
                check_zombie_reentry!(ctx, entered_at_start, "lease extension");
            }
            Err(e) => {
                warn!(
                    topic = &*topic, partition, error = %e,
                    "Lost partition ownership during zombie mode - cannot re-open"
                );
                crate::cluster::metrics::OWNED_PARTITIONS
                    .with_label_values(&[&topic])
                    .dec();
                continue;
            }
        }

        // Re-acquire to bump the leader epoch. The fresh epoch both fences
        // any stale handle (including our own pre-zombie one) and is REQUIRED
        // for the reopen below: opening without `.leader_epoch(...)` would
        // disable per-write epoch validation entirely, leaving SlateDB
        // single-writer fencing as the only guard on a broker that was just
        // suspected dead — exactly when epoch fencing matters most.
        let reacquired_epoch = match ctx
            .coordinator
            .acquire_partition_with_epoch(&topic, partition, ctx.lease_secs)
            .await
        {
            Ok(Some(epoch)) => {
                check_zombie_reentry!(ctx, entered_at_start, "epoch re-acquisition");
                epoch
            }
            Ok(None) => {
                warn!(
                    topic = &*topic,
                    partition,
                    "Lost partition to another broker during zombie recovery - cannot re-open"
                );
                crate::cluster::metrics::OWNED_PARTITIONS
                    .with_label_values(&[&topic])
                    .dec();
                continue;
            }
            Err(e) => {
                warn!(
                    topic = &*topic, partition, error = %e,
                    "Failed to re-acquire epoch during zombie recovery - cannot re-open"
                );
                crate::cluster::metrics::OWNED_PARTITIONS
                    .with_label_values(&[&topic])
                    .dec();
                continue;
            }
        };

        // Open fresh SlateDB instance - lease is verified and extended,
        // epoch validation armed with the freshly bumped epoch.
        match apply_store_tuning(PartitionStore::builder(), ctx)
            .object_store(ctx.object_store.clone())
            .base_path(&ctx.base_path)
            .topic(&topic)
            .partition(partition)
            .zombie_mode(ctx.zombie_state.clone())
            .leader_epoch(reacquired_epoch)
            .build()
            .await
        {
            Ok(store) => {
                // Check for re-entry after SlateDB open
                if ctx.zombie_state.entered_at() != entered_at_start {
                    warn!(
                        ctx.broker_id,
                        topic = &*topic,
                        partition,
                        "Zombie mode re-entered after opening SlateDB - closing store and aborting"
                    );
                    // Close the store we just opened since our verification is now invalid
                    let _ = store.close().await;
                    return Ok(false);
                }

                ctx.partition_states.insert(
                    (Arc::clone(&topic), partition),
                    PartitionState::acquired(Arc::new(store)),
                );
                info!(
                    topic = &*topic,
                    partition, "Successfully re-opened partition with fresh SlateDB handle"
                );
            }
            Err(e) => {
                error!(topic = &*topic, partition, error = %e, "Failed to re-open partition - releasing ownership");
                let _ = ctx.coordinator.release_partition(&topic, partition).await;
                crate::cluster::metrics::OWNED_PARTITIONS
                    .with_label_values(&[&topic])
                    .dec();
            }
        }
    }

    // CRITICAL: Use try_exit which atomically checks:
    // 1. We're still in zombie mode (someone else might have exited it)
    // 2. The zombie mode entry timestamp hasn't changed (heartbeat didn't fail again)
    //
    // This prevents the race where heartbeat fails again during our verification,
    // causing us to exit zombie mode while the broker is actually unhealthy.
    if ctx.zombie_state.try_exit(entered_at_start, "recovered") {
        info!(
            ctx.broker_id,
            "EXITED ZOMBIE MODE - all partition handles verified"
        );
        Ok(true)
    } else {
        // Either zombie mode was re-entered or another thread already exited
        if !ctx.zombie_state.is_active() {
            info!(ctx.broker_id, "Another thread already exited zombie mode");
            Ok(true)
        } else {
            warn!(
                ctx.broker_id,
                "Zombie mode was re-entered during verification (heartbeat failed again) - not exiting"
            );
            Ok(false)
        }
    }
}
