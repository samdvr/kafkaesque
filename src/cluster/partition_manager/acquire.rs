//! Partition acquire / release helpers and ownership verification.

use std::sync::Arc;
use tracing::{debug, error, info, warn};

use super::ownership::{OwnershipContext, apply_store_tuning};
use crate::cluster::error::SlateDBResult;
use crate::cluster::partition_state::PartitionState;
use crate::cluster::partition_store::PartitionStore;
use crate::cluster::traits::ClusterCoordinator;

/// RAII guard that decrements the broker-wide `pending_acquires` counter
/// when dropped. Used to release the in-flight slot reserved against the
/// `max_owned_partitions_per_broker` cap, so every return path out of the
/// cap-checked region (including panic-via-`?`) gives the slot back.
struct PendingAcquireGuard {
    counter: Arc<std::sync::atomic::AtomicUsize>,
}

impl Drop for PendingAcquireGuard {
    fn drop(&mut self) {
        self.counter
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
    }
}

pub(super) async fn acquire_partition_core<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) -> SlateDBResult<bool> {
    // A broker that has lost coordination must not acquire fresh
    // partitions: opening a SlateDB instance and bumping the leader
    // epoch from a stale node is the split-brain scenario zombie mode
    // exists to prevent. Reject before taking any locks.
    if ctx.zombie_state.is_active() {
        return Ok(false);
    }

    let key = ctx.partition_key(topic, partition);

    // Per-key serialization. Two concurrent acquires for the same partition
    // would otherwise both pass the existence check, build separate
    // `PartitionStore` instances, and let the second insert silently replace
    // the first — leaking the orphaned SlateDB handle (and any in-flight
    // writes still holding it). The mutex is keyed by partition, so different
    // partitions still acquire concurrently.
    let lock = ctx
        .acquire_locks
        .entry(key.clone())
        .or_insert_with(|| Arc::new(tokio::sync::Mutex::new(())))
        .clone();
    let _guard = lock.lock().await;

    // Recheck under the lock: the winning concurrent caller may have already
    // acquired and inserted while we were queued.
    if let Some(state) = ctx.partition_states.get(&key)
        && state.is_owned()
    {
        return Ok(true);
    }

    // Enforce the per-broker owned-partition cap before opening another
    // SlateDB instance. Each owned partition holds a live LSM engine
    // (memtable, WAL, block cache, background tasks), so an unbounded owned
    // set is the most likely OOM vector at high partition density (audit
    // P1-2). Rejecting here turns that into a bounded, observable limit: the
    // partition simply isn't acquired and stays available to another broker.
    // `0` disables the cap.
    //
    // The check is now atomic across concurrent acquires for *different*
    // partitions: we `fetch_add` an in-flight reservation BEFORE counting
    // the live owned set, and compare the post-increment total against the
    // cap. Without that, N concurrent acquires would each read `owned < cap`
    // independently and all proceed, briefly overshooting the cap by N - 1.
    // The reservation is released on every exit from the cap-checked block
    // (success or failure) via `pending_release`.
    let pending_release = if ctx.max_owned_partitions_per_broker > 0 {
        use std::sync::atomic::Ordering;
        let prev = ctx.pending_acquires.fetch_add(1, Ordering::SeqCst);
        let owned = ctx
            .partition_states
            .iter()
            .filter(|e| e.value().is_owned())
            .count();
        // Post-increment count of in-flight reservations is `prev + 1`. We
        // reject when adding *this* reservation would push the projected
        // total above the cap. Using `>` lets the very last slot succeed.
        if owned + prev + 1 > ctx.max_owned_partitions_per_broker {
            ctx.pending_acquires.fetch_sub(1, Ordering::SeqCst);
            warn!(
                ctx.broker_id,
                topic,
                partition,
                owned,
                pending = prev + 1,
                max_owned = ctx.max_owned_partitions_per_broker,
                "Rejecting partition acquisition: broker is at its max_owned_partitions_per_broker cap"
            );
            crate::cluster::metrics::record_partition_acquire_rejected("max_owned");
            crate::cluster::metrics::record_lease_operation("acquire", "rejected_max_owned");
            return Ok(false);
        }
        // Scope guard: decrement on every return path below (Ok(true),
        // Ok(false), Err) so the counter only reflects truly in-flight
        // acquires. Decrement happens in the guard's Drop.
        Some(PendingAcquireGuard {
            counter: ctx.pending_acquires.clone(),
        })
    } else {
        None
    };
    // Keep the guard alive across the rest of the function via a `_` bind
    // — its Drop fires at the end of the scope.
    let _pending_release = pending_release;

    // Use acquire_partition_with_epoch to get the leader epoch for TOCTOU prevention
    let leader_epoch = match ctx
        .coordinator
        .acquire_partition_with_epoch(topic, partition, ctx.lease_secs)
        .await?
    {
        Some(epoch) => epoch,
        None => {
            crate::cluster::metrics::record_lease_operation("acquire", "failure");
            return Ok(false);
        }
    };

    let build_result = apply_store_tuning(PartitionStore::builder(), ctx)
        .object_store(ctx.object_store.clone())
        .base_path(&ctx.base_path)
        .topic(topic)
        .partition(partition)
        .zombie_mode(ctx.zombie_state.clone())
        .leader_epoch(leader_epoch) // Pass epoch for TOCTOU prevention
        .build()
        .await;

    match build_result {
        Ok(store) => {
            ctx.partition_states
                .insert(key, PartitionState::acquired(Arc::new(store)));
            crate::cluster::metrics::record_lease_operation("acquire", "success");
            crate::cluster::metrics::OWNED_PARTITIONS
                .with_label_values(&[topic])
                .inc();
            info!(
                ctx.broker_id,
                topic, partition, leader_epoch, "Acquired partition with epoch"
            );
            Ok(true)
        }
        Err(e) => {
            if e.is_fenced() {
                warn!(
                    topic,
                    partition, "Fenced during open - another broker owns this partition"
                );
                crate::cluster::metrics::record_lease_operation("acquire", "fenced");
                ctx.coordinator
                    .invalidate_ownership_cache(topic, partition)
                    .await;
            } else {
                error!(topic, partition, error = %e, "Failed to open partition store");
                crate::cluster::metrics::record_lease_operation("acquire", "error");
                if let Err(release_err) = ctx.coordinator.release_partition(topic, partition).await
                {
                    warn!(topic, partition, error = %release_err, "Failed to release partition after open error");
                }
            }
            Err(e)
        }
    }
}

pub(super) async fn acquire_partition<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) {
    match acquire_partition_core(ctx, topic, partition).await {
        Ok(true) => {}
        Ok(false) => debug!(topic, partition, "Partition owned by another broker"),
        Err(_) => {}
    }
}

pub(super) async fn verify_ownership<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) {
    let key = ctx.partition_key(topic, partition);
    match ctx
        .coordinator
        .owns_partition_for_read(topic, partition)
        .await
    {
        Ok(true) => {}
        Ok(false) => {
            warn!(topic, partition, "Lost partition ownership unexpectedly");
            // Close the store on this loss path too — see release_partition.
            if let Some(store) = ctx
                .partition_states
                .remove(&key)
                .and_then(|(_, s)| s.store())
            {
                store.clear_load_metrics();
                if let Err(e) = store.close().await {
                    warn!(topic, partition, error = %e, "Failed to close partition store after ownership loss");
                }
            }
            ctx.lease_cache.remove(&key);
            crate::cluster::metrics::OWNED_PARTITIONS
                .with_label_values(&[topic])
                .dec();
        }
        Err(e) => warn!(topic, partition, error = %e, "Failed to verify partition ownership"),
    }
}

/// Release a partition that has been reassigned away from this broker.
///
/// SlateDB is closed immediately to keep the background compactor from
/// panicking when the new owner opens and fences us. The coordinator
/// release is best-effort — if it fails, the lease will expire naturally.
pub(super) async fn release_reassigned_partition<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) {
    let key = ctx.partition_key(topic, partition);

    if let Some((_, state)) = ctx.partition_states.remove(&key)
        && let Some(store) = state.store()
    {
        info!(
            ctx.broker_id,
            topic, partition, "Releasing reassigned partition - closing SlateDB"
        );

        store.clear_load_metrics();
        if let Err(e) = store.close().await {
            if e.is_fenced() {
                debug!(
                    topic,
                    partition, "Fenced during close (expected after reassignment)"
                );
            } else {
                warn!(topic, partition, error = %e, "Error closing store during reassignment");
            }
        }

        match ctx.coordinator.release_partition(topic, partition).await {
            Ok(()) => {
                info!(
                    ctx.broker_id,
                    topic, partition, "Released partition through coordinator"
                );
            }
            Err(e) => {
                warn!(
                    ctx.broker_id,
                    topic,
                    partition,
                    error = %e,
                    "Failed to release partition through coordinator - lease will expire naturally"
                );
            }
        }

        ctx.lease_cache.remove(&key);

        crate::cluster::metrics::record_lease_operation("release", "reassigned");
        crate::cluster::metrics::OWNED_PARTITIONS
            .with_label_values(&[topic])
            .dec();
    }
}

pub(super) async fn release_partition_for_deleted_topic<C: ClusterCoordinator>(
    ctx: &OwnershipContext<C>,
    topic: &str,
    partition: i32,
) {
    info!(
        ctx.broker_id,
        topic, partition, "Releasing partition for deleted topic"
    );
    let key = ctx.partition_key(topic, partition);
    if let Some(store) = ctx
        .partition_states
        .remove(&key)
        .and_then(|(_, s)| s.store())
    {
        store.clear_load_metrics();
        // close() takes &self so this always works
        let _ = store.close().await;
    }
    // Drop the cluster lease too. Skipping the coordinator release leaves the
    // owner record in the Raft state machine until natural lease expiry; with
    // the default 60s lease the partition is unrecoverable for that window
    // even though the topic is gone, and the metric label below claims a
    // release happened.
    ctx.lease_cache.remove(&key);
    if let Err(e) = ctx.coordinator.release_partition(topic, partition).await {
        warn!(
            ctx.broker_id,
            topic,
            partition,
            error = %e,
            "Failed to release coordinator lease for deleted topic - lease will expire naturally"
        );
    }
    crate::cluster::metrics::record_lease_operation("release", "topic_deleted");
    crate::cluster::metrics::OWNED_PARTITIONS
        .with_label_values(&[topic])
        .dec();
}

