//! PartitionTransferCoordinator implementation for RaftCoordinator.
//!
//! This module handles:
//! - Single-partition transfers within one shard
//! - Batch transfers within one shard (cross-shard batches are out of scope)
//! - Broker failure marking — fences the broker on control AND fans out a
//!   failure-marker command to every shard so each shard can release the
//!   partitions it owns for that broker locally
//!
//! Per-partition lease state lives in the shard owning `hash(topic) %
//! metadata_shards`, so every transfer routes by `topic`.

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use super::super::commands::{ShardCommand, ShardResponse};
use super::super::domains::{
    BrokerStatus, PartitionTransfer, TransferCommand, TransferReason, TransferResponse,
};
use super::{RaftCoordinator, current_time_ms};

use crate::cluster::PartitionKey;
use crate::cluster::error::{SlateDBError, SlateDBResult};
use crate::cluster::traits::PartitionTransferCoordinator;

#[async_trait]
impl PartitionTransferCoordinator for RaftCoordinator {
    /// Transfer ownership of a partition from one broker to another.
    ///
    /// Single-shard. Cross-shard transfers (rare; would imply moving a
    /// topic to a different shard) are out of scope per the sharding plan
    /// — every partition of a topic lives on one shard, so a same-topic
    /// transfer always lands on the same shard.
    async fn transfer_partition(
        &self,
        topic: &str,
        partition: i32,
        from_broker_id: i32,
        to_broker_id: i32,
        reason: TransferReason,
        lease_duration_ms: u64,
    ) -> SlateDBResult<()> {
        let command = ShardCommand::Transfer(TransferCommand::TransferPartition {
            topic: topic.to_string(),
            partition,
            from_broker_id,
            to_broker_id,
            reason,
            lease_duration_ms,
            timestamp_ms: current_time_ms(),
        });

        let response = self.cluster().write_shard_for_topic(topic, command).await?;
        match response {
            ShardResponse::Transfer(TransferResponse::PartitionTransferred { .. }) => {
                // Update cache to reflect new ownership.
                let key = (Arc::from(topic), partition);
                let stamp = self.owner_cache_read_stamp(&key);
                self.owner_cache_insert_if_fresh(key, to_broker_id, stamp)
                    .await;
                Ok(())
            }
            ShardResponse::Transfer(TransferResponse::TransferSourceNotOwner {
                topic,
                partition,
                expected_owner,
                actual_owner,
            }) => Err(SlateDBError::Storage(format!(
                "Transfer failed: partition {}/{} expected owner {} but actual owner is {:?}",
                topic, partition, expected_owner, actual_owner
            ))),
            ShardResponse::Transfer(TransferResponse::TransferDestinationUnavailable {
                topic,
                partition,
                broker_id,
            }) => Err(SlateDBError::Storage(format!(
                "Transfer failed: destination broker {} unavailable for {}/{}",
                broker_id, topic, partition
            ))),
            ShardResponse::Transfer(TransferResponse::PartitionNotFoundForTransfer {
                topic,
                partition,
            }) => Err(SlateDBError::Storage(format!(
                "Transfer failed: partition {}/{} not found",
                topic, partition
            ))),
            other => Err(SlateDBError::Storage(format!(
                "Unexpected transfer response: {:?}",
                other
            ))),
        }
    }

    /// Transfer multiple partitions atomically as a batch.
    ///
    /// Batches are sharded by topic — partitions sharing a shard ride one
    /// proposal; cross-shard batches submit one proposal per shard and
    /// surface partial results. The legacy single-group invariant
    /// "atomicity across the whole batch" only ever held within one Raft
    /// log; with N shards a true cluster-wide atomic batch would need a
    /// 2PC layer that the plan explicitly defers.
    async fn batch_transfer_partitions(
        &self,
        transfers: Vec<PartitionTransfer>,
        reason: TransferReason,
        lease_duration_ms: u64,
    ) -> SlateDBResult<(usize, Vec<(String, i32, String)>)> {
        // Group by destination shard so each shard sees one proposal.
        let mut by_shard: HashMap<u16, Vec<PartitionTransfer>> = HashMap::new();
        for t in &transfers {
            let id = self.cluster().router().shard_for_topic(&t.topic);
            by_shard.entry(id).or_default().push(t.clone());
        }

        let mut total_successful = 0usize;
        let mut total_failed: Vec<(String, i32, String)> = Vec::new();

        // Counter layout matches the legacy single-shard path: broker_id
        // (top 16 bits) | per-call monotonic (bottom 48 bits). Burned once
        // here, propagated to each per-shard proposal so a follower
        // applying the entries can correlate them by transaction id.
        let counter = self
            .transaction_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let timestamp_ms = current_time_ms();
        let transaction_id =
            ((self.broker_id as u64 & 0xFFFF) << 48) | (counter & 0x0000_FFFF_FFFF_FFFF);

        for (shard_id, shard_transfers) in by_shard {
            let command = ShardCommand::Transfer(TransferCommand::BatchTransferPartitions {
                transfers: shard_transfers.clone(),
                transaction_id,
                reason: reason.clone(),
                lease_duration_ms,
                timestamp_ms,
            });
            let response = self.cluster().write_shard(shard_id, command).await?;
            match response {
                ShardResponse::Transfer(TransferResponse::BatchTransferCompleted {
                    successful_transfers,
                    failed_transfers,
                    ..
                }) => {
                    // Update cache for successful transfers in this batch.
                    for transfer in &shard_transfers {
                        let key: PartitionKey =
                            (Arc::from(transfer.topic.as_str()), transfer.partition);
                        let is_failed = failed_transfers
                            .iter()
                            .any(|(t, p, _)| t == &transfer.topic && *p == transfer.partition);
                        if !is_failed {
                            let stamp = self.owner_cache_read_stamp(&key);
                            self.owner_cache_insert_if_fresh(key, transfer.to_broker_id, stamp)
                                .await;
                        }
                    }
                    total_successful += successful_transfers;
                    total_failed.extend(failed_transfers);
                }
                other => {
                    return Err(SlateDBError::Storage(format!(
                        "Unexpected batch transfer response on shard {}: {:?}",
                        shard_id, other
                    )));
                }
            }
        }
        Ok((total_successful, total_failed))
    }

    /// Mark a broker as failed and track it in the cluster state.
    ///
    /// Two-phase fan-out:
    /// 1. Fence the broker on the **control** group via the broker domain.
    ///    This is the authoritative liveness change; the reconciler (step 7)
    ///    will propagate the new status into every shard's liveness shadow.
    /// 2. Issue `MarkBrokerFailed` to every shard so each shard releases the
    ///    partitions it owns for that broker locally and bumps their leader
    ///    epochs. The combined `released_partitions` list is the union
    ///    across shards.
    ///
    /// The fan-out is the natural consequence of partitions being sharded by
    /// topic: a single broker may own partitions across all N shards.
    async fn mark_broker_failed(
        &self,
        broker_id: i32,
        reason: &str,
    ) -> SlateDBResult<Vec<(String, i32)>> {
        let detected_at_ms = current_time_ms();

        // Phase 1: fence on control. The broker domain owns liveness.
        // Don't `?` the result — even if the control commit reports
        // BrokerNotFound (the broker was never registered or already
        // unregistered) we still want to attempt the per-shard cleanup so
        // any stale leases tied to that id get released.
        let _control_resp = self
            .cluster()
            .write_control(super::super::commands::ControlCommand::Broker(
                super::super::domains::BrokerCommand::Fence {
                    broker_id,
                    reason: reason.to_string(),
                },
            ))
            .await;

        // Phase 2: fan out the per-shard cleanup. Each shard's local
        // partitions get released and their leader epochs bumped; the
        // FSM also remembers the failure so a duplicate fan-out doesn't
        // double-bump epochs.
        let mut released: Vec<(String, i32)> = Vec::new();
        for shard_idx in 0..self.cluster().metadata_shards() {
            let command = ShardCommand::Transfer(TransferCommand::MarkBrokerFailed {
                broker_id,
                detected_at_ms,
                reason: reason.to_string(),
            });
            let response = self.cluster().write_shard(shard_idx, command).await?;
            match response {
                ShardResponse::Transfer(TransferResponse::BrokerMarkedFailed {
                    released_partitions,
                    ..
                }) => {
                    released.extend(released_partitions);
                }
                ShardResponse::Transfer(TransferResponse::BrokerAlreadyFailed { .. }) => {}
                other => {
                    return Err(SlateDBError::Storage(format!(
                        "Unexpected mark_broker_failed response on shard {}: {:?}",
                        shard_idx, other
                    )));
                }
            }
        }

        // Stale ownership entries for this broker may exist for any
        // partition; bulk-invalidate rather than walking the cache.
        self.owner_cache_invalidate_all().await;
        Ok(released)
    }

    /// Get all partition owners as a map from (topic, partition) to broker_id.
    ///
    /// Read fans across every shard's `partition_state` map. Each shard's
    /// `lease_clock_ms` is independent; we use *that shard's* clock to gate
    /// expiry, so a clock-skewed shard doesn't disqualify another shard's
    /// owners.
    async fn get_all_partition_owners(&self) -> HashMap<(String, i32), i32> {
        let mut owners = HashMap::new();
        for shard in self.cluster().shards() {
            let sm = shard.state_machine();
            let state = sm.state().await;
            let now = state.lease_clock_ms;
            for ((topic, partition), partition_state) in &state.partition_state.partitions {
                if partition_state.lease_expires_at_ms > now
                    && let Some(owner) = partition_state.owner_broker_id
                {
                    owners.insert((topic.to_string(), *partition), owner);
                }
            }
        }
        owners
    }

    /// Get all partition owners along with the Raft commit index.
    ///
    /// The "commit index" returned here is the **control** group's last
    /// applied index. With N+1 logs there's no single global commit index;
    /// callers using this for cache-staleness comparisons should treat it as
    /// the controller's view (which is what the legacy callers wanted —
    /// failover decisions hinge on the controller view, not the shards').
    async fn get_all_partition_owners_with_index(&self) -> (HashMap<(String, i32), i32>, u64) {
        let owners = self.get_all_partition_owners().await;
        let metrics = self.cluster().control().raft().metrics().borrow().clone();
        let commit_index = metrics.last_applied.map(|log_id| log_id.index).unwrap_or(0);
        (owners, commit_index)
    }

    /// Get list of active broker IDs from the control group's broker domain.
    async fn get_active_broker_ids(&self) -> Vec<i32> {
        let sm = self.cluster().control().state_machine();
        let state = sm.state().await;

        let now = current_time_ms();
        let ttl_ms = self.config.broker_heartbeat_ttl.as_millis() as u64;

        state
            .broker_domain
            .brokers
            .values()
            .filter(|b| b.status == BrokerStatus::Active && now - b.last_heartbeat_ms < ttl_ms)
            .map(|b| b.broker_id)
            .collect()
    }
}
