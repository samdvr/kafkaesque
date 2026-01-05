//! PartitionTransferCoordinator implementation for RaftCoordinator.
//!
//! This module handles:
//! - Partition ownership transfers between brokers
//! - Batch transfers for failover
//! - Broker failure tracking

use std::collections::HashMap;
use std::sync::Arc;

use async_trait::async_trait;

use super::super::commands::{CoordinationCommand, CoordinationResponse};
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
    async fn transfer_partition(
        &self,
        topic: &str,
        partition: i32,
        from_broker_id: i32,
        to_broker_id: i32,
        reason: TransferReason,
        lease_duration_ms: u64,
    ) -> SlateDBResult<()> {
        let command = CoordinationCommand::TransferDomain(TransferCommand::TransferPartition {
            topic: topic.to_string(),
            partition,
            from_broker_id,
            to_broker_id,
            reason,
            lease_duration_ms,
            timestamp_ms: current_time_ms(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::TransferDomainResponse(
                TransferResponse::PartitionTransferred { .. },
            ) => {
                // Update cache to reflect new ownership
                self.owner_cache
                    .insert((Arc::from(topic), partition), to_broker_id)
                    .await;
                Ok(())
            }
            CoordinationResponse::TransferDomainResponse(
                TransferResponse::TransferSourceNotOwner {
                    topic,
                    partition,
                    expected_owner,
                    actual_owner,
                },
            ) => Err(SlateDBError::Storage(format!(
                "Transfer failed: partition {}/{} expected owner {} but actual owner is {:?}",
                topic, partition, expected_owner, actual_owner
            ))),
            CoordinationResponse::TransferDomainResponse(
                TransferResponse::TransferDestinationUnavailable {
                    topic,
                    partition,
                    broker_id,
                },
            ) => Err(SlateDBError::Storage(format!(
                "Transfer failed: destination broker {} unavailable for {}/{}",
                broker_id, topic, partition
            ))),
            CoordinationResponse::TransferDomainResponse(
                TransferResponse::PartitionNotFoundForTransfer { topic, partition },
            ) => Err(SlateDBError::Storage(format!(
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
    async fn batch_transfer_partitions(
        &self,
        transfers: Vec<PartitionTransfer>,
        reason: TransferReason,
        lease_duration_ms: u64,
    ) -> SlateDBResult<(usize, Vec<(String, i32, String)>)> {
        // Generate a unique transaction ID using broker_id + counter + timestamp
        let counter = self
            .transaction_counter
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let timestamp_ms = current_time_ms();
        // Format: broker_id (16 bits) | counter (24 bits) | timestamp_ms lower bits (24 bits)
        let transaction_id = ((self.broker_id as u64 & 0xFFFF) << 48)
            | ((counter & 0xFFFFFF) << 24)
            | (timestamp_ms & 0xFFFFFF);

        let command =
            CoordinationCommand::TransferDomain(TransferCommand::BatchTransferPartitions {
                transfers: transfers.clone(),
                transaction_id,
                reason,
                lease_duration_ms,
                timestamp_ms,
            });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::TransferDomainResponse(
                TransferResponse::BatchTransferCompleted {
                    successful_transfers,
                    failed_transfers,
                    ..
                },
            ) => {
                // Update cache for successful transfers
                for transfer in &transfers {
                    let key: PartitionKey =
                        (Arc::from(transfer.topic.as_str()), transfer.partition);
                    let is_failed = failed_transfers
                        .iter()
                        .any(|(t, p, _)| t == &transfer.topic && *p == transfer.partition);
                    if !is_failed {
                        self.owner_cache.insert(key, transfer.to_broker_id).await;
                    }
                }
                Ok((successful_transfers, failed_transfers))
            }
            other => Err(SlateDBError::Storage(format!(
                "Unexpected batch transfer response: {:?}",
                other
            ))),
        }
    }

    /// Mark a broker as failed and track it in the cluster state.
    async fn mark_broker_failed(&self, broker_id: i32, reason: &str) -> SlateDBResult<usize> {
        let detected_at_ms = current_time_ms();
        let command = CoordinationCommand::TransferDomain(TransferCommand::MarkBrokerFailed {
            broker_id,
            detected_at_ms,
            reason: reason.to_string(),
        });

        let response = self.node.write(command).await?;
        match response {
            CoordinationResponse::TransferDomainResponse(
                TransferResponse::BrokerMarkedFailed {
                    partitions_affected,
                    ..
                },
            ) => {
                // Invalidate any cached ownership for this broker
                self.owner_cache.invalidate_all();
                self.owner_cache.run_pending_tasks().await;
                Ok(partitions_affected)
            }
            CoordinationResponse::TransferDomainResponse(
                TransferResponse::BrokerAlreadyFailed { .. },
            ) => Ok(0),
            CoordinationResponse::BrokerDomainResponse(_) => Ok(0), // Handle BrokerNotFound from broker domain
            other => Err(SlateDBError::Storage(format!(
                "Unexpected mark_broker_failed response: {:?}",
                other
            ))),
        }
    }

    /// Get all partition owners as a map from (topic, partition) to broker_id.
    async fn get_all_partition_owners(&self) -> HashMap<(String, i32), i32> {
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let now = current_time_ms();
        let mut owners = HashMap::new();

        for ((topic, partition), partition_state) in &inner_state.partition_domain.partitions {
            // Only include partitions with valid (non-expired) leases
            if partition_state.lease_expires_at_ms > now
                && let Some(owner) = partition_state.owner_broker_id
            {
                owners.insert((topic.to_string(), *partition), owner);
            }
        }

        owners
    }

    /// Get all partition owners along with the Raft commit index.
    async fn get_all_partition_owners_with_index(&self) -> (HashMap<(String, i32), i32>, u64) {
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        // Get the commit index from Raft metrics
        let metrics = self.node.metrics();
        let commit_index = metrics.last_applied.map(|log_id| log_id.index).unwrap_or(0);

        let now = current_time_ms();
        let mut owners = HashMap::new();

        for ((topic, partition), partition_state) in &inner_state.partition_domain.partitions {
            if partition_state.lease_expires_at_ms > now
                && let Some(owner) = partition_state.owner_broker_id
            {
                owners.insert((topic.to_string(), *partition), owner);
            }
        }

        (owners, commit_index)
    }

    /// Get list of active broker IDs.
    async fn get_active_broker_ids(&self) -> Vec<i32> {
        let state_machine = self.node.state_machine();
        let state = state_machine.read().await;
        let inner_state = state.state().await;

        let now = current_time_ms();
        let ttl_ms = self.config.broker_heartbeat_ttl.as_millis() as u64;

        inner_state
            .broker_domain
            .brokers
            .values()
            .filter(|b| b.status == BrokerStatus::Active && now - b.last_heartbeat_ms < ttl_ms)
            .map(|b| b.broker_id)
            .collect()
    }
}
