//! Transfer domain for the Raft state machine.
//!
//! Handles fast failover and batch partition transfers.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::broker::BrokerStatus;
use super::partition::PartitionInfo;

/// Type alias for partition lookup function to reduce type complexity.
pub type GetPartitionFn<'a> = Box<dyn Fn(&str, i32) -> Option<PartitionInfo> + 'a>;

/// Type alias for partition update function to reduce type complexity.
/// Parameters: topic, partition, owner_broker_id, leader_epoch, lease_expires_at_ms
pub type UpdatePartitionFn<'a> = Box<dyn FnMut(&str, i32, Option<i32>, i32, u64) + 'a>;

/// Reason for transferring partition ownership.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransferReason {
    /// Broker failed and partitions need redistribution.
    BrokerFailure,
    /// Load-based rebalancing decision.
    LoadBalancing,
    /// Manual operator-triggered rebalance.
    ManualRebalance,
    /// Broker is shutting down gracefully.
    Shutdown,
}

/// A single partition transfer operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct PartitionTransfer {
    pub topic: String,
    pub partition: i32,
    pub from_broker_id: i32,
    pub to_broker_id: i32,
    /// Expected leader epoch for validation.
    #[serde(default)]
    pub expected_leader_epoch: Option<i32>,
}

/// Information about a failed broker.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct FailedBrokerInfo {
    pub broker_id: i32,
    pub failed_at_ms: u64,
    pub reason: String,
    pub partitions_reassigned: bool,
}

/// State of a batch transfer operation.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub struct BatchTransferState {
    pub transaction_id: u64,
    pub transfers: Vec<PartitionTransfer>,
    pub started_at_ms: u64,
    pub completed_count: usize,
}

/// Commands for the transfer domain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransferCommand {
    /// Transfer partition ownership from one broker to another.
    TransferPartition {
        topic: String,
        partition: i32,
        from_broker_id: i32,
        to_broker_id: i32,
        reason: TransferReason,
        lease_duration_ms: u64,
        timestamp_ms: u64,
    },

    /// Mark a broker as failed and trigger partition redistribution.
    MarkBrokerFailed {
        broker_id: i32,
        detected_at_ms: u64,
        reason: String,
    },

    /// Transfer multiple partitions atomically as a batch.
    BatchTransferPartitions {
        transfers: Vec<PartitionTransfer>,
        transaction_id: u64,
        reason: TransferReason,
        lease_duration_ms: u64,
        timestamp_ms: u64,
    },
}

/// Responses from transfer domain operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransferResponse {
    /// Partition was transferred.
    PartitionTransferred {
        topic: String,
        partition: i32,
        from_broker_id: i32,
        to_broker_id: i32,
        new_epoch: i32,
        lease_expires_at_ms: u64,
    },

    /// Transfer destination broker is unavailable.
    TransferDestinationUnavailable {
        topic: String,
        partition: i32,
        broker_id: i32,
    },

    /// Transfer source broker doesn't own the partition.
    TransferSourceNotOwner {
        topic: String,
        partition: i32,
        expected_owner: i32,
        actual_owner: Option<i32>,
    },

    /// Partition not found for transfer.
    PartitionNotFoundForTransfer { topic: String, partition: i32 },

    /// Broker was marked as failed.
    BrokerMarkedFailed {
        broker_id: i32,
        partitions_affected: usize,
    },

    /// Broker was already marked as failed.
    BrokerAlreadyFailed { broker_id: i32 },

    /// Batch transfer completed.
    BatchTransferCompleted {
        transaction_id: u64,
        successful_transfers: usize,
        failed_transfers: Vec<(String, i32, String)>,
    },
}

/// State for the transfer domain.
///
/// Note: This domain depends on broker and partition state for validation.
/// The apply method takes references to those states for read-only access.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct TransferDomainState {
    /// Failed brokers with their failure information.
    pub failed_brokers: HashMap<i32, FailedBrokerInfo>,

    /// Pending batch transfers for atomic multi-partition operations.
    pub pending_transfers: HashMap<u64, BatchTransferState>,
}

/// Context required for transfer operations.
pub struct TransferContext<'a> {
    /// Check if a broker is active.
    pub is_broker_active: Box<dyn Fn(i32) -> bool + 'a>,
    /// Get partition info.
    pub get_partition: GetPartitionFn<'a>,
    /// Update partition ownership.
    pub update_partition: UpdatePartitionFn<'a>,
}

impl TransferDomainState {
    /// Create a new empty transfer state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a transfer command and return the response.
    ///
    /// # Arguments
    /// * `cmd` - The transfer command to apply
    /// * `is_broker_active` - Function to check if a broker is active
    /// * `partitions` - Mutable reference to partition state for updates
    /// * `broker_check` - Function to get broker status
    pub fn apply_with_context<F, G>(
        &mut self,
        cmd: TransferCommand,
        is_broker_active: F,
        partitions: &mut HashMap<(Arc<str>, i32), PartitionInfo>,
        broker_status: G,
    ) -> TransferResponse
    where
        F: Fn(i32) -> bool,
        G: Fn(i32) -> Option<BrokerStatus>,
    {
        match cmd {
            TransferCommand::TransferPartition {
                topic,
                partition,
                from_broker_id,
                to_broker_id,
                reason: _,
                lease_duration_ms,
                timestamp_ms,
            } => {
                let topic_arc: Arc<str> = Arc::from(topic.as_str());
                let key = (Arc::clone(&topic_arc), partition);

                if !is_broker_active(to_broker_id) {
                    return TransferResponse::TransferDestinationUnavailable {
                        topic,
                        partition,
                        broker_id: to_broker_id,
                    };
                }

                if let Some(partition_state) = partitions.get_mut(&key) {
                    if partition_state.owner_broker_id != Some(from_broker_id) {
                        return TransferResponse::TransferSourceNotOwner {
                            topic,
                            partition,
                            expected_owner: from_broker_id,
                            actual_owner: partition_state.owner_broker_id,
                        };
                    }

                    partition_state.owner_broker_id = Some(to_broker_id);
                    partition_state.leader_epoch += 1;
                    partition_state.lease_expires_at_ms = timestamp_ms + lease_duration_ms;

                    TransferResponse::PartitionTransferred {
                        topic,
                        partition,
                        from_broker_id,
                        to_broker_id,
                        new_epoch: partition_state.leader_epoch,
                        lease_expires_at_ms: partition_state.lease_expires_at_ms,
                    }
                } else {
                    TransferResponse::PartitionNotFoundForTransfer { topic, partition }
                }
            }

            TransferCommand::MarkBrokerFailed {
                broker_id,
                detected_at_ms,
                reason,
            } => {
                if self.failed_brokers.contains_key(&broker_id) {
                    return TransferResponse::BrokerAlreadyFailed { broker_id };
                }

                let partitions_affected = partitions
                    .values()
                    .filter(|p| p.owner_broker_id == Some(broker_id))
                    .count();

                self.failed_brokers.insert(
                    broker_id,
                    FailedBrokerInfo {
                        broker_id,
                        failed_at_ms: detected_at_ms,
                        reason,
                        partitions_reassigned: false,
                    },
                );

                TransferResponse::BrokerMarkedFailed {
                    broker_id,
                    partitions_affected,
                }
            }

            TransferCommand::BatchTransferPartitions {
                transfers,
                transaction_id,
                reason: _,
                lease_duration_ms,
                timestamp_ms,
            } => {
                let mut successful_transfers = 0;
                let mut failed_transfers = Vec::new();

                // Store pending transfer state
                self.pending_transfers.insert(
                    transaction_id,
                    BatchTransferState {
                        transaction_id,
                        transfers: transfers.clone(),
                        started_at_ms: timestamp_ms,
                        completed_count: 0,
                    },
                );

                // Execute each transfer
                for transfer in transfers {
                    let topic_arc: Arc<str> = Arc::from(transfer.topic.as_str());
                    let key = (Arc::clone(&topic_arc), transfer.partition);

                    let dest_ok = broker_status(transfer.to_broker_id)
                        .is_some_and(|s| s == BrokerStatus::Active);

                    if !dest_ok {
                        failed_transfers.push((
                            transfer.topic.clone(),
                            transfer.partition,
                            format!("Destination broker {} unavailable", transfer.to_broker_id),
                        ));
                        continue;
                    }

                    if let Some(partition_state) = partitions.get_mut(&key) {
                        if let Some(expected_epoch) = transfer.expected_leader_epoch
                            && partition_state.leader_epoch != expected_epoch
                        {
                            failed_transfers.push((
                                transfer.topic.clone(),
                                transfer.partition,
                                format!(
                                    "Leader epoch mismatch - expected {}, actual {}",
                                    expected_epoch, partition_state.leader_epoch
                                ),
                            ));
                            continue;
                        }

                        // For batch transfers during failover, allow transfers even if
                        // source broker no longer owns (they may have already failed)
                        if partition_state.owner_broker_id == Some(transfer.from_broker_id)
                            || partition_state.owner_broker_id.is_none()
                        {
                            partition_state.owner_broker_id = Some(transfer.to_broker_id);
                            partition_state.leader_epoch += 1;
                            partition_state.lease_expires_at_ms = timestamp_ms + lease_duration_ms;
                            successful_transfers += 1;
                        } else {
                            failed_transfers.push((
                                transfer.topic.clone(),
                                transfer.partition,
                                format!(
                                    "Partition owned by broker {:?}, expected {}",
                                    partition_state.owner_broker_id, transfer.from_broker_id
                                ),
                            ));
                        }
                    } else {
                        failed_transfers.push((
                            transfer.topic.clone(),
                            transfer.partition,
                            "Partition not found".to_string(),
                        ));
                    }
                }

                if let Some(pending) = self.pending_transfers.get_mut(&transaction_id) {
                    pending.completed_count = successful_transfers;
                }

                self.pending_transfers.remove(&transaction_id);

                TransferResponse::BatchTransferCompleted {
                    transaction_id,
                    successful_transfers,
                    failed_transfers,
                }
            }
        }
    }

    /// Check if a broker is marked as failed.
    pub fn is_broker_failed(&self, broker_id: i32) -> bool {
        self.failed_brokers.contains_key(&broker_id)
    }

    /// Get failed broker info.
    pub fn get_failed_broker(&self, broker_id: i32) -> Option<&FailedBrokerInfo> {
        self.failed_brokers.get(&broker_id)
    }

    /// Clear failed broker status (e.g., after recovery).
    pub fn clear_failed_broker(&mut self, broker_id: i32) {
        self.failed_brokers.remove(&broker_id);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn create_partition_info(topic: &str, partition: i32, owner: Option<i32>) -> PartitionInfo {
        PartitionInfo {
            topic: Arc::from(topic),
            partition,
            owner_broker_id: owner,
            leader_epoch: 1,
            lease_expires_at_ms: 0,
        }
    }

    // ========================================================================
    // TransferReason Tests
    // ========================================================================

    #[test]
    fn test_transfer_reason_variants() {
        let reasons = [
            TransferReason::BrokerFailure,
            TransferReason::LoadBalancing,
            TransferReason::ManualRebalance,
            TransferReason::Shutdown,
        ];

        for reason in &reasons {
            let debug_str = format!("{:?}", reason);
            assert!(!debug_str.is_empty());
        }
    }

    #[test]
    fn test_transfer_reason_equality() {
        assert_eq!(TransferReason::BrokerFailure, TransferReason::BrokerFailure);
        assert_ne!(TransferReason::BrokerFailure, TransferReason::LoadBalancing);
        assert_ne!(TransferReason::ManualRebalance, TransferReason::Shutdown);
    }

    #[test]
    fn test_transfer_reason_clone() {
        let reason = TransferReason::LoadBalancing;
        let cloned = reason.clone();
        assert_eq!(reason, cloned);
    }

    #[test]
    fn test_transfer_reason_serialization() {
        let reason = TransferReason::BrokerFailure;
        let serialized = bincode::serialize(&reason).unwrap();
        let deserialized: TransferReason = bincode::deserialize(&serialized).unwrap();
        assert_eq!(reason, deserialized);
    }

    // ========================================================================
    // PartitionTransfer Tests
    // ========================================================================

    #[test]
    fn test_partition_transfer_creation() {
        let transfer = PartitionTransfer {
            topic: "test-topic".to_string(),
            partition: 5,
            from_broker_id: 1,
            to_broker_id: 2,
            expected_leader_epoch: Some(3),
        };

        assert_eq!(transfer.topic, "test-topic");
        assert_eq!(transfer.partition, 5);
        assert_eq!(transfer.from_broker_id, 1);
        assert_eq!(transfer.to_broker_id, 2);
        assert_eq!(transfer.expected_leader_epoch, Some(3));
    }

    #[test]
    fn test_partition_transfer_without_epoch() {
        let transfer = PartitionTransfer {
            topic: "topic".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 2,
            expected_leader_epoch: None,
        };

        assert!(transfer.expected_leader_epoch.is_none());
    }

    #[test]
    fn test_partition_transfer_clone() {
        let transfer = PartitionTransfer {
            topic: "topic".to_string(),
            partition: 1,
            from_broker_id: 1,
            to_broker_id: 2,
            expected_leader_epoch: Some(5),
        };

        let cloned = transfer.clone();
        assert_eq!(transfer, cloned);
    }

    #[test]
    fn test_partition_transfer_debug() {
        let transfer = PartitionTransfer {
            topic: "topic".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 2,
            expected_leader_epoch: None,
        };

        let debug_str = format!("{:?}", transfer);
        assert!(debug_str.contains("PartitionTransfer"));
        assert!(debug_str.contains("topic"));
    }

    #[test]
    fn test_partition_transfer_serialization() {
        let transfer = PartitionTransfer {
            topic: "test".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 2,
            expected_leader_epoch: Some(1),
        };

        let serialized = bincode::serialize(&transfer).unwrap();
        let deserialized: PartitionTransfer = bincode::deserialize(&serialized).unwrap();
        assert_eq!(transfer, deserialized);
    }

    // ========================================================================
    // FailedBrokerInfo Tests
    // ========================================================================

    #[test]
    fn test_failed_broker_info_creation() {
        let info = FailedBrokerInfo {
            broker_id: 1,
            failed_at_ms: 123456,
            reason: "heartbeat_timeout".to_string(),
            partitions_reassigned: false,
        };

        assert_eq!(info.broker_id, 1);
        assert_eq!(info.failed_at_ms, 123456);
        assert_eq!(info.reason, "heartbeat_timeout");
        assert!(!info.partitions_reassigned);
    }

    #[test]
    fn test_failed_broker_info_clone() {
        let info = FailedBrokerInfo {
            broker_id: 5,
            failed_at_ms: 1000,
            reason: "test".to_string(),
            partitions_reassigned: true,
        };

        let cloned = info.clone();
        assert_eq!(info, cloned);
    }

    #[test]
    fn test_failed_broker_info_serialization() {
        let info = FailedBrokerInfo {
            broker_id: 1,
            failed_at_ms: 1000,
            reason: "test".to_string(),
            partitions_reassigned: false,
        };

        let serialized = bincode::serialize(&info).unwrap();
        let deserialized: FailedBrokerInfo = bincode::deserialize(&serialized).unwrap();
        assert_eq!(info, deserialized);
    }

    // ========================================================================
    // BatchTransferState Tests
    // ========================================================================

    #[test]
    fn test_batch_transfer_state_creation() {
        let state = BatchTransferState {
            transaction_id: 12345,
            transfers: vec![],
            started_at_ms: 1000,
            completed_count: 0,
        };

        assert_eq!(state.transaction_id, 12345);
        assert!(state.transfers.is_empty());
        assert_eq!(state.started_at_ms, 1000);
        assert_eq!(state.completed_count, 0);
    }

    #[test]
    fn test_batch_transfer_state_with_transfers() {
        let transfers = vec![
            PartitionTransfer {
                topic: "t1".to_string(),
                partition: 0,
                from_broker_id: 1,
                to_broker_id: 2,
                expected_leader_epoch: None,
            },
            PartitionTransfer {
                topic: "t1".to_string(),
                partition: 1,
                from_broker_id: 1,
                to_broker_id: 2,
                expected_leader_epoch: None,
            },
        ];

        let state = BatchTransferState {
            transaction_id: 1,
            transfers: transfers.clone(),
            started_at_ms: 1000,
            completed_count: 1,
        };

        assert_eq!(state.transfers.len(), 2);
        assert_eq!(state.completed_count, 1);
    }

    #[test]
    fn test_batch_transfer_state_serialization() {
        let state = BatchTransferState {
            transaction_id: 100,
            transfers: vec![],
            started_at_ms: 5000,
            completed_count: 0,
        };

        let serialized = bincode::serialize(&state).unwrap();
        let deserialized: BatchTransferState = bincode::deserialize(&serialized).unwrap();
        assert_eq!(state, deserialized);
    }

    // ========================================================================
    // TransferCommand Tests
    // ========================================================================

    #[test]
    fn test_transfer_command_transfer_partition() {
        let cmd = TransferCommand::TransferPartition {
            topic: "test".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 2,
            reason: TransferReason::LoadBalancing,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        };

        match cmd {
            TransferCommand::TransferPartition {
                topic, partition, ..
            } => {
                assert_eq!(topic, "test");
                assert_eq!(partition, 0);
            }
            _ => panic!("Expected TransferPartition"),
        }
    }

    #[test]
    fn test_transfer_command_mark_broker_failed() {
        let cmd = TransferCommand::MarkBrokerFailed {
            broker_id: 5,
            detected_at_ms: 123456,
            reason: "heartbeat_timeout".to_string(),
        };

        match cmd {
            TransferCommand::MarkBrokerFailed {
                broker_id, reason, ..
            } => {
                assert_eq!(broker_id, 5);
                assert_eq!(reason, "heartbeat_timeout");
            }
            _ => panic!("Expected MarkBrokerFailed"),
        }
    }

    #[test]
    fn test_transfer_command_batch_transfer() {
        let cmd = TransferCommand::BatchTransferPartitions {
            transfers: vec![],
            transaction_id: 999,
            reason: TransferReason::BrokerFailure,
            lease_duration_ms: 30000,
            timestamp_ms: 2000,
        };

        match cmd {
            TransferCommand::BatchTransferPartitions { transaction_id, .. } => {
                assert_eq!(transaction_id, 999);
            }
            _ => panic!("Expected BatchTransferPartitions"),
        }
    }

    #[test]
    fn test_transfer_command_serialization() {
        let cmd = TransferCommand::TransferPartition {
            topic: "test".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 2,
            reason: TransferReason::Shutdown,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        };

        let serialized = bincode::serialize(&cmd).unwrap();
        let deserialized: TransferCommand = bincode::deserialize(&serialized).unwrap();
        assert_eq!(cmd, deserialized);
    }

    // ========================================================================
    // TransferResponse Tests
    // ========================================================================

    #[test]
    fn test_transfer_response_partition_transferred() {
        let response = TransferResponse::PartitionTransferred {
            topic: "test".to_string(),
            partition: 0,
            from_broker_id: 1,
            to_broker_id: 2,
            new_epoch: 5,
            lease_expires_at_ms: 100000,
        };

        match response {
            TransferResponse::PartitionTransferred { new_epoch, .. } => {
                assert_eq!(new_epoch, 5);
            }
            _ => panic!("Expected PartitionTransferred"),
        }
    }

    #[test]
    fn test_transfer_response_destination_unavailable() {
        let response = TransferResponse::TransferDestinationUnavailable {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 3,
        };

        match response {
            TransferResponse::TransferDestinationUnavailable { broker_id, .. } => {
                assert_eq!(broker_id, 3);
            }
            _ => panic!("Expected TransferDestinationUnavailable"),
        }
    }

    #[test]
    fn test_transfer_response_source_not_owner() {
        let response = TransferResponse::TransferSourceNotOwner {
            topic: "test".to_string(),
            partition: 0,
            expected_owner: 1,
            actual_owner: Some(2),
        };

        match response {
            TransferResponse::TransferSourceNotOwner {
                expected_owner,
                actual_owner,
                ..
            } => {
                assert_eq!(expected_owner, 1);
                assert_eq!(actual_owner, Some(2));
            }
            _ => panic!("Expected TransferSourceNotOwner"),
        }
    }

    #[test]
    fn test_transfer_response_partition_not_found() {
        let response = TransferResponse::PartitionNotFoundForTransfer {
            topic: "test".to_string(),
            partition: 99,
        };

        match response {
            TransferResponse::PartitionNotFoundForTransfer { partition, .. } => {
                assert_eq!(partition, 99);
            }
            _ => panic!("Expected PartitionNotFoundForTransfer"),
        }
    }

    #[test]
    fn test_transfer_response_broker_marked_failed() {
        let response = TransferResponse::BrokerMarkedFailed {
            broker_id: 1,
            partitions_affected: 10,
        };

        match response {
            TransferResponse::BrokerMarkedFailed {
                broker_id,
                partitions_affected,
            } => {
                assert_eq!(broker_id, 1);
                assert_eq!(partitions_affected, 10);
            }
            _ => panic!("Expected BrokerMarkedFailed"),
        }
    }

    #[test]
    fn test_transfer_response_broker_already_failed() {
        let response = TransferResponse::BrokerAlreadyFailed { broker_id: 5 };

        match response {
            TransferResponse::BrokerAlreadyFailed { broker_id } => {
                assert_eq!(broker_id, 5);
            }
            _ => panic!("Expected BrokerAlreadyFailed"),
        }
    }

    #[test]
    fn test_transfer_response_batch_completed() {
        let response = TransferResponse::BatchTransferCompleted {
            transaction_id: 123,
            successful_transfers: 8,
            failed_transfers: vec![("topic".to_string(), 0, "error".to_string())],
        };

        match response {
            TransferResponse::BatchTransferCompleted {
                successful_transfers,
                failed_transfers,
                ..
            } => {
                assert_eq!(successful_transfers, 8);
                assert_eq!(failed_transfers.len(), 1);
            }
            _ => panic!("Expected BatchTransferCompleted"),
        }
    }

    #[test]
    fn test_transfer_response_serialization() {
        let response = TransferResponse::BrokerMarkedFailed {
            broker_id: 1,
            partitions_affected: 5,
        };

        let serialized = bincode::serialize(&response).unwrap();
        let deserialized: TransferResponse = bincode::deserialize(&serialized).unwrap();
        assert_eq!(response, deserialized);
    }

    // ========================================================================
    // TransferDomainState Tests
    // ========================================================================

    #[test]
    fn test_transfer_domain_state_new() {
        let state = TransferDomainState::new();
        assert!(state.failed_brokers.is_empty());
        assert!(state.pending_transfers.is_empty());
    }

    #[test]
    fn test_transfer_domain_state_default() {
        let state = TransferDomainState::default();
        assert!(state.failed_brokers.is_empty());
        assert!(state.pending_transfers.is_empty());
    }

    #[test]
    fn test_is_broker_failed() {
        let mut state = TransferDomainState::new();

        assert!(!state.is_broker_failed(1));

        state.failed_brokers.insert(
            1,
            FailedBrokerInfo {
                broker_id: 1,
                failed_at_ms: 1000,
                reason: "test".to_string(),
                partitions_reassigned: false,
            },
        );

        assert!(state.is_broker_failed(1));
        assert!(!state.is_broker_failed(2));
    }

    #[test]
    fn test_get_failed_broker() {
        let mut state = TransferDomainState::new();

        assert!(state.get_failed_broker(1).is_none());

        state.failed_brokers.insert(
            1,
            FailedBrokerInfo {
                broker_id: 1,
                failed_at_ms: 1000,
                reason: "test_reason".to_string(),
                partitions_reassigned: false,
            },
        );

        let info = state.get_failed_broker(1);
        assert!(info.is_some());
        assert_eq!(info.unwrap().reason, "test_reason");
    }

    #[test]
    fn test_clear_failed_broker() {
        let mut state = TransferDomainState::new();

        state.failed_brokers.insert(
            1,
            FailedBrokerInfo {
                broker_id: 1,
                failed_at_ms: 1000,
                reason: "test".to_string(),
                partitions_reassigned: false,
            },
        );

        assert!(state.is_broker_failed(1));

        state.clear_failed_broker(1);
        assert!(!state.is_broker_failed(1));
    }

    #[test]
    fn test_clear_nonexistent_broker() {
        let mut state = TransferDomainState::new();

        // Should not panic
        state.clear_failed_broker(999);
        assert!(!state.is_broker_failed(999));
    }

    #[test]
    fn test_transfer_domain_state_debug() {
        let state = TransferDomainState::new();
        let debug_str = format!("{:?}", state);
        assert!(debug_str.contains("TransferDomainState"));
    }

    #[test]
    fn test_transfer_domain_state_serialization() {
        let mut state = TransferDomainState::new();
        state.failed_brokers.insert(
            1,
            FailedBrokerInfo {
                broker_id: 1,
                failed_at_ms: 1000,
                reason: "test".to_string(),
                partitions_reassigned: false,
            },
        );

        let serialized = bincode::serialize(&state).unwrap();
        let deserialized: TransferDomainState = bincode::deserialize(&serialized).unwrap();

        assert!(deserialized.is_broker_failed(1));
    }

    // ========================================================================
    // TransferDomainState Apply Tests (original tests)
    // ========================================================================

    #[test]
    fn test_mark_broker_failed() {
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();
        partitions.insert(
            (Arc::from("test"), 0),
            create_partition_info("test", 0, Some(1)),
        );
        partitions.insert(
            (Arc::from("test"), 1),
            create_partition_info("test", 1, Some(1)),
        );
        partitions.insert(
            (Arc::from("test"), 2),
            create_partition_info("test", 2, Some(2)),
        );

        let response = state.apply_with_context(
            TransferCommand::MarkBrokerFailed {
                broker_id: 1,
                detected_at_ms: 1000,
                reason: "Test failure".to_string(),
            },
            |_| true,
            &mut partitions,
            |_| Some(BrokerStatus::Active),
        );

        match response {
            TransferResponse::BrokerMarkedFailed {
                broker_id,
                partitions_affected,
            } => {
                assert_eq!(broker_id, 1);
                assert_eq!(partitions_affected, 2);
            }
            _ => panic!("Expected BrokerMarkedFailed"),
        }

        assert!(state.is_broker_failed(1));
    }

    #[test]
    fn test_mark_broker_already_failed() {
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();

        // First mark as failed
        let _ = state.apply_with_context(
            TransferCommand::MarkBrokerFailed {
                broker_id: 1,
                detected_at_ms: 1000,
                reason: "First failure".to_string(),
            },
            |_| true,
            &mut partitions,
            |_| Some(BrokerStatus::Active),
        );

        // Second attempt should return BrokerAlreadyFailed
        let response = state.apply_with_context(
            TransferCommand::MarkBrokerFailed {
                broker_id: 1,
                detected_at_ms: 2000,
                reason: "Second attempt".to_string(),
            },
            |_| true,
            &mut partitions,
            |_| Some(BrokerStatus::Active),
        );

        assert!(matches!(
            response,
            TransferResponse::BrokerAlreadyFailed { broker_id: 1 }
        ));
    }

    #[test]
    fn test_transfer_partition() {
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();
        partitions.insert(
            (Arc::from("test"), 0),
            create_partition_info("test", 0, Some(1)),
        );

        let response = state.apply_with_context(
            TransferCommand::TransferPartition {
                topic: "test".to_string(),
                partition: 0,
                from_broker_id: 1,
                to_broker_id: 2,
                reason: TransferReason::LoadBalancing,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
            |id| id == 2, // Broker 2 is active
            &mut partitions,
            |_| Some(BrokerStatus::Active),
        );

        match response {
            TransferResponse::PartitionTransferred {
                to_broker_id,
                new_epoch,
                ..
            } => {
                assert_eq!(to_broker_id, 2);
                assert_eq!(new_epoch, 2);
            }
            _ => panic!("Expected PartitionTransferred"),
        }

        let partition = partitions.get(&(Arc::from("test"), 0)).unwrap();
        assert_eq!(partition.owner_broker_id, Some(2));
    }

    #[test]
    fn test_transfer_partition_source_not_owner() {
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();
        partitions.insert(
            (Arc::from("test"), 0),
            create_partition_info("test", 0, Some(3)), // Owned by broker 3, not 1
        );

        let response = state.apply_with_context(
            TransferCommand::TransferPartition {
                topic: "test".to_string(),
                partition: 0,
                from_broker_id: 1, // Claiming transfer from broker 1
                to_broker_id: 2,
                reason: TransferReason::LoadBalancing,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
            |id| id == 2,
            &mut partitions,
            |_| Some(BrokerStatus::Active),
        );

        match response {
            TransferResponse::TransferSourceNotOwner {
                expected_owner,
                actual_owner,
                ..
            } => {
                assert_eq!(expected_owner, 1);
                assert_eq!(actual_owner, Some(3));
            }
            _ => panic!("Expected TransferSourceNotOwner"),
        }
    }

    #[test]
    fn test_transfer_partition_not_found() {
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();

        let response = state.apply_with_context(
            TransferCommand::TransferPartition {
                topic: "nonexistent".to_string(),
                partition: 99,
                from_broker_id: 1,
                to_broker_id: 2,
                reason: TransferReason::LoadBalancing,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
            |_| true,
            &mut partitions,
            |_| Some(BrokerStatus::Active),
        );

        assert!(matches!(
            response,
            TransferResponse::PartitionNotFoundForTransfer { partition: 99, .. }
        ));
    }

    #[test]
    fn test_transfer_partition_dest_unavailable() {
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();
        partitions.insert(
            (Arc::from("test"), 0),
            create_partition_info("test", 0, Some(1)),
        );

        let response = state.apply_with_context(
            TransferCommand::TransferPartition {
                topic: "test".to_string(),
                partition: 0,
                from_broker_id: 1,
                to_broker_id: 2,
                reason: TransferReason::LoadBalancing,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
            |_| false, // No brokers are active
            &mut partitions,
            |_| Some(BrokerStatus::Fenced),
        );

        assert!(matches!(
            response,
            TransferResponse::TransferDestinationUnavailable { broker_id: 2, .. }
        ));
    }

    #[test]
    fn test_batch_transfer() {
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();
        partitions.insert(
            (Arc::from("test"), 0),
            create_partition_info("test", 0, Some(1)),
        );
        partitions.insert(
            (Arc::from("test"), 1),
            create_partition_info("test", 1, Some(1)),
        );

        let response = state.apply_with_context(
            TransferCommand::BatchTransferPartitions {
                transfers: vec![
                    PartitionTransfer {
                        topic: "test".to_string(),
                        partition: 0,
                        from_broker_id: 1,
                        to_broker_id: 2,
                        expected_leader_epoch: None,
                    },
                    PartitionTransfer {
                        topic: "test".to_string(),
                        partition: 1,
                        from_broker_id: 1,
                        to_broker_id: 2,
                        expected_leader_epoch: None,
                    },
                ],
                transaction_id: 1,
                reason: TransferReason::BrokerFailure,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
            |id| id == 2,
            &mut partitions,
            |_| Some(BrokerStatus::Active),
        );

        match response {
            TransferResponse::BatchTransferCompleted {
                successful_transfers,
                failed_transfers,
                ..
            } => {
                assert_eq!(successful_transfers, 2);
                assert!(failed_transfers.is_empty());
            }
            _ => panic!("Expected BatchTransferCompleted"),
        }
    }

    #[test]
    fn test_batch_transfer_with_epoch_mismatch() {
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();
        partitions.insert(
            (Arc::from("test"), 0),
            create_partition_info("test", 0, Some(1)),
        );

        let response = state.apply_with_context(
            TransferCommand::BatchTransferPartitions {
                transfers: vec![PartitionTransfer {
                    topic: "test".to_string(),
                    partition: 0,
                    from_broker_id: 1,
                    to_broker_id: 2,
                    expected_leader_epoch: Some(5), // Wrong epoch
                }],
                transaction_id: 1,
                reason: TransferReason::BrokerFailure,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
            |id| id == 2,
            &mut partitions,
            |_| Some(BrokerStatus::Active),
        );

        match response {
            TransferResponse::BatchTransferCompleted {
                successful_transfers,
                failed_transfers,
                ..
            } => {
                assert_eq!(successful_transfers, 0);
                assert_eq!(failed_transfers.len(), 1);
                assert!(failed_transfers[0].2.contains("epoch mismatch"));
            }
            _ => panic!("Expected BatchTransferCompleted"),
        }
    }

    #[test]
    fn test_batch_transfer_partition_not_found() {
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();

        let response = state.apply_with_context(
            TransferCommand::BatchTransferPartitions {
                transfers: vec![PartitionTransfer {
                    topic: "nonexistent".to_string(),
                    partition: 0,
                    from_broker_id: 1,
                    to_broker_id: 2,
                    expected_leader_epoch: None,
                }],
                transaction_id: 1,
                reason: TransferReason::BrokerFailure,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
            |_| true,
            &mut partitions,
            |_| Some(BrokerStatus::Active),
        );

        match response {
            TransferResponse::BatchTransferCompleted {
                successful_transfers,
                failed_transfers,
                ..
            } => {
                assert_eq!(successful_transfers, 0);
                assert_eq!(failed_transfers.len(), 1);
                assert!(failed_transfers[0].2.contains("not found"));
            }
            _ => panic!("Expected BatchTransferCompleted"),
        }
    }

    #[test]
    fn test_batch_transfer_dest_unavailable() {
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();
        partitions.insert(
            (Arc::from("test"), 0),
            create_partition_info("test", 0, Some(1)),
        );

        let response = state.apply_with_context(
            TransferCommand::BatchTransferPartitions {
                transfers: vec![PartitionTransfer {
                    topic: "test".to_string(),
                    partition: 0,
                    from_broker_id: 1,
                    to_broker_id: 2,
                    expected_leader_epoch: None,
                }],
                transaction_id: 1,
                reason: TransferReason::BrokerFailure,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
            |_| true,
            &mut partitions,
            |id| {
                if id == 2 {
                    None
                } else {
                    Some(BrokerStatus::Active)
                }
            }, // Broker 2 doesn't exist
        );

        match response {
            TransferResponse::BatchTransferCompleted {
                successful_transfers,
                failed_transfers,
                ..
            } => {
                assert_eq!(successful_transfers, 0);
                assert_eq!(failed_transfers.len(), 1);
                assert!(failed_transfers[0].2.contains("unavailable"));
            }
            _ => panic!("Expected BatchTransferCompleted"),
        }
    }

    #[test]
    fn test_batch_transfer_wrong_owner() {
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();
        partitions.insert(
            (Arc::from("test"), 0),
            create_partition_info("test", 0, Some(3)), // Owned by broker 3, not 1
        );

        let response = state.apply_with_context(
            TransferCommand::BatchTransferPartitions {
                transfers: vec![PartitionTransfer {
                    topic: "test".to_string(),
                    partition: 0,
                    from_broker_id: 1,
                    to_broker_id: 2,
                    expected_leader_epoch: None,
                }],
                transaction_id: 1,
                reason: TransferReason::BrokerFailure,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
            |_| true,
            &mut partitions,
            |_| Some(BrokerStatus::Active),
        );

        match response {
            TransferResponse::BatchTransferCompleted {
                successful_transfers,
                failed_transfers,
                ..
            } => {
                assert_eq!(successful_transfers, 0);
                assert_eq!(failed_transfers.len(), 1);
                assert!(failed_transfers[0].2.contains("owned by broker"));
            }
            _ => panic!("Expected BatchTransferCompleted"),
        }
    }

    #[test]
    fn test_batch_transfer_unowned_partition_succeeds() {
        // During failover, partitions may become unowned - transfers should still work
        let mut state = TransferDomainState::new();
        let mut partitions = HashMap::new();
        partitions.insert(
            (Arc::from("test"), 0),
            create_partition_info("test", 0, None), // Unowned
        );

        let response = state.apply_with_context(
            TransferCommand::BatchTransferPartitions {
                transfers: vec![PartitionTransfer {
                    topic: "test".to_string(),
                    partition: 0,
                    from_broker_id: 1,
                    to_broker_id: 2,
                    expected_leader_epoch: None,
                }],
                transaction_id: 1,
                reason: TransferReason::BrokerFailure,
                lease_duration_ms: 60000,
                timestamp_ms: 1000,
            },
            |_| true,
            &mut partitions,
            |_| Some(BrokerStatus::Active),
        );

        match response {
            TransferResponse::BatchTransferCompleted {
                successful_transfers,
                failed_transfers,
                ..
            } => {
                assert_eq!(successful_transfers, 1);
                assert!(failed_transfers.is_empty());
            }
            _ => panic!("Expected BatchTransferCompleted"),
        }

        // Verify ownership was transferred
        let partition = partitions.get(&(Arc::from("test"), 0)).unwrap();
        assert_eq!(partition.owner_broker_id, Some(2));
    }
}
