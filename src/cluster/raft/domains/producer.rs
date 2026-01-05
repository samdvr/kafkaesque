//! Producer domain for the Raft state machine.
//!
//! Handles producer ID allocation, idempotency state, and transactions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

/// Producer state for idempotency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProducerIdempotencyState {
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub last_sequence: i32,
    /// Timestamp of last update (used for eviction).
    pub last_updated_ms: u64,
}

/// Transaction state.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum TransactionState {
    Empty,
    Ongoing,
    PrepareCommit,
    PrepareAbort,
    CompleteCommit,
    CompleteAbort,
    Dead,
}

/// Transactional producer state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TransactionalProducerInfo {
    pub transactional_id: String,
    pub producer_id: i64,
    pub producer_epoch: i16,
    pub state: TransactionState,
    pub timeout_ms: i32,
    pub last_update_ms: u64,
}

/// Commands for the producer domain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProducerCommand {
    /// Allocate a new producer ID.
    AllocateProducerId,

    /// Initialize a producer (possibly transactional).
    InitProducerId {
        transactional_id: Option<String>,
        producer_id: i64,
        epoch: i16,
        timeout_ms: i32,
        timestamp_ms: u64,
    },

    /// Store producer state for idempotency.
    StoreProducerState {
        topic: String,
        partition: i32,
        producer_id: i64,
        last_sequence: i32,
        producer_epoch: i16,
        timestamp_ms: u64,
    },

    /// Expire producer states that haven't been updated within the TTL.
    ExpireProducerStates { current_time_ms: u64, ttl_ms: u64 },
}

/// Responses from producer domain operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum ProducerResponse {
    /// Producer ID was allocated.
    ProducerIdAllocated { producer_id: i64, epoch: i16 },

    /// Producer state was stored.
    ProducerStateStored,

    /// Producer states were expired.
    ProducerStatesExpired { count: usize },

    /// Generic success.
    Ok,
}

/// State for the producer domain.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ProducerDomainState {
    /// Next producer ID to allocate.
    pub next_producer_id: i64,

    /// Transactional producers.
    pub transactional_producers: HashMap<String, TransactionalProducerInfo>,

    /// Producer states for idempotency: (topic, partition, producer_id) -> ProducerIdempotencyState.
    pub producer_states: HashMap<(Arc<str>, i32, i64), ProducerIdempotencyState>,
}

impl ProducerDomainState {
    /// Create a new empty producer state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a producer command and return the response.
    pub fn apply(&mut self, cmd: ProducerCommand) -> ProducerResponse {
        match cmd {
            ProducerCommand::AllocateProducerId => {
                let producer_id = self.next_producer_id;
                self.next_producer_id += 1;
                ProducerResponse::ProducerIdAllocated {
                    producer_id,
                    epoch: 0,
                }
            }

            ProducerCommand::InitProducerId {
                transactional_id,
                producer_id: _,
                epoch: _,
                timeout_ms,
                timestamp_ms,
            } => {
                if let Some(txn_id) = transactional_id {
                    // Transactional producer
                    if let Some(entry) = self.transactional_producers.get_mut(&txn_id) {
                        // Bump epoch
                        entry.producer_epoch = entry.producer_epoch.wrapping_add(1);
                        entry.last_update_ms = timestamp_ms;
                        ProducerResponse::ProducerIdAllocated {
                            producer_id: entry.producer_id,
                            epoch: entry.producer_epoch,
                        }
                    } else {
                        // Create new producer
                        let pid = self.next_producer_id;
                        self.next_producer_id += 1;
                        self.transactional_producers.insert(
                            txn_id.clone(),
                            TransactionalProducerInfo {
                                transactional_id: txn_id,
                                producer_id: pid,
                                producer_epoch: 0,
                                state: TransactionState::Empty,
                                timeout_ms,
                                last_update_ms: timestamp_ms,
                            },
                        );
                        ProducerResponse::ProducerIdAllocated {
                            producer_id: pid,
                            epoch: 0,
                        }
                    }
                } else {
                    // Non-transactional producer
                    let producer_id = self.next_producer_id;
                    self.next_producer_id += 1;
                    ProducerResponse::ProducerIdAllocated {
                        producer_id,
                        epoch: 0,
                    }
                }
            }

            ProducerCommand::StoreProducerState {
                topic,
                partition,
                producer_id,
                last_sequence,
                producer_epoch,
                timestamp_ms,
            } => {
                let topic: Arc<str> = Arc::from(topic);
                let key = (topic, partition, producer_id);
                self.producer_states.insert(
                    key,
                    ProducerIdempotencyState {
                        producer_id,
                        producer_epoch,
                        last_sequence,
                        last_updated_ms: timestamp_ms,
                    },
                );
                ProducerResponse::ProducerStateStored
            }

            ProducerCommand::ExpireProducerStates {
                current_time_ms,
                ttl_ms,
            } => {
                // Collect keys to remove
                let keys_to_remove: Vec<_> = self
                    .producer_states
                    .iter()
                    .filter(|(_, state)| {
                        current_time_ms.saturating_sub(state.last_updated_ms) > ttl_ms
                    })
                    .map(|(key, _)| key.clone())
                    .collect();

                let count = keys_to_remove.len();
                for key in keys_to_remove {
                    self.producer_states.remove(&key);
                }

                ProducerResponse::ProducerStatesExpired { count }
            }
        }
    }

    /// Get producer idempotency state.
    pub fn get_producer_state(
        &self,
        topic: &str,
        partition: i32,
        producer_id: i64,
    ) -> Option<&ProducerIdempotencyState> {
        let key = (Arc::from(topic), partition, producer_id);
        self.producer_states.get(&key)
    }

    /// Get transactional producer info.
    pub fn get_transactional_producer(
        &self,
        transactional_id: &str,
    ) -> Option<&TransactionalProducerInfo> {
        self.transactional_producers.get(transactional_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_allocate_producer_id() {
        let mut state = ProducerDomainState::new();

        let response = state.apply(ProducerCommand::AllocateProducerId);
        match response {
            ProducerResponse::ProducerIdAllocated { producer_id, epoch } => {
                assert_eq!(producer_id, 0);
                assert_eq!(epoch, 0);
            }
            _ => panic!("Expected ProducerIdAllocated"),
        }

        let response = state.apply(ProducerCommand::AllocateProducerId);
        match response {
            ProducerResponse::ProducerIdAllocated { producer_id, .. } => {
                assert_eq!(producer_id, 1);
            }
            _ => panic!("Expected ProducerIdAllocated"),
        }
    }

    #[test]
    fn test_init_transactional_producer() {
        let mut state = ProducerDomainState::new();

        let response = state.apply(ProducerCommand::InitProducerId {
            transactional_id: Some("txn-1".to_string()),
            producer_id: 0,
            epoch: 0,
            timeout_ms: 60000,
            timestamp_ms: 1000,
        });

        match response {
            ProducerResponse::ProducerIdAllocated { producer_id, epoch } => {
                assert_eq!(producer_id, 0);
                assert_eq!(epoch, 0);
            }
            _ => panic!("Expected ProducerIdAllocated"),
        }

        // Re-init should bump epoch
        let response = state.apply(ProducerCommand::InitProducerId {
            transactional_id: Some("txn-1".to_string()),
            producer_id: 0,
            epoch: 0,
            timeout_ms: 60000,
            timestamp_ms: 2000,
        });

        match response {
            ProducerResponse::ProducerIdAllocated { producer_id, epoch } => {
                assert_eq!(producer_id, 0);
                assert_eq!(epoch, 1);
            }
            _ => panic!("Expected ProducerIdAllocated"),
        }
    }

    #[test]
    fn test_store_producer_state() {
        let mut state = ProducerDomainState::new();

        let response = state.apply(ProducerCommand::StoreProducerState {
            topic: "test".to_string(),
            partition: 0,
            producer_id: 123,
            last_sequence: 5,
            producer_epoch: 0,
            timestamp_ms: 1000,
        });

        assert!(matches!(response, ProducerResponse::ProducerStateStored));

        let producer_state = state.get_producer_state("test", 0, 123).unwrap();
        assert_eq!(producer_state.last_sequence, 5);
    }

    #[test]
    fn test_expire_producer_states() {
        let mut state = ProducerDomainState::new();

        state.apply(ProducerCommand::StoreProducerState {
            topic: "test".to_string(),
            partition: 0,
            producer_id: 1,
            last_sequence: 1,
            producer_epoch: 0,
            timestamp_ms: 1000,
        });

        state.apply(ProducerCommand::StoreProducerState {
            topic: "test".to_string(),
            partition: 1,
            producer_id: 2,
            last_sequence: 1,
            producer_epoch: 0,
            timestamp_ms: 2000,
        });

        // Expire with TTL of 500ms, current time 1600ms
        // First state (1000ms) should expire, second (2000ms) should not
        let response = state.apply(ProducerCommand::ExpireProducerStates {
            current_time_ms: 1600,
            ttl_ms: 500,
        });

        match response {
            ProducerResponse::ProducerStatesExpired { count } => {
                assert_eq!(count, 1);
            }
            _ => panic!("Expected ProducerStatesExpired"),
        }

        assert!(state.get_producer_state("test", 0, 1).is_none());
        assert!(state.get_producer_state("test", 1, 2).is_some());
    }
}
