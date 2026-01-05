//! Broker domain for the Raft state machine.
//!
//! Handles broker lifecycle: registration, heartbeat, fencing.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Broker status in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerStatus {
    Active,
    Fenced,
    ShuttingDown,
}

/// Information about a broker in the cluster.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BrokerInfo {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    pub registered_at_ms: u64,
    pub last_heartbeat_ms: u64,
    pub status: BrokerStatus,
    /// The broker's local timestamp when it sent the heartbeat.
    /// Used to detect clock skew between brokers.
    #[serde(default)]
    pub reported_timestamp_ms: u64,
}

/// Commands for the broker domain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerCommand {
    /// Register a broker in the cluster.
    Register {
        broker_id: i32,
        host: String,
        port: i32,
        timestamp_ms: u64,
    },

    /// Update broker heartbeat timestamp.
    Heartbeat {
        broker_id: i32,
        timestamp_ms: u64,
        /// The broker's local clock timestamp when it sent the heartbeat.
        /// Used to detect clock skew between brokers.
        #[serde(default)]
        reported_local_timestamp_ms: u64,
    },

    /// Unregister a broker from the cluster.
    Unregister { broker_id: i32 },

    /// Fence a broker (mark as unavailable).
    Fence { broker_id: i32, reason: String },
}

/// Responses from broker domain operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum BrokerResponse {
    /// Broker was successfully registered.
    Registered { broker_id: i32 },

    /// Heartbeat was recorded successfully.
    HeartbeatAck,

    /// Broker was successfully unregistered.
    Unregistered { broker_id: i32 },

    /// Broker was successfully fenced.
    Fenced { broker_id: i32 },

    /// Broker was not found.
    NotFound { broker_id: i32 },
}

/// State for the broker domain.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct BrokerDomainState {
    /// Registered brokers by ID.
    pub brokers: HashMap<i32, BrokerInfo>,
    /// Maximum observed clock skew in milliseconds per broker.
    /// Calculated as: abs(leader_timestamp - broker_reported_timestamp)
    #[serde(default)]
    pub observed_clock_skew_ms: HashMap<i32, i64>,
}

impl BrokerDomainState {
    /// Create a new empty broker state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a broker command and return the response.
    pub fn apply(&mut self, cmd: BrokerCommand) -> BrokerResponse {
        match cmd {
            BrokerCommand::Register {
                broker_id,
                host,
                port,
                timestamp_ms,
            } => {
                self.brokers.insert(
                    broker_id,
                    BrokerInfo {
                        broker_id,
                        host,
                        port,
                        registered_at_ms: timestamp_ms,
                        last_heartbeat_ms: timestamp_ms,
                        status: BrokerStatus::Active,
                        reported_timestamp_ms: timestamp_ms,
                    },
                );
                BrokerResponse::Registered { broker_id }
            }

            BrokerCommand::Heartbeat {
                broker_id,
                timestamp_ms,
                reported_local_timestamp_ms,
            } => {
                if let Some(broker) = self.brokers.get_mut(&broker_id) {
                    broker.last_heartbeat_ms = timestamp_ms;
                    // Heartbeat reactivates a fenced broker
                    broker.status = BrokerStatus::Active;

                    // Track clock skew if broker reported its local timestamp
                    if reported_local_timestamp_ms > 0 {
                        broker.reported_timestamp_ms = reported_local_timestamp_ms;
                        // Calculate skew: difference between leader's timestamp and broker's reported timestamp
                        let skew_ms = (timestamp_ms as i64) - (reported_local_timestamp_ms as i64);
                        let abs_skew = skew_ms.abs();

                        // Update max observed skew for this broker
                        let max_skew = self.observed_clock_skew_ms.entry(broker_id).or_insert(0);
                        if abs_skew > *max_skew {
                            *max_skew = abs_skew;
                        }

                        // Record metric for monitoring
                        crate::cluster::metrics::record_clock_skew(broker_id, abs_skew);

                        // Log warning if skew exceeds threshold (1 second)
                        if abs_skew > 1000 {
                            tracing::warn!(
                                broker_id,
                                skew_ms = abs_skew,
                                "Clock skew detected: broker {} has {}ms skew from leader",
                                broker_id,
                                abs_skew
                            );
                        }
                    }

                    BrokerResponse::HeartbeatAck
                } else {
                    BrokerResponse::NotFound { broker_id }
                }
            }

            BrokerCommand::Unregister { broker_id } => {
                self.brokers.remove(&broker_id);
                self.observed_clock_skew_ms.remove(&broker_id);
                BrokerResponse::Unregistered { broker_id }
            }

            BrokerCommand::Fence { broker_id, .. } => {
                if let Some(broker) = self.brokers.get_mut(&broker_id) {
                    broker.status = BrokerStatus::Fenced;
                    BrokerResponse::Fenced { broker_id }
                } else {
                    BrokerResponse::NotFound { broker_id }
                }
            }
        }
    }

    /// Check if a broker is active.
    pub fn is_active(&self, broker_id: i32) -> bool {
        self.brokers
            .get(&broker_id)
            .is_some_and(|b| b.status == BrokerStatus::Active)
    }

    /// Get broker info by ID.
    pub fn get(&self, broker_id: i32) -> Option<&BrokerInfo> {
        self.brokers.get(&broker_id)
    }

    /// Get all active broker IDs.
    pub fn active_broker_ids(&self) -> Vec<i32> {
        self.brokers
            .iter()
            .filter(|(_, b)| b.status == BrokerStatus::Active)
            .map(|(id, _)| *id)
            .collect()
    }

    /// Get the maximum observed clock skew for a specific broker (in milliseconds).
    ///
    /// Returns None if no skew has been observed for this broker.
    pub fn get_clock_skew_ms(&self, broker_id: i32) -> Option<i64> {
        self.observed_clock_skew_ms.get(&broker_id).copied()
    }

    /// Get the maximum observed clock skew across all brokers (in milliseconds).
    ///
    /// Returns 0 if no skew has been observed.
    pub fn max_clock_skew_ms(&self) -> i64 {
        self.observed_clock_skew_ms
            .values()
            .copied()
            .max()
            .unwrap_or(0)
    }

    /// Check if any broker has clock skew exceeding the given threshold.
    ///
    /// # Arguments
    /// * `threshold_ms` - The maximum acceptable clock skew in milliseconds
    ///
    /// # Returns
    /// A list of (broker_id, skew_ms) tuples for brokers exceeding the threshold.
    pub fn brokers_with_excessive_skew(&self, threshold_ms: i64) -> Vec<(i32, i64)> {
        self.observed_clock_skew_ms
            .iter()
            .filter(|(_, skew)| **skew > threshold_ms)
            .map(|(&id, &skew)| (id, skew))
            .collect()
    }

    /// Get all observed clock skews.
    pub fn all_clock_skews(&self) -> &HashMap<i32, i64> {
        &self.observed_clock_skew_ms
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_register_broker() {
        let mut state = BrokerDomainState::new();

        let response = state.apply(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        assert!(matches!(
            response,
            BrokerResponse::Registered { broker_id: 1 }
        ));
        assert!(state.is_active(1));

        let broker = state.get(1).unwrap();
        assert_eq!(broker.host, "localhost");
        assert_eq!(broker.port, 9092);
    }

    #[test]
    fn test_heartbeat() {
        let mut state = BrokerDomainState::new();

        // Register first
        state.apply(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        // Heartbeat
        let response = state.apply(BrokerCommand::Heartbeat {
            broker_id: 1,
            timestamp_ms: 2000,
            reported_local_timestamp_ms: 2000,
        });

        assert!(matches!(response, BrokerResponse::HeartbeatAck));
        assert_eq!(state.get(1).unwrap().last_heartbeat_ms, 2000);
    }

    #[test]
    fn test_heartbeat_unknown_broker() {
        let mut state = BrokerDomainState::new();

        let response = state.apply(BrokerCommand::Heartbeat {
            broker_id: 999,
            timestamp_ms: 1000,
            reported_local_timestamp_ms: 1000,
        });

        assert!(matches!(
            response,
            BrokerResponse::NotFound { broker_id: 999 }
        ));
    }

    #[test]
    fn test_unregister_broker() {
        let mut state = BrokerDomainState::new();

        state.apply(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        let response = state.apply(BrokerCommand::Unregister { broker_id: 1 });

        assert!(matches!(
            response,
            BrokerResponse::Unregistered { broker_id: 1 }
        ));
        assert!(state.get(1).is_none());
    }

    #[test]
    fn test_fence_broker() {
        let mut state = BrokerDomainState::new();

        state.apply(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        let response = state.apply(BrokerCommand::Fence {
            broker_id: 1,
            reason: "Test fencing".to_string(),
        });

        assert!(matches!(response, BrokerResponse::Fenced { broker_id: 1 }));
        assert!(!state.is_active(1));
        assert_eq!(state.get(1).unwrap().status, BrokerStatus::Fenced);
    }

    #[test]
    fn test_heartbeat_reactivates_fenced_broker() {
        let mut state = BrokerDomainState::new();

        state.apply(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        state.apply(BrokerCommand::Fence {
            broker_id: 1,
            reason: "Test".to_string(),
        });

        assert!(!state.is_active(1));

        state.apply(BrokerCommand::Heartbeat {
            broker_id: 1,
            timestamp_ms: 2000,
            reported_local_timestamp_ms: 2000,
        });

        assert!(state.is_active(1));
    }

    #[test]
    fn test_active_broker_ids() {
        let mut state = BrokerDomainState::new();

        state.apply(BrokerCommand::Register {
            broker_id: 1,
            host: "host1".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });
        state.apply(BrokerCommand::Register {
            broker_id: 2,
            host: "host2".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });
        state.apply(BrokerCommand::Register {
            broker_id: 3,
            host: "host3".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        state.apply(BrokerCommand::Fence {
            broker_id: 2,
            reason: "Test".to_string(),
        });

        let active = state.active_broker_ids();
        assert_eq!(active.len(), 2);
        assert!(active.contains(&1));
        assert!(active.contains(&3));
        assert!(!active.contains(&2));
    }

    #[test]
    fn test_clock_skew_detection() {
        let mut state = BrokerDomainState::new();

        // Register broker
        state.apply(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        // Heartbeat with no skew
        state.apply(BrokerCommand::Heartbeat {
            broker_id: 1,
            timestamp_ms: 2000,
            reported_local_timestamp_ms: 2000,
        });

        // No skew should be recorded
        assert_eq!(state.get_clock_skew_ms(1), Some(0));

        // Heartbeat with 500ms skew (broker thinks it's 500ms behind)
        state.apply(BrokerCommand::Heartbeat {
            broker_id: 1,
            timestamp_ms: 3000,
            reported_local_timestamp_ms: 2500,
        });

        assert_eq!(state.get_clock_skew_ms(1), Some(500));

        // Heartbeat with 100ms skew - max should remain 500
        state.apply(BrokerCommand::Heartbeat {
            broker_id: 1,
            timestamp_ms: 4000,
            reported_local_timestamp_ms: 3900,
        });

        assert_eq!(state.get_clock_skew_ms(1), Some(500));
    }

    #[test]
    fn test_clock_skew_multiple_brokers() {
        let mut state = BrokerDomainState::new();

        // Register two brokers
        state.apply(BrokerCommand::Register {
            broker_id: 1,
            host: "host1".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });
        state.apply(BrokerCommand::Register {
            broker_id: 2,
            host: "host2".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        // Broker 1 has 200ms skew
        state.apply(BrokerCommand::Heartbeat {
            broker_id: 1,
            timestamp_ms: 2000,
            reported_local_timestamp_ms: 1800,
        });

        // Broker 2 has 1500ms skew
        state.apply(BrokerCommand::Heartbeat {
            broker_id: 2,
            timestamp_ms: 2000,
            reported_local_timestamp_ms: 500,
        });

        assert_eq!(state.get_clock_skew_ms(1), Some(200));
        assert_eq!(state.get_clock_skew_ms(2), Some(1500));
        assert_eq!(state.max_clock_skew_ms(), 1500);

        // Check excessive skew detection (1 second threshold)
        let excessive = state.brokers_with_excessive_skew(1000);
        assert_eq!(excessive.len(), 1);
        assert!(excessive.iter().any(|(id, _)| *id == 2));
    }

    #[test]
    fn test_clock_skew_cleared_on_unregister() {
        let mut state = BrokerDomainState::new();

        state.apply(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        state.apply(BrokerCommand::Heartbeat {
            broker_id: 1,
            timestamp_ms: 2000,
            reported_local_timestamp_ms: 1500,
        });

        assert_eq!(state.get_clock_skew_ms(1), Some(500));

        // Unregister should clear skew data
        state.apply(BrokerCommand::Unregister { broker_id: 1 });

        assert_eq!(state.get_clock_skew_ms(1), None);
    }

    #[test]
    fn test_clock_skew_with_zero_reported_timestamp() {
        let mut state = BrokerDomainState::new();

        state.apply(BrokerCommand::Register {
            broker_id: 1,
            host: "localhost".to_string(),
            port: 9092,
            timestamp_ms: 1000,
        });

        // Heartbeat with 0 reported timestamp (legacy broker)
        // Should not record skew
        state.apply(BrokerCommand::Heartbeat {
            broker_id: 1,
            timestamp_ms: 2000,
            reported_local_timestamp_ms: 0,
        });

        // No skew should be recorded for legacy brokers
        assert_eq!(state.get_clock_skew_ms(1), None);
    }
}
