//! Partition domain for the Raft state machine.
//!
//! Handles topic management and partition ownership.
//!
//! # Lease-clock invariants
//!
//! Lease lifecycle decisions (grant / renew / expire) must be deterministic
//! across replicas, so they can never read a local wall clock at apply time.
//! Instead, every timestamp-carrying command brings the *proposer's*
//! `timestamp_ms` through the Raft log (one agreed value for all replicas),
//! and the domain maintains a replicated, monotonic lease clock:
//!
//! 1. **Monotonicity**: the effective time used by a command is
//!    `max(lease_clock, command.timestamp_ms)`, and the clock is advanced to
//!    that value. The lease clock never moves backward, so a proposer with a
//!    slow (backdated) wall clock can neither shrink an existing lease nor
//!    un-expire one. The clock is part of the snapshotted state.
//! 2. **Symmetric skew tolerance**: a forward-skewed proposer is tolerated by
//!    [`LEASE_TAKEOVER_GRACE_MS`]: another broker may only take over an owned
//!    partition, and an owner may only renew its own lease, while
//!    `now < lease_expires_at_ms + grace` (takeover) /
//!    `now < lease_expires_at_ms + grace` (renew). Past that point the lease
//!    is dead and ownership must be re-acquired, which bumps `leader_epoch`
//!    and fences stale writers. The sweep side already subtracts the
//!    configured skew tolerance at the propose site.
//! 3. **Idempotency**: `ExpireLeases` is expire-by-deadline; re-applying the
//!    same sweep (or sweeps from multiple brokers) is a no-op after the first
//!    application.

use serde::{Deserialize, Serialize, Serializer, ser::SerializeMap};
use std::collections::HashMap;
use std::sync::Arc;

/// Serialize a `HashMap<String, String>` with entries sorted by key.
///
/// `HashMap` iteration order is randomized per process, so two replicas (or
/// the same process re-encoding the same logical value) can produce different
/// byte sequences for the same map. That breaks postcard's canonical-form
/// invariant — a fuzz target caught this on `CreateTopic.config` — and is
/// also load-bearing for Raft determinism: replicas that hash log entries
/// (e.g. for snapshot diffing or wire integrity) must agree byte-for-byte.
/// Wire format is unchanged from the default `Serialize` impl, so existing
/// serialized data still round-trips.
fn serialize_sorted_map<S>(map: &HashMap<String, String>, ser: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    let mut entries: Vec<(&String, &String)> = map.iter().collect();
    entries.sort_by(|a, b| a.0.cmp(b.0));
    let mut m = ser.serialize_map(Some(entries.len()))?;
    for (k, v) in entries {
        m.serialize_entry(k, v)?;
    }
    m.end()
}

/// Grace window in milliseconds applied symmetrically to lease-expiry
/// comparisons on the grant (takeover) and renew paths.
///
/// This mirrors the default `clock_skew_tolerance_ms` (5s). It is a
/// compile-time constant — NOT a config value — because it participates in
/// replicated state transitions: if two replicas used different values they
/// would diverge. The `ExpireLeases` sweep applies the configured tolerance
/// at the propose site instead (the proposed `current_time_ms` is already
/// skew-adjusted), which is safe because the proposed value itself is
/// replicated.
pub const LEASE_TAKEOVER_GRACE_MS: u64 = 5_000;

/// Advance the replicated lease clock to `timestamp_ms` if it is ahead, and
/// return the effective (clamped, monotonic) time for this command.
#[inline]
fn advance_lease_clock(lease_clock_ms: &mut u64, timestamp_ms: u64) -> u64 {
    *lease_clock_ms = (*lease_clock_ms).max(timestamp_ms);
    *lease_clock_ms
}

/// State of a topic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicInfo {
    pub name: Arc<str>,
    pub partition_count: i32,
    pub created_at_ms: u64,
    #[serde(serialize_with = "serialize_sorted_map")]
    pub config: HashMap<String, String>,
}

/// State of a partition.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PartitionInfo {
    pub topic: Arc<str>,
    pub partition: i32,
    pub owner_broker_id: Option<i32>,
    pub leader_epoch: i32,
    pub lease_expires_at_ms: u64,
}

/// Commands for the partition domain.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionCommand {
    /// Create a new topic.
    CreateTopic {
        name: String,
        partitions: i32,
        #[serde(serialize_with = "serialize_sorted_map")]
        config: HashMap<String, String>,
        timestamp_ms: u64,
    },

    /// Delete a topic.
    DeleteTopic { name: String },

    /// Update topic configuration.
    UpdateTopicConfig {
        name: String,
        #[serde(serialize_with = "serialize_sorted_map")]
        config: HashMap<String, String>,
    },

    /// Acquire ownership of a partition.
    AcquirePartition {
        topic: String,
        partition: i32,
        broker_id: i32,
        lease_duration_ms: u64,
        timestamp_ms: u64,
    },

    /// Renew partition lease.
    RenewLease {
        topic: String,
        partition: i32,
        broker_id: i32,
        lease_duration_ms: u64,
        timestamp_ms: u64,
    },

    /// Release ownership of a partition.
    ReleasePartition {
        topic: String,
        partition: i32,
        broker_id: i32,
    },

    /// Expire leases that have passed their deadline.
    ExpireLeases { current_time_ms: u64 },
}

/// Responses from partition domain operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionResponse {
    /// Topic was created.
    TopicCreated { name: String, partitions: i32 },

    /// Topic already exists.
    TopicAlreadyExists { topic: String },

    /// Topic was deleted.
    TopicDeleted { name: String },

    /// Topic was not found.
    TopicNotFound { topic: String },

    /// Topic config was updated.
    TopicConfigUpdated { name: String },

    /// Partition was acquired.
    PartitionAcquired {
        topic: String,
        partition: i32,
        leader_epoch: i32,
        lease_expires_at_ms: u64,
    },

    /// Partition is owned by another broker.
    PartitionOwnedByOther {
        topic: String,
        partition: i32,
        owner: i32,
    },

    /// Partition lease was renewed.
    LeaseRenewed {
        topic: String,
        partition: i32,
        lease_expires_at_ms: u64,
    },

    /// Partition is not owned by the requesting broker.
    PartitionNotOwned { topic: String, partition: i32 },

    /// The requesting broker is not Active (fenced or shutting down) and
    /// may not acquire partitions or renew leases. Returned by the
    /// state-machine cross-domain gate, not by the partition domain itself.
    BrokerNotActive { broker_id: i32 },

    /// Partition was released.
    PartitionReleased { topic: String, partition: i32 },

    /// Leases were expired.
    LeasesExpired { count: usize },
}

/// State for the partition domain.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PartitionDomainState {
    /// Topic metadata.
    pub topics: HashMap<Arc<str>, TopicInfo>,

    /// Partition ownership: (topic, partition) -> PartitionInfo.
    pub partitions: HashMap<(Arc<str>, i32), PartitionInfo>,
}

impl PartitionDomainState {
    /// Create a new empty partition state.
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a partition command and return the response.
    ///
    /// `lease_clock_ms` is the replicated, monotonic lease clock owned by the
    /// enclosing `CoordinationState`. All lease-lifecycle comparisons use the
    /// clamped value `max(*lease_clock_ms, cmd.timestamp_ms)` — see the
    /// module docs for the invariants this enforces.
    pub fn apply(&mut self, cmd: PartitionCommand, lease_clock_ms: &mut u64) -> PartitionResponse {
        match cmd {
            PartitionCommand::CreateTopic {
                name,
                partitions,
                config,
                timestamp_ms,
            } => {
                // Advance the clock; `created_at_ms` keeps the raw proposer
                // timestamp since it is informational metadata, not a lease.
                advance_lease_clock(lease_clock_ms, timestamp_ms);
                let name: Arc<str> = Arc::from(name);

                if self.topics.contains_key(&name) {
                    return PartitionResponse::TopicAlreadyExists {
                        topic: name.to_string(),
                    };
                }

                self.topics.insert(
                    Arc::clone(&name),
                    TopicInfo {
                        name: Arc::clone(&name),
                        partition_count: partitions,
                        created_at_ms: timestamp_ms,
                        config,
                    },
                );

                // Initialize partition states
                for partition in 0..partitions {
                    self.partitions.insert(
                        (Arc::clone(&name), partition),
                        PartitionInfo {
                            topic: Arc::clone(&name),
                            partition,
                            owner_broker_id: None,
                            leader_epoch: 0,
                            lease_expires_at_ms: 0,
                        },
                    );
                }

                PartitionResponse::TopicCreated {
                    name: name.to_string(),
                    partitions,
                }
            }

            PartitionCommand::DeleteTopic { name } => {
                let name: Arc<str> = Arc::from(name);

                if self.topics.remove(&name).is_none() {
                    return PartitionResponse::TopicNotFound {
                        topic: name.to_string(),
                    };
                }

                // Remove all partitions for this topic
                self.partitions.retain(|(topic, _), _| topic != &name);

                PartitionResponse::TopicDeleted {
                    name: name.to_string(),
                }
            }

            PartitionCommand::UpdateTopicConfig { name, config } => {
                let name: Arc<str> = Arc::from(name);

                if let Some(topic) = self.topics.get_mut(&name) {
                    topic.config = config;
                    PartitionResponse::TopicConfigUpdated {
                        name: name.to_string(),
                    }
                } else {
                    PartitionResponse::TopicNotFound {
                        topic: name.to_string(),
                    }
                }
            }

            PartitionCommand::AcquirePartition {
                topic,
                partition,
                broker_id,
                lease_duration_ms,
                timestamp_ms,
            } => {
                let now = advance_lease_clock(lease_clock_ms, timestamp_ms);
                let topic: Arc<str> = Arc::from(topic);
                let key = (Arc::clone(&topic), partition);

                if let Some(partition_state) = self.partitions.get_mut(&key) {
                    // Check if currently owned by another broker with an
                    // active lease. The grace window tolerates a
                    // forward-skewed acquirer clock: takeover is only allowed
                    // once the lease is expired *beyond* the skew tolerance.
                    if let Some(owner) = partition_state.owner_broker_id
                        && owner != broker_id
                        && partition_state
                            .lease_expires_at_ms
                            .saturating_add(LEASE_TAKEOVER_GRACE_MS)
                            > now
                    {
                        return PartitionResponse::PartitionOwnedByOther {
                            topic: topic.to_string(),
                            partition,
                            owner,
                        };
                    }

                    // Acquire the partition
                    partition_state.owner_broker_id = Some(broker_id);
                    partition_state.leader_epoch += 1;
                    partition_state.lease_expires_at_ms = now.saturating_add(lease_duration_ms);

                    PartitionResponse::PartitionAcquired {
                        topic: topic.to_string(),
                        partition,
                        leader_epoch: partition_state.leader_epoch,
                        lease_expires_at_ms: partition_state.lease_expires_at_ms,
                    }
                } else {
                    // Partition doesn't exist - create it (auto-create)
                    let partition_state = PartitionInfo {
                        topic: Arc::clone(&topic),
                        partition,
                        owner_broker_id: Some(broker_id),
                        leader_epoch: 1,
                        lease_expires_at_ms: now.saturating_add(lease_duration_ms),
                    };
                    let leader_epoch = partition_state.leader_epoch;
                    let lease_expires_at_ms = partition_state.lease_expires_at_ms;
                    self.partitions.insert(key, partition_state);

                    PartitionResponse::PartitionAcquired {
                        topic: topic.to_string(),
                        partition,
                        leader_epoch,
                        lease_expires_at_ms,
                    }
                }
            }

            PartitionCommand::RenewLease {
                topic,
                partition,
                broker_id,
                lease_duration_ms,
                timestamp_ms,
            } => {
                let now = advance_lease_clock(lease_clock_ms, timestamp_ms);
                let topic: Arc<str> = Arc::from(topic);
                let key = (Arc::clone(&topic), partition);

                if let Some(partition_state) = self.partitions.get_mut(&key) {
                    if partition_state.owner_broker_id != Some(broker_id) {
                        return PartitionResponse::PartitionNotOwned {
                            topic: topic.to_string(),
                            partition,
                        };
                    }

                    // Symmetric skew tolerance on renew: a lease that is
                    // expired beyond the grace window is dead — the owner
                    // (which may have been paused/partitioned past its lease)
                    // must re-acquire, bumping `leader_epoch` so stale
                    // writers are fenced. Renewal must not resurrect it.
                    if now
                        >= partition_state
                            .lease_expires_at_ms
                            .saturating_add(LEASE_TAKEOVER_GRACE_MS)
                    {
                        return PartitionResponse::PartitionNotOwned {
                            topic: topic.to_string(),
                            partition,
                        };
                    }

                    // `now` is monotonic (clamped against the lease clock),
                    // so a backdated renewal can never move the expiry
                    // earlier than `lease_clock + duration`.
                    partition_state.lease_expires_at_ms = now.saturating_add(lease_duration_ms);

                    PartitionResponse::LeaseRenewed {
                        topic: topic.to_string(),
                        partition,
                        lease_expires_at_ms: partition_state.lease_expires_at_ms,
                    }
                } else {
                    PartitionResponse::PartitionNotOwned {
                        topic: topic.to_string(),
                        partition,
                    }
                }
            }

            PartitionCommand::ReleasePartition {
                topic,
                partition,
                broker_id,
            } => {
                let topic: Arc<str> = Arc::from(topic);
                let key = (Arc::clone(&topic), partition);

                if let Some(partition_state) = self.partitions.get_mut(&key)
                    && partition_state.owner_broker_id == Some(broker_id)
                {
                    partition_state.owner_broker_id = None;
                    partition_state.lease_expires_at_ms = 0;
                }

                PartitionResponse::PartitionReleased {
                    topic: topic.to_string(),
                    partition,
                }
            }

            PartitionCommand::ExpireLeases { current_time_ms } => {
                // Expire-by-deadline is naturally idempotent: a second sweep
                // with the same (or earlier, clamped) timestamp finds no
                // owned partitions past their deadline and is a no-op.
                let now = advance_lease_clock(lease_clock_ms, current_time_ms);
                let mut count = 0;
                for partition_state in self.partitions.values_mut() {
                    if partition_state.owner_broker_id.is_some()
                        && partition_state.lease_expires_at_ms <= now
                    {
                        partition_state.owner_broker_id = None;
                        partition_state.lease_expires_at_ms = 0;
                        count += 1;
                    }
                }
                PartitionResponse::LeasesExpired { count }
            }
        }
    }

    /// Get partition info.
    pub fn get_partition(&self, topic: &str, partition: i32) -> Option<&PartitionInfo> {
        let key = (Arc::from(topic), partition);
        self.partitions.get(&key)
    }

    /// Get partition owner.
    pub fn get_owner(&self, topic: &str, partition: i32) -> Option<i32> {
        self.get_partition(topic, partition)
            .and_then(|p| p.owner_broker_id)
    }

    /// Get all partitions owned by a broker.
    pub fn partitions_owned_by(&self, broker_id: i32) -> Vec<(Arc<str>, i32)> {
        self.partitions
            .iter()
            .filter(|(_, p)| p.owner_broker_id == Some(broker_id))
            .map(|((topic, partition), _)| (Arc::clone(topic), *partition))
            .collect()
    }

    /// Release all partitions owned by a broker.
    pub fn release_broker_partitions(&mut self, broker_id: i32) {
        for partition in self.partitions.values_mut() {
            if partition.owner_broker_id == Some(broker_id) {
                partition.owner_broker_id = None;
                partition.lease_expires_at_ms = 0;
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Test harness pairing a `PartitionDomainState` with the replicated
    /// lease clock that `CoordinationState` owns in production.
    struct TestState {
        state: PartitionDomainState,
        lease_clock_ms: u64,
    }

    impl TestState {
        fn new() -> Self {
            Self {
                state: PartitionDomainState::new(),
                lease_clock_ms: 0,
            }
        }

        fn apply(&mut self, cmd: PartitionCommand) -> PartitionResponse {
            self.state.apply(cmd, &mut self.lease_clock_ms)
        }
    }

    impl std::ops::Deref for TestState {
        type Target = PartitionDomainState;

        fn deref(&self) -> &PartitionDomainState {
            &self.state
        }
    }

    #[test]
    fn test_create_topic() {
        let mut state = TestState::new();

        let response = state.apply(PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        match response {
            PartitionResponse::TopicCreated { name, partitions } => {
                assert_eq!(name, "test-topic");
                assert_eq!(partitions, 3);
            }
            _ => panic!("Expected TopicCreated"),
        }

        // Verify partitions were created
        for p in 0..3 {
            assert!(state.get_partition("test-topic", p).is_some());
        }
    }

    #[test]
    fn test_create_duplicate_topic() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        let response = state.apply(PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 5,
            config: HashMap::new(),
            timestamp_ms: 2000,
        });

        assert!(matches!(
            response,
            PartitionResponse::TopicAlreadyExists { .. }
        ));
    }

    #[test]
    fn test_delete_topic() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test-topic".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        let response = state.apply(PartitionCommand::DeleteTopic {
            name: "test-topic".to_string(),
        });

        assert!(matches!(response, PartitionResponse::TopicDeleted { .. }));
        assert!(state.get_partition("test-topic", 0).is_none());
    }

    #[test]
    fn test_acquire_partition() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        let response = state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        match response {
            PartitionResponse::PartitionAcquired {
                leader_epoch,
                lease_expires_at_ms,
                ..
            } => {
                assert!(leader_epoch > 0);
                assert_eq!(lease_expires_at_ms, 61000);
            }
            _ => panic!("Expected PartitionAcquired"),
        }

        assert_eq!(state.get_owner("test", 0), Some(1));
    }

    #[test]
    fn test_acquire_partition_owned_by_other() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        // Try to acquire with different broker while lease is active
        let response = state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 2,
            lease_duration_ms: 60000,
            timestamp_ms: 2000, // Before lease expires
        });

        assert!(matches!(
            response,
            PartitionResponse::PartitionOwnedByOther { owner: 1, .. }
        ));
    }

    #[test]
    fn test_renew_lease() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test".to_string(),
            partitions: 1,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        let response = state.apply(PartitionCommand::RenewLease {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 120000,
            timestamp_ms: 50000,
        });

        match response {
            PartitionResponse::LeaseRenewed {
                lease_expires_at_ms,
                ..
            } => {
                assert_eq!(lease_expires_at_ms, 170000);
            }
            _ => panic!("Expected LeaseRenewed"),
        }
    }

    #[test]
    fn test_expire_leases() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test".to_string(),
            partitions: 2,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 1000,
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 1,
            broker_id: 2,
            lease_duration_ms: 1000,
            timestamp_ms: 1000,
        });

        let response = state.apply(PartitionCommand::ExpireLeases {
            current_time_ms: 10000,
        });

        match response {
            PartitionResponse::LeasesExpired { count } => {
                assert_eq!(count, 2);
            }
            _ => panic!("Expected LeasesExpired"),
        }

        assert!(state.get_owner("test", 0).is_none());
        assert!(state.get_owner("test", 1).is_none());
    }

    #[test]
    fn test_partitions_owned_by() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::CreateTopic {
            name: "test".to_string(),
            partitions: 3,
            config: HashMap::new(),
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        state.apply(PartitionCommand::AcquirePartition {
            topic: "test".to_string(),
            partition: 2,
            broker_id: 1,
            lease_duration_ms: 60000,
            timestamp_ms: 1000,
        });

        let owned = state.partitions_owned_by(1);
        assert_eq!(owned.len(), 2);
    }

    #[test]
    fn test_backdated_renew_cannot_shrink_lease() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::AcquirePartition {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 10_000,
            timestamp_ms: 100_000,
        });
        assert_eq!(
            state.get_partition("t", 0).unwrap().lease_expires_at_ms,
            110_000
        );

        // A renewal with a badly backdated timestamp (e.g. proposer's wall
        // clock jumped backward) is clamped to the lease clock: the expiry
        // stays at clock + duration instead of regressing to 50k + 10k.
        let response = state.apply(PartitionCommand::RenewLease {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 10_000,
            timestamp_ms: 50_000,
        });
        match response {
            PartitionResponse::LeaseRenewed {
                lease_expires_at_ms,
                ..
            } => assert_eq!(lease_expires_at_ms, 110_000),
            other => panic!("Expected LeaseRenewed, got {:?}", other),
        }
    }

    #[test]
    fn test_backdated_sweep_is_clamped_and_noop() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::AcquirePartition {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 10_000,
            timestamp_ms: 100_000,
        });

        // Sweep proposed with an ancient timestamp: clamped to the lease
        // clock (100_000), which is still before the expiry (110_000).
        let response = state.apply(PartitionCommand::ExpireLeases {
            current_time_ms: 1_000,
        });
        assert!(matches!(
            response,
            PartitionResponse::LeasesExpired { count: 0 }
        ));
        assert_eq!(state.get_owner("t", 0), Some(1));
        // The clock itself never moved backward.
        assert_eq!(state.lease_clock_ms, 100_000);
    }

    #[test]
    fn test_takeover_blocked_within_grace_window() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::AcquirePartition {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 10_000,
            timestamp_ms: 100_000,
        });

        // Lease expires at 110_000. A takeover attempt inside the grace
        // window (expiry + LEASE_TAKEOVER_GRACE_MS) is refused, tolerating a
        // forward-skewed acquirer clock.
        let response = state.apply(PartitionCommand::AcquirePartition {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 2,
            lease_duration_ms: 10_000,
            timestamp_ms: 110_000 + LEASE_TAKEOVER_GRACE_MS - 1,
        });
        assert!(matches!(
            response,
            PartitionResponse::PartitionOwnedByOther { owner: 1, .. }
        ));

        // At expiry + grace, takeover succeeds and bumps the epoch.
        let response = state.apply(PartitionCommand::AcquirePartition {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 2,
            lease_duration_ms: 10_000,
            timestamp_ms: 110_000 + LEASE_TAKEOVER_GRACE_MS,
        });
        match response {
            PartitionResponse::PartitionAcquired { leader_epoch, .. } => {
                assert_eq!(leader_epoch, 2)
            }
            other => panic!("Expected PartitionAcquired, got {:?}", other),
        }
        assert_eq!(state.get_owner("t", 0), Some(2));
    }

    #[test]
    fn test_renewal_of_long_expired_lease_rejected() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::AcquirePartition {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 1_000,
            timestamp_ms: 100_000,
        });

        // Expiry is 101_000. Renewal at expiry + grace is refused — the
        // owner slept past its lease and must re-acquire (epoch bump).
        let response = state.apply(PartitionCommand::RenewLease {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 1_000,
            timestamp_ms: 101_000 + LEASE_TAKEOVER_GRACE_MS,
        });
        assert!(matches!(
            response,
            PartitionResponse::PartitionNotOwned { .. }
        ));

        // Re-acquiring (same broker) works and bumps the epoch.
        let response = state.apply(PartitionCommand::AcquirePartition {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 1_000,
            timestamp_ms: 101_000 + LEASE_TAKEOVER_GRACE_MS,
        });
        match response {
            PartitionResponse::PartitionAcquired { leader_epoch, .. } => {
                assert_eq!(leader_epoch, 2)
            }
            other => panic!("Expected PartitionAcquired, got {:?}", other),
        }
    }

    #[test]
    fn test_expire_leases_is_idempotent() {
        let mut state = TestState::new();

        state.apply(PartitionCommand::AcquirePartition {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 1_000,
            timestamp_ms: 1_000,
        });

        let response = state.apply(PartitionCommand::ExpireLeases {
            current_time_ms: 10_000,
        });
        assert!(matches!(
            response,
            PartitionResponse::LeasesExpired { count: 1 }
        ));

        // Duplicate sweeps (same or earlier timestamp, e.g. from a broker
        // that still believed it was leader) are cheap no-ops.
        for ts in [10_000, 9_000, 10_000] {
            let response = state.apply(PartitionCommand::ExpireLeases {
                current_time_ms: ts,
            });
            assert!(matches!(
                response,
                PartitionResponse::LeasesExpired { count: 0 }
            ));
        }
    }
}
