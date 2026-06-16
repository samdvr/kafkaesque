//! Partition state domain — per-partition ownership and lease state.
//!
//! This is the shard-group half of the old `partition` domain. It owns the
//! per-partition `(topic, partition) -> PartitionInfo` map and all hot-path
//! lease commands (`AcquirePartition`, `RenewLease`, `RenewLeases`,
//! `ReleasePartition`, `ExpireLeases`).
//!
//! Topic existence (the registry) lives in the control group
//! (`topic_registry`). Reconcilers running on every broker propagate two
//! commands from control into the relevant shard:
//!
//! - `InitPartition` — issued after `CreateTopic` commits in control, seeds
//!   an empty `PartitionInfo` for each partition the topic owns. Idempotent.
//! - `PurgePartition` — issued after `DeleteTopic`, removes per-partition
//!   state. Idempotent.
//!
//! See the lease-clock invariants documented on the legacy `partition`
//! module — they apply unchanged here, except the clock is per-shard rather
//! than cluster-wide.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::sync::Arc;

use super::partition::{
    BatchRenewOutcome, LEASE_TAKEOVER_GRACE_MS, PartitionInfo, advance_lease_clock,
    bump_leader_epoch,
};
use super::serde_helpers::serialize_sorted_map;

/// Commands for the partition state domain. Lives in shard groups.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionStateCommand {
    /// Reconciler-issued: seed an empty partition entry. Idempotent.
    InitPartition {
        topic: String,
        partition: i32,
        created_at_ms: u64,
    },

    /// Reconciler-issued: remove a partition's state. Idempotent.
    PurgePartition { topic: String, partition: i32 },

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

    /// Renew leases for a batch of partitions owned by `broker_id` in one
    /// proposal. See the legacy `PartitionCommand::RenewLeases` docs for the
    /// motivation (one commit per renewal cycle instead of N).
    RenewLeases {
        broker_id: i32,
        partitions: Vec<(String, i32)>,
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

/// Responses from partition state operations.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
pub enum PartitionStateResponse {
    /// Partition entry was created by `InitPartition`.
    PartitionInitialized { topic: String, partition: i32 },

    /// Partition entry already existed; `InitPartition` was a no-op.
    PartitionAlreadyExists { topic: String, partition: i32 },

    /// Partition entry was removed by `PurgePartition`.
    PartitionPurged { topic: String, partition: i32 },

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

    /// Outcome of a batch `RenewLeases`. `results[i]` corresponds to
    /// `partitions[i]` in the request order.
    LeasesRenewed { results: Vec<BatchRenewOutcome> },

    /// Partition is not owned by the requesting broker.
    PartitionNotOwned { topic: String, partition: i32 },

    /// The requesting broker is not Active (fenced or shutting down) and
    /// may not acquire partitions or renew leases. Set by the shard-level
    /// cross-domain gate against the broker liveness shadow.
    BrokerNotActive { broker_id: i32 },

    /// Partition was released.
    PartitionReleased { topic: String, partition: i32 },

    /// Leases were expired.
    LeasesExpired { count: usize },
}

/// State for the partition state domain.
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct PartitionStateDomain {
    /// Partition ownership: (topic, partition) -> PartitionInfo.
    #[serde(serialize_with = "serialize_sorted_map")]
    pub partitions: HashMap<(Arc<str>, i32), PartitionInfo>,
}

impl PartitionStateDomain {
    pub fn new() -> Self {
        Self::default()
    }

    /// Apply a partition-state command. `lease_clock_ms` is the shard's
    /// replicated monotonic lease clock — independent across shards.
    pub fn apply(
        &mut self,
        cmd: PartitionStateCommand,
        lease_clock_ms: &mut u64,
    ) -> PartitionStateResponse {
        match cmd {
            PartitionStateCommand::InitPartition {
                topic,
                partition,
                created_at_ms,
            } => {
                advance_lease_clock(lease_clock_ms, created_at_ms);
                let topic_arc: Arc<str> = Arc::from(topic);
                let key = (Arc::clone(&topic_arc), partition);
                if self.partitions.contains_key(&key) {
                    return PartitionStateResponse::PartitionAlreadyExists {
                        topic: topic_arc.to_string(),
                        partition,
                    };
                }
                self.partitions.insert(
                    key,
                    PartitionInfo {
                        topic: Arc::clone(&topic_arc),
                        partition,
                        owner_broker_id: None,
                        leader_epoch: 0,
                        lease_expires_at_ms: 0,
                    },
                );
                PartitionStateResponse::PartitionInitialized {
                    topic: topic_arc.to_string(),
                    partition,
                }
            }

            PartitionStateCommand::PurgePartition { topic, partition } => {
                let topic_arc: Arc<str> = Arc::from(topic);
                let key = (Arc::clone(&topic_arc), partition);
                self.partitions.remove(&key);
                PartitionStateResponse::PartitionPurged {
                    topic: topic_arc.to_string(),
                    partition,
                }
            }

            PartitionStateCommand::AcquirePartition {
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
                    if let Some(owner) = partition_state.owner_broker_id
                        && owner != broker_id
                        && partition_state
                            .lease_expires_at_ms
                            .saturating_add(LEASE_TAKEOVER_GRACE_MS)
                            > now
                    {
                        return PartitionStateResponse::PartitionOwnedByOther {
                            topic: topic.to_string(),
                            partition,
                            owner,
                        };
                    }

                    partition_state.owner_broker_id = Some(broker_id);
                    bump_leader_epoch(&mut partition_state.leader_epoch);
                    partition_state.lease_expires_at_ms = now.saturating_add(lease_duration_ms);

                    PartitionStateResponse::PartitionAcquired {
                        topic: topic.to_string(),
                        partition,
                        leader_epoch: partition_state.leader_epoch,
                        lease_expires_at_ms: partition_state.lease_expires_at_ms,
                    }
                } else {
                    // Auto-create on acquire mirrors the legacy domain. Once
                    // the reconciler is in place this branch may be tightened
                    // to reject unknown partitions.
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

                    PartitionStateResponse::PartitionAcquired {
                        topic: topic.to_string(),
                        partition,
                        leader_epoch,
                        lease_expires_at_ms,
                    }
                }
            }

            PartitionStateCommand::RenewLease {
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
                        return PartitionStateResponse::PartitionNotOwned {
                            topic: topic.to_string(),
                            partition,
                        };
                    }
                    if now
                        >= partition_state
                            .lease_expires_at_ms
                            .saturating_add(LEASE_TAKEOVER_GRACE_MS)
                    {
                        return PartitionStateResponse::PartitionNotOwned {
                            topic: topic.to_string(),
                            partition,
                        };
                    }
                    partition_state.lease_expires_at_ms = now.saturating_add(lease_duration_ms);
                    PartitionStateResponse::LeaseRenewed {
                        topic: topic.to_string(),
                        partition,
                        lease_expires_at_ms: partition_state.lease_expires_at_ms,
                    }
                } else {
                    PartitionStateResponse::PartitionNotOwned {
                        topic: topic.to_string(),
                        partition,
                    }
                }
            }

            PartitionStateCommand::RenewLeases {
                broker_id,
                partitions,
                lease_duration_ms,
                timestamp_ms,
            } => {
                let now = advance_lease_clock(lease_clock_ms, timestamp_ms);
                let new_expiry = now.saturating_add(lease_duration_ms);

                let mut results = Vec::with_capacity(partitions.len());
                for (topic, partition) in partitions {
                    let topic_arc: Arc<str> = Arc::from(topic);
                    let key = (Arc::clone(&topic_arc), partition);

                    let outcome = if let Some(state) = self.partitions.get_mut(&key) {
                        if state.owner_broker_id != Some(broker_id) {
                            BatchRenewOutcome::NotOwned
                        } else if now
                            >= state
                                .lease_expires_at_ms
                                .saturating_add(LEASE_TAKEOVER_GRACE_MS)
                        {
                            // Lease is past the grace window; the owner must
                            // re-acquire (same skew-tolerance check as the
                            // single-partition RenewLease above). Reported
                            // as NotOwned so the caller treats it as a lost
                            // lease.
                            BatchRenewOutcome::NotOwned
                        } else {
                            state.lease_expires_at_ms = new_expiry;
                            BatchRenewOutcome::Renewed {
                                lease_expires_at_ms: new_expiry,
                            }
                        }
                    } else {
                        BatchRenewOutcome::NotOwned
                    };
                    results.push(outcome);
                }
                PartitionStateResponse::LeasesRenewed { results }
            }

            PartitionStateCommand::ReleasePartition {
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
                    bump_leader_epoch(&mut partition_state.leader_epoch);
                }

                PartitionStateResponse::PartitionReleased {
                    topic: topic.to_string(),
                    partition,
                }
            }

            PartitionStateCommand::ExpireLeases { current_time_ms } => {
                let now = advance_lease_clock(lease_clock_ms, current_time_ms);
                let mut count = 0;
                for partition_state in self.partitions.values_mut() {
                    if partition_state.owner_broker_id.is_some()
                        && partition_state.lease_expires_at_ms <= now
                    {
                        partition_state.owner_broker_id = None;
                        partition_state.lease_expires_at_ms = 0;
                        bump_leader_epoch(&mut partition_state.leader_epoch);
                        count += 1;
                    }
                }
                PartitionStateResponse::LeasesExpired { count }
            }
        }
    }

    pub fn get_partition(&self, topic: &str, partition: i32) -> Option<&PartitionInfo> {
        let key = (Arc::from(topic), partition);
        self.partitions.get(&key)
    }

    pub fn get_owner(&self, topic: &str, partition: i32) -> Option<i32> {
        self.get_partition(topic, partition)
            .and_then(|p| p.owner_broker_id)
    }

    pub fn partitions_owned_by(&self, broker_id: i32) -> Vec<(Arc<str>, i32)> {
        self.partitions
            .iter()
            .filter(|(_, p)| p.owner_broker_id == Some(broker_id))
            .map(|((topic, partition), _)| (Arc::clone(topic), *partition))
            .collect()
    }

    pub fn release_broker_partitions(&mut self, broker_id: i32) {
        for partition in self.partitions.values_mut() {
            if partition.owner_broker_id == Some(broker_id) {
                partition.owner_broker_id = None;
                partition.lease_expires_at_ms = 0;
                bump_leader_epoch(&mut partition.leader_epoch);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    struct TestState {
        state: PartitionStateDomain,
        lease_clock_ms: u64,
    }

    impl TestState {
        fn new() -> Self {
            Self {
                state: PartitionStateDomain::new(),
                lease_clock_ms: 0,
            }
        }

        fn apply(&mut self, cmd: PartitionStateCommand) -> PartitionStateResponse {
            self.state.apply(cmd, &mut self.lease_clock_ms)
        }
    }

    #[test]
    fn init_partition_inserts_empty_entry() {
        let mut s = TestState::new();
        let resp = s.apply(PartitionStateCommand::InitPartition {
            topic: "t".into(),
            partition: 0,
            created_at_ms: 100,
        });
        assert!(matches!(resp, PartitionStateResponse::PartitionInitialized { .. }));
        let p = s.state.get_partition("t", 0).unwrap();
        assert_eq!(p.owner_broker_id, None);
        assert_eq!(p.leader_epoch, 0);
    }

    #[test]
    fn init_partition_is_idempotent() {
        let mut s = TestState::new();
        s.apply(PartitionStateCommand::InitPartition {
            topic: "t".into(),
            partition: 0,
            created_at_ms: 100,
        });
        let resp = s.apply(PartitionStateCommand::InitPartition {
            topic: "t".into(),
            partition: 0,
            created_at_ms: 100,
        });
        assert!(matches!(resp, PartitionStateResponse::PartitionAlreadyExists { .. }));
    }

    #[test]
    fn purge_partition_removes_entry_and_is_idempotent_when_absent() {
        let mut s = TestState::new();
        s.apply(PartitionStateCommand::InitPartition {
            topic: "t".into(),
            partition: 0,
            created_at_ms: 1,
        });
        let resp = s.apply(PartitionStateCommand::PurgePartition {
            topic: "t".into(),
            partition: 0,
        });
        assert!(matches!(resp, PartitionStateResponse::PartitionPurged { .. }));
        assert!(s.state.get_partition("t", 0).is_none());
        // Second purge: no-op, same response.
        let resp = s.apply(PartitionStateCommand::PurgePartition {
            topic: "t".into(),
            partition: 0,
        });
        assert!(matches!(resp, PartitionStateResponse::PartitionPurged { .. }));
    }

    #[test]
    fn acquire_after_init_assigns_owner_and_bumps_epoch() {
        let mut s = TestState::new();
        s.apply(PartitionStateCommand::InitPartition {
            topic: "t".into(),
            partition: 0,
            created_at_ms: 1,
        });
        let resp = s.apply(PartitionStateCommand::AcquirePartition {
            topic: "t".into(),
            partition: 0,
            broker_id: 7,
            lease_duration_ms: 1000,
            timestamp_ms: 100,
        });
        match resp {
            PartitionStateResponse::PartitionAcquired {
                leader_epoch,
                lease_expires_at_ms,
                ..
            } => {
                assert_eq!(leader_epoch, 1);
                assert_eq!(lease_expires_at_ms, 1100);
            }
            _ => panic!("expected PartitionAcquired"),
        }
    }

    #[test]
    fn renew_unknown_partition_is_not_owned() {
        let mut s = TestState::new();
        let resp = s.apply(PartitionStateCommand::RenewLease {
            topic: "t".into(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 1000,
            timestamp_ms: 1,
        });
        assert!(matches!(resp, PartitionStateResponse::PartitionNotOwned { .. }));
    }

    #[test]
    fn expire_leases_clears_owners_past_deadline() {
        let mut s = TestState::new();
        s.apply(PartitionStateCommand::AcquirePartition {
            topic: "t".into(),
            partition: 0,
            broker_id: 1,
            lease_duration_ms: 100,
            timestamp_ms: 0,
        });
        let resp = s.apply(PartitionStateCommand::ExpireLeases {
            current_time_ms: 1_000,
        });
        assert!(matches!(resp, PartitionStateResponse::LeasesExpired { count: 1 }));
        assert!(s.state.get_owner("t", 0).is_none());
    }
}
