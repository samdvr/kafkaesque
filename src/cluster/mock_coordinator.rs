//! In-memory mock coordinator for testing.
//!
//! This provides a full-featured in-memory implementation of all coordinator traits
//! for comprehensive testing without requiring external dependencies.
//!
//! # Usage
//!
//! This module is available when the `test-utilities` feature is enabled,
//! or during unit tests:
//!
//! ```toml
//! [dev-dependencies]
//! kafkaesque = { path = ".", features = ["test-utilities"] }
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicI32, AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use async_trait::async_trait;
use chrono::Utc;
use tokio::sync::RwLock;

use super::coordinator::BrokerInfo;
use super::error::{HeartbeatResult, SlateDBError, SlateDBResult};
use super::raft::domains::{PartitionTransfer, TransferReason};
use super::traits::{
    ConsumerGroupCoordinator, PartitionCoordinator, PartitionTransferCoordinator,
    ProducerCoordinator,
};

/// Type alias for partition ownership map
/// Key: (topic, partition)
/// Value: (owner_broker_id, lease_expiry)
pub type PartitionOwnersMap = HashMap<(String, i32), (i32, Instant)>;

/// Type alias for producer state map
/// Key: (topic, partition, producer_id)
/// Value: (last_sequence, producer_epoch)
pub type ProducerStatesMap = HashMap<(String, i32, i64), (i32, i16)>;

/// Consumer group member state for testing.
#[derive(Debug, Clone)]
pub struct MockMember {
    pub member_id: String,
    pub metadata: Vec<u8>,
    /// Full `(protocol_name, metadata)` list from the member's last join,
    /// in preference order. Mirrors the Raft group domain so the
    /// protocol-aware join path can be tested against the mock.
    pub protocols: Vec<(String, Vec<u8>)>,
    pub assignment: Vec<u8>,
    pub session_timeout_ms: i32,
    pub last_heartbeat: Instant,
}

/// Consumer group state for testing.
#[derive(Debug, Clone)]
pub struct MockGroup {
    pub generation_id: i32,
    pub leader_id: Option<String>,
    pub members: HashMap<String, MockMember>,
    pub state: GroupState,
    /// Protocol negotiated at the last rebalance, if any.
    pub protocol_name: Option<String>,
}

/// Group state machine states.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GroupState {
    Empty,
    PreparingRebalance,
    CompletingRebalance,
    Stable,
}

impl Default for MockGroup {
    fn default() -> Self {
        Self {
            generation_id: 0,
            leader_id: None,
            members: HashMap::new(),
            state: GroupState::Empty,
            protocol_name: None,
        }
    }
}

/// Offset storage key.
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct OffsetKey {
    pub group_id: String,
    pub topic: String,
    pub partition: i32,
}

/// Stored offset data.
#[derive(Debug, Clone)]
pub struct StoredOffset {
    pub offset: i64,
    pub metadata: Option<String>,
    pub commit_timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct MockCoordinator {
    pub broker_id: i32,
    pub host: String,
    pub port: i32,
    /// Shared broker registry state
    pub brokers: Arc<RwLock<HashMap<i32, (BrokerInfo, Instant)>>>,
    /// Shared topic registry: topic -> partition_count
    pub topics: Arc<RwLock<HashMap<String, i32>>>,
    /// Shared topic config registry: topic -> config map.
    /// Kept in lockstep with `topics` — every entry in `topics` has a
    /// corresponding entry here (possibly empty).
    pub topic_configs: Arc<RwLock<HashMap<String, HashMap<String, String>>>>,
    /// Shared partition ownership: (topic, partition) -> (broker_id, lease_expiry)
    pub partition_owners: Arc<RwLock<PartitionOwnersMap>>,
    /// Per-partition leader epoch. Bumped on every acquire/transfer/mark-
    /// broker-failed transition so callers can fence stale writers — the
    /// same contract `RaftCoordinator` implements via the FSM. Without it
    /// the mock returned `Some(0)` for every acquire and let contract
    /// tests pass behavior the real coordinator would fence as a TOCTOU
    /// race.
    pub partition_epochs: Arc<RwLock<HashMap<(String, i32), i32>>>,
    /// Consumer group state (per-coordinator for now)
    pub consumer_groups: Arc<RwLock<HashMap<String, MockGroup>>>,
    /// Offset storage
    pub offsets: Arc<RwLock<HashMap<OffsetKey, StoredOffset>>>,
    /// Producer ID allocator
    pub next_producer_id: Arc<AtomicI64>,
    /// Member ID allocator
    pub next_member_id: Arc<AtomicI32>,
    /// Producer state storage for idempotency persistence
    pub producer_states: Arc<RwLock<ProducerStatesMap>>,
    /// Mock Raft index for testing consistency snapshots
    pub mock_raft_index: Arc<AtomicU64>,
}

impl MockCoordinator {
    pub fn new(broker_id: i32, host: &str, port: i32) -> Self {
        Self {
            broker_id,
            host: host.to_string(),
            port,
            brokers: Arc::new(RwLock::new(HashMap::new())),
            topics: Arc::new(RwLock::new(HashMap::new())),
            topic_configs: Arc::new(RwLock::new(HashMap::new())),
            partition_owners: Arc::new(RwLock::new(HashMap::new())),
            partition_epochs: Arc::new(RwLock::new(HashMap::new())),
            consumer_groups: Arc::new(RwLock::new(HashMap::new())),
            offsets: Arc::new(RwLock::new(HashMap::new())),
            next_producer_id: Arc::new(AtomicI64::new(1000)),
            next_member_id: Arc::new(AtomicI32::new(1)),
            producer_states: Arc::new(RwLock::new(HashMap::new())),
            mock_raft_index: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Get the number of registered topics.
    pub async fn topic_count(&self) -> usize {
        self.topics.read().await.len()
    }

    /// Get the number of consumer groups.
    pub async fn group_count(&self) -> usize {
        self.consumer_groups.read().await.len()
    }

    /// Get a copy of group state for testing verification.
    pub async fn get_group_state(&self, group_id: &str) -> Option<MockGroup> {
        self.consumer_groups.read().await.get(group_id).cloned()
    }

    /// Manually set partition owner for testing scenarios.
    pub async fn set_partition_owner(
        &self,
        topic: &str,
        partition: i32,
        owner_id: i32,
        lease_secs: u64,
    ) {
        let mut owners = self.partition_owners.write().await;
        owners.insert(
            (topic.to_string(), partition),
            (owner_id, Instant::now() + Duration::from_secs(lease_secs)),
        );
    }

    /// Expire all leases (for testing zombie mode scenarios).
    pub async fn expire_all_leases(&self) {
        let mut owners = self.partition_owners.write().await;
        let expired = Instant::now() - Duration::from_secs(1);
        for (_, (_, expiry)) in owners.iter_mut() {
            *expiry = expired;
        }
    }

    /// Expire only this broker's leases (for simulating single broker failure).
    ///
    /// This is more precise than `expire_all_leases()` and is useful for testing
    /// scenarios where only one broker fails while others remain healthy.
    pub async fn expire_my_leases(&self) {
        let mut owners = self.partition_owners.write().await;
        let expired = Instant::now() - Duration::from_secs(1);
        for (_, (owner_id, expiry)) in owners.iter_mut() {
            if *owner_id == self.broker_id {
                *expiry = expired;
            }
        }
    }
}

#[async_trait]
impl PartitionCoordinator for MockCoordinator {
    fn broker_id(&self) -> i32 {
        self.broker_id
    }

    async fn register_broker(&self) -> SlateDBResult<()> {
        let mut brokers = self.brokers.write().await;
        let broker_info = BrokerInfo {
            broker_id: self.broker_id,
            host: self.host.clone(),
            port: self.port,
            registered_at: Utc::now().timestamp_millis(),
        };
        brokers.insert(self.broker_id, (broker_info, Instant::now()));
        Ok(())
    }

    async fn heartbeat(&self) -> SlateDBResult<()> {
        let mut brokers = self.brokers.write().await;
        if let Some((_info, last_seen)) = brokers.get_mut(&self.broker_id) {
            *last_seen = Instant::now();
            Ok(())
        } else {
            Err(SlateDBError::Storage(
                "Broker not registered in mock".to_string(),
            ))
        }
    }

    async fn get_live_brokers(&self) -> SlateDBResult<Vec<BrokerInfo>> {
        let brokers = self.brokers.read().await;
        // In this mock, all registered brokers are considered live.
        Ok(brokers.values().map(|(info, _)| info.clone()).collect())
    }

    async fn current_leader_id(&self) -> SlateDBResult<Option<i32>> {
        // The mock is a standalone single-broker "cluster": this broker is
        // always the controller.
        Ok(Some(self.broker_id))
    }

    async fn unregister_broker(&self) -> SlateDBResult<()> {
        let mut brokers = self.brokers.write().await;
        brokers.remove(&self.broker_id);
        Ok(())
    }

    async fn acquire_partition(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<bool> {
        let mut owners = self.partition_owners.write().await;
        let key = (topic.to_string(), partition);

        match owners.get(&key) {
            Some((_owner_id, expiry)) if *expiry > Instant::now() => {
                // Lease is still valid
                Ok(false)
            }
            _ => {
                // Lease is expired or doesn't exist, acquire it
                owners.insert(
                    key.clone(),
                    (
                        self.broker_id,
                        Instant::now() + Duration::from_secs(lease_secs),
                    ),
                );
                // Bump leader epoch on every acquire so a stale writer
                // that still believes it owns the partition is fenced —
                // the same contract RaftCoordinator implements via the
                // FSM. Without the bump the mock returned epoch 0 every
                // time and let TOCTOU races slip past contract tests.
                let mut epochs = self.partition_epochs.write().await;
                let next = epochs.get(&key).copied().unwrap_or(0).saturating_add(1);
                epochs.insert(key, next);
                Ok(true)
            }
        }
    }

    async fn acquire_partition_with_epoch(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<Option<i32>> {
        let acquired = self.acquire_partition(topic, partition, lease_secs).await?;
        if !acquired {
            return Ok(None);
        }
        let epochs = self.partition_epochs.read().await;
        Ok(epochs
            .get(&(topic.to_string(), partition))
            .copied()
            .or(Some(1)))
    }

    async fn renew_partition_lease(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<bool> {
        let mut owners = self.partition_owners.write().await;
        let key = (topic.to_string(), partition);

        if let Some((owner_id, expiry)) = owners.get_mut(&key)
            && *owner_id == self.broker_id
        {
            *expiry = Instant::now() + Duration::from_secs(lease_secs);
            return Ok(true);
        }
        Ok(false)
    }

    async fn release_partition(&self, topic: &str, partition: i32) -> SlateDBResult<()> {
        let mut owners = self.partition_owners.write().await;
        let key = (topic.to_string(), partition);
        if let Some((owner_id, _)) = owners.get(&key)
            && *owner_id == self.broker_id
        {
            owners.remove(&key);
        }
        Ok(())
    }

    async fn get_partition_owner(&self, topic: &str, partition: i32) -> SlateDBResult<Option<i32>> {
        let owners = self.partition_owners.read().await;
        let key = (topic.to_string(), partition);
        // Only return owner if lease hasn't expired (match production semantics)
        Ok(owners.get(&key).and_then(|(id, expiry)| {
            if *expiry > Instant::now() {
                Some(*id)
            } else {
                None
            }
        }))
    }

    async fn owns_partition_for_read(&self, topic: &str, partition: i32) -> SlateDBResult<bool> {
        let owners = self.partition_owners.read().await;
        let key = (topic.to_string(), partition);
        match owners.get(&key) {
            Some((owner_id, expiry)) => Ok(*owner_id == self.broker_id && *expiry > Instant::now()),
            None => Ok(false),
        }
    }

    async fn owns_partition_for_write(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<u64> {
        self.verify_and_extend_lease(topic, partition, lease_secs)
            .await
    }

    async fn verify_and_extend_lease(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<u64> {
        let mut owners = self.partition_owners.write().await;
        let key = (topic.to_string(), partition);
        let now = Instant::now();

        if let Some((owner_id, expiry)) = owners.get_mut(&key)
            && *owner_id == self.broker_id
            && *expiry > now
        {
            let new_expiry = now + Duration::from_secs(lease_secs);
            *expiry = new_expiry;
            return Ok(lease_secs);
        }
        Err(SlateDBError::Fenced)
    }

    async fn invalidate_ownership_cache(&self, _topic: &str, _partition: i32) {
        // No-op for the mock, as it doesn't have a cache layer.
    }

    async fn invalidate_all_ownership_cache(&self) {
        // No-op
    }

    async fn should_own_partition(&self, topic: &str, partition: i32) -> SlateDBResult<bool> {
        use super::coordinator::consistent_hash_assignment;

        // Use consistent hash assignment like production for multi-broker testing
        let brokers_map = self.brokers.read().await;
        let brokers: Vec<BrokerInfo> = brokers_map.values().map(|(info, _)| info.clone()).collect();

        // If only one broker or no brokers, this broker owns everything
        if brokers.len() <= 1 {
            return Ok(true);
        }

        // Use consistent hashing to determine ownership
        let assigned_broker = consistent_hash_assignment(topic, partition, &brokers);
        Ok(assigned_broker == self.broker_id)
    }

    async fn register_topic(&self, topic: &str, partitions: i32) -> SlateDBResult<()> {
        self.register_topic_with_config(topic, partitions, HashMap::new())
            .await
    }

    async fn register_topic_with_config(
        &self,
        topic: &str,
        partitions: i32,
        config: HashMap<String, String>,
    ) -> SlateDBResult<()> {
        let mut topics = self.topics.write().await;
        let mut configs = self.topic_configs.write().await;
        topics.insert(topic.to_string(), partitions);
        // Seed/replace the config entry. Keeping the maps lockstep is the
        // mock's analogue of `topic_configs` being a field on the registry's
        // `TopicInfo` in the real Raft coordinator.
        configs.insert(topic.to_string(), config);
        Ok(())
    }

    async fn get_topic_config(
        &self,
        topic: &str,
    ) -> SlateDBResult<Option<HashMap<String, String>>> {
        let configs = self.topic_configs.read().await;
        Ok(configs.get(topic).cloned())
    }

    async fn update_topic_config(
        &self,
        topic: &str,
        config: HashMap<String, String>,
    ) -> SlateDBResult<bool> {
        let topics = self.topics.read().await;
        if !topics.contains_key(topic) {
            return Ok(false);
        }
        drop(topics);
        let mut configs = self.topic_configs.write().await;
        configs.insert(topic.to_string(), config);
        Ok(true)
    }

    async fn get_topics(&self) -> SlateDBResult<Vec<String>> {
        let topics = self.topics.read().await;
        Ok(topics.keys().cloned().collect())
    }

    async fn topic_exists(&self, topic: &str) -> SlateDBResult<bool> {
        let topics = self.topics.read().await;
        Ok(topics.contains_key(topic))
    }

    async fn get_partition_count(&self, topic: &str) -> SlateDBResult<Option<i32>> {
        let topics = self.topics.read().await;
        Ok(topics.get(topic).copied())
    }

    async fn delete_topic(&self, topic: &str) -> SlateDBResult<bool> {
        let mut topics = self.topics.write().await;
        let removed = topics.remove(topic).is_some();
        drop(topics);
        // Drop the matching config entry; otherwise a recreate-with-same-name
        // would silently inherit the stale config and a Describe immediately
        // after Delete would report a topic the broker says doesn't exist.
        if removed {
            self.topic_configs.write().await.remove(topic);
        }
        Ok(removed)
    }

    async fn get_assigned_partitions(&self) -> SlateDBResult<Vec<(String, i32)>> {
        // This is a simplified assignment. A real implementation would use a consistent
        // hashing or round-robin strategy.
        let owners = self.partition_owners.read().await;
        let my_partitions = owners
            .iter()
            .filter(|(_key, (owner_id, _))| *owner_id == self.broker_id)
            .map(|(key, _)| key.clone())
            .collect();
        Ok(my_partitions)
    }

    async fn get_partition_owners(&self, topic: &str) -> SlateDBResult<Vec<(i32, Option<i32>)>> {
        let topics = self.topics.read().await;
        let count = topics.get(topic).copied().unwrap_or(0);
        let owners = self.partition_owners.read().await;
        let now = Instant::now();

        let mut result = Vec::new();
        for i in 0..count {
            // Only return owner if lease hasn't expired (match production semantics)
            let owner = owners
                .get(&(topic.to_string(), i))
                .and_then(|(id, expiry)| if *expiry > now { Some(*id) } else { None });
            result.push((i, owner));
        }
        Ok(result)
    }
}

// Dummy implementations for the other traits
#[async_trait]
impl ConsumerGroupCoordinator for MockCoordinator {
    async fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        protocol_metadata: &[u8],
        session_timeout_ms: i32,
    ) -> SlateDBResult<(i32, String, bool, String, Vec<String>)> {
        let mut groups = self.consumer_groups.write().await;
        let group = groups.entry(group_id.to_string()).or_default();

        // Assign member ID if empty
        let final_member_id = if member_id.is_empty() {
            let id = self.next_member_id.fetch_add(1, Ordering::SeqCst);
            format!("member-{}-{}", group_id, id)
        } else {
            member_id.to_string()
        };

        // Add or update member
        let member = MockMember {
            member_id: final_member_id.clone(),
            metadata: protocol_metadata.to_vec(),
            protocols: vec![("range".to_string(), protocol_metadata.to_vec())],
            assignment: Vec::new(),
            session_timeout_ms,
            last_heartbeat: Instant::now(),
        };
        group.members.insert(final_member_id.clone(), member);

        // Trigger rebalance by incrementing generation
        // Handle overflow: wrap to 1 if we reach i32::MAX (0 is avoided)
        group.generation_id = if group.generation_id == i32::MAX {
            1
        } else {
            group.generation_id + 1
        };
        group.state = GroupState::PreparingRebalance;

        // First member becomes leader
        let is_leader =
            group.leader_id.is_none() || group.leader_id.as_ref() == Some(&final_member_id);
        if is_leader {
            group.leader_id = Some(final_member_id.clone());
        }

        let leader_id = group.leader_id.clone().unwrap_or_default();
        let members: Vec<String> = group.members.keys().cloned().collect();

        Ok((
            group.generation_id,
            final_member_id,
            is_leader,
            leader_id,
            members,
        ))
    }

    async fn join_group_with_protocols(
        &self,
        group_id: &str,
        member_id: &str,
        protocols: &[(String, Vec<u8>)],
        session_timeout_ms: i32,
        _rebalance_timeout_ms: i32,
    ) -> SlateDBResult<super::traits::GroupJoinOutcome> {
        use super::raft::domains::group::{ProtocolSelection, select_group_protocol};
        use super::traits::{GroupJoinOutcome, GroupJoinResult};

        let mut groups = self.consumer_groups.write().await;
        let group = groups.entry(group_id.to_string()).or_default();

        let final_member_id = if member_id.is_empty() {
            let id = self.next_member_id.fetch_add(1, Ordering::SeqCst);
            format!("member-{}-{}", group_id, id)
        } else {
            member_id.to_string()
        };

        // Negotiate the protocol across existing members plus the joiner
        // BEFORE mutating state, mirroring the Raft group domain. The
        // prospective leader's preferences go first to drive tie-breaks.
        let prospective_leader = group
            .leader_id
            .clone()
            .unwrap_or_else(|| final_member_id.clone());
        let mut member_prefs: Vec<(String, Vec<String>)> = group
            .members
            .iter()
            .filter(|(id, _)| **id != final_member_id)
            .map(|(id, m)| {
                (
                    id.clone(),
                    m.protocols.iter().map(|(n, _)| n.clone()).collect(),
                )
            })
            .collect();
        member_prefs.push((
            final_member_id.clone(),
            protocols.iter().map(|(n, _)| n.clone()).collect(),
        ));
        member_prefs.sort_by(|a, b| {
            let a_is_leader = a.0 == prospective_leader;
            let b_is_leader = b.0 == prospective_leader;
            b_is_leader.cmp(&a_is_leader).then_with(|| a.0.cmp(&b.0))
        });
        let preference_lists: Vec<Vec<String>> =
            member_prefs.into_iter().map(|(_, prefs)| prefs).collect();

        let selected_protocol = match select_group_protocol(&preference_lists) {
            ProtocolSelection::Selected(name) => Some(name),
            ProtocolSelection::None => None,
            ProtocolSelection::Inconsistent => {
                return Ok(GroupJoinOutcome::InconsistentProtocol);
            }
        };

        // Add or refresh the member with its full protocol list.
        group.members.insert(
            final_member_id.clone(),
            MockMember {
                member_id: final_member_id.clone(),
                metadata: protocols
                    .first()
                    .map(|(_, m)| m.clone())
                    .unwrap_or_default(),
                protocols: protocols.to_vec(),
                assignment: Vec::new(),
                session_timeout_ms,
                last_heartbeat: Instant::now(),
            },
        );

        // Trigger a rebalance.
        group.generation_id = if group.generation_id == i32::MAX {
            1
        } else {
            group.generation_id + 1
        };
        group.state = GroupState::PreparingRebalance;
        group.protocol_name = selected_protocol.clone();

        if group.leader_id.is_none() {
            group.leader_id = Some(final_member_id.clone());
        }
        let leader_id = group.leader_id.clone().unwrap_or_default();
        let is_leader = leader_id == final_member_id;

        // Only the leader receives the member roster, each entry carrying
        // that member's own metadata for the selected protocol.
        let members: Vec<(String, Vec<u8>)> = if is_leader {
            let mut described: Vec<(String, Vec<u8>)> = group
                .members
                .values()
                .map(|m| {
                    let metadata = match selected_protocol.as_deref() {
                        Some(name) => m
                            .protocols
                            .iter()
                            .find(|(n, _)| n == name)
                            .map(|(_, md)| md.clone())
                            .unwrap_or_else(|| m.metadata.clone()),
                        None => m.metadata.clone(),
                    };
                    (m.member_id.clone(), metadata)
                })
                .collect();
            described.sort_by(|a, b| a.0.cmp(&b.0));
            described
        } else {
            Vec::new()
        };

        Ok(GroupJoinOutcome::Joined(GroupJoinResult {
            generation_id: group.generation_id,
            member_id: final_member_id,
            is_leader,
            leader_id,
            protocol_name: selected_protocol.unwrap_or_default(),
            members,
        }))
    }

    async fn get_group_protocol(&self, group_id: &str) -> SlateDBResult<Option<String>> {
        let groups = self.consumer_groups.read().await;
        Ok(groups.get(group_id).and_then(|g| g.protocol_name.clone()))
    }

    async fn complete_rebalance(
        &self,
        group_id: &str,
        expected_generation: i32,
    ) -> SlateDBResult<bool> {
        let mut groups = self.consumer_groups.write().await;
        if let Some(group) = groups.get_mut(group_id)
            && group.generation_id == expected_generation
        {
            group.state = GroupState::Stable;
            return Ok(true);
        }
        Ok(false)
    }

    async fn get_generation(&self, group_id: &str) -> SlateDBResult<i32> {
        let groups = self.consumer_groups.read().await;
        Ok(groups.get(group_id).map(|g| g.generation_id).unwrap_or(0))
    }

    async fn upsert_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        metadata: &[u8],
    ) -> SlateDBResult<()> {
        let mut groups = self.consumer_groups.write().await;
        let group = groups.entry(group_id.to_string()).or_default();

        if let Some(member) = group.members.get_mut(member_id) {
            member.metadata = metadata.to_vec();
            member.last_heartbeat = Instant::now();
        } else {
            group.members.insert(
                member_id.to_string(),
                MockMember {
                    member_id: member_id.to_string(),
                    metadata: metadata.to_vec(),
                    protocols: Vec::new(),
                    assignment: Vec::new(),
                    session_timeout_ms: 30000,
                    last_heartbeat: Instant::now(),
                },
            );
        }
        Ok(())
    }

    async fn get_group_members(&self, group_id: &str) -> SlateDBResult<Vec<String>> {
        let groups = self.consumer_groups.read().await;
        Ok(groups
            .get(group_id)
            .map(|g| g.members.keys().cloned().collect())
            .unwrap_or_default())
    }

    async fn get_group_leader(&self, group_id: &str) -> SlateDBResult<Option<String>> {
        let groups = self.consumer_groups.read().await;
        Ok(groups.get(group_id).and_then(|g| g.leader_id.clone()))
    }

    async fn remove_group_member(&self, group_id: &str, member_id: &str) -> SlateDBResult<()> {
        let mut groups = self.consumer_groups.write().await;
        if let Some(group) = groups.get_mut(group_id) {
            group.members.remove(member_id);

            // If leader left, elect new leader
            if group.leader_id.as_ref() == Some(&member_id.to_string()) {
                group.leader_id = group.members.keys().next().cloned();
            }

            // Trigger rebalance if members remain
            if !group.members.is_empty() {
                // Handle overflow: wrap to 1 if we reach i32::MAX (0 is avoided)
                group.generation_id = if group.generation_id == i32::MAX {
                    1
                } else {
                    group.generation_id + 1
                };
                group.state = GroupState::PreparingRebalance;
            } else {
                group.state = GroupState::Empty;
            }
        }
        Ok(())
    }

    async fn leave_group(&self, group_id: &str, member_id: &str) -> SlateDBResult<bool> {
        // Report whether the member actually existed so LeaveGroup can
        // answer UNKNOWN_MEMBER_ID; an unknown member must not trigger a
        // rebalance.
        {
            let groups = self.consumer_groups.read().await;
            let known = groups
                .get(group_id)
                .is_some_and(|g| g.members.contains_key(member_id));
            if !known {
                return Ok(false);
            }
        }
        self.remove_group_member(group_id, member_id).await?;
        Ok(true)
    }

    async fn update_member_heartbeat_with_generation(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> SlateDBResult<HeartbeatResult> {
        let mut groups = self.consumer_groups.write().await;
        if let Some(group) = groups.get_mut(group_id) {
            if generation_id != group.generation_id {
                return Ok(HeartbeatResult::IllegalGeneration);
            }
            if let Some(member) = group.members.get_mut(member_id) {
                member.last_heartbeat = Instant::now();
                return Ok(HeartbeatResult::Success);
            }
            return Ok(HeartbeatResult::UnknownMember);
        }
        Ok(HeartbeatResult::UnknownMember)
    }

    async fn validate_member_for_commit(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> SlateDBResult<HeartbeatResult> {
        //Read-only validation (doesn't update heartbeat timestamp)
        let groups = self.consumer_groups.read().await;
        if let Some(group) = groups.get(group_id) {
            if generation_id != group.generation_id {
                return Ok(HeartbeatResult::IllegalGeneration);
            }
            if group.members.contains_key(member_id) {
                return Ok(HeartbeatResult::Success);
            }
            return Ok(HeartbeatResult::UnknownMember);
        }
        Ok(HeartbeatResult::UnknownMember)
    }

    async fn set_assignments_atomically(
        &self,
        group_id: &str,
        leader_id: &str,
        expected_generation: i32,
        assignments: &[(String, Vec<u8>)],
    ) -> SlateDBResult<bool> {
        let mut groups = self.consumer_groups.write().await;
        if let Some(group) = groups.get_mut(group_id) {
            // Verify leader and generation
            if group.leader_id.as_ref() != Some(&leader_id.to_string()) {
                return Ok(false);
            }
            if group.generation_id != expected_generation {
                return Ok(false);
            }

            // Apply assignments
            for (member_id, assignment) in assignments {
                if let Some(member) = group.members.get_mut(member_id) {
                    member.assignment = assignment.clone();
                }
            }
            group.state = GroupState::CompletingRebalance;
            return Ok(true);
        }
        Ok(false)
    }

    async fn evict_expired_members(
        &self,
        default_session_timeout_ms: u64,
    ) -> SlateDBResult<Vec<(String, String)>> {
        let mut groups = self.consumer_groups.write().await;
        let now = Instant::now();
        let mut evicted = Vec::new();

        for (group_id, group) in groups.iter_mut() {
            let expired_members: Vec<String> = group
                .members
                .iter()
                .filter(|(_, m)| {
                    let timeout = Duration::from_millis(m.session_timeout_ms as u64)
                        .max(Duration::from_millis(default_session_timeout_ms));
                    now.duration_since(m.last_heartbeat) > timeout
                })
                .map(|(id, _)| id.clone())
                .collect();

            if expired_members.is_empty() {
                continue;
            }

            for member_id in &expired_members {
                group.members.remove(member_id);
                if group.leader_id.as_ref() == Some(member_id) {
                    group.leader_id = None;
                }
                evicted.push((group_id.clone(), member_id.clone()));
            }

            // Mirror remove_group_member's post-conditions: a silent
            // session-timeout eviction must drive the same state machine
            // as an explicit LeaveGroup. Without this, surviving members
            // never get reassigned because state, generation, and leader
            // never refresh — a stuck group.
            if group.members.is_empty() {
                group.state = GroupState::Empty;
                group.leader_id = None;
            } else {
                if group.leader_id.is_none() {
                    group.leader_id = group.members.keys().next().cloned();
                }
                group.generation_id = if group.generation_id == i32::MAX {
                    1
                } else {
                    group.generation_id + 1
                };
                group.state = GroupState::PreparingRebalance;
            }
        }
        Ok(evicted)
    }

    async fn expire_committed_offsets(&self, ttl_ms: u64) -> SlateDBResult<usize> {
        let now_ms = chrono::Utc::now().timestamp_millis();
        let mut offsets = self.offsets.write().await;
        let before = offsets.len();
        offsets.retain(|_, stored| {
            let age = now_ms.saturating_sub(stored.commit_timestamp);
            age <= ttl_ms as i64
        });
        Ok(before - offsets.len())
    }

    async fn get_all_groups(&self) -> SlateDBResult<Vec<String>> {
        let groups = self.consumer_groups.read().await;
        Ok(groups.keys().cloned().collect())
    }

    async fn delete_consumer_group(&self, group_id: &str) -> SlateDBResult<()> {
        let mut groups = self.consumer_groups.write().await;
        groups.remove(group_id);
        Ok(())
    }

    #[allow(clippy::collapsible_if)]
    async fn set_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        assignment: &[u8],
    ) -> SlateDBResult<()> {
        let mut groups = self.consumer_groups.write().await;
        if let Some(group) = groups.get_mut(group_id) {
            if let Some(member) = group.members.get_mut(member_id) {
                member.assignment = assignment.to_vec();
            }
        }
        Ok(())
    }

    async fn get_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> SlateDBResult<Vec<u8>> {
        let groups = self.consumer_groups.read().await;
        Ok(groups
            .get(group_id)
            .and_then(|g| g.members.get(member_id))
            .map(|m| m.assignment.clone())
            .unwrap_or_default())
    }

    async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> SlateDBResult<()> {
        let mut offsets = self.offsets.write().await;
        let key = OffsetKey {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        };
        offsets.insert(
            key,
            StoredOffset {
                offset,
                metadata: metadata.map(String::from),
                commit_timestamp: Utc::now().timestamp_millis(),
            },
        );
        Ok(())
    }

    async fn fetch_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<(i64, Option<String>)> {
        let offsets = self.offsets.read().await;
        let key = OffsetKey {
            group_id: group_id.to_string(),
            topic: topic.to_string(),
            partition,
        };
        Ok(offsets
            .get(&key)
            .map(|o| (o.offset, o.metadata.clone()))
            .unwrap_or((-1, None)))
    }
}

#[async_trait]
impl ProducerCoordinator for MockCoordinator {
    async fn next_producer_id(&self) -> SlateDBResult<i64> {
        Ok(self.next_producer_id.fetch_add(1, Ordering::SeqCst))
    }

    async fn init_producer_id(
        &self,
        _transactional_id: Option<&str>,
        _requested_producer_id: i64,
        _requested_epoch: i16,
    ) -> SlateDBResult<(i64, i16)> {
        let producer_id = self.next_producer_id.fetch_add(1, Ordering::SeqCst);
        Ok((producer_id, 0))
    }

    async fn store_producer_state(
        &self,
        topic: &str,
        partition: i32,
        producer_id: i64,
        last_sequence: i32,
        producer_epoch: i16,
    ) -> SlateDBResult<()> {
        let mut states = self.producer_states.write().await;
        states.insert(
            (topic.to_string(), partition, producer_id),
            (last_sequence, producer_epoch),
        );
        Ok(())
    }

    async fn load_producer_states(
        &self,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<
        std::collections::HashMap<i64, super::coordinator::producer::PersistedProducerState>,
    > {
        let states = self.producer_states.read().await;
        let mut result = std::collections::HashMap::new();

        for ((t, p, pid), (seq, epoch)) in states.iter() {
            if t == topic && *p == partition {
                result.insert(
                    *pid,
                    super::coordinator::producer::PersistedProducerState {
                        last_sequence: *seq,
                        producer_epoch: *epoch,
                    },
                );
            }
        }

        Ok(result)
    }
}

// ============================================================================
// PartitionTransferCoordinator Implementation for Fast Failover
// ============================================================================

#[async_trait]
impl PartitionTransferCoordinator for MockCoordinator {
    async fn transfer_partition(
        &self,
        topic: &str,
        partition: i32,
        from_broker_id: i32,
        to_broker_id: i32,
        _reason: TransferReason,
        lease_secs: u64,
    ) -> SlateDBResult<()> {
        let mut owners = self.partition_owners.write().await;
        let key = (topic.to_string(), partition);

        // Verify from_broker_id is the current owner
        if let Some((owner, expiry)) = owners.get(&key) {
            if *owner != from_broker_id || *expiry <= Instant::now() {
                return Err(SlateDBError::Storage(format!(
                    "Transfer failed: broker {} is not the current owner of {}/{}",
                    from_broker_id, topic, partition
                )));
            }
        } else {
            return Err(SlateDBError::Storage(format!(
                "Transfer failed: partition {}/{} not found",
                topic, partition
            )));
        }

        // Verify to_broker_id is registered
        let brokers = self.brokers.read().await;
        if !brokers.contains_key(&to_broker_id) {
            return Err(SlateDBError::Storage(format!(
                "Transfer failed: destination broker {} not registered",
                to_broker_id
            )));
        }
        drop(brokers);

        // Transfer ownership
        owners.insert(
            key.clone(),
            (
                to_broker_id,
                Instant::now() + Duration::from_secs(lease_secs),
            ),
        );

        // Bump leader epoch — transfer is a leadership change and the
        // outgoing broker must be fenced if it later writes under its
        // stale epoch.
        let mut epochs = self.partition_epochs.write().await;
        let next = epochs.get(&key).copied().unwrap_or(0).saturating_add(1);
        epochs.insert(key, next);

        Ok(())
    }

    async fn batch_transfer_partitions(
        &self,
        transfers: Vec<PartitionTransfer>,
        reason: TransferReason,
        lease_duration_ms: u64,
    ) -> SlateDBResult<(usize, Vec<(String, i32, String)>)> {
        let mut successful = 0;
        let mut failed = Vec::new();

        let lease_secs = lease_duration_ms / 1000;

        for transfer in transfers {
            match self
                .transfer_partition(
                    &transfer.topic,
                    transfer.partition,
                    transfer.from_broker_id,
                    transfer.to_broker_id,
                    reason.clone(),
                    lease_secs,
                )
                .await
            {
                Ok(()) => successful += 1,
                Err(e) => {
                    failed.push((transfer.topic, transfer.partition, e.to_string()));
                }
            }
        }

        Ok((successful, failed))
    }

    async fn mark_broker_failed(
        &self,
        broker_id: i32,
        _reason: &str,
    ) -> SlateDBResult<Vec<(String, i32)>> {
        // Release (and collect) partitions owned by this broker, mirroring
        // the atomic fence+release semantics of the Raft implementation.
        let mut owners = self.partition_owners.write().await;
        let now = Instant::now();
        let released: Vec<(String, i32)> = owners
            .iter()
            .filter(|(_, (owner, expiry))| *owner == broker_id && *expiry > now)
            .map(|((topic, partition), _)| (topic.clone(), *partition))
            .collect();
        for key in &released {
            owners.remove(key);
        }
        drop(owners);

        // Bump epochs for every released partition. The fence-and-release
        // semantics here must include an epoch bump or a future writer
        // re-acquires at the same epoch the failed broker last held —
        // defeating the point of fencing.
        if !released.is_empty() {
            let mut epochs = self.partition_epochs.write().await;
            for key in &released {
                let next = epochs.get(key).copied().unwrap_or(0).saturating_add(1);
                epochs.insert(key.clone(), next);
            }
        }

        // Remove broker from active brokers
        let mut brokers = self.brokers.write().await;
        brokers.remove(&broker_id);

        Ok(released)
    }

    async fn get_all_partition_owners(&self) -> HashMap<(String, i32), i32> {
        let owners = self.partition_owners.read().await;
        let now = Instant::now();

        owners
            .iter()
            .filter(|(_, (_, expiry))| *expiry > now)
            .map(|((topic, partition), (owner, _))| ((topic.clone(), *partition), *owner))
            .collect()
    }

    async fn get_all_partition_owners_with_index(&self) -> (HashMap<(String, i32), i32>, u64) {
        let owners = self.partition_owners.read().await;
        let now = Instant::now();
        let index = self.mock_raft_index.load(Ordering::SeqCst);

        let owners_map = owners
            .iter()
            .filter(|(_, (_, expiry))| *expiry > now)
            .map(|((topic, partition), (owner, _))| ((topic.clone(), *partition), *owner))
            .collect();

        (owners_map, index)
    }

    async fn get_active_broker_ids(&self) -> Vec<i32> {
        let brokers = self.brokers.read().await;
        brokers.keys().copied().collect()
    }
}

// ============================================================================
// Comprehensive Tests for MockCoordinator
// ============================================================================

#[cfg(test)]
#[path = "mock_coordinator_tests.rs"]
mod tests;
