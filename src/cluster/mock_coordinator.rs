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
    #[allow(dead_code)]
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
    /// Shared partition ownership: (topic, partition) -> (broker_id, lease_expiry)
    pub partition_owners: Arc<RwLock<PartitionOwnersMap>>,
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
            partition_owners: Arc::new(RwLock::new(HashMap::new())),
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
                    key,
                    (
                        self.broker_id,
                        Instant::now() + Duration::from_secs(lease_secs),
                    ),
                );
                Ok(true)
            }
        }
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
        let mut topics = self.topics.write().await;
        topics.insert(topic.to_string(), partitions);
        Ok(())
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
        Ok(topics.remove(topic).is_some())
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

            for member_id in expired_members {
                group.members.remove(&member_id);
                evicted.push((group_id.clone(), member_id));
            }
        }
        Ok(evicted)
    }

    async fn get_all_groups(&self) -> SlateDBResult<Vec<String>> {
        let groups = self.consumer_groups.read().await;
        Ok(groups.keys().cloned().collect())
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
            key,
            (
                to_broker_id,
                Instant::now() + Duration::from_secs(lease_secs),
            ),
        );

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

    async fn mark_broker_failed(&self, broker_id: i32, _reason: &str) -> SlateDBResult<usize> {
        // Count partitions owned by this broker
        let owners = self.partition_owners.read().await;
        let affected_count = owners
            .iter()
            .filter(|(_, (owner, expiry))| *owner == broker_id && *expiry > Instant::now())
            .count();
        drop(owners);

        // Remove broker from active brokers
        let mut brokers = self.brokers.write().await;
        brokers.remove(&broker_id);

        Ok(affected_count)
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
mod tests {
    use super::*;

    // ========================================================================
    // Broker Registration Tests
    // ========================================================================

    #[tokio::test]
    async fn test_broker_registration() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        assert!(coordinator.register_broker().await.is_ok());

        let brokers = coordinator.get_live_brokers().await.unwrap();
        assert_eq!(brokers.len(), 1);
        assert_eq!(brokers[0].broker_id, 1);
        assert_eq!(brokers[0].host, "localhost");
        assert_eq!(brokers[0].port, 9092);
    }

    #[tokio::test]
    async fn test_heartbeat_requires_registration() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        // Heartbeat should fail without registration
        assert!(coordinator.heartbeat().await.is_err());

        // After registration, heartbeat should succeed
        coordinator.register_broker().await.unwrap();
        assert!(coordinator.heartbeat().await.is_ok());
    }

    #[tokio::test]
    async fn test_unregister_broker() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        coordinator.register_broker().await.unwrap();
        assert_eq!(coordinator.get_live_brokers().await.unwrap().len(), 1);

        coordinator.unregister_broker().await.unwrap();
        assert_eq!(coordinator.get_live_brokers().await.unwrap().len(), 0);
    }

    // ========================================================================
    // Partition Ownership Tests
    // ========================================================================

    #[tokio::test]
    async fn test_acquire_partition() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("test-topic", 3).await.unwrap();

        // First acquisition should succeed
        let acquired = coordinator
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        assert!(acquired);

        // Second acquisition by same broker should fail (already owned)
        let acquired_again = coordinator
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        assert!(!acquired_again);
    }

    #[tokio::test]
    async fn test_partition_ownership_check() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("test-topic", 3).await.unwrap();

        // Not owned initially
        assert!(
            !coordinator
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );

        // Acquire and check
        coordinator
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        assert!(
            coordinator
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );
        assert!(
            coordinator
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_renew_partition_lease() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("test-topic", 1).await.unwrap();

        // Acquire first
        coordinator
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();

        // Renew should succeed
        let renewed = coordinator
            .renew_partition_lease("test-topic", 0, 120)
            .await
            .unwrap();
        assert!(renewed);

        // Renew non-owned partition should fail
        let renewed_other = coordinator
            .renew_partition_lease("test-topic", 1, 60)
            .await
            .unwrap();
        assert!(!renewed_other);
    }

    #[tokio::test]
    async fn test_release_partition() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("test-topic", 1).await.unwrap();

        coordinator
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        assert!(
            coordinator
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );

        coordinator
            .release_partition("test-topic", 0)
            .await
            .unwrap();
        assert!(
            !coordinator
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_verify_and_extend_lease() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("test-topic", 1).await.unwrap();

        coordinator
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();

        // Verify and extend should succeed
        let remaining = coordinator
            .verify_and_extend_lease("test-topic", 0, 120)
            .await
            .unwrap();
        assert_eq!(remaining, 120);

        // Non-owned partition should return error
        let result = coordinator
            .verify_and_extend_lease("test-topic", 1, 60)
            .await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_get_partition_owner() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("test-topic", 1).await.unwrap();

        // No owner initially
        assert_eq!(
            coordinator
                .get_partition_owner("test-topic", 0)
                .await
                .unwrap(),
            None
        );

        // Acquire and check owner
        coordinator
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        assert_eq!(
            coordinator
                .get_partition_owner("test-topic", 0)
                .await
                .unwrap(),
            Some(1)
        );
    }

    #[tokio::test]
    async fn test_expire_all_leases() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("test-topic", 2).await.unwrap();

        coordinator
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        coordinator
            .acquire_partition("test-topic", 1, 60)
            .await
            .unwrap();

        assert!(
            coordinator
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );
        assert!(
            coordinator
                .owns_partition_for_read("test-topic", 1)
                .await
                .unwrap()
        );

        // Expire all leases
        coordinator.expire_all_leases().await;

        assert!(
            !coordinator
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );
        assert!(
            !coordinator
                .owns_partition_for_read("test-topic", 1)
                .await
                .unwrap()
        );
    }

    // ========================================================================
    // Topic Management Tests
    // ========================================================================

    #[tokio::test]
    async fn test_register_topic() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        coordinator.register_topic("my-topic", 5).await.unwrap();

        assert!(coordinator.topic_exists("my-topic").await.unwrap());
        assert_eq!(
            coordinator.get_partition_count("my-topic").await.unwrap(),
            Some(5)
        );
    }

    #[tokio::test]
    async fn test_get_topics() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        coordinator.register_topic("topic-a", 1).await.unwrap();
        coordinator.register_topic("topic-b", 2).await.unwrap();

        let topics = coordinator.get_topics().await.unwrap();
        assert_eq!(topics.len(), 2);
        assert!(topics.contains(&"topic-a".to_string()));
        assert!(topics.contains(&"topic-b".to_string()));
    }

    #[tokio::test]
    async fn test_delete_topic() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        coordinator.register_topic("delete-me", 1).await.unwrap();
        assert!(coordinator.topic_exists("delete-me").await.unwrap());

        let deleted = coordinator.delete_topic("delete-me").await.unwrap();
        assert!(deleted);
        assert!(!coordinator.topic_exists("delete-me").await.unwrap());

        // Deleting non-existent topic returns false
        let deleted_again = coordinator.delete_topic("delete-me").await.unwrap();
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn test_topic_count() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        assert_eq!(coordinator.topic_count().await, 0);

        coordinator.register_topic("t1", 1).await.unwrap();
        assert_eq!(coordinator.topic_count().await, 1);

        coordinator.register_topic("t2", 1).await.unwrap();
        assert_eq!(coordinator.topic_count().await, 2);
    }

    // ========================================================================
    // Consumer Group Tests
    // ========================================================================

    #[tokio::test]
    async fn test_join_group_first_member() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        let (gen_id, member_id, is_leader, leader_id, members) = coordinator
            .join_group("my-group", "", &[], 30000)
            .await
            .unwrap();

        assert_eq!(gen_id, 1);
        assert!(!member_id.is_empty());
        assert!(is_leader);
        assert_eq!(leader_id, member_id);
        assert_eq!(members.len(), 1);
    }

    #[tokio::test]
    async fn test_join_group_second_member() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        let (_, first_member, _, _, _) = coordinator
            .join_group("my-group", "", &[], 30000)
            .await
            .unwrap();

        let (gen_id, second_member, is_leader, leader_id, members) = coordinator
            .join_group("my-group", "", &[], 30000)
            .await
            .unwrap();

        // Generation increments on each join
        assert_eq!(gen_id, 2);
        assert!(!is_leader);
        assert_eq!(leader_id, first_member);
        assert_eq!(members.len(), 2);
        assert!(members.contains(&second_member));
    }

    #[tokio::test]
    async fn test_leave_group() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        let (_, member_id, _, _, _) = coordinator
            .join_group("my-group", "", &[], 30000)
            .await
            .unwrap();

        assert!(
            coordinator
                .get_group_members("my-group")
                .await
                .unwrap()
                .contains(&member_id)
        );

        coordinator
            .remove_group_member("my-group", &member_id)
            .await
            .unwrap();

        assert!(
            !coordinator
                .get_group_members("my-group")
                .await
                .unwrap()
                .contains(&member_id)
        );
    }

    #[tokio::test]
    async fn test_leader_election_on_leave() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        // First member is leader
        let (_, first_member, _, _, _) = coordinator
            .join_group("my-group", "member-1", &[], 30000)
            .await
            .unwrap();

        // Second member joins
        let (_, second_member, _, _, _) = coordinator
            .join_group("my-group", "member-2", &[], 30000)
            .await
            .unwrap();

        assert_eq!(
            coordinator.get_group_leader("my-group").await.unwrap(),
            Some(first_member.clone())
        );

        // Leader leaves
        coordinator
            .remove_group_member("my-group", &first_member)
            .await
            .unwrap();

        // Second member should become leader
        assert_eq!(
            coordinator.get_group_leader("my-group").await.unwrap(),
            Some(second_member)
        );
    }

    #[tokio::test]
    async fn test_heartbeat_with_generation() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        let (gen_id, member_id, _, _, _) = coordinator
            .join_group("my-group", "", &[], 30000)
            .await
            .unwrap();

        // Correct generation should succeed
        let result = coordinator
            .update_member_heartbeat_with_generation("my-group", &member_id, gen_id)
            .await
            .unwrap();
        assert_eq!(result, HeartbeatResult::Success);

        // Wrong generation should fail
        let result = coordinator
            .update_member_heartbeat_with_generation("my-group", &member_id, gen_id + 1)
            .await
            .unwrap();
        assert_eq!(result, HeartbeatResult::IllegalGeneration);

        // Unknown member should fail
        let result = coordinator
            .update_member_heartbeat_with_generation("my-group", "unknown-member", gen_id)
            .await
            .unwrap();
        assert_eq!(result, HeartbeatResult::UnknownMember);
    }

    #[tokio::test]
    async fn test_set_assignments_atomically() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        let (gen_id, leader_id, _, _, _) = coordinator
            .join_group("my-group", "", &[], 30000)
            .await
            .unwrap();

        let assignment = vec![0u8, 1, 2, 3];
        let assignments = vec![(leader_id.clone(), assignment.clone())];

        // Set assignments as leader
        let success = coordinator
            .set_assignments_atomically("my-group", &leader_id, gen_id, &assignments)
            .await
            .unwrap();
        assert!(success);

        // Verify assignment was stored
        let stored = coordinator
            .get_member_assignment("my-group", &leader_id)
            .await
            .unwrap();
        assert_eq!(stored, assignment);
    }

    #[tokio::test]
    async fn test_set_assignments_wrong_leader() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        let (gen_id, _, _, _, _) = coordinator
            .join_group("my-group", "", &[], 30000)
            .await
            .unwrap();

        // Try to set assignments as non-leader
        let success = coordinator
            .set_assignments_atomically("my-group", "wrong-leader", gen_id, &[])
            .await
            .unwrap();
        assert!(!success);
    }

    #[tokio::test]
    async fn test_complete_rebalance() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        let (gen_id, _, _, _, _) = coordinator
            .join_group("my-group", "", &[], 30000)
            .await
            .unwrap();

        // Complete with correct generation
        let success = coordinator
            .complete_rebalance("my-group", gen_id)
            .await
            .unwrap();
        assert!(success);

        // Complete with wrong generation should fail
        let success = coordinator
            .complete_rebalance("my-group", gen_id + 1)
            .await
            .unwrap();
        assert!(!success);
    }

    // ========================================================================
    // Offset Management Tests
    // ========================================================================

    #[tokio::test]
    async fn test_commit_and_fetch_offset() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        // Fetch non-existent offset
        let (offset, metadata) = coordinator
            .fetch_offset("my-group", "my-topic", 0)
            .await
            .unwrap();
        assert_eq!(offset, -1);
        assert!(metadata.is_none());

        // Commit offset
        coordinator
            .commit_offset("my-group", "my-topic", 0, 100, Some("test-metadata"))
            .await
            .unwrap();

        // Fetch committed offset
        let (offset, metadata) = coordinator
            .fetch_offset("my-group", "my-topic", 0)
            .await
            .unwrap();
        assert_eq!(offset, 100);
        assert_eq!(metadata, Some("test-metadata".to_string()));
    }

    #[tokio::test]
    async fn test_offset_update() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        coordinator
            .commit_offset("my-group", "my-topic", 0, 50, None)
            .await
            .unwrap();

        let (offset, _) = coordinator
            .fetch_offset("my-group", "my-topic", 0)
            .await
            .unwrap();
        assert_eq!(offset, 50);

        // Update to higher offset
        coordinator
            .commit_offset("my-group", "my-topic", 0, 100, None)
            .await
            .unwrap();

        let (offset, _) = coordinator
            .fetch_offset("my-group", "my-topic", 0)
            .await
            .unwrap();
        assert_eq!(offset, 100);
    }

    #[tokio::test]
    async fn test_offset_per_partition() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        coordinator
            .commit_offset("my-group", "my-topic", 0, 10, None)
            .await
            .unwrap();
        coordinator
            .commit_offset("my-group", "my-topic", 1, 20, None)
            .await
            .unwrap();
        coordinator
            .commit_offset("my-group", "my-topic", 2, 30, None)
            .await
            .unwrap();

        let (o0, _) = coordinator
            .fetch_offset("my-group", "my-topic", 0)
            .await
            .unwrap();
        let (o1, _) = coordinator
            .fetch_offset("my-group", "my-topic", 1)
            .await
            .unwrap();
        let (o2, _) = coordinator
            .fetch_offset("my-group", "my-topic", 2)
            .await
            .unwrap();

        assert_eq!(o0, 10);
        assert_eq!(o1, 20);
        assert_eq!(o2, 30);
    }

    // ========================================================================
    // Producer ID Tests
    // ========================================================================

    #[tokio::test]
    async fn test_next_producer_id() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        let id1 = coordinator.next_producer_id().await.unwrap();
        let id2 = coordinator.next_producer_id().await.unwrap();
        let id3 = coordinator.next_producer_id().await.unwrap();

        assert_eq!(id2, id1 + 1);
        assert_eq!(id3, id2 + 1);
    }

    #[tokio::test]
    async fn test_init_producer_id() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);

        let (id1, epoch1) = coordinator.init_producer_id(None, -1, -1).await.unwrap();
        let (id2, epoch2) = coordinator.init_producer_id(None, -1, -1).await.unwrap();

        assert!(id1 > 0);
        assert!(id2 > id1);
        assert_eq!(epoch1, 0);
        assert_eq!(epoch2, 0);
    }

    // ========================================================================
    // Partition Assignment Tests
    // ========================================================================

    #[tokio::test]
    async fn test_get_assigned_partitions() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("test-topic", 3).await.unwrap();

        // No assignments initially
        let assigned = coordinator.get_assigned_partitions().await.unwrap();
        assert!(assigned.is_empty());

        // Acquire some partitions
        coordinator
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        coordinator
            .acquire_partition("test-topic", 2, 60)
            .await
            .unwrap();

        let assigned = coordinator.get_assigned_partitions().await.unwrap();
        assert_eq!(assigned.len(), 2);
        assert!(assigned.contains(&("test-topic".to_string(), 0)));
        assert!(assigned.contains(&("test-topic".to_string(), 2)));
    }

    #[tokio::test]
    async fn test_get_partition_owners() {
        let coordinator = MockCoordinator::new(1, "localhost", 9092);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("test-topic", 3).await.unwrap();

        // Set some owners
        coordinator
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        // Manually set different owner for partition 1
        coordinator
            .set_partition_owner("test-topic", 1, 2, 60)
            .await;
        // Partition 2 has no owner

        let owners = coordinator
            .get_partition_owners("test-topic")
            .await
            .unwrap();
        assert_eq!(owners.len(), 3);

        assert_eq!(owners[0], (0, Some(1))); // Owned by broker 1
        assert_eq!(owners[1], (1, Some(2))); // Owned by broker 2
        assert_eq!(owners[2], (2, None)); // No owner
    }

    // ========================================================================
    // Partition Failover Tests
    // ========================================================================
    //
    // These tests simulate partition failover scenarios including broker failure,
    // lease expiration, and ownership transfer between brokers.

    #[tokio::test]
    async fn test_partition_failover_on_lease_expiry() {
        // Simulate partition failover when a broker's lease expires
        let broker1 = MockCoordinator::new(1, "localhost", 9092);
        let broker2 = MockCoordinator::new(2, "localhost", 9093);

        // Setup: Share the same state between brokers
        let shared_topics = broker1.topics.clone();
        let shared_owners = broker1.partition_owners.clone();
        let shared_brokers = broker1.brokers.clone();

        // Recreate broker2 with shared state
        let broker2 = MockCoordinator {
            broker_id: 2,
            host: "localhost".to_string(),
            port: 9093,
            brokers: shared_brokers,
            topics: shared_topics,
            partition_owners: shared_owners,
            consumer_groups: broker2.consumer_groups.clone(),
            offsets: broker2.offsets.clone(),
            next_producer_id: broker2.next_producer_id.clone(),
            next_member_id: broker2.next_member_id.clone(),
            producer_states: broker2.producer_states.clone(),
            mock_raft_index: Arc::new(AtomicU64::new(0)),
        };

        // Broker1 registers and acquires partition
        broker1.register_broker().await.unwrap();
        broker1.register_topic("test-topic", 1).await.unwrap();
        let acquired = broker1
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        assert!(acquired, "Broker1 should acquire partition");
        assert!(
            broker1
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );

        // Broker2 tries to acquire - should fail (lease still valid)
        broker2.register_broker().await.unwrap();
        let acquired = broker2
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        assert!(
            !acquired,
            "Broker2 should not acquire partition while Broker1's lease is valid"
        );

        // Simulate Broker1 failure by expiring all leases
        broker1.expire_all_leases().await;

        // Now Broker2 should be able to acquire the partition
        let acquired = broker2
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        assert!(
            acquired,
            "Broker2 should acquire partition after Broker1's lease expired"
        );
        assert!(
            broker2
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );
        assert!(
            !broker1
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap(),
            "Broker1 should no longer own partition"
        );
    }

    #[tokio::test]
    async fn test_partition_ownership_fencing() {
        // Test that verify_and_extend_lease fails when ownership is lost
        let broker1 = MockCoordinator::new(1, "localhost", 9092);
        let broker2 = MockCoordinator::new(2, "localhost", 9093);

        // Share state
        let shared_topics = broker1.topics.clone();
        let shared_owners = broker1.partition_owners.clone();
        let shared_brokers = broker1.brokers.clone();

        let broker2 = MockCoordinator {
            broker_id: 2,
            host: "localhost".to_string(),
            port: 9093,
            brokers: shared_brokers,
            topics: shared_topics,
            partition_owners: shared_owners,
            consumer_groups: broker2.consumer_groups.clone(),
            offsets: broker2.offsets.clone(),
            next_producer_id: broker2.next_producer_id.clone(),
            next_member_id: broker2.next_member_id.clone(),
            producer_states: broker2.producer_states.clone(),
            mock_raft_index: Arc::new(AtomicU64::new(0)),
        };

        // Broker1 acquires partition
        broker1.register_broker().await.unwrap();
        broker2.register_broker().await.unwrap();
        broker1.register_topic("test-topic", 1).await.unwrap();
        broker1
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();

        // Broker1 can extend lease
        let result = broker1.verify_and_extend_lease("test-topic", 0, 60).await;
        assert!(result.is_ok(), "Broker1 should be able to extend lease");

        // Expire and transfer ownership to Broker2
        broker1.expire_all_leases().await;
        broker2
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();

        // Broker1's verify_and_extend should now fail (fenced)
        let result = broker1.verify_and_extend_lease("test-topic", 0, 60).await;
        assert!(result.is_err(), "Broker1 should be fenced out");
        match result {
            Err(SlateDBError::Fenced) => (), // Expected
            Err(e) => panic!("Expected Fenced error, got: {:?}", e),
            Ok(_) => panic!("Should not succeed after being fenced"),
        }
    }

    #[tokio::test]
    async fn test_concurrent_partition_acquisition_race() {
        // Test race condition where two brokers try to acquire the same partition
        let broker1 = MockCoordinator::new(1, "localhost", 9092);
        let broker2 = MockCoordinator::new(2, "localhost", 9093);

        // Share state
        let shared_topics = broker1.topics.clone();
        let shared_owners = broker1.partition_owners.clone();
        let shared_brokers = broker1.brokers.clone();

        let broker2 = MockCoordinator {
            broker_id: 2,
            host: "localhost".to_string(),
            port: 9093,
            brokers: shared_brokers,
            topics: shared_topics,
            partition_owners: shared_owners,
            consumer_groups: broker2.consumer_groups.clone(),
            offsets: broker2.offsets.clone(),
            next_producer_id: broker2.next_producer_id.clone(),
            next_member_id: broker2.next_member_id.clone(),
            producer_states: broker2.producer_states.clone(),
            mock_raft_index: Arc::new(AtomicU64::new(0)),
        };

        broker1.register_broker().await.unwrap();
        broker2.register_broker().await.unwrap();
        broker1.register_topic("test-topic", 1).await.unwrap();

        // Both try to acquire simultaneously - only one should succeed
        let result1 = broker1
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        let result2 = broker2
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();

        // Exactly one should succeed
        assert!(
            result1 ^ result2,
            "Exactly one broker should acquire the partition: broker1={}, broker2={}",
            result1,
            result2
        );

        // Verify consistent ownership
        let owner = broker1.get_partition_owner("test-topic", 0).await.unwrap();
        if result1 {
            assert_eq!(owner, Some(1));
        } else {
            assert_eq!(owner, Some(2));
        }
    }

    #[tokio::test]
    async fn test_partition_release_and_reacquisition() {
        // Test releasing a partition and having another broker acquire it
        let broker1 = MockCoordinator::new(1, "localhost", 9092);
        let broker2 = MockCoordinator::new(2, "localhost", 9093);

        // Share state
        let shared_topics = broker1.topics.clone();
        let shared_owners = broker1.partition_owners.clone();
        let shared_brokers = broker1.brokers.clone();

        let broker2 = MockCoordinator {
            broker_id: 2,
            host: "localhost".to_string(),
            port: 9093,
            brokers: shared_brokers,
            topics: shared_topics,
            partition_owners: shared_owners,
            consumer_groups: broker2.consumer_groups.clone(),
            offsets: broker2.offsets.clone(),
            next_producer_id: broker2.next_producer_id.clone(),
            next_member_id: broker2.next_member_id.clone(),
            producer_states: broker2.producer_states.clone(),
            mock_raft_index: Arc::new(AtomicU64::new(0)),
        };

        broker1.register_broker().await.unwrap();
        broker2.register_broker().await.unwrap();
        broker1.register_topic("test-topic", 1).await.unwrap();

        // Broker1 acquires partition
        broker1
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        assert!(
            broker1
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );

        // Broker1 gracefully releases partition
        broker1.release_partition("test-topic", 0).await.unwrap();
        assert!(
            !broker1
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );

        // Broker2 can now acquire
        let acquired = broker2
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        assert!(acquired, "Broker2 should acquire released partition");
        assert!(
            broker2
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );
    }

    #[tokio::test]
    async fn test_zombie_mode_simulation() {
        // Simulate zombie mode: broker loses all leases but doesn't know it yet
        let broker1 = MockCoordinator::new(1, "localhost", 9092);
        broker1.register_broker().await.unwrap();
        broker1.register_topic("test-topic", 3).await.unwrap();

        // Acquire multiple partitions
        broker1
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        broker1
            .acquire_partition("test-topic", 1, 60)
            .await
            .unwrap();
        broker1
            .acquire_partition("test-topic", 2, 60)
            .await
            .unwrap();

        // All owned
        assert!(
            broker1
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );
        assert!(
            broker1
                .owns_partition_for_read("test-topic", 1)
                .await
                .unwrap()
        );
        assert!(
            broker1
                .owns_partition_for_read("test-topic", 2)
                .await
                .unwrap()
        );

        // Simulate coordinator failure - all leases expire
        broker1.expire_all_leases().await;

        // Now none are owned (zombie state)
        assert!(
            !broker1
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );
        assert!(
            !broker1
                .owns_partition_for_read("test-topic", 1)
                .await
                .unwrap()
        );
        assert!(
            !broker1
                .owns_partition_for_read("test-topic", 2)
                .await
                .unwrap()
        );

        // verify_and_extend should fail for all
        for partition in 0..3 {
            let result = broker1
                .verify_and_extend_lease("test-topic", partition, 60)
                .await;
            assert!(
                result.is_err(),
                "Should be fenced for partition {}",
                partition
            );
        }
    }

    #[tokio::test]
    async fn test_multi_partition_failover() {
        // Test failover of multiple partitions across brokers
        let broker1 = MockCoordinator::new(1, "localhost", 9092);
        let broker2 = MockCoordinator::new(2, "localhost", 9093);
        let broker3 = MockCoordinator::new(3, "localhost", 9094);

        // Share state among all brokers
        let shared_topics = broker1.topics.clone();
        let shared_owners = broker1.partition_owners.clone();
        let shared_brokers = broker1.brokers.clone();

        let broker2 = MockCoordinator {
            broker_id: 2,
            host: "localhost".to_string(),
            port: 9093,
            brokers: shared_brokers.clone(),
            topics: shared_topics.clone(),
            partition_owners: shared_owners.clone(),
            consumer_groups: broker2.consumer_groups.clone(),
            offsets: broker2.offsets.clone(),
            next_producer_id: broker2.next_producer_id.clone(),
            next_member_id: broker2.next_member_id.clone(),
            producer_states: broker2.producer_states.clone(),
            mock_raft_index: Arc::new(AtomicU64::new(0)),
        };

        let broker3 = MockCoordinator {
            broker_id: 3,
            host: "localhost".to_string(),
            port: 9094,
            brokers: shared_brokers,
            topics: shared_topics,
            partition_owners: shared_owners,
            consumer_groups: broker3.consumer_groups.clone(),
            offsets: broker3.offsets.clone(),
            next_producer_id: broker3.next_producer_id.clone(),
            next_member_id: broker3.next_member_id.clone(),
            producer_states: broker3.producer_states.clone(),
            mock_raft_index: Arc::new(AtomicU64::new(0)),
        };

        // Register all brokers and create topic
        broker1.register_broker().await.unwrap();
        broker2.register_broker().await.unwrap();
        broker3.register_broker().await.unwrap();
        broker1.register_topic("test-topic", 6).await.unwrap();

        // Distribute partitions: Broker1 gets 0,1,2 - Broker2 gets 3,4,5
        broker1
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        broker1
            .acquire_partition("test-topic", 1, 60)
            .await
            .unwrap();
        broker1
            .acquire_partition("test-topic", 2, 60)
            .await
            .unwrap();
        broker2
            .acquire_partition("test-topic", 3, 60)
            .await
            .unwrap();
        broker2
            .acquire_partition("test-topic", 4, 60)
            .await
            .unwrap();
        broker2
            .acquire_partition("test-topic", 5, 60)
            .await
            .unwrap();

        // Verify distribution
        for p in 0..3 {
            assert_eq!(
                broker1.get_partition_owner("test-topic", p).await.unwrap(),
                Some(1)
            );
        }
        for p in 3..6 {
            assert_eq!(
                broker1.get_partition_owner("test-topic", p).await.unwrap(),
                Some(2)
            );
        }

        // Broker1 fails - expire only broker1's leases (not broker2's)
        broker1.expire_my_leases().await;

        // Broker3 takes over partitions 0,1,2
        broker3
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        broker3
            .acquire_partition("test-topic", 1, 60)
            .await
            .unwrap();
        broker3
            .acquire_partition("test-topic", 2, 60)
            .await
            .unwrap();

        // Verify new distribution
        for p in 0..3 {
            assert_eq!(
                broker1.get_partition_owner("test-topic", p).await.unwrap(),
                Some(3)
            );
        }
        for p in 3..6 {
            assert_eq!(
                broker1.get_partition_owner("test-topic", p).await.unwrap(),
                Some(2)
            );
        }
    }

    #[tokio::test]
    async fn test_ownership_check_during_write() {
        // Simulate the pattern used in PartitionStore::append_batch
        let broker1 = MockCoordinator::new(1, "localhost", 9092);
        broker1.register_broker().await.unwrap();
        broker1.register_topic("test-topic", 1).await.unwrap();
        broker1
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();

        // Simulate write path: verify_and_extend_lease before write
        let result = broker1.verify_and_extend_lease("test-topic", 0, 60).await;
        assert!(
            result.is_ok(),
            "Should be able to extend lease before write"
        );

        // Simulate lease expiry during long write (race condition)
        broker1.expire_all_leases().await;

        // Post-write verification would fail (this is caught by SlateDB fencing in production)
        let result = broker1.verify_and_extend_lease("test-topic", 0, 60).await;
        assert!(
            result.is_err(),
            "Should fail after lease expired during write"
        );
    }

    #[tokio::test]
    async fn test_lease_renewal_keeps_ownership() {
        // Test that regular lease renewal maintains ownership
        let broker1 = MockCoordinator::new(1, "localhost", 9092);
        let broker2 = MockCoordinator::new(2, "localhost", 9093);

        // Share state
        let shared_topics = broker1.topics.clone();
        let shared_owners = broker1.partition_owners.clone();
        let shared_brokers = broker1.brokers.clone();

        let broker2 = MockCoordinator {
            broker_id: 2,
            host: "localhost".to_string(),
            port: 9093,
            brokers: shared_brokers,
            topics: shared_topics,
            partition_owners: shared_owners,
            consumer_groups: broker2.consumer_groups.clone(),
            offsets: broker2.offsets.clone(),
            next_producer_id: broker2.next_producer_id.clone(),
            next_member_id: broker2.next_member_id.clone(),
            producer_states: broker2.producer_states.clone(),
            mock_raft_index: Arc::new(AtomicU64::new(0)),
        };

        broker1.register_broker().await.unwrap();
        broker2.register_broker().await.unwrap();
        broker1.register_topic("test-topic", 1).await.unwrap();

        // Broker1 acquires with short lease
        broker1
            .acquire_partition("test-topic", 0, 10)
            .await
            .unwrap();

        // Renew lease multiple times
        for _ in 0..5 {
            let renewed = broker1
                .renew_partition_lease("test-topic", 0, 10)
                .await
                .unwrap();
            assert!(renewed, "Renewal should succeed");
        }

        // Broker1 still owns partition
        assert!(
            broker1
                .owns_partition_for_read("test-topic", 0)
                .await
                .unwrap()
        );

        // Broker2 cannot acquire (lease is still valid)
        let acquired = broker2
            .acquire_partition("test-topic", 0, 60)
            .await
            .unwrap();
        assert!(
            !acquired,
            "Broker2 should not acquire while Broker1 keeps renewing"
        );
    }
}
