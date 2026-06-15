//! Coordinator traits for cluster-wide state management.
//!
//! These traits abstract the coordination layer, allowing for:
//! - Different backend implementations (Raft, in-memory for testing)
//! - Easier testing with mock coordinators
//! - Clear separation of concerns
//!
//! # Available Implementations
//!
//! - [`RaftCoordinator`](super::raft::RaftCoordinator): Production Raft backend (default, recommended)
//! - [`MockCoordinator`](super::MockCoordinator): In-memory mock for testing
//!
//! # Design Philosophy
//!
//! This project uses Raft as the sole coordination mechanism because:
//! 1. **Strong consistency**: Raft provides linearizable reads/writes
//! 2. **No external dependencies**: Embedded consensus, no external services needed
//! 3. **Split-brain prevention**: Leader election prevents multiple writers
//! 4. **Proven correctness**: Raft is a well-studied consensus protocol
//!
//! # Example: Mock Coordinator for Testing
//!
//! ```text
//! use kafkaesque::cluster::traits::PartitionCoordinator;
//! use async_trait::async_trait;
//!
//! struct MockCoordinator {
//!     broker_id: i32,
//! }
//!
//! #[async_trait]
//! impl PartitionCoordinator for MockCoordinator {
//!     fn broker_id(&self) -> i32 { self.broker_id }
//!     // ... implement other methods with test behavior
//! }
//! ```
//!
//! # Trait Hierarchy
//!
//! - [`PartitionCoordinator`]: Broker registration, partition ownership, topic metadata
//! - [`ConsumerGroupCoordinator`]: Consumer groups, membership, offsets
//! - [`ProducerCoordinator`]: Producer ID allocation, epoch tracking
//! - [`ClusterCoordinator`]: Combined trait (auto-implemented via blanket impl)

use async_trait::async_trait;

use super::coordinator::BrokerInfo;
use super::error::SlateDBResult;

/// Coordinator for partition ownership and cluster metadata.
///
/// This trait handles:
/// - Broker registration and heartbeat
/// - Partition ownership (acquire/renew/release)
/// - Topic metadata
#[async_trait]
pub trait PartitionCoordinator: Send + Sync {
    /// Get this broker's ID.
    fn broker_id(&self) -> i32;

    // ========================================================================
    // Broker Registration
    // ========================================================================

    /// Register this broker in the cluster.
    async fn register_broker(&self) -> SlateDBResult<()>;

    /// Send a heartbeat to indicate this broker is alive.
    async fn heartbeat(&self) -> SlateDBResult<()>;

    /// Get list of live brokers.
    async fn get_live_brokers(&self) -> SlateDBResult<Vec<BrokerInfo>>;

    /// Get the broker ID of the current cluster controller (the Raft
    /// leader for Raft-backed coordinators).
    ///
    /// Returns `Ok(None)` when no leader is currently known (e.g. during
    /// an election); callers should map that to `-1` per Kafka convention.
    async fn current_leader_id(&self) -> SlateDBResult<Option<i32>> {
        Ok(None)
    }

    /// Unregister this broker (on shutdown).
    async fn unregister_broker(&self) -> SlateDBResult<()>;

    // ========================================================================
    // Partition Ownership
    // ========================================================================

    /// Attempt to acquire ownership of a partition.
    ///
    /// Returns `Ok(true)` if acquired, `Ok(false)` if owned by another broker.
    async fn acquire_partition(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<bool>;

    /// Attempt to acquire ownership of a partition, returning the leader epoch.
    ///
    /// This is the preferred method for production use as it returns the
    /// `leader_epoch` needed for epoch-based fencing to prevent TOCTOU races.
    ///
    /// # Returns
    /// - `Ok(Some(epoch))` if acquired, with the current leader epoch
    /// - `Ok(None)` if owned by another broker
    /// - `Err(_)` on coordination failure
    ///
    /// # Default Implementation
    /// Falls back to `acquire_partition()` with epoch 0 for coordinators that
    /// don't track epochs (e.g., mock implementations).
    async fn acquire_partition_with_epoch(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<Option<i32>> {
        match self.acquire_partition(topic, partition, lease_secs).await {
            Ok(true) => Ok(Some(0)),
            Ok(false) => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Renew ownership lease for a partition we own.
    async fn renew_partition_lease(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<bool>;

    /// Renew leases for a batch of owned partitions in a single Raft proposal.
    ///
    /// Default implementation falls back to N sequential `renew_partition_lease`
    /// calls for coordinators that don't override (e.g. mocks). The Raft impl
    /// overrides this with one Raft commit covering every entry, replacing N
    /// round trips with one and amortizing fsync cost across the batch.
    async fn renew_partition_leases_batch(
        &self,
        partitions: Vec<(std::sync::Arc<str>, i32)>,
        lease_secs: u64,
    ) -> SlateDBResult<Vec<(std::sync::Arc<str>, i32, bool)>> {
        let mut out = Vec::with_capacity(partitions.len());
        for (topic, partition) in partitions {
            let renewed = self
                .renew_partition_lease(&topic, partition, lease_secs)
                .await?;
            out.push((topic, partition, renewed));
        }
        Ok(out)
    }

    /// Release ownership of a partition.
    async fn release_partition(&self, topic: &str, partition: i32) -> SlateDBResult<()>;

    /// Get current owner of a partition.
    async fn get_partition_owner(&self, topic: &str, partition: i32) -> SlateDBResult<Option<i32>>;

    // ========================================================================
    // Simplified Ownership API (2-method design)
    // ========================================================================

    /// Check ownership for read operations (fetch, consume).
    ///
    /// This is the simplified API for read-path ownership checks.
    /// It tolerates slight staleness for performance.
    ///
    /// # Behavior
    ///
    /// 1. Checks local cache first (zero latency)
    /// 2. Falls back to state machine read if cache miss
    ///
    /// This is appropriate for:
    /// - Fetch requests
    /// - Consume operations
    /// - Any operation that can tolerate reading from a stale replica briefly
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// if coordinator.owns_partition_for_read("orders", 0).await? {
    ///     // Safe to serve fetch request
    /// }
    /// ```
    async fn owns_partition_for_read(&self, topic: &str, partition: i32) -> SlateDBResult<bool>;

    /// Check and verify ownership for write operations (produce).
    ///
    /// This is the simplified API for write-path ownership checks.
    /// It guarantees linearizability and extends the lease atomically.
    ///
    /// # Behavior
    ///
    /// 1. Verifies ownership with linearizable read
    /// 2. Extends the lease to ensure it won't expire during write
    /// 3. Returns remaining TTL for monitoring
    ///
    /// This is appropriate for:
    /// - Produce requests
    /// - Any operation that modifies partition data
    ///
    /// # Returns
    ///
    /// - `Ok(remaining_ttl_secs)` if we own the partition
    /// - `Err(NotOwned)` if we don't own it
    ///
    /// # Example
    ///
    /// ```rust,ignore
    /// match coordinator.owns_partition_for_write("orders", 0, 60).await {
    ///     Ok(ttl) => {
    ///         // Safe to write, lease extended
    ///         tracing::debug!(remaining_ttl = ttl, "Ownership verified");
    ///     }
    ///     Err(e) => {
    ///         // Not owner, reject write
    ///     }
    /// }
    /// ```
    async fn owns_partition_for_write(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<u64> {
        self.verify_and_extend_lease(topic, partition, lease_secs)
            .await
    }

    /// Verify ownership and extend lease atomically.
    ///
    /// This addresses the race window between lease check and write by:
    /// 1. Verifying we still own the partition
    /// 2. Extending the lease to ensure it won't expire during the write
    /// 3. Returns the remaining TTL for monitoring near-misses
    ///
    /// Returns Ok(remaining_secs) if we own it, Err(NotOwned) if we don't.
    async fn verify_and_extend_lease(
        &self,
        topic: &str,
        partition: i32,
        lease_secs: u64,
    ) -> SlateDBResult<u64>;

    /// Invalidate the ownership cache for a specific partition.
    async fn invalidate_ownership_cache(&self, topic: &str, partition: i32);

    /// Invalidate the entire ownership cache.
    async fn invalidate_all_ownership_cache(&self);

    /// Check if this broker SHOULD own a partition according to consistent hashing.
    ///
    /// This is used to prevent "partition stealing" where a broker tries to
    /// acquire partitions that should belong to other brokers. Essential for
    /// proper horizontal scaling.
    ///
    /// # Returns
    /// - `true` if this broker is the designated owner for this partition
    /// - `false` if another broker should own it
    async fn should_own_partition(&self, topic: &str, partition: i32) -> SlateDBResult<bool>;

    // ========================================================================
    // Topic Metadata
    // ========================================================================

    /// Register a topic with its partition count.
    async fn register_topic(&self, topic: &str, partitions: i32) -> SlateDBResult<()>;

    /// Get all topics.
    async fn get_topics(&self) -> SlateDBResult<Vec<String>>;

    /// Check if a topic exists in the cluster.
    async fn topic_exists(&self, topic: &str) -> SlateDBResult<bool>;

    /// Get partition count for a topic.
    async fn get_partition_count(&self, topic: &str) -> SlateDBResult<Option<i32>>;

    /// Delete a topic.
    async fn delete_topic(&self, topic: &str) -> SlateDBResult<bool>;

    // ========================================================================
    // Partition Assignment
    // ========================================================================

    /// Get partitions that should be assigned to this broker.
    async fn get_assigned_partitions(&self) -> SlateDBResult<Vec<(String, i32)>>;

    /// Get all partitions for a topic with their current owners.
    async fn get_partition_owners(&self, topic: &str) -> SlateDBResult<Vec<(i32, Option<i32>)>>;
}

/// Result of a successful consumer-group join.
///
/// Unlike the tuple returned by [`ConsumerGroupCoordinator::join_group`],
/// this carries the per-member protocol metadata the leader needs to run
/// its assignor, plus the protocol negotiated across all members.
#[derive(Debug, Clone)]
pub struct GroupJoinResult {
    /// Generation after this join (every join triggers a rebalance).
    pub generation_id: i32,
    /// The member id assigned to (or confirmed for) the caller.
    pub member_id: String,
    /// Whether the caller is the group leader for this generation.
    pub is_leader: bool,
    /// The current group leader's member id.
    pub leader_id: String,
    /// The protocol negotiated across all members (e.g. "range").
    /// Empty when no member declared any protocols.
    pub protocol_name: String,
    /// `(member_id, metadata)` for every member, where `metadata` is the
    /// opaque subscription blob that member sent for the selected protocol.
    /// Populated only for the leader; followers receive an empty list
    /// (matching Kafka's JoinGroup response semantics).
    pub members: Vec<(String, Vec<u8>)>,
}

/// Outcome of a consumer-group join attempt.
#[derive(Debug, Clone)]
pub enum GroupJoinOutcome {
    /// The member joined; a rebalance is now in progress.
    Joined(GroupJoinResult),
    /// The member's protocol list has no protocol in common with the
    /// rest of the group (Kafka: INCONSISTENT_GROUP_PROTOCOL).
    InconsistentProtocol,
}

/// Coordinator for consumer group state.
///
/// This trait handles:
/// - Consumer group membership (join, leave, heartbeat)
/// - Partition assignment within groups
/// - Committed offsets
#[async_trait]
pub trait ConsumerGroupCoordinator: Send + Sync {
    // ========================================================================
    // Consumer Groups
    // ========================================================================

    /// Join a consumer group atomically.
    ///
    /// Returns (generation_id, member_id, is_leader, leader_id, members)
    async fn join_group(
        &self,
        group_id: &str,
        member_id: &str,
        protocol_metadata: &[u8],
        session_timeout_ms: i32,
    ) -> SlateDBResult<(i32, String, bool, String, Vec<String>)>;

    /// Join a consumer group with the member's full protocol list.
    ///
    /// This is the protocol-aware variant of [`join_group`](Self::join_group):
    /// it persists every `(protocol_name, metadata)` pair the member sent so
    /// the JoinGroup response to the group leader can carry each member's own
    /// subscription metadata, and it negotiates the group protocol across all
    /// members (the first protocol, by member preference order, that every
    /// member supports — ties broken by member vote like Kafka).
    ///
    /// The default implementation delegates to [`join_group`](Self::join_group)
    /// with the first protocol's metadata, performing no negotiation and
    /// returning empty per-member metadata. Real coordinators should override
    /// it.
    async fn join_group_with_protocols(
        &self,
        group_id: &str,
        member_id: &str,
        protocols: &[(String, Vec<u8>)],
        session_timeout_ms: i32,
        rebalance_timeout_ms: i32,
    ) -> SlateDBResult<GroupJoinOutcome> {
        let _ = rebalance_timeout_ms;
        let metadata = protocols
            .first()
            .map(|(_, m)| m.clone())
            .unwrap_or_default();
        let (generation_id, member_id, is_leader, leader_id, member_ids) = self
            .join_group(group_id, member_id, &metadata, session_timeout_ms)
            .await?;
        Ok(GroupJoinOutcome::Joined(GroupJoinResult {
            generation_id,
            member_id,
            is_leader,
            leader_id,
            protocol_name: protocols
                .first()
                .map(|(n, _)| n.clone())
                .unwrap_or_default(),
            members: if is_leader {
                member_ids.into_iter().map(|m| (m, Vec::new())).collect()
            } else {
                Vec::new()
            },
        }))
    }

    /// Get the protocol negotiated for a group at its last rebalance.
    ///
    /// Returns `Ok(None)` when the group doesn't exist or no protocol has
    /// been negotiated yet.
    async fn get_group_protocol(&self, group_id: &str) -> SlateDBResult<Option<String>> {
        let _ = group_id;
        Ok(None)
    }

    /// Complete a rebalance (called after sync_group from leader).
    ///
    /// Now verifies generation hasn't changed since the leader submitted
    /// assignments. Returns Ok(true) if completed, Ok(false) if generation changed.
    async fn complete_rebalance(
        &self,
        group_id: &str,
        expected_generation: i32,
    ) -> SlateDBResult<bool>;

    /// Get current generation for a group.
    async fn get_generation(&self, group_id: &str) -> SlateDBResult<i32>;

    /// Add or update a member in a consumer group.
    async fn upsert_group_member(
        &self,
        group_id: &str,
        member_id: &str,
        metadata: &[u8],
    ) -> SlateDBResult<()>;

    /// Get all members of a consumer group.
    async fn get_group_members(&self, group_id: &str) -> SlateDBResult<Vec<String>>;

    /// Get the leader of a consumer group.
    async fn get_group_leader(&self, group_id: &str) -> SlateDBResult<Option<String>>;

    /// Remove a member from a consumer group and trigger rebalance.
    async fn remove_group_member(&self, group_id: &str, member_id: &str) -> SlateDBResult<()>;

    /// Remove a member from a consumer group, reporting whether the member
    /// was actually part of the group.
    ///
    /// Returns `Ok(true)` when the member existed and was removed (a
    /// rebalance is triggered for the remaining members) and `Ok(false)`
    /// when the group or member was unknown, so LeaveGroup can answer with
    /// `UNKNOWN_MEMBER_ID` per Kafka semantics.
    ///
    /// The default implementation delegates to
    /// [`remove_group_member`](Self::remove_group_member) and assumes the
    /// member existed.
    async fn leave_group(&self, group_id: &str, member_id: &str) -> SlateDBResult<bool> {
        self.remove_group_member(group_id, member_id).await?;
        Ok(true)
    }

    /// Update member heartbeat with generation verification.
    ///
    /// Returns HeartbeatResult indicating success, unknown member, or illegal generation.
    async fn update_member_heartbeat_with_generation(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> SlateDBResult<super::error::HeartbeatResult>;

    /// Validate member and generation for offset commit.
    ///
    /// This prevents stale consumers (from before a rebalance) from committing offsets.
    /// A consumer must be a valid member of the group with the correct generation.
    ///
    /// Returns:
    /// - `Ok(HeartbeatResult::Success)` if member and generation are valid
    /// - `Ok(HeartbeatResult::UnknownMember)` if member doesn't exist
    /// - `Ok(HeartbeatResult::IllegalGeneration)` if generation is stale
    async fn validate_member_for_commit(
        &self,
        group_id: &str,
        member_id: &str,
        generation_id: i32,
    ) -> SlateDBResult<super::error::HeartbeatResult>;

    /// Set member assignments atomically with leader/generation verification.
    ///
    /// This atomically:
    /// 1. Verifies the caller is the current leader
    /// 2. Verifies generation hasn't changed
    /// 3. Stores all assignments in a single transaction
    ///
    /// Returns Ok(true) if stored, Ok(false) if rejected.
    async fn set_assignments_atomically(
        &self,
        group_id: &str,
        leader_id: &str,
        expected_generation: i32,
        assignments: &[(String, Vec<u8>)],
    ) -> SlateDBResult<bool>;

    /// Evict expired members from all consumer groups.
    async fn evict_expired_members(
        &self,
        default_session_timeout_ms: u64,
    ) -> SlateDBResult<Vec<(String, String)>>;

    /// Get all consumer group IDs.
    async fn get_all_groups(&self) -> SlateDBResult<Vec<String>>;

    /// Delete a consumer group entirely.
    async fn delete_consumer_group(&self, group_id: &str) -> SlateDBResult<()>;

    /// Store member assignment.
    async fn set_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
        assignment: &[u8],
    ) -> SlateDBResult<()>;

    /// Get member assignment.
    async fn get_member_assignment(
        &self,
        group_id: &str,
        member_id: &str,
    ) -> SlateDBResult<Vec<u8>>;

    // ========================================================================
    // Committed Offsets
    // ========================================================================

    /// Commit an offset for a consumer group.
    ///
    /// # Delivery Semantics
    ///
    /// This operation provides **at-least-once** delivery semantics:
    ///
    /// - If `commit_offset` returns `Ok(())`, the offset was durably committed
    /// - If it returns an error, the client should retry
    /// - A client crash after processing a message but before committing will
    ///   result in re-processing that message on restart
    ///
    /// This is standard Kafka behavior. For **exactly-once** semantics, use
    /// producer transactions with the `read_committed` isolation level.
    ///
    /// # Atomicity
    ///
    /// Offset commits are stored in Raft state, separate from message data.
    /// This means:
    ///
    /// - Offset commits may succeed even if message data is temporarily unavailable
    /// - After a broker crash, offsets are recovered from Raft, ensuring no
    ///   committed offsets are lost
    /// - However, there is no atomic commit of "message consumed + offset committed"
    ///
    /// # Best Practices
    ///
    /// 1. **Auto-commit disabled**: For at-least-once guarantees, disable auto-commit
    ///    and commit explicitly after processing
    /// 2. **Idempotent processing**: Design consumers to handle duplicate messages
    /// 3. **Commit frequency**: Balance between commit frequency (data freshness)
    ///    and throughput (fewer commits = better performance)
    async fn commit_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
        offset: i64,
        metadata: Option<&str>,
    ) -> SlateDBResult<()>;

    /// Fetch committed offset for a consumer group.
    async fn fetch_offset(
        &self,
        group_id: &str,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<(i64, Option<String>)>;
}

/// Coordinator for producer state.
///
/// This trait handles:
/// - Producer ID allocation
/// - Transactional producer epoch tracking
/// - Producer state persistence for idempotency
#[async_trait]
pub trait ProducerCoordinator: Send + Sync {
    /// Get next producer ID (atomic increment).
    async fn next_producer_id(&self) -> SlateDBResult<i64>;

    /// Initialize or bump a producer ID with epoch tracking.
    ///
    /// Returns (producer_id, producer_epoch)
    async fn init_producer_id(
        &self,
        transactional_id: Option<&str>,
        requested_producer_id: i64,
        requested_epoch: i16,
    ) -> SlateDBResult<(i64, i16)>;

    /// Store producer state for a topic/partition
    ///
    /// Persists producer idempotency state so it survives broker failover.
    async fn store_producer_state(
        &self,
        topic: &str,
        partition: i32,
        producer_id: i64,
        last_sequence: i32,
        producer_epoch: i16,
    ) -> SlateDBResult<()>;

    /// Load producer states for a topic/partition
    ///
    /// Loads all persisted producer states when acquiring a partition.
    async fn load_producer_states(
        &self,
        topic: &str,
        partition: i32,
    ) -> SlateDBResult<
        std::collections::HashMap<i64, super::coordinator::producer::PersistedProducerState>,
    >;
}

use std::collections::HashMap;

use super::raft::domains::{PartitionTransfer, TransferReason};

/// Coordinator for partition transfers during fast failover and auto-balancing.
///
/// This trait handles:
/// - Partition ownership transfers between brokers
/// - Broker failure marking
/// - Batch partition transfers for coordinated operations
#[async_trait]
pub trait PartitionTransferCoordinator: Send + Sync {
    /// Transfer partition ownership from one broker to another.
    ///
    /// This is an atomic operation that immediately changes ownership without
    /// waiting for lease expiration. Used for fast failover and load balancing.
    ///
    /// # Arguments
    /// * `topic` - The topic name
    /// * `partition` - The partition index
    /// * `from_broker_id` - The current owner broker ID
    /// * `to_broker_id` - The new owner broker ID
    /// * `reason` - The reason for the transfer
    /// * `lease_duration_ms` - New lease duration for the destination broker
    ///
    /// # Returns
    /// * `Ok(())` - Transfer succeeded
    /// * `Err(...)` - Transfer failed (source not owner, dest unavailable, etc.)
    async fn transfer_partition(
        &self,
        topic: &str,
        partition: i32,
        from_broker_id: i32,
        to_broker_id: i32,
        reason: TransferReason,
        lease_duration_ms: u64,
    ) -> SlateDBResult<()>;

    /// Transfer multiple partitions atomically as a batch.
    ///
    /// This is used for coordinated failover and rebalancing operations
    /// where multiple partitions need to move together.
    ///
    /// # Arguments
    /// * `transfers` - List of partition transfers to execute
    /// * `reason` - The reason for the transfers
    /// * `lease_duration_ms` - New lease duration for destination brokers
    ///
    /// # Returns
    /// * `Ok((successful_count, failed_list))` - Results of the batch operation
    async fn batch_transfer_partitions(
        &self,
        transfers: Vec<PartitionTransfer>,
        reason: TransferReason,
        lease_duration_ms: u64,
    ) -> SlateDBResult<(usize, Vec<(String, i32, String)>)>;

    /// Mark a broker as failed and track it in the cluster state.
    ///
    /// This should be called when fast failure detection determines a broker
    /// is no longer responsive. The implementation must atomically fence the
    /// broker and release every partition it owned (with a leader-epoch bump
    /// so in-flight writes from the failed broker are rejected).
    ///
    /// # Arguments
    /// * `broker_id` - The broker ID that failed
    /// * `reason` - Human-readable reason for the failure
    ///
    /// # Returns
    /// * `Ok(released_partitions)` - The `(topic, partition)` pairs released
    ///   from the failed broker; the caller reassigns exactly this set.
    async fn mark_broker_failed(
        &self,
        broker_id: i32,
        reason: &str,
    ) -> SlateDBResult<Vec<(String, i32)>>;

    /// Get all partition owners as a map from (topic, partition) to broker_id.
    ///
    /// This is used by the auto-balancer to understand current partition distribution.
    async fn get_all_partition_owners(&self) -> HashMap<(String, i32), i32>;

    /// Get all partition owners along with the Raft commit index.
    ///
    /// This provides a consistent snapshot of partition ownership at a specific
    /// Raft log index. The rebalance coordinator uses this to ensure decisions
    /// are made on consistent data.
    ///
    /// # Returns
    /// A tuple of (partition_owners_map, raft_commit_index)
    async fn get_all_partition_owners_with_index(&self) -> (HashMap<(String, i32), i32>, u64);

    /// Get list of active broker IDs.
    ///
    /// This is used by the rebalance coordinator to determine available targets
    /// for partition transfers.
    async fn get_active_broker_ids(&self) -> Vec<i32>;
}

/// Combined coordinator trait for convenience.
///
/// Most implementations will implement all three traits, so this provides
/// a single trait bound for code that needs the full coordinator interface.
pub trait ClusterCoordinator:
    PartitionCoordinator + ConsumerGroupCoordinator + ProducerCoordinator + PartitionTransferCoordinator
{
}

// Blanket implementation for any type that implements all four traits
impl<T> ClusterCoordinator for T where
    T: PartitionCoordinator
        + ConsumerGroupCoordinator
        + ProducerCoordinator
        + PartitionTransferCoordinator
{
}

#[cfg(test)]
#[path = "traits_tests.rs"]
mod tests;
