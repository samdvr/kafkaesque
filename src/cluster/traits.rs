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
    /// is no longer responsive.
    ///
    /// # Arguments
    /// * `broker_id` - The broker ID that failed
    /// * `reason` - Human-readable reason for the failure
    ///
    /// # Returns
    /// * `Ok(partitions_affected)` - Number of partitions owned by the failed broker
    async fn mark_broker_failed(&self, broker_id: i32, reason: &str) -> SlateDBResult<usize>;

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
mod tests {
    use super::*;
    use crate::cluster::mock_coordinator::MockCoordinator;

    // ========================================================================
    // Trait Compilation Tests
    // ========================================================================

    #[test]
    fn test_mock_coordinator_implements_partition_coordinator() {
        // This test verifies that MockCoordinator implements PartitionCoordinator
        fn assert_partition_coordinator<T: PartitionCoordinator>() {}
        assert_partition_coordinator::<MockCoordinator>();
    }

    #[test]
    fn test_mock_coordinator_implements_consumer_group_coordinator() {
        // This test verifies that MockCoordinator implements ConsumerGroupCoordinator
        fn assert_consumer_group_coordinator<T: ConsumerGroupCoordinator>() {}
        assert_consumer_group_coordinator::<MockCoordinator>();
    }

    #[test]
    fn test_mock_coordinator_implements_producer_coordinator() {
        // This test verifies that MockCoordinator implements ProducerCoordinator
        fn assert_producer_coordinator<T: ProducerCoordinator>() {}
        assert_producer_coordinator::<MockCoordinator>();
    }

    #[test]
    fn test_mock_coordinator_implements_partition_transfer_coordinator() {
        // This test verifies that MockCoordinator implements PartitionTransferCoordinator
        fn assert_partition_transfer_coordinator<T: PartitionTransferCoordinator>() {}
        assert_partition_transfer_coordinator::<MockCoordinator>();
    }

    #[test]
    fn test_mock_coordinator_implements_cluster_coordinator() {
        // This test verifies the blanket implementation of ClusterCoordinator
        fn assert_cluster_coordinator<T: ClusterCoordinator>() {}
        assert_cluster_coordinator::<MockCoordinator>();
    }

    // ========================================================================
    // MockCoordinator Basic Tests
    // ========================================================================

    #[test]
    fn test_mock_coordinator_new() {
        let mock = MockCoordinator::new(1, "localhost", 9092);
        assert_eq!(mock.broker_id, 1);
        assert_eq!(mock.host, "localhost");
        assert_eq!(mock.port, 9092);
    }

    #[test]
    fn test_mock_coordinator_broker_id() {
        let mock = MockCoordinator::new(42, "host", 9092);
        // Using trait method
        assert_eq!(PartitionCoordinator::broker_id(&mock), 42);
    }

    // ========================================================================
    // Default Implementation Tests
    // ========================================================================

    #[tokio::test]
    async fn test_acquire_partition_with_epoch_default() {
        // Test the default implementation of acquire_partition_with_epoch
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Register topic first
        mock.register_topic("test-topic", 3).await.unwrap();

        // Acquire should return Some(epoch) on success
        let result = mock.acquire_partition_with_epoch("test-topic", 0, 60).await;
        assert!(result.is_ok());
        // Either we get an epoch or None depending on implementation
        let epoch_opt = result.unwrap();
        // MockCoordinator should return Some(epoch) on successful acquire
        assert!(epoch_opt.is_some());
    }

    #[tokio::test]
    async fn test_acquire_partition_with_epoch_already_owned() {
        // Test when partition is owned by another broker
        let mock1 = MockCoordinator::new(1, "localhost", 9092);
        let mut mock2 = mock1.clone();
        mock2.broker_id = 2;

        // Register topic
        mock1.register_topic("test-topic", 3).await.unwrap();

        // First broker acquires
        mock1.acquire_partition("test-topic", 0, 60).await.unwrap();

        // Second broker tries to acquire - should fail (return None)
        let result = mock2
            .acquire_partition_with_epoch("test-topic", 0, 60)
            .await;
        assert!(result.is_ok());
        assert!(result.unwrap().is_none());
    }

    #[tokio::test]
    async fn test_owns_partition_for_write_success() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Register topic and acquire partition
        mock.register_topic("test-topic", 3).await.unwrap();
        mock.acquire_partition("test-topic", 0, 60).await.unwrap();

        // owns_partition_for_write should succeed and return remaining TTL
        let result = mock.owns_partition_for_write("test-topic", 0, 60).await;
        assert!(result.is_ok());
        // TTL should be positive
        let ttl = result.unwrap();
        assert!(ttl > 0);
    }

    #[tokio::test]
    async fn test_owns_partition_for_write_not_owner() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Register topic but don't acquire
        mock.register_topic("test-topic", 3).await.unwrap();

        // owns_partition_for_write should fail since we don't own it
        let result = mock.owns_partition_for_write("test-topic", 0, 60).await;
        assert!(result.is_err());
    }

    // ========================================================================
    // Trait Object Tests
    // ========================================================================

    #[test]
    fn test_partition_coordinator_trait_object() {
        // Verify PartitionCoordinator can be used as a trait object
        let mock = MockCoordinator::new(1, "localhost", 9092);
        let _trait_obj: &dyn PartitionCoordinator = &mock;
    }

    #[test]
    fn test_consumer_group_coordinator_trait_object() {
        // Verify ConsumerGroupCoordinator can be used as a trait object
        let mock = MockCoordinator::new(1, "localhost", 9092);
        let _trait_obj: &dyn ConsumerGroupCoordinator = &mock;
    }

    #[test]
    fn test_producer_coordinator_trait_object() {
        // Verify ProducerCoordinator can be used as a trait object
        let mock = MockCoordinator::new(1, "localhost", 9092);
        let _trait_obj: &dyn ProducerCoordinator = &mock;
    }

    #[test]
    fn test_partition_transfer_coordinator_trait_object() {
        // Verify PartitionTransferCoordinator can be used as a trait object
        let mock = MockCoordinator::new(1, "localhost", 9092);
        let _trait_obj: &dyn PartitionTransferCoordinator = &mock;
    }

    // ========================================================================
    // PartitionCoordinator Method Tests
    // ========================================================================

    #[tokio::test]
    async fn test_register_and_get_topics() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Register multiple topics
        mock.register_topic("topic-a", 3).await.unwrap();
        mock.register_topic("topic-b", 5).await.unwrap();
        mock.register_topic("topic-c", 1).await.unwrap();

        // Get all topics
        let topics = mock.get_topics().await.unwrap();
        assert!(topics.contains(&"topic-a".to_string()));
        assert!(topics.contains(&"topic-b".to_string()));
        assert!(topics.contains(&"topic-c".to_string()));
    }

    #[tokio::test]
    async fn test_topic_exists() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Register a topic
        mock.register_topic("existing-topic", 3).await.unwrap();

        // Check existence
        assert!(mock.topic_exists("existing-topic").await.unwrap());
        assert!(!mock.topic_exists("nonexistent-topic").await.unwrap());
    }

    #[tokio::test]
    async fn test_get_partition_count() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Register topic with 10 partitions
        mock.register_topic("my-topic", 10).await.unwrap();

        // Get partition count
        let count = mock.get_partition_count("my-topic").await.unwrap();
        assert_eq!(count, Some(10));

        // Nonexistent topic should return None
        let count = mock.get_partition_count("no-topic").await.unwrap();
        assert!(count.is_none());
    }

    #[tokio::test]
    async fn test_delete_topic() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Register and delete
        mock.register_topic("delete-me", 1).await.unwrap();
        assert!(mock.topic_exists("delete-me").await.unwrap());

        let deleted = mock.delete_topic("delete-me").await.unwrap();
        assert!(deleted);
        assert!(!mock.topic_exists("delete-me").await.unwrap());

        // Deleting again should return false
        let deleted_again = mock.delete_topic("delete-me").await.unwrap();
        assert!(!deleted_again);
    }

    #[tokio::test]
    async fn test_partition_ownership_lifecycle() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        mock.register_topic("test", 1).await.unwrap();

        // Acquire
        let acquired = mock.acquire_partition("test", 0, 60).await.unwrap();
        assert!(acquired);

        // Get owner
        let owner = mock.get_partition_owner("test", 0).await.unwrap();
        assert_eq!(owner, Some(1));

        // Renew
        let renewed = mock.renew_partition_lease("test", 0, 120).await.unwrap();
        assert!(renewed);

        // Release
        mock.release_partition("test", 0).await.unwrap();

        // Owner should be None after release
        let owner = mock.get_partition_owner("test", 0).await.unwrap();
        assert!(owner.is_none());
    }

    // ========================================================================
    // ProducerCoordinator Method Tests
    // ========================================================================

    #[tokio::test]
    async fn test_next_producer_id() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Get first producer ID
        let id1 = mock.next_producer_id().await.unwrap();
        // Get second producer ID
        let id2 = mock.next_producer_id().await.unwrap();

        // IDs should be different and increasing
        assert!(id2 > id1);
    }

    #[tokio::test]
    async fn test_init_producer_id_new() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Initialize new producer (no transactional_id)
        let (pid, epoch) = mock.init_producer_id(None, -1, -1).await.unwrap();

        // Should get a valid producer ID and epoch 0
        assert!(pid >= 0);
        assert_eq!(epoch, 0);
    }

    #[tokio::test]
    async fn test_init_producer_id_with_txn() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Initialize with transactional_id
        let (pid1, epoch1) = mock.init_producer_id(Some("my-txn"), -1, -1).await.unwrap();
        assert!(pid1 >= 0);
        assert_eq!(epoch1, 0);

        // MockCoordinator doesn't track transactional IDs, so each call returns a new ID
        // This is simplified behavior - real Kafka would return same ID with bumped epoch
        let (pid2, epoch2) = mock
            .init_producer_id(Some("my-txn"), pid1, epoch1)
            .await
            .unwrap();
        assert!(pid2 >= 0);
        assert_eq!(epoch2, 0); // Mock always returns epoch 0
    }

    // ========================================================================
    // ConsumerGroupCoordinator Method Tests
    // ========================================================================

    #[tokio::test]
    async fn test_consumer_group_join_and_leave() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Join group
        let (generation, member_id, is_leader, leader_id, members) = mock
            .join_group("test-group", "", &[1, 2, 3], 30000)
            .await
            .unwrap();

        // First member should be leader
        assert!(generation > 0);
        assert!(!member_id.is_empty());
        assert!(is_leader);
        assert_eq!(leader_id, member_id);
        assert_eq!(members.len(), 1);

        // Leave group
        mock.remove_group_member("test-group", &member_id)
            .await
            .unwrap();

        // Members should be empty
        let members = mock.get_group_members("test-group").await.unwrap();
        assert!(members.is_empty());
    }

    #[tokio::test]
    async fn test_get_all_groups() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Join multiple groups
        mock.join_group("group-a", "", &[], 30000).await.unwrap();
        mock.join_group("group-b", "", &[], 30000).await.unwrap();

        // Get all groups
        let groups = mock.get_all_groups().await.unwrap();
        assert!(groups.contains(&"group-a".to_string()));
        assert!(groups.contains(&"group-b".to_string()));
    }

    // ========================================================================
    // PartitionTransferCoordinator Method Tests
    // ========================================================================

    #[tokio::test]
    async fn test_get_active_broker_ids() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Register this broker
        mock.register_broker().await.unwrap();

        // Get active brokers
        let active = mock.get_active_broker_ids().await;
        assert!(active.contains(&1));
    }

    #[tokio::test]
    async fn test_get_all_partition_owners() {
        let mock = MockCoordinator::new(1, "localhost", 9092);

        // Register topics and acquire partitions
        mock.register_topic("topic-a", 2).await.unwrap();
        mock.acquire_partition("topic-a", 0, 60).await.unwrap();
        mock.acquire_partition("topic-a", 1, 60).await.unwrap();

        // Get all owners
        let owners = mock.get_all_partition_owners().await;
        assert_eq!(owners.get(&("topic-a".to_string(), 0)), Some(&1));
        assert_eq!(owners.get(&("topic-a".to_string(), 1)), Some(&1));
    }
}
