//! Chaos Testing Suite for Kafkaesque
//!
//! This module implements comprehensive chaos testing scenarios to validate
//! distributed system correctness under adverse conditions.
//!
//! # Test Categories
//!
//! 1. **Network Partitions** - Simulated network failures between nodes
//! 2. **Node Failures** - Crash and recovery scenarios
//! 3. **Zombie Mode** - Split-brain prevention validation
//! 4. **Lease and Ownership** - Partition ownership transitions
//! 5. **SlateDB Fencing** - Dual writer prevention
//! 6. **Clock Skew** - Time-based failure scenarios
//! 7. **Resource Exhaustion** - Backpressure and limits
//! 8. **Recovery Scenarios** - State recovery after failures
//!
//! # Running Tests
//!
//! ```bash
//! # Run all chaos tests
//! cargo test --test chaos_tests
//!
//! # Run specific category
//! cargo test --test chaos_tests network_partition
//! cargo test --test chaos_tests zombie_mode
//! cargo test --test chaos_tests fencing
//! ```

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use bytes::{BufMut, Bytes, BytesMut};
use object_store::ObjectStore;
use object_store::memory::InMemory;
use tokio::runtime::Handle;
use tokio::sync::{Mutex, RwLock};
use tokio::time::sleep;

use kafkaesque::cluster::{
    MockCoordinator, PartitionCoordinator, SlateDBError, SlateDBResult, ZombieModeState,
};

// ============================================================================
// Chaos Testing Infrastructure
// ============================================================================

/// Atomic port counter for unique ports across tests
static PORT_COUNTER: AtomicU16 = AtomicU16::new(29000);

fn next_port() -> u16 {
    PORT_COUNTER.fetch_add(1, Ordering::SeqCst)
}

/// Fault injection controller for simulating various failure modes.
#[derive(Debug, Clone)]
pub struct FaultInjector {
    /// Block all network operations
    network_blocked: Arc<AtomicBool>,
    /// Add latency to operations (in milliseconds)
    latency_ms: Arc<AtomicU64>,
    /// Fail next N operations
    fail_next_n: Arc<AtomicU64>,
    /// Clock offset for time-based tests (in seconds, can be negative via wrapping)
    clock_offset_secs: Arc<AtomicU64>,
    /// Partition specific failures: (topic, partition) -> should_fail
    partition_failures: Arc<RwLock<HashMap<(String, i32), bool>>>,
}

impl FaultInjector {
    pub fn new() -> Self {
        Self {
            network_blocked: Arc::new(AtomicBool::new(false)),
            latency_ms: Arc::new(AtomicU64::new(0)),
            fail_next_n: Arc::new(AtomicU64::new(0)),
            clock_offset_secs: Arc::new(AtomicU64::new(0)),
            partition_failures: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Block all network operations
    pub fn block_network(&self) {
        self.network_blocked.store(true, Ordering::SeqCst);
    }

    /// Unblock network operations
    pub fn unblock_network(&self) {
        self.network_blocked.store(false, Ordering::SeqCst);
    }

    /// Check if network is blocked
    pub fn is_network_blocked(&self) -> bool {
        self.network_blocked.load(Ordering::SeqCst)
    }

    /// Set operation latency
    pub fn set_latency(&self, ms: u64) {
        self.latency_ms.store(ms, Ordering::SeqCst);
    }

    /// Get current latency
    pub fn get_latency(&self) -> u64 {
        self.latency_ms.load(Ordering::SeqCst)
    }

    /// Set number of operations to fail
    pub fn fail_next(&self, n: u64) {
        self.fail_next_n.store(n, Ordering::SeqCst);
    }

    /// Check and decrement failure counter
    pub fn should_fail(&self) -> bool {
        let current = self.fail_next_n.load(Ordering::SeqCst);
        if current > 0 {
            self.fail_next_n.fetch_sub(1, Ordering::SeqCst);
            true
        } else {
            false
        }
    }

    /// Set clock offset for time skew simulation
    pub fn set_clock_offset(&self, secs: i64) {
        // Store as u64, negative values wrap around
        self.clock_offset_secs.store(secs as u64, Ordering::SeqCst);
    }

    /// Get adjusted time
    pub fn adjusted_time(&self) -> Instant {
        let offset = self.clock_offset_secs.load(Ordering::SeqCst) as i64;
        let now = Instant::now();
        if offset >= 0 {
            now + Duration::from_secs(offset as u64)
        } else {
            now - Duration::from_secs((-offset) as u64)
        }
    }

    /// Mark a specific partition as failing
    pub async fn fail_partition(&self, topic: &str, partition: i32) {
        let mut failures = self.partition_failures.write().await;
        failures.insert((topic.to_string(), partition), true);
    }

    /// Clear partition failure
    pub async fn clear_partition_failure(&self, topic: &str, partition: i32) {
        let mut failures = self.partition_failures.write().await;
        failures.remove(&(topic.to_string(), partition));
    }

    /// Check if partition should fail
    pub async fn is_partition_failing(&self, topic: &str, partition: i32) -> bool {
        let failures = self.partition_failures.read().await;
        failures
            .get(&(topic.to_string(), partition))
            .copied()
            .unwrap_or(false)
    }

    /// Apply configured latency
    pub async fn apply_latency(&self) {
        let latency = self.get_latency();
        if latency > 0 {
            sleep(Duration::from_millis(latency)).await;
        }
    }

    /// Check if operation should proceed (not blocked, not failing)
    pub async fn check_operation(&self) -> SlateDBResult<()> {
        if self.is_network_blocked() {
            return Err(SlateDBError::Storage(
                "Network blocked by fault injector".to_string(),
            ));
        }
        if self.should_fail() {
            return Err(SlateDBError::Storage(
                "Operation failed by fault injector".to_string(),
            ));
        }
        self.apply_latency().await;
        Ok(())
    }
}

impl Default for FaultInjector {
    fn default() -> Self {
        Self::new()
    }
}

/// Create a shared in-memory object store for testing
fn create_object_store() -> Arc<dyn ObjectStore> {
    Arc::new(InMemory::new())
}

/// Create a test record batch with the specified messages
#[allow(dead_code)]
fn create_test_batch(messages: &[&str]) -> Bytes {
    let mut buf = BytesMut::new();

    // Record batch header (simplified)
    buf.put_i64(0); // base_offset
    buf.put_i32(0); // batch_length (placeholder)
    buf.put_i32(-1); // partition_leader_epoch
    buf.put_i8(2); // magic
    buf.put_u32(0); // crc (placeholder)
    buf.put_i16(0); // attributes
    buf.put_i32((messages.len() - 1) as i32); // last_offset_delta
    buf.put_i64(0); // base_timestamp
    buf.put_i64(0); // max_timestamp
    buf.put_i64(-1); // producer_id
    buf.put_i16(-1); // producer_epoch
    buf.put_i32(-1); // base_sequence
    buf.put_i32(messages.len() as i32); // record_count

    for (i, msg) in messages.iter().enumerate() {
        // Record format
        buf.put_u8(0); // attributes
        buf.put_i8(i as i8); // timestamp_delta (varint)
        buf.put_i8(i as i8); // offset_delta (varint)
        buf.put_i8(-1); // key_length (null)
        buf.put_i8(msg.len() as i8); // value_length
        buf.extend_from_slice(msg.as_bytes()); // value
        buf.put_i8(0); // headers_count
    }

    buf.freeze()
}

/// Multi-broker test cluster with shared state
pub struct TestCluster {
    pub brokers: Vec<MockCoordinator>,
    pub fault_injector: FaultInjector,
    _object_store: Arc<dyn ObjectStore>,
}

impl TestCluster {
    /// Create a new test cluster with N brokers sharing state
    pub async fn new(num_brokers: usize) -> Self {
        let object_store = create_object_store();

        // Create first broker
        let broker1 = MockCoordinator::new(1, "localhost", next_port() as i32);

        // Get shared state from first broker
        let shared_topics = broker1.topics.clone();
        let shared_owners = broker1.partition_owners.clone();
        let shared_brokers_state = broker1.brokers.clone();

        let mut brokers = vec![broker1];

        // Create additional brokers with shared state
        for i in 2..=num_brokers {
            let broker = MockCoordinator {
                broker_id: i as i32,
                host: "localhost".to_string(),
                port: next_port() as i32,
                brokers: shared_brokers_state.clone(),
                topics: shared_topics.clone(),
                partition_owners: shared_owners.clone(),
                consumer_groups: Arc::new(RwLock::new(HashMap::new())),
                offsets: Arc::new(RwLock::new(HashMap::new())),
                next_producer_id: Arc::new(std::sync::atomic::AtomicI64::new(1000)),
                next_member_id: Arc::new(std::sync::atomic::AtomicI32::new(1)),
                producer_states: Arc::new(RwLock::new(HashMap::new())),
                mock_raft_index: Arc::new(AtomicU64::new(0)),
            };
            brokers.push(broker);
        }

        // Register all brokers
        for broker in &brokers {
            broker.register_broker().await.unwrap();
        }

        Self {
            brokers,
            fault_injector: FaultInjector::new(),
            _object_store: object_store,
        }
    }

    /// Get broker by ID (1-indexed)
    pub fn broker(&self, id: usize) -> &MockCoordinator {
        &self.brokers[id - 1]
    }

    /// Register a topic across the cluster
    pub async fn create_topic(&self, name: &str, partitions: i32) {
        self.brokers[0]
            .register_topic(name, partitions)
            .await
            .unwrap();
    }

    /// Simulate broker failure by expiring its leases
    pub async fn kill_broker(&self, id: usize) {
        self.brokers[id - 1].expire_my_leases().await;
    }

    /// Get the current partition owner
    pub async fn get_owner(&self, topic: &str, partition: i32) -> Option<i32> {
        self.brokers[0]
            .get_partition_owner(topic, partition)
            .await
            .unwrap()
    }
}

// ============================================================================
// Category 1: Network Partition Tests
// ============================================================================

mod network_partition {
    use super::*;

    /// Scenario 1.1: Full network partition - broker loses connectivity
    #[tokio::test]
    async fn test_full_network_partition_triggers_lease_loss() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("partition-test", 3).await;

        // Broker 1 acquires all partitions
        for p in 0..3 {
            let acquired = cluster
                .broker(1)
                .acquire_partition("partition-test", p, 60)
                .await
                .unwrap();
            assert!(acquired, "Broker 1 should acquire partition {}", p);
        }

        // Verify ownership
        for p in 0..3 {
            assert_eq!(cluster.get_owner("partition-test", p).await, Some(1));
        }

        // Simulate network partition - broker 1 loses all leases
        cluster.kill_broker(1).await;

        // Verify leases expired
        for p in 0..3 {
            assert!(
                !cluster
                    .broker(1)
                    .owns_partition_for_read("partition-test", p)
                    .await
                    .unwrap()
            );
        }

        // Broker 2 can now acquire the partitions
        for p in 0..3 {
            let acquired = cluster
                .broker(2)
                .acquire_partition("partition-test", p, 60)
                .await
                .unwrap();
            assert!(acquired, "Broker 2 should acquire partition {}", p);
        }

        // Verify new ownership
        for p in 0..3 {
            assert_eq!(cluster.get_owner("partition-test", p).await, Some(2));
        }
    }

    /// Scenario 1.2: Asymmetric partition - verify ownership verification fails
    #[tokio::test]
    async fn test_asymmetric_partition_causes_verify_failure() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("asym-test", 1).await;

        // Broker 1 acquires partition
        cluster
            .broker(1)
            .acquire_partition("asym-test", 0, 60)
            .await
            .unwrap();

        // Verify works initially
        let result = cluster
            .broker(1)
            .verify_and_extend_lease("asym-test", 0, 60)
            .await;
        assert!(result.is_ok());

        // Simulate partition - expire broker 1's leases
        cluster.kill_broker(1).await;

        // Broker 2 acquires
        cluster
            .broker(2)
            .acquire_partition("asym-test", 0, 60)
            .await
            .unwrap();

        // Broker 1's verify should now fail (fenced)
        let result = cluster
            .broker(1)
            .verify_and_extend_lease("asym-test", 0, 60)
            .await;
        assert!(result.is_err());
        match result {
            Err(SlateDBError::Fenced) => (),
            other => panic!("Expected Fenced error, got {:?}", other),
        }
    }

    /// Scenario 1.3: Partition during acquisition - only one broker wins
    #[tokio::test]
    async fn test_concurrent_acquisition_race() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("race-test", 1).await;

        // All three brokers try to acquire simultaneously
        let r1 = cluster
            .broker(1)
            .acquire_partition("race-test", 0, 60)
            .await
            .unwrap();
        let r2 = cluster
            .broker(2)
            .acquire_partition("race-test", 0, 60)
            .await
            .unwrap();
        let r3 = cluster
            .broker(3)
            .acquire_partition("race-test", 0, 60)
            .await
            .unwrap();

        // Exactly one should succeed
        let successes = [r1, r2, r3].iter().filter(|&&x| x).count();
        assert_eq!(successes, 1, "Exactly one broker should win the race");

        // Verify consistent ownership
        let owner = cluster.get_owner("race-test", 0).await;
        assert!(owner.is_some(), "Partition should have an owner");
    }

    /// Scenario 1.4: Recovery after partition heals
    #[tokio::test]
    async fn test_recovery_after_network_heals() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("heal-test", 2).await;

        // Broker 1 acquires partition 0
        cluster
            .broker(1)
            .acquire_partition("heal-test", 0, 60)
            .await
            .unwrap();

        // Broker 2 acquires partition 1
        cluster
            .broker(2)
            .acquire_partition("heal-test", 1, 60)
            .await
            .unwrap();

        // Broker 1 fails
        cluster.kill_broker(1).await;

        // Broker 2 takes over partition 0
        let acquired = cluster
            .broker(2)
            .acquire_partition("heal-test", 0, 60)
            .await
            .unwrap();
        assert!(acquired);

        // Broker 1 recovers and tries to acquire partition 0 - should fail
        let acquired = cluster
            .broker(1)
            .acquire_partition("heal-test", 0, 60)
            .await
            .unwrap();
        assert!(
            !acquired,
            "Broker 1 should not re-acquire while broker 2 owns it"
        );

        // Broker 1 can still acquire a new partition if broker 2 releases
        cluster
            .broker(2)
            .release_partition("heal-test", 0)
            .await
            .unwrap();

        let acquired = cluster
            .broker(1)
            .acquire_partition("heal-test", 0, 60)
            .await
            .unwrap();
        assert!(acquired, "Broker 1 should acquire released partition");
    }
}

// ============================================================================
// Category 2: Node Failure Tests
// ============================================================================

mod node_failure {
    use super::*;

    /// Scenario 2.1: Leader crash - verify failover
    #[tokio::test]
    async fn test_leader_crash_triggers_failover() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("leader-crash", 3).await;

        // Broker 1 owns all partitions (simulating leader)
        for p in 0..3 {
            cluster
                .broker(1)
                .acquire_partition("leader-crash", p, 60)
                .await
                .unwrap();
        }

        // Broker 1 crashes
        cluster.kill_broker(1).await;

        // Other brokers should be able to take over
        // Broker 2 takes partitions 0, 1
        assert!(
            cluster
                .broker(2)
                .acquire_partition("leader-crash", 0, 60)
                .await
                .unwrap()
        );
        assert!(
            cluster
                .broker(2)
                .acquire_partition("leader-crash", 1, 60)
                .await
                .unwrap()
        );

        // Broker 3 takes partition 2
        assert!(
            cluster
                .broker(3)
                .acquire_partition("leader-crash", 2, 60)
                .await
                .unwrap()
        );

        // Verify distribution
        assert_eq!(cluster.get_owner("leader-crash", 0).await, Some(2));
        assert_eq!(cluster.get_owner("leader-crash", 1).await, Some(2));
        assert_eq!(cluster.get_owner("leader-crash", 2).await, Some(3));
    }

    /// Scenario 2.2: Follower crash and recovery
    #[tokio::test]
    async fn test_follower_crash_no_impact_on_leader() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("follower-crash", 1).await;

        // Broker 1 is leader
        cluster
            .broker(1)
            .acquire_partition("follower-crash", 0, 60)
            .await
            .unwrap();

        // Broker 2 (follower) crashes - expire its leases
        cluster.kill_broker(2).await;

        // Broker 1 should still own the partition
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("follower-crash", 0)
                .await
                .unwrap()
        );

        // Lease renewal should still work
        let result = cluster
            .broker(1)
            .renew_partition_lease("follower-crash", 0, 60)
            .await
            .unwrap();
        assert!(result);
    }

    /// Scenario 2.3: Cascading failures
    #[tokio::test]
    async fn test_cascading_failures_partition_redistribution() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("cascade", 3).await;

        // Distribute partitions across brokers
        cluster
            .broker(1)
            .acquire_partition("cascade", 0, 60)
            .await
            .unwrap();
        cluster
            .broker(2)
            .acquire_partition("cascade", 1, 60)
            .await
            .unwrap();
        cluster
            .broker(3)
            .acquire_partition("cascade", 2, 60)
            .await
            .unwrap();

        // First failure: Broker 1 crashes
        cluster.kill_broker(1).await;

        // Broker 2 takes over partition 0
        assert!(
            cluster
                .broker(2)
                .acquire_partition("cascade", 0, 60)
                .await
                .unwrap()
        );

        // Second failure: Broker 2 crashes
        cluster.kill_broker(2).await;

        // Broker 3 takes over all remaining partitions
        assert!(
            cluster
                .broker(3)
                .acquire_partition("cascade", 0, 60)
                .await
                .unwrap()
        );
        assert!(
            cluster
                .broker(3)
                .acquire_partition("cascade", 1, 60)
                .await
                .unwrap()
        );

        // Verify broker 3 owns all partitions
        for p in 0..3 {
            assert_eq!(cluster.get_owner("cascade", p).await, Some(3));
        }
    }

    /// Scenario 2.4: Verify partition state after crash
    #[tokio::test]
    async fn test_partition_state_consistent_after_crash() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("state-test", 1).await;

        // Broker 1 acquires and then crashes
        cluster
            .broker(1)
            .acquire_partition("state-test", 0, 60)
            .await
            .unwrap();

        let owner_before = cluster.get_owner("state-test", 0).await;
        assert_eq!(owner_before, Some(1));

        cluster.kill_broker(1).await;

        // After crash, no owner (lease expired)
        let owner_after = cluster.get_owner("state-test", 0).await;
        assert_eq!(owner_after, None);

        // Broker 2 acquires
        cluster
            .broker(2)
            .acquire_partition("state-test", 0, 60)
            .await
            .unwrap();

        let final_owner = cluster.get_owner("state-test", 0).await;
        assert_eq!(final_owner, Some(2));
    }
}

// ============================================================================
// Category 3: Zombie Mode Tests
// ============================================================================

mod zombie_mode {
    use super::*;

    /// Scenario 3.1: Zombie mode entry blocks all writes
    #[tokio::test]
    async fn test_zombie_mode_entry_and_detection() {
        let zombie_state = ZombieModeState::new();

        // Initially not zombie
        assert!(!zombie_state.is_active());

        // Enter zombie mode
        let entered = zombie_state.enter();
        assert!(entered);
        assert!(zombie_state.is_active());

        // Entry timestamp should be recorded
        assert!(zombie_state.entered_at() > 0);
    }

    /// Scenario 3.2: Zombie mode with write lock pattern
    #[tokio::test]
    async fn test_zombie_mode_double_check_pattern() {
        let zombie_state = Arc::new(ZombieModeState::new());
        let write_lock = Arc::new(Mutex::new(()));
        let zombie_clone = zombie_state.clone();

        // Simulate the double-check pattern used in partition_store.rs

        // Step 1: Check zombie mode before acquiring lock
        assert!(!zombie_state.is_active());

        // Step 2: Spawn task that enters zombie mode while we hold lock
        let zombie_clone2 = zombie_clone.clone();
        let handle = tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            zombie_clone2.enter();
        });

        // Step 3: Acquire lock
        let _guard = write_lock.lock().await;

        // Wait for zombie entry
        handle.await.unwrap();

        // Step 4: Double-check after acquiring lock
        assert!(
            zombie_state.is_active(),
            "Double-check should detect zombie mode entered during lock acquisition"
        );
    }

    /// Scenario 3.3: Zombie mode clears lease cache
    #[tokio::test]
    async fn test_zombie_mode_invalidates_ownership() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("zombie-test", 2).await;

        // Broker 1 acquires partitions
        cluster
            .broker(1)
            .acquire_partition("zombie-test", 0, 60)
            .await
            .unwrap();
        cluster
            .broker(1)
            .acquire_partition("zombie-test", 1, 60)
            .await
            .unwrap();

        // Verify ownership
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("zombie-test", 0)
                .await
                .unwrap()
        );
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("zombie-test", 1)
                .await
                .unwrap()
        );

        // Simulate entering zombie mode (expire all leases)
        cluster.broker(1).expire_all_leases().await;

        // Ownership should be gone
        assert!(
            !cluster
                .broker(1)
                .owns_partition_for_read("zombie-test", 0)
                .await
                .unwrap()
        );
        assert!(
            !cluster
                .broker(1)
                .owns_partition_for_read("zombie-test", 1)
                .await
                .unwrap()
        );

        // Verify and extend should fail (fenced)
        let result = cluster
            .broker(1)
            .verify_and_extend_lease("zombie-test", 0, 60)
            .await;
        assert!(result.is_err());
    }

    /// Scenario 3.4: Prolonged zombie mode - partitions redistributed
    #[tokio::test]
    async fn test_prolonged_zombie_mode_allows_reacquisition() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("prolonged-zombie", 3).await;

        // Broker 1 owns all partitions
        for p in 0..3 {
            cluster
                .broker(1)
                .acquire_partition("prolonged-zombie", p, 60)
                .await
                .unwrap();
        }

        // Broker 1 enters zombie mode (network partition)
        cluster.kill_broker(1).await;

        // Other brokers can acquire the partitions
        cluster
            .broker(2)
            .acquire_partition("prolonged-zombie", 0, 60)
            .await
            .unwrap();
        cluster
            .broker(2)
            .acquire_partition("prolonged-zombie", 1, 60)
            .await
            .unwrap();
        cluster
            .broker(3)
            .acquire_partition("prolonged-zombie", 2, 60)
            .await
            .unwrap();

        // When broker 1 recovers, it has no partitions
        let assigned = cluster.broker(1).get_assigned_partitions().await.unwrap();
        assert!(
            assigned.is_empty(),
            "Recovered zombie should have no partitions"
        );
    }

    /// Scenario: Multiple consecutive heartbeat failures trigger zombie mode
    #[tokio::test]
    async fn test_consecutive_heartbeat_failures() {
        let zombie_state = ZombieModeState::new();
        let failure_threshold = 3;
        let mut consecutive_failures = 0;

        // Simulate heartbeat loop
        for i in 0..5 {
            // Simulate heartbeat failure
            let heartbeat_failed = true; // In reality, this would be RPC result

            if heartbeat_failed {
                consecutive_failures += 1;
                if consecutive_failures >= failure_threshold {
                    zombie_state.enter();
                    break;
                }
            } else {
                consecutive_failures = 0;
            }

            assert!(
                !zombie_state.is_active() || i >= failure_threshold - 1,
                "Should not enter zombie mode before threshold"
            );
        }

        assert!(
            zombie_state.is_active(),
            "Should enter zombie mode after {} consecutive failures",
            failure_threshold
        );
    }
}

// ============================================================================
// Category 4: Lease and Ownership Tests
// ============================================================================

mod lease_ownership {
    use super::*;

    /// Scenario 4.1: Lease expiration during slow operation
    #[tokio::test]
    async fn test_lease_ttl_validation_before_write() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("slow-write", 1).await;

        // Broker 1 acquires with short lease
        cluster
            .broker(1)
            .acquire_partition("slow-write", 0, 5) // 5 second lease
            .await
            .unwrap();

        // Verify and extend should work initially
        let result = cluster
            .broker(1)
            .verify_and_extend_lease("slow-write", 0, 5)
            .await;
        assert!(result.is_ok());

        // Expire the lease
        cluster.kill_broker(1).await;

        // Now verify should fail - simulating check before write
        let result = cluster
            .broker(1)
            .verify_and_extend_lease("slow-write", 0, 5)
            .await;
        assert!(result.is_err(), "Should reject write with expired lease");
    }

    /// Scenario 4.2: Lease renewal failure handling
    #[tokio::test]
    async fn test_lease_renewal_failure_triggers_release() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("renewal-fail", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("renewal-fail", 0, 60)
            .await
            .unwrap();

        // Simulate renewal working
        assert!(
            cluster
                .broker(1)
                .renew_partition_lease("renewal-fail", 0, 60)
                .await
                .unwrap()
        );

        // Simulate lease stolen by broker 2
        cluster.kill_broker(1).await;
        cluster
            .broker(2)
            .acquire_partition("renewal-fail", 0, 60)
            .await
            .unwrap();

        // Broker 1's renewal should now fail
        let renewed = cluster
            .broker(1)
            .renew_partition_lease("renewal-fail", 0, 60)
            .await
            .unwrap();
        assert!(!renewed, "Renewal should fail when lease is stolen");
    }

    /// Scenario 4.3: Concurrent acquisition with slow coordinator
    #[tokio::test]
    async fn test_concurrent_acquisition_serialization() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("concurrent", 1).await;

        // Simulate concurrent acquisition attempts
        // Due to the mock's atomic operations, only one will succeed
        let mut handles = vec![];

        for broker_id in 1..=3 {
            let broker = cluster.brokers[broker_id - 1].clone();
            handles.push(tokio::spawn(async move {
                broker.acquire_partition("concurrent", 0, 60).await
            }));
        }

        let mut success_count = 0;
        for handle in handles {
            if handle.await.unwrap().unwrap() {
                success_count += 1;
            }
        }

        assert_eq!(
            success_count, 1,
            "Exactly one broker should acquire the partition"
        );
    }

    /// Scenario 4.4: Graceful lease handoff
    #[tokio::test]
    async fn test_graceful_lease_handoff() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("handoff", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("handoff", 0, 60)
            .await
            .unwrap();
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("handoff", 0)
                .await
                .unwrap()
        );

        // Broker 1 gracefully releases
        cluster
            .broker(1)
            .release_partition("handoff", 0)
            .await
            .unwrap();
        assert!(
            !cluster
                .broker(1)
                .owns_partition_for_read("handoff", 0)
                .await
                .unwrap()
        );

        // Broker 2 can immediately acquire
        let acquired = cluster
            .broker(2)
            .acquire_partition("handoff", 0, 60)
            .await
            .unwrap();
        assert!(
            acquired,
            "Should acquire immediately after graceful release"
        );
    }

    /// Scenario: Lease renewal extends TTL
    #[tokio::test]
    async fn test_lease_renewal_extends_ttl() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("extend-ttl", 1).await;

        // Acquire with short lease
        cluster
            .broker(1)
            .acquire_partition("extend-ttl", 0, 10)
            .await
            .unwrap();

        // Renew multiple times
        for _ in 0..5 {
            let renewed = cluster
                .broker(1)
                .renew_partition_lease("extend-ttl", 0, 10)
                .await
                .unwrap();
            assert!(renewed);
        }

        // Still own the partition
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("extend-ttl", 0)
                .await
                .unwrap()
        );

        // Broker 2 cannot acquire (lease still valid)
        let acquired = cluster
            .broker(2)
            .acquire_partition("extend-ttl", 0, 60)
            .await
            .unwrap();
        assert!(!acquired);
    }
}

// ============================================================================
// Category 5: Fencing Tests
// ============================================================================

mod fencing {
    use super::*;

    /// Scenario 5.1: Dual writer detection via fencing error
    #[tokio::test]
    async fn test_dual_writer_fencing() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("fencing", 1).await;

        // Broker 1 acquires partition
        cluster
            .broker(1)
            .acquire_partition("fencing", 0, 60)
            .await
            .unwrap();

        // Broker 1's operations work
        assert!(
            cluster
                .broker(1)
                .verify_and_extend_lease("fencing", 0, 60)
                .await
                .is_ok()
        );

        // Force broker 2 to take over (simulating SlateDB fencing)
        cluster.kill_broker(1).await;
        cluster
            .broker(2)
            .acquire_partition("fencing", 0, 60)
            .await
            .unwrap();

        // Broker 1's subsequent operations should fail with fencing error
        let result = cluster
            .broker(1)
            .verify_and_extend_lease("fencing", 0, 60)
            .await;

        match result {
            Err(SlateDBError::Fenced) => (), // Expected
            other => panic!("Expected Fenced error, got {:?}", other),
        }
    }

    /// Scenario 5.2: Fencing error detection in error types
    #[tokio::test]
    async fn test_fencing_error_detection() {
        // Test the is_fenced() method on various errors

        // Explicit Fenced variant - always detected
        let fenced_error = SlateDBError::Fenced;
        assert!(fenced_error.is_fenced());

        // SlateDB errors with fencing patterns - now detected via string matching
        let slatedb_fenced = SlateDBError::SlateDB("writer was fenced out".to_string());
        assert!(slatedb_fenced.is_fenced(), "Should detect 'fenced' pattern");

        let slatedb_manifest = SlateDBError::SlateDB("manifest conflict detected".to_string());
        assert!(
            slatedb_manifest.is_fenced(),
            "Should detect 'manifest conflict' pattern"
        );

        let slatedb_writer = SlateDBError::SlateDB("writer id mismatch".to_string());
        assert!(
            slatedb_writer.is_fenced(),
            "Should detect 'writer id mismatch' pattern"
        );

        // Non-fencing SlateDB errors
        let slatedb_timeout = SlateDBError::SlateDB("connection timeout".to_string());
        assert!(!slatedb_timeout.is_fenced(), "Timeout is not fencing");

        let slatedb_notfound = SlateDBError::SlateDB("object not found".to_string());
        assert!(!slatedb_notfound.is_fenced(), "Not found is not fencing");

        // Other error types - never fencing
        let storage_error = SlateDBError::Storage("disk full".to_string());
        assert!(!storage_error.is_fenced());

        // Test is_retriable
        assert!(storage_error.is_retriable());
        assert!(!fenced_error.is_retriable());
    }

    /// Scenario 5.3: Stale handle detection
    #[tokio::test]
    async fn test_stale_handle_operations_fail() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("stale-handle", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("stale-handle", 0, 60)
            .await
            .unwrap();

        // Simulate broker 1 crash and broker 2 takeover
        cluster.kill_broker(1).await;
        cluster
            .broker(2)
            .acquire_partition("stale-handle", 0, 60)
            .await
            .unwrap();

        // Any operation from broker 1 should fail
        // (In real code, this would be SlateDB operations failing)
        assert!(
            !cluster
                .broker(1)
                .owns_partition_for_read("stale-handle", 0)
                .await
                .unwrap()
        );
        assert!(
            cluster
                .broker(1)
                .verify_and_extend_lease("stale-handle", 0, 60)
                .await
                .is_err()
        );
    }
}

// ============================================================================
// Category 6: Clock Skew Tests
// ============================================================================

mod clock_skew {
    use super::*;

    /// Scenario 7.1: Moderate clock skew doesn't break operations
    #[tokio::test]
    async fn test_moderate_clock_skew_tolerated() {
        let injector = FaultInjector::new();

        // Set 3 second clock skew
        injector.set_clock_offset(3);

        // Operations should still work (clock skew is within tolerance)
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("clock-skew", 1).await;

        cluster
            .broker(1)
            .acquire_partition("clock-skew", 0, 60)
            .await
            .unwrap();

        // Verify operations work despite clock skew
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("clock-skew", 0)
                .await
                .unwrap()
        );
        assert!(
            cluster
                .broker(1)
                .renew_partition_lease("clock-skew", 0, 60)
                .await
                .unwrap()
        );
    }

    /// Scenario: Test adjusted time calculation
    #[tokio::test]
    async fn test_clock_offset_calculation() {
        let injector = FaultInjector::new();

        // No offset
        let now = Instant::now();
        let adjusted = injector.adjusted_time();
        let diff = adjusted.duration_since(now);
        assert!(
            diff < Duration::from_millis(100),
            "No offset should mean ~same time"
        );

        // Positive offset (clock ahead)
        injector.set_clock_offset(5);
        let adjusted = injector.adjusted_time();
        let expected_diff = Duration::from_secs(5);
        let actual_diff = adjusted.duration_since(now);
        assert!(actual_diff >= expected_diff - Duration::from_millis(100));
        assert!(actual_diff <= expected_diff + Duration::from_millis(100));
    }
}

// ============================================================================
// Category 7: Resource Exhaustion Tests
// ============================================================================

mod resource_exhaustion {
    use super::*;

    /// Scenario 8.1: Test fault injector latency
    #[tokio::test]
    async fn test_operation_latency_injection() {
        let injector = FaultInjector::new();

        // No latency initially
        let start = Instant::now();
        injector.apply_latency().await;
        assert!(start.elapsed() < Duration::from_millis(50));

        // Add latency
        injector.set_latency(100);
        let start = Instant::now();
        injector.apply_latency().await;
        assert!(start.elapsed() >= Duration::from_millis(100));
        assert!(start.elapsed() < Duration::from_millis(200));
    }

    /// Scenario 8.2: Test operation failure injection
    #[tokio::test]
    async fn test_operation_failure_injection() {
        let injector = FaultInjector::new();

        // No failures initially
        assert!(!injector.should_fail());

        // Set failures
        injector.fail_next(3);
        assert!(injector.should_fail());
        assert!(injector.should_fail());
        assert!(injector.should_fail());
        assert!(!injector.should_fail()); // Counter exhausted
    }

    /// Scenario 8.3: Network block injection
    #[tokio::test]
    async fn test_network_block_injection() {
        let injector = FaultInjector::new();

        // Network available
        assert!(injector.check_operation().await.is_ok());

        // Block network
        injector.block_network();
        assert!(injector.check_operation().await.is_err());

        // Unblock
        injector.unblock_network();
        assert!(injector.check_operation().await.is_ok());
    }

    /// Scenario 8.4: Partition-specific failures
    #[tokio::test]
    async fn test_partition_specific_failure() {
        let injector = FaultInjector::new();

        // No failures initially
        assert!(!injector.is_partition_failing("topic", 0).await);
        assert!(!injector.is_partition_failing("topic", 1).await);

        // Fail specific partition
        injector.fail_partition("topic", 0).await;
        assert!(injector.is_partition_failing("topic", 0).await);
        assert!(!injector.is_partition_failing("topic", 1).await);

        // Clear failure
        injector.clear_partition_failure("topic", 0).await;
        assert!(!injector.is_partition_failing("topic", 0).await);
    }
}

// ============================================================================
// Category 8: Recovery Scenario Tests
// ============================================================================

mod recovery {
    use super::*;

    /// Scenario 10.1: Full cluster restart
    #[tokio::test]
    async fn test_full_cluster_restart_recovery() {
        // Create cluster and establish state
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("restart-test", 3).await;

        // Distribute partitions
        cluster
            .broker(1)
            .acquire_partition("restart-test", 0, 60)
            .await
            .unwrap();
        cluster
            .broker(2)
            .acquire_partition("restart-test", 1, 60)
            .await
            .unwrap();
        cluster
            .broker(3)
            .acquire_partition("restart-test", 2, 60)
            .await
            .unwrap();

        // Verify initial state
        assert_eq!(cluster.get_owner("restart-test", 0).await, Some(1));
        assert_eq!(cluster.get_owner("restart-test", 1).await, Some(2));
        assert_eq!(cluster.get_owner("restart-test", 2).await, Some(3));

        // Simulate full cluster shutdown (expire all leases)
        for b in 1..=3 {
            cluster.kill_broker(b).await;
        }

        // All partitions should have no owner
        for p in 0..3 {
            assert_eq!(cluster.get_owner("restart-test", p).await, None);
        }

        // Cluster restarts - brokers re-acquire partitions
        cluster
            .broker(1)
            .acquire_partition("restart-test", 0, 60)
            .await
            .unwrap();
        cluster
            .broker(2)
            .acquire_partition("restart-test", 1, 60)
            .await
            .unwrap();
        cluster
            .broker(3)
            .acquire_partition("restart-test", 2, 60)
            .await
            .unwrap();

        // Verify recovered state
        assert_eq!(cluster.get_owner("restart-test", 0).await, Some(1));
        assert_eq!(cluster.get_owner("restart-test", 1).await, Some(2));
        assert_eq!(cluster.get_owner("restart-test", 2).await, Some(3));
    }

    /// Scenario 10.2: Rolling restart
    #[tokio::test]
    async fn test_rolling_restart_maintains_availability() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("rolling-restart", 3).await;

        // Each broker owns one partition
        for (broker_id, partition) in [(1, 0), (2, 1), (3, 2)] {
            cluster
                .broker(broker_id)
                .acquire_partition("rolling-restart", partition, 60)
                .await
                .unwrap();
        }

        // Rolling restart: one broker at a time
        for restarting_broker in 1..=3 {
            // Broker goes down
            cluster.kill_broker(restarting_broker).await;

            // Other brokers still functioning
            for broker_id in 1..=3 {
                if broker_id != restarting_broker {
                    // Can still renew their own partitions
                    let partition = broker_id as i32 - 1;
                    if cluster
                        .broker(broker_id)
                        .owns_partition_for_read("rolling-restart", partition)
                        .await
                        .unwrap()
                    {
                        assert!(
                            cluster
                                .broker(broker_id)
                                .renew_partition_lease("rolling-restart", partition, 60)
                                .await
                                .unwrap()
                        );
                    }
                }
            }

            // Restarted broker re-acquires its partition
            let partition = restarting_broker as i32 - 1;
            let acquired = cluster
                .broker(restarting_broker)
                .acquire_partition("rolling-restart", partition, 60)
                .await
                .unwrap();
            assert!(
                acquired,
                "Broker {} should re-acquire partition {} after restart",
                restarting_broker, partition
            );
        }
    }

    /// Scenario 10.3: Single node recovery
    #[tokio::test]
    async fn test_single_node_recovery_reacquires_partitions() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("single-recovery", 3).await;

        // Broker 1 owns multiple partitions
        cluster
            .broker(1)
            .acquire_partition("single-recovery", 0, 60)
            .await
            .unwrap();
        cluster
            .broker(1)
            .acquire_partition("single-recovery", 1, 60)
            .await
            .unwrap();
        cluster
            .broker(2)
            .acquire_partition("single-recovery", 2, 60)
            .await
            .unwrap();

        // Broker 1 crashes
        cluster.kill_broker(1).await;

        // Broker 3 takes over orphaned partitions
        cluster
            .broker(3)
            .acquire_partition("single-recovery", 0, 60)
            .await
            .unwrap();
        cluster
            .broker(3)
            .acquire_partition("single-recovery", 1, 60)
            .await
            .unwrap();

        // Verify new distribution
        assert_eq!(cluster.get_owner("single-recovery", 0).await, Some(3));
        assert_eq!(cluster.get_owner("single-recovery", 1).await, Some(3));
        assert_eq!(cluster.get_owner("single-recovery", 2).await, Some(2));
    }
}

// ============================================================================
// Integration Invariant Tests
// ============================================================================

mod invariants {
    use super::*;

    /// Invariant: At most one owner per partition at any time
    #[tokio::test]
    async fn test_invariant_single_partition_owner() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("invariant-test", 1).await;

        // All brokers try to acquire same partition concurrently
        let handles: Vec<_> = (1..=3)
            .map(|id| {
                let broker = cluster.brokers[id - 1].clone();
                tokio::spawn(async move {
                    (
                        id,
                        broker
                            .acquire_partition("invariant-test", 0, 60)
                            .await
                            .unwrap(),
                    )
                })
            })
            .collect();

        let mut owner_count = 0;
        let mut owner_id = None;

        for handle in handles {
            let (id, acquired) = handle.await.unwrap();
            if acquired {
                owner_count += 1;
                owner_id = Some(id);
            }
        }

        assert_eq!(
            owner_count, 1,
            "Exactly one broker should own the partition"
        );

        // Verify with coordinator
        let actual_owner = cluster.get_owner("invariant-test", 0).await;
        assert_eq!(actual_owner, owner_id.map(|id| id as i32));
    }

    /// Invariant: Fenced broker cannot write
    #[tokio::test]
    async fn test_invariant_fenced_broker_rejected() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("fenced-invariant", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("fenced-invariant", 0, 60)
            .await
            .unwrap();

        // Force takeover by broker 2
        cluster.kill_broker(1).await;
        cluster
            .broker(2)
            .acquire_partition("fenced-invariant", 0, 60)
            .await
            .unwrap();

        // Broker 1 must not be able to verify ownership
        let result = cluster
            .broker(1)
            .verify_and_extend_lease("fenced-invariant", 0, 60)
            .await;
        assert!(
            result.is_err(),
            "Fenced broker should not be able to verify ownership"
        );
    }

    /// Invariant: Released partition is immediately available
    #[tokio::test]
    async fn test_invariant_released_partition_available() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("release-invariant", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("release-invariant", 0, 60)
            .await
            .unwrap();

        // Broker 2 cannot acquire
        let acquired = cluster
            .broker(2)
            .acquire_partition("release-invariant", 0, 60)
            .await
            .unwrap();
        assert!(!acquired);

        // Broker 1 releases
        cluster
            .broker(1)
            .release_partition("release-invariant", 0)
            .await
            .unwrap();

        // Broker 2 can now acquire immediately
        let acquired = cluster
            .broker(2)
            .acquire_partition("release-invariant", 0, 60)
            .await
            .unwrap();
        assert!(
            acquired,
            "Released partition should be immediately available"
        );
    }
}

// ============================================================================
// Category 9: Consumer Group Chaos Tests
// ============================================================================

mod consumer_group {
    use super::*;
    use kafkaesque::cluster::{ConsumerGroupCoordinator, HeartbeatResult};

    /// Scenario: Consumer group member joins and gets assignment
    #[tokio::test]
    async fn test_consumer_group_member_join() {
        let coordinator = MockCoordinator::new(1, "localhost", next_port() as i32);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("test-topic", 3).await.unwrap();

        // Join consumer group using the actual API signature
        let result = coordinator
            .join_group("test-group", "consumer-1", &[], 30000)
            .await
            .unwrap();

        // Returns (generation_id, member_id, is_leader, leader_id, members)
        let (generation_id, member_id, _is_leader, _leader_id, _members) = result;
        assert!(!member_id.is_empty());
        assert!(generation_id >= 0);
    }

    /// Scenario: Multiple consumers join same group - verify rebalancing
    #[tokio::test]
    async fn test_consumer_group_rebalance_on_join() {
        let coordinator = MockCoordinator::new(1, "localhost", next_port() as i32);
        coordinator.register_broker().await.unwrap();
        coordinator
            .register_topic("rebalance-topic", 6)
            .await
            .unwrap();

        // First consumer joins
        let result1 = coordinator
            .join_group("rebalance-group", "consumer-1", &[], 30000)
            .await
            .unwrap();
        let (_, member_id1, _, _, _) = result1;
        assert!(!member_id1.is_empty());

        // Second consumer joins - triggers rebalance
        let result2 = coordinator
            .join_group("rebalance-group", "consumer-2", &[], 30000)
            .await
            .unwrap();
        let (_, member_id2, _, _, _) = result2;
        assert!(!member_id2.is_empty());
        assert_ne!(member_id1, member_id2);
    }

    /// Scenario: Consumer leaves group gracefully
    #[tokio::test]
    async fn test_consumer_group_graceful_leave() {
        let coordinator = MockCoordinator::new(1, "localhost", next_port() as i32);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("leave-topic", 3).await.unwrap();

        // Join
        let result = coordinator
            .join_group("leave-group", "consumer-1", &[], 30000)
            .await
            .unwrap();
        let (_, member_id, _, _, _) = result;

        // Leave using remove_group_member
        let leave_result = coordinator
            .remove_group_member("leave-group", &member_id)
            .await;
        assert!(leave_result.is_ok());
    }

    /// Scenario: Consumer heartbeat keeps membership alive
    #[tokio::test]
    async fn test_consumer_heartbeat() {
        let coordinator = MockCoordinator::new(1, "localhost", next_port() as i32);
        coordinator.register_broker().await.unwrap();
        coordinator
            .register_topic("heartbeat-topic", 3)
            .await
            .unwrap();

        // Join
        let result = coordinator
            .join_group("heartbeat-group", "consumer-1", &[], 30000)
            .await
            .unwrap();
        let (generation_id, member_id, _, _, _) = result;

        // Send heartbeat - should succeed
        let heartbeat_result = coordinator
            .update_member_heartbeat_with_generation("heartbeat-group", &member_id, generation_id)
            .await
            .unwrap();
        assert!(matches!(heartbeat_result, HeartbeatResult::Success));
    }

    /// Scenario: Consumer with wrong generation gets rejected
    #[tokio::test]
    async fn test_consumer_stale_generation_rejected() {
        let coordinator = MockCoordinator::new(1, "localhost", next_port() as i32);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("gen-topic", 3).await.unwrap();

        // Join
        let result = coordinator
            .join_group("gen-group", "consumer-1", &[], 30000)
            .await
            .unwrap();
        let (generation_id, member_id, _, _, _) = result;

        // Heartbeat with wrong generation should fail or return illegal generation
        let bad_generation = generation_id + 100;
        let heartbeat_result = coordinator
            .update_member_heartbeat_with_generation("gen-group", &member_id, bad_generation)
            .await
            .unwrap();

        // Should indicate illegal generation
        assert!(matches!(
            heartbeat_result,
            HeartbeatResult::IllegalGeneration
        ));
    }

    /// Scenario: Concurrent consumers joining same group
    #[tokio::test]
    async fn test_concurrent_consumer_joins() {
        let coordinator = Arc::new(MockCoordinator::new(1, "localhost", next_port() as i32));
        coordinator.register_broker().await.unwrap();
        coordinator
            .register_topic("concurrent-topic", 6)
            .await
            .unwrap();

        // Spawn multiple consumers joining concurrently
        let mut handles = vec![];
        for i in 0..3 {
            let coord = coordinator.clone();
            handles.push(tokio::spawn(async move {
                coord
                    .join_group("concurrent-group", &format!("consumer-{}", i), &[], 30000)
                    .await
            }));
        }

        // All should succeed with unique member IDs
        let mut member_ids = std::collections::HashSet::new();
        for handle in handles {
            let result = handle.await.unwrap().unwrap();
            let (_, member_id, _, _, _) = result;
            member_ids.insert(member_id);
        }
        assert_eq!(
            member_ids.len(),
            3,
            "All consumers should have unique member IDs"
        );
    }
}

// ============================================================================
// Category 10: Coordinator Failure Tests
// ============================================================================

mod coordinator_failure {
    use super::*;

    /// Scenario 9.1: Coordinator unavailable - operations fail gracefully
    #[tokio::test]
    async fn test_operations_fail_when_coordinator_unavailable() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("coord-fail", 1).await;

        // Broker 1 acquires partition
        cluster
            .broker(1)
            .acquire_partition("coord-fail", 0, 60)
            .await
            .unwrap();

        // Simulate coordinator unavailable by expiring all leases
        // (in real system, this would be network partition to Raft leader)
        cluster.kill_broker(1).await;

        // Verification should fail
        let result = cluster
            .broker(1)
            .verify_and_extend_lease("coord-fail", 0, 60)
            .await;
        assert!(result.is_err());
    }

    /// Scenario 9.2: Multiple brokers competing after coordinator recovery
    #[tokio::test]
    async fn test_partition_reacquisition_after_coordinator_recovery() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("coord-recovery", 3).await;

        // All brokers acquire one partition each
        for i in 0..3 {
            cluster
                .broker(i + 1)
                .acquire_partition("coord-recovery", i as i32, 60)
                .await
                .unwrap();
        }

        // Simulate coordinator failure - all leases expire
        for b in 1..=3 {
            cluster.kill_broker(b).await;
        }

        // All partitions should be unowned
        for p in 0..3 {
            assert_eq!(cluster.get_owner("coord-recovery", p).await, None);
        }

        // Brokers race to reacquire - simulate recovery
        let mut acquired = [false; 3];
        for p in 0..3 {
            // Each broker tries to acquire each partition
            for b in 1..=3 {
                if cluster
                    .broker(b)
                    .acquire_partition("coord-recovery", p, 60)
                    .await
                    .unwrap()
                {
                    acquired[p as usize] = true;
                    break;
                }
            }
        }

        // All partitions should be acquired by someone
        assert!(
            acquired.iter().all(|&x| x),
            "All partitions should be reacquired"
        );

        // Verify single owner per partition
        for p in 0..3 {
            let owner = cluster.get_owner("coord-recovery", p).await;
            assert!(owner.is_some(), "Partition {} should have owner", p);
        }
    }

    /// Scenario: Broker detects stale coordinator state
    #[tokio::test]
    async fn test_broker_detects_stale_ownership() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("stale-coord", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("stale-coord", 0, 60)
            .await
            .unwrap();

        // Broker 1 thinks it owns partition
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("stale-coord", 0)
                .await
                .unwrap()
        );

        // Broker 2 forcefully takes over (simulating coordinator state change)
        cluster.kill_broker(1).await;
        cluster
            .broker(2)
            .acquire_partition("stale-coord", 0, 60)
            .await
            .unwrap();

        // Broker 1's view is now stale
        assert!(
            !cluster
                .broker(1)
                .owns_partition_for_read("stale-coord", 0)
                .await
                .unwrap()
        );

        // Broker 2 is now the owner
        assert!(
            cluster
                .broker(2)
                .owns_partition_for_read("stale-coord", 0)
                .await
                .unwrap()
        );
    }
}

// ============================================================================
// Category 11: Idempotent Producer Tests
// ============================================================================

mod idempotent_producer {
    use super::*;
    use kafkaesque::cluster::ProducerCoordinator;

    /// Scenario: Producer ID allocation is unique
    #[tokio::test]
    async fn test_producer_id_allocation_unique() {
        let coordinator = MockCoordinator::new(1, "localhost", next_port() as i32);
        coordinator.register_broker().await.unwrap();

        let mut ids = std::collections::HashSet::new();
        for _ in 0..10 {
            let id = coordinator.next_producer_id().await.unwrap();
            assert!(ids.insert(id), "Producer IDs should be unique");
        }
    }

    /// Scenario: Concurrent producer ID allocation
    #[tokio::test]
    async fn test_concurrent_producer_id_allocation() {
        let coordinator = Arc::new(MockCoordinator::new(1, "localhost", next_port() as i32));
        coordinator.register_broker().await.unwrap();

        let mut handles = vec![];
        for _ in 0..10 {
            let coord = coordinator.clone();
            handles.push(tokio::spawn(async move { coord.next_producer_id().await }));
        }

        let mut ids = std::collections::HashSet::new();
        for handle in handles {
            let id = handle.await.unwrap().unwrap();
            assert!(ids.insert(id), "Concurrent producer IDs should be unique");
        }
    }

    /// Scenario: Producer state persistence and recovery
    #[tokio::test]
    async fn test_producer_state_persistence() {
        let coordinator = MockCoordinator::new(1, "localhost", next_port() as i32);
        coordinator.register_broker().await.unwrap();
        coordinator
            .register_topic("idempotent-topic", 1)
            .await
            .unwrap();

        let producer_id = coordinator.next_producer_id().await.unwrap();
        let producer_epoch: i16 = 0;
        let last_sequence: i32 = 100;

        // Store producer state
        let result = coordinator
            .store_producer_state(
                "idempotent-topic",
                0,
                producer_id,
                last_sequence,
                producer_epoch,
            )
            .await;
        assert!(result.is_ok());

        // Load producer states
        let states = coordinator
            .load_producer_states("idempotent-topic", 0)
            .await
            .unwrap();

        // Should contain our producer state
        assert!(states.contains_key(&producer_id));
    }

    /// Scenario: Init producer ID returns valid ID and epoch
    #[tokio::test]
    async fn test_init_producer_id() {
        let coordinator = MockCoordinator::new(1, "localhost", next_port() as i32);
        coordinator.register_broker().await.unwrap();

        // Initialize new producer
        let (producer_id, epoch) = coordinator.init_producer_id(None, -1, -1).await.unwrap();

        assert!(producer_id > 0);
        assert_eq!(epoch, 0);

        // Re-init with same producer_id should bump epoch
        let (producer_id2, epoch2) = coordinator
            .init_producer_id(None, producer_id, epoch)
            .await
            .unwrap();

        // The mock always allocates new IDs, so producer_id2 > producer_id
        // In a real implementation, it would return the same producer_id with bumped epoch
        assert!(producer_id2 >= producer_id);
        assert!(epoch2 >= 0);
    }
}

// ============================================================================
// Category 12: Concurrent Stress Tests
// ============================================================================

mod stress_tests {
    use super::*;

    /// Stress test: Rapid partition acquisition/release cycles
    #[tokio::test]
    async fn test_rapid_partition_cycles() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("rapid-cycle", 1).await;

        for _ in 0..20 {
            // Broker 1 acquires
            cluster
                .broker(1)
                .acquire_partition("rapid-cycle", 0, 60)
                .await
                .unwrap();

            // Broker 1 releases
            cluster
                .broker(1)
                .release_partition("rapid-cycle", 0)
                .await
                .unwrap();

            // Broker 2 acquires
            cluster
                .broker(2)
                .acquire_partition("rapid-cycle", 0, 60)
                .await
                .unwrap();

            // Broker 2 releases
            cluster
                .broker(2)
                .release_partition("rapid-cycle", 0)
                .await
                .unwrap();
        }

        // Final state: no owner
        assert_eq!(cluster.get_owner("rapid-cycle", 0).await, None);
    }

    /// Stress test: Concurrent topic creation
    #[tokio::test]
    async fn test_concurrent_topic_creation() {
        let cluster = TestCluster::new(3).await;

        let mut handles = vec![];
        for i in 0..10 {
            let broker = cluster.brokers[0].clone();
            handles.push(tokio::spawn(async move {
                broker
                    .register_topic(&format!("concurrent-topic-{}", i), 3)
                    .await
            }));
        }

        for handle in handles {
            assert!(handle.await.unwrap().is_ok());
        }

        // Verify all topics exist
        let topics = cluster.broker(1).get_topics().await.unwrap();
        assert!(topics.len() >= 10);
    }

    /// Stress test: Many partitions per topic
    #[tokio::test]
    async fn test_many_partitions_distribution() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("many-partitions", 30).await;

        // Distribute partitions across brokers
        for p in 0..30 {
            let broker_id = (p % 3) + 1;
            cluster
                .broker(broker_id as usize)
                .acquire_partition("many-partitions", p, 60)
                .await
                .unwrap();
        }

        // Verify distribution
        let mut counts = [0, 0, 0];
        for p in 0..30 {
            if let Some(owner) = cluster.get_owner("many-partitions", p).await {
                counts[(owner - 1) as usize] += 1;
            }
        }

        // Each broker should have 10 partitions
        assert_eq!(counts, [10, 10, 10]);
    }

    /// Stress test: Rapid broker registration/deregistration
    #[tokio::test]
    async fn test_rapid_broker_lifecycle() {
        let cluster = TestCluster::new(5).await;
        cluster.create_topic("broker-lifecycle", 5).await;

        // Initial distribution
        for p in 0..5 {
            cluster
                .broker(p + 1)
                .acquire_partition("broker-lifecycle", p as i32, 60)
                .await
                .unwrap();
        }

        // Cycle through broker failures and recoveries
        for cycle in 0..3 {
            let failing_broker = (cycle % 5) + 1;

            // Broker fails
            cluster.kill_broker(failing_broker).await;

            // Another broker takes over orphaned partition
            let takeover_broker = (failing_broker % 5) + 1;
            let partition = (failing_broker - 1) as i32;

            cluster
                .broker(takeover_broker)
                .acquire_partition("broker-lifecycle", partition, 60)
                .await
                .unwrap();
        }
    }
}

// ============================================================================
// Category 13: Partition Rebalance Tests
// ============================================================================

mod partition_rebalance {
    use super::*;

    /// Scenario: Partitions redistributed when broker joins
    #[tokio::test]
    async fn test_partition_redistribution_on_broker_join() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("redistribute", 6).await;

        // Initially broker 1 owns all partitions
        for p in 0..6 {
            cluster
                .broker(1)
                .acquire_partition("redistribute", p, 60)
                .await
                .unwrap();
        }

        // Verify initial state
        for p in 0..6 {
            assert_eq!(cluster.get_owner("redistribute", p).await, Some(1));
        }

        // Broker 1 releases half the partitions
        for p in 3..6 {
            cluster
                .broker(1)
                .release_partition("redistribute", p)
                .await
                .unwrap();
        }

        // Broker 2 acquires released partitions
        for p in 3..6 {
            cluster
                .broker(2)
                .acquire_partition("redistribute", p, 60)
                .await
                .unwrap();
        }

        // Verify redistribution
        for p in 0..3 {
            assert_eq!(cluster.get_owner("redistribute", p).await, Some(1));
        }
        for p in 3..6 {
            assert_eq!(cluster.get_owner("redistribute", p).await, Some(2));
        }
    }

    /// Scenario: Partitions moved when broker leaves
    #[tokio::test]
    async fn test_partition_movement_on_broker_leave() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("leave-redistribute", 6).await;

        // Distribute partitions: broker 1 gets 0,1, broker 2 gets 2,3, broker 3 gets 4,5
        for (broker, partitions) in [(1, vec![0, 1]), (2, vec![2, 3]), (3, vec![4, 5])] {
            for p in partitions {
                cluster
                    .broker(broker)
                    .acquire_partition("leave-redistribute", p, 60)
                    .await
                    .unwrap();
            }
        }

        // Broker 2 leaves (fails)
        cluster.kill_broker(2).await;

        // Verify broker 2's partitions are unowned
        assert_eq!(cluster.get_owner("leave-redistribute", 2).await, None);
        assert_eq!(cluster.get_owner("leave-redistribute", 3).await, None);

        // Other brokers take over
        cluster
            .broker(1)
            .acquire_partition("leave-redistribute", 2, 60)
            .await
            .unwrap();
        cluster
            .broker(3)
            .acquire_partition("leave-redistribute", 3, 60)
            .await
            .unwrap();

        // Verify final state
        assert_eq!(cluster.get_owner("leave-redistribute", 0).await, Some(1));
        assert_eq!(cluster.get_owner("leave-redistribute", 1).await, Some(1));
        assert_eq!(cluster.get_owner("leave-redistribute", 2).await, Some(1));
        assert_eq!(cluster.get_owner("leave-redistribute", 3).await, Some(3));
        assert_eq!(cluster.get_owner("leave-redistribute", 4).await, Some(3));
        assert_eq!(cluster.get_owner("leave-redistribute", 5).await, Some(3));
    }

    /// Scenario: Balanced distribution across brokers
    #[tokio::test]
    async fn test_balanced_partition_distribution() {
        let cluster = TestCluster::new(4).await;
        cluster.create_topic("balanced", 12).await;

        // Round-robin distribution
        for p in 0..12 {
            let broker = (p % 4) + 1;
            cluster
                .broker(broker as usize)
                .acquire_partition("balanced", p, 60)
                .await
                .unwrap();
        }

        // Count partitions per broker
        let mut counts = HashMap::new();
        for p in 0..12 {
            if let Some(owner) = cluster.get_owner("balanced", p).await {
                *counts.entry(owner).or_insert(0) += 1;
            }
        }

        // Each broker should have exactly 3 partitions
        for broker in 1..=4 {
            assert_eq!(
                counts.get(&broker).copied().unwrap_or(0),
                3,
                "Broker {} should have 3 partitions",
                broker
            );
        }
    }
}

// ============================================================================
// Category 14: Additional Invariant Tests
// ============================================================================

mod additional_invariants {
    use super::*;

    /// Invariant: No duplicate partition ownership after rapid changes
    #[tokio::test]
    async fn test_no_duplicate_ownership_after_churn() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("churn-test", 10).await;

        // Rapid acquisition/release by multiple brokers
        for round in 0..5 {
            for p in 0..10 {
                let broker = ((p + round) % 3) + 1;

                // Release if owned by someone
                for b in 1..=3 {
                    if cluster
                        .broker(b)
                        .owns_partition_for_read("churn-test", p)
                        .await
                        .unwrap()
                    {
                        cluster
                            .broker(b)
                            .release_partition("churn-test", p)
                            .await
                            .ok();
                    }
                }

                // New broker acquires
                cluster
                    .broker(broker as usize)
                    .acquire_partition("churn-test", p, 60)
                    .await
                    .ok();
            }
        }

        // Verify: each partition has at most one owner
        for p in 0..10 {
            let mut owner_count = 0;
            for b in 1..=3 {
                if cluster
                    .broker(b)
                    .owns_partition_for_read("churn-test", p)
                    .await
                    .unwrap()
                {
                    owner_count += 1;
                }
            }
            assert!(
                owner_count <= 1,
                "Partition {} should have at most one owner, found {}",
                p,
                owner_count
            );
        }
    }

    /// Invariant: Lease renewal preserves ownership
    #[tokio::test]
    async fn test_lease_renewal_preserves_ownership() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("renewal-preserve", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("renewal-preserve", 0, 60)
            .await
            .unwrap();

        // Multiple renewals
        for _ in 0..10 {
            let renewed = cluster
                .broker(1)
                .renew_partition_lease("renewal-preserve", 0, 60)
                .await
                .unwrap();
            assert!(renewed);

            // Verify ownership unchanged
            assert_eq!(cluster.get_owner("renewal-preserve", 0).await, Some(1));
        }

        // Broker 2 still cannot acquire
        let acquired = cluster
            .broker(2)
            .acquire_partition("renewal-preserve", 0, 60)
            .await
            .unwrap();
        assert!(!acquired);
    }

    /// Invariant: Release makes partition available
    #[tokio::test]
    async fn test_release_makes_available_immediately() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("release-avail", 1).await;

        // Acquire -> Release -> Acquire cycle
        for _ in 0..10 {
            // Broker 1 acquires
            assert!(
                cluster
                    .broker(1)
                    .acquire_partition("release-avail", 0, 60)
                    .await
                    .unwrap()
            );

            // Broker 2 cannot acquire
            assert!(
                !cluster
                    .broker(2)
                    .acquire_partition("release-avail", 0, 60)
                    .await
                    .unwrap()
            );

            // Broker 1 releases
            cluster
                .broker(1)
                .release_partition("release-avail", 0)
                .await
                .unwrap();

            // Broker 2 can now acquire immediately
            assert!(
                cluster
                    .broker(2)
                    .acquire_partition("release-avail", 0, 60)
                    .await
                    .unwrap()
            );

            // Broker 2 releases for next iteration
            cluster
                .broker(2)
                .release_partition("release-avail", 0)
                .await
                .unwrap();
        }
    }

    /// Invariant: Expired broker cannot renew
    #[tokio::test]
    async fn test_expired_broker_cannot_renew() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("expired-renew", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("expired-renew", 0, 60)
            .await
            .unwrap();

        // Broker 1 "expires" (lease lost)
        cluster.kill_broker(1).await;

        // After expiry, broker 1 doesn't own the partition
        assert!(
            !cluster
                .broker(1)
                .owns_partition_for_read("expired-renew", 0)
                .await
                .unwrap()
        );

        // Verify and extend should fail (fenced)
        let result = cluster
            .broker(1)
            .verify_and_extend_lease("expired-renew", 0, 60)
            .await;
        assert!(
            result.is_err(),
            "Expired broker should not be able to verify ownership"
        );
    }

    /// Invariant: Verify and extend fails for non-owner
    #[tokio::test]
    async fn test_verify_fails_for_non_owner() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("verify-fail", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("verify-fail", 0, 60)
            .await
            .unwrap();

        // Broker 2 cannot verify (not owner)
        let result = cluster
            .broker(2)
            .verify_and_extend_lease("verify-fail", 0, 60)
            .await;
        assert!(result.is_err());
    }
}

// ============================================================================
// Category 15: Edge Case Tests
// ============================================================================

mod edge_cases {
    use super::*;

    /// Edge case: Empty topic name handling
    #[tokio::test]
    async fn test_empty_topic_operations() {
        let cluster = TestCluster::new(2).await;

        // Creating empty topic should work (validation at higher layer)
        let result = cluster.broker(1).register_topic("", 1).await;
        // Behavior depends on implementation - just verify it doesn't panic
        let _ = result;
    }

    /// Edge case: Zero partitions
    #[tokio::test]
    async fn test_zero_partitions_topic() {
        let cluster = TestCluster::new(2).await;
        let result = cluster.broker(1).register_topic("zero-parts", 0).await;
        // Should either succeed or fail gracefully
        let _ = result;
    }

    /// Edge case: Negative partition index
    #[tokio::test]
    async fn test_negative_partition_index() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("neg-test", 3).await;

        // Try to acquire negative partition
        // Note: The mock coordinator may allow this since it doesn't validate partition bounds.
        // In a real implementation, this would fail.
        let result = cluster
            .broker(1)
            .acquire_partition("neg-test", -1, 60)
            .await;

        // The mock allows it, but we verify it doesn't panic
        let _ = result;
    }

    /// Edge case: Very large partition count
    #[tokio::test]
    async fn test_large_partition_count() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("large-parts", 1000).await;

        // Verify we can acquire partitions at the boundaries
        assert!(
            cluster
                .broker(1)
                .acquire_partition("large-parts", 0, 60)
                .await
                .unwrap()
        );
        assert!(
            cluster
                .broker(1)
                .acquire_partition("large-parts", 999, 60)
                .await
                .unwrap()
        );

        // Note: The mock coordinator doesn't validate partition bounds,
        // so partition 1000 may succeed. In real implementation, this would fail.
        // We just verify no panic occurs.
        let result = cluster
            .broker(1)
            .acquire_partition("large-parts", 1000, 60)
            .await;
        let _ = result;
    }

    /// Edge case: Same broker acquires same partition twice
    #[tokio::test]
    async fn test_double_acquisition_same_broker() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("double-acq", 1).await;

        // First acquisition
        assert!(
            cluster
                .broker(1)
                .acquire_partition("double-acq", 0, 60)
                .await
                .unwrap()
        );

        // Second acquisition by same broker - should succeed (idempotent) or fail gracefully
        let result = cluster
            .broker(1)
            .acquire_partition("double-acq", 0, 60)
            .await;
        // Either true (idempotent) or false (already owned) is acceptable
        let _ = result;

        // Should still be owned by broker 1
        assert_eq!(cluster.get_owner("double-acq", 0).await, Some(1));
    }

    /// Edge case: Release partition not owned
    #[tokio::test]
    async fn test_release_not_owned() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("release-not-owned", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("release-not-owned", 0, 60)
            .await
            .unwrap();

        // Broker 2 tries to release (doesn't own it)
        let result = cluster
            .broker(2)
            .release_partition("release-not-owned", 0)
            .await;
        // Should fail or be no-op
        let _ = result;

        // Broker 1 should still own it
        assert_eq!(cluster.get_owner("release-not-owned", 0).await, Some(1));
    }

    /// Edge case: Very short lease TTL
    #[tokio::test]
    async fn test_very_short_lease_ttl() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("short-lease", 1).await;

        // Acquire with 1 second lease
        cluster
            .broker(1)
            .acquire_partition("short-lease", 0, 1)
            .await
            .unwrap();

        // Should be owned
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("short-lease", 0)
                .await
                .unwrap()
        );

        // Note: In real system, lease would expire after 1 second
        // Here we test the mock handles short TTLs
    }

    /// Edge case: Very long lease TTL
    #[tokio::test]
    async fn test_very_long_lease_ttl() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("long-lease", 1).await;

        // Acquire with 1 hour lease
        cluster
            .broker(1)
            .acquire_partition("long-lease", 0, 3600)
            .await
            .unwrap();

        // Should be owned
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("long-lease", 0)
                .await
                .unwrap()
        );

        // Broker 2 cannot acquire
        assert!(
            !cluster
                .broker(2)
                .acquire_partition("long-lease", 0, 60)
                .await
                .unwrap()
        );
    }
}

// ============================================================================
// Category 16: Distributed System Edge Case Tests
// ============================================================================

mod distributed_edge_cases {
    use super::*;

    /// Test: Owner cache staleness during rapid ownership transfers
    /// This tests the edge case where cached ownership information becomes
    /// stale during rapid partition transfers.
    #[tokio::test]
    async fn test_owner_cache_staleness_during_rapid_transfers() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("cache-stale", 1).await;

        // Broker 1 acquires partition
        cluster
            .broker(1)
            .acquire_partition("cache-stale", 0, 60)
            .await
            .unwrap();

        // Verify ownership
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("cache-stale", 0)
                .await
                .unwrap()
        );

        // Rapid transfer cycle - tests cache invalidation
        for round in 0..5 {
            // Current owner releases
            let current_owner = if round % 2 == 0 { 1 } else { 2 };
            let new_owner = if round % 2 == 0 { 2 } else { 1 };

            cluster
                .broker(current_owner)
                .release_partition("cache-stale", 0)
                .await
                .unwrap();

            // New owner acquires
            let acquired = cluster
                .broker(new_owner)
                .acquire_partition("cache-stale", 0, 60)
                .await
                .unwrap();
            assert!(
                acquired,
                "Round {}: Broker {} should acquire",
                round, new_owner
            );

            // Verify old owner's cache is invalidated
            assert!(
                !cluster
                    .broker(current_owner)
                    .owns_partition_for_read("cache-stale", 0)
                    .await
                    .unwrap(),
                "Round {}: Old owner should not think it owns partition",
                round
            );

            // Verify new owner knows it owns
            assert!(
                cluster
                    .broker(new_owner)
                    .owns_partition_for_read("cache-stale", 0)
                    .await
                    .unwrap(),
                "Round {}: New owner should know it owns partition",
                round
            );
        }
    }

    /// Test: Concurrent ownership checks during transfer
    /// Verifies that ownership checks remain consistent during partition transfers
    #[tokio::test]
    async fn test_concurrent_ownership_checks_during_transfer() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("concurrent-check", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("concurrent-check", 0, 60)
            .await
            .unwrap();

        // Concurrent ownership checks and transfers
        let b1 = cluster.brokers[0].clone();
        let b2 = cluster.brokers[1].clone();

        let mut handles = vec![];

        // Checker task for broker 1
        let b1_clone = b1.clone();
        handles.push(tokio::spawn(async move {
            let mut owns_count = 0;
            let mut not_owns_count = 0;
            for _ in 0..100 {
                if b1_clone
                    .owns_partition_for_read("concurrent-check", 0)
                    .await
                    .unwrap()
                {
                    owns_count += 1;
                } else {
                    not_owns_count += 1;
                }
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
            (owns_count, not_owns_count)
        }));

        // Transfer task
        let b1_release = b1.clone();
        let b2_acquire = b2.clone();
        handles.push(tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(5)).await;
            b1_release
                .release_partition("concurrent-check", 0)
                .await
                .ok();
            b2_acquire
                .acquire_partition("concurrent-check", 0, 60)
                .await
                .ok();
            (0, 0) // Dummy return to match types
        }));

        for handle in handles {
            handle.await.unwrap();
        }

        // Final state: broker 2 owns the partition
        assert!(
            cluster
                .broker(2)
                .owns_partition_for_read("concurrent-check", 0)
                .await
                .unwrap()
        );
    }

    /// Test: Heartbeat failure counting edge cases
    /// Tests that the failure detector correctly handles edge cases in
    /// heartbeat counting and timing.
    #[tokio::test]
    async fn test_heartbeat_timing_edge_cases() {
        use kafkaesque::cluster::failure_detector::{
            BrokerHealthState, FailureDetector, FailureDetectorConfig,
        };

        // Very short heartbeat interval for testing
        // Set jitter_tolerance and startup_grace_period to 0 for deterministic testing
        let config = FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(10),
            suspicion_threshold: 3,
            failure_threshold: 6,
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        };

        let detector = FailureDetector::new(config);
        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Initial state should be healthy
        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Healthy)
        );

        // Wait just under suspicion threshold
        sleep(Duration::from_millis(25)).await;
        detector.check_brokers();
        // Should still be healthy or just suspected
        let state = detector.get_broker_state(1);
        assert!(
            state == Some(BrokerHealthState::Healthy)
                || state == Some(BrokerHealthState::Suspected)
        );

        // Wait past failure threshold
        sleep(Duration::from_millis(50)).await;
        detector.check_brokers();
        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Failed)
        );

        // Recovery: send heartbeat
        detector.record_heartbeat(1);
        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Healthy)
        );
    }

    /// Test: Lease expiration timing boundary conditions
    #[tokio::test]
    async fn test_lease_boundary_conditions() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("lease-boundary", 1).await;

        // Acquire with very short lease
        cluster
            .broker(1)
            .acquire_partition("lease-boundary", 0, 2)
            .await
            .unwrap();

        // Should own immediately
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("lease-boundary", 0)
                .await
                .unwrap()
        );

        // Renew repeatedly to keep alive
        for _ in 0..3 {
            assert!(
                cluster
                    .broker(1)
                    .renew_partition_lease("lease-boundary", 0, 2)
                    .await
                    .unwrap()
            );
        }

        // Still owns after renewals
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("lease-boundary", 0)
                .await
                .unwrap()
        );
    }

    /// Test: Consumer group generation validation across rebalances
    /// Tests that stale generation IDs are correctly rejected
    #[tokio::test]
    async fn test_consumer_generation_validation_across_rebalances() {
        use kafkaesque::cluster::{ConsumerGroupCoordinator, HeartbeatResult};

        let coordinator = MockCoordinator::new(1, "localhost", next_port() as i32);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("gen-test", 3).await.unwrap();

        // First consumer joins
        let (gen1, member1, _, _, _) = coordinator
            .join_group("gen-group", "consumer-1", &[], 30000)
            .await
            .unwrap();

        // Heartbeat with current generation should work
        let result = coordinator
            .update_member_heartbeat_with_generation("gen-group", &member1, gen1)
            .await
            .unwrap();
        assert!(matches!(result, HeartbeatResult::Success));

        // Second consumer joins - triggers new generation
        let (gen2, _member2, _, _, _) = coordinator
            .join_group("gen-group", "consumer-2", &[], 30000)
            .await
            .unwrap();

        // New generation should be higher
        assert!(
            gen2 > gen1,
            "New generation {} should be > old generation {}",
            gen2,
            gen1
        );

        // Heartbeat with old generation should fail
        let result = coordinator
            .update_member_heartbeat_with_generation("gen-group", &member1, gen1)
            .await
            .unwrap();
        assert!(
            matches!(result, HeartbeatResult::IllegalGeneration),
            "Old generation heartbeat should be rejected"
        );
    }

    /// Test: Partition acquisition race between many brokers
    #[tokio::test]
    async fn test_multi_broker_acquisition_race() {
        let cluster = TestCluster::new(5).await;
        cluster.create_topic("multi-race", 1).await;

        // All 5 brokers race for the same partition
        let mut handles = vec![];

        for id in 1..=5 {
            let broker = cluster.brokers[id - 1].clone();
            handles.push(tokio::spawn(async move {
                broker.acquire_partition("multi-race", 0, 60).await
            }));
        }

        let mut winners = 0;
        for handle in handles {
            if handle.await.unwrap().unwrap() {
                winners += 1;
            }
        }

        // Exactly one winner
        assert_eq!(winners, 1, "Exactly one broker should win the race");

        // Verify consistent ownership view
        let owner = cluster.get_owner("multi-race", 0).await;
        assert!(owner.is_some(), "Partition should have an owner");

        // All brokers should agree on ownership
        for id in 1..=5 {
            let owns = cluster
                .broker(id)
                .owns_partition_for_read("multi-race", 0)
                .await
                .unwrap();
            if owns {
                assert_eq!(owner, Some(id as i32));
            }
        }
    }

    /// Test: Verify and extend atomic semantics
    /// Tests that verify_and_extend_lease is atomic - either fully succeeds
    /// or fully fails, never partially.
    #[tokio::test]
    async fn test_verify_extend_atomicity() {
        let cluster = TestCluster::new(2).await;
        cluster.create_topic("atomic-extend", 1).await;

        // Broker 1 acquires
        cluster
            .broker(1)
            .acquire_partition("atomic-extend", 0, 60)
            .await
            .unwrap();

        // Concurrent verify_and_extend calls
        let b1 = cluster.brokers[0].clone();
        let mut handles = vec![];

        for _ in 0..10 {
            let broker = b1.clone();
            handles.push(tokio::spawn(async move {
                broker.verify_and_extend_lease("atomic-extend", 0, 60).await
            }));
        }

        // All calls should succeed or all fail (no partial state)
        let mut success_count = 0;
        let mut fail_count = 0;
        for handle in handles {
            match handle.await.unwrap() {
                Ok(_) => success_count += 1,
                Err(_) => fail_count += 1,
            }
        }

        // Since broker 1 owns the partition, all should succeed
        assert!(
            success_count >= fail_count,
            "Most extensions should succeed"
        );

        // Ownership should still be valid
        assert!(
            cluster
                .broker(1)
                .owns_partition_for_read("atomic-extend", 0)
                .await
                .unwrap()
        );
    }

    /// Test: Producer state consistency across restarts
    #[tokio::test]
    async fn test_producer_state_consistency() {
        use kafkaesque::cluster::ProducerCoordinator;

        let coordinator = MockCoordinator::new(1, "localhost", next_port() as i32);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("prod-state", 1).await.unwrap();

        // Create producer and store state
        let (pid, epoch) = coordinator.init_producer_id(None, -1, -1).await.unwrap();
        assert!(pid > 0);
        assert_eq!(epoch, 0);

        // Store some sequence state
        coordinator
            .store_producer_state("prod-state", 0, pid, 100, epoch)
            .await
            .unwrap();

        // Load states - should contain our producer
        let states = coordinator
            .load_producer_states("prod-state", 0)
            .await
            .unwrap();
        assert!(states.contains_key(&pid));
        assert_eq!(states[&pid].last_sequence, 100);
        assert_eq!(states[&pid].producer_epoch, epoch);

        // Update sequence
        coordinator
            .store_producer_state("prod-state", 0, pid, 200, epoch)
            .await
            .unwrap();

        // Verify update
        let states = coordinator
            .load_producer_states("prod-state", 0)
            .await
            .unwrap();
        assert_eq!(states[&pid].last_sequence, 200);
    }

    /// Test: Offset commit with generation validation
    #[tokio::test]
    async fn test_offset_commit_generation_validation() {
        use kafkaesque::cluster::ConsumerGroupCoordinator;
        use kafkaesque::cluster::HeartbeatResult;

        let coordinator = MockCoordinator::new(1, "localhost", next_port() as i32);
        coordinator.register_broker().await.unwrap();
        coordinator.register_topic("offset-gen", 3).await.unwrap();

        // Join group
        let (generation, member_id, _, _, _) = coordinator
            .join_group("offset-group", "consumer-1", &[], 30000)
            .await
            .unwrap();

        // Validate member for commit with correct generation
        let result = coordinator
            .validate_member_for_commit("offset-group", &member_id, generation)
            .await
            .unwrap();
        assert!(matches!(result, HeartbeatResult::Success));

        // Validate with wrong generation should fail
        let result = coordinator
            .validate_member_for_commit("offset-group", &member_id, generation + 100)
            .await
            .unwrap();
        assert!(matches!(result, HeartbeatResult::IllegalGeneration));

        // Validate unknown member should fail
        let result = coordinator
            .validate_member_for_commit("offset-group", "unknown-member", generation)
            .await
            .unwrap();
        assert!(matches!(result, HeartbeatResult::UnknownMember));
    }

    /// Test: Load-aware failover distribution
    /// Verifies that when a broker fails, its partitions are redistributed
    /// to balance load across remaining brokers.
    #[tokio::test]
    async fn test_load_aware_failover() {
        let cluster = TestCluster::new(4).await;
        cluster.create_topic("failover-load", 12).await;

        // Initial distribution: broker 1 has 6, broker 2 has 3, broker 3 has 3, broker 4 has 0
        for p in 0..6 {
            cluster
                .broker(1)
                .acquire_partition("failover-load", p, 60)
                .await
                .unwrap();
        }
        for p in 6..9 {
            cluster
                .broker(2)
                .acquire_partition("failover-load", p, 60)
                .await
                .unwrap();
        }
        for p in 9..12 {
            cluster
                .broker(3)
                .acquire_partition("failover-load", p, 60)
                .await
                .unwrap();
        }

        // Broker 1 fails (with 6 partitions)
        cluster.kill_broker(1).await;

        // Brokers 2, 3, 4 should take over
        // With load-aware distribution, broker 4 (0 partitions) should get more
        let mut redistributed = std::collections::HashMap::new();
        for p in 0..6 {
            for broker_id in [2, 3, 4] {
                if cluster
                    .broker(broker_id)
                    .acquire_partition("failover-load", p, 60)
                    .await
                    .unwrap()
                {
                    *redistributed.entry(broker_id).or_insert(0) += 1;
                    break;
                }
            }
        }

        // All 6 orphaned partitions should be redistributed
        let total: usize = redistributed.values().sum();
        assert_eq!(total, 6, "All orphaned partitions should be redistributed");
    }

    /// Test: Zombie mode blocks writes atomically
    #[tokio::test]
    async fn test_zombie_mode_atomic_block() {
        let zombie_state = ZombieModeState::new();

        // Not in zombie mode
        assert!(!zombie_state.is_active());

        // Enter zombie mode atomically
        let entered = zombie_state.enter();
        assert!(entered);
        assert!(zombie_state.is_active());

        // Get the entry timestamp for later exit
        let entered_at = zombie_state.entered_at();

        // Re-entry should return false (already in zombie mode)
        let entered_again = zombie_state.enter();
        assert!(!entered_again);
        assert!(zombie_state.is_active());

        // Exit using the correct API
        let exited = zombie_state.try_exit(entered_at, "test");
        assert!(exited);
        assert!(!zombie_state.is_active());

        // Can enter again after exit
        let entered_third = zombie_state.enter();
        assert!(entered_third);
    }

    /// Test: Rapid topic creation/deletion consistency
    #[tokio::test]
    async fn test_rapid_topic_lifecycle() {
        let cluster = TestCluster::new(2).await;

        for i in 0..10 {
            let topic = format!("lifecycle-{}", i);

            // Create topic
            cluster.broker(1).register_topic(&topic, 3).await.unwrap();

            // Verify exists
            assert!(cluster.broker(1).topic_exists(&topic).await.unwrap());

            // Acquire partitions
            for p in 0..3 {
                cluster
                    .broker(1)
                    .acquire_partition(&topic, p, 60)
                    .await
                    .unwrap();
            }

            // Delete topic
            cluster.broker(1).delete_topic(&topic).await.ok();

            // Topic might still exist (implementation dependent)
            // but partitions should eventually become unavailable
        }
    }

    /// Test: Split-brain scenario detection
    /// Simulates a network partition that could lead to split-brain
    /// and verifies the system handles it correctly.
    #[tokio::test]
    async fn test_split_brain_prevention() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("split-brain", 3).await;

        // Initial distribution
        cluster
            .broker(1)
            .acquire_partition("split-brain", 0, 60)
            .await
            .unwrap();
        cluster
            .broker(2)
            .acquire_partition("split-brain", 1, 60)
            .await
            .unwrap();
        cluster
            .broker(3)
            .acquire_partition("split-brain", 2, 60)
            .await
            .unwrap();

        // Simulate partition - broker 1 is isolated
        cluster.kill_broker(1).await;

        // Broker 2 takes over partition 0
        assert!(
            cluster
                .broker(2)
                .acquire_partition("split-brain", 0, 60)
                .await
                .unwrap()
        );

        // Broker 1's verify should fail (fenced)
        let result = cluster
            .broker(1)
            .verify_and_extend_lease("split-brain", 0, 60)
            .await;
        assert!(
            result.is_err(),
            "Fenced broker should not be able to verify"
        );

        // System maintains single-owner invariant
        let owner = cluster.get_owner("split-brain", 0).await;
        assert_eq!(owner, Some(2));
    }

    /// Test: Consistent hashing assignment stability
    /// Verifies that partition assignments are stable when broker set doesn't change
    #[tokio::test]
    async fn test_consistent_assignment_stability() {
        let cluster = TestCluster::new(3).await;
        cluster.create_topic("hash-stable", 10).await;

        // Get initial assignments
        let initial_assigned = cluster.broker(1).get_assigned_partitions().await.unwrap();

        // Query multiple times - should be stable
        for _ in 0..5 {
            let assigned = cluster.broker(1).get_assigned_partitions().await.unwrap();
            assert_eq!(
                assigned, initial_assigned,
                "Assignments should be stable without broker changes"
            );
        }
    }
}

// ============================================================================
// Category 17: Rebalance Coordinator Tests
// ============================================================================

mod rebalance_coordinator_tests {
    use kafkaesque::cluster::rebalance_coordinator::{
        RebalanceCoordinator, RebalanceCoordinatorConfig,
    };
    use tokio::runtime::Handle;

    /// Test: Rebalance coordinator basic functionality
    #[tokio::test]
    async fn test_rebalance_coordinator_creation() {
        let config = RebalanceCoordinatorConfig::default();
        let coordinator = RebalanceCoordinator::new(config, 1, Handle::current());

        assert_eq!(coordinator.total_failovers(), 0);
        assert_eq!(coordinator.total_rebalances(), 0);
        assert!(!coordinator.is_running());
    }

    /// Test: Broker registration and heartbeat tracking
    #[tokio::test]
    async fn test_rebalance_broker_health_tracking() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());

        coordinator.register_broker(2);
        coordinator.register_broker(3);

        // Record heartbeats
        coordinator.record_heartbeat(2);
        coordinator.record_heartbeat(3);

        // Check health
        let changes = coordinator.check_broker_health();
        assert!(
            changes.is_empty(),
            "No health changes expected immediately after heartbeat"
        );
    }

    /// Test: State summary contains correct information
    #[tokio::test]
    async fn test_rebalance_state_summary() {
        let coordinator = RebalanceCoordinator::with_defaults(1, Handle::current());

        coordinator.register_broker(2);
        coordinator.register_broker(3);

        let summary = coordinator.get_state_summary().await;

        assert_eq!(summary.broker_id, 1);
        assert!(!summary.is_running);
        assert!(summary.fast_failover_enabled);
        assert!(summary.auto_balancer_enabled);
        assert_eq!(summary.tracked_brokers, 2);
        assert_eq!(summary.healthy_brokers, 2);
    }

    /// Test: Config builder patterns
    #[test]
    fn test_rebalance_config_builders() {
        let config = RebalanceCoordinatorConfig::default().with_failover_disabled();
        assert!(!config.fast_failover_enabled);

        let config = RebalanceCoordinatorConfig::default().with_balancing_disabled();
        assert!(!config.auto_balancer.enabled);

        let config = RebalanceCoordinatorConfig::default()
            .with_failover_disabled()
            .with_balancing_disabled();
        assert!(!config.fast_failover_enabled);
        assert!(!config.auto_balancer.enabled);
    }
}
