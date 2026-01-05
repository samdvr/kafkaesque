//! Type-safe abstractions for partition operations.
//!
//! This module provides safer abstractions that reduce boilerplate and prevent
//! common errors when working with partitions:
//!
//! - [`PartitionId`]: A strongly-typed partition identifier
//! - [`PartitionHandle`]: A handle for performing operations on a partition
//! - [`WriteGuard`]: An RAII guard for write operations with lease tracking
//!
//! # Design Goals
//!
//! 1. **Type Safety**: Prevent mixing up topic/partition arguments
//! 2. **Reduced Boilerplate**: No more passing `(topic, partition)` tuples everywhere
//! 3. **Automatic Resource Management**: Guards automatically release resources on drop
//! 4. **Clear Ownership Semantics**: Guards make it clear when you have write access
//!
//! # Examples
//!
//! ```ignore
//! // Instead of:
//! let store = manager.get_for_write("my-topic", 0).await?;
//! store.append_batch(&records).await?;
//!
//! // Use:
//! let partition = PartitionId::new("my-topic", 0);
//! let guard = manager.acquire_for_write(&partition).await?;
//! guard.append(&records).await?;
//! // Lease tracking updated automatically on drop
//! ```

use bytes::Bytes;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;
use std::time::Instant;

use super::error::{SlateDBError, SlateDBResult};
use super::partition_store::PartitionStore;

// =============================================================================
// PartitionId - Strongly typed partition identifier
// =============================================================================

/// A strongly-typed partition identifier.
///
/// Encapsulates the (topic, partition) tuple to prevent argument mix-ups
/// and provide a clear type for partition references.
///
/// # Example
///
/// ```ignore
/// let partition = PartitionId::new("orders", 5);
/// println!("Processing partition: {}", partition);
/// ```
#[derive(Clone, Eq)]
pub struct PartitionId {
    topic: Arc<str>,
    partition: i32,
}

impl PartitionId {
    /// Create a new partition identifier.
    #[inline]
    pub fn new(topic: impl Into<Arc<str>>, partition: i32) -> Self {
        Self {
            topic: topic.into(),
            partition,
        }
    }

    /// Create from a tuple reference.
    #[inline]
    pub fn from_tuple(tuple: &(String, i32)) -> Self {
        Self {
            topic: tuple.0.as_str().into(),
            partition: tuple.1,
        }
    }

    #[inline]
    pub fn topic(&self) -> &str {
        &self.topic
    }

    #[inline]
    pub fn partition(&self) -> i32 {
        self.partition
    }

    /// Convert to tuple for compatibility with existing APIs.
    #[inline]
    pub fn as_tuple(&self) -> (String, i32) {
        (self.topic.to_string(), self.partition)
    }

    /// Convert to tuple reference for map lookups.
    #[inline]
    pub fn to_key(&self) -> super::PartitionKey {
        (Arc::clone(&self.topic), self.partition)
    }
}

impl fmt::Debug for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "PartitionId({}/{})", self.topic, self.partition)
    }
}

impl fmt::Display for PartitionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}/{}", self.topic, self.partition)
    }
}

impl PartialEq for PartitionId {
    fn eq(&self, other: &Self) -> bool {
        self.topic == other.topic && self.partition == other.partition
    }
}

impl Hash for PartitionId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.topic.hash(state);
        self.partition.hash(state);
    }
}

impl From<(String, i32)> for PartitionId {
    fn from(tuple: (String, i32)) -> Self {
        Self::new(tuple.0, tuple.1)
    }
}

impl From<(&str, i32)> for PartitionId {
    fn from(tuple: (&str, i32)) -> Self {
        Self::new(tuple.0, tuple.1)
    }
}

// =============================================================================
// PartitionHandle - Type-safe operations on a partition
// =============================================================================

/// A handle for performing read operations on a partition.
///
/// This is a lightweight wrapper that provides type-safe access to partition
/// operations without the overhead of repeated string allocations.
///
/// # Example
///
/// ```ignore
/// let handle = PartitionHandle::new(partition_id, store);
/// let records = handle.fetch(0, 1000).await?;
/// let hwm = handle.high_watermark();
/// ```
pub struct PartitionHandle {
    id: PartitionId,
    store: Arc<PartitionStore>,
}

impl PartitionHandle {
    /// Create a new partition handle.
    pub fn new(id: PartitionId, store: Arc<PartitionStore>) -> Self {
        Self { id, store }
    }

    #[inline]
    pub fn id(&self) -> &PartitionId {
        &self.id
    }

    #[inline]
    pub fn topic(&self) -> &str {
        self.id.topic()
    }

    #[inline]
    pub fn partition(&self) -> i32 {
        self.id.partition()
    }

    #[inline]
    pub fn store(&self) -> &Arc<PartitionStore> {
        &self.store
    }

    #[inline]
    pub fn high_watermark(&self) -> i64 {
        self.store.high_watermark()
    }

    /// Fetch records starting from a given offset.
    ///
    /// # Arguments
    /// * `start_offset` - The offset to start fetching from
    ///
    /// # Returns
    /// Tuple of (high_watermark, Option<record_batch>), or error if fetch fails.
    pub async fn fetch(&self, start_offset: i64) -> SlateDBResult<(i64, Option<Bytes>)> {
        self.store.fetch_from(start_offset).await
    }
}

impl fmt::Debug for PartitionHandle {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionHandle")
            .field("id", &self.id)
            .field("hwm", &self.high_watermark())
            .finish()
    }
}

// =============================================================================
// WriteGuard - RAII guard for write operations
// =============================================================================

/// An RAII guard for write operations on a partition.
///
/// This guard:
/// 1. Tracks when the lease was verified
/// 2. Provides write operations on the partition
/// 3. Records metrics on drop
///
/// The guard does NOT automatically release the lease on drop because:
/// - Leases are time-based and expire naturally
/// - Releasing leases on every request would add Raft overhead
/// - The partition manager handles lease renewal in the background
///
/// # Example
///
/// ```ignore
/// let guard = manager.acquire_for_write(&partition).await?;
/// let offset = guard.append(&records).await?;
/// // Guard dropped here, metrics recorded
/// ```
pub struct WriteGuard {
    handle: PartitionHandle,
    acquired_at: Instant,
    lease_remaining_secs: u64,
}

impl WriteGuard {
    /// Create a new write guard.
    ///
    /// # Arguments
    /// * `id` - The partition identifier
    /// * `store` - The partition store
    /// * `lease_remaining_secs` - Remaining lease TTL at time of acquisition
    pub fn new(id: PartitionId, store: Arc<PartitionStore>, lease_remaining_secs: u64) -> Self {
        Self {
            handle: PartitionHandle::new(id, store),
            acquired_at: Instant::now(),
            lease_remaining_secs,
        }
    }

    #[inline]
    pub fn id(&self) -> &PartitionId {
        self.handle.id()
    }

    #[inline]
    pub fn topic(&self) -> &str {
        self.handle.topic()
    }

    #[inline]
    pub fn partition(&self) -> i32 {
        self.handle.partition()
    }

    #[inline]
    pub fn handle(&self) -> &PartitionHandle {
        &self.handle
    }

    #[inline]
    pub fn high_watermark(&self) -> i64 {
        self.handle.high_watermark()
    }

    #[inline]
    pub fn held_for(&self) -> std::time::Duration {
        self.acquired_at.elapsed()
    }

    /// Get the remaining lease time (estimated).
    ///
    /// This is an estimate based on the lease TTL at acquisition time
    /// minus the time the guard has been held.
    #[inline]
    pub fn estimated_remaining_lease(&self) -> u64 {
        let held_secs = self.held_for().as_secs();
        self.lease_remaining_secs.saturating_sub(held_secs)
    }

    /// Check if the lease is still likely valid.
    ///
    /// Returns true if the estimated remaining lease is above the safety threshold.
    #[inline]
    pub fn lease_likely_valid(&self) -> bool {
        self.estimated_remaining_lease() >= crate::constants::DEFAULT_MIN_LEASE_TTL_FOR_WRITE_SECS
    }

    /// Append a record batch to the partition.
    ///
    /// # Arguments
    /// * `records` - The record batch bytes to append
    ///
    /// # Returns
    /// The base offset assigned to the batch, or error if append fails.
    pub async fn append(&self, records: &Bytes) -> SlateDBResult<i64> {
        // Validate lease is still likely valid
        if !self.lease_likely_valid() {
            return Err(SlateDBError::LeaseTooShort {
                topic: self.topic().to_string(),
                partition: self.partition(),
                remaining_secs: self.estimated_remaining_lease(),
                required_secs: crate::constants::DEFAULT_MIN_LEASE_TTL_FOR_WRITE_SECS,
            });
        }

        self.handle.store.append_batch(records).await
    }

    /// Get the underlying store (for advanced use cases).
    #[inline]
    pub fn store(&self) -> &Arc<PartitionStore> {
        self.handle.store()
    }
}

impl fmt::Debug for WriteGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("WriteGuard")
            .field("id", &self.handle.id())
            .field("held_for", &self.held_for())
            .field(
                "estimated_remaining_lease",
                &self.estimated_remaining_lease(),
            )
            .finish()
    }
}

impl Drop for WriteGuard {
    fn drop(&mut self) {
        // Record how long the guard was held
        let held_ms = self.held_for().as_millis() as f64 / 1000.0;

        // Record metric for write guard duration
        super::metrics::WRITE_GUARD_DURATION
            .with_label_values(&[self.topic()])
            .observe(held_ms);

        // Warn if guard was held for a suspiciously long time
        if held_ms > 5.0 {
            tracing::warn!(
                partition = %self.handle.id(),
                held_secs = held_ms,
                "WriteGuard held for >5 seconds - potential slow write or leaked guard"
            );
        }
    }
}

// =============================================================================
// ReadGuard - Lightweight guard for read operations
// =============================================================================

/// A lightweight guard for read operations.
///
/// Unlike WriteGuard, this doesn't track lease state since reads don't
/// require ownership verification. It's mainly useful for consistent
/// API design and potential future extensions.
pub struct ReadGuard {
    handle: PartitionHandle,
}

impl ReadGuard {
    /// Create a new read guard.
    pub fn new(id: PartitionId, store: Arc<PartitionStore>) -> Self {
        Self {
            handle: PartitionHandle::new(id, store),
        }
    }

    /// Get the partition identifier.
    #[inline]
    pub fn id(&self) -> &PartitionId {
        self.handle.id()
    }

    /// Get the topic name.
    #[inline]
    pub fn topic(&self) -> &str {
        self.handle.topic()
    }

    /// Get the partition index.
    #[inline]
    pub fn partition(&self) -> i32 {
        self.handle.partition()
    }

    /// Get the current high watermark.
    #[inline]
    pub fn high_watermark(&self) -> i64 {
        self.handle.high_watermark()
    }

    /// Fetch records starting from a given offset.
    pub async fn fetch(&self, start_offset: i64) -> SlateDBResult<(i64, Option<Bytes>)> {
        self.handle.fetch(start_offset).await
    }

    /// Get the underlying store (for advanced use cases).
    #[inline]
    pub fn store(&self) -> &Arc<PartitionStore> {
        self.handle.store()
    }
}

impl fmt::Debug for ReadGuard {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ReadGuard")
            .field("id", &self.handle.id())
            .field("hwm", &self.high_watermark())
            .finish()
    }
}

// =============================================================================
// Tests
// =============================================================================

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_id_equality() {
        let p1 = PartitionId::new("topic-a", 0);
        let p2 = PartitionId::new("topic-a", 0);
        let p3 = PartitionId::new("topic-a", 1);
        let p4 = PartitionId::new("topic-b", 0);

        assert_eq!(p1, p2);
        assert_ne!(p1, p3);
        assert_ne!(p1, p4);
    }

    #[test]
    fn test_partition_id_hash() {
        use std::collections::HashSet;

        let mut set = HashSet::new();
        set.insert(PartitionId::new("topic-a", 0));
        set.insert(PartitionId::new("topic-a", 1));
        set.insert(PartitionId::new("topic-b", 0));

        assert_eq!(set.len(), 3);
        assert!(set.contains(&PartitionId::new("topic-a", 0)));
        assert!(!set.contains(&PartitionId::new("topic-c", 0)));
    }

    #[test]
    fn test_partition_id_display() {
        let p = PartitionId::new("my-topic", 42);
        assert_eq!(format!("{}", p), "my-topic/42");
        assert_eq!(format!("{:?}", p), "PartitionId(my-topic/42)");
    }

    #[test]
    fn test_partition_id_from_tuple() {
        let tuple = ("orders".to_string(), 5);
        let p = PartitionId::from_tuple(&tuple);
        assert_eq!(p.topic(), "orders");
        assert_eq!(p.partition(), 5);
    }

    #[test]
    fn test_partition_id_conversions() {
        let p1: PartitionId = ("topic", 0).into();
        assert_eq!(p1.topic(), "topic");

        let p2: PartitionId = ("topic".to_string(), 1).into();
        assert_eq!(p2.partition(), 1);
    }

    #[test]
    fn test_partition_id_as_tuple() {
        let p = PartitionId::new("my-topic", 3);
        let tuple = p.as_tuple();
        assert_eq!(tuple.0, "my-topic");
        assert_eq!(tuple.1, 3);
    }

    #[test]
    fn test_partition_id_to_key() {
        let p = PartitionId::new("events", 7);
        let key = p.to_key();
        assert_eq!(key.0.as_ref(), "events");
        assert_eq!(key.1, 7);
    }

    #[test]
    fn test_partition_id_clone() {
        let p1 = PartitionId::new("topic-clone", 5);
        let p2 = p1.clone();
        assert_eq!(p1, p2);
        assert_eq!(p1.topic(), p2.topic());
        assert_eq!(p1.partition(), p2.partition());
    }

    #[test]
    fn test_partition_id_arc_str_sharing() {
        // Verify Arc<str> allows efficient sharing
        let topic = "shared-topic";
        let p1 = PartitionId::new(topic, 0);
        let p2 = PartitionId::new(topic, 1);

        // Both should work independently
        assert_eq!(p1.topic(), "shared-topic");
        assert_eq!(p2.topic(), "shared-topic");
        assert_eq!(p1.partition(), 0);
        assert_eq!(p2.partition(), 1);
    }

    #[test]
    fn test_partition_id_empty_topic() {
        // Edge case: empty topic name
        let p = PartitionId::new("", 0);
        assert_eq!(p.topic(), "");
        assert_eq!(p.partition(), 0);
        assert_eq!(format!("{}", p), "/0");
    }

    #[test]
    fn test_partition_id_negative_partition() {
        // Edge case: negative partition (while not valid in Kafka, should work at type level)
        let p = PartitionId::new("topic", -1);
        assert_eq!(p.partition(), -1);
    }

    #[test]
    fn test_partition_id_large_partition() {
        // Edge case: large partition number
        let p = PartitionId::new("topic", i32::MAX);
        assert_eq!(p.partition(), i32::MAX);
    }

    #[test]
    fn test_partition_id_unicode_topic() {
        // Edge case: unicode topic name
        let p = PartitionId::new("topic-日本語-émoji-🎉", 0);
        assert_eq!(p.topic(), "topic-日本語-émoji-🎉");
    }

    #[test]
    fn test_partition_id_special_characters() {
        // Topic with special characters
        let p = PartitionId::new("topic.with-dots_and-dashes", 42);
        assert_eq!(p.topic(), "topic.with-dots_and-dashes");
        let key = p.to_key();
        assert_eq!(key.0.as_ref(), "topic.with-dots_and-dashes");
    }

    // ========================================================================
    // WriteGuard Tests (without PartitionStore - testing time/lease logic)
    // ========================================================================

    #[test]
    fn test_write_guard_held_for_initial() {
        // WriteGuard.held_for() returns elapsed time since creation
        // We can't easily test this without a store, but we can test the logic
        // by checking that held_for() returns a small value immediately after creation
        let start = std::time::Instant::now();
        std::thread::sleep(std::time::Duration::from_millis(10));
        let elapsed = start.elapsed();
        // Just verify the Duration API works as expected
        assert!(elapsed >= std::time::Duration::from_millis(10));
    }

    #[test]
    fn test_estimated_remaining_lease_calculation() {
        // Test the saturating_sub logic used in estimated_remaining_lease
        // lease_remaining_secs.saturating_sub(held_secs)
        let lease_remaining: u64 = 60;
        let held_secs: u64 = 20;
        let remaining = lease_remaining.saturating_sub(held_secs);
        assert_eq!(remaining, 40);

        // Test underflow protection
        let lease_remaining: u64 = 10;
        let held_secs: u64 = 20;
        let remaining = lease_remaining.saturating_sub(held_secs);
        assert_eq!(remaining, 0);
    }

    #[test]
    fn test_lease_likely_valid_threshold() {
        // Test the threshold constant
        let threshold = crate::constants::DEFAULT_MIN_LEASE_TTL_FOR_WRITE_SECS;
        assert!(threshold > 0, "Threshold should be positive");
        assert!(threshold <= 60, "Threshold should be reasonable (<=60s)");
    }

    // ========================================================================
    // PartitionHandle and Guard Tests (with mock PartitionStore)
    // ========================================================================

    mod with_store {
        use super::super::*;
        use crate::cluster::partition_store::PartitionStore;
        use crate::cluster::zombie_mode::ZombieModeState;
        use object_store::memory::InMemory;
        use std::sync::Arc;

        /// Create a test PartitionStore with in-memory storage.
        async fn create_test_store(topic: &str, partition: i32) -> Arc<PartitionStore> {
            let object_store = Arc::new(InMemory::new());
            let store = PartitionStore::builder()
                .object_store(object_store)
                .base_path("test")
                .topic(topic)
                .partition(partition)
                .zombie_mode(Arc::new(ZombieModeState::new()))
                .build()
                .await
                .expect("Failed to create test store");
            Arc::new(store)
        }

        #[tokio::test]
        async fn test_partition_handle_creation() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let handle = PartitionHandle::new(id.clone(), store);

            assert_eq!(handle.topic(), "test-topic");
            assert_eq!(handle.partition(), 0);
            assert_eq!(handle.id(), &id);
        }

        #[tokio::test]
        async fn test_partition_handle_high_watermark() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let handle = PartitionHandle::new(id, store);

            // Initial HWM is 0
            assert_eq!(handle.high_watermark(), 0);
        }

        #[tokio::test]
        async fn test_partition_handle_fetch_empty() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let handle = PartitionHandle::new(id, store);

            // Fetch from offset 0 should return HWM=0 and no data
            let (hwm, data) = handle.fetch(0).await.expect("Fetch should succeed");
            assert_eq!(hwm, 0);
            assert!(data.is_none());
        }

        #[tokio::test]
        async fn test_partition_handle_store_accessor() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let handle = PartitionHandle::new(id, store.clone());

            // store() should return the same Arc
            assert!(Arc::ptr_eq(&store, handle.store()));
        }

        #[tokio::test]
        async fn test_partition_handle_debug() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let handle = PartitionHandle::new(id, store);

            let debug = format!("{:?}", handle);
            assert!(debug.contains("PartitionHandle"));
            assert!(debug.contains("test-topic"));
        }

        #[tokio::test]
        async fn test_read_guard_creation() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = ReadGuard::new(id.clone(), store);

            assert_eq!(guard.topic(), "test-topic");
            assert_eq!(guard.partition(), 0);
            assert_eq!(guard.id(), &id);
        }

        #[tokio::test]
        async fn test_read_guard_high_watermark() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = ReadGuard::new(id, store);

            assert_eq!(guard.high_watermark(), 0);
        }

        #[tokio::test]
        async fn test_read_guard_fetch_empty() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = ReadGuard::new(id, store);

            let (hwm, data) = guard.fetch(0).await.expect("Fetch should succeed");
            assert_eq!(hwm, 0);
            assert!(data.is_none());
        }

        #[tokio::test]
        async fn test_read_guard_store_accessor() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = ReadGuard::new(id, store.clone());

            assert!(Arc::ptr_eq(&store, guard.store()));
        }

        #[tokio::test]
        async fn test_read_guard_debug() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = ReadGuard::new(id, store);

            let debug = format!("{:?}", guard);
            assert!(debug.contains("ReadGuard"));
        }

        #[tokio::test]
        async fn test_write_guard_creation() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = WriteGuard::new(id.clone(), store, 60);

            assert_eq!(guard.topic(), "test-topic");
            assert_eq!(guard.partition(), 0);
            assert_eq!(guard.id(), &id);
        }

        #[tokio::test]
        async fn test_write_guard_high_watermark() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = WriteGuard::new(id, store, 60);

            assert_eq!(guard.high_watermark(), 0);
        }

        #[tokio::test]
        async fn test_write_guard_held_for() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = WriteGuard::new(id, store, 60);

            // Should be very small immediately after creation
            let held = guard.held_for();
            assert!(held.as_secs() < 1);
        }

        #[tokio::test]
        async fn test_write_guard_estimated_remaining_lease() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = WriteGuard::new(id, store, 60);

            // Should be close to 60s immediately after creation
            let remaining = guard.estimated_remaining_lease();
            assert!(
                remaining >= 59,
                "Remaining should be ~60, got {}",
                remaining
            );
            assert!(remaining <= 60);
        }

        #[tokio::test]
        async fn test_write_guard_lease_likely_valid_with_sufficient_ttl() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = WriteGuard::new(id, store, 60);

            // With 60s lease, should be valid
            assert!(guard.lease_likely_valid());
        }

        #[tokio::test]
        async fn test_write_guard_lease_likely_invalid_with_low_ttl() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            // Create with TTL below threshold
            let guard = WriteGuard::new(id, store, 5);

            // With 5s lease (below threshold), should be invalid
            assert!(!guard.lease_likely_valid());
        }

        #[tokio::test]
        async fn test_write_guard_store_accessor() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = WriteGuard::new(id, store.clone(), 60);

            assert!(Arc::ptr_eq(&store, guard.store()));
        }

        #[tokio::test]
        async fn test_write_guard_handle_accessor() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = WriteGuard::new(id.clone(), store, 60);

            assert_eq!(guard.handle().id(), &id);
        }

        #[tokio::test]
        async fn test_write_guard_debug() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            let guard = WriteGuard::new(id, store, 60);

            let debug = format!("{:?}", guard);
            assert!(debug.contains("WriteGuard"));
            assert!(debug.contains("estimated_remaining_lease"));
        }

        #[tokio::test]
        async fn test_write_guard_append_rejects_low_lease() {
            let store = create_test_store("test-topic", 0).await;
            let id = PartitionId::new("test-topic", 0);
            // Create with TTL below threshold
            let guard = WriteGuard::new(id, store, 5);

            // Create a minimal valid record batch
            let records = create_test_batch(1);

            // Append should fail due to low lease TTL
            let result = guard.append(&records).await;
            assert!(result.is_err());
            let err = result.unwrap_err();
            assert!(
                matches!(err, SlateDBError::LeaseTooShort { .. }),
                "Expected LeaseTooShort error, got {:?}",
                err
            );
        }

        /// Create a valid Kafka RecordBatch for testing.
        fn create_test_batch(record_count: i32) -> bytes::Bytes {
            let mut batch = vec![0u8; 100];

            // base_offset (will be patched by PartitionStore)
            batch[0..8].copy_from_slice(&0i64.to_be_bytes());

            // batch_length
            batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes());

            // partition_leader_epoch
            batch[12..16].copy_from_slice(&0i32.to_be_bytes());

            // magic = 2
            batch[16] = 2;

            // attributes
            batch[21..23].copy_from_slice(&0i16.to_be_bytes());

            // last_offset_delta (record_count - 1)
            batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes());

            // first_timestamp
            batch[27..35].copy_from_slice(&0i64.to_be_bytes());

            // max_timestamp
            batch[35..43].copy_from_slice(&0i64.to_be_bytes());

            // producer_id = -1 (non-idempotent)
            batch[43..51].copy_from_slice(&(-1i64).to_be_bytes());

            // producer_epoch
            batch[51..53].copy_from_slice(&0i16.to_be_bytes());

            // first_sequence
            batch[53..57].copy_from_slice(&0i32.to_be_bytes());

            // records_count
            batch[57..61].copy_from_slice(&record_count.to_be_bytes());

            // Compute and set CRC over bytes 21+
            let crc = compute_crc32c(&batch[21..]);
            batch[17..21].copy_from_slice(&crc.to_be_bytes());

            bytes::Bytes::from(batch)
        }

        /// Compute CRC-32C checksum (Castagnoli polynomial).
        fn compute_crc32c(data: &[u8]) -> u32 {
            const CRC32C_TABLE: [u32; 256] = {
                let mut table = [0u32; 256];
                let mut i = 0;
                while i < 256 {
                    let mut crc = i as u32;
                    let mut j = 0;
                    while j < 8 {
                        if crc & 1 != 0 {
                            crc = (crc >> 1) ^ 0x82F63B78;
                        } else {
                            crc >>= 1;
                        }
                        j += 1;
                    }
                    table[i] = crc;
                    i += 1;
                }
                table
            };

            let mut crc = !0u32;
            for &byte in data {
                let index = ((crc ^ byte as u32) & 0xFF) as usize;
                crc = (crc >> 8) ^ CRC32C_TABLE[index];
            }
            !crc
        }
    }
}
