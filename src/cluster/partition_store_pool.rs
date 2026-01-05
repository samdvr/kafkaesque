//! Partition store pool for managing SlateDB instances.
//!
//! This module provides an LRU-based pool for SlateDB partition stores to limit
//! memory usage when a broker owns many partitions. Stores are opened on-demand
//! and automatically closed when evicted from the pool.
//!
//! # Design
//!
//! - Uses `moka::future::Cache` for async LRU caching with automatic eviction
//! - Configurable maximum number of open stores (default: 1000)
//! - Stores are opened lazily on first access
//! - Idle stores are evicted when capacity is reached
//! - Evicted stores are automatically closed (SlateDB Drop)
//!
//! # Usage
//!
//! ```rust,ignore
//! use kafkaesque::cluster::partition_store_pool::{PartitionStorePool, PoolConfig};
//!
//! let pool = PartitionStorePool::new(config, object_store, zombie_state);
//!
//! // Get or create a store
//! let store = pool.get_or_open("my-topic", 0, leader_epoch).await?;
//!
//! // Use the store...
//! store.append_batch(...).await?;
//!
//! // Store is automatically managed by the pool
//! ```
//!
//! # Metrics
//!
//! The pool exposes the following metrics:
//! - `partition_store_pool_size`: Current number of open stores
//! - `partition_store_pool_hits`: Cache hits
//! - `partition_store_pool_misses`: Cache misses (required opening)
//! - `partition_store_pool_evictions`: Stores evicted due to capacity

use moka::future::Cache;
use object_store::ObjectStore;
use std::sync::Arc;
use std::time::Duration;
use tracing::{debug, info};

use super::PartitionKey;
use super::config::ClusterConfig;
use super::error::SlateDBResult;
use super::partition_store::PartitionStore;
use super::zombie_mode::ZombieModeState;

/// Default maximum number of partition stores to keep open.
pub const DEFAULT_MAX_OPEN_STORES: u64 = 1000;

/// Default idle timeout for stores (5 minutes).
pub const DEFAULT_IDLE_TIMEOUT_SECS: u64 = 300;

/// Configuration for the partition store pool.
#[derive(Debug, Clone)]
pub struct PoolConfig {
    /// Maximum number of stores to keep open simultaneously.
    pub max_open_stores: u64,

    /// Idle timeout for stores. Stores not accessed for this duration
    /// may be evicted even before capacity is reached.
    pub idle_timeout: Duration,

    /// Maximum fetch response size for opened stores.
    pub max_fetch_response_size: usize,

    /// Producer state cache TTL in seconds.
    pub producer_state_cache_ttl_secs: u64,

    /// Whether to fail on recovery gaps when opening stores.
    pub fail_on_recovery_gap: bool,

    /// Minimum lease TTL required for write operations.
    pub min_lease_ttl_for_write_secs: u64,

    /// Base path in the object store.
    pub base_path: String,

    /// SlateDB max unflushed bytes before backpressure.
    pub slatedb_max_unflushed_bytes: usize,

    /// SlateDB L0 SST size in bytes.
    pub slatedb_l0_sst_size_bytes: usize,

    /// SlateDB flush interval in milliseconds.
    pub slatedb_flush_interval_ms: u64,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            max_open_stores: DEFAULT_MAX_OPEN_STORES,
            idle_timeout: Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS),
            max_fetch_response_size: 1024 * 1024, // 1 MB
            producer_state_cache_ttl_secs: 900,   // 15 minutes
            fail_on_recovery_gap: false,
            min_lease_ttl_for_write_secs: crate::constants::DEFAULT_MIN_LEASE_TTL_FOR_WRITE_SECS,
            base_path: String::new(),
            slatedb_max_unflushed_bytes: 256 * 1024 * 1024, // 256 MB
            slatedb_l0_sst_size_bytes: 64 * 1024 * 1024,    // 64 MB
            slatedb_flush_interval_ms: 100,
        }
    }
}

impl PoolConfig {
    /// Create pool config from cluster config.
    pub fn from_cluster_config(config: &ClusterConfig) -> Self {
        Self {
            max_open_stores: config.max_open_partition_stores,
            idle_timeout: Duration::from_secs(config.partition_store_idle_timeout_secs),
            max_fetch_response_size: config.max_fetch_response_size,
            producer_state_cache_ttl_secs: config.producer_state_cache_ttl_secs,
            fail_on_recovery_gap: config.fail_on_recovery_gap,
            min_lease_ttl_for_write_secs: config.min_lease_ttl_for_write_secs,
            base_path: config.object_store_path.clone(),
            slatedb_max_unflushed_bytes: config.slatedb_max_unflushed_bytes,
            slatedb_l0_sst_size_bytes: config.slatedb_l0_sst_size_bytes,
            slatedb_flush_interval_ms: config.slatedb_flush_interval_ms,
        }
    }
}

/// Pool for managing partition stores with LRU eviction.
///
/// This pool limits the number of simultaneously open SlateDB instances
/// to bound memory usage. Stores are opened on-demand and automatically
/// closed when evicted from the pool.
pub struct PartitionStorePool {
    /// LRU cache of open partition stores.
    cache: Cache<PartitionKey, Arc<PartitionStore>>,

    /// Object store for opening new stores.
    object_store: Arc<dyn ObjectStore>,

    /// Configuration.
    config: PoolConfig,

    /// Zombie mode state for store validation.
    zombie_state: Arc<ZombieModeState>,
}

impl PartitionStorePool {
    /// Create a new partition store pool.
    pub fn new(
        config: PoolConfig,
        object_store: Arc<dyn ObjectStore>,
        zombie_state: Arc<ZombieModeState>,
    ) -> Self {
        let cache = Cache::builder()
            .max_capacity(config.max_open_stores)
            .time_to_idle(config.idle_timeout)
            .eviction_listener(|key: Arc<PartitionKey>, _store, cause| {
                let (topic, partition) = key.as_ref();
                debug!(
                    topic = %topic,
                    partition,
                    cause = ?cause,
                    "Partition store evicted from pool"
                );
                super::metrics::PARTITION_STORE_POOL_EVICTIONS.inc();
            })
            .build();

        info!(
            max_stores = config.max_open_stores,
            idle_timeout_secs = config.idle_timeout.as_secs(),
            "Partition store pool initialized"
        );

        Self {
            cache,
            object_store,
            config,
            zombie_state,
        }
    }

    /// Create a pool from cluster configuration.
    pub fn from_cluster_config(
        config: &ClusterConfig,
        object_store: Arc<dyn ObjectStore>,
        zombie_state: Arc<ZombieModeState>,
    ) -> Self {
        Self::new(
            PoolConfig::from_cluster_config(config),
            object_store,
            zombie_state,
        )
    }

    /// Get a partition store from the pool, opening it if necessary.
    ///
    /// This is the primary way to access partition stores. If the store is
    /// already in the pool, it's returned immediately (cache hit). Otherwise,
    /// the store is opened and added to the pool (cache miss).
    ///
    /// # Arguments
    /// * `topic` - Topic name
    /// * `partition` - Partition index
    /// * `leader_epoch` - Leader epoch for fencing (pass 0 to disable)
    ///
    /// # Errors
    /// Returns an error if the store cannot be opened (e.g., fenced, I/O error).
    pub async fn get_or_open(
        &self,
        topic: &str,
        partition: i32,
        leader_epoch: i32,
    ) -> SlateDBResult<Arc<PartitionStore>> {
        let key: PartitionKey = (Arc::from(topic), partition);

        // Try to get from cache first
        if let Some(store) = self.cache.get(&key).await {
            super::metrics::PARTITION_STORE_POOL_HITS.inc();
            return Ok(store);
        }

        // Cache miss - need to open the store
        super::metrics::PARTITION_STORE_POOL_MISSES.inc();

        let store = self.open_store(topic, partition, leader_epoch).await?;
        let store = Arc::new(store);

        // Insert into cache
        self.cache.insert(key, store.clone()).await;

        // Update pool size metric
        super::metrics::PARTITION_STORE_POOL_SIZE.set(self.cache.entry_count() as i64);

        Ok(store)
    }

    /// Open a new partition store.
    async fn open_store(
        &self,
        topic: &str,
        partition: i32,
        leader_epoch: i32,
    ) -> SlateDBResult<PartitionStore> {
        debug!(topic, partition, leader_epoch, "Opening partition store");

        PartitionStore::builder()
            .object_store(self.object_store.clone())
            .base_path(&self.config.base_path)
            .topic(topic)
            .partition(partition)
            .max_fetch_response_size(self.config.max_fetch_response_size)
            .producer_state_cache_ttl_secs(self.config.producer_state_cache_ttl_secs)
            .zombie_mode(self.zombie_state.clone())
            .fail_on_recovery_gap(self.config.fail_on_recovery_gap)
            .min_lease_ttl_for_write_secs(self.config.min_lease_ttl_for_write_secs)
            .leader_epoch(leader_epoch)
            .slatedb_max_unflushed_bytes(self.config.slatedb_max_unflushed_bytes)
            .slatedb_l0_sst_size_bytes(self.config.slatedb_l0_sst_size_bytes)
            .slatedb_flush_interval_ms(self.config.slatedb_flush_interval_ms)
            .build()
            .await
    }

    /// Check if a store is currently in the pool.
    pub async fn contains(&self, topic: &str, partition: i32) -> bool {
        let key: PartitionKey = (Arc::from(topic), partition);
        self.cache.contains_key(&key)
    }

    /// Remove a store from the pool.
    ///
    /// This should be called when ownership of a partition is released.
    /// The store will be closed when dropped.
    pub async fn remove(&self, topic: &str, partition: i32) {
        let key: PartitionKey = (Arc::from(topic), partition);
        self.cache.remove(&key).await;
        super::metrics::PARTITION_STORE_POOL_SIZE.set(self.cache.entry_count() as i64);
    }

    /// Invalidate a store from the pool.
    ///
    /// Similar to remove, but intended for cases where the store may be
    /// in an invalid state (e.g., after fencing).
    pub async fn invalidate(&self, topic: &str, partition: i32) {
        let key: PartitionKey = (Arc::from(topic), partition);
        self.cache.invalidate(&key).await;
        super::metrics::PARTITION_STORE_POOL_SIZE.set(self.cache.entry_count() as i64);
    }

    /// Clear all stores from the pool.
    ///
    /// This should be called during shutdown or when entering zombie mode.
    pub async fn clear(&self) {
        self.cache.invalidate_all();
        self.cache.run_pending_tasks().await;
        super::metrics::PARTITION_STORE_POOL_SIZE.set(0);
        info!("Partition store pool cleared");
    }

    /// Get the current number of stores in the pool.
    pub fn size(&self) -> u64 {
        self.cache.entry_count()
    }

    /// Get the maximum capacity of the pool.
    pub fn capacity(&self) -> u64 {
        self.config.max_open_stores
    }

    /// Run pending maintenance tasks.
    ///
    /// This triggers eviction of expired entries. Normally this is done
    /// automatically, but can be called explicitly for testing or to
    /// free memory sooner.
    pub async fn run_pending_tasks(&self) {
        self.cache.run_pending_tasks().await;
    }
}

impl std::fmt::Debug for PartitionStorePool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PartitionStorePool")
            .field("size", &self.cache.entry_count())
            .field("capacity", &self.config.max_open_stores)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Full integration tests require object store setup.
    // These unit tests verify the pool's API and configuration.

    // ========================================================================
    // Constants Tests
    // ========================================================================

    #[test]
    fn test_default_constants() {
        assert_eq!(DEFAULT_MAX_OPEN_STORES, 1000);
        assert_eq!(DEFAULT_IDLE_TIMEOUT_SECS, 300); // 5 minutes
    }

    // ========================================================================
    // PoolConfig Tests
    // ========================================================================

    #[test]
    fn test_pool_config_defaults() {
        let config = PoolConfig::default();
        assert_eq!(config.max_open_stores, DEFAULT_MAX_OPEN_STORES);
        assert_eq!(
            config.idle_timeout,
            Duration::from_secs(DEFAULT_IDLE_TIMEOUT_SECS)
        );
        assert_eq!(config.max_fetch_response_size, 1024 * 1024); // 1 MB
        assert_eq!(config.producer_state_cache_ttl_secs, 900); // 15 minutes
        assert!(!config.fail_on_recovery_gap);
        assert!(config.base_path.is_empty());
    }

    #[test]
    fn test_pool_config_from_cluster_config() {
        let cluster_config = ClusterConfig {
            max_open_partition_stores: 500,
            partition_store_idle_timeout_secs: 600,
            object_store_path: "/data".to_string(),
            ..Default::default()
        };

        let pool_config = PoolConfig::from_cluster_config(&cluster_config);
        assert_eq!(pool_config.max_open_stores, 500);
        assert_eq!(pool_config.idle_timeout, Duration::from_secs(600));
        assert_eq!(pool_config.base_path, "/data");
    }

    #[test]
    fn test_pool_config_from_cluster_config_all_fields() {
        let cluster_config = ClusterConfig {
            max_open_partition_stores: 250,
            partition_store_idle_timeout_secs: 120,
            max_fetch_response_size: 512 * 1024,
            producer_state_cache_ttl_secs: 600,
            fail_on_recovery_gap: true,
            min_lease_ttl_for_write_secs: 30,
            object_store_path: "/custom/path".to_string(),
            ..Default::default()
        };

        let pool_config = PoolConfig::from_cluster_config(&cluster_config);

        assert_eq!(pool_config.max_open_stores, 250);
        assert_eq!(pool_config.idle_timeout, Duration::from_secs(120));
        assert_eq!(pool_config.max_fetch_response_size, 512 * 1024);
        assert_eq!(pool_config.producer_state_cache_ttl_secs, 600);
        assert!(pool_config.fail_on_recovery_gap);
        assert_eq!(pool_config.min_lease_ttl_for_write_secs, 30);
        assert_eq!(pool_config.base_path, "/custom/path");
    }

    #[test]
    fn test_pool_config_debug() {
        let config = PoolConfig::default();
        let debug_str = format!("{:?}", config);

        assert!(debug_str.contains("PoolConfig"));
        assert!(debug_str.contains("max_open_stores"));
        assert!(debug_str.contains("idle_timeout"));
    }

    #[test]
    fn test_pool_config_clone() {
        let config = PoolConfig {
            max_open_stores: 100,
            idle_timeout: Duration::from_secs(60),
            max_fetch_response_size: 2048,
            producer_state_cache_ttl_secs: 300,
            fail_on_recovery_gap: true,
            min_lease_ttl_for_write_secs: 15,
            base_path: "/test".to_string(),
            slatedb_max_unflushed_bytes: 128 * 1024 * 1024,
            slatedb_l0_sst_size_bytes: 32 * 1024 * 1024,
            slatedb_flush_interval_ms: 50,
        };

        let cloned = config.clone();

        assert_eq!(cloned.max_open_stores, 100);
        assert_eq!(cloned.idle_timeout, Duration::from_secs(60));
        assert_eq!(cloned.max_fetch_response_size, 2048);
        assert_eq!(cloned.producer_state_cache_ttl_secs, 300);
        assert!(cloned.fail_on_recovery_gap);
        assert_eq!(cloned.min_lease_ttl_for_write_secs, 15);
        assert_eq!(cloned.base_path, "/test");
        assert_eq!(cloned.slatedb_max_unflushed_bytes, 128 * 1024 * 1024);
        assert_eq!(cloned.slatedb_l0_sst_size_bytes, 32 * 1024 * 1024);
        assert_eq!(cloned.slatedb_flush_interval_ms, 50);
    }

    // ========================================================================
    // PartitionStorePool Tests (without object store)
    // ========================================================================

    #[tokio::test]
    async fn test_pool_creation_with_in_memory_store() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());
        let config = PoolConfig::default();

        let pool = PartitionStorePool::new(config, object_store, zombie_state);

        assert_eq!(pool.size(), 0);
        assert_eq!(pool.capacity(), DEFAULT_MAX_OPEN_STORES);
    }

    #[tokio::test]
    async fn test_pool_capacity() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());
        let config = PoolConfig {
            max_open_stores: 500,
            ..Default::default()
        };

        let pool = PartitionStorePool::new(config, object_store, zombie_state);

        assert_eq!(pool.capacity(), 500);
    }

    #[tokio::test]
    async fn test_pool_debug() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());
        let config = PoolConfig::default();

        let pool = PartitionStorePool::new(config, object_store, zombie_state);
        let debug_str = format!("{:?}", pool);

        assert!(debug_str.contains("PartitionStorePool"));
        assert!(debug_str.contains("size"));
        assert!(debug_str.contains("capacity"));
    }

    #[tokio::test]
    async fn test_pool_contains_empty() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());
        let config = PoolConfig::default();

        let pool = PartitionStorePool::new(config, object_store, zombie_state);

        // Empty pool should not contain any partitions
        assert!(!pool.contains("test-topic", 0).await);
        assert!(!pool.contains("nonexistent", 99).await);
    }

    #[tokio::test]
    async fn test_pool_remove_nonexistent() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());
        let config = PoolConfig::default();

        let pool = PartitionStorePool::new(config, object_store, zombie_state);

        // Removing non-existent should not panic
        pool.remove("test-topic", 0).await;
        assert_eq!(pool.size(), 0);
    }

    #[tokio::test]
    async fn test_pool_invalidate_nonexistent() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());
        let config = PoolConfig::default();

        let pool = PartitionStorePool::new(config, object_store, zombie_state);

        // Invalidating non-existent should not panic
        pool.invalidate("test-topic", 0).await;
        assert_eq!(pool.size(), 0);
    }

    #[tokio::test]
    async fn test_pool_clear_empty() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());
        let config = PoolConfig::default();

        let pool = PartitionStorePool::new(config, object_store, zombie_state);

        // Clearing empty pool should not panic
        pool.clear().await;
        assert_eq!(pool.size(), 0);
    }

    #[tokio::test]
    async fn test_pool_run_pending_tasks() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());
        let config = PoolConfig::default();

        let pool = PartitionStorePool::new(config, object_store, zombie_state);

        // Running pending tasks should not panic
        pool.run_pending_tasks().await;
    }

    #[tokio::test]
    async fn test_pool_from_cluster_config() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());

        let cluster_config = ClusterConfig {
            max_open_partition_stores: 200,
            ..Default::default()
        };

        let pool =
            PartitionStorePool::from_cluster_config(&cluster_config, object_store, zombie_state);

        assert_eq!(pool.capacity(), 200);
        assert_eq!(pool.size(), 0);
    }

    // ========================================================================
    // PoolConfig Edge Cases
    // ========================================================================

    #[test]
    fn test_pool_config_small_values() {
        let config = PoolConfig {
            max_open_stores: 1,
            idle_timeout: Duration::from_secs(1),
            max_fetch_response_size: 1,
            producer_state_cache_ttl_secs: 1,
            fail_on_recovery_gap: false,
            min_lease_ttl_for_write_secs: 1,
            base_path: String::new(),
            slatedb_max_unflushed_bytes: 1,
            slatedb_l0_sst_size_bytes: 1,
            slatedb_flush_interval_ms: 1,
        };

        assert_eq!(config.max_open_stores, 1);
        assert_eq!(config.idle_timeout, Duration::from_secs(1));
    }

    #[test]
    fn test_pool_config_large_values() {
        let config = PoolConfig {
            max_open_stores: u64::MAX,
            idle_timeout: Duration::from_secs(u64::MAX),
            max_fetch_response_size: usize::MAX,
            producer_state_cache_ttl_secs: u64::MAX,
            fail_on_recovery_gap: true,
            min_lease_ttl_for_write_secs: u64::MAX,
            base_path: "a".repeat(10000),
            slatedb_max_unflushed_bytes: usize::MAX,
            slatedb_l0_sst_size_bytes: usize::MAX,
            slatedb_flush_interval_ms: u64::MAX,
        };

        assert_eq!(config.max_open_stores, u64::MAX);
        assert_eq!(config.base_path.len(), 10000);
    }

    #[test]
    fn test_pool_config_zero_timeout() {
        let config = PoolConfig {
            idle_timeout: Duration::ZERO,
            ..Default::default()
        };

        assert_eq!(config.idle_timeout, Duration::ZERO);
    }

    // ========================================================================
    // PartitionKey Tests
    // ========================================================================

    #[test]
    fn test_partition_key_creation() {
        let key: PartitionKey = (Arc::from("test-topic"), 0);

        assert_eq!(key.0.as_ref(), "test-topic");
        assert_eq!(key.1, 0);
    }

    #[test]
    fn test_partition_key_equality() {
        let key1: PartitionKey = (Arc::from("topic"), 5);
        let key2: PartitionKey = (Arc::from("topic"), 5);
        let key3: PartitionKey = (Arc::from("topic"), 6);
        let key4: PartitionKey = (Arc::from("other"), 5);

        assert_eq!(key1, key2);
        assert_ne!(key1, key3);
        assert_ne!(key1, key4);
    }

    #[test]
    fn test_partition_key_hash() {
        use std::collections::HashMap;

        let mut map: HashMap<PartitionKey, i32> = HashMap::new();
        let key1: PartitionKey = (Arc::from("topic"), 0);
        let key2: PartitionKey = (Arc::from("topic"), 1);

        map.insert(key1.clone(), 100);
        map.insert(key2.clone(), 200);

        assert_eq!(map.get(&key1), Some(&100));
        assert_eq!(map.get(&key2), Some(&200));
    }

    // ========================================================================
    // Pool Size Tracking Tests
    // ========================================================================

    #[tokio::test]
    async fn test_pool_initial_size_zero() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());

        let pool = PartitionStorePool::new(PoolConfig::default(), object_store, zombie_state);

        assert_eq!(pool.size(), 0);
    }

    #[tokio::test]
    async fn test_pool_multiple_removes_same_key() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());

        let pool = PartitionStorePool::new(PoolConfig::default(), object_store, zombie_state);

        // Multiple removes of the same non-existent key should be safe
        for _ in 0..10 {
            pool.remove("topic", 0).await;
        }

        assert_eq!(pool.size(), 0);
    }

    #[tokio::test]
    async fn test_pool_multiple_invalidates_same_key() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());

        let pool = PartitionStorePool::new(PoolConfig::default(), object_store, zombie_state);

        // Multiple invalidates of the same non-existent key should be safe
        for _ in 0..10 {
            pool.invalidate("topic", 0).await;
        }

        assert_eq!(pool.size(), 0);
    }

    #[tokio::test]
    async fn test_pool_concurrent_operations_on_empty() {
        let object_store = Arc::new(object_store::memory::InMemory::new());
        let zombie_state = Arc::new(ZombieModeState::new());

        let pool = Arc::new(PartitionStorePool::new(
            PoolConfig::default(),
            object_store,
            zombie_state,
        ));

        // Concurrent operations on empty pool should not deadlock
        let pool1 = pool.clone();
        let pool2 = pool.clone();
        let pool3 = pool.clone();

        let h1 = tokio::spawn(async move {
            for i in 0..10 {
                pool1.contains("topic", i).await;
            }
        });

        let h2 = tokio::spawn(async move {
            for i in 0..10 {
                pool2.remove("topic", i).await;
            }
        });

        let h3 = tokio::spawn(async move {
            for _ in 0..5 {
                pool3.clear().await;
            }
        });

        h1.await.unwrap();
        h2.await.unwrap();
        h3.await.unwrap();

        assert_eq!(pool.size(), 0);
    }
}
