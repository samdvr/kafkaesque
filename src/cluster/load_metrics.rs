//! Load metrics collection for auto-balancing.
//!
//! This module collects and aggregates throughput metrics per partition and broker
//! to enable intelligent load-based partition assignment. Metrics are tracked in
//! rolling windows to calculate real-time throughput rates.
//!
//! # Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │                  LoadMetricsCollector                    │
//! │                                                          │
//! │  ┌─────────────────────────────────────────────────────┐ │
//! │  │ DashMap<(topic, partition), PartitionLoadMetrics>   │ │
//! │  │                                                      │ │
//! │  │  record_produce() ──▶ atomic increment bytes_in     │ │
//! │  │  record_fetch()   ──▶ atomic increment bytes_out    │ │
//! │  └─────────────────────────────────────────────────────┘ │
//! │                           │                              │
//! │                           ▼                              │
//! │  ┌─────────────────────────────────────────────────────┐ │
//! │  │ aggregate_broker_loads(partition_owners)             │ │
//! │  │                                                      │ │
//! │  │  ──▶ HashMap<broker_id, BrokerLoadSummary>          │ │
//! │  └─────────────────────────────────────────────────────┘ │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! # Usage
//!
//! ```rust,ignore
//! let collector = LoadMetricsCollector::new(Duration::from_secs(60));
//!
//! // Record produce/fetch operations
//! collector.record_produce("my-topic", 0, 1024, 10);
//! collector.record_fetch("my-topic", 0, 2048, 20);
//!
//! // Get partition load (bytes/sec)
//! let load = collector.get_partition_load("my-topic", 0);
//!
//! // Aggregate for rebalancing decisions
//! let broker_loads = collector.aggregate_broker_loads(&partition_owners);
//! ```

use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};

/// Metrics for a single partition.
///
/// All counters are atomic for lock-free concurrent access from
/// multiple produce/fetch handlers.
#[derive(Debug)]
pub struct PartitionLoadMetrics {
    /// Total bytes produced in current window.
    bytes_in: AtomicU64,
    /// Total bytes fetched in current window.
    bytes_out: AtomicU64,
    /// Total messages produced in current window.
    messages_in: AtomicU64,
    /// Total messages fetched in current window.
    messages_out: AtomicU64,
    /// Window start time (Unix millis).
    window_start_ms: AtomicU64,
}

impl Default for PartitionLoadMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl PartitionLoadMetrics {
    /// Create new partition metrics with current time as window start.
    pub fn new() -> Self {
        Self {
            bytes_in: AtomicU64::new(0),
            bytes_out: AtomicU64::new(0),
            messages_in: AtomicU64::new(0),
            messages_out: AtomicU64::new(0),
            window_start_ms: AtomicU64::new(current_time_ms()),
        }
    }

    /// Record a produce operation.
    pub fn record_produce(&self, bytes: u64, messages: u64) {
        self.bytes_in.fetch_add(bytes, Ordering::Relaxed);
        self.messages_in.fetch_add(messages, Ordering::Relaxed);
    }

    /// Record a fetch operation.
    pub fn record_fetch(&self, bytes: u64, messages: u64) {
        self.bytes_out.fetch_add(bytes, Ordering::Relaxed);
        self.messages_out.fetch_add(messages, Ordering::Relaxed);
    }

    /// Get total bytes (in + out).
    pub fn total_bytes(&self) -> u64 {
        self.bytes_in.load(Ordering::Relaxed) + self.bytes_out.load(Ordering::Relaxed)
    }

    /// Get total messages (in + out).
    pub fn total_messages(&self) -> u64 {
        self.messages_in.load(Ordering::Relaxed) + self.messages_out.load(Ordering::Relaxed)
    }

    /// Calculate load as bytes per second.
    pub fn calculate_load(&self, window_duration: Duration) -> f64 {
        let duration_secs = window_duration.as_secs_f64();
        if duration_secs <= 0.0 {
            return 0.0;
        }
        self.total_bytes() as f64 / duration_secs
    }

    /// Calculate message rate (messages per second).
    pub fn calculate_message_rate(&self, window_duration: Duration) -> f64 {
        let duration_secs = window_duration.as_secs_f64();
        if duration_secs <= 0.0 {
            return 0.0;
        }
        self.total_messages() as f64 / duration_secs
    }

    /// Get bytes in (produce).
    pub fn bytes_in(&self) -> u64 {
        self.bytes_in.load(Ordering::Relaxed)
    }

    /// Get bytes out (fetch).
    pub fn bytes_out(&self) -> u64 {
        self.bytes_out.load(Ordering::Relaxed)
    }

    /// Get messages in (produce).
    pub fn messages_in(&self) -> u64 {
        self.messages_in.load(Ordering::Relaxed)
    }

    /// Get messages out (fetch).
    pub fn messages_out(&self) -> u64 {
        self.messages_out.load(Ordering::Relaxed)
    }

    /// Reset all counters and update window start.
    pub fn reset(&self) {
        self.bytes_in.store(0, Ordering::Relaxed);
        self.bytes_out.store(0, Ordering::Relaxed);
        self.messages_in.store(0, Ordering::Relaxed);
        self.messages_out.store(0, Ordering::Relaxed);
        self.window_start_ms
            .store(current_time_ms(), Ordering::Relaxed);
    }

    /// Get window start time.
    pub fn window_start_ms(&self) -> u64 {
        self.window_start_ms.load(Ordering::Relaxed)
    }

    /// Get snapshot of current metrics.
    pub fn snapshot(&self) -> PartitionLoadSnapshot {
        PartitionLoadSnapshot {
            bytes_in: self.bytes_in.load(Ordering::Relaxed),
            bytes_out: self.bytes_out.load(Ordering::Relaxed),
            messages_in: self.messages_in.load(Ordering::Relaxed),
            messages_out: self.messages_out.load(Ordering::Relaxed),
            window_start_ms: self.window_start_ms.load(Ordering::Relaxed),
        }
    }
}

/// Snapshot of partition load metrics (non-atomic, for reporting).
#[derive(Debug, Clone)]
pub struct PartitionLoadSnapshot {
    pub bytes_in: u64,
    pub bytes_out: u64,
    pub messages_in: u64,
    pub messages_out: u64,
    pub window_start_ms: u64,
}

impl PartitionLoadSnapshot {
    /// Calculate load as bytes per second.
    pub fn calculate_load(&self, window_duration: Duration) -> f64 {
        let duration_secs = window_duration.as_secs_f64();
        if duration_secs <= 0.0 {
            return 0.0;
        }
        (self.bytes_in + self.bytes_out) as f64 / duration_secs
    }
}

/// Aggregated load metrics for a broker.
#[derive(Debug, Clone)]
pub struct BrokerLoadSummary {
    /// Broker ID.
    pub broker_id: i32,
    /// Number of partitions owned by this broker.
    pub partition_count: usize,
    /// Total bytes per second (all partitions).
    pub total_bytes_per_sec: f64,
    /// Total messages per second (all partitions).
    pub total_messages_per_sec: f64,
    /// Normalized load score (0.0 to 1.0).
    pub load_score: f64,
    /// Individual partition loads for this broker.
    pub partitions: Vec<PartitionLoadInfo>,
}

/// Load info for a specific partition.
#[derive(Debug, Clone)]
pub struct PartitionLoadInfo {
    pub topic: String,
    pub partition: i32,
    pub bytes_per_sec: f64,
    pub messages_per_sec: f64,
}

/// Configuration for the load metrics collector.
#[derive(Debug, Clone)]
pub struct LoadMetricsConfig {
    /// Duration of the rolling window for rate calculation.
    pub window_duration: Duration,
    /// How often to export metrics to Prometheus.
    pub export_interval: Duration,
    /// Maximum number of partitions to track (for memory limits).
    pub max_tracked_partitions: usize,
}

impl Default for LoadMetricsConfig {
    fn default() -> Self {
        Self {
            window_duration: Duration::from_secs(60),
            export_interval: Duration::from_secs(15),
            max_tracked_partitions: 100_000,
        }
    }
}

/// Load metrics collector for the cluster.
///
/// This collector tracks throughput per partition and provides aggregated
/// views for rebalancing decisions.
pub struct LoadMetricsCollector {
    /// Per-partition metrics.
    partitions: DashMap<(String, i32), PartitionLoadMetrics>,
    /// Configuration.
    config: LoadMetricsConfig,
    /// Collector creation time.
    created_at: Instant,
    /// Last reset time.
    last_reset: AtomicU64,
}

impl LoadMetricsCollector {
    /// Create a new collector with the given window duration.
    pub fn new(window_duration: Duration) -> Self {
        Self {
            partitions: DashMap::new(),
            config: LoadMetricsConfig {
                window_duration,
                ..Default::default()
            },
            created_at: Instant::now(),
            last_reset: AtomicU64::new(current_time_ms()),
        }
    }

    /// Create a new collector with full configuration.
    pub fn with_config(config: LoadMetricsConfig) -> Self {
        Self {
            partitions: DashMap::new(),
            config,
            created_at: Instant::now(),
            last_reset: AtomicU64::new(current_time_ms()),
        }
    }

    pub fn config(&self) -> &LoadMetricsConfig {
        &self.config
    }

    pub fn window_duration(&self) -> Duration {
        self.config.window_duration
    }

    /// Record a produce operation for a partition.
    pub fn record_produce(&self, topic: &str, partition: i32, bytes: u64, messages: u64) {
        // Check partition limit
        if self.partitions.len() >= self.config.max_tracked_partitions {
            // Just don't track new partitions if at limit
            if !self
                .partitions
                .contains_key(&(topic.to_string(), partition))
            {
                return;
            }
        }

        let key = (topic.to_string(), partition);
        self.partitions
            .entry(key)
            .or_default()
            .record_produce(bytes, messages);
    }

    /// Record a fetch operation for a partition.
    pub fn record_fetch(&self, topic: &str, partition: i32, bytes: u64, messages: u64) {
        // Check partition limit
        if self.partitions.len() >= self.config.max_tracked_partitions
            && !self
                .partitions
                .contains_key(&(topic.to_string(), partition))
        {
            return;
        }

        let key = (topic.to_string(), partition);
        self.partitions
            .entry(key)
            .or_default()
            .record_fetch(bytes, messages);
    }

    /// Get load for a specific partition (bytes/sec).
    pub fn get_partition_load(&self, topic: &str, partition: i32) -> f64 {
        let key = (topic.to_string(), partition);
        self.partitions
            .get(&key)
            .map(|m| m.calculate_load(self.config.window_duration))
            .unwrap_or(0.0)
    }

    /// Get snapshot of a specific partition's metrics.
    pub fn get_partition_snapshot(
        &self,
        topic: &str,
        partition: i32,
    ) -> Option<PartitionLoadSnapshot> {
        let key = (topic.to_string(), partition);
        self.partitions.get(&key).map(|m| m.snapshot())
    }

    /// Get all partition loads as a map.
    pub fn get_all_partition_loads(&self) -> HashMap<(String, i32), f64> {
        self.partitions
            .iter()
            .map(|entry| {
                let key = entry.key().clone();
                let load = entry.value().calculate_load(self.config.window_duration);
                (key, load)
            })
            .collect()
    }

    /// Aggregate load metrics per broker.
    ///
    /// Takes a mapping of (topic, partition) -> broker_id to determine which
    /// broker owns each partition.
    pub fn aggregate_broker_loads(
        &self,
        partition_owners: &HashMap<(String, i32), i32>,
    ) -> HashMap<i32, BrokerLoadSummary> {
        let window = self.config.window_duration;

        // Collect per-broker partition info
        let mut broker_partitions: HashMap<i32, Vec<PartitionLoadInfo>> = HashMap::new();

        for entry in self.partitions.iter() {
            let (topic, partition) = entry.key();
            if let Some(&broker_id) = partition_owners.get(&(topic.clone(), *partition)) {
                let metrics = entry.value();
                let bytes_per_sec = metrics.calculate_load(window);
                let messages_per_sec = metrics.calculate_message_rate(window);

                broker_partitions
                    .entry(broker_id)
                    .or_default()
                    .push(PartitionLoadInfo {
                        topic: topic.clone(),
                        partition: *partition,
                        bytes_per_sec,
                        messages_per_sec,
                    });
            }
        }

        // Calculate max load for normalization
        // Use total_cmp instead of partial_cmp to handle NaN safely.
        // f64::total_cmp provides a total ordering that handles NaN values without panic.
        let max_load: f64 = broker_partitions
            .values()
            .map(|partitions| partitions.iter().map(|p| p.bytes_per_sec).sum::<f64>())
            .max_by(|a, b| a.total_cmp(b))
            .unwrap_or(1.0)
            .max(1.0);

        // Build broker summaries
        broker_partitions
            .into_iter()
            .map(|(broker_id, partitions)| {
                let partition_count = partitions.len();
                let total_bytes_per_sec: f64 = partitions.iter().map(|p| p.bytes_per_sec).sum();
                let total_messages_per_sec: f64 =
                    partitions.iter().map(|p| p.messages_per_sec).sum();
                let load_score = total_bytes_per_sec / max_load;

                (
                    broker_id,
                    BrokerLoadSummary {
                        broker_id,
                        partition_count,
                        total_bytes_per_sec,
                        total_messages_per_sec,
                        load_score,
                        partitions,
                    },
                )
            })
            .collect()
    }

    /// Reset all partition metrics.
    ///
    /// This should be called at window boundaries to start fresh counters.
    pub fn reset_all(&self) {
        for entry in self.partitions.iter() {
            entry.value().reset();
        }
        self.last_reset.store(current_time_ms(), Ordering::Relaxed);
    }

    /// Clear metrics for a specific partition.
    pub fn clear_partition(&self, topic: &str, partition: i32) {
        let key = (topic.to_string(), partition);
        self.partitions.remove(&key);
    }

    pub fn partition_count(&self) -> usize {
        self.partitions.len()
    }

    pub fn uptime(&self) -> Duration {
        self.created_at.elapsed()
    }

    pub fn time_since_reset(&self) -> Duration {
        let last = self.last_reset.load(Ordering::Relaxed);
        let now = current_time_ms();
        Duration::from_millis(now.saturating_sub(last))
    }

    /// Check if the window has elapsed and a reset is due.
    pub fn should_reset(&self) -> bool {
        self.time_since_reset() >= self.config.window_duration
    }
}

impl Default for LoadMetricsCollector {
    fn default() -> Self {
        Self::new(Duration::from_secs(60))
    }
}

/// Get current time in milliseconds since UNIX epoch.
///
/// Use unwrap_or_default instead of unwrap to handle the edge case
/// where system time is before UNIX epoch (theoretically possible on misconfigured
/// systems, returns 0 instead of panicking).
fn current_time_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_partition_metrics_produce() {
        let metrics = PartitionLoadMetrics::new();
        metrics.record_produce(1000, 10);
        metrics.record_produce(500, 5);

        assert_eq!(metrics.bytes_in(), 1500);
        assert_eq!(metrics.messages_in(), 15);
        assert_eq!(metrics.bytes_out(), 0);
        assert_eq!(metrics.total_bytes(), 1500);
    }

    #[test]
    fn test_partition_metrics_fetch() {
        let metrics = PartitionLoadMetrics::new();
        metrics.record_fetch(2000, 20);

        assert_eq!(metrics.bytes_out(), 2000);
        assert_eq!(metrics.messages_out(), 20);
        assert_eq!(metrics.bytes_in(), 0);
        assert_eq!(metrics.total_bytes(), 2000);
    }

    #[test]
    fn test_partition_metrics_load_calculation() {
        let metrics = PartitionLoadMetrics::new();
        metrics.record_produce(60_000, 60); // 60KB in 1 minute = 1KB/s

        let load = metrics.calculate_load(Duration::from_secs(60));
        assert!((load - 1000.0).abs() < 0.01); // ~1000 bytes/sec
    }

    #[test]
    fn test_partition_metrics_reset() {
        let metrics = PartitionLoadMetrics::new();
        metrics.record_produce(1000, 10);
        assert_eq!(metrics.bytes_in(), 1000);

        metrics.reset();
        assert_eq!(metrics.bytes_in(), 0);
        assert_eq!(metrics.messages_in(), 0);
    }

    #[test]
    fn test_collector_record_produce() {
        let collector = LoadMetricsCollector::new(Duration::from_secs(60));
        collector.record_produce("test-topic", 0, 1024, 10);
        collector.record_produce("test-topic", 0, 2048, 20);

        let snapshot = collector.get_partition_snapshot("test-topic", 0).unwrap();
        assert_eq!(snapshot.bytes_in, 3072);
        assert_eq!(snapshot.messages_in, 30);
    }

    #[test]
    fn test_collector_record_fetch() {
        let collector = LoadMetricsCollector::new(Duration::from_secs(60));
        collector.record_fetch("test-topic", 0, 4096, 40);

        let snapshot = collector.get_partition_snapshot("test-topic", 0).unwrap();
        assert_eq!(snapshot.bytes_out, 4096);
        assert_eq!(snapshot.messages_out, 40);
    }

    #[test]
    fn test_collector_multiple_partitions() {
        let collector = LoadMetricsCollector::new(Duration::from_secs(60));

        collector.record_produce("topic-a", 0, 1000, 10);
        collector.record_produce("topic-a", 1, 2000, 20);
        collector.record_produce("topic-b", 0, 3000, 30);

        assert_eq!(collector.partition_count(), 3);
    }

    #[test]
    fn test_broker_load_aggregation() {
        let collector = LoadMetricsCollector::new(Duration::from_secs(1));

        // Broker 1 owns partitions with more load
        collector.record_produce("topic", 0, 60_000, 60);
        collector.record_produce("topic", 1, 40_000, 40);

        // Broker 2 owns partitions with less load
        collector.record_produce("topic", 2, 10_000, 10);

        let mut partition_owners = HashMap::new();
        partition_owners.insert(("topic".to_string(), 0), 1);
        partition_owners.insert(("topic".to_string(), 1), 1);
        partition_owners.insert(("topic".to_string(), 2), 2);

        let broker_loads = collector.aggregate_broker_loads(&partition_owners);

        assert_eq!(broker_loads.len(), 2);

        let broker1 = broker_loads.get(&1).unwrap();
        assert_eq!(broker1.partition_count, 2);
        assert!(broker1.total_bytes_per_sec > 0.0);

        let broker2 = broker_loads.get(&2).unwrap();
        assert_eq!(broker2.partition_count, 1);

        // Broker 1 should have higher load score
        assert!(broker1.load_score > broker2.load_score);
    }

    #[test]
    fn test_collector_reset_all() {
        let collector = LoadMetricsCollector::new(Duration::from_secs(60));

        collector.record_produce("topic", 0, 1000, 10);
        collector.record_fetch("topic", 1, 2000, 20);

        collector.reset_all();

        let snap0 = collector.get_partition_snapshot("topic", 0).unwrap();
        let snap1 = collector.get_partition_snapshot("topic", 1).unwrap();

        assert_eq!(snap0.bytes_in, 0);
        assert_eq!(snap1.bytes_out, 0);
    }

    #[test]
    fn test_collector_clear_partition() {
        let collector = LoadMetricsCollector::new(Duration::from_secs(60));

        collector.record_produce("topic", 0, 1000, 10);
        assert_eq!(collector.partition_count(), 1);

        collector.clear_partition("topic", 0);
        assert_eq!(collector.partition_count(), 0);
        assert!(collector.get_partition_snapshot("topic", 0).is_none());
    }

    #[test]
    fn test_config_defaults() {
        let config = LoadMetricsConfig::default();
        assert_eq!(config.window_duration, Duration::from_secs(60));
        assert_eq!(config.max_tracked_partitions, 100_000);
    }

    #[test]
    fn test_partition_snapshot_calculate_load() {
        let snapshot = PartitionLoadSnapshot {
            bytes_in: 30_000,
            bytes_out: 30_000,
            messages_in: 100,
            messages_out: 100,
            window_start_ms: 0,
        };

        let load = snapshot.calculate_load(Duration::from_secs(60));
        assert!((load - 1000.0).abs() < 0.01); // 60KB / 60s = 1KB/s

        // Test zero duration
        let load_zero = snapshot.calculate_load(Duration::ZERO);
        assert_eq!(load_zero, 0.0);
    }

    #[test]
    fn test_get_partition_load() {
        let collector = LoadMetricsCollector::new(Duration::from_secs(1));
        collector.record_produce("topic", 0, 1000, 10);

        let load = collector.get_partition_load("topic", 0);
        assert!(load > 0.0);

        // Non-existent partition returns 0
        let load_missing = collector.get_partition_load("other", 99);
        assert_eq!(load_missing, 0.0);
    }

    #[test]
    fn test_get_all_partition_loads() {
        let collector = LoadMetricsCollector::new(Duration::from_secs(1));
        collector.record_produce("topic-a", 0, 1000, 10);
        collector.record_produce("topic-b", 1, 2000, 20);

        let loads = collector.get_all_partition_loads();
        assert_eq!(loads.len(), 2);
        assert!(loads.contains_key(&("topic-a".to_string(), 0)));
        assert!(loads.contains_key(&("topic-b".to_string(), 1)));
    }

    #[test]
    fn test_uptime() {
        let collector = LoadMetricsCollector::new(Duration::from_secs(60));
        std::thread::sleep(Duration::from_millis(10));
        assert!(collector.uptime() >= Duration::from_millis(10));
    }

    #[test]
    fn test_time_since_reset() {
        let collector = LoadMetricsCollector::new(Duration::from_secs(60));
        std::thread::sleep(Duration::from_millis(10));
        assert!(collector.time_since_reset() >= Duration::from_millis(10));
    }

    #[test]
    fn test_should_reset() {
        let collector = LoadMetricsCollector::new(Duration::from_millis(10));
        std::thread::sleep(Duration::from_millis(15));
        assert!(collector.should_reset());
    }

    #[test]
    fn test_max_tracked_partitions_limit() {
        let config = LoadMetricsConfig {
            max_tracked_partitions: 3,
            ..Default::default()
        };
        let collector = LoadMetricsCollector::with_config(config);

        // Add 3 partitions
        collector.record_produce("topic", 0, 100, 1);
        collector.record_produce("topic", 1, 100, 1);
        collector.record_produce("topic", 2, 100, 1);

        assert_eq!(collector.partition_count(), 3);

        // Try to add a 4th - should be silently ignored
        collector.record_produce("topic", 3, 100, 1);
        assert_eq!(collector.partition_count(), 3);

        // But existing partitions should still be updated
        collector.record_produce("topic", 0, 100, 1);
        let snapshot = collector.get_partition_snapshot("topic", 0).unwrap();
        assert_eq!(snapshot.bytes_in, 200);
    }

    #[test]
    fn test_fetch_max_partitions_limit() {
        let config = LoadMetricsConfig {
            max_tracked_partitions: 2,
            ..Default::default()
        };
        let collector = LoadMetricsCollector::with_config(config);

        collector.record_fetch("topic", 0, 100, 1);
        collector.record_fetch("topic", 1, 100, 1);

        assert_eq!(collector.partition_count(), 2);

        // Try to add a 3rd via fetch - should be ignored
        collector.record_fetch("topic", 2, 100, 1);
        assert_eq!(collector.partition_count(), 2);
    }

    #[test]
    fn test_default_collector() {
        let collector = LoadMetricsCollector::default();
        assert_eq!(collector.window_duration(), Duration::from_secs(60));
    }

    #[test]
    fn test_config_accessor() {
        let config = LoadMetricsConfig {
            window_duration: Duration::from_secs(120),
            ..Default::default()
        };
        let collector = LoadMetricsCollector::with_config(config);
        assert_eq!(collector.config().window_duration, Duration::from_secs(120));
    }

    #[test]
    fn test_partition_metrics_message_rate() {
        let metrics = PartitionLoadMetrics::new();
        metrics.record_produce(0, 60);

        let rate = metrics.calculate_message_rate(Duration::from_secs(60));
        assert!((rate - 1.0).abs() < 0.01); // 60 messages / 60s = 1 msg/s

        // Test zero duration
        let rate_zero = metrics.calculate_message_rate(Duration::ZERO);
        assert_eq!(rate_zero, 0.0);
    }

    #[test]
    fn test_partition_metrics_zero_duration_load() {
        let metrics = PartitionLoadMetrics::new();
        metrics.record_produce(1000, 10);

        let load = metrics.calculate_load(Duration::ZERO);
        assert_eq!(load, 0.0);
    }

    #[test]
    fn test_partition_metrics_window_start() {
        let metrics = PartitionLoadMetrics::new();
        let window_start = metrics.window_start_ms();
        assert!(window_start > 0);

        // After reset, window_start should be updated
        std::thread::sleep(Duration::from_millis(5));
        metrics.reset();
        let new_window_start = metrics.window_start_ms();
        assert!(new_window_start > window_start);
    }

    #[test]
    fn test_aggregate_with_unmapped_partitions() {
        let collector = LoadMetricsCollector::new(Duration::from_secs(1));

        // Record data for some partitions
        collector.record_produce("topic", 0, 1000, 10);
        collector.record_produce("topic", 1, 2000, 20);
        collector.record_produce("topic", 2, 3000, 30);

        // Only map some partitions to brokers
        let mut partition_owners = HashMap::new();
        partition_owners.insert(("topic".to_string(), 0), 1);
        // Partitions 1 and 2 are unmapped

        let broker_loads = collector.aggregate_broker_loads(&partition_owners);

        // Only broker 1 should appear
        assert_eq!(broker_loads.len(), 1);
        assert!(broker_loads.contains_key(&1));
    }

    #[test]
    fn test_partition_load_info_fields() {
        let info = PartitionLoadInfo {
            topic: "test".to_string(),
            partition: 5,
            bytes_per_sec: 1000.0,
            messages_per_sec: 10.0,
        };

        assert_eq!(info.topic, "test");
        assert_eq!(info.partition, 5);
        assert_eq!(info.bytes_per_sec, 1000.0);
        assert_eq!(info.messages_per_sec, 10.0);
    }

    #[test]
    fn test_broker_load_summary_fields() {
        let summary = BrokerLoadSummary {
            broker_id: 1,
            partition_count: 5,
            total_bytes_per_sec: 5000.0,
            total_messages_per_sec: 50.0,
            load_score: 0.8,
            partitions: vec![],
        };

        assert_eq!(summary.broker_id, 1);
        assert_eq!(summary.partition_count, 5);
        assert_eq!(summary.total_bytes_per_sec, 5000.0);
        assert_eq!(summary.load_score, 0.8);
    }
}
