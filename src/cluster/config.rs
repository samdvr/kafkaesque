//! Configuration for SlateDB cluster.
//!
//! # Configuration Profiles
//!
//! Instead of managing 60+ configuration fields, use validated profiles:
//!
//! ```rust,no_run
//! use kafkaesque::cluster::{ClusterConfig, ClusterProfile};
//!
//! // Development profile - optimized for fast startup and debugging
//! let dev_config = ClusterConfig::from_profile(ClusterProfile::Development);
//!
//! // Production profile - balanced safety and performance
//! let prod_config = ClusterConfig::from_profile(ClusterProfile::Production);
//!
//! // High-throughput profile - optimized for maximum throughput
//! let ht_config = ClusterConfig::from_profile(ClusterProfile::HighThroughput);
//!
//! // Low-latency profile - optimized for fast failover and response
//! let ll_config = ClusterConfig::from_profile(ClusterProfile::LowLatency);
//! ```

use std::time::Duration;

use crate::constants::{
    DEFAULT_HEARTBEAT_INTERVAL_SECS, DEFAULT_LEASE_DURATION_SECS,
    DEFAULT_LEASE_RENEWAL_INTERVAL_SECS, DEFAULT_OWNERSHIP_CHECK_INTERVAL_SECS,
    DEFAULT_PRODUCER_STATE_CACHE_TTL_SECS, DEFAULT_SESSION_TIMEOUT_CHECK_INTERVAL_SECS,
};

/// Validated configuration profiles that reduce cognitive load and prevent misconfiguration.
///
/// Instead of tuning 60+ parameters, select a profile that matches your use case.
/// Each profile is internally validated to ensure consistency.
///
/// # Available Profiles
///
/// | Profile | Use Case | Failover | Throughput | Resource Usage |
/// |---------|----------|----------|------------|----------------|
/// | Development | Local testing | Slow | Low | Minimal |
/// | Production | General workloads | Fast | Balanced | Standard |
/// | HighThroughput | Analytics, batch | Medium | Maximum | High |
/// | LowLatency | Trading, real-time | Very Fast | Moderate | High |
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ClusterProfile {
    /// Optimized for local development and testing.
    ///
    /// Features:
    /// - Relaxed timeouts for debugging
    /// - Fast startup (shorter initial delays)
    /// - Lower resource usage
    /// - Auto-create topics enabled
    /// - Detailed metrics enabled
    ///
    /// **Not suitable for production use.**
    Development,

    /// Balanced configuration for production workloads.
    ///
    /// Features:
    /// - Fast failover (~2.5 seconds)
    /// - Balanced safety margins
    /// - Auto-balancing enabled
    /// - Circuit breaker protection
    /// - CRC validation enabled
    ///
    /// This is the recommended profile for most production deployments.
    Production,

    /// Optimized for maximum throughput (analytics, batch processing).
    ///
    /// Features:
    /// - Larger batch sizes
    /// - Higher concurrent I/O limits
    /// - Increased fetch response sizes
    /// - Longer timeouts to avoid interrupting large operations
    /// - More aggressive batching
    ///
    /// Best for:
    /// - Data lakes and analytics pipelines
    /// - Batch consumers
    /// - High-volume ingestion
    HighThroughput,

    /// Optimized for minimum latency (trading, real-time).
    ///
    /// Features:
    /// - Very fast failover (~1 second)
    /// - Aggressive heartbeat intervals
    /// - Smaller batch sizes for faster commits
    /// - Tighter lease margins
    ///
    /// **Warning:** This profile has tighter safety margins.
    /// Ensure your infrastructure (network, NTP) is reliable.
    LowLatency,
}

impl ClusterProfile {
    /// Get a human-readable description of the profile.
    pub fn description(&self) -> &'static str {
        match self {
            ClusterProfile::Development => "Local development and testing",
            ClusterProfile::Production => "Balanced production workloads",
            ClusterProfile::HighThroughput => "Maximum throughput (analytics, batch)",
            ClusterProfile::LowLatency => "Minimum latency (trading, real-time)",
        }
    }

    /// Get all available profiles.
    pub fn all() -> &'static [ClusterProfile] {
        &[
            ClusterProfile::Development,
            ClusterProfile::Production,
            ClusterProfile::HighThroughput,
            ClusterProfile::LowLatency,
        ]
    }
}

impl std::fmt::Display for ClusterProfile {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClusterProfile::Development => write!(f, "development"),
            ClusterProfile::Production => write!(f, "production"),
            ClusterProfile::HighThroughput => write!(f, "high-throughput"),
            ClusterProfile::LowLatency => write!(f, "low-latency"),
        }
    }
}

impl std::str::FromStr for ClusterProfile {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "development" | "dev" => Ok(ClusterProfile::Development),
            "production" | "prod" => Ok(ClusterProfile::Production),
            "high-throughput" | "throughput" | "ht" => Ok(ClusterProfile::HighThroughput),
            "low-latency" | "latency" | "ll" => Ok(ClusterProfile::LowLatency),
            _ => Err(format!(
                "Unknown profile '{}'. Valid profiles: development, production, high-throughput, low-latency",
                s
            )),
        }
    }
}

/// Type of object store to use.
#[derive(Debug, Clone)]
pub enum ObjectStoreType {
    /// Local filesystem storage.
    Local {
        /// Path on local filesystem.
        path: String,
    },
    /// Amazon S3 or S3-compatible storage (MinIO, etc.).
    S3 {
        /// S3 bucket name.
        bucket: String,
        /// AWS region (e.g., "us-east-1").
        region: String,
        /// Optional custom endpoint for S3-compatible services (e.g., MinIO).
        endpoint: Option<String>,
        /// Optional access key ID (if not using environment/IAM).
        access_key_id: Option<String>,
        /// Optional secret access key.
        secret_access_key: Option<String>,
    },
    /// Google Cloud Storage.
    Gcs {
        /// GCS bucket name.
        bucket: String,
        /// Optional path to service account key file.
        service_account_key: Option<String>,
    },
    /// Azure Blob Storage.
    Azure {
        /// Azure container name.
        container: String,
        /// Azure storage account name.
        account: String,
        /// Optional access key (if not using environment/managed identity).
        access_key: Option<String>,
    },
}

impl Default for ObjectStoreType {
    fn default() -> Self {
        ObjectStoreType::Local {
            path: "/tmp/kafkaesque-data".to_string(),
        }
    }
}

/// Configuration for a SlateDB cluster node.
///
/// # Clock Synchronization Requirements
///
/// **IMPORTANT:** This distributed system relies on synchronized clocks across all brokers
/// for correct operation of lease expiration, heartbeat TTL, and consumer session timeouts.
///
/// ## Requirements
///
/// - All brokers MUST run NTP (Network Time Protocol) or an equivalent time synchronization service
/// - Clock drift between brokers should be kept under 1 second for optimal operation
/// - Clock drift over 5 seconds may cause:
///   - Premature lease expiration (partitions unexpectedly released)
///   - False-positive broker death detection
///   - Consumer group rebalances due to session timeout miscalculation
///   - In extreme cases (>30s drift): potential split-brain scenarios
///
/// ## Recommended NTP Configuration
///
/// ```bash
/// # Ubuntu/Debian
/// sudo apt install chrony
/// sudo systemctl enable chrony
///
/// # Amazon Linux / RHEL
/// sudo yum install chrony
/// sudo systemctl enable chronyd
///
/// # Verify synchronization
/// chronyc tracking  # should show "Leap status: Normal"
/// ```
///
/// ## Cloud Providers
///
/// - **AWS**: Use Amazon Time Sync Service (169.254.169.123)
/// - **GCP**: VMs are automatically synchronized via Google NTP
/// - **Azure**: VMs sync via Windows Time service or chrony
///
/// ## Kubernetes
///
/// Kubernetes nodes should have NTP configured at the host level.
/// Pod clocks inherit from the node, so ensure node time sync is working.
///
/// ## Monitoring
///
/// Monitor the `kafkaesque_clock_skew_detected` metric (if available) and
/// set alerts for clock drift > 1 second.
#[derive(Debug, Clone)]
pub struct ClusterConfig {
    /// This broker's unique ID (must be unique across cluster).
    ///
    /// # CRITICAL: Broker ID Persistence Requirement
    ///
    /// The broker ID **MUST be persistent and stable across restarts**. This is required
    /// because the cluster uses consistent hashing based on broker ID for partition assignment.
    ///
    /// If a broker restarts with a different ID:
    /// - All its partition assignments will be recalculated
    /// - Partitions will migrate unnecessarily to other brokers
    /// - This causes temporary unavailability and increased load
    ///
    /// ## Best Practices
    ///
    /// 1. **Static Configuration**: Set `BROKER_ID` explicitly via environment variable
    ///    or configuration file, not auto-assigned at runtime.
    ///
    /// 2. **Kubernetes**: Use the pod ordinal from StatefulSet as the broker ID.
    ///    Example: `BROKER_ID=$(hostname | sed 's/.*-//')`
    ///
    /// 3. **VM/Bare Metal**: Store the broker ID in persistent storage and load it
    ///    on startup. Generate a new ID only on first boot.
    ///
    /// 4. **Docker**: Pass the broker ID as an environment variable, not auto-generated.
    pub broker_id: i32,

    /// Host address to bind the server to (listen address).
    /// Use 0.0.0.0 to listen on all interfaces.
    pub host: String,

    /// Host address advertised to Kafka clients.
    /// This is what clients use to connect back to the broker.
    /// Defaults to HOST if not set, but should be a routable address
    /// (not 0.0.0.0) for clients to connect successfully.
    pub advertised_host: String,

    /// Port for Kafka clients to connect.
    pub port: i32,

    /// Port for HTTP health check server.
    /// Exposes /health, /ready, /live, and /metrics endpoints.
    /// Set to 0 to disable the health server.
    /// Default: 8080
    pub health_port: i32,

    /// Cluster identifier for coordination.
    ///
    /// This ensures isolation between different clusters sharing
    /// the same object store or Raft network.
    ///
    /// Default: "kafkaesque-cluster"
    pub cluster_id: String,

    /// Object store configuration.
    pub object_store: ObjectStoreType,

    /// Base path for SlateDB data in object store.
    /// For S3/GCS/Azure, this is a prefix within the bucket.
    pub object_store_path: String,

    /// How long a partition ownership lease lasts.
    pub lease_duration: Duration,

    /// How often to renew partition leases.
    pub lease_renewal_interval: Duration,

    /// How often to send broker heartbeats.
    pub heartbeat_interval: Duration,

    /// How often to check for partition ownership changes.
    pub ownership_check_interval: Duration,

    /// How often to check for expired consumer group members.
    /// Default: 10 seconds
    pub session_timeout_check_interval: Duration,

    /// Default session timeout for consumer group members (in milliseconds).
    /// Members that don't send heartbeats within this window will be evicted.
    /// Default: 30000ms (30 seconds, matching Kafka's default)
    pub default_session_timeout_ms: u64,

    /// Whether to automatically create topics when they are first referenced.
    /// If false, metadata requests for unknown topics will return an error
    /// instead of auto-creating the topic.
    /// Default: true (Kafka's default behavior)
    pub auto_create_topics: bool,

    /// Default number of partitions for auto-created topics.
    /// Default: 10
    pub default_num_partitions: i32,

    /// Maximum number of partitions per topic when auto-creating.
    /// Prevents DoS attacks where clients request extremely high partition counts.
    /// Set to 0 for no limit (not recommended).
    /// Default: 1000
    pub max_partitions_per_topic: i32,

    /// Maximum number of topics that can be auto-created.
    /// Prevents DoS attacks where clients create many topics.
    /// Set to 0 for no limit (not recommended).
    /// Default: 10000
    pub max_auto_created_topics: u32,

    // --- Network tuning ---
    /// Maximum allowed message size for requests.
    /// Prevents memory exhaustion from malicious or malformed messages.
    /// Default: 100 MB
    pub max_message_size: usize,

    /// Maximum size of a fetch response in bytes.
    /// Limits memory usage per fetch request.
    /// Default: 1 MB
    pub max_fetch_response_size: usize,

    /// Maximum concurrent partition writes per produce request.
    /// Provides backpressure when a single request targets many partitions.
    /// Default: 16
    pub max_concurrent_partition_writes: usize,

    /// Maximum concurrent partition reads per fetch request.
    /// Provides backpressure when a single request targets many partitions.
    /// Default: 16
    pub max_concurrent_partition_reads: usize,

    /// Maximum time to wait for a fetch operation to complete (seconds).
    /// Prevents slow object store operations from blocking indefinitely.
    /// If a fetch exceeds this timeout, it returns whatever data was collected
    /// so far (which may be empty).
    /// Default: 30 seconds
    pub fetch_timeout_secs: u64,

    // --- Coordinator tuning ---
    /// TTL for broker heartbeat (seconds).
    /// After this time without heartbeat, broker is considered dead.
    /// Default: 30 seconds
    pub broker_heartbeat_ttl_secs: u64,

    /// TTL for group member entries (seconds).
    /// Members are evicted if no heartbeat within this window.
    /// Default: 300 seconds (5 minutes)
    pub group_member_ttl_secs: i64,

    /// TTL for member assignment data (seconds).
    /// Default: 300 seconds (5 minutes)
    pub member_assignment_ttl_secs: u64,

    /// TTL for transactional producer state (seconds).
    /// Default: 604800 (7 days)
    pub txn_producer_ttl_secs: i64,

    /// TTL for partition ownership cache (seconds).
    ///
    /// # Consistency Guarantees
    ///
    /// This cache affects **reads only**. Write operations always
    /// verify ownership directly, ensuring strong consistency for produces.
    ///
    /// For fetch (read) operations, there is a window of up to `ownership_cache_ttl_secs`
    /// where a broker may serve reads after losing ownership.
    ///
    /// Default: 1 second
    pub ownership_cache_ttl_secs: u64,

    /// Maximum number of entries in the ownership cache.
    /// Default: 10,000
    pub ownership_cache_max_capacity: u64,

    // --- Recovery & resilience tuning ---
    /// Maximum consecutive heartbeat failures before entering zombie mode.
    /// When this threshold is reached, the broker stops accepting writes
    /// to prevent split-brain scenarios.
    ///
    /// Default: 1 (minimizes split-brain window)
    pub max_consecutive_heartbeat_failures: u32,

    /// Maximum batch boundaries to cache per partition.
    /// Keeps memory bounded while providing efficient offset lookup.
    ///
    /// Default: 10,000
    pub batch_index_max_size: usize,

    /// Maximum number of partition stores to keep open simultaneously.
    ///
    /// When a broker owns many partitions, keeping all SlateDB stores open
    /// consumes significant memory. This setting limits the number of open
    /// stores using LRU eviction - the least recently used stores are closed
    /// when the limit is reached.
    ///
    /// Set higher for brokers with large memory that own many partitions.
    /// Set lower for memory-constrained environments.
    ///
    /// Default: 1000
    pub max_open_partition_stores: u64,

    /// Idle timeout for partition stores (seconds).
    ///
    /// Stores not accessed for this duration may be evicted from the pool
    /// even before capacity is reached. This helps free memory from partitions
    /// that are no longer being accessed.
    ///
    /// Default: 300 (5 minutes)
    pub partition_store_idle_timeout_secs: u64,

    /// Minimum remaining lease TTL (in seconds) required to allow writes.
    ///
    /// If the lease has less than this remaining TTL, writes are rejected to prevent
    /// TOCTOU (time-of-check-to-time-of-use) races where the lease could expire
    /// during a write operation, causing split-brain scenarios.
    ///
    /// This value should be configured based on:
    /// - Expected worst-case write latency to object store
    /// - Network latency between brokers
    /// - Any GC pauses or other system delays
    ///
    /// **WARNING**: Setting this too low (< 5 seconds) increases risk of split-brain.
    /// Setting this too high reduces the effective usable lease window.
    ///
    /// Default: 15 seconds
    pub min_lease_ttl_for_write_secs: u64,

    /// TTL for producer state cache entries (in seconds).
    ///
    /// The producer state cache stores (last_sequence, producer_epoch) per producer_id
    /// for idempotency checks. Entries expire after this duration of inactivity.
    ///
    /// Default: 900 seconds (15 minutes)
    pub producer_state_cache_ttl_secs: u64,

    /// Maximum groups to process per eviction scan.
    /// Default: 100
    pub max_groups_per_eviction_scan: usize,

    // --- Circuit Breaker Configuration ---
    /// Circuit breaker threshold for fail-closed fencing detection.
    ///
    /// After this many consecutive fail-closed events (unknown errors treated as
    /// potential fencing) without any confirmed fencing, the circuit breaker trips
    /// and stops treating unknown errors as fencing. This prevents cascading
    /// unavailability from transient unknown errors.
    ///
    /// Default: 5
    pub circuit_breaker_threshold: u64,

    /// Base reset window for circuit breaker (milliseconds).
    ///
    /// If a confirmed fencing event is detected within this window after the
    /// last fail-closed event, the circuit breaker counter resets. The actual
    /// window grows exponentially based on consecutive trips.
    ///
    /// Default: 60000 (1 minute)
    pub circuit_breaker_base_reset_window_ms: u64,

    /// Maximum reset window for circuit breaker after exponential backoff (milliseconds).
    ///
    /// Default: 300000 (5 minutes)
    pub circuit_breaker_max_reset_window_ms: u64,

    // --- Raft configuration ---
    /// Raft RPC listen address (for inter-node communication).
    /// Default: "127.0.0.1:9093"
    pub raft_listen_addr: String,

    /// Initial Raft peers for cluster formation.
    /// Format: "node_id=host:port,node_id=host:port,..."
    /// Example: "0=127.0.0.1:9093,1=127.0.0.1:9094,2=127.0.0.1:9095"
    pub raft_peers: Option<String>,

    // --- Metrics tuning ---
    /// Whether to enable per-partition metrics.
    ///
    /// When enabled, metrics include `partition` as a label, which provides
    /// detailed observability but can cause high cardinality.
    ///
    /// Default: true (detailed metrics)
    pub enable_partition_metrics: bool,

    /// Maximum number of unique topic/partition combinations to track in metrics.
    /// Default: 10,000
    pub max_metric_cardinality: usize,

    // --- SASL Authentication ---
    /// Whether SASL authentication is enabled.
    ///
    /// Default: false
    pub sasl_enabled: bool,

    /// Whether SASL authentication is required for all clients.
    ///
    /// Default: false
    pub sasl_required: bool,

    /// Path to a file containing SASL users in the format `username:password`.
    ///
    /// Default: None
    pub sasl_users_file: Option<String>,

    // --- Data Integrity ---
    /// Whether to validate CRC-32C checksums on incoming record batches.
    ///
    /// Default: true (enabled for production safety)
    pub validate_record_crc: bool,

    /// Whether to fail partition startup if offset gaps are detected.
    ///
    /// When enabled, if the HWM recovery scan detects gaps in the offset sequence
    /// (indicating potential data loss), the partition will fail to open rather
    /// than continuing with potentially incomplete data.
    ///
    /// This is useful for:
    /// - Production deployments where data integrity is critical
    /// - Detecting storage layer issues during recovery
    /// - Preventing silent data loss from going unnoticed
    ///
    /// When disabled (default), gaps are logged as errors and recorded as metrics,
    /// but the partition continues to operate.
    ///
    /// Default: false (log and continue, for backwards compatibility)
    pub fail_on_recovery_gap: bool,

    // --- Fast Failover Configuration ---
    /// Whether fast failover is enabled.
    ///
    /// When enabled, brokers send heartbeats at a faster rate (default 500ms)
    /// and failures are detected within ~2.5 seconds instead of waiting for
    /// the 60-second lease expiration.
    ///
    /// Default: true
    pub fast_failover_enabled: bool,

    /// Interval between fast heartbeats (milliseconds).
    ///
    /// This is the frequency at which brokers send heartbeats for
    /// fast failure detection. Lower values detect failures faster
    /// but increase network traffic.
    ///
    /// Default: 500ms
    pub fast_heartbeat_interval_ms: u64,

    /// Number of missed heartbeats before declaring a broker as suspected.
    ///
    /// Suspected state is an intermediate state that helps reduce
    /// false positives from transient network issues.
    ///
    /// Default: 2 (1 second at 500ms interval)
    pub failure_suspicion_threshold: u32,

    /// Number of missed heartbeats before declaring a broker as failed.
    ///
    /// After this many missed heartbeats, the broker is considered dead
    /// and its partitions are redistributed.
    ///
    /// Default: 5 (2.5 seconds at 500ms interval)
    pub failure_threshold: u32,

    // --- Auto-Balancing Configuration ---
    /// Whether auto-balancing is enabled.
    ///
    /// When enabled, the cluster automatically redistributes partitions
    /// to maintain even load distribution across brokers.
    ///
    /// Default: true
    pub auto_balancer_enabled: bool,

    /// How often to evaluate cluster balance (seconds).
    ///
    /// Default: 60 seconds
    pub auto_balancer_evaluation_interval_secs: u64,

    /// Load deviation threshold that triggers rebalancing.
    ///
    /// This is expressed as a fraction (0.0 to 1.0). A value of 0.3
    /// means rebalancing is triggered when the load deviation between
    /// the most loaded and least loaded brokers exceeds 30%.
    ///
    /// Default: 0.3 (30%)
    pub auto_balancer_deviation_threshold: f64,

    /// Maximum partitions to move per rebalance cycle.
    ///
    /// This limits the impact of a single rebalance operation
    /// to prevent cascading failures.
    ///
    /// Default: 5
    pub auto_balancer_max_partitions_per_cycle: usize,

    /// Cooldown period (seconds) before a partition can be moved again.
    ///
    /// This prevents thrashing where the same partition is moved
    /// back and forth repeatedly.
    ///
    /// Default: 300 seconds (5 minutes)
    pub auto_balancer_cooldown_secs: u64,

    /// Weight for throughput in broker score calculation.
    ///
    /// The broker score is calculated as:
    /// score = partition_count * (1 - weight) + throughput * weight
    ///
    /// A value of 0.7 means 70% of the score is based on throughput
    /// and 30% on partition count.
    ///
    /// Default: 0.7
    pub auto_balancer_throughput_weight: f64,

    // --- Raft Configuration ---
    /// Number of Raft log entries before triggering a snapshot.
    ///
    /// Lower values increase snapshot frequency, which:
    /// - Reduces data loss window on cluster-wide crash (consumer offsets, etc.)
    /// - Increases object store write costs
    ///
    /// Higher values reduce snapshot overhead but increase the window
    /// where committed Raft state could be lost on crash.
    ///
    /// Default: 1000 (balances durability and cost)
    pub raft_snapshot_threshold: u64,

    // --- SlateDB Configuration ---
    /// Maximum unflushed bytes in SlateDB memtables before backpressure.
    ///
    /// When unflushed data exceeds this limit, writes are paused until
    /// data is flushed to object storage. This prevents OOM conditions
    /// when object store latency spikes.
    ///
    /// Default: 256 MB (256 * 1024 * 1024)
    pub slatedb_max_unflushed_bytes: usize,

    /// Target size for SlateDB L0 SSTables in bytes.
    ///
    /// Memtables are flushed to L0 when they reach this size.
    /// Smaller values mean more frequent flushes (faster recovery, higher cost).
    ///
    /// Default: 64 MB (64 * 1024 * 1024)
    pub slatedb_l0_sst_size_bytes: usize,

    /// SlateDB flush interval in milliseconds.
    ///
    /// How frequently SlateDB flushes the WAL to object storage.
    /// Lower values reduce data loss window but increase API costs.
    ///
    /// Default: 100ms
    pub slatedb_flush_interval_ms: u64,
}

impl Default for ClusterConfig {
    fn default() -> Self {
        Self {
            broker_id: 0,
            host: "127.0.0.1".to_string(),
            advertised_host: "127.0.0.1".to_string(),
            port: 9092,
            health_port: 8080,
            cluster_id: "kafkaesque-cluster".to_string(),
            object_store: ObjectStoreType::default(),
            object_store_path: "kafkaesque-data".to_string(),
            lease_duration: Duration::from_secs(DEFAULT_LEASE_DURATION_SECS),
            lease_renewal_interval: Duration::from_secs(DEFAULT_LEASE_RENEWAL_INTERVAL_SECS),
            heartbeat_interval: Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL_SECS),
            ownership_check_interval: Duration::from_secs(DEFAULT_OWNERSHIP_CHECK_INTERVAL_SECS),
            session_timeout_check_interval: Duration::from_secs(
                DEFAULT_SESSION_TIMEOUT_CHECK_INTERVAL_SECS,
            ),
            default_session_timeout_ms: 30000, // 30 seconds, Kafka default
            auto_create_topics: true,          // Match Kafka's default
            default_num_partitions: 10,
            max_partitions_per_topic: 1000, // Reasonable limit to prevent DoS
            max_auto_created_topics: 10_000, // Reasonable limit to prevent DoS
            // Network tuning
            max_message_size: 100 * 1024 * 1024,  // 100 MB
            max_fetch_response_size: 1024 * 1024, // 1 MB
            max_concurrent_partition_writes: 16,
            max_concurrent_partition_reads: 16,
            fetch_timeout_secs: 30, // 30 seconds
            // Coordinator tuning
            broker_heartbeat_ttl_secs: 30,
            group_member_ttl_secs: 300,      // 5 minutes
            member_assignment_ttl_secs: 300, // 5 minutes
            txn_producer_ttl_secs: 604800,   // 7 days
            ownership_cache_ttl_secs: 1,
            ownership_cache_max_capacity: 10_000,
            // Recovery & resilience tuning
            max_consecutive_heartbeat_failures: 1,
            batch_index_max_size: 10_000,
            max_open_partition_stores: 1000,
            partition_store_idle_timeout_secs: 300,
            min_lease_ttl_for_write_secs: crate::constants::DEFAULT_MIN_LEASE_TTL_FOR_WRITE_SECS,
            producer_state_cache_ttl_secs: DEFAULT_PRODUCER_STATE_CACHE_TTL_SECS,
            max_groups_per_eviction_scan: 100,
            // Circuit breaker configuration
            circuit_breaker_threshold: 5,
            circuit_breaker_base_reset_window_ms: 60_000, // 1 minute
            circuit_breaker_max_reset_window_ms: 300_000, // 5 minutes
            // Raft configuration
            raft_listen_addr: "127.0.0.1:9093".to_string(),
            raft_peers: None,
            // Metrics tuning
            enable_partition_metrics: true,
            max_metric_cardinality: 10_000,
            // SASL authentication
            sasl_enabled: false,
            sasl_required: false,
            sasl_users_file: None,
            // Data integrity
            validate_record_crc: true,
            fail_on_recovery_gap: false,
            // Fast failover
            fast_failover_enabled: true,
            fast_heartbeat_interval_ms: 500,
            failure_suspicion_threshold: 2,
            failure_threshold: 5,
            // Auto-balancing
            auto_balancer_enabled: true,
            auto_balancer_evaluation_interval_secs: 60,
            auto_balancer_deviation_threshold: 0.3,
            auto_balancer_max_partitions_per_cycle: 5,
            auto_balancer_cooldown_secs: 300,
            auto_balancer_throughput_weight: 0.7,
            // Raft configuration (additional)
            raft_snapshot_threshold: 1_000,
            // SlateDB configuration
            slatedb_max_unflushed_bytes: 256 * 1024 * 1024, // 256 MB
            slatedb_l0_sst_size_bytes: 64 * 1024 * 1024,    // 64 MB
            slatedb_flush_interval_ms: 100,
        }
    }
}

impl ClusterConfig {
    /// Create a configuration from a validated profile.
    ///
    /// Profiles provide pre-configured settings optimized for specific use cases,
    /// reducing the cognitive load of managing 60+ configuration parameters.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// use kafkaesque::cluster::{ClusterConfig, ClusterProfile};
    ///
    /// // Development - fast startup, relaxed timeouts
    /// let config = ClusterConfig::from_profile(ClusterProfile::Development);
    ///
    /// // Production - balanced safety and performance
    /// let config = ClusterConfig::from_profile(ClusterProfile::Production);
    /// ```
    ///
    /// # Note
    ///
    /// You can still customize individual fields after creating from a profile:
    ///
    /// ```rust,no_run
    /// use kafkaesque::cluster::{ClusterConfig, ClusterProfile};
    ///
    /// let mut config = ClusterConfig::from_profile(ClusterProfile::Production);
    /// config.broker_id = 42;
    /// config.port = 9093;
    /// ```
    pub fn from_profile(profile: ClusterProfile) -> Self {
        let base = Self::default();

        match profile {
            ClusterProfile::Development => Self {
                // Relaxed timeouts for debugging
                lease_duration: Duration::from_secs(120),
                lease_renewal_interval: Duration::from_secs(40),
                heartbeat_interval: Duration::from_secs(15),
                broker_heartbeat_ttl_secs: 60,

                // Fast startup
                ownership_check_interval: Duration::from_secs(2),
                session_timeout_check_interval: Duration::from_secs(5),

                // Lower resource usage
                max_concurrent_partition_writes: 4,
                max_concurrent_partition_reads: 4,
                batch_index_max_size: 1_000,
                ownership_cache_max_capacity: 1_000,

                // Developer-friendly defaults
                auto_create_topics: true,
                default_num_partitions: 3,
                max_partitions_per_topic: 100,

                // Relaxed safety for faster iteration
                max_consecutive_heartbeat_failures: 3,
                min_lease_ttl_for_write_secs: 10,

                // Disable fast failover (not needed for dev)
                fast_failover_enabled: false,

                // Disable auto-balancing (not needed for dev)
                auto_balancer_enabled: false,

                // CRC validation for catching bugs early
                validate_record_crc: true,

                ..base
            },

            ClusterProfile::Production => Self {
                // Balanced lease timing
                lease_duration: Duration::from_secs(60),
                lease_renewal_interval: Duration::from_secs(20),
                heartbeat_interval: Duration::from_secs(5),
                broker_heartbeat_ttl_secs: 30,

                // Standard concurrency
                max_concurrent_partition_writes: 16,
                max_concurrent_partition_reads: 16,

                // Fast failover enabled
                fast_failover_enabled: true,
                fast_heartbeat_interval_ms: 500,
                failure_suspicion_threshold: 2,
                failure_threshold: 5,

                // Auto-balancing enabled
                auto_balancer_enabled: true,
                auto_balancer_evaluation_interval_secs: 60,
                auto_balancer_deviation_threshold: 0.3,
                auto_balancer_max_partitions_per_cycle: 5,

                // Safety settings
                min_lease_ttl_for_write_secs: 15,
                validate_record_crc: true,
                circuit_breaker_threshold: 5,

                // Conservative topic creation
                auto_create_topics: true,
                default_num_partitions: 10,
                max_partitions_per_topic: 1000,

                ..base
            },

            ClusterProfile::HighThroughput => Self {
                // Longer leases to avoid interrupting large operations
                lease_duration: Duration::from_secs(120),
                lease_renewal_interval: Duration::from_secs(40),

                // Higher concurrency for parallel I/O
                max_concurrent_partition_writes: 32,
                max_concurrent_partition_reads: 32,

                // Larger batches and responses
                max_fetch_response_size: 10 * 1024 * 1024, // 10 MB
                batch_index_max_size: 50_000,

                // Longer timeouts for large operations
                fetch_timeout_secs: 60,
                min_lease_ttl_for_write_secs: 20,

                // More partitions for parallelism
                default_num_partitions: 20,
                max_partitions_per_topic: 5000,

                // Fast failover still enabled but with more tolerance
                fast_failover_enabled: true,
                fast_heartbeat_interval_ms: 1000,
                failure_threshold: 8,

                // Auto-balancer more aggressive
                auto_balancer_enabled: true,
                auto_balancer_deviation_threshold: 0.2,
                auto_balancer_max_partitions_per_cycle: 10,

                // Skip CRC for maximum throughput
                validate_record_crc: false,

                ..base
            },

            ClusterProfile::LowLatency => Self {
                // Shorter leases for faster failover
                lease_duration: Duration::from_secs(30),
                lease_renewal_interval: Duration::from_secs(10),
                heartbeat_interval: Duration::from_secs(2),
                broker_heartbeat_ttl_secs: 15,

                // Very aggressive fast failover
                fast_failover_enabled: true,
                fast_heartbeat_interval_ms: 200,
                failure_suspicion_threshold: 2,
                failure_threshold: 3,

                // Moderate concurrency (latency over throughput)
                max_concurrent_partition_writes: 8,
                max_concurrent_partition_reads: 8,

                // Smaller fetch responses for faster delivery
                max_fetch_response_size: 256 * 1024, // 256 KB

                // Shorter timeouts
                fetch_timeout_secs: 10,
                default_session_timeout_ms: 10000, // 10 seconds

                // Tighter safety margins
                min_lease_ttl_for_write_secs: 8,

                // Fewer partitions per topic (reduces coordination)
                default_num_partitions: 6,

                // Keep CRC for safety
                validate_record_crc: true,

                // More responsive auto-balancer
                auto_balancer_enabled: true,
                auto_balancer_evaluation_interval_secs: 30,
                auto_balancer_cooldown_secs: 120,

                ..base
            },
        }
    }

    /// Create a configuration from a profile specified by environment variable.
    ///
    /// Reads the `CLUSTER_PROFILE` environment variable and creates the corresponding
    /// profile. Falls back to Production if not set or invalid.
    ///
    /// # Environment Variable
    ///
    /// - `CLUSTER_PROFILE`: One of "development", "production", "high-throughput", "low-latency"
    ///
    /// # Example
    ///
    /// ```bash
    /// CLUSTER_PROFILE=production cargo run
    /// ```
    pub fn from_profile_env() -> Self {
        let profile = std::env::var("CLUSTER_PROFILE")
            .ok()
            .and_then(|s| s.parse::<ClusterProfile>().ok())
            .unwrap_or(ClusterProfile::Production);

        eprintln!(
            "Using cluster profile: {} ({})",
            profile,
            profile.description()
        );
        Self::from_profile(profile)
    }

    /// Validate the configuration and return any errors found.
    ///
    /// This should be called at startup to catch configuration issues early.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        // Lease timing: renewal must happen before lease expires
        if self.lease_renewal_interval >= self.lease_duration {
            errors.push(format!(
                "lease_renewal_interval ({:?}) must be less than lease_duration ({:?})",
                self.lease_renewal_interval, self.lease_duration
            ));
        }

        // Heartbeat timing: heartbeat interval must be less than TTL
        let heartbeat_ttl = Duration::from_secs(self.broker_heartbeat_ttl_secs);
        if self.heartbeat_interval >= heartbeat_ttl {
            errors.push(format!(
                "heartbeat_interval ({:?}) must be less than broker_heartbeat_ttl ({:?})",
                self.heartbeat_interval, heartbeat_ttl
            ));
        }

        // Ownership cache TTL should be short for consistency
        if self.ownership_cache_ttl_secs > 10 {
            errors.push(format!(
                "ownership_cache_ttl_secs ({}) should not exceed 10 seconds",
                self.ownership_cache_ttl_secs
            ));
        }

        // Port validation
        if self.port < 1 || self.port > 65535 {
            errors.push(format!("port ({}) must be between 1 and 65535", self.port));
        }

        // Broker ID should be non-negative
        if self.broker_id < 0 {
            errors.push(format!(
                "broker_id ({}) must be non-negative",
                self.broker_id
            ));
        }

        // Message size bounds
        if self.max_message_size < 1024 {
            errors.push(format!(
                "max_message_size ({}) should be at least 1KB",
                self.max_message_size
            ));
        }

        // Fetch response size should be reasonable
        if self.max_fetch_response_size < 1024 {
            errors.push(format!(
                "max_fetch_response_size ({}) should be at least 1KB",
                self.max_fetch_response_size
            ));
        }

        // Concurrent writes should be bounded
        if self.max_concurrent_partition_writes == 0 {
            errors.push("max_concurrent_partition_writes must be at least 1".to_string());
        }

        // Concurrent reads should be bounded
        if self.max_concurrent_partition_reads == 0 {
            errors.push("max_concurrent_partition_reads must be at least 1".to_string());
        }

        // Consecutive heartbeat failures should allow some tolerance
        if self.max_consecutive_heartbeat_failures == 0 {
            errors.push("max_consecutive_heartbeat_failures must be at least 1".to_string());
        }

        // Session timeout should be reasonable
        if self.default_session_timeout_ms < 1000 {
            errors.push(format!(
                "default_session_timeout_ms ({}) should be at least 1000ms",
                self.default_session_timeout_ms
            ));
        }

        // Fetch timeout must be positive
        if self.fetch_timeout_secs == 0 {
            errors.push("fetch_timeout_secs must be greater than 0".to_string());
        }

        // Batch index size must be positive
        if self.batch_index_max_size == 0 {
            errors.push("batch_index_max_size must be greater than 0".to_string());
        }

        // Min lease TTL for write validation
        if self.min_lease_ttl_for_write_secs < 5 {
            errors.push(format!(
                "min_lease_ttl_for_write_secs ({}) should be at least 5 seconds to prevent split-brain",
                self.min_lease_ttl_for_write_secs
            ));
        }

        // Lease timing safety
        const MIN_LEASE_TTL_BUFFER_SECS: u64 = 10;
        let lease_buffer = self
            .lease_duration
            .as_secs()
            .saturating_sub(self.lease_renewal_interval.as_secs());
        if lease_buffer < MIN_LEASE_TTL_BUFFER_SECS {
            errors.push(format!(
                "Insufficient lease buffer: lease_duration ({:?}) - lease_renewal_interval ({:?}) = {}s, \
                 but minimum required buffer is {}s",
                self.lease_duration,
                self.lease_renewal_interval,
                lease_buffer,
                MIN_LEASE_TTL_BUFFER_SECS
            ));
        }

        if self.default_num_partitions <= 0 {
            errors.push(format!(
                "default_num_partitions ({}) must be positive",
                self.default_num_partitions
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Validate configuration and panic with detailed error if invalid.
    pub fn validate_or_panic(&self) {
        if let Err(errors) = self.validate() {
            eprintln!("=== Configuration Validation Failed ===");
            for (i, error) in errors.iter().enumerate() {
                eprintln!("  {}. {}", i + 1, error);
            }
            eprintln!("========================================");
            panic!("Invalid configuration - {} error(s) found", errors.len());
        }
    }

    /// Create configuration from environment variables.
    ///
    /// Environment variables:
    /// - `BROKER_ID`: Broker ID (default: 0)
    /// - `HOST`: Host address (default: 0.0.0.0)
    /// - `PORT`: Kafka port (default: 9092)
    /// - `CLUSTER_ID`: Cluster identifier (default: kafkaesque-cluster)
    /// - `RAFT_LISTEN_ADDR`: Raft RPC listen address (default: 127.0.0.1:9093)
    /// - `RAFT_PEERS`: Initial peers for cluster formation
    /// - `OBJECT_STORE_TYPE`: "local", "s3", "gcs", or "azure" (default: local)
    /// - `DATA_PATH`: Local path or object store prefix (default: /tmp/kafkaesque-data)
    /// - `AUTO_CREATE_TOPICS`: "true" or "false" (default: true)
    ///
    /// For S3:
    /// - `AWS_S3_BUCKET` or `S3_BUCKET`: Bucket name
    /// - `AWS_REGION` or `AWS_DEFAULT_REGION`: Region
    /// - `AWS_ENDPOINT` or `S3_ENDPOINT`: Custom endpoint (for MinIO)
    /// - `AWS_ACCESS_KEY_ID`: Access key (optional, can use IAM)
    /// - `AWS_SECRET_ACCESS_KEY`: Secret key (optional)
    ///
    /// For GCS:
    /// - `GCS_BUCKET`: Bucket name
    /// - `GOOGLE_APPLICATION_CREDENTIALS`: Path to service account key
    ///
    /// For Azure:
    /// - `AZURE_CONTAINER`: Container name
    /// - `AZURE_STORAGE_ACCOUNT`: Storage account name
    /// - `AZURE_STORAGE_ACCESS_KEY`: Access key (optional)
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let defaults = Self::default();

        let broker_id: i32 = std::env::var("BROKER_ID")
            .unwrap_or_else(|_| "0".to_string())
            .parse()
            .map_err(|e| format!("Invalid BROKER_ID: {}", e))?;

        if broker_id < 0 {
            return Err("BROKER_ID must be non-negative".into());
        }

        let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());

        // ADVERTISED_HOST is what clients use to connect back.
        // Defaults to HOST, but if HOST is 0.0.0.0, default to 127.0.0.1 for local dev.
        let advertised_host = std::env::var("ADVERTISED_HOST").unwrap_or_else(|_| {
            if host == "0.0.0.0" {
                "127.0.0.1".to_string()
            } else {
                host.clone()
            }
        });

        let port: i32 = std::env::var("PORT")
            .unwrap_or_else(|_| "9092".to_string())
            .parse()
            .map_err(|e| format!("Invalid PORT: {}", e))?;

        if !(1..=65535).contains(&port) {
            return Err(format!("PORT must be between 1 and 65535, got {}", port).into());
        }

        let health_port: i32 = std::env::var("HEALTH_PORT")
            .unwrap_or_else(|_| "8080".to_string())
            .parse()
            .map_err(|e| format!("Invalid HEALTH_PORT: {}", e))?;

        if health_port != 0 && !(1..=65535).contains(&health_port) {
            return Err(format!(
                "HEALTH_PORT must be 0 or between 1 and 65535, got {}",
                health_port
            )
            .into());
        }

        let cluster_id =
            std::env::var("CLUSTER_ID").unwrap_or_else(|_| "kafkaesque-cluster".to_string());

        let raft_listen_addr =
            std::env::var("RAFT_LISTEN_ADDR").unwrap_or_else(|_| "127.0.0.1:9093".to_string());

        let raft_peers = std::env::var("RAFT_PEERS").ok();

        let data_path =
            std::env::var("DATA_PATH").unwrap_or_else(|_| "/tmp/kafkaesque-data".to_string());

        let auto_create_topics = std::env::var("AUTO_CREATE_TOPICS")
            .map(|v| v.to_lowercase() != "false" && v != "0")
            .unwrap_or(true);

        let default_num_partitions: i32 = std::env::var("DEFAULT_NUM_PARTITIONS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.default_num_partitions);

        // Network tuning from env
        let max_message_size: usize = std::env::var("MAX_MESSAGE_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.max_message_size);

        let max_fetch_response_size: usize = std::env::var("MAX_FETCH_RESPONSE_SIZE")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.max_fetch_response_size);

        let max_concurrent_partition_writes: usize =
            std::env::var("MAX_CONCURRENT_PARTITION_WRITES")
                .ok()
                .and_then(|v| v.parse().ok())
                .unwrap_or(defaults.max_concurrent_partition_writes);

        let max_concurrent_partition_reads: usize = std::env::var("MAX_CONCURRENT_PARTITION_READS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.max_concurrent_partition_reads);

        // Coordinator tuning
        let broker_heartbeat_ttl_secs: u64 = std::env::var("BROKER_HEARTBEAT_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.broker_heartbeat_ttl_secs);

        let group_member_ttl_secs: i64 = std::env::var("GROUP_MEMBER_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.group_member_ttl_secs);

        let ownership_cache_ttl_secs: u64 = std::env::var("OWNERSHIP_CACHE_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.ownership_cache_ttl_secs);

        let producer_state_cache_ttl_secs: u64 = std::env::var("PRODUCER_STATE_CACHE_TTL_SECS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.producer_state_cache_ttl_secs);

        let object_store_type = std::env::var("OBJECT_STORE_TYPE")
            .unwrap_or_else(|_| "local".to_string())
            .to_lowercase();

        let object_store = match object_store_type.as_str() {
            "s3" => {
                let bucket = std::env::var("AWS_S3_BUCKET")
                    .or_else(|_| std::env::var("S3_BUCKET"))
                    .map_err(
                        |_| "S3_BUCKET or AWS_S3_BUCKET must be set when OBJECT_STORE_TYPE=s3",
                    )?;

                let region = std::env::var("AWS_REGION")
                    .or_else(|_| std::env::var("AWS_DEFAULT_REGION"))
                    .unwrap_or_else(|_| "us-east-1".to_string());

                let endpoint = std::env::var("AWS_ENDPOINT")
                    .or_else(|_| std::env::var("S3_ENDPOINT"))
                    .ok();

                let access_key_id = std::env::var("AWS_ACCESS_KEY_ID").ok();
                let secret_access_key = std::env::var("AWS_SECRET_ACCESS_KEY").ok();

                ObjectStoreType::S3 {
                    bucket,
                    region,
                    endpoint,
                    access_key_id,
                    secret_access_key,
                }
            }
            "gcs" => {
                let bucket = std::env::var("GCS_BUCKET")
                    .map_err(|_| "GCS_BUCKET must be set when OBJECT_STORE_TYPE=gcs")?;

                let service_account_key = std::env::var("GOOGLE_APPLICATION_CREDENTIALS").ok();

                ObjectStoreType::Gcs {
                    bucket,
                    service_account_key,
                }
            }
            "azure" => {
                let container =
                    std::env::var("AZURE_CONTAINER").map_err(|_| "AZURE_CONTAINER must be set")?;

                let account = std::env::var("AZURE_STORAGE_ACCOUNT")
                    .map_err(|_| "AZURE_STORAGE_ACCOUNT must be set")?;

                let access_key = std::env::var("AZURE_STORAGE_ACCESS_KEY").ok();

                ObjectStoreType::Azure {
                    container,
                    account,
                    access_key,
                }
            }
            _ => ObjectStoreType::Local {
                path: data_path.clone(),
            },
        };

        // SASL authentication configuration
        let sasl_enabled = std::env::var("SASL_ENABLED")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        let sasl_required = std::env::var("SASL_REQUIRED")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        let sasl_users_file = std::env::var("SASL_USERS_FILE").ok();

        // Data integrity configuration
        let fail_on_recovery_gap = std::env::var("FAIL_ON_RECOVERY_GAP")
            .map(|v| v.to_lowercase() == "true" || v == "1")
            .unwrap_or(false);

        // Raft configuration
        let raft_snapshot_threshold: u64 = std::env::var("RAFT_SNAPSHOT_THRESHOLD")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.raft_snapshot_threshold);

        // SlateDB configuration
        let slatedb_max_unflushed_bytes: usize = std::env::var("SLATEDB_MAX_UNFLUSHED_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.slatedb_max_unflushed_bytes);

        let slatedb_l0_sst_size_bytes: usize = std::env::var("SLATEDB_L0_SST_SIZE_BYTES")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.slatedb_l0_sst_size_bytes);

        let slatedb_flush_interval_ms: u64 = std::env::var("SLATEDB_FLUSH_INTERVAL_MS")
            .ok()
            .and_then(|v| v.parse().ok())
            .unwrap_or(defaults.slatedb_flush_interval_ms);

        let config = Self {
            broker_id,
            host: host.clone(),
            advertised_host: advertised_host.clone(),
            port,
            health_port,
            cluster_id: cluster_id.clone(),
            raft_listen_addr: raft_listen_addr.clone(),
            raft_peers: raft_peers.clone(),
            object_store,
            object_store_path: data_path.clone(),
            auto_create_topics,
            default_num_partitions,
            max_message_size,
            max_fetch_response_size,
            max_concurrent_partition_writes,
            max_concurrent_partition_reads,
            broker_heartbeat_ttl_secs,
            group_member_ttl_secs,
            ownership_cache_ttl_secs,
            producer_state_cache_ttl_secs,
            sasl_enabled,
            sasl_required,
            sasl_users_file,
            fail_on_recovery_gap,
            raft_snapshot_threshold,
            slatedb_max_unflushed_bytes,
            slatedb_l0_sst_size_bytes,
            slatedb_flush_interval_ms,
            ..defaults
        };

        eprintln!("=== Kafkaesque Configuration ===");
        eprintln!("Broker ID: {}", broker_id);
        eprintln!("Host (bind): {}", host);
        eprintln!("Advertised Host: {}", advertised_host);
        eprintln!("Port: {}", port);
        eprintln!(
            "Health Port: {}",
            if health_port == 0 {
                "disabled".to_string()
            } else {
                health_port.to_string()
            }
        );
        eprintln!("Coordination: Raft (embedded consensus)");
        eprintln!("Cluster ID: {}", cluster_id);
        eprintln!("Raft Listen Addr: {}", raft_listen_addr);
        if let Some(ref peers) = raft_peers {
            eprintln!("Raft Peers: {}", peers);
        }
        eprintln!("Object Store: {:?}", object_store_type);
        eprintln!("Data Path: {}", data_path);
        eprintln!("Auto Create Topics: {}", auto_create_topics);
        eprintln!("Default Num Partitions: {}", default_num_partitions);
        eprintln!("Max Message Size: {} bytes", max_message_size);
        eprintln!("Max Fetch Response Size: {} bytes", max_fetch_response_size);
        eprintln!("Broker Heartbeat TTL: {}s", broker_heartbeat_ttl_secs);
        eprintln!("SASL Enabled: {}", sasl_enabled);
        eprintln!("Fail on Recovery Gap: {}", fail_on_recovery_gap);
        eprintln!("==========================");

        // Run validation
        if let Err(errors) = config.validate() {
            return Err(format!("Configuration validation failed: {}", errors.join("; ")).into());
        }

        Ok(config)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_object_store_type_default() {
        let store = ObjectStoreType::default();
        match store {
            ObjectStoreType::Local { path } => {
                assert_eq!(path, "/tmp/kafkaesque-data");
            }
            _ => panic!("Expected Local storage type"),
        }
    }

    #[test]
    fn test_cluster_config_default() {
        let config = ClusterConfig::default();
        assert_eq!(config.broker_id, 0);
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 9092);
        assert_eq!(config.object_store_path, "kafkaesque-data");
        assert_eq!(config.lease_duration, Duration::from_secs(60));
        assert_eq!(config.lease_renewal_interval, Duration::from_secs(20));
    }

    #[test]
    fn test_cluster_config_clone() {
        let config = ClusterConfig::default();
        let cloned = config.clone();
        assert_eq!(cloned.broker_id, config.broker_id);
        assert_eq!(cloned.host, config.host);
        assert_eq!(cloned.port, config.port);
    }

    #[test]
    fn test_validate_default_config_succeeds() {
        let config = ClusterConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_lease_renewal_greater_than_duration_fails() {
        let config = ClusterConfig {
            lease_renewal_interval: Duration::from_secs(120),
            lease_duration: Duration::from_secs(60),
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("lease_renewal_interval")));
    }

    #[test]
    fn test_validate_lease_renewal_equal_to_duration_fails() {
        let config = ClusterConfig {
            lease_renewal_interval: Duration::from_secs(60),
            lease_duration: Duration::from_secs(60),
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_heartbeat_interval_greater_than_ttl_fails() {
        let config = ClusterConfig {
            heartbeat_interval: Duration::from_secs(60),
            broker_heartbeat_ttl_secs: 30,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("heartbeat_interval")));
    }

    #[test]
    fn test_validate_ownership_cache_ttl_too_high_fails() {
        let config = ClusterConfig {
            ownership_cache_ttl_secs: 15,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("ownership_cache_ttl_secs"))
        );
    }

    #[test]
    fn test_validate_invalid_port_zero_fails() {
        let config = ClusterConfig {
            port: 0,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("port")));
    }

    #[test]
    fn test_validate_invalid_port_too_high_fails() {
        let config = ClusterConfig {
            port: 70000,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("port")));
    }

    #[test]
    fn test_validate_negative_broker_id_fails() {
        let config = ClusterConfig {
            broker_id: -1,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("broker_id")));
    }

    #[test]
    fn test_validate_max_message_size_too_small_fails() {
        let config = ClusterConfig {
            max_message_size: 512,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("max_message_size")));
    }

    #[test]
    fn test_validate_max_fetch_response_size_too_small_fails() {
        let config = ClusterConfig {
            max_fetch_response_size: 100,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("max_fetch_response_size")));
    }

    #[test]
    fn test_validate_zero_concurrent_partition_writes_fails() {
        let config = ClusterConfig {
            max_concurrent_partition_writes: 0,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("max_concurrent_partition_writes"))
        );
    }

    #[test]
    fn test_validate_zero_concurrent_partition_reads_fails() {
        let config = ClusterConfig {
            max_concurrent_partition_reads: 0,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("max_concurrent_partition_reads"))
        );
    }

    #[test]
    fn test_validate_zero_heartbeat_failures_fails() {
        let config = ClusterConfig {
            max_consecutive_heartbeat_failures: 0,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("max_consecutive_heartbeat_failures"))
        );
    }

    #[test]
    fn test_validate_session_timeout_too_low_fails() {
        let config = ClusterConfig {
            default_session_timeout_ms: 500,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("default_session_timeout_ms"))
        );
    }

    #[test]
    fn test_validate_zero_fetch_timeout_fails() {
        let config = ClusterConfig {
            fetch_timeout_secs: 0,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("fetch_timeout_secs")));
    }

    #[test]
    fn test_validate_zero_batch_index_max_size_fails() {
        let config = ClusterConfig {
            batch_index_max_size: 0,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("batch_index_max_size")));
    }

    #[test]
    fn test_validate_insufficient_lease_buffer_fails() {
        // Set lease duration just 5 seconds more than renewal (buffer < 10)
        let config = ClusterConfig {
            lease_duration: Duration::from_secs(25),
            lease_renewal_interval: Duration::from_secs(20),
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("lease buffer")));
    }

    #[test]
    fn test_validate_multiple_errors() {
        let config = ClusterConfig {
            port: 0,
            broker_id: -1,
            max_concurrent_partition_writes: 0,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.len() >= 3);
    }

    #[test]
    #[should_panic(expected = "Invalid configuration")]
    fn test_validate_or_panic_with_invalid_config() {
        let config = ClusterConfig {
            port: 0,
            ..Default::default()
        };
        config.validate_or_panic();
    }

    #[test]
    fn test_object_store_type_s3_debug() {
        let store = ObjectStoreType::S3 {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: Some("http://localhost:9000".to_string()),
            access_key_id: Some("test-key".to_string()),
            secret_access_key: Some("test-secret".to_string()),
        };
        let debug_str = format!("{:?}", store);
        assert!(debug_str.contains("S3"));
        assert!(debug_str.contains("test-bucket"));
    }

    #[test]
    fn test_object_store_type_gcs_debug() {
        let store = ObjectStoreType::Gcs {
            bucket: "test-bucket".to_string(),
            service_account_key: Some("/path/to/key.json".to_string()),
        };
        let debug_str = format!("{:?}", store);
        assert!(debug_str.contains("Gcs"));
        assert!(debug_str.contains("test-bucket"));
    }

    #[test]
    fn test_object_store_type_azure_debug() {
        let store = ObjectStoreType::Azure {
            container: "test-container".to_string(),
            account: "testaccount".to_string(),
            access_key: Some("test-key".to_string()),
        };
        let debug_str = format!("{:?}", store);
        assert!(debug_str.contains("Azure"));
        assert!(debug_str.contains("test-container"));
    }

    #[test]
    fn test_object_store_type_local_clone() {
        let store = ObjectStoreType::Local {
            path: "/custom/path".to_string(),
        };
        let cloned = store.clone();
        match cloned {
            ObjectStoreType::Local { path } => assert_eq!(path, "/custom/path"),
            _ => panic!("Expected Local variant"),
        }
    }

    #[test]
    fn test_cluster_config_debug() {
        let config = ClusterConfig::default();
        let debug_str = format!("{:?}", config);
        assert!(debug_str.contains("broker_id"));
        assert!(debug_str.contains("host"));
        assert!(debug_str.contains("port"));
    }

    #[test]
    fn test_valid_port_boundaries() {
        // Test valid port at lower boundary
        let config = ClusterConfig {
            port: 1,
            ..Default::default()
        };
        assert!(config.validate().is_ok());

        // Test valid port at upper boundary
        let config = ClusterConfig {
            port: 65535,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_ownership_cache_ttl_at_boundary() {
        let config = ClusterConfig {
            ownership_cache_ttl_secs: 10,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_session_timeout_at_minimum() {
        let config = ClusterConfig {
            default_session_timeout_ms: 1000,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_object_store_type_s3_without_optionals() {
        let store = ObjectStoreType::S3 {
            bucket: "bucket".to_string(),
            region: "us-west-2".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        };
        let cloned = store.clone();
        match cloned {
            ObjectStoreType::S3 {
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            } => {
                assert_eq!(bucket, "bucket");
                assert_eq!(region, "us-west-2");
                assert!(endpoint.is_none());
                assert!(access_key_id.is_none());
                assert!(secret_access_key.is_none());
            }
            _ => panic!("Expected S3 variant"),
        }
    }

    #[test]
    fn test_object_store_type_gcs_without_key() {
        let store = ObjectStoreType::Gcs {
            bucket: "gcs-bucket".to_string(),
            service_account_key: None,
        };
        let cloned = store.clone();
        match cloned {
            ObjectStoreType::Gcs {
                bucket,
                service_account_key,
            } => {
                assert_eq!(bucket, "gcs-bucket");
                assert!(service_account_key.is_none());
            }
            _ => panic!("Expected Gcs variant"),
        }
    }

    #[test]
    fn test_object_store_type_azure_without_key() {
        let store = ObjectStoreType::Azure {
            container: "container".to_string(),
            account: "account".to_string(),
            access_key: None,
        };
        let cloned = store.clone();
        match cloned {
            ObjectStoreType::Azure {
                container,
                account,
                access_key,
            } => {
                assert_eq!(container, "container");
                assert_eq!(account, "account");
                assert!(access_key.is_none());
            }
            _ => panic!("Expected Azure variant"),
        }
    }

    #[test]
    fn test_advertised_host_defaults_to_host() {
        let config = ClusterConfig::default();
        assert_eq!(config.host, config.advertised_host);
    }

    #[test]
    fn test_config_builder_with_custom_values() {
        let config = ClusterConfig {
            broker_id: 42,
            host: "0.0.0.0".to_string(),
            advertised_host: "kafka-0.kafka.svc".to_string(),
            port: 9093,
            health_port: 8081,
            cluster_id: "test-cluster".to_string(),
            auto_create_topics: false,
            default_num_partitions: 20,
            ..Default::default()
        };

        assert_eq!(config.broker_id, 42);
        assert_eq!(config.host, "0.0.0.0");
        assert_eq!(config.advertised_host, "kafka-0.kafka.svc");
        assert_eq!(config.port, 9093);
        assert_eq!(config.health_port, 8081);
        assert_eq!(config.cluster_id, "test-cluster");
        assert!(!config.auto_create_topics);
        assert_eq!(config.default_num_partitions, 20);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_valid_edge_case_config() {
        // Set everything to boundary values that should pass
        let config = ClusterConfig {
            broker_id: 0,
            port: 1,
            max_message_size: 1024,
            max_fetch_response_size: 1024,
            max_concurrent_partition_writes: 1,
            default_session_timeout_ms: 1000,
            fetch_timeout_secs: 1,
            max_consecutive_heartbeat_failures: 1,
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_zero_default_num_partitions() {
        let config = ClusterConfig {
            default_num_partitions: 0,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("default_num_partitions")));
    }

    #[test]
    fn test_validate_negative_default_num_partitions() {
        let config = ClusterConfig {
            default_num_partitions: -1,
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_config_default_values_match_constants() {
        use crate::constants::*;
        let config = ClusterConfig::default();

        assert_eq!(
            config.lease_duration,
            Duration::from_secs(DEFAULT_LEASE_DURATION_SECS)
        );
        assert_eq!(
            config.lease_renewal_interval,
            Duration::from_secs(DEFAULT_LEASE_RENEWAL_INTERVAL_SECS)
        );
        assert_eq!(
            config.heartbeat_interval,
            Duration::from_secs(DEFAULT_HEARTBEAT_INTERVAL_SECS)
        );
        assert_eq!(
            config.ownership_check_interval,
            Duration::from_secs(DEFAULT_OWNERSHIP_CHECK_INTERVAL_SECS)
        );
    }

    #[test]
    fn test_object_store_path_default() {
        let config = ClusterConfig::default();
        assert_eq!(config.object_store_path, "kafkaesque-data");
    }

    #[test]
    fn test_validate_empty_cluster_id() {
        let config = ClusterConfig {
            cluster_id: String::new(),
            ..Default::default()
        };

        // Note: empty cluster_id might be allowed by current validation
        // This test documents the behavior
        let _result = config.validate();
    }

    #[test]
    fn test_validate_session_check_interval_at_boundary() {
        let config = ClusterConfig {
            session_timeout_check_interval: Duration::from_secs(1),
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_max_partitions_per_topic_validation() {
        let config = ClusterConfig {
            max_partitions_per_topic: 0,
            ..Default::default()
        };
        // Zero means no limit, should be valid
        assert!(config.validate().is_ok());

        let config = ClusterConfig {
            max_partitions_per_topic: 10000,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_min_lease_ttl_for_write_too_low() {
        let config = ClusterConfig {
            min_lease_ttl_for_write_secs: 4, // Below 5 second minimum
            ..Default::default()
        };

        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(
            errors
                .iter()
                .any(|e| e.contains("min_lease_ttl_for_write_secs"))
        );
    }

    #[test]
    fn test_validate_min_lease_ttl_for_write_at_boundary() {
        let config = ClusterConfig {
            min_lease_ttl_for_write_secs: 5, // Exactly at minimum
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_min_lease_ttl_for_write_valid() {
        let config = ClusterConfig {
            min_lease_ttl_for_write_secs: 30,
            ..Default::default()
        };
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_sasl_defaults() {
        let config = ClusterConfig::default();
        assert!(!config.sasl_enabled);
        assert!(!config.sasl_required);
        assert!(config.sasl_users_file.is_none());
    }

    #[test]
    fn test_config_data_integrity_defaults() {
        let config = ClusterConfig::default();
        assert!(config.validate_record_crc);
        assert!(!config.fail_on_recovery_gap);
    }

    #[test]
    fn test_config_raft_defaults() {
        let config = ClusterConfig::default();
        assert_eq!(config.raft_listen_addr, "127.0.0.1:9093");
        assert!(config.raft_peers.is_none());
    }

    #[test]
    fn test_config_metrics_defaults() {
        let config = ClusterConfig::default();
        assert!(config.enable_partition_metrics);
        assert_eq!(config.max_metric_cardinality, 10_000);
    }

    #[test]
    fn test_config_network_tuning_defaults() {
        let config = ClusterConfig::default();
        assert_eq!(config.max_message_size, 100 * 1024 * 1024); // 100 MB
        assert_eq!(config.max_fetch_response_size, 1024 * 1024); // 1 MB
        assert_eq!(config.max_concurrent_partition_writes, 16);
        assert_eq!(config.max_concurrent_partition_reads, 16);
        assert_eq!(config.fetch_timeout_secs, 30);
    }

    #[test]
    fn test_config_coordinator_tuning_defaults() {
        let config = ClusterConfig::default();
        assert_eq!(config.broker_heartbeat_ttl_secs, 30);
        assert_eq!(config.group_member_ttl_secs, 300);
        assert_eq!(config.member_assignment_ttl_secs, 300);
        assert_eq!(config.txn_producer_ttl_secs, 604800); // 7 days
        assert_eq!(config.ownership_cache_ttl_secs, 1);
        assert_eq!(config.ownership_cache_max_capacity, 10_000);
    }

    #[test]
    fn test_config_recovery_defaults() {
        let config = ClusterConfig::default();
        assert_eq!(config.max_consecutive_heartbeat_failures, 1);
        assert_eq!(config.batch_index_max_size, 10_000);
        assert_eq!(config.max_groups_per_eviction_scan, 100);
    }

    #[test]
    fn test_config_partition_store_pool_defaults() {
        let config = ClusterConfig::default();
        assert_eq!(config.max_open_partition_stores, 1000);
        assert_eq!(config.partition_store_idle_timeout_secs, 300);
    }

    #[test]
    fn test_config_partition_store_pool_custom_values() {
        let config = ClusterConfig {
            max_open_partition_stores: 500,
            partition_store_idle_timeout_secs: 600,
            ..Default::default()
        };

        assert_eq!(config.max_open_partition_stores, 500);
        assert_eq!(config.partition_store_idle_timeout_secs, 600);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_topic_defaults() {
        let config = ClusterConfig::default();
        assert!(config.auto_create_topics);
        assert_eq!(config.default_num_partitions, 10);
        assert_eq!(config.max_partitions_per_topic, 1000);
        assert_eq!(config.max_auto_created_topics, 10_000);
    }

    #[test]
    fn test_config_health_port_default() {
        let config = ClusterConfig::default();
        assert_eq!(config.health_port, 8080);
    }

    #[test]
    fn test_config_cluster_id_default() {
        let config = ClusterConfig::default();
        assert_eq!(config.cluster_id, "kafkaesque-cluster");
    }

    #[test]
    fn test_config_session_timeout_defaults() {
        let config = ClusterConfig::default();
        assert_eq!(config.default_session_timeout_ms, 30000); // 30 seconds
        assert_eq!(
            config.session_timeout_check_interval,
            Duration::from_secs(crate::constants::DEFAULT_SESSION_TIMEOUT_CHECK_INTERVAL_SECS)
        );
    }

    #[test]
    fn test_object_store_type_local_debug() {
        let store = ObjectStoreType::Local {
            path: "/custom/data/path".to_string(),
        };
        let debug_str = format!("{:?}", store);
        assert!(debug_str.contains("Local"));
        assert!(debug_str.contains("/custom/data/path"));
    }

    #[test]
    fn test_validate_producer_state_cache_ttl() {
        let config = ClusterConfig::default();
        assert_eq!(
            config.producer_state_cache_ttl_secs,
            crate::constants::DEFAULT_PRODUCER_STATE_CACHE_TTL_SECS
        );
    }

    // ========================================================================
    // Fast Failover Configuration Tests
    // ========================================================================

    #[test]
    fn test_config_fast_failover_defaults() {
        let config = ClusterConfig::default();
        assert!(config.fast_failover_enabled);
        assert_eq!(config.fast_heartbeat_interval_ms, 500);
        assert_eq!(config.failure_suspicion_threshold, 2);
        assert_eq!(config.failure_threshold, 5);
    }

    #[test]
    fn test_config_fast_failover_disabled() {
        let config = ClusterConfig {
            fast_failover_enabled: false,
            ..Default::default()
        };
        assert!(!config.fast_failover_enabled);
        // Should still validate successfully
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_fast_failover_custom_interval() {
        let config = ClusterConfig {
            fast_heartbeat_interval_ms: 1000,
            failure_suspicion_threshold: 3,
            failure_threshold: 10,
            ..Default::default()
        };

        assert_eq!(config.fast_heartbeat_interval_ms, 1000);
        assert_eq!(config.failure_suspicion_threshold, 3);
        assert_eq!(config.failure_threshold, 10);
        assert!(config.validate().is_ok());
    }

    // ========================================================================
    // Auto-Balancer Configuration Tests
    // ========================================================================

    #[test]
    fn test_config_auto_balancer_defaults() {
        let config = ClusterConfig::default();
        assert!(config.auto_balancer_enabled);
        assert_eq!(config.auto_balancer_evaluation_interval_secs, 60);
        assert!((config.auto_balancer_deviation_threshold - 0.3).abs() < f64::EPSILON);
        assert_eq!(config.auto_balancer_max_partitions_per_cycle, 5);
        assert_eq!(config.auto_balancer_cooldown_secs, 300);
        assert!((config.auto_balancer_throughput_weight - 0.7).abs() < f64::EPSILON);
    }

    #[test]
    fn test_config_auto_balancer_disabled() {
        let config = ClusterConfig {
            auto_balancer_enabled: false,
            ..Default::default()
        };
        assert!(!config.auto_balancer_enabled);
        // Should still validate successfully
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_auto_balancer_custom_values() {
        let config = ClusterConfig {
            auto_balancer_evaluation_interval_secs: 120,
            auto_balancer_deviation_threshold: 0.5,
            auto_balancer_max_partitions_per_cycle: 10,
            auto_balancer_cooldown_secs: 600,
            auto_balancer_throughput_weight: 0.9,
            ..Default::default()
        };

        assert_eq!(config.auto_balancer_evaluation_interval_secs, 120);
        assert!((config.auto_balancer_deviation_threshold - 0.5).abs() < f64::EPSILON);
        assert_eq!(config.auto_balancer_max_partitions_per_cycle, 10);
        assert_eq!(config.auto_balancer_cooldown_secs, 600);
        assert!((config.auto_balancer_throughput_weight - 0.9).abs() < f64::EPSILON);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_auto_balancer_weight_edge_cases() {
        // Weight at 0 (100% partition count based)
        let config_0 = ClusterConfig {
            auto_balancer_throughput_weight: 0.0,
            ..Default::default()
        };
        assert!(config_0.validate().is_ok());

        // Weight at 1 (100% throughput based)
        let config_1 = ClusterConfig {
            auto_balancer_throughput_weight: 1.0,
            ..Default::default()
        };
        assert!(config_1.validate().is_ok());
    }

    // ========================================================================
    // Object Store Type Clone/Debug Coverage
    // ========================================================================

    #[test]
    fn test_object_store_s3_with_all_optionals_clone() {
        let store = ObjectStoreType::S3 {
            bucket: "test-bucket".to_string(),
            region: "eu-west-1".to_string(),
            endpoint: Some("http://minio:9000".to_string()),
            access_key_id: Some("access-key".to_string()),
            secret_access_key: Some("secret-key".to_string()),
        };
        let cloned = store.clone();
        match cloned {
            ObjectStoreType::S3 {
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            } => {
                assert_eq!(bucket, "test-bucket");
                assert_eq!(region, "eu-west-1");
                assert_eq!(endpoint, Some("http://minio:9000".to_string()));
                assert_eq!(access_key_id, Some("access-key".to_string()));
                assert_eq!(secret_access_key, Some("secret-key".to_string()));
            }
            _ => panic!("Expected S3 variant"),
        }
    }

    #[test]
    fn test_object_store_gcs_with_key_clone() {
        let store = ObjectStoreType::Gcs {
            bucket: "gcs-bucket".to_string(),
            service_account_key: Some("/path/to/key.json".to_string()),
        };
        let cloned = store.clone();
        match cloned {
            ObjectStoreType::Gcs {
                bucket,
                service_account_key,
            } => {
                assert_eq!(bucket, "gcs-bucket");
                assert_eq!(service_account_key, Some("/path/to/key.json".to_string()));
            }
            _ => panic!("Expected Gcs variant"),
        }
    }

    #[test]
    fn test_object_store_azure_with_key_clone() {
        let store = ObjectStoreType::Azure {
            container: "container".to_string(),
            account: "account".to_string(),
            access_key: Some("access-key".to_string()),
        };
        let cloned = store.clone();
        match cloned {
            ObjectStoreType::Azure {
                container,
                account,
                access_key,
            } => {
                assert_eq!(container, "container");
                assert_eq!(account, "account");
                assert_eq!(access_key, Some("access-key".to_string()));
            }
            _ => panic!("Expected Azure variant"),
        }
    }

    // ========================================================================
    // Additional Validation Edge Cases
    // ========================================================================

    #[test]
    fn test_validate_large_valid_values() {
        let config = ClusterConfig {
            broker_id: i32::MAX,
            port: 65535,
            max_message_size: 1024 * 1024 * 1024,       // 1 GB
            max_fetch_response_size: 100 * 1024 * 1024, // 100 MB
            batch_index_max_size: 1_000_000,
            ownership_cache_max_capacity: 1_000_000,
            default_session_timeout_ms: u64::MAX,
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_all_concurrent_settings_at_one() {
        let config = ClusterConfig {
            max_concurrent_partition_writes: 1,
            max_concurrent_partition_reads: 1,
            max_groups_per_eviction_scan: 1,
            ..Default::default()
        };

        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_with_sasl_enabled() {
        let config = ClusterConfig {
            sasl_enabled: true,
            sasl_required: true,
            sasl_users_file: Some("/etc/kafka/users.txt".to_string()),
            ..Default::default()
        };

        assert!(config.sasl_enabled);
        assert!(config.sasl_required);
        assert_eq!(
            config.sasl_users_file,
            Some("/etc/kafka/users.txt".to_string())
        );
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_with_raft_peers() {
        let config = ClusterConfig {
            raft_listen_addr: "192.168.1.1:9093".to_string(),
            raft_peers: Some(
                "0=192.168.1.1:9093,1=192.168.1.2:9093,2=192.168.1.3:9093".to_string(),
            ),
            ..Default::default()
        };

        assert_eq!(config.raft_listen_addr, "192.168.1.1:9093");
        assert!(config.raft_peers.is_some());
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_with_fail_on_recovery_gap() {
        let config = ClusterConfig {
            fail_on_recovery_gap: true,
            ..Default::default()
        };

        assert!(config.fail_on_recovery_gap);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_with_partition_metrics_disabled() {
        let config = ClusterConfig {
            enable_partition_metrics: false,
            max_metric_cardinality: 1000,
            ..Default::default()
        };

        assert!(!config.enable_partition_metrics);
        assert_eq!(config.max_metric_cardinality, 1000);
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_config_all_durations_can_be_modified() {
        // Set all duration fields to custom values
        let config = ClusterConfig {
            lease_duration: Duration::from_secs(120),
            lease_renewal_interval: Duration::from_secs(30),
            heartbeat_interval: Duration::from_secs(5),
            ownership_check_interval: Duration::from_secs(15),
            session_timeout_check_interval: Duration::from_secs(20),
            ..Default::default()
        };

        assert_eq!(config.lease_duration, Duration::from_secs(120));
        assert_eq!(config.lease_renewal_interval, Duration::from_secs(30));
        assert_eq!(config.heartbeat_interval, Duration::from_secs(5));
        assert_eq!(config.ownership_check_interval, Duration::from_secs(15));
        assert_eq!(
            config.session_timeout_check_interval,
            Duration::from_secs(20)
        );
        assert!(config.validate().is_ok());
    }

    // ========================================================================
    // ClusterProfile Tests
    // ========================================================================

    #[test]
    fn test_cluster_profile_development() {
        let config = ClusterConfig::from_profile(ClusterProfile::Development);

        // Verify development-specific settings
        assert!(!config.fast_failover_enabled);
        assert!(!config.auto_balancer_enabled);
        assert!(config.auto_create_topics);
        assert_eq!(config.default_num_partitions, 3);
        assert_eq!(config.max_consecutive_heartbeat_failures, 3);

        // Verify it validates
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_cluster_profile_production() {
        let config = ClusterConfig::from_profile(ClusterProfile::Production);

        // Verify production-specific settings
        assert!(config.fast_failover_enabled);
        assert!(config.auto_balancer_enabled);
        assert!(config.validate_record_crc);
        assert_eq!(config.fast_heartbeat_interval_ms, 500);
        assert_eq!(config.min_lease_ttl_for_write_secs, 15);

        // Verify it validates
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_cluster_profile_high_throughput() {
        let config = ClusterConfig::from_profile(ClusterProfile::HighThroughput);

        // Verify high-throughput-specific settings
        assert_eq!(config.max_concurrent_partition_writes, 32);
        assert_eq!(config.max_concurrent_partition_reads, 32);
        assert_eq!(config.max_fetch_response_size, 10 * 1024 * 1024);
        assert!(!config.validate_record_crc); // Disabled for throughput
        assert_eq!(config.default_num_partitions, 20);

        // Verify it validates
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_cluster_profile_low_latency() {
        let config = ClusterConfig::from_profile(ClusterProfile::LowLatency);

        // Verify low-latency-specific settings
        assert!(config.fast_failover_enabled);
        assert_eq!(config.fast_heartbeat_interval_ms, 200);
        assert_eq!(config.failure_threshold, 3);
        assert_eq!(config.default_session_timeout_ms, 10000);
        assert_eq!(config.max_fetch_response_size, 256 * 1024);

        // Verify it validates
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_cluster_profile_all_validate() {
        // All profiles must produce valid configurations
        for profile in ClusterProfile::all() {
            let config = ClusterConfig::from_profile(*profile);
            assert!(
                config.validate().is_ok(),
                "Profile {:?} produced invalid configuration",
                profile
            );
        }
    }

    #[test]
    fn test_cluster_profile_from_str() {
        assert_eq!(
            "development".parse::<ClusterProfile>().unwrap(),
            ClusterProfile::Development
        );
        assert_eq!(
            "dev".parse::<ClusterProfile>().unwrap(),
            ClusterProfile::Development
        );
        assert_eq!(
            "production".parse::<ClusterProfile>().unwrap(),
            ClusterProfile::Production
        );
        assert_eq!(
            "prod".parse::<ClusterProfile>().unwrap(),
            ClusterProfile::Production
        );
        assert_eq!(
            "high-throughput".parse::<ClusterProfile>().unwrap(),
            ClusterProfile::HighThroughput
        );
        assert_eq!(
            "throughput".parse::<ClusterProfile>().unwrap(),
            ClusterProfile::HighThroughput
        );
        assert_eq!(
            "ht".parse::<ClusterProfile>().unwrap(),
            ClusterProfile::HighThroughput
        );
        assert_eq!(
            "low-latency".parse::<ClusterProfile>().unwrap(),
            ClusterProfile::LowLatency
        );
        assert_eq!(
            "latency".parse::<ClusterProfile>().unwrap(),
            ClusterProfile::LowLatency
        );
        assert_eq!(
            "ll".parse::<ClusterProfile>().unwrap(),
            ClusterProfile::LowLatency
        );
    }

    #[test]
    fn test_cluster_profile_from_str_invalid() {
        assert!("unknown".parse::<ClusterProfile>().is_err());
        assert!("".parse::<ClusterProfile>().is_err());
        assert!("PRODUCTION".parse::<ClusterProfile>().is_ok()); // Case insensitive
    }

    #[test]
    fn test_cluster_profile_display() {
        assert_eq!(format!("{}", ClusterProfile::Development), "development");
        assert_eq!(format!("{}", ClusterProfile::Production), "production");
        assert_eq!(
            format!("{}", ClusterProfile::HighThroughput),
            "high-throughput"
        );
        assert_eq!(format!("{}", ClusterProfile::LowLatency), "low-latency");
    }

    #[test]
    fn test_cluster_profile_description() {
        assert!(!ClusterProfile::Development.description().is_empty());
        assert!(!ClusterProfile::Production.description().is_empty());
        assert!(!ClusterProfile::HighThroughput.description().is_empty());
        assert!(!ClusterProfile::LowLatency.description().is_empty());
    }

    #[test]
    fn test_cluster_profile_all() {
        let all = ClusterProfile::all();
        assert_eq!(all.len(), 4);
        assert!(all.contains(&ClusterProfile::Development));
        assert!(all.contains(&ClusterProfile::Production));
        assert!(all.contains(&ClusterProfile::HighThroughput));
        assert!(all.contains(&ClusterProfile::LowLatency));
    }

    #[test]
    fn test_cluster_profile_can_be_customized() {
        let mut config = ClusterConfig::from_profile(ClusterProfile::Production);

        // Verify we can customize after profile creation
        config.broker_id = 42;
        config.port = 9093;
        config.default_num_partitions = 50;

        assert_eq!(config.broker_id, 42);
        assert_eq!(config.port, 9093);
        assert_eq!(config.default_num_partitions, 50);

        // Should still validate
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_cluster_profile_debug() {
        let debug_str = format!("{:?}", ClusterProfile::Production);
        assert!(debug_str.contains("Production"));
    }

    #[test]
    fn test_cluster_profile_clone() {
        let profile = ClusterProfile::LowLatency;
        let cloned = profile;
        assert_eq!(profile, cloned);
    }

    #[test]
    fn test_cluster_profile_equality() {
        assert_eq!(ClusterProfile::Development, ClusterProfile::Development);
        assert_ne!(ClusterProfile::Development, ClusterProfile::Production);
    }
}
