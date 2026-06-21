//! Configuration for the Raft consensus layer.

use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

use super::auth::RaftAuthKeys;
use super::hash::HASH_ALGO;
use super::tls::RaftTlsConfig;

/// Configuration for a Raft node.
#[derive(Debug, Clone)]
pub struct RaftConfig {
    /// This node's unique ID (must be unique across cluster).
    /// By convention, this equals the broker_id.
    pub node_id: u64,

    /// This node's broker ID (for Kafka protocol).
    pub broker_id: i32,

    /// Host address for Kafka clients.
    pub host: String,

    /// Port for Kafka clients.
    pub port: i32,

    /// Address for Raft communication (separate from Kafka port).
    pub raft_addr: String,

    /// Addresses of other Raft nodes in the cluster.
    /// Format: "node_id=host:port,node_id=host:port,..."
    pub cluster_members: Vec<(u64, String)>,

    /// Directory for Raft log storage.
    pub raft_log_dir: String,

    /// Directory for Raft snapshots.
    pub snapshot_dir: String,

    /// Heartbeat interval for Raft leader.
    pub heartbeat_interval: Duration,

    /// Election timeout range (min).
    pub election_timeout_min: Duration,

    /// Election timeout range (max).
    pub election_timeout_max: Duration,

    /// Maximum entries per AppendEntries RPC.
    pub max_payload_entries: u64,

    /// Snapshot policy: create snapshot after N log entries.
    pub snapshot_threshold: u64,

    /// Whether this node should start as a voter.
    pub is_voter: bool,

    // ========================================================================
    // Coordination settings (matching ClusterConfig)
    // ========================================================================
    /// How long a partition ownership lease lasts.
    pub lease_duration: Duration,

    /// How often to renew partition leases.
    pub lease_renewal_interval: Duration,

    /// How often to send broker heartbeats (Raft heartbeat is separate).
    pub broker_heartbeat_interval: Duration,

    /// TTL for broker heartbeat before considering broker dead.
    pub broker_heartbeat_ttl: Duration,

    /// Default session timeout for consumer group members.
    pub default_session_timeout_ms: u64,

    /// How often to check for expired members.
    pub session_timeout_check_interval: Duration,

    /// Whether to automatically create topics.
    pub auto_create_topics: bool,

    /// Maximum partitions per topic when auto-creating.
    pub max_partitions_per_topic: i32,

    // ========================================================================
    // Backpressure settings
    // ========================================================================
    /// Maximum number of pending Raft proposals before backpressure kicks in.
    /// This prevents memory exhaustion under heavy metadata operation load.
    /// When this limit is reached, new proposals will wait until slots are available.
    pub max_pending_proposals: usize,

    /// Timeout for waiting on proposal backpressure semaphore.
    /// If a proposal cannot acquire a slot within this time, it fails with an error.
    pub proposal_timeout: Duration,

    /// HMAC keys used to authenticate every Raft RPC frame.
    ///
    /// Loaded from `RAFT_CLUSTER_SECRET` (steady-state traffic) and
    /// `RAFT_JOIN_TOKEN` (`JoinCluster` requests). When neither is set the
    /// Raft port runs unauthenticated — see [`RaftAuthKeys`].
    pub auth_keys: Arc<RaftAuthKeys>,

    /// Optional mTLS configuration for the Raft port. When
    /// `Some`, `RaftRpcServer` requires client certs that chain to the
    /// configured CA, and outbound Raft connections present this broker's
    /// cert. HMAC framing remains in place on top — TLS adds peer
    /// identity and rotation; HMAC adds message integrity plus a
    /// timestamp/nonce replay cache on authenticated frames.
    pub tls: Option<RaftTlsConfig>,

    /// Tolerance buffer applied to lease and member-expiry checks to absorb
    /// leader/broker clock skew. The expiry sweep treats `now - tolerance`
    /// as the effective timestamp, so a leader whose clock jumps slightly
    /// forward doesn't mass-expire valid leases.
    pub clock_skew_tolerance_ms: u64,

    /// Number of metadata Raft shard groups. Topics, consumer groups and
    /// producer-id-keyed state are partitioned across these shards by
    /// xxh3_64. Persisted in `cluster_meta.bin` at bootstrap; a restart with
    /// a different value refuses to start.
    ///
    /// Default 8. Validated to `1..=1024` at config load.
    pub metadata_shards: u16,
}

/// Parse a Duration in milliseconds from an environment variable, falling
/// back to `default` when the variable is unset or empty. Returns an error
/// only on a malformed value, so an operator's typo can't silently revert
/// to a different timing regime.
fn parse_env_duration_ms(
    var: &str,
    default: Duration,
) -> Result<Duration, Box<dyn std::error::Error>> {
    match std::env::var(var) {
        Ok(s) if !s.is_empty() => {
            let ms: u64 = s
                .parse()
                .map_err(|e| format!("Invalid {} (expected milliseconds): {}", var, e))?;
            Ok(Duration::from_millis(ms))
        }
        _ => Ok(default),
    }
}

impl Default for RaftConfig {
    /// Defaults tuned for **in-memory tests / local-disk Raft**, not
    /// production. Heartbeat 100 ms / election 200-400 ms keeps the unit
    /// test suite fast.
    ///
    /// Production callers go through [`RaftConfig::from_env`], which
    /// applies object-store-aware defaults (500 ms / 1500-3000 ms). Override
    /// either with `RAFT_HEARTBEAT_INTERVAL_MS`,
    /// `RAFT_ELECTION_TIMEOUT_MIN_MS`, `RAFT_ELECTION_TIMEOUT_MAX_MS`.
    fn default() -> Self {
        Self {
            node_id: 0,
            broker_id: 0,
            host: "127.0.0.1".to_string(),
            port: 9092,
            raft_addr: "127.0.0.1:9191".to_string(),
            cluster_members: Vec::new(),
            raft_log_dir: "/tmp/kafkaesque-raft/log".to_string(),
            snapshot_dir: "/tmp/kafkaesque-raft/snapshots".to_string(),
            heartbeat_interval: Duration::from_millis(100),
            election_timeout_min: Duration::from_millis(200),
            election_timeout_max: Duration::from_millis(400),
            max_payload_entries: 100,
            snapshot_threshold: 1_000,
            is_voter: true,
            lease_duration: Duration::from_secs(60),
            lease_renewal_interval: Duration::from_secs(20),
            broker_heartbeat_interval: Duration::from_secs(10),
            broker_heartbeat_ttl: Duration::from_secs(30),
            default_session_timeout_ms: 30_000,
            session_timeout_check_interval: Duration::from_secs(10),
            auto_create_topics: true,
            max_partitions_per_topic: 1000,
            max_pending_proposals: 1000,
            proposal_timeout: Duration::from_secs(30),
            auth_keys: Arc::new(RaftAuthKeys::default()),
            tls: None,
            clock_skew_tolerance_ms: 5_000,
            metadata_shards: 8,
        }
    }
}

impl RaftConfig {
    /// Create config from environment variables.
    pub fn from_env() -> Result<Self, Box<dyn std::error::Error>> {
        let defaults = Self::default();

        let node_id: u64 = std::env::var("RAFT_NODE_ID")
            .or_else(|_| std::env::var("BROKER_ID"))
            .unwrap_or_else(|_| "0".to_string())
            .parse()
            .map_err(|e| format!("Invalid RAFT_NODE_ID: {}", e))?;

        let broker_id = node_id as i32;

        let host = std::env::var("HOST").unwrap_or_else(|_| "0.0.0.0".to_string());

        let port: i32 = std::env::var("PORT")
            .unwrap_or_else(|_| "9092".to_string())
            .parse()
            .map_err(|e| format!("Invalid PORT: {}", e))?;

        let raft_port: i32 = std::env::var("RAFT_PORT")
            .unwrap_or_else(|_| "9191".to_string())
            .parse()
            .map_err(|e| format!("Invalid RAFT_PORT: {}", e))?;

        let raft_addr = format!("{}:{}", host, raft_port);

        // Parse cluster members from RAFT_CLUSTER_MEMBERS
        // Format: "0=host1:port1,1=host2:port2,2=host3:port3"
        let cluster_members: Vec<(u64, String)> = std::env::var("RAFT_CLUSTER_MEMBERS")
            .unwrap_or_default()
            .split(',')
            .filter(|s| !s.is_empty())
            .filter_map(|s| {
                let parts: Vec<&str> = s.split('=').collect();
                if parts.len() == 2 {
                    parts[0]
                        .parse::<u64>()
                        .ok()
                        .map(|id| (id, parts[1].to_string()))
                } else {
                    None
                }
            })
            .collect();

        let raft_log_dir = std::env::var("RAFT_LOG_DIR")
            .unwrap_or_else(|_| format!("/tmp/kafkaesque-raft/log/{}", node_id));

        let snapshot_dir = std::env::var("RAFT_SNAPSHOT_DIR")
            .unwrap_or_else(|_| format!("/tmp/kafkaesque-raft/snapshots/{}", node_id));

        let auto_create_topics = std::env::var("AUTO_CREATE_TOPICS")
            .map(|v| v.to_lowercase() != "false" && v != "0")
            .unwrap_or(true);

        // Object-store-aware Raft timing defaults.
        //
        // `RaftConfig::default()` keeps fast in-memory test values
        // (100ms / 200-400ms). Production runs the Raft log behind SlateDB
        // on object storage, where P99 read/write latency routinely crosses
        // 200ms — the default election window then triggers spurious leader
        // changes under load. Outside tests we therefore raise the floor:
        // 500ms heartbeat, 1500-3000ms election. Operators on local-disk
        // deployments can tighten via env var.
        let heartbeat_interval =
            parse_env_duration_ms("RAFT_HEARTBEAT_INTERVAL_MS", Duration::from_millis(500))?;
        let election_timeout_min =
            parse_env_duration_ms("RAFT_ELECTION_TIMEOUT_MIN_MS", Duration::from_millis(1500))?;
        let election_timeout_max =
            parse_env_duration_ms("RAFT_ELECTION_TIMEOUT_MAX_MS", Duration::from_millis(3000))?;

        let metadata_shards: u16 = match std::env::var("RAFT_METADATA_SHARDS") {
            Ok(s) if !s.is_empty() => s
                .parse::<u16>()
                .map_err(|e| format!("Invalid RAFT_METADATA_SHARDS: {}", e))?,
            _ => defaults.metadata_shards,
        };

        Ok(Self {
            node_id,
            broker_id,
            host,
            port,
            raft_addr,
            cluster_members,
            raft_log_dir,
            snapshot_dir,
            auto_create_topics,
            heartbeat_interval,
            election_timeout_min,
            election_timeout_max,
            auth_keys: Arc::new(RaftAuthKeys::from_env()),
            metadata_shards,
            ..defaults
        })
    }

    /// Validate the configuration.
    pub fn validate(&self) -> Result<(), Vec<String>> {
        let mut errors = Vec::new();

        if self.port < 1 || self.port > 65535 {
            errors.push(format!("port ({}) must be between 1 and 65535", self.port));
        }

        if self.election_timeout_min >= self.election_timeout_max {
            errors.push(format!(
                "election_timeout_min ({:?}) must be less than election_timeout_max ({:?})",
                self.election_timeout_min, self.election_timeout_max
            ));
        }

        if self.heartbeat_interval >= self.election_timeout_min {
            errors.push(format!(
                "heartbeat_interval ({:?}) should be much less than election_timeout_min ({:?})",
                self.heartbeat_interval, self.election_timeout_min
            ));
        }

        if self.lease_renewal_interval >= self.lease_duration {
            errors.push(format!(
                "lease_renewal_interval ({:?}) must be less than lease_duration ({:?})",
                self.lease_renewal_interval, self.lease_duration
            ));
        }

        if self.metadata_shards == 0 || self.metadata_shards > 1024 {
            errors.push(format!(
                "metadata_shards ({}) must be in 1..=1024",
                self.metadata_shards
            ));
        }

        if errors.is_empty() {
            Ok(())
        } else {
            Err(errors)
        }
    }

    /// Get openraft Config from this RaftConfig.
    pub fn to_openraft_config(&self) -> openraft::Config {
        openraft::Config {
            cluster_name: "kafkaesque-cluster".to_string(),
            heartbeat_interval: self.heartbeat_interval.as_millis() as u64,
            election_timeout_min: self.election_timeout_min.as_millis() as u64,
            election_timeout_max: self.election_timeout_max.as_millis() as u64,
            max_payload_entries: self.max_payload_entries,
            snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(self.snapshot_threshold),
            ..Default::default()
        }
    }

    /// Create RaftConfig from ClusterConfig.
    pub fn from_cluster_config(config: &crate::cluster::config::ClusterConfig) -> Self {
        // Parse raft peers if provided
        // Format: "0=host:port,1=host:port,..."
        let cluster_members: Vec<(u64, String)> = config
            .raft_peers
            .as_ref()
            .map(|peers| {
                peers
                    .split(',')
                    .filter(|s| !s.is_empty())
                    .filter_map(|s| {
                        let parts: Vec<&str> = s.split('=').collect();
                        if parts.len() == 2 {
                            parts[0]
                                .parse::<u64>()
                                .ok()
                                .map(|id| (id, parts[1].to_string()))
                        } else {
                            None
                        }
                    })
                    .collect()
            })
            .unwrap_or_default();

        Self {
            node_id: config.broker_id as u64,
            broker_id: config.broker_id,
            host: config.advertised_host.clone(),
            port: config.port,
            raft_addr: config.raft_listen_addr.clone(),
            cluster_members,
            raft_log_dir: format!("{}/raft/log/{}", config.object_store_path, config.broker_id),
            snapshot_dir: format!(
                "{}/raft/snapshots/{}",
                config.object_store_path, config.broker_id
            ),
            lease_duration: config.lease_duration,
            lease_renewal_interval: config.lease_renewal_interval,
            broker_heartbeat_interval: config.heartbeat_interval,
            broker_heartbeat_ttl: std::time::Duration::from_secs(config.broker_heartbeat_ttl_secs),
            default_session_timeout_ms: config.default_session_timeout_ms,
            session_timeout_check_interval: config.session_timeout_check_interval,
            auto_create_topics: config.auto_create_topics,
            max_partitions_per_topic: config.max_partitions_per_topic,
            snapshot_threshold: config.raft_snapshot_threshold,
            // Load HMAC keys from env. Default-off (legacy behavior); when set,
            // every Raft RPC must present a matching signature.
            auth_keys: Arc::new(RaftAuthKeys::from_env()),
            // Load mTLS config from env. When the three
            // RAFT_TLS_* vars are set, the Raft port runs TLS on top of
            // HMAC framing. Errors propagate out so a half-configured
            // cluster fails loudly rather than silently dropping mTLS.
            tls: RaftTlsConfig::from_env().unwrap_or(None),
            clock_skew_tolerance_ms: config.clock_skew_tolerance_ms,
            ..Default::default()
        }
    }
}

// ============================================================================
// cluster_meta.bin — bootstrap-time pinning of metadata_shards / hash_algo
// ============================================================================
//
// At first boot a broker writes the chosen `metadata_shards` plus the hash
// algorithm name to `{raft_log_dir}/node-{id}/cluster_meta.bin`. On every
// subsequent boot the file is read back and the configured values must
// match exactly, otherwise the broker refuses to start. This catches the
// most dangerous silent-corruption mode of the sharded layout: an operator
// changes `RAFT_METADATA_SHARDS` between restarts and every key reshards
// onto a different group.
//
// **Per-node directory** (`node-{id}` segment): brokers in a shared
// development setup may point multiple `node_id`s at the same
// `raft_log_dir` parent. Per-node scoping ensures each broker validates
// its OWN bootstrap independently; without it, the first broker to boot
// would write the file and every subsequent broker would inherit its
// settings without revisiting them.

/// Persisted bootstrap descriptor for the sharded metadata Raft layout.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ClusterMeta {
    /// Number of shard groups pinned at bootstrap.
    pub metadata_shards: u16,
    /// Cluster-stable hash algorithm name. See `super::hash::HASH_ALGO`.
    pub hash_algo: String,
}

impl ClusterMeta {
    /// Path to `cluster_meta.bin` for a given Raft log directory. The file
    /// lives directly under the directory passed in — callers that want
    /// per-node scoping pass `{raft_log_dir}/node-{id}` here.
    pub fn path_in(dir: impl AsRef<Path>) -> PathBuf {
        dir.as_ref().join("cluster_meta.bin")
    }

    /// Load `cluster_meta.bin` from `dir`. Returns `Ok(None)` when
    /// the file does not exist (first boot); errors only on I/O failure or
    /// a corrupt file (malformed magic / unknown layout).
    pub fn load(dir: impl AsRef<Path>) -> std::io::Result<Option<Self>> {
        let path = Self::path_in(dir);
        match std::fs::read(&path) {
            Ok(bytes) => postcard::from_bytes::<ClusterMeta>(&bytes).map(Some).map_err(|e| {
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    format!("cluster_meta.bin at {} is corrupt: {}", path.display(), e),
                )
            }),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(None),
            Err(e) => Err(e),
        }
    }

    /// Write `cluster_meta.bin` atomically (write-temp-and-rename) so a crash
    /// mid-write cannot leave a half-written file that fails the next boot.
    pub fn save(&self, dir: impl AsRef<Path>) -> std::io::Result<()> {
        let dir = dir.as_ref();
        std::fs::create_dir_all(dir)?;
        let final_path = Self::path_in(dir);
        let tmp_path = dir.join("cluster_meta.bin.tmp");
        let bytes = postcard::to_stdvec(self).map_err(|e| {
            std::io::Error::other(format!("encoding cluster_meta.bin: {}", e))
        })?;
        std::fs::write(&tmp_path, &bytes)?;
        std::fs::rename(&tmp_path, &final_path)?;
        Ok(())
    }
}

impl RaftConfig {
    /// Per-node directory holding `cluster_meta.bin` and the per-group log
    /// + snapshot subtrees: `{raft_log_dir}/node-{node_id}`.
    ///
    /// Public so the cluster bootstrap can use the same path it does for
    /// log directories — drift between this helper and the bootstrap
    /// would surface as a broker that validates a meta file under a
    /// different directory than the one it actually persists logs into.
    pub fn node_dir(&self) -> PathBuf {
        Path::new(&self.raft_log_dir).join(format!("node-{}", self.node_id))
    }

    /// Validate against an on-disk `cluster_meta.bin`, persisting the
    /// configured values on first boot.
    ///
    /// Behaviour:
    /// - If `cluster_meta.bin` does not exist: write the configured
    ///   `metadata_shards` + `hash_algo` and return `Ok(())`.
    /// - If it exists and matches: return `Ok(())`.
    /// - If it exists and differs: return a descriptive error so the broker
    ///   refuses to start. The mismatch is unrecoverable without a full
    ///   re-bootstrap — every key would be on the wrong shard.
    ///
    /// Reads from / writes to [`Self::node_dir`] (i.e.
    /// `{raft_log_dir}/node-{node_id}/cluster_meta.bin`) per the
    /// sharding plan's per-node layout.
    pub fn validate_or_init_cluster_meta(&self) -> Result<(), String> {
        let dir = self.node_dir();
        match ClusterMeta::load(&dir).map_err(|e| e.to_string())? {
            Some(persisted) => {
                if persisted.metadata_shards != self.metadata_shards {
                    return Err(format!(
                        "cluster bootstrapped with metadata_shards={}, config says {} — refusing",
                        persisted.metadata_shards, self.metadata_shards
                    ));
                }
                if persisted.hash_algo != HASH_ALGO {
                    return Err(format!(
                        "cluster bootstrapped with hash_algo={:?}, build expects {:?} — refusing",
                        persisted.hash_algo, HASH_ALGO
                    ));
                }
                Ok(())
            }
            None => {
                let meta = ClusterMeta {
                    metadata_shards: self.metadata_shards,
                    hash_algo: HASH_ALGO.to_string(),
                };
                meta.save(&dir).map_err(|e| {
                    format!("failed to write cluster_meta.bin to {}: {}", dir.display(), e)
                })
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // Default Tests
    // ========================================================================

    #[test]
    fn test_default_values() {
        let config = RaftConfig::default();

        assert_eq!(config.node_id, 0);
        assert_eq!(config.broker_id, 0);
        assert_eq!(config.host, "127.0.0.1");
        assert_eq!(config.port, 9092);
        assert_eq!(config.raft_addr, "127.0.0.1:9191");
        assert!(config.cluster_members.is_empty());
        assert!(config.is_voter);
        assert!(config.auto_create_topics);
    }

    #[test]
    fn test_default_timeouts() {
        let config = RaftConfig::default();

        assert_eq!(config.heartbeat_interval, Duration::from_millis(100));
        assert_eq!(config.election_timeout_min, Duration::from_millis(200));
        assert_eq!(config.election_timeout_max, Duration::from_millis(400));
        assert_eq!(config.lease_duration, Duration::from_secs(60));
        assert_eq!(config.lease_renewal_interval, Duration::from_secs(20));
    }

    #[test]
    fn test_default_thresholds() {
        let config = RaftConfig::default();

        assert_eq!(config.max_payload_entries, 100);
        assert_eq!(config.snapshot_threshold, 1_000);
        assert_eq!(config.max_partitions_per_topic, 1000);
        assert_eq!(config.max_pending_proposals, 1000);
    }

    // ========================================================================
    // Validation Tests
    // ========================================================================

    #[test]
    fn test_validate_success() {
        let config = RaftConfig::default();
        assert!(config.validate().is_ok());
    }

    #[test]
    fn test_validate_invalid_port_zero() {
        let config = RaftConfig {
            port: 0,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("port")));
    }

    #[test]
    fn test_validate_invalid_port_too_high() {
        let config = RaftConfig {
            port: 70000,
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("port")));
    }

    #[test]
    fn test_validate_election_timeout_order() {
        let config = RaftConfig {
            election_timeout_min: Duration::from_millis(500),
            election_timeout_max: Duration::from_millis(400),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("election_timeout_min")));
    }

    #[test]
    fn test_validate_election_timeout_equal() {
        let config = RaftConfig {
            election_timeout_min: Duration::from_millis(300),
            election_timeout_max: Duration::from_millis(300),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_heartbeat_too_slow() {
        let config = RaftConfig {
            heartbeat_interval: Duration::from_millis(300),
            election_timeout_min: Duration::from_millis(200),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.iter().any(|e| e.contains("heartbeat_interval")));
    }

    #[test]
    fn test_validate_lease_renewal_too_long() {
        let config = RaftConfig {
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
    fn test_validate_multiple_errors() {
        let config = RaftConfig {
            port: 0,
            election_timeout_min: Duration::from_millis(500),
            election_timeout_max: Duration::from_millis(400),
            ..Default::default()
        };
        let result = config.validate();
        assert!(result.is_err());
        let errors = result.unwrap_err();
        assert!(errors.len() >= 2);
    }

    // ========================================================================
    // to_openraft_config Tests
    // ========================================================================

    #[test]
    fn test_to_openraft_config() {
        let config = RaftConfig::default();
        let openraft_config = config.to_openraft_config();

        assert_eq!(openraft_config.cluster_name, "kafkaesque-cluster");
        assert_eq!(openraft_config.heartbeat_interval, 100);
        assert_eq!(openraft_config.election_timeout_min, 200);
        assert_eq!(openraft_config.election_timeout_max, 400);
        assert_eq!(openraft_config.max_payload_entries, 100);
    }

    #[test]
    fn test_to_openraft_config_custom_values() {
        let config = RaftConfig {
            heartbeat_interval: Duration::from_millis(50),
            election_timeout_min: Duration::from_millis(100),
            election_timeout_max: Duration::from_millis(200),
            max_payload_entries: 50,
            snapshot_threshold: 5000,
            ..Default::default()
        };

        let openraft_config = config.to_openraft_config();

        assert_eq!(openraft_config.heartbeat_interval, 50);
        assert_eq!(openraft_config.election_timeout_min, 100);
        assert_eq!(openraft_config.election_timeout_max, 200);
        assert_eq!(openraft_config.max_payload_entries, 50);
    }

    // ========================================================================
    // from_cluster_config Tests
    // ========================================================================

    #[test]
    fn test_from_cluster_config_basic() {
        use crate::cluster::config::ClusterConfig;

        let cluster_config = ClusterConfig {
            broker_id: 42,
            port: 9094,
            advertised_host: "192.168.1.100".to_string(),
            raft_listen_addr: "192.168.1.100:19094".to_string(),
            auto_create_topics: false,
            ..Default::default()
        };

        let raft_config = RaftConfig::from_cluster_config(&cluster_config);

        assert_eq!(raft_config.node_id, 42);
        assert_eq!(raft_config.broker_id, 42);
        assert_eq!(raft_config.port, 9094);
        assert_eq!(raft_config.host, "192.168.1.100");
        assert_eq!(raft_config.raft_addr, "192.168.1.100:19094");
        assert!(!raft_config.auto_create_topics);
    }

    #[test]
    fn test_from_cluster_config_with_peers() {
        use crate::cluster::config::ClusterConfig;

        let cluster_config = ClusterConfig {
            broker_id: 1,
            raft_peers: Some("0=host0:9191,2=host2:9191,3=host3:9191".to_string()),
            ..Default::default()
        };

        let raft_config = RaftConfig::from_cluster_config(&cluster_config);

        assert_eq!(raft_config.cluster_members.len(), 3);
        assert!(
            raft_config
                .cluster_members
                .contains(&(0, "host0:9191".to_string()))
        );
        assert!(
            raft_config
                .cluster_members
                .contains(&(2, "host2:9191".to_string()))
        );
        assert!(
            raft_config
                .cluster_members
                .contains(&(3, "host3:9191".to_string()))
        );
    }

    #[test]
    fn test_from_cluster_config_no_peers() {
        use crate::cluster::config::ClusterConfig;

        let cluster_config = ClusterConfig {
            broker_id: 1,
            raft_peers: None,
            ..Default::default()
        };

        let raft_config = RaftConfig::from_cluster_config(&cluster_config);

        assert!(raft_config.cluster_members.is_empty());
    }

    #[test]
    fn test_from_cluster_config_invalid_peer_format() {
        use crate::cluster::config::ClusterConfig;

        let cluster_config = ClusterConfig {
            broker_id: 1,
            // Invalid format - should be filtered out
            raft_peers: Some("invalid,0=valid:9191,also-invalid".to_string()),
            ..Default::default()
        };

        let raft_config = RaftConfig::from_cluster_config(&cluster_config);

        // Only the valid one should be parsed
        assert_eq!(raft_config.cluster_members.len(), 1);
        assert!(
            raft_config
                .cluster_members
                .contains(&(0, "valid:9191".to_string()))
        );
    }

    #[test]
    fn test_from_cluster_config_empty_peers() {
        use crate::cluster::config::ClusterConfig;

        let cluster_config = ClusterConfig {
            broker_id: 1,
            raft_peers: Some("".to_string()),
            ..Default::default()
        };

        let raft_config = RaftConfig::from_cluster_config(&cluster_config);

        assert!(raft_config.cluster_members.is_empty());
    }

    // ========================================================================
    // Debug Trait Tests
    // ========================================================================

    #[test]
    fn test_debug_format() {
        let config = RaftConfig::default();
        let debug = format!("{:?}", config);

        assert!(debug.contains("RaftConfig"));
        assert!(debug.contains("node_id"));
        assert!(debug.contains("broker_id"));
    }

    // ========================================================================
    // Clone Trait Tests
    // ========================================================================

    #[test]
    fn test_clone() {
        let config = RaftConfig::default();
        let cloned = config.clone();

        assert_eq!(config.node_id, cloned.node_id);
        assert_eq!(config.broker_id, cloned.broker_id);
        assert_eq!(config.host, cloned.host);
        assert_eq!(config.port, cloned.port);
    }

    // ========================================================================
    // metadata_shards / cluster_meta.bin Tests
    // ========================================================================

    fn unique_tmp_dir(label: &str) -> PathBuf {
        let pid = std::process::id();
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        let path = std::env::temp_dir().join(format!("kafkaesque-cm-{label}-{pid}-{nanos}"));
        std::fs::create_dir_all(&path).unwrap();
        path
    }

    #[test]
    fn metadata_shards_default_is_eight() {
        assert_eq!(RaftConfig::default().metadata_shards, 8);
    }

    #[test]
    fn validate_rejects_zero_metadata_shards() {
        let cfg = RaftConfig {
            metadata_shards: 0,
            ..Default::default()
        };
        let errors = cfg.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("metadata_shards")));
    }

    #[test]
    fn validate_rejects_too_many_metadata_shards() {
        let cfg = RaftConfig {
            metadata_shards: 1025,
            ..Default::default()
        };
        let errors = cfg.validate().unwrap_err();
        assert!(errors.iter().any(|e| e.contains("metadata_shards")));
    }

    #[test]
    fn cluster_meta_first_boot_writes_file() {
        let dir = unique_tmp_dir("first-boot");
        let cfg = RaftConfig {
            raft_log_dir: dir.to_string_lossy().to_string(),
            node_id: 7,
            metadata_shards: 4,
            ..Default::default()
        };
        cfg.validate_or_init_cluster_meta().unwrap();
        // Per-node scoping: file lands under node-{id}, NOT the parent.
        let on_disk = ClusterMeta::load(cfg.node_dir())
            .unwrap()
            .expect("file written");
        assert_eq!(on_disk.metadata_shards, 4);
        assert_eq!(on_disk.hash_algo, HASH_ALGO);
        // No file at the parent — this used to be the layout pre-step-9
        // and would shadow per-node validation.
        assert!(
            !ClusterMeta::path_in(&dir).exists(),
            "no file expected at the parent dir, only under node-{{id}}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cluster_meta_match_on_restart_succeeds() {
        let dir = unique_tmp_dir("restart-match");
        let cfg = RaftConfig {
            raft_log_dir: dir.to_string_lossy().to_string(),
            node_id: 1,
            metadata_shards: 16,
            ..Default::default()
        };
        cfg.validate_or_init_cluster_meta().unwrap();
        // Second boot with the same values: no error, no rewrite needed.
        cfg.validate_or_init_cluster_meta().unwrap();
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cluster_meta_mismatch_refuses_with_exact_error() {
        let dir = unique_tmp_dir("restart-mismatch");
        let initial = RaftConfig {
            raft_log_dir: dir.to_string_lossy().to_string(),
            node_id: 1,
            metadata_shards: 8,
            ..Default::default()
        };
        initial.validate_or_init_cluster_meta().unwrap();
        let changed = RaftConfig {
            raft_log_dir: dir.to_string_lossy().to_string(),
            node_id: 1,
            metadata_shards: 16,
            ..Default::default()
        };
        let err = changed.validate_or_init_cluster_meta().unwrap_err();
        assert!(
            err.contains("metadata_shards=8") && err.contains("config says 16"),
            "unexpected error: {err}"
        );
        // Pin the exact wording the plan calls out, so a refactor that
        // reorders the `format!` arguments doesn't silently break alerts /
        // operator runbooks keyed on this string.
        assert_eq!(
            err,
            "cluster bootstrapped with metadata_shards=8, config says 16 — refusing"
        );
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cluster_meta_corrupt_file_returns_error() {
        let dir = unique_tmp_dir("corrupt");
        let cfg = RaftConfig {
            raft_log_dir: dir.to_string_lossy().to_string(),
            node_id: 1,
            ..Default::default()
        };
        std::fs::create_dir_all(cfg.node_dir()).unwrap();
        std::fs::write(
            ClusterMeta::path_in(cfg.node_dir()),
            b"\xff\xff\xff not-postcard",
        )
        .unwrap();
        let err = cfg.validate_or_init_cluster_meta().unwrap_err();
        assert!(err.contains("corrupt"), "unexpected error: {err}");
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cluster_meta_per_node_paths_dont_collide() {
        // Two brokers sharing one `raft_log_dir` parent must each
        // validate against their OWN node-{id}/cluster_meta.bin. Without
        // per-node scoping, the first boot's file would shadow every
        // subsequent broker's check.
        let dir = unique_tmp_dir("per-node");
        let cfg_a = RaftConfig {
            raft_log_dir: dir.to_string_lossy().to_string(),
            node_id: 1,
            metadata_shards: 4,
            ..Default::default()
        };
        let cfg_b = RaftConfig {
            raft_log_dir: dir.to_string_lossy().to_string(),
            node_id: 2,
            metadata_shards: 8,
            ..Default::default()
        };
        cfg_a.validate_or_init_cluster_meta().unwrap();
        // B is not corrupted by A's file — it writes its own.
        cfg_b.validate_or_init_cluster_meta().unwrap();
        let a_meta = ClusterMeta::load(cfg_a.node_dir()).unwrap().unwrap();
        let b_meta = ClusterMeta::load(cfg_b.node_dir()).unwrap().unwrap();
        assert_eq!(a_meta.metadata_shards, 4);
        assert_eq!(b_meta.metadata_shards, 8);
        std::fs::remove_dir_all(&dir).ok();
    }

    #[test]
    fn cluster_meta_hash_algo_mismatch_refuses() {
        // Defensive: if a future build ever ships a different hash algorithm
        // and a broker boots against a `cluster_meta.bin` written by an old
        // build, refuse rather than silently re-shard every key. The error
        // must mention `hash_algo` so the operator can see the cause.
        let dir = unique_tmp_dir("hash-mismatch");
        let cfg = RaftConfig {
            raft_log_dir: dir.to_string_lossy().to_string(),
            node_id: 1,
            metadata_shards: 8,
            ..Default::default()
        };
        std::fs::create_dir_all(cfg.node_dir()).unwrap();
        let foreign = ClusterMeta {
            metadata_shards: 8,
            hash_algo: "sha256-alien".to_string(),
        };
        foreign.save(cfg.node_dir()).unwrap();
        let err = cfg.validate_or_init_cluster_meta().unwrap_err();
        assert!(
            err.contains("hash_algo") && err.contains("sha256-alien"),
            "unexpected error: {err}"
        );
        std::fs::remove_dir_all(&dir).ok();
    }
}
