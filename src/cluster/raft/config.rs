//! Configuration for the Raft consensus layer.

use std::time::Duration;

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
}

impl Default for RaftConfig {
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
            ..Default::default()
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
}
