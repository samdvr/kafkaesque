//! Additional tests for RaftConfig.

use kafkaesque::cluster::RaftConfig;

#[test]
fn test_raft_config_custom_values() {
    let config = RaftConfig {
        node_id: 42,
        broker_id: 42,
        host: "raft-node-42.local".to_string(),
        port: 9192,
        raft_addr: "raft-node-42.local:9193".to_string(),
        ..RaftConfig::default()
    };

    assert_eq!(config.node_id, 42);
    assert_eq!(config.broker_id, 42);
    assert_eq!(config.host, "raft-node-42.local");
    assert_eq!(config.port, 9192);
    assert_eq!(config.raft_addr, "raft-node-42.local:9193");
}

#[test]
fn test_raft_config_timing_values() {
    let config = RaftConfig {
        heartbeat_interval: std::time::Duration::from_millis(200),
        election_timeout_min: std::time::Duration::from_millis(400),
        election_timeout_max: std::time::Duration::from_millis(800),
        ..RaftConfig::default()
    };

    assert_eq!(
        config.heartbeat_interval,
        std::time::Duration::from_millis(200)
    );
    assert_eq!(
        config.election_timeout_min,
        std::time::Duration::from_millis(400)
    );
    assert_eq!(
        config.election_timeout_max,
        std::time::Duration::from_millis(800)
    );
}

#[test]
fn test_raft_config_cluster_members() {
    let config = RaftConfig {
        cluster_members: vec![
            (0, "host0:9093".to_string()),
            (1, "host1:9093".to_string()),
            (2, "host2:9093".to_string()),
        ],
        ..RaftConfig::default()
    };

    assert_eq!(config.cluster_members.len(), 3);
}
