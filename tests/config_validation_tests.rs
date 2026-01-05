//! Integration tests for ClusterConfig.
//!
//! These tests verify ClusterConfig validation logic and defaults.

use std::time::Duration;

use kafkaesque::cluster::{ClusterConfig, ObjectStoreType};

// ============================================================================
// ObjectStoreType Tests
// ============================================================================

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
fn test_object_store_type_s3() {
    let store = ObjectStoreType::S3 {
        bucket: "my-bucket".to_string(),
        region: "us-west-2".to_string(),
        endpoint: Some("http://localhost:9000".to_string()),
        access_key_id: Some("AKIAIOSFODNN7EXAMPLE".to_string()),
        secret_access_key: Some("secret".to_string()),
    };

    let debug_str = format!("{:?}", store);
    assert!(debug_str.contains("S3"));
    assert!(debug_str.contains("my-bucket"));
    assert!(debug_str.contains("us-west-2"));
}

#[test]
fn test_object_store_type_gcs() {
    let store = ObjectStoreType::Gcs {
        bucket: "gcs-bucket".to_string(),
        service_account_key: Some("/path/to/key.json".to_string()),
    };

    let debug_str = format!("{:?}", store);
    assert!(debug_str.contains("Gcs"));
    assert!(debug_str.contains("gcs-bucket"));
}

#[test]
fn test_object_store_type_azure() {
    let store = ObjectStoreType::Azure {
        container: "azure-container".to_string(),
        account: "myaccount".to_string(),
        access_key: Some("key123".to_string()),
    };

    let debug_str = format!("{:?}", store);
    assert!(debug_str.contains("Azure"));
    assert!(debug_str.contains("azure-container"));
    assert!(debug_str.contains("myaccount"));
}

#[test]
fn test_object_store_type_clone() {
    let store = ObjectStoreType::Local {
        path: "/custom/path".to_string(),
    };
    let cloned = store.clone();

    match cloned {
        ObjectStoreType::Local { path } => assert_eq!(path, "/custom/path"),
        _ => panic!("Expected Local variant"),
    }
}

// ============================================================================
// ClusterConfig Default Tests
// ============================================================================

#[test]
fn test_cluster_config_default() {
    let config = ClusterConfig::default();

    assert_eq!(config.broker_id, 0);
    assert_eq!(config.host, "127.0.0.1");
    assert_eq!(config.advertised_host, "127.0.0.1");
    assert_eq!(config.port, 9092);
    assert_eq!(config.cluster_id, "kafkaesque-cluster");
    assert_eq!(config.object_store_path, "kafkaesque-data");
    assert_eq!(config.lease_duration, Duration::from_secs(60));
    assert_eq!(config.lease_renewal_interval, Duration::from_secs(20));
    assert!(config.auto_create_topics);
    assert_eq!(config.max_partitions_per_topic, 1000);
    assert_eq!(config.max_auto_created_topics, 10_000);
    assert_eq!(config.max_message_size, 100 * 1024 * 1024);
    assert_eq!(config.max_fetch_response_size, 1024 * 1024);
    assert!(!config.sasl_enabled);
    assert!(config.validate_record_crc);
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
fn test_cluster_config_clone() {
    let config = ClusterConfig::default();
    let cloned = config.clone();
    assert_eq!(cloned.broker_id, config.broker_id);
    assert_eq!(cloned.port, config.port);
    assert_eq!(cloned.cluster_id, config.cluster_id);
}

// ============================================================================
// ClusterConfig Validation Tests
// ============================================================================

#[test]
fn test_validate_default_config_succeeds() {
    let config = ClusterConfig::default();
    assert!(config.validate().is_ok());
}

#[test]
fn test_validate_lease_renewal_greater_than_duration() {
    let config = ClusterConfig {
        lease_renewal_interval: Duration::from_secs(120),
        lease_duration: Duration::from_secs(60),
        ..ClusterConfig::default()
    };

    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("lease_renewal_interval")));
}

#[test]
fn test_validate_heartbeat_interval_greater_than_ttl() {
    let config = ClusterConfig {
        heartbeat_interval: Duration::from_secs(60),
        broker_heartbeat_ttl_secs: 30,
        ..ClusterConfig::default()
    };

    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("heartbeat_interval")));
}

#[test]
fn test_validate_ownership_cache_ttl_too_high() {
    let config = ClusterConfig {
        ownership_cache_ttl_secs: 15,
        ..ClusterConfig::default()
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
fn test_validate_invalid_port_zero() {
    let config = ClusterConfig {
        port: 0,
        ..ClusterConfig::default()
    };

    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("port")));
}

#[test]
fn test_validate_invalid_port_too_high() {
    let config = ClusterConfig {
        port: 70000,
        ..ClusterConfig::default()
    };

    let result = config.validate();
    assert!(result.is_err());
}

#[test]
fn test_validate_negative_broker_id() {
    let config = ClusterConfig {
        broker_id: -1,
        ..ClusterConfig::default()
    };

    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("broker_id")));
}

#[test]
fn test_validate_max_message_size_too_small() {
    let config = ClusterConfig {
        max_message_size: 512,
        ..ClusterConfig::default()
    };

    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.iter().any(|e| e.contains("max_message_size")));
}

#[test]
fn test_validate_max_fetch_response_size_too_small() {
    let config = ClusterConfig {
        max_fetch_response_size: 100,
        ..ClusterConfig::default()
    };

    let result = config.validate();
    assert!(result.is_err());
}

#[test]
fn test_validate_zero_concurrent_partition_writes() {
    let config = ClusterConfig {
        max_concurrent_partition_writes: 0,
        ..ClusterConfig::default()
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
fn test_validate_zero_heartbeat_failures() {
    let config = ClusterConfig {
        max_consecutive_heartbeat_failures: 0,
        ..ClusterConfig::default()
    };

    let result = config.validate();
    assert!(result.is_err());
}

#[test]
fn test_validate_session_timeout_too_low() {
    let config = ClusterConfig {
        default_session_timeout_ms: 500,
        ..ClusterConfig::default()
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
fn test_validate_zero_fetch_timeout() {
    let config = ClusterConfig {
        fetch_timeout_secs: 0,
        ..ClusterConfig::default()
    };

    let result = config.validate();
    assert!(result.is_err());
}

#[test]
fn test_validate_zero_batch_index_max_size() {
    let config = ClusterConfig {
        batch_index_max_size: 0,
        ..ClusterConfig::default()
    };

    let result = config.validate();
    assert!(result.is_err());
}

#[test]
fn test_validate_insufficient_lease_buffer() {
    // Set lease duration just 5 seconds more than renewal (buffer < 10)
    let config = ClusterConfig {
        lease_duration: Duration::from_secs(25),
        lease_renewal_interval: Duration::from_secs(20),
        ..ClusterConfig::default()
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
        fetch_timeout_secs: 0,
        ..ClusterConfig::default()
    };

    let result = config.validate();
    assert!(result.is_err());
    let errors = result.unwrap_err();
    assert!(errors.len() >= 4);
}

#[test]
fn test_validate_valid_boundary_values() {
    // Port at lower boundary
    let config = ClusterConfig {
        port: 1,
        ..ClusterConfig::default()
    };
    assert!(config.validate().is_ok());

    // Port at upper boundary
    let config = ClusterConfig {
        port: 65535,
        ..ClusterConfig::default()
    };
    assert!(config.validate().is_ok());

    // Cache TTL at boundary
    let config = ClusterConfig {
        ownership_cache_ttl_secs: 10,
        ..ClusterConfig::default()
    };
    assert!(config.validate().is_ok());

    // Session timeout at minimum
    let config = ClusterConfig {
        default_session_timeout_ms: 1000,
        ..ClusterConfig::default()
    };
    assert!(config.validate().is_ok());
}

#[test]
fn test_validate_sasl_configuration() {
    let config = ClusterConfig {
        sasl_enabled: true,
        sasl_required: true,
        sasl_users_file: Some("/path/to/users.txt".to_string()),
        ..ClusterConfig::default()
    };

    // SASL configuration doesn't affect validation
    assert!(config.validate().is_ok());
}

#[test]
fn test_raft_peers_configuration() {
    let config = ClusterConfig {
        raft_listen_addr: "0.0.0.0:9093".to_string(),
        raft_peers: Some("0=host1:9093,1=host2:9093".to_string()),
        ..ClusterConfig::default()
    };

    assert!(config.validate().is_ok());
}

// ============================================================================
// Advertised Host Tests
// ============================================================================
// These tests verify the advertised_host feature which ensures clients receive
// a routable address in metadata responses, not the bind address (e.g., 0.0.0.0).

#[test]
fn test_advertised_host_default_matches_host() {
    // When host is a routable address, advertised_host should match by default
    let config = ClusterConfig::default();
    assert_eq!(config.host, "127.0.0.1");
    assert_eq!(config.advertised_host, "127.0.0.1");
}

#[test]
fn test_advertised_host_can_be_set_independently() {
    // advertised_host can differ from host (e.g., bind to all interfaces but advertise specific IP)
    let config = ClusterConfig {
        host: "0.0.0.0".to_string(),
        advertised_host: "192.168.1.100".to_string(),
        ..ClusterConfig::default()
    };

    assert_eq!(config.host, "0.0.0.0");
    assert_eq!(config.advertised_host, "192.168.1.100");
    assert!(config.validate().is_ok());
}

#[test]
fn test_advertised_host_in_debug_output() {
    let config = ClusterConfig {
        advertised_host: "custom.host.example.com".to_string(),
        ..ClusterConfig::default()
    };

    let debug_str = format!("{:?}", config);
    assert!(
        debug_str.contains("advertised_host"),
        "Debug output should contain advertised_host"
    );
    assert!(
        debug_str.contains("custom.host.example.com"),
        "Debug output should contain the advertised_host value"
    );
}

#[test]
fn test_advertised_host_clone() {
    let config = ClusterConfig {
        advertised_host: "advertised.example.com".to_string(),
        ..ClusterConfig::default()
    };

    let cloned = config.clone();
    assert_eq!(cloned.advertised_host, "advertised.example.com");
}
