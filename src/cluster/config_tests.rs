//! Inline tests previously embedded in `config.rs`. Moved to a sibling
//! file so reading the configuration logic isn't gated on scrolling past
//! ~1300 lines of asserts. Behavior is unchanged: `super::*` re-exports
//! the entire config surface, including private helpers, because tests
//! still want to reach `validate_env_security_posture` and friends.

#![allow(clippy::module_inception)]

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
fn test_validate_max_owned_partitions_is_advisory_only() {
    // Unbounded (0) is always valid.
    let unbounded = ClusterConfig {
        max_owned_partitions_per_broker: 0,
        ..Default::default()
    };
    assert!(unbounded.validate().is_ok());

    // A positive cap below default_num_partitions is advisory (warns) but
    // must not be a hard validation error.
    let low_cap = ClusterConfig {
        max_owned_partitions_per_broker: 1,
        default_num_partitions: 8,
        ..Default::default()
    };
    assert!(low_cap.validate().is_ok());

    // A cap at/above the default is fine too.
    let ample_cap = ClusterConfig {
        max_owned_partitions_per_broker: 100,
        default_num_partitions: 8,
        ..Default::default()
    };
    assert!(ample_cap.validate().is_ok());
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
    assert!(
        !debug_str.contains("test-secret"),
        "secret_access_key must not appear in Debug output: {}",
        debug_str
    );
    assert!(
        !debug_str.contains("test-key"),
        "access_key_id must not appear in Debug output: {}",
        debug_str
    );
    assert!(debug_str.contains("<redacted>"));
}

#[test]
fn test_object_store_type_s3_debug_redacts_unique_secret() {
    // Use a value that cannot collide with field names or variant names.
    let secret = "ZZ-UNIQUE-SECRET-AAAA-BBBB-CCCC";
    let store = ObjectStoreType::S3 {
        bucket: "b".to_string(),
        region: "r".to_string(),
        endpoint: None,
        access_key_id: None,
        secret_access_key: Some(secret.to_string()),
    };
    let debug_str = format!("{:?}", store);
    assert!(
        !debug_str.contains(secret),
        "Debug output leaked the secret: {}",
        debug_str
    );
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
        access_key: Some("ZZ-AZURE-KEY-XXXX".to_string()),
    };
    let debug_str = format!("{:?}", store);
    assert!(debug_str.contains("Azure"));
    assert!(debug_str.contains("test-container"));
    assert!(
        !debug_str.contains("ZZ-AZURE-KEY-XXXX"),
        "Azure access_key must not appear in Debug output: {}",
        debug_str
    );
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
        object_store: ObjectStoreType::S3 {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        },
        ..Default::default()
    };

    let result = config.validate();
    let errs = result.expect_err("empty cluster_id with S3 backend must fail validation");
    assert!(
        errs.iter().any(|e| e.contains("cluster_id")),
        "expected cluster_id error, got: {errs:?}"
    );
}

#[test]
fn test_validate_empty_cluster_id_allowed_for_local() {
    let config = ClusterConfig {
        cluster_id: String::new(),
        object_store: ObjectStoreType::Local {
            path: "/tmp/test".to_string(),
        },
        ..Default::default()
    };
    assert!(config.validate().is_ok());
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
    assert!(config.fail_on_recovery_gap);
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
    assert_eq!(config.max_consecutive_heartbeat_failures, 3);
    assert_eq!(config.batch_index_max_size, 10_000);
    assert_eq!(config.max_groups_per_eviction_scan, 100);
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
    // failure_threshold of 10 at the default 10s heartbeat cadence means
    // 100s detection time, so the lease must outlast it for fast
    // failover to fire before lease expiry.
    let config = ClusterConfig {
        fast_heartbeat_interval_ms: 1000,
        failure_suspicion_threshold: 3,
        failure_threshold: 10,
        lease_duration: Duration::from_secs(120),
        ..Default::default()
    };

    assert_eq!(config.fast_heartbeat_interval_ms, 1000);
    assert_eq!(config.failure_suspicion_threshold, 3);
    assert_eq!(config.failure_threshold, 10);
    assert!(config.validate().is_ok());
}

#[test]
fn test_config_fast_failover_detection_slower_than_lease_rejected() {
    // Same thresholds but with the default 60s lease: detection (100s)
    // would never beat lease expiry, so validation must reject it.
    let config = ClusterConfig {
        fast_heartbeat_interval_ms: 1000,
        failure_suspicion_threshold: 3,
        failure_threshold: 10,
        ..Default::default()
    };

    let errors = config.validate().unwrap_err();
    assert!(
        errors.iter().any(|e| e.contains("detection time")),
        "expected a detection-time validation error, got: {:?}",
        errors
    );
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
fn test_config_sasl_plain_require_tls_defaults_true() {
    let config = ClusterConfig::default();
    assert!(config.sasl_plain_require_tls);
}

#[test]
fn test_validate_plain_without_tls_rejected_when_sasl_enabled() {
    let config = ClusterConfig {
        sasl_enabled: true,
        sasl_plain_require_tls: false,
        tls_enabled: false,
        ..Default::default()
    };
    let errors = config.validate().unwrap_err();
    assert!(
        errors.iter().any(|e| e.contains("sasl_plain_require_tls")),
        "expected plain/TLS validation error, got: {:?}",
        errors
    );
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
fn test_config_sasl_required_without_enabled_fails_validation() {
    // SASL_REQUIRED without SASL_ENABLED would reject every client
    // connection at runtime; validation must catch it at startup.
    let config = ClusterConfig {
        sasl_enabled: false,
        sasl_required: true,
        ..Default::default()
    };

    let errors = config.validate().unwrap_err();
    assert!(
        errors.iter().any(|e| e.contains("sasl_required")),
        "expected a sasl_required validation error, got: {:?}",
        errors
    );
}

#[test]
fn test_config_with_raft_peers() {
    // A multi-broker config (3 raft peers) needs a *shared* object store:
    // validation rejects `Local` storage with more than one peer (failover
    // would lose data) unless CLUSTER_PROFILE=development. Pin a remote (S3)
    // backend so this test is deterministic instead of depending on an
    // ambient CLUSTER_PROFILE env var — the default `cluster_id` is non-empty,
    // so the remote-backend cluster_id rule is satisfied too.
    let config = ClusterConfig {
        raft_listen_addr: "192.168.1.1:9093".to_string(),
        raft_peers: Some("0=192.168.1.1:9093,1=192.168.1.2:9093,2=192.168.1.3:9093".to_string()),
        object_store: ObjectStoreType::S3 {
            bucket: "test-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        },
        ..Default::default()
    };

    assert_eq!(config.raft_listen_addr, "192.168.1.1:9093");
    assert!(config.raft_peers.is_some());
    assert!(
        config.validate().is_ok(),
        "multi-peer config with a shared S3 backend should validate: {:?}",
        config.validate()
    );
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
