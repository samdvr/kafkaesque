//! Integration tests for ClusterConfig::from_env()
//!
//! These tests verify configuration loading from environment variables.
//! Note: Tests that require removing env vars mid-test are omitted due to
//! unsafe requirements in recent Rust editions.

use kafkaesque::cluster::{ClusterConfig, ObjectStoreType};
use std::env;
use std::sync::Mutex;

/// Global mutex to serialize all env-based tests.
/// Environment variables are process-global, so we must prevent concurrent access.
static ENV_MUTEX: Mutex<()> = Mutex::new(());

/// All environment variables read by ClusterConfig::from_env().
/// We must save/restore ALL of these to prevent test pollution when running in parallel.
const ALL_CONFIG_ENV_VARS: &[&str] = &[
    "BROKER_ID",
    "HOST",
    "ADVERTISED_HOST",
    "PORT",
    "HEALTH_PORT",
    "CLUSTER_ID",
    "RAFT_LISTEN_ADDR",
    "RAFT_PEERS",
    "DATA_PATH",
    "AUTO_CREATE_TOPICS",
    "DEFAULT_NUM_PARTITIONS",
    "MAX_MESSAGE_SIZE",
    "MAX_FETCH_RESPONSE_SIZE",
    "MAX_CONCURRENT_PARTITION_WRITES",
    "MAX_CONCURRENT_PARTITION_READS",
    "BROKER_HEARTBEAT_TTL_SECS",
    "GROUP_MEMBER_TTL_SECS",
    "OWNERSHIP_CACHE_TTL_SECS",
    "PRODUCER_STATE_CACHE_TTL_SECS",
    "OBJECT_STORE_TYPE",
    "AWS_S3_BUCKET",
    "S3_BUCKET",
    "AWS_REGION",
    "AWS_DEFAULT_REGION",
    "AWS_ENDPOINT",
    "S3_ENDPOINT",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "GCS_BUCKET",
    "GOOGLE_APPLICATION_CREDENTIALS",
    "AZURE_CONTAINER",
    "AZURE_STORAGE_ACCOUNT",
    "AZURE_STORAGE_ACCESS_KEY",
    "SASL_ENABLED",
    "SASL_REQUIRED",
    "SASL_USERS_FILE",
    "FAIL_ON_RECOVERY_GAP",
];

/// Helper to run a test with specific environment variables set.
/// This helper:
/// 1. Acquires a mutex to serialize all env-based tests
/// 2. Saves and restores ALL config-related env vars
fn with_env_vars<F, R>(vars: &[(&str, &str)], f: F) -> R
where
    F: FnOnce() -> R,
{
    // Acquire the mutex to ensure exclusive access to env vars
    let _guard = ENV_MUTEX.lock().unwrap_or_else(|e| e.into_inner());

    // Save ALL config-related env vars (not just the ones being tested)
    let all_originals: Vec<_> = ALL_CONFIG_ENV_VARS
        .iter()
        .map(|k| (*k, env::var(*k).ok()))
        .collect();

    // Clear ALL config-related env vars first to ensure clean state
    for key in ALL_CONFIG_ENV_VARS {
        unsafe { env::remove_var(key) };
    }

    // Set the test-specific values
    for (key, value) in vars {
        unsafe { env::set_var(key, value) };
    }

    let result = f();

    // Restore ALL original values
    for (key, original) in all_originals {
        match original {
            Some(v) => unsafe { env::set_var(key, v) },
            None => unsafe { env::remove_var(key) },
        }
    }

    result
}

// ============================================================================
// Basic Environment Variable Tests
// ============================================================================

#[test]
fn test_from_env_with_broker_id() {
    with_env_vars(
        &[("BROKER_ID", "42"), ("HOST", "localhost"), ("PORT", "9092")],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.broker_id, 42);
        },
    );
}

#[test]
fn test_from_env_with_custom_host_and_port() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "0.0.0.0"),
            ("PORT", "9094"),
            ("ADVERTISED_HOST", "kafka-broker-1.example.com"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.host, "0.0.0.0");
            assert_eq!(config.port, 9094);
            assert_eq!(config.advertised_host, "kafka-broker-1.example.com");
        },
    );
}

#[test]
fn test_from_env_with_health_port() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("HEALTH_PORT", "8081"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.health_port, 8081);
        },
    );
}

#[test]
fn test_from_env_health_port_zero_disables() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("HEALTH_PORT", "0"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.health_port, 0);
        },
    );
}

#[test]
fn test_from_env_invalid_broker_id_fails() {
    with_env_vars(
        &[
            ("BROKER_ID", "not-a-number"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
        ],
        || {
            let result = ClusterConfig::from_env();
            assert!(result.is_err());
        },
    );
}

#[test]
fn test_from_env_invalid_port_fails() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "invalid"),
        ],
        || {
            let result = ClusterConfig::from_env();
            assert!(result.is_err());
        },
    );
}

#[test]
fn test_from_env_port_out_of_range_fails() {
    with_env_vars(
        &[("BROKER_ID", "1"), ("HOST", "localhost"), ("PORT", "99999")],
        || {
            let result = ClusterConfig::from_env();
            assert!(result.is_err());
        },
    );
}

// ============================================================================
// Raft Configuration Tests
// ============================================================================

#[test]
fn test_from_env_with_raft_config() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("CLUSTER_ID", "test-cluster"),
            ("RAFT_LISTEN_ADDR", "192.168.1.1:19092"),
            ("RAFT_PEERS", "0=192.168.1.1:19092,2=192.168.1.3:19092"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.cluster_id, "test-cluster");
            assert_eq!(config.raft_listen_addr, "192.168.1.1:19092");
            assert!(config.raft_peers.is_some());
            assert!(
                config
                    .raft_peers
                    .as_ref()
                    .unwrap()
                    .contains("0=192.168.1.1:19092")
            );
        },
    );
}

// ============================================================================
// Object Store Configuration Tests
// ============================================================================

#[test]
fn test_from_env_local_object_store_default() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("DATA_PATH", "/custom/data/path"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            match &config.object_store {
                ObjectStoreType::Local { path } => {
                    assert_eq!(path, "/custom/data/path");
                }
                _ => panic!("Expected Local object store"),
            }
        },
    );
}

#[test]
fn test_from_env_s3_object_store() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("OBJECT_STORE_TYPE", "s3"),
            ("AWS_S3_BUCKET", "my-kafka-bucket"),
            ("AWS_REGION", "us-west-2"),
            ("AWS_ENDPOINT", "http://localhost:9000"),
            ("AWS_ACCESS_KEY_ID", "test-key"),
            ("AWS_SECRET_ACCESS_KEY", "test-secret"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            match &config.object_store {
                ObjectStoreType::S3 {
                    bucket,
                    region,
                    endpoint,
                    access_key_id,
                    secret_access_key,
                } => {
                    assert_eq!(bucket, "my-kafka-bucket");
                    assert_eq!(region, "us-west-2");
                    assert_eq!(endpoint, &Some("http://localhost:9000".to_string()));
                    assert_eq!(access_key_id, &Some("test-key".to_string()));
                    assert_eq!(secret_access_key, &Some("test-secret".to_string()));
                }
                _ => panic!("Expected S3 object store"),
            }
        },
    );
}

#[test]
fn test_from_env_s3_with_s3_bucket_env() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("OBJECT_STORE_TYPE", "s3"),
            ("S3_BUCKET", "fallback-bucket"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            match &config.object_store {
                ObjectStoreType::S3 { bucket, .. } => {
                    assert_eq!(bucket, "fallback-bucket");
                }
                _ => panic!("Expected S3 object store"),
            }
        },
    );
}

#[test]
fn test_from_env_gcs_object_store() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("OBJECT_STORE_TYPE", "gcs"),
            ("GCS_BUCKET", "my-gcs-bucket"),
            (
                "GOOGLE_APPLICATION_CREDENTIALS",
                "/path/to/credentials.json",
            ),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            match &config.object_store {
                ObjectStoreType::Gcs {
                    bucket,
                    service_account_key,
                } => {
                    assert_eq!(bucket, "my-gcs-bucket");
                    assert_eq!(
                        service_account_key,
                        &Some("/path/to/credentials.json".to_string())
                    );
                }
                _ => panic!("Expected GCS object store"),
            }
        },
    );
}

#[test]
fn test_from_env_azure_object_store() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("OBJECT_STORE_TYPE", "azure"),
            ("AZURE_CONTAINER", "my-container"),
            ("AZURE_STORAGE_ACCOUNT", "mystorageaccount"),
            ("AZURE_STORAGE_ACCESS_KEY", "my-access-key"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            match &config.object_store {
                ObjectStoreType::Azure {
                    container,
                    account,
                    access_key,
                } => {
                    assert_eq!(container, "my-container");
                    assert_eq!(account, "mystorageaccount");
                    assert_eq!(access_key, &Some("my-access-key".to_string()));
                }
                _ => panic!("Expected Azure object store"),
            }
        },
    );
}

// ============================================================================
// Topic Configuration Tests
// ============================================================================

#[test]
fn test_from_env_auto_create_topics_true() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("AUTO_CREATE_TOPICS", "true"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert!(config.auto_create_topics);
        },
    );
}

#[test]
fn test_from_env_auto_create_topics_false() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("AUTO_CREATE_TOPICS", "false"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert!(!config.auto_create_topics);
        },
    );
}

#[test]
fn test_from_env_auto_create_topics_zero() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("AUTO_CREATE_TOPICS", "0"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert!(!config.auto_create_topics);
        },
    );
}

#[test]
fn test_from_env_default_num_partitions() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("DEFAULT_NUM_PARTITIONS", "20"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.default_num_partitions, 20);
        },
    );
}

// ============================================================================
// Network Tuning Tests
// ============================================================================

#[test]
fn test_from_env_max_message_size() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("MAX_MESSAGE_SIZE", "10485760"), // 10 MB
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.max_message_size, 10485760);
        },
    );
}

#[test]
fn test_from_env_max_fetch_response_size() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("MAX_FETCH_RESPONSE_SIZE", "5242880"), // 5 MB
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.max_fetch_response_size, 5242880);
        },
    );
}

#[test]
fn test_from_env_concurrent_partition_limits() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("MAX_CONCURRENT_PARTITION_WRITES", "32"),
            ("MAX_CONCURRENT_PARTITION_READS", "64"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.max_concurrent_partition_writes, 32);
            assert_eq!(config.max_concurrent_partition_reads, 64);
        },
    );
}

// ============================================================================
// Coordinator Tuning Tests
// ============================================================================

#[test]
fn test_from_env_broker_heartbeat_ttl() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("BROKER_HEARTBEAT_TTL_SECS", "60"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.broker_heartbeat_ttl_secs, 60);
        },
    );
}

#[test]
fn test_from_env_group_member_ttl() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("GROUP_MEMBER_TTL_SECS", "600"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.group_member_ttl_secs, 600);
        },
    );
}

#[test]
fn test_from_env_ownership_cache_ttl() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("OWNERSHIP_CACHE_TTL_SECS", "5"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.ownership_cache_ttl_secs, 5);
        },
    );
}

#[test]
fn test_from_env_producer_state_cache_ttl() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("PRODUCER_STATE_CACHE_TTL_SECS", "3600"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert_eq!(config.producer_state_cache_ttl_secs, 3600);
        },
    );
}

// ============================================================================
// SASL Configuration Tests
// ============================================================================

#[test]
fn test_from_env_sasl_enabled() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("SASL_ENABLED", "true"),
            ("SASL_REQUIRED", "true"),
            ("SASL_USERS_FILE", "/etc/kafka/users.txt"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert!(config.sasl_enabled);
            assert!(config.sasl_required);
            assert_eq!(
                config.sasl_users_file,
                Some("/etc/kafka/users.txt".to_string())
            );
        },
    );
}

#[test]
fn test_from_env_sasl_disabled() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("SASL_ENABLED", "false"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert!(!config.sasl_enabled);
        },
    );
}

// ============================================================================
// Data Integrity Configuration Tests
// ============================================================================

#[test]
fn test_from_env_fail_on_recovery_gap() {
    with_env_vars(
        &[
            ("BROKER_ID", "1"),
            ("HOST", "localhost"),
            ("PORT", "9092"),
            ("FAIL_ON_RECOVERY_GAP", "true"),
        ],
        || {
            let config = ClusterConfig::from_env().expect("Should parse config");
            assert!(config.fail_on_recovery_gap);
        },
    );
}

// ============================================================================
// Validation Tests
// ============================================================================

#[test]
fn test_from_env_validates_config() {
    // This should fail validation due to negative broker_id
    with_env_vars(
        &[("BROKER_ID", "-1"), ("HOST", "localhost"), ("PORT", "9092")],
        || {
            let result = ClusterConfig::from_env();
            assert!(result.is_err());
        },
    );
}
