//! Tests for ObjectStoreType and additional ClusterConfig tests.

use kafkaesque::cluster::{ClusterConfig, ObjectStoreType};

// ============================================================================
// ObjectStoreType Tests
// ============================================================================

#[test]
fn test_object_store_type_local_custom_path() {
    let store = ObjectStoreType::Local {
        path: "/custom/storage/path".to_string(),
    };
    match store {
        ObjectStoreType::Local { path } => {
            assert_eq!(path, "/custom/storage/path");
        }
        _ => panic!("Expected Local variant"),
    }
}

#[test]
fn test_object_store_type_s3_minimal() {
    let store = ObjectStoreType::S3 {
        bucket: "my-bucket".to_string(),
        region: "us-east-1".to_string(),
        endpoint: None,
        access_key_id: None,
        secret_access_key: None,
    };

    match store {
        ObjectStoreType::S3 {
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
        } => {
            assert_eq!(bucket, "my-bucket");
            assert_eq!(region, "us-east-1");
            assert!(endpoint.is_none());
            assert!(access_key_id.is_none());
            assert!(secret_access_key.is_none());
        }
        _ => panic!("Expected S3 variant"),
    }
}

#[test]
fn test_object_store_type_gcs_with_creds() {
    let store = ObjectStoreType::Gcs {
        bucket: "my-gcs-bucket".to_string(),
        service_account_key: Some("/path/to/credentials.json".to_string()),
    };

    match store {
        ObjectStoreType::Gcs {
            bucket,
            service_account_key,
        } => {
            assert_eq!(bucket, "my-gcs-bucket");
            assert_eq!(
                service_account_key,
                Some("/path/to/credentials.json".to_string())
            );
        }
        _ => panic!("Expected Gcs variant"),
    }
}

#[test]
fn test_object_store_type_azure_with_key() {
    let store = ObjectStoreType::Azure {
        container: "my-container".to_string(),
        account: "mystorageaccount".to_string(),
        access_key: Some("supersecretkey".to_string()),
    };

    match store {
        ObjectStoreType::Azure {
            container,
            account,
            access_key,
        } => {
            assert_eq!(container, "my-container");
            assert_eq!(account, "mystorageaccount");
            assert_eq!(access_key, Some("supersecretkey".to_string()));
        }
        _ => panic!("Expected Azure variant"),
    }
}

// ============================================================================
// Additional ClusterConfig Tests
// ============================================================================

#[test]
fn test_cluster_config_custom_values() {
    let config = ClusterConfig {
        broker_id: 5,
        host: "192.168.1.100".to_string(),
        port: 9192,
        cluster_id: "my-custom-cluster".to_string(),
        ..ClusterConfig::default()
    };

    assert_eq!(config.broker_id, 5);
    assert_eq!(config.host, "192.168.1.100");
    assert_eq!(config.port, 9192);
    assert_eq!(config.cluster_id, "my-custom-cluster");
}

#[test]
fn test_cluster_config_raft_peers() {
    let config = ClusterConfig {
        raft_peers: Some("0=host1:9093,1=host2:9093".to_string()),
        raft_listen_addr: "0.0.0.0:9093".to_string(),
        ..ClusterConfig::default()
    };

    assert!(config.raft_peers.is_some());
    assert_eq!(config.raft_listen_addr, "0.0.0.0:9093");
}

#[test]
fn test_cluster_config_sasl() {
    let config = ClusterConfig {
        sasl_enabled: true,
        sasl_required: true,
        sasl_users_file: Some("/etc/kafkaesque/sasl-users.txt".to_string()),
        ..ClusterConfig::default()
    };

    assert!(config.sasl_enabled);
    assert!(config.sasl_required);
    assert_eq!(
        config.sasl_users_file,
        Some("/etc/kafkaesque/sasl-users.txt".to_string())
    );
}

#[test]
fn test_cluster_config_timeouts() {
    let config = ClusterConfig {
        fetch_timeout_secs: 30,
        default_session_timeout_ms: 15000,
        ..ClusterConfig::default()
    };

    assert_eq!(config.fetch_timeout_secs, 30);
    assert_eq!(config.default_session_timeout_ms, 15000);
}
