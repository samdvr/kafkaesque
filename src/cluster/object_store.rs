//! Object store creation and configuration.
//!
//! This module handles creation of object stores from configuration,
//! supporting multiple backends: Local, S3, GCS, and Azure.

use object_store::ObjectStore;
use object_store::local::LocalFileSystem;
use std::sync::Arc;
use tracing::info;

use super::config::{ClusterConfig, ObjectStoreType};
use super::error::{SlateDBError, SlateDBResult};

/// Create an object store from configuration.
///
/// This function handles creation of object stores for different backends:
/// - Local filesystem
/// - Amazon S3
/// - Google Cloud Storage
/// - Microsoft Azure Blob Storage
///
/// # Arguments
/// * `config` - Cluster configuration containing object store settings
///
/// # Returns
/// An `Arc<dyn ObjectStore>` ready for use with SlateDB
///
/// # Errors
///
/// Returns an error if:
/// - The data directory cannot be created (for local filesystem)
/// - Cloud provider configuration is invalid
pub fn create_object_store(config: &ClusterConfig) -> SlateDBResult<Arc<dyn ObjectStore>> {
    match &config.object_store {
        ObjectStoreType::Local { path } => {
            // Create directory if it doesn't exist
            std::fs::create_dir_all(path).map_err(|e| {
                SlateDBError::Config(format!("Failed to create data directory: {}", e))
            })?;

            let store = LocalFileSystem::new_with_prefix(path)?;
            Ok(Arc::new(store))
        }
        ObjectStoreType::S3 {
            bucket,
            region,
            endpoint,
            access_key_id,
            secret_access_key,
        } => {
            use object_store::aws::AmazonS3Builder;

            let mut builder = AmazonS3Builder::new()
                .with_bucket_name(bucket)
                .with_region(region);

            if let Some(ep) = endpoint {
                builder = builder.with_endpoint(ep).with_allow_http(true);
            }

            if let (Some(key), Some(secret)) = (access_key_id, secret_access_key) {
                builder = builder
                    .with_access_key_id(key)
                    .with_secret_access_key(secret);
            }

            let store = builder.build().map_err(|e| {
                SlateDBError::Config(format!("Failed to create S3 object store: {}", e))
            })?;

            info!(bucket = %bucket, region = %region, "Using S3 object store");
            Ok(Arc::new(store))
        }
        ObjectStoreType::Gcs {
            bucket,
            service_account_key,
        } => {
            use object_store::gcp::GoogleCloudStorageBuilder;

            let mut builder = GoogleCloudStorageBuilder::new().with_bucket_name(bucket);

            if let Some(key_path) = service_account_key {
                builder = builder.with_service_account_path(key_path);
            }

            let store = builder.build().map_err(|e| {
                SlateDBError::Config(format!("Failed to create GCS object store: {}", e))
            })?;

            info!(bucket = %bucket, "Using GCS object store");
            Ok(Arc::new(store))
        }
        ObjectStoreType::Azure {
            container,
            account,
            access_key,
        } => {
            use object_store::azure::MicrosoftAzureBuilder;

            let mut builder = MicrosoftAzureBuilder::new()
                .with_container_name(container)
                .with_account(account);

            if let Some(key) = access_key {
                builder = builder.with_access_key(key);
            }

            let store = builder.build().map_err(|e| {
                SlateDBError::Config(format!("Failed to create Azure object store: {}", e))
            })?;

            info!(container = %container, account = %account, "Using Azure object store");
            Ok(Arc::new(store))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tempfile::TempDir;

    // ========================================================================
    // Local Object Store Tests
    // ========================================================================

    #[test]
    fn test_create_local_object_store() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("data");

        let config = ClusterConfig {
            broker_id: 0,
            host: "localhost".to_string(),
            port: 9092,
            object_store: ObjectStoreType::Local {
                path: path.to_string_lossy().to_string(),
            },
            object_store_path: path.to_string_lossy().to_string(),
            auto_create_topics: true,
            ..Default::default()
        };

        let result = create_object_store(&config);
        assert!(result.is_ok());

        // Verify directory was created
        assert!(path.exists());
    }

    #[test]
    fn test_create_local_object_store_existing_dir() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let config = ClusterConfig {
            broker_id: 0,
            host: "localhost".to_string(),
            port: 9092,
            object_store: ObjectStoreType::Local {
                path: path.to_string_lossy().to_string(),
            },
            object_store_path: path.to_string_lossy().to_string(),
            auto_create_topics: true,
            ..Default::default()
        };

        // Should work even if directory already exists
        let result = create_object_store(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_local_object_store_nested_dirs() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("a").join("b").join("c").join("data");

        let config = ClusterConfig {
            broker_id: 0,
            host: "localhost".to_string(),
            port: 9092,
            object_store: ObjectStoreType::Local {
                path: path.to_string_lossy().to_string(),
            },
            object_store_path: path.to_string_lossy().to_string(),
            auto_create_topics: true,
            ..Default::default()
        };

        // Should create nested directories
        let result = create_object_store(&config);
        assert!(result.is_ok());
        assert!(path.exists());
    }

    #[test]
    fn test_local_object_store_is_usable() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("store");

        let config = ClusterConfig {
            object_store: ObjectStoreType::Local {
                path: path.to_string_lossy().to_string(),
            },
            object_store_path: path.to_string_lossy().to_string(),
            ..Default::default()
        };

        let store = create_object_store(&config).unwrap();
        // The store should implement ObjectStore trait
        // We can verify this compiles and the store is Arc<dyn ObjectStore>
        let _: Arc<dyn ObjectStore> = store;
    }

    #[test]
    fn test_local_store_multiple_creates() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("store");

        let config = ClusterConfig {
            object_store: ObjectStoreType::Local {
                path: path.to_string_lossy().to_string(),
            },
            object_store_path: path.to_string_lossy().to_string(),
            ..Default::default()
        };

        // Should be able to create multiple times
        let store1 = create_object_store(&config);
        let store2 = create_object_store(&config);

        assert!(store1.is_ok());
        assert!(store2.is_ok());
    }

    #[tokio::test]
    async fn test_local_store_basic_operations() {
        use object_store::path::Path;

        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("store");

        let config = ClusterConfig {
            object_store: ObjectStoreType::Local {
                path: path.to_string_lossy().to_string(),
            },
            object_store_path: path.to_string_lossy().to_string(),
            ..Default::default()
        };

        let store = create_object_store(&config).unwrap();

        // Write some data
        let data = bytes::Bytes::from("test data");
        let location = Path::from("test/file.txt");
        store.put(&location, data.clone().into()).await.unwrap();

        // Read it back
        let result = store.get(&location).await.unwrap();
        let read_data = result.bytes().await.unwrap();
        assert_eq!(read_data, data);

        // Delete it
        store.delete(&location).await.unwrap();

        // Verify it's gone
        let result = store.get(&location).await;
        assert!(result.is_err());
    }

    // ========================================================================
    // ObjectStoreType Structure Tests
    // ========================================================================

    #[test]
    fn test_object_store_type_local_default() {
        // Verify ObjectStoreType::Local can be matched and used
        let store_type = ObjectStoreType::Local {
            path: "/tmp/test".to_string(),
        };
        match &store_type {
            ObjectStoreType::Local { path } => {
                assert_eq!(path, "/tmp/test");
            }
            _ => panic!("Expected Local variant"),
        }
    }

    #[test]
    fn test_object_store_type_s3_fields() {
        let store_type = ObjectStoreType::S3 {
            bucket: "my-bucket".to_string(),
            region: "us-west-2".to_string(),
            endpoint: Some("http://localhost:9000".to_string()),
            access_key_id: Some("test-key".to_string()),
            secret_access_key: Some("test-secret".to_string()),
        };

        match &store_type {
            ObjectStoreType::S3 {
                bucket,
                region,
                endpoint,
                access_key_id,
                secret_access_key,
            } => {
                assert_eq!(bucket, "my-bucket");
                assert_eq!(region, "us-west-2");
                assert_eq!(endpoint, &Some("http://localhost:9000".to_string()));
                assert!(access_key_id.is_some());
                assert!(secret_access_key.is_some());
            }
            _ => panic!("Expected S3 variant"),
        }
    }

    #[test]
    fn test_object_store_type_s3_minimal() {
        // S3 with minimal config (no endpoint, no credentials)
        let store_type = ObjectStoreType::S3 {
            bucket: "my-bucket".to_string(),
            region: "us-east-1".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        };

        match &store_type {
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
    fn test_object_store_type_s3_with_endpoint_only() {
        // S3 with endpoint but no credentials (IAM role assumed)
        let store_type = ObjectStoreType::S3 {
            bucket: "my-bucket".to_string(),
            region: "us-west-2".to_string(),
            endpoint: Some("http://minio:9000".to_string()),
            access_key_id: None,
            secret_access_key: None,
        };

        match &store_type {
            ObjectStoreType::S3 {
                endpoint,
                access_key_id,
                ..
            } => {
                assert!(endpoint.is_some());
                assert!(access_key_id.is_none());
            }
            _ => panic!("Expected S3 variant"),
        }
    }

    #[test]
    fn test_object_store_type_gcs_fields() {
        let store_type = ObjectStoreType::Gcs {
            bucket: "gcs-bucket".to_string(),
            service_account_key: Some("/path/to/key.json".to_string()),
        };

        match &store_type {
            ObjectStoreType::Gcs {
                bucket,
                service_account_key,
            } => {
                assert_eq!(bucket, "gcs-bucket");
                assert_eq!(service_account_key, &Some("/path/to/key.json".to_string()));
            }
            _ => panic!("Expected Gcs variant"),
        }
    }

    #[test]
    fn test_object_store_type_gcs_minimal() {
        // GCS without service account (uses default credentials)
        let store_type = ObjectStoreType::Gcs {
            bucket: "gcs-bucket".to_string(),
            service_account_key: None,
        };

        match &store_type {
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
    fn test_object_store_type_azure_fields() {
        let store_type = ObjectStoreType::Azure {
            container: "my-container".to_string(),
            account: "storageaccount".to_string(),
            access_key: Some("secret-key".to_string()),
        };

        match &store_type {
            ObjectStoreType::Azure {
                container,
                account,
                access_key,
            } => {
                assert_eq!(container, "my-container");
                assert_eq!(account, "storageaccount");
                assert!(access_key.is_some());
            }
            _ => panic!("Expected Azure variant"),
        }
    }

    #[test]
    fn test_object_store_type_azure_minimal() {
        // Azure without access key (uses managed identity)
        let store_type = ObjectStoreType::Azure {
            container: "my-container".to_string(),
            account: "storageaccount".to_string(),
            access_key: None,
        };

        match &store_type {
            ObjectStoreType::Azure {
                container,
                account,
                access_key,
            } => {
                assert_eq!(container, "my-container");
                assert_eq!(account, "storageaccount");
                assert!(access_key.is_none());
            }
            _ => panic!("Expected Azure variant"),
        }
    }

    // ========================================================================
    // S3 Builder Tests (without actual S3 connection)
    // ========================================================================

    #[test]
    fn test_create_s3_store_with_credentials() {
        // Test S3 builder path with credentials
        // This doesn't make actual connections, just tests the builder
        let config = ClusterConfig {
            object_store: ObjectStoreType::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-west-2".to_string(),
                endpoint: Some("http://localhost:9000".to_string()),
                access_key_id: Some("test-key".to_string()),
                secret_access_key: Some("test-secret".to_string()),
            },
            ..Default::default()
        };

        // Builder should succeed (doesn't actually connect)
        let result = create_object_store(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_s3_store_without_credentials() {
        // Test S3 builder path without credentials (will use env/IAM)
        let config = ClusterConfig {
            object_store: ObjectStoreType::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-west-2".to_string(),
                endpoint: None,
                access_key_id: None,
                secret_access_key: None,
            },
            ..Default::default()
        };

        // Builder should succeed (doesn't actually connect)
        let result = create_object_store(&config);
        assert!(result.is_ok());
    }

    #[test]
    fn test_create_s3_store_with_endpoint_no_creds() {
        // Test S3 with custom endpoint but no explicit credentials
        let config = ClusterConfig {
            object_store: ObjectStoreType::S3 {
                bucket: "test-bucket".to_string(),
                region: "us-east-1".to_string(),
                endpoint: Some("http://minio:9000".to_string()),
                access_key_id: None,
                secret_access_key: None,
            },
            ..Default::default()
        };

        let result = create_object_store(&config);
        assert!(result.is_ok());
    }

    // ========================================================================
    // ObjectStoreType Debug and Clone Tests
    // ========================================================================

    #[test]
    fn test_object_store_type_debug() {
        let local = ObjectStoreType::Local {
            path: "/tmp/test".to_string(),
        };
        let debug_str = format!("{:?}", local);
        assert!(debug_str.contains("Local"));
        assert!(debug_str.contains("/tmp/test"));

        let s3 = ObjectStoreType::S3 {
            bucket: "bucket".to_string(),
            region: "region".to_string(),
            endpoint: None,
            access_key_id: None,
            secret_access_key: None,
        };
        let debug_str = format!("{:?}", s3);
        assert!(debug_str.contains("S3"));
        assert!(debug_str.contains("bucket"));
    }

    #[test]
    fn test_object_store_type_clone() {
        let original = ObjectStoreType::Local {
            path: "/tmp/test".to_string(),
        };
        let cloned = original.clone();

        match (&original, &cloned) {
            (ObjectStoreType::Local { path: p1 }, ObjectStoreType::Local { path: p2 }) => {
                assert_eq!(p1, p2);
            }
            _ => panic!("Clone should produce same variant"),
        }
    }

    #[test]
    fn test_object_store_type_s3_clone() {
        let original = ObjectStoreType::S3 {
            bucket: "bucket".to_string(),
            region: "region".to_string(),
            endpoint: Some("http://endpoint".to_string()),
            access_key_id: Some("key".to_string()),
            secret_access_key: Some("secret".to_string()),
        };
        let cloned = original.clone();

        match (&original, &cloned) {
            (ObjectStoreType::S3 { bucket: b1, .. }, ObjectStoreType::S3 { bucket: b2, .. }) => {
                assert_eq!(b1, b2);
            }
            _ => panic!("Clone should produce same variant"),
        }
    }

    // ========================================================================
    // Error Message Tests
    // ========================================================================

    #[test]
    fn test_slatedb_error_config_message() {
        let error = SlateDBError::Config("test error message".to_string());
        let error_str = format!("{}", error);
        assert!(error_str.contains("test error message"));
    }

    // ========================================================================
    // Path Edge Cases
    // ========================================================================

    #[test]
    fn test_local_store_path_with_spaces() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("path with spaces").join("data");

        let config = ClusterConfig {
            object_store: ObjectStoreType::Local {
                path: path.to_string_lossy().to_string(),
            },
            object_store_path: path.to_string_lossy().to_string(),
            ..Default::default()
        };

        let result = create_object_store(&config);
        assert!(result.is_ok());
        assert!(path.exists());
    }

    #[test]
    fn test_local_store_path_with_unicode() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().join("데이터").join("ストア");

        let config = ClusterConfig {
            object_store: ObjectStoreType::Local {
                path: path.to_string_lossy().to_string(),
            },
            object_store_path: path.to_string_lossy().to_string(),
            ..Default::default()
        };

        let result = create_object_store(&config);
        assert!(result.is_ok());
        assert!(path.exists());
    }

    #[test]
    fn test_local_store_deep_nesting() {
        let temp_dir = TempDir::new().unwrap();
        let mut path = temp_dir.path().to_path_buf();

        // Create a deeply nested path
        for i in 0..20 {
            path = path.join(format!("level{}", i));
        }

        let config = ClusterConfig {
            object_store: ObjectStoreType::Local {
                path: path.to_string_lossy().to_string(),
            },
            object_store_path: path.to_string_lossy().to_string(),
            ..Default::default()
        };

        let result = create_object_store(&config);
        assert!(result.is_ok());
        assert!(path.exists());
    }
}
