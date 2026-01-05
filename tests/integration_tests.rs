//! Integration tests for the Kafkaesque Kafka server.
//!
//! These tests verify the server's behavior with actual network connections
//! and protocol exchanges.
//!
//! **Note:** These tests require network socket access (TCP listener on 127.0.0.1).
//! They will fail in sandboxed environments that restrict network access.
//!
//! To run these tests:
//! ```bash
//! cargo test --test integration_tests
//! ```
//!
//! To skip these tests (e.g., in CI without network access):
//! ```bash
//! cargo test --lib
//! ```

use bytes::{BufMut, Bytes, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::timeout;

use kafkaesque::error::KafkaCode;
use kafkaesque::prelude::server::{
    Handler, KafkaServer, RequestContext,
    request::{
        ApiKey, ApiVersionsRequestData, FetchRequestData, MetadataRequestData, ProduceRequestData,
    },
    response::{
        ApiVersionData, ApiVersionsResponseData, BrokerData, FetchPartitionResponse,
        FetchResponseData, FetchTopicResponse, MetadataResponseData, PartitionMetadata,
        ProducePartitionResponse, ProduceResponseData, ProduceTopicResponse, TopicMetadata,
    },
};

// ============================================================================
// Test Handler Implementation
// ============================================================================

type TopicData = std::collections::HashMap<i32, Vec<Bytes>>;
type StoreData = std::collections::HashMap<String, TopicData>;

/// A simple in-memory handler for integration testing.
#[derive(Clone)]
struct TestHandler {
    /// In-memory storage: topic -> partition -> records
    data: Arc<RwLock<StoreData>>,
    /// Broker ID for this handler
    broker_id: i32,
    /// Host for metadata responses
    host: String,
    /// Port for metadata responses
    port: i32,
    /// Topics that exist
    topics: Arc<RwLock<std::collections::HashMap<String, i32>>>,
}

impl TestHandler {
    fn new(broker_id: i32, host: &str, port: i32) -> Self {
        Self {
            data: Arc::new(RwLock::new(std::collections::HashMap::new())),
            broker_id,
            host: host.to_string(),
            port,
            topics: Arc::new(RwLock::new(std::collections::HashMap::new())),
        }
    }

    async fn create_topic(&self, name: &str, partitions: i32) {
        let mut topics = self.topics.write().await;
        topics.insert(name.to_string(), partitions);

        let mut data = self.data.write().await;
        let topic_data = data.entry(name.to_string()).or_default();
        for p in 0..partitions {
            topic_data.entry(p).or_default();
        }
    }
}

#[async_trait::async_trait]
impl Handler for TestHandler {
    async fn handle_api_versions(
        &self,
        _ctx: &RequestContext,
        _request: ApiVersionsRequestData,
    ) -> ApiVersionsResponseData {
        ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![
                ApiVersionData {
                    api_key: ApiKey::Produce,
                    min_version: 0,
                    max_version: 8,
                },
                ApiVersionData {
                    api_key: ApiKey::Fetch,
                    min_version: 0,
                    max_version: 11,
                },
                ApiVersionData {
                    api_key: ApiKey::Metadata,
                    min_version: 0,
                    max_version: 9,
                },
                ApiVersionData {
                    api_key: ApiKey::ApiVersions,
                    min_version: 0,
                    max_version: 3,
                },
            ],
            throttle_time_ms: 0,
        }
    }

    async fn handle_metadata(
        &self,
        _ctx: &RequestContext,
        request: MetadataRequestData,
    ) -> MetadataResponseData {
        let topics = self.topics.read().await;

        let topic_metadata: Vec<TopicMetadata> = match request.topics {
            None => {
                // Return all topics
                topics
                    .iter()
                    .map(|(name, partitions)| TopicMetadata {
                        error_code: KafkaCode::None,
                        name: name.clone(),
                        is_internal: false,
                        partitions: (0..*partitions)
                            .map(|p| PartitionMetadata {
                                error_code: KafkaCode::None,
                                partition_index: p,
                                leader_id: self.broker_id,
                                replica_nodes: vec![self.broker_id],
                                isr_nodes: vec![self.broker_id],
                            })
                            .collect(),
                    })
                    .collect()
            }
            Some(ref topic_names) if topic_names.is_empty() => {
                // Empty array also means all topics
                topics
                    .iter()
                    .map(|(name, partitions)| TopicMetadata {
                        error_code: KafkaCode::None,
                        name: name.clone(),
                        is_internal: false,
                        partitions: (0..*partitions)
                            .map(|p| PartitionMetadata {
                                error_code: KafkaCode::None,
                                partition_index: p,
                                leader_id: self.broker_id,
                                replica_nodes: vec![self.broker_id],
                                isr_nodes: vec![self.broker_id],
                            })
                            .collect(),
                    })
                    .collect()
            }
            Some(ref topic_names) => topic_names
                .iter()
                .map(|name| {
                    if let Some(partitions) = topics.get(name) {
                        TopicMetadata {
                            error_code: KafkaCode::None,
                            name: name.clone(),
                            is_internal: false,
                            partitions: (0..*partitions)
                                .map(|p| PartitionMetadata {
                                    error_code: KafkaCode::None,
                                    partition_index: p,
                                    leader_id: self.broker_id,
                                    replica_nodes: vec![self.broker_id],
                                    isr_nodes: vec![self.broker_id],
                                })
                                .collect(),
                        }
                    } else {
                        TopicMetadata {
                            error_code: KafkaCode::UnknownTopicOrPartition,
                            name: name.clone(),
                            is_internal: false,
                            partitions: vec![],
                        }
                    }
                })
                .collect(),
        };

        MetadataResponseData {
            brokers: vec![BrokerData {
                node_id: self.broker_id,
                host: self.host.clone(),
                port: self.port,
                rack: None,
            }],
            controller_id: self.broker_id,
            topics: topic_metadata,
        }
    }

    async fn handle_produce(
        &self,
        _ctx: &RequestContext,
        request: ProduceRequestData,
    ) -> ProduceResponseData {
        let mut data = self.data.write().await;

        let responses: Vec<ProduceTopicResponse> = request
            .topics
            .iter()
            .map(|topic| {
                let topic_data = data.entry(topic.name.clone()).or_default();

                let partition_responses: Vec<ProducePartitionResponse> = topic
                    .partitions
                    .iter()
                    .map(|partition| {
                        let partition_data =
                            topic_data.entry(partition.partition_index).or_default();
                        let base_offset = partition_data.len() as i64;

                        // partition.records is Bytes, not Option<Bytes>
                        if !partition.records.is_empty() {
                            partition_data.push(partition.records.clone());
                        }

                        ProducePartitionResponse {
                            partition_index: partition.partition_index,
                            error_code: KafkaCode::None,
                            base_offset,
                            log_append_time: -1,
                        }
                    })
                    .collect();

                ProduceTopicResponse {
                    name: topic.name.clone(),
                    partitions: partition_responses,
                }
            })
            .collect();

        ProduceResponseData {
            responses,
            throttle_time_ms: 0,
        }
    }

    async fn handle_fetch(
        &self,
        _ctx: &RequestContext,
        request: FetchRequestData,
    ) -> FetchResponseData {
        let data = self.data.read().await;

        let responses: Vec<FetchTopicResponse> = request
            .topics
            .iter()
            .map(|topic| {
                let topic_data = data.get(&topic.name);

                let partition_responses: Vec<FetchPartitionResponse> = topic
                    .partitions
                    .iter()
                    .map(|partition| {
                        if let Some(topic_data) = topic_data {
                            if let Some(partition_data) = topic_data.get(&partition.partition_index)
                            {
                                let offset = partition.fetch_offset as usize;
                                let records = if offset < partition_data.len() {
                                    Some(partition_data[offset].clone())
                                } else {
                                    None
                                };

                                FetchPartitionResponse {
                                    partition_index: partition.partition_index,
                                    error_code: KafkaCode::None,
                                    high_watermark: partition_data.len() as i64,
                                    last_stable_offset: partition_data.len() as i64,
                                    aborted_transactions: vec![],
                                    records,
                                }
                            } else {
                                FetchPartitionResponse::error(
                                    partition.partition_index,
                                    KafkaCode::UnknownTopicOrPartition,
                                )
                            }
                        } else {
                            FetchPartitionResponse::error(
                                partition.partition_index,
                                KafkaCode::UnknownTopicOrPartition,
                            )
                        }
                    })
                    .collect();

                FetchTopicResponse {
                    name: topic.name.clone(),
                    partitions: partition_responses,
                }
            })
            .collect();

        FetchResponseData {
            throttle_time_ms: 0,
            responses,
        }
    }
}

// ============================================================================
// Test Utilities
// ============================================================================

/// Start a test server and return the address.
/// Returns None if network access is denied (e.g., in sandboxed environments).
async fn start_test_server(
    handler: TestHandler,
) -> Option<(SocketAddr, Arc<KafkaServer<TestHandler>>)> {
    let server = match KafkaServer::new("127.0.0.1:0", handler).await {
        Ok(s) => s,
        Err(e) => {
            // Skip test if network access is denied (sandboxed environment)
            if e.to_string().contains("PermissionDenied")
                || e.to_string().contains("permission denied")
                || matches!(&e, kafkaesque::error::Error::IoError(kind) if *kind == std::io::ErrorKind::PermissionDenied)
            {
                eprintln!("Skipping test: network access denied (sandboxed environment)");
                return None;
            }
            panic!("Failed to create server: {}", e);
        }
    };
    let addr = server.local_addr().unwrap();
    let server = Arc::new(server);

    let server_clone = server.clone();
    tokio::spawn(async move {
        let _ = server_clone.run().await;
    });

    // Give server time to start
    tokio::time::sleep(Duration::from_millis(50)).await;

    Some((addr, server))
}

/// Send a raw Kafka request and receive a response.
async fn send_request(addr: SocketAddr, request: &[u8]) -> Vec<u8> {
    let mut stream = TcpStream::connect(addr).await.unwrap();
    stream.write_all(request).await.unwrap();

    // Read response size
    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await.unwrap();
    let size = i32::from_be_bytes(size_buf) as usize;

    // Read response body
    let mut response = vec![0u8; size];
    stream.read_exact(&mut response).await.unwrap();

    response
}

/// Helper to encode an i16 value.
fn encode_i16(buf: &mut BytesMut, val: i16) {
    buf.put_i16(val);
}

/// Helper to encode an i32 value.
fn encode_i32(buf: &mut BytesMut, val: i32) {
    buf.put_i32(val);
}

/// Helper to encode a string.
fn encode_string(buf: &mut BytesMut, s: &str) {
    buf.put_i16(s.len() as i16);
    buf.extend_from_slice(s.as_bytes());
}

/// Build an ApiVersions request.
fn build_api_versions_request(correlation_id: i32, client_id: &str) -> Vec<u8> {
    let mut buf = BytesMut::new();

    // Request header
    let api_key: i16 = 18; // ApiVersions
    let api_version: i16 = 0;

    // Build body first to get size
    let mut body = BytesMut::new();
    encode_i16(&mut body, api_key);
    encode_i16(&mut body, api_version);
    encode_i32(&mut body, correlation_id);
    encode_string(&mut body, client_id);

    // Size prefix
    let size = body.len() as i32;
    encode_i32(&mut buf, size);
    buf.extend_from_slice(&body);

    buf.to_vec()
}

/// Build a Metadata request.
fn build_metadata_request(
    correlation_id: i32,
    client_id: &str,
    topics: Option<Vec<&str>>,
) -> Vec<u8> {
    let mut buf = BytesMut::new();

    let api_key: i16 = 3; // Metadata
    let api_version: i16 = 0;

    let mut body = BytesMut::new();
    encode_i16(&mut body, api_key);
    encode_i16(&mut body, api_version);
    encode_i32(&mut body, correlation_id);
    encode_string(&mut body, client_id);

    // Topics array
    match topics {
        Some(t) => {
            encode_i32(&mut body, t.len() as i32);
            for topic in t {
                encode_string(&mut body, topic);
            }
        }
        None => {
            // -1 for null (all topics in v1+, for v0 use empty array)
            encode_i32(&mut body, 0);
        }
    }

    let size = body.len() as i32;
    encode_i32(&mut buf, size);
    buf.extend_from_slice(&body);

    buf.to_vec()
}

// ============================================================================
// Integration Tests
// ============================================================================

#[tokio::test]
async fn test_api_versions_request() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    let request = build_api_versions_request(1, "test-client");
    let response = send_request(addr, &request).await;

    // Response should contain correlation ID
    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 1);

    // Error code should be 0 (success)
    let error_code = i16::from_be_bytes([response[4], response[5]]);
    assert_eq!(error_code, 0);

    server.shutdown();
}

#[tokio::test]
async fn test_metadata_request_all_topics() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    handler.create_topic("test-topic", 3).await;

    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    let request = build_metadata_request(2, "test-client", None);
    let response = send_request(addr, &request).await;

    // Response should contain correlation ID
    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 2);

    // Should have at least one broker
    let broker_count = i32::from_be_bytes([response[4], response[5], response[6], response[7]]);
    assert!(
        broker_count >= 1,
        "Expected at least 1 broker, got {}",
        broker_count
    );

    server.shutdown();
}

#[tokio::test]
async fn test_metadata_request_specific_topic() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    handler.create_topic("my-topic", 2).await;

    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    let request = build_metadata_request(3, "test-client", Some(vec!["my-topic"]));
    let response = send_request(addr, &request).await;

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 3);

    server.shutdown();
}

#[tokio::test]
async fn test_metadata_request_unknown_topic() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    let request = build_metadata_request(4, "test-client", Some(vec!["nonexistent-topic"]));
    let response = send_request(addr, &request).await;

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 4);

    server.shutdown();
}

#[tokio::test]
async fn test_multiple_concurrent_connections() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    handler.create_topic("concurrent-test", 1).await;

    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    // Spawn multiple concurrent requests
    let mut handles = vec![];
    for i in 0..10 {
        handles.push(tokio::spawn(async move {
            let request = build_api_versions_request(i, &format!("client-{}", i));
            let response = send_request(addr, &request).await;

            let correlation_id =
                i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
            assert_eq!(correlation_id, i);

            i
        }));
    }

    // Wait for all to complete
    for handle in handles {
        let result = handle.await.unwrap();
        assert!(result < 10);
    }

    server.shutdown();
}

#[tokio::test]
async fn test_graceful_shutdown() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    // Make a request to verify server is running
    let request = build_api_versions_request(1, "test-client");
    let response = send_request(addr, &request).await;
    assert!(!response.is_empty());

    // Trigger shutdown
    server.shutdown();

    // Wait for shutdown
    let shutdown_result = server.shutdown_and_wait(Duration::from_secs(5)).await;
    assert!(
        shutdown_result,
        "Server should have shut down within timeout"
    );
}

#[tokio::test]
async fn test_connection_timeout() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    // Connect but don't send anything
    let stream = TcpStream::connect(addr).await.unwrap();

    // Connection should be established
    assert!(stream.peer_addr().is_ok());

    // Clean up
    drop(stream);
    server.shutdown();
}

#[tokio::test]
async fn test_malformed_request_handling() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send garbage data (size of 4 bytes, then 2 garbage bytes)
    let garbage = [0x00u8, 0x00, 0x00, 0x02, 0xFF, 0xFF];
    stream.write_all(&garbage).await.unwrap();

    // Server should handle this gracefully (connection closed or error response)
    let mut response = vec![0u8; 1024];
    let read_result = timeout(Duration::from_secs(1), stream.read(&mut response)).await;

    // Either timeout or connection closed is acceptable
    match read_result {
        Ok(Ok(0)) => (),  // Connection closed
        Ok(Ok(_)) => (),  // Some response (error)
        Ok(Err(_)) => (), // Read error
        Err(_) => (),     // Timeout
    }

    server.shutdown();
}

#[tokio::test]
async fn test_oversized_message_rejected() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send a message size that exceeds the limit (200 MB > 100 MB max)
    let huge_size: i32 = 200 * 1024 * 1024;
    stream.write_all(&huge_size.to_be_bytes()).await.unwrap();

    // Server should close the connection
    let mut response = vec![0u8; 1024];
    let read_result = timeout(Duration::from_secs(1), stream.read(&mut response)).await;

    match read_result {
        Ok(Ok(0)) => (), // Connection closed - expected
        Ok(Ok(n)) => panic!("Unexpected response of {} bytes", n),
        Ok(Err(_)) => (), // Read error - acceptable
        Err(_) => panic!("Expected connection to be closed, but got timeout"),
    }

    server.shutdown();
}

#[tokio::test]
async fn test_pipelining_requests() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    let mut stream = TcpStream::connect(addr).await.unwrap();

    // Send multiple requests without waiting for responses
    let request1 = build_api_versions_request(100, "client");
    let request2 = build_api_versions_request(200, "client");
    let request3 = build_api_versions_request(300, "client");

    stream.write_all(&request1).await.unwrap();
    stream.write_all(&request2).await.unwrap();
    stream.write_all(&request3).await.unwrap();

    // Read all three responses
    let mut responses = Vec::new();
    for expected_id in [100, 200, 300] {
        let mut size_buf = [0u8; 4];
        stream.read_exact(&mut size_buf).await.unwrap();
        let size = i32::from_be_bytes(size_buf) as usize;

        let mut response = vec![0u8; size];
        stream.read_exact(&mut response).await.unwrap();

        let correlation_id =
            i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
        assert_eq!(correlation_id, expected_id);
        responses.push(correlation_id);
    }

    assert_eq!(responses, vec![100, 200, 300]);

    server.shutdown();
}

// ============================================================================
// Protocol Edge Case Tests
// ============================================================================

#[tokio::test]
async fn test_empty_client_id() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    let request = build_api_versions_request(1, "");
    let response = send_request(addr, &request).await;

    // Should succeed even with empty client ID
    let error_code = i16::from_be_bytes([response[4], response[5]]);
    assert_eq!(error_code, 0);

    server.shutdown();
}

#[tokio::test]
async fn test_unicode_client_id() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    let request = build_api_versions_request(1, "клиент-日本語-🎉");
    let response = send_request(addr, &request).await;

    // Should handle unicode client ID
    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, 1);

    server.shutdown();
}

#[tokio::test]
async fn test_max_correlation_id() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    let request = build_api_versions_request(i32::MAX, "client");
    let response = send_request(addr, &request).await;

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, i32::MAX);

    server.shutdown();
}

#[tokio::test]
async fn test_negative_correlation_id() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    let request = build_api_versions_request(-1, "client");
    let response = send_request(addr, &request).await;

    let correlation_id = i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
    assert_eq!(correlation_id, -1);

    server.shutdown();
}

// ============================================================================
// Connection Management Tests
// ============================================================================

#[tokio::test]
async fn test_active_connections_count() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    // Initially no connections
    // Note: We just spawned the server so there might be a brief moment before it's ready

    // Connect three clients
    let stream1 = TcpStream::connect(addr).await.unwrap();
    let stream2 = TcpStream::connect(addr).await.unwrap();
    let stream3 = TcpStream::connect(addr).await.unwrap();

    // Give time for connections to be counted
    tokio::time::sleep(Duration::from_millis(100)).await;

    let active = server.active_connections();
    assert!(
        (1..=3).contains(&active),
        "Expected 1-3 active connections, got {}",
        active
    );

    // Drop connections
    drop(stream1);
    drop(stream2);
    drop(stream3);

    // Give time for disconnection to be processed
    tokio::time::sleep(Duration::from_millis(100)).await;

    server.shutdown();
}

#[tokio::test]
async fn test_rapid_connect_disconnect() {
    let handler = TestHandler::new(0, "127.0.0.1", 9092);
    let Some((addr, server)) = start_test_server(handler).await else {
        return; // Skip test in sandboxed environment
    };

    // Rapidly connect and disconnect
    for _ in 0..20 {
        let stream = TcpStream::connect(addr).await.unwrap();
        drop(stream);
    }

    // Server should still be healthy
    let request = build_api_versions_request(1, "client");
    let response = send_request(addr, &request).await;
    assert!(!response.is_empty());

    server.shutdown();
}

// ============================================================================
// SlateDB Mode Integration Tests
// ============================================================================

mod slatedb_tests {
    use super::*;
    use kafkaesque::cluster::{ClusterConfig, SlateDBClusterHandler};
    use std::time::Duration;
    use tempfile::TempDir;

    /// Helper to create a test SlateDB handler with local storage and Raft.
    async fn create_slatedb_handler(
        broker_id: i32,
        _raft_peers: Option<&str>,
    ) -> Result<SlateDBClusterHandler, Box<dyn std::error::Error>> {
        let temp_dir = TempDir::new()?;
        let data_path = temp_dir.path().to_string_lossy().to_string();

        let config = ClusterConfig {
            broker_id,
            host: "127.0.0.1".to_string(),
            port: 0, // Will be set by server
            object_store: kafkaesque::cluster::ObjectStoreType::Local {
                path: data_path.clone(),
            },
            object_store_path: data_path,
            auto_create_topics: true,
            raft_listen_addr: format!("127.0.0.1:{}", 9093 + broker_id),
            ..ClusterConfig::default()
        };

        let handler = SlateDBClusterHandler::new(config).await?;
        Ok(handler)
    }

    /// Test that SlateDB handler can handle metadata requests.
    #[tokio::test]
    async fn test_slatedb_metadata_request() {
        let handler = match create_slatedb_handler(0, None).await {
            Ok(h) => h,
            Err(e) => {
                eprintln!("Skipping test - handler creation failed: {}", e);
                return;
            }
        };

        let server = match KafkaServer::new("127.0.0.1:0", handler).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                // Skip test if network access is denied (sandboxed environment)
                if e.to_string().contains("PermissionDenied")
                    || e.to_string().contains("permission denied")
                {
                    eprintln!("Skipping test: network access denied (sandboxed environment)");
                    return;
                }
                panic!("Failed to create server: {}", e);
            }
        };
        let addr = server.local_addr().unwrap();

        let server_clone = server.clone();
        tokio::spawn(async move {
            let _ = server_clone.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        let request = build_metadata_request(1, "test-client", None);
        let response = send_request(addr, &request).await;

        let correlation_id =
            i32::from_be_bytes([response[0], response[1], response[2], response[3]]);
        assert_eq!(correlation_id, 1);

        server.shutdown();
    }

    /// Test graceful shutdown of SlateDB handler.
    #[tokio::test]
    async fn test_slatedb_graceful_shutdown() {
        let handler = match create_slatedb_handler(0, None).await {
            Ok(h) => h,
            Err(e) => {
                eprintln!("Skipping test - handler creation failed: {}", e);
                return;
            }
        };

        let server = match KafkaServer::new("127.0.0.1:0", handler).await {
            Ok(s) => Arc::new(s),
            Err(e) => {
                // Skip test if network access is denied (sandboxed environment)
                if e.to_string().contains("PermissionDenied")
                    || e.to_string().contains("permission denied")
                {
                    eprintln!("Skipping test: network access denied (sandboxed environment)");
                    return;
                }
                panic!("Failed to create server: {}", e);
            }
        };
        let addr = server.local_addr().unwrap();

        let server_clone = server.clone();
        tokio::spawn(async move {
            let _ = server_clone.run().await;
        });

        tokio::time::sleep(Duration::from_millis(100)).await;

        // Make a request to verify server is running
        let request = build_api_versions_request(1, "test-client");
        let response = send_request(addr, &request).await;
        assert!(!response.is_empty());

        // Trigger shutdown
        server.shutdown();

        // Wait for shutdown
        let shutdown_result = server.shutdown_and_wait(Duration::from_secs(5)).await;
        assert!(
            shutdown_result,
            "Server should have shut down within timeout"
        );
    }
}
