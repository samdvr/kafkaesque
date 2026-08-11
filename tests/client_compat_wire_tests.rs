//! Over-the-wire client compatibility honesty.
//!
//! Complements `client_compatibility_matrix_tests.rs` (table locks) with
//! TCP-level checks: ApiVersions advertises the documented ceilings, and
//! forcing a modern group API version (JoinGroup v5) yields
//! `UnsupportedVersion` (35) without tearing down the connection.

use std::sync::Arc;
use std::time::Duration;

use bytes::{Buf, BufMut, BytesMut};
use kafkaesque::cluster::{ClusterConfig, ObjectStoreType, SlateDBClusterHandler};
use kafkaesque::error::KafkaCode;
use kafkaesque::server::KafkaServer;
use kafkaesque::server::request::ApiKey;
use tempfile::TempDir;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::time::timeout;

mod common;
use common::{enable_single_node_bootstrap, next_port};

async fn start_in_memory_broker()
-> Option<(String, Arc<KafkaServer<SlateDBClusterHandler>>, TempDir)> {
    enable_single_node_bootstrap();

    let tempdir = TempDir::new().expect("tempdir");
    let data_path = tempdir.path().to_string_lossy().to_string();
    let kafka_port = next_port();
    let raft_port = next_port();

    let config = ClusterConfig {
        broker_id: 0,
        host: "127.0.0.1".to_string(),
        advertised_host: "127.0.0.1".to_string(),
        port: kafka_port as i32,
        raft_listen_addr: format!("127.0.0.1:{raft_port}"),
        auto_create_topics: true,
        default_num_partitions: 1,
        object_store: ObjectStoreType::Local {
            path: data_path.clone(),
        },
        object_store_path: data_path,
        ..Default::default()
    };

    let handler = SlateDBClusterHandler::new(config)
        .await
        .expect("handler init");

    let bind = format!("127.0.0.1:{kafka_port}");
    let server = match KafkaServer::new(&bind, handler).await {
        Ok(s) => s,
        Err(e) => {
            if e.to_string().to_lowercase().contains("permission denied") {
                return None;
            }
            panic!("KafkaServer::new failed: {e}");
        }
    };

    let addr = server.local_addr().expect("addr").to_string();
    let server = Arc::new(server);
    let server_clone = server.clone();
    tokio::spawn(async move {
        let _ = server_clone.run().await;
    });

    let deadline = std::time::Instant::now() + Duration::from_secs(5);
    while std::time::Instant::now() < deadline {
        if TcpStream::connect(&addr).await.is_ok() {
            return Some((addr, server, tempdir));
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    panic!("broker did not accept connections within 5s")
}

fn request_header(api_key: i16, api_version: i16, correlation_id: i32) -> BytesMut {
    let mut buf = BytesMut::new();
    buf.put_i16(api_key);
    buf.put_i16(api_version);
    buf.put_i32(correlation_id);
    buf.put_i16(-1); // null client_id
    buf
}

async fn write_frame(stream: &mut TcpStream, body: &[u8]) {
    let mut frame = BytesMut::with_capacity(4 + body.len());
    frame.put_i32(body.len() as i32);
    frame.extend_from_slice(body);
    stream.write_all(&frame).await.expect("write frame");
}

async fn read_frame(stream: &mut TcpStream) -> BytesMut {
    let mut len_buf = [0u8; 4];
    stream.read_exact(&mut len_buf).await.expect("read len");
    let len = i32::from_be_bytes(len_buf) as usize;
    let mut body = vec![0u8; len];
    stream.read_exact(&mut body).await.expect("read body");
    BytesMut::from(&body[..])
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn api_versions_advertises_documented_group_ceilings() {
    let Some((addr, _server, _tempdir)) = start_in_memory_broker().await else {
        eprintln!("Skipping: no TCP bind permission");
        return;
    };

    let mut stream = timeout(Duration::from_secs(5), TcpStream::connect(&addr))
        .await
        .expect("connect timeout")
        .expect("connect");

    let body = request_header(ApiKey::ApiVersions.into(), 0, 7);
    write_frame(&mut stream, &body).await;
    let mut resp = read_frame(&mut stream).await;
    assert_eq!(resp.get_i32(), 7);
    assert_eq!(resp.get_i16(), 0, "ApiVersions must succeed");
    let n = resp.get_i32();
    assert!(n > 0);

    let mut join_max = None;
    let mut offset_commit_max = None;
    let mut offset_fetch_max = None;
    for _ in 0..n {
        let key = resp.get_i16();
        let _min = resp.get_i16();
        let max = resp.get_i16();
        match ApiKey::from(key) {
            ApiKey::JoinGroup => join_max = Some(max),
            ApiKey::OffsetCommit => offset_commit_max = Some(max),
            ApiKey::OffsetFetch => offset_fetch_max = Some(max),
            _ => {}
        }
    }
    assert_eq!(join_max, Some(4), "JoinGroup ceiling must stay at v4");
    assert_eq!(offset_commit_max, Some(6), "OffsetCommit ceiling is v6");
    assert_eq!(offset_fetch_max, Some(5), "OffsetFetch ceiling is v5");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn join_group_v5_returns_unsupported_version_and_keeps_connection() {
    let Some((addr, _server, _tempdir)) = start_in_memory_broker().await else {
        eprintln!("Skipping: no TCP bind permission");
        return;
    };

    let mut stream = timeout(Duration::from_secs(5), TcpStream::connect(&addr))
        .await
        .expect("connect timeout")
        .expect("connect");

    let body = request_header(ApiKey::JoinGroup.into(), 5, 99);
    write_frame(&mut stream, &body).await;
    let mut resp = read_frame(&mut stream).await;
    assert_eq!(resp.get_i32(), 99);
    assert_eq!(
        resp.get_i16(),
        KafkaCode::UnsupportedVersion as i16,
        "JoinGroup v5 must return UnsupportedVersion (35)"
    );

    let body = request_header(ApiKey::ApiVersions.into(), 0, 100);
    write_frame(&mut stream, &body).await;
    let mut resp = read_frame(&mut stream).await;
    assert_eq!(resp.get_i32(), 100);
    assert_eq!(resp.get_i16(), 0, "follow-up ApiVersions must succeed");
}

/// kafka-clients 3.7 OffsetFetch v5 body: `group_id` + nullable topics
/// array, **no** `require_stable` (that flag is v7+ only). A broker that
/// still expects the phantom BOOLEAN returns the generic 2-byte
/// `ErrorResponseData`; Java then underflows at `throttleTimeMs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn offset_fetch_v5_java_shaped_request_returns_valid_response_body() {
    let Some((addr, _server, _tempdir)) = start_in_memory_broker().await else {
        eprintln!("Skipping: no TCP bind permission");
        return;
    };

    let mut stream = timeout(Duration::from_secs(5), TcpStream::connect(&addr))
        .await
        .expect("connect timeout")
        .expect("connect");

    let correlation_id = 5_501i32;
    let mut body = request_header(ApiKey::OffsetFetch.into(), 5, correlation_id);
    // client_id like kafka-clients ("consumer-…") already null in header helper;
    // body matches OffsetFetchRequest v2–v5:
    //   group_id STRING + topics NULLABLE_ARRAY of {name, [partition_indexes]}
    let group = b"java-smoke-group";
    body.put_i16(group.len() as i16);
    body.extend_from_slice(group);
    body.put_i32(1); // 1 topic (not null)
    let topic = b"java-smoke-topic";
    body.put_i16(topic.len() as i16);
    body.extend_from_slice(topic);
    body.put_i32(1); // 1 partition index
    body.put_i32(0);
    // NO require_stable byte

    write_frame(&mut stream, &body).await;
    let mut resp = read_frame(&mut stream).await;
    assert_eq!(resp.get_i32(), correlation_id);

    // Generic InvalidRequest is only 2 bytes after correlation_id. A valid
    // OffsetFetch v5 empty-ish body is at least throttle(4)+topics_len(4)+error(2).
    assert!(
        resp.remaining() >= 10,
        "OffsetFetch v5 response body too short ({} bytes): likely generic 2-byte ErrorResponse \
         from a failed request parse (require_stable phantom byte?)",
        resp.remaining()
    );

    let throttle = resp.get_i32();
    assert_eq!(throttle, 0, "v5 response must start with throttleTimeMs");
    let topics_len = resp.get_i32();
    assert_eq!(topics_len, 1, "echo the requested topic");
    let name_len = resp.get_i16() as usize;
    let mut name = vec![0u8; name_len];
    resp.copy_to_slice(&mut name);
    assert_eq!(name, topic);
    let parts_len = resp.get_i32();
    assert_eq!(parts_len, 1);
    let partition_index = resp.get_i32();
    assert_eq!(partition_index, 0);
    let _committed_offset = resp.get_i64();
    let leader_epoch = resp.get_i32();
    assert_eq!(leader_epoch, -1, "v5 committed_leader_epoch");
    let meta_len = resp.get_i16();
    assert_eq!(meta_len, -1, "null metadata");
    let part_error = resp.get_i16();
    assert_eq!(part_error, 0);
    let top_error = resp.get_i16();
    assert_eq!(top_error, 0, "top-level OffsetFetch error_code");
    assert_eq!(resp.remaining(), 0, "must consume entire v5 body");
}
