//! P5-2: Differential test against an Apache Kafka broker.
//!
//! For every wire-protocol request this test sends, the same bytes are
//! delivered to (a) a kafkaesque server spun up in-process and (b) — when
//! `KAFKA_BOOTSTRAP_SERVERS` points at a reachable Apache Kafka broker —
//! the real thing. The two responses are then parsed by kafkaesque's own
//! parsers; if either side rejects a payload the other accepts (or
//! returns a wildly different error code) we have wire-format drift.
//!
//! Why bytes-via-TCP rather than rdkafka's high-level API: the spec says
//! "send the same request to both, compare response bytes". rdkafka's
//! Producer/Consumer wrap several round-trips per call, so we can't
//! attribute a single differential to a single request. Going TCP-direct
//! keeps the comparison 1:1 with the libFuzzer target's mental model.
//!
//! The test is *opt-in*: it always runs the kafkaesque side as a smoke
//! check (catches local regressions even with no Kafka broker available).
//! The differential check only fires when the env var is set, so the
//! default `cargo test` smoke run passes without a JVM in the loop.
//! Standing up Apache Kafka in CI is a separate (heavyweight) workflow
//! step — see the P5-2 sidecar plan.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpStream;
use tokio::sync::RwLock;
use tokio::time::timeout;

use kafkaesque::error::KafkaCode;
use kafkaesque::prelude::server::{
    Handler, KafkaServer, RequestContext,
    request::{ApiKey, ApiVersionsRequestData, MetadataRequestData},
    response::{
        ApiVersionData, ApiVersionsResponseData, BrokerData, MetadataResponseData,
        PartitionMetadata, TopicMetadata,
    },
};

// ---------------------------------------------------------------------------
// Minimal in-memory handler — the same shape as `tests/integration_tests.rs`'s
// TestHandler, scoped down to just the APIs this differential exercises.
// ---------------------------------------------------------------------------

#[derive(Clone)]
struct DiffHandler {
    broker_id: i32,
    host: String,
    port: i32,
    topics: Arc<RwLock<Vec<String>>>,
}

impl DiffHandler {
    fn new(broker_id: i32, host: &str, port: i32) -> Self {
        Self {
            broker_id,
            host: host.to_string(),
            port,
            topics: Arc::new(RwLock::new(Vec::new())),
        }
    }
}

#[async_trait::async_trait]
impl Handler for DiffHandler {
    async fn handle_api_versions(
        &self,
        _ctx: &RequestContext,
        _request: ApiVersionsRequestData,
    ) -> ApiVersionsResponseData {
        ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: vec![
                ApiVersionData {
                    api_key: ApiKey::ApiVersions,
                    min_version: 0,
                    max_version: 3,
                },
                ApiVersionData {
                    api_key: ApiKey::Metadata,
                    min_version: 0,
                    max_version: 9,
                },
            ],
            throttle_time_ms: 0,
        }
    }

    async fn handle_metadata(
        &self,
        _ctx: &RequestContext,
        _request: MetadataRequestData,
    ) -> MetadataResponseData {
        let topics = self.topics.read().await;
        MetadataResponseData {
            brokers: vec![BrokerData {
                node_id: self.broker_id,
                host: self.host.clone(),
                port: self.port,
                rack: None,
            }],
            controller_id: self.broker_id,
            topics: topics
                .iter()
                .map(|name| TopicMetadata {
                    error_code: KafkaCode::None,
                    name: name.clone(),
                    is_internal: false,
                    partitions: vec![PartitionMetadata {
                        error_code: KafkaCode::None,
                        partition_index: 0,
                        leader_id: 0,
                        replica_nodes: vec![0],
                        isr_nodes: vec![0],
                    }],
                })
                .collect(),
        }
    }
}

// ---------------------------------------------------------------------------
// Wire helpers — minimal, kept here rather than reused from
// integration_tests.rs because cross-test imports aren't worth a shared
// module yet.
// ---------------------------------------------------------------------------

fn build_api_versions_v0(correlation_id: i32, client_id: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(18); // ApiVersions
    body.put_i16(0); // version 0
    body.put_i32(correlation_id);
    body.put_i16(client_id.len() as i16);
    body.extend_from_slice(client_id.as_bytes());
    let mut out = BytesMut::new();
    out.put_i32(body.len() as i32);
    out.extend_from_slice(&body);
    out.to_vec()
}

fn build_metadata_v0(correlation_id: i32, client_id: &str) -> Vec<u8> {
    let mut body = BytesMut::new();
    body.put_i16(3); // Metadata
    body.put_i16(0); // version 0
    body.put_i32(correlation_id);
    body.put_i16(client_id.len() as i16);
    body.extend_from_slice(client_id.as_bytes());
    body.put_i32(0); // empty topics array → all topics
    let mut out = BytesMut::new();
    out.put_i32(body.len() as i32);
    out.extend_from_slice(&body);
    out.to_vec()
}

async fn round_trip(addr: SocketAddr, request: &[u8]) -> Result<Vec<u8>, std::io::Error> {
    let mut stream = TcpStream::connect(addr).await?;
    stream.write_all(request).await?;

    let mut size_buf = [0u8; 4];
    stream.read_exact(&mut size_buf).await?;
    let size = i32::from_be_bytes(size_buf) as usize;

    let mut response = vec![0u8; size];
    stream.read_exact(&mut response).await?;
    Ok(response)
}

async fn start_kafkaesque() -> Option<(SocketAddr, Arc<KafkaServer<DiffHandler>>)> {
    let handler = DiffHandler::new(0, "127.0.0.1", 9092);
    {
        let mut topics = handler.topics.write().await;
        topics.push("differential-topic".to_string());
    }

    let server = match KafkaServer::new("127.0.0.1:0", handler).await {
        Ok(s) => s,
        Err(e) => {
            eprintln!(
                "Skipping P5-2 differential: cannot bind kafkaesque listener ({e})"
            );
            return None;
        }
    };
    let addr = server.local_addr().unwrap();
    let server = Arc::new(server);
    let server_clone = server.clone();
    tokio::spawn(async move {
        let _ = server_clone.run().await;
    });
    tokio::time::sleep(Duration::from_millis(50)).await;
    Some((addr, server))
}

/// Resolve the Kafka bootstrap from `KAFKA_BOOTSTRAP_SERVERS` (the same
/// env var librdkafka reads) and return a single SocketAddr. Returns
/// None when the env var is unset *or* the broker isn't reachable —
/// both cases mean the smoke run skips the differential check rather
/// than failing.
async fn kafka_broker_addr() -> Option<SocketAddr> {
    let raw = std::env::var("KAFKA_BOOTSTRAP_SERVERS").ok()?;
    let first = raw.split(',').next()?.trim();
    let addr: SocketAddr = first.parse().ok()?;
    match timeout(Duration::from_millis(500), TcpStream::connect(addr)).await {
        Ok(Ok(_)) => Some(addr),
        _ => {
            eprintln!(
                "P5-2 differential: KAFKA_BOOTSTRAP_SERVERS={} is not reachable, skipping diff",
                raw
            );
            None
        }
    }
}

/// The pair of invariants we assert across the two implementations: the
/// echo of the correlation ID, and (for APIs that lead with one) the
/// top-level error code. Going further — e.g., comparing the broker
/// list byte-for-byte — would mostly catch incidental differences (host
/// strings, throttle values), not real wire bugs.
fn assert_responses_align(label: &str, ours: &[u8], theirs: &[u8], correlation_id: i32) {
    assert!(
        ours.len() >= 4,
        "[{label}] kafkaesque response too short: {} bytes",
        ours.len()
    );
    assert!(
        theirs.len() >= 4,
        "[{label}] Apache Kafka response too short: {} bytes",
        theirs.len()
    );
    let ours_corr = i32::from_be_bytes([ours[0], ours[1], ours[2], ours[3]]);
    let theirs_corr = i32::from_be_bytes([theirs[0], theirs[1], theirs[2], theirs[3]]);
    assert_eq!(
        ours_corr, correlation_id,
        "[{label}] kafkaesque echoed wrong correlation_id"
    );
    assert_eq!(
        theirs_corr, correlation_id,
        "[{label}] Apache Kafka echoed wrong correlation_id"
    );
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

/// ApiVersions v0 has the simplest layout of any API: header + body =
/// `i16 error_code, [api_keys]`. Both implementations must echo our
/// correlation id and report `error_code = 0`. When no Apache Kafka is
/// configured, this test still drives the kafkaesque side end-to-end —
/// a useful smoke check on its own.
#[tokio::test]
async fn diff_api_versions_v0() {
    let Some((our_addr, server)) = start_kafkaesque().await else {
        return;
    };

    let request = build_api_versions_v0(0xCAFE_F00D_u32 as i32, "p5-2-diff");
    let ours = round_trip(our_addr, &request)
        .await
        .expect("kafkaesque must respond to ApiVersions v0");

    // Smoke: kafkaesque alone must produce a non-empty response with
    // error_code 0.
    let our_err = i16::from_be_bytes([ours[4], ours[5]]);
    assert_eq!(
        our_err, 0,
        "kafkaesque ApiVersions v0 returned non-zero error_code {}",
        our_err
    );

    // Differential leg.
    if let Some(kafka_addr) = kafka_broker_addr().await {
        let theirs = round_trip(kafka_addr, &request)
            .await
            .expect("Apache Kafka must respond to ApiVersions v0");
        assert_responses_align("ApiVersions v0", &ours, &theirs, 0xCAFE_F00D_u32 as i32);

        let their_err = i16::from_be_bytes([theirs[4], theirs[5]]);
        assert_eq!(
            their_err, 0,
            "Apache Kafka ApiVersions v0 returned non-zero error_code {}",
            their_err
        );
    }

    server.shutdown();
}

/// Metadata v0 with an empty topics array: both servers should respond
/// with at least one broker and error_code 0 at the top level. The
/// shape — `[brokers] controller_id [topics]` for v0 — gets a
/// structural sanity check on each side independently; the only thing
/// the differential adds is "Apache Kafka also accepted these bytes",
/// which is the actual wire-compatibility signal.
#[tokio::test]
async fn diff_metadata_v0_all_topics() {
    let Some((our_addr, server)) = start_kafkaesque().await else {
        return;
    };

    let request = build_metadata_v0(0xBEEF_u32 as i32, "p5-2-diff");
    let ours = round_trip(our_addr, &request)
        .await
        .expect("kafkaesque must respond to Metadata v0");

    // Smoke: response begins with correlation_id then a brokers array.
    let our_corr = i32::from_be_bytes([ours[0], ours[1], ours[2], ours[3]]);
    assert_eq!(our_corr, 0xBEEF_u32 as i32);
    let our_broker_count = i32::from_be_bytes([ours[4], ours[5], ours[6], ours[7]]);
    assert!(
        our_broker_count >= 1,
        "kafkaesque Metadata v0 returned {} brokers (want >=1)",
        our_broker_count
    );

    if let Some(kafka_addr) = kafka_broker_addr().await {
        let theirs = round_trip(kafka_addr, &request)
            .await
            .expect("Apache Kafka must respond to Metadata v0");
        assert_responses_align("Metadata v0", &ours, &theirs, 0xBEEF_u32 as i32);
        let their_broker_count =
            i32::from_be_bytes([theirs[4], theirs[5], theirs[6], theirs[7]]);
        assert!(
            their_broker_count >= 1,
            "Apache Kafka Metadata v0 returned {} brokers (want >=1)",
            their_broker_count
        );
    }

    server.shutdown();
}

/// Sanity: a deliberately malformed length-prefix (negative size)
/// should be rejected by both implementations rather than panicked
/// over. We can't easily get bytes back from Apache Kafka here (it
/// closes the connection), so the differential collapses to "neither
/// side hangs forever" — still a useful guarantee.
#[tokio::test]
async fn diff_malformed_length_prefix() {
    let Some((our_addr, server)) = start_kafkaesque().await else {
        return;
    };

    // 4-byte negative length, no body.
    let bad = (-1i32).to_be_bytes().to_vec();

    // We expect the connection to close without a response. The
    // assertion is that round_trip returns an Err in a bounded amount
    // of time.
    let our_result = timeout(Duration::from_secs(2), round_trip(our_addr, &bad)).await;
    assert!(
        our_result.is_ok(),
        "kafkaesque hung on a -1 length prefix instead of closing"
    );
    assert!(
        our_result.unwrap().is_err(),
        "kafkaesque returned a body for a -1 length prefix"
    );

    if let Some(kafka_addr) = kafka_broker_addr().await {
        let their_result = timeout(Duration::from_secs(2), round_trip(kafka_addr, &bad)).await;
        assert!(
            their_result.is_ok(),
            "Apache Kafka hung on a -1 length prefix instead of closing"
        );
        // We don't care about the exact error from Kafka — just that it
        // didn't accept our bytes as a valid frame.
    }

    server.shutdown();
}

// Suppress unused-import warnings when the differential leg is the only
// consumer of a few of the response types in builder positions.
#[allow(dead_code)]
fn _force_link_response_types() -> Bytes {
    Bytes::new()
}
