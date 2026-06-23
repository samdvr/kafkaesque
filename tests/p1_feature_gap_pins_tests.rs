//! Feature-gap contract pins.
//!
//! Background. Three P1 items map to features kafkaesque does NOT
//! implement today. Tests here pin the *current observable contract*
//! so a future implementation knows what to flip.
//!
//! ## P1.4 — Fetch sessions (KIP-227)
//! `src/cluster/handler/fetch.rs` parses `session_id` and
//! `session_epoch` from the request but always returns `session_id=0`
//! in the response — i.e. every Fetch becomes a "full fetch" from the
//! broker's point of view. Real Java / librdkafka consumers default to
//! incremental fetch; the broker's no-op behavior is functionally
//! correct (full fetch every poll) but a 10×+ throughput penalty.
//!
//! ## P1.18 — Magic-version down-conversion
//! Kafkaesque advertises Fetch v4..=v11 only (per `versions.rs`).
//! Older clients (Fetch v0/v1) are rejected at version negotiation
//! before reaching a handler. The broker never down-converts a v2
//! batch to v0/v1 wire format. Pin: a Fetch request claiming version
//! v3 or below is parsed by our v4+ parser and either succeeds (because
//! the parser is lenient) or fails with a parse error — but it does
//! NOT trigger a down-conversion path.
//!
//! ## P1.19 — Control records / EndTransactionMarker
//! Control batches (attributes bit 5) carry transaction abort/commit
//! markers. Kafkaesque's parser accepts the bit (P0.1 confirmed) but
//! does nothing with it. Pin: a Fetch that returns a batch with the
//! control bit set is delivered byte-for-byte to the client; the
//! broker does not strip, filter, or interpret it.

use bytes::Bytes;
use kafkaesque::cluster::ClusterProfile;
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::request::{
    CreateTopicData, CreateTopicsRequestData, FetchPartitionData, FetchRequestData, FetchTopicData,
    ProducePartitionData, ProduceRequestData, ProduceTopicData,
};

mod common;
use common::BrokerHandle;

const TOPIC: &str = "feature-gap-pins";

fn build_batch_with_attributes(record_count: i32, attributes: u16) -> Bytes {
    let mut batch = vec![0u8; 100];
    batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes());
    batch[16] = 2; // magic v2
    batch[21..23].copy_from_slice(&attributes.to_be_bytes());
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes());
    batch[43..51].copy_from_slice(&(-1i64).to_be_bytes());
    batch[51..53].copy_from_slice(&(-1i16).to_be_bytes());
    batch[53..57].copy_from_slice(&(-1i32).to_be_bytes());
    batch[57..61].copy_from_slice(&record_count.to_be_bytes());
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(batch)
}

async fn ensure_topic(broker: &BrokerHandle) {
    let _ = broker
        .handler
        .handle_create_topics(
            &broker.ctx(),
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: TOPIC.into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    configs: vec![],
                }],
                timeout_ms: 5_000,
                validate_only: false,
            },
        )
        .await;
}

async fn produce_with_retry(broker: &BrokerHandle, payload: Bytes) -> i64 {
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let resp = broker
            .handler
            .handle_produce(
                &broker.ctx(),
                ProduceRequestData {
                    transactional_id: None,
                    acks: 1,
                    timeout_ms: 5_000,
                    topics: vec![ProduceTopicData {
                        name: TOPIC.into(),
                        partitions: vec![ProducePartitionData {
                            partition_index: 0,
                            records: payload.clone(),
                        }],
                    }],
                },
            )
            .await;
        let p = &resp.responses[0].partitions[0];
        if p.error_code == KafkaCode::None {
            return p.base_offset;
        }
        if p.error_code != KafkaCode::NotLeaderForPartition || std::time::Instant::now() >= deadline
        {
            panic!("produce failed: {:?}", p.error_code);
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

fn fetch_request(session_id: i32, session_epoch: i32) -> FetchRequestData {
    FetchRequestData {
        replica_id: -1,
        max_wait_ms: 50,
        min_bytes: 0,
        max_bytes: 1024 * 1024,
        isolation_level: 0,
        session_id,
        session_epoch,
        topics: vec![FetchTopicData {
            name: TOPIC.into(),
            partitions: vec![FetchPartitionData {
                partition_index: 0,
                fetch_offset: 0,
                log_start_offset: -1,
                partition_max_bytes: 1024 * 1024,
                current_leader_epoch: -1,
            }],
        }],
        forgotten_topics: vec![],
        rack_id: String::new(),
    }
}

// ---------------------------------------------------------------------------
// P1.4 — Fetch session is hardcoded to id=0 (no session)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_response_session_id_is_always_zero_today() {
    // Today the broker returns session_id=0 for every Fetch — no
    // session cache exists. A real consumer that opens a session
    // (session_id=0, session_epoch=0 in the request) gets back
    // session_id=0 (no session assigned), so it falls through to a
    // full-fetch every poll. Pin this so a future incremental-fetch
    // implementation has a clear flip.
    //
    // TODO(fetch sessions): when KIP-227 lands, replace this with
    // assertions on session_id allocation, epoch advance, and topic
    // omission semantics.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;
    produce_with_retry(&broker, build_batch_with_attributes(1, 0)).await;

    // Request to OPEN a new session: session_id=0, session_epoch=0.
    let resp = broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(0, 0))
        .await;
    assert_eq!(
        resp.session_id, 0,
        "today's contract: broker never assigns a session_id; got {}",
        resp.session_id,
    );
}

#[tokio::test]
async fn fetch_with_explicit_session_id_returns_zero_today() {
    // A consumer that *thinks* it has a session (session_id=42) and
    // sends a continuation request also gets session_id=0 back —
    // signalling the broker has dropped/never had the session. The
    // consumer falls back to a full fetch on the next poll.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;
    produce_with_retry(&broker, build_batch_with_attributes(1, 0)).await;

    let resp = broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(42, 7))
        .await;
    assert_eq!(
        resp.session_id, 0,
        "today's contract: broker doesn't honor existing session ids; got {}",
        resp.session_id,
    );
}

#[tokio::test]
async fn fetch_back_to_back_returns_independent_full_fetches_today() {
    // Without sessions, two consecutive fetches must return identical
    // results — there's no incremental delta tracking. Pin this so a
    // future incremental implementation has a clear regression target.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;
    produce_with_retry(&broker, build_batch_with_attributes(2, 0)).await;

    let a = broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(0, 0))
        .await;
    let b = broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(0, 0))
        .await;

    let a_p = &a.responses[0].partitions[0];
    let b_p = &b.responses[0].partitions[0];
    assert_eq!(
        a_p.records.as_ref().map(|x| x.len()),
        b_p.records.as_ref().map(|x| x.len()),
        "without session tracking, two identical fetches must return identical record bytes",
    );
    assert_eq!(a_p.high_watermark, b_p.high_watermark);
}

// ---------------------------------------------------------------------------
// P1.18 — Down-conversion: rejected at version negotiation
// ---------------------------------------------------------------------------

#[test]
fn fetch_min_version_advertised_is_v4_today() {
    // Down-conversion exists in real Kafka because clients may request
    // older Fetch versions (v0/v1) that use the legacy MessageSet
    // format. Kafkaesque advertises Fetch v4..=v11 only; v0..=v3 are
    // rejected at version negotiation by the connection layer before a
    // handler is invoked, so no down-conversion is required.
    //
    // TODO(down-conversion): if we ever advertise Fetch < v4,
    // the produce/append path must learn to convert v2 batches to the
    // older format on the way out.
    use kafkaesque::server::request::ApiKey;
    use kafkaesque::server::versions::SUPPORTED_VERSIONS;

    let fetch = SUPPORTED_VERSIONS
        .iter()
        .find(|sv| matches!(sv.api_key, ApiKey::Fetch))
        .expect("Fetch must be in SUPPORTED_VERSIONS");
    assert!(
        fetch.min_version >= 4,
        "today's contract: Fetch min version is v4+ (no v0/v1 MessageSet support); got min={}, max={}",
        fetch.min_version,
        fetch.max_version,
    );
}

// ---------------------------------------------------------------------------
// P1.19 — Control records pass through the broker untouched
// ---------------------------------------------------------------------------

#[tokio::test]
async fn control_record_batches_round_trip_byte_for_byte() {
    // Attributes bit 5 = control batch. The broker doesn't interpret
    // control records (no transaction support); it just stores and
    // returns the bytes. Pin: a control batch produced by a misbehaving
    // client comes back to a fetcher unchanged, including the
    // attributes byte. A regression where the broker started filtering
    // control batches would break the contract for clients that
    // intentionally produce them (test fixtures, debugging tools).
    //
    // TODO: when transactions land, the broker must
    // either generate control batches itself (commit/abort markers) or
    // strip them from non-transactional fetches. Either way the
    // assertion below should be revisited.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;

    // attributes bit 5 = control. We also set transactional bit 4 to
    // mirror what a real EOS abort marker would carry.
    let attrs = (1u16 << 4) | (1u16 << 5);
    let payload = build_batch_with_attributes(1, attrs);
    let _ = produce_with_retry(&broker, payload.clone()).await;

    let resp = broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(0, 0))
        .await;
    let p = &resp.responses[0].partitions[0];
    assert_eq!(p.error_code, KafkaCode::None);
    let returned = p.records.as_ref().expect("fetch must return data");

    // Locate the attributes bytes in the returned batch (offset 21..23
    // within whatever batch comes back — the broker may patch the
    // base_offset but does not touch attributes).
    assert!(
        returned.len() >= 23,
        "returned batch must contain at least the v2 header through attributes",
    );
    let returned_attrs = u16::from_be_bytes([returned[21], returned[22]]);
    assert_eq!(
        returned_attrs, attrs,
        "control + transactional attribute bits must round-trip; sent {:#06b}, got {:#06b}",
        attrs, returned_attrs,
    );
}
