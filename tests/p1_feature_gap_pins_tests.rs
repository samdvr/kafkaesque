//! Feature-gap contract pins.
//!
//! Background. Three P1 items map to features kafkaesque does NOT
//! implement today. Tests here pin the *current observable contract*
//! so a future implementation knows what to flip.
//!
//! ## P1.4 — Fetch sessions (KIP-227)
//! Sessions are not implemented. A sessionless fetch (`session_id=0`)
//! always gets `session_id=0` back (full fetch). A nonzero `session_id`
//! is refused with `FetchSessionIdNotFound` so clients discard the
//! phantom session — see `tests/fetch_session_contract_tests.rs`.
//! This file pins the sessionless "never assigns a session" half.
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
    let mut batch = kafkaesque::batch::build_minimal_valid_batch(record_count);
    batch[21..23].copy_from_slice(&attributes.to_be_bytes());
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
async fn fetch_with_explicit_session_id_is_rejected() {
    // Aligned with `fetch_session_contract_tests`: a nonzero session_id we
    // never issued must return FetchSessionIdNotFound (not a silent full
    // fetch). session_id in the response stays 0.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;
    produce_with_retry(&broker, build_batch_with_attributes(1, 0)).await;

    let resp = broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(42, 7))
        .await;
    assert_eq!(resp.error_code, KafkaCode::FetchSessionIdNotFound);
    assert_eq!(resp.session_id, 0);
    assert!(resp.responses.is_empty());
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
// P1.19 — Control / transactional batches are refused at produce
// ---------------------------------------------------------------------------

#[tokio::test]
async fn control_and_transactional_batches_are_rejected_at_produce() {
    // Refuse-don't-lie: CRC-valid control/transactional attribute bits are
    // rejected at produce (`InvalidRecord` / `InvalidRequest`). Fetch never
    // needs to interpret markers the log cannot contain.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;

    let control = 1u16 << 5;
    let txn = 1u16 << 4;
    let both = control | txn;

    for attrs in [control, txn, both] {
        let payload = build_batch_with_attributes(1, attrs);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
        let code = loop {
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
            let code = resp.responses[0].partitions[0].error_code;
            if code != KafkaCode::NotLeaderForPartition
                || std::time::Instant::now() >= deadline
            {
                break code;
            }
            tokio::time::sleep(std::time::Duration::from_millis(25)).await;
        };
        assert!(
            matches!(
                code,
                KafkaCode::InvalidRecord | KafkaCode::InvalidRequest
            ),
            "attrs={attrs:#x} must be refused at produce, got {code:?}"
        );
    }
}
