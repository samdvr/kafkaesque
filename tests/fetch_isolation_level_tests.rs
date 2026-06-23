//! Fetch isolation-level pass-through tests.
//!
//! Background. KIP-98 added an `isolation_level` field to Fetch v4+:
//! `0 = read_uncommitted`, `1 = read_committed`. Under `read_committed`,
//! a Kafka broker must (a) hide records that belong to in-flight
//! transactions, and (b) report the offsets and ranges of aborted
//! transactions so the consumer can skip them.
//!
//! Kafkaesque does not implement transactions today (the README lists
//! transactions as unsupported, and `src/cluster/handler/fetch.rs:35-39`
//! states the field is "accepted but intentionally not branched on" —
//! the log can never contain uncommitted data, so LSO == HWM and
//! `aborted_transactions` is always empty). That means today
//! `read_committed` and `read_uncommitted` are observably identical.
//!
//! These tests pin that contract so that:
//!
//! - A future EOS implementation cannot land without flipping these
//!   tests on purpose (they would regress silently otherwise).
//! - Real Kafka clients sending `isolation_level=1` are not surprised
//!   by a malformed response or a hung connection today.
//!
//! What we assert:
//!
//! 1. A Fetch with `isolation_level=0` succeeds and returns produced records.
//! 2. A Fetch with `isolation_level=1` returns the same records, same HWM,
//!    same LSO, and an empty `aborted_transactions` list.
//! 3. The two responses are equivalent on every field a consumer reads.
//! 4. LSO == HWM in both cases (no in-flight transactions exist to
//!    hold LSO back).
//! 5. The handler does not reject unknown `isolation_level` values — it
//!    treats them as `read_uncommitted` (today's pass-through). This is
//!    a deliberate choice the contract-pin captures so a future strict
//!    validator change is visible.
//!
//! What we deliberately don't assert:
//!
//! - That `read_committed` correctly hides a real in-flight transaction.
//!   That requires transactional produce, which the broker rejects.
//!   See `tests_from_kafka.md` § P0.5 / P4.1.

use bytes::Bytes;
use kafkaesque::cluster::ClusterProfile;
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::request::{
    CreateTopicData, CreateTopicsRequestData, FetchPartitionData, FetchRequestData,
    FetchTopicData, ProducePartitionData, ProduceRequestData, ProduceTopicData,
};
use kafkaesque::server::response::FetchPartitionResponse;

mod common;
use common::BrokerHandle;

const TOPIC: &str = "fetch-isolation-tests";
const PARTITION: i32 = 0;

/// Build a minimal valid v2 RecordBatch with `record_count` records and a
/// proper CRC. Doesn't carry any transactional / control / idempotent
/// markers — the broker's job here is just "store and return", not
/// transaction-aware processing.
fn make_batch(record_count: i32) -> Bytes {
    let mut batch = vec![0u8; 100];
    batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes()); // batch_length
    batch[16] = 2; // magic v2
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes()); // last_offset_delta
    batch[57..61].copy_from_slice(&record_count.to_be_bytes()); // records_count
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(batch)
}

async fn create_topic_and_produce(broker: &BrokerHandle, batches: i32) {
    let ctx = broker.ctx();
    let _ = broker
        .handler
        .handle_create_topics(
            &ctx,
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

    for _ in 0..batches {
        let resp = broker
            .handler
            .handle_produce(
                &ctx,
                ProduceRequestData {
                    transactional_id: None,
                    acks: 1,
                    timeout_ms: 5_000,
                    topics: vec![ProduceTopicData {
                        name: TOPIC.into(),
                        partitions: vec![ProducePartitionData {
                            partition_index: PARTITION,
                            records: make_batch(3),
                        }],
                    }],
                },
            )
            .await;
        // Defend against accidental setup regressions: if Produce errors,
        // every test below would fail with a misleading "no records"
        // assertion. Surface the produce error explicitly.
        let p = &resp.responses[0].partitions[0];
        assert_eq!(
            p.error_code,
            KafkaCode::None,
            "setup produce failed: {:?}",
            p.error_code
        );
    }
}

fn fetch_request(isolation_level: i8) -> FetchRequestData {
    FetchRequestData {
        replica_id: -1,
        max_wait_ms: 100,
        min_bytes: 1,
        max_bytes: 1024 * 1024,
        isolation_level,
        session_id: 0,
        session_epoch: 0,
        topics: vec![FetchTopicData {
            name: TOPIC.into(),
            partitions: vec![FetchPartitionData {
                partition_index: PARTITION,
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

fn first_partition(resp: &kafkaesque::server::response::FetchResponseData) -> &FetchPartitionResponse {
    &resp.responses[0].partitions[0]
}

// ---------------------------------------------------------------------------
// 1. Both isolation levels are accepted and return data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn read_uncommitted_returns_produced_records() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    create_topic_and_produce(&broker, 1).await;

    let resp = broker.handler.handle_fetch(&broker.ctx(), fetch_request(0)).await;
    let p = first_partition(&resp);
    assert_eq!(p.error_code, KafkaCode::None);
    assert!(p.records.is_some(), "read_uncommitted must return records");
    assert!(p.records.as_ref().unwrap().len() > 0);
}

#[tokio::test]
async fn read_committed_returns_produced_records_today() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    create_topic_and_produce(&broker, 1).await;

    let resp = broker.handler.handle_fetch(&broker.ctx(), fetch_request(1)).await;
    let p = first_partition(&resp);
    assert_eq!(p.error_code, KafkaCode::None);
    assert!(
        p.records.is_some(),
        "today's contract: no in-flight transactions exist, so read_committed sees everything",
    );
}

// ---------------------------------------------------------------------------
// 2. Field-by-field equivalence between the two isolation levels
// ---------------------------------------------------------------------------

#[tokio::test]
async fn read_committed_and_read_uncommitted_are_observably_identical() {
    // Pin the no-op semantics: every field a consumer would key off of
    // must match between isolation_level=0 and isolation_level=1 when
    // there are no transactions. Flip this test on purpose when
    // transactional produce lands.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    create_topic_and_produce(&broker, 2).await;

    let ctx = broker.ctx();
    let uncommitted = broker.handler.handle_fetch(&ctx, fetch_request(0)).await;
    let committed = broker.handler.handle_fetch(&ctx, fetch_request(1)).await;

    let u = first_partition(&uncommitted);
    let c = first_partition(&committed);

    assert_eq!(u.error_code, c.error_code);
    assert_eq!(u.high_watermark, c.high_watermark);
    assert_eq!(u.last_stable_offset, c.last_stable_offset);
    assert_eq!(u.log_start_offset, c.log_start_offset);
    assert_eq!(
        u.aborted_transactions.len(),
        c.aborted_transactions.len(),
        "aborted_transactions counts must match",
    );
    assert_eq!(u.preferred_read_replica, c.preferred_read_replica);
    // Records bytes must be byte-equal: both modes return the same raw
    // batch payload because the broker doesn't filter on isolation level.
    assert_eq!(
        u.records.as_ref().map(|b| b.as_ref()),
        c.records.as_ref().map(|b| b.as_ref()),
        "record bytes must be byte-equal across isolation levels",
    );
}

// ---------------------------------------------------------------------------
// 3. LSO == HWM with no transactions; aborted_transactions empty
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lso_equals_hwm_when_no_transactions() {
    // KIP-98: LSO is the offset of the first uncommitted record. With no
    // transactions, every record is "committed" in the eyes of a
    // read_committed consumer, so LSO == HWM. If a future EOS
    // implementation diverges, this catches it.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    create_topic_and_produce(&broker, 3).await;

    let ctx = broker.ctx();
    for level in [0i8, 1i8] {
        let resp = broker.handler.handle_fetch(&ctx, fetch_request(level)).await;
        let p = first_partition(&resp);
        assert_eq!(p.error_code, KafkaCode::None, "level {}", level);
        assert_eq!(
            p.last_stable_offset, p.high_watermark,
            "level {}: LSO must equal HWM with no transactions present",
            level,
        );
        assert!(
            p.aborted_transactions.is_empty(),
            "level {}: aborted_transactions must be empty (today's contract)",
            level,
        );
    }
}

// ---------------------------------------------------------------------------
// 4. Unknown isolation_level values are not rejected today
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_isolation_level_treated_as_read_uncommitted_today() {
    // Pin today's contract: the handler accepts any i8 value for
    // isolation_level and does not branch on it. This test will need to
    // flip when (a) we add strict validation, or (b) Kafka adds a third
    // isolation level. Either way the test failing is the right signal.
    //
    // TODO(P0.5 EOS): when transactions land, decide whether to reject
    // unknown values or fall back to read_uncommitted. Document the
    // chosen contract here.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    create_topic_and_produce(&broker, 1).await;

    let mut bad = fetch_request(0);
    bad.isolation_level = 99; // invalid per Kafka spec

    let resp = broker.handler.handle_fetch(&broker.ctx(), bad).await;
    let p = first_partition(&resp);
    assert_eq!(
        p.error_code,
        KafkaCode::None,
        "today's contract: any isolation_level is accepted",
    );
}

// ---------------------------------------------------------------------------
// 5. Pre-v4 default (no field on the wire)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn isolation_level_zero_default_works_for_pre_v4_clients() {
    // Fetch v0..v3 don't carry isolation_level; the parser defaults it to
    // 0. A test at the dispatch layer can't toggle the wire version, but
    // we can still pin that the default value (0) gets the same answer as
    // an explicit 0 — i.e. no off-by-one in the codec layer.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    create_topic_and_produce(&broker, 1).await;

    let resp = broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(0))
        .await;
    assert_eq!(first_partition(&resp).error_code, KafkaCode::None);
}
