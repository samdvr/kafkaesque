//! High-watermark and last-stable-offset advancement tests.
//!
//! Background. Kafka's `PartitionTest` (142 tests in core) covers HWM
//! monotonicity, LSO under in-flight transactions, and HWM-vs-LSO
//! divergence. Even with no peer replication (object store is the data
//! plane), kafkaesque still owes clients these semantics on every Fetch:
//!
//! - **HWM monotonicity**: HWM never decreases across appends.
//! - **HWM == log_end_offset after acks=1 produce**: there's no ISR to
//!   lag the HWM behind in this architecture; once the SlateDB write is
//!   durable, the records are committed.
//! - **LSO == HWM with no transactions**: kafkaesque rejects
//!   transactional produce, so the log can never contain uncommitted
//!   data and LSO can't trail HWM.
//! - **Fetch beyond HWM returns empty**: a consumer at the end of the
//!   log gets back zero records but a valid HWM in the response.
//! - **log_start_offset behavior**: starts at 0, never exceeds HWM.
//!
//! These are pinned at the dispatch layer (Fetch/Produce response
//! fields) because that's the surface clients actually see. Internal
//! `PartitionStore` invariants live in `src/cluster/partition_store_tests.rs`.
//!
//! What we deliberately don't assert (and the audit P0.4 spells out):
//!
//! - HWM lagging LSO due to in-flight transactions — requires
//!   transactional produce, which the broker rejects today.
//! - LSO advancing on a transaction abort marker — same prerequisite.
//! - HWM blocked by uncommitted-first-batch — same prerequisite.

use bytes::Bytes;
use kafkaesque::cluster::ClusterProfile;
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::request::{
    CreateTopicData, CreateTopicsRequestData, FetchPartitionData, FetchRequestData,
    FetchTopicData, ProducePartitionData, ProduceRequestData, ProduceTopicData,
};

mod common;
use common::BrokerHandle;

const TOPIC: &str = "hwm-tests";

fn build_batch(record_count: i32) -> Bytes {
    let mut batch = vec![0u8; 100];
    batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes());
    batch[16] = 2;
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes());
    // producer_id = -1 marks the batch non-idempotent. Without this, the
    // broker treats producer_id=0 as a real idempotent producer and
    // exact-replays of identical batches get dedup'd to the original
    // base_offset — which silently breaks every test that produces the
    // same fixture twice and expects HWM to advance.
    batch[43..51].copy_from_slice(&(-1i64).to_be_bytes());
    batch[51..53].copy_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
    batch[53..57].copy_from_slice(&(-1i32).to_be_bytes()); // base_sequence
    batch[57..61].copy_from_slice(&record_count.to_be_bytes());
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(batch)
}

async fn ensure_topic(broker: &BrokerHandle, name: &str) {
    let _ = broker
        .handler
        .handle_create_topics(
            &broker.ctx(),
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: name.into(),
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

/// Produce a batch with retry until partition write-leadership settles.
/// `handle_create_topics` only writes to Raft and returns before the
/// partition manager is ready to accept writes; a follow-up produce can
/// race and see `NotLeaderForPartition`. Centralized retry so each test
/// stays focused on the invariant it's proving.
async fn produce_records(broker: &BrokerHandle, partition: i32, record_count: i32) -> i64 {
    let ctx = broker.ctx();
    let payload = build_batch(record_count);
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
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
                            partition_index: partition,
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
        if std::time::Instant::now() >= deadline {
            panic!("produce kept failing: {:?}", p.error_code);
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

fn fetch_request(partition: i32, offset: i64) -> FetchRequestData {
    FetchRequestData {
        replica_id: -1,
        max_wait_ms: 50,
        min_bytes: 0, // accept empty so beyond-HWM tests don't long-poll
        max_bytes: 1024 * 1024,
        isolation_level: 0,
        session_id: 0,
        session_epoch: 0,
        topics: vec![FetchTopicData {
            name: TOPIC.into(),
            partitions: vec![FetchPartitionData {
                partition_index: partition,
                fetch_offset: offset,
                log_start_offset: -1,
                partition_max_bytes: 1024 * 1024,
                current_leader_epoch: -1,
            }],
        }],
        forgotten_topics: vec![],
        rack_id: String::new(),
    }
}

async fn fetch_partition(
    broker: &BrokerHandle,
    partition: i32,
    offset: i64,
) -> kafkaesque::server::response::FetchPartitionResponse {
    let resp = broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(partition, offset))
        .await;
    resp.responses[0].partitions[0].clone()
}

// ---------------------------------------------------------------------------
// 1. HWM monotonicity across multiple appends
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hwm_increases_monotonically_across_appends() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    // Drive the partition through a sequence of appends and assert the
    // HWM read after each one is strictly greater than the previous.
    let mut last_hwm = -1i64;
    for round in 0..5 {
        produce_records(&broker, 0, 3).await;
        let p = fetch_partition(&broker, 0, 0).await;
        assert_eq!(p.error_code, KafkaCode::None, "round {} fetch error", round);
        assert!(
            p.high_watermark > last_hwm,
            "round {}: HWM must strictly increase (was {}, now {})",
            round,
            last_hwm,
            p.high_watermark,
        );
        last_hwm = p.high_watermark;
    }
}

// ---------------------------------------------------------------------------
// 2. HWM advances by record_count (no ISR-induced lag in this architecture)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hwm_advances_by_record_count_after_acks_one_produce() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    // Settle write leadership before reading the baseline.
    produce_records(&broker, 0, 1).await;

    let before = fetch_partition(&broker, 0, 0).await.high_watermark;
    produce_records(&broker, 0, 7).await;
    let after = fetch_partition(&broker, 0, 0).await.high_watermark;

    assert_eq!(
        after - before,
        7,
        "HWM must advance by exactly the record_count; before={} after={}",
        before,
        after,
    );
}

// ---------------------------------------------------------------------------
// 3. LSO == HWM in the absence of transactional data
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lso_tracks_hwm_with_no_transactions() {
    // The broker never accepts transactional appends today, so LSO and
    // HWM are by definition the same offset. Pin this so a future EOS
    // implementation has to consciously decide whether (and where) the
    // two can diverge.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    for batch in 1..=3 {
        produce_records(&broker, 0, 4).await;
        let p = fetch_partition(&broker, 0, 0).await;
        assert_eq!(p.error_code, KafkaCode::None);
        assert_eq!(
            p.high_watermark, p.last_stable_offset,
            "batch {}: LSO must equal HWM (no in-flight transactions)",
            batch,
        );
    }
}

// ---------------------------------------------------------------------------
// 4. Fetch beyond HWM returns empty + valid HWM
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fetch_at_hwm_returns_empty_records_with_valid_hwm() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    produce_records(&broker, 0, 2).await;

    // Read the current HWM, then re-fetch starting AT the HWM — by
    // definition there's no committed data at offset HWM, so the
    // response must be empty (or None) but still report the correct
    // HWM. A consumer that just polled past the end of the log gets
    // exactly this.
    let hwm_probe = fetch_partition(&broker, 0, 0).await.high_watermark;
    assert!(hwm_probe > 0);
    let p = fetch_partition(&broker, 0, hwm_probe).await;
    assert_eq!(p.error_code, KafkaCode::None);
    assert_eq!(
        p.high_watermark, hwm_probe,
        "HWM should be unchanged across this consultation",
    );
    let empty = match &p.records {
        None => true,
        Some(b) => b.is_empty(),
    };
    assert!(
        empty,
        "fetch at HWM must yield no records; got {} bytes",
        p.records.as_ref().map(|b| b.len()).unwrap_or(0),
    );
}

#[tokio::test]
async fn fetch_well_beyond_hwm_does_not_panic_or_lie() {
    // A consumer with a stale offset cache may request very-high
    // offsets. Pin that the broker answers (rather than hangs) and
    // doesn't claim records exist.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    produce_records(&broker, 0, 1).await;

    let p = fetch_partition(&broker, 0, 1_000_000).await;
    let empty = match &p.records {
        None => true,
        Some(b) => b.is_empty(),
    };
    assert!(empty, "fetch far beyond HWM must yield no records");
    assert!(
        p.high_watermark < 1_000_000,
        "HWM should be the broker's real HWM, not echoed back from the request",
    );
}

// ---------------------------------------------------------------------------
// 5. log_start_offset bounds
// ---------------------------------------------------------------------------

#[tokio::test]
async fn log_start_offset_is_zero_or_minus_one_on_fresh_partition() {
    // Pre-v5 Fetch responses don't carry log_start_offset; the broker
    // signals "unknown" with `-1`. The recon noted today's broker
    // returns `-1` for it. Pin that contract: -1 OR 0 (a future change
    // that always populates the field with 0 should be deliberate, not
    // accidental).
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    produce_records(&broker, 0, 1).await;

    let p = fetch_partition(&broker, 0, 0).await;
    assert_eq!(p.error_code, KafkaCode::None);
    assert!(
        p.log_start_offset == -1 || p.log_start_offset == 0,
        "log_start_offset must be -1 (unknown) or 0 (true start) on a fresh partition; got {}",
        p.log_start_offset,
    );
}

#[tokio::test]
async fn log_start_offset_never_exceeds_hwm() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    for _ in 0..3 {
        produce_records(&broker, 0, 2).await;
    }

    let p = fetch_partition(&broker, 0, 0).await;
    if p.log_start_offset >= 0 {
        assert!(
            p.log_start_offset <= p.high_watermark,
            "log_start_offset ({}) must not exceed HWM ({})",
            p.log_start_offset,
            p.high_watermark,
        );
    }
}

// ---------------------------------------------------------------------------
// 6. HWM survives across separate Fetch RPCs (no per-request state)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn hwm_is_consistent_across_consecutive_fetches() {
    // Without any intervening produce, two back-to-back Fetches must
    // report the same HWM. Catches regressions where HWM is computed
    // from per-request state instead of partition state.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    produce_records(&broker, 0, 4).await;

    let a = fetch_partition(&broker, 0, 0).await.high_watermark;
    let b = fetch_partition(&broker, 0, 0).await.high_watermark;
    assert_eq!(a, b, "HWM must be stable when no produce intervenes");
}
