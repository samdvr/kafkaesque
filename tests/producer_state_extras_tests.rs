//! Producer-state error-code mapping at the produce dispatch layer.
//!
//! Background. `src/cluster/partition_store_tests.rs` covers the
//! producer-state machine at the storage layer — sequence increments,
//! exact-replay re-ack, duplicate / out-of-order / fenced-epoch
//! detection. The audit's P0.2 flagged that the *Kafka error code*
//! returned to the client per failure mode is not asserted at the
//! handler boundary.
//!
//! The mapping (per `src/cluster/error.rs::to_kafka_code`):
//!
//! | Storage error                  | Kafka response code              |
//! |--------------------------------|----------------------------------|
//! | `DuplicateSequence`            | `DuplicateSequenceNumber`        |
//! | `OutOfOrderSequence`           | `OutOfOrderSequenceNumber`       |
//! | `FencedProducer` (stale epoch) | `InvalidProducerEpoch`           |
//!
//! Real Kafka clients special-case these: `DuplicateSequenceNumber`
//! is treated as success (the previous attempt succeeded);
//! `OutOfOrderSequenceNumber` triggers an exception that aborts the
//! producer; `InvalidProducerEpoch` likewise. Returning the wrong code
//! causes silent data loss (treating a real failure as a duplicate)
//! or infinite retry loops (treating a duplicate as an out-of-order).
//!
//! What we assert:
//!
//! 1. Exact-replay of the *most recent* idempotent batch returns
//!    success with the ORIGINAL `base_offset` (Kafka idempotent contract:
//!    a network-retry of the most recent batch is re-acked, not errored).
//! 2. Replay of an OLDER (non-most-recent) idempotent batch returns
//!    `DuplicateSequenceNumber`.
//! 3. A sequence gap (e.g. broker has seen 0..4, client sends 10..)
//!    returns `OutOfOrderSequenceNumber`.
//! 4. A batch from a stale producer epoch returns
//!    `InvalidProducerEpoch`.
//! 5. Non-idempotent batches (`producer_id = -1`) bypass all of the
//!    above — back-to-back identical bytes both succeed.
//! 6. Multiple producer ids on the same partition are isolated: A's
//!    OOO error doesn't affect B.

use bytes::Bytes;
use kafkaesque::cluster::ClusterProfile;
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::request::{
    CreateTopicData, CreateTopicsRequestData, ProducePartitionData, ProduceRequestData,
    ProduceTopicData,
};

mod common;
use common::BrokerHandle;

const TOPIC: &str = "producer-state-extras";

fn build_batch(producer_id: i64, producer_epoch: i16, base_sequence: i32, record_count: i32) -> Bytes {
    let mut batch = vec![0u8; 100];
    batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes());
    batch[16] = 2;
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes());
    batch[27..35].copy_from_slice(&1_000i64.to_be_bytes());
    batch[35..43].copy_from_slice(&1_000i64.to_be_bytes());
    batch[43..51].copy_from_slice(&producer_id.to_be_bytes());
    batch[51..53].copy_from_slice(&producer_epoch.to_be_bytes());
    batch[53..57].copy_from_slice(&base_sequence.to_be_bytes());
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

/// Send a Produce. Returns (error_code, base_offset). Includes the same
/// post-create write-leadership retry the other dispatch tests use:
/// the only retryable error is `NotLeaderForPartition`. Producer-state
/// errors are terminal.
async fn produce(broker: &BrokerHandle, batch: Bytes) -> (KafkaCode, i64) {
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
                            records: batch.clone(),
                        }],
                    }],
                },
            )
            .await;
        let p = &resp.responses[0].partitions[0];
        if p.error_code != KafkaCode::NotLeaderForPartition
            || std::time::Instant::now() >= deadline
        {
            return (p.error_code, p.base_offset);
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

// ---------------------------------------------------------------------------
// 1. Exact-replay of the most recent batch is re-acked
// ---------------------------------------------------------------------------

#[tokio::test]
async fn exact_replay_of_most_recent_idempotent_batch_returns_original_offset() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;

    let pid: i64 = 5_555;
    let (code, original) = produce(&broker, build_batch(pid, 0, 0, 3)).await;
    assert_eq!(code, KafkaCode::None);
    assert!(original >= 0);

    // Same producer_id, epoch, sequence, count → the broker treats this
    // as a network retry and re-acks at the original offset.
    let (replay_code, replay_offset) = produce(&broker, build_batch(pid, 0, 0, 3)).await;
    assert_eq!(
        replay_code,
        KafkaCode::None,
        "exact-replay must be re-acked, not error",
    );
    assert_eq!(
        replay_offset, original,
        "replay must return the ORIGINAL base_offset, not a fresh one",
    );
}

// ---------------------------------------------------------------------------
// 2. Replay of an older (non-most-recent) batch is DuplicateSequenceNumber
// ---------------------------------------------------------------------------

#[tokio::test]
async fn replay_of_older_idempotent_batch_returns_duplicate_sequence() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;

    let pid: i64 = 6_000;
    // Three batches: seq 0..2, 3..5, 6..8.
    let (c1, _) = produce(&broker, build_batch(pid, 0, 0, 3)).await;
    let (c2, _) = produce(&broker, build_batch(pid, 0, 3, 3)).await;
    let (c3, _) = produce(&broker, build_batch(pid, 0, 6, 3)).await;
    assert_eq!(c1, KafkaCode::None);
    assert_eq!(c2, KafkaCode::None);
    assert_eq!(c3, KafkaCode::None);

    // Replay of the very first batch (seq 0..2) is now well below the
    // most recent — it must NOT be re-acked.
    let (replay_code, _) = produce(&broker, build_batch(pid, 0, 0, 3)).await;
    assert_eq!(
        replay_code,
        KafkaCode::DuplicateSequenceNumber,
        "older-than-most-recent replay must be DuplicateSequenceNumber, got {:?}",
        replay_code,
    );
}

// ---------------------------------------------------------------------------
// 3. Sequence gap → OutOfOrderSequenceNumber
// ---------------------------------------------------------------------------

#[tokio::test]
async fn sequence_gap_returns_out_of_order_sequence() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;

    let pid: i64 = 7_000;
    let (ok, _) = produce(&broker, build_batch(pid, 0, 0, 5)).await;
    assert_eq!(ok, KafkaCode::None);

    // Broker now expects sequence 5 next. Send 10 instead — gap.
    let (gap_code, _) = produce(&broker, build_batch(pid, 0, 10, 5)).await;
    assert_eq!(
        gap_code,
        KafkaCode::OutOfOrderSequenceNumber,
        "sequence gap must be OutOfOrderSequenceNumber, got {:?}",
        gap_code,
    );
}

// ---------------------------------------------------------------------------
// 4. Stale producer epoch → InvalidProducerEpoch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stale_producer_epoch_returns_invalid_producer_epoch() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;

    let pid: i64 = 8_000;
    // First write at epoch=5 (a real producer might bump epoch via
    // InitProducerId; we simulate by writing at a higher epoch first).
    let (ok, _) = produce(&broker, build_batch(pid, 5, 0, 2)).await;
    assert_eq!(ok, KafkaCode::None);

    // A "zombie" producer at epoch=4 returns from the dead and tries
    // to write — must be fenced as InvalidProducerEpoch.
    let (fenced_code, _) = produce(&broker, build_batch(pid, 4, 2, 2)).await;
    assert_eq!(
        fenced_code,
        KafkaCode::InvalidProducerEpoch,
        "stale-epoch producer must be fenced with InvalidProducerEpoch, got {:?}",
        fenced_code,
    );
}

// ---------------------------------------------------------------------------
// 5. Non-idempotent batches: identical fixtures both succeed
// ---------------------------------------------------------------------------

#[tokio::test]
async fn non_idempotent_back_to_back_identical_batches_both_succeed() {
    // producer_id = -1 disables idempotent-producer dedup. Two
    // byte-identical batches must both be appended at distinct offsets.
    // Pinning this catches a regression where the exact-replay
    // detection accidentally extends to non-idempotent producers.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;

    let (c1, off1) = produce(&broker, build_batch(-1, -1, -1, 3)).await;
    let (c2, off2) = produce(&broker, build_batch(-1, -1, -1, 3)).await;
    assert_eq!(c1, KafkaCode::None);
    assert_eq!(c2, KafkaCode::None);
    assert_ne!(
        off1, off2,
        "non-idempotent identical batches must land at distinct offsets",
    );
    assert!(off2 > off1);
}

// ---------------------------------------------------------------------------
// 6. Producer-id isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn out_of_order_in_producer_a_does_not_affect_producer_b() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker).await;

    // Producer A sets up a state.
    let (ok, _) = produce(&broker, build_batch(/*A=*/ 1_000, 0, 0, 5)).await;
    assert_eq!(ok, KafkaCode::None);

    // Producer A immediately sends an OOO batch.
    let (a_oo, _) = produce(&broker, build_batch(1_000, 0, 50, 5)).await;
    assert_eq!(a_oo, KafkaCode::OutOfOrderSequenceNumber);

    // Producer B (different id) writes for the first time. Must be
    // accepted even though A is broken.
    let (b_ok, b_off) = produce(&broker, build_batch(/*B=*/ 2_000, 0, 0, 4)).await;
    assert_eq!(
        b_ok,
        KafkaCode::None,
        "B's first batch must be accepted regardless of A's OOO state",
    );
    assert!(b_off > 0);
}
