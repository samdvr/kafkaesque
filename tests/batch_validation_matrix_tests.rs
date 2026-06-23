//! RecordBatch validation matrix at the produce dispatch layer.
//!
//! Background. Apache Kafka's `LogValidatorTest` is a 35-test gatekeeper
//! enumerating every way a malformed batch must be rejected at append:
//! offset assignment, batch-vs-record-count consistency, magic-version
//! gating, timestamp-type policy, sequence-within-batch monotonicity,
//! transactional/control-bit consistency. Kafkaesque has fuzz coverage
//! for parsing (`api_produce`, `record_batch`, `record_batch_header`)
//! and unit tests for the protocol library (`tests/protocol_tests.rs`,
//! `tests/wire_protocol_tests.rs`), but the *append-time* response codes
//! the produce handler emits per failure mode are not pinned anywhere.
//!
//! This file pins the produce handler's response code per failure mode
//! at the public dispatch surface a real Kafka client would hit. When
//! `validate_record_crc` is on (the default), the handler runs:
//!
//! 1. Empty payload — special-cased to success with `base_offset = -1`.
//! 2. `validate_batch_crc_async` on the raw bytes:
//!    - CRC mismatch → `CorruptMessage`
//!    - Frame too small → `CorruptMessage`
//!    - `batch_length` disagrees with payload → `CorruptMessage`
//!    - Non-v2 magic byte → `CorruptMessage`
//!    - Async offload failed → `CorruptMessage`
//! 3. Partition lookup / acquisition (`NotLeaderForPartition`, etc.).
//! 4. `append_batch_durable` (or `append_batch` for acks=0):
//!    - Zero / negative record count → `CorruptMessage` via `CorruptBatch`
//!    - Producer-state violations → `OutOfOrderSequence`,
//!      `DuplicateSequence`, `InvalidProducerEpoch` (covered by
//!      `src/cluster/partition_store_tests.rs`).
//!
//! What we deliberately do NOT assert (and the audit P0.1 spells out):
//!
//! - **Timestamp-type policy** (createTime vs. logAppendTime rewriting):
//!   the broker doesn't enforce a topic-config-driven policy yet.
//! - **Transactional/control-bit consistency**: transactional batches
//!   are accepted (the bit is ignored) but no transactional support
//!   exists. See P4.1.
//! - **Sequence-within-batch monotonicity**: requires decompressing and
//!   walking inner records. See P0.6 inner-decode.
//! - **Magic-version down-conversion**: handled at the connection layer
//!   in Fetch (not Produce). See P1.18.

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

const TOPIC: &str = "validation-tests";

/// Build a well-formed v2 RecordBatch with `record_count` records and a
/// valid CRC. Sets producer_id = -1 so back-to-back identical fixtures
/// don't trip exact-replay deduplication.
fn well_formed_batch(record_count: i32) -> Vec<u8> {
    let mut batch = vec![0u8; 100];
    batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes()); // batch_length
    batch[16] = 2; // magic v2
    batch[23..27].copy_from_slice(&(record_count - 1).to_be_bytes()); // last_offset_delta
    batch[43..51].copy_from_slice(&(-1i64).to_be_bytes()); // producer_id
    batch[51..53].copy_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
    batch[53..57].copy_from_slice(&(-1i32).to_be_bytes()); // base_sequence
    batch[57..61].copy_from_slice(&record_count.to_be_bytes()); // records_count
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    batch
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

/// Send `bytes` as a Produce request to partition 0; return the per-partition
/// error code the handler chose. Includes a brief retry to absorb the
/// post-create write-leadership race that affects every single-broker
/// dispatch test (see notes in `tests/hwm_advancement_tests.rs`).
async fn produce_and_get_error(broker: &BrokerHandle, bytes: Vec<u8>) -> (KafkaCode, i64) {
    let payload = Bytes::from(bytes);
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
        // Stop retrying if we got a *terminal* answer: success, or any
        // validation rejection. The only error worth retrying is
        // `NotLeaderForPartition`, which is the post-create-topic
        // write-leadership race.
        if p.error_code != KafkaCode::NotLeaderForPartition {
            return (p.error_code, p.base_offset);
        }
        if std::time::Instant::now() >= deadline {
            return (p.error_code, p.base_offset);
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

// ---------------------------------------------------------------------------
// 1. Well-formed batch goes through (control test for the matrix below)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn well_formed_batch_succeeds() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    let (code, base) = produce_and_get_error(&broker, well_formed_batch(1)).await;
    assert_eq!(code, KafkaCode::None);
    assert!(
        base >= 0,
        "well-formed batch must yield a real base_offset, got {}",
        base
    );
}

// ---------------------------------------------------------------------------
// 2. CRC mismatch
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flipped_crc_returns_corrupt_message() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let mut batch = well_formed_batch(1);
    batch[17] ^= 0xFF; // flip a CRC byte
    let (code, _) = produce_and_get_error(&broker, batch).await;
    assert_eq!(
        code,
        KafkaCode::CorruptMessage,
        "CRC mismatch must produce CorruptMessage, got {:?}",
        code,
    );
}

#[tokio::test]
async fn payload_byte_flipped_after_valid_crc_returns_corrupt_message() {
    // Sanity: corrupting bytes inside the CRC region (offset 21+) without
    // re-CRCing must also be rejected. This catches a regression where
    // CRC validation only looks at the header.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let mut batch = well_formed_batch(1);
    batch[80] ^= 0x01; // flip a byte well inside the CRC region
    let (code, _) = produce_and_get_error(&broker, batch).await;
    assert_eq!(code, KafkaCode::CorruptMessage);
}

// ---------------------------------------------------------------------------
// 3. Frame mismatch (batch_length lies)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn batch_length_larger_than_payload_returns_corrupt_message() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let mut batch = well_formed_batch(1);
    // Stamp batch_length = 10 KiB even though payload is 100 bytes.
    batch[8..12].copy_from_slice(&10_000i32.to_be_bytes());
    // (We don't recompute CRC — frame check fires before CRC compare,
    // and even if CRC ran, the wrong batch_length means the wrong
    // region is checksummed.)
    let (code, _) = produce_and_get_error(&broker, batch).await;
    assert_eq!(
        code,
        KafkaCode::CorruptMessage,
        "frame mismatch must produce CorruptMessage, got {:?}",
        code,
    );
}

#[tokio::test]
async fn batch_length_zero_returns_corrupt_message() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let mut batch = well_formed_batch(1);
    batch[8..12].copy_from_slice(&0i32.to_be_bytes());
    let (code, _) = produce_and_get_error(&broker, batch).await;
    assert_eq!(code, KafkaCode::CorruptMessage);
}

// ---------------------------------------------------------------------------
// 4. Magic-byte gating (only v2 is supported)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn legacy_magic_v0_returns_corrupt_message() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let mut batch = well_formed_batch(1);
    batch[16] = 0; // v0 MessageSet — different layout, not supported
    // Recompute CRC over the same region to make this a "structurally
    // valid v0" rather than a CRC-corrupt v2. Whichever check fires
    // first, the answer must still be CorruptMessage.
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());

    let (code, _) = produce_and_get_error(&broker, batch).await;
    assert_eq!(code, KafkaCode::CorruptMessage);
}

#[tokio::test]
async fn legacy_magic_v1_returns_corrupt_message() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let mut batch = well_formed_batch(1);
    batch[16] = 1; // v1 MessageSet — also different layout
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());

    let (code, _) = produce_and_get_error(&broker, batch).await;
    assert_eq!(code, KafkaCode::CorruptMessage);
}

// ---------------------------------------------------------------------------
// 5. Too-small payload
// ---------------------------------------------------------------------------

#[tokio::test]
async fn payload_smaller_than_header_returns_corrupt_message_or_empty() {
    // A 30-byte payload can't even contain the v2 header (61 bytes), so
    // either the empty-payload short-circuit or the CRC TooSmall path
    // must fire. The recon report flagged this as a real validation gap
    // worth pinning at the dispatch layer.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let (code, base) = produce_and_get_error(&broker, vec![0u8; 30]).await;
    // The handler may classify this as either:
    // (a) `CorruptMessage` — the CRC validator returned `TooSmall`, OR
    // (b) downstream as a CorruptBatch from `parse_record_count_checked`,
    // which surfaces as CorruptMessage too.
    assert_eq!(
        code,
        KafkaCode::CorruptMessage,
        "30-byte payload must be rejected, got code={:?} base={}",
        code,
        base,
    );
}

// ---------------------------------------------------------------------------
// 6. Empty payload — handler-specific success short-circuit
// ---------------------------------------------------------------------------

#[tokio::test]
async fn empty_payload_returns_success_with_base_offset_minus_one() {
    // The produce handler short-circuits a structurally-empty payload
    // to success with `base_offset = -1` — see the comment block in
    // `src/cluster/handler/produce.rs:510`. Pin it explicitly so a
    // future refactor to "reject empty" is a deliberate contract change,
    // not silent.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let (code, base) = produce_and_get_error(&broker, vec![]).await;
    assert_eq!(code, KafkaCode::None);
    assert_eq!(
        base, -1,
        "empty payload must return base_offset = -1 sentinel, got {}",
        base,
    );
}

// ---------------------------------------------------------------------------
// 7. records_count vs. last_offset_delta consistency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn record_count_zero_returns_corrupt_message() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let mut batch = well_formed_batch(1);
    // Zero record_count with last_offset_delta = -1. The recon noted
    // `partition_store_tests.rs:736-777` rejects this as `CorruptBatch`,
    // which surfaces at the handler as CorruptMessage.
    batch[23..27].copy_from_slice(&(-1i32).to_be_bytes()); // last_offset_delta
    batch[57..61].copy_from_slice(&0i32.to_be_bytes()); // records_count
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());

    let (code, _) = produce_and_get_error(&broker, batch).await;
    assert_eq!(code, KafkaCode::CorruptMessage);
}

#[tokio::test]
async fn record_count_disagrees_with_last_offset_delta_returns_corrupt_message() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let mut batch = well_formed_batch(5);
    // Header now says: last_offset_delta = 4, records_count = 5.
    // Tamper the records_count to 100 so they disagree.
    batch[57..61].copy_from_slice(&100i32.to_be_bytes());
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());

    let (code, _) = produce_and_get_error(&broker, batch).await;
    assert_eq!(
        code,
        KafkaCode::CorruptMessage,
        "records_count != last_offset_delta + 1 must be rejected",
    );
}

// ---------------------------------------------------------------------------
// 8. TODOs — pinned with explanatory negative tests for the audit gaps
// ---------------------------------------------------------------------------

#[tokio::test]
async fn transactional_bit_in_attributes_is_currently_accepted() {
    // The transactional bit (attributes bit 4) marks a batch as part of
    // a transaction. The broker rejects transactions at the produce
    // path semantically (no coordinator), but currently DOES accept the
    // bit set without rejecting the batch — the audit's P4.1 / P0.1
    // gap. Pin today's pass-through so a future strict check is a
    // visible flip.
    //
    // TODO(transactions): when transactions land, decide whether
    // to reject transactional bits without a transactional_id, or fence
    // the batch as `InvalidProducerEpoch` / `OperationNotAttempted`.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let mut batch = well_formed_batch(1);
    // Attributes bit 4 = transactional. Bytes 21-22 are attributes.
    let attrs = u16::from_be_bytes([batch[21], batch[22]]) | (1u16 << 4);
    batch[21..23].copy_from_slice(&attrs.to_be_bytes());
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());

    let (code, _) = produce_and_get_error(&broker, batch).await;
    assert_eq!(
        code,
        KafkaCode::None,
        "today's contract: transactional bit accepted without coordinator (TODO P4.1)",
    );
}

#[tokio::test]
async fn control_batch_bit_is_currently_accepted() {
    // Control records (attributes bit 5) carry transaction abort/commit
    // markers. Producers should never emit them — only the broker (or
    // the txn coordinator) does. Today the broker accepts them. Pin
    // that contract.
    //
    // TODO(transactions): reject control batches from clients at
    // the produce path; they must only be emitted internally by the
    // transaction coordinator.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let mut batch = well_formed_batch(1);
    let attrs = u16::from_be_bytes([batch[21], batch[22]]) | (1u16 << 5);
    batch[21..23].copy_from_slice(&attrs.to_be_bytes());
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());

    let (code, _) = produce_and_get_error(&broker, batch).await;
    assert_eq!(
        code,
        KafkaCode::None,
        "today's contract: control bit accepted from clients (TODO P4.1)",
    );
}
