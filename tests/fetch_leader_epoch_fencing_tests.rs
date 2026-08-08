//! Fetch leader-epoch fencing tests (KIP-320), client-epoch edge cases.
//!
//! Background. Fetch v9+ carries a per-partition `current_leader_epoch`:
//! the epoch the client believes is current for that partition. A broker
//! uses it to fence a consumer whose view predates a failover, so the
//! consumer refreshes metadata instead of silently reading from a
//! partition that moved. `src/cluster/handler/fetch.rs` implements that:
//! client older → `FencedLeaderEpoch`, client newer → `UnknownLeaderEpoch`.
//!
//! The subtlety these tests pin is which client values mean "I have no
//! opinion" and must therefore never be fenced:
//!
//! - `-1` is the sentinel the Kafka protocol documents, and what our own
//!   request parser defaults to for versions below v9.
//! - `0` is what librdkafka actually puts on the wire for a partition it
//!   has no epoch for. Verified against librdkafka 2.14 (kcat) on the
//!   wire: its first Fetch for a fresh partition carries a literal
//!   `current_leader_epoch = 0`, even though its own debug log prints
//!   "leader epoch -1".
//!
//! Our leader epochs start at 1 on first acquire, so `0` is never an
//! epoch this broker hands out. Fencing it used to reject *every* fetch
//! from a fresh librdkafka consumer: the client backed off, refreshed
//! metadata, re-fetched, and got fenced again forever — no records, no
//! error surfaced to the application, just a hang. That is what broke
//! the `Consume` / `Keyed consume` / `Verify ordering` steps in
//! `scripts/run-e2e.sh`.
//!
//! What we assert:
//!
//! 1. `current_leader_epoch = 0` returns records (the librdkafka case).
//! 2. `current_leader_epoch = -1` returns records (documented sentinel).
//! 3. Fencing is still wired up: a client epoch newer than the broker's
//!    still gets `UnknownLeaderEpoch`.
//!
//! What we deliberately don't assert:
//!
//! - The "client older than broker" → `FencedLeaderEpoch` direction. A
//!   freshly acquired partition sits at epoch 1, and the only strictly
//!   smaller value is `0`, which is now the "no opinion" case. Exercising
//!   it needs a partition whose epoch has been bumped by a real
//!   ownership change; `tests/offset_for_leader_epoch_tests.rs` covers
//!   the same fencing split on the OffsetForLeaderEpoch handler.

use bytes::Bytes;
use kafkaesque::cluster::ClusterProfile;
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::request::{
    CreateTopicData, CreateTopicsRequestData, FetchPartitionData, FetchRequestData, FetchTopicData,
    ProducePartitionData, ProduceRequestData, ProduceTopicData,
};
use kafkaesque::server::response::FetchPartitionResponse;

mod common;
use common::BrokerHandle;

const TOPIC: &str = "fetch-epoch-fencing-tests";
const PARTITION: i32 = 0;

/// Minimal valid v2 RecordBatch with a correct CRC. Mirrors the helper in
/// `tests/fetch_isolation_level_tests.rs`.
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

async fn create_topic_and_produce(broker: &BrokerHandle) {
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

    // `handle_create_topics` returns once the Raft write lands, which is
    // before the local PartitionManager has finished acquiring
    // leadership — a produce that races it sees
    // `NotLeaderForPartition`. Poll until ownership settles so the
    // assertions below test fencing, not startup timing.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
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
                            partition_index: PARTITION,
                            records: make_batch(3),
                        }],
                    }],
                },
            )
            .await;
        let code = resp.responses[0].partitions[0].error_code;
        if code == KafkaCode::None {
            return;
        }
        // Surface a setup failure directly; otherwise every assertion
        // below fails with a misleading "no records".
        assert!(
            std::time::Instant::now() < deadline,
            "timed out waiting for partition ownership; last produce error: {:?}",
            code
        );
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }
}

fn fetch_request(current_leader_epoch: i32) -> FetchRequestData {
    FetchRequestData {
        replica_id: -1,
        max_wait_ms: 100,
        min_bytes: 1,
        max_bytes: 1024 * 1024,
        isolation_level: 0,
        session_id: 0,
        session_epoch: 0,
        topics: vec![FetchTopicData {
            name: TOPIC.into(),
            partitions: vec![FetchPartitionData {
                partition_index: PARTITION,
                fetch_offset: 0,
                log_start_offset: -1,
                partition_max_bytes: 1024 * 1024,
                current_leader_epoch,
            }],
        }],
        forgotten_topics: vec![],
        rack_id: String::new(),
    }
}

fn first_partition(
    resp: &kafkaesque::server::response::FetchResponseData,
) -> &FetchPartitionResponse {
    &resp.responses[0].partitions[0]
}

/// The librdkafka case: epoch `0` means "I don't know", not "I am stale".
#[tokio::test]
async fn client_epoch_zero_is_not_fenced_and_returns_records() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    create_topic_and_produce(&broker).await;

    let resp = broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(0))
        .await;
    let p = first_partition(&resp);
    assert_eq!(
        p.error_code,
        KafkaCode::None,
        "current_leader_epoch=0 must not fence (librdkafka sends 0 for an \
         unknown epoch); got {:?}",
        p.error_code
    );
    assert!(
        p.records.as_ref().is_some_and(|r| !r.is_empty()),
        "epoch-0 fetch must return records, got {:?}",
        p.records.as_ref().map(|r| r.len())
    );
}

/// The documented sentinel keeps working.
#[tokio::test]
async fn client_epoch_negative_one_is_not_fenced_and_returns_records() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    create_topic_and_produce(&broker).await;

    let resp = broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(-1))
        .await;
    let p = first_partition(&resp);
    assert_eq!(p.error_code, KafkaCode::None);
    assert!(
        p.records.as_ref().is_some_and(|r| !r.is_empty()),
        "epoch--1 fetch must return records"
    );
}

/// Fencing is still active — this is what stops the "0 is no opinion"
/// rule from being read as "epoch validation is off".
#[tokio::test]
async fn client_epoch_newer_than_broker_returns_unknown_leader_epoch() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    create_topic_and_produce(&broker).await;

    let resp = broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(i32::MAX))
        .await;
    let p = first_partition(&resp);
    assert_eq!(
        p.error_code,
        KafkaCode::UnknownLeaderEpoch,
        "a client epoch newer than the broker's must still be rejected; got {:?}",
        p.error_code
    );
    assert!(
        p.records.is_none(),
        "a fenced fetch must not return records"
    );
}
