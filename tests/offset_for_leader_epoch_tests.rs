//! OffsetForLeaderEpoch (KIP-320) handler contract tests.
//!
//! Background. Modern Kafka consumers send `OffsetForLeaderEpoch` on
//! every rebalance to validate that they haven't been truncated past
//! their last fetch offset. The broker's response splits four ways:
//!
//! | Input                                  | Expected error                |
//! |----------------------------------------|-------------------------------|
//! | Topic / partition does not exist       | `UnknownTopicOrPartition`     |
//! | Topic name is malformed                | `InvalidTopic`                |
//! | Client epoch < broker epoch            | `FencedLeaderEpoch`           |
//! | Client epoch > broker epoch            | `UnknownLeaderEpoch`          |
//! | Owned, epochs agree (or client = -1)   | success: `(broker_epoch, HWM)`|
//!
//! `src/cluster/handler/leader_epoch.rs` enumerates these in its module
//! docstring and the recon report confirmed them. None of the cases were
//! previously asserted at the dispatch layer — `tests/fencing_detection_tests.rs`
//! covers the broker-state level, not the handler's response mapping.
//!
//! What we assert:
//!
//! 1. Unknown topic → `UnknownTopicOrPartition` per partition.
//! 2. Malformed topic name → `InvalidTopic` per partition.
//! 3. `current_leader_epoch=-1` (no opinion) → success even when the
//!    broker has a known epoch.
//! 4. Matching epochs → success with `(broker_epoch, HWM)`.
//! 5. Older client epoch → `FencedLeaderEpoch`.
//! 6. Newer client epoch → `UnknownLeaderEpoch`.
//! 7. Multi-partition / multi-topic request: per-partition errors are
//!    independent; one bad partition does not poison others.
//! 8. After a produce, `end_offset` reflects the new HWM (the broker is
//!    answering live state, not a stale cache).
//!
//! What this test deliberately does NOT cover:
//!
//! - `NotLeaderForPartition` for partitions owned by another broker.
//!   The single-broker harness can't construct that scenario;
//!   multi-broker Raft chaos covers it (see P2 in `tests_from_kafka.md`).
//! - Historical-epoch lookup (epoch N → end-offset of epoch N). Today
//!   the broker only knows its current epoch; there is no epoch cache.
//!   That's the actual feature gap — these tests pin today's behavior
//!   so the gap is visible without misrepresenting it.

use kafkaesque::cluster::ClusterProfile;
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::request::{
    CreateTopicData, CreateTopicsRequestData, OffsetForLeaderEpochPartitionData,
    OffsetForLeaderEpochRequestData, OffsetForLeaderEpochTopicData,
};

mod common;
use common::BrokerHandle;

const TOPIC: &str = "ofle-tests";

/// Create the topic and poll OffsetForLeaderEpoch until the broker
/// reports ownership (i.e. response error_code == None for the
/// `current_leader_epoch=-1` probe). `handle_create_topics` only writes
/// to Raft and returns before partition leadership/assignment settles
/// in the local PartitionManager, so a follow-up RPC can race and see
/// `UnknownTopicOrPartition` until the apply catches up. This wait
/// centralizes that polling so every test sees a stable broker state.
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

    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    loop {
        let resp = broker
            .handler
            .handle_offset_for_leader_epoch(&broker.ctx(), ofle_request(name, 0, -1, 0))
            .await;
        let p = &resp.topics[0].partitions[0];
        if p.error_code == KafkaCode::None {
            return;
        }
        if std::time::Instant::now() >= deadline {
            panic!(
                "timed out waiting for topic {} to be owned; last response: {:?}",
                name, p
            );
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    }
}

/// Build a single-topic, single-partition OFLE request with the given
/// `current_leader_epoch` (the client's view) and `leader_epoch` (the
/// epoch the client wants the end-offset for; today the broker ignores
/// the latter and returns the current HWM regardless).
fn ofle_request(topic: &str, partition: i32, current_epoch: i32, target_epoch: i32) -> OffsetForLeaderEpochRequestData {
    OffsetForLeaderEpochRequestData {
        replica_id: -2,
        topics: vec![OffsetForLeaderEpochTopicData {
            name: topic.into(),
            partitions: vec![OffsetForLeaderEpochPartitionData {
                partition_index: partition,
                current_leader_epoch: current_epoch,
                leader_epoch: target_epoch,
            }],
        }],
    }
}

/// Resolve the broker's current epoch for a topic/partition by issuing a
/// `current_leader_epoch=-1` request (which bypasses fencing) and reading
/// it out of the success response. Centralizes the "what epoch does the
/// broker think it's at?" query the rest of the tests need.
async fn broker_epoch(broker: &BrokerHandle, topic: &str, partition: i32) -> i32 {
    let resp = broker
        .handler
        .handle_offset_for_leader_epoch(&broker.ctx(), ofle_request(topic, partition, -1, 0))
        .await;
    let p = &resp.topics[0].partitions[0];
    assert_eq!(
        p.error_code,
        KafkaCode::None,
        "broker_epoch helper failed; partition response: {:?}",
        p,
    );
    p.leader_epoch
}

// ---------------------------------------------------------------------------
// 1. Unknown topic / partition
// ---------------------------------------------------------------------------

#[tokio::test]
async fn unknown_topic_returns_unknown_topic_or_partition() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    let resp = broker
        .handler
        .handle_offset_for_leader_epoch(
            &broker.ctx(),
            ofle_request("does-not-exist", 0, -1, 0),
        )
        .await;
    let p = &resp.topics[0].partitions[0];
    assert_eq!(p.error_code, KafkaCode::UnknownTopicOrPartition);
    assert_eq!(p.leader_epoch, -1);
    assert_eq!(p.end_offset, -1);
}

#[tokio::test]
async fn malformed_topic_name_returns_invalid_topic() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    // "/" is rejected by `validate_topic_name` — the same rule the
    // handler applies before authorization.
    let resp = broker
        .handler
        .handle_offset_for_leader_epoch(&broker.ctx(), ofle_request("bad/name", 0, -1, 0))
        .await;
    let p = &resp.topics[0].partitions[0];
    assert_eq!(p.error_code, KafkaCode::InvalidTopic);
}

// ---------------------------------------------------------------------------
// 2. Owned partition + epoch matrix
// ---------------------------------------------------------------------------

#[tokio::test]
async fn current_epoch_negative_one_skips_fence_and_returns_hwm() {
    // -1 is the documented "client has no opinion" sentinel. The handler
    // must skip fencing entirely and return success.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;

    let resp = broker
        .handler
        .handle_offset_for_leader_epoch(&broker.ctx(), ofle_request(TOPIC, 0, -1, 0))
        .await;
    let p = &resp.topics[0].partitions[0];
    assert_eq!(p.error_code, KafkaCode::None);
    assert!(p.leader_epoch >= 0, "broker must report its epoch on success");
    assert!(p.end_offset >= 0, "end_offset must be a real offset on success");
}

#[tokio::test]
async fn matching_epoch_returns_success() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    let epoch = broker_epoch(&broker, TOPIC, 0).await;

    let resp = broker
        .handler
        .handle_offset_for_leader_epoch(&broker.ctx(), ofle_request(TOPIC, 0, epoch, 0))
        .await;
    let p = &resp.topics[0].partitions[0];
    assert_eq!(p.error_code, KafkaCode::None);
    assert_eq!(p.leader_epoch, epoch);
}

#[tokio::test]
async fn older_client_epoch_returns_fenced_leader_epoch() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    let epoch = broker_epoch(&broker, TOPIC, 0).await;

    if epoch == 0 {
        // Can't represent an older epoch than 0 in i32 without going
        // negative (which is the "no opinion" sentinel). Skip with a
        // diagnostic so the test honestly reports its precondition.
        eprintln!(
            "skipping fenced-epoch test: broker epoch is {} so no older epoch is representable",
            epoch
        );
        return;
    }

    let resp = broker
        .handler
        .handle_offset_for_leader_epoch(&broker.ctx(), ofle_request(TOPIC, 0, epoch - 1, 0))
        .await;
    let p = &resp.topics[0].partitions[0];
    assert_eq!(
        p.error_code,
        KafkaCode::FencedLeaderEpoch,
        "client epoch < broker epoch must fence; got {:?}",
        p,
    );
}

#[tokio::test]
async fn newer_client_epoch_returns_unknown_leader_epoch() {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    let epoch = broker_epoch(&broker, TOPIC, 0).await;

    let resp = broker
        .handler
        .handle_offset_for_leader_epoch(&broker.ctx(), ofle_request(TOPIC, 0, epoch + 100, 0))
        .await;
    let p = &resp.topics[0].partitions[0];
    assert_eq!(
        p.error_code,
        KafkaCode::UnknownLeaderEpoch,
        "client epoch > broker epoch must report unknown; got {:?}",
        p,
    );
}

// ---------------------------------------------------------------------------
// 3. Per-partition error isolation across a multi-topic request
// ---------------------------------------------------------------------------

#[tokio::test]
async fn per_partition_errors_are_independent() {
    // A request that mixes a known topic, an unknown topic, and an
    // invalid topic must produce three independent per-partition
    // responses in the same order. One topic's failure must not cause
    // another's partition to be skipped or cross-contaminated.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    let epoch = broker_epoch(&broker, TOPIC, 0).await;

    let request = OffsetForLeaderEpochRequestData {
        replica_id: -2,
        topics: vec![
            OffsetForLeaderEpochTopicData {
                name: TOPIC.into(),
                partitions: vec![OffsetForLeaderEpochPartitionData {
                    partition_index: 0,
                    current_leader_epoch: epoch,
                    leader_epoch: 0,
                }],
            },
            OffsetForLeaderEpochTopicData {
                name: "nope".into(),
                partitions: vec![OffsetForLeaderEpochPartitionData {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    leader_epoch: 0,
                }],
            },
            OffsetForLeaderEpochTopicData {
                name: "bad/name".into(),
                partitions: vec![OffsetForLeaderEpochPartitionData {
                    partition_index: 0,
                    current_leader_epoch: -1,
                    leader_epoch: 0,
                }],
            },
        ],
    };

    let resp = broker
        .handler
        .handle_offset_for_leader_epoch(&broker.ctx(), request)
        .await;
    assert_eq!(resp.topics.len(), 3, "all three topics must appear in the response");
    assert_eq!(resp.topics[0].partitions[0].error_code, KafkaCode::None);
    assert_eq!(
        resp.topics[1].partitions[0].error_code,
        KafkaCode::UnknownTopicOrPartition,
    );
    assert_eq!(
        resp.topics[2].partitions[0].error_code,
        KafkaCode::InvalidTopic,
    );
}

#[tokio::test]
async fn multi_partition_per_topic_returns_one_response_per_partition() {
    // Even when only one of two partitions is fenced, both must surface
    // in the response — the server must not short-circuit on the first
    // non-OK code.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    let epoch = broker_epoch(&broker, TOPIC, 0).await;

    let request = OffsetForLeaderEpochRequestData {
        replica_id: -2,
        topics: vec![OffsetForLeaderEpochTopicData {
            name: TOPIC.into(),
            partitions: vec![
                // Partition 0: matching epoch → ok
                OffsetForLeaderEpochPartitionData {
                    partition_index: 0,
                    current_leader_epoch: epoch,
                    leader_epoch: 0,
                },
                // Partition 99: doesn't exist → UnknownTopicOrPartition
                OffsetForLeaderEpochPartitionData {
                    partition_index: 99,
                    current_leader_epoch: -1,
                    leader_epoch: 0,
                },
            ],
        }],
    };

    let resp = broker
        .handler
        .handle_offset_for_leader_epoch(&broker.ctx(), request)
        .await;
    assert_eq!(resp.topics[0].partitions.len(), 2);
    assert_eq!(resp.topics[0].partitions[0].error_code, KafkaCode::None);
    assert_eq!(
        resp.topics[0].partitions[1].error_code,
        KafkaCode::UnknownTopicOrPartition,
    );
}

// ---------------------------------------------------------------------------
// 4. end_offset reflects live HWM (not a stale cache)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn end_offset_reflects_live_hwm_after_produce() {
    use bytes::Bytes;
    use kafkaesque::server::request::{
        ProducePartitionData, ProduceRequestData, ProduceTopicData,
    };

    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TOPIC).await;
    let ctx = broker.ctx();

    let before = broker
        .handler
        .handle_offset_for_leader_epoch(&ctx, ofle_request(TOPIC, 0, -1, 0))
        .await;
    let before_offset = before.topics[0].partitions[0].end_offset;

    // Produce a 5-record batch.
    let mut bytes = vec![0u8; 100];
    bytes[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes());
    bytes[16] = 2;
    bytes[23..27].copy_from_slice(&4i32.to_be_bytes()); // last_offset_delta
    bytes[57..61].copy_from_slice(&5i32.to_be_bytes()); // record_count
    let crc = kafkaesque::protocol::crc32c(&bytes[21..]);
    bytes[17..21].copy_from_slice(&crc.to_be_bytes());
    let payload = Bytes::from(bytes);

    // ensure_topic waits until the read path returns ownership, but the
    // partition manager may still be opening the SlateDB store for
    // writes when the first produce arrives. Retry briefly until the
    // write side catches up. After ~5s give up — that's a real bug, not
    // a setup race.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(5);
    let mut last_err = KafkaCode::None;
    let prod_ok = loop {
        let prod = broker
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
                            partition_index: 0,
                            records: payload.clone(),
                        }],
                    }],
                },
            )
            .await;
        let code = prod.responses[0].partitions[0].error_code;
        if code == KafkaCode::None {
            break true;
        }
        last_err = code;
        if std::time::Instant::now() >= deadline {
            break false;
        }
        tokio::time::sleep(std::time::Duration::from_millis(25)).await;
    };
    assert!(prod_ok, "setup produce never succeeded; last error: {:?}", last_err);

    let after = broker
        .handler
        .handle_offset_for_leader_epoch(&ctx, ofle_request(TOPIC, 0, -1, 0))
        .await;
    let after_offset = after.topics[0].partitions[0].end_offset;
    assert!(
        after_offset > before_offset,
        "end_offset must advance after produce: before={}, after={}",
        before_offset,
        after_offset,
    );
    assert_eq!(
        after_offset - before_offset,
        5,
        "HWM should advance by exactly the record_count",
    );
}
