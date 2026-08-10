//! Incremental fetch session (KIP-227) contract.
//!
//! Fetch v7 added incremental fetch sessions: a client asks the broker to
//! remember its partition set, then sends *partial* requests that only carry
//! partitions whose state changed, plus a `forgotten_topics` list to drop
//! partitions from the session.
//!
//! Kafkaesque does not implement sessions. Fetch v7–v11 is advertised for its
//! other additions (`log_start_offset` in v5, incremental-session *framing* in
//! v7, `current_leader_epoch` fencing in v9, rack-aware replica selection in
//! v11), and every response carries `session_id = 0` — `INVALID_SESSION_ID`,
//! the spec's signal for "no session established, keep sending full fetches".
//! Clients handle that by falling back to full fetches, so it is a legitimate
//! degradation rather than a break.
//!
//! The gap this file closes: the broker used to *ignore* `session_id` and
//! `session_epoch` entirely. A client presenting a session id believes the
//! broker is holding incremental state for it, and is therefore entitled to
//! send a partial partition set and to trust `forgotten_topics` to remove
//! partitions. Serving such a request as if it were a full fetch silently
//! answers for the wrong partition set. Kafka's answer to an unknown session
//! is `FETCH_SESSION_ID_NOT_FOUND`, which makes the client discard its
//! session and reissue a full fetch; we now return the same, so the fallback
//! is driven by the protocol rather than by luck.
//!
//! What we assert:
//!
//! 1. A sessionless fetch (`session_id = 0`) works and returns records — the
//!    normal path, which must not regress.
//! 2. Responses always carry `session_id = 0`, i.e. the broker never claims
//!    to have created a session.
//! 3. A fetch presenting a nonzero `session_id` is rejected with
//!    `FETCH_SESSION_ID_NOT_FOUND` regardless of `session_epoch`.
//! 4. The rejection is a well-formed response (empty topics), not a hang or a
//!    partial answer.
//!
//! When sessions are implemented, replace this file with tests that create a
//! session, send an incremental follow-up, and assert `forgotten_topics`
//! actually removes a partition.

use bytes::Bytes;
use kafkaesque::cluster::ClusterProfile;
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::request::{
    CreateTopicData, CreateTopicsRequestData, FetchPartitionData, FetchRequestData, FetchTopicData,
    ProducePartitionData, ProduceRequestData, ProduceTopicData,
};
use kafkaesque::server::response::FetchResponseData;

mod common;
use common::BrokerHandle;

const TOPIC: &str = "fetch-session-contract";

/// Minimal valid v2 record batch carrying one record, matching the helper
/// the other fetch/produce tests use (the CRC must be real — the broker
/// validates it).
fn make_batch() -> Bytes {
    let mut batch = vec![0u8; 100];
    batch[8..12].copy_from_slice(&(100i32 - 12).to_be_bytes()); // batch_length
    batch[16] = 2; // magic v2
    batch[23..27].copy_from_slice(&0i32.to_be_bytes()); // last_offset_delta
    batch[43..51].copy_from_slice(&(-1i64).to_be_bytes()); // producer_id
    batch[51..53].copy_from_slice(&(-1i16).to_be_bytes()); // producer_epoch
    batch[53..57].copy_from_slice(&(-1i32).to_be_bytes()); // base_sequence
    batch[57..61].copy_from_slice(&1i32.to_be_bytes()); // records_count
    let crc = kafkaesque::protocol::crc32c(&batch[21..]);
    batch[17..21].copy_from_slice(&crc.to_be_bytes());
    Bytes::from(batch)
}

async fn setup() -> BrokerHandle {
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
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

    // Produce one batch so a successful fetch has something to return; a
    // rejection must be distinguishable from "empty log". Fresh partitions are
    // acquired lazily by the ownership loop, so retry until it lands.
    let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
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
                            records: make_batch(),
                        }],
                    }],
                },
            )
            .await;
        let last = resp.responses[0].partitions[0].error_code;
        if last == KafkaCode::None {
            return broker;
        }
        assert!(
            std::time::Instant::now() < deadline,
            "produce never succeeded while setting up the fetch-session test; \
             last error: {last:?}"
        );
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
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

async fn fetch(broker: &BrokerHandle, session_id: i32, session_epoch: i32) -> FetchResponseData {
    broker
        .handler
        .handle_fetch(&broker.ctx(), fetch_request(session_id, session_epoch))
        .await
}

#[tokio::test]
async fn sessionless_fetch_succeeds_and_never_claims_a_session() {
    let broker = setup().await;
    let resp = fetch(&broker, 0, 0).await;

    assert_eq!(
        resp.error_code,
        KafkaCode::None,
        "the sessionless path is the normal path and must work"
    );
    assert_eq!(
        resp.session_id, 0,
        "the broker must not claim to have created a session it can't honor"
    );
    let partition = &resp.responses[0].partitions[0];
    assert_eq!(partition.error_code, KafkaCode::None);
    assert!(
        partition.records.as_ref().is_some_and(|r| !r.is_empty()),
        "sessionless fetch must return the produced records"
    );
}

/// The core fix: a session id the broker never issued must be refused, not
/// served as though it were a full fetch.
#[tokio::test]
async fn unknown_session_id_is_rejected_with_session_id_not_found() {
    let broker = setup().await;

    for (session_id, session_epoch) in [(1, 0), (42, 7), (i32::MAX, 1), (-1, 0)] {
        let resp = fetch(&broker, session_id, session_epoch).await;
        assert_eq!(
            resp.error_code,
            KafkaCode::FetchSessionIdNotFound,
            "session_id={session_id} epoch={session_epoch} was never issued by \
             this broker and must be refused so the client falls back to a \
             full fetch"
        );
        assert_eq!(
            resp.session_id, 0,
            "the rejection must not hand back a session id either"
        );
        assert!(
            resp.responses.is_empty(),
            "a rejected fetch must not also return partition data"
        );
    }
}

/// A client that keeps incrementing its epoch against a session we don't have
/// must keep getting the same answer rather than eventually being served.
#[tokio::test]
async fn session_epoch_does_not_make_an_unknown_session_valid() {
    let broker = setup().await;
    for epoch in [-1, 0, 1, 2, 100] {
        let resp = fetch(&broker, 77, epoch).await;
        assert_eq!(
            resp.error_code,
            KafkaCode::FetchSessionIdNotFound,
            "epoch {epoch} must not resurrect an unknown session"
        );
    }
}
