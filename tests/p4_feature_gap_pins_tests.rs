//! Feature-gap contract pins for the P4 band.
//!
//! Background. P4 in `tests_from_kafka.md` covers features the README
//! explicitly lists as "Not yet supported": transactions / EOS, log
//! compaction, delegation tokens, ACL administration via Kafka RPC, and
//! the KIP-848 next-gen consumer group protocol. The audit's framing is
//! that "tests follow the feature" — but until the features land we still
//! benefit from pinning the *current observable contract*. That way:
//!
//! - A future implementation has a single failing test to flip per
//!   capability instead of having to rediscover the gap from the audit.
//! - A regression that silently *partially* enables one of these features
//!   (e.g. accepting a transactional produce that quietly downgrades to
//!   idempotent semantics) breaks a test instead of breaking a real
//!   client weeks later in production.
//!
//! ## P4.1 — Transactions / EOS
//! No transaction coordinator exists. The two entry points that touch
//! transactional state today are:
//!   - `InitProducerId` with a `transactional_id` is rejected with
//!     `TransactionalIdAuthorizationFailed` (53). Clients special-case
//!     this code: the producer aborts permanently rather than retry-storming.
//!   - `Produce` with a `transactional_id` is rejected with `InvalidRequest`
//!     on every partition. This is honest: silently accepting it would
//!     downgrade EOS to idempotence-only.
//!
//! The transaction-coordinator API keys (`AddPartitionsToTxn` 24,
//! `AddOffsetsToTxn` 25, `EndTxn` 26, `TxnOffsetCommit` 28) are not in
//! the broker's `ApiKey` enum at all.
//!
//! ## P4.2 — Log compaction
//! There is no cleaner: no compaction loop, no `LogCleaner`, no tombstone
//! collapsing. `cleanup.policy=compact` used to parse, validate and persist
//! anyway, so a keyed topic looked compacted and grew forever. The policy is
//! now **refused** at the write gate
//! (`TopicCompactionConfig::validate_raw`, `src/cluster/topic_config_view.rs`)
//! with `INVALID_CONFIG`, so the broker no longer advertises a guarantee it
//! doesn't keep. The pins below assert the refusal, and that a
//! `delete`-policy topic keeps every batch (nothing collapses records behind
//! the operator's back).
//!
//! ## P4.3 — Delegation tokens
//! Delegation-token RPCs (`CreateDelegationToken` 38, `RenewDelegationToken`
//! 39, `ExpireDelegationToken` 40, `DescribeDelegationToken` 41) are not
//! in the `ApiKey` enum. A client sending one falls into `ApiKey::Unknown(_)`
//! and the dispatcher rejects the connection at version negotiation.
//!
//! ## P4.4 — ACL administration via Kafka RPC
//! ACL state is managed through the Raft control plane
//! (`RaftCoordinator::{create,delete,describe}_acls`). The Kafka wire
//! surface (`DescribeAcls` 29, `CreateAcls` 30, `DeleteAcls` 31) is now
//! wired at v0..=v1 — see `src/cluster/handler/acls.rs`. The pin below
//! asserts those keys stay advertised.
//!
//! ## P4.5 — KIP-848 next-gen group protocol
//! The classic group protocol (`JoinGroup` 11, `SyncGroup` 14,
//! `Heartbeat` 12) is implemented. The next-gen protocol's API keys
//! (`ConsumerGroupHeartbeat` 68, `ConsumerGroupDescribe` 69) are absent.
//! A 2025-vintage Java consumer that prefers the new protocol will fall
//! back to the classic one based on the `ApiVersions` response — this
//! pins both halves of that contract.

use bytes::Bytes;
use kafkaesque::cluster::ClusterProfile;
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::request::{
    ApiKey, CreateTopicData, CreateTopicsRequestData, FetchPartitionData, FetchRequestData,
    FetchTopicData, InitProducerIdRequestData, ProducePartitionData, ProduceRequestData,
    ProduceTopicData,
};
use kafkaesque::server::versions::SUPPORTED_VERSIONS;

mod common;
use common::BrokerHandle;

const TXN_TOPIC: &str = "p4-txn-pins";
const COMPACT_TOPIC: &str = "p4-compaction-pins";

/// Build a minimal valid v2 RecordBatch. We never construct a transactional
/// or control batch here — the txn pins exercise the request-level
/// transactional_id field, not record-level attribute bits.
fn make_batch(record_count: i32) -> Bytes {
    Bytes::from(kafkaesque::batch::build_minimal_valid_batch(record_count))
}

async fn ensure_topic(broker: &BrokerHandle, name: &str, configs: Vec<(String, Option<String>)>) {
    let _ = broker
        .handler
        .handle_create_topics(
            &broker.ctx(),
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: name.into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    configs,
                }],
                timeout_ms: 5_000,
                validate_only: false,
            },
        )
        .await;
}

async fn produce_with_retry(broker: &BrokerHandle, topic: &str, payload: Bytes) -> i64 {
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
                        name: topic.into(),
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

// ---------------------------------------------------------------------------
// P4.1 — Transactions / EOS
// ---------------------------------------------------------------------------

#[tokio::test]
async fn init_producer_id_with_transactional_id_rejects_today() {
    // A transactional producer's first call is `InitProducerId` with the
    // configured `transactional.id`. Returning a happy producer-id here
    // would let the client believe it has EOS; instead, the broker
    // returns `TransactionalIdAuthorizationFailed` so the producer
    // aborts permanently.
    //
    // TODO(transactions): when the transaction coordinator lands,
    // this call should succeed and return a producer-id bound to the
    // transactional_id, with a fenced-bumped epoch on recovery.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;

    let resp = broker
        .handler
        .handle_init_producer_id(
            &broker.ctx(),
            InitProducerIdRequestData {
                transactional_id: Some("eos-producer-1".to_string()),
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
            },
        )
        .await;

    assert_eq!(
        resp.error_code,
        KafkaCode::TransactionalIdAuthorizationFailed,
        "today's contract: txn-id init must be rejected with code 53; got {:?}",
        resp.error_code,
    );
    assert_eq!(resp.producer_id, -1);
    assert_eq!(resp.producer_epoch, -1);
}

#[tokio::test]
async fn init_producer_id_without_transactional_id_succeeds_today() {
    // The non-transactional path (idempotent producer) IS supported and
    // must continue to work. This is the regression backstop: a future
    // change that flips on transactions must not break the idempotent-only
    // path that real clients use today.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;

    let resp = broker
        .handler
        .handle_init_producer_id(
            &broker.ctx(),
            InitProducerIdRequestData {
                transactional_id: None,
                transaction_timeout_ms: 60_000,
                producer_id: -1,
                producer_epoch: -1,
            },
        )
        .await;

    assert_eq!(resp.error_code, KafkaCode::None);
    assert!(
        resp.producer_id >= 0,
        "idempotent producer must get an assigned (non-sentinel) producer-id; got {}",
        resp.producer_id,
    );
    assert_eq!(resp.producer_epoch, 0);
}

#[tokio::test]
async fn produce_with_transactional_id_rejects_today() {
    // A `Produce` carrying a non-null `transactional_id` is only valid
    // inside an open transaction. Without a coordinator the broker can't
    // verify that, so it rejects every partition with `InvalidRequest`
    // rather than silently writing the records as if non-transactional.
    //
    // TODO(transactions): when EOS lands, this should succeed when
    // the producer-id+epoch match an open transaction registered via
    // AddPartitionsToTxn, and reject only on protocol violations.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, TXN_TOPIC, vec![]).await;

    let resp = broker
        .handler
        .handle_produce(
            &broker.ctx(),
            ProduceRequestData {
                transactional_id: Some("eos-txn-1".to_string()),
                acks: 1,
                timeout_ms: 5_000,
                topics: vec![ProduceTopicData {
                    name: TXN_TOPIC.into(),
                    partitions: vec![ProducePartitionData {
                        partition_index: 0,
                        records: make_batch(1),
                    }],
                }],
            },
        )
        .await;

    let p = &resp.responses[0].partitions[0];
    assert_eq!(
        p.error_code,
        KafkaCode::InvalidRequest,
        "today's contract: transactional produce is rejected partition-wise; got {:?}",
        p.error_code,
    );
    assert_eq!(p.base_offset, -1);
}

#[test]
fn transaction_coordinator_api_keys_are_unknown_today() {
    // Spec keys for the transaction coordinator surface that don't exist
    // in our broker. A client that probes them via ApiVersions should see
    // them omitted from `SUPPORTED_VERSIONS`; a client that sends them
    // anyway falls into `ApiKey::Unknown(_)`.
    //
    // | Key | Name                |
    // |-----|---------------------|
    // |  24 | AddPartitionsToTxn  |
    // |  25 | AddOffsetsToTxn     |
    // |  26 | EndTxn              |
    // |  28 | TxnOffsetCommit     |
    //
    // (`InitProducerId` 22 and the `transactional_id` field on Produce
    // are present — see the runtime tests above for their rejection
    // contract.)
    for key in [24i16, 25, 26, 28] {
        let parsed = ApiKey::from(key);
        assert!(
            matches!(parsed, ApiKey::Unknown(_)),
            "txn coordinator key {} must remain ApiKey::Unknown(_) until \
             transactions land; got {:?}",
            key,
            parsed,
        );
        assert!(
            !SUPPORTED_VERSIONS
                .iter()
                .any(|sv| i16::from(sv.api_key) == key),
            "txn coordinator key {} must not appear in SUPPORTED_VERSIONS \
             until the handler is implemented",
            key,
        );
    }
}

// ---------------------------------------------------------------------------
// P4.2 — Log compaction
// ---------------------------------------------------------------------------

#[tokio::test]
async fn cleanup_policy_compact_is_refused_while_no_cleaner_exists() {
    // Compaction is not implemented, so the policy is refused rather than
    // accepted-and-ignored. This is the inverse of the pin that used to live
    // here: it asserted `KafkaCode::None`, which encoded the broker promising
    // key-collapsing that never happened.
    //
    // TODO(log compaction): when a cleaner lands, flip this back to
    // asserting acceptance and add a test that the policy actually drives
    // compaction scheduling.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;

    let resp = broker
        .handler
        .handle_create_topics(
            &broker.ctx(),
            CreateTopicsRequestData {
                topics: vec![CreateTopicData {
                    name: COMPACT_TOPIC.into(),
                    num_partitions: 1,
                    replication_factor: 1,
                    configs: vec![("cleanup.policy".to_string(), Some("compact".to_string()))],
                }],
                timeout_ms: 5_000,
                validate_only: false,
            },
        )
        .await;

    let topic = &resp.topics[0];
    assert_eq!(
        topic.error_code,
        KafkaCode::InvalidConfig,
        "cleanup.policy=compact must be refused while no cleaner exists; got {:?} ({:?})",
        topic.error_code,
        topic.error_message,
    );
}

#[tokio::test]
async fn keyed_batches_are_never_collapsed_today() {
    // No cleaner runs anywhere, so two batches produced back-to-back both
    // survive into the log and are returned by Fetch. The topic uses the
    // default `delete` policy because `compact` is now refused at create
    // time — the point of the pin is that nothing collapses same-key records
    // behind the operator's back, which holds for every accepted policy.
    //
    // TODO(log compaction): when the cleaner lands, add the mirror of this
    // test on a `compact` topic — after the dirty-bytes threshold is
    // exceeded and the cleaner has run, only the latest record per key
    // remains.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    ensure_topic(&broker, COMPACT_TOPIC, vec![]).await;

    // Produce two independent batches. Without compaction both survive.
    let off_a = produce_with_retry(&broker, COMPACT_TOPIC, make_batch(1)).await;
    let off_b = produce_with_retry(&broker, COMPACT_TOPIC, make_batch(1)).await;
    assert!(
        off_b > off_a,
        "log offsets must advance: {} -> {}",
        off_a,
        off_b,
    );

    let fetch = broker
        .handler
        .handle_fetch(
            &broker.ctx(),
            FetchRequestData {
                replica_id: -1,
                max_wait_ms: 50,
                min_bytes: 0,
                max_bytes: 1024 * 1024,
                isolation_level: 0,
                session_id: 0,
                session_epoch: 0,
                topics: vec![FetchTopicData {
                    name: COMPACT_TOPIC.into(),
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
            },
        )
        .await;

    let p = &fetch.responses[0].partitions[0];
    assert_eq!(p.error_code, KafkaCode::None);
    assert!(
        p.high_watermark >= 2,
        "two acked batches must advance HWM to >= 2; got {}",
        p.high_watermark,
    );
    let returned = p.records.as_ref().expect("fetch must return records");
    let one = make_batch(1).len();
    // Both batches must round-trip intact, with nothing collapsing them.
    assert!(
        returned.len() >= one * 2,
        "both batches must be present (no compaction); got {} bytes (one batch is {one})",
        returned.len(),
    );
}

// ---------------------------------------------------------------------------
// P4.3 — Delegation tokens
// ---------------------------------------------------------------------------

#[test]
fn delegation_token_api_keys_are_unknown_today() {
    // Delegation tokens are not on the README's roadmap. Pin: the four
    // token RPC keys are absent from the broker's enum and from
    // SUPPORTED_VERSIONS, so a client that probes them via ApiVersions
    // simply doesn't see them.
    //
    // | Key | Name                       |
    // |-----|----------------------------|
    // |  38 | CreateDelegationToken      |
    // |  39 | RenewDelegationToken       |
    // |  40 | ExpireDelegationToken      |
    // |  41 | DescribeDelegationToken    |
    //
    // TODO(delegation tokens): if the roadmap adopts them, replace
    // this with positive assertions on token issue / renew / describe.
    for key in [38i16, 39, 40, 41] {
        let parsed = ApiKey::from(key);
        assert!(
            matches!(parsed, ApiKey::Unknown(_)),
            "delegation token key {} must remain ApiKey::Unknown(_); got {:?}",
            key,
            parsed,
        );
        assert!(
            !SUPPORTED_VERSIONS
                .iter()
                .any(|sv| i16::from(sv.api_key) == key),
            "delegation token key {} must not appear in SUPPORTED_VERSIONS",
            key,
        );
    }
}

// ---------------------------------------------------------------------------
// P4.4 — ACL administration via Kafka RPC
// ---------------------------------------------------------------------------

#[test]
fn acl_admin_rpc_keys_are_supported() {
    // Wire adapters for DescribeAcls / CreateAcls / DeleteAcls land on the
    // existing Raft ACL state machine. Pin: the keys are named ApiKey
    // variants and advertised in SUPPORTED_VERSIONS at v0..=v1.
    let expected = [
        (29i16, "DescribeAcls", ApiKey::DescribeAcls),
        (30, "CreateAcls", ApiKey::CreateAcls),
        (31, "DeleteAcls", ApiKey::DeleteAcls),
    ];
    for (key, name, variant) in expected {
        let parsed = ApiKey::from(key);
        assert_eq!(
            parsed, variant,
            "ACL admin key {key} ({name}) must resolve to {variant:?}; got {parsed:?}"
        );
        let sv = SUPPORTED_VERSIONS
            .iter()
            .find(|sv| i16::from(sv.api_key) == key)
            .unwrap_or_else(|| panic!("{name} missing from SUPPORTED_VERSIONS"));
        assert_eq!(sv.min_version, 0, "{name} min_version");
        assert_eq!(sv.max_version, 1, "{name} max_version");
        assert_eq!(parsed.as_str(), name);
    }
}

// ---------------------------------------------------------------------------
// P4.5 — KIP-848 next-gen group protocol
// ---------------------------------------------------------------------------

#[test]
fn kip_848_consumer_group_keys_are_unknown_today() {
    // Pin: the next-gen protocol's two new RPCs are absent. A 4.x Java
    // consumer that prefers KIP-848 will fall back to the classic
    // protocol after seeing them missing in the ApiVersions response.
    //
    // | Key | Name                    |
    // |-----|-------------------------|
    // |  68 | ConsumerGroupHeartbeat  |
    // |  69 | ConsumerGroupDescribe   |
    //
    // TODO(KIP-848): the new protocol is a parallel implementation
    // of the group coordinator, not a refactor of the existing one;
    // when it lands, the classic-protocol pins below should still hold.
    for key in [68i16, 69] {
        let parsed = ApiKey::from(key);
        assert!(
            matches!(parsed, ApiKey::Unknown(_)),
            "KIP-848 key {} must remain ApiKey::Unknown(_); got {:?}",
            key,
            parsed,
        );
        assert!(
            !SUPPORTED_VERSIONS
                .iter()
                .any(|sv| i16::from(sv.api_key) == key),
            "KIP-848 key {} must not appear in SUPPORTED_VERSIONS until \
             the new-protocol handlers are wired",
            key,
        );
    }
}

#[test]
fn classic_group_protocol_remains_supported_today() {
    // The KIP-848 pin's complement: confirm the classic protocol is
    // still the path real consumers take. JoinGroup/SyncGroup/Heartbeat
    // are present in SUPPORTED_VERSIONS today; a regression that drops
    // them while the new protocol isn't ready would silently break every
    // existing consumer.
    for (key, name) in [(11i16, "JoinGroup"), (12, "Heartbeat"), (14, "SyncGroup")] {
        assert!(
            SUPPORTED_VERSIONS
                .iter()
                .any(|sv| i16::from(sv.api_key) == key),
            "{} (key {}) must remain in SUPPORTED_VERSIONS for classic-protocol consumers",
            name,
            key,
        );
    }
}
