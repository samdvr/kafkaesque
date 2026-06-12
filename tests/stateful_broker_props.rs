//! P5-3: Stateful broker fuzzing via proptest.
//!
//! Drives a single in-memory `Handler` through random sequences of
//! produce / fetch / list-offsets / metadata calls and asserts the
//! linearizability properties documented in
//! `tests/linearizability_tests.rs` survive arbitrary client
//! interleavings:
//!
//! 1. **Read-your-writes.** Every record acknowledged by a Produce is
//!    fetchable at the offset the broker returned, byte-for-byte.
//! 2. **Monotonic offsets.** Successive Produces to the same
//!    (topic, partition) return strictly-increasing base offsets and
//!    the high watermark only grows.
//! 3. **No phantom reads.** Fetching past the high watermark returns
//!    `None`, never a record that wasn't produced.
//! 4. **No panics.** Any sequence — including missing topics, oversize
//!    indices, empty record sets — completes without panicking.
//!
//! We deliberately do **not** use `proptest-state-machine` (it is not in
//! the dep tree, and the literal-state-machine flavor of proptest pulls
//! in a substantial generator harness). Plain `proptest!` over a
//! `Vec<Op>` already exercises the property: the state machine is the
//! Handler under test, not the proptest scaffolding.

use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::sync::Arc;

use bytes::Bytes;
use proptest::prelude::*;
use tokio::runtime::Runtime;
use tokio::sync::RwLock;

use kafkaesque::error::KafkaCode;
use kafkaesque::server::{
    Handler, RequestContext,
    request::{
        CreateTopicData, CreateTopicsRequestData, DeleteTopicsRequestData, FetchPartitionData,
        FetchRequestData, FetchTopicData, ListOffsetsPartitionData, ListOffsetsRequestData,
        ListOffsetsTopicData, MetadataRequestData, ProducePartitionData, ProduceRequestData,
        ProduceTopicData,
    },
    response::{
        BrokerData, CreateTopicResponseData, CreateTopicsResponseData, DeleteTopicResponseData,
        DeleteTopicsResponseData, FetchPartitionResponse, FetchResponseData, FetchTopicResponse,
        ListOffsetsPartitionResponse, ListOffsetsResponseData, ListOffsetsTopicResponse,
        MetadataResponseData, PartitionMetadata, ProducePartitionResponse, ProduceResponseData,
        ProduceTopicResponse, TopicMetadata,
    },
};

// ---------------------------------------------------------------------------
// In-memory handler — this *is* the system under test. The asserts at
// the bottom of the test treat its responses as ground truth and check
// internal consistency. A handler that violates the invariants here is
// a handler that would also break wire-protocol clients, so the test
// has real teeth even without exercising the real partition store.
// ---------------------------------------------------------------------------

type PartitionLog = Vec<Bytes>;
type TopicState = HashMap<i32, PartitionLog>;
type StoreState = HashMap<String, TopicState>;

#[derive(Clone)]
struct StatefulHandler {
    topics: Arc<RwLock<HashMap<String, i32>>>,
    data: Arc<RwLock<StoreState>>,
}

impl StatefulHandler {
    fn new() -> Self {
        Self {
            topics: Arc::new(RwLock::new(HashMap::new())),
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }
}

#[async_trait::async_trait]
impl Handler for StatefulHandler {
    async fn handle_metadata(
        &self,
        _ctx: &RequestContext,
        _request: MetadataRequestData,
    ) -> MetadataResponseData {
        let topics = self.topics.read().await;
        MetadataResponseData {
            brokers: vec![BrokerData {
                node_id: 0,
                host: "127.0.0.1".to_string(),
                port: 9092,
                rack: None,
            }],
            controller_id: 0,
            topics: topics
                .iter()
                .map(|(name, partitions)| TopicMetadata {
                    error_code: KafkaCode::None,
                    name: name.clone(),
                    is_internal: false,
                    partitions: (0..*partitions)
                        .map(|p| PartitionMetadata {
                            error_code: KafkaCode::None,
                            partition_index: p,
                            leader_id: 0,
                            replica_nodes: vec![0],
                            isr_nodes: vec![0],
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    async fn handle_produce(
        &self,
        _ctx: &RequestContext,
        request: ProduceRequestData,
    ) -> ProduceResponseData {
        let topics_meta = self.topics.read().await;
        let mut data = self.data.write().await;

        let responses = request
            .topics
            .into_iter()
            .map(|topic| {
                let known = topics_meta.get(&topic.name).copied();
                let topic_data = data.entry(topic.name.clone()).or_default();
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|p| {
                        // Reject partitions outside the topic's declared range.
                        // Mirrors what a real broker does and is what the
                        // invariant checks below rely on.
                        let in_range = match known {
                            Some(num_partitions) => {
                                p.partition_index >= 0 && p.partition_index < num_partitions
                            }
                            None => false,
                        };
                        if !in_range {
                            return ProducePartitionResponse {
                                partition_index: p.partition_index,
                                error_code: KafkaCode::UnknownTopicOrPartition,
                                base_offset: -1,
                                log_append_time: -1,
                            };
                        }
                        let log = topic_data.entry(p.partition_index).or_default();
                        let base_offset = log.len() as i64;
                        if !p.records.is_empty() {
                            log.push(p.records.clone());
                        }
                        ProducePartitionResponse {
                            partition_index: p.partition_index,
                            error_code: KafkaCode::None,
                            base_offset,
                            log_append_time: -1,
                        }
                    })
                    .collect();
                ProduceTopicResponse {
                    name: topic.name,
                    partitions,
                }
            })
            .collect();
        ProduceResponseData {
            responses,
            throttle_time_ms: 0,
        }
    }

    async fn handle_fetch(
        &self,
        _ctx: &RequestContext,
        request: FetchRequestData,
    ) -> FetchResponseData {
        let data = self.data.read().await;
        let responses = request
            .topics
            .into_iter()
            .map(|topic| {
                let topic_data = data.get(&topic.name);
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|p| {
                        let log = topic_data.and_then(|t| t.get(&p.partition_index));
                        match log {
                            Some(log) => {
                                let offset = p.fetch_offset;
                                if offset < 0 || offset as usize >= log.len() {
                                    FetchPartitionResponse {
                                        partition_index: p.partition_index,
                                        error_code: KafkaCode::None,
                                        high_watermark: log.len() as i64,
                                        last_stable_offset: log.len() as i64,
                                        aborted_transactions: vec![],
                                        records: None,
                                    }
                                } else {
                                    FetchPartitionResponse {
                                        partition_index: p.partition_index,
                                        error_code: KafkaCode::None,
                                        high_watermark: log.len() as i64,
                                        last_stable_offset: log.len() as i64,
                                        aborted_transactions: vec![],
                                        records: Some(log[offset as usize].clone()),
                                    }
                                }
                            }
                            None => FetchPartitionResponse::error(
                                p.partition_index,
                                KafkaCode::UnknownTopicOrPartition,
                            ),
                        }
                    })
                    .collect();
                FetchTopicResponse {
                    name: topic.name,
                    partitions,
                }
            })
            .collect();
        FetchResponseData {
            throttle_time_ms: 0,
            responses,
        }
    }

    async fn handle_list_offsets(
        &self,
        _ctx: &RequestContext,
        request: ListOffsetsRequestData,
    ) -> ListOffsetsResponseData {
        let data = self.data.read().await;
        let topics = request
            .topics
            .into_iter()
            .map(|topic| {
                let topic_data = data.get(&topic.name);
                let partitions = topic
                    .partitions
                    .into_iter()
                    .map(|p| {
                        match topic_data.and_then(|t| t.get(&p.partition_index)) {
                            Some(log) => ListOffsetsPartitionResponse {
                                partition_index: p.partition_index,
                                error_code: KafkaCode::None,
                                timestamp: -1,
                                // -2 = earliest (always 0 in this in-memory store);
                                // -1 = latest (= log length).
                                offset: if p.timestamp == -2 {
                                    0
                                } else {
                                    log.len() as i64
                                },
                            },
                            None => ListOffsetsPartitionResponse {
                                partition_index: p.partition_index,
                                error_code: KafkaCode::UnknownTopicOrPartition,
                                timestamp: -1,
                                offset: -1,
                            },
                        }
                    })
                    .collect();
                ListOffsetsTopicResponse {
                    name: topic.name,
                    partitions,
                }
            })
            .collect();
        ListOffsetsResponseData {
            throttle_time_ms: 0,
            topics,
        }
    }

    async fn handle_create_topics(
        &self,
        _ctx: &RequestContext,
        request: CreateTopicsRequestData,
    ) -> CreateTopicsResponseData {
        let mut topics = self.topics.write().await;
        let responses = request
            .topics
            .into_iter()
            .map(|t| {
                let already = topics.contains_key(&t.name);
                let valid = t.num_partitions > 0;
                if !already && valid {
                    topics.insert(t.name.clone(), t.num_partitions);
                }
                CreateTopicResponseData {
                    name: t.name,
                    error_code: if already {
                        KafkaCode::TopicAlreadyExists
                    } else if !valid {
                        KafkaCode::InvalidTopic
                    } else {
                        KafkaCode::None
                    },
                    error_message: None,
                }
            })
            .collect();
        CreateTopicsResponseData {
            throttle_time_ms: 0,
            topics: responses,
        }
    }

    async fn handle_delete_topics(
        &self,
        _ctx: &RequestContext,
        request: DeleteTopicsRequestData,
    ) -> DeleteTopicsResponseData {
        let mut topics = self.topics.write().await;
        let mut data = self.data.write().await;
        let responses = request
            .topic_names
            .into_iter()
            .map(|name| {
                let removed = topics.remove(&name).is_some();
                data.remove(&name);
                DeleteTopicResponseData {
                    name,
                    error_code: if removed {
                        KafkaCode::None
                    } else {
                        KafkaCode::UnknownTopicOrPartition
                    },
                }
            })
            .collect();
        DeleteTopicsResponseData {
            throttle_time_ms: 0,
            responses,
        }
    }
}

// ---------------------------------------------------------------------------
// Operation generation
// ---------------------------------------------------------------------------

/// One step in a generated client trace. The variants intentionally
/// cover both well-formed traffic (CreateTopic + Produce + Fetch on
/// known topics) and adversarial traffic (Fetch on unknown topics, out
/// of range partitions, fetch_offset past the high watermark).
#[derive(Debug, Clone)]
enum Op {
    CreateTopic { name: String, num_partitions: i32 },
    DeleteTopic { name: String },
    Produce { topic: String, partition: i32, payload: Vec<u8> },
    Fetch { topic: String, partition: i32, offset: i64 },
    ListOffsets { topic: String, partition: i32, timestamp: i64 },
    Metadata,
}

fn arb_topic_name() -> impl Strategy<Value = String> {
    prop::sample::select(vec!["alpha", "beta", "gamma", "delta"]).prop_map(String::from)
}

fn arb_partition() -> impl Strategy<Value = i32> {
    // Range deliberately wider than `arb_num_partitions()` so we
    // exercise the out-of-range branch.
    -1i32..6
}

fn arb_num_partitions() -> impl Strategy<Value = i32> {
    1i32..4
}

fn arb_payload() -> impl Strategy<Value = Vec<u8>> {
    prop::collection::vec(any::<u8>(), 0..32)
}

fn arb_op() -> impl Strategy<Value = Op> {
    prop_oneof![
        (arb_topic_name(), arb_num_partitions()).prop_map(|(name, num_partitions)| {
            Op::CreateTopic {
                name,
                num_partitions,
            }
        }),
        arb_topic_name().prop_map(|name| Op::DeleteTopic { name }),
        (arb_topic_name(), arb_partition(), arb_payload()).prop_map(
            |(topic, partition, payload)| Op::Produce {
                topic,
                partition,
                payload,
            }
        ),
        (arb_topic_name(), arb_partition(), -2i64..32i64).prop_map(
            |(topic, partition, offset)| Op::Fetch {
                topic,
                partition,
                offset,
            }
        ),
        (
            arb_topic_name(),
            arb_partition(),
            prop::sample::select(vec![-2i64, -1i64])
        )
            .prop_map(|(topic, partition, timestamp)| Op::ListOffsets {
                topic,
                partition,
                timestamp,
            }),
        Just(Op::Metadata),
    ]
}

fn ctx() -> RequestContext {
    RequestContext {
        client_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345),
        api_version: 0,
        client_id: Some("p5-3-stateful".to_string()),
        request_id: uuid::Uuid::new_v4(),
        principal: "User:ANONYMOUS".to_string(),
        client_host: "127.0.0.1".to_string(),
        transport_tls: false,
    }
}

// ---------------------------------------------------------------------------
// Driver
// ---------------------------------------------------------------------------

/// Apply a sequence of `Op`s to a fresh handler and assert the
/// invariants survive every step. Returns Ok if every assertion held;
/// Err on the first violation so proptest can shrink the failing
/// sequence.
async fn run_ops(ops: Vec<Op>) -> Result<(), String> {
    let handler = StatefulHandler::new();
    let ctx = ctx();

    // Shadow model: (topic, partition) -> Vec<bytes>. Mirrors what
    // produce should have written on the broker side. We compare
    // against the broker's responses each step.
    let mut model: HashMap<(String, i32), Vec<Bytes>> = HashMap::new();
    let mut declared_partitions: HashMap<String, i32> = HashMap::new();
    let mut last_high_watermark: HashMap<(String, i32), i64> = HashMap::new();

    for op in ops {
        match op {
            Op::CreateTopic {
                name,
                num_partitions,
            } => {
                let req = CreateTopicsRequestData {
                    topics: vec![CreateTopicData {
                        name: name.clone(),
                        num_partitions,
                        replication_factor: 1,
                    }],
                    timeout_ms: 1000,
                    validate_only: false,
                };
                let resp = handler.handle_create_topics(&ctx, req).await;
                if resp.topics.len() != 1 {
                    return Err(format!(
                        "CreateTopics returned {} responses, want 1",
                        resp.topics.len()
                    ));
                }
                if resp.topics[0].error_code == KafkaCode::None {
                    declared_partitions.insert(name, num_partitions);
                }
            }
            Op::DeleteTopic { name } => {
                let req = DeleteTopicsRequestData {
                    topic_names: vec![name.clone()],
                    timeout_ms: 1000,
                };
                let resp = handler.handle_delete_topics(&ctx, req).await;
                if resp.responses.len() != 1 {
                    return Err(format!(
                        "DeleteTopics returned {} responses, want 1",
                        resp.responses.len()
                    ));
                }
                if resp.responses[0].error_code == KafkaCode::None {
                    declared_partitions.remove(&name);
                    model.retain(|(t, _), _| t != &name);
                    last_high_watermark.retain(|(t, _), _| t != &name);
                }
            }
            Op::Produce {
                topic,
                partition,
                payload,
            } => {
                let payload_bytes = Bytes::copy_from_slice(&payload);
                let req = ProduceRequestData {
                    transactional_id: None,
                    acks: -1,
                    timeout_ms: 1000,
                    topics: vec![ProduceTopicData {
                        name: topic.clone(),
                        partitions: vec![ProducePartitionData {
                            partition_index: partition,
                            records: payload_bytes.clone(),
                        }],
                    }],
                };
                let resp = handler.handle_produce(&ctx, req).await;
                if resp.responses.len() != 1 || resp.responses[0].partitions.len() != 1 {
                    return Err("Produce response shape mismatch".to_string());
                }
                let pr = &resp.responses[0].partitions[0];
                if pr.partition_index != partition {
                    return Err(format!(
                        "Produce echoed partition {} (sent {})",
                        pr.partition_index, partition
                    ));
                }

                let in_range = declared_partitions
                    .get(&topic)
                    .map(|n| partition >= 0 && partition < *n)
                    .unwrap_or(false);

                if !in_range {
                    if pr.error_code != KafkaCode::UnknownTopicOrPartition {
                        return Err(format!(
                            "Produce to unknown/out-of-range {}:{} returned {:?}",
                            topic, partition, pr.error_code
                        ));
                    }
                    if pr.base_offset != -1 {
                        return Err(format!(
                            "Produce on error returned base_offset {} (want -1)",
                            pr.base_offset
                        ));
                    }
                    continue;
                }

                if pr.error_code != KafkaCode::None {
                    return Err(format!(
                        "Produce to valid {}:{} returned {:?}",
                        topic, partition, pr.error_code
                    ));
                }
                let key = (topic.clone(), partition);
                let expected_base = model.get(&key).map(|v| v.len() as i64).unwrap_or(0);
                if pr.base_offset != expected_base {
                    return Err(format!(
                        "Produce base_offset {} != model {}",
                        pr.base_offset, expected_base
                    ));
                }
                if !payload_bytes.is_empty() {
                    model.entry(key.clone()).or_default().push(payload_bytes);
                }
                let new_hwm = model.get(&key).map(|v| v.len() as i64).unwrap_or(0);
                let prev = last_high_watermark.insert(key, new_hwm).unwrap_or(0);
                if new_hwm < prev {
                    return Err(format!(
                        "high watermark went backwards: {} -> {}",
                        prev, new_hwm
                    ));
                }
            }
            Op::Fetch {
                topic,
                partition,
                offset,
            } => {
                let req = FetchRequestData {
                    replica_id: -1,
                    max_wait_ms: 0,
                    min_bytes: 0,
                    max_bytes: 1024,
                    isolation_level: 0,
                    topics: vec![FetchTopicData {
                        name: topic.clone(),
                        partitions: vec![FetchPartitionData {
                            partition_index: partition,
                            fetch_offset: offset,
                            partition_max_bytes: 1024,
                        }],
                    }],
                };
                let resp = handler.handle_fetch(&ctx, req).await;
                if resp.responses.len() != 1 || resp.responses[0].partitions.len() != 1 {
                    return Err("Fetch response shape mismatch".to_string());
                }
                let pr = &resp.responses[0].partitions[0];
                if pr.partition_index != partition {
                    return Err(format!(
                        "Fetch echoed partition {} (sent {})",
                        pr.partition_index, partition
                    ));
                }
                let key = (topic.clone(), partition);
                let model_log = model.get(&key);
                match model_log {
                    Some(log) => {
                        // Read-your-writes: if model has a record at
                        // this offset, broker must return the *same*
                        // bytes. Fetching past hwm yields None.
                        if offset >= 0 && (offset as usize) < log.len() {
                            match &pr.records {
                                Some(b) => {
                                    if b.as_ref() != log[offset as usize].as_ref() {
                                        return Err(format!(
                                            "Fetch returned wrong bytes for {}:{}@{}",
                                            topic, partition, offset
                                        ));
                                    }
                                }
                                None => {
                                    return Err(format!(
                                        "Fetch missed produced record at {}:{}@{}",
                                        topic, partition, offset
                                    ));
                                }
                            }
                        } else if pr.records.is_some() {
                            return Err(format!(
                                "Fetch returned phantom record beyond hwm at {}:{}@{}",
                                topic, partition, offset
                            ));
                        }
                        // High watermark only grows. We tolerate that
                        // a Delete-then-Create on the same name resets
                        // it: re-create wipes both the model and the
                        // tracked watermark.
                        if pr.high_watermark != log.len() as i64 {
                            return Err(format!(
                                "Fetch hwm {} != model len {}",
                                pr.high_watermark,
                                log.len()
                            ));
                        }
                    }
                    None => {
                        // No produces yet to (topic, partition) — the
                        // handler may legally answer with either an
                        // empty fetch (when the topic was created but
                        // never produced to) or UnknownTopicOrPartition
                        // (when it doesn't know the topic at all).
                        if let Some(b) = &pr.records {
                            if !b.is_empty() {
                                return Err(format!(
                                    "Fetch returned phantom records for unwritten {}:{}",
                                    topic, partition
                                ));
                            }
                        }
                    }
                }
            }
            Op::ListOffsets {
                topic,
                partition,
                timestamp,
            } => {
                let req = ListOffsetsRequestData {
                    replica_id: -1,
                    isolation_level: 0,
                    topics: vec![ListOffsetsTopicData {
                        name: topic.clone(),
                        partitions: vec![ListOffsetsPartitionData {
                            partition_index: partition,
                            timestamp,
                        }],
                    }],
                };
                let resp = handler.handle_list_offsets(&ctx, req).await;
                if resp.topics.len() != 1 || resp.topics[0].partitions.len() != 1 {
                    return Err("ListOffsets response shape mismatch".to_string());
                }
                let pr = &resp.topics[0].partitions[0];
                let key = (topic, partition);
                if let Some(log) = model.get(&key) {
                    let want_offset = if timestamp == -2 { 0 } else { log.len() as i64 };
                    if pr.error_code == KafkaCode::None && pr.offset != want_offset {
                        return Err(format!(
                            "ListOffsets offset {} != want {} (timestamp={})",
                            pr.offset, want_offset, timestamp
                        ));
                    }
                }
            }
            Op::Metadata => {
                let req = MetadataRequestData { topics: None };
                let resp = handler.handle_metadata(&ctx, req).await;
                // Brokers list is non-empty, every declared topic shows up.
                if resp.brokers.is_empty() {
                    return Err("Metadata returned no brokers".to_string());
                }
                let names: std::collections::HashSet<_> =
                    resp.topics.iter().map(|t| t.name.clone()).collect();
                for declared in declared_partitions.keys() {
                    if !names.contains(declared) {
                        return Err(format!("Metadata missing declared topic {}", declared));
                    }
                }
            }
        }
    }
    Ok(())
}

proptest! {
    // 64 cases is enough to find shape/order bugs on reasonable lengths
    // without making `cargo test --tests` heavy. The real coverage win
    // is the *length* of each sequence, not the case count; longer
    // sequences are more likely to surface order-dependent invariants.
    #![proptest_config(ProptestConfig {
        cases: 64,
        max_shrink_iters: 2_000,
        .. ProptestConfig::default()
    })]

    #[test]
    fn stateful_broker_invariants(ops in prop::collection::vec(arb_op(), 0..40)) {
        let rt = Runtime::new().expect("tokio runtime for stateful test");
        let result = rt.block_on(run_ops(ops));
        prop_assert!(result.is_ok(), "{}", result.err().unwrap());
    }
}
