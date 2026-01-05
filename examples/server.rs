//! In-memory Kafka-compatible server implementation.
//!
//! This example demonstrates a fully functional Kafka-compatible server
//! that stores messages in memory. Connect with any Kafka client.
//!
//! **WARNING**: This is for testing/development only. The in-memory storage
//! has configurable size limits but data is lost on restart.
//!
//! Run with: cargo run --example server
//!
//! Then connect with: kafka-console-producer --bootstrap-server localhost:9092 --topic test

use async_trait::async_trait;
use bytes::Bytes;
use kafkaesque::constants::DEFAULT_NUM_PARTITIONS;
use kafkaesque::error::KafkaCode;
use kafkaesque::protocol::{parse_record_count, patch_base_offset};
use kafkaesque::server::request::*;
use kafkaesque::server::response::*;
use kafkaesque::server::{Handler, KafkaServer, RequestContext};
use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, AtomicUsize, Ordering};
use tokio::sync::RwLock;
use tracing::{debug, info, warn};

/// Default maximum memory size for stored messages (100 MB).
/// Configurable via constructor.
const DEFAULT_MAX_MEMORY_BYTES: usize = 100 * 1024 * 1024;

/// Consumer group state for rebalance protocol.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
enum GroupState {
    /// No members in the group
    #[default]
    Empty,
    /// Members are joining, waiting for all to join
    PreparingRebalance,
    /// Group is stable, assignments distributed
    Stable,
}

/// In-memory Kafka-compatible server.
///
/// Stores messages in memory and supports:
/// - Metadata requests (list brokers/topics)
/// - Produce (store messages)
/// - Fetch (retrieve messages)
/// - ListOffsets (get earliest/latest offsets)
/// - CreateTopics / DeleteTopics
/// - Consumer group coordination (FindCoordinator, JoinGroup, SyncGroup, Heartbeat, LeaveGroup)
/// - Offset management (OffsetCommit, OffsetFetch)
/// - Idempotent producer support (InitProducerId)
///
/// **Memory Limits**: This handler enforces a configurable maximum memory size.
/// When the limit is exceeded, the oldest batches are evicted. This provides
/// a bounded memory footprint suitable for development and testing.
///
/// Note: Uses tokio::sync::RwLock for async-friendly locking to avoid
/// blocking the async runtime during lock acquisition.
#[allow(clippy::type_complexity)]
pub struct InMemoryHandler {
    /// Topic -> Partition -> PartitionLog
    data: RwLock<HashMap<String, HashMap<i32, PartitionLog>>>,
    /// Group ID -> Topic -> Partition -> Committed Offset
    committed_offsets: RwLock<HashMap<String, HashMap<String, HashMap<i32, i64>>>>,
    /// Group ID -> Generation ID
    group_generations: RwLock<HashMap<String, i32>>,
    /// Group ID -> Member ID -> Last heartbeat timestamp
    group_members: RwLock<HashMap<String, HashMap<String, u64>>>,
    /// Group ID -> Leader Member ID
    /// Tracks the actual leader for each consumer group
    group_leaders: RwLock<HashMap<String, String>>,
    /// Group ID -> Group State (Empty, PreparingRebalance, Stable)
    /// Tracks the state of each consumer group for proper generation management
    group_states: RwLock<HashMap<String, GroupState>>,
    /// Group ID -> Member ID -> Assignment bytes
    /// Stores assignments from leader for non-leader members to retrieve
    group_assignments: RwLock<HashMap<String, HashMap<String, Bytes>>>,
    /// Next producer ID to assign
    next_producer_id: AtomicI64,
    /// Current total memory usage for stored batches
    current_memory_bytes: AtomicUsize,
    /// Maximum allowed memory for stored batches
    max_memory_bytes: usize,
    /// Server configuration
    broker_id: i32,
    host: String,
    port: i32,
}

/// A stored record batch with its metadata.
#[derive(Clone)]
struct StoredBatch {
    base_offset: i64,
    record_count: i32,
    data: Bytes,
}

/// Log for a partition, tracking batches and offsets.
#[derive(Default)]
struct PartitionLog {
    batches: Vec<StoredBatch>,
    high_watermark: i64,
    /// Earliest available offset (updated when batches are evicted)
    earliest_offset: i64,
}

impl InMemoryHandler {
    /// Create a new in-memory handler with default memory limits (100 MB).
    pub fn new(host: &str, port: i32) -> Self {
        Self::with_max_memory(host, port, DEFAULT_MAX_MEMORY_BYTES)
    }

    /// Create a new in-memory handler with a custom memory limit.
    ///
    /// # Arguments
    /// * `host` - The hostname to advertise to clients
    /// * `port` - The port to advertise to clients
    /// * `max_memory_bytes` - Maximum memory for stored batches before eviction
    pub fn with_max_memory(host: &str, port: i32, max_memory_bytes: usize) -> Self {
        info!(
            max_memory_mb = max_memory_bytes / 1024 / 1024,
            "Creating in-memory handler with memory limit"
        );
        Self {
            data: RwLock::new(HashMap::new()),
            committed_offsets: RwLock::new(HashMap::new()),
            group_generations: RwLock::new(HashMap::new()),
            group_members: RwLock::new(HashMap::new()),
            group_leaders: RwLock::new(HashMap::new()),
            group_states: RwLock::new(HashMap::new()),
            group_assignments: RwLock::new(HashMap::new()),
            next_producer_id: AtomicI64::new(1000),
            current_memory_bytes: AtomicUsize::new(0),
            max_memory_bytes,
            broker_id: 0,
            host: host.to_string(),
            port,
        }
    }

    /// Ensure a topic/partition exists, creating it if necessary.
    async fn ensure_topic_partition(&self, topic: &str, partition: i32) {
        let mut data = self.data.write().await;
        data.entry(topic.to_string())
            .or_default()
            .entry(partition)
            .or_default();
    }

    /// Evict oldest batches from a partition until we're under the memory limit.
    ///
    /// This is called after adding a new batch and checks if eviction is needed.
    /// Eviction removes the oldest batches first (FIFO), updating the earliest
    /// available offset for the partition.
    async fn maybe_evict_oldest(&self) {
        let current = self.current_memory_bytes.load(Ordering::SeqCst);
        if current <= self.max_memory_bytes {
            return;
        }

        let bytes_to_free = current - self.max_memory_bytes;
        let mut freed = 0usize;

        let mut data = self.data.write().await;

        // Iterate through all partitions and evict oldest batches
        // This is a simple round-robin eviction strategy
        'outer: for (_topic_name, partitions) in data.iter_mut() {
            for (_partition_id, log) in partitions.iter_mut() {
                while !log.batches.is_empty() && freed < bytes_to_free {
                    let batch = log.batches.remove(0);
                    let batch_size = batch.data.len();
                    freed += batch_size;

                    // Update earliest offset to reflect eviction
                    if !log.batches.is_empty() {
                        log.earliest_offset = log.batches[0].base_offset;
                    }

                    if freed >= bytes_to_free {
                        break 'outer;
                    }
                }
            }
        }

        // Update the memory counter
        self.current_memory_bytes.fetch_sub(freed, Ordering::SeqCst);
        warn!(
            freed_bytes = freed,
            new_total = self.current_memory_bytes.load(Ordering::SeqCst),
            "Evicted oldest batches to stay within memory limit"
        );
    }

    /// Generate a unique member ID.
    fn generate_member_id(&self) -> String {
        use std::time::{SystemTime, UNIX_EPOCH};
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        format!("member-{}", timestamp)
    }

    /// Get current timestamp in milliseconds.
    fn now_ms(&self) -> u64 {
        use std::time::{SystemTime, UNIX_EPOCH};
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as u64
    }
}

#[async_trait]
impl Handler for InMemoryHandler {
    async fn handle_metadata(
        &self,
        _ctx: &RequestContext,
        request: MetadataRequestData,
    ) -> MetadataResponseData {
        let topics: Vec<TopicMetadata> = match request.topics {
            Some(topic_names) => {
                // First pass: auto-create missing topics
                for name in &topic_names {
                    let exists = {
                        let data = self.data.read().await;
                        data.contains_key(name)
                    };
                    if !exists {
                        self.ensure_topic_partition(name, 0).await;
                    }
                }

                // Second pass: build metadata
                let data = self.data.read().await;
                topic_names
                    .into_iter()
                    .map(|name| {
                        let partitions = data
                            .get(&name)
                            .map(|parts| {
                                parts
                                    .keys()
                                    .map(|&partition_index| PartitionMetadata {
                                        error_code: KafkaCode::None,
                                        partition_index,
                                        leader_id: self.broker_id,
                                        replica_nodes: vec![self.broker_id],
                                        isr_nodes: vec![self.broker_id],
                                    })
                                    .collect()
                            })
                            .unwrap_or_else(|| {
                                vec![PartitionMetadata {
                                    error_code: KafkaCode::None,
                                    partition_index: 0,
                                    leader_id: self.broker_id,
                                    replica_nodes: vec![self.broker_id],
                                    isr_nodes: vec![self.broker_id],
                                }]
                            });

                        TopicMetadata {
                            error_code: KafkaCode::None,
                            name,
                            is_internal: false,
                            partitions,
                        }
                    })
                    .collect()
            }
            None => {
                let data = self.data.read().await;
                data.iter()
                    .map(|(name, partitions)| TopicMetadata {
                        error_code: KafkaCode::None,
                        name: name.clone(),
                        is_internal: false,
                        partitions: partitions
                            .keys()
                            .map(|&partition_index| PartitionMetadata {
                                error_code: KafkaCode::None,
                                partition_index,
                                leader_id: self.broker_id,
                                replica_nodes: vec![self.broker_id],
                                isr_nodes: vec![self.broker_id],
                            })
                            .collect(),
                    })
                    .collect()
            }
        };

        MetadataResponseData {
            brokers: vec![BrokerData {
                node_id: self.broker_id,
                host: self.host.clone(),
                port: self.port,
                rack: None,
            }],
            controller_id: self.broker_id,
            topics,
        }
    }

    async fn handle_produce(
        &self,
        _ctx: &RequestContext,
        request: ProduceRequestData,
    ) -> ProduceResponseData {
        let mut responses = Vec::new();
        let mut total_bytes_added: usize = 0;

        debug!(
            topic_count = request.topics.len(),
            "PRODUCE request received"
        );

        for topic in request.topics {
            let mut partition_responses = Vec::new();

            debug!(topic = %topic.name, partition_count = topic.partitions.len(), "PRODUCE processing topic");

            for partition in topic.partitions {
                self.ensure_topic_partition(&topic.name, partition.partition_index)
                    .await;

                // Get offset and store message atomically to avoid race conditions
                let (base_offset, bytes_added) = {
                    let mut data = self.data.write().await;
                    if let Some(partitions) = data.get_mut(&topic.name) {
                        if let Some(log) = partitions.get_mut(&partition.partition_index) {
                            let offset = log.high_watermark;
                            let record_count = parse_record_count(&partition.records);

                            // Patch the base_offset in the RecordBatch header (first 8 bytes)
                            // This is critical: the producer sends batches with base_offset=0,
                            // but the consumer uses base_offset to determine record positions
                            let mut patched_records = partition.records.to_vec();
                            patch_base_offset(&mut patched_records, offset);

                            let batch_size = patched_records.len();
                            log.batches.push(StoredBatch {
                                base_offset: offset,
                                record_count,
                                data: Bytes::from(patched_records),
                            });
                            log.high_watermark = offset + record_count as i64;

                            debug!(
                                partition = partition.partition_index,
                                record_bytes = partition.records.len(),
                                base_offset = offset,
                                record_count,
                                high_watermark = log.high_watermark,
                                "PRODUCE stored batch"
                            );
                            (offset, batch_size)
                        } else {
                            (-1, 0)
                        }
                    } else {
                        (-1, 0)
                    }
                };

                total_bytes_added += bytes_added;

                partition_responses.push(ProducePartitionResponse {
                    partition_index: partition.partition_index,
                    error_code: KafkaCode::None,
                    base_offset,
                    log_append_time: -1,
                });
            }

            responses.push(ProduceTopicResponse {
                name: topic.name,
                partitions: partition_responses,
            });
        }

        // Update memory tracking and potentially evict old batches
        if total_bytes_added > 0 {
            self.current_memory_bytes
                .fetch_add(total_bytes_added, Ordering::SeqCst);
            self.maybe_evict_oldest().await;
        }

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
        let mut responses = Vec::new();

        debug!(topic_count = request.topics.len(), "FETCH request received");

        for topic in request.topics {
            let mut partition_responses = Vec::new();

            for partition in topic.partitions {
                let (error_code, high_watermark, records) = match data.get(&topic.name) {
                    Some(partitions) => match partitions.get(&partition.partition_index) {
                        Some(log) => {
                            let fetch_offset = partition.fetch_offset;
                            debug!(
                                topic = %topic.name,
                                partition = partition.partition_index,
                                batch_count = log.batches.len(),
                                high_watermark = log.high_watermark,
                                fetch_offset,
                                "FETCH processing partition"
                            );

                            // Find batches that contain records >= fetch_offset
                            let mut combined = Vec::new();
                            for batch in &log.batches {
                                // Include batch if it contains records at or after fetch_offset
                                let batch_end_offset =
                                    batch.base_offset + batch.record_count as i64;
                                if batch_end_offset > fetch_offset {
                                    combined.extend_from_slice(&batch.data);
                                }
                            }

                            let records = if combined.is_empty() {
                                debug!("FETCH no new messages");
                                None
                            } else {
                                debug!(bytes = combined.len(), "FETCH returning records");
                                Some(Bytes::from(combined))
                            };
                            (KafkaCode::None, log.high_watermark, records)
                        }
                        None => {
                            debug!(
                                partition = partition.partition_index,
                                "FETCH partition not found"
                            );
                            (KafkaCode::UnknownTopicOrPartition, -1, None)
                        }
                    },
                    None => {
                        debug!(topic = %topic.name, "FETCH topic not found");
                        (KafkaCode::UnknownTopicOrPartition, -1, None)
                    }
                };

                partition_responses.push(FetchPartitionResponse {
                    partition_index: partition.partition_index,
                    error_code,
                    high_watermark,
                    last_stable_offset: high_watermark,
                    aborted_transactions: vec![],
                    records,
                });
            }

            responses.push(FetchTopicResponse {
                name: topic.name,
                partitions: partition_responses,
            });
        }

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
        let mut topics = Vec::new();

        for topic in request.topics {
            let mut partitions = Vec::new();

            for partition in topic.partitions {
                let (error_code, offset, timestamp) = match data.get(&topic.name) {
                    Some(topic_data) => match topic_data.get(&partition.partition_index) {
                        Some(log) => {
                            match partition.timestamp {
                                -2 => {
                                    // Earliest offset (accounts for eviction)
                                    (KafkaCode::None, log.earliest_offset, -1i64)
                                }
                                -1 => {
                                    // Latest offset
                                    (KafkaCode::None, log.high_watermark, -1i64)
                                }
                                ts => {
                                    // Timestamp-based lookup not supported
                                    debug!(
                                        topic = %topic.name,
                                        partition = partition.partition_index,
                                        timestamp = ts,
                                        "Timestamp-based offset lookup not supported"
                                    );
                                    (KafkaCode::UnsupportedForMessageFormat, -1i64, ts)
                                }
                            }
                        }
                        None => (KafkaCode::UnknownTopicOrPartition, -1, -1),
                    },
                    None => (KafkaCode::UnknownTopicOrPartition, -1, -1),
                };

                partitions.push(ListOffsetsPartitionResponse {
                    partition_index: partition.partition_index,
                    error_code,
                    timestamp,
                    offset,
                });
            }

            topics.push(ListOffsetsTopicResponse {
                name: topic.name,
                partitions,
            });
        }

        ListOffsetsResponseData {
            throttle_time_ms: 0,
            topics,
        }
    }

    async fn handle_find_coordinator(
        &self,
        _ctx: &RequestContext,
        _request: FindCoordinatorRequestData,
    ) -> FindCoordinatorResponseData {
        FindCoordinatorResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            error_message: None,
            node_id: self.broker_id,
            host: self.host.clone(),
            port: self.port,
        }
    }

    async fn handle_join_group(
        &self,
        _ctx: &RequestContext,
        request: JoinGroupRequestData,
    ) -> JoinGroupResponseData {
        let member_id = if request.member_id.is_empty() {
            self.generate_member_id()
        } else {
            request.member_id.clone()
        };

        // Check if this is a new member or existing member rejoining
        let is_new_member = {
            let members = self.group_members.read().await;
            members
                .get(&request.group_id)
                .map(|g| !g.contains_key(&member_id))
                .unwrap_or(true)
        };

        // Get current group state
        let current_state = {
            let states = self.group_states.read().await;
            states
                .get(&request.group_id)
                .copied()
                .unwrap_or(GroupState::Empty)
        };

        // Only increment generation if:
        // 1. This is a new member joining, OR
        // 2. The group is in Empty state (first member), OR
        // 3. We're triggering a rebalance (not Stable)
        let should_increment = is_new_member || current_state == GroupState::Empty;

        let generation_id = {
            let mut generations = self.group_generations.write().await;
            let generation = generations.entry(request.group_id.clone()).or_insert(0);
            if should_increment {
                *generation += 1;
            }
            *generation
        };

        // Update group state to PreparingRebalance if new member or was Empty
        if should_increment {
            let mut states = self.group_states.write().await;
            states.insert(request.group_id.clone(), GroupState::PreparingRebalance);
        }

        // Add member to group
        {
            let mut members = self.group_members.write().await;
            let group = members.entry(request.group_id.clone()).or_default();
            group.insert(member_id.clone(), self.now_ms());
        }

        // Determine and track the leader correctly
        // The leader is the first member (lexicographically smallest member_id)
        // among all current members. This ensures all members see the same leader.
        let leader = {
            let members = self.group_members.read().await;
            let mut leaders = self.group_leaders.write().await;

            let current_leader = if let Some(group) = members.get(&request.group_id) {
                // Sort member IDs and pick the first one
                let mut member_ids: Vec<&String> = group.keys().collect();
                member_ids.sort();
                member_ids.first().map(|s| (*s).clone())
            } else {
                None
            };

            // Update stored leader
            if let Some(ref leader_id) = current_leader {
                leaders.insert(request.group_id.clone(), leader_id.clone());
            }

            current_leader.unwrap_or_else(|| member_id.clone())
        };

        // Get protocol name from first protocol
        let protocol_name = request
            .protocols
            .first()
            .map(|p| p.name.clone())
            .unwrap_or_default();

        // Build member list (for leader) - sorted for consistency
        let mut member_list: Vec<JoinGroupMemberData> = {
            let members = self.group_members.read().await;
            members
                .get(&request.group_id)
                .map(|group| {
                    group
                        .keys()
                        .map(|mid| JoinGroupMemberData {
                            member_id: mid.clone(),
                            metadata: request
                                .protocols
                                .first()
                                .map(|p| p.metadata.clone())
                                .unwrap_or_default(),
                        })
                        .collect()
                })
                .unwrap_or_default()
        };
        member_list.sort_by(|a, b| a.member_id.cmp(&b.member_id));

        JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            generation_id,
            protocol_name,
            leader, // Use the actual leader, not the joining member
            member_id: member_id.clone(),
            members: member_list,
        }
    }

    async fn handle_sync_group(
        &self,
        _ctx: &RequestContext,
        request: SyncGroupRequestData,
    ) -> SyncGroupResponseData {
        // Leader sends assignments for ALL members, which we store.
        // Non-leader members then retrieve their assignment from storage.

        // If this request contains assignments, store them (leader sends these)
        if !request.assignments.is_empty() {
            let mut assignments = self.group_assignments.write().await;
            let group_assignments = assignments.entry(request.group_id.clone()).or_default();

            for assignment in &request.assignments {
                group_assignments
                    .insert(assignment.member_id.clone(), assignment.assignment.clone());
            }

            // Leader has distributed assignments - group is now Stable
            let mut states = self.group_states.write().await;
            states.insert(request.group_id.clone(), GroupState::Stable);
        }

        // Retrieve assignment for this member (works for both leader and non-leader)
        let assignment = {
            let assignments = self.group_assignments.read().await;
            assignments
                .get(&request.group_id)
                .and_then(|group| group.get(&request.member_id))
                .cloned()
                .unwrap_or_default()
        };

        SyncGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            assignment,
        }
    }

    #[allow(clippy::collapsible_if)]
    async fn handle_heartbeat(
        &self,
        _ctx: &RequestContext,
        request: HeartbeatRequestData,
    ) -> HeartbeatResponseData {
        // Update heartbeat timestamp
        {
            let mut members = self.group_members.write().await;
            if let Some(group) = members.get_mut(&request.group_id) {
                if let Some(ts) = group.get_mut(&request.member_id) {
                    *ts = self.now_ms();
                }
            }
        }

        HeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
        }
    }

    async fn handle_leave_group(
        &self,
        _ctx: &RequestContext,
        request: LeaveGroupRequestData,
    ) -> LeaveGroupResponseData {
        let is_now_empty = {
            let mut members = self.group_members.write().await;
            if let Some(group) = members.get_mut(&request.group_id) {
                group.remove(&request.member_id);
                group.is_empty()
            } else {
                true
            }
        };

        // Update group state when member leaves
        {
            let mut states = self.group_states.write().await;
            if is_now_empty {
                // No members left - group is Empty
                states.insert(request.group_id.clone(), GroupState::Empty);
            } else {
                // Members left need to rebalance
                states.insert(request.group_id.clone(), GroupState::PreparingRebalance);
            }
        }

        // Clear this member's assignment
        {
            let mut assignments = self.group_assignments.write().await;
            if let Some(group_assignments) = assignments.get_mut(&request.group_id) {
                group_assignments.remove(&request.member_id);
            }
        }

        LeaveGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
        }
    }

    async fn handle_offset_commit(
        &self,
        _ctx: &RequestContext,
        request: OffsetCommitRequestData,
    ) -> OffsetCommitResponseData {
        let mut responses = Vec::new();

        {
            let mut offsets = self.committed_offsets.write().await;
            let group_offsets = offsets.entry(request.group_id.clone()).or_default();

            for topic in &request.topics {
                let mut partition_responses = Vec::new();
                let topic_offsets = group_offsets.entry(topic.name.clone()).or_default();

                for partition in &topic.partitions {
                    topic_offsets.insert(partition.partition_index, partition.committed_offset);
                    partition_responses.push(OffsetCommitPartitionResponse {
                        partition_index: partition.partition_index,
                        error_code: KafkaCode::None,
                    });
                }

                responses.push(OffsetCommitTopicResponse {
                    name: topic.name.clone(),
                    partitions: partition_responses,
                });
            }
        }

        OffsetCommitResponseData {
            throttle_time_ms: 0,
            topics: responses,
        }
    }

    async fn handle_offset_fetch(
        &self,
        _ctx: &RequestContext,
        request: OffsetFetchRequestData,
    ) -> OffsetFetchResponseData {
        let offsets = self.committed_offsets.read().await;
        let mut responses = Vec::new();

        let group_offsets = offsets.get(&request.group_id);

        for topic in &request.topics {
            let mut partition_responses = Vec::new();
            let topic_offsets = group_offsets.and_then(|g| g.get(&topic.name));

            for &partition_index in &topic.partition_indexes {
                let committed_offset = topic_offsets
                    .and_then(|t| t.get(&partition_index))
                    .copied()
                    .unwrap_or(-1);

                partition_responses.push(OffsetFetchPartitionResponse {
                    partition_index,
                    committed_offset,
                    metadata: None,
                    error_code: KafkaCode::None,
                });
            }

            responses.push(OffsetFetchTopicResponse {
                name: topic.name.clone(),
                partitions: partition_responses,
            });
        }

        OffsetFetchResponseData {
            throttle_time_ms: 0,
            topics: responses,
            error_code: KafkaCode::None,
        }
    }

    async fn handle_create_topics(
        &self,
        _ctx: &RequestContext,
        request: CreateTopicsRequestData,
    ) -> CreateTopicsResponseData {
        let mut topics = Vec::new();

        for topic in request.topics {
            let num_partitions = if topic.num_partitions <= 0 {
                DEFAULT_NUM_PARTITIONS
            } else {
                topic.num_partitions
            };

            for partition in 0..num_partitions {
                self.ensure_topic_partition(&topic.name, partition).await;
            }

            topics.push(CreateTopicResponseData {
                name: topic.name,
                error_code: KafkaCode::None,
                error_message: None,
            });
        }

        CreateTopicsResponseData {
            throttle_time_ms: 0,
            topics,
        }
    }

    async fn handle_delete_topics(
        &self,
        _ctx: &RequestContext,
        request: DeleteTopicsRequestData,
    ) -> DeleteTopicsResponseData {
        let mut data = self.data.write().await;
        let mut responses = Vec::new();

        for name in request.topic_names {
            let error_code = if data.remove(&name).is_some() {
                KafkaCode::None
            } else {
                KafkaCode::UnknownTopicOrPartition
            };

            responses.push(DeleteTopicResponseData { name, error_code });
        }

        DeleteTopicsResponseData {
            throttle_time_ms: 0,
            responses,
        }
    }

    async fn handle_init_producer_id(
        &self,
        _ctx: &RequestContext,
        _request: InitProducerIdRequestData,
    ) -> InitProducerIdResponseData {
        // Assign a unique producer ID
        let producer_id = self.next_producer_id.fetch_add(1, Ordering::SeqCst);

        InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            producer_id,
            producer_epoch: 0,
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let host = "127.0.0.1";
    let port = 9092;
    let addr = format!("{}:{}", host, port);

    info!(address = %addr, "Starting Kafka-compatible server");

    let handler = InMemoryHandler::new(host, port);
    let server = KafkaServer::new(&addr, handler).await?;

    info!("Server is running. Connect with any Kafka client.");
    info!(
        "Example producer: kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test"
    );
    info!(
        "Example consumer: kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning"
    );

    server.run().await?;

    Ok(())
}
