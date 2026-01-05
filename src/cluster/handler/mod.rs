//! SlateDB cluster handler implementing the Kafka Handler trait.
//!
//! This module is split into several submodules by handler category:
//! - `metadata` - Metadata request handling
//! - `produce` - Produce request handling
//! - `fetch` - Fetch request handling
//! - `offsets` - Offset listing, commit, and fetch
//! - `groups` - Consumer group coordination
//! - `admin` - Topic creation and deletion
//! - `producer_id` - Producer ID initialization

mod admin;
mod fetch;
mod groups;
mod metadata;
mod offsets;
mod produce;
mod producer_id;

use async_trait::async_trait;
use moka::sync::Cache;
use object_store::ObjectStore;
use std::sync::Arc;
use tracing::{error, info, warn};

use crate::error::KafkaCode;
use crate::protocol::{CrcValidationResult, validate_batch_crc};
use crate::server::request::*;
use crate::server::response::*;
use crate::server::{Handler, RequestContext};
use crate::types::BrokerId;

use super::config::ClusterConfig;
use super::error::SlateDBResult;
use super::object_store::create_object_store;
use super::partition_manager::PartitionManager;
use super::raft::{RaftConfig, RaftCoordinator, request_cluster_join};
use super::traits::PartitionCoordinator;

/// Health status of the cluster handler.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HealthStatus {
    /// Handler is healthy and accepting requests.
    Healthy,
    /// Handler is in zombie mode (lost Raft coordination).
    Zombie,
    /// Handler is degraded (some partitions unavailable).
    Degraded,
    /// Handler is shutting down.
    ShuttingDown,
}

/// SlateDB cluster handler for Kafka protocol.
///
/// This handler uses:
/// - SlateDB for partition data storage (one instance per partition)
/// - Raft for coordination, consumer groups, and offsets
/// - Object storage (S3, GCS, or local) for durability
pub struct SlateDBClusterHandler {
    /// Partition manager handles ownership and storage.
    pub(crate) partition_manager: Arc<PartitionManager<RaftCoordinator>>,

    /// Raft coordinator for consumer groups and metadata.
    pub(crate) coordinator: Arc<RaftCoordinator>,

    /// Broker ID for this server.
    pub(crate) broker_id: BrokerId,

    /// Host address for this broker.
    pub(crate) host: String,

    /// Port for this broker.
    pub(crate) port: i32,

    /// Whether to auto-create topics on first reference.
    /// If false, metadata requests for unknown topics return UnknownTopicOrPartition.
    pub(crate) auto_create_topics: bool,

    /// Whether to validate CRC-32C checksums on incoming record batches.
    /// When enabled, corrupted batches are rejected with CorruptMessage.
    pub(crate) validate_record_crc: bool,

    /// Maximum concurrent partition writes per produce request.
    pub(crate) max_concurrent_partition_writes: usize,

    /// Maximum concurrent partition reads per fetch request.
    pub(crate) max_concurrent_partition_reads: usize,

    /// Cache for topic name Arc<str> allocations to reduce hot path allocations.
    topic_name_cache: Cache<String, Arc<str>>,
}

impl SlateDBClusterHandler {
    /// Create a new SlateDB cluster handler.
    ///
    /// Uses the `object_store` field from config to determine storage backend.
    pub async fn new(config: ClusterConfig) -> SlateDBResult<Self> {
        let object_store = create_object_store(&config)?;
        Self::with_object_store(config, object_store).await
    }

    /// Create a new handler with a custom object store.
    pub async fn with_object_store(
        config: ClusterConfig,
        object_store: Arc<dyn ObjectStore>,
    ) -> SlateDBResult<Self> {
        // Initialize circuit breaker config from ClusterConfig
        super::metrics::init_circuit_breaker_config(
            config.circuit_breaker_threshold,
            config.circuit_breaker_base_reset_window_ms,
            config.circuit_breaker_max_reset_window_ms,
        );

        // Create Raft coordinator with object store for snapshot persistence
        let raft_config = RaftConfig::from_cluster_config(&config);
        let coordinator =
            Arc::new(RaftCoordinator::new(raft_config.clone(), object_store.clone()).await?);

        // Check if the cluster is already initialized from persisted state.
        // This handles the restart case where snapshots were loaded.
        let already_initialized = coordinator.is_initialized();

        // Initialize cluster:
        // - Skip if cluster is already initialized (restart with persisted state)
        // - Single node (no peers): always initialize on fresh start
        // - Multi-node: only the node with the lowest broker_id initializes on fresh start
        let should_initialize = if already_initialized {
            info!(
                broker_id = config.broker_id,
                "Cluster already initialized from persisted state, skipping initialization"
            );
            false
        } else {
            match &config.raft_peers {
                None => true, // Single node
                Some(peers) => {
                    // Parse peer IDs from format "node_id=host:port,node_id=host:port,..."
                    let peer_ids: Vec<i32> = peers
                        .split(',')
                        .filter(|s| !s.is_empty())
                        .filter_map(|s| {
                            let parts: Vec<&str> = s.split('=').collect();
                            if parts.len() == 2 {
                                parts[0].parse::<i32>().ok()
                            } else {
                                None
                            }
                        })
                        .collect();

                    // This broker should initialize if its ID is lower than all peer IDs
                    let min_peer_id = peer_ids.iter().min().copied().unwrap_or(i32::MAX);
                    config.broker_id < min_peer_id
                }
            }
        };

        if should_initialize {
            info!(broker_id = config.broker_id, "Initializing Raft cluster");
            coordinator.initialize_cluster().await?;
        }

        // Start background tasks
        coordinator.start_background_tasks().await;

        // For non-initializing nodes on fresh start (no persisted state),
        // we need to join the existing cluster by adding ourselves as a
        // learner and then promoting to voter.
        // Skip joining if we already have persisted state - we're already a member.
        if !should_initialize && !already_initialized {
            info!(
                broker_id = config.broker_id,
                "Joining existing Raft cluster as new member"
            );

            // Try to join the cluster via any known peer
            let node_id = config.broker_id as u64;
            let raft_addr = config.raft_listen_addr.clone();

            // Try each peer until we successfully join
            if let Some(peers) = &config.raft_peers {
                let mut joined = false;
                for peer_spec in peers.split(',').filter(|s| !s.is_empty()) {
                    let parts: Vec<&str> = peer_spec.split('=').collect();
                    if parts.len() == 2 {
                        let peer_addr = parts[1];
                        info!(
                            broker_id = config.broker_id,
                            peer_addr = %peer_addr,
                            "Requesting to join cluster via peer"
                        );

                        // Send join request to the peer (who will forward to leader if needed)
                        match request_cluster_join(peer_addr, node_id, &raft_addr).await {
                            Ok(()) => {
                                info!(
                                    broker_id = config.broker_id,
                                    "Successfully requested to join cluster"
                                );
                                joined = true;
                                break;
                            }
                            Err(e) => {
                                warn!(
                                    broker_id = config.broker_id,
                                    peer_addr = %peer_addr,
                                    error = %e,
                                    "Failed to join via peer, trying next"
                                );
                            }
                        }
                    }
                }

                if !joined {
                    warn!(
                        broker_id = config.broker_id,
                        "Could not join cluster via any peer, will retry during operation"
                    );
                }
            }
        }

        // Wait for leader election before registering
        // This ensures the cluster is ready for writes
        coordinator.wait_for_leader().await?;

        // Register this broker
        coordinator.register_broker().await?;

        // Create partition manager
        let partition_manager = Arc::new(PartitionManager::new(
            coordinator.clone(),
            object_store,
            config.clone(),
        ));

        // Start background tasks
        partition_manager.start().await;

        info!(
            broker_id = config.broker_id,
            bind_host = %config.host,
            advertised_host = %config.advertised_host,
            port = config.port,
            auto_create_topics = config.auto_create_topics,
            validate_record_crc = config.validate_record_crc,
            "SlateDB cluster handler initialized with Raft coordination"
        );

        Ok(Self {
            partition_manager,
            coordinator,
            broker_id: BrokerId::new(config.broker_id),
            host: config.advertised_host.clone(),
            port: config.port,
            auto_create_topics: config.auto_create_topics,
            validate_record_crc: config.validate_record_crc,
            max_concurrent_partition_writes: config.max_concurrent_partition_writes,
            max_concurrent_partition_reads: config.max_concurrent_partition_reads,
            topic_name_cache: Cache::new(10_000),
        })
    }

    /// Shutdown the handler gracefully.
    pub async fn shutdown(&self) -> SlateDBResult<()> {
        self.partition_manager.shutdown().await?;
        self.coordinator.shutdown().await
    }

    /// Check if the broker is in zombie mode.
    ///
    /// Zombie mode indicates the broker has lost coordination with Raft
    /// and cannot reliably determine partition ownership. In this state,
    /// all write operations are rejected to prevent split-brain scenarios.
    ///
    /// # Returns
    /// `true` if the broker is in zombie mode and writes should be rejected.
    pub fn is_zombie(&self) -> bool {
        self.partition_manager.is_zombie()
    }

    /// Get a cached Arc<str> for a topic name.
    ///
    /// This method returns a cached Arc<str> for the topic name,
    /// or creates and caches a new one if not present. This avoids allocation
    /// pressure from creating `Arc::from(topic.name.as_str())` on every request.
    pub(crate) fn cached_topic_name(&self, topic: &str) -> Arc<str> {
        self.topic_name_cache
            .get_with(topic.to_string(), || Arc::from(topic))
    }

    /// Build topic metadata response for a single topic.
    ///
    /// This method queries the coordinator for partition information and builds
    /// a complete metadata response including leader information for each partition.
    pub(crate) async fn build_topic_metadata(&self, topic: &str) -> TopicMetadata {
        let partition_count = self
            .coordinator
            .get_partition_count(topic)
            .await
            .ok()
            .flatten()
            .unwrap_or(1);

        let mut partitions = Vec::with_capacity(partition_count as usize);
        for p in 0..partition_count {
            let owner = self
                .coordinator
                .get_partition_owner(topic, p)
                .await
                .ok()
                .flatten();

            // When no owner is known, return LeaderNotAvailable instead of
            // falsely claiming this broker is the leader. This prevents clients
            // from sending produce requests to brokers that don't own the partition,
            // which would result in NOT_LEADER_OR_FOLLOWER errors.
            let (leader_id, error_code) = match owner {
                Some(id) => (id, KafkaCode::None),
                None => (-1, KafkaCode::LeaderNotAvailable),
            };

            partitions.push(PartitionMetadata {
                error_code,
                partition_index: p,
                leader_id,
                replica_nodes: if leader_id >= 0 {
                    vec![leader_id]
                } else {
                    vec![]
                },
                isr_nodes: if leader_id >= 0 {
                    vec![leader_id]
                } else {
                    vec![]
                },
            });
        }

        TopicMetadata {
            error_code: KafkaCode::None,
            name: topic.to_string(),
            is_internal: false,
            partitions,
        }
    }

    /// Produce records to a single partition.
    ///
    /// This method handles the complete produce flow for a single partition:
    /// 1. Checks if broker is in zombie mode (rejects with NotLeaderForPartition)
    /// 2. Attempts to get an existing partition store from the local cache
    /// 3. If not cached, tries to acquire ownership of the partition
    /// 4. Appends the record batch to the partition's log
    /// 5. Records produce metrics on success
    ///
    /// Produce a batch to a single partition.
    ///
    /// # Arguments
    /// * `topic` - The topic name
    /// * `partition` - The partition data containing records
    /// * `acks` - Acknowledgment level (0=fire-and-forget, 1/-1=wait for flush)
    pub(crate) async fn produce_to_partition(
        &self,
        topic: &str,
        partition: ProducePartitionData,
        _acks: i16,
    ) -> ProducePartitionResponse {
        // Reject writes when in zombie mode
        if self.partition_manager.is_zombie() {
            return ProducePartitionResponse {
                partition_index: partition.partition_index,
                error_code: KafkaCode::NotLeaderForPartition,
                base_offset: -1,
                log_append_time: -1,
            };
        }

        // Use get_for_write which verifies fresh ownership with coordinator
        // IMPORTANT: We do NOT try to acquire partitions here. Partition acquisition
        // happens only in the background ownership loop. This prevents "partition stealing"
        // where produce requests cause brokers to compete for partition ownership.
        let store = match self
            .partition_manager
            .get_for_write(topic, partition.partition_index)
            .await
        {
            Ok(s) => s,
            Err(_) => {
                // We don't own this partition - tell client to contact correct broker
                // The client will refresh metadata and retry to the actual owner
                return ProducePartitionResponse {
                    partition_index: partition.partition_index,
                    error_code: KafkaCode::NotLeaderForPartition,
                    base_offset: -1,
                    log_append_time: -1,
                };
            }
        };

        // Validate CRC if enabled
        if self.validate_record_crc {
            match validate_batch_crc(&partition.records) {
                CrcValidationResult::Valid => {}
                CrcValidationResult::Invalid { expected, actual } => {
                    warn!(
                        topic = %topic,
                        partition = partition.partition_index,
                        expected_crc = format!("{:#x}", expected),
                        actual_crc = format!("{:#x}", actual),
                        "Rejecting batch with invalid CRC"
                    );
                    super::metrics::record_coordinator_failure("crc_validation_failed");
                    return ProducePartitionResponse {
                        partition_index: partition.partition_index,
                        error_code: KafkaCode::CorruptMessage,
                        base_offset: -1,
                        log_append_time: -1,
                    };
                }
                CrcValidationResult::TooSmall => {
                    warn!(
                        topic = %topic,
                        partition = partition.partition_index,
                        batch_size = partition.records.len(),
                        "Rejecting batch too small to contain valid CRC"
                    );
                    super::metrics::record_coordinator_failure("crc_validation_failed");
                    return ProducePartitionResponse {
                        partition_index: partition.partition_index,
                        error_code: KafkaCode::CorruptMessage,
                        base_offset: -1,
                        log_append_time: -1,
                    };
                }
            }
        }

        // Append the batch
        match store.append_batch(&partition.records).await {
            Ok(base_offset) => {
                let bytes = partition.records.len() as u64;
                super::metrics::record_produce(topic, partition.partition_index, 1, bytes);

                ProducePartitionResponse {
                    partition_index: partition.partition_index,
                    error_code: KafkaCode::None,
                    base_offset,
                    log_append_time: -1,
                }
            }
            Err(e) => {
                // Use unified error-to-Kafka-code mapping (R3 refactoring)
                let error_code = e.to_kafka_code();
                if e.is_not_leader() {
                    error!(
                        topic,
                        partition = partition.partition_index,
                        error = %e,
                        "Fenced during produce - returning NotLeaderForPartition"
                    );
                } else {
                    error!(error = %e, "Failed to append batch");
                }
                ProducePartitionResponse {
                    partition_index: partition.partition_index,
                    error_code,
                    base_offset: -1,
                    log_append_time: -1,
                }
            }
        }
    }
}

#[async_trait]
impl Handler for SlateDBClusterHandler {
    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id))]
    async fn handle_metadata(
        &self,
        _ctx: &RequestContext,
        request: MetadataRequestData,
    ) -> MetadataResponseData {
        metadata::handle_metadata(self, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id, topic_count = request.topics.len()))]
    async fn handle_produce(
        &self,
        ctx: &RequestContext,
        request: ProduceRequestData,
    ) -> ProduceResponseData {
        produce::handle_produce(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, ctx, request), fields(request_id = %ctx.request_id, topic_count = request.topics.len()))]
    async fn handle_fetch(
        &self,
        ctx: &RequestContext,
        request: FetchRequestData,
    ) -> FetchResponseData {
        fetch::handle_fetch(self, ctx, request).await
    }

    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id))]
    async fn handle_list_offsets(
        &self,
        _ctx: &RequestContext,
        request: ListOffsetsRequestData,
    ) -> ListOffsetsResponseData {
        offsets::handle_list_offsets(self, request).await
    }

    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id, key = %request.key))]
    async fn handle_find_coordinator(
        &self,
        _ctx: &RequestContext,
        request: FindCoordinatorRequestData,
    ) -> FindCoordinatorResponseData {
        groups::handle_find_coordinator(self, request).await
    }

    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id, group_id = %request.group_id))]
    async fn handle_join_group(
        &self,
        _ctx: &RequestContext,
        request: JoinGroupRequestData,
    ) -> JoinGroupResponseData {
        groups::handle_join_group(self, request).await
    }

    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id, group_id = %request.group_id))]
    async fn handle_sync_group(
        &self,
        _ctx: &RequestContext,
        request: SyncGroupRequestData,
    ) -> SyncGroupResponseData {
        groups::handle_sync_group(self, request).await
    }

    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id, group_id = %request.group_id))]
    async fn handle_heartbeat(
        &self,
        _ctx: &RequestContext,
        request: HeartbeatRequestData,
    ) -> HeartbeatResponseData {
        groups::handle_heartbeat(self, request).await
    }

    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id, group_id = %request.group_id))]
    async fn handle_leave_group(
        &self,
        _ctx: &RequestContext,
        request: LeaveGroupRequestData,
    ) -> LeaveGroupResponseData {
        groups::handle_leave_group(self, request).await
    }

    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id, group_id = %request.group_id))]
    async fn handle_offset_commit(
        &self,
        _ctx: &RequestContext,
        request: OffsetCommitRequestData,
    ) -> OffsetCommitResponseData {
        offsets::handle_offset_commit(self, request).await
    }

    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id, group_id = %request.group_id))]
    async fn handle_offset_fetch(
        &self,
        _ctx: &RequestContext,
        request: OffsetFetchRequestData,
    ) -> OffsetFetchResponseData {
        offsets::handle_offset_fetch(self, request).await
    }

    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id))]
    async fn handle_create_topics(
        &self,
        _ctx: &RequestContext,
        request: CreateTopicsRequestData,
    ) -> CreateTopicsResponseData {
        admin::handle_create_topics(self, request).await
    }

    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id))]
    async fn handle_delete_topics(
        &self,
        _ctx: &RequestContext,
        request: DeleteTopicsRequestData,
    ) -> DeleteTopicsResponseData {
        admin::handle_delete_topics(self, request).await
    }

    #[tracing::instrument(skip(self, _ctx, request), fields(request_id = %_ctx.request_id))]
    async fn handle_init_producer_id(
        &self,
        _ctx: &RequestContext,
        request: InitProducerIdRequestData,
    ) -> InitProducerIdResponseData {
        producer_id::handle_init_producer_id(self, request).await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // ========================================================================
    // HealthStatus Tests
    // ========================================================================

    #[test]
    fn test_health_status_debug() {
        assert!(format!("{:?}", HealthStatus::Healthy).contains("Healthy"));
        assert!(format!("{:?}", HealthStatus::Zombie).contains("Zombie"));
        assert!(format!("{:?}", HealthStatus::Degraded).contains("Degraded"));
        assert!(format!("{:?}", HealthStatus::ShuttingDown).contains("ShuttingDown"));
    }

    #[test]
    fn test_health_status_clone() {
        let healthy = HealthStatus::Healthy;
        let cloned = healthy;
        assert_eq!(healthy, cloned);
    }

    #[test]
    fn test_health_status_copy() {
        let healthy = HealthStatus::Healthy;
        let copied: HealthStatus = healthy; // Copy, not move
        assert_eq!(healthy, copied);
    }

    #[test]
    fn test_health_status_equality() {
        assert_eq!(HealthStatus::Healthy, HealthStatus::Healthy);
        assert_eq!(HealthStatus::Zombie, HealthStatus::Zombie);
        assert_eq!(HealthStatus::Degraded, HealthStatus::Degraded);
        assert_eq!(HealthStatus::ShuttingDown, HealthStatus::ShuttingDown);

        assert_ne!(HealthStatus::Healthy, HealthStatus::Zombie);
        assert_ne!(HealthStatus::Healthy, HealthStatus::Degraded);
        assert_ne!(HealthStatus::Healthy, HealthStatus::ShuttingDown);
        assert_ne!(HealthStatus::Zombie, HealthStatus::Degraded);
    }

    #[test]
    fn test_health_status_all_variants() {
        // Ensure we can create all variants
        let statuses = [
            HealthStatus::Healthy,
            HealthStatus::Zombie,
            HealthStatus::Degraded,
            HealthStatus::ShuttingDown,
        ];

        assert_eq!(statuses.len(), 4);

        // All should be distinct
        for (i, s1) in statuses.iter().enumerate() {
            for (j, s2) in statuses.iter().enumerate() {
                if i == j {
                    assert_eq!(s1, s2);
                } else {
                    assert_ne!(s1, s2);
                }
            }
        }
    }

    // ========================================================================
    // Topic Name Cache Tests
    // ========================================================================

    #[test]
    fn test_topic_name_cache_creation() {
        // Test that moka Cache can be created with expected capacity
        let cache: Cache<String, Arc<str>> = Cache::new(10_000);
        assert_eq!(cache.entry_count(), 0);
    }

    #[test]
    fn test_arc_str_caching() {
        // Test the pattern used in cached_topic_name
        let cache: Cache<String, Arc<str>> = Cache::new(100);

        // First access creates the Arc
        let topic1 = cache.get_with("test-topic".to_string(), || Arc::from("test-topic"));
        assert_eq!(topic1.as_ref(), "test-topic");

        // Second access returns cached Arc (same pointer)
        let topic2 = cache.get_with("test-topic".to_string(), || Arc::from("test-topic"));
        assert!(Arc::ptr_eq(&topic1, &topic2));

        // Different topic creates different Arc
        let topic3 = cache.get_with("other-topic".to_string(), || Arc::from("other-topic"));
        assert_eq!(topic3.as_ref(), "other-topic");
        assert!(!Arc::ptr_eq(&topic1, &topic3));
    }

    #[test]
    fn test_arc_str_cache_many_topics() {
        let cache: Cache<String, Arc<str>> = Cache::new(10);

        // Add more topics than cache capacity
        for i in 0..20 {
            let name = format!("topic-{}", i);
            let arc = cache.get_with(name.clone(), || Arc::from(name.as_str()));
            assert_eq!(arc.as_ref(), name.as_str());
        }

        // moka cache may defer eviction, so entry count could be 0 or more
        // Just verify we can call entry_count without error
        let _count = cache.entry_count();
    }

    // ========================================================================
    // CRC Validation Response Tests
    // ========================================================================

    #[test]
    fn test_crc_validation_result_variants() {
        // Test that CrcValidationResult can be used in pattern matching
        let valid = CrcValidationResult::Valid;
        let invalid = CrcValidationResult::Invalid {
            expected: 0x12345678,
            actual: 0x87654321,
        };
        let too_small = CrcValidationResult::TooSmall;

        match valid {
            CrcValidationResult::Valid => {}
            _ => panic!("Expected Valid"),
        }

        match invalid {
            CrcValidationResult::Invalid { expected, actual } => {
                assert_eq!(expected, 0x12345678);
                assert_eq!(actual, 0x87654321);
            }
            _ => panic!("Expected Invalid"),
        }

        match too_small {
            CrcValidationResult::TooSmall => {}
            _ => panic!("Expected TooSmall"),
        }
    }

    // ========================================================================
    // Produce Partition Response Tests
    // ========================================================================

    #[test]
    fn test_produce_partition_response_zombie_mode() {
        // Verify the structure of a zombie mode response
        let response = ProducePartitionResponse {
            partition_index: 5,
            error_code: KafkaCode::NotLeaderForPartition,
            base_offset: -1,
            log_append_time: -1,
        };

        assert_eq!(response.partition_index, 5);
        assert_eq!(response.error_code, KafkaCode::NotLeaderForPartition);
        assert_eq!(response.base_offset, -1);
        assert_eq!(response.log_append_time, -1);
    }

    #[test]
    fn test_produce_partition_response_success() {
        let response = ProducePartitionResponse {
            partition_index: 0,
            error_code: KafkaCode::None,
            base_offset: 42,
            log_append_time: -1,
        };

        assert_eq!(response.partition_index, 0);
        assert_eq!(response.error_code, KafkaCode::None);
        assert_eq!(response.base_offset, 42);
    }

    #[test]
    fn test_produce_partition_response_corrupt_message() {
        let response = ProducePartitionResponse {
            partition_index: 3,
            error_code: KafkaCode::CorruptMessage,
            base_offset: -1,
            log_append_time: -1,
        };

        assert_eq!(response.error_code, KafkaCode::CorruptMessage);
    }

    // ========================================================================
    // Partition Metadata Tests
    // ========================================================================

    #[test]
    fn test_partition_metadata_structure() {
        let meta = PartitionMetadata {
            error_code: KafkaCode::None,
            partition_index: 0,
            leader_id: 1,
            replica_nodes: vec![1],
            isr_nodes: vec![1],
        };

        assert_eq!(meta.error_code, KafkaCode::None);
        assert_eq!(meta.partition_index, 0);
        assert_eq!(meta.leader_id, 1);
        assert_eq!(meta.replica_nodes, vec![1]);
        assert_eq!(meta.isr_nodes, vec![1]);
    }

    #[test]
    fn test_topic_metadata_structure() {
        let partitions = vec![
            PartitionMetadata {
                error_code: KafkaCode::None,
                partition_index: 0,
                leader_id: 1,
                replica_nodes: vec![1],
                isr_nodes: vec![1],
            },
            PartitionMetadata {
                error_code: KafkaCode::None,
                partition_index: 1,
                leader_id: 2,
                replica_nodes: vec![2],
                isr_nodes: vec![2],
            },
        ];

        let topic = TopicMetadata {
            error_code: KafkaCode::None,
            name: "my-topic".to_string(),
            is_internal: false,
            partitions,
        };

        assert_eq!(topic.name, "my-topic");
        assert!(!topic.is_internal);
        assert_eq!(topic.partitions.len(), 2);
        assert_eq!(topic.partitions[0].partition_index, 0);
        assert_eq!(topic.partitions[1].partition_index, 1);
    }

    // ========================================================================
    // BrokerId Tests
    // ========================================================================

    #[test]
    fn test_broker_id_new() {
        let id = BrokerId::new(42);
        assert_eq!(id.value(), 42);
    }

    #[test]
    fn test_broker_id_zero() {
        let id = BrokerId::new(0);
        assert_eq!(id.value(), 0);
    }

    #[test]
    fn test_broker_id_negative() {
        let id = BrokerId::new(-1);
        assert_eq!(id.value(), -1);
    }

    #[test]
    fn test_broker_id_max() {
        let id = BrokerId::new(i32::MAX);
        assert_eq!(id.value(), i32::MAX);
    }

    // ========================================================================
    // Topic Metadata Response Tests
    // ========================================================================

    #[test]
    fn test_topic_metadata_internal_topic() {
        let topic = TopicMetadata {
            error_code: KafkaCode::None,
            name: "__consumer_offsets".to_string(),
            is_internal: true,
            partitions: vec![],
        };

        assert!(topic.is_internal);
        assert_eq!(topic.name, "__consumer_offsets");
    }

    #[test]
    fn test_topic_metadata_error() {
        let topic = TopicMetadata {
            error_code: KafkaCode::UnknownTopicOrPartition,
            name: "nonexistent".to_string(),
            is_internal: false,
            partitions: vec![],
        };

        assert_eq!(topic.error_code, KafkaCode::UnknownTopicOrPartition);
        assert!(topic.partitions.is_empty());
    }

    #[test]
    fn test_partition_metadata_with_replicas() {
        let meta = PartitionMetadata {
            error_code: KafkaCode::None,
            partition_index: 5,
            leader_id: 1,
            replica_nodes: vec![1, 2, 3],
            isr_nodes: vec![1, 2],
        };

        assert_eq!(meta.partition_index, 5);
        assert_eq!(meta.replica_nodes.len(), 3);
        assert_eq!(meta.isr_nodes.len(), 2);
    }

    #[test]
    fn test_partition_metadata_leader_election() {
        // Simulate a partition with no leader elected yet
        let meta = PartitionMetadata {
            error_code: KafkaCode::LeaderNotAvailable,
            partition_index: 0,
            leader_id: -1,
            replica_nodes: vec![1, 2, 3],
            isr_nodes: vec![],
        };

        assert_eq!(meta.error_code, KafkaCode::LeaderNotAvailable);
        assert_eq!(meta.leader_id, -1);
        assert!(meta.isr_nodes.is_empty());
    }

    // ========================================================================
    // KafkaCode Response Tests
    // ========================================================================

    #[test]
    fn test_produce_error_codes() {
        // All error codes that can be returned from produce_to_partition
        let error_codes = [
            KafkaCode::None,
            KafkaCode::NotLeaderForPartition,
            KafkaCode::CorruptMessage,
            KafkaCode::Unknown,
        ];

        for code in error_codes {
            let response = ProducePartitionResponse {
                partition_index: 0,
                error_code: code,
                base_offset: if code == KafkaCode::None { 0 } else { -1 },
                log_append_time: -1,
            };
            assert_eq!(response.error_code, code);
        }
    }

    // ========================================================================
    // Multiple Partition Tests
    // ========================================================================

    #[test]
    fn test_topic_metadata_many_partitions() {
        let partitions: Vec<PartitionMetadata> = (0..100)
            .map(|i| PartitionMetadata {
                error_code: KafkaCode::None,
                partition_index: i,
                leader_id: (i % 3) + 1, // Distribute across brokers 1, 2, 3
                replica_nodes: vec![(i % 3) + 1],
                isr_nodes: vec![(i % 3) + 1],
            })
            .collect();

        let topic = TopicMetadata {
            error_code: KafkaCode::None,
            name: "high-partition-topic".to_string(),
            is_internal: false,
            partitions,
        };

        assert_eq!(topic.partitions.len(), 100);
        assert_eq!(topic.partitions[0].leader_id, 1);
        assert_eq!(topic.partitions[1].leader_id, 2);
        assert_eq!(topic.partitions[2].leader_id, 3);
        assert_eq!(topic.partitions[99].partition_index, 99);
    }

    // ========================================================================
    // Cache Entry Tests
    // ========================================================================

    #[test]
    fn test_arc_str_cache_unicode_topics() {
        let cache: Cache<String, Arc<str>> = Cache::new(100);

        // Test with unicode topic names
        let topics = [
            "普通话-topic",
            "日本語-トピック",
            "한국어-주제",
            "emoji-🎉-topic",
        ];

        for topic in topics {
            let arc = cache.get_with(topic.to_string(), || Arc::from(topic));
            assert_eq!(arc.as_ref(), topic);

            // Verify it's cached
            let arc2 = cache.get_with(topic.to_string(), || Arc::from(topic));
            assert!(Arc::ptr_eq(&arc, &arc2));
        }
    }

    #[test]
    fn test_arc_str_cache_empty_string() {
        let cache: Cache<String, Arc<str>> = Cache::new(100);

        let empty = cache.get_with(String::new(), || Arc::from(""));
        assert_eq!(empty.as_ref(), "");
    }

    #[test]
    fn test_arc_str_cache_long_topic_name() {
        let cache: Cache<String, Arc<str>> = Cache::new(100);

        // Kafka allows topic names up to 249 characters
        let long_name = "a".repeat(249);
        let arc = cache.get_with(long_name.clone(), || Arc::from(long_name.as_str()));
        assert_eq!(arc.len(), 249);
    }

    // ========================================================================
    // Response Structure Tests
    // ========================================================================

    #[test]
    fn test_produce_topic_response_structure() {
        let partitions = vec![
            ProducePartitionResponse {
                partition_index: 0,
                error_code: KafkaCode::None,
                base_offset: 100,
                log_append_time: -1,
            },
            ProducePartitionResponse {
                partition_index: 1,
                error_code: KafkaCode::NotLeaderForPartition,
                base_offset: -1,
                log_append_time: -1,
            },
        ];

        let topic_response = ProduceTopicResponse {
            name: "test".to_string(),
            partitions,
        };

        assert_eq!(topic_response.name, "test");
        assert_eq!(topic_response.partitions.len(), 2);
        assert_eq!(topic_response.partitions[0].base_offset, 100);
        assert_eq!(
            topic_response.partitions[1].error_code,
            KafkaCode::NotLeaderForPartition
        );
    }

    #[test]
    fn test_produce_response_data_structure() {
        let response = ProduceResponseData {
            responses: vec![
                ProduceTopicResponse {
                    name: "topic-a".to_string(),
                    partitions: vec![],
                },
                ProduceTopicResponse {
                    name: "topic-b".to_string(),
                    partitions: vec![],
                },
            ],
            throttle_time_ms: 100,
        };

        assert_eq!(response.responses.len(), 2);
        assert_eq!(response.throttle_time_ms, 100);
    }

    // ========================================================================
    // CRC Validation Edge Cases
    // ========================================================================

    #[test]
    fn test_validate_batch_crc_empty() {
        use bytes::Bytes;
        let empty = Bytes::new();
        let result = validate_batch_crc(&empty);
        assert!(matches!(result, CrcValidationResult::TooSmall));
    }

    #[test]
    fn test_validate_batch_crc_garbage() {
        use bytes::Bytes;
        let garbage = Bytes::from_static(b"not a valid kafka batch");
        let result = validate_batch_crc(&garbage);
        // Should not be Valid (either Invalid or TooSmall depending on implementation)
        assert!(!matches!(result, CrcValidationResult::Valid));
    }

    #[test]
    fn test_validate_batch_crc_minimum_size() {
        use bytes::Bytes;
        // 16 bytes is less than the minimum for a valid batch header
        let too_small = Bytes::from(vec![0u8; 16]);
        let result = validate_batch_crc(&too_small);
        assert!(matches!(result, CrcValidationResult::TooSmall));
    }
}
