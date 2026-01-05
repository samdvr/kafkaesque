//! Handler trait for processing Kafka requests.
//!
//! Implement the `Handler` trait to provide custom logic for handling
//! each type of Kafka request.
//!
//! The Handler trait is composed of several sub-traits organized by functionality:
//! - `MetadataHandler`: API versions and metadata
//! - `ProduceHandler`: Produce requests
//! - `FetchHandler`: Fetch and list offsets
//! - `ConsumerGroupHandler`: Consumer group operations
//! - `AdminHandler`: Topic management
//! - `AuthHandler`: SASL authentication
//! - `ProducerHandler`: Producer ID initialization
//!
//! See `handler_traits` module for the sub-trait definitions.

use std::net::SocketAddr;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::KafkaCode;

use super::request::*;
use super::response::*;
use super::versions;

/// Context for a request, containing connection information.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// The client's address.
    pub client_addr: SocketAddr,
    /// The API version of the request.
    pub api_version: i16,
    /// The client ID from the request header.
    pub client_id: Option<String>,
    /// Unique request ID for correlation across logs and traces.
    pub request_id: uuid::Uuid,
}

impl RequestContext {
    /// Get the request ID as a string for logging.
    pub fn request_id(&self) -> &uuid::Uuid {
        &self.request_id
    }
}

/// Trait for handling Kafka protocol requests.
///
/// Implement this trait to provide custom behavior for your Kafka-compatible server.
/// Default implementations return appropriate error responses.
///
/// This trait is composed of several sub-traits organized by functionality.
/// For better organization, consider implementing the sub-traits:
/// - `MetadataHandler` for API versions and metadata
/// - `ProduceHandler` for produce requests
/// - `FetchHandler` for fetch and list offsets
/// - `ConsumerGroupHandler` for consumer group operations
/// - `AdminHandler` for topic management
/// - `AuthHandler` for SASL authentication
/// - `ProducerHandler` for producer ID initialization
///
/// Types that implement all sub-traits automatically implement `Handler` via blanket implementation.
#[async_trait]
pub trait Handler: Send + Sync {
    /// Handle an ApiVersions request.
    /// This is typically the first request a client makes to discover supported API versions.
    async fn handle_api_versions(
        &self,
        _ctx: &RequestContext,
        _request: ApiVersionsRequestData,
    ) -> ApiVersionsResponseData {
        // Return a default set of supported API versions
        ApiVersionsResponseData {
            error_code: KafkaCode::None,
            api_keys: versions::default_api_versions(),
            throttle_time_ms: 0,
        }
    }

    /// Handle a Metadata request.
    async fn handle_metadata(
        &self,
        _ctx: &RequestContext,
        _request: MetadataRequestData,
    ) -> MetadataResponseData {
        // Default: return empty metadata
        MetadataResponseData {
            brokers: vec![],
            controller_id: -1,
            topics: vec![],
        }
    }

    /// Handle a Produce request.
    async fn handle_produce(
        &self,
        _ctx: &RequestContext,
        request: ProduceRequestData,
    ) -> ProduceResponseData {
        // Default: return error for all partitions
        ProduceResponseData {
            responses: request
                .topics
                .into_iter()
                .map(|topic| ProduceTopicResponse {
                    name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|p| ProducePartitionResponse {
                            partition_index: p.partition_index,
                            error_code: KafkaCode::UnknownTopicOrPartition,
                            base_offset: -1,
                            log_append_time: -1,
                        })
                        .collect(),
                })
                .collect(),
            throttle_time_ms: 0,
        }
    }

    /// Handle a Fetch request.
    async fn handle_fetch(
        &self,
        _ctx: &RequestContext,
        request: FetchRequestData,
    ) -> FetchResponseData {
        // Default: return error for all partitions
        FetchResponseData {
            throttle_time_ms: 0,
            responses: request
                .topics
                .into_iter()
                .map(|topic| FetchTopicResponse {
                    name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|p| FetchPartitionResponse {
                            partition_index: p.partition_index,
                            error_code: KafkaCode::UnknownTopicOrPartition,
                            high_watermark: -1,
                            last_stable_offset: -1,
                            aborted_transactions: vec![],
                            records: None,
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    /// Handle a ListOffsets request.
    async fn handle_list_offsets(
        &self,
        _ctx: &RequestContext,
        request: ListOffsetsRequestData,
    ) -> ListOffsetsResponseData {
        // Default: return error for all partitions
        ListOffsetsResponseData {
            throttle_time_ms: 0,
            topics: request
                .topics
                .into_iter()
                .map(|topic| ListOffsetsTopicResponse {
                    name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|p| ListOffsetsPartitionResponse {
                            partition_index: p.partition_index,
                            error_code: KafkaCode::UnknownTopicOrPartition,
                            timestamp: -1,
                            offset: -1,
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    /// Handle an OffsetCommit request.
    async fn handle_offset_commit(
        &self,
        _ctx: &RequestContext,
        request: OffsetCommitRequestData,
    ) -> OffsetCommitResponseData {
        OffsetCommitResponseData {
            throttle_time_ms: 0,
            topics: request
                .topics
                .into_iter()
                .map(|topic| OffsetCommitTopicResponse {
                    name: topic.name,
                    partitions: topic
                        .partitions
                        .into_iter()
                        .map(|p| OffsetCommitPartitionResponse {
                            partition_index: p.partition_index,
                            error_code: KafkaCode::None,
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    /// Handle an OffsetFetch request.
    async fn handle_offset_fetch(
        &self,
        _ctx: &RequestContext,
        request: OffsetFetchRequestData,
    ) -> OffsetFetchResponseData {
        OffsetFetchResponseData {
            throttle_time_ms: 0,
            topics: request
                .topics
                .into_iter()
                .map(|topic| OffsetFetchTopicResponse {
                    name: topic.name,
                    partitions: topic
                        .partition_indexes
                        .into_iter()
                        .map(|idx| OffsetFetchPartitionResponse {
                            partition_index: idx,
                            committed_offset: -1,
                            metadata: None,
                            error_code: KafkaCode::None,
                        })
                        .collect(),
                })
                .collect(),
            error_code: KafkaCode::None,
        }
    }

    /// Handle a FindCoordinator request.
    async fn handle_find_coordinator(
        &self,
        _ctx: &RequestContext,
        _request: FindCoordinatorRequestData,
    ) -> FindCoordinatorResponseData {
        FindCoordinatorResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::GroupCoordinatorNotAvailable,
            error_message: None,
            node_id: -1,
            host: String::new(),
            port: 0,
        }
    }

    /// Handle a JoinGroup request.
    async fn handle_join_group(
        &self,
        _ctx: &RequestContext,
        _request: JoinGroupRequestData,
    ) -> JoinGroupResponseData {
        JoinGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::GroupCoordinatorNotAvailable,
            generation_id: -1,
            protocol_name: String::new(),
            leader: String::new(),
            member_id: String::new(),
            members: vec![],
        }
    }

    /// Handle a Heartbeat request.
    async fn handle_heartbeat(
        &self,
        _ctx: &RequestContext,
        _request: HeartbeatRequestData,
    ) -> HeartbeatResponseData {
        HeartbeatResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
        }
    }

    /// Handle a LeaveGroup request.
    async fn handle_leave_group(
        &self,
        _ctx: &RequestContext,
        _request: LeaveGroupRequestData,
    ) -> LeaveGroupResponseData {
        LeaveGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
        }
    }

    /// Handle a SyncGroup request.
    async fn handle_sync_group(
        &self,
        _ctx: &RequestContext,
        _request: SyncGroupRequestData,
    ) -> SyncGroupResponseData {
        SyncGroupResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::GroupCoordinatorNotAvailable,
            assignment: Bytes::new(),
        }
    }

    /// Handle a DescribeGroups request.
    async fn handle_describe_groups(
        &self,
        _ctx: &RequestContext,
        request: DescribeGroupsRequestData,
    ) -> DescribeGroupsResponseData {
        // Default: return error for all requested groups
        DescribeGroupsResponseData {
            throttle_time_ms: 0,
            groups: request
                .group_ids
                .into_iter()
                .map(|group_id| {
                    DescribedGroup::error(group_id, KafkaCode::GroupCoordinatorNotAvailable)
                })
                .collect(),
        }
    }

    /// Handle a ListGroups request.
    async fn handle_list_groups(
        &self,
        _ctx: &RequestContext,
        _request: ListGroupsRequestData,
    ) -> ListGroupsResponseData {
        // Default: return empty list
        ListGroupsResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            groups: vec![],
        }
    }

    /// Handle a DeleteGroups request.
    async fn handle_delete_groups(
        &self,
        _ctx: &RequestContext,
        request: DeleteGroupsRequestData,
    ) -> DeleteGroupsResponseData {
        // Default: return error for all requested groups
        DeleteGroupsResponseData {
            throttle_time_ms: 0,
            results: request
                .group_ids
                .into_iter()
                .map(|group_id| {
                    DeleteGroupResult::error(group_id, KafkaCode::GroupCoordinatorNotAvailable)
                })
                .collect(),
        }
    }

    /// Handle a SaslHandshake request.
    async fn handle_sasl_handshake(
        &self,
        _ctx: &RequestContext,
        _request: SaslHandshakeRequestData,
    ) -> SaslHandshakeResponseData {
        SaslHandshakeResponseData {
            error_code: KafkaCode::UnsupportedSaslMechanism,
            mechanisms: vec![],
        }
    }

    /// Handle a SaslAuthenticate request.
    async fn handle_sasl_authenticate(
        &self,
        _ctx: &RequestContext,
        _request: SaslAuthenticateRequestData,
    ) -> SaslAuthenticateResponseData {
        SaslAuthenticateResponseData {
            error_code: KafkaCode::SaslAuthenticationFailed,
            error_message: Some("SASL not configured".to_string()),
            auth_bytes: Bytes::new(),
        }
    }

    /// Handle a CreateTopics request.
    async fn handle_create_topics(
        &self,
        _ctx: &RequestContext,
        request: CreateTopicsRequestData,
    ) -> CreateTopicsResponseData {
        CreateTopicsResponseData {
            throttle_time_ms: 0,
            topics: request
                .topics
                .into_iter()
                .map(|t| CreateTopicResponseData {
                    name: t.name,
                    error_code: KafkaCode::None,
                    error_message: None,
                })
                .collect(),
        }
    }

    /// Handle a DeleteTopics request.
    async fn handle_delete_topics(
        &self,
        _ctx: &RequestContext,
        request: DeleteTopicsRequestData,
    ) -> DeleteTopicsResponseData {
        DeleteTopicsResponseData {
            throttle_time_ms: 0,
            responses: request
                .topic_names
                .into_iter()
                .map(|name| DeleteTopicResponseData {
                    name,
                    error_code: KafkaCode::None,
                })
                .collect(),
        }
    }

    /// Handle an InitProducerId request.
    /// This is used by idempotent/transactional producers to get a producer ID.
    async fn handle_init_producer_id(
        &self,
        _ctx: &RequestContext,
        _request: InitProducerIdRequestData,
    ) -> InitProducerIdResponseData {
        // Default implementation assigns a random producer ID
        InitProducerIdResponseData {
            throttle_time_ms: 0,
            error_code: KafkaCode::None,
            producer_id: 1,
            producer_epoch: 0,
        }
    }

    /// Handle an unknown or unsupported request.
    async fn handle_unknown(
        &self,
        _ctx: &RequestContext,
        _api_key: ApiKey,
        _data: Bytes,
    ) -> ErrorResponseData {
        ErrorResponseData {
            error_code: KafkaCode::UnsupportedVersion,
        }
    }
}
