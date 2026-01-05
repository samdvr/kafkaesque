//! Sub-traits for the Handler trait.
//!
//! These sub-traits organize handler methods by functionality,
//! making it easier to understand which methods are required for
//! different use cases.

use async_trait::async_trait;

use super::request::*;
use super::response::*;
use super::versions;
use super::{Handler, RequestContext};

/// Handler for metadata and version discovery requests.
#[async_trait]
pub trait MetadataHandler: Send + Sync {
    /// Handle an ApiVersions request.
    async fn handle_api_versions(
        &self,
        _ctx: &RequestContext,
        _request: ApiVersionsRequestData,
    ) -> ApiVersionsResponseData {
        // Return a default set of supported API versions
        ApiVersionsResponseData {
            error_code: crate::error::KafkaCode::None,
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
}

/// Handler for produce requests.
#[async_trait]
pub trait ProduceHandler: Send + Sync {
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
                            error_code: crate::error::KafkaCode::UnknownTopicOrPartition,
                            base_offset: -1,
                            log_append_time: -1,
                        })
                        .collect(),
                })
                .collect(),
            throttle_time_ms: 0,
        }
    }
}

/// Handler for fetch and offset queries.
#[async_trait]
pub trait FetchHandler: Send + Sync {
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
                            error_code: crate::error::KafkaCode::UnknownTopicOrPartition,
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
                            error_code: crate::error::KafkaCode::UnknownTopicOrPartition,
                            timestamp: -1,
                            offset: -1,
                        })
                        .collect(),
                })
                .collect(),
        }
    }
}

/// Handler for consumer group operations.
#[async_trait]
pub trait ConsumerGroupHandler: Send + Sync {
    /// Handle a FindCoordinator request.
    async fn handle_find_coordinator(
        &self,
        _ctx: &RequestContext,
        _request: FindCoordinatorRequestData,
    ) -> FindCoordinatorResponseData {
        FindCoordinatorResponseData {
            throttle_time_ms: 0,
            error_code: crate::error::KafkaCode::GroupCoordinatorNotAvailable,
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
            error_code: crate::error::KafkaCode::GroupCoordinatorNotAvailable,
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
            error_code: crate::error::KafkaCode::None,
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
            error_code: crate::error::KafkaCode::None,
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
            error_code: crate::error::KafkaCode::GroupCoordinatorNotAvailable,
            assignment: bytes::Bytes::new(),
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
                            error_code: crate::error::KafkaCode::None,
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
                            error_code: crate::error::KafkaCode::None,
                        })
                        .collect(),
                })
                .collect(),
            error_code: crate::error::KafkaCode::None,
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
                    DescribedGroup::error(
                        group_id,
                        crate::error::KafkaCode::GroupCoordinatorNotAvailable,
                    )
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
            error_code: crate::error::KafkaCode::None,
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
                    DeleteGroupResult::error(
                        group_id,
                        crate::error::KafkaCode::GroupCoordinatorNotAvailable,
                    )
                })
                .collect(),
        }
    }
}

/// Handler for administrative operations (topic management).
#[async_trait]
pub trait AdminHandler: Send + Sync {
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
                    error_code: crate::error::KafkaCode::None,
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
                    error_code: crate::error::KafkaCode::None,
                })
                .collect(),
        }
    }
}

/// Handler for authentication operations.
#[async_trait]
pub trait AuthHandler: Send + Sync {
    /// Handle a SaslHandshake request.
    async fn handle_sasl_handshake(
        &self,
        _ctx: &RequestContext,
        _request: SaslHandshakeRequestData,
    ) -> SaslHandshakeResponseData {
        SaslHandshakeResponseData {
            error_code: crate::error::KafkaCode::UnsupportedSaslMechanism,
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
            error_code: crate::error::KafkaCode::SaslAuthenticationFailed,
            error_message: Some("SASL not configured".to_string()),
            auth_bytes: bytes::Bytes::new(),
        }
    }
}

/// Handler for producer ID initialization (idempotent/transactional producers).
#[async_trait]
pub trait ProducerHandler: Send + Sync {
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
            error_code: crate::error::KafkaCode::None,
            producer_id: 1,
            producer_epoch: 0,
        }
    }
}

// Blanket implementations to compose sub-traits into the main Handler trait
#[async_trait]
impl<T> Handler for T
where
    T: MetadataHandler
        + ProduceHandler
        + FetchHandler
        + ConsumerGroupHandler
        + AdminHandler
        + AuthHandler
        + ProducerHandler,
{
    async fn handle_api_versions(
        &self,
        ctx: &RequestContext,
        request: ApiVersionsRequestData,
    ) -> ApiVersionsResponseData {
        MetadataHandler::handle_api_versions(self, ctx, request).await
    }

    async fn handle_metadata(
        &self,
        ctx: &RequestContext,
        request: MetadataRequestData,
    ) -> MetadataResponseData {
        MetadataHandler::handle_metadata(self, ctx, request).await
    }

    async fn handle_produce(
        &self,
        ctx: &RequestContext,
        request: ProduceRequestData,
    ) -> ProduceResponseData {
        ProduceHandler::handle_produce(self, ctx, request).await
    }

    async fn handle_fetch(
        &self,
        ctx: &RequestContext,
        request: FetchRequestData,
    ) -> FetchResponseData {
        FetchHandler::handle_fetch(self, ctx, request).await
    }

    async fn handle_list_offsets(
        &self,
        ctx: &RequestContext,
        request: ListOffsetsRequestData,
    ) -> ListOffsetsResponseData {
        FetchHandler::handle_list_offsets(self, ctx, request).await
    }

    async fn handle_find_coordinator(
        &self,
        ctx: &RequestContext,
        request: FindCoordinatorRequestData,
    ) -> FindCoordinatorResponseData {
        ConsumerGroupHandler::handle_find_coordinator(self, ctx, request).await
    }

    async fn handle_join_group(
        &self,
        ctx: &RequestContext,
        request: JoinGroupRequestData,
    ) -> JoinGroupResponseData {
        ConsumerGroupHandler::handle_join_group(self, ctx, request).await
    }

    async fn handle_heartbeat(
        &self,
        ctx: &RequestContext,
        request: HeartbeatRequestData,
    ) -> HeartbeatResponseData {
        ConsumerGroupHandler::handle_heartbeat(self, ctx, request).await
    }

    async fn handle_leave_group(
        &self,
        ctx: &RequestContext,
        request: LeaveGroupRequestData,
    ) -> LeaveGroupResponseData {
        ConsumerGroupHandler::handle_leave_group(self, ctx, request).await
    }

    async fn handle_sync_group(
        &self,
        ctx: &RequestContext,
        request: SyncGroupRequestData,
    ) -> SyncGroupResponseData {
        ConsumerGroupHandler::handle_sync_group(self, ctx, request).await
    }

    async fn handle_offset_commit(
        &self,
        ctx: &RequestContext,
        request: OffsetCommitRequestData,
    ) -> OffsetCommitResponseData {
        ConsumerGroupHandler::handle_offset_commit(self, ctx, request).await
    }

    async fn handle_offset_fetch(
        &self,
        ctx: &RequestContext,
        request: OffsetFetchRequestData,
    ) -> OffsetFetchResponseData {
        ConsumerGroupHandler::handle_offset_fetch(self, ctx, request).await
    }

    async fn handle_describe_groups(
        &self,
        ctx: &RequestContext,
        request: DescribeGroupsRequestData,
    ) -> DescribeGroupsResponseData {
        ConsumerGroupHandler::handle_describe_groups(self, ctx, request).await
    }

    async fn handle_list_groups(
        &self,
        ctx: &RequestContext,
        request: ListGroupsRequestData,
    ) -> ListGroupsResponseData {
        ConsumerGroupHandler::handle_list_groups(self, ctx, request).await
    }

    async fn handle_delete_groups(
        &self,
        ctx: &RequestContext,
        request: DeleteGroupsRequestData,
    ) -> DeleteGroupsResponseData {
        ConsumerGroupHandler::handle_delete_groups(self, ctx, request).await
    }

    async fn handle_sasl_handshake(
        &self,
        ctx: &RequestContext,
        request: SaslHandshakeRequestData,
    ) -> SaslHandshakeResponseData {
        AuthHandler::handle_sasl_handshake(self, ctx, request).await
    }

    async fn handle_sasl_authenticate(
        &self,
        ctx: &RequestContext,
        request: SaslAuthenticateRequestData,
    ) -> SaslAuthenticateResponseData {
        AuthHandler::handle_sasl_authenticate(self, ctx, request).await
    }

    async fn handle_create_topics(
        &self,
        ctx: &RequestContext,
        request: CreateTopicsRequestData,
    ) -> CreateTopicsResponseData {
        AdminHandler::handle_create_topics(self, ctx, request).await
    }

    async fn handle_delete_topics(
        &self,
        ctx: &RequestContext,
        request: DeleteTopicsRequestData,
    ) -> DeleteTopicsResponseData {
        AdminHandler::handle_delete_topics(self, ctx, request).await
    }

    async fn handle_init_producer_id(
        &self,
        ctx: &RequestContext,
        request: InitProducerIdRequestData,
    ) -> InitProducerIdResponseData {
        ProducerHandler::handle_init_producer_id(self, ctx, request).await
    }

    async fn handle_unknown(
        &self,
        _ctx: &RequestContext,
        _api_key: crate::server::request::ApiKey,
        _data: bytes::Bytes,
    ) -> ErrorResponseData {
        ErrorResponseData {
            error_code: crate::error::KafkaCode::UnsupportedVersion,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::KafkaCode;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    /// A minimal test handler that uses all default implementations.
    struct TestHandler;

    #[async_trait]
    impl MetadataHandler for TestHandler {}

    #[async_trait]
    impl ProduceHandler for TestHandler {}

    #[async_trait]
    impl FetchHandler for TestHandler {}

    #[async_trait]
    impl ConsumerGroupHandler for TestHandler {}

    #[async_trait]
    impl AdminHandler for TestHandler {}

    #[async_trait]
    impl AuthHandler for TestHandler {}

    #[async_trait]
    impl ProducerHandler for TestHandler {}

    fn test_ctx() -> RequestContext {
        RequestContext {
            client_id: Some("test-client".to_string()),
            request_id: uuid::Uuid::new_v4(),
            api_version: 0,
            client_addr: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)), 12345),
        }
    }

    #[tokio::test]
    async fn test_metadata_handler_api_versions_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = ApiVersionsRequestData {
            client_software_name: Some("test".to_string()),
            client_software_version: Some("1.0".to_string()),
        };

        let response = MetadataHandler::handle_api_versions(&handler, &ctx, request).await;
        assert_eq!(response.error_code, KafkaCode::None);
        assert!(!response.api_keys.is_empty());
        assert_eq!(response.throttle_time_ms, 0);
    }

    #[tokio::test]
    async fn test_metadata_handler_metadata_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = MetadataRequestData { topics: None };

        let response = MetadataHandler::handle_metadata(&handler, &ctx, request).await;
        assert!(response.brokers.is_empty());
        assert_eq!(response.controller_id, -1);
        assert!(response.topics.is_empty());
    }

    #[tokio::test]
    async fn test_produce_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = ProduceRequestData {
            transactional_id: None,
            acks: -1,
            timeout_ms: 1000,
            topics: vec![ProduceTopicData {
                name: "test-topic".to_string(),
                partitions: vec![ProducePartitionData {
                    partition_index: 0,
                    records: bytes::Bytes::new(),
                }],
            }],
        };

        let response = ProduceHandler::handle_produce(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.responses.len(), 1);
        assert_eq!(response.responses[0].name, "test-topic");
        assert_eq!(response.responses[0].partitions[0].partition_index, 0);
        assert_eq!(
            response.responses[0].partitions[0].error_code,
            KafkaCode::UnknownTopicOrPartition
        );
        assert_eq!(response.responses[0].partitions[0].base_offset, -1);
    }

    #[tokio::test]
    async fn test_fetch_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = FetchRequestData {
            replica_id: -1,
            max_wait_ms: 1000,
            min_bytes: 1,
            max_bytes: 1024,
            isolation_level: 0,
            topics: vec![FetchTopicData {
                name: "test-topic".to_string(),
                partitions: vec![FetchPartitionData {
                    partition_index: 0,
                    fetch_offset: 0,
                    partition_max_bytes: 1024,
                }],
            }],
        };

        let response = FetchHandler::handle_fetch(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.responses.len(), 1);
        assert_eq!(
            response.responses[0].partitions[0].error_code,
            KafkaCode::UnknownTopicOrPartition
        );
        assert_eq!(response.responses[0].partitions[0].high_watermark, -1);
    }

    #[tokio::test]
    async fn test_list_offsets_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = ListOffsetsRequestData {
            replica_id: -1,
            isolation_level: 0,
            topics: vec![ListOffsetsTopicData {
                name: "test-topic".to_string(),
                partitions: vec![ListOffsetsPartitionData {
                    partition_index: 0,
                    timestamp: -1,
                }],
            }],
        };

        let response = FetchHandler::handle_list_offsets(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(
            response.topics[0].partitions[0].error_code,
            KafkaCode::UnknownTopicOrPartition
        );
    }

    #[tokio::test]
    async fn test_find_coordinator_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = FindCoordinatorRequestData {
            key: "test-group".to_string(),
            key_type: 0,
        };

        let response = ConsumerGroupHandler::handle_find_coordinator(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
        assert_eq!(response.node_id, -1);
    }

    #[tokio::test]
    async fn test_join_group_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = JoinGroupRequestData {
            group_id: "test-group".to_string(),
            session_timeout_ms: 30000,
            rebalance_timeout_ms: 60000,
            member_id: "".to_string(),
            protocol_type: "consumer".to_string(),
            protocols: vec![],
        };

        let response = ConsumerGroupHandler::handle_join_group(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
        assert_eq!(response.generation_id, -1);
    }

    #[tokio::test]
    async fn test_heartbeat_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = HeartbeatRequestData {
            group_id: "test-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
        };

        let response = ConsumerGroupHandler::handle_heartbeat(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[tokio::test]
    async fn test_leave_group_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = LeaveGroupRequestData {
            group_id: "test-group".to_string(),
            member_id: "member-1".to_string(),
        };

        let response = ConsumerGroupHandler::handle_leave_group(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, KafkaCode::None);
    }

    #[tokio::test]
    async fn test_sync_group_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = SyncGroupRequestData {
            group_id: "test-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            assignments: vec![],
        };

        let response = ConsumerGroupHandler::handle_sync_group(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, KafkaCode::GroupCoordinatorNotAvailable);
    }

    #[tokio::test]
    async fn test_offset_commit_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = OffsetCommitRequestData {
            group_id: "test-group".to_string(),
            generation_id: 1,
            member_id: "member-1".to_string(),
            topics: vec![OffsetCommitTopicData {
                name: "test-topic".to_string(),
                partitions: vec![OffsetCommitPartitionData {
                    partition_index: 0,
                    committed_offset: 100,
                    committed_metadata: None,
                }],
            }],
        };

        let response = ConsumerGroupHandler::handle_offset_commit(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].partitions[0].error_code, KafkaCode::None);
    }

    #[tokio::test]
    async fn test_offset_fetch_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = OffsetFetchRequestData {
            group_id: "test-group".to_string(),
            topics: vec![OffsetFetchTopicData {
                name: "test-topic".to_string(),
                partition_indexes: vec![0],
            }],
        };

        let response = ConsumerGroupHandler::handle_offset_fetch(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].partitions[0].committed_offset, -1);
    }

    #[tokio::test]
    async fn test_describe_groups_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = DescribeGroupsRequestData {
            group_ids: vec!["test-group".to_string()],
        };

        let response = ConsumerGroupHandler::handle_describe_groups(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.groups.len(), 1);
        assert_eq!(
            response.groups[0].error_code,
            KafkaCode::GroupCoordinatorNotAvailable
        );
    }

    #[tokio::test]
    async fn test_list_groups_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = ListGroupsRequestData {
            states_filter: vec![],
        };

        let response = ConsumerGroupHandler::handle_list_groups(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, KafkaCode::None);
        assert!(response.groups.is_empty());
    }

    #[tokio::test]
    async fn test_delete_groups_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = DeleteGroupsRequestData {
            group_ids: vec!["test-group".to_string()],
        };

        let response = ConsumerGroupHandler::handle_delete_groups(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.results.len(), 1);
        assert_eq!(
            response.results[0].error_code,
            KafkaCode::GroupCoordinatorNotAvailable
        );
    }

    #[tokio::test]
    async fn test_create_topics_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = CreateTopicsRequestData {
            topics: vec![CreateTopicData {
                name: "new-topic".to_string(),
                num_partitions: 3,
                replication_factor: 1,
            }],
            timeout_ms: 30000,
        };

        let response = AdminHandler::handle_create_topics(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.topics.len(), 1);
        assert_eq!(response.topics[0].name, "new-topic");
        assert_eq!(response.topics[0].error_code, KafkaCode::None);
    }

    #[tokio::test]
    async fn test_delete_topics_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = DeleteTopicsRequestData {
            topic_names: vec!["old-topic".to_string()],
            timeout_ms: 30000,
        };

        let response = AdminHandler::handle_delete_topics(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.responses.len(), 1);
        assert_eq!(response.responses[0].name, "old-topic");
        assert_eq!(response.responses[0].error_code, KafkaCode::None);
    }

    #[tokio::test]
    async fn test_sasl_handshake_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = SaslHandshakeRequestData {
            mechanism: "PLAIN".to_string(),
        };

        let response = AuthHandler::handle_sasl_handshake(&handler, &ctx, request).await;
        assert_eq!(response.error_code, KafkaCode::UnsupportedSaslMechanism);
        assert!(response.mechanisms.is_empty());
    }

    #[tokio::test]
    async fn test_sasl_authenticate_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = SaslAuthenticateRequestData {
            auth_bytes: bytes::Bytes::new(),
        };

        let response = AuthHandler::handle_sasl_authenticate(&handler, &ctx, request).await;
        assert_eq!(response.error_code, KafkaCode::SaslAuthenticationFailed);
        assert!(response.error_message.is_some());
    }

    #[tokio::test]
    async fn test_init_producer_id_handler_default() {
        let handler = TestHandler;
        let ctx = test_ctx();
        let request = InitProducerIdRequestData {
            transactional_id: None,
            transaction_timeout_ms: 60000,
            producer_id: -1,
            producer_epoch: -1,
        };

        let response = ProducerHandler::handle_init_producer_id(&handler, &ctx, request).await;
        assert_eq!(response.throttle_time_ms, 0);
        assert_eq!(response.error_code, KafkaCode::None);
        assert_eq!(response.producer_id, 1);
        assert_eq!(response.producer_epoch, 0);
    }

    #[tokio::test]
    async fn test_blanket_handler_implementation() {
        // Test that the blanket impl works correctly
        let handler = TestHandler;
        let ctx = test_ctx();

        // Via Handler trait (blanket impl)
        let request = ApiVersionsRequestData {
            client_software_name: Some("test".to_string()),
            client_software_version: Some("1.0".to_string()),
        };
        let response = Handler::handle_api_versions(&handler, &ctx, request).await;
        assert_eq!(response.error_code, KafkaCode::None);

        // Test handle_unknown
        let api_key = crate::server::request::ApiKey::Produce;
        let response = Handler::handle_unknown(&handler, &ctx, api_key, bytes::Bytes::new()).await;
        assert_eq!(response.error_code, KafkaCode::UnsupportedVersion);
    }
}
