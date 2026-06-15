//! Handler trait for processing Kafka requests.
//!
//! Implement the `Handler` trait to provide custom logic for handling
//! each type of Kafka request.
//!
//! # Two ways to implement
//!
//! There are two patterns. New code should prefer `dispatch`.
//!
//! 1. **Single-method** (recommended): override [`Handler::dispatch`].
//!    One match on [`Request`], returning [`RequestResponse`]. Adding
//!    a new API key is one new match arm, nothing else.
//!
//! 2. **Per-API-key** (legacy): override individual `handle_*` methods
//!    (e.g. [`Handler::handle_produce`]). The default [`Handler::dispatch`]
//!    fans the request out to these methods. Useful when a handler only
//!    cares about a few APIs and wants the rest to return the protocol
//!    default.
//!
//! Mixing the two is fine: override `dispatch` for some APIs and let
//! per-method defaults take care of the rest.

use std::net::SocketAddr;
use std::sync::Arc;

use async_trait::async_trait;
use bytes::Bytes;

use crate::error::KafkaCode;

use super::request::*;
use super::response::*;
use super::versions;

/// Outcome of a SaslAuthenticate exchange that the dispatcher uses to
/// decide whether to open the connection's auth gate.
///
/// - `complete = true` means the handshake is finished and the gate
///   should flip. `principal` is the authenticated identity, if any.
/// - `complete = false` (with `error_code = None` on the wire) is an
///   intermediate SCRAM challenge — the handshake continues on the next
///   SaslAuthenticate request.
#[derive(Debug, Clone)]
pub struct SaslPostAuth {
    pub principal: Option<String>,
    pub complete: bool,
}

/// Context for a request, containing connection information.
#[derive(Debug, Clone)]
pub struct RequestContext {
    /// The client's address.
    pub client_addr: SocketAddr,
    /// Process-unique identifier for the connection that produced this
    /// request. Stable for the lifetime of the underlying socket and
    /// reset to a fresh value on every new connection — including TCP
    /// socket-address reuse — so server-side per-connection state
    /// (SASL post-auth, SCRAM challenge, etc.) cannot bleed across
    /// connections that happen to share a `client_addr`.
    pub conn_id: u64,
    /// The API version of the request.
    pub api_version: i16,
    /// The client ID from the request header.
    pub client_id: Option<String>,
    /// Unique request ID for correlation across logs and traces.
    pub request_id: uuid::Uuid,
    /// Authenticated principal in `User:<name>` form. Defaults to
    /// `User:ANONYMOUS` for connections that have not (yet) completed
    /// SaslAuthenticate. The cluster-handler authorizer keys ACL
    /// decisions against this value.
    pub principal: Arc<str>,
    /// Client host string used for ACL host matching. Mirrors the IP from
    /// `client_addr`; ACL bindings can wildcard with `*`.
    pub client_host: Arc<str>,
    /// True when the request arrived over a TLS-wrapped transport.
    pub transport_tls: bool,
}

impl RequestContext {
    /// Get the request ID as a string for logging.
    pub fn request_id(&self) -> &uuid::Uuid {
        &self.request_id
    }
}

/// Typed response that pairs every variant of [`Request`] with its
/// response data. The connection-layer encoder turns this into the
/// wire-level frame; handlers never touch the wire format.
#[derive(Debug)]
pub enum RequestResponse {
    Produce(ProduceResponseData),
    Fetch(FetchResponseData),
    ListOffsets(ListOffsetsResponseData),
    Metadata(MetadataResponseData),
    OffsetCommit(OffsetCommitResponseData),
    OffsetFetch(OffsetFetchResponseData),
    FindCoordinator(FindCoordinatorResponseData),
    JoinGroup(JoinGroupResponseData),
    Heartbeat(HeartbeatResponseData),
    LeaveGroup(LeaveGroupResponseData),
    SyncGroup(SyncGroupResponseData),
    DescribeGroups(DescribeGroupsResponseData),
    ListGroups(ListGroupsResponseData),
    SaslHandshake(SaslHandshakeResponseData),
    SaslAuthenticate(SaslAuthenticateResponseData),
    ApiVersions(ApiVersionsResponseData),
    CreateTopics(CreateTopicsResponseData),
    DeleteTopics(DeleteTopicsResponseData),
    InitProducerId(InitProducerIdResponseData),
    DeleteGroups(DeleteGroupsResponseData),
    /// Used for both `Request::UnsupportedVersion` and `Request::Unknown`.
    Error(ErrorResponseData),
}

/// Trait for handling Kafka protocol requests.
///
/// Implement this trait to provide custom behavior for your Kafka-compatible server.
/// Default implementations return appropriate error responses.
#[async_trait]
pub trait Handler: Send + Sync {
    /// Dispatch a parsed request to a typed response.
    ///
    /// This is the canonical single-method handler entry point. The
    /// default implementation matches on [`Request`] and forwards each
    /// variant to the corresponding `handle_*` method, so an
    /// implementer that prefers per-API-key methods doesn't need to
    /// override this. Implementers who'd rather have a single match
    /// override `dispatch` directly and let the per-API-key methods
    /// stay at their defaults.
    async fn dispatch(&self, ctx: &RequestContext, request: Request) -> RequestResponse {
        match request {
            Request::Produce(_, req) => {
                RequestResponse::Produce(self.handle_produce(ctx, req).await)
            }
            Request::Fetch(_, req) => RequestResponse::Fetch(self.handle_fetch(ctx, req).await),
            Request::ListOffsets(_, req) => {
                RequestResponse::ListOffsets(self.handle_list_offsets(ctx, req).await)
            }
            Request::Metadata(_, req) => {
                RequestResponse::Metadata(self.handle_metadata(ctx, req).await)
            }
            Request::OffsetCommit(_, req) => {
                RequestResponse::OffsetCommit(self.handle_offset_commit(ctx, req).await)
            }
            Request::OffsetFetch(_, req) => {
                RequestResponse::OffsetFetch(self.handle_offset_fetch(ctx, req).await)
            }
            Request::FindCoordinator(_, req) => {
                RequestResponse::FindCoordinator(self.handle_find_coordinator(ctx, req).await)
            }
            Request::JoinGroup(_, req) => {
                RequestResponse::JoinGroup(self.handle_join_group(ctx, req).await)
            }
            Request::Heartbeat(_, req) => {
                RequestResponse::Heartbeat(self.handle_heartbeat(ctx, req).await)
            }
            Request::LeaveGroup(_, req) => {
                RequestResponse::LeaveGroup(self.handle_leave_group(ctx, req).await)
            }
            Request::SyncGroup(_, req) => {
                RequestResponse::SyncGroup(self.handle_sync_group(ctx, req).await)
            }
            Request::DescribeGroups(_, req) => {
                RequestResponse::DescribeGroups(self.handle_describe_groups(ctx, req).await)
            }
            Request::ListGroups(_, req) => {
                RequestResponse::ListGroups(self.handle_list_groups(ctx, req).await)
            }
            Request::DeleteGroups(_, req) => {
                RequestResponse::DeleteGroups(self.handle_delete_groups(ctx, req).await)
            }
            Request::SaslHandshake(_, req) => {
                RequestResponse::SaslHandshake(self.handle_sasl_handshake(ctx, req).await)
            }
            Request::SaslAuthenticate(_, req) => {
                RequestResponse::SaslAuthenticate(self.handle_sasl_authenticate(ctx, req).await)
            }
            Request::ApiVersions(_, req) => {
                RequestResponse::ApiVersions(self.handle_api_versions(ctx, req).await)
            }
            Request::CreateTopics(_, req) => {
                RequestResponse::CreateTopics(self.handle_create_topics(ctx, req).await)
            }
            Request::DeleteTopics(_, req) => {
                RequestResponse::DeleteTopics(self.handle_delete_topics(ctx, req).await)
            }
            Request::InitProducerId(_, req) => {
                RequestResponse::InitProducerId(self.handle_init_producer_id(ctx, req).await)
            }
            Request::UnsupportedVersion(_) => RequestResponse::Error(ErrorResponseData {
                error_code: KafkaCode::UnsupportedVersion,
            }),
            Request::Unknown(h, body) => {
                RequestResponse::Error(self.handle_unknown(ctx, h.api_key, body).await)
            }
        }
    }

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

    /// Whether SASL must complete before this connection can issue any
    /// non-handshake API. Default `false` so existing handlers / tests are
    /// unchanged. The Kafkaesque cluster handler returns
    /// `ClusterConfig::sasl_required`.
    fn sasl_required(&self) -> bool {
        false
    }

    /// Read out the post-authenticate state for a connection, if any.
    /// Multi-step mechanisms — SCRAM-SHA-256 is the
    /// one we ship — return `error_code = None` for *intermediate*
    /// challenges as well as final successes; the dispatcher needs to
    /// know which so it doesn't open the auth gate after just the first
    /// round-trip. The cluster handler stashes a `(principal, complete)`
    /// record keyed by `conn_id` (a process-unique connection id, NOT
    /// `SocketAddr`, so TCP socket-address reuse can't cross-pollinate
    /// session state) while running `handle_sasl_authenticate`, and the
    /// dispatcher takes it back out here. Default `None` keeps PLAIN-only
    /// and test handlers unchanged.
    async fn take_sasl_post_auth(&self, _conn_id: u64) -> Option<SaslPostAuth> {
        None
    }

    /// Called when a client connection closes (clean or error). Handlers can
    /// drop per-connection auth state keyed by `conn_id`.
    async fn on_connection_closed(&self, _conn_id: u64) {}

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
