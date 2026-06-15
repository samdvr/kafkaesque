//! Inline tests previously embedded in `network.rs`. Moved to a sibling
//! file so the implementation isn't gated on scrolling past the test
//! block. `super::*` re-exports private helpers the tests rely on.

#![allow(clippy::module_inception)]
#![allow(unused_imports)]

use super::*;

use super::*;

#[test]
fn test_raft_network_factory_new() {
    let factory = RaftNetworkFactoryImpl::new();
    // Factory should be created with empty nodes
    let nodes = factory.nodes.try_read().unwrap();
    assert!(nodes.is_empty());
}

#[test]
fn test_raft_network_factory_default() {
    let factory = RaftNetworkFactoryImpl::default();
    // Default should create empty factory
    let nodes = factory.nodes.try_read().unwrap();
    assert!(nodes.is_empty());
}

#[test]
fn test_raft_network_factory_clone() {
    let factory = RaftNetworkFactoryImpl::new();
    let cloned = factory.clone();
    // Clone should share the same Arc
    assert!(Arc::ptr_eq(&factory.nodes, &cloned.nodes));
}

#[tokio::test]
async fn test_raft_network_factory_add_node() {
    let factory = RaftNetworkFactoryImpl::new();

    factory.add_node(1, "127.0.0.1:9093".to_string()).await;
    factory.add_node(2, "127.0.0.1:9094".to_string()).await;

    let nodes = factory.nodes.read().await;
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes.get(&1), Some(&"127.0.0.1:9093".to_string()));
    assert_eq!(nodes.get(&2), Some(&"127.0.0.1:9094".to_string()));
}

#[tokio::test]
async fn test_raft_network_factory_update_node() {
    let factory = RaftNetworkFactoryImpl::new();

    // Add a node
    factory.add_node(1, "127.0.0.1:9093".to_string()).await;

    // Update the same node
    factory.add_node(1, "192.168.1.1:9093".to_string()).await;

    let nodes = factory.nodes.read().await;
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes.get(&1), Some(&"192.168.1.1:9093".to_string()));
}

#[test]
fn test_raft_rpc_message_serialization() {
    // Test that messages can be serialized and deserialized
    let vote_msg = RaftRpcMessage::Vote(openraft::raft::VoteRequest {
        vote: openraft::Vote::new(1, 42),
        last_log_id: None,
    });

    let serialized = postcard::to_stdvec(&vote_msg).unwrap();
    let deserialized: RaftRpcMessage = postcard::from_bytes(&serialized).unwrap();

    match deserialized {
        RaftRpcMessage::Vote(req) => {
            assert_eq!(req.vote.leader_id().voted_for(), Some(42));
        }
        _ => panic!("Expected Vote message"),
    }
}

#[test]
fn test_raft_rpc_response_serialization() {
    // Test Error response serialization
    let error_resp = RaftRpcResponse::Error("test error".to_string());
    let serialized = postcard::to_stdvec(&error_resp).unwrap();
    let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

    match deserialized {
        RaftRpcResponse::Error(msg) => {
            assert_eq!(msg, "test error");
        }
        _ => panic!("Expected Error response"),
    }
}

#[test]
fn test_raft_rpc_message_debug() {
    let vote_msg = RaftRpcMessage::Vote(openraft::raft::VoteRequest {
        vote: openraft::Vote::new(1, 1),
        last_log_id: None,
    });

    let debug_str = format!("{:?}", vote_msg);
    assert!(debug_str.contains("Vote"));
}

#[test]
fn test_raft_rpc_response_debug() {
    let error_resp = RaftRpcResponse::Error("debug test".to_string());
    let debug_str = format!("{:?}", error_resp);
    assert!(debug_str.contains("Error"));
    assert!(debug_str.contains("debug test"));
}

#[tokio::test]
async fn test_new_client_stores_node() {
    use openraft::BasicNode;
    use openraft::network::RaftNetworkFactory;

    let mut factory = RaftNetworkFactoryImpl::new();
    let node = BasicNode::new("127.0.0.1:9999");

    let _connection = factory.new_client(42, &node).await;

    // Verify node was stored
    let nodes = factory.nodes.read().await;
    assert_eq!(nodes.get(&42), Some(&"127.0.0.1:9999".to_string()));
}

#[test]
fn test_raft_rpc_message_clone() {
    let msg = RaftRpcMessage::Vote(openraft::raft::VoteRequest {
        vote: openraft::Vote::new(1, 1),
        last_log_id: None,
    });

    let cloned = msg.clone();
    match (msg, cloned) {
        (RaftRpcMessage::Vote(orig), RaftRpcMessage::Vote(clone)) => {
            assert_eq!(
                orig.vote.leader_id().voted_for(),
                clone.vote.leader_id().voted_for()
            );
        }
        _ => panic!("Clone should match"),
    }
}

#[test]
fn test_vote_response_serialization() {
    use openraft::Vote;
    use openraft::raft::VoteResponse;

    let vote_resp = RaftRpcResponse::Vote(VoteResponse {
        vote: Vote::new(1, 1),
        vote_granted: true,
        last_log_id: None,
    });

    let serialized = postcard::to_stdvec(&vote_resp).unwrap();
    let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

    match deserialized {
        RaftRpcResponse::Vote(resp) => {
            assert!(resp.vote_granted);
        }
        _ => panic!("Expected Vote response"),
    }
}

#[test]
fn test_append_entries_response_serialization() {
    use openraft::raft::AppendEntriesResponse;

    // AppendEntriesResponse is an enum in openraft 0.9
    let append_resp = RaftRpcResponse::AppendEntries(AppendEntriesResponse::Success);

    let serialized = postcard::to_stdvec(&append_resp).unwrap();
    let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

    match deserialized {
        RaftRpcResponse::AppendEntries(AppendEntriesResponse::Success) => {
            // Success - AppendEntries response serialized and deserialized correctly
        }
        _ => panic!("Expected AppendEntries Success response"),
    }
}

#[test]
fn test_install_snapshot_response_serialization() {
    use openraft::raft::InstallSnapshotResponse;

    let snapshot_resp = RaftRpcResponse::InstallSnapshot(InstallSnapshotResponse {
        vote: openraft::Vote::new(1, 1),
    });

    let serialized = postcard::to_stdvec(&snapshot_resp).unwrap();
    let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

    match deserialized {
        RaftRpcResponse::InstallSnapshot(resp) => {
            assert_eq!(resp.vote.leader_id().voted_for(), Some(1));
        }
        _ => panic!("Expected InstallSnapshot response"),
    }
}

#[tokio::test]
async fn test_multiple_nodes_ordering() {
    let factory = RaftNetworkFactoryImpl::new();

    // Add nodes in various order
    factory.add_node(5, "host5:9093".to_string()).await;
    factory.add_node(1, "host1:9093".to_string()).await;
    factory.add_node(3, "host3:9093".to_string()).await;

    let nodes = factory.nodes.read().await;

    // BTreeMap maintains sorted order
    let keys: Vec<_> = nodes.keys().collect();
    assert_eq!(keys, vec![&1, &3, &5]);
}

// ========================================================================
// RpcErrorKind Tests
// ========================================================================

#[test]
fn test_rpc_error_kind_is_retryable() {
    // Retryable errors
    assert!(RpcErrorKind::LeadershipChanged.is_retryable());
    assert!(RpcErrorKind::NotLeader { leader_hint: None }.is_retryable());
    assert!(
        RpcErrorKind::NotLeader {
            leader_hint: Some(42)
        }
        .is_retryable()
    );
    assert!(RpcErrorKind::Internal.is_retryable());
    assert!(RpcErrorKind::Timeout.is_retryable());
    assert!(RpcErrorKind::Network.is_retryable());

    // Non-retryable errors
    assert!(!RpcErrorKind::ForwardLoopDetected.is_retryable());
    assert!(!RpcErrorKind::InvalidRequest.is_retryable());
}

#[test]
fn test_rpc_error_kind_should_refresh_leader() {
    // Should refresh leader
    assert!(RpcErrorKind::LeadershipChanged.should_refresh_leader());
    assert!(RpcErrorKind::NotLeader { leader_hint: None }.should_refresh_leader());
    assert!(
        RpcErrorKind::NotLeader {
            leader_hint: Some(1)
        }
        .should_refresh_leader()
    );

    // Should not refresh leader
    assert!(!RpcErrorKind::ForwardLoopDetected.should_refresh_leader());
    assert!(!RpcErrorKind::InvalidRequest.should_refresh_leader());
    assert!(!RpcErrorKind::Internal.should_refresh_leader());
    assert!(!RpcErrorKind::Timeout.should_refresh_leader());
    assert!(!RpcErrorKind::Network.should_refresh_leader());
}

#[test]
fn test_rpc_error_kind_serialization() {
    let kinds = [
        RpcErrorKind::LeadershipChanged,
        RpcErrorKind::NotLeader { leader_hint: None },
        RpcErrorKind::NotLeader {
            leader_hint: Some(42),
        },
        RpcErrorKind::ForwardLoopDetected,
        RpcErrorKind::InvalidRequest,
        RpcErrorKind::Internal,
        RpcErrorKind::Timeout,
        RpcErrorKind::Network,
    ];

    for kind in kinds {
        let serialized = postcard::to_stdvec(&kind).unwrap();
        let deserialized: RpcErrorKind = postcard::from_bytes(&serialized).unwrap();

        // Verify round-trip preserves retryable semantics
        assert_eq!(
            kind.is_retryable(),
            deserialized.is_retryable(),
            "Retryable semantics should be preserved for {:?}",
            kind
        );
        assert_eq!(
            kind.should_refresh_leader(),
            deserialized.should_refresh_leader(),
            "Refresh leader semantics should be preserved for {:?}",
            kind
        );
    }
}

#[test]
fn test_rpc_error_kind_debug() {
    assert!(format!("{:?}", RpcErrorKind::LeadershipChanged).contains("LeadershipChanged"));
    assert!(
        format!(
            "{:?}",
            RpcErrorKind::NotLeader {
                leader_hint: Some(5)
            }
        )
        .contains("5")
    );
    assert!(format!("{:?}", RpcErrorKind::ForwardLoopDetected).contains("ForwardLoopDetected"));
}

// ========================================================================
// RpcErrorInfo Tests
// ========================================================================

#[test]
fn test_rpc_error_info_new() {
    let error = RpcErrorInfo::new(RpcErrorKind::Timeout, "Connection timed out");
    assert!(matches!(error.kind, RpcErrorKind::Timeout));
    assert_eq!(error.message, "Connection timed out");
}

#[test]
fn test_rpc_error_info_internal() {
    let error = RpcErrorInfo::internal("Something went wrong");
    assert!(matches!(error.kind, RpcErrorKind::Internal));
    assert_eq!(error.message, "Something went wrong");
}

#[test]
fn test_rpc_error_info_internal_from_error() {
    let io_error = std::io::Error::new(std::io::ErrorKind::NotFound, "File not found");
    let error = RpcErrorInfo::internal(io_error);
    assert!(matches!(error.kind, RpcErrorKind::Internal));
    assert!(error.message.contains("File not found"));
}

#[test]
fn test_rpc_error_info_serialization() {
    let error = RpcErrorInfo::new(
        RpcErrorKind::NotLeader {
            leader_hint: Some(3),
        },
        "Not the leader",
    );

    let serialized = postcard::to_stdvec(&error).unwrap();
    let deserialized: RpcErrorInfo = postcard::from_bytes(&serialized).unwrap();

    assert!(matches!(
        deserialized.kind,
        RpcErrorKind::NotLeader {
            leader_hint: Some(3)
        }
    ));
    assert_eq!(deserialized.message, "Not the leader");
}

#[test]
fn test_rpc_error_info_debug() {
    let error = RpcErrorInfo::new(RpcErrorKind::Network, "Connection refused");
    let debug_str = format!("{:?}", error);
    assert!(debug_str.contains("Network"));
    assert!(debug_str.contains("Connection refused"));
}

// ========================================================================
// CircuitBreakerState Tests
// ========================================================================

#[test]
fn test_circuit_breaker_new_is_closed() {
    let cb = CircuitBreakerState::new();
    assert!(!cb.is_open(), "New circuit breaker should be closed");
    assert_eq!(cb.consecutive_failures, 0);
    assert!(cb.last_failure_time.is_none());
}

#[test]
fn test_circuit_breaker_stays_closed_below_threshold() {
    let mut cb = CircuitBreakerState::new();

    // Record failures below threshold
    for i in 0..(CIRCUIT_BREAKER_THRESHOLD - 1) {
        cb.record_failure();
        assert!(
            !cb.is_open(),
            "Circuit should stay closed at {} failures",
            i + 1
        );
    }
}

#[test]
fn test_circuit_breaker_opens_at_threshold() {
    let mut cb = CircuitBreakerState::new();

    // Record failures until threshold
    for _ in 0..CIRCUIT_BREAKER_THRESHOLD {
        cb.record_failure();
    }

    assert!(
        cb.is_open(),
        "Circuit should open at {} failures",
        CIRCUIT_BREAKER_THRESHOLD
    );
}

#[test]
fn test_circuit_breaker_success_resets() {
    let mut cb = CircuitBreakerState::new();

    // Accumulate some failures
    for _ in 0..3 {
        cb.record_failure();
    }
    assert_eq!(cb.consecutive_failures, 3);

    // Success resets the counter
    cb.record_success();
    assert_eq!(cb.consecutive_failures, 0);
    assert!(cb.last_failure_time.is_none());
    assert!(!cb.is_open());
}

#[test]
fn test_circuit_breaker_resets_after_timeout() {
    let mut cb = CircuitBreakerState::new();

    // Open the circuit breaker
    for _ in 0..CIRCUIT_BREAKER_THRESHOLD {
        cb.record_failure();
    }
    assert!(cb.is_open());

    // Simulate time passing (beyond reset timeout)
    // We can't easily simulate time in tests, but we can test the logic
    // by setting last_failure_time to a past instant
    cb.last_failure_time =
        Some(std::time::Instant::now() - CIRCUIT_BREAKER_RESET_TIMEOUT - Duration::from_secs(1));

    // Now the circuit should allow a probe (is_open returns false)
    assert!(
        !cb.is_open(),
        "Circuit should allow probe after reset timeout"
    );
}

#[test]
fn test_circuit_breaker_failure_count_accumulates() {
    let mut cb = CircuitBreakerState::new();

    cb.record_failure();
    assert_eq!(cb.consecutive_failures, 1);

    cb.record_failure();
    assert_eq!(cb.consecutive_failures, 2);

    cb.record_failure();
    assert_eq!(cb.consecutive_failures, 3);
}

#[test]
fn test_circuit_breaker_no_last_failure_time_is_closed() {
    let mut cb = CircuitBreakerState::new();
    cb.consecutive_failures = CIRCUIT_BREAKER_THRESHOLD + 10;
    cb.last_failure_time = None;

    // Even with high failure count, no last_failure_time means closed
    assert!(!cb.is_open());
}

// ========================================================================
// ClientWriteWithTerm Message Tests
// ========================================================================

#[test]
fn test_client_write_with_term_serialization() {
    use super::super::{CoordinationCommand, domains};

    let msg = RaftRpcMessage::ClientWriteWithTerm {
        command: CoordinationCommand::BrokerDomain(domains::BrokerCommand::Heartbeat {
            broker_id: 1,
            timestamp_ms: 123456,
            reported_local_timestamp_ms: 123456,
        }),
        expected_term: 42,
        forward_hops: 2,
    };

    let serialized = postcard::to_stdvec(&msg).unwrap();
    let deserialized: RaftRpcMessage = postcard::from_bytes(&serialized).unwrap();

    match deserialized {
        RaftRpcMessage::ClientWriteWithTerm {
            expected_term,
            forward_hops,
            ..
        } => {
            assert_eq!(expected_term, 42);
            assert_eq!(forward_hops, 2);
        }
        _ => panic!("Expected ClientWriteWithTerm message"),
    }
}

#[test]
fn test_join_cluster_message_serialization() {
    let msg = RaftRpcMessage::JoinCluster {
        node_id: 99,
        raft_addr: "192.168.1.100:9093".to_string(),
    };

    let serialized = postcard::to_stdvec(&msg).unwrap();
    let deserialized: RaftRpcMessage = postcard::from_bytes(&serialized).unwrap();

    match deserialized {
        RaftRpcMessage::JoinCluster { node_id, raft_addr } => {
            assert_eq!(node_id, 99);
            assert_eq!(raft_addr, "192.168.1.100:9093");
        }
        _ => panic!("Expected JoinCluster message"),
    }
}

// ========================================================================
// ErrorV2 Response Tests
// ========================================================================

#[test]
fn test_error_v2_response_serialization() {
    let error_info = RpcErrorInfo::new(RpcErrorKind::LeadershipChanged, "Term changed");
    let resp = RaftRpcResponse::ErrorV2(error_info);

    let serialized = postcard::to_stdvec(&resp).unwrap();
    let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

    match deserialized {
        RaftRpcResponse::ErrorV2(info) => {
            assert!(matches!(info.kind, RpcErrorKind::LeadershipChanged));
            assert_eq!(info.message, "Term changed");
        }
        _ => panic!("Expected ErrorV2 response"),
    }
}

#[test]
fn test_join_cluster_ok_response_serialization() {
    let resp = RaftRpcResponse::JoinClusterOk;

    let serialized = postcard::to_stdvec(&resp).unwrap();
    let deserialized: RaftRpcResponse = postcard::from_bytes(&serialized).unwrap();

    assert!(matches!(deserialized, RaftRpcResponse::JoinClusterOk));
}

// ========================================================================
// Constants Tests
// ========================================================================

#[test]
#[allow(clippy::assertions_on_constants)]
fn test_max_forward_hops_is_reasonable() {
    // MAX_FORWARD_HOPS should be small to prevent loops but allow some forwarding
    assert!(MAX_FORWARD_HOPS >= 1, "Should allow at least 1 hop");
    assert!(MAX_FORWARD_HOPS <= 10, "Should prevent runaway forwarding");
}

#[test]
#[allow(clippy::assertions_on_constants)]
fn test_circuit_breaker_threshold_is_reasonable() {
    // Threshold should be high enough to tolerate transient failures
    assert!(
        CIRCUIT_BREAKER_THRESHOLD >= 3,
        "Should tolerate some failures"
    );
    assert!(
        CIRCUIT_BREAKER_THRESHOLD <= 20,
        "Should not require too many failures"
    );
}

#[test]
#[allow(clippy::assertions_on_constants)]
fn test_timeouts_are_reasonable() {
    assert!(
        RPC_CONNECT_TIMEOUT >= Duration::from_secs(1),
        "Connect timeout should be at least 1s"
    );
    assert!(
        RPC_OPERATION_TIMEOUT >= Duration::from_secs(5),
        "Operation timeout should be at least 5s"
    );
    assert!(
        RPC_OPERATION_TIMEOUT >= RPC_CONNECT_TIMEOUT,
        "Operation timeout should be >= connect timeout"
    );
}

#[test]
#[allow(clippy::assertions_on_constants)]
fn test_retry_delays_are_reasonable() {
    assert!(
        RPC_RETRY_BASE_DELAY >= Duration::from_millis(10),
        "Base delay should be at least 10ms"
    );
    assert!(
        RPC_RETRY_MAX_DELAY >= RPC_RETRY_BASE_DELAY,
        "Max delay should be >= base delay"
    );
    assert!(RPC_MAX_RETRIES >= 1, "Should allow at least 1 retry");
}

// ========================================================================
// Get Node Address Tests
// ========================================================================

#[tokio::test]
async fn test_get_node_addr_existing() {
    let factory = RaftNetworkFactoryImpl::new();
    factory.add_node(1, "127.0.0.1:9093".to_string()).await;

    let addr = factory.get_node_addr(1).await;
    assert_eq!(addr, Some("127.0.0.1:9093".to_string()));
}

#[tokio::test]
async fn test_get_node_addr_nonexistent() {
    let factory = RaftNetworkFactoryImpl::new();

    let addr = factory.get_node_addr(999).await;
    assert_eq!(addr, None);
}

// ========================================================================
// RpcErrorKind Clone Tests
// ========================================================================

#[test]
fn test_rpc_error_kind_clone() {
    let original = RpcErrorKind::NotLeader {
        leader_hint: Some(42),
    };
    let cloned = original.clone();

    match (original, cloned) {
        (
            RpcErrorKind::NotLeader {
                leader_hint: Some(a),
            },
            RpcErrorKind::NotLeader {
                leader_hint: Some(b),
            },
        ) => {
            assert_eq!(a, b);
        }
        _ => panic!("Clone should preserve variant"),
    }
}

#[test]
fn test_rpc_error_info_clone() {
    let original = RpcErrorInfo::new(RpcErrorKind::Timeout, "Test message");
    let cloned = original.clone();

    assert!(matches!(cloned.kind, RpcErrorKind::Timeout));
    assert_eq!(cloned.message, "Test message");
}
