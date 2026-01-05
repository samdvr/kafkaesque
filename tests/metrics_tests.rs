//! Tests for the metrics module.
//!
//! These tests verify metric recording, circuit breaker behavior,
//! and cardinality limiting.

use kafkaesque::cluster::metrics;

// ============================================================================
// Circuit Breaker Tests
//
// Note: The circuit breaker uses global atomic state. Tests that depend on
// exact counter values are unreliable when running in parallel. These tests
// focus on behavioral properties that are observable regardless of state.
// ============================================================================

#[test]
fn test_circuit_breaker_reset_works() {
    // Reset should bring count to 0
    metrics::reset_circuit_breaker();
    let (count, _tripped) = metrics::get_circuit_breaker_state();
    assert_eq!(count, 0, "Count should be 0 immediately after reset");
}

#[test]
fn test_circuit_breaker_typed_detection() {
    // Typed detection should return true (should fence)
    let should_fence = metrics::record_fencing_detection_with_circuit_breaker("typed");
    assert!(should_fence, "Typed detection should signal fencing");
}

#[test]
fn test_circuit_breaker_pattern_detection() {
    // Pattern detection should return true (should fence)
    let should_fence = metrics::record_fencing_detection_with_circuit_breaker("pattern");
    assert!(should_fence, "Pattern detection should signal fencing");
}

#[test]
fn test_circuit_breaker_not_fencing_detection() {
    // not_fencing should return false (should not fence)
    let should_fence = metrics::record_fencing_detection_with_circuit_breaker("not_fencing");
    assert!(
        !should_fence,
        "not_fencing detection should not signal fencing"
    );
}

#[test]
fn test_circuit_breaker_api_exists() {
    // Just verify the API exists and doesn't panic
    let _ = metrics::fail_closed_circuit_breaker_tripped();
    let _ = metrics::get_circuit_breaker_state();
    metrics::reset_circuit_breaker();
}

// ============================================================================
// Request Recording Tests
// ============================================================================

#[test]
fn test_record_request() {
    // This should not panic
    metrics::record_request("Produce", "success", 0.1);
    metrics::record_request("Fetch", "error", 0.5);
    metrics::record_request("Metadata", "success", 0.01);
}

// ============================================================================
// Produce/Fetch Recording Tests
// ============================================================================

#[test]
fn test_record_produce() {
    // This should not panic
    metrics::record_produce("test-topic", 0, 100, 10240);
    metrics::record_produce("test-topic", 1, 50, 5120);
}

#[test]
fn test_record_fetch() {
    // This should not panic
    metrics::record_fetch("test-topic", 0, 100, 10240);
    metrics::record_fetch("test-topic", 1, 50, 5120);
}

// ============================================================================
// Lease Operation Tests
// ============================================================================

#[test]
fn test_record_lease_operation() {
    metrics::record_lease_operation("acquire", "success");
    metrics::record_lease_operation("acquire", "failure");
    metrics::record_lease_operation("renew", "success");
    metrics::record_lease_operation("release", "success");
}

// ============================================================================
// Cache Lookup Tests
// ============================================================================

#[test]
fn test_record_cache_lookup() {
    metrics::record_cache_lookup("owner", true);
    metrics::record_cache_lookup("owner", false);
    metrics::record_cache_lookup("metadata", true);
}

// ============================================================================
// Coordinator Operation Tests
// ============================================================================

#[test]
fn test_record_coordinator_operation() {
    metrics::record_coordinator_operation("get_owner", 0.001);
    metrics::record_coordinator_operation("set_owner", 0.002);
    metrics::record_coordinator_operation("join_group", 0.01);
}

#[test]
fn test_record_coordinator_failure() {
    metrics::record_coordinator_failure("heartbeat");
    metrics::record_coordinator_failure("lease_renewal");
}

#[test]
fn test_record_coordinator_recovery() {
    metrics::record_coordinator_recovery("zombie_mode");
    metrics::record_coordinator_recovery("heartbeat");
}

// ============================================================================
// Consumer Group Operation Tests
// ============================================================================

#[test]
fn test_record_group_operation() {
    metrics::record_group_operation("join", "success");
    metrics::record_group_operation("sync", "success");
    metrics::record_group_operation("heartbeat", "success");
    metrics::record_group_operation("leave", "success");
    metrics::record_group_operation("join", "error");
}

#[test]
fn test_record_group_operation_latency() {
    metrics::record_group_operation_latency("join", "success", 0.1);
    metrics::record_group_operation_latency("sync", "success", 0.05);
    metrics::record_group_operation_latency("heartbeat", "success", 0.001);
}

#[test]
fn test_record_group_operation_with_latency() {
    metrics::record_group_operation_with_latency("join", "success", 0.15);
    metrics::record_group_operation_with_latency("sync", "error", 0.2);
}

// ============================================================================
// Active Groups Tests
// ============================================================================

#[test]
fn test_active_groups_operations() {
    metrics::set_active_groups(0);
    metrics::inc_active_groups();
    metrics::inc_active_groups();
    metrics::dec_active_groups();
}

// ============================================================================
// Offset Commit Tests
// ============================================================================

#[test]
fn test_record_offset_commit() {
    metrics::record_offset_commit("test-group", "test-topic", "success");
    metrics::record_offset_commit("test-group", "other-topic", "error");
}

// ============================================================================
// Produce/Fetch Latency Tests
// ============================================================================

#[test]
fn test_record_produce_latency() {
    metrics::record_produce_latency("test-topic", "success", 0.05);
    metrics::record_produce_latency("test-topic", "error", 0.1);
    metrics::record_produce_latency("_multi", "success", 0.2);
}

#[test]
fn test_record_fetch_latency() {
    metrics::record_fetch_latency("test-topic", "success", 0.01);
    metrics::record_fetch_latency("test-topic", "timeout", 30.0);
    metrics::record_fetch_latency("_multi", "error", 0.5);
}

// ============================================================================
// Rebalance Tests
// ============================================================================

#[test]
fn test_record_rebalance_duration() {
    metrics::record_rebalance_duration("test-group", 5.0);
    metrics::record_rebalance_duration("other-group", 2.5);
}

#[test]
fn test_pending_rebalances() {
    metrics::inc_pending_rebalances();
    metrics::inc_pending_rebalances();
    metrics::dec_pending_rebalances();
}

// ============================================================================
// Partition Acquisition Tests
// ============================================================================

#[test]
fn test_record_partition_acquisition() {
    metrics::record_partition_acquisition("test-topic", "success", 0.01);
    metrics::record_partition_acquisition("test-topic", "error", 0.5);
    metrics::record_partition_acquisition("test-topic", "fenced", 0.02);
}

// ============================================================================
// Topic/Group Metadata Tests
// ============================================================================

#[test]
fn test_set_topic_partition_count() {
    metrics::set_topic_partition_count("test-topic", 3);
    metrics::set_topic_partition_count("large-topic", 100);
}

#[test]
fn test_set_group_member_count() {
    metrics::set_group_member_count("test-group", 5);
    metrics::set_group_member_count("other-group", 10);
}

#[test]
fn test_set_group_generation() {
    metrics::set_group_generation("test-group", 1);
    metrics::set_group_generation("test-group", 2);
}

// ============================================================================
// Storage Operation Tests
// ============================================================================

#[test]
fn test_record_storage_operation() {
    metrics::record_storage_operation("get", 0.001);
    metrics::record_storage_operation("put", 0.002);
    metrics::record_storage_operation("delete", 0.0005);
}

// ============================================================================
// HWM Recovery Tests
// ============================================================================

#[test]
fn test_record_hwm_recovery() {
    metrics::record_hwm_recovery("test-topic", 0, true);
    metrics::record_hwm_recovery("test-topic", 1, false);
}

#[test]
fn test_record_recovery_gap() {
    metrics::record_recovery_gap(2, 100);
}

// ============================================================================
// Lease TTL Tests
// ============================================================================

#[test]
fn test_record_lease_ttl_at_write() {
    metrics::record_lease_ttl_at_write("test-topic", 30);
    metrics::record_lease_ttl_at_write("test-topic", 15);
}

#[test]
fn test_record_lease_too_short() {
    metrics::record_lease_too_short("test-topic", 0);
    metrics::record_lease_too_short("test-topic", 1);
}

// ============================================================================
// Zombie Mode Tests
// ============================================================================

#[test]
fn test_enter_zombie_mode() {
    metrics::enter_zombie_mode();
    // Should not panic
}

#[test]
fn test_exit_zombie_mode() {
    metrics::exit_zombie_mode(60.0, "recovered");
    metrics::exit_zombie_mode(30.0, "manual");
}

// ============================================================================
// Batch Index Tests
// ============================================================================

#[test]
fn test_batch_index_operations() {
    metrics::record_batch_index_hit();
    metrics::record_batch_index_miss();
    metrics::set_batch_index_size("test-topic", 0, 100);
    metrics::record_batch_index_eviction("test-topic", 0, 10);
    metrics::record_batch_index_warm_entries(50);
}

// ============================================================================
// Consumer Lag Tests
// ============================================================================

#[test]
fn test_record_consumer_lag() {
    metrics::record_consumer_lag("test-group", "test-topic", 0, 100, 150);
    metrics::record_consumer_lag("test-group", "test-topic", 1, 50, 50);
}

#[test]
fn test_set_consumer_lag() {
    metrics::set_consumer_lag("test-group", "test-topic", 0, 50);
    metrics::set_consumer_lag("test-group", "test-topic", 0, -10); // Should clamp to 0
}

// ============================================================================
// Producer ID Tests
// ============================================================================

#[test]
fn test_set_producer_id_counter() {
    metrics::set_producer_id_counter(1000);
}

#[test]
fn test_producer_state_metrics() {
    metrics::record_producer_state_persistence_failure("test-topic", 0);
    metrics::set_producer_state_recovery_count("test-topic", 0, 50);
}

// ============================================================================
// Idempotency Tests
// ============================================================================

#[test]
fn test_record_idempotency_rejection() {
    metrics::record_idempotency_rejection("duplicate");
    metrics::record_idempotency_rejection("out_of_order");
    metrics::record_idempotency_rejection("fenced_epoch");
}

// ============================================================================
// Raft Metrics Tests
// ============================================================================

#[test]
fn test_raft_state_metrics() {
    metrics::set_raft_state(0); // Follower
    metrics::set_raft_state(1); // Candidate
    metrics::set_raft_state(2); // Leader
}

#[test]
fn test_raft_term_metrics() {
    metrics::set_raft_term(1);
    metrics::set_raft_term(5);
}

#[test]
fn test_raft_index_metrics() {
    metrics::set_raft_commit_index(100);
    metrics::set_raft_applied_index(99);
}

#[test]
fn test_raft_election_metrics() {
    metrics::record_raft_election("won");
    metrics::record_raft_election("lost");
    metrics::record_raft_election("timeout");
}

#[test]
fn test_raft_proposal_metrics() {
    metrics::record_raft_proposal("success", 0.01);
    metrics::record_raft_proposal("error", 0.5);
}

#[test]
fn test_raft_snapshot_metrics() {
    metrics::record_raft_snapshot("create", "success");
    metrics::record_raft_snapshot("install", "success");
    metrics::record_raft_snapshot("create", "error");
}

#[test]
fn test_raft_log_entries() {
    metrics::set_raft_log_entries(1000);
}

#[test]
fn test_raft_pending_proposals() {
    metrics::set_raft_pending_proposals(5);
}

#[test]
fn test_raft_backpressure() {
    metrics::record_raft_backpressure("acquired");
    metrics::record_raft_backpressure("waiting");
    metrics::record_raft_backpressure("timeout");
}

// ============================================================================
// Broker Info Tests
// ============================================================================

#[test]
fn test_init_broker_info() {
    metrics::init_broker_info(1, "localhost", "0.1.0");
}

#[test]
fn test_update_broker_uptime() {
    metrics::update_broker_uptime();
}

// ============================================================================
// Request/Response Size Tests
// ============================================================================

#[test]
fn test_record_request_size() {
    metrics::record_request_size("Produce", 1024);
    metrics::record_request_size("Fetch", 100);
}

#[test]
fn test_record_response_size() {
    metrics::record_response_size("Produce", 50);
    metrics::record_response_size("Fetch", 10240);
}

#[test]
fn test_record_request_response_sizes() {
    metrics::record_request_response_sizes("Produce", 1024, 50);
}

// ============================================================================
// Object Store Metrics Tests
// ============================================================================

#[test]
fn test_object_store_operation() {
    metrics::record_object_store_operation("get", "success", 0.01);
    metrics::record_object_store_operation("put", "success", 0.05);
    metrics::record_object_store_operation("delete", "error", 0.1);
}

#[test]
fn test_object_store_bytes() {
    metrics::record_object_store_bytes("read", 10240);
    metrics::record_object_store_bytes("write", 20480);
}

#[test]
fn test_object_store_errors() {
    metrics::record_object_store_error("get", "not_found");
    metrics::record_object_store_error("put", "permission");
    metrics::record_object_store_error("delete", "timeout");
}

// ============================================================================
// Network Metrics Tests
// ============================================================================

#[test]
fn test_network_bytes() {
    metrics::record_network_received(1024);
    metrics::record_network_sent(2048);
}

// ============================================================================
// SASL Auth Tests
// ============================================================================

#[test]
fn test_record_sasl_auth() {
    metrics::record_sasl_auth("PLAIN", "success");
    metrics::record_sasl_auth("PLAIN", "failure");
    metrics::record_sasl_auth("SCRAM-SHA-256", "success");
}

// ============================================================================
// Fencing Detection Tests
// ============================================================================

#[test]
fn test_record_fencing_detection() {
    // The deprecated version should still work
    metrics::record_fencing_detection("typed");
    metrics::record_fencing_detection("pattern");
    metrics::record_fencing_detection("fail_closed");
    metrics::record_fencing_detection("not_fencing");
}

// ============================================================================
// Metrics Configuration Tests
// ============================================================================

#[test]
fn test_configure_metrics() {
    // Enable partition metrics with limit
    metrics::configure_metrics(true, 1000);

    // Disable partition metrics
    metrics::configure_metrics(false, 0);

    // Re-enable with unlimited
    metrics::configure_metrics(true, 0);
}

// ============================================================================
// Metrics Initialization Tests
// ============================================================================

#[test]
fn test_init_metrics() {
    // Should not panic even when called multiple times
    metrics::init_metrics();
    metrics::init_metrics();
}

// ============================================================================
// Metrics Encoding Tests
// ============================================================================

#[test]
fn test_encode_metrics() {
    // Record some metrics first so there's something to encode
    metrics::record_request("Produce", "success", 0.1);

    let result = metrics::encode_metrics();
    assert!(result.is_ok());
    // Note: encoded may be empty if no metrics are registered globally
    // The important thing is that it doesn't fail
}

#[test]
fn test_gather_metrics() {
    let families = metrics::gather_metrics();
    // Should have some metric families registered
    assert!(!families.is_empty());
}

// ============================================================================
// Async Metrics Tests
// ============================================================================

#[tokio::test]
async fn test_record_produce_async() {
    metrics::configure_metrics(true, 100);
    metrics::record_produce_async("async-topic", 0, 100, 10240).await;
    metrics::record_produce_async("async-topic", 1, 50, 5120).await;
}

#[tokio::test]
async fn test_record_fetch_async() {
    metrics::configure_metrics(true, 100);
    metrics::record_fetch_async("async-topic", 0, 100, 10240).await;
    metrics::record_fetch_async("async-topic", 1, 50, 5120).await;
}
