use super::*;
use serial_test::serial;

// ========================================================================
// Circuit Breaker Tests
// ========================================================================

#[test]
#[serial]
fn test_circuit_breaker_initial_state() {
    // Reset before testing
    reset_circuit_breaker();
    let (count, tripped) = get_circuit_breaker_state();
    assert_eq!(count, 0);
    assert!(!tripped);
}

#[test]
#[serial]
fn test_circuit_breaker_reset() {
    // Force some state
    for _ in 0..5 {
        let _ = record_fencing_detection_with_circuit_breaker("fail_closed");
    }

    // Reset should clear state
    reset_circuit_breaker();
    let (count, tripped) = get_circuit_breaker_state();
    assert_eq!(count, 0);
    assert!(!tripped);
}

// ========================================================================
// Metric Recording Helper Tests
// ========================================================================

#[test]
fn test_record_request() {
    // Should not panic
    record_request("Produce", "success", 0.001);
    record_request("Fetch", "error", 0.1);
}

#[test]
fn test_record_produce() {
    // Should not panic
    record_produce("test-topic", 0, 10, 1024);
    record_produce("test-topic", 1, 100, 10240);
}

#[test]
fn test_record_fetch() {
    // Should not panic
    record_fetch("test-topic", 0, 10, 1024);
}

#[test]
fn test_record_lease_operation() {
    record_lease_operation("acquire", "success");
    record_lease_operation("release", "error");
    record_lease_operation("renew", "fenced");
}

#[test]
fn test_record_lease_cache() {
    record_lease_cache("hit");
    record_lease_cache("miss");
}

#[test]
fn test_record_cache_lookup() {
    record_cache_lookup("owner", true);
    record_cache_lookup("owner", false);
    record_cache_lookup("metadata", true);
}

#[test]
fn test_record_coordinator_operation() {
    record_coordinator_operation("get_owner", 0.001);
    record_coordinator_operation("set_owner", 0.002);
}

#[test]
fn test_record_group_operation() {
    record_group_operation("join", "success");
    record_group_operation("sync", "error");
    record_group_operation("heartbeat", "success");
    record_group_operation("leave", "success");
}

#[test]
fn test_record_offset_commit() {
    record_offset_commit("my-group", "my-topic", "success");
    record_offset_commit("my-group", "my-topic", "error");
}

#[test]
fn test_active_groups_gauge() {
    set_active_groups(10);
    inc_active_groups();
    dec_active_groups();
}

#[test]
fn test_record_coordinator_failure() {
    record_coordinator_failure("heartbeat");
    record_coordinator_failure("lease_renewal");
}

#[test]
fn test_record_coordinator_recovery() {
    record_coordinator_recovery("zombie_mode");
    record_coordinator_recovery("heartbeat");
}

// Serialized with the circuit-breaker tests: `record_fencing_detection`
// delegates to the circuit-breaker counter, so running concurrently with
// `test_circuit_breaker_initial_state` / `_reset` races the shared global
// (the reset-then-assert sees another test's increment).
#[test]
#[serial]
fn test_record_fencing_detection() {
    record_fencing_detection("typed");
    record_fencing_detection("pattern");
    record_fencing_detection("not_fencing");
}

// ========================================================================
// Latency Recording Tests
// ========================================================================

#[test]
fn test_record_produce_latency() {
    record_produce_latency("test-topic", "success", 0.005);
    record_produce_latency("test-topic", "error", 0.1);
}

#[test]
fn test_record_fetch_latency() {
    record_fetch_latency("test-topic", "success", 0.01);
    record_fetch_latency("test-topic", "timeout", 30.0);
}

#[test]
fn test_record_group_operation_latency() {
    record_group_operation_latency("join", "success", 0.05);
    record_group_operation_latency("sync", "error", 0.1);
}

#[test]
fn test_record_group_operation_with_latency() {
    record_group_operation_with_latency("heartbeat", "success", 0.001);
}

// ========================================================================
// Consumer Group Metrics Tests
// ========================================================================

#[test]
fn test_record_rebalance_duration() {
    record_rebalance_duration("my-group", 2.5);
}

#[test]
fn test_partition_acquisition() {
    record_partition_acquisition("test-topic", "success", 0.1);
    record_partition_acquisition("test-topic", "error", 0.2);
    record_partition_acquisition("test-topic", "fenced", 0.05);
}

#[test]
fn test_set_topic_partition_count() {
    set_topic_partition_count("my-topic", 10);
    set_topic_partition_count("big-topic", 100);
}

#[test]
fn test_set_group_member_count() {
    set_group_member_count("my-group", 5);
}

#[test]
fn test_set_group_generation() {
    set_group_generation("my-group", 42);
}

#[test]
fn test_pending_rebalances() {
    inc_pending_rebalances();
    inc_pending_rebalances();
    dec_pending_rebalances();
}

// ========================================================================
// Storage Metrics Tests
// ========================================================================

#[test]
fn test_record_storage_operation() {
    record_storage_operation("get", 0.001);
    record_storage_operation("put", 0.002);
    record_storage_operation("delete", 0.001);
}

#[test]
fn test_record_hwm_recovery() {
    record_hwm_recovery("topic", 0, true);
    record_hwm_recovery("topic", 1, false);
}

#[test]
fn test_record_recovery_gap() {
    record_recovery_gap(2, 100);
}

// ========================================================================
// Zombie Mode Metrics Tests
// ========================================================================

#[test]
fn test_zombie_mode_metrics() {
    enter_zombie_mode();
    exit_zombie_mode(10.5, "recovered");
}

// ========================================================================
// Batch Index Metrics Tests
// ========================================================================

#[test]
fn test_batch_index_metrics() {
    record_batch_index_hit();
    record_batch_index_miss();
    set_batch_index_size("topic", 0, 100);
    record_batch_index_eviction("topic", 0, 10);
    record_batch_index_warm_entries(50);
}

// ========================================================================
// Lease TTL Metrics Tests
// ========================================================================

#[test]
fn test_record_lease_ttl_at_write() {
    record_lease_ttl_at_write("my-topic", 45);
    record_lease_ttl_at_write("my-topic", 10);
}

#[test]
fn test_record_lease_too_short() {
    record_lease_too_short("my-topic", 0);
}

// ========================================================================
// Consumer Lag Metrics Tests
// ========================================================================

#[test]
fn test_record_consumer_lag() {
    record_consumer_lag("my-group", "my-topic", 0, 100, 150);
    // Test negative lag (race condition) gets clamped to 0
    record_consumer_lag("my-group", "my-topic", 0, 200, 150);
}

#[test]
fn test_set_consumer_lag() {
    set_consumer_lag("my-group", "my-topic", 0, 50);
    // Negative lag gets clamped
    set_consumer_lag("my-group", "my-topic", 0, -10);
}

// ========================================================================
// Producer ID Metrics Tests
// ========================================================================

#[test]
fn test_set_producer_id_counter() {
    set_producer_id_counter(12345);
}

#[test]
fn test_record_producer_state_persistence_failure() {
    record_producer_state_persistence_failure("topic", 0);
}

#[test]
fn test_set_producer_state_recovery_count() {
    set_producer_state_recovery_count("topic", 0, 10);
}

// ========================================================================
// Sequence Number Metrics Tests
// ========================================================================

#[test]
fn test_record_sequence_number_normal() {
    // Normal sequence number
    let warning = record_sequence_number("topic", 0, 1, 1000);
    assert!(!warning);
}

// ========================================================================
// Raft Metrics Tests
// ========================================================================

#[test]
fn test_raft_state_metrics() {
    set_raft_state(0); // follower
    set_raft_state(1); // candidate
    set_raft_state(2); // leader
}

#[test]
fn test_raft_term_metrics() {
    set_raft_term(10);
}

#[test]
fn test_raft_index_metrics() {
    set_raft_commit_index(100);
    set_raft_applied_index(99);
}

#[test]
fn test_record_raft_election() {
    record_raft_election("won");
    record_raft_election("lost");
    record_raft_election("timeout");
}

#[test]
fn test_record_raft_proposal() {
    record_raft_proposal("success", 0.01);
    record_raft_proposal("error", 0.5);
}

#[test]
fn test_record_raft_snapshot() {
    record_raft_snapshot("create", "success");
    record_raft_snapshot("install", "error");
}

#[test]
fn test_raft_log_metrics() {
    set_raft_log_entries(1000);
    set_raft_pending_proposals(5);
}

#[test]
fn test_record_raft_backpressure() {
    record_raft_backpressure("acquired");
    record_raft_backpressure("waiting");
    record_raft_backpressure("timeout");
}

// ========================================================================
// Broker Info Metrics Tests
// ========================================================================

#[test]
fn test_init_broker_info() {
    init_broker_info(1, "localhost", "0.1.0");
}

#[test]
fn test_update_broker_uptime() {
    init_broker_info(1, "localhost", "0.1.0");
    update_broker_uptime();
}

// ========================================================================
// Request/Response Size Metrics Tests
// ========================================================================

#[test]
fn test_record_request_size() {
    record_request_size("Produce", 1024);
    record_request_size("Fetch", 512);
}

#[test]
fn test_record_response_size() {
    record_response_size("Produce", 128);
    record_response_size("Fetch", 65536);
}

#[test]
fn test_record_request_response_sizes() {
    record_request_response_sizes("Metadata", 100, 500);
}

// ========================================================================
// Object Store Metrics Tests
// ========================================================================

#[test]
fn test_record_object_store_operation() {
    record_object_store_operation("get", "success", 0.01);
    record_object_store_operation("put", "error", 0.5);
    record_object_store_operation("delete", "success", 0.001);
    record_object_store_operation("list", "success", 0.1);
}

#[test]
fn test_record_object_store_bytes() {
    record_object_store_bytes("read", 10240);
    record_object_store_bytes("write", 5120);
}

#[test]
fn test_record_object_store_error() {
    record_object_store_error("get", "timeout");
    record_object_store_error("put", "permission");
    record_object_store_error("delete", "not_found");
}

#[test]
#[serial]
fn test_track_object_store_health_success() {
    // Reset state
    OBJECT_STORE_CONSECUTIVE_FAILURES.set(0);

    let healthy = track_object_store_health(true);
    assert!(healthy);
    assert_eq!(OBJECT_STORE_CONSECUTIVE_FAILURES.get(), 0);
}

#[test]
#[serial]
fn test_track_object_store_health_failure() {
    // Reset state
    OBJECT_STORE_CONSECUTIVE_FAILURES.set(0);

    // First few failures should still be "healthy"
    for i in 1..10 {
        let healthy = track_object_store_health(false);
        assert!(healthy, "Should be healthy with {} failures", i);
    }

    // 10th failure should make it unhealthy
    let healthy = track_object_store_health(false);
    assert!(!healthy, "Should be unhealthy after 10 failures");
}

#[test]
#[serial]
fn test_is_object_store_healthy() {
    OBJECT_STORE_CONSECUTIVE_FAILURES.set(0);
    assert!(is_object_store_healthy());

    OBJECT_STORE_CONSECUTIVE_FAILURES.set(5);
    assert!(is_object_store_healthy());

    OBJECT_STORE_CONSECUTIVE_FAILURES.set(10);
    assert!(!is_object_store_healthy());
}

#[test]
#[serial]
fn test_object_store_consecutive_failures() {
    OBJECT_STORE_CONSECUTIVE_FAILURES.set(7);
    assert_eq!(object_store_consecutive_failures(), 7);
}

// ========================================================================
// Network Metrics Tests
// ========================================================================

#[test]
fn test_record_network_metrics() {
    record_network_received(1024);
    record_network_sent(512);
}

// ========================================================================
// Clock Skew Metrics Tests
// ========================================================================

#[test]
fn test_record_heartbeat_rtt() {
    record_heartbeat_rtt(2, 0.05);
    record_heartbeat_rtt(3, 0.1);
}

#[test]
fn test_record_heartbeat_rtt_with_skew() {
    // Test without skew warning
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_millis() as u64;
    record_heartbeat_rtt_with_skew(2, 0.05, Some(now), now - 25, now + 25);
}

#[test]
fn test_get_clock_skew() {
    // Just verify it doesn't panic
    let _skew = get_clock_skew(2);
}

#[test]
fn test_get_max_clock_skew() {
    // Just verify it doesn't panic
    let _max = get_max_clock_skew();
}

// ========================================================================
// Fast Failover Metrics Tests
// ========================================================================

#[test]
fn test_record_broker_failure() {
    record_broker_failure("heartbeat_timeout");
    record_broker_failure("lease_expired");
}

#[test]
fn test_record_partition_transfer() {
    record_partition_transfer("broker_failure", true);
    record_partition_transfer("load_balancing", false);
}

#[test]
fn test_set_suspected_brokers() {
    set_suspected_brokers(2);
}

#[test]
fn test_set_failed_brokers() {
    set_failed_brokers(1);
}

#[test]
fn test_set_broker_load() {
    set_broker_load(1, 1_000_000.0);
}

#[test]
fn test_set_broker_partition_count() {
    set_broker_partition_count(1, 10);
}

#[test]
fn test_set_cluster_load_deviation() {
    set_cluster_load_deviation(0.15);
}

#[test]
fn test_record_auto_rebalance() {
    record_auto_rebalance("performed");
    record_auto_rebalance("skipped_balanced");
    record_auto_rebalance("skipped_cooldown");
}

#[test]
fn test_set_active_cooldowns() {
    set_active_cooldowns(5);
}

#[test]
fn test_record_failover_duration() {
    record_failover_duration("heartbeat_timeout", 2.5);
}

// ========================================================================
// SASL and Idempotency Metrics Tests
// ========================================================================

#[test]
fn test_record_sasl_auth() {
    record_sasl_auth("PLAIN", "success");
    record_sasl_auth("SCRAM-SHA-256", "failure");
}

#[test]
fn test_record_idempotency_rejection() {
    record_idempotency_rejection("duplicate");
    record_idempotency_rejection("out_of_order");
    record_idempotency_rejection("fenced_epoch");
}

#[test]
fn test_record_epoch_mismatch() {
    record_epoch_mismatch("topic", 0);
}

// ========================================================================
// Metrics Encoding and Gathering Tests
// ========================================================================

#[test]
fn test_encode_metrics() {
    init_metrics();
    let result = encode_metrics();
    assert!(result.is_ok());
    let encoded = result.unwrap();
    // Should contain at least some metric output
    assert!(!encoded.is_empty());
}

#[test]
fn test_gather_metrics() {
    init_metrics();
    let families = gather_metrics();
    // Should have metrics
    assert!(!families.is_empty());
}

#[test]
fn test_init_metrics_idempotent() {
    // Should not panic when called multiple times
    init_metrics();
    init_metrics();
    init_metrics();
}

// ========================================================================
// Cardinality Configuration Tests
// ========================================================================

// The partition-label and async-record tests below all mutate the process-
// global metric config via `configure_metrics` (PARTITION_METRICS_ENABLED /
// MAX_METRIC_CARDINALITY). They must be `#[serial]` with each other, or one
// test's `configure_metrics(false, ..)` lands between another's configure and
// read and corrupts the observed label.
#[test]
#[serial]
fn test_configure_metrics() {
    configure_metrics(true, 5000);
    configure_metrics(false, 0);
}

#[tokio::test]
#[serial]
async fn test_get_partition_label_disabled() {
    configure_metrics(false, 0);
    let label = get_partition_label("topic", 5).await;
    assert_eq!(label, "_all");
}

#[tokio::test]
#[serial]
async fn test_get_partition_label_unlimited() {
    configure_metrics(true, 0);
    let label = get_partition_label("topic", 42).await;
    assert_eq!(label, "42");
}

#[test]
#[serial]
fn test_get_partition_label_sync_disabled() {
    configure_metrics(false, 1000);
    let label = get_partition_label_sync("topic", 5);
    assert_eq!(label, "_all");
}

#[test]
#[serial]
fn test_get_partition_label_sync_enabled() {
    configure_metrics(true, 1000);
    let label = get_partition_label_sync("topic", 5);
    assert_eq!(label, "5");
}

// ========================================================================
// Async Metric Recording Tests
// ========================================================================

#[tokio::test]
#[serial]
async fn test_record_produce_async() {
    configure_metrics(true, 10000);
    record_produce_async("async-topic", 0, 10, 1024).await;
}

#[tokio::test]
#[serial]
async fn test_record_fetch_async() {
    configure_metrics(true, 10000);
    record_fetch_async("async-topic", 0, 10, 1024).await;
}

// ========================================================================
// Distributed Systems Observability Metrics Tests
// ========================================================================

#[test]
fn test_record_raft_query() {
    record_raft_query("get_owner", 0.005);
    record_raft_query("get_brokers", 0.01);
    record_raft_query("get_topics", 0.003);
    record_raft_query("get_group_state", 0.008);
}

#[test]
fn test_record_slatedb_flush() {
    record_slatedb_flush("test-topic", 0.1);
    record_slatedb_flush("_all", 0.5);
}

#[test]
fn test_record_rebalance_stale_decision() {
    record_rebalance_stale_decision("ownership_changed");
    record_rebalance_stale_decision("raft_index_mismatch");
}

#[test]
fn test_record_failover_batch() {
    record_failover_batch(1, 5, "success");
    record_failover_batch(2, 10, "partial");
    record_failover_batch(3, 3, "failed");
}
