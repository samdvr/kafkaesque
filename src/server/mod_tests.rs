//! Inline tests previously embedded in `mod.rs`. Moved to a sibling
//! file so the implementation isn't gated on scrolling past the test
//! block. `super::*` re-exports private helpers the tests rely on.

#![allow(clippy::module_inception)]
#![allow(unused_imports)]

use super::*;

use super::*;
use crate::server::request::*;
use crate::server::response::*;
use async_trait::async_trait;

/// Simple test handler that does nothing.
struct TestHandler;

#[async_trait]
impl Handler for TestHandler {
    async fn handle_metadata(
        &self,
        _ctx: &RequestContext,
        _request: MetadataRequestData,
    ) -> MetadataResponseData {
        MetadataResponseData {
            brokers: vec![],
            controller_id: 0,
            topics: vec![],
            throttle_time_ms: 0,
            cluster_id: None,
            cluster_authorized_operations: 0,
        }
    }
}

// ========================================================================
// KafkaServer Creation Tests
// ========================================================================

#[tokio::test]
async fn test_kafka_server_new() {
    match KafkaServer::new("127.0.0.1:0", TestHandler).await {
        Ok(server) => {
            let addr = server.local_addr().unwrap();
            assert!(addr.port() > 0);
            server.shutdown();
        }
        Err(crate::error::Error::IoError(crate::error::PreservedIoError {
            kind: std::io::ErrorKind::PermissionDenied,
            ..
        })) => {
            // Skip test if we can't bind (CI environments may have restrictions)
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

#[tokio::test]
async fn test_kafka_server_with_config() {
    match KafkaServer::with_config("127.0.0.1:0", TestHandler, 50, 100, Handle::current()).await {
        Ok(server) => {
            let addr = server.local_addr().unwrap();
            assert!(addr.port() > 0);
            assert_eq!(server.max_connections_per_ip, 50);
            assert_eq!(server.max_total_connections, 100);
            server.shutdown();
        }
        Err(crate::error::Error::IoError(crate::error::PreservedIoError {
            kind: std::io::ErrorKind::PermissionDenied,
            ..
        })) => {
            // Skip test if we can't bind
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

#[tokio::test]
async fn test_kafka_server_unlimited_connections() {
    // max_total_connections = 0 means unlimited
    match KafkaServer::with_config("127.0.0.1:0", TestHandler, 100, 0, Handle::current()).await {
        Ok(server) => {
            assert_eq!(server.max_total_connections, 0);
            server.shutdown();
        }
        Err(crate::error::Error::IoError(crate::error::PreservedIoError {
            kind: std::io::ErrorKind::PermissionDenied,
            ..
        })) => {
            // Skip test if we can't bind
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

// ========================================================================
// KafkaServer Method Tests
// ========================================================================

#[tokio::test]
async fn test_kafka_server_local_addr() {
    match KafkaServer::new("127.0.0.1:0", TestHandler).await {
        Ok(server) => {
            let addr = server.local_addr().unwrap();
            assert_eq!(
                addr.ip(),
                std::net::IpAddr::V4(std::net::Ipv4Addr::new(127, 0, 0, 1))
            );
            assert!(addr.port() > 0);
            server.shutdown();
        }
        Err(crate::error::Error::IoError(crate::error::PreservedIoError {
            kind: std::io::ErrorKind::PermissionDenied,
            ..
        })) => {
            // Skip test if we can't bind
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

#[tokio::test]
async fn test_kafka_server_active_connections() {
    match KafkaServer::new("127.0.0.1:0", TestHandler).await {
        Ok(server) => {
            // Initially no connections
            assert_eq!(server.active_connections(), 0);
            server.shutdown();
        }
        Err(crate::error::Error::IoError(crate::error::PreservedIoError {
            kind: std::io::ErrorKind::PermissionDenied,
            ..
        })) => {
            // Skip test if we can't bind
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

#[tokio::test]
async fn test_kafka_server_auth_rate_limiter() {
    match KafkaServer::new("127.0.0.1:0", TestHandler).await {
        Ok(server) => {
            let limiter = server.auth_rate_limiter();
            // Just verify we can get the rate limiter
            assert!(Arc::strong_count(&limiter) >= 1);
            server.shutdown();
        }
        Err(crate::error::Error::IoError(crate::error::PreservedIoError {
            kind: std::io::ErrorKind::PermissionDenied,
            ..
        })) => {
            // Skip test if we can't bind
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

#[tokio::test]
async fn test_kafka_server_shutdown() {
    match KafkaServer::new("127.0.0.1:0", TestHandler).await {
        Ok(server) => {
            // Shutdown should not panic
            server.shutdown();
            // Can shutdown multiple times
            server.shutdown();
        }
        Err(crate::error::Error::IoError(crate::error::PreservedIoError {
            kind: std::io::ErrorKind::PermissionDenied,
            ..
        })) => {
            // Skip test if we can't bind
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

#[tokio::test]
async fn test_kafka_server_shutdown_and_wait_no_connections() {
    match KafkaServer::new("127.0.0.1:0", TestHandler).await {
        Ok(server) => {
            // With no connections, shutdown should complete immediately
            let drained = server
                .shutdown_and_wait(std::time::Duration::from_millis(100))
                .await;
            assert!(drained);
        }
        Err(crate::error::Error::IoError(crate::error::PreservedIoError {
            kind: std::io::ErrorKind::PermissionDenied,
            ..
        })) => {
            // Skip test if we can't bind
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

// ========================================================================
// Connection Counter Tests
// ========================================================================

#[test]
fn test_atomic_counter_increment_decrement() {
    let counter = Arc::new(AtomicUsize::new(0));

    counter.fetch_add(1, Ordering::SeqCst);
    assert_eq!(counter.load(Ordering::SeqCst), 1);

    counter.fetch_add(1, Ordering::SeqCst);
    assert_eq!(counter.load(Ordering::SeqCst), 2);

    counter.fetch_sub(1, Ordering::SeqCst);
    assert_eq!(counter.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn test_per_ip_connection_map() {
    let connections_per_ip: Arc<StdMutex<HashMap<IpAddr, usize>>> =
        Arc::new(StdMutex::new(HashMap::new()));
    let ip = "192.168.1.1".parse::<IpAddr>().unwrap();

    // Add connection
    {
        let mut counts = connections_per_ip.lock().unwrap();
        *counts.entry(ip).or_insert(0) += 1;
    }

    // Verify count
    {
        let counts = connections_per_ip.lock().unwrap();
        assert_eq!(*counts.get(&ip).unwrap_or(&0), 1);
    }

    // Add another
    {
        let mut counts = connections_per_ip.lock().unwrap();
        *counts.entry(ip).or_insert(0) += 1;
    }

    // Verify
    {
        let counts = connections_per_ip.lock().unwrap();
        assert_eq!(*counts.get(&ip).unwrap_or(&0), 2);
    }

    // Remove one
    {
        let mut counts = connections_per_ip.lock().unwrap();
        if let Some(count) = counts.get_mut(&ip) {
            *count = count.saturating_sub(1);
            if *count == 0 {
                counts.remove(&ip);
            }
        }
    }

    // Verify
    {
        let counts = connections_per_ip.lock().unwrap();
        assert_eq!(*counts.get(&ip).unwrap_or(&0), 1);
    }
}

// ========================================================================
// Slot Reservation Tests (atomic connection-limit checks)
// ========================================================================

#[tokio::test]
async fn test_reserve_slots_accepts_below_limits() {
    let active = AtomicUsize::new(0);
    let per_ip = StdMutex::new(HashMap::new());
    let ip: IpAddr = "10.0.0.1".parse().unwrap();

    let outcome = try_reserve_connection_slots(&active, &per_ip, ip, 2, 2).await;
    assert!(matches!(outcome, SlotReservation::Reserved));
    assert_eq!(active.load(Ordering::SeqCst), 1);
    assert_eq!(*per_ip.lock().unwrap().get(&ip).unwrap(), 1);
}

#[tokio::test]
async fn test_reserve_slots_rejects_at_global_limit() {
    let active = AtomicUsize::new(3);
    let per_ip = StdMutex::new(HashMap::new());
    let ip: IpAddr = "10.0.0.1".parse().unwrap();

    let outcome = try_reserve_connection_slots(&active, &per_ip, ip, 100, 3).await;
    assert!(matches!(
        outcome,
        SlotReservation::GlobalLimitReached { current: 3 }
    ));
    // Nothing incremented on rejection.
    assert_eq!(active.load(Ordering::SeqCst), 3);
    assert!(per_ip.lock().unwrap().is_empty());
}

#[tokio::test]
async fn test_reserve_slots_rolls_back_global_on_per_ip_reject() {
    let active = AtomicUsize::new(0);
    let per_ip = StdMutex::new(HashMap::new());
    let ip: IpAddr = "10.0.0.1".parse().unwrap();
    per_ip.lock().unwrap().insert(ip, 5);

    let outcome = try_reserve_connection_slots(&active, &per_ip, ip, 5, 100).await;
    assert!(matches!(
        outcome,
        SlotReservation::PerIpLimitReached { current: 5 }
    ));
    // The speculative global increment must be rolled back.
    assert_eq!(active.load(Ordering::SeqCst), 0);
    assert_eq!(*per_ip.lock().unwrap().get(&ip).unwrap(), 5);
}

#[tokio::test]
async fn test_reserve_slots_unlimited_total() {
    let active = AtomicUsize::new(usize::MAX / 2);
    let per_ip = StdMutex::new(HashMap::new());
    let ip: IpAddr = "10.0.0.1".parse().unwrap();

    // max_total_connections == 0 means unlimited.
    let outcome = try_reserve_connection_slots(&active, &per_ip, ip, 10, 0).await;
    assert!(matches!(outcome, SlotReservation::Reserved));
    assert_eq!(active.load(Ordering::SeqCst), usize::MAX / 2 + 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_reservations_never_exceed_global_limit() {
    const MAX_TOTAL: usize = 10;
    const ATTEMPTS: usize = 100;

    let active = Arc::new(AtomicUsize::new(0));
    let per_ip = Arc::new(StdMutex::new(HashMap::new()));

    let mut tasks = Vec::with_capacity(ATTEMPTS);
    for i in 0..ATTEMPTS {
        let active = active.clone();
        let per_ip = per_ip.clone();
        // Spread across IPs so the per-IP limit never interferes.
        let ip: IpAddr = format!("10.0.0.{}", i % 250 + 1).parse().unwrap();
        tasks.push(tokio::spawn(async move {
            matches!(
                try_reserve_connection_slots(&active, &per_ip, ip, ATTEMPTS, MAX_TOTAL).await,
                SlotReservation::Reserved
            )
        }));
    }

    let mut accepted = 0;
    for task in tasks {
        if task.await.unwrap() {
            accepted += 1;
        }
    }

    assert_eq!(accepted, MAX_TOTAL, "exactly max_total slots reserved");
    assert_eq!(active.load(Ordering::SeqCst), MAX_TOTAL);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_concurrent_reservations_never_exceed_per_ip_limit() {
    const MAX_PER_IP: usize = 4;
    const ATTEMPTS: usize = 64;

    let active = Arc::new(AtomicUsize::new(0));
    let per_ip = Arc::new(StdMutex::new(HashMap::new()));
    let ip: IpAddr = "10.0.0.1".parse().unwrap();

    let mut tasks = Vec::with_capacity(ATTEMPTS);
    for _ in 0..ATTEMPTS {
        let active = active.clone();
        let per_ip = per_ip.clone();
        tasks.push(tokio::spawn(async move {
            matches!(
                try_reserve_connection_slots(&active, &per_ip, ip, MAX_PER_IP, 0).await,
                SlotReservation::Reserved
            )
        }));
    }

    let mut accepted = 0;
    for task in tasks {
        if task.await.unwrap() {
            accepted += 1;
        }
    }

    assert_eq!(accepted, MAX_PER_IP, "exactly max_per_ip slots reserved");
    // Per-IP rejections must roll the global counter back.
    assert_eq!(active.load(Ordering::SeqCst), MAX_PER_IP);
    assert_eq!(*per_ip.lock().unwrap().get(&ip).unwrap(), MAX_PER_IP);
}

// ========================================================================
// Force-cancel signal tests
// ========================================================================

#[tokio::test]
async fn test_force_cancelled_resolves_on_signal() {
    let (tx, rx) = watch::channel(false);
    let waiter = tokio::spawn(force_cancelled(rx));
    tx.send(true).unwrap();
    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("force_cancelled must resolve once the signal fires")
        .unwrap();
}

#[tokio::test]
async fn test_force_cancelled_resolves_on_sender_drop() {
    let (tx, rx) = watch::channel(false);
    let waiter = tokio::spawn(force_cancelled(rx));
    drop(tx);
    tokio::time::timeout(std::time::Duration::from_secs(1), waiter)
        .await
        .expect("force_cancelled must resolve when the server is dropped")
        .unwrap();
}

#[tokio::test]
async fn test_force_cancelled_pending_without_signal() {
    let (tx, rx) = watch::channel(false);
    let result =
        tokio::time::timeout(std::time::Duration::from_millis(50), force_cancelled(rx)).await;
    assert!(result.is_err(), "must stay pending until signalled");
    drop(tx);
}

// ========================================================================
// Server Run Tests (with shutdown)
// ========================================================================

#[tokio::test]
async fn test_kafka_server_run_shutdown() {
    match KafkaServer::new("127.0.0.1:0", TestHandler).await {
        Ok(server) => {
            let server = Arc::new(server);
            let server_clone = server.clone();

            // Start server in background
            let handle = tokio::spawn(async move { server_clone.run().await });

            // Give server time to start
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;

            // Shutdown
            server.shutdown();

            // Wait for server to exit
            let result = handle.await.unwrap();
            assert!(result.is_ok());
        }
        Err(crate::error::Error::IoError(crate::error::PreservedIoError {
            kind: std::io::ErrorKind::PermissionDenied,
            ..
        })) => {
            // Skip test if we can't bind
        }
        Err(e) => panic!("Unexpected error: {:?}", e),
    }
}

// ========================================================================
// Default Constants Tests
// ========================================================================

#[test]
#[allow(clippy::assertions_on_constants)]
fn test_default_connection_limits() {
    use crate::constants::{DEFAULT_MAX_CONNECTIONS_PER_IP, DEFAULT_MAX_TOTAL_CONNECTIONS};

    // Just verify the constants are reasonable values
    assert!(DEFAULT_MAX_CONNECTIONS_PER_IP > 0);
    assert!(DEFAULT_MAX_TOTAL_CONNECTIONS > 0);
}

// ========================================================================
// Handler Trait Test
// ========================================================================

#[test]
fn test_handler_is_send_sync() {
    fn assert_send_sync<T: Send + Sync>() {}
    assert_send_sync::<TestHandler>();
}
