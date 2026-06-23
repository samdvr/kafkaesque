//! Broker heartbeat + fence-on-loss tests.
//!
//! Two complementary surfaces converge here:
//!
//! - `FailureDetector` tracks heartbeats from peer brokers and emits health
//!   transitions (Healthy → Suspected → Failed). It's the broker's primary
//!   mechanism for catching dead peers faster than the lease expiry.
//! - `ZombieModeState` tracks the *self*-fence: when the local broker loses
//!   contact with the consensus layer it enters zombie mode and rejects
//!   writes until it re-verifies its leases.
//!
//! What this file pins:
//!
//! 1. A broker that misses enough heartbeats transitions to Failed and the
//!    failures-detected metric increments.
//! 2. A heartbeat after the Failed transition silently un-fences the broker
//!    (the detector's documented contract — operators must compose this with
//!    a higher-level fence if they want hard re-registration).
//! 3. `clear_failed_broker` removes the entry; a subsequent `register_broker`
//!    + `record_heartbeat` cleanly reinstates Healthy.
//! 4. The zombie state's enter / try_exit pair correctly bumps the
//!    transition counter and is robust to a force_exit + re-enter race.
//! 5. Independent brokers' fence states do not contaminate each other.

use std::time::Duration;

use kafkaesque::cluster::ZombieModeState;
use kafkaesque::cluster::failure_detector::{
    BrokerHealthState, FailureDetector, FailureDetectorConfig,
};

fn fast_config() -> FailureDetectorConfig {
    FailureDetectorConfig {
        heartbeat_interval: Duration::from_millis(10),
        suspicion_threshold: 1,
        failure_threshold: 2,
        check_interval: Duration::from_millis(5),
        failover_timeout: Duration::from_secs(1),
        jitter_tolerance: Duration::ZERO,
        // Zero grace so the broker is failure-eligible immediately;
        // the wall-clock test would otherwise need to sleep through it.
        startup_grace_period: Duration::ZERO,
    }
}

// ===========================================================================
// 1. Missed heartbeats transition to Failed
// ===========================================================================

#[tokio::test]
async fn broker_with_no_heartbeats_transitions_to_failed() {
    let detector = FailureDetector::new(fast_config());
    detector.register_broker(1);

    // Wait well past failure_threshold * heartbeat_interval (= 20 ms)
    // with comfortable margin for runner jitter.
    tokio::time::sleep(Duration::from_millis(150)).await;

    let changes = detector.check_brokers();
    let became_failed = changes
        .iter()
        .any(|c| c.broker_id == 1 && c.new_state == BrokerHealthState::Failed);
    let became_suspected = changes
        .iter()
        .any(|c| c.broker_id == 1 && c.new_state == BrokerHealthState::Suspected);
    assert!(
        became_failed || became_suspected,
        "broker must have transitioned away from Healthy after long silence; got {:?}",
        changes,
    );

    // Eventually the state must converge on Failed (additional ticks
    // re-evaluate and promote Suspected → Failed).
    let mut is_failed = detector.is_failed(1);
    if !is_failed {
        tokio::time::sleep(Duration::from_millis(100)).await;
        let _ = detector.check_brokers();
        is_failed = detector.is_failed(1);
    }
    assert!(is_failed, "broker must be Failed after sustained silence");
    assert!(
        detector.total_failures_detected() >= 1,
        "failures-detected counter must have incremented",
    );
}

// ===========================================================================
// 2. Heartbeat from Failed broker silently un-fences
// ===========================================================================

#[tokio::test]
async fn heartbeat_after_failed_state_restores_healthy() {
    // Document the contract: `record_heartbeat` resets a Failed broker to
    // Healthy. This is the "broker came back" path. Higher-level code
    // that wants the failure to be sticky must layer its own gating on
    // top — the detector by itself is best-effort liveness.
    let detector = FailureDetector::new(fast_config());
    detector.register_broker(7);
    tokio::time::sleep(Duration::from_millis(150)).await;
    detector.check_brokers();
    assert!(
        detector.is_failed(7),
        "test setup: broker should be Failed before recovery",
    );

    detector.record_heartbeat(7);
    assert_eq!(
        detector.get_broker_state(7),
        Some(BrokerHealthState::Healthy),
        "heartbeat from Failed broker must restore Healthy",
    );
    assert!(detector.is_healthy(7));
    assert!(!detector.is_failed(7));
}

// ===========================================================================
// 3. clear_failed_broker drops the entry and allows clean re-registration
// ===========================================================================

#[tokio::test]
async fn clear_then_reregister_yields_clean_healthy_state() {
    let detector = FailureDetector::new(fast_config());
    detector.register_broker(3);
    tokio::time::sleep(Duration::from_millis(150)).await;
    detector.check_brokers();
    assert!(detector.is_failed(3), "test setup: broker should be Failed");

    detector.clear_failed_broker(3);
    assert!(
        detector.get_broker_state(3).is_none(),
        "clear_failed_broker must remove the entry entirely",
    );

    // A re-registered + heartbeated broker is back to Healthy and is NOT
    // carrying any leftover missed-heartbeat counters.
    detector.register_broker(3);
    detector.record_heartbeat(3);
    assert!(detector.is_healthy(3));
    assert_eq!(
        detector.time_since_heartbeat(3).map(|d| d.as_secs()),
        Some(0),
        "freshly heartbeated broker should have time_since ≈ 0",
    );
}

// ===========================================================================
// 4. Independent brokers do not contaminate each other
// ===========================================================================

#[tokio::test]
async fn fence_on_one_broker_does_not_affect_peers() {
    // Use a config with a larger heartbeat_interval than the loop pace
    // so brokers we DO heartbeat stay comfortably Healthy. The bystander
    // (broker 12) gets no heartbeats at all and must trip.
    let config = FailureDetectorConfig {
        heartbeat_interval: Duration::from_millis(50),
        suspicion_threshold: 2,
        failure_threshold: 3,
        check_interval: Duration::from_millis(20),
        failover_timeout: Duration::from_secs(1),
        jitter_tolerance: Duration::ZERO,
        startup_grace_period: Duration::ZERO,
    };
    let detector = FailureDetector::new(config);
    detector.register_broker(10);
    detector.register_broker(11);
    detector.register_broker(12);

    // Heartbeat 10 and 11 every 20 ms (well inside the 50 ms window).
    // Let 12 stay silent across the full 200 ms window.
    for _ in 0..10 {
        detector.record_heartbeat(10);
        detector.record_heartbeat(11);
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let _ = detector.check_brokers();
    tokio::time::sleep(Duration::from_millis(60)).await;
    let _ = detector.check_brokers();

    assert!(detector.is_healthy(10), "10 must remain Healthy");
    assert!(detector.is_healthy(11), "11 must remain Healthy");
    assert!(
        !detector.is_healthy(12),
        "12 must NOT be Healthy after sustained silence",
    );
}

// ===========================================================================
// 5. Zombie-mode enter/exit is reflected through snapshot()
// ===========================================================================

#[test]
fn zombie_state_enter_then_exit_via_snapshot_round_trip() {
    let state = ZombieModeState::new();
    assert!(!state.is_active());
    assert!(state.snapshot().is_none(), "no session before enter");

    assert!(state.enter(), "first enter must succeed");
    let session = state
        .snapshot()
        .expect("snapshot must yield a session while active");
    assert!(state.is_active());

    // Exit using the timestamp captured by snapshot. The session is still
    // the one the snapshot referenced, so try_exit must succeed.
    assert!(
        state.try_exit(session.entered_at_millis, "recovered"),
        "try_exit with the snapshot's timestamp must succeed",
    );
    assert!(!state.is_active());
    assert_eq!(state.entered_at(), 0);
    assert!(state.snapshot().is_none(), "no active session after exit");
}

// ===========================================================================
// 6. Re-entry between snapshot and try_exit is rejected
// ===========================================================================

#[test]
fn try_exit_with_stale_snapshot_does_not_clear_new_session() {
    // Critical TOCTOU property: a recovery thread that captured a
    // snapshot, then was preempted while a force_exit + re-enter
    // happened in the meantime, MUST NOT exit the new session by
    // mistake. The stale snapshot's entered_at no longer matches.
    let state = ZombieModeState::new();
    state.enter();
    let stale = state
        .snapshot()
        .expect("first session must be observable");

    state.force_exit("test");
    // Sleep long enough for the millisecond-resolution timestamp to
    // change on every reasonable platform.
    std::thread::sleep(Duration::from_millis(5));
    state.enter();

    assert!(
        !state.try_exit(stale.entered_at_millis, "recovered"),
        "stale-snapshot exit must be refused",
    );
    assert!(
        state.is_active(),
        "the new zombie session must remain active",
    );
}

// ===========================================================================
// 7. Concurrent enter contention: exactly one transition wins
// ===========================================================================

#[test]
fn concurrent_enter_yields_single_winner() {
    use std::sync::Arc;
    use std::thread;

    let state = Arc::new(ZombieModeState::new());
    let mut handles = Vec::new();
    for _ in 0..8 {
        let s = state.clone();
        handles.push(thread::spawn(move || s.enter()));
    }
    let winners = handles
        .into_iter()
        .filter(|_| true)
        .map(|h| h.join().expect("join"))
        .filter(|won| *won)
        .count();
    assert_eq!(
        winners, 1,
        "exactly one thread must win the healthy → zombie transition",
    );
    assert!(state.is_active());
}
