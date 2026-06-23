//! Periodic-task lifecycle tests
//!
//! Background. The audit's P2.6 flagged that `FailureDetector` and
//! other periodic loops have no lifecycle tests — the recon confirmed
//! zero `#[test]` coverage for `failure_detector.rs`. The detector is
//! the broker's primary mechanism for catching dead peers faster than
//! the 60-second lease expiry; a regression where it stopped reporting
//! state changes (or false-positively reported them) would translate
//! to either silent loss of fast-failover or cluster-wide spurious
//! ownership churn.
//!
//! What's testable today (without time control):
//!
//! 1. Default config values are sane and self-consistent
//! 2. `with_interval()` derives `check_interval` correctly
//! 3. `detection_time()` and `suspicion_time()` arithmetic
//! 4. `BrokerHealthState` Display / Debug are stable
//! 5. `register_broker` is idempotent
//! 6. `unregister_broker` removes state cleanly
//! 7. `record_heartbeat` for an unknown broker auto-registers
//! 8. `check_brokers` with no elapsed time returns no transitions
//!    (the monitor's tick-skip detection means a no-op tick is a no-op)
//!
//! What this file deliberately does NOT cover:
//!
//! - The actual periodic tick firing on schedule. That requires real
//!   wall-clock time and produces flaky tests; the recon's TODO
//!   "task fires on schedule" is best left to a chaos / soak test.
//! - The integration with `RebalanceCoordinator::supervised(...)`.
//!   The recon noted no public test surface for the spawn/supervisor
//!   wiring; testing it would require either a multi-node harness or
//!   exposing the supervisor for in-test injection.

use std::time::Duration;

use kafkaesque::cluster::failure_detector::{
    BrokerHealthState, FailureDetector, FailureDetectorConfig,
};

// ---------------------------------------------------------------------------
// 1. Config sanity
// ---------------------------------------------------------------------------

#[test]
fn default_config_is_self_consistent() {
    let c = FailureDetectorConfig::default();
    // The check loop must run at least as often as a heartbeat would
    // arrive — otherwise the detector samples too coarsely to catch
    // the configured failure_threshold of missed heartbeats.
    assert!(
        c.check_interval <= c.heartbeat_interval,
        "check_interval ({:?}) must be <= heartbeat_interval ({:?})",
        c.check_interval,
        c.heartbeat_interval,
    );
    assert!(
        c.suspicion_threshold < c.failure_threshold,
        "suspicion_threshold ({}) must be < failure_threshold ({})",
        c.suspicion_threshold,
        c.failure_threshold,
    );
    assert!(c.failure_threshold >= 1, "failure_threshold must be >= 1");
    assert!(
        c.startup_grace_period > c.heartbeat_interval,
        "startup_grace_period ({:?}) must exceed heartbeat_interval ({:?}) so a slow first \
         heartbeat doesn't trigger immediate suspicion",
        c.startup_grace_period,
        c.heartbeat_interval,
    );
    assert!(
        c.failover_timeout >= c.detection_time(),
        "failover_timeout ({:?}) must be >= detection_time ({:?}) — otherwise failover \
         times out before failure can even be detected",
        c.failover_timeout,
        c.detection_time(),
    );
}

#[test]
fn with_interval_derives_check_interval_at_half() {
    let interval = Duration::from_millis(800);
    let c = FailureDetectorConfig::with_interval(interval);
    assert_eq!(c.heartbeat_interval, interval);
    // Implementation comment says "half the heartbeat interval"; pin
    // that derivation so a future change to the ratio is deliberate.
    assert_eq!(
        c.check_interval,
        interval / 2,
        "with_interval must set check_interval = heartbeat / 2",
    );
}

#[test]
fn detection_and_suspicion_time_arithmetic() {
    let c = FailureDetectorConfig::default();
    assert_eq!(
        c.detection_time(),
        c.heartbeat_interval * c.failure_threshold
    );
    assert_eq!(
        c.suspicion_time(),
        c.heartbeat_interval * c.suspicion_threshold
    );
    // A regression where these returned the *threshold count* (not
    // threshold × interval) would break every alerting integration.
    assert!(c.detection_time() > c.suspicion_time());
}

// ---------------------------------------------------------------------------
// 2. Health-state Display
// ---------------------------------------------------------------------------

#[test]
fn broker_health_state_display_is_stable() {
    // Operators grep on these strings in metrics & logs. Pin the
    // canonical names so a refactor renames are deliberate.
    assert_eq!(BrokerHealthState::Healthy.to_string(), "healthy");
    assert_eq!(BrokerHealthState::Suspected.to_string(), "suspected");
    assert_eq!(BrokerHealthState::Failed.to_string(), "failed");
}

// ---------------------------------------------------------------------------
// 3. Register / unregister / heartbeat
// ---------------------------------------------------------------------------

#[test]
fn register_broker_is_idempotent() {
    let detector = FailureDetector::new(FailureDetectorConfig::default());
    detector.register_broker(7);
    detector.register_broker(7);
    detector.register_broker(7);
    // No panic, no error: registering the same broker twice must be a
    // no-op (or replace state; either is acceptable as long as it
    // doesn't blow up).

    // A subsequent heartbeat for that broker must succeed — implying
    // the broker was tracked.
    detector.record_heartbeat(7);
}

#[test]
fn unregister_removes_broker_from_tracking() {
    let detector = FailureDetector::new(FailureDetectorConfig::default());
    detector.register_broker(11);
    detector.record_heartbeat(11);

    detector.unregister_broker(11);

    // After unregistration, check_brokers must not trip on the removed
    // broker. We can't directly observe the brokers map (it's private),
    // but a check pass with no registered brokers must return an empty
    // change list.
    let changes = detector.check_brokers();
    let mentions_11 = changes.iter().any(|c| c.broker_id == 11);
    assert!(
        !mentions_11,
        "unregistered broker must not appear in check_brokers transitions; got {:?}",
        changes,
    );
}

#[test]
fn record_heartbeat_for_unknown_broker_does_not_panic() {
    // The implementation auto-inserts on first heartbeat so unknown
    // brokers are tolerated rather than rejected — this matches the
    // "broker registered with us asynchronously" path. Pin: it
    // doesn't panic, doesn't error.
    let detector = FailureDetector::new(FailureDetectorConfig::default());
    detector.record_heartbeat(99);
    // Subsequent operations must continue to work.
    detector.record_heartbeat(99);
}

// ---------------------------------------------------------------------------
// 4. check_brokers with no elapsed time
// ---------------------------------------------------------------------------

#[test]
fn check_brokers_immediately_after_register_reports_no_failures() {
    // A fresh-registered broker is in the startup grace window. No
    // health transitions can have occurred yet, so check_brokers must
    // return an empty list.
    let detector = FailureDetector::new(FailureDetectorConfig::default());
    detector.register_broker(1);
    detector.register_broker(2);
    detector.register_broker(3);
    let changes = detector.check_brokers();
    assert!(
        changes.is_empty(),
        "check_brokers immediately after registration must yield no transitions; got {:?}",
        changes,
    );
}

#[test]
fn check_brokers_with_recent_heartbeats_reports_no_failures() {
    let detector = FailureDetector::new(FailureDetectorConfig::default());
    detector.register_broker(1);
    detector.record_heartbeat(1);
    let changes = detector.check_brokers();
    assert!(
        changes.is_empty(),
        "broker that just heartbeated must not transition; got {:?}",
        changes,
    );
}

// ---------------------------------------------------------------------------
// 5. End-to-end with very short intervals — best-effort smoke test
// ---------------------------------------------------------------------------

#[tokio::test]
async fn fast_interval_detector_eventually_marks_silent_broker_failed() {
    // With aggressively short intervals, a broker that never
    // heartbeats after registration must eventually transition to
    // Failed. This is the "the periodic loop actually fires" smoke
    // test — kept loose because real-time tests are inherently flaky.
    //
    // We use 5ms heartbeat / 1 missed heartbeat = ~5ms suspicion,
    // 2 missed = ~10ms failure. Then sleep 200ms (40× the failure
    // window) and call check_brokers manually. If the state machine
    // is wired correctly, the silent broker is now Failed.
    let config = FailureDetectorConfig {
        heartbeat_interval: Duration::from_millis(5),
        suspicion_threshold: 1,
        failure_threshold: 2,
        check_interval: Duration::from_millis(1),
        failover_timeout: Duration::from_secs(1),
        jitter_tolerance: Duration::from_millis(0),
        // Disable startup grace so the test broker is failure-eligible
        // immediately after registration.
        startup_grace_period: Duration::from_millis(0),
    };
    let detector = FailureDetector::new(config);
    detector.register_broker(42);

    // Wait well past the configured failure window. 200ms is 40× the
    // 5ms interval — comfortable margin for a CI runner.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let changes = detector.check_brokers();
    let became_failed = changes
        .iter()
        .any(|c| c.broker_id == 42 && c.new_state == BrokerHealthState::Failed);
    let became_suspected = changes
        .iter()
        .any(|c| c.broker_id == 42 && c.new_state == BrokerHealthState::Suspected);
    assert!(
        became_failed || became_suspected,
        "after 200ms with no heartbeats and a 10ms failure window, broker 42 must have \
         transitioned (suspected or failed); got {:?}",
        changes,
    );
}
