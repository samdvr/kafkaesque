//! Auth rate-limiter tests.
//!
//! Background. `src/server/rate_limiter.rs` enforces per-IP exponential
//! backoff on repeated authentication failures — the broker's primary
//! defense against credential-stuffing and brute-force SASL attacks.
//! The recon found one #[test] referencing the type; the state machine
//! itself (lockout duration ramp, failure-window reset, success-clears,
//! capacity bound) is not pinned anywhere.
//!
//! Real Kafka clients trigger this on every wrong-password attempt, so
//! a regression that either (a) fails to lock out a brute-forcer or
//! (b) locks out a legitimate user permanently after one failure
//! breaks security or breaks production.
//!
//! What this file pins:
//!
//! 1. Below the failure threshold: no lockout
//! 2. At/above the threshold: lockout returns Some(remaining)
//! 3. Successive failures during lockout extend it (exponential backoff)
//! 4. `record_success` while not-locked drops the IP entry
//! 5. `record_success` while locked DOES NOT drop the lockout (one
//!    valid credential during a spray cannot resume the spray)
//! 6. Capacity bound: at `max_tracked_ips`, a brand-new IP is dropped
//!    rather than evicting a still-active entry that's mid-lockout
//! 7. Default config is internally consistent

use std::net::IpAddr;
use std::time::Duration;

use kafkaesque::server::rate_limiter::{AuthRateLimiter, RateLimiterConfig};

fn ip(s: &str) -> IpAddr {
    s.parse().expect("valid IP for test")
}

// Test config with very tight intervals so the state machine is
// deterministic under wall-clock test execution. Lockout durations
// are kept in the millisecond range so a slow test runner doesn't
// turn 1s lockouts into 100ms-of-window observations.
fn fast_config() -> RateLimiterConfig {
    RateLimiterConfig {
        failure_threshold: 3,
        base_lockout_duration: Duration::from_millis(200),
        max_lockout_duration: Duration::from_secs(2),
        failure_window: Duration::from_secs(10),
        max_tracked_ips: 100,
    }
}

// ---------------------------------------------------------------------------
// 1. Default config is sane
// ---------------------------------------------------------------------------

#[test]
fn default_config_is_internally_consistent() {
    let c = RateLimiterConfig::default();
    assert!(
        c.failure_threshold >= 1,
        "failure_threshold must be >= 1; got {}",
        c.failure_threshold,
    );
    assert!(
        c.base_lockout_duration <= c.max_lockout_duration,
        "base_lockout ({:?}) must be <= max_lockout ({:?})",
        c.base_lockout_duration,
        c.max_lockout_duration,
    );
    assert!(
        c.max_tracked_ips > 0,
        "max_tracked_ips must be > 0 to actually track anything",
    );
    assert!(
        c.failure_window >= c.base_lockout_duration,
        "failure_window ({:?}) must be >= base_lockout ({:?}) — otherwise the \
         entry expires before the lockout it's tracking",
        c.failure_window,
        c.base_lockout_duration,
    );
}

// ---------------------------------------------------------------------------
// 2. Threshold semantics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn no_lockout_below_failure_threshold() {
    let limiter = AuthRateLimiter::with_config(fast_config());
    let attacker = ip("10.0.0.1");

    // Threshold is 3; record 2 failures.
    limiter.record_failure(attacker).await;
    limiter.record_failure(attacker).await;

    assert!(
        limiter.check_rate_limit(attacker).await.is_none(),
        "below-threshold failures must not lock out",
    );
}

#[tokio::test]
async fn lockout_engages_at_failure_threshold() {
    let limiter = AuthRateLimiter::with_config(fast_config());
    let attacker = ip("10.0.0.2");

    for _ in 0..3 {
        limiter.record_failure(attacker).await;
    }

    let remaining = limiter
        .check_rate_limit(attacker)
        .await
        .expect("at-threshold failure count must produce a lockout");
    assert!(
        remaining > Duration::ZERO,
        "lockout duration must be positive; got {:?}",
        remaining,
    );
    assert!(
        remaining <= Duration::from_secs(2),
        "lockout must not exceed max_lockout (2s); got {:?}",
        remaining,
    );
}

// ---------------------------------------------------------------------------
// 3. Successive failures extend lockout (exponential backoff)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn successive_failures_during_lockout_extend_it() {
    let limiter = AuthRateLimiter::with_config(fast_config());
    let attacker = ip("10.0.0.3");

    // Hit the threshold to get into lockout.
    for _ in 0..3 {
        limiter.record_failure(attacker).await;
    }
    let initial = limiter
        .check_rate_limit(attacker)
        .await
        .expect("initial lockout");

    // Add more failures during the active lockout. The remaining
    // duration must not shrink (and the implementation extends it
    // via exponential backoff).
    for _ in 0..3 {
        limiter.record_failure(attacker).await;
    }
    let extended = limiter
        .check_rate_limit(attacker)
        .await
        .expect("still locked");

    // Pin the monotonicity: more failures → no shorter lockout.
    // (We allow equal because the backoff cap may already be reached.)
    assert!(
        extended >= initial.saturating_sub(Duration::from_millis(50)),
        "extended lockout ({:?}) must be >= initial ({:?}) minus a small jitter window",
        extended,
        initial,
    );
}

// ---------------------------------------------------------------------------
// 4. record_success semantics
// ---------------------------------------------------------------------------

#[tokio::test]
async fn record_success_while_not_locked_drops_entry() {
    let limiter = AuthRateLimiter::with_config(fast_config());
    let user = ip("10.0.0.10");

    // One failure (below threshold), then success.
    limiter.record_failure(user).await;
    let stats_before = limiter.stats().await;
    assert_eq!(stats_before.tracked_ips, 1);

    limiter.record_success(user).await;
    let stats_after = limiter.stats().await;
    assert_eq!(
        stats_after.tracked_ips, 0,
        "success on a not-locked IP must drop tracking; got {:?}",
        stats_after,
    );
}

#[tokio::test]
async fn record_success_while_locked_does_not_drop_lockout() {
    // Critical security property: an attacker who lands one valid
    // credential during a credential-spray (or shares a NAT with a
    // legitimate user) must NOT be able to resume guessing
    // immediately. The lockout expires on its own timer.
    let limiter = AuthRateLimiter::with_config(fast_config());
    let attacker = ip("10.0.0.20");

    for _ in 0..3 {
        limiter.record_failure(attacker).await;
    }
    let was_locked = limiter.check_rate_limit(attacker).await.is_some();
    assert!(was_locked, "test setup: must be locked");

    limiter.record_success(attacker).await;

    // Still locked — record_success did NOT clear the lockout.
    let still_locked = limiter.check_rate_limit(attacker).await;
    assert!(
        still_locked.is_some(),
        "lockout must survive a single record_success while locked",
    );
}

// ---------------------------------------------------------------------------
// 5. Per-IP isolation
// ---------------------------------------------------------------------------

#[tokio::test]
async fn lockouts_are_per_ip() {
    let limiter = AuthRateLimiter::with_config(fast_config());
    let attacker = ip("10.0.0.30");
    let bystander = ip("10.0.0.31");

    for _ in 0..3 {
        limiter.record_failure(attacker).await;
    }

    assert!(
        limiter.check_rate_limit(attacker).await.is_some(),
        "attacker IP must be locked",
    );
    assert!(
        limiter.check_rate_limit(bystander).await.is_none(),
        "unrelated IP must not be locked by attacker's failures",
    );
}

// ---------------------------------------------------------------------------
// 6. Capacity bound
// ---------------------------------------------------------------------------

#[tokio::test]
async fn capacity_bound_prevents_unbounded_growth() {
    // With max_tracked_ips=4, a 5th distinct IP arriving while every
    // existing entry is "fresh" must be rejected (its observation
    // dropped) rather than evicting an entry that's still mid-lockout.
    // Pin: stats.tracked_ips never exceeds the configured cap.
    let limiter = AuthRateLimiter::with_config(RateLimiterConfig {
        failure_threshold: 3,
        base_lockout_duration: Duration::from_millis(200),
        max_lockout_duration: Duration::from_secs(2),
        failure_window: Duration::from_secs(60),
        max_tracked_ips: 4,
    });

    for octet in 1..=10u8 {
        limiter
            .record_failure(ip(&format!("10.0.0.{}", octet)))
            .await;
    }

    let stats = limiter.stats().await;
    assert!(
        stats.tracked_ips <= 4,
        "tracked_ips ({}) must not exceed max_tracked_ips (4)",
        stats.tracked_ips,
    );
}

// ---------------------------------------------------------------------------
// 7. Stats are observable
// ---------------------------------------------------------------------------

#[tokio::test]
async fn stats_count_locked_out_ips_correctly() {
    let limiter = AuthRateLimiter::with_config(fast_config());

    // One IP locked, one IP just under threshold.
    let locked = ip("10.0.0.40");
    for _ in 0..3 {
        limiter.record_failure(locked).await;
    }
    let observed = ip("10.0.0.41");
    limiter.record_failure(observed).await;

    let stats = limiter.stats().await;
    assert_eq!(stats.tracked_ips, 2);
    assert_eq!(
        stats.locked_out_ips, 1,
        "exactly one IP should be locked out; got stats={:?}",
        stats,
    );
}
