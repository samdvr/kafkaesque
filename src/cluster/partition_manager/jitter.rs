//! Timing jitter helpers for background loops.
//!
//! Keeps lease-renewal / ownership sweeps from synchronizing across brokers.

use std::time::Duration;

/// Add jitter to a duration to prevent thundering herd.
///
/// Adds +/- 15% pseudo-random jitter to the base interval. This prevents all brokers
/// from sending heartbeats or renewing leases at exactly the same time after
/// a coordinator reconnection or cluster restart.
///
/// Uses `fastrand` crate for thread-local PRNG, which provides:
/// - Better entropy than system time nanoseconds
/// - Fast, non-blocking operation
/// - Automatically seeded per-thread from system entropy
///
/// # Example
/// A 10 second interval becomes anywhere from 8.5 to 11.5 seconds.
pub(super) fn with_jitter(base: Duration) -> Duration {
    // Generate random factor in range [0.85, 1.15] using fastrand
    // fastrand::f64() returns a value in [0.0, 1.0)
    let jitter_factor = 0.85 + fastrand::f64() * 0.30;

    Duration::from_secs_f64(base.as_secs_f64() * jitter_factor)
}

/// One-shot uniform offset in `[0, base)` used as a per-task initial sleep.
///
/// `with_jitter` only varies a tick by ±15%, which still synchronizes the
/// *first* tick of every loop spawned in the same instant (e.g. all lease
/// renewal loops after a rebalance). This helper spreads first ticks
/// uniformly across a full interval, eliminating the resulting hot-spot on
/// the Raft leader.
pub(super) fn initial_jitter(base: Duration) -> Duration {
    let millis = base.as_millis().min(u64::MAX as u128) as u64;
    if millis == 0 {
        return Duration::ZERO;
    }
    Duration::from_millis(fastrand::u64(0..millis))
}

/// Convert a "remaining TTL in seconds" lease window into an absolute
/// wall-clock expiry (epoch millis), suitable for publishing into the
/// partition store's `lease_expiry_ms`. Saturates the addition so a
/// malicious or buggy `remaining_secs` near `u64::MAX` cannot wrap
/// negative.
pub(super) fn wall_clock_expiry_ms(remaining_secs: u64) -> i64 {
    let now_ms = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0);
    let remaining_ms: i64 = remaining_secs.saturating_mul(1000).min(i64::MAX as u64) as i64;
    now_ms.saturating_add(remaining_ms)
}
