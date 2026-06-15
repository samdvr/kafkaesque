//! Rate limiting for authentication failures.
//!
//! This module provides protection against brute-force authentication attacks
//! by tracking failures per IP address and implementing exponential backoff.
//!
//! # Security Features
//!
//! - Tracks auth failures per IP address
//! - Implements exponential backoff after repeated failures
//! - Automatically expires old failure records
//! - Configurable thresholds and timeouts

use dashmap::DashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

/// Configuration for auth rate limiting.
#[derive(Debug, Clone)]
pub struct RateLimiterConfig {
    /// Number of failures before rate limiting kicks in.
    pub failure_threshold: u32,
    /// Base lockout duration after threshold is reached.
    pub base_lockout_duration: Duration,
    /// Maximum lockout duration (caps exponential backoff).
    pub max_lockout_duration: Duration,
    /// Time after which failure counts are reset for an IP.
    pub failure_window: Duration,
    /// Maximum number of IPs to track (prevents memory exhaustion).
    pub max_tracked_ips: usize,
}

impl Default for RateLimiterConfig {
    fn default() -> Self {
        Self {
            failure_threshold: 5,
            base_lockout_duration: Duration::from_secs(1),
            max_lockout_duration: Duration::from_secs(300), // 5 minutes
            failure_window: Duration::from_secs(600),       // 10 minutes
            max_tracked_ips: 10_000,
        }
    }
}

/// State for a single IP address.
#[derive(Debug, Clone)]
struct IpState {
    /// Number of consecutive failures.
    failure_count: u32,
    /// Time of first failure in current window.
    first_failure: Instant,
    /// Time of last failure.
    last_failure: Instant,
    /// Lockout end time (if currently locked out).
    lockout_until: Option<Instant>,
}

impl IpState {
    /// Empty placeholder used by `entry().or_insert_with` to seed a slot
    /// before the failure is actually counted. `failure_count == 0`
    /// distinguishes a freshly-inserted slot from one carrying real
    /// history.
    fn empty(now: Instant) -> Self {
        Self {
            failure_count: 0,
            first_failure: now,
            last_failure: now,
            lockout_until: None,
        }
    }
}

/// Rate limiter for authentication failures.
///
/// Thread-safe and designed for high concurrency.
pub struct AuthRateLimiter {
    /// Per-IP state. `DashMap` shards the map so concurrent updates from
    /// different IPs don't serialize through one writer; previous design
    /// took an exclusive `RwLock<HashMap>` on every `record_failure` and
    /// re-walked the whole table when at capacity.
    state: Arc<DashMap<IpAddr, IpState>>,
    /// Configuration.
    config: RateLimiterConfig,
    /// Set when a background cleanup pass is already in flight, so a
    /// second writer hitting the cap doesn't queue up another full sweep.
    cleanup_running: Arc<AtomicBool>,
}

impl AuthRateLimiter {
    /// Create a new rate limiter with default configuration.
    pub fn new() -> Self {
        Self::with_config(RateLimiterConfig::default())
    }

    /// Create a new rate limiter with custom configuration.
    pub fn with_config(config: RateLimiterConfig) -> Self {
        Self {
            state: Arc::new(DashMap::new()),
            config,
            cleanup_running: Arc::new(AtomicBool::new(false)),
        }
    }

    /// Check if an IP is currently rate-limited.
    ///
    /// Returns `Some(remaining_duration)` if the IP is locked out,
    /// `None` if the IP is allowed to attempt authentication.
    pub async fn check_rate_limit(&self, ip: IpAddr) -> Option<Duration> {
        if let Some(ip_state) = self.state.get(&ip)
            && let Some(lockout_until) = ip_state.lockout_until
        {
            let now = Instant::now();
            if now < lockout_until {
                return Some(lockout_until - now);
            }
        }
        None
    }

    /// Record an authentication failure for an IP.
    ///
    /// This may trigger rate limiting if the threshold is exceeded.
    pub async fn record_failure(&self, ip: IpAddr) {
        let now = Instant::now();
        let max_tracked = self.config.max_tracked_ips;
        let failure_threshold = self.config.failure_threshold;
        let failure_window = self.config.failure_window;
        let base_lockout = self.config.base_lockout_duration;
        let max_lockout = self.config.max_lockout_duration;

        // Hard memory bound. The async sweep below handles steady-state
        // maintenance, but it removes at most a batch per run and only one
        // sweep runs at a time — so a flood of *distinct* source IPs arriving
        // faster than entries expire could otherwise grow the map without
        // limit (the "prevents memory exhaustion" guarantee would be false).
        // Before inserting a brand-new IP at capacity, evict synchronously;
        // if eviction can't free a slot (every tracked entry is fresh), drop
        // this observation rather than grow. An untracked IP is treated as
        // "not yet rate-limited", which matches existing `check_rate_limit`
        // semantics, so the only loss is one failure data-point under a flood.
        if self.state.len() >= max_tracked && !self.state.contains_key(&ip) {
            Self::evict_to_capacity(&self.state, max_tracked, failure_window);
            if self.state.len() >= max_tracked && !self.state.contains_key(&ip) {
                tracing::warn!(
                    ip = %ip,
                    max_tracked,
                    "Auth rate-limiter at capacity; dropping new-IP failure observation"
                );
                return;
            }
        }

        let mut entry = self.state.entry(ip).or_insert_with(|| IpState::empty(now));

        if entry.failure_count == 0 {
            // Newly inserted slot — count this failure.
            entry.failure_count = 1;
            entry.first_failure = now;
            entry.last_failure = now;
            if entry.failure_count >= failure_threshold {
                entry.lockout_until = Some(now + base_lockout);
            }
        } else if now.duration_since(entry.first_failure) > failure_window {
            // Failure window expired; reset.
            entry.failure_count = 1;
            entry.first_failure = now;
            entry.last_failure = now;
            entry.lockout_until = None;
        } else {
            entry.failure_count += 1;
            entry.last_failure = now;

            if entry.failure_count >= failure_threshold {
                // Cap excess_failures to avoid the `2^32 -> 0 (cast to u32)`
                // truncation pitfall: at 32 above-threshold attempts, the
                // u64 multiplier reaches 2^32 = 4_294_967_296 which casts
                // to 0 as u32, the saturating_mul returns Duration::ZERO,
                // and the lockout vanishes — defeating the entire rate
                // limiter for a persistent attacker. Clamp to 30 so the
                // cap is reached well before any cast can saturate.
                let excess_failures = (entry.failure_count - failure_threshold).min(30);
                let multiplier = 2u64.saturating_pow(excess_failures);
                let multiplier_u32 = u32::try_from(multiplier).unwrap_or(u32::MAX);
                let lockout_duration = base_lockout.saturating_mul(multiplier_u32).min(max_lockout);
                entry.lockout_until = Some(now + lockout_duration);

                tracing::warn!(
                    ip = %ip,
                    failure_count = entry.failure_count,
                    lockout_secs = lockout_duration.as_secs(),
                    "IP rate-limited due to auth failures"
                );
            }
        }
        drop(entry);

        // If we're at or over the cap, trigger an asynchronous cleanup
        // sweep. The previous design ran the full O(n) retain inline under
        // the global write lock — distributed brute-force fanned out by
        // attacker IP made every record_failure walk 10k entries.
        if self.state.len() >= max_tracked
            && self
                .cleanup_running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
        {
            let state = Arc::clone(&self.state);
            let flag = Arc::clone(&self.cleanup_running);
            tokio::spawn(async move {
                Self::evict_to_capacity(&state, max_tracked, failure_window);
                flag.store(false, Ordering::Release);
            });
        }
    }

    /// Record a successful authentication for an IP.
    ///
    /// Clears the IP's accumulated *failure count* but does NOT lift an
    /// in-force lockout: an attacker who lands one valid credential during a
    /// spray (or who shares a NAT with a legitimate user) must not be able to
    /// reset exponential backoff and immediately resume high-rate guessing.
    /// The lockout expires on its own timer; only when no lockout is active is
    /// the entry dropped entirely.
    pub async fn record_success(&self, ip: IpAddr) {
        // Decide under the shard guard, then act after dropping it — calling
        // `state.remove(&ip)` while holding a `get_mut` guard on the same shard
        // would deadlock.
        let mut drop_entry = false;
        if let Some(mut entry) = self.state.get_mut(&ip) {
            let now = Instant::now();
            let locked = entry
                .lockout_until
                .map(|until| until > now)
                .unwrap_or(false);
            if locked {
                // Reset failure accounting so that, once the lockout expires,
                // the IP starts from a clean slate — but keep `lockout_until`
                // in force until then. `failure_count = 0` reuses the
                // freshly-inserted-slot semantics in `record_failure`.
                entry.failure_count = 0;
                entry.first_failure = now;
            } else {
                drop_entry = true;
            }
        }
        if drop_entry {
            self.state.remove(&ip);
        }
    }

    /// Evict entries to keep the tracking map hard-bounded.
    ///
    /// First drops entries past their failure window. If the map is still at
    /// or above `max_tracked` (a flood of live, distinct IPs), evicts the
    /// oldest entries in one pass down to ~90% of capacity. Removing a batch
    /// rather than a single oldest entry per run amortizes the O(n) scan
    /// across many subsequent inserts, so a sustained unique-IP flood can't
    /// outrun eviction the way the previous one-per-sweep design could.
    fn evict_to_capacity(
        state: &DashMap<IpAddr, IpState>,
        max_tracked: usize,
        failure_window: Duration,
    ) {
        let now = Instant::now();
        state.retain(|_, ip_state| now.duration_since(ip_state.last_failure) < failure_window);

        if state.len() < max_tracked {
            return;
        }

        // Target ~90% capacity so the next ~10% of inserts are scan-free.
        let target = max_tracked - max_tracked / 10;
        let to_remove = state.len().saturating_sub(target);
        if to_remove == 0 {
            return;
        }

        let mut by_age: Vec<(IpAddr, Instant)> = state
            .iter()
            .map(|e| (*e.key(), e.value().last_failure))
            .collect();
        by_age.sort_unstable_by_key(|(_, last)| *last);
        for (ip, _) in by_age.into_iter().take(to_remove) {
            state.remove(&ip);
        }
    }

    /// Get statistics about current rate limiting state.
    pub async fn stats(&self) -> RateLimiterStats {
        let now = Instant::now();
        let tracked_ips = self.state.len();
        let locked_out_ips = self
            .state
            .iter()
            .filter(|e| e.value().lockout_until.map(|t| t > now).unwrap_or(false))
            .count();

        RateLimiterStats {
            tracked_ips,
            locked_out_ips,
        }
    }
}

impl Default for AuthRateLimiter {
    fn default() -> Self {
        Self::new()
    }
}

/// Statistics about the rate limiter state.
#[derive(Debug, Clone)]
pub struct RateLimiterStats {
    /// Number of IPs currently being tracked.
    pub tracked_ips: usize,
    /// Number of IPs currently locked out.
    pub locked_out_ips: usize,
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::net::{IpAddr, Ipv4Addr};

    fn test_ip(last_octet: u8) -> IpAddr {
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, last_octet))
    }

    #[tokio::test]
    async fn test_no_rate_limit_initially() {
        let limiter = AuthRateLimiter::new();
        let ip = test_ip(1);

        assert!(limiter.check_rate_limit(ip).await.is_none());
    }

    #[tokio::test]
    async fn test_rate_limit_after_threshold() {
        let config = RateLimiterConfig {
            failure_threshold: 3,
            base_lockout_duration: Duration::from_millis(100),
            max_lockout_duration: Duration::from_secs(1),
            failure_window: Duration::from_secs(60),
            max_tracked_ips: 100,
        };
        let limiter = AuthRateLimiter::with_config(config);
        let ip = test_ip(2);

        // First failure: count = 1, no rate limit
        limiter.record_failure(ip).await;
        assert!(limiter.check_rate_limit(ip).await.is_none());

        // Second failure: count = 2, no rate limit
        limiter.record_failure(ip).await;
        assert!(limiter.check_rate_limit(ip).await.is_none());

        // Third failure: count = 3, equals threshold, triggers rate limit
        limiter.record_failure(ip).await;
        assert!(limiter.check_rate_limit(ip).await.is_some());
    }

    #[tokio::test]
    async fn test_success_clears_failures_when_not_locked_out() {
        let config = RateLimiterConfig {
            failure_threshold: 3,
            ..Default::default()
        };
        let limiter = AuthRateLimiter::with_config(config);
        let ip = test_ip(3);

        // Below threshold: tracked but not locked out.
        limiter.record_failure(ip).await;
        limiter.record_failure(ip).await;
        assert!(limiter.check_rate_limit(ip).await.is_none());
        assert_eq!(limiter.stats().await.tracked_ips, 1);

        // Success on a non-locked IP drops the entry entirely.
        limiter.record_success(ip).await;
        assert_eq!(limiter.stats().await.tracked_ips, 0);
    }

    #[tokio::test]
    async fn test_success_does_not_lift_active_lockout() {
        // Security: a single success (credential spray landing one hit, or a
        // shared-NAT legitimate login) must NOT reset an in-force lockout and
        // let the attacker resume guessing. The lockout expires on its own
        // timer instead.
        let config = RateLimiterConfig {
            failure_threshold: 2,
            base_lockout_duration: Duration::from_millis(150),
            max_lockout_duration: Duration::from_secs(1),
            failure_window: Duration::from_secs(60),
            max_tracked_ips: 100,
        };
        let limiter = AuthRateLimiter::with_config(config);
        let ip = test_ip(3);

        limiter.record_failure(ip).await;
        limiter.record_failure(ip).await;
        assert!(limiter.check_rate_limit(ip).await.is_some());

        // Success during lockout: still locked out.
        limiter.record_success(ip).await;
        assert!(
            limiter.check_rate_limit(ip).await.is_some(),
            "success must not lift an active lockout"
        );

        // Once the lockout elapses, the IP is clear again and its failure
        // count was reset (so it starts fresh, not one-below-threshold).
        tokio::time::sleep(Duration::from_millis(200)).await;
        assert!(limiter.check_rate_limit(ip).await.is_none());
        limiter.record_failure(ip).await;
        assert!(
            limiter.check_rate_limit(ip).await.is_none(),
            "post-lockout failure count should have been reset by the success"
        );
    }

    #[tokio::test]
    async fn test_rate_limiter_hard_caps_tracked_ips() {
        // A flood of distinct source IPs must not grow the map without bound.
        let config = RateLimiterConfig {
            failure_threshold: 100, // never lock out; just accumulate entries
            base_lockout_duration: Duration::from_secs(1),
            max_lockout_duration: Duration::from_secs(1),
            failure_window: Duration::from_secs(600),
            max_tracked_ips: 64,
        };
        let limiter = AuthRateLimiter::with_config(config);

        for i in 0..5000u32 {
            let octets = i.to_be_bytes();
            let ip = IpAddr::V4(Ipv4Addr::new(10, octets[1], octets[2], octets[3]));
            limiter.record_failure(ip).await;
        }

        // Hard bound holds: never exceeds the configured cap (the synchronous
        // eviction keeps the map at/below max_tracked even under a unique-IP
        // flood that outruns the async sweep).
        assert!(
            limiter.stats().await.tracked_ips <= 64,
            "tracked IPs {} exceeded the hard cap",
            limiter.stats().await.tracked_ips
        );
    }

    #[tokio::test]
    async fn test_exponential_backoff() {
        let config = RateLimiterConfig {
            failure_threshold: 1,
            base_lockout_duration: Duration::from_millis(100),
            max_lockout_duration: Duration::from_secs(10),
            failure_window: Duration::from_secs(60),
            max_tracked_ips: 100,
        };
        let limiter = AuthRateLimiter::with_config(config);
        let ip = test_ip(4);

        // First failure triggers base lockout
        limiter.record_failure(ip).await;
        let lockout1 = limiter.check_rate_limit(ip).await.unwrap();

        // Wait for lockout to expire
        tokio::time::sleep(lockout1 + Duration::from_millis(10)).await;

        // Second failure should have longer lockout
        limiter.record_failure(ip).await;
        let lockout2 = limiter.check_rate_limit(ip).await.unwrap();

        // lockout2 should be approximately 2x lockout1 (within tolerance)
        assert!(lockout2 > lockout1);
    }

    #[tokio::test]
    async fn test_stats() {
        let limiter = AuthRateLimiter::new();
        let ip1 = test_ip(10);
        let ip2 = test_ip(11);

        limiter.record_failure(ip1).await;
        limiter.record_failure(ip2).await;

        let stats = limiter.stats().await;
        assert_eq!(stats.tracked_ips, 2);
    }

    #[tokio::test]
    async fn test_multiple_ips_independent() {
        let config = RateLimiterConfig {
            failure_threshold: 2,
            base_lockout_duration: Duration::from_millis(100),
            max_lockout_duration: Duration::from_secs(1),
            failure_window: Duration::from_secs(60),
            max_tracked_ips: 100,
        };
        let limiter = AuthRateLimiter::with_config(config);

        let ip1 = test_ip(20);
        let ip2 = test_ip(21);

        // IP1 should not be rate limited
        assert!(limiter.check_rate_limit(ip1).await.is_none());

        // IP2 triggers rate limiting
        limiter.record_failure(ip2).await;
        limiter.record_failure(ip2).await;

        // IP2 is locked, IP1 is not
        assert!(limiter.check_rate_limit(ip2).await.is_some());
        assert!(limiter.check_rate_limit(ip1).await.is_none());
    }

    #[tokio::test]
    async fn test_max_lockout_cap() {
        let config = RateLimiterConfig {
            failure_threshold: 1,
            base_lockout_duration: Duration::from_millis(100),
            max_lockout_duration: Duration::from_millis(200), // Cap at 200ms
            failure_window: Duration::from_secs(60),
            max_tracked_ips: 100,
        };
        let limiter = AuthRateLimiter::with_config(config);
        let ip = test_ip(30);

        // Many failures should still cap at max_lockout_duration
        for _ in 0..10 {
            limiter.record_failure(ip).await;
        }

        // Lockout should be at most 200ms (with some tolerance)
        let lockout = limiter.check_rate_limit(ip).await.unwrap();
        assert!(lockout <= Duration::from_millis(250));
    }

    #[tokio::test]
    async fn test_default_limiter() {
        let limiter = AuthRateLimiter::default();
        let ip = test_ip(40);
        assert!(limiter.check_rate_limit(ip).await.is_none());
    }

    #[test]
    fn test_config_default_values() {
        let config = RateLimiterConfig::default();
        assert_eq!(config.failure_threshold, 5);
        assert_eq!(config.base_lockout_duration, Duration::from_secs(1));
        assert_eq!(config.max_lockout_duration, Duration::from_secs(300));
        assert_eq!(config.failure_window, Duration::from_secs(600));
        assert_eq!(config.max_tracked_ips, 10_000);
    }

    #[test]
    fn test_config_debug() {
        let config = RateLimiterConfig::default();
        let debug = format!("{:?}", config);
        assert!(debug.contains("RateLimiterConfig"));
        assert!(debug.contains("failure_threshold"));
    }

    #[test]
    fn test_config_clone() {
        let config = RateLimiterConfig {
            failure_threshold: 10,
            base_lockout_duration: Duration::from_secs(5),
            max_lockout_duration: Duration::from_secs(60),
            failure_window: Duration::from_secs(120),
            max_tracked_ips: 5000,
        };
        let cloned = config.clone();
        assert_eq!(config.failure_threshold, cloned.failure_threshold);
        assert_eq!(config.max_tracked_ips, cloned.max_tracked_ips);
    }

    #[test]
    fn test_stats_debug() {
        let stats = RateLimiterStats {
            tracked_ips: 100,
            locked_out_ips: 5,
        };
        let debug = format!("{:?}", stats);
        assert!(debug.contains("RateLimiterStats"));
        assert!(debug.contains("100"));
        assert!(debug.contains("5"));
    }

    #[test]
    fn test_stats_clone() {
        let stats = RateLimiterStats {
            tracked_ips: 50,
            locked_out_ips: 10,
        };
        let cloned = stats.clone();
        assert_eq!(stats.tracked_ips, cloned.tracked_ips);
        assert_eq!(stats.locked_out_ips, cloned.locked_out_ips);
    }

    #[tokio::test]
    async fn test_locked_out_ips_count() {
        let config = RateLimiterConfig {
            failure_threshold: 1,
            base_lockout_duration: Duration::from_secs(60), // Long lockout
            max_lockout_duration: Duration::from_secs(60),
            failure_window: Duration::from_secs(600),
            max_tracked_ips: 100,
        };
        let limiter = AuthRateLimiter::with_config(config);

        let ip1 = test_ip(50);
        let ip2 = test_ip(51);

        // Lock out both IPs
        limiter.record_failure(ip1).await;
        limiter.record_failure(ip2).await;

        let stats = limiter.stats().await;
        assert_eq!(stats.tracked_ips, 2);
        assert_eq!(stats.locked_out_ips, 2);
    }
}
