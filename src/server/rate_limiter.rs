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

use std::collections::HashMap;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::RwLock;

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
    fn new() -> Self {
        let now = Instant::now();
        Self {
            failure_count: 1,
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
    /// Per-IP state.
    state: Arc<RwLock<HashMap<IpAddr, IpState>>>,
    /// Configuration.
    config: RateLimiterConfig,
}

impl AuthRateLimiter {
    /// Create a new rate limiter with default configuration.
    pub fn new() -> Self {
        Self::with_config(RateLimiterConfig::default())
    }

    /// Create a new rate limiter with custom configuration.
    pub fn with_config(config: RateLimiterConfig) -> Self {
        Self {
            state: Arc::new(RwLock::new(HashMap::new())),
            config,
        }
    }

    /// Check if an IP is currently rate-limited.
    ///
    /// Returns `Some(remaining_duration)` if the IP is locked out,
    /// `None` if the IP is allowed to attempt authentication.
    pub async fn check_rate_limit(&self, ip: IpAddr) -> Option<Duration> {
        let state = self.state.read().await;
        if let Some(ip_state) = state.get(&ip)
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
        let mut state = self.state.write().await;
        let now = Instant::now();

        // Clean up old entries if we're at capacity
        if state.len() >= self.config.max_tracked_ips {
            self.cleanup_old_entries(&mut state, now);
        }

        // Check if entry exists
        let is_new = !state.contains_key(&ip);
        let ip_state = state.entry(ip).or_insert_with(IpState::new);

        // For new entries, IpState::new() already sets failure_count = 1
        if is_new {
            // Don't increment - new() already set count to 1
            // Check threshold (unlikely to hit on first failure unless threshold is 1)
            if ip_state.failure_count >= self.config.failure_threshold {
                ip_state.lockout_until = Some(now + self.config.base_lockout_duration);
            }
            return;
        }

        // Check if failure window has expired
        if now.duration_since(ip_state.first_failure) > self.config.failure_window {
            // Reset the window
            ip_state.failure_count = 1;
            ip_state.first_failure = now;
            ip_state.last_failure = now;
            ip_state.lockout_until = None;
            return;
        }

        // Increment failure count for existing entry
        ip_state.failure_count += 1;
        ip_state.last_failure = now;

        // Check if we need to apply rate limiting
        if ip_state.failure_count >= self.config.failure_threshold {
            // Calculate lockout duration with exponential backoff
            let excess_failures = ip_state.failure_count - self.config.failure_threshold;
            let multiplier = 2u64.saturating_pow(excess_failures);
            let lockout_duration = self
                .config
                .base_lockout_duration
                .saturating_mul(multiplier as u32)
                .min(self.config.max_lockout_duration);

            ip_state.lockout_until = Some(now + lockout_duration);

            tracing::warn!(
                ip = %ip,
                failure_count = ip_state.failure_count,
                lockout_secs = lockout_duration.as_secs(),
                "IP rate-limited due to auth failures"
            );
        }
    }

    /// Record a successful authentication for an IP.
    ///
    /// This clears the failure count for the IP.
    pub async fn record_success(&self, ip: IpAddr) {
        let mut state = self.state.write().await;
        state.remove(&ip);
    }

    /// Clean up old entries to prevent memory exhaustion.
    fn cleanup_old_entries(&self, state: &mut HashMap<IpAddr, IpState>, now: Instant) {
        // Remove entries older than failure_window
        state.retain(|_, ip_state| {
            now.duration_since(ip_state.last_failure) < self.config.failure_window
        });

        // If still at capacity, remove oldest entries
        if state.len() >= self.config.max_tracked_ips {
            // Find the oldest entry
            let oldest_ip = state
                .iter()
                .min_by_key(|(_, s)| s.last_failure)
                .map(|(ip, _)| *ip);

            if let Some(ip) = oldest_ip {
                state.remove(&ip);
            }
        }
    }

    /// Get statistics about current rate limiting state.
    pub async fn stats(&self) -> RateLimiterStats {
        let state = self.state.read().await;
        let now = Instant::now();

        let tracked_ips = state.len();
        let locked_out_ips = state
            .values()
            .filter(|s| s.lockout_until.map(|t| t > now).unwrap_or(false))
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
    async fn test_success_clears_failures() {
        let config = RateLimiterConfig {
            failure_threshold: 2,
            ..Default::default()
        };
        let limiter = AuthRateLimiter::with_config(config);
        let ip = test_ip(3);

        // Record failures
        limiter.record_failure(ip).await;
        limiter.record_failure(ip).await;
        assert!(limiter.check_rate_limit(ip).await.is_some());

        // Success should clear
        limiter.record_success(ip).await;
        assert!(limiter.check_rate_limit(ip).await.is_none());
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
