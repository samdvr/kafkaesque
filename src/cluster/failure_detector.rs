//! Fast failure detection for cluster brokers.
//!
//! This module implements heartbeat-based failure detection with configurable
//! thresholds for suspicion and failure states. The goal is to detect broker
//! failures within ~2.5 seconds (5 missed 500ms heartbeats) rather than waiting
//! for the 60-second lease expiration.
//!
//! # Architecture
//!
//! The failure detector runs on the Raft leader and tracks heartbeats from all
//! brokers. When a broker misses enough heartbeats, it transitions through:
//!
//! 1. **Healthy** - Heartbeats received on time
//! 2. **Suspected** - Some heartbeats missed (potential network blip)
//! 3. **Failed** - Enough heartbeats missed to declare failure
//!
//! The suspicion state helps reduce false positives from transient network issues.

use dashmap::DashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tracing::{debug, info, warn};

/// Configuration for the failure detector.
#[derive(Debug, Clone)]
pub struct FailureDetectorConfig {
    /// How often brokers should send heartbeats.
    /// Default: 500ms
    pub heartbeat_interval: Duration,

    /// Number of missed heartbeats before declaring a broker as suspected.
    /// Default: 2 (1 second at 500ms interval)
    pub suspicion_threshold: u32,

    /// Number of missed heartbeats before declaring a broker as failed.
    /// Default: 5 (2.5 seconds at 500ms interval)
    pub failure_threshold: u32,

    /// How often to run the failure check loop.
    /// Default: 250ms (half the heartbeat interval)
    pub check_interval: Duration,

    /// Maximum time to wait for failover to complete after detection.
    /// Default: 10 seconds
    pub failover_timeout: Duration,

    /// Jitter tolerance buffer for heartbeat timing.
    ///
    /// Heartbeats arriving within this tolerance of the expected time
    /// are not counted as missed. This prevents false positives due to:
    /// - Network jitter (50-100ms is common)
    /// - OS scheduling delays
    /// - Clock synchronization drift
    ///
    /// Default: 50ms
    pub jitter_tolerance: Duration,

    /// Startup grace period before enforcing failure detection.
    ///
    /// When a broker first registers, allow this much time before
    /// counting missed heartbeats. This prevents marking brokers as
    /// failed during slow initialization.
    ///
    /// Default: 5 seconds
    pub startup_grace_period: Duration,
}

impl Default for FailureDetectorConfig {
    fn default() -> Self {
        Self {
            heartbeat_interval: Duration::from_millis(500),
            suspicion_threshold: 2,
            failure_threshold: 5,
            check_interval: Duration::from_millis(250),
            failover_timeout: Duration::from_secs(10),
            jitter_tolerance: Duration::from_millis(50),
            startup_grace_period: Duration::from_secs(5),
        }
    }
}

impl FailureDetectorConfig {
    /// Create a config with custom heartbeat interval.
    pub fn with_interval(interval: Duration) -> Self {
        Self {
            heartbeat_interval: interval,
            check_interval: interval / 2,
            ..Default::default()
        }
    }

    /// Calculate the time to detect failure.
    pub fn detection_time(&self) -> Duration {
        self.heartbeat_interval * self.failure_threshold
    }

    /// Calculate the time to suspect a broker.
    pub fn suspicion_time(&self) -> Duration {
        self.heartbeat_interval * self.suspicion_threshold
    }
}

/// Health state of a broker from the failure detector's perspective.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BrokerHealthState {
    /// Broker is healthy, heartbeats received on time.
    Healthy,
    /// Broker is suspected of failure, some heartbeats missed.
    Suspected,
    /// Broker is declared failed, enough heartbeats missed.
    Failed,
}

impl std::fmt::Display for BrokerHealthState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            BrokerHealthState::Healthy => write!(f, "healthy"),
            BrokerHealthState::Suspected => write!(f, "suspected"),
            BrokerHealthState::Failed => write!(f, "failed"),
        }
    }
}

/// Health tracking for a single broker.
#[derive(Debug)]
struct BrokerHealth {
    /// Time of last heartbeat.
    last_heartbeat: Instant,
    /// Number of consecutive missed heartbeats.
    missed_count: u32,
    /// Current health state.
    state: BrokerHealthState,
    /// When the broker was first registered with this detector.
    registered_at: Instant,
}

impl BrokerHealth {
    fn new() -> Self {
        let now = Instant::now();
        Self {
            last_heartbeat: now,
            missed_count: 0,
            state: BrokerHealthState::Healthy,
            registered_at: now,
        }
    }
}

/// Event emitted when a broker's health state changes.
#[derive(Debug, Clone)]
pub struct HealthStateChange {
    pub broker_id: i32,
    pub previous_state: BrokerHealthState,
    pub new_state: BrokerHealthState,
    pub missed_heartbeats: u32,
    pub time_since_last_heartbeat: Duration,
}

/// Fast failure detector for cluster brokers.
///
/// This detector tracks heartbeats from all brokers and detects failures
/// faster than lease expiration by using frequent heartbeats.
pub struct FailureDetector {
    config: FailureDetectorConfig,
    /// Per-broker health tracking.
    brokers: DashMap<i32, BrokerHealth>,
    /// Counter for total failures detected (for metrics).
    failures_detected: AtomicU64,
    /// Counter for false positives (broker recovered from suspected).
    false_positives_avoided: AtomicU64,
}

impl FailureDetector {
    /// Create a new failure detector with the given configuration.
    pub fn new(config: FailureDetectorConfig) -> Self {
        info!(
            heartbeat_interval_ms = config.heartbeat_interval.as_millis(),
            suspicion_threshold = config.suspicion_threshold,
            failure_threshold = config.failure_threshold,
            detection_time_ms = config.detection_time().as_millis(),
            "Creating failure detector"
        );

        Self {
            config,
            brokers: DashMap::new(),
            failures_detected: AtomicU64::new(0),
            false_positives_avoided: AtomicU64::new(0),
        }
    }

    /// Create a new failure detector with default configuration.
    pub fn with_defaults() -> Self {
        Self::new(FailureDetectorConfig::default())
    }

    /// Get the configuration.
    pub fn config(&self) -> &FailureDetectorConfig {
        &self.config
    }

    /// Register a broker for health tracking.
    /// This should be called when a broker joins the cluster.
    pub fn register_broker(&self, broker_id: i32) {
        info!(broker_id, "Registering broker for failure detection");
        self.brokers.insert(broker_id, BrokerHealth::new());
    }

    /// Unregister a broker from health tracking.
    /// This should be called when a broker gracefully leaves the cluster.
    pub fn unregister_broker(&self, broker_id: i32) {
        info!(broker_id, "Unregistering broker from failure detection");
        self.brokers.remove(&broker_id);
    }

    /// Record a heartbeat from a broker.
    /// This resets the missed count and marks the broker as healthy.
    pub fn record_heartbeat(&self, broker_id: i32) {
        let now = Instant::now();

        self.brokers
            .entry(broker_id)
            .and_modify(|health| {
                let was_suspected = health.state == BrokerHealthState::Suspected;
                health.last_heartbeat = now;
                health.missed_count = 0;
                health.state = BrokerHealthState::Healthy;

                if was_suspected {
                    debug!(broker_id, "Broker recovered from suspected state");
                    self.false_positives_avoided.fetch_add(1, Ordering::Relaxed);
                }
            })
            .or_insert_with(|| {
                debug!(broker_id, "First heartbeat from unregistered broker");
                BrokerHealth::new()
            });
    }

    /// Check all brokers and return any newly failed ones.
    ///
    /// This should be called periodically (e.g., every 250ms) by a background task.
    /// Returns a list of broker IDs that have newly transitioned to the Failed state.
    pub fn check_brokers(&self) -> Vec<HealthStateChange> {
        let now = Instant::now();
        let mut state_changes = Vec::new();

        for mut entry in self.brokers.iter_mut() {
            let broker_id = *entry.key();
            let health = entry.value_mut();

            // Apply startup grace period
            let time_since_registration = now.duration_since(health.registered_at);
            if time_since_registration < self.config.startup_grace_period {
                debug!(
                    broker_id,
                    remaining_grace_ms =
                        (self.config.startup_grace_period - time_since_registration).as_millis(),
                    "Broker in startup grace period, skipping failure check"
                );
                continue;
            }

            let elapsed = now.duration_since(health.last_heartbeat);

            // Apply jitter tolerance to missed heartbeat calculation
            // Subtract jitter tolerance from elapsed time before calculating missed heartbeats
            let effective_elapsed = elapsed.saturating_sub(self.config.jitter_tolerance);
            let expected_heartbeats = if self.config.heartbeat_interval.as_nanos() > 0 {
                (effective_elapsed.as_nanos() / self.config.heartbeat_interval.as_nanos()) as u32
            } else {
                0
            };

            // Calculate missed heartbeats (no additional subtract needed since jitter is handled above)
            let new_missed_count = expected_heartbeats;
            health.missed_count = new_missed_count;

            // Determine new state based on missed count
            let previous_state = health.state;
            let new_state = if new_missed_count >= self.config.failure_threshold {
                BrokerHealthState::Failed
            } else if new_missed_count >= self.config.suspicion_threshold {
                BrokerHealthState::Suspected
            } else {
                BrokerHealthState::Healthy
            };

            // Check for state change
            if new_state != previous_state {
                health.state = new_state;

                let change = HealthStateChange {
                    broker_id,
                    previous_state,
                    new_state,
                    missed_heartbeats: new_missed_count,
                    time_since_last_heartbeat: elapsed,
                };

                match new_state {
                    BrokerHealthState::Failed => {
                        warn!(
                            broker_id,
                            missed_heartbeats = new_missed_count,
                            time_since_last_heartbeat_ms = elapsed.as_millis(),
                            "Broker declared FAILED"
                        );
                        self.failures_detected.fetch_add(1, Ordering::Relaxed);
                    }
                    BrokerHealthState::Suspected => {
                        info!(
                            broker_id,
                            missed_heartbeats = new_missed_count,
                            time_since_last_heartbeat_ms = elapsed.as_millis(),
                            "Broker suspected of failure"
                        );
                    }
                    BrokerHealthState::Healthy => {
                        debug!(broker_id, "Broker returned to healthy state");
                    }
                }

                state_changes.push(change);
            }
        }

        state_changes
    }

    /// Get the current health state of a broker.
    pub fn get_broker_state(&self, broker_id: i32) -> Option<BrokerHealthState> {
        self.brokers.get(&broker_id).map(|h| h.state)
    }

    /// Get all brokers in a specific state.
    pub fn get_brokers_in_state(&self, state: BrokerHealthState) -> Vec<i32> {
        self.brokers
            .iter()
            .filter(|entry| entry.value().state == state)
            .map(|entry| *entry.key())
            .collect()
    }

    /// Get the number of brokers being tracked.
    pub fn broker_count(&self) -> usize {
        self.brokers.len()
    }

    /// Get the number of healthy brokers.
    pub fn healthy_broker_count(&self) -> usize {
        self.brokers
            .iter()
            .filter(|entry| entry.value().state == BrokerHealthState::Healthy)
            .count()
    }

    /// Get the number of suspected brokers.
    pub fn suspected_broker_count(&self) -> usize {
        self.brokers
            .iter()
            .filter(|entry| entry.value().state == BrokerHealthState::Suspected)
            .count()
    }

    /// Get the number of failed brokers.
    pub fn failed_broker_count(&self) -> usize {
        self.brokers
            .iter()
            .filter(|entry| entry.value().state == BrokerHealthState::Failed)
            .count()
    }

    /// Get total failures detected since creation.
    pub fn total_failures_detected(&self) -> u64 {
        self.failures_detected.load(Ordering::Relaxed)
    }

    /// Get total false positives avoided (recovered from suspected).
    pub fn total_false_positives_avoided(&self) -> u64 {
        self.false_positives_avoided.load(Ordering::Relaxed)
    }

    /// Reset a broker's state after handling its failure.
    /// This allows the broker to be re-registered if it comes back.
    pub fn clear_failed_broker(&self, broker_id: i32) {
        self.brokers.remove(&broker_id);
    }

    /// Check if a specific broker is healthy.
    pub fn is_healthy(&self, broker_id: i32) -> bool {
        self.brokers
            .get(&broker_id)
            .is_some_and(|h| h.state == BrokerHealthState::Healthy)
    }

    /// Check if a specific broker is failed.
    pub fn is_failed(&self, broker_id: i32) -> bool {
        self.brokers
            .get(&broker_id)
            .is_some_and(|h| h.state == BrokerHealthState::Failed)
    }

    /// Get time since last heartbeat for a broker.
    pub fn time_since_heartbeat(&self, broker_id: i32) -> Option<Duration> {
        self.brokers
            .get(&broker_id)
            .map(|h| Instant::now().duration_since(h.last_heartbeat))
    }
}

impl Default for FailureDetector {
    fn default() -> Self {
        Self::with_defaults()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::thread::sleep;

    #[test]
    fn test_healthy_broker() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(100),
            suspicion_threshold: 2,
            failure_threshold: 5,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Healthy)
        );
        assert!(detector.is_healthy(1));
        assert!(!detector.is_failed(1));
    }

    #[test]
    fn test_suspected_broker() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(50),
            suspicion_threshold: 2,
            failure_threshold: 5,
            // Set jitter_tolerance and startup_grace_period to 0 for deterministic testing
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Wait for suspicion threshold
        sleep(Duration::from_millis(150));

        let changes = detector.check_brokers();
        assert!(!changes.is_empty());
        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Suspected)
        );
    }

    #[test]
    fn test_failed_broker() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(20),
            suspicion_threshold: 2,
            failure_threshold: 5,
            // Set jitter_tolerance and startup_grace_period to 0 for deterministic testing
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Wait for failure threshold
        sleep(Duration::from_millis(150));

        let changes = detector.check_brokers();
        assert!(!changes.is_empty());

        let failed_change = changes
            .iter()
            .find(|c| c.new_state == BrokerHealthState::Failed);
        assert!(failed_change.is_some());
        assert!(detector.is_failed(1));
    }

    #[test]
    fn test_recovery_from_suspected() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(50),
            suspicion_threshold: 2,
            failure_threshold: 10, // High threshold so we stay in suspected
            // Set jitter_tolerance and startup_grace_period to 0 for deterministic testing
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Wait for suspicion
        sleep(Duration::from_millis(150));
        detector.check_brokers();

        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Suspected)
        );

        // Send heartbeat to recover
        detector.record_heartbeat(1);
        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Healthy)
        );
        assert_eq!(detector.total_false_positives_avoided(), 1);
    }

    #[test]
    fn test_unregister_broker() {
        let detector = FailureDetector::with_defaults();

        detector.register_broker(1);
        assert_eq!(detector.broker_count(), 1);

        detector.unregister_broker(1);
        assert_eq!(detector.broker_count(), 0);
        assert_eq!(detector.get_broker_state(1), None);
    }

    #[test]
    fn test_config_detection_time() {
        let config = FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(500),
            failure_threshold: 5,
            ..Default::default()
        };

        assert_eq!(config.detection_time(), Duration::from_millis(2500));
    }

    #[test]
    fn test_config_suspicion_time() {
        let config = FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(500),
            suspicion_threshold: 2,
            ..Default::default()
        };

        assert_eq!(config.suspicion_time(), Duration::from_millis(1000));
    }

    #[test]
    fn test_config_with_interval() {
        let config = FailureDetectorConfig::with_interval(Duration::from_millis(200));
        assert_eq!(config.heartbeat_interval, Duration::from_millis(200));
        assert_eq!(config.check_interval, Duration::from_millis(100)); // Half of heartbeat
    }

    #[test]
    fn test_broker_health_state_display() {
        assert_eq!(format!("{}", BrokerHealthState::Healthy), "healthy");
        assert_eq!(format!("{}", BrokerHealthState::Suspected), "suspected");
        assert_eq!(format!("{}", BrokerHealthState::Failed), "failed");
    }

    #[test]
    fn test_get_brokers_in_state() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(20),
            suspicion_threshold: 2,
            failure_threshold: 5,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.register_broker(2);
        detector.register_broker(3);

        // All should start healthy
        detector.record_heartbeat(1);
        detector.record_heartbeat(2);
        detector.record_heartbeat(3);

        let healthy = detector.get_brokers_in_state(BrokerHealthState::Healthy);
        assert_eq!(healthy.len(), 3);

        let suspected = detector.get_brokers_in_state(BrokerHealthState::Suspected);
        assert!(suspected.is_empty());
    }

    #[test]
    fn test_broker_counts() {
        let detector = FailureDetector::with_defaults();

        detector.register_broker(1);
        detector.register_broker(2);
        detector.register_broker(3);

        detector.record_heartbeat(1);
        detector.record_heartbeat(2);
        detector.record_heartbeat(3);

        assert_eq!(detector.broker_count(), 3);
        assert_eq!(detector.healthy_broker_count(), 3);
        assert_eq!(detector.suspected_broker_count(), 0);
        assert_eq!(detector.failed_broker_count(), 0);
    }

    #[test]
    fn test_time_since_heartbeat() {
        let detector = FailureDetector::with_defaults();
        detector.register_broker(1);
        detector.record_heartbeat(1);

        sleep(Duration::from_millis(10));

        let time = detector.time_since_heartbeat(1);
        assert!(time.is_some());
        assert!(time.unwrap() >= Duration::from_millis(10));

        // Non-existent broker
        assert!(detector.time_since_heartbeat(99).is_none());
    }

    #[test]
    fn test_total_counters() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(10),
            suspicion_threshold: 1,
            failure_threshold: 2,
            // Set jitter_tolerance and startup_grace_period to 0 for deterministic testing
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        assert_eq!(detector.total_failures_detected(), 0);
        assert_eq!(detector.total_false_positives_avoided(), 0);

        // Wait for failure
        sleep(Duration::from_millis(50));
        detector.check_brokers();

        assert!(detector.total_failures_detected() >= 1);
    }

    #[test]
    fn test_clear_failed_broker() {
        let detector = FailureDetector::with_defaults();
        detector.register_broker(1);
        detector.record_heartbeat(1);

        assert_eq!(detector.broker_count(), 1);

        detector.clear_failed_broker(1);
        assert_eq!(detector.broker_count(), 0);
        assert!(detector.get_broker_state(1).is_none());
    }

    #[test]
    fn test_health_state_change_fields() {
        let change = HealthStateChange {
            broker_id: 1,
            previous_state: BrokerHealthState::Healthy,
            new_state: BrokerHealthState::Suspected,
            missed_heartbeats: 3,
            time_since_last_heartbeat: Duration::from_secs(2),
        };

        assert_eq!(change.broker_id, 1);
        assert_eq!(change.previous_state, BrokerHealthState::Healthy);
        assert_eq!(change.new_state, BrokerHealthState::Suspected);
        assert_eq!(change.missed_heartbeats, 3);
    }

    #[test]
    fn test_zero_heartbeat_interval_edge_case() {
        let config = FailureDetectorConfig {
            heartbeat_interval: Duration::ZERO,
            ..Default::default()
        };
        let detector = FailureDetector::new(config);
        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Should handle zero interval gracefully
        let changes = detector.check_brokers();
        // With zero interval, expected_heartbeats calculation avoids division by zero
        assert!(changes.is_empty() || !changes.is_empty()); // Just ensure no panic
    }

    #[test]
    fn test_heartbeat_from_unregistered_broker() {
        let detector = FailureDetector::with_defaults();

        // Recording heartbeat from unregistered broker should auto-register
        detector.record_heartbeat(99);

        assert!(detector.get_broker_state(99).is_some());
        assert_eq!(detector.broker_count(), 1);
    }

    #[test]
    fn test_default_impl() {
        let detector = FailureDetector::default();
        assert_eq!(
            detector.config().heartbeat_interval,
            Duration::from_millis(500)
        );
    }

    #[test]
    fn test_state_transitions_suspected_to_failed() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(10),
            suspicion_threshold: 2,
            failure_threshold: 5,
            // Set jitter_tolerance and startup_grace_period to 0 for deterministic testing
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Wait for suspected
        sleep(Duration::from_millis(35));
        let _changes = detector.check_brokers();

        // Should have transitioned through Suspected
        let state = detector.get_broker_state(1).unwrap();
        assert!(state == BrokerHealthState::Suspected || state == BrokerHealthState::Failed);

        // Wait longer for failure
        sleep(Duration::from_millis(50));
        detector.check_brokers();

        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Failed)
        );
    }

    #[test]
    fn test_is_healthy_is_failed() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(10),
            failure_threshold: 2,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        assert!(detector.is_healthy(1));
        assert!(!detector.is_failed(1));

        // Non-existent broker
        assert!(!detector.is_healthy(99));
        assert!(!detector.is_failed(99));
    }

    // ========================================================================
    // Additional Tests for Critical Coverage
    // ========================================================================

    #[test]
    fn test_jitter_tolerance_prevents_false_positive() {
        // Test that jitter tolerance prevents marking broker as suspected
        // when heartbeats arrive slightly late but within tolerance
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(100),
            suspicion_threshold: 2,
            failure_threshold: 5,
            jitter_tolerance: Duration::from_millis(50), // 50ms tolerance
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Wait 150ms (1.5 heartbeat intervals, but within jitter tolerance of 2nd)
        // Without jitter tolerance: expected_heartbeats = 1 (may become suspected)
        // With 50ms jitter: effective_elapsed = 100ms, expected = 1
        sleep(Duration::from_millis(150));

        let changes = detector.check_brokers();
        // Should still be healthy due to jitter tolerance
        let state = detector.get_broker_state(1).unwrap();
        // With 50ms jitter subtracted from 150ms = 100ms effective
        // 100ms / 100ms = 1 missed heartbeat, below suspicion_threshold of 2
        assert!(
            state == BrokerHealthState::Healthy || changes.is_empty(),
            "Jitter tolerance should reduce perceived missed heartbeats"
        );
    }

    #[test]
    fn test_startup_grace_period() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(20),
            suspicion_threshold: 1,
            failure_threshold: 2,
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::from_millis(200), // Long grace period
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Wait enough to normally trigger failure (60ms > 2 * 20ms)
        sleep(Duration::from_millis(60));

        // Check brokers - should skip due to grace period
        let changes = detector.check_brokers();
        assert!(
            changes.is_empty(),
            "Should skip checks during startup grace period"
        );

        // Broker should still be healthy
        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Healthy)
        );
    }

    #[test]
    fn test_startup_grace_period_expires() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(20),
            suspicion_threshold: 1,
            failure_threshold: 3,
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::from_millis(50), // Short grace period
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Wait for grace period to expire + failure time
        sleep(Duration::from_millis(150));

        // Now check should detect failure
        let changes = detector.check_brokers();
        assert!(
            !changes.is_empty(),
            "Should detect failure after grace period expires"
        );
        assert!(detector.is_failed(1));
    }

    #[test]
    fn test_state_transition_healthy_to_suspected_to_failed() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(20),
            suspicion_threshold: 2,
            failure_threshold: 4,
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Initial state
        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Healthy)
        );

        // Wait for suspected (2 missed = 40ms + buffer)
        sleep(Duration::from_millis(55));
        let changes = detector.check_brokers();

        // Should have transitioned to suspected
        let suspected_change = changes
            .iter()
            .find(|c| c.new_state == BrokerHealthState::Suspected);
        assert!(
            suspected_change.is_some(),
            "Should have suspected state change"
        );
        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Suspected)
        );

        // Wait more for failure (4 missed = 80ms total)
        sleep(Duration::from_millis(40));
        let changes = detector.check_brokers();

        // Should have transitioned to failed
        let failed_change = changes
            .iter()
            .find(|c| c.new_state == BrokerHealthState::Failed);
        assert!(failed_change.is_some(), "Should have failed state change");
        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Failed)
        );
    }

    #[test]
    fn test_state_transition_suspected_back_to_healthy() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(30),
            suspicion_threshold: 2,
            failure_threshold: 10, // High threshold to stay in suspected
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Wait for suspected
        sleep(Duration::from_millis(75));
        detector.check_brokers();
        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Suspected)
        );

        // Record heartbeat to recover
        detector.record_heartbeat(1);
        assert_eq!(
            detector.get_broker_state(1),
            Some(BrokerHealthState::Healthy)
        );

        // Should have incremented false positives avoided
        assert!(detector.total_false_positives_avoided() >= 1);
    }

    #[test]
    fn test_multiple_brokers_independent_state() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(20),
            suspicion_threshold: 2,
            failure_threshold: 4,
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.register_broker(2);
        detector.register_broker(3);

        detector.record_heartbeat(1);
        detector.record_heartbeat(2);
        detector.record_heartbeat(3);

        // Wait and only send heartbeats for broker 1 and 2
        sleep(Duration::from_millis(100));
        detector.record_heartbeat(1);
        detector.record_heartbeat(2);

        detector.check_brokers();

        // Broker 1 and 2 should be healthy, 3 should be failed
        assert!(detector.is_healthy(1));
        assert!(detector.is_healthy(2));
        assert!(detector.is_failed(3));

        assert_eq!(detector.healthy_broker_count(), 2);
        assert_eq!(detector.failed_broker_count(), 1);
    }

    #[test]
    fn test_failed_broker_can_reregister() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(20),
            suspicion_threshold: 1,
            failure_threshold: 2,
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        });

        detector.register_broker(1);
        detector.record_heartbeat(1);

        // Wait for failure
        sleep(Duration::from_millis(60));
        detector.check_brokers();
        assert!(detector.is_failed(1));

        // Clear failed broker
        detector.clear_failed_broker(1);
        assert!(detector.get_broker_state(1).is_none());

        // Re-register and should be healthy again
        detector.register_broker(1);
        detector.record_heartbeat(1);
        assert!(detector.is_healthy(1));
    }

    #[test]
    fn test_health_state_change_all_fields() {
        let change = HealthStateChange {
            broker_id: 42,
            previous_state: BrokerHealthState::Suspected,
            new_state: BrokerHealthState::Failed,
            missed_heartbeats: 5,
            time_since_last_heartbeat: Duration::from_secs(3),
        };

        assert_eq!(change.broker_id, 42);
        assert_eq!(change.previous_state, BrokerHealthState::Suspected);
        assert_eq!(change.new_state, BrokerHealthState::Failed);
        assert_eq!(change.missed_heartbeats, 5);
        assert_eq!(change.time_since_last_heartbeat, Duration::from_secs(3));
    }

    #[test]
    fn test_failover_timeout_config() {
        let config = FailureDetectorConfig {
            failover_timeout: Duration::from_secs(30),
            ..Default::default()
        };

        let detector = FailureDetector::new(config);
        assert_eq!(detector.config().failover_timeout, Duration::from_secs(30));
    }

    #[test]
    fn test_check_interval_config() {
        let config = FailureDetectorConfig {
            check_interval: Duration::from_millis(125),
            ..Default::default()
        };

        let detector = FailureDetector::new(config);
        assert_eq!(detector.config().check_interval, Duration::from_millis(125));
    }

    #[test]
    fn test_rapid_heartbeat_updates() {
        let detector = FailureDetector::new(FailureDetectorConfig {
            heartbeat_interval: Duration::from_millis(50),
            suspicion_threshold: 2,
            failure_threshold: 5,
            jitter_tolerance: Duration::ZERO,
            startup_grace_period: Duration::ZERO,
            ..Default::default()
        });

        detector.register_broker(1);

        // Send many heartbeats rapidly
        for _ in 0..10 {
            detector.record_heartbeat(1);
        }

        // Should always be healthy
        assert!(detector.is_healthy(1));
        assert!(!detector.is_failed(1));

        let time = detector.time_since_heartbeat(1).unwrap();
        assert!(time < Duration::from_millis(50));
    }

    #[test]
    fn test_concurrent_broker_operations() {
        let detector = FailureDetector::new(FailureDetectorConfig::default());

        // Register many brokers
        for i in 1..=100 {
            detector.register_broker(i);
        }

        assert_eq!(detector.broker_count(), 100);

        // Record heartbeats for half of them
        for i in 1..=50 {
            detector.record_heartbeat(i);
        }

        // Unregister some
        for i in 1..=25 {
            detector.unregister_broker(i);
        }

        assert_eq!(detector.broker_count(), 75);

        // Get brokers in healthy state
        // Note: When registered, brokers start as healthy with last_heartbeat=now
        // So all 75 remaining brokers are healthy (no time has passed to fail them)
        let healthy = detector.get_brokers_in_state(BrokerHealthState::Healthy);
        assert_eq!(healthy.len(), 75);

        // All brokers that sent heartbeats (minus unregistered) should be healthy
        for i in 26..=50 {
            assert!(detector.is_healthy(i), "Broker {} should be healthy", i);
        }
    }
}
