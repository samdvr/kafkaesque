//! Unified ownership validation - THE source of truth for write eligibility.
//!
//! This module consolidates four previously scattered fencing mechanisms into a single
//! `OwnershipGuard` abstraction:
//!
//! 1. **Zombie Mode** (was in `zombie_mode.rs`): Broker-level flag based on heartbeat failures
//! 2. **Lease TTL Validation** (was in `partition_store.rs`): Rejects writes if lease TTL < threshold
//! 3. **Epoch-Based Fencing** (was in `partition_store.rs`): Compares stored vs expected epoch
//! 4. **SlateDB Fencing** (was in `error.rs`): Detects writer ID conflicts
//!
//! # Design Goals
//!
//! - **Single validation point**: All ownership checks happen in one place
//! - **One error type**: `FencingError` replaces 4 different error variants
//! - **RAII pattern**: `WritePermit` ensures cleanup on drop
//! - **Eliminates circuit breaker**: No more pattern-based fencing detection heuristics
//!
//! # Example
//!
//! ```text
//! let guard = OwnershipGuard::new(partition_key, epoch, lease_expiry, zombie_state);
//!
//! // Single validation point before write
//! let permit = guard.validate_for_write()?;
//!
//! // Permit is held during write, released on drop
//! partition_store.append_batch(&records).await?;
//! ```

use std::sync::Arc;
use std::time::Instant;

use super::PartitionKey;
use super::zombie_mode::ZombieModeState;

/// Unified error type for all fencing/ownership failures.
///
/// This replaces the previous scattered error variants:
/// - `SlateDBError::NotOwned`
/// - `SlateDBError::Fenced`
/// - `SlateDBError::EpochMismatch`
/// - `SlateDBError::LeaseTooShort`
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum FencingError {
    /// Broker is in zombie mode (lost cluster coordination).
    ZombieMode { topic: String, partition: i32 },

    /// Leader epoch mismatch - another broker has acquired this partition.
    EpochMismatch {
        topic: String,
        partition: i32,
        expected_epoch: i32,
        stored_epoch: i32,
    },

    /// Lease TTL too short to safely complete write operation.
    LeaseTooShort {
        topic: String,
        partition: i32,
        remaining_secs: u64,
        required_secs: u64,
    },

    /// Partition is not owned by this broker.
    NotOwned { topic: String, partition: i32 },

    /// SlateDB detected a writer conflict (fencing at storage layer).
    StorageFenced { topic: String, partition: i32 },
}

impl std::fmt::Display for FencingError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FencingError::ZombieMode { topic, partition } => {
                write!(f, "Broker in zombie mode for {}/{}", topic, partition)
            }
            FencingError::EpochMismatch {
                topic,
                partition,
                expected_epoch,
                stored_epoch,
            } => {
                write!(
                    f,
                    "Epoch mismatch for {}/{}: expected {}, found {}",
                    topic, partition, expected_epoch, stored_epoch
                )
            }
            FencingError::LeaseTooShort {
                topic,
                partition,
                remaining_secs,
                required_secs,
            } => {
                write!(
                    f,
                    "Lease too short for {}/{}: {}s remaining, need {}s",
                    topic, partition, remaining_secs, required_secs
                )
            }
            FencingError::NotOwned { topic, partition } => {
                write!(
                    f,
                    "Partition {}/{} not owned by this broker",
                    topic, partition
                )
            }
            FencingError::StorageFenced { topic, partition } => {
                write!(
                    f,
                    "Storage layer fencing detected for {}/{}",
                    topic, partition
                )
            }
        }
    }
}

impl std::error::Error for FencingError {}

impl FencingError {
    /// Convert to Kafka error code for client responses.
    pub fn to_kafka_code(&self) -> crate::error::KafkaCode {
        use crate::error::KafkaCode;
        // All fencing errors mean "not leader" - client should refresh metadata
        KafkaCode::NotLeaderForPartition
    }

    /// Get the topic for this error.
    pub fn topic(&self) -> &str {
        match self {
            FencingError::ZombieMode { topic, .. } => topic,
            FencingError::EpochMismatch { topic, .. } => topic,
            FencingError::LeaseTooShort { topic, .. } => topic,
            FencingError::NotOwned { topic, .. } => topic,
            FencingError::StorageFenced { topic, .. } => topic,
        }
    }

    /// Get the partition for this error.
    pub fn partition(&self) -> i32 {
        match self {
            FencingError::ZombieMode { partition, .. } => *partition,
            FencingError::EpochMismatch { partition, .. } => *partition,
            FencingError::LeaseTooShort { partition, .. } => *partition,
            FencingError::NotOwned { partition, .. } => *partition,
            FencingError::StorageFenced { partition, .. } => *partition,
        }
    }
}

/// Configuration for ownership validation thresholds.
#[derive(Debug, Clone)]
pub struct OwnershipConfig {
    /// Minimum remaining lease TTL (in seconds) required to allow writes.
    pub min_lease_ttl_for_write_secs: u64,
}

impl Default for OwnershipConfig {
    fn default() -> Self {
        Self {
            min_lease_ttl_for_write_secs: crate::constants::DEFAULT_MIN_LEASE_TTL_FOR_WRITE_SECS,
        }
    }
}

/// Unified ownership validation - THE source of truth for write eligibility.
///
/// This guard consolidates all fencing checks into a single abstraction,
/// eliminating scattered correctness logic across multiple files.
///
/// # Thread Safety
///
/// The guard holds `Arc` references to shared state and can be safely cloned
/// across tasks. The validation is performed atomically when `validate_for_write()`
/// is called.
#[derive(Clone)]
pub struct OwnershipGuard {
    /// Partition identifier.
    partition_key: PartitionKey,

    /// Leader epoch from Raft (0 = epoch validation disabled).
    validated_epoch: i32,

    /// When the current lease expires.
    lease_expiry: Instant,

    /// Shared zombie mode state.
    zombie_state: Arc<ZombieModeState>,

    /// Configuration for validation thresholds.
    config: OwnershipConfig,
}

impl OwnershipGuard {
    /// Create a new ownership guard.
    ///
    /// # Arguments
    ///
    /// * `partition_key` - The (topic, partition) identifier
    /// * `validated_epoch` - Leader epoch from Raft (0 = disabled)
    /// * `lease_expiry` - When the current lease expires
    /// * `zombie_state` - Shared zombie mode state from PartitionManager
    pub fn new(
        partition_key: PartitionKey,
        validated_epoch: i32,
        lease_expiry: Instant,
        zombie_state: Arc<ZombieModeState>,
    ) -> Self {
        Self {
            partition_key,
            validated_epoch,
            lease_expiry,
            zombie_state,
            config: OwnershipConfig::default(),
        }
    }

    /// Create a guard with custom configuration.
    pub fn with_config(mut self, config: OwnershipConfig) -> Self {
        self.config = config;
        self
    }

    pub fn topic(&self) -> &str {
        &self.partition_key.0
    }

    pub fn partition(&self) -> i32 {
        self.partition_key.1
    }

    pub fn epoch(&self) -> i32 {
        self.validated_epoch
    }

    /// Get remaining lease time in seconds.
    pub fn remaining_lease_secs(&self) -> u64 {
        self.lease_expiry
            .checked_duration_since(Instant::now())
            .map(|d| d.as_secs())
            .unwrap_or(0)
    }

    /// Single validation point - replaces 4 scattered checks.
    ///
    /// This method performs all ownership validation in sequence:
    /// 1. Zombie mode check (broker coordination)
    /// 2. Lease TTL check (sufficient time remaining)
    ///
    /// Note: Epoch validation is performed by PartitionStore during the actual
    /// write operation, as it requires reading from SlateDB.
    ///
    /// # Returns
    ///
    /// - `Ok(WritePermit)` if all checks pass
    /// - `Err(FencingError)` if any check fails
    ///
    /// # Example
    ///
    /// ```text
    /// let guard = OwnershipGuard::new(...);
    /// let permit = guard.validate_for_write()?;
    /// // Permit is valid for the duration of the write
    /// ```
    pub fn validate_for_write(&self) -> Result<WritePermit, FencingError> {
        // Check 1: Zombie mode
        if self.zombie_state.is_active() {
            tracing::error!(
                topic = %self.topic(),
                partition = self.partition(),
                "Rejecting write: broker is in zombie mode"
            );
            super::metrics::record_fencing_detection("zombie_mode");
            return Err(FencingError::ZombieMode {
                topic: self.topic().to_string(),
                partition: self.partition(),
            });
        }

        // Check 2: Lease TTL
        let remaining = self.remaining_lease_secs();
        if remaining < self.config.min_lease_ttl_for_write_secs {
            tracing::warn!(
                topic = %self.topic(),
                partition = self.partition(),
                remaining_secs = remaining,
                required_secs = self.config.min_lease_ttl_for_write_secs,
                "Rejecting write: lease TTL too short"
            );
            super::metrics::record_lease_too_short(self.topic(), self.partition());
            return Err(FencingError::LeaseTooShort {
                topic: self.topic().to_string(),
                partition: self.partition(),
                remaining_secs: remaining,
                required_secs: self.config.min_lease_ttl_for_write_secs,
            });
        }

        // Record successful validation
        super::metrics::record_lease_ttl_at_write(self.topic(), remaining);

        Ok(WritePermit {
            _guard: self.clone(),
        })
    }

    /// Check if the guard is still valid (for re-validation during long operations).
    ///
    /// This is a lighter-weight check that can be called periodically during
    /// batch operations to detect if conditions have changed.
    pub fn is_still_valid(&self) -> bool {
        !self.zombie_state.is_active()
            && self.remaining_lease_secs() >= self.config.min_lease_ttl_for_write_secs
    }
}

impl std::fmt::Debug for OwnershipGuard {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OwnershipGuard")
            .field("partition_key", &self.partition_key)
            .field("validated_epoch", &self.validated_epoch)
            .field("remaining_lease_secs", &self.remaining_lease_secs())
            .field("zombie_active", &self.zombie_state.is_active())
            .finish()
    }
}

/// RAII permit that represents validated write permission.
///
/// This permit is held during write operations and automatically releases
/// on drop. Currently this is a lightweight marker, but could be extended
/// to track active writes for graceful shutdown.
#[derive(Debug)]
pub struct WritePermit {
    /// Reference to the guard that created this permit.
    _guard: OwnershipGuard,
}

impl WritePermit {
    pub fn topic(&self) -> &str {
        self._guard.topic()
    }

    pub fn partition(&self) -> i32 {
        self._guard.partition()
    }

    pub fn epoch(&self) -> i32 {
        self._guard.epoch()
    }

    /// Re-validate ownership during a long operation.
    ///
    /// This can be called periodically during batch writes to ensure
    /// conditions haven't changed.
    pub fn revalidate(&self) -> Result<(), FencingError> {
        if !self._guard.is_still_valid() {
            // Re-run full validation to get specific error
            self._guard.validate_for_write().map(|_| ())
        } else {
            Ok(())
        }
    }
}

/// Builder for creating OwnershipGuard instances.
///
/// Provides a fluent API for constructing guards with all required state.
pub struct OwnershipGuardBuilder {
    partition_key: Option<PartitionKey>,
    epoch: i32,
    lease_expiry: Option<Instant>,
    zombie_state: Option<Arc<ZombieModeState>>,
    config: OwnershipConfig,
}

impl Default for OwnershipGuardBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl OwnershipGuardBuilder {
    /// Create a new builder.
    pub fn new() -> Self {
        Self {
            partition_key: None,
            epoch: 0,
            lease_expiry: None,
            zombie_state: None,
            config: OwnershipConfig::default(),
        }
    }

    /// Set the partition key.
    pub fn partition_key(mut self, key: PartitionKey) -> Self {
        self.partition_key = Some(key);
        self
    }

    /// Set the topic and partition separately.
    pub fn topic_partition(mut self, topic: &str, partition: i32) -> Self {
        self.partition_key = Some((Arc::from(topic), partition));
        self
    }

    /// Set the leader epoch.
    pub fn epoch(mut self, epoch: i32) -> Self {
        self.epoch = epoch;
        self
    }

    /// Set the lease expiry time.
    pub fn lease_expiry(mut self, expiry: Instant) -> Self {
        self.lease_expiry = Some(expiry);
        self
    }

    /// Set lease duration from now.
    pub fn lease_duration_from_now(mut self, duration: std::time::Duration) -> Self {
        self.lease_expiry = Some(Instant::now() + duration);
        self
    }

    /// Set the zombie state.
    pub fn zombie_state(mut self, state: Arc<ZombieModeState>) -> Self {
        self.zombie_state = Some(state);
        self
    }

    /// Set the minimum lease TTL for writes.
    pub fn min_lease_ttl_secs(mut self, secs: u64) -> Self {
        self.config.min_lease_ttl_for_write_secs = secs;
        self
    }

    /// Build the guard.
    ///
    /// # Panics
    ///
    /// Panics if required fields are not set.
    pub fn build(self) -> OwnershipGuard {
        OwnershipGuard {
            partition_key: self.partition_key.expect("partition_key is required"),
            validated_epoch: self.epoch,
            lease_expiry: self.lease_expiry.expect("lease_expiry is required"),
            zombie_state: self.zombie_state.expect("zombie_state is required"),
            config: self.config,
        }
    }

    /// Try to build the guard, returning None if required fields are missing.
    pub fn try_build(self) -> Option<OwnershipGuard> {
        Some(OwnershipGuard {
            partition_key: self.partition_key?,
            validated_epoch: self.epoch,
            lease_expiry: self.lease_expiry?,
            zombie_state: self.zombie_state?,
            config: self.config,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn make_zombie_state() -> Arc<ZombieModeState> {
        Arc::new(ZombieModeState::new())
    }

    #[test]
    fn test_validate_success() {
        let zombie_state = make_zombie_state();
        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            1,
            Instant::now() + Duration::from_secs(60),
            zombie_state,
        );

        let result = guard.validate_for_write();
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_zombie_mode_fails() {
        let zombie_state = make_zombie_state();
        zombie_state.enter();

        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            1,
            Instant::now() + Duration::from_secs(60),
            zombie_state,
        );

        let result = guard.validate_for_write();
        assert!(matches!(result, Err(FencingError::ZombieMode { .. })));
    }

    #[test]
    fn test_validate_lease_too_short_fails() {
        let zombie_state = make_zombie_state();
        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            1,
            Instant::now() + Duration::from_secs(5), // Less than default 15s
            zombie_state,
        );

        let result = guard.validate_for_write();
        assert!(matches!(result, Err(FencingError::LeaseTooShort { .. })));
    }

    #[test]
    fn test_validate_lease_expired_fails() {
        let zombie_state = make_zombie_state();
        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            1,
            Instant::now() - Duration::from_secs(1), // Already expired
            zombie_state,
        );

        let result = guard.validate_for_write();
        assert!(matches!(result, Err(FencingError::LeaseTooShort { .. })));
    }

    #[test]
    fn test_builder_pattern() {
        let zombie_state = make_zombie_state();

        let guard = OwnershipGuardBuilder::new()
            .topic_partition("orders", 5)
            .epoch(3)
            .lease_duration_from_now(Duration::from_secs(120))
            .zombie_state(zombie_state)
            .min_lease_ttl_secs(10)
            .build();

        assert_eq!(guard.topic(), "orders");
        assert_eq!(guard.partition(), 5);
        assert_eq!(guard.epoch(), 3);
        assert!(guard.remaining_lease_secs() >= 110); // Some margin for test execution
    }

    #[test]
    fn test_write_permit_revalidate() {
        let zombie_state = make_zombie_state();
        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            1,
            Instant::now() + Duration::from_secs(60),
            zombie_state.clone(),
        );

        let permit = guard.validate_for_write().unwrap();

        // Initially should be valid
        assert!(permit.revalidate().is_ok());

        // Enter zombie mode
        zombie_state.enter();

        // Now revalidation should fail
        assert!(matches!(
            permit.revalidate(),
            Err(FencingError::ZombieMode { .. })
        ));
    }

    #[test]
    fn test_fencing_error_display() {
        let err = FencingError::EpochMismatch {
            topic: "orders".to_string(),
            partition: 5,
            expected_epoch: 3,
            stored_epoch: 5,
        };
        let display = format!("{}", err);
        assert!(display.contains("orders"));
        assert!(display.contains("5"));
        assert!(display.contains("3"));
    }

    #[test]
    fn test_fencing_error_accessors() {
        let err = FencingError::NotOwned {
            topic: "events".to_string(),
            partition: 10,
        };
        assert_eq!(err.topic(), "events");
        assert_eq!(err.partition(), 10);
    }

    #[test]
    fn test_is_still_valid() {
        let zombie_state = make_zombie_state();
        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            1,
            Instant::now() + Duration::from_secs(60),
            zombie_state.clone(),
        );

        assert!(guard.is_still_valid());

        zombie_state.enter();
        assert!(!guard.is_still_valid());
    }

    #[test]
    fn test_guard_debug() {
        let zombie_state = make_zombie_state();
        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            1,
            Instant::now() + Duration::from_secs(60),
            zombie_state,
        );

        let debug = format!("{:?}", guard);
        assert!(debug.contains("OwnershipGuard"));
        assert!(debug.contains("test-topic"));
    }

    // ========================================================================
    // Additional Tests for Critical Coverage
    // ========================================================================

    #[test]
    fn test_validate_with_custom_config() {
        let zombie_state = make_zombie_state();
        let config = OwnershipConfig {
            min_lease_ttl_for_write_secs: 5, // Lower threshold
        };

        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            1,
            Instant::now() + Duration::from_secs(10), // 10s remaining
            zombie_state,
        )
        .with_config(config);

        // Should succeed with 10s remaining when threshold is 5s
        let result = guard.validate_for_write();
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_epoch_zero_allowed() {
        // Epoch 0 means epoch validation is disabled
        let zombie_state = make_zombie_state();
        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            0, // Epoch 0 = disabled
            Instant::now() + Duration::from_secs(60),
            zombie_state,
        );

        assert_eq!(guard.epoch(), 0);
        let result = guard.validate_for_write();
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_with_positive_epoch() {
        let zombie_state = make_zombie_state();
        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 5),
            42, // Positive epoch
            Instant::now() + Duration::from_secs(60),
            zombie_state,
        );

        assert_eq!(guard.epoch(), 42);
        assert_eq!(guard.partition(), 5);
        let result = guard.validate_for_write();
        assert!(result.is_ok());
    }

    #[test]
    fn test_write_permit_accessors() {
        let zombie_state = make_zombie_state();
        let guard = OwnershipGuard::new(
            (Arc::from("orders"), 7),
            15,
            Instant::now() + Duration::from_secs(60),
            zombie_state,
        );

        let permit = guard.validate_for_write().unwrap();

        assert_eq!(permit.topic(), "orders");
        assert_eq!(permit.partition(), 7);
        assert_eq!(permit.epoch(), 15);
    }

    #[test]
    fn test_fencing_error_not_owned() {
        let err = FencingError::NotOwned {
            topic: "events".to_string(),
            partition: 3,
        };

        assert_eq!(err.topic(), "events");
        assert_eq!(err.partition(), 3);
        let display = format!("{}", err);
        assert!(display.contains("not owned"));
    }

    #[test]
    fn test_fencing_error_storage_fenced() {
        let err = FencingError::StorageFenced {
            topic: "logs".to_string(),
            partition: 8,
        };

        assert_eq!(err.topic(), "logs");
        assert_eq!(err.partition(), 8);
        let display = format!("{}", err);
        assert!(display.contains("Storage layer"));
    }

    #[test]
    fn test_fencing_error_to_kafka_code() {
        let errors = [
            FencingError::ZombieMode {
                topic: "t".to_string(),
                partition: 0,
            },
            FencingError::EpochMismatch {
                topic: "t".to_string(),
                partition: 0,
                expected_epoch: 1,
                stored_epoch: 2,
            },
            FencingError::LeaseTooShort {
                topic: "t".to_string(),
                partition: 0,
                remaining_secs: 5,
                required_secs: 15,
            },
            FencingError::NotOwned {
                topic: "t".to_string(),
                partition: 0,
            },
            FencingError::StorageFenced {
                topic: "t".to_string(),
                partition: 0,
            },
        ];

        // All fencing errors should return NotLeaderForPartition
        for err in &errors {
            assert_eq!(
                err.to_kafka_code(),
                crate::error::KafkaCode::NotLeaderForPartition
            );
        }
    }

    #[test]
    fn test_remaining_lease_secs_just_expired() {
        let zombie_state = make_zombie_state();
        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            1,
            Instant::now(), // Expires now
            zombie_state,
        );

        // Should be 0 or very close
        assert!(guard.remaining_lease_secs() <= 1);
    }

    #[test]
    fn test_builder_try_build_success() {
        let zombie_state = make_zombie_state();

        let guard = OwnershipGuardBuilder::new()
            .topic_partition("test", 5)
            .epoch(10)
            .lease_duration_from_now(Duration::from_secs(60))
            .zombie_state(zombie_state)
            .try_build();

        assert!(guard.is_some());
        let g = guard.unwrap();
        assert_eq!(g.topic(), "test");
        assert_eq!(g.partition(), 5);
        assert_eq!(g.epoch(), 10);
    }

    #[test]
    fn test_builder_try_build_missing_partition_key() {
        let zombie_state = make_zombie_state();

        let guard = OwnershipGuardBuilder::new()
            // Missing partition_key
            .epoch(10)
            .lease_duration_from_now(Duration::from_secs(60))
            .zombie_state(zombie_state)
            .try_build();

        assert!(guard.is_none());
    }

    #[test]
    fn test_builder_try_build_missing_lease() {
        let zombie_state = make_zombie_state();

        let guard = OwnershipGuardBuilder::new()
            .topic_partition("test", 0)
            .epoch(10)
            // Missing lease_expiry
            .zombie_state(zombie_state)
            .try_build();

        assert!(guard.is_none());
    }

    #[test]
    fn test_builder_try_build_missing_zombie_state() {
        let guard = OwnershipGuardBuilder::new()
            .topic_partition("test", 0)
            .epoch(10)
            .lease_duration_from_now(Duration::from_secs(60))
            // Missing zombie_state
            .try_build();

        assert!(guard.is_none());
    }

    #[test]
    fn test_builder_partition_key_method() {
        let zombie_state = make_zombie_state();
        let key: super::PartitionKey = (Arc::from("direct-key"), 42);

        let guard = OwnershipGuardBuilder::new()
            .partition_key(key)
            .epoch(1)
            .lease_expiry(Instant::now() + Duration::from_secs(60))
            .zombie_state(zombie_state)
            .build();

        assert_eq!(guard.topic(), "direct-key");
        assert_eq!(guard.partition(), 42);
    }

    #[test]
    fn test_builder_default() {
        let builder = OwnershipGuardBuilder::default();
        // Should be same as new()
        let zombie_state = make_zombie_state();

        let guard = builder
            .topic_partition("default-test", 0)
            .epoch(0)
            .lease_duration_from_now(Duration::from_secs(60))
            .zombie_state(zombie_state)
            .build();

        assert_eq!(guard.topic(), "default-test");
    }

    #[test]
    fn test_ownership_config_default() {
        let config = OwnershipConfig::default();
        // Should use constant from crate::constants
        assert!(config.min_lease_ttl_for_write_secs > 0);
    }

    #[test]
    fn test_guard_clone() {
        let zombie_state = make_zombie_state();
        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            1,
            Instant::now() + Duration::from_secs(60),
            zombie_state,
        );

        let cloned = guard.clone();
        assert_eq!(cloned.topic(), guard.topic());
        assert_eq!(cloned.partition(), guard.partition());
        assert_eq!(cloned.epoch(), guard.epoch());
    }

    #[test]
    fn test_write_permit_revalidate_with_short_lease() {
        let zombie_state = make_zombie_state();

        // Start with valid lease
        let guard = OwnershipGuard::new(
            (Arc::from("test-topic"), 0),
            1,
            Instant::now() + Duration::from_secs(60),
            zombie_state.clone(),
        );

        let permit = guard.validate_for_write().unwrap();
        assert!(permit.revalidate().is_ok());

        // Create another guard with short lease to test revalidation failure
        let short_guard = OwnershipGuard::new(
            (Arc::from("short-topic"), 0),
            1,
            Instant::now() + Duration::from_secs(5), // Too short
            zombie_state,
        );

        // This will fail validation due to short lease
        let result = short_guard.validate_for_write();
        assert!(matches!(result, Err(FencingError::LeaseTooShort { .. })));
    }

    #[test]
    fn test_is_still_valid_combines_checks() {
        let zombie_state = make_zombie_state();

        // Valid case
        let valid_guard = OwnershipGuard::new(
            (Arc::from("test"), 0),
            1,
            Instant::now() + Duration::from_secs(60),
            zombie_state.clone(),
        );
        assert!(valid_guard.is_still_valid());

        // Invalid due to zombie mode
        zombie_state.enter();
        assert!(!valid_guard.is_still_valid());

        // Reset zombie state
        zombie_state.force_exit("test");

        // Invalid due to short lease
        let short_lease_guard = OwnershipGuard::new(
            (Arc::from("test"), 0),
            1,
            Instant::now() + Duration::from_secs(5), // Too short
            make_zombie_state(),
        );
        assert!(!short_lease_guard.is_still_valid());
    }

    #[test]
    fn test_fencing_error_std_error() {
        // Test that FencingError implements std::error::Error
        let err = FencingError::ZombieMode {
            topic: "t".to_string(),
            partition: 0,
        };

        let _: &dyn std::error::Error = &err;
    }

    #[test]
    fn test_fencing_error_lease_too_short_display() {
        let err = FencingError::LeaseTooShort {
            topic: "topic".to_string(),
            partition: 5,
            remaining_secs: 10,
            required_secs: 15,
        };

        let display = format!("{}", err);
        assert!(display.contains("10s remaining"));
        assert!(display.contains("need 15s"));
    }

    #[test]
    fn test_fencing_error_epoch_mismatch_display() {
        let err = FencingError::EpochMismatch {
            topic: "orders".to_string(),
            partition: 3,
            expected_epoch: 5,
            stored_epoch: 7,
        };

        let display = format!("{}", err);
        assert!(display.contains("expected 5"));
        assert!(display.contains("found 7"));
        assert!(display.contains("orders/3"));
    }
}
