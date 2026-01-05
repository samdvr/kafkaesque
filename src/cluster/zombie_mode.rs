//! Zombie mode state management for split-brain prevention.
//!
//! This module provides a type-safe wrapper around the atomic flags used to track
//! zombie mode status. Zombie mode is entered when the broker loses coordination
//! with Raft (consecutive heartbeat failures), and all write operations are rejected
//! until the broker successfully re-verifies its partition leases.
//!
//! # Safety Properties
//!
//! 1. **Atomic transitions**: All state changes use `SeqCst` ordering for visibility
//! 2. **Timestamp tracking**: Entry time is recorded to detect re-entry during exit verification
//! 3. **Metrics integration**: All transitions are automatically recorded to Prometheus
//!
//! # Example
//!
//! ```rust,no_run
//! use kafkaesque::cluster::zombie_mode::ZombieModeState;
//! use std::sync::Arc;
//!
//! let zombie_state = Arc::new(ZombieModeState::new());
//!
//! // Check if active
//! if zombie_state.is_active() {
//!     println!("Broker is in zombie mode!");
//! }
//!
//! // Enter zombie mode
//! if zombie_state.enter() {
//!     println!("Entered zombie mode for the first time");
//! }
//!
//! // Try to exit (with re-entry detection)
//! let entered_at = zombie_state.entered_at();
//! // ... perform verification ...
//! if zombie_state.try_exit(entered_at, "recovered") {
//!     println!("Successfully exited zombie mode");
//! }
//! ```

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::metrics;

/// Type-safe wrapper for zombie mode state.
///
/// Encapsulates the atomic flags and timestamp tracking for zombie mode,
/// providing a safer interface than raw atomic operations scattered throughout
/// the codebase.
#[derive(Debug)]
pub struct ZombieModeState {
    /// Whether the broker is currently in zombie mode.
    active: AtomicBool,
    /// Timestamp (epoch millis) when zombie mode was entered.
    /// Used to detect re-entry during exit verification.
    entered_at_millis: AtomicU64,
}

impl Default for ZombieModeState {
    fn default() -> Self {
        Self::new()
    }
}

impl ZombieModeState {
    /// Create a new zombie mode state (not in zombie mode).
    pub fn new() -> Self {
        Self {
            active: AtomicBool::new(false),
            entered_at_millis: AtomicU64::new(0),
        }
    }

    /// Check if the broker is currently in zombie mode.
    ///
    /// This is a cheap atomic load with `SeqCst` ordering to ensure
    /// visibility across all threads.
    #[inline]
    pub fn is_active(&self) -> bool {
        self.active.load(Ordering::SeqCst)
    }

    /// Get the timestamp (epoch millis) when zombie mode was entered.
    ///
    /// Returns 0 if not in zombie mode.
    pub fn entered_at(&self) -> u64 {
        self.entered_at_millis.load(Ordering::SeqCst)
    }

    /// Enter zombie mode.
    ///
    /// This atomically sets the zombie flag and records the entry timestamp.
    /// If already in zombie mode, this is a no-op and returns `false`.
    ///
    /// # Returns
    ///
    /// `true` if this call transitioned into zombie mode (first entry),
    /// `false` if already in zombie mode.
    ///
    /// # Metrics
    ///
    /// On successful entry, records:
    /// - `kafkaesque_zombie_mode_active` gauge set to 1
    /// - `kafkaesque_zombie_mode_transitions_total{direction="enter"}` incremented
    pub fn enter(&self) -> bool {
        // swap returns the previous value; if it was false, we just entered
        if !self.active.swap(true, Ordering::SeqCst) {
            let now_millis = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .map(|d| d.as_millis() as u64)
                .unwrap_or(0);
            self.entered_at_millis.store(now_millis, Ordering::SeqCst);
            metrics::enter_zombie_mode();
            true
        } else {
            false
        }
    }

    /// Try to exit zombie mode with re-entry detection.
    ///
    /// This method performs a safe exit that detects if zombie mode was
    /// re-entered during verification. This prevents the race condition where:
    ///
    /// 1. We start verification (zombie_mode = true, entered_at = T1)
    /// 2. Heartbeat fails again, re-enters zombie mode (entered_at = T2)
    /// 3. We finish verification and try to exit
    /// 4. BAD: If we exited, we'd be operating with stale state
    ///
    /// Uses compare_exchange to atomically verify and transition state,
    /// eliminating the TOCTOU race between checking `active` and storing `false`.
    ///
    /// # Arguments
    ///
    /// * `expected_entered_at` - The timestamp captured at the start of verification
    /// * `exit_reason` - Reason for exiting (for metrics: "recovered", "manual", "shutdown")
    ///
    /// # Returns
    ///
    /// `true` if successfully exited zombie mode,
    /// `false` if:
    /// - Not currently in zombie mode
    /// - Zombie mode was re-entered during verification (timestamp mismatch)
    /// - Another thread exited concurrently (CAS failed)
    ///
    /// # Metrics
    ///
    /// On successful exit, records:
    /// - `kafkaesque_zombie_mode_active` gauge set to 0
    /// - `kafkaesque_zombie_mode_transitions_total{direction="exit"}` incremented
    /// - `kafkaesque_zombie_mode_duration_seconds{exit_reason}` histogram observation
    pub fn try_exit(&self, expected_entered_at: u64, exit_reason: &str) -> bool {
        // Check if zombie mode was re-entered during verification
        let current_entered_at = self.entered_at_millis.load(Ordering::SeqCst);
        if current_entered_at != expected_entered_at {
            // Zombie mode was re-entered - don't exit
            return false;
        }

        // Use compare_exchange to atomically check and swap.
        // This eliminates the TOCTOU race between checking `active` and storing `false`.
        // If compare_exchange fails, either:
        // 1. Another thread already exited (was false) - return false
        // 2. Never in zombie mode - return false
        match self.active.compare_exchange(
            true,  // expected: currently in zombie mode
            false, // desired: exit zombie mode
            Ordering::SeqCst,
            Ordering::SeqCst,
        ) {
            Ok(_) => {
                // Successfully transitioned from true -> false
                // Calculate duration before clearing timestamp
                let duration_secs = if current_entered_at > 0 {
                    let now_millis = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    (now_millis.saturating_sub(current_entered_at)) as f64 / 1000.0
                } else {
                    0.0
                };

                // Clear the timestamp
                self.entered_at_millis.store(0, Ordering::SeqCst);
                metrics::exit_zombie_mode(duration_secs, exit_reason);

                true
            }
            Err(_) => {
                // compare_exchange failed - was not in zombie mode or another thread exited
                false
            }
        }
    }

    /// Force exit zombie mode without re-entry detection.
    ///
    /// This is a simpler exit that doesn't check for re-entry. Use this only
    /// when you're certain no other thread could be entering zombie mode
    /// (e.g., during shutdown).
    ///
    /// # Arguments
    ///
    /// * `exit_reason` - Reason for exiting (for metrics: "recovered", "manual", "shutdown")
    ///
    /// # Returns
    ///
    /// `true` if was in zombie mode and exited,
    /// `false` if was not in zombie mode.
    pub fn force_exit(&self, exit_reason: &str) -> bool {
        if self.active.swap(false, Ordering::SeqCst) {
            let entered_at = self.entered_at_millis.swap(0, Ordering::SeqCst);
            let duration_secs = if entered_at > 0 {
                let now_millis = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                (now_millis.saturating_sub(entered_at)) as f64 / 1000.0
            } else {
                0.0
            };
            metrics::exit_zombie_mode(duration_secs, exit_reason);
            true
        } else {
            false
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_initial_state() {
        let state = ZombieModeState::new();
        assert!(!state.is_active());
        assert_eq!(state.entered_at(), 0);
    }

    #[test]
    fn test_enter_zombie_mode() {
        let state = ZombieModeState::new();

        // First entry should return true
        assert!(state.enter());
        assert!(state.is_active());
        assert!(state.entered_at() > 0);

        // Second entry should return false (already in zombie mode)
        assert!(!state.enter());
        assert!(state.is_active());
    }

    #[test]
    fn test_try_exit_success() {
        let state = ZombieModeState::new();

        // Enter zombie mode
        state.enter();
        let entered_at = state.entered_at();

        // Exit should succeed with matching timestamp
        assert!(state.try_exit(entered_at, "recovered"));
        assert!(!state.is_active());
        assert_eq!(state.entered_at(), 0);
    }

    #[test]
    fn test_try_exit_reentry_detection() {
        let state = ZombieModeState::new();

        // Enter zombie mode
        state.enter();
        let old_entered_at = state.entered_at();

        // Simulate re-entry by forcing exit and re-entering
        state.force_exit("test");
        // Small sleep to ensure different timestamp
        thread::sleep(std::time::Duration::from_millis(2));
        state.enter();

        // Try to exit with old timestamp should fail because entered_at changed
        assert!(!state.try_exit(old_entered_at, "recovered"));
        assert!(state.is_active()); // Still in zombie mode
    }

    #[test]
    fn test_try_exit_not_in_zombie_mode() {
        let state = ZombieModeState::new();

        // Try to exit when not in zombie mode should return false
        assert!(!state.try_exit(0, "recovered"));
        assert!(!state.is_active());
    }

    #[test]
    fn test_force_exit() {
        let state = ZombieModeState::new();

        // Enter zombie mode
        state.enter();
        assert!(state.is_active());

        // Force exit should succeed
        assert!(state.force_exit("shutdown"));
        assert!(!state.is_active());

        // Force exit again should return false
        assert!(!state.force_exit("shutdown"));
    }

    #[test]
    fn test_concurrent_access() {
        let state = Arc::new(ZombieModeState::new());
        let mut handles = vec![];

        // Spawn multiple threads trying to enter
        for _ in 0..10 {
            let state_clone = state.clone();
            handles.push(thread::spawn(move || state_clone.enter()));
        }

        let results: Vec<bool> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // Exactly one thread should have successfully entered first
        let first_entries: usize = results.iter().filter(|&&r| r).count();
        assert_eq!(first_entries, 1);

        // State should be in zombie mode
        assert!(state.is_active());
    }

    #[test]
    fn test_force_exit_clears_both_flag_and_timestamp() {
        let state = ZombieModeState::new();

        state.enter();
        assert!(state.is_active());
        assert!(state.entered_at() > 0);

        state.force_exit("test");

        assert!(!state.is_active());
        assert_eq!(state.entered_at(), 0, "force_exit should clear timestamp");
    }

    // ========================================================================
    // Edge Case Tests
    // ========================================================================

    #[test]
    fn test_double_enter_idempotent() {
        let state = ZombieModeState::new();

        let first = state.enter();
        let second = state.enter();

        assert!(first);
        assert!(!second);
        assert!(state.is_active());
    }

    #[test]
    fn test_double_force_exit() {
        let state = ZombieModeState::new();

        state.enter();
        let first = state.force_exit("first");
        let second = state.force_exit("second");

        assert!(first);
        assert!(!second);
        assert!(!state.is_active());
    }

    #[test]
    fn test_entered_at_zero_when_not_active() {
        let state = ZombieModeState::new();

        assert_eq!(state.entered_at(), 0);

        state.enter();
        assert!(state.entered_at() > 0);

        state.force_exit("test");
        assert_eq!(state.entered_at(), 0);
    }

    #[test]
    fn test_default_creates_inactive_state() {
        let state = ZombieModeState::default();

        assert!(!state.is_active());
        assert_eq!(state.entered_at(), 0);
    }

    #[test]
    fn test_debug_format() {
        let state = ZombieModeState::new();
        let debug_str = format!("{:?}", state);

        assert!(debug_str.contains("ZombieModeState"));
    }

    #[test]
    fn test_timestamp_increases_on_reentry() {
        let state = ZombieModeState::new();

        state.enter();
        let first_timestamp = state.entered_at();

        thread::sleep(std::time::Duration::from_millis(5));

        state.force_exit("test");
        state.enter();
        let second_timestamp = state.entered_at();

        assert!(
            second_timestamp > first_timestamp,
            "Second entry should have later timestamp"
        );
    }
}
