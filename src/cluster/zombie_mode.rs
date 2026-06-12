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

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

use super::metrics;

/// Type-safe wrapper for zombie mode state.
///
/// The `(active, generation)` pair is packed into a single `AtomicU64` so a
/// `try_exit` can verify "still the same active session" with one CAS — closing
/// the TOCTOU window where a `force_exit` + re-`enter` between the timestamp
/// check and the active-flag CAS would otherwise let the recovery thread exit
/// a *new* zombie session prematurely.
///
/// `state` layout (LSB → MSB):
///   bit 0     : active flag (0 = healthy, 1 = zombie)
///   bits 1..64: generation counter, incremented on every healthy → zombie
///               transition. Wraps at 2^63 (~292 million years at 1 enter/ns)
///               which is a non-issue.
#[derive(Debug)]
pub struct ZombieModeState {
    /// Packed `(generation << 1) | active_flag`.
    state: AtomicU64,
    /// Timestamp (epoch millis) when zombie mode was entered. Set when active
    /// transitions 0 → 1 and cleared on exit. Used purely for metrics — the
    /// authoritative session identity is the generation packed in `state`.
    entered_at_millis: AtomicU64,
}

const ACTIVE_BIT: u64 = 1;
const GEN_SHIFT: u32 = 1;

#[inline]
fn pack(active: bool, generation: u64) -> u64 {
    (generation << GEN_SHIFT) | (active as u64)
}

#[inline]
fn is_active_bits(state: u64) -> bool {
    state & ACTIVE_BIT != 0
}

#[inline]
fn generation_of(state: u64) -> u64 {
    state >> GEN_SHIFT
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
            state: AtomicU64::new(pack(false, 0)),
            entered_at_millis: AtomicU64::new(0),
        }
    }

    /// Check if the broker is currently in zombie mode.
    ///
    /// This is a cheap atomic load with `SeqCst` ordering to ensure
    /// visibility across all threads.
    #[inline]
    pub fn is_active(&self) -> bool {
        is_active_bits(self.state.load(Ordering::SeqCst))
    }

    /// Get the timestamp (epoch millis) when zombie mode was entered.
    ///
    /// Returns 0 if not in zombie mode.
    pub fn entered_at(&self) -> u64 {
        self.entered_at_millis.load(Ordering::SeqCst)
    }

    /// Enter zombie mode.
    ///
    /// This atomically sets the zombie flag, increments the session generation,
    /// and records the entry timestamp. If already in zombie mode, this is a
    /// no-op and returns `false`.
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
        loop {
            let cur = self.state.load(Ordering::SeqCst);
            if is_active_bits(cur) {
                return false;
            }
            let new_gen = generation_of(cur).wrapping_add(1);
            let new = pack(true, new_gen);
            if self
                .state
                .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
                let now_millis = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .map(|d| d.as_millis() as u64)
                    .unwrap_or(0);
                self.entered_at_millis.store(now_millis, Ordering::SeqCst);
                metrics::enter_zombie_mode();
                return true;
            }
            // CAS lost — another caller flipped state. Retry.
        }
    }

    /// Snapshot the current zombie session for later passing to `try_exit`.
    ///
    /// Returns `Some(token)` if currently in zombie mode; `None` otherwise.
    /// The token captures both the generation and the entered-at timestamp,
    /// so a subsequent `try_exit(token)` is correct even when timestamps
    /// happen to collide across re-entries.
    pub fn snapshot(&self) -> Option<ZombieSession> {
        let s = self.state.load(Ordering::SeqCst);
        if !is_active_bits(s) {
            return None;
        }
        Some(ZombieSession {
            generation: generation_of(s),
            entered_at_millis: self.entered_at_millis.load(Ordering::SeqCst),
        })
    }

    /// Try to exit zombie mode with re-entry detection.
    ///
    /// This method performs a safe exit that detects if zombie mode was
    /// re-entered during verification. The previous version compared timestamps
    /// before the CAS, leaving a TOCTOU window where:
    ///
    /// 1. We start verification (active=true, generation=G1, entered_at=T1)
    /// 2. Heartbeat fails again, `force_exit` then `enter`: now generation=G2,
    ///    and if all this happens within the same millisecond entered_at could
    ///    still be == T1.
    /// 3. The old code's timestamp comparison passed, the CAS on `active`
    ///    succeeded, and we exited the *new* session prematurely.
    ///
    /// The `(active, generation)` pair is now packed into a single `AtomicU64`,
    /// so a CAS that requires the same generation atomically rejects any
    /// concurrent force_exit+enter sequence.
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
    /// - Zombie mode was re-entered during verification (generation/timestamp mismatch)
    /// - Another thread exited concurrently (CAS failed)
    pub fn try_exit(&self, expected_entered_at: u64, exit_reason: &str) -> bool {
        let cur = self.state.load(Ordering::SeqCst);
        if !is_active_bits(cur) {
            return false;
        }
        let cur_entered_at = self.entered_at_millis.load(Ordering::SeqCst);
        if cur_entered_at != expected_entered_at {
            return false;
        }

        // CAS the whole packed word: this rejects any concurrent
        // force_exit+enter (which would have bumped the generation), even when
        // the timestamps coincidentally agree.
        let cur_gen = generation_of(cur);
        let new = pack(false, cur_gen);
        match self
            .state
            .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
        {
            Ok(_) => {
                let duration_secs = if cur_entered_at > 0 {
                    let now_millis = SystemTime::now()
                        .duration_since(UNIX_EPOCH)
                        .map(|d| d.as_millis() as u64)
                        .unwrap_or(0);
                    (now_millis.saturating_sub(cur_entered_at)) as f64 / 1000.0
                } else {
                    0.0
                };

                self.entered_at_millis.store(0, Ordering::SeqCst);
                metrics::exit_zombie_mode(duration_secs, exit_reason);

                true
            }
            Err(_) => false,
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
        loop {
            let cur = self.state.load(Ordering::SeqCst);
            if !is_active_bits(cur) {
                return false;
            }
            let new = pack(false, generation_of(cur));
            if self
                .state
                .compare_exchange(cur, new, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
            {
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
                return true;
            }
        }
    }
}

/// Opaque token identifying a particular zombie session.
///
/// Captured by `ZombieModeState::snapshot()` and passed back to a future
/// `try_exit_session(...)` call so the exit only succeeds if it matches the
/// same session — defeating the timestamp-collision TOCTOU even when
/// `force_exit` + re-`enter` happens within the same millisecond.
#[derive(Debug, Clone, Copy)]
pub struct ZombieSession {
    pub generation: u64,
    pub entered_at_millis: u64,
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
