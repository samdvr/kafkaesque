//! Partition state machine for tracking partition lifecycle.
//!
//! This module provides an explicit state machine for partition ownership transitions:
//! - Unowned: Partition is not owned by this broker
//! - Acquiring: Attempting to acquire ownership (lease obtained, opening SlateDB)
//! - Owned: Successfully acquired, SlateDB store is open and ready
//! - Releasing: Releasing ownership (flushing, closing store, releasing lease)
//! - Fenced: Another broker took ownership (fenced out)
//!
//! # Integration
//!
//! This state machine is fully integrated with `PartitionManager`. All partition ownership
//! is tracked using `PartitionState` enum, providing:
//! - Type-safe state representation with explicit lifecycle tracking
//! - Built-in timing information (duration in each state)
//! - Clear ownership semantics via `is_owned()`, `is_transitioning()`, `is_fenced()`
//!
//! # State Transitions
//!
//! ```text
//! Unowned -> Acquiring -> Owned -> (rebalance) -> Unowned (SlateDB closed immediately)
//!                  |         |
//!                  v         v
//!               (fail)    Fenced -> Unowned
//! ```
//!
//! When a partition is rebalanced to another broker, SlateDB is closed immediately
//! to prevent the background compactor from panicking when the new broker opens
//! and fences the old writer. The lease is allowed to expire naturally.

use std::fmt;
use std::sync::Arc;
use std::time::Instant;

use super::partition_store::PartitionStore;

/// State of a partition from this broker's perspective.
#[derive(Default)]
pub enum PartitionState {
    /// Partition is not owned by this broker.
    #[default]
    Unowned,

    /// Attempting to acquire ownership.
    /// Lease may have been obtained, SlateDB is being opened.
    Acquiring {
        /// When we started the acquisition attempt.
        started_at: Instant,
    },

    /// Successfully acquired and actively owned.
    Owned {
        /// The open partition store.
        store: Arc<PartitionStore>,
        /// When we acquired ownership.
        acquired_at: Instant,
    },

    /// Draining: partition is being rebalanced to another broker.
    /// We stop renewing the lease but keep SlateDB open until the lease expires.
    /// This ensures a graceful handoff without racing with the new owner.
    Draining {
        /// The open partition store (still serving reads).
        store: Arc<PartitionStore>,
        /// When we started draining.
        started_at: Instant,
        /// When the lease is expected to expire (so we know when to release).
        lease_expires_at: Instant,
    },

    /// Releasing ownership (flushing, closing store, releasing lease).
    Releasing {
        /// When we started releasing.
        started_at: Instant,
    },

    /// Another broker took ownership (we were fenced out).
    Fenced {
        /// When we detected the fencing.
        fenced_at: Instant,
    },
}

impl fmt::Debug for PartitionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            PartitionState::Unowned => write!(f, "Unowned"),
            PartitionState::Acquiring { started_at } => f
                .debug_struct("Acquiring")
                .field("duration", &started_at.elapsed())
                .finish(),
            PartitionState::Owned { acquired_at, .. } => f
                .debug_struct("Owned")
                .field("duration", &acquired_at.elapsed())
                .finish(),
            PartitionState::Draining {
                started_at,
                lease_expires_at,
                ..
            } => f
                .debug_struct("Draining")
                .field("duration", &started_at.elapsed())
                .field(
                    "lease_remaining",
                    &lease_expires_at.saturating_duration_since(Instant::now()),
                )
                .finish(),
            PartitionState::Releasing { started_at } => f
                .debug_struct("Releasing")
                .field("duration", &started_at.elapsed())
                .finish(),
            PartitionState::Fenced { fenced_at } => f
                .debug_struct("Fenced")
                .field("duration", &fenced_at.elapsed())
                .finish(),
        }
    }
}

impl PartitionState {
    /// Create a new unowned state.
    pub fn unowned() -> Self {
        PartitionState::Unowned
    }

    /// Transition to acquiring state.
    pub fn start_acquiring() -> Self {
        PartitionState::Acquiring {
            started_at: Instant::now(),
        }
    }

    /// Transition to owned state with the given store.
    pub fn acquired(store: Arc<PartitionStore>) -> Self {
        PartitionState::Owned {
            store,
            acquired_at: Instant::now(),
        }
    }

    /// Transition to draining state (partition is being rebalanced away).
    /// The store is kept open and the lease_expires_at indicates when we can release.
    pub fn start_draining(store: Arc<PartitionStore>, lease_duration: std::time::Duration) -> Self {
        PartitionState::Draining {
            store,
            started_at: Instant::now(),
            lease_expires_at: Instant::now() + lease_duration,
        }
    }

    /// Transition to releasing state.
    pub fn start_releasing() -> Self {
        PartitionState::Releasing {
            started_at: Instant::now(),
        }
    }

    /// Transition to fenced state.
    pub fn fenced() -> Self {
        PartitionState::Fenced {
            fenced_at: Instant::now(),
        }
    }

    /// Check if the partition is owned and ready for operations.
    /// Note: Draining partitions are still considered "owned" for read operations.
    pub fn is_owned(&self) -> bool {
        matches!(
            self,
            PartitionState::Owned { .. } | PartitionState::Draining { .. }
        )
    }

    /// Check if the partition is actively owned (not draining).
    /// Use this to check if writes should be accepted.
    pub fn is_actively_owned(&self) -> bool {
        matches!(self, PartitionState::Owned { .. })
    }

    /// Check if the partition is draining (being rebalanced away).
    pub fn is_draining(&self) -> bool {
        matches!(self, PartitionState::Draining { .. })
    }

    /// Check if the lease has expired for a draining partition.
    pub fn is_drain_complete(&self) -> bool {
        match self {
            PartitionState::Draining {
                lease_expires_at, ..
            } => Instant::now() >= *lease_expires_at,
            _ => false,
        }
    }

    /// Check if the partition is in a transitional state (acquiring or releasing).
    pub fn is_transitioning(&self) -> bool {
        matches!(
            self,
            PartitionState::Acquiring { .. } | PartitionState::Releasing { .. }
        )
    }

    /// Check if the partition was fenced.
    pub fn is_fenced(&self) -> bool {
        matches!(self, PartitionState::Fenced { .. })
    }

    /// Get the store if the partition is owned (including draining).
    pub fn store(&self) -> Option<Arc<PartitionStore>> {
        match self {
            PartitionState::Owned { store, .. } => Some(store.clone()),
            PartitionState::Draining { store, .. } => Some(store.clone()),
            _ => None,
        }
    }

    /// Get how long the partition has been in the current state.
    pub fn duration_in_state(&self) -> Option<std::time::Duration> {
        match self {
            PartitionState::Unowned => None,
            PartitionState::Acquiring { started_at } => Some(started_at.elapsed()),
            PartitionState::Owned { acquired_at, .. } => Some(acquired_at.elapsed()),
            PartitionState::Draining { started_at, .. } => Some(started_at.elapsed()),
            PartitionState::Releasing { started_at } => Some(started_at.elapsed()),
            PartitionState::Fenced { fenced_at } => Some(fenced_at.elapsed()),
        }
    }

    /// Get a human-readable state name.
    pub fn state_name(&self) -> &'static str {
        match self {
            PartitionState::Unowned => "unowned",
            PartitionState::Acquiring { .. } => "acquiring",
            PartitionState::Owned { .. } => "owned",
            PartitionState::Draining { .. } => "draining",
            PartitionState::Releasing { .. } => "releasing",
            PartitionState::Fenced { .. } => "fenced",
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_unowned_state() {
        let state = PartitionState::unowned();
        assert!(!state.is_owned());
        assert!(!state.is_transitioning());
        assert!(!state.is_fenced());
        assert!(state.store().is_none());
        assert_eq!(state.state_name(), "unowned");
    }

    #[test]
    fn test_acquiring_state() {
        let state = PartitionState::start_acquiring();
        assert!(!state.is_owned());
        assert!(state.is_transitioning());
        assert!(!state.is_fenced());
        assert!(state.store().is_none());
        assert_eq!(state.state_name(), "acquiring");
        assert!(state.duration_in_state().is_some());
    }

    #[test]
    fn test_releasing_state() {
        let state = PartitionState::start_releasing();
        assert!(!state.is_owned());
        assert!(state.is_transitioning());
        assert!(!state.is_fenced());
        assert!(!state.is_draining());
        assert_eq!(state.state_name(), "releasing");
    }

    #[test]
    fn test_fenced_state() {
        let state = PartitionState::fenced();
        assert!(!state.is_owned());
        assert!(!state.is_transitioning());
        assert!(state.is_fenced());
        assert!(!state.is_draining());
        assert_eq!(state.state_name(), "fenced");
    }

    #[test]
    fn test_default() {
        let state = PartitionState::default();
        assert!(!state.is_owned());
        assert_eq!(state.state_name(), "unowned");
    }

    // ========================================================================
    // Additional tests for improved coverage
    // ========================================================================

    #[test]
    fn test_unowned_duration_is_none() {
        let state = PartitionState::unowned();
        assert!(state.duration_in_state().is_none());
    }

    #[test]
    fn test_acquiring_duration() {
        let state = PartitionState::start_acquiring();
        let duration = state.duration_in_state();
        assert!(duration.is_some());
        // Duration should be very small (just created)
        assert!(duration.unwrap() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn test_releasing_duration() {
        let state = PartitionState::start_releasing();
        let duration = state.duration_in_state();
        assert!(duration.is_some());
        assert!(duration.unwrap() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn test_fenced_duration() {
        let state = PartitionState::fenced();
        let duration = state.duration_in_state();
        assert!(duration.is_some());
        assert!(duration.unwrap() < std::time::Duration::from_secs(1));
    }

    #[test]
    fn test_is_actively_owned_for_unowned() {
        let state = PartitionState::unowned();
        assert!(!state.is_actively_owned());
    }

    #[test]
    fn test_is_actively_owned_for_acquiring() {
        let state = PartitionState::start_acquiring();
        assert!(!state.is_actively_owned());
    }

    #[test]
    fn test_is_actively_owned_for_releasing() {
        let state = PartitionState::start_releasing();
        assert!(!state.is_actively_owned());
    }

    #[test]
    fn test_is_actively_owned_for_fenced() {
        let state = PartitionState::fenced();
        assert!(!state.is_actively_owned());
    }

    #[test]
    fn test_is_draining_for_non_draining_states() {
        assert!(!PartitionState::unowned().is_draining());
        assert!(!PartitionState::start_acquiring().is_draining());
        assert!(!PartitionState::start_releasing().is_draining());
        assert!(!PartitionState::fenced().is_draining());
    }

    #[test]
    fn test_is_drain_complete_for_non_draining() {
        assert!(!PartitionState::unowned().is_drain_complete());
        assert!(!PartitionState::start_acquiring().is_drain_complete());
        assert!(!PartitionState::start_releasing().is_drain_complete());
        assert!(!PartitionState::fenced().is_drain_complete());
    }

    #[test]
    fn test_store_returns_none_for_unowned() {
        assert!(PartitionState::unowned().store().is_none());
    }

    #[test]
    fn test_store_returns_none_for_acquiring() {
        assert!(PartitionState::start_acquiring().store().is_none());
    }

    #[test]
    fn test_store_returns_none_for_releasing() {
        assert!(PartitionState::start_releasing().store().is_none());
    }

    #[test]
    fn test_store_returns_none_for_fenced() {
        assert!(PartitionState::fenced().store().is_none());
    }

    // ========================================================================
    // Debug formatting tests
    // ========================================================================

    #[test]
    fn test_debug_unowned() {
        let state = PartitionState::unowned();
        let debug = format!("{:?}", state);
        assert_eq!(debug, "Unowned");
    }

    #[test]
    fn test_debug_acquiring() {
        let state = PartitionState::start_acquiring();
        let debug = format!("{:?}", state);
        assert!(debug.contains("Acquiring"));
        assert!(debug.contains("duration"));
    }

    #[test]
    fn test_debug_releasing() {
        let state = PartitionState::start_releasing();
        let debug = format!("{:?}", state);
        assert!(debug.contains("Releasing"));
        assert!(debug.contains("duration"));
    }

    #[test]
    fn test_debug_fenced() {
        let state = PartitionState::fenced();
        let debug = format!("{:?}", state);
        assert!(debug.contains("Fenced"));
        assert!(debug.contains("duration"));
    }

    // ========================================================================
    // State name tests for all states
    // ========================================================================

    #[test]
    fn test_all_state_names() {
        assert_eq!(PartitionState::unowned().state_name(), "unowned");
        assert_eq!(PartitionState::start_acquiring().state_name(), "acquiring");
        assert_eq!(PartitionState::start_releasing().state_name(), "releasing");
        assert_eq!(PartitionState::fenced().state_name(), "fenced");
    }

    // ========================================================================
    // Owned and Draining state tests (with mock store)
    // ========================================================================

    mod with_store {
        use super::*;
        use crate::cluster::zombie_mode::ZombieModeState;
        use object_store::memory::InMemory;

        /// Create a test PartitionStore with in-memory storage.
        async fn create_test_store() -> Arc<PartitionStore> {
            let object_store = Arc::new(InMemory::new());
            let store = PartitionStore::builder()
                .object_store(object_store)
                .base_path("test")
                .topic("test-topic")
                .partition(0)
                .zombie_mode(Arc::new(ZombieModeState::new()))
                .build()
                .await
                .expect("Failed to create test store");
            Arc::new(store)
        }

        #[tokio::test]
        async fn test_owned_state() {
            let store = create_test_store().await;
            let state = PartitionState::acquired(store);

            assert!(state.is_owned());
            assert!(state.is_actively_owned());
            assert!(!state.is_transitioning());
            assert!(!state.is_fenced());
            assert!(!state.is_draining());
            assert!(state.store().is_some());
            assert_eq!(state.state_name(), "owned");
        }

        #[tokio::test]
        async fn test_owned_state_duration() {
            let store = create_test_store().await;
            let state = PartitionState::acquired(store);

            let duration = state.duration_in_state();
            assert!(duration.is_some());
            assert!(duration.unwrap() < std::time::Duration::from_secs(1));
        }

        #[tokio::test]
        async fn test_owned_state_debug() {
            let store = create_test_store().await;
            let state = PartitionState::acquired(store);

            let debug = format!("{:?}", state);
            assert!(debug.contains("Owned"));
            assert!(debug.contains("duration"));
        }

        #[tokio::test]
        async fn test_draining_state() {
            let store = create_test_store().await;
            // Lease expires in 60 seconds
            let state = PartitionState::start_draining(store, std::time::Duration::from_secs(60));

            assert!(state.is_owned());
            assert!(!state.is_actively_owned());
            assert!(!state.is_transitioning());
            assert!(!state.is_fenced());
            assert!(state.is_draining());
            assert!(state.store().is_some());
            assert_eq!(state.state_name(), "draining");
        }

        #[tokio::test]
        async fn test_draining_state_not_complete() {
            let store = create_test_store().await;
            // Lease expires in 60 seconds - not complete yet
            let state = PartitionState::start_draining(store, std::time::Duration::from_secs(60));

            assert!(!state.is_drain_complete());
        }

        #[tokio::test]
        async fn test_draining_state_complete_when_expired() {
            let store = create_test_store().await;
            // Lease already expired (0 duration)
            let state = PartitionState::start_draining(store, std::time::Duration::ZERO);

            // Give a tiny bit of time for the instant to pass
            tokio::time::sleep(std::time::Duration::from_millis(1)).await;

            assert!(state.is_drain_complete());
        }

        #[tokio::test]
        async fn test_draining_state_duration() {
            let store = create_test_store().await;
            let state = PartitionState::start_draining(store, std::time::Duration::from_secs(60));

            let duration = state.duration_in_state();
            assert!(duration.is_some());
            assert!(duration.unwrap() < std::time::Duration::from_secs(1));
        }

        #[tokio::test]
        async fn test_draining_state_debug() {
            let store = create_test_store().await;
            let state = PartitionState::start_draining(store, std::time::Duration::from_secs(60));

            let debug = format!("{:?}", state);
            assert!(debug.contains("Draining"));
            assert!(debug.contains("duration"));
            assert!(debug.contains("remaining"));
        }

        #[tokio::test]
        async fn test_store_returns_store_for_owned() {
            let store = create_test_store().await;
            let state = PartitionState::acquired(store.clone());

            let returned_store = state.store();
            assert!(returned_store.is_some());
            assert!(Arc::ptr_eq(&store, &returned_store.unwrap()));
        }

        #[tokio::test]
        async fn test_store_returns_store_for_draining() {
            let store = create_test_store().await;
            let state =
                PartitionState::start_draining(store.clone(), std::time::Duration::from_secs(60));

            let returned_store = state.store();
            assert!(returned_store.is_some());
            assert!(Arc::ptr_eq(&store, &returned_store.unwrap()));
        }
    }
}
