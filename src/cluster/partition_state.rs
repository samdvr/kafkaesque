//! Partition state machine for tracking partition lifecycle.
//!
//! This module provides an explicit state machine for partition ownership transitions:
//! - Unowned: Partition is not owned by this broker
//! - Owned: Successfully acquired, SlateDB store is open and ready
//! - Fenced: Another broker took ownership (fenced out)
//!
//! Acquisition and release happen synchronously inside `PartitionManager` while
//! holding the per-partition entry, so there are no separate `Acquiring`/
//! `Releasing` states — the entry is either absent or `Owned`. If a future
//! change splits acquisition over multiple Raft round-trips, reintroduce the
//! intermediate states then.
//!
//! # Integration
//!
//! This state machine is fully integrated with `PartitionManager`. All partition ownership
//! is tracked using `PartitionState` enum, providing:
//! - Type-safe state representation with explicit lifecycle tracking
//! - Built-in timing information (duration in each state)
//! - Clear ownership semantics via `is_owned()` and `is_fenced()`
//!
//! # State Transitions
//!
//! ```text
//! Unowned -> Owned -> (rebalance / release) -> Unowned
//!              |
//!              v
//!           Fenced -> Unowned
//! ```
//!
//! When a partition is rebalanced to another broker, SlateDB is closed immediately
//! and the entry is removed from the state map. The lease is allowed to expire
//! naturally (or is explicitly released through the coordinator).

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

    /// Successfully acquired and actively owned.
    Owned {
        /// The open partition store.
        store: Arc<PartitionStore>,
        /// When we acquired ownership.
        acquired_at: Instant,
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
            PartitionState::Owned { acquired_at, .. } => f
                .debug_struct("Owned")
                .field("duration", &acquired_at.elapsed())
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

    /// Transition to owned state with the given store.
    pub fn acquired(store: Arc<PartitionStore>) -> Self {
        PartitionState::Owned {
            store,
            acquired_at: Instant::now(),
        }
    }

    /// Transition to fenced state.
    pub fn fenced() -> Self {
        PartitionState::Fenced {
            fenced_at: Instant::now(),
        }
    }

    /// Check if the partition is owned and ready for operations.
    pub fn is_owned(&self) -> bool {
        matches!(self, PartitionState::Owned { .. })
    }

    /// Check if the partition is actively owned (accepting writes).
    /// Kept as a separate method so call sites that distinguish "owned vs
    /// owned-but-not-writable" stay explicit if that distinction is ever
    /// reintroduced.
    pub fn is_actively_owned(&self) -> bool {
        matches!(self, PartitionState::Owned { .. })
    }

    /// Check if the partition was fenced.
    pub fn is_fenced(&self) -> bool {
        matches!(self, PartitionState::Fenced { .. })
    }

    /// Get the store if the partition is owned.
    pub fn store(&self) -> Option<Arc<PartitionStore>> {
        match self {
            PartitionState::Owned { store, .. } => Some(store.clone()),
            _ => None,
        }
    }

    /// Get how long the partition has been in the current state.
    pub fn duration_in_state(&self) -> Option<std::time::Duration> {
        match self {
            PartitionState::Unowned => None,
            PartitionState::Owned { acquired_at, .. } => Some(acquired_at.elapsed()),
            PartitionState::Fenced { fenced_at } => Some(fenced_at.elapsed()),
        }
    }

    /// Get a human-readable state name.
    pub fn state_name(&self) -> &'static str {
        match self {
            PartitionState::Unowned => "unowned",
            PartitionState::Owned { .. } => "owned",
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
        assert!(!state.is_fenced());
        assert!(state.store().is_none());
        assert_eq!(state.state_name(), "unowned");
    }

    #[test]
    fn test_fenced_state() {
        let state = PartitionState::fenced();
        assert!(!state.is_owned());
        assert!(state.is_fenced());
        assert_eq!(state.state_name(), "fenced");
    }

    #[test]
    fn test_default() {
        let state = PartitionState::default();
        assert!(!state.is_owned());
        assert_eq!(state.state_name(), "unowned");
    }

    #[test]
    fn test_unowned_duration_is_none() {
        let state = PartitionState::unowned();
        assert!(state.duration_in_state().is_none());
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
    fn test_is_actively_owned_for_fenced() {
        let state = PartitionState::fenced();
        assert!(!state.is_actively_owned());
    }

    #[test]
    fn test_store_returns_none_for_unowned() {
        assert!(PartitionState::unowned().store().is_none());
    }

    #[test]
    fn test_store_returns_none_for_fenced() {
        assert!(PartitionState::fenced().store().is_none());
    }

    #[test]
    fn test_debug_unowned() {
        let state = PartitionState::unowned();
        let debug = format!("{:?}", state);
        assert_eq!(debug, "Unowned");
    }

    #[test]
    fn test_debug_fenced() {
        let state = PartitionState::fenced();
        let debug = format!("{:?}", state);
        assert!(debug.contains("Fenced"));
        assert!(debug.contains("duration"));
    }

    #[test]
    fn test_all_state_names() {
        assert_eq!(PartitionState::unowned().state_name(), "unowned");
        assert_eq!(PartitionState::fenced().state_name(), "fenced");
    }

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
            assert!(!state.is_fenced());
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
        async fn test_store_returns_store_for_owned() {
            let store = create_test_store().await;
            let state = PartitionState::acquired(store.clone());

            let returned_store = state.store();
            assert!(returned_store.is_some());
            assert!(Arc::ptr_eq(&store, &returned_store.unwrap()));
        }
    }
}
