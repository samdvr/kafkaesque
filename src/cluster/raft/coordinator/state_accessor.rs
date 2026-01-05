//! State accessor helper for Raft coordinator.
//!
//! This module provides a helper that encapsulates common patterns when
//! accessing the Raft state machine.
//!
//! Note: The actual state reading pattern still needs to be done inline
//! in the calling code due to Rust's lifetime rules. This module provides
//! the write helper that is reusable.

use std::sync::Arc;

use super::super::commands::{CoordinationCommand, CoordinationResponse};
use super::super::node::RaftNode;
use crate::cluster::error::SlateDBResult;

/// Helper for accessing Raft coordinator state.
///
/// Provides convenient access to common Raft operations.
pub struct StateAccessor<'a> {
    node: &'a Arc<RaftNode>,
}

impl<'a> StateAccessor<'a> {
    /// Create a new state accessor.
    #[inline]
    pub fn new(node: &'a Arc<RaftNode>) -> Self {
        Self { node }
    }

    /// Write a command to Raft with unified error handling.
    ///
    /// This handles forwarding to the leader if we're not the leader.
    #[inline]
    pub async fn write(&self, command: CoordinationCommand) -> SlateDBResult<CoordinationResponse> {
        self.node.write(command).await
    }

    /// Ensure linearizable state before reading.
    ///
    /// This is used for write-path operations that need the strongest consistency.
    #[inline]
    pub async fn ensure_linearizable(&self) -> SlateDBResult<()> {
        self.node.ensure_linearizable().await
    }
}

#[cfg(test)]
mod tests {
    // Tests for StateAccessor would require a full Raft node setup
    // which is covered by integration tests
}
