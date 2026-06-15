//! Producer state types for idempotency.

use serde::{Deserialize, Serialize};

/// Persisted state for a producer, used for idempotency.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PersistedProducerState {
    /// Last sequence number written by this producer.
    pub last_sequence: i32,
    /// Producer epoch for fencing.
    pub producer_epoch: i16,
}
