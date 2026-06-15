//! Tokio-flavored helpers wrapping the runtime-independent
//! `kafkaesque-protocol` crate.
//!
//! `kafkaesque-protocol` is intentionally runtime-independent (no
//! tokio dependency), but the broker hot path needs an async
//! CRC-validation entry point so large batches can be offloaded to
//! `spawn_blocking`. Keeping the wrapper here, in the umbrella
//! crate, lets the protocol crate stay tokio-free without forcing
//! every caller to roll their own offload logic.

pub use crate::protocol::{CRC_OFFLOAD_THRESHOLD, CrcValidationResult, validate_batch_crc};

/// Validate batch CRC, offloading to `spawn_blocking` when the batch
/// is large enough that the synchronous computation would block the
/// runtime.
///
/// Wire-protocol entry points should prefer this over
/// [`validate_batch_crc`] when running inside a tokio task; small
/// batches still execute inline.
///
/// Takes `&Bytes` so the offload path can `clone()` the refcount
/// instead of memcpy'ing the entire payload — a `Bytes` clone is
/// constant-time regardless of size, while a `&[u8]` signature would
/// force a full copy via `Bytes::copy_from_slice` for every batch
/// over the offload threshold.
///
/// # Failure semantics
///
/// If the offload future fails to complete (the blocking pool
/// panicked or is saturated), this function returns
/// `CrcValidationResult::OffloadFailed`. Falling back to inline
/// CRC computation under load defeats the offload's purpose: the
/// fallback runs the very block we tried to avoid, on the runtime
/// worker that's already starved. Callers should treat
/// `OffloadFailed` as a server-side rejection (e.g.
/// `KafkaCode::CorruptMessage`) so the broker stays responsive.
pub async fn validate_batch_crc_async(batch: &bytes::Bytes) -> CrcValidationResult {
    if batch.len() < CRC_OFFLOAD_THRESHOLD {
        return validate_batch_crc(batch);
    }
    let owned = batch.clone();
    match tokio::task::spawn_blocking(move || validate_batch_crc(&owned)).await {
        Ok(result) => result,
        Err(_) => CrcValidationResult::OffloadFailed,
    }
}
