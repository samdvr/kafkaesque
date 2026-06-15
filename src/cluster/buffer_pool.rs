//! Shared buffer pool for reducing allocations in hot paths.
//!
//! Provides a process-wide lock-free pool that reuses [`Vec<u8>`] buffers
//! across all connections and async tasks on the produce path.
//!
//! # Usage
//!
//! ```ignore
//! use kafkaesque::cluster::buffer_pool::with_batch_buffer;
//!
//! let result = with_batch_buffer(estimated_size, |buffer| {
//!     buffer.extend_from_slice(&hwm.to_be_bytes());
//!     buffer.extend_from_slice(records);
//!     buffer.to_vec()
//! });
//! ```
//!
//! # Design
//!
//! - Single global [`crossbeam_queue::ArrayQueue`] shared by every connection
//!   so short-lived connections still benefit from buffer reuse.
//! - Two size buckets (small / large) so a small request doesn't pop and
//!   discard a megabyte-sized buffer.
//! - Buffers are cleared (not deallocated) between uses; capacity is
//!   capped at [`MAX_POOLED_BUFFER_SIZE`].

use crossbeam_queue::ArrayQueue;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{SystemTime, UNIX_EPOCH};

/// Maximum buffer capacity to keep in the pool.
const MAX_POOLED_BUFFER_SIZE: usize = 1024 * 1024;

/// Default initial capacity for pooled buffers.
const DEFAULT_BUFFER_CAPACITY: usize = 64 * 1024;

/// Boundary between the small and large buckets.
const LARGE_BUCKET_THRESHOLD: usize = 128 * 1024;

/// Maximum buffers retained per bucket. With a 10k-connection broker each
/// connection rarely needs more than one in-flight buffer at a time, so
/// 256 covers steady-state churn while bounding heap to ~33 MiB worst
/// case (256 × 128 KiB) for the small bucket and ~256 MiB for the large
/// bucket.
const POOL_CAPACITY: usize = 256;

struct Bucket {
    queue: ArrayQueue<Vec<u8>>,
    /// Last time a caller popped a buffer from this bucket
    /// (millis since UNIX_EPOCH). Used by [`drain_idle`].
    last_used_ms: AtomicU64,
    /// Maximum queue depth observed since process start. Operators can
    /// graph this to right-size [`POOL_CAPACITY`].
    high_water: AtomicUsize,
}

impl Bucket {
    fn new(cap: usize) -> Self {
        Self {
            queue: ArrayQueue::new(cap),
            last_used_ms: AtomicU64::new(now_ms()),
            high_water: AtomicUsize::new(0),
        }
    }

    fn record_use(&self) {
        self.last_used_ms.store(now_ms(), Ordering::Relaxed);
    }

    fn record_push(&self) {
        let depth = self.queue.len();
        let mut hw = self.high_water.load(Ordering::Relaxed);
        while depth > hw {
            match self.high_water.compare_exchange_weak(
                hw,
                depth,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(observed) => hw = observed,
            }
        }
    }
}

struct GlobalPool {
    small: Bucket,
    large: Bucket,
}

static GLOBAL_POOL: Lazy<GlobalPool> = Lazy::new(|| GlobalPool {
    small: Bucket::new(POOL_CAPACITY),
    large: Bucket::new(POOL_CAPACITY),
});

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

fn bucket_for(capacity: usize) -> &'static Bucket {
    if capacity <= LARGE_BUCKET_THRESHOLD {
        &GLOBAL_POOL.small
    } else {
        &GLOBAL_POOL.large
    }
}

/// Get a buffer from the pool, or create a new one.
///
/// The returned buffer is empty (cleared) and has at least
/// `capacity_hint` capacity, capped at [`MAX_POOLED_BUFFER_SIZE`].
pub fn get_buffer(capacity_hint: usize) -> Vec<u8> {
    let bucket = bucket_for(capacity_hint);
    bucket.record_use();
    while let Some(mut buf) = bucket.queue.pop() {
        if buf.capacity() >= capacity_hint {
            buf.clear();
            return buf;
        }
        // Too small for this caller; let it drop. Continuing to drain
        // gives subsequent callers a fresh shot at a larger buffer
        // without wasting this one in another caller's hands.
    }
    let capacity = capacity_hint.clamp(DEFAULT_BUFFER_CAPACITY, MAX_POOLED_BUFFER_SIZE);
    Vec::with_capacity(capacity)
}

/// Return a buffer to the pool.
///
/// Buffers exceeding [`MAX_POOLED_BUFFER_SIZE`] are dropped to bound heap.
/// If the pool is full, the buffer is dropped.
pub fn return_buffer(buffer: Vec<u8>) {
    if buffer.capacity() > MAX_POOLED_BUFFER_SIZE {
        return;
    }
    let bucket = bucket_for(buffer.capacity());
    if bucket.queue.push(buffer).is_ok() {
        bucket.record_push();
    }
}

/// Execute a closure with a pooled buffer, then return the buffer to the pool.
///
/// The closure receives a mutable reference to a cleared buffer.
pub fn with_batch_buffer<F, R>(capacity_hint: usize, f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    let mut buffer = get_buffer(capacity_hint);
    let result = f(&mut buffer);
    return_buffer(buffer);
    result
}

/// Drain pooled buffers from any bucket whose last `get_buffer` happened
/// more than `idle_threshold_ms` ago. Returns the number of buffers
/// dropped. Intended to be called from a periodic operator task — the
/// large bucket alone retains 256 MiB indefinitely without this.
pub fn drain_idle(idle_threshold_ms: u64) -> usize {
    let now = now_ms();
    let mut dropped = 0;
    for bucket in [&GLOBAL_POOL.small, &GLOBAL_POOL.large] {
        let last = bucket.last_used_ms.load(Ordering::Relaxed);
        if now.saturating_sub(last) >= idle_threshold_ms {
            while bucket.queue.pop().is_some() {
                dropped += 1;
            }
        }
    }
    dropped
}

/// Snapshot of per-bucket pool metrics for observability.
pub struct PoolStats {
    pub small_depth: usize,
    pub large_depth: usize,
    pub small_high_water: usize,
    pub large_high_water: usize,
}

pub fn pool_stats() -> PoolStats {
    PoolStats {
        small_depth: GLOBAL_POOL.small.queue.len(),
        large_depth: GLOBAL_POOL.large.queue.len(),
        small_high_water: GLOBAL_POOL.small.high_water.load(Ordering::Relaxed),
        large_high_water: GLOBAL_POOL.large.high_water.load(Ordering::Relaxed),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic() {
        let buf1 = get_buffer(1000);
        assert!(buf1.capacity() >= 1000);
        return_buffer(buf1);

        let buf2 = get_buffer(1000);
        assert!(buf2.capacity() >= 1000);
        return_buffer(buf2);
    }

    #[test]
    fn test_with_batch_buffer() {
        let result = with_batch_buffer(100, |buf| {
            buf.extend_from_slice(b"hello");
            buf.len()
        });
        assert_eq!(result, 5);

        let result2 = with_batch_buffer(100, |buf| {
            assert!(buf.is_empty());
            42
        });
        assert_eq!(result2, 42);
    }

    #[test]
    fn test_large_buffer_capped_and_pooled() {
        let large_buf = get_buffer(MAX_POOLED_BUFFER_SIZE + 1000);
        assert_eq!(large_buf.capacity(), MAX_POOLED_BUFFER_SIZE);
        return_buffer(large_buf);

        let reused_buf = get_buffer(MAX_POOLED_BUFFER_SIZE);
        assert_eq!(reused_buf.capacity(), MAX_POOLED_BUFFER_SIZE);
        return_buffer(reused_buf);
    }

    #[test]
    fn test_externally_large_buffer_not_pooled() {
        let mut external_buf = Vec::with_capacity(MAX_POOLED_BUFFER_SIZE + 10000);
        external_buf.push(0);
        let capacity = external_buf.capacity();

        return_buffer(external_buf);

        let new_buf = get_buffer(DEFAULT_BUFFER_CAPACITY);
        assert!(
            new_buf.capacity() < capacity,
            "New buffer capacity {} should be < external buffer capacity {}",
            new_buf.capacity(),
            capacity
        );
        return_buffer(new_buf);
    }

    #[test]
    fn test_small_request_does_not_consume_large_buffer() {
        let large = Vec::with_capacity(LARGE_BUCKET_THRESHOLD * 2);
        return_buffer(large);

        let small = get_buffer(1024);
        assert!(small.capacity() <= LARGE_BUCKET_THRESHOLD);

        let large_again = get_buffer(LARGE_BUCKET_THRESHOLD * 2);
        assert!(large_again.capacity() >= LARGE_BUCKET_THRESHOLD * 2);

        return_buffer(small);
        return_buffer(large_again);
    }
}
