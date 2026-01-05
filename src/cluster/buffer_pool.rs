//! Buffer pool for reducing allocations in hot paths.
//!
//! This module provides thread-local buffer pooling to reduce allocation overhead
//! in the produce path. Instead of allocating a new Vec for each batch rewrite,
//! we reuse buffers from a thread-local pool.
//!
//! # Usage
//!
//! ```ignore
//! use kafkaesque::cluster::buffer_pool::with_batch_buffer;
//!
//! let result = with_batch_buffer(estimated_size, |buffer| {
//!     buffer.extend_from_slice(&hwm.to_be_bytes());
//!     buffer.extend_from_slice(records);
//!     // Return owned data when done
//!     buffer.to_vec()
//! });
//! ```
//!
//! # Design
//!
//! - Thread-local storage avoids contention between threads
//! - Buffers are cleared (not deallocated) between uses
//! - Maximum buffer size cap prevents unbounded memory growth
//! - Falls back to fresh allocation for very large batches

use std::cell::RefCell;

/// Maximum buffer size to keep in the pool (1 MB).
/// Buffers larger than this are not pooled to prevent memory bloat.
const MAX_POOLED_BUFFER_SIZE: usize = 1024 * 1024;

/// Default initial capacity for pooled buffers (64 KB).
/// This covers most common batch sizes without reallocation.
const DEFAULT_BUFFER_CAPACITY: usize = 64 * 1024;

/// Number of buffers to keep in the thread-local pool.
const POOL_SIZE: usize = 4;

thread_local! {
    /// Thread-local pool of reusable buffers.
    static BUFFER_POOL: RefCell<BufferPool> = RefCell::new(BufferPool::new());
}

/// A simple buffer pool using a stack of Vec<u8>.
struct BufferPool {
    buffers: Vec<Vec<u8>>,
}

impl BufferPool {
    fn new() -> Self {
        Self {
            buffers: Vec::with_capacity(POOL_SIZE),
        }
    }

    /// Get a buffer from the pool, or create a new one.
    fn get(&mut self, capacity_hint: usize) -> Vec<u8> {
        // Try to find a buffer with sufficient capacity
        if let Some(idx) = self
            .buffers
            .iter()
            .position(|b| b.capacity() >= capacity_hint)
        {
            let mut buffer = self.buffers.swap_remove(idx);
            buffer.clear();
            return buffer;
        }

        // No suitable buffer found, create new one
        // Cap at MAX_POOLED_BUFFER_SIZE to avoid allocating huge buffers
        let capacity = capacity_hint.clamp(DEFAULT_BUFFER_CAPACITY, MAX_POOLED_BUFFER_SIZE);
        Vec::with_capacity(capacity)
    }

    /// Return a buffer to the pool.
    fn put(&mut self, buffer: Vec<u8>) {
        // Don't pool very large buffers to prevent memory bloat
        if buffer.capacity() > MAX_POOLED_BUFFER_SIZE {
            return;
        }

        // Only keep up to POOL_SIZE buffers
        if self.buffers.len() < POOL_SIZE {
            self.buffers.push(buffer);
        }
        // If pool is full, just drop the buffer
    }
}

/// Execute a closure with a pooled buffer, then return the buffer to the pool.
///
/// The closure receives a mutable reference to a cleared buffer.
/// After the closure returns, the buffer is returned to the thread-local pool.
///
/// # Example
///
/// ```ignore
/// let data = with_batch_buffer(1000, |buf| {
///     buf.extend_from_slice(b"hello");
///     buf.clone() // Return owned data
/// });
/// ```
pub fn with_batch_buffer<F, R>(capacity_hint: usize, f: F) -> R
where
    F: FnOnce(&mut Vec<u8>) -> R,
{
    BUFFER_POOL.with(|pool| {
        let mut buffer = pool.borrow_mut().get(capacity_hint);
        let result = f(&mut buffer);
        pool.borrow_mut().put(buffer);
        result
    })
}

/// Get a buffer from the pool without automatic return.
///
/// The caller is responsible for returning the buffer via `return_buffer`.
/// Use this when you need to pass the buffer across async boundaries.
pub fn get_buffer(capacity_hint: usize) -> Vec<u8> {
    BUFFER_POOL.with(|pool| pool.borrow_mut().get(capacity_hint))
}

/// Return a buffer to the pool.
pub fn return_buffer(buffer: Vec<u8>) {
    BUFFER_POOL.with(|pool| pool.borrow_mut().put(buffer));
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_buffer_pool_basic() {
        let buf1 = get_buffer(1000);
        assert!(buf1.capacity() >= 1000);
        return_buffer(buf1);

        // Should get the same buffer back
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

        // Buffer should be returned and cleared
        let result2 = with_batch_buffer(100, |buf| {
            assert!(buf.is_empty());
            42
        });
        assert_eq!(result2, 42);
    }

    #[test]
    fn test_large_buffer_capped_and_pooled() {
        // Request a huge buffer - should be capped at MAX_POOLED_BUFFER_SIZE
        let large_buf = get_buffer(MAX_POOLED_BUFFER_SIZE + 1000);

        // Verify it was capped (not allocated at requested size)
        assert_eq!(large_buf.capacity(), MAX_POOLED_BUFFER_SIZE);

        return_buffer(large_buf);

        // The capped buffer should be pooled and reused
        // (since it's exactly at the limit, not over it)
        let reused_buf = get_buffer(MAX_POOLED_BUFFER_SIZE);
        assert_eq!(reused_buf.capacity(), MAX_POOLED_BUFFER_SIZE);
        return_buffer(reused_buf);
    }

    #[test]
    fn test_externally_large_buffer_not_pooled() {
        // Simulate a buffer that came from outside the pool with huge capacity
        // (e.g., grown during use beyond the cap)
        let mut external_buf = Vec::with_capacity(MAX_POOLED_BUFFER_SIZE + 10000);
        external_buf.push(0); // Use it so it's not optimized away
        let capacity = external_buf.capacity();

        // Return it - should NOT be pooled because capacity > MAX_POOLED_BUFFER_SIZE
        return_buffer(external_buf);

        // Get a new buffer - should be fresh (the large one wasn't pooled)
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
    fn test_pool_size_limit() {
        // Fill the pool
        let mut buffers = Vec::new();
        for _ in 0..POOL_SIZE + 2 {
            buffers.push(get_buffer(1000));
        }

        // Return all buffers
        for buf in buffers {
            return_buffer(buf);
        }

        // Pool should only have POOL_SIZE buffers
        // (remaining were dropped)
    }
}
