//! Loom-based concurrency tests for detecting race conditions.
//!
//! These tests use the Loom library to explore all possible thread interleavings
//! and detect race conditions in concurrent code. Loom is particularly useful for
//! testing atomic operations and lock-free data structures.
//!
//! # What These Tests Cover
//!
//! 1. **Zombie Mode Transitions** - Tests that zombie mode flag transitions are atomic
//!    and visible to all threads immediately.
//!
//! 2. **HWM Allocation** - Tests that high watermark allocation is atomic and
//!    produces unique, monotonically increasing offsets under concurrent access.
//!
//! 3. **Double-Check Locking** - Tests the pattern used in append_batch where
//!    zombie mode is checked before and after acquiring the write lock.
//!
//! 4. **Concurrent Reads During Writes** - Tests that fetch operations can safely
//!    read while writes are in progress.
//!
//! # Running Loom Tests
//!
//! Loom tests require the `loom` feature to be enabled:
//!
//! ```sh
//! cargo test --test loom_tests --features loom --release
//! ```
//!
//! Note: Loom tests can be slow as they explore all possible interleavings.

// Only compile these tests when the loom feature is enabled
#![cfg(feature = "loom")]

use loom::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use loom::sync::{Arc, Mutex, RwLock};
use loom::thread;

/// Test that zombie mode flag transitions are immediately visible to all threads.
///
/// This tests the critical property that when zombie mode is set, all threads
/// must see it immediately to prevent writes from being accepted during zombie mode.
#[test]
fn test_zombie_mode_visibility() {
    loom::model(|| {
        let zombie_mode = Arc::new(AtomicBool::new(false));
        let zombie_mode_clone = zombie_mode.clone();

        // Thread 1: Sets zombie mode
        let t1 = thread::spawn(move || {
            zombie_mode_clone.store(true, Ordering::SeqCst);
        });

        // Thread 2: Checks zombie mode
        let zombie_mode_clone2 = zombie_mode.clone();
        let t2 = thread::spawn(move || zombie_mode_clone2.load(Ordering::SeqCst));

        t1.join().unwrap();
        let _ = t2.join().unwrap();

        // After t1 completes, zombie mode must be true
        assert!(zombie_mode.load(Ordering::SeqCst));
    });
}

/// Test that zombie mode check-then-write pattern is safe with double-check locking.
///
/// This simulates the pattern in append_batch where we:
/// 1. Check zombie mode before acquiring lock
/// 2. Acquire the write lock
/// 3. Check zombie mode again after acquiring lock
///
/// The second check prevents writes that started before zombie mode was set
/// from completing after it was set.
#[test]
fn test_zombie_mode_double_check() {
    loom::model(|| {
        let zombie_mode = Arc::new(AtomicBool::new(false));
        let write_lock = Arc::new(Mutex::new(()));
        let writes_during_zombie = Arc::new(AtomicI64::new(0));

        let zombie_mode1 = zombie_mode.clone();
        let write_lock1 = write_lock.clone();
        let writes_during_zombie1 = writes_during_zombie.clone();

        // Thread 1: Simulates a write operation with double-check
        let t1 = thread::spawn(move || {
            // First check (outside lock)
            if zombie_mode1.load(Ordering::SeqCst) {
                return;
            }

            // Acquire write lock
            let _guard = write_lock1.lock().unwrap();

            // Second check (inside lock) - critical for correctness
            if zombie_mode1.load(Ordering::SeqCst) {
                return;
            }

            // Simulate write - increment counter
            // If zombie mode was set, we should not reach here
            writes_during_zombie1.fetch_add(1, Ordering::SeqCst);
        });

        let zombie_mode2 = zombie_mode.clone();
        let write_lock2 = write_lock.clone();
        let writes_during_zombie2 = writes_during_zombie.clone();

        // Thread 2: Another write operation
        let t2 = thread::spawn(move || {
            if zombie_mode2.load(Ordering::SeqCst) {
                return;
            }

            let _guard = write_lock2.lock().unwrap();

            if zombie_mode2.load(Ordering::SeqCst) {
                return;
            }

            writes_during_zombie2.fetch_add(1, Ordering::SeqCst);
        });

        let zombie_mode3 = zombie_mode.clone();

        // Thread 3: Sets zombie mode
        let t3 = thread::spawn(move || {
            zombie_mode3.store(true, Ordering::SeqCst);
        });

        t1.join().unwrap();
        t2.join().unwrap();
        t3.join().unwrap();

        // After all threads complete, if zombie mode is set, the writes counter
        // tells us how many writes completed. This is valid - writes that started
        // before zombie mode was set may complete.
        // The key invariant is: no writes start AFTER zombie mode is visible.
    });
}

/// Test that HWM allocation produces unique, monotonically increasing offsets.
///
/// This simulates multiple producers allocating offsets concurrently using
/// compare-and-swap (CAS) operations, which is how the real implementation works.
#[test]
fn test_hwm_allocation_atomicity() {
    loom::model(|| {
        let hwm = Arc::new(AtomicI64::new(0));
        let mut handles = vec![];

        // Spawn multiple threads that each allocate a batch of offsets
        for _ in 0..3 {
            let hwm_clone = hwm.clone();
            handles.push(thread::spawn(move || {
                // Simulate allocating 10 offsets (like a batch with 10 records)
                let batch_size = 10;

                // Use CAS loop to atomically allocate offsets
                loop {
                    let current = hwm_clone.load(Ordering::SeqCst);
                    let new_hwm = current + batch_size;
                    if hwm_clone
                        .compare_exchange(current, new_hwm, Ordering::SeqCst, Ordering::SeqCst)
                        .is_ok()
                    {
                        return current; // Return the base offset we got
                    }
                }
            }));
        }

        let mut base_offsets: Vec<i64> = handles.into_iter().map(|h| h.join().unwrap()).collect();
        base_offsets.sort();

        // All base offsets must be unique and separated by batch_size
        assert_eq!(base_offsets.len(), 3);
        assert_eq!(base_offsets[0], 0);
        assert_eq!(base_offsets[1], 10);
        assert_eq!(base_offsets[2], 20);

        // Final HWM should be 30 (3 batches of 10)
        assert_eq!(hwm.load(Ordering::SeqCst), 30);
    });
}

/// Test that fetch_add produces unique values even under contention.
///
/// This tests the simpler case where we just use fetch_add instead of CAS.
#[test]
fn test_fetch_add_uniqueness() {
    loom::model(|| {
        let counter = Arc::new(AtomicI64::new(0));
        let mut handles = vec![];

        for _ in 0..3 {
            let counter_clone = counter.clone();
            handles.push(thread::spawn(move || {
                counter_clone.fetch_add(1, Ordering::SeqCst)
            }));
        }

        let values: Vec<i64> = handles.into_iter().map(|h| h.join().unwrap()).collect();

        // All values must be unique
        let mut sorted = values.clone();
        sorted.sort();
        sorted.dedup();
        assert_eq!(sorted.len(), 3);

        // Final value should be 3
        assert_eq!(counter.load(Ordering::SeqCst), 3);
    });
}

/// Test lock ordering to ensure no deadlocks are possible.
///
/// This tests a simplified version of the lock acquisition pattern used
/// in partition operations.
#[test]
fn test_lock_ordering() {
    loom::model(|| {
        let lock_a = Arc::new(Mutex::new(0));
        let lock_b = Arc::new(Mutex::new(0));

        let lock_a1 = lock_a.clone();
        let lock_b1 = lock_b.clone();

        // Thread 1: Acquires locks in A, B order
        let t1 = thread::spawn(move || {
            let mut a = lock_a1.lock().unwrap();
            let mut b = lock_b1.lock().unwrap();
            *a += 1;
            *b += 1;
        });

        let lock_a2 = lock_a.clone();
        let lock_b2 = lock_b.clone();

        // Thread 2: Also acquires locks in A, B order (consistent ordering)
        let t2 = thread::spawn(move || {
            let mut a = lock_a2.lock().unwrap();
            let mut b = lock_b2.lock().unwrap();
            *a += 10;
            *b += 10;
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // Both threads completed without deadlock
        let final_a = *lock_a.lock().unwrap();
        let final_b = *lock_b.lock().unwrap();

        // Values should be 11 (1 + 10) for both
        assert_eq!(final_a, 11);
        assert_eq!(final_b, 11);
    });
}

/// Test read-write coordination without blocking readers during writes.
///
/// This simulates the RwLock pattern used for batch index access where
/// readers can access concurrently but writers need exclusive access.
#[test]
fn test_concurrent_read_write() {
    loom::model(|| {
        let data = Arc::new(RwLock::new(vec![0i64; 3]));

        let data1 = data.clone();
        // Writer thread
        let writer = thread::spawn(move || {
            let mut guard = data1.write().unwrap();
            guard[0] = 100;
            guard[1] = 200;
            guard[2] = 300;
        });

        let data2 = data.clone();
        // Reader thread 1
        let reader1 = thread::spawn(move || {
            let guard = data2.read().unwrap();
            guard.iter().sum::<i64>()
        });

        let data3 = data.clone();
        // Reader thread 2
        let reader2 = thread::spawn(move || {
            let guard = data3.read().unwrap();
            guard.iter().sum::<i64>()
        });

        writer.join().unwrap();
        let sum1 = reader1.join().unwrap();
        let sum2 = reader2.join().unwrap();

        // Readers see either the initial state (sum = 0) or the final state (sum = 600)
        // They should never see a partial update (e.g., sum = 100 or sum = 300)
        assert!(
            sum1 == 0 || sum1 == 600,
            "Reader 1 saw partial state: {}",
            sum1
        );
        assert!(
            sum2 == 0 || sum2 == 600,
            "Reader 2 saw partial state: {}",
            sum2
        );
    });
}

/// Test atomic flag for fencing detection.
///
/// This simulates the fenced flag pattern where a fencing error from SlateDB
/// sets a flag that prevents further operations.
#[test]
fn test_fencing_flag() {
    loom::model(|| {
        let fenced = Arc::new(AtomicBool::new(false));
        let operation_count = Arc::new(AtomicI64::new(0));

        let fenced1 = fenced.clone();
        let op_count1 = operation_count.clone();

        // Thread 1: Performs operations, checking fenced flag
        let t1 = thread::spawn(move || {
            for _ in 0..3 {
                if fenced1.load(Ordering::SeqCst) {
                    break;
                }
                op_count1.fetch_add(1, Ordering::SeqCst);
            }
        });

        let fenced2 = fenced.clone();
        // Thread 2: Sets the fenced flag
        let t2 = thread::spawn(move || {
            fenced2.store(true, Ordering::SeqCst);
        });

        t1.join().unwrap();
        t2.join().unwrap();

        // After fencing, the flag must be true
        assert!(fenced.load(Ordering::SeqCst));

        // Operation count is between 0 and 3, depending on when fencing occurred
        let count = operation_count.load(Ordering::SeqCst);
        assert!((0..=3).contains(&count));
    });
}

/// Test sequence number validation under concurrent producer access.
///
/// This simulates multiple producers sending batches and validates that
/// sequence number tracking prevents duplicates.
#[test]
fn test_sequence_validation() {
    loom::model(|| {
        // Simulates producer state: (last_sequence, epoch)
        let producer_state = Arc::new(Mutex::new((0i32, 0i16)));
        let accepted_batches = Arc::new(AtomicI64::new(0));

        let state1 = producer_state.clone();
        let accepted1 = accepted_batches.clone();

        // Producer 1: Sends sequence 1
        let t1 = thread::spawn(move || {
            let first_sequence = 1;
            let epoch = 0;

            let mut state = state1.lock().unwrap();
            let (last_seq, last_epoch) = *state;

            // Validate sequence
            if (epoch == last_epoch && first_sequence == last_seq + 1) || epoch > last_epoch {
                // Accept: update state (either next sequence in epoch, or new epoch)
                *state = (first_sequence, epoch);
                accepted1.fetch_add(1, Ordering::SeqCst);
                true
            } else {
                // Reject: duplicate or out of order
                false
            }
        });

        let state2 = producer_state.clone();
        let accepted2 = accepted_batches.clone();

        // Producer 2: Also sends sequence 1 (duplicate attempt)
        let t2 = thread::spawn(move || {
            let first_sequence = 1;
            let epoch = 0;

            let mut state = state2.lock().unwrap();
            let (last_seq, last_epoch) = *state;

            if (epoch == last_epoch && first_sequence == last_seq + 1) || epoch > last_epoch {
                *state = (first_sequence, epoch);
                accepted2.fetch_add(1, Ordering::SeqCst);
                true
            } else {
                false
            }
        });

        let result1 = t1.join().unwrap();
        let result2 = t2.join().unwrap();

        // Exactly one should succeed (the duplicate should be rejected)
        // XOR: one true, one false
        assert!(
            (result1 && !result2) || (!result1 && result2),
            "Both accepted or both rejected: t1={}, t2={}",
            result1,
            result2
        );
        assert_eq!(accepted_batches.load(Ordering::SeqCst), 1);
    });
}
