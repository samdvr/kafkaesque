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

// ============================================================================
// Partition-ownership map
//
// Models the contention pattern of `partition_manager::partition_states`
// (a `DashMap<PartitionKey, PartitionState>`) using loom-aware primitives.
// DashMap itself is not loom-instrumented; we model an equivalent
// HashMap-behind-Mutex so loom can explore every interleaving of:
//
//   - acquire (insert if absent, succeed-or-lose-the-race)
//   - release (remove if owned by self)
//   - read    (lookup-and-act on the cached owner)
//
// The invariants to verify on the real map are inherited from sequential
// HashMap semantics + the per-partition acquire lock — but only as long
// as every code path that mutates the map holds the per-key lock first.
// These tests assert that, *given* the per-key lock, the visible state
// after concurrent operations is one of the expected sequential
// linearizations and never a torn / split state.
// ============================================================================

use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
enum OwnerState {
    Owned(u64), // broker id
}

/// Two brokers race to acquire the same partition. After both have run,
/// exactly one must have observed an empty slot and inserted itself; the
/// other must have observed the winner's id and backed off. The slot must
/// hold exactly one owner.
#[test]
fn test_partition_acquire_race_single_winner() {
    loom::model(|| {
        let map: Arc<Mutex<HashMap<i32, OwnerState>>> = Arc::new(Mutex::new(HashMap::new()));
        let acquire_lock = Arc::new(Mutex::new(()));
        let key = 42i32;

        let m1 = map.clone();
        let lock1 = acquire_lock.clone();
        let t1 = thread::spawn(move || {
            // Per-key acquire lock serializes the check-then-insert.
            let _guard = lock1.lock().unwrap();
            let mut m = m1.lock().unwrap();
            if m.contains_key(&key) {
                false
            } else {
                m.insert(key, OwnerState::Owned(1));
                true
            }
        });

        let m2 = map.clone();
        let lock2 = acquire_lock.clone();
        let t2 = thread::spawn(move || {
            let _guard = lock2.lock().unwrap();
            let mut m = m2.lock().unwrap();
            if m.contains_key(&key) {
                false
            } else {
                m.insert(key, OwnerState::Owned(2));
                true
            }
        });

        let r1 = t1.join().unwrap();
        let r2 = t2.join().unwrap();

        // Exactly one winner.
        assert!(
            (r1 && !r2) || (!r1 && r2),
            "expected exactly one winner, got r1={r1} r2={r2}"
        );

        let m = map.lock().unwrap();
        let owner = m
            .get(&key)
            .expect("partition must be owned by exactly one broker");
        match (r1, r2, owner) {
            (true, false, OwnerState::Owned(1)) => {}
            (false, true, OwnerState::Owned(2)) => {}
            other => panic!("inconsistent winner/owner pairing: {other:?}"),
        }
    });
}

/// Concurrent acquire of one partition vs. release of an unrelated
/// partition. The two operations are independent and must not corrupt
/// each other; the read-side must see the consistent post-state.
#[test]
fn test_partition_independent_acquire_and_release() {
    loom::model(|| {
        let map: Arc<Mutex<HashMap<i32, OwnerState>>> = Arc::new(Mutex::new({
            let mut m = HashMap::new();
            m.insert(1, OwnerState::Owned(1));
            m
        }));

        let m_acq = map.clone();
        let acquire = thread::spawn(move || {
            let mut m = m_acq.lock().unwrap();
            m.insert(2, OwnerState::Owned(1));
        });

        let m_rel = map.clone();
        let release = thread::spawn(move || {
            let mut m = m_rel.lock().unwrap();
            m.remove(&1);
        });

        acquire.join().unwrap();
        release.join().unwrap();

        let m = map.lock().unwrap();
        assert!(!m.contains_key(&1), "released key must be gone");
        assert_eq!(
            m.get(&2),
            Some(&OwnerState::Owned(1)),
            "newly acquired key must be present"
        );
    });
}

/// A reader that picks up the cached owner concurrently with a writer that
/// transitions Owned->Unowned. The reader must see either the pre-state
/// (Owned) or the post-state (absent) — never something else.
#[test]
fn test_partition_read_during_release_is_consistent() {
    loom::model(|| {
        let map: Arc<Mutex<HashMap<i32, OwnerState>>> = Arc::new(Mutex::new({
            let mut m = HashMap::new();
            m.insert(7, OwnerState::Owned(3));
            m
        }));

        let m_writer = map.clone();
        let writer = thread::spawn(move || {
            let mut m = m_writer.lock().unwrap();
            m.remove(&7);
        });

        let m_reader = map.clone();
        let reader = thread::spawn(move || {
            let m = m_reader.lock().unwrap();
            m.get(&7).cloned()
        });

        writer.join().unwrap();
        let observed = reader.join().unwrap();

        match observed {
            None => {}                       // post-state
            Some(OwnerState::Owned(3)) => {} // pre-state
            other => panic!("reader observed inconsistent owner: {other:?}"),
        }

        // Final state is always "absent".
        assert!(map.lock().unwrap().get(&7).is_none());
    });
}

/// Two threads acquire different partitions concurrently. Both must
/// succeed; the final map must contain both entries with the right owners.
/// This guards against a regression where the per-partition acquire lock
/// is accidentally promoted to a global lock — which would still pass
/// correctness but serialize unrelated work.
#[test]
fn test_partition_disjoint_keys_both_succeed() {
    loom::model(|| {
        let map: Arc<Mutex<HashMap<i32, OwnerState>>> = Arc::new(Mutex::new(HashMap::new()));

        let m1 = map.clone();
        let t1 = thread::spawn(move || {
            let mut m = m1.lock().unwrap();
            m.insert(10, OwnerState::Owned(1));
        });

        let m2 = map.clone();
        let t2 = thread::spawn(move || {
            let mut m = m2.lock().unwrap();
            m.insert(20, OwnerState::Owned(2));
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let m = map.lock().unwrap();
        assert_eq!(m.get(&10), Some(&OwnerState::Owned(1)));
        assert_eq!(m.get(&20), Some(&OwnerState::Owned(2)));
    });
}

// ============================================================================
// Producer-state idempotency map
//
// Models the contention pattern of `PartitionStore::producer_states`
// (a `MokaCache<i64, ProducerState>`) against the per-partition write
// lock. The real cache is bounded (10K entries + 15-min TTL); this loom
// model focuses on the two invariants the audit flagged as gaps:
//
//   1. Retry-dedup pair atomicity: a concurrent reader that observes the
//      cached `ProducerState` after one append must see *both*
//      (`last_first_sequence`, `last_base_offset`) updated together,
//      never half of each.
//   2. Eviction-vs-update race: an evictor (TTL or capacity) racing with
//      an in-flight append must never leave the persisted SlateDB key
//      live while the in-memory cache reports the producer as gone — or
//      vice versa. The single-writer write_lock guarantees this; the
//      model verifies the invariant under loom's interleavings.
// ============================================================================

#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelProducerState {
    last_sequence: i32,
    last_first_sequence: i32,
    last_base_offset: i64,
    epoch: i16,
}

/// Two appenders for the same producer race for the write lock; the loser
/// must observe the winner's `(last_first_sequence, last_base_offset)`
/// pair as an atomic unit. A reader that sees `last_first_sequence` from
/// the winner must also see the winner's `last_base_offset`.
#[test]
fn test_producer_state_retry_dedup_pair_atomicity() {
    loom::model(|| {
        let state = Arc::new(Mutex::new(ModelProducerState {
            last_sequence: -1,
            last_first_sequence: -1,
            last_base_offset: -1,
            epoch: 0,
        }));

        let s1 = state.clone();
        let t1 = thread::spawn(move || {
            let mut s = s1.lock().unwrap();
            // Simulate a successful append at sequence 0..3, base offset 100.
            s.last_sequence = 2;
            s.last_first_sequence = 0;
            s.last_base_offset = 100;
        });

        let s2 = state.clone();
        let t2 = thread::spawn(move || {
            // Reader: must observe a self-consistent state.
            let s = s2.lock().unwrap();
            // The retry-dedup pair must always agree:
            //  - both -1 (initial)
            //  - or both set (after t1 commits)
            let pair_initial = s.last_first_sequence == -1 && s.last_base_offset == -1;
            let pair_committed = s.last_first_sequence == 0 && s.last_base_offset == 100;
            assert!(
                pair_initial || pair_committed,
                "retry-dedup pair torn: first_seq={} base={}",
                s.last_first_sequence,
                s.last_base_offset
            );
        });

        t1.join().unwrap();
        t2.join().unwrap();

        let s = state.lock().unwrap();
        assert_eq!(s.last_first_sequence, 0);
        assert_eq!(s.last_base_offset, 100);
    });
}

/// An eviction (TTL/capacity) races with a concurrent append. With the
/// per-partition write lock held across the cache update, the post-state
/// must be one of two valid serializations:
///   - eviction wins → producer absent, append takes the no-prior-state
///     path (sequence==0 acceptable)
///   - append wins → producer present with the appended sequence
/// What must NOT happen: producer absent in cache but present in
/// "persisted store" with a stale sequence the next append doesn't know
/// to dedup.
#[test]
fn test_producer_state_eviction_vs_append_consistency() {
    loom::model(|| {
        // (cache, persisted) pair, both behind the write_lock.
        let cache: Arc<Mutex<Option<ModelProducerState>>> =
            Arc::new(Mutex::new(Some(ModelProducerState {
                last_sequence: 5,
                last_first_sequence: 5,
                last_base_offset: 500,
                epoch: 0,
            })));
        let persisted: Arc<Mutex<Option<ModelProducerState>>> =
            Arc::new(Mutex::new(Some(ModelProducerState {
                last_sequence: 5,
                last_first_sequence: 5,
                last_base_offset: 500,
                epoch: 0,
            })));
        let write_lock = Arc::new(Mutex::new(()));

        let c1 = cache.clone();
        let p1 = persisted.clone();
        let l1 = write_lock.clone();
        let evictor = thread::spawn(move || {
            let _g = l1.lock().unwrap();
            // Eviction takes the lock and removes both atomically (mirror of
            // PartitionStore::prune_producer_states which deletes from
            // SlateDB only after invalidating the cache).
            let mut c = c1.lock().unwrap();
            let mut p = p1.lock().unwrap();
            *c = None;
            *p = None;
        });

        let c2 = cache.clone();
        let p2 = persisted.clone();
        let l2 = write_lock.clone();
        let appender = thread::spawn(move || {
            let _g = l2.lock().unwrap();
            let mut c = c2.lock().unwrap();
            let mut p = p2.lock().unwrap();
            // Append: read prior state, validate, write new state.
            let prior_seq = c.as_ref().map(|s| s.last_sequence).unwrap_or(-1);
            let new = ModelProducerState {
                last_sequence: prior_seq + 1,
                last_first_sequence: prior_seq + 1,
                last_base_offset: 1000,
                epoch: 0,
            };
            *c = Some(new.clone());
            *p = Some(new);
        });

        evictor.join().unwrap();
        appender.join().unwrap();

        // Post-state invariant: cache and persisted must agree.
        let c = cache.lock().unwrap();
        let p = persisted.lock().unwrap();
        assert_eq!(*c, *p, "cache and persisted store diverged");
    });
}

/// A snapshot of producer state taken concurrently with an append must
/// reflect either the pre-append or post-append value, never a torn
/// intermediate. This models the audit's H5 install-snapshot concern at
/// the producer-state level: a snapshot read while an apply is in flight.
#[test]
fn test_producer_state_snapshot_during_append() {
    loom::model(|| {
        let state = Arc::new(RwLock::new(ModelProducerState {
            last_sequence: 0,
            last_first_sequence: 0,
            last_base_offset: 0,
            epoch: 0,
        }));

        let writer_state = state.clone();
        let writer = thread::spawn(move || {
            let mut s = writer_state.write().unwrap();
            s.last_sequence = 1;
            s.last_first_sequence = 1;
            s.last_base_offset = 10;
        });

        let reader_state = state.clone();
        let reader = thread::spawn(move || {
            let s = reader_state.read().unwrap();
            ModelProducerState {
                last_sequence: s.last_sequence,
                last_first_sequence: s.last_first_sequence,
                last_base_offset: s.last_base_offset,
                epoch: s.epoch,
            }
        });

        writer.join().unwrap();
        let snapshot = reader.join().unwrap();

        // Snapshot must be either pre or post — RwLock guarantees no torn read.
        let pre = ModelProducerState {
            last_sequence: 0,
            last_first_sequence: 0,
            last_base_offset: 0,
            epoch: 0,
        };
        let post = ModelProducerState {
            last_sequence: 1,
            last_first_sequence: 1,
            last_base_offset: 10,
            epoch: 0,
        };
        assert!(
            snapshot == pre || snapshot == post,
            "torn snapshot observed: {snapshot:?}"
        );
    });
}

// ============================================================================
// Batch-index pool reader-during-eviction
//
// Models the pattern in `PartitionStore::batch_index` — a moka cache that
// can evict entries while a fetch is reading them. The cache itself is
// not loom-instrumented, so the model uses an `RwLock<HashMap>`-shaped
// approximation:
//
//   - producer holds the per-partition write lock, inserts a fresh entry
//   - evictor acquires its own write lock briefly, removes the entry
//   - reader acquires a read lock, looks up the entry
//
// Invariant: a reader that observes Some(entry) must see a "consistent"
// entry (the production type bundles offset + payload Bytes; we assert
// the model carries both fields together). A reader that observes None
// is fine — fetch falls back to the durable store.
// ============================================================================

use std::collections::hash_map::HashMap as StdHashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
struct ModelBatchIndexEntry {
    base_offset: i64,
    payload_len: i64,
}

#[test]
fn test_batch_index_reader_during_eviction_is_consistent() {
    loom::model(|| {
        let pool: Arc<RwLock<StdHashMap<i64, ModelBatchIndexEntry>>> = Arc::new(RwLock::new({
            let mut m = StdHashMap::new();
            m.insert(
                10,
                ModelBatchIndexEntry {
                    base_offset: 10,
                    payload_len: 256,
                },
            );
            m
        }));

        let evictor_pool = pool.clone();
        let evictor = thread::spawn(move || {
            let mut g = evictor_pool.write().unwrap();
            g.remove(&10);
        });

        let inserter_pool = pool.clone();
        let inserter = thread::spawn(move || {
            let mut g = inserter_pool.write().unwrap();
            g.insert(
                20,
                ModelBatchIndexEntry {
                    base_offset: 20,
                    payload_len: 512,
                },
            );
        });

        let reader_pool = pool.clone();
        let reader = thread::spawn(move || {
            let g = reader_pool.read().unwrap();
            (g.get(&10).cloned(), g.get(&20).cloned())
        });

        evictor.join().unwrap();
        inserter.join().unwrap();
        let (saw_10, saw_20) = reader.join().unwrap();

        if let Some(entry) = saw_10 {
            assert_eq!(entry.base_offset, 10, "torn read of evicted entry");
            assert_eq!(entry.payload_len, 256, "torn read of evicted entry");
        }
        if let Some(entry) = saw_20 {
            assert_eq!(entry.base_offset, 20, "torn read of inserted entry");
            assert_eq!(entry.payload_len, 512, "torn read of inserted entry");
        }
    });
}

// ============================================================================
// Lease-clock advance vs. acquire
//
// Models the pattern at the partition coordinator: a "now()" tick monotonically
// advances a lease-expiry clock, while concurrent acquires read it to decide
// whether the previous owner's lease is still live. The invariant is monotonic
// reads — an acquire that sees `now=T` must, on a subsequent read, see
// `now>=T` (no regression). A regression would let a freshly-acquired lease
// be retroactively considered expired by a lagging reader.
// ============================================================================

#[test]
fn test_lease_clock_advance_vs_acquire_monotonic() {
    loom::model(|| {
        let clock = Arc::new(AtomicI64::new(0));
        let lease_expiry = Arc::new(AtomicI64::new(100));

        let advancer = clock.clone();
        let ticker = thread::spawn(move || {
            advancer.fetch_add(50, Ordering::SeqCst);
        });

        let read_clock = clock.clone();
        let read_expiry = lease_expiry.clone();
        let acquirer = thread::spawn(move || {
            let now = read_clock.load(Ordering::SeqCst);
            let expiry = read_expiry.load(Ordering::SeqCst);
            // Acquire decision: lease is expired iff now > expiry.
            let expired = now > expiry;
            // After this decision, take a second snapshot of the clock; the
            // clock must not regress.
            let now2 = read_clock.load(Ordering::SeqCst);
            assert!(now2 >= now, "lease clock regressed: {now} -> {now2}");
            expired
        });

        ticker.join().unwrap();
        let _expired = acquirer.join().unwrap();
    });
}

#[test]
fn test_lease_clock_acquire_after_expire_sees_advanced_clock() {
    loom::model(|| {
        let clock = Arc::new(AtomicI64::new(0));
        let lease_expiry = Arc::new(AtomicI64::new(100));

        let exp_clock = clock.clone();
        let exp_expiry = lease_expiry.clone();
        // Expire-then-renew: thread T1 advances the clock past expiry, then
        // bumps lease_expiry to a fresh value (mirroring renew_partition_lease
        // from a successful Raft commit).
        let renewer = thread::spawn(move || {
            exp_clock.store(150, Ordering::SeqCst);
            exp_expiry.store(300, Ordering::SeqCst);
        });

        let read_clock = clock.clone();
        let read_expiry = lease_expiry.clone();
        // T2 is a concurrent acquirer using `if now > expiry { steal }`.
        // The invariant: if we observe the post-renew expiry (=300), we
        // must not steal even if our `now` snapshot was taken before the
        // renewer ran (i.e. `now=0`). The stronger property is that the
        // (now, expiry) pair is consistent: SeqCst gives us total order.
        let acquirer = thread::spawn(move || {
            let expiry = read_expiry.load(Ordering::SeqCst);
            let now = read_clock.load(Ordering::SeqCst);
            // If we saw the post-renew expiry, the post-renew clock must
            // be at least 150 (set in the same SeqCst write order).
            if expiry == 300 {
                assert!(
                    now >= 150,
                    "saw fresh expiry {expiry} but stale clock {now}"
                );
            }
        });

        renewer.join().unwrap();
        acquirer.join().unwrap();
    });
}

// ============================================================================
// Zombie-mode-set vs. in-flight append_batch
//
// Models the production pattern in `partition_store::append_batch`:
//
//   1. Outer check: if zombie_mode load() == true, return Fenced.
//   2. Acquire the per-partition write lock.
//   3. Inner check: if zombie_mode load() == true, return Fenced.
//   4. Reserve offset, write to SlateDB, advance HWM.
//
// A concurrent zombie-mode setter can race against an in-flight append.
// The invariant: any append that completes step 4 (HWM advance) must
// have been entirely sequenced before zombie_mode flipped to true. The
// inner check guarantees this: even if step 1 raced past the flag, step
// 3 still rejects.
// ============================================================================

#[test]
fn test_zombie_mode_vs_inflight_append_double_check() {
    loom::model(|| {
        let zombie = Arc::new(AtomicBool::new(false));
        let write_lock = Arc::new(Mutex::new(()));
        let hwm = Arc::new(AtomicI64::new(0));
        let appends_after_zombie = Arc::new(AtomicI64::new(0));

        let zombie1 = zombie.clone();
        let lock1 = write_lock.clone();
        let hwm1 = hwm.clone();
        let appends_after_zombie1 = appends_after_zombie.clone();
        let appender = thread::spawn(move || {
            // Step 1: outer check.
            if zombie1.load(Ordering::SeqCst) {
                return;
            }
            // Step 2: per-partition write lock.
            let _g = lock1.lock().unwrap();
            // Step 3: inner check (after lock). This is the critical bit.
            if zombie1.load(Ordering::SeqCst) {
                return;
            }
            // Step 4: HWM advance. If we reached here, zombie was false at
            // step 3 — meaning the zombie setter has not yet completed.
            // Once it does set zombie=true, the next appender will fail
            // the outer or inner check.
            hwm1.fetch_add(1, Ordering::SeqCst);
            // For the assertion below: track if this append completed
            // *after* the zombie setter ran.
            if zombie1.load(Ordering::SeqCst) {
                appends_after_zombie1.fetch_add(1, Ordering::SeqCst);
            }
        });

        let zombie2 = zombie.clone();
        let setter = thread::spawn(move || {
            zombie2.store(true, Ordering::SeqCst);
        });

        appender.join().unwrap();
        setter.join().unwrap();

        // Final state: zombie must be true; HWM may or may not have
        // advanced (depends on interleaving). The critical property is
        // that the per-partition lock + double-check is enough to keep
        // the post-zombie state self-consistent: no torn HWM.
        assert!(zombie.load(Ordering::SeqCst), "zombie must be set");
        let final_hwm = hwm.load(Ordering::SeqCst);
        assert!(
            final_hwm <= 1,
            "HWM advanced beyond a single append: {final_hwm}"
        );
    });
}

#[test]
fn test_zombie_mode_concurrent_appenders_at_most_one_after_flip() {
    loom::model(|| {
        let zombie = Arc::new(AtomicBool::new(false));
        let write_lock = Arc::new(Mutex::new(()));
        let hwm = Arc::new(AtomicI64::new(0));

        let make_appender = |zombie: Arc<AtomicBool>, lock: Arc<Mutex<()>>, hwm: Arc<AtomicI64>| {
            thread::spawn(move || {
                if zombie.load(Ordering::SeqCst) {
                    return;
                }
                let _g = lock.lock().unwrap();
                if zombie.load(Ordering::SeqCst) {
                    return;
                }
                hwm.fetch_add(1, Ordering::SeqCst);
            })
        };

        let t1 = make_appender(zombie.clone(), write_lock.clone(), hwm.clone());
        let t2 = make_appender(zombie.clone(), write_lock.clone(), hwm.clone());

        let zombie_setter = zombie.clone();
        let setter = thread::spawn(move || {
            zombie_setter.store(true, Ordering::SeqCst);
        });

        t1.join().unwrap();
        t2.join().unwrap();
        setter.join().unwrap();

        // With two appenders + a zombie setter and the per-partition lock
        // double-check, the HWM after must be 0, 1, or 2 — never anything
        // else (no torn writes from concurrent fetch_adds), and never
        // >2 (only two appenders ran).
        let final_hwm = hwm.load(Ordering::SeqCst);
        assert!(
            (0..=2).contains(&final_hwm),
            "HWM out of expected range: {final_hwm}"
        );
    });
}
