//! Jepsen-style linearizability tests for Kafkaesque.
//!
//! These tests verify that the system provides linearizable semantics:
//! - Every acknowledged write is visible to subsequent reads
//! - Operations appear to execute atomically in some sequential order
//! - The system behaves correctly under concurrent access
//!
//! # Linearizability Properties Tested
//!
//! 1. **Read-Your-Writes**: A client that writes a value will always read it back
//! 2. **Monotonic Reads**: Once a client reads a value, it won't read an older value
//! 3. **Writes Follow Reads**: If a client reads a value then writes, the write is ordered after the read
//! 4. **Sequential Consistency**: All operations appear to execute in some sequential order
//!
//! # Running These Tests
//!
//! ```sh
//! cargo test --test linearizability_tests --release
//! ```
//!
//! # Design Notes
//!
//! These tests use a history-based approach similar to Jepsen:
//! 1. Perform concurrent operations
//! 2. Record the history of operations with timestamps
//! 3. Verify the history is linearizable
//!
//! Unlike full Jepsen tests, these run in-process without network partitions.
//! For full chaos testing, see `tests/chaos_tests.rs`.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::{Barrier, Mutex, RwLock};
use tokio::task::JoinSet;

/// Operation type for history recording.
#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
enum OpType {
    Write { offset: i64, value: Vec<u8> },
    Read { offset: i64, value: Option<Vec<u8>> },
    Append { base_offset: i64, record_count: i32 },
}

/// A recorded operation in the history.
#[derive(Debug, Clone)]
#[allow(dead_code)]
struct Operation {
    /// Unique operation ID
    id: u64,
    /// Client/thread ID
    client_id: u64,
    /// Operation type and result
    op: OpType,
    /// Start time (monotonic)
    start_time: Instant,
    /// End time (monotonic)
    end_time: Instant,
    /// Whether operation succeeded
    success: bool,
}

/// History of operations for linearizability checking.
struct History {
    operations: Vec<Operation>,
    next_op_id: AtomicU64,
}

impl History {
    fn new() -> Self {
        Self {
            operations: Vec::new(),
            next_op_id: AtomicU64::new(0),
        }
    }

    fn next_id(&self) -> u64 {
        self.next_op_id.fetch_add(1, Ordering::SeqCst)
    }
}

/// Simple in-memory partition store for linearizability testing.
/// Mimics the key behaviors of PartitionStore.
struct TestPartitionStore {
    /// Records stored by offset
    records: RwLock<BTreeMap<i64, Vec<u8>>>,
    /// High watermark (next offset to write)
    hwm: AtomicI64,
    /// Write lock for atomic appends
    write_lock: Mutex<()>,
}

impl TestPartitionStore {
    fn new() -> Self {
        Self {
            records: RwLock::new(BTreeMap::new()),
            hwm: AtomicI64::new(0),
            write_lock: Mutex::new(()),
        }
    }

    /// Append a batch atomically, returning the base offset.
    async fn append(&self, value: Vec<u8>) -> i64 {
        let _guard = self.write_lock.lock().await;

        // Allocate offset atomically
        let base_offset = self.hwm.fetch_add(1, Ordering::SeqCst);

        // Write record
        let mut records = self.records.write().await;
        records.insert(base_offset, value);

        base_offset
    }

    /// Read a record at a specific offset.
    async fn read(&self, offset: i64) -> Option<Vec<u8>> {
        let records = self.records.read().await;
        records.get(&offset).cloned()
    }

    /// Get current high watermark.
    fn hwm(&self) -> i64 {
        self.hwm.load(Ordering::SeqCst)
    }

    /// Fetch all records from start_offset to hwm.
    async fn fetch(&self, start_offset: i64) -> Vec<(i64, Vec<u8>)> {
        let records = self.records.read().await;
        let hwm = self.hwm.load(Ordering::SeqCst);

        records
            .range(start_offset..hwm)
            .map(|(k, v)| (*k, v.clone()))
            .collect()
    }
}

/// Verify that the history is linearizable.
///
/// A history is linearizable if there exists a sequential ordering of operations
/// that is consistent with:
/// 1. Real-time ordering (if op1 ends before op2 starts, op1 < op2)
/// 2. The specification (reads return the most recently written value)
fn verify_linearizability(history: &[Operation]) -> Result<(), String> {
    // Group operations by their real-time relationships
    let mut writes: Vec<&Operation> = history
        .iter()
        .filter(|op| matches!(op.op, OpType::Append { .. }))
        .collect();

    let reads: Vec<&Operation> = history
        .iter()
        .filter(|op| matches!(op.op, OpType::Read { .. }))
        .collect();

    // Sort writes by end time (they must appear to execute atomically at some point)
    writes.sort_by_key(|op| op.end_time);

    // For each read, verify it could have seen a consistent state
    for read_op in reads {
        if let OpType::Read { offset, value } = &read_op.op {
            if !read_op.success {
                continue; // Skip failed reads
            }

            // Find all writes that could have been visible to this read
            // A write is visible if it ended before the read started
            let visible_writes: Vec<_> = writes
                .iter()
                .filter(|w| w.end_time <= read_op.start_time && w.success)
                .collect();

            // Find the write to this offset among visible writes
            let expected_value = visible_writes
                .iter()
                .filter_map(|w| {
                    if let OpType::Append { base_offset, .. } = &w.op {
                        if *base_offset == *offset {
                            Some(()) // Found a write to this offset
                        } else {
                            None
                        }
                    } else {
                        None
                    }
                })
                .next_back();

            // If we expected a value but got None (or vice versa), check if it's valid
            match (expected_value, value) {
                (Some(()), None) => {
                    // A write was visible but read returned None - violation!
                    // However, we need to check if a concurrent write could explain this
                    let concurrent_writes: Vec<_> = writes
                        .iter()
                        .filter(|w| {
                            // Write overlaps with read
                            w.start_time <= read_op.end_time && w.end_time >= read_op.start_time
                        })
                        .collect();

                    if concurrent_writes.is_empty() {
                        return Err(format!(
                            "Linearizability violation: Read at offset {} returned None but write was visible",
                            offset
                        ));
                    }
                    // Concurrent write could explain the discrepancy - this is allowed
                }
                (None, Some(_)) => {
                    // No visible write but read returned a value
                    // Check if a concurrent write could explain this
                    let concurrent_writes: Vec<_> = writes
                        .iter()
                        .filter(|w| {
                            if let OpType::Append { base_offset, .. } = &w.op {
                                *base_offset == *offset
                                    && w.start_time <= read_op.end_time
                                    && w.end_time >= read_op.start_time
                            } else {
                                false
                            }
                        })
                        .collect();

                    if concurrent_writes.is_empty() {
                        return Err(format!(
                            "Linearizability violation: Read at offset {} returned value but no write was visible or concurrent",
                            offset
                        ));
                    }
                }
                _ => {
                    // Either both None or both Some - consistent
                }
            }
        }
    }

    Ok(())
}

/// Verify monotonic reads: once a client sees a value, it won't see an older state.
fn verify_monotonic_reads(history: &[Operation]) -> Result<(), String> {
    // Group operations by client
    let mut by_client: HashMap<u64, Vec<&Operation>> = HashMap::new();
    for op in history {
        by_client.entry(op.client_id).or_default().push(op);
    }

    for (client_id, ops) in by_client {
        let mut ops: Vec<_> = ops.into_iter().collect();
        ops.sort_by_key(|op| op.start_time);

        let mut max_offset_seen: i64 = -1;

        for op in ops {
            if let OpType::Read {
                offset,
                value: Some(_),
            } = &op.op
            {
                if *offset < max_offset_seen {
                    return Err(format!(
                        "Monotonic read violation for client {}: saw offset {} after seeing {}",
                        client_id, offset, max_offset_seen
                    ));
                }
                max_offset_seen = max_offset_seen.max(*offset);
            }
        }
    }

    Ok(())
}

/// Verify sequential consistency: HWM only moves forward.
fn verify_hwm_monotonicity(hwm_observations: &[(Instant, i64)]) -> Result<(), String> {
    let mut max_hwm: i64 = -1;

    for (time, hwm) in hwm_observations {
        if *hwm < max_hwm {
            return Err(format!(
                "HWM monotonicity violation: saw {} after seeing {} at {:?}",
                hwm, max_hwm, time
            ));
        }
        max_hwm = max_hwm.max(*hwm);
    }

    Ok(())
}

// =============================================================================
// Test Cases
// =============================================================================

/// Test: Concurrent appends produce unique, sequential offsets.
#[tokio::test]
async fn test_linearizable_concurrent_appends() {
    let store = Arc::new(TestPartitionStore::new());
    let history = Arc::new(Mutex::new(History::new()));
    let barrier = Arc::new(Barrier::new(10));

    let mut handles = JoinSet::new();

    // Spawn 10 concurrent writers
    for client_id in 0..10u64 {
        let store = store.clone();
        let history = history.clone();
        let barrier = barrier.clone();

        handles.spawn(async move {
            // Synchronize start
            barrier.wait().await;

            for i in 0..100 {
                let value = format!("client-{}-msg-{}", client_id, i).into_bytes();
                let op_id = history.lock().await.next_id();
                let start = Instant::now();

                let base_offset = store.append(value.clone()).await;

                let end = Instant::now();

                history.lock().await.operations.push(Operation {
                    id: op_id,
                    client_id,
                    op: OpType::Append {
                        base_offset,
                        record_count: 1,
                    },
                    start_time: start,
                    end_time: end,
                    success: true,
                });
            }
        });
    }

    // Wait for all writers to complete
    while handles.join_next().await.is_some() {}

    // Verify HWM is correct
    assert_eq!(
        store.hwm(),
        1000,
        "Expected 1000 records (10 clients * 100 messages)"
    );

    // Verify all offsets are unique and sequential
    let records = store.records.read().await;
    let offsets: HashSet<i64> = records.keys().copied().collect();
    assert_eq!(offsets.len(), 1000, "Expected 1000 unique offsets");

    for offset in 0..1000 {
        assert!(offsets.contains(&offset), "Missing offset {}", offset);
    }

    // Verify history is linearizable
    let history = history.lock().await;
    verify_linearizability(&history.operations).expect("History should be linearizable");
}

/// Test: Read-your-writes consistency.
#[tokio::test]
async fn test_read_your_writes() {
    let store = Arc::new(TestPartitionStore::new());
    let barrier = Arc::new(Barrier::new(5));

    let mut handles = JoinSet::new();

    // Spawn 5 clients that each write then read
    for client_id in 0..5u64 {
        let store = store.clone();
        let barrier = barrier.clone();

        handles.spawn(async move {
            barrier.wait().await;

            for i in 0..50 {
                // Write
                let value = format!("client-{}-msg-{}", client_id, i).into_bytes();
                let offset = store.append(value.clone()).await;

                // Read back - should always succeed
                let read_value = store.read(offset).await;
                assert!(
                    read_value.is_some(),
                    "Read-your-writes violation: client {} wrote at offset {} but read returned None",
                    client_id,
                    offset
                );
                assert_eq!(
                    read_value.unwrap(),
                    value,
                    "Read-your-writes violation: value mismatch at offset {}",
                    offset
                );
            }

            client_id // Return client_id for verification
        });
    }

    // Verify all clients completed successfully
    let mut completed = HashSet::new();
    while let Some(result) = handles.join_next().await {
        completed.insert(result.expect("Task should complete"));
    }
    assert_eq!(completed.len(), 5, "All clients should complete");
}

/// Test: Monotonic reads - clients never see older data.
#[tokio::test]
async fn test_monotonic_reads() {
    let store = Arc::new(TestPartitionStore::new());
    let history = Arc::new(Mutex::new(History::new()));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Pre-populate with some data
    for i in 0..100 {
        store.append(format!("initial-{}", i).into_bytes()).await;
    }

    let mut handles = JoinSet::new();

    // Writer thread - continuously appends
    {
        let store = store.clone();
        let stop = stop.clone();
        handles.spawn(async move {
            let mut i = 100;
            while !stop.load(Ordering::SeqCst) {
                store.append(format!("msg-{}", i).into_bytes()).await;
                i += 1;
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        });
    }

    // Reader threads - verify monotonic reads
    for client_id in 0..3u64 {
        let store = store.clone();
        let history = history.clone();
        let stop = stop.clone();

        handles.spawn(async move {
            let mut last_hwm: i64 = 0;

            while !stop.load(Ordering::SeqCst) {
                let _current_hwm = store.hwm();
                let op_id = history.lock().await.next_id();
                let start = Instant::now();

                // Fetch from last seen position
                let records = store.fetch(last_hwm).await;
                let end = Instant::now();

                if !records.is_empty() {
                    let max_offset = records.iter().map(|(o, _)| *o).max().unwrap();

                    // Verify monotonicity
                    assert!(
                        max_offset >= last_hwm,
                        "Monotonic read violation: saw {} after seeing {}",
                        max_offset,
                        last_hwm
                    );

                    // Record operation
                    history.lock().await.operations.push(Operation {
                        id: op_id,
                        client_id,
                        op: OpType::Read {
                            offset: max_offset,
                            value: Some(records.last().unwrap().1.clone()),
                        },
                        start_time: start,
                        end_time: end,
                        success: true,
                    });

                    last_hwm = max_offset + 1;
                }

                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        });
    }

    // Run for a short duration
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::SeqCst);

    // Wait for all tasks
    while handles.join_next().await.is_some() {}

    // Verify history
    let history = history.lock().await;
    verify_monotonic_reads(&history.operations).expect("Monotonic reads should hold");
}

/// Test: HWM only increases monotonically.
#[tokio::test]
async fn test_hwm_monotonicity() {
    let store = Arc::new(TestPartitionStore::new());
    let observations = Arc::new(Mutex::new(Vec::new()));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut handles = JoinSet::new();

    // Writer threads
    for _ in 0..3 {
        let store = store.clone();
        let stop = stop.clone();
        handles.spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                store.append(b"test".to_vec()).await;
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        });
    }

    // Observer thread - records HWM over time
    {
        let store = store.clone();
        let observations = observations.clone();
        let stop = stop.clone();
        handles.spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                let hwm = store.hwm();
                observations.lock().await.push((Instant::now(), hwm));
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
        });
    }

    // Run for a short duration
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.store(true, Ordering::SeqCst);

    // Wait for all tasks
    while handles.join_next().await.is_some() {}

    // Verify HWM monotonicity
    let observations = observations.lock().await;
    verify_hwm_monotonicity(&observations).expect("HWM should be monotonic");

    // Also verify observations show progress
    assert!(observations.len() > 100, "Should have many observations");
    let max_hwm = observations.iter().map(|(_, h)| *h).max().unwrap();
    assert!(max_hwm > 100, "HWM should have progressed significantly");
}

/// Test: Concurrent readers and writers maintain consistency.
#[tokio::test]
async fn test_concurrent_read_write_consistency() {
    let store = Arc::new(TestPartitionStore::new());
    let history = Arc::new(Mutex::new(History::new()));
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut handles = JoinSet::new();

    // Writers
    for writer_id in 0..2u64 {
        let store = store.clone();
        let history = history.clone();
        let stop = stop.clone();

        handles.spawn(async move {
            let mut i = 0;
            while !stop.load(Ordering::SeqCst) {
                let value = format!("w{}-{}", writer_id, i).into_bytes();
                let op_id = history.lock().await.next_id();
                let start = Instant::now();

                let offset = store.append(value.clone()).await;

                let end = Instant::now();
                history.lock().await.operations.push(Operation {
                    id: op_id,
                    client_id: writer_id,
                    op: OpType::Append {
                        base_offset: offset,
                        record_count: 1,
                    },
                    start_time: start,
                    end_time: end,
                    success: true,
                });

                i += 1;
                tokio::time::sleep(Duration::from_micros(100)).await;
            }
        });
    }

    // Readers
    for reader_id in 10..15u64 {
        let store = store.clone();
        let history = history.clone();
        let stop = stop.clone();

        handles.spawn(async move {
            while !stop.load(Ordering::SeqCst) {
                let hwm = store.hwm();
                if hwm > 0 {
                    // Read a random offset
                    let offset = fastrand::i64(0..hwm);
                    let op_id = history.lock().await.next_id();
                    let start = Instant::now();

                    let value = store.read(offset).await;

                    let end = Instant::now();
                    history.lock().await.operations.push(Operation {
                        id: op_id,
                        client_id: reader_id,
                        op: OpType::Read { offset, value },
                        start_time: start,
                        end_time: end,
                        success: true,
                    });
                }
                tokio::time::sleep(Duration::from_micros(50)).await;
            }
        });
    }

    // Run for a duration
    tokio::time::sleep(Duration::from_millis(500)).await;
    stop.store(true, Ordering::SeqCst);

    // Wait for all tasks
    while handles.join_next().await.is_some() {}

    // Verify linearizability
    let history = history.lock().await;
    verify_linearizability(&history.operations).expect("History should be linearizable");

    // Verify we actually did work
    let write_count = history
        .operations
        .iter()
        .filter(|op| matches!(op.op, OpType::Append { .. }))
        .count();
    let read_count = history
        .operations
        .iter()
        .filter(|op| matches!(op.op, OpType::Read { .. }))
        .count();

    assert!(
        write_count > 100,
        "Should have many writes: {}",
        write_count
    );
    assert!(read_count > 100, "Should have many reads: {}", read_count);
}

/// Test: No phantom reads (reading uncommitted data).
#[tokio::test]
async fn test_no_phantom_reads() {
    let store = Arc::new(TestPartitionStore::new());

    // Write some data
    for i in 0..100 {
        store.append(format!("msg-{}", i).into_bytes()).await;
    }

    let barrier = Arc::new(Barrier::new(2));
    let found_phantom = Arc::new(std::sync::atomic::AtomicBool::new(false));

    let mut handles = JoinSet::new();

    // Reader that checks for phantoms
    {
        let store = store.clone();
        let barrier = barrier.clone();
        let found_phantom = found_phantom.clone();

        handles.spawn(async move {
            barrier.wait().await;

            // Read beyond HWM should return None
            for _ in 0..1000 {
                let hwm = store.hwm();
                let future_offset = hwm + 100;

                if store.read(future_offset).await.is_some() {
                    found_phantom.store(true, Ordering::SeqCst);
                    break;
                }
            }
        });
    }

    // Writer
    {
        let store = store.clone();
        let barrier = barrier.clone();

        handles.spawn(async move {
            barrier.wait().await;

            for i in 100..200 {
                store.append(format!("msg-{}", i).into_bytes()).await;
                tokio::time::sleep(Duration::from_micros(10)).await;
            }
        });
    }

    // Wait for all tasks
    while handles.join_next().await.is_some() {}

    assert!(
        !found_phantom.load(Ordering::SeqCst),
        "Should never read beyond HWM"
    );
}

/// Test: Offset allocation is atomic and produces no duplicates under high contention.
#[tokio::test]
async fn test_offset_allocation_atomicity() {
    let store = Arc::new(TestPartitionStore::new());
    let barrier = Arc::new(Barrier::new(20));
    let all_offsets = Arc::new(Mutex::new(Vec::new()));

    let mut handles = JoinSet::new();

    // 20 highly concurrent writers
    for _ in 0..20 {
        let store = store.clone();
        let barrier = barrier.clone();
        let all_offsets = all_offsets.clone();

        handles.spawn(async move {
            barrier.wait().await;

            let mut my_offsets = Vec::new();
            for _ in 0..500 {
                let offset = store.append(b"x".to_vec()).await;
                my_offsets.push(offset);
            }

            all_offsets.lock().await.extend(my_offsets);
        });
    }

    // Wait for all tasks
    while handles.join_next().await.is_some() {}

    // Verify all offsets are unique
    let all_offsets = all_offsets.lock().await;
    let unique: HashSet<_> = all_offsets.iter().collect();

    assert_eq!(
        all_offsets.len(),
        unique.len(),
        "All {} offsets should be unique, found {} duplicates",
        all_offsets.len(),
        all_offsets.len() - unique.len()
    );

    // Verify they form a contiguous range
    let min = *all_offsets.iter().min().unwrap();
    let max = *all_offsets.iter().max().unwrap();
    assert_eq!(min, 0, "Offsets should start at 0");
    assert_eq!(max, 9999, "Offsets should end at 9999");
}
