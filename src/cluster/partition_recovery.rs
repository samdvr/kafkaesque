//! Recovery logic for partition stores.
//!
//! This module handles recovery operations when opening a partition:
//! - HWM (high watermark) recovery from record data
//! - Producer state loading for idempotency restoration
//! - Offset continuity validation
//!
//! # Recovery Strategy
//!
//! When a partition is opened, we need to recover state that may not have been
//! persisted due to `await_durable=false` writes:
//!
//! 1. **HWM Recovery**: The persisted HWM may be stale if the broker crashed
//!    before flushing. We scan records to find the actual highest offset.
//!
//! 2. **Gap Detection**: During HWM recovery, we validate offset continuity.
//!    Gaps before the persisted HWM indicate confirmed data loss. Gaps after
//!    the HWM might be unflushed writes (false positives).
//!
//! 3. **Producer State**: Load persisted (producer_id -> sequence, epoch)
//!    mappings to restore idempotency checking after restart.

use slatedb::Db;
use std::collections::HashMap;
use tracing::{debug, error, info, warn};

use super::keys::{
    PRODUCER_STATE_KEY_PREFIX, PersistedProducerState, RECORD_KEY_PREFIX, decode_producer_id,
    decode_producer_state_value, decode_record_offset, encode_record_key, parse_record_count,
};

/// Result of the recovery scan over record keys.
pub struct RecoveryOutcome {
    /// The recovered high watermark.
    pub high_watermark: i64,
    /// `(base_offset, record_count)` of every batch seen by the scan, in
    /// offset order. Reused by the caller to warm the batch index so opening
    /// a partition needs only one pass over the record keyspace instead of
    /// two (recovery + warm) — a 2x open/failover latency win on large logs.
    pub batches: Vec<(i64, i32)>,
}

/// Recover the high watermark by scanning records.
///
/// This handles the case where `await_durable=false` writes may not have
/// persisted the HWM key before a crash. We scan records at or above
/// `scan_floor` to find the actual highest committed offset.
///
/// # Bounded Recovery Scan
///
/// `scan_floor` should be `max(log_start_offset, checkpointed_hwm)`:
/// - Records below the log start offset were deleted by retention.
/// - Records below a *checkpointed* HWM were durable at checkpoint time
///   (the checkpoint is written with the batch in one atomic `WriteBatch`),
///   so re-validating them on every open is wasted I/O. The scan only needs
///   to cover the window where unflushed writes could have landed.
///
/// Passing `scan_floor = 0` performs the legacy full-log scan.
///
/// # Gap Detection
///
/// Gaps are classified as:
/// - **Confirmed gaps**: Before persisted HWM - definite data loss
/// - **Potential gaps**: At/after persisted HWM - may be unflushed writes
///
/// Only confirmed gaps trigger errors when `fail_on_gap` is true.
/// Continuity is validated from `scan_floor` (offsets below it are covered
/// by the checkpoint invariant).
///
/// # Arguments
///
/// * `db` - The SlateDB instance
/// * `persisted_hwm` - The HWM stored in the database
/// * `fail_on_gap` - If true, return an error when confirmed gaps are detected
/// * `scan_floor` - Lowest offset the scan must cover (see above)
///
/// # Returns
///
/// The recovered high watermark and the scanned batch boundaries, or an
/// error if confirmed gaps are detected and `fail_on_gap` is true.
pub async fn recover_hwm_from_records(
    db: &Db,
    persisted_hwm: i64,
    fail_on_gap: bool,
    scan_floor: i64,
) -> Result<RecoveryOutcome, slatedb::Error> {
    let mut highest_found = persisted_hwm;
    let scan_floor = scan_floor.max(0);

    // Track batches for continuity validation
    // Vec of (base_offset, record_count) sorted by offset
    let mut batches: Vec<(i64, i32)> = Vec::new();

    let start_key = encode_record_key(scan_floor);
    let end_key = [RECORD_KEY_PREFIX + 1];

    // Scan records from the floor. Errors propagate: a mid-scan storage
    // failure must fail the open, not silently truncate recovery (which
    // would under-recover the HWM and overwrite committed offsets).
    let mut iter = db.scan(start_key.as_slice()..end_key.as_slice()).await?;

    while let Some(item) = iter.next().await? {
        // Decode offset from key
        if let Some(offset) = decode_record_offset(&item.key) {
            // Extract HWM from metadata if present
            // Format: [new_hwm: i64][record_batch: bytes]
            let (hwm_from_metadata, batch_data) = if item.value.len() >= 8 {
                // Use expect() with descriptive message instead of unwrap().
                // The length check above guarantees we have 8 bytes, so this should never fail,
                // but expect() provides better diagnostics if it somehow does.
                let hwm_bytes: [u8; 8] = item.value[0..8]
                    .try_into()
                    .expect("slice of exactly 8 bytes should convert to [u8; 8]");
                let hwm = i64::from_be_bytes(hwm_bytes);
                (Some(hwm), &item.value[8..])
            } else {
                // Old format - no metadata
                (None, item.value.as_ref())
            };

            let record_count = parse_record_count(batch_data);
            if record_count > 0 {
                // Prefer HWM from metadata (atomically written with batch)
                if let Some(hwm) = hwm_from_metadata
                    && hwm > highest_found
                {
                    highest_found = hwm;
                }

                // Also compute from offsets (fallback for old format)
                let batch_end = offset + record_count as i64;
                if batch_end > highest_found {
                    highest_found = batch_end;
                }

                // Track batch for continuity validation
                batches.push((offset, record_count));
            }
        }
    }

    // Validate offset continuity from the scan floor.
    //
    // Use `highest_found` (the max of the standalone `_hwm` checkpoint and
    // every batch-embedded HWM observed during the scan) as the reference
    // line between "confirmed gap" and "potential unflushed write". The
    // standalone checkpoint advances every 64 batches, so a fresh-crash
    // partition with two batches written but no checkpoint would have all
    // its gaps mis-classified as potential and the broker would come online
    // silently. Batch-embedded HWMs are written atomically with the batch,
    // so they accurately reflect what the broker had committed.
    validate_offset_continuity_from(
        &batches,
        highest_found,
        fail_on_gap,
        highest_found,
        scan_floor,
    )?;

    // The bounded scan can legitimately come back empty (no writes since the
    // checkpoint), so the "empty log but non-zero HWM" truncation check moves
    // to a single cheap existence probe over the full record range.
    if batches.is_empty() && persisted_hwm > scan_floor.max(0) && scan_floor > 0 {
        let probe_start = [RECORD_KEY_PREFIX];
        let probe_end = [RECORD_KEY_PREFIX + 1];
        let mut probe = db
            .scan(probe_start.as_slice()..probe_end.as_slice())
            .await?;
        if probe.next().await?.is_none() {
            error!(
                persisted_hwm,
                scan_floor, "Empty record log with non-zero persisted HWM - undetected truncation"
            );
            if fail_on_gap {
                return Err(slatedb::Error::invalid(format!(
                    "Empty record log but persisted HWM = {}; refusing to open. \
                     Set FAIL_ON_RECOVERY_GAP=false to override.",
                    persisted_hwm
                )));
            }
        }
    }

    Ok(RecoveryOutcome {
        high_watermark: highest_found,
        batches,
    })
}

/// Validate that record batches form a continuous offset sequence,
/// starting at offset 0 (full-log scan semantics).
///
/// This detects gaps that could indicate data loss.
#[cfg(test)]
fn validate_offset_continuity(
    batches: &[(i64, i32)],
    persisted_hwm: i64,
    fail_on_gap: bool,
    highest_found: i64,
) -> Result<(), slatedb::Error> {
    validate_offset_continuity_from(batches, persisted_hwm, fail_on_gap, highest_found, 0)
}

/// Validate that record batches form a continuous offset sequence from
/// `start_offset` onward. Offsets below `start_offset` are covered by the
/// HWM-checkpoint / log-start-offset invariants and are not re-validated.
fn validate_offset_continuity_from(
    batches: &[(i64, i32)],
    persisted_hwm: i64,
    fail_on_gap: bool,
    highest_found: i64,
    start_offset: i64,
) -> Result<(), slatedb::Error> {
    // Invariant (full scans only): a non-zero persisted HWM with no records
    // means the log was truncated under us. Before this check the partition
    // would have come back online empty and silently advertised HWM=0.
    // For bounded scans (start_offset > 0) the caller performs a cheap
    // existence probe instead, since an empty window is normal.
    if start_offset == 0 && batches.is_empty() && persisted_hwm > 0 {
        error!(
            persisted_hwm,
            "Empty record log with non-zero persisted HWM - undetected truncation"
        );
        if fail_on_gap {
            return Err(slatedb::Error::invalid(format!(
                "Empty record log but persisted HWM = {}; refusing to open. \
                 Set FAIL_ON_RECOVERY_GAP=false to override.",
                persisted_hwm
            )));
        }
    }

    // Sort batches by offset (should already be sorted from SlateDB scan, but be safe)
    let mut sorted_batches = batches.to_vec();
    sorted_batches.sort_by_key(|(offset, _)| *offset);

    let mut expected_offset: i64 = start_offset.max(0);
    let mut gap_count = 0;
    let mut total_gap_records: i64 = 0;
    let mut overlap_count = 0;
    // Track gaps separately for confirmed (before HWM) vs potential (after HWM)
    let mut confirmed_gap_count = 0;
    let mut potential_gap_count = 0;

    for (batch_offset, record_count) in &sorted_batches {
        if *batch_offset > expected_offset {
            // Gap detected!
            let gap_size = batch_offset - expected_offset;
            gap_count += 1;
            total_gap_records += gap_size;

            // Only log as error if the gap is before the persisted HWM
            // (confirmed data loss). Gaps at/after HWM might be unflushed writes.
            if expected_offset < persisted_hwm {
                confirmed_gap_count += 1;
                error!(
                    expected_offset,
                    actual_offset = batch_offset,
                    gap_size,
                    persisted_hwm,
                    "Confirmed offset gap detected during HWM recovery - data loss before persisted HWM!"
                );
            } else {
                potential_gap_count += 1;
                // Downgrade to warning for gaps after HWM
                warn!(
                    expected_offset,
                    actual_offset = batch_offset,
                    gap_size,
                    persisted_hwm,
                    "Potential offset gap detected after persisted HWM - may be unflushed writes"
                );
            }
        } else if *batch_offset < expected_offset {
            // Overlapping batches break the per-offset uniqueness assumption
            // every downstream consumer relies on. Promote to error and fail
            // when gap-checking is enabled — silently accepting overlap risks
            // double-delivery on read.
            overlap_count += 1;
            error!(
                expected_offset,
                actual_offset = batch_offset,
                "Overlapping batches detected during HWM recovery - duplicate offsets"
            );
        }
        expected_offset = batch_offset + *record_count as i64;
    }

    if gap_count > 0 {
        log_gap_summary(
            confirmed_gap_count,
            potential_gap_count,
            total_gap_records,
            highest_found,
            persisted_hwm,
            fail_on_gap,
        );

        // Record metric for monitoring
        super::metrics::record_recovery_gap(gap_count, total_gap_records);

        // Only fail if there are CONFIRMED gaps (before persisted HWM)
        if fail_on_gap && confirmed_gap_count > 0 {
            return Err(slatedb::Error::invalid(format!(
                "Offset gaps detected during recovery: {} gaps, {} potentially lost records. \
                 Set FAIL_ON_RECOVERY_GAP=false to continue despite gaps.",
                gap_count, total_gap_records
            )));
        }
    } else if !sorted_batches.is_empty() {
        info!(
            batch_count = sorted_batches.len(),
            highest_found, "HWM recovery completed - offset continuity validated"
        );
    }

    if fail_on_gap && overlap_count > 0 {
        return Err(slatedb::Error::invalid(format!(
            "Overlapping record batches detected during recovery: {} overlap(s). \
             Set FAIL_ON_RECOVERY_GAP=false to continue.",
            overlap_count
        )));
    }

    Ok(())
}

/// Log a summary of detected gaps.
fn log_gap_summary(
    confirmed_gap_count: usize,
    potential_gap_count: usize,
    total_gap_records: i64,
    highest_found: i64,
    persisted_hwm: i64,
    fail_on_gap: bool,
) {
    if confirmed_gap_count > 0 {
        error!(
            confirmed_gap_count,
            potential_gap_count,
            total_gap_records,
            highest_found,
            persisted_hwm,
            fail_on_gap,
            "HWM recovery found {} confirmed gaps (before HWM) and {} potential gaps (after HWM)",
            confirmed_gap_count,
            potential_gap_count
        );
    } else {
        // All gaps are potential (after HWM) - downgrade to warning
        warn!(
            potential_gap_count,
            total_gap_records,
            highest_found,
            persisted_hwm,
            "HWM recovery found {} potential gaps after persisted HWM (likely unflushed writes)",
            potential_gap_count
        );
    }
}

/// Load persisted producer states from SlateDB.
///
/// This is called during partition open to restore idempotency state that
/// survives broker restarts. Producer states are stored with key prefix 'p'.
///
/// # Producer State Format
///
/// - Key: `[PRODUCER_STATE_KEY_PREFIX][producer_id: i64 big-endian]`
/// - Value: see [`PersistedProducerState`] (legacy 6-byte values accepted)
///
/// # Returns
///
/// A map of `producer_id -> PersistedProducerState`
pub async fn load_producer_states(
    db: &Db,
) -> Result<HashMap<i64, PersistedProducerState>, slatedb::Error> {
    let mut states = HashMap::new();

    // Scan all producer state keys
    let start_key = [PRODUCER_STATE_KEY_PREFIX];
    let end_key = [PRODUCER_STATE_KEY_PREFIX + 1];

    let mut iter = db.scan(start_key.as_slice()..end_key.as_slice()).await?;

    // Propagate scan errors: silently stopping mid-scan would drop producer
    // states and accept duplicate batches as new after restart.
    while let Some(item) = iter.next().await? {
        if let Some(producer_id) = decode_producer_id(&item.key)
            && let Some(state) = decode_producer_state_value(&item.value)
        {
            debug!(
                producer_id,
                last_sequence = state.last_sequence,
                producer_epoch = state.producer_epoch,
                "Loaded producer state"
            );
            states.insert(producer_id, state);
        }
    }

    if !states.is_empty() {
        info!(
            producer_count = states.len(),
            "Loaded producer states for idempotency"
        );
    }

    Ok(states)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_validate_offset_continuity_no_gaps() {
        // Continuous sequence: [0-10), [10-20), [20-30)
        let batches = vec![(0, 10), (10, 10), (20, 10)];
        let result = validate_offset_continuity(&batches, 30, true, 30);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_with_potential_gap() {
        // Gap after HWM: [0-10), [20-30) with HWM=10
        // This is a "potential" gap (might be unflushed writes)
        let batches = vec![(0, 10), (20, 10)];
        // With fail_on_gap=true, should still succeed since gap is after HWM
        let result = validate_offset_continuity(&batches, 10, true, 30);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_with_confirmed_gap() {
        // Gap before HWM: [0-10), [20-30) with HWM=30
        // This is a "confirmed" gap (definite data loss)
        let batches = vec![(0, 10), (20, 10)];
        // With fail_on_gap=true, should fail
        let result = validate_offset_continuity(&batches, 30, true, 30);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_offset_continuity_fail_disabled() {
        // Same confirmed gap but with fail_on_gap=false
        let batches = vec![(0, 10), (20, 10)];
        let result = validate_offset_continuity(&batches, 30, false, 30);
        assert!(result.is_ok()); // Should succeed despite gap
    }

    #[test]
    fn test_validate_offset_continuity_empty() {
        let batches: Vec<(i64, i32)> = vec![];
        let result = validate_offset_continuity(&batches, 0, true, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_unsorted() {
        // Batches out of order should still work (we sort internally)
        let batches = vec![(20, 10), (0, 10), (10, 10)];
        let result = validate_offset_continuity(&batches, 30, true, 30);
        assert!(result.is_ok());
    }

    // ========================================================================
    // Additional Edge Case Tests
    // ========================================================================

    #[test]
    fn test_validate_offset_continuity_single_batch() {
        // Just one batch: [0-10)
        let batches = vec![(0, 10)];
        let result = validate_offset_continuity(&batches, 10, true, 10);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_gap_at_start() {
        // Gap at the start: [10-20) with HWM=20
        // This is a confirmed gap (offsets 0-9 are missing before HWM)
        let batches = vec![(10, 10)];
        let result = validate_offset_continuity(&batches, 20, true, 20);
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_offset_continuity_gap_at_start_fail_disabled() {
        // Gap at the start with fail_on_gap=false
        let batches = vec![(10, 10)];
        let result = validate_offset_continuity(&batches, 20, false, 20);
        assert!(result.is_ok()); // Should succeed despite gap
    }

    #[test]
    fn test_validate_offset_continuity_multiple_gaps() {
        // Multiple gaps: [0-5), [10-15), [20-25) with HWM=25
        let batches = vec![(0, 5), (10, 5), (20, 5)];
        let result = validate_offset_continuity(&batches, 25, true, 25);
        assert!(result.is_err()); // Confirmed gaps
    }

    #[test]
    fn test_validate_offset_continuity_overlap() {
        // Overlapping batches: [0-10), [5-15) - shouldn't happen but test the handling
        let batches = vec![(0, 10), (5, 10)];
        // Promoted to error: with fail_on_gap=true this now fails.
        let result = validate_offset_continuity(&batches, 15, true, 15);
        assert!(result.is_err());
        // With fail_on_gap=false we tolerate it (logs only).
        let result_loose = validate_offset_continuity(&batches, 15, false, 15);
        assert!(result_loose.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_all_potential_gaps() {
        // All gaps are after HWM (potential gaps only)
        // HWM=0, but we have [10-20), [30-40)
        let batches = vec![(10, 10), (30, 10)];
        // All gaps are after HWM=0, so they're "potential"
        let result = validate_offset_continuity(&batches, 0, true, 40);
        assert!(result.is_ok()); // Should succeed because no confirmed gaps
    }

    #[test]
    fn test_validate_offset_continuity_zero_hwm() {
        // Starting fresh with HWM=0
        let batches = vec![(0, 10), (10, 10)];
        let result = validate_offset_continuity(&batches, 0, true, 20);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_large_batch() {
        // A single large batch
        let batches = vec![(0, 10000)];
        let result = validate_offset_continuity(&batches, 10000, true, 10000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_high_offsets() {
        // Very high offset numbers
        let batches = vec![(1_000_000_000, 100), (1_000_000_100, 100)];
        let result = validate_offset_continuity(&batches, 1_000_000_200, true, 1_000_000_200);
        // Has a gap at the beginning (0 to 1B)
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_offset_continuity_mixed_gaps() {
        // Mix of confirmed and potential gaps
        // Batches: [0-10), [20-30), [50-60)
        // HWM=30, highest_found=60
        // Gap 10-20 is before HWM (confirmed)
        // Gap 30-50 is at/after HWM (potential)
        let batches = vec![(0, 10), (20, 10), (50, 10)];
        let result = validate_offset_continuity(&batches, 30, true, 60);
        assert!(result.is_err()); // Should fail due to confirmed gap 10-20
    }

    #[test]
    fn test_validate_offset_continuity_gap_exactly_at_hwm() {
        // Gap starts exactly at HWM
        // Batches: [0-10), [20-30)
        // HWM=10 (gap 10-20 starts at HWM)
        let batches = vec![(0, 10), (20, 10)];
        let result = validate_offset_continuity(&batches, 10, true, 30);
        // The gap starts at expected_offset=10 which is NOT < HWM=10
        // So this should be classified as a potential gap
        assert!(result.is_ok());
    }

    // ========================================================================
    // Log Gap Summary Tests (via side effects in validate_offset_continuity)
    // ========================================================================

    #[test]
    fn test_log_gap_summary_confirmed_only() {
        // This test exercises the confirmed gap logging path
        // by triggering a confirmed gap
        let batches = vec![(0, 10), (30, 10)];
        let _result = validate_offset_continuity(&batches, 40, false, 40);
        // Just verify it completes without panic
    }

    #[test]
    fn test_log_gap_summary_potential_only() {
        // This test exercises the potential gap logging path
        // All gaps are after HWM
        let batches = vec![(10, 10), (30, 10)];
        let _result = validate_offset_continuity(&batches, 0, false, 40);
        // Just verify it completes without panic
    }

    // ========================================================================
    // Additional Edge Case Tests for Critical Coverage
    // ========================================================================

    #[test]
    fn test_validate_offset_continuity_single_batch_no_gap() {
        // Single batch starting at 0
        let batches = vec![(0, 5)];
        let result = validate_offset_continuity(&batches, 5, true, 5);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_single_batch_with_initial_gap() {
        // Single batch not starting at 0 - creates initial gap
        let batches = vec![(5, 5)];
        let result = validate_offset_continuity(&batches, 10, true, 10);
        // Gap from 0-5 is before HWM=10, so it's a confirmed gap
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_offset_continuity_empty_batches() {
        // No batches - should pass
        let batches: Vec<(i64, i32)> = vec![];
        let result = validate_offset_continuity(&batches, 0, true, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_empty_batches_nonzero_hwm() {
        // No batches but HWM > 0 implies the log was truncated.
        // With fail_on_gap=true (the new default) this is now rejected.
        let batches: Vec<(i64, i32)> = vec![];
        let result = validate_offset_continuity(&batches, 10, true, 10);
        assert!(result.is_err());
        let result_loose = validate_offset_continuity(&batches, 10, false, 10);
        assert!(result_loose.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_zero_record_count() {
        // Batches with 0 record count should be skipped
        let batches = vec![(0, 10), (10, 0), (10, 10)];
        let result = validate_offset_continuity(&batches, 20, true, 20);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_negative_hwm() {
        // Negative HWM (shouldn't happen, but test robustness)
        let batches = vec![(0, 10)];
        let result = validate_offset_continuity(&batches, -5, true, 10);
        // HWM=-5 means all gaps are after HWM (potential gaps)
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_hwm_equals_highest() {
        // HWM equals the highest found offset (common case)
        let batches = vec![(0, 10), (10, 10), (20, 10)];
        let result = validate_offset_continuity(&batches, 30, true, 30);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_hwm_greater_than_highest() {
        // HWM greater than highest found (could happen during recovery)
        let batches = vec![(0, 10)];
        let result = validate_offset_continuity(&batches, 100, true, 10);
        // Gap from 10-100 starts at 10 which is < HWM=100, so it's confirmed
        // But since batches only contain up to 10 and we stop iterating,
        // the expected_offset at end is 10 which is < HWM=100
        // This depends on exact implementation - test the behavior
        assert!(result.is_ok()); // No gap detected because we don't iterate past batches
    }

    #[test]
    fn test_validate_offset_continuity_unsorted_input() {
        // Batches provided in unsorted order
        let batches = vec![(20, 10), (0, 10), (10, 10)];
        let result = validate_offset_continuity(&batches, 30, true, 30);
        // Function should sort internally
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_duplicate_offsets() {
        // Duplicate base offsets are now treated as overlaps and rejected when
        // fail_on_gap=true.
        let batches = vec![(0, 10), (0, 10), (10, 10)];
        let result = validate_offset_continuity(&batches, 20, true, 20);
        assert!(result.is_err());
        let result_loose = validate_offset_continuity(&batches, 20, false, 20);
        assert!(result_loose.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_gap_spans_hwm() {
        // Gap that spans across the HWM boundary
        // Batches: [0-10), [30-40)
        // HWM=20
        // Gap 10-30: 10-20 is confirmed, 20-30 is potential
        let batches = vec![(0, 10), (30, 10)];
        let result = validate_offset_continuity(&batches, 20, true, 40);
        // Should fail because part of the gap (10-20) is confirmed
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_offset_continuity_large_record_count() {
        // Large record count in a single batch
        let batches = vec![(0, i32::MAX)];
        let result = validate_offset_continuity(&batches, i32::MAX as i64, true, i32::MAX as i64);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_many_small_batches() {
        // Many small consecutive batches
        let batches: Vec<(i64, i32)> = (0..1000).map(|i| (i, 1)).collect();
        let result = validate_offset_continuity(&batches, 1000, true, 1000);
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_many_gaps() {
        // Many gaps in the sequence
        // Every other batch is missing: [0-10), [20-30), [40-50)...
        let batches: Vec<(i64, i32)> = (0..10).map(|i| (i * 20, 10)).collect();
        let result = validate_offset_continuity(&batches, 200, true, 200);
        // Should fail due to many confirmed gaps
        assert!(result.is_err());
    }

    #[test]
    fn test_validate_offset_continuity_fail_on_gap_false_allows_gaps() {
        // With fail_on_gap=false, confirmed gaps should not cause error
        let batches = vec![(0, 10), (30, 10)];
        let result = validate_offset_continuity(&batches, 40, false, 40);
        // Should succeed even with confirmed gap 10-30
        assert!(result.is_ok());
    }

    #[test]
    fn test_validate_offset_continuity_boundary_at_i64_max() {
        // Test near i64::MAX boundary
        let base = i64::MAX - 100;
        let batches = vec![(base, 50), (base + 50, 50)];
        let result = validate_offset_continuity(&batches, base + 100, false, base + 100);
        // Gap from 0 to base, but fail_on_gap=false
        assert!(result.is_ok());
    }
}
