//! Append path for [`PartitionStore`].

use super::super::error::{SlateDBError, SlateDBResult};
use super::super::keys::{
    HIGH_WATERMARK_KEY, encode_producer_state_key, encode_producer_state_value, encode_record_key,
    parse_record_count_checked, patch_base_offset,
};
use super::PartitionStore;
use super::offset_reservation::OffsetReservation;
use super::producer_state::ProducerState;
use super::{DURABLE_WRITE_OPTIONS, FAST_WRITE_OPTIONS, HWM_CHECKPOINT_INTERVAL_BATCHES};
use crate::protocol::parse_producer_info;
use bytes::Bytes;
use slatedb::WriteBatch;
use std::sync::atomic::Ordering;
use tracing::{debug, error, warn};

impl PartitionStore {
    /// Append a record batch to this partition.
    ///
    /// Returns the base offset of the appended batch.
    /// Uses a mutex to ensure atomic offset allocation.
    ///
    /// HWM is now embedded in the batch value to ensure
    /// atomicity. If the batch write succeeds but we crash before updating HWM,
    /// recovery will find the HWM from the batch itself.
    ///
    /// # Errors
    ///
    /// Returns `SlateDBError::Fenced` if another writer has taken over this partition.
    /// The caller should release ownership and stop writing to this partition.
    ///
    /// Returns `SlateDBError::NotOwned` if the broker is in zombie mode (lost cluster
    /// coordination). This is a safety check to prevent writes during split-brain.
    ///
    /// Returns `SlateDBError::EpochMismatch` if the stored epoch in SlateDB doesn't
    /// match our expected epoch, indicating another broker has acquired this partition.
    ///
    /// # Throughput ceiling
    ///
    /// Per-partition produce throughput is serialized on [`write_lock`] and,
    /// for [`append_batch_durable`] / `acks>=1`, bounded by the SlateDB
    /// durable round-trip latency. Offset allocation, epoch verification, and
    /// the storage write all happen under the same lock — durability is not
    /// pipelined because the fencing model requires the epoch check to
    /// precede every append atomically.
    pub async fn append_batch(&self, records: &Bytes) -> SlateDBResult<i64> {
        self.append_batch_inner(records, false).await
    }

    /// Append a record batch and wait for SlateDB to confirm durability before
    /// returning. Required for `acks>=1` to honor Kafka's ack contract — the
    /// fast `append_batch` path can lose acknowledged data in the ~100ms WAL
    /// flush window if the broker dies between ack and flush.
    pub async fn append_batch_durable(&self, records: &Bytes) -> SlateDBResult<i64> {
        self.append_batch_inner(records, true).await
    }

    async fn append_batch_inner(&self, records: &Bytes, durable: bool) -> SlateDBResult<i64> {
        use super::super::keys::{LEADER_EPOCH_KEY, decode_leader_epoch};

        // Reject immediately if a previous append left a permanent offset
        // gap (see `append_failed` field doc). Continuing would write the
        // next batch *past* the gap, producing the records→gap→records
        // pattern that bricks recovery under `fail_on_recovery_gap=true`.
        // The lease holder is expected to release this partition on this
        // signal so the next acquirer reopens cleanly from durable state.
        if self.append_failed.load(Ordering::Acquire) {
            return Err(SlateDBError::NotOwned {
                topic: self.topic.clone(),
                partition: self.partition,
            });
        }

        // Wall-clock timestamp captured once at the top of the append. Used
        // to stamp the producer-state value with `last_used_at_ms` so the
        // periodic retention sweep can age out keys for producers that have
        // gone quiet.
        let now_ms = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_millis() as i64)
            .unwrap_or(0);

        // Check zombie mode BEFORE acquiring write lock
        // This prevents writes when the broker has lost cluster coordination
        if let Some(ref zombie_state) = self.zombie_mode
            && zombie_state.is_active()
        {
            error!(
                topic = %self.topic,
                partition = self.partition,
                "Rejecting write: broker is in zombie mode (lost cluster coordination)"
            );
            return Err(SlateDBError::NotOwned {
                topic: self.topic.clone(),
                partition: self.partition,
            });
        }

        // Lease TOCTOU re-check at write time. `get_for_write` admitted this
        // request based on the cached lease expiry; while the request was
        // queued behind the write_lock the lease may have decayed below the
        // safe-write floor. Reject early with `LeaseTooShort` instead of
        // letting SlateDB's epoch fence catch the eventually-stolen
        // partition with the much less actionable `NotOwned`.
        self.revalidate_lease_at_write()?;

        // ==================================================================
        // EPOCH-BASED FENCING: read epoch BEFORE acquiring the write lock.
        // ==================================================================
        // The previous shape held the per-partition write_lock across this
        // SlateDB get plus the actual durable write — every concurrent
        // producer to the partition serialized end-to-end on object-store
        // latency. Reading the epoch outside the lock lets concurrent
        // writers parallelize the epoch fetch; the actual write below
        // remains atomic via SlateDB's own single-writer fencing.
        //
        // SAFETY: An adversarial broker bumping the epoch between this read
        // and our write is detected by SlateDB at `write_with_options` time,
        // which fails with `is_fenced()`. The check below provides an
        // earlier reject for the common case (cleaner error code, avoids
        // building the WriteBatch) without being load-bearing for safety.
        //
        // Gate is `!= 0` (not `> 0`): a negative on-disk epoch — produced
        // by an i32-wrapping bump in any past version, or a corrupted
        // stored value — must still be fenced. Epoch=0 retains its legacy
        // meaning ("validation disabled / never set").
        let prefetched_epoch: Option<i32> = if self.leader_epoch != 0 {
            match self.db.get(LEADER_EPOCH_KEY).await {
                Ok(Some(bytes)) => Some(decode_leader_epoch(&bytes).unwrap_or(0)),
                Ok(None) => Some(0),
                Err(e) => {
                    let err = SlateDBError::from(e);
                    if err.is_fenced() {
                        error!(
                            topic = %self.topic,
                            partition = self.partition,
                            "FENCED during epoch check"
                        );
                        return Err(err);
                    }
                    // SAFETY: Do NOT fall back to cached epoch on storage errors.
                    // If we cannot verify the epoch, we must fail the write to prevent
                    // split-brain scenarios where:
                    // 1. Storage is temporarily unavailable (e.g., network partition)
                    // 2. Another broker acquires partition and writes new epoch
                    // 3. We fall back to cached (stale) epoch and write anyway
                    // 4. Both brokers write to same partition = data corruption
                    error!(
                        topic = %self.topic,
                        partition = self.partition,
                        error = %err,
                        "Failed to read epoch from storage - rejecting write for safety"
                    );
                    return Err(SlateDBError::Storage(format!(
                        "Cannot verify epoch for {}/{}: {}",
                        self.topic, self.partition, err
                    )));
                }
            }
        } else {
            None
        };

        // Acquire write lock to ensure atomic offset allocation
        let _guard = self.write_lock.lock().await;

        // Re-check zombie mode AFTER acquiring lock (double-check pattern)
        // A broker could enter zombie mode while we were waiting for the lock
        if let Some(ref zombie_state) = self.zombie_mode
            && zombie_state.is_active()
        {
            error!(
                topic = %self.topic,
                partition = self.partition,
                "Rejecting write: broker entered zombie mode while waiting for lock"
            );
            return Err(SlateDBError::NotOwned {
                topic: self.topic.clone(),
                partition: self.partition,
            });
        }

        // Pure in-memory compare against the prefetched epoch — no I/O held
        // under the lock. SlateDB's own fence enforcement catches any race
        // between this check and the actual write below.
        if let Some(stored_epoch) = prefetched_epoch
            && stored_epoch != self.leader_epoch
        {
            error!(
                topic = %self.topic,
                partition = self.partition,
                expected_epoch = self.leader_epoch,
                stored_epoch,
                "EPOCH MISMATCH: Another broker has acquired this partition"
            );
            super::super::metrics::record_epoch_mismatch(&self.topic, self.partition);
            return Err(SlateDBError::EpochMismatch {
                topic: self.topic.clone(),
                partition: self.partition,
                expected_epoch: self.leader_epoch,
                stored_epoch,
            });
        }

        // Reject invalid record counts BEFORE we reserve any offsets — a
        // bumped `next_offset` followed by an early-return would create a
        // permanent offset gap for a request that was always going to fail.
        let record_count = match parse_record_count_checked(records) {
            Ok(n) => n,
            Err(e) => {
                error!(
                    topic = %self.topic,
                    partition = self.partition,
                    error = %e,
                    "Rejecting batch with invalid record count"
                );
                return Err(SlateDBError::CorruptBatch {
                    topic: self.topic.to_string(),
                    partition: self.partition,
                    reason: format!("Invalid record count: {}", e),
                });
            }
        };

        // RESERVE the offset range for this batch BEFORE the SlateDB await.
        //
        // Cancellation safety: if we read the offset from
        // `high_watermark` and *then* awaited the SlateDB write, a cancelled
        // future would leave `high_watermark` unchanged and the next caller
        // would reuse the same `base_offset`. SlateDB's queued cancelled
        // write may still eventually persist at that offset — clobbered by
        // the second caller's batch at the same key.
        //
        // Bumping `next_offset` here makes that impossible: every caller
        // gets a unique offset range, even across cancellation. The reader-
        // visible `high_watermark` only advances on a successful durable
        // write below, so consumers never see uncommitted offsets — the
        // failure mode degrades to a gap in the offset range, which the
        // producer recovers from by retrying (idempotent producers de-dup
        // by sequence number; non-idempotent producers accept Kafka's
        // standard "may duplicate on error" contract).
        let base_offset = self
            .next_offset
            .fetch_add(record_count as i64, Ordering::SeqCst);

        // RAII rollback guard. Any reject path between here and `dismiss()`
        // below restores `next_offset` to `base_offset` on Drop, preventing
        // permanent offset gaps. We hold `write_lock` for the entire lifetime
        // of this guard, so no concurrent appender can advance past us — the
        // guard's invariants live on the lock.
        let reservation = OffsetReservation::new(&self.next_offset, base_offset);

        // Parse producer info once; reused by both the idempotency check here
        // and the producer-state persistence below (previously parsed twice
        // per append).
        let idempotent_producer_info = parse_producer_info(records).filter(|i| i.is_idempotent());

        // Idempotency check: detects and rejects duplicate or out-of-order messages.
        // For *exact-replay* duplicates we return the original base_offset (success)
        // rather than DuplicateSequence so retries that the network ate
        // don't break the producer.
        //
        // On an in-memory cache miss we consult the persisted `p<producer_id>`
        // key before treating the producer as new. Without this, idle-producer
        // cache eviction (time_to_idle TTL) silently accepted duplicate
        // batches as new — an idempotence violation.
        let mut idempotent_dup_offset: Option<i64> = None;
        let cached_producer_state = match &idempotent_producer_info {
            Some(info) => match self.producer_states.get(&info.producer_id) {
                Some(state) => Some(state),
                None => match self.load_persisted_producer_state(info.producer_id).await {
                    Ok(state) => state,
                    Err(err) => {
                        // The offset was reserved before the producer-state
                        // lookup; a transient storage error here would otherwise
                        // leave a permanent gap in the log. The `reservation`
                        // guard's Drop releases the offset on the way out.
                        return Err(err);
                    }
                },
            },
            None => None,
        };
        if let Some(producer_info) = idempotent_producer_info
            && let Some(state) = cached_producer_state
        {
            // Check for epoch fencing (zombie producer detection)
            if producer_info.producer_epoch < state.producer_epoch {
                warn!(
                    topic = %self.topic,
                    partition = self.partition,
                    producer_id = producer_info.producer_id,
                    batch_epoch = producer_info.producer_epoch,
                    current_epoch = state.producer_epoch,
                    "Rejecting batch from fenced producer (stale epoch)"
                );
                super::super::metrics::record_idempotency_rejection("fenced_epoch");
                // Reservation is released by the `reservation` guard on return.
                return Err(SlateDBError::FencedProducer {
                    producer_id: producer_info.producer_id,
                    expected_epoch: state.producer_epoch,
                    actual_epoch: producer_info.producer_epoch,
                });
            }

            // Higher epoch indicates new producer incarnation. Kafka's contract
            // is that the producer resets its sequence to 0 on epoch bump, so
            // we MUST require first_sequence == 0 here. Without
            // this gate, a higher-epoch batch with an arbitrary replayed
            // sequence would be accepted as fresh.
            if producer_info.producer_epoch > state.producer_epoch {
                if producer_info.first_sequence != 0 {
                    warn!(
                        topic = %self.topic,
                        partition = self.partition,
                        producer_id = producer_info.producer_id,
                        new_epoch = producer_info.producer_epoch,
                        first_sequence = producer_info.first_sequence,
                        "Rejecting batch on epoch bump: first_sequence must be 0"
                    );
                    super::super::metrics::record_idempotency_rejection("out_of_order");
                    return Err(SlateDBError::OutOfOrderSequence {
                        producer_id: producer_info.producer_id,
                        expected_sequence: 0,
                        received_sequence: producer_info.first_sequence,
                    });
                }
                debug!(
                    topic = %self.topic,
                    partition = self.partition,
                    producer_id = producer_info.producer_id,
                    old_epoch = state.producer_epoch,
                    new_epoch = producer_info.producer_epoch,
                    "Producer epoch increased, resetting sequence tracking"
                );
            } else {
                // Use checked_add to detect overflow
                let expected_seq = match state.last_sequence.checked_add(1) {
                    Some(seq) => seq,
                    None => {
                        // Sequence numbers exhausted - producer should get a new producer_id
                        warn!(
                            topic = %self.topic,
                            partition = self.partition,
                            producer_id = producer_info.producer_id,
                            last_sequence = state.last_sequence,
                            "Sequence number overflow detected"
                        );
                        return Err(SlateDBError::SequenceOverflow {
                            producer_id: producer_info.producer_id,
                            topic: self.topic.clone(),
                            partition: self.partition,
                        });
                    }
                };

                if producer_info.first_sequence <= state.last_sequence {
                    // Exact-replay of the most recent batch: return the cached
                    // base_offset as success rather than DuplicateSequence —
                    // matches Kafka's idempotent producer contract. Older or
                    // partial replays still fall through to the error below.
                    if state.last_first_sequence == producer_info.first_sequence
                        && state.last_base_offset >= 0
                    {
                        debug!(
                            topic = %self.topic,
                            partition = self.partition,
                            producer_id = producer_info.producer_id,
                            first_sequence = producer_info.first_sequence,
                            cached_offset = state.last_base_offset,
                            "Returning cached base_offset for duplicate retry"
                        );
                        super::super::metrics::record_idempotency_rejection(
                            "duplicate_idempotent_ok",
                        );
                        idempotent_dup_offset = Some(state.last_base_offset);
                    } else {
                        warn!(
                            topic = %self.topic,
                            partition = self.partition,
                            producer_id = producer_info.producer_id,
                            first_sequence = producer_info.first_sequence,
                            last_seen = state.last_sequence,
                            "Rejecting duplicate batch (no cached offset for retry)"
                        );
                        super::super::metrics::record_idempotency_rejection("duplicate");
                        return Err(SlateDBError::DuplicateSequence {
                            producer_id: producer_info.producer_id,
                            expected_sequence: expected_seq,
                            received_sequence: producer_info.first_sequence,
                        });
                    }
                }

                if idempotent_dup_offset.is_none() && producer_info.first_sequence != expected_seq {
                    warn!(
                        topic = %self.topic,
                        partition = self.partition,
                        producer_id = producer_info.producer_id,
                        first_sequence = producer_info.first_sequence,
                        expected = expected_seq,
                        "Rejecting out-of-order batch"
                    );
                    super::super::metrics::record_idempotency_rejection("out_of_order");
                    return Err(SlateDBError::OutOfOrderSequence {
                        producer_id: producer_info.producer_id,
                        expected_sequence: expected_seq,
                        received_sequence: producer_info.first_sequence,
                    });
                }
            }
        }

        // If this turned out to be a duplicate retry of the most recent batch,
        // return the cached base_offset without writing again. The records are
        // already durable from the first append. The `reservation` guard
        // releases the offsets we reserved when this function returns, since
        // no SlateDB write happened.
        if let Some(cached_offset) = idempotent_dup_offset {
            return Ok(cached_offset);
        }

        let new_hwm = (record_count as i64)
            .checked_add(base_offset)
            .ok_or_else(|| {
                SlateDBError::Config(format!(
                    "HWM overflow: base_offset={} + record_count={} would overflow i64",
                    base_offset, record_count
                ))
            })?;

        // Build value with metadata using pooled buffer to reduce allocations.
        // Format: [new_hwm: i64][record_batch: bytes]
        // This ensures HWM is stored atomically with the batch.
        //
        // PERFORMANCE: Using get_buffer/return_buffer pattern instead of with_batch_buffer
        // to avoid clone() across async boundary. The buffer is borrowed for the SlateDB
        // write and returned to the pool afterward, eliminating allocation overhead.
        // RAII: the buffer is returned to the pool on every exit from this
        // function, including a cancellation at the `write_with_options`
        // await below. The previous explicit `return_buffer` sat *after* that
        // await, so a cancelled produce dropped its buffer on the floor.
        let mut buffer = super::super::buffer_pool::PooledBuffer::new(8 + records.len());
        buffer.extend_from_slice(&new_hwm.to_be_bytes());
        buffer.extend_from_slice(records);

        // Patch base_offset in record batch (at offset 8, where the batch starts).
        // A short batch here is a defense-in-depth case (the produce path
        // already validates the record batch upstream), but if it slipped
        // through, silently storing it would persist a batch with
        // base_offset=0 under a real key — confusing the fetch path.
        // Returning the buffer to the pool keeps us honest about the
        // failure path.
        if let Err(e) = patch_base_offset(&mut buffer[8..], base_offset) {
            return Err(SlateDBError::CorruptBatch {
                topic: self.topic.to_string(),
                partition: self.partition,
                reason: format!("patch_base_offset rejected: {e}"),
            });
        }

        // For acks>=1 we must wait for SlateDB durability before acking the
        // producer; otherwise an OOM-kill or rolling restart inside the WAL
        // flush window silently loses already-acknowledged data.
        //
        // Idempotent producers always force durability regardless of the
        // caller's `durable` flag: their guarantee depends on producer-state
        // (last_sequence, last_base_offset) surviving any crash, not just a
        // graceful shutdown. With `await_durable: false` + producer-state
        // cache TTL, an idle producer whose cache entry expired would
        // re-issue sequence 0 after a broker crash that lost the unflushed
        // producer-state write — and the broker would accept the duplicate
        // as a fresh batch.
        let write_options = if durable || idempotent_producer_info.is_some() {
            DURABLE_WRITE_OPTIONS
        } else {
            FAST_WRITE_OPTIONS
        };
        let key = encode_record_key(base_offset);

        // Pull producer-state metadata up-front so we can atomically commit it
        // alongside the record batch. Persisting after the batch
        // write let the two diverge: a persist failure either rejected an
        // already-committed append or silently succeeded non-durably, both of
        // which break idempotency across restart.
        //
        // The persisted value includes the retry-dedup pair
        // (last_first_sequence / last_base_offset) so an exact retry of this
        // batch is re-acked with its original offset even across a restart.
        let pending_producer_state = idempotent_producer_info.map(|info| {
            let last_sequence = info.last_sequence().unwrap_or(info.first_sequence);
            let key = encode_producer_state_key(info.producer_id);
            let value = encode_producer_state_value(&super::super::keys::PersistedProducerState {
                last_sequence,
                producer_epoch: info.producer_epoch,
                last_first_sequence: info.first_sequence,
                last_base_offset: base_offset,
                last_used_at_ms: now_ms,
            });
            (info, last_sequence, key, value)
        });

        // Periodically checkpoint `_hwm` inside the same atomic WriteBatch.
        // The checkpoint bounds the recovery scan on the next open to batches
        // appended after it (SlateDB's WAL is ordered, so a persisted
        // checkpoint implies every earlier batch is persisted too).
        let checkpoint_hwm = self
            .appends_since_checkpoint
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(HWM_CHECKPOINT_INTERVAL_BATCHES);

        let mut batch = WriteBatch::new();
        batch.put(key.as_slice(), buffer.as_slice());
        if let Some((_, _, ps_key, ps_value)) = &pending_producer_state {
            batch.put(ps_key.as_slice(), ps_value.as_slice());
        }
        if checkpoint_hwm {
            batch.put(HIGH_WATERMARK_KEY, new_hwm.to_be_bytes());
        }
        let durable_write = write_options.await_durable;
        let write_started_at = std::time::Instant::now();
        // Disarm the offset rollback BEFORE handing the batch to SlateDB.
        // Once `write_with_options` is called, SlateDB's writer thread may
        // complete the queued batch even if our future is cancelled. If
        // OffsetReservation::Drop ran on cancellation it would roll
        // `next_offset` back to `base_offset`, the next appender would re-take
        // `base_offset`, and SlateDB would silently clobber one of the two
        // batches at the same key. We accept the alternative trade-off — a
        // permanent offset gap on transient write errors — so the clobber can
        // never happen. See `OffsetReservation`'s `cancellation hand-off`
        // safety note.
        let mut reservation = reservation;
        reservation.disarm();
        let write_result = self.db.write_with_options(batch, &write_options).await;
        if durable_write {
            super::super::metrics::record_slatedb_flush(
                &self.topic,
                write_started_at.elapsed().as_secs_f64(),
            );
        }

        if let Err(e) = write_result {
            let err = SlateDBError::from(e);

            // The reservation was disarmed before `write_with_options`, so
            // its Drop is a no-op and `next_offset` stays at
            // `base_offset + record_count`. This is by design: SlateDB's
            // writer queue may have processed the batch by the time we
            // observe the error, and a rollback that races a queued write
            // would let the next appender clobber the same key. We accept
            // a permanent offset gap on this transient error path —
            // `fail_on_recovery_gap=true` decides whether such gaps abort
            // recovery; with the flag off, consumers see HWM jumps over an
            // empty range. Track the gap as a metric so operators can
            // alarm on persistent gap rates.
            super::super::metrics::record_partition_offset_gap(
                &self.topic,
                self.partition,
                record_count,
            );

            // Trip the sticky `append_failed` flag so subsequent appends
            // on this instance are rejected. Without this guard, the next
            // successful append would write at the post-gap offset and
            // produce the "records → gap → records" pattern that DOAs the
            // partition under `fail_on_recovery_gap=true`. Skip the trip
            // for fenced errors: a fenced writer didn't create a gap (the
            // in-memory next_offset advance is irrelevant — this instance
            // is being torn down anyway) and `EpochMismatch`/`Fenced`
            // already drives the lease release path.
            if !err.is_fenced() {
                self.append_failed.store(true, Ordering::Release);
                error!(
                    topic = %self.topic,
                    partition = self.partition,
                    base_offset,
                    record_count,
                    "Marking partition append-failed after durable write error: \
                     next_offset advanced past high_watermark with no durable \
                     record. Releasing this lease so the next acquirer can \
                     re-derive next_offset from durable state."
                );
            }

            // Track object store health on failure
            // This helps detect partial network partitions where broker can reach
            // Raft (for coordination) but not the object store (for data I/O)
            let still_healthy = super::super::metrics::track_object_store_health(false);
            if !still_healthy {
                error!(
                    topic = %self.topic,
                    partition = self.partition,
                    consecutive_failures = super::super::metrics::object_store_consecutive_failures(),
                    "PARTIAL NETWORK PARTITION DETECTED: Object store unreachable. \
                     Broker may need to release partitions."
                );
            }

            if err.is_fenced() {
                error!(
                    topic = %self.topic,
                    partition = self.partition,
                    "FENCED: Another writer has taken over this partition. Releasing ownership."
                );
            } else {
                error!(
                    topic = %self.topic,
                    partition = self.partition,
                    error = %err,
                    "Failed to append batch"
                );
            }
            return Err(err);
        }

        // Track object store health on success
        super::super::metrics::track_object_store_health(true);

        // The SlateDB write committed; the offset reservation is now durable.
        // Dismissing the guard prevents Drop from rolling it back.
        reservation.dismiss();

        // Update high watermark in memory.
        // The HWM is also embedded in the record-batch value (first 8 bytes)
        // and recovery scans for the highest record key on open, so a crash
        // here cannot regress the watermark below durably-persisted records.
        self.high_watermark.store(new_hwm, Ordering::SeqCst);

        // Durability summary for this path:
        //  - durable=true (acks>=1) or idempotent producer → SlateDB has
        //    already fsynced the WAL before we got here.
        //  - acks=0 fast path → caller opted out of durability; SlateDB's
        //    periodic flush (~100 ms) closes the window. Documented in
        //    README "Durability contract".
        //  - On graceful shutdown, PartitionManager::shutdown flushes every
        //    owned store before releasing its lease (see partition_manager.rs).

        // Add to batch index for efficient lookup during fetch
        self.add_to_batch_index(base_offset, record_count);

        // Update producer state cache after successful atomic write. The
        // producer-state key was already written in the same WriteBatch
        // above, so we don't need a separate persist call.
        if let Some((producer_info, last_sequence, _, _)) = pending_producer_state {
            // Update in-memory cache
            self.producer_states.insert(
                producer_info.producer_id,
                ProducerState {
                    last_sequence,
                    producer_epoch: producer_info.producer_epoch,
                    last_first_sequence: producer_info.first_sequence,
                    last_base_offset: base_offset,
                },
            );

            // Proactive sequence number monitoring
            // Track sequence numbers and alert when approaching exhaustion
            super::super::metrics::record_sequence_number(
                &self.topic,
                self.partition,
                producer_info.producer_id,
                last_sequence,
            );

            debug!(
                topic = %self.topic,
                partition = self.partition,
                producer_id = producer_info.producer_id,
                producer_epoch = producer_info.producer_epoch,
                last_sequence,
                "Updated and persisted producer state"
            );
        }

        debug!(
            topic = %self.topic,
            partition = self.partition,
            base_offset,
            record_count,
            new_hwm,
            "Appended batch"
        );

        // Record load metrics for auto-balancing
        self.record_produce_metrics(records.len() as u64, record_count as u64);

        Ok(base_offset)
    }

    /// Load a producer's persisted idempotency state on an in-memory cache
    /// miss, repopulating the cache.
    ///
    /// Returns `Ok(None)` for a genuinely unknown producer. Storage errors
    /// propagate — if we cannot verify whether a producer was seen before,
    /// accepting the batch could admit a duplicate, so the write must fail
    /// (same fail-closed posture as the epoch check).
    async fn load_persisted_producer_state(
        &self,
        producer_id: i64,
    ) -> SlateDBResult<Option<ProducerState>> {
        let key = encode_producer_state_key(producer_id);
        let bytes = match self.db.get(key.as_slice()).await {
            Ok(maybe) => maybe,
            Err(e) => {
                let err = SlateDBError::from(e);
                error!(
                    topic = %self.topic,
                    partition = self.partition,
                    producer_id,
                    error = %err,
                    "Failed to read persisted producer state - rejecting write for safety"
                );
                return Err(err);
            }
        };
        let Some(bytes) = bytes else {
            return Ok(None);
        };
        let Some(persisted) = super::super::keys::decode_producer_state_value(&bytes) else {
            warn!(
                topic = %self.topic,
                partition = self.partition,
                producer_id,
                "Undecodable persisted producer state - treating producer as new"
            );
            return Ok(None);
        };
        let state = ProducerState {
            last_sequence: persisted.last_sequence,
            producer_epoch: persisted.producer_epoch,
            last_first_sequence: persisted.last_first_sequence,
            last_base_offset: persisted.last_base_offset,
        };
        debug!(
            topic = %self.topic,
            partition = self.partition,
            producer_id,
            last_sequence = state.last_sequence,
            "Restored producer state from storage after cache miss"
        );
        self.producer_states.insert(producer_id, state);
        Ok(Some(state))
    }
}
