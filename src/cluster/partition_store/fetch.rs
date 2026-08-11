//! Fetch and batch-index lookup path for [`PartitionStore`].

use super::super::error::{SlateDBError, SlateDBResult};
use super::super::keys::{encode_record_key, parse_record_count_checked};
use super::INITIAL_BATCH_BACKSCAN_WINDOW;
use super::PartitionStore;
use bytes::{Bytes, BytesMut};
use std::sync::atomic::Ordering;
use tracing::{debug, error};

impl PartitionStore {
    /// default byte budget. See [`Self::fetch_from_with_budget`].
    pub async fn fetch_from(&self, fetch_offset: i64) -> SlateDBResult<(i64, Option<Bytes>)> {
        let max_size = self.max_fetch_response_size.load(Ordering::Relaxed);
        self.fetch_from_with_budget(fetch_offset, max_size).await
    }

    /// Fetch records starting from the given offset.
    ///
    /// Returns (high_watermark, records).
    ///
    /// Now strips the HWM metadata (first 8 bytes) from each batch.
    ///
    /// This uses SlateDB range scan for efficient sequential access:
    /// 1. Find the batch containing or following the fetch offset
    /// 2. Use range scan to iterate through consecutive batches
    /// 3. Collect batches until the byte budget is reached
    ///
    /// `max_bytes` is the per-call byte budget — the smaller of the client's
    /// `partition_max_bytes`, the remaining request-level `max_bytes`, and
    /// the broker's `max_fetch_response_size`. Kafka's contract that the
    /// first batch is always returned whole (even if oversized) is preserved.
    pub async fn fetch_from_with_budget(
        &self,
        fetch_offset: i64,
        max_bytes: usize,
    ) -> SlateDBResult<(i64, Option<Bytes>)> {
        use super::super::keys::decode_record_offset;

        let high_watermark = self.high_watermark.load(Ordering::SeqCst);

        if fetch_offset >= high_watermark {
            return Ok((high_watermark, None)); // No new data
        }

        if fetch_offset < 0 {
            return Ok((high_watermark, None)); // Invalid offset
        }

        // Find the batch that contains or follows fetch_offset
        let start_offset = match self.find_batch_start(fetch_offset, high_watermark).await? {
            Some(offset) => offset,
            None => {
                // No batch found containing or after fetch_offset
                return Ok((high_watermark, None));
            }
        };

        // Never exceed the broker-wide cap regardless of the client's ask.
        let max_size = max_bytes.min(self.max_fetch_response_size.load(Ordering::Relaxed));

        // Collect refcounted batch slices straight from SlateDB. Each
        // `item.value` is already a `Bytes`; `slice(8..)` returns a
        // refcounted view, no copy. We only flatten into a single buffer
        // at the end if the fetch returned more than one batch — for the
        // common low-throughput case (one batch per fetch) the SlateDB
        // value is handed straight to the caller.
        let mut chunks: Vec<Bytes> = Vec::new();
        let mut total_len: usize = 0;
        let mut batch_count = 0u32;

        // Use range scan from start_offset to high_watermark for efficient sequential access
        let start_key = encode_record_key(start_offset);
        let end_key = encode_record_key(high_watermark);

        let mut iter = match self.db.scan(start_key.as_slice()..end_key.as_slice()).await {
            Ok(iter) => {
                // Track object store health on successful scan
                super::super::metrics::track_object_store_health(true);
                iter
            }
            Err(e) => {
                // Track object store health on scan failure
                let still_healthy = super::super::metrics::track_object_store_health(false);
                if !still_healthy {
                    error!(
                        topic = %self.topic,
                        partition = self.partition,
                        consecutive_failures = super::super::metrics::object_store_consecutive_failures(),
                        "PARTIAL NETWORK PARTITION DETECTED: Object store unreachable during fetch"
                    );
                }
                return Err(e.into());
            }
        };

        // Propagate scan errors instead of treating them as end-of-data: a
        // transient storage error mid-scan must not silently truncate the
        // response (consumers would interpret it as "caught up").
        while let Some(item) = iter.next().await.map_err(SlateDBError::from)? {
            // Verify this is a record key and decode offset
            let current_offset = match decode_record_offset(&item.key) {
                Some(offset) => offset,
                None => continue, // Skip non-record keys
            };

            // Strip HWM metadata (first 8 bytes) via a refcounted slice on
            // the SlateDB value. Format: [new_hwm: i64][record_batch: bytes]
            let batch_bytes: Bytes = if item.value.len() >= 8 {
                item.value.slice(8..)
            } else {
                // Old format or corrupted - use as-is
                item.value.clone()
            };

            let record_count = match parse_record_count_checked(&batch_bytes) {
                Ok(n) => n,
                Err(e) => {
                    error!(
                        topic = %self.topic,
                        partition = self.partition,
                        offset = current_offset,
                        error = %e,
                        "Batch with invalid record count during fetch"
                    );
                    return Err(SlateDBError::Storage(format!(
                        "Corrupt record batch at offset {} in {}/{}: {}",
                        current_offset, self.topic, self.partition, e
                    )));
                }
            };

            // Kafka's fetch contract: always return at least one complete
            // record batch even if it exceeds `max_bytes`, otherwise the
            // consumer will be stuck (it can't parse a torn batch). Only
            // batches *after* the first are gated by the size budget.
            if !chunks.is_empty() && total_len + batch_bytes.len() > max_size {
                break;
            }

            // Add to batch index for future lookups
            self.add_to_batch_index(current_offset, record_count);

            total_len += batch_bytes.len();
            chunks.push(batch_bytes);
            batch_count += 1;

            // After we've included the first (possibly oversized) batch,
            // stop if we've already met or exceeded the byte budget.
            if total_len >= max_size {
                break;
            }
        }

        let records = if chunks.is_empty() {
            None
        } else {
            debug!(
                topic = %self.topic,
                partition = self.partition,
                fetch_offset,
                start_offset,
                bytes = total_len,
                batch_count,
                "Fetched records"
            );
            // Single-batch fetch: hand the SlateDB-owned `Bytes` straight
            // back without reallocating or memcpying. Multi-batch fetches
            // still need a contiguous buffer for the wire encoding's
            // length-prefixed-bytes shape; allocate it exactly once at the
            // measured size instead of growing a `BytesMut`.
            if chunks.len() == 1 {
                Some(chunks.into_iter().next().unwrap())
            } else {
                let mut combined = BytesMut::with_capacity(total_len);
                for c in chunks {
                    combined.extend_from_slice(&c);
                }
                Some(combined.freeze())
            }
        };

        // Record load metrics for auto-balancing
        if let Some(ref r) = records {
            self.record_fetch_metrics(r.len() as u64, batch_count as u64);
        }

        Ok((high_watermark, records))
    }

    /// Find the batch that contains or follows the given offset.
    ///
    /// Strategy:
    /// 1. Check the in-memory batch index for the largest base_offset at or
    ///    before `fetch_offset`. If that batch covers `fetch_offset` we're
    ///    done — no SlateDB scan at all.
    /// 2. Otherwise fall back to a bounded back-scan that widens until a
    ///    covering batch is found or the log start offset is reached.
    ///
    /// The bounded back-scan replaces the old fallback that scanned from
    /// offset 0 — O(partition size) object-store reads per fetch for any
    /// lagging consumer with a cold index. A batch containing `fetch_offset`
    /// must start within one batch-length of it, so the first (small) window
    /// almost always suffices; the widening loop is only taken on gappy logs.
    ///
    /// Returns None if no batch exists at or after fetch_offset.
    async fn find_batch_start(
        &self,
        fetch_offset: i64,
        high_watermark: i64,
    ) -> SlateDBResult<Option<i64>> {
        use super::super::keys::decode_record_offset;

        // Strategy 1: range lookup against the in-memory index.
        if let Some((base_offset, record_count)) = self.batch_index.floor(fetch_offset) {
            let batch_end = base_offset
                .checked_add(record_count as i64)
                .unwrap_or(i64::MAX);
            if batch_end > fetch_offset {
                super::super::metrics::record_batch_index_hit();
                return Ok(Some(base_offset));
            }
        }

        super::super::metrics::record_batch_index_miss();

        // Bounded back-scan with widening window.
        let log_start = self.log_start_offset.load(Ordering::SeqCst);
        let mut window = INITIAL_BATCH_BACKSCAN_WINDOW;
        // Only batch boundaries within the initial back-scan window are
        // worth indexing — older entries get evicted as soon as newer
        // boundaries arrive in the same scan, wasting BatchIndex inserts
        // on a wide cold-cache fetch.
        let index_lower_bound = fetch_offset.saturating_sub(INITIAL_BATCH_BACKSCAN_WINDOW);

        loop {
            let scan_start = (fetch_offset - window).max(log_start);
            let start_key = encode_record_key(scan_start);
            let end_key = encode_record_key(high_watermark);

            let mut iter = self
                .db
                .scan(start_key.as_slice()..end_key.as_slice())
                .await?;

            // Track whether the window contained any batch starting at or
            // before fetch_offset. If not, a batch containing fetch_offset
            // could still start before the window — we must widen rather
            // than wrongly return the next-following batch.
            let mut saw_batch_at_or_before = false;

            while let Some(item) = iter.next().await.map_err(SlateDBError::from)? {
                if let Some(offset) = decode_record_offset(&item.key) {
                    // Strip HWM metadata if present
                    let batch_data = if item.value.len() >= 8 {
                        &item.value[8..]
                    } else {
                        item.value.as_ref()
                    };
                    let record_count = match parse_record_count_checked(batch_data) {
                        Ok(n) => n,
                        Err(e) => {
                            // Surface the corruption: a single bad batch in
                            // the back-scan path used to be silently treated
                            // as `record_count = 0`, which made the loop
                            // skip past it and consumers saw an unexplained
                            // hole. Emit a metric so operators can alarm on
                            // it; preserve the legacy behavior of treating
                            // it as 0 to avoid throwing on every fetch from
                            // the same poisoned partition.
                            super::super::metrics::record_corrupt_batch(
                                &self.topic,
                                self.partition,
                                "fetch_index_scan",
                            );
                            tracing::warn!(
                                topic = %self.topic,
                                partition = self.partition,
                                offset,
                                error = %e,
                                "Corrupt batch encountered during fetch index scan"
                            );
                            0
                        }
                    };

                    if offset >= index_lower_bound && offset < high_watermark {
                        self.add_to_batch_index(offset, record_count);
                    }

                    let batch_end = offset.checked_add(record_count as i64).unwrap_or(i64::MAX);
                    if offset <= fetch_offset {
                        saw_batch_at_or_before = true;
                        if batch_end > fetch_offset {
                            // This batch contains fetch_offset
                            return Ok(Some(offset));
                        }
                    } else {
                        // First batch after fetch_offset. This is the right
                        // answer only if we can rule out an earlier batch
                        // containing fetch_offset — i.e. we saw at least one
                        // batch at/before it in this window, or the window
                        // already reaches the log start.
                        if saw_batch_at_or_before || scan_start == log_start {
                            return Ok(Some(offset));
                        }
                        break; // widen the window and retry
                    }
                }
            }

            if scan_start == log_start {
                // Whole remaining range scanned: no batch contains or
                // follows fetch_offset.
                return Ok(None);
            }

            // Saw only batches before the window edge (or none) without a
            // conclusion — widen and retry.
            window = window.saturating_mul(2);
        }
    }
}
