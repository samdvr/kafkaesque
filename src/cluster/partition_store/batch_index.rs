//! In-memory batch-boundary index keyed by `base_offset`.

use std::collections::BTreeMap;
use std::sync::RwLock as StdRwLock;

/// Default maximum number of batch boundaries to cache per partition.
/// Keeps memory bounded while providing efficient offset lookup.
/// Can be overridden via ClusterConfig.batch_index_max_size.
pub(super) const DEFAULT_BATCH_INDEX_MAX_SIZE: usize = 10_000;

/// In-memory batch boundary index keyed by `base_offset`.
///
/// Backed by a `BTreeMap` so the fetch path can answer range queries
/// (the largest base_offset at or before `fetch_offset`) in O(log n).
/// Hash-only caches (the previous moka layout) only hit on exact-batch-
/// boundary lookups; mid-batch fetches fell through to a windowed SlateDB
/// scan even though the boundary was already known locally.
///
/// Capacity is enforced by evicting the smallest offset (oldest batch) on
/// overflow. Tail reads (`fetch.offset` near `HWM`) are the hot pattern, so
/// older entries falling out of cache is exactly what we want.
pub(super) struct BatchIndex {
    inner: StdRwLock<BTreeMap<i64, i32>>,
    capacity: usize,
}

impl BatchIndex {
    pub(super) fn new(capacity: usize) -> Self {
        Self {
            inner: StdRwLock::new(BTreeMap::new()),
            capacity,
        }
    }

    /// Largest entry whose `base_offset <= target`, if any.
    pub(super) fn floor(&self, target: i64) -> Option<(i64, i32)> {
        let g = self.inner.read().unwrap();
        g.range(..=target).next_back().map(|(&k, &v)| (k, v))
    }

    pub(super) fn insert(&self, base_offset: i64, record_count: i32) {
        let mut g = self.inner.write().unwrap();
        g.insert(base_offset, record_count);
        while g.len() > self.capacity {
            let smallest = match g.keys().next().copied() {
                Some(k) => k,
                None => break,
            };
            g.remove(&smallest);
        }
    }

    pub(super) fn invalidate(&self, offset: i64) {
        let mut g = self.inner.write().unwrap();
        g.remove(&offset);
    }
}
