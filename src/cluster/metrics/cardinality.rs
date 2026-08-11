//! Cardinality-aware metric recording and partition label bounding.

use once_cell::sync::Lazy;
use prometheus::IntCounter;
use std::borrow::Cow;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use super::{
    BYTES_FETCHED, BYTES_PRODUCED, MESSAGES_FETCHED, MESSAGES_PRODUCED, METRIC_LABELS_DROPPED,
};

// ============================================================================
// Cardinality-aware metric recording
// ============================================================================

/// Pre-formatted partition labels for the common range. Most clusters keep
/// partition counts under 1024; pre-formatting amortizes the per-record
/// `itoa::Buffer` work and keeps a stable `&'static str` so the metric
/// `with_label_values` call doesn't allocate. Outside this range we fall
/// back to `format_partition_label`.
static PARTITION_LABEL_CACHE: Lazy<&'static [String]> = Lazy::new(|| {
    let v: Vec<String> = (0..1024).map(|i: i32| i.to_string()).collect();
    Box::leak(v.into_boxed_slice())
});

fn cached_partition_label(partition: i32) -> Option<&'static str> {
    if (0..1024).contains(&partition) {
        Some(PARTITION_LABEL_CACHE[partition as usize].as_str())
    } else {
        None
    }
}

/// Global configuration for partition-level metrics.
/// Set via `configure_metrics()` at startup.
static PARTITION_METRICS_ENABLED: AtomicBool = AtomicBool::new(true);
static MAX_METRIC_CARDINALITY: AtomicUsize = AtomicUsize::new(10_000);

/// Tracks unique topic/partition combinations for cardinality limiting.
///
/// `DashSet` gives wait-free reads (sharded — concurrent threads rarely
/// contend on the same shard) and lock-free first-time inserts, so the
/// produce/fetch hot path no longer serializes through a single async
/// rwlock for every record's cardinality check.
/// Tracks unique (topic, partition) pairs already labelled. Stored as a
/// 64-bit hash so the steady-state probe — `record_produce`/`record_fetch` —
/// hits a single `DashSet::contains(&u64)` with zero allocation. The
/// previous shape (`DashSet<(String, i32)>`) forced a `topic.to_string()`
/// per call to build the lookup key, allocating on every produce and
/// fetch. The cap is a soft limit on cardinality growth, so the negligible
/// false-positive rate from a 64-bit hash collision is benign — at most
/// one fewer label is dropped to `_overflow`.
static TRACKED_PARTITIONS: Lazy<dashmap::DashSet<u64>> = Lazy::new(dashmap::DashSet::new);

/// Hash a (topic, partition) pair into the cardinality-tracking key space.
/// Collision odds at the 10k-entry cap are ~10⁻¹², so a stray collision
/// just means one fewer label gets dropped to `_overflow` — benign on a
/// soft cap.
fn tracked_partition_key(topic: &str, partition: i32) -> u64 {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};
    let mut h = DefaultHasher::new();
    topic.hash(&mut h);
    partition.hash(&mut h);
    h.finish()
}

/// Tracks unique topic labels used on latency histograms.
static TRACKED_LATENCY_TOPICS: Lazy<dashmap::DashSet<String>> = Lazy::new(dashmap::DashSet::new);

/// Tracks unique principals seen on labelled metrics. Bounded so a hostile
/// or buggy client can't blow up cardinality by churning through random
/// `User:<uuid>` strings.
static TRACKED_PRINCIPALS: Lazy<dashmap::DashSet<String>> = Lazy::new(dashmap::DashSet::new);

/// Tracks unique consumer-group IDs seen on labelled metrics. Bounded so a
/// runaway group-ID generator (random group IDs, per-request groups) cannot
/// inflate Prometheus cardinality without limit.
static TRACKED_GROUPS: Lazy<dashmap::DashSet<String>> = Lazy::new(dashmap::DashSet::new);

/// Configure metrics cardinality settings.
///
/// Call this once at startup before recording any partition metrics.
///
/// # Arguments
/// * `enable_partition_metrics` - Whether to include partition labels
/// * `max_cardinality` - Maximum unique topic/partition combinations to track (0 = unlimited)
pub fn configure_metrics(enable_partition_metrics: bool, max_cardinality: usize) {
    PARTITION_METRICS_ENABLED.store(enable_partition_metrics, Ordering::SeqCst);
    MAX_METRIC_CARDINALITY.store(max_cardinality, Ordering::SeqCst);
}

/// Check if a topic/partition should be tracked or overflow.
///
/// Returns the partition label to use:
/// - The actual partition number if under cardinality limit
/// - "_overflow" if limit is reached and this is a new topic/partition
/// - "_all" if partition metrics are disabled
pub(crate) async fn get_partition_label(topic: &str, partition: i32) -> String {
    if !PARTITION_METRICS_ENABLED.load(Ordering::Relaxed) {
        return "_all".to_string();
    }

    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality == 0 {
        return format_partition_label(partition);
    }

    let key = tracked_partition_key(topic, partition);

    // Wait-free read on DashSet — different shards never contend.
    if TRACKED_PARTITIONS.contains(&key) {
        return format_partition_label(partition);
    }
    if TRACKED_PARTITIONS.len() >= max_cardinality {
        METRIC_LABELS_DROPPED
            .with_label_values(&["partition"])
            .inc();
        return "_overflow".to_string();
    }
    // Insert is lock-free at the shard granularity. We may briefly exceed the
    // cap under concurrent inserts; that's acceptable — the cap is a
    // soft limit on cardinality growth, not a hard ceiling.
    TRACKED_PARTITIONS.insert(key);
    format_partition_label(partition)
}

fn format_partition_label(partition: i32) -> String {
    if let Some(cached) = cached_partition_label(partition) {
        return cached.to_string();
    }
    let mut buf = itoa::Buffer::new();
    buf.format(partition).to_string()
}

/// Bound topic labels on latency histograms to avoid unbounded Prometheus
/// cardinality when clients auto-create topics.
pub(super) fn bounded_topic_label(topic: &str) -> String {
    if topic == "_multi" {
        return topic.to_string();
    }

    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality == 0 {
        return topic.to_string();
    }

    if TRACKED_LATENCY_TOPICS.contains(topic) {
        return topic.to_string();
    }
    if TRACKED_LATENCY_TOPICS.len() >= max_cardinality {
        METRIC_LABELS_DROPPED.with_label_values(&["topic"]).inc();
        return "_overflow".to_string();
    }
    TRACKED_LATENCY_TOPICS.insert(topic.to_string());
    topic.to_string()
}

/// Bound principal labels on metrics that include the principal as a
/// dimension. Same overflow semantics as [`bounded_topic_label`].
pub(crate) fn bounded_principal_label(principal: &str) -> String {
    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality == 0 {
        return principal.to_string();
    }
    if TRACKED_PRINCIPALS.contains(principal) {
        return principal.to_string();
    }
    if TRACKED_PRINCIPALS.len() >= max_cardinality {
        METRIC_LABELS_DROPPED
            .with_label_values(&["principal"])
            .inc();
        return "_other".to_string();
    }
    TRACKED_PRINCIPALS.insert(principal.to_string());
    principal.to_string()
}

/// Bound consumer-group labels on metrics that include `group` as a dimension.
/// Same overflow semantics as [`bounded_topic_label`].— without
/// this, every `record_offset_commit` / `record_rebalance_duration` call
/// became a cardinality bomb when a buggy client used random group IDs.
pub(crate) fn bounded_group_label(group: &str) -> String {
    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality == 0 {
        return group.to_string();
    }
    if TRACKED_GROUPS.contains(group) {
        return group.to_string();
    }
    if TRACKED_GROUPS.len() >= max_cardinality {
        METRIC_LABELS_DROPPED.with_label_values(&["group"]).inc();
        return "_overflow".to_string();
    }
    TRACKED_GROUPS.insert(group.to_string());
    group.to_string()
}

/// Bound (topic, partition) labels for per-partition counters/gauges. Same
/// overflow semantics as [`bounded_topic_label`].
pub(crate) fn bounded_partition_label(topic: &str, partition: i32) -> (String, String) {
    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality == 0 {
        return (topic.to_string(), partition.to_string());
    }
    let key = tracked_partition_key(topic, partition);
    if TRACKED_PARTITIONS.contains(&key) {
        return (topic.to_string(), partition.to_string());
    }
    if TRACKED_PARTITIONS.len() >= max_cardinality {
        METRIC_LABELS_DROPPED
            .with_label_values(&["partition"])
            .inc();
        return ("_overflow".to_string(), "_overflow".to_string());
    }
    TRACKED_PARTITIONS.insert(key);
    (topic.to_string(), partition.to_string())
}

/// Synchronous partition label for the produce/fetch hot path.
///
/// Cardinality limiting is identical to the async [`get_partition_label`], but
/// the result is a `Cow<'static, str>` that avoids a per-call `String`
/// allocation in the overwhelming common case: partition-metrics disabled
/// (`_all`), the cardinality-overflow placeholder (`_overflow`), or a small
/// partition index served as a `&'static str` from `PARTITION_LABEL_CACHE`.
/// Only partition indices outside that cache (>= 1024, uncommon) allocate.
pub(crate) fn get_partition_label_sync(topic: &str, partition: i32) -> Cow<'static, str> {
    if !PARTITION_METRICS_ENABLED.load(Ordering::Relaxed) {
        return Cow::Borrowed("_all");
    }

    let max_cardinality = MAX_METRIC_CARDINALITY.load(Ordering::Relaxed);
    if max_cardinality != 0 {
        let key = tracked_partition_key(topic, partition);
        if !TRACKED_PARTITIONS.contains(&key) {
            if TRACKED_PARTITIONS.len() >= max_cardinality {
                METRIC_LABELS_DROPPED
                    .with_label_values(&["partition"])
                    .inc();
                return Cow::Borrowed("_overflow");
            }
            TRACKED_PARTITIONS.insert(key);
        }
    }

    match cached_partition_label(partition) {
        Some(label) => Cow::Borrowed(label),
        None => {
            let mut buf = itoa::Buffer::new();
            Cow::Owned(buf.format(partition).to_string())
        }
    }
}

/// Prometheus counters for one `(topic, partition)`, resolved once.
///
/// `record_produce` / `record_fetch` each cost a `TRACKED_PARTITIONS` set probe
/// plus two `with_label_values` label-set hashes — roughly four map lookups per
/// record batch, on the two hottest paths in the broker. A `PartitionStore`
/// lives for as long as the broker owns the partition, so it can resolve these
/// handles once and then pay only the atomic `inc_by`.
///
/// Resolution applies the same cardinality limiting as the free functions, so a
/// partition beyond `MAX_METRIC_CARDINALITY` still collapses into the
/// `_overflow` series. Because the label is resolved on first use and then
/// held, a store's series is stable for its lifetime rather than able to flip
/// between its own label and `_overflow` as other partitions come and go.
#[derive(Clone)]
pub struct PartitionCounters {
    messages_produced: IntCounter,
    bytes_produced: IntCounter,
    messages_fetched: IntCounter,
    bytes_fetched: IntCounter,
}

impl PartitionCounters {
    /// Add to the produce counters.
    pub fn add_produce(&self, message_count: u64, bytes: u64) {
        self.messages_produced.inc_by(message_count);
        self.bytes_produced.inc_by(bytes);
    }

    /// Add to the fetch counters.
    pub fn add_fetch(&self, message_count: u64, bytes: u64) {
        self.messages_fetched.inc_by(message_count);
        self.bytes_fetched.inc_by(bytes);
    }
}

/// Resolve the four per-partition throughput counters for `(topic, partition)`.
pub fn partition_counters(topic: &str, partition: i32) -> PartitionCounters {
    let partition_str = get_partition_label_sync(topic, partition);
    let labels = [topic, partition_str.as_ref()];
    PartitionCounters {
        messages_produced: MESSAGES_PRODUCED.with_label_values(&labels),
        bytes_produced: BYTES_PRODUCED.with_label_values(&labels),
        messages_fetched: MESSAGES_FETCHED.with_label_values(&labels),
        bytes_fetched: BYTES_FETCHED.with_label_values(&labels),
    }
}

/// Record produced messages with cardinality awareness.
pub fn record_produce(topic: &str, partition: i32, message_count: u64, bytes: u64) {
    let partition_str = get_partition_label_sync(topic, partition);
    MESSAGES_PRODUCED
        .with_label_values(&[topic, partition_str.as_ref()])
        .inc_by(message_count);
    BYTES_PRODUCED
        .with_label_values(&[topic, partition_str.as_ref()])
        .inc_by(bytes);
}

/// Record produced messages with async cardinality limiting.
///
/// Use this version when calling from async context for proper cardinality enforcement.
pub async fn record_produce_async(topic: &str, partition: i32, message_count: u64, bytes: u64) {
    let partition_str = get_partition_label(topic, partition).await;
    MESSAGES_PRODUCED
        .with_label_values(&[topic, &partition_str])
        .inc_by(message_count);
    BYTES_PRODUCED
        .with_label_values(&[topic, &partition_str])
        .inc_by(bytes);
}

/// Record fetched messages with cardinality awareness.
pub fn record_fetch(topic: &str, partition: i32, message_count: u64, bytes: u64) {
    let partition_str = get_partition_label_sync(topic, partition);
    MESSAGES_FETCHED
        .with_label_values(&[topic, partition_str.as_ref()])
        .inc_by(message_count);
    BYTES_FETCHED
        .with_label_values(&[topic, partition_str.as_ref()])
        .inc_by(bytes);
}

/// Record fetched messages with async cardinality limiting.
///
/// Use this version when calling from async context for proper cardinality enforcement.
pub async fn record_fetch_async(topic: &str, partition: i32, message_count: u64, bytes: u64) {
    let partition_str = get_partition_label(topic, partition).await;
    MESSAGES_FETCHED
        .with_label_values(&[topic, &partition_str])
        .inc_by(message_count);
    BYTES_FETCHED
        .with_label_values(&[topic, &partition_str])
        .inc_by(bytes);
}
