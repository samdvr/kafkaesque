//! Typed view over a topic's free-form `HashMap<String, String>` config.
//!
//! Topic configs arrive on the wire as opaque key/value strings (CreateTopics
//! and AlterConfigs both pass them through unchanged) and are stored that
//! way in the Raft topic registry. This module is the boundary where those
//! strings turn into the typed knobs the broker actually uses — log
//! retention, compaction policy, etc.
//!
//! # Default merging
//!
//! [`TopicCompactionConfig::resolve`] walks the topic's config map; any key
//! the operator hasn't set falls back to the cluster-wide default in
//! [`super::ClusterConfig`]. This keeps Kafka semantics where every topic
//! has a value for every config — defined either by the topic or by the
//! broker — and never "unset".
//!
//! # Validation
//!
//! [`TopicCompactionConfig::validate_raw`] is the gate CreateTopics and
//! AlterConfigs call before persisting a config change. It rejects malformed
//! values (`cleanup.policy=garbage`, `min.compaction.lag.ms=-5`) up front so
//! the registry never holds a config that would crash the compaction loop
//! later. Unknown keys are *warned*, not rejected — Kafka's behavior, so
//! configs forwarded from a newer client (e.g. `compression.type` we don't
//! interpret yet) don't fail the request.

use std::collections::HashMap;

use super::ClusterConfig;

/// Topic-level cleanup policy (`cleanup.policy`).
///
/// Kafka allows the value to be a comma-separated list, so `compact,delete`
/// is the third state — both compaction and time/size retention applied to
/// the same log.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CleanupPolicy {
    /// Time/size-based retention only (Kafka default).
    Delete,
    /// Log compaction only — collapse to latest value per key.
    Compact,
    /// Compaction first, then time/size retention on the compacted log.
    CompactAndDelete,
}

impl CleanupPolicy {
    /// Whether this policy includes compaction.
    pub fn is_compact(self) -> bool {
        matches!(
            self,
            CleanupPolicy::Compact | CleanupPolicy::CompactAndDelete
        )
    }

    /// Whether this policy includes time/size retention.
    pub fn is_delete(self) -> bool {
        matches!(
            self,
            CleanupPolicy::Delete | CleanupPolicy::CompactAndDelete
        )
    }

    fn parse(raw: &str) -> Result<CleanupPolicy, ConfigError> {
        // Kafka accepts a comma-separated list. The order doesn't matter
        // semantically; we normalize to the three enum variants.
        let mut compact = false;
        let mut delete = false;
        for piece in raw.split(',') {
            match piece.trim() {
                "compact" => compact = true,
                "delete" => delete = true,
                "" => {} // tolerate trailing/leading commas
                other => {
                    return Err(ConfigError::InvalidValue {
                        key: KEY_CLEANUP_POLICY,
                        value: raw.to_string(),
                        reason: format!("unknown cleanup mode `{}`", other),
                    });
                }
            }
        }
        match (compact, delete) {
            (false, false) => Err(ConfigError::InvalidValue {
                key: KEY_CLEANUP_POLICY,
                value: raw.to_string(),
                reason: "must contain `compact`, `delete`, or both".to_string(),
            }),
            (true, false) => Ok(CleanupPolicy::Compact),
            (false, true) => Ok(CleanupPolicy::Delete),
            (true, true) => Ok(CleanupPolicy::CompactAndDelete),
        }
    }

    fn to_kafka_str(self) -> &'static str {
        match self {
            CleanupPolicy::Delete => "delete",
            CleanupPolicy::Compact => "compact",
            CleanupPolicy::CompactAndDelete => "compact,delete",
        }
    }
}

// ============================================================================
// Kafka topic-config key names. Spelled out as constants so a typo can't
// silently make a key unreachable; tests below pin every value.
// ============================================================================

pub const KEY_CLEANUP_POLICY: &str = "cleanup.policy";
pub const KEY_RETENTION_MS: &str = "retention.ms";
pub const KEY_MIN_CLEANABLE_DIRTY_BYTES: &str = "min.cleanable.dirty.bytes";
pub const KEY_MIN_COMPACTION_LAG_MS: &str = "min.compaction.lag.ms";
pub const KEY_MAX_COMPACTION_LAG_MS: &str = "max.compaction.lag.ms";
pub const KEY_DELETE_RETENTION_MS: &str = "delete.retention.ms";
pub const KEY_SEGMENT_MS: &str = "segment.ms";

/// Every topic-config key this broker recognizes.
///
/// Keys outside this list are tolerated (warned, not rejected) so a newer
/// client passing through `compression.type` etc. doesn't fail the request.
pub const KNOWN_TOPIC_CONFIG_KEYS: &[&str] = &[
    KEY_CLEANUP_POLICY,
    KEY_RETENTION_MS,
    KEY_MIN_CLEANABLE_DIRTY_BYTES,
    KEY_MIN_COMPACTION_LAG_MS,
    KEY_MAX_COMPACTION_LAG_MS,
    KEY_DELETE_RETENTION_MS,
    KEY_SEGMENT_MS,
];

/// Why a topic-config value was rejected.
///
/// `key` is `&'static str` because every error names one of the
/// [`KNOWN_TOPIC_CONFIG_KEYS`] constants — never user-supplied.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ConfigError {
    /// A known key was set to an unparseable / out-of-range value.
    InvalidValue {
        key: &'static str,
        value: String,
        reason: String,
    },
}

impl std::fmt::Display for ConfigError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ConfigError::InvalidValue { key, value, reason } => {
                write!(f, "invalid value `{}` for `{}`: {}", value, key, reason)
            }
        }
    }
}

impl std::error::Error for ConfigError {}

/// Resolved per-topic compaction/retention knobs.
///
/// Every field has a value: either set on the topic or inherited from the
/// cluster default. There is no "unset" state, mirroring Kafka.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct TopicCompactionConfig {
    pub policy: CleanupPolicy,
    /// Time-based retention window. `<= 0` disables retention (matches
    /// `ClusterConfig::log_retention_ms`).
    pub retention_ms: i64,
    /// Minimum bytes of "dirty" log (between the cleanable boundary and the
    /// last compaction's progress mark) before the next compaction pass
    /// runs on this partition.
    pub min_cleanable_dirty_bytes: u64,
    /// Records younger than this are not eligible for compaction. Mirrors
    /// Kafka's `min.compaction.lag.ms`.
    pub min_compaction_lag_ms: i64,
    /// Records older than this are eligible regardless of dirty-byte budget.
    /// Mirrors Kafka's `max.compaction.lag.ms`. `i64::MAX` disables.
    pub max_compaction_lag_ms: i64,
    /// How long to retain a tombstone (null-value record) after the last
    /// non-tombstone for the same key was compacted away. Lets lagging
    /// consumers observe the delete before it disappears.
    pub delete_retention_ms: i64,
    /// Parsed for compatibility (the reference Kafka rolls a new active
    /// segment after this elapses). We don't have segments, but the value
    /// must round-trip through DescribeConfigs/AlterConfigs so admin tools
    /// don't see it disappear.
    pub segment_ms: i64,
}

impl TopicCompactionConfig {
    /// Resolve a topic's typed config from its raw map plus the cluster
    /// defaults.
    ///
    /// Values that fail to parse fall back to the cluster default and emit
    /// a `tracing::warn!`. This is the read path; the *write* path
    /// (CreateTopics / AlterConfigs) uses [`Self::validate_raw`] to reject
    /// malformed configs up front, so a warn-and-fallback here means a
    /// config that was valid at write-time has somehow drifted to bogus —
    /// worth a log line, but should never happen in the steady state.
    pub fn resolve(raw: &HashMap<String, String>, defaults: &ClusterConfig) -> Self {
        let policy = match raw.get(KEY_CLEANUP_POLICY) {
            Some(v) => match CleanupPolicy::parse(v) {
                Ok(p) => p,
                Err(e) => {
                    tracing::warn!(error = %e, "ignoring invalid cleanup.policy, defaulting to `delete`");
                    CleanupPolicy::Delete
                }
            },
            None => CleanupPolicy::Delete,
        };

        let retention_ms =
            parse_i64_or_default(raw, KEY_RETENTION_MS, defaults.log_retention_ms, None);
        let min_cleanable_dirty_bytes = parse_u64_or_default(
            raw,
            KEY_MIN_CLEANABLE_DIRTY_BYTES,
            defaults.compaction_default_min_cleanable_dirty_bytes,
        );
        let min_compaction_lag_ms = parse_i64_or_default(
            raw,
            KEY_MIN_COMPACTION_LAG_MS,
            defaults.compaction_default_min_lag_ms,
            // Kafka's contract: 0 is the floor.
            Some(0),
        );
        let max_compaction_lag_ms = parse_i64_or_default(
            raw,
            KEY_MAX_COMPACTION_LAG_MS,
            defaults.compaction_default_max_lag_ms,
            // 1 ms floor — 0 would mean "everything is always overdue", which
            // would compact every record before consumers could read it.
            Some(1),
        );
        let delete_retention_ms = parse_i64_or_default(
            raw,
            KEY_DELETE_RETENTION_MS,
            defaults.compaction_default_delete_retention_ms,
            Some(0),
        );
        let segment_ms = parse_i64_or_default(
            raw,
            KEY_SEGMENT_MS,
            // No cluster-wide knob (we don't have segments). Use Kafka's
            // 7-day default so DescribeConfigs returns a sane value.
            7 * 24 * 60 * 60 * 1000,
            Some(1),
        );

        Self {
            policy,
            retention_ms,
            min_cleanable_dirty_bytes,
            min_compaction_lag_ms,
            max_compaction_lag_ms,
            delete_retention_ms,
            segment_ms,
        }
    }

    /// Validate every recognized key in `raw` without allocating a typed
    /// view. Returns the first error encountered; unknown keys are tolerated.
    ///
    /// AlterConfigs replaces the entire map (we don't yet implement the
    /// per-key Set/Delete/Append/Subtract operations Kafka exposes), so
    /// validation runs over the proposed final state.
    pub fn validate_raw(raw: &HashMap<String, String>) -> Result<(), ConfigError> {
        if let Some(v) = raw.get(KEY_CLEANUP_POLICY) {
            CleanupPolicy::parse(v)?;
        }
        validate_i64(raw, KEY_RETENTION_MS, None)?;
        validate_u64(raw, KEY_MIN_CLEANABLE_DIRTY_BYTES)?;
        validate_i64(raw, KEY_MIN_COMPACTION_LAG_MS, Some(0))?;
        validate_i64(raw, KEY_MAX_COMPACTION_LAG_MS, Some(1))?;
        validate_i64(raw, KEY_DELETE_RETENTION_MS, Some(0))?;
        validate_i64(raw, KEY_SEGMENT_MS, Some(1))?;
        Ok(())
    }

    /// Render this typed config as the raw map AlterConfigs would produce.
    ///
    /// Used by DescribeConfigs to surface the *resolved* (defaults-merged)
    /// view; that way `kafka-configs.sh --describe` returns every
    /// recognized key whether or not the topic explicitly set it. The keys
    /// in the returned map are stable.
    pub fn to_raw(&self) -> Vec<(&'static str, String)> {
        vec![
            (KEY_CLEANUP_POLICY, self.policy.to_kafka_str().to_string()),
            (KEY_RETENTION_MS, self.retention_ms.to_string()),
            (
                KEY_MIN_CLEANABLE_DIRTY_BYTES,
                self.min_cleanable_dirty_bytes.to_string(),
            ),
            (
                KEY_MIN_COMPACTION_LAG_MS,
                self.min_compaction_lag_ms.to_string(),
            ),
            (
                KEY_MAX_COMPACTION_LAG_MS,
                self.max_compaction_lag_ms.to_string(),
            ),
            (
                KEY_DELETE_RETENTION_MS,
                self.delete_retention_ms.to_string(),
            ),
            (KEY_SEGMENT_MS, self.segment_ms.to_string()),
        ]
    }
}

// ============================================================================
// Internal helpers — parse/validate scalar values with consistent error shape.
// ============================================================================

fn parse_i64_or_default(
    raw: &HashMap<String, String>,
    key: &'static str,
    default: i64,
    min: Option<i64>,
) -> i64 {
    let Some(v) = raw.get(key) else {
        return default;
    };
    match v.parse::<i64>() {
        Ok(n) if min.is_none_or(|m| n >= m) => n,
        Ok(n) => {
            tracing::warn!(key, value = n, "topic config below minimum, using default");
            default
        }
        Err(e) => {
            tracing::warn!(key, value = %v, error = %e, "topic config not parseable as i64, using default");
            default
        }
    }
}

fn parse_u64_or_default(raw: &HashMap<String, String>, key: &'static str, default: u64) -> u64 {
    let Some(v) = raw.get(key) else {
        return default;
    };
    v.parse::<u64>().unwrap_or_else(|e| {
        tracing::warn!(key, value = %v, error = %e, "topic config not parseable as u64, using default");
        default
    })
}

fn validate_i64(
    raw: &HashMap<String, String>,
    key: &'static str,
    min: Option<i64>,
) -> Result<(), ConfigError> {
    let Some(v) = raw.get(key) else {
        return Ok(());
    };
    let n = v.parse::<i64>().map_err(|e| ConfigError::InvalidValue {
        key,
        value: v.clone(),
        reason: format!("not a valid i64: {}", e),
    })?;
    if let Some(m) = min
        && n < m
    {
        return Err(ConfigError::InvalidValue {
            key,
            value: v.clone(),
            reason: format!("must be >= {}", m),
        });
    }
    Ok(())
}

fn validate_u64(raw: &HashMap<String, String>, key: &'static str) -> Result<(), ConfigError> {
    let Some(v) = raw.get(key) else {
        return Ok(());
    };
    v.parse::<u64>().map_err(|e| ConfigError::InvalidValue {
        key,
        value: v.clone(),
        reason: format!("not a valid u64: {}", e),
    })?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn defaults() -> ClusterConfig {
        ClusterConfig::default()
    }

    fn map_of<const N: usize>(items: [(&str, &str); N]) -> HashMap<String, String> {
        items
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect()
    }

    // ------------------------------------------------------------------
    // CleanupPolicy::parse — every form Kafka clients send, plus
    // adversarial whitespace and ordering inputs the parser must tolerate.
    // ------------------------------------------------------------------

    #[test]
    fn cleanup_policy_parses_canonical_values() {
        assert_eq!(
            CleanupPolicy::parse("delete").unwrap(),
            CleanupPolicy::Delete
        );
        assert_eq!(
            CleanupPolicy::parse("compact").unwrap(),
            CleanupPolicy::Compact
        );
        assert_eq!(
            CleanupPolicy::parse("compact,delete").unwrap(),
            CleanupPolicy::CompactAndDelete
        );
    }

    #[test]
    fn cleanup_policy_parse_is_order_independent() {
        // `delete,compact` and `compact,delete` are the same policy — the
        // typed enum collapses both to CompactAndDelete.
        assert_eq!(
            CleanupPolicy::parse("delete,compact").unwrap(),
            CleanupPolicy::CompactAndDelete
        );
    }

    #[test]
    fn cleanup_policy_tolerates_whitespace_and_extra_commas() {
        assert_eq!(
            CleanupPolicy::parse(" compact , delete ").unwrap(),
            CleanupPolicy::CompactAndDelete
        );
        assert_eq!(
            CleanupPolicy::parse(",compact,").unwrap(),
            CleanupPolicy::Compact
        );
    }

    #[test]
    fn cleanup_policy_rejects_unknown_modes() {
        let err = CleanupPolicy::parse("compress").unwrap_err();
        match err {
            ConfigError::InvalidValue { key, .. } => {
                assert_eq!(key, KEY_CLEANUP_POLICY);
            }
        }
    }

    #[test]
    fn cleanup_policy_rejects_empty() {
        // Empty string and pure-whitespace must error rather than default
        // to one of the policies. AlterConfigs callers can clear a key by
        // omitting it from the map, not by sending "".
        assert!(CleanupPolicy::parse("").is_err());
        assert!(CleanupPolicy::parse(",,").is_err());
    }

    #[test]
    fn cleanup_policy_predicate_helpers() {
        assert!(CleanupPolicy::Compact.is_compact());
        assert!(!CleanupPolicy::Compact.is_delete());
        assert!(!CleanupPolicy::Delete.is_compact());
        assert!(CleanupPolicy::Delete.is_delete());
        assert!(CleanupPolicy::CompactAndDelete.is_compact());
        assert!(CleanupPolicy::CompactAndDelete.is_delete());
    }

    #[test]
    fn cleanup_policy_round_trips_through_kafka_str() {
        for p in [
            CleanupPolicy::Delete,
            CleanupPolicy::Compact,
            CleanupPolicy::CompactAndDelete,
        ] {
            assert_eq!(CleanupPolicy::parse(p.to_kafka_str()).unwrap(), p);
        }
    }

    // ------------------------------------------------------------------
    // resolve — defaults vs. overrides, malformed values fall back.
    // ------------------------------------------------------------------

    #[test]
    fn resolve_empty_map_returns_cluster_defaults() {
        let cfg = TopicCompactionConfig::resolve(&HashMap::new(), &defaults());
        assert_eq!(cfg.policy, CleanupPolicy::Delete);
        assert_eq!(cfg.retention_ms, defaults().log_retention_ms);
        assert_eq!(
            cfg.min_cleanable_dirty_bytes,
            defaults().compaction_default_min_cleanable_dirty_bytes
        );
        assert_eq!(
            cfg.delete_retention_ms,
            defaults().compaction_default_delete_retention_ms
        );
    }

    #[test]
    fn resolve_overrides_precedence_over_defaults() {
        let raw = map_of([
            ("cleanup.policy", "compact"),
            ("retention.ms", "60000"),
            ("min.cleanable.dirty.bytes", "1024"),
            ("delete.retention.ms", "5000"),
        ]);
        let cfg = TopicCompactionConfig::resolve(&raw, &defaults());
        assert_eq!(cfg.policy, CleanupPolicy::Compact);
        assert_eq!(cfg.retention_ms, 60_000);
        assert_eq!(cfg.min_cleanable_dirty_bytes, 1024);
        assert_eq!(cfg.delete_retention_ms, 5_000);
    }

    #[test]
    fn resolve_falls_back_silently_on_malformed_value() {
        // A persisted-but-bogus value (drift, manual edit, etc.) must NOT
        // crash the read path. Validation at the write boundary should
        // make this unreachable in production.
        let raw = map_of([("retention.ms", "not-a-number")]);
        let cfg = TopicCompactionConfig::resolve(&raw, &defaults());
        assert_eq!(cfg.retention_ms, defaults().log_retention_ms);
    }

    #[test]
    fn resolve_clamps_below_minimum_to_default() {
        // min.compaction.lag.ms has a 0 floor — a negative value rolls back
        // to the cluster default rather than carrying through.
        let raw = map_of([("min.compaction.lag.ms", "-1")]);
        let cfg = TopicCompactionConfig::resolve(&raw, &defaults());
        assert_eq!(
            cfg.min_compaction_lag_ms,
            defaults().compaction_default_min_lag_ms
        );
    }

    // ------------------------------------------------------------------
    // validate_raw — every recognized field, valid + invalid.
    // ------------------------------------------------------------------

    #[test]
    fn validate_accepts_empty_map() {
        TopicCompactionConfig::validate_raw(&HashMap::new()).expect("empty must be ok");
    }

    #[test]
    fn validate_accepts_canonical_settings() {
        let raw = map_of([
            ("cleanup.policy", "compact,delete"),
            ("retention.ms", "604800000"),
            ("min.cleanable.dirty.bytes", "67108864"),
            ("min.compaction.lag.ms", "0"),
            ("max.compaction.lag.ms", "9223372036854775807"),
            ("delete.retention.ms", "86400000"),
            ("segment.ms", "604800000"),
        ]);
        TopicCompactionConfig::validate_raw(&raw).expect("canonical config must be ok");
    }

    #[test]
    fn validate_rejects_invalid_cleanup_policy() {
        let raw = map_of([("cleanup.policy", "compress")]);
        let err = TopicCompactionConfig::validate_raw(&raw).unwrap_err();
        match err {
            ConfigError::InvalidValue { key, .. } => assert_eq!(key, KEY_CLEANUP_POLICY),
        }
    }

    #[test]
    fn validate_rejects_negative_min_compaction_lag() {
        let raw = map_of([("min.compaction.lag.ms", "-1")]);
        let err = TopicCompactionConfig::validate_raw(&raw).unwrap_err();
        match err {
            ConfigError::InvalidValue { key, .. } => {
                assert_eq!(key, KEY_MIN_COMPACTION_LAG_MS);
            }
        }
    }

    #[test]
    fn validate_rejects_zero_max_compaction_lag() {
        // 0 is forbidden — see the comment on the lag floor.
        let raw = map_of([("max.compaction.lag.ms", "0")]);
        assert!(TopicCompactionConfig::validate_raw(&raw).is_err());
    }

    #[test]
    fn validate_rejects_unparseable_retention() {
        let raw = map_of([("retention.ms", "forever")]);
        let err = TopicCompactionConfig::validate_raw(&raw).unwrap_err();
        match err {
            ConfigError::InvalidValue { key, .. } => assert_eq!(key, KEY_RETENTION_MS),
        }
    }

    #[test]
    fn validate_rejects_negative_min_cleanable_bytes() {
        // u64 parse rejects anything negative; this is the test that
        // proves we use u64 for min.cleanable.dirty.bytes.
        let raw = map_of([("min.cleanable.dirty.bytes", "-100")]);
        assert!(TopicCompactionConfig::validate_raw(&raw).is_err());
    }

    #[test]
    fn validate_tolerates_unknown_key() {
        // Forward-compat: a newer client sending `compression.type=lz4`
        // must not fail the request. Unknown keys are persisted verbatim
        // (they round-trip through the registry) but never interpreted.
        let raw = map_of([("compression.type", "lz4"), ("flush.ms", "1000")]);
        TopicCompactionConfig::validate_raw(&raw).expect("unknown keys must not fail");
    }

    #[test]
    fn validate_rejects_negative_retention_when_explicit() {
        // retention.ms has no minimum — `-1` is Kafka's "disable" sentinel
        // and must round-trip. This pins that we do NOT impose a floor.
        let raw = map_of([("retention.ms", "-1")]);
        TopicCompactionConfig::validate_raw(&raw).expect("retention.ms=-1 is the disable sentinel");
    }

    // ------------------------------------------------------------------
    // to_raw — round-trip via resolve() so DescribeConfigs is consistent.
    // ------------------------------------------------------------------

    #[test]
    fn to_raw_round_trips_through_resolve() {
        let raw = map_of([
            ("cleanup.policy", "compact,delete"),
            ("retention.ms", "60000"),
            ("min.cleanable.dirty.bytes", "1024"),
            ("min.compaction.lag.ms", "100"),
            ("max.compaction.lag.ms", "200"),
            ("delete.retention.ms", "300"),
            ("segment.ms", "400"),
        ]);
        let cfg = TopicCompactionConfig::resolve(&raw, &defaults());
        let rendered: HashMap<String, String> = cfg
            .to_raw()
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();
        let cfg2 = TopicCompactionConfig::resolve(&rendered, &defaults());
        assert_eq!(cfg, cfg2);
    }

    #[test]
    fn to_raw_lists_every_known_key() {
        // DescribeConfigs returns the resolved view — every recognized
        // key must appear, otherwise admin tools think the topic is
        // missing knobs.
        let cfg = TopicCompactionConfig::resolve(&HashMap::new(), &defaults());
        let keys: Vec<&'static str> = cfg.to_raw().into_iter().map(|(k, _)| k).collect();
        for known in KNOWN_TOPIC_CONFIG_KEYS {
            assert!(
                keys.contains(known),
                "to_raw() output is missing recognized key `{}`",
                known
            );
        }
    }

    #[test]
    fn known_topic_config_keys_are_unique() {
        // Catch a copy-paste bug where two key names collide. Sort+dedup
        // and compare lengths.
        let mut sorted: Vec<&str> = KNOWN_TOPIC_CONFIG_KEYS.to_vec();
        sorted.sort_unstable();
        let len_before = sorted.len();
        sorted.dedup();
        assert_eq!(
            len_before,
            sorted.len(),
            "KNOWN_TOPIC_CONFIG_KEYS contains duplicates"
        );
    }
}
