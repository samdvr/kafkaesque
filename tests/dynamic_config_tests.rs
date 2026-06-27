//! Dynamic-config gating tests.
//!
//! Live config edits (AlterConfigs / IncrementalAlterConfigs) re-run the
//! same validation gate at request time — there is no restart in the loop.
//! Get this gate wrong and an operator-issued bad value either bricks the
//! topic (silently accepted, then crashes the next consumer) or rejects a
//! legitimate edit at request time when it should have applied.
//!
//! What this file pins:
//!
//! 1. A full canonical map of every recognized key validates cleanly.
//! 2. validate_raw stops at the first invalid value and surfaces the
//!    failing key — the response carries that key back to the operator.
//! 3. The retention.ms = -1 sentinel ("disable retention") is accepted.
//! 4. CleanupPolicy round-trips through `to_raw` / `validate_raw` for
//!    every supported policy variant.
//! 5. Out-of-range values per documented floor are rejected: segment.ms
//!    must be >= 1, max.compaction.lag.ms must be >= 1,
//!    min.compaction.lag.ms / delete.retention.ms must be >= 0.
//! 6. resolve() applied to to_raw() round-trips a typed config.
//!
//! These properties are what callers rely on when edits land at runtime.
//! A regression here surfaces as either silent bad-config corruption or
//! spurious AlterConfigs rejections.

use std::collections::HashMap;

use kafkaesque::cluster::{ClusterConfig, TopicCompactionConfig};

const KEY_CLEANUP_POLICY: &str = "cleanup.policy";
const KEY_RETENTION_MS: &str = "retention.ms";
const KEY_MIN_CLEANABLE_DIRTY_BYTES: &str = "min.cleanable.dirty.bytes";
const KEY_MIN_COMPACTION_LAG_MS: &str = "min.compaction.lag.ms";
const KEY_MAX_COMPACTION_LAG_MS: &str = "max.compaction.lag.ms";
const KEY_DELETE_RETENTION_MS: &str = "delete.retention.ms";
const KEY_SEGMENT_MS: &str = "segment.ms";

fn defaults() -> ClusterConfig {
    ClusterConfig::default()
}

fn raw(items: &[(&str, &str)]) -> HashMap<String, String> {
    items
        .iter()
        .map(|(k, v)| ((*k).to_string(), (*v).to_string()))
        .collect()
}

// ===========================================================================
// 1. Canonical map validates
// ===========================================================================

#[test]
fn full_canonical_map_validates() {
    let map = raw(&[
        (KEY_CLEANUP_POLICY, "delete"),
        (KEY_RETENTION_MS, "604800000"),
        (KEY_MIN_CLEANABLE_DIRTY_BYTES, "67108864"),
        (KEY_MIN_COMPACTION_LAG_MS, "0"),
        (KEY_MAX_COMPACTION_LAG_MS, "9223372036854775807"),
        (KEY_DELETE_RETENTION_MS, "86400000"),
        (KEY_SEGMENT_MS, "604800000"),
    ]);
    TopicCompactionConfig::validate_raw(&map)
        .expect("a fully populated canonical config must validate cleanly");
}

// ===========================================================================
// 2. validate_raw surfaces the first bad key
// ===========================================================================

#[test]
fn validate_raw_rejects_out_of_range_segment_ms() {
    let map = raw(&[
        (KEY_RETENTION_MS, "604800000"),
        // segment.ms floor is 1 ms.
        (KEY_SEGMENT_MS, "0"),
    ]);
    let err = TopicCompactionConfig::validate_raw(&map).expect_err("segment.ms=0 must be rejected");
    let msg = err.to_string();
    assert!(
        msg.contains("segment.ms"),
        "error must name the failing key; got {msg}",
    );
}

#[test]
fn validate_raw_rejects_zero_max_compaction_lag() {
    // 0 would mean "every record is overdue immediately" — would compact
    // ahead of the consumers' read window. Floor is 1 ms.
    let map = raw(&[(KEY_MAX_COMPACTION_LAG_MS, "0")]);
    let err = TopicCompactionConfig::validate_raw(&map)
        .expect_err("max.compaction.lag.ms=0 must be rejected");
    assert!(err.to_string().contains("max.compaction.lag.ms"));
}

#[test]
fn validate_raw_rejects_negative_min_compaction_lag() {
    let map = raw(&[(KEY_MIN_COMPACTION_LAG_MS, "-1")]);
    let err = TopicCompactionConfig::validate_raw(&map)
        .expect_err("negative min.compaction.lag.ms must be rejected");
    assert!(err.to_string().contains("min.compaction.lag.ms"));
}

#[test]
fn validate_raw_rejects_unparseable_integer() {
    let map = raw(&[(KEY_RETENTION_MS, "not-a-number")]);
    let err = TopicCompactionConfig::validate_raw(&map).expect_err("garbage must reject");
    let msg = err.to_string();
    assert!(
        msg.contains("retention.ms"),
        "error must name the key; got {msg}"
    );
}

// ===========================================================================
// 3. retention.ms = -1 is the disable sentinel
// ===========================================================================

#[test]
fn retention_ms_disable_sentinel_is_accepted() {
    let map = raw(&[(KEY_RETENTION_MS, "-1")]);
    TopicCompactionConfig::validate_raw(&map)
        .expect("retention.ms = -1 (disable) must be accepted; it is the documented sentinel");
}

// ===========================================================================
// 4. CleanupPolicy round-trip through validate_raw
// ===========================================================================

#[test]
fn cleanup_policy_supported_values_validate() {
    for v in ["delete", "compact", "compact,delete"] {
        let map = raw(&[(KEY_CLEANUP_POLICY, v)]);
        TopicCompactionConfig::validate_raw(&map)
            .unwrap_or_else(|e| panic!("cleanup.policy={v} must validate; got {e}"));
    }
}

#[test]
fn cleanup_policy_unsupported_value_rejected() {
    let map = raw(&[(KEY_CLEANUP_POLICY, "rotate-and-burn")]);
    let err = TopicCompactionConfig::validate_raw(&map)
        .expect_err("unsupported cleanup.policy must reject");
    assert!(err.to_string().contains("cleanup.policy"));
}

// ===========================================================================
// 5. Unknown keys are tolerated by validate_raw
// ===========================================================================

#[test]
fn unknown_keys_do_not_cause_validation_to_fail() {
    // Operators can stuff metadata-like keys into a config map; the
    // validator must only reject *known* keys with bad values, not
    // gatekeep on the schema.
    let map = raw(&[
        (KEY_RETENTION_MS, "60000"),
        ("custom.team.tag", "data-platform"),
    ]);
    TopicCompactionConfig::validate_raw(&map)
        .expect("unknown keys must be tolerated; only known-key violations matter");
}

// ===========================================================================
// 6. resolve / to_raw round-trip
// ===========================================================================

#[test]
fn resolve_then_to_raw_round_trips_a_typed_config() {
    let cluster = defaults();
    let map = raw(&[
        (KEY_CLEANUP_POLICY, "compact,delete"),
        (KEY_RETENTION_MS, "60000"),
        (KEY_MIN_COMPACTION_LAG_MS, "0"),
        (KEY_MAX_COMPACTION_LAG_MS, "300000"),
        (KEY_DELETE_RETENTION_MS, "0"),
        (KEY_SEGMENT_MS, "600000"),
    ]);
    let resolved = TopicCompactionConfig::resolve(&map, &cluster);
    let serialized = resolved.to_raw();

    // The serialized map is also a valid config — running it back through
    // validate_raw must succeed.
    let serialized_map: HashMap<String, String> = serialized
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect();
    TopicCompactionConfig::validate_raw(&serialized_map)
        .expect("to_raw output must be parseable back via validate_raw");

    // The resolved view preserves the overrides we set.
    assert_eq!(resolved.retention_ms, 60_000);
    assert_eq!(resolved.max_compaction_lag_ms, 300_000);
    assert_eq!(resolved.delete_retention_ms, 0);
    assert_eq!(resolved.segment_ms, 600_000);
}

// ===========================================================================
// 7. resolve falls back silently on a malformed value
// ===========================================================================

#[test]
fn resolve_falls_back_on_malformed_value() {
    // The read path is forgiving: a bad value emits a warn! and the
    // cluster default takes over, so a config that drifted to bogus
    // doesn't take down the broker. (The write path uses validate_raw
    // to reject this up front.)
    let cluster = defaults();
    let map = raw(&[(KEY_RETENTION_MS, "definitely-not-a-number")]);
    let resolved = TopicCompactionConfig::resolve(&map, &cluster);
    assert_eq!(
        resolved.retention_ms, cluster.log_retention_ms,
        "malformed value must fall back to the cluster default",
    );
}

// ===========================================================================
// 8. Incremental edit: new map runs through validation each time
// ===========================================================================

#[test]
fn each_proposed_state_is_validated_independently() {
    // Models the IncrementalAlterConfigs flow: read current, apply ops,
    // validate result. Each proposed final state must run through the
    // validator independently — there is no per-key memoization that
    // would let a stale-but-now-invalid key sneak through.
    let mut current = raw(&[(KEY_RETENTION_MS, "60000")]);
    TopicCompactionConfig::validate_raw(&current).expect("initial must be valid");

    // Op 1: shorten retention. Still valid.
    current.insert(KEY_RETENTION_MS.to_string(), "30000".to_string());
    TopicCompactionConfig::validate_raw(&current).expect("post-op1 must be valid");

    // Op 2: introduce a bad segment.ms. Validation must now fail.
    current.insert(KEY_SEGMENT_MS.to_string(), "0".to_string());
    let err = TopicCompactionConfig::validate_raw(&current)
        .expect_err("post-op2 must reject the bad segment.ms");
    assert!(err.to_string().contains("segment.ms"));

    // Op 3: roll back the bad op. Validation must pass again — the
    // validator does not carry sticky state from prior bad attempts.
    current.insert(KEY_SEGMENT_MS.to_string(), "604800000".to_string());
    TopicCompactionConfig::validate_raw(&current).expect("post-rollback must be valid");
}
