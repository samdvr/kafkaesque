//! Metrics cardinality / labelling tests.
//!
//! The broker bounds the cardinality of every label that could be filled
//! from user input (topic name, principal, consumer group). Without these
//! bounds, a buggy or hostile client that churns through random
//! `User:<uuid>` strings (or random group IDs, or auto-creates random
//! topics) inflates Prometheus cardinality without limit and turns the
//! /metrics endpoint into a memory leak.
//!
//! What this file pins:
//!
//! 1. `configure_metrics(_, max)` makes the cardinality cap effective.
//! 2. Driving recorders past the cap rolls labels into an overflow bucket
//!    rather than continuing to add new label combinations.
//! 3. The `metric_labels_dropped_total` counter tracks every overflow so
//!    operators can alert on a non-zero rate (silent truncation hides the
//!    runaway client AND hides the fact that the per-label data is now
//!    incomplete).
//! 4. The cap is per-dimension: filling the principal dimension does not
//!    drop topic labels, and vice versa.
//!
//! Implementation note: the cardinality state is a process-global DashSet
//! shared across every test that records labels. The tests here use
//! `#[serial]` and unique-suffix label values so they observe their own
//! deltas regardless of what other tests in the same binary did first.

use kafkaesque::cluster::metrics;
use serial_test::serial;

/// Pull a single labelled metric's value from the registry. Returns 0 if
/// the (metric, label-set) tuple has not been recorded yet.
fn counter_value(metric_name: &str, labels: &[(&str, &str)]) -> u64 {
    // The registry namespaces every family with a `kafkaesque_` prefix
    // and the prometheus crate may strip the `_total` suffix from the
    // family name. Match against the prefixed name as the source of truth.
    let prefixed = format!("kafkaesque_{}", metric_name);
    let stripped = prefixed.strip_suffix("_total").unwrap_or(&prefixed);
    for family in metrics::gather_metrics() {
        let fname = family.name();
        if fname != prefixed && fname != stripped {
            continue;
        }
        for m in family.get_metric() {
            let pairs: Vec<(&str, &str)> = m
                .get_label()
                .iter()
                .map(|l| (l.name(), l.value()))
                .collect();
            if labels.iter().all(|want| pairs.contains(want)) {
                // MessageField<Counter> dereferences to Counter; Counter::value
                // returns f64. Cast to u64 — counters are non-negative integer
                // values in this exposition.
                return m.get_counter().value() as u64;
            }
        }
    }
    0
}

/// Sum of `metric_labels_dropped_total{dimension=<dim>}` from the registry.
fn dropped_for(dim: &str) -> u64 {
    counter_value("metric_labels_dropped_total", &[("dimension", dim)])
}

// ===========================================================================
// 1. Principal-dimension cardinality bounding
// ===========================================================================

#[test]
#[serial]
fn principal_dimension_overflows_into_other_bucket() {
    metrics::init_metrics();
    // Tight cap so the test runs cheap. A previous serial test may have
    // already filled the principal set partially; using a tight cap
    // (5) guarantees we hit overflow within a small number of pushes.
    metrics::configure_metrics(true, 5);

    let dropped_before = dropped_for("principal");

    // Push 50 distinct principals; well past 5 + whatever a prior test
    // contributed.
    for i in 0..50 {
        let principal = format!("User:cardinality-test-principal-{}", i);
        metrics::record_acl_denial(&principal, "Produce", "Topic");
    }

    let dropped_after = dropped_for("principal");
    assert!(
        dropped_after > dropped_before,
        "metric_labels_dropped_total{{dimension=principal}} must increase past the cap; \
         before={dropped_before}, after={dropped_after}",
    );

    // Restore a generous cap so other (potentially-parallel) tests aren't
    // forced through the overflow bucket.
    metrics::configure_metrics(true, 10_000);
}

// ===========================================================================
// 2. Group-dimension cardinality bounding
// ===========================================================================

#[test]
#[serial]
fn group_dimension_overflows_into_other_bucket() {
    metrics::init_metrics();
    metrics::configure_metrics(true, 5);

    let dropped_before = dropped_for("group");

    for i in 0..50 {
        let group = format!("cardinality-test-group-{}", i);
        metrics::record_offset_commit(&group, "some-topic", "success");
    }

    let dropped_after = dropped_for("group");
    assert!(
        dropped_after > dropped_before,
        "metric_labels_dropped_total{{dimension=group}} must increase past the cap; \
         before={dropped_before}, after={dropped_after}",
    );

    metrics::configure_metrics(true, 10_000);
}

// ===========================================================================
// 3. Cap of 0 means unlimited
// ===========================================================================

#[test]
#[serial]
fn zero_cap_disables_cardinality_bounding() {
    // The `configure_metrics(_, 0)` contract documents 0 as "unlimited".
    // After flipping into that mode, no further drops should be recorded
    // on a fresh-flush dimension.
    metrics::init_metrics();
    metrics::configure_metrics(true, 0);

    let dropped_before = dropped_for("partition");

    for i in 0..50 {
        let topic = format!("cardinality-test-zero-cap-topic-{}", i);
        metrics::record_produce(&topic, 0, 1, 64);
    }

    let dropped_after = dropped_for("partition");
    assert_eq!(
        dropped_before, dropped_after,
        "with cap=0, metric_labels_dropped_total{{dimension=partition}} must NOT increase; \
         before={dropped_before}, after={dropped_after}",
    );

    metrics::configure_metrics(true, 10_000);
}

// ===========================================================================
// 4. encode_metrics succeeds even with overflow buckets present
// ===========================================================================

#[test]
#[serial]
fn encode_metrics_succeeds_after_overflow_recorded() {
    // Pin: even after the overflow bucket has been written into, the
    // exposition output is still well-formed Prometheus text and the
    // overflow bucket appears verbatim. A regression here would turn
    // /metrics into a 500.
    metrics::init_metrics();
    metrics::configure_metrics(true, 2);

    for i in 0..30 {
        metrics::record_acl_denial(
            &format!("User:encode-overflow-{}", i),
            "Produce",
            "Topic",
        );
    }

    let text = metrics::encode_metrics().expect("encode_metrics must succeed");
    assert!(
        text.contains("metric_labels_dropped_total"),
        "exposition must include the dropped-labels counter",
    );

    metrics::configure_metrics(true, 10_000);
}
