//! Raft consensus metrics and record helpers.

use once_cell::sync::Lazy;
use prometheus::{HistogramVec, IntCounterVec, IntGauge};

use super::REGISTRY;
use super::registration::{
    register_histogram_vec_safe, register_int_counter_vec_safe, register_int_gauge_safe,
};

// =============================================================================
// Raft Consensus Metrics
// =============================================================================

/// Raft node state (-2=uninitialized, -1=shutdown, 0=follower, 1=candidate, 2=leader).
///
/// Seeded to [`RAFT_STATE_UNINITIALIZED`] so a broker that hasn't yet polled
/// openraft is **not** treated as a healthy follower by `/ready`. The first
/// metrics tick from the Raft coordinator overwrites this value.
pub static RAFT_STATE: Lazy<IntGauge> = Lazy::new(|| {
    let g = register_int_gauge_safe(
        &REGISTRY,
        "raft_state",
        "Current Raft node state (-2=uninitialized, -1=shutdown, 0=follower, 1=candidate, 2=leader)",
    );
    g.set(RAFT_STATE_UNINITIALIZED);
    g
});

/// Raft term counter.
pub static RAFT_TERM: Lazy<IntGauge> =
    Lazy::new(|| register_int_gauge_safe(&REGISTRY, "raft_term", "Current Raft term number"));

/// Raft commit index.
pub static RAFT_COMMIT_INDEX: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "raft_commit_index",
        "Current Raft commit index (last committed log entry)",
    )
});

/// Raft applied index.
pub static RAFT_APPLIED_INDEX: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "raft_applied_index",
        "Current Raft applied index (last applied to state machine)",
    )
});

/// Raft leader elections total.
pub static RAFT_ELECTIONS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "raft_elections_total",
        "Total Raft leader elections",
        &["result"], // result=won/lost/timeout
    )
});

/// Raft proposal latency histogram.
pub static RAFT_PROPOSAL_DURATION: Lazy<HistogramVec> = Lazy::new(|| {
    register_histogram_vec_safe(
        &REGISTRY,
        "raft_proposal_duration_seconds",
        "Raft proposal (write) latency in seconds",
        &["status"], // status=success/error
        vec![0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5],
    )
});

/// Raft snapshot operations.
pub static RAFT_SNAPSHOTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "raft_snapshots_total",
        "Total Raft snapshot operations",
        &["operation", "status"], // operation=create/install, status=success/error
    )
});

/// Raft log entries.
pub static RAFT_LOG_ENTRIES: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "raft_log_entries",
        "Current number of entries in Raft log",
    )
});

/// Raft pending proposals for backpressure monitoring.
pub static RAFT_PENDING_PROPOSALS: Lazy<IntGauge> = Lazy::new(|| {
    register_int_gauge_safe(
        &REGISTRY,
        "raft_pending_proposals",
        "Current number of pending Raft proposals",
    )
});

/// Raft proposal backpressure events.
pub static RAFT_BACKPRESSURE_EVENTS: Lazy<IntCounterVec> = Lazy::new(|| {
    register_int_counter_vec_safe(
        &REGISTRY,
        "raft_backpressure_events_total",
        "Total Raft proposal backpressure events",
        &["result"], // result=acquired/timeout/waiting
    )
});

/// Set the current Raft state.
///
/// # Arguments
/// * `state` - The state: 0=follower, 1=candidate, 2=leader, -1=shutdown,
///   or [`RAFT_STATE_UNINITIALIZED`] before the Raft coordinator's first
///   metrics poll.
pub fn set_raft_state(state: i64) {
    RAFT_STATE.set(state);
}

/// Sentinel for "Raft has not reported a state yet". The Prometheus gauge
/// defaults to 0, which collides with the Follower state — so we publish
/// this distinct value at coordinator startup before the first poll lands,
/// and `/ready` treats it as "not ready" instead of "follower / OK".
pub const RAFT_STATE_UNINITIALIZED: i64 = -2;

/// Whether this broker's Raft node is currently ready to serve traffic.
///
/// Returns `true` for Follower (0) and Leader (2) — both states imply we
/// are participating in a quorum that has a leader. Candidate (1),
/// Shutdown (-1), and the pre-init sentinel (-2) all return `false`, so
/// Kubernetes will keep the pod out of rotation until the cluster reaches
/// consensus.
///
/// Learners are reported by the coordinator as Follower, so this returns
/// true for a learner; that matches openraft's "learner is a healthy
/// non-voting replica" model.
pub fn is_raft_ready() -> bool {
    matches!(RAFT_STATE.get(), 0 | 2)
}

/// Set the current Raft term.
pub fn set_raft_term(term: i64) {
    RAFT_TERM.set(term);
}

/// Set the current Raft commit index.
pub fn set_raft_commit_index(index: i64) {
    RAFT_COMMIT_INDEX.set(index);
}

/// Set the current Raft applied index.
pub fn set_raft_applied_index(index: i64) {
    RAFT_APPLIED_INDEX.set(index);
}

/// Record a Raft election event.
///
/// # Arguments
/// * `result` - The election result: "won", "lost", or "timeout"
pub fn record_raft_election(result: &str) {
    RAFT_ELECTIONS.with_label_values(&[result]).inc();
}

/// Record a Raft proposal latency.
///
/// # Arguments
/// * `status` - The proposal result: "success" or "error"
/// * `duration_secs` - The proposal duration in seconds
pub fn record_raft_proposal(status: &str, duration_secs: f64) {
    RAFT_PROPOSAL_DURATION
        .with_label_values(&[status])
        .observe(duration_secs);
}

/// Record a Raft snapshot operation.
///
/// # Arguments
/// * `operation` - The operation: "create" or "install"
/// * `status` - The result: "success" or "error"
pub fn record_raft_snapshot(operation: &str, status: &str) {
    RAFT_SNAPSHOTS.with_label_values(&[operation, status]).inc();
}

/// Set the current Raft log entry count.
pub fn set_raft_log_entries(count: i64) {
    RAFT_LOG_ENTRIES.set(count);
}

/// Set the current number of pending Raft proposals.
///
/// This should be called periodically to track backpressure state.
pub fn set_raft_pending_proposals(count: i64) {
    RAFT_PENDING_PROPOSALS.set(count);
}

/// Record a Raft backpressure event.
///
/// # Arguments
/// * `result` - The result: "acquired" (slot acquired immediately),
///   "waiting" (had to wait for slot), or "timeout" (failed to acquire)
pub fn record_raft_backpressure(result: &str) {
    RAFT_BACKPRESSURE_EVENTS.with_label_values(&[result]).inc();
}
