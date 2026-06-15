//! Clock-skew → lease-expiry coverage.
//!
//! The audit flagged that the clock-skew infrastructure exists but does not
//! drive the lease lifecycle in tests. The Kafkaesque lease state machine
//! is *deterministic in time*: every lease command carries an explicit
//! `timestamp_ms`, the `PartitionDomainState` advances a monotonic
//! `lease_clock_ms` per `max(clock, cmd.timestamp_ms)`, and all
//! grant/renew/expiry comparisons read that clamped value.
//!
//! That makes "what happens under clock skew?" testable without any
//! sleeping or real-time. We just feed the state machine the timestamps a
//! skewed broker would have proposed.
//!
//! Invariants asserted:
//!
//! 1. **Forward-skewed acquire** — a broker whose clock is ahead of the
//!    cluster cannot evict an active owner inside the takeover-grace window.
//!    The lease clock clamps forward but the existing lease's expiry is
//!    still in the future relative to the clamped clock.
//!
//! 2. **Backward-skewed renew** — a renew with a stale (backward) timestamp
//!    must NOT be able to move the expiry earlier than the clock would
//!    allow; the monotonic clamp guarantees `now >= lease_clock`, so the
//!    new expiry is anchored to the cluster clock, not the proposer's clock.
//!
//! 3. **Lease expires beyond grace window** — once the clamped clock
//!    advances past `expiry + LEASE_TAKEOVER_GRACE_MS`, the lease is dead:
//!    the original owner's renew is rejected and a peer's acquire takes
//!    over with a fresh epoch.
//!
//! 4. **Backward-skewed AcquirePartition** cannot rewind the lease clock —
//!    even if a broker submits an older `timestamp_ms` than the current
//!    clock, the apply uses the clamped (newer) value so the new lease
//!    starts at the clamped time, not the proposer's stale time.

use kafkaesque::cluster::raft::{PartitionCommand, PartitionDomainState, PartitionResponse};

/// Mirror of `cluster::raft::domains::partition::LEASE_TAKEOVER_GRACE_MS`.
/// That constant is module-private; redefining it here is intentional —
/// these tests pin the public, observable contract (a lease is dead at
/// `expiry + 5s`), so a future change to the grace window is supposed to
/// trip these assertions and force a deliberate update.
const LEASE_TAKEOVER_GRACE_MS: u64 = 5_000;

/// Convenience: drive `AcquirePartition` against a fresh state.
fn acquire(
    state: &mut PartitionDomainState,
    clock: &mut u64,
    topic: &str,
    partition: i32,
    broker: i32,
    lease_ms: u64,
    timestamp_ms: u64,
) -> PartitionResponse {
    state.apply(
        PartitionCommand::AcquirePartition {
            topic: topic.to_string(),
            partition,
            broker_id: broker,
            lease_duration_ms: lease_ms,
            timestamp_ms,
        },
        clock,
    )
}

fn renew(
    state: &mut PartitionDomainState,
    clock: &mut u64,
    topic: &str,
    partition: i32,
    broker: i32,
    lease_ms: u64,
    timestamp_ms: u64,
) -> PartitionResponse {
    state.apply(
        PartitionCommand::RenewLease {
            topic: topic.to_string(),
            partition,
            broker_id: broker,
            lease_duration_ms: lease_ms,
            timestamp_ms,
        },
        clock,
    )
}

#[test]
fn forward_skew_cannot_evict_active_owner_inside_grace() {
    // Broker 1 acquires at T=10s with a 60s lease. Broker 2's clock is
    // 30 seconds ahead — it submits an Acquire at T=40s. The lease (which
    // expires at 70s) is still well within its grace window relative to
    // the clamped clock; the takeover must fail.
    let mut state = PartitionDomainState::new();
    let mut clock = 0u64;

    let r = acquire(
        &mut state, &mut clock, "t", 0, /*broker*/ 1, 60_000, 10_000,
    );
    assert!(matches!(r, PartitionResponse::PartitionAcquired { .. }));
    assert_eq!(clock, 10_000);

    // Forward-skewed broker 2 attempts takeover at T=40s.
    let r = acquire(&mut state, &mut clock, "t", 0, 2, 60_000, 40_000);
    match r {
        PartitionResponse::PartitionOwnedByOther { owner, .. } => assert_eq!(owner, 1),
        other => panic!("expected PartitionOwnedByOther under forward skew, got {other:?}"),
    }
    // Clock advanced (clamped forward); the lease still belongs to broker 1.
    assert_eq!(clock, 40_000);
}

#[test]
fn lease_dies_past_expiry_plus_grace_and_peer_takes_over() {
    // Broker 1 acquires at T=0 with a 10s lease (expiry = 10_000).
    // After the clock crosses 10_000 + LEASE_TAKEOVER_GRACE_MS = 15_000,
    // the lease is dead: a peer's takeover at T=15_001 must succeed and
    // bump the leader epoch to fence stale writers.
    let mut state = PartitionDomainState::new();
    let mut clock = 0u64;

    let acquired = acquire(&mut state, &mut clock, "t", 0, 1, 10_000, 0);
    let original_epoch = match acquired {
        PartitionResponse::PartitionAcquired { leader_epoch, .. } => leader_epoch,
        other => panic!("expected PartitionAcquired, got {other:?}"),
    };

    let dead_clock = 10_000 + LEASE_TAKEOVER_GRACE_MS + 1;
    let takeover = acquire(&mut state, &mut clock, "t", 0, 2, 10_000, dead_clock);
    match takeover {
        PartitionResponse::PartitionAcquired {
            leader_epoch,
            lease_expires_at_ms,
            ..
        } => {
            assert!(
                leader_epoch > original_epoch,
                "epoch must bump on takeover: original={original_epoch}, new={leader_epoch}"
            );
            // New lease anchored to clamped clock, not to broker 2's
            // proposed timestamp (they happen to coincide here).
            assert_eq!(lease_expires_at_ms, dead_clock + 10_000);
        }
        other => panic!("expected PartitionAcquired on takeover, got {other:?}"),
    }
}

#[test]
fn original_owner_cannot_renew_a_lease_dead_beyond_grace() {
    // Broker 1's broker pauses (clock skew, GC pause, partition) past
    // `expiry + grace`. When it wakes up and tries to RENEW with what its
    // local wall clock thinks is "current", the renewal must be refused
    // — without this check the broker would resurrect a dead lease and
    // race with the legitimate successor.
    let mut state = PartitionDomainState::new();
    let mut clock = 0u64;

    let _ = acquire(&mut state, &mut clock, "t", 0, 1, 10_000, 0);

    // Broker 1 wakes up at T = 10_000 + grace + 100 ms; renew must fail.
    let stale_renew = renew(
        &mut state,
        &mut clock,
        "t",
        0,
        1,
        10_000,
        10_000 + LEASE_TAKEOVER_GRACE_MS + 100,
    );
    match stale_renew {
        PartitionResponse::PartitionNotOwned { .. } => {}
        other => panic!("expected PartitionNotOwned for renew of dead lease, got {other:?}"),
    }
}

#[test]
fn backward_skewed_renew_cannot_rewind_the_lease_clock() {
    // Broker 1 acquires at T=10s. It then submits a renew with a stale
    // `timestamp_ms = 5_000` (clock skew backwards by 5 s). The state
    // machine's clamp must keep `now = max(clock, ts) = 10_000`, so the
    // new expiry is anchored to the cluster clock, NOT the proposer's
    // stale timestamp. Without the clamp, a paused broker waking up with
    // a backward-skewed clock could shorten the lease and look unowned to
    // itself on the next read.
    let mut state = PartitionDomainState::new();
    let mut clock = 0u64;

    let _ = acquire(&mut state, &mut clock, "t", 0, 1, 60_000, 10_000);
    assert_eq!(clock, 10_000);

    // Backward-skewed renew at "T=5_000" (broker thinks time went
    // backwards by 5s). The clamp kicks in: `now` stays at 10_000.
    let r = renew(&mut state, &mut clock, "t", 0, 1, 60_000, 5_000);
    let new_expiry = match r {
        PartitionResponse::LeaseRenewed {
            lease_expires_at_ms,
            ..
        } => lease_expires_at_ms,
        other => panic!("expected LeaseRenewed, got {other:?}"),
    };
    // Lease expiry must be at least the clamped-clock + duration.
    assert_eq!(
        new_expiry, 70_000,
        "backward-skewed renew must anchor to clock, not proposer timestamp"
    );
    assert_eq!(clock, 10_000, "lease clock must not move backwards");
}

#[test]
fn backward_skewed_acquire_cannot_rewind_the_lease_clock() {
    // Same property for the AcquirePartition path. Broker 1 acquires at
    // T=10s; broker 2 (after broker 1 releases) submits Acquire with a
    // backward-skewed timestamp. The new lease still anchors at the
    // clamped clock.
    let mut state = PartitionDomainState::new();
    let mut clock = 0u64;

    let _ = acquire(&mut state, &mut clock, "t", 0, 1, 1_000, 10_000);
    assert_eq!(clock, 10_000);

    // Wait past expiry+grace by feeding a high timestamp via release+acquire.
    state.apply(
        PartitionCommand::ReleasePartition {
            topic: "t".to_string(),
            partition: 0,
            broker_id: 1,
        },
        &mut clock,
    );
    // Broker 2 with a stale timestamp.
    let r = acquire(&mut state, &mut clock, "t", 0, 2, 60_000, 5_000);
    match r {
        PartitionResponse::PartitionAcquired {
            lease_expires_at_ms,
            ..
        } => {
            // Must anchor to clamped clock (10_000), not stale 5_000.
            assert_eq!(
                lease_expires_at_ms, 70_000,
                "lease must anchor to clamped clock, not stale 5_000 timestamp"
            );
        }
        other => panic!("expected PartitionAcquired, got {other:?}"),
    }
    assert_eq!(clock, 10_000);
}

#[test]
fn forward_skewed_acquire_during_grace_is_rejected_and_takeover_succeeds_after() {
    // End-to-end clock-skew scenario: broker 2's clock runs fast.
    // - At skewed-T=40s (real T=40s), broker 2 attempts takeover of a
    //   lease that was granted at real-T=10s with 60s duration. Real
    //   expiry is 70s; the takeover must fail.
    // - Once the cluster clock crosses real expiry+grace, broker 2's
    //   takeover succeeds.
    let mut state = PartitionDomainState::new();
    let mut clock = 0u64;

    let _ = acquire(&mut state, &mut clock, "t", 0, 1, 60_000, 10_000);

    let denied = acquire(&mut state, &mut clock, "t", 0, 2, 60_000, 40_000);
    assert!(matches!(
        denied,
        PartitionResponse::PartitionOwnedByOther { .. }
    ));

    let after = 70_000 + LEASE_TAKEOVER_GRACE_MS + 1;
    let allowed = acquire(&mut state, &mut clock, "t", 0, 2, 60_000, after);
    assert!(matches!(
        allowed,
        PartitionResponse::PartitionAcquired { .. }
    ));
}
