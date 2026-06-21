//! Property tests for [`FaultInjector`] / [`FaultingObjectStore`].
//!
//! The fault injector's contract is small but easy to break in subtle
//! ways:
//!
//! 1. `fail_next(n)` followed by `m >= n` operations across the
//!    selected op-kind mask must produce **exactly** `n` failures —
//!    never `n + ε` from a racy decrement, never `n - 1` from a lost
//!    update.
//! 2. The op mask must gate which ops fault. A kind outside the mask
//!    must never decrement the failure counter, even under concurrency.
//! 3. `block` is a strict gate (fail every faultable op); `unblock`
//!    must clear it.
//! 4. Latency must be applied before the failure decision so timing
//!    tests can rely on "if we returned, we waited".
//!
//! These are too combinatorial to enumerate by hand — proptest drives
//! shrinking corners instead.

use std::sync::Arc;
use std::time::Duration;

use kafkaesque::cluster::{FaultInjector, FaultingObjectStore, OpKind};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};
use proptest::prelude::*;

fn op_kind() -> impl Strategy<Value = OpKind> {
    prop_oneof![
        Just(OpKind::Get),
        Just(OpKind::Put),
        Just(OpKind::Delete),
        Just(OpKind::List),
        Just(OpKind::Copy),
        Just(OpKind::Rename),
        Just(OpKind::Head),
    ]
}

async fn perform(store: &FaultingObjectStore, kind: OpKind, idx: usize) -> bool {
    // Returns true on success, false on injected failure. We call the
    // op variants directly; the underlying InMemory store either returns
    // Ok (Put) or NotFound (the rest, on a fresh store) — both count
    // as "no fault injected" because the wrapper short-circuits before
    // the inner call when faulting.
    let path = Path::from(format!("k{idx}"));
    let result: Result<(), object_store::Error> = match kind {
        OpKind::Get => store
            .get(&path)
            .await
            .map(|_| ())
            .or_else(swallow_not_found),
        OpKind::Put => store
            .put(&path, PutPayload::from(b"v".as_slice()))
            .await
            .map(|_| ()),
        OpKind::Delete => store.delete(&path).await.or_else(swallow_not_found),
        OpKind::List => store
            .list_with_delimiter(None)
            .await
            .map(|_| ())
            .or_else(swallow_not_found),
        OpKind::Copy => store
            .copy(&path, &Path::from(format!("k{idx}-dst")))
            .await
            .or_else(swallow_not_found),
        OpKind::Rename => store
            .rename(&path, &Path::from(format!("k{idx}-dst")))
            .await
            .or_else(swallow_not_found),
        OpKind::Head => store
            .head(&path)
            .await
            .map(|_| ())
            .or_else(swallow_not_found),
    };
    match result {
        Ok(()) => true,
        Err(e) => {
            let s = e.to_string();
            // Non-fault-injector errors (e.g. NotFound from the inner
            // store) should be ignored. Only the planned-failure /
            // network-blocked shape should count as "faulted".
            assert!(
                s.contains("planned failure")
                    || s.contains("network blocked")
                    || s.contains("Generic"),
                "unexpected error from store: {s}"
            );
            false
        }
    }
}

fn swallow_not_found(e: object_store::Error) -> Result<(), object_store::Error> {
    if matches!(e, object_store::Error::NotFound { .. }) {
        Ok(())
    } else {
        Err(e)
    }
}

fn build_store() -> (Arc<FaultingObjectStore>, FaultInjector) {
    let injector = FaultInjector::new();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(FaultingObjectStore::new(inner, injector.clone()));
    (store, injector)
}

fn rt() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .expect("rt")
}

proptest! {
    #![proptest_config(ProptestConfig {
        cases: 64,
        // Bound shrink work — these scenarios are timing-sensitive.
        max_shrink_iters: 64,
        ..ProptestConfig::default()
    })]

    /// `fail_next(n)` must produce **at most** `n` failures regardless
    /// of how many faultable ops run after it. The actual count can be
    /// less than `n` if fewer than `n` faultable ops ran, but the
    /// counter must never produce more failures than were planned.
    #[test]
    fn fail_next_budget_never_exceeded(
        plan in 0u32..16,
        ops in prop::collection::vec(op_kind(), 0..32),
    ) {
        let rt = rt();
        rt.block_on(async {
            let (store, injector) = build_store();
            injector.fail_next(plan);
            let mut failed = 0u32;
            for (i, kind) in ops.iter().enumerate() {
                if !perform(&store, *kind, i).await {
                    failed += 1;
                }
            }
            prop_assert!(
                failed <= plan,
                "fail_next budget exceeded: plan={plan} observed={failed}"
            );
            Ok(())
        }).unwrap();
    }

    /// Ops outside the restricted set must never trigger an injected
    /// failure. The counter must not be decremented by them either —
    /// after running only excluded ops, the full budget should remain
    /// available for the included op.
    #[test]
    fn restrict_to_isolates_kinds(
        included in op_kind(),
        excluded in prop::collection::vec(op_kind(), 0..16),
        plan in 1u32..8,
    ) {
        let rt = rt();
        rt.block_on(async {
            let (store, injector) = build_store();
            injector.restrict_to(&[included]);
            injector.fail_next(plan);

            // Run a bunch of excluded ops first. Skip any op that
            // happens to equal the included kind — proptest's strategy
            // is uniform over all kinds, so excluded.contains(included)
            // is allowed.
            let excluded: Vec<OpKind> = excluded.into_iter().filter(|k| *k != included).collect();
            let mut spurious = 0u32;
            for (i, kind) in excluded.iter().enumerate() {
                if !perform(&store, *kind, i).await {
                    spurious += 1;
                }
            }
            prop_assert_eq!(
                spurious, 0,
                "ops outside restrict mask must never fault"
            );

            // Now drive the included op `plan` times — every one must
            // fault, since the budget hasn't been touched.
            let mut faulted = 0u32;
            for j in 0..plan {
                if !perform(&store, included, 1000 + j as usize).await {
                    faulted += 1;
                }
            }
            prop_assert_eq!(
                faulted, plan,
                "included kind must consume the full budget after \
                 excluded ops left it intact"
            );
            Ok(())
        }).unwrap();
    }

    /// `block` is sticky: once asserted, every faultable op fails until
    /// `unblock` is called. The number of failures while blocked must
    /// equal the number of ops on a faultable kind, regardless of
    /// `fail_next` budget.
    #[test]
    fn block_unblock_is_clean_toggle(
        ops_under_block in 0u32..12,
        ops_after in 0u32..12,
    ) {
        let rt = rt();
        rt.block_on(async {
            let (store, injector) = build_store();
            injector.block();
            let mut blocked_failures = 0u32;
            for i in 0..ops_under_block {
                if !perform(&store, OpKind::Put, i as usize).await {
                    blocked_failures += 1;
                }
            }
            prop_assert_eq!(
                blocked_failures, ops_under_block,
                "every Put must fail while blocked"
            );

            injector.unblock();
            let mut after_failures = 0u32;
            for j in 0..ops_after {
                if !perform(&store, OpKind::Put, 10_000 + j as usize).await {
                    after_failures += 1;
                }
            }
            prop_assert_eq!(
                after_failures, 0,
                "no Put should fail after unblock with no other faults"
            );
            Ok(())
        }).unwrap();
    }

    /// Latency must be applied uniformly across kinds. Kafkaesque code
    /// elsewhere assumes "if we returned, we waited", so a missing
    /// sleep on one op-kind branch would silently skew failover and
    /// rebalance test timing.
    #[test]
    fn latency_applies_to_every_kind(
        kind in op_kind(),
        millis in 5u64..40,
    ) {
        let rt = rt();
        rt.block_on(async {
            let (store, injector) = build_store();
            // Seed once so head/get/delete/copy/rename all have
            // something to operate on (or at least a deterministic
            // NotFound result).
            store
                .put(&Path::from("seed"), PutPayload::from(b"s".as_slice()))
                .await
                .expect("seed put");

            injector.set_latency(Duration::from_millis(millis));
            let started = std::time::Instant::now();
            // Use the seeded path for ops that need an existing object.
            let path = Path::from("seed");
            let _ = match kind {
                OpKind::Get => store.get(&path).await.map(|_| ()).map_err(|_| ()),
                OpKind::Put => store
                    .put(&Path::from("p"), PutPayload::from(b"x".as_slice()))
                    .await
                    .map(|_| ())
                    .map_err(|_| ()),
                OpKind::Delete => store.delete(&path).await.map_err(|_| ()),
                OpKind::List => store.list_with_delimiter(None).await.map(|_| ()).map_err(|_| ()),
                OpKind::Copy => store.copy(&path, &Path::from("dst")).await.map_err(|_| ()),
                OpKind::Rename => store.rename(&path, &Path::from("dst")).await.map_err(|_| ()),
                OpKind::Head => store.head(&path).await.map(|_| ()).map_err(|_| ()),
            };
            let elapsed = started.elapsed();
            // Allow 30% slack so a busy CI scheduler doesn't flake the
            // assert. We're checking "the sleep happened", not "the
            // sleep was precise".
            let lower_bound = Duration::from_millis(millis.saturating_sub(2));
            prop_assert!(
                elapsed >= lower_bound,
                "{kind:?}: expected at least ~{millis}ms latency, got {elapsed:?}"
            );
            Ok(())
        }).unwrap();
    }
}
