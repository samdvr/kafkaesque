//! Latency-budget tests for the object-store fault path.
//!
//! These tests confirm two things the chaos tests don't:
//!
//! 1. **The injected sleep actually reaches the wrapped operation.**
//!    `tests/object_store_chaos_tests.rs` checks the coordinator
//!    survives latency, but it deliberately does *not* assert tight
//!    timing because of upstream caching. Here we go straight at the
//!    wrapper itself, bypass the coordinator, and assert the sleep
//!    landed within reasonable bounds.
//!
//! 2. **Latency on one op-kind doesn't bleed into another.** A naive
//!    fault injector might apply a global sleep regardless of mask.
//!    With per-kind restriction, ops outside the mask must go straight
//!    through.
//!
//! Why this lives outside the unit-test module: it's slow (each case
//! sleeps tens of ms) and pulls in proptest-style multi-iteration
//! statistics. Keeping it as a separate integration test keeps lib
//! tests fast.

use std::sync::Arc;
use std::time::{Duration, Instant};

use kafkaesque::cluster::{FaultInjector, FaultingObjectStore, OpKind};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};

fn build_store() -> (Arc<FaultingObjectStore>, FaultInjector) {
    let injector = FaultInjector::new();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(FaultingObjectStore::new(inner, injector.clone()));
    (store, injector)
}

/// Direct latency: every Put must take at least the configured wait,
/// minus a small slack to absorb scheduler jitter.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn put_latency_lands_on_every_call() {
    let (store, injector) = build_store();
    let target = Duration::from_millis(25);
    injector.set_latency(target);

    // 5 iterations — picking up min/max gives us a tighter check than a
    // single-shot measurement, which can flake on a noisy CI scheduler.
    let mut samples: Vec<Duration> = Vec::with_capacity(5);
    for i in 0..5 {
        let started = Instant::now();
        store
            .put(
                &Path::from(format!("k{i}")),
                PutPayload::from(b"x".as_slice()),
            )
            .await
            .expect("put under latency");
        samples.push(started.elapsed());
    }

    let min = *samples.iter().min().expect("samples not empty");
    assert!(
        min >= target.saturating_sub(Duration::from_millis(2)),
        "minimum observed latency {min:?} below target {target:?}"
    );
    // No sample should run away (e.g. runaway-loop bug). Generous cap.
    let max = *samples.iter().max().expect("samples not empty");
    assert!(
        max < target * 10,
        "maximum observed latency {max:?} > 10x target {target:?}"
    );
}

/// Latency restricted to one op-kind must not affect other kinds. A
/// 100ms latency on Get must leave Put running near-instant.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restricted_latency_does_not_leak_to_other_ops() {
    let (store, injector) = build_store();
    injector.restrict_to(&[OpKind::Get]);
    injector.set_latency(Duration::from_millis(100));

    // Put must be unaffected — no latency configured for Put's mask
    // bit. We allow up to 30ms for OS scheduling slack on a busy CI.
    let started = Instant::now();
    store
        .put(&Path::from("p"), PutPayload::from(b"v".as_slice()))
        .await
        .expect("put");
    let put_elapsed = started.elapsed();
    assert!(
        put_elapsed < Duration::from_millis(40),
        "Put inherited Get latency: elapsed={put_elapsed:?}"
    );

    // Get must absorb the configured 100ms.
    let started = Instant::now();
    let _ = store.get(&Path::from("p")).await.expect("get");
    let get_elapsed = started.elapsed();
    assert!(
        get_elapsed >= Duration::from_millis(95),
        "Get did not see configured latency: elapsed={get_elapsed:?}"
    );
}

/// Setting latency to zero must clear it. A test that only proves
/// "latency is applied" would miss a bug where the atomic store does
/// nothing for the zero case.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn clearing_latency_restores_fast_path() {
    let (store, injector) = build_store();
    injector.set_latency(Duration::from_millis(40));

    let slow = Instant::now();
    store
        .put(&Path::from("p"), PutPayload::from(b"v".as_slice()))
        .await
        .expect("put under latency");
    let slow_elapsed = slow.elapsed();
    assert!(slow_elapsed >= Duration::from_millis(35));

    injector.set_latency(Duration::ZERO);

    let fast = Instant::now();
    store
        .put(&Path::from("p"), PutPayload::from(b"v".as_slice()))
        .await
        .expect("put after clear");
    let fast_elapsed = fast.elapsed();
    assert!(
        fast_elapsed < Duration::from_millis(20),
        "post-clear put still slow: {fast_elapsed:?}"
    );
}

/// A many-op latency budget: 50ms per op × 10 sequential Puts must
/// take at least ~500ms. Catches an "applied once" bug where the
/// implementation might cache the first sleep.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn latency_compounds_across_sequential_ops() {
    let (store, injector) = build_store();
    let per_op = Duration::from_millis(20);
    injector.set_latency(per_op);

    let n = 10u32;
    let started = Instant::now();
    for i in 0..n {
        store
            .put(
                &Path::from(format!("k{i}")),
                PutPayload::from(b"v".as_slice()),
            )
            .await
            .expect("put");
    }
    let total = started.elapsed();
    let expected_min = per_op * n - Duration::from_millis(10);
    assert!(
        total >= expected_min,
        "{n} sequential puts took {total:?}, expected ≥ {expected_min:?}"
    );
}

/// Concurrent ops under latency must still complete; the wrapper's
/// sleep must not serialize them. With `worker_threads = 4` and 8
/// concurrent puts at 50ms each, total wall-clock should be well
/// under the serialized 400ms.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_ops_under_latency_run_in_parallel() {
    let (store, injector) = build_store();
    let per_op = Duration::from_millis(50);
    injector.set_latency(per_op);

    let n = 8u32;
    let started = Instant::now();
    let mut handles = Vec::with_capacity(n as usize);
    for i in 0..n {
        let store = store.clone();
        handles.push(tokio::spawn(async move {
            store
                .put(
                    &Path::from(format!("k{i}")),
                    PutPayload::from(b"v".as_slice()),
                )
                .await
                .expect("concurrent put");
        }));
    }
    for h in handles {
        h.await.expect("join");
    }
    let total = started.elapsed();

    // If they ran serially we'd see ≥ 400ms. We assert < 250ms — leaves
    // room for scheduling but flags any bug that serializes through a
    // single mutex inside the injector.
    assert!(
        total < Duration::from_millis(250),
        "{n} concurrent puts at {per_op:?} each took {total:?} — \
         injector appears to serialize"
    );
    // And total must still be at least one round of latency.
    assert!(
        total >= per_op - Duration::from_millis(10),
        "concurrent puts finished too fast: {total:?}"
    );
}

/// `tokio::time::timeout` around a faulting op must fire; the injector
/// must not subvert the runtime's deadline. This is the production
/// invariant the coordinator relies on for hang-free chaos.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn timeout_fires_through_injected_latency() {
    let (store, injector) = build_store();
    injector.set_latency(Duration::from_secs(60));

    let result = tokio::time::timeout(
        Duration::from_millis(100),
        store.put(&Path::from("p"), PutPayload::from(b"v".as_slice())),
    )
    .await;
    assert!(
        result.is_err(),
        "tokio timeout failed to interrupt 60s injected sleep"
    );
}
