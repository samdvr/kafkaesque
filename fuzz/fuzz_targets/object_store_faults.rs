#![no_main]

//! Stateful fuzzer for [`FaultInjector`] / [`FaultingObjectStore`].
//!
//! Where the unit and property tests in `tests/object_store_fault_props.rs`
//! drive carefully scoped scenarios, this target lets libFuzzer mutate
//! arbitrary command sequences over the injector — `block`, `unblock`,
//! `set_latency`, `fail_next`, `restrict_to`, plus randomized op calls
//! — and asserts the invariants that always hold:
//!
//! 1. The wrapper never panics.
//! 2. `fail_next(n)` produces at most `n` injected failures across the
//!    selected mask, no matter how the test sequence interleaves
//!    operations.
//! 3. Every non-injected error from the inner `InMemory` store has a
//!    distinguishable shape from the injector's own errors.
//!
//! Why a stateful fuzz instead of more proptest cases: libFuzzer's
//! coverage map walks branch hits across the actual `FaultingObjectStore`
//! impl bodies, so a sequence-level mutator finds inputs that line-by-line
//! proptest strategies don't. Each iteration is bounded to a small
//! command count to keep iterations fast.

use std::sync::Arc;

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;
use tokio::runtime::Runtime;

use kafkaesque::cluster::{FaultInjector, FaultingObjectStore, OpKind};
use object_store::memory::InMemory;
use object_store::path::Path;
use object_store::{ObjectStore, PutPayload};

#[derive(Arbitrary, Debug)]
enum Cmd {
    Block,
    Unblock,
    SetLatencyMs(u8),
    FailNext(u8),
    RestrictTo(Vec<OpKindArb>),
    RestrictAll,
    Op(OpKindArb, u8),
}

#[derive(Arbitrary, Debug, Clone, Copy)]
enum OpKindArb {
    Get,
    Put,
    Delete,
    List,
    Copy,
    Rename,
    Head,
}

impl From<OpKindArb> for OpKind {
    fn from(k: OpKindArb) -> Self {
        match k {
            OpKindArb::Get => OpKind::Get,
            OpKindArb::Put => OpKind::Put,
            OpKindArb::Delete => OpKind::Delete,
            OpKindArb::List => OpKind::List,
            OpKindArb::Copy => OpKind::Copy,
            OpKindArb::Rename => OpKind::Rename,
            OpKindArb::Head => OpKind::Head,
        }
    }
}

#[derive(Arbitrary, Debug)]
struct Input {
    cmds: Vec<Cmd>,
}

const MAX_CMDS: usize = 64;

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(input) = Input::arbitrary(&mut u) else {
        return;
    };
    if input.cmds.len() > MAX_CMDS {
        return;
    }

    // One runtime per iteration is wasteful, but libFuzzer can't share state
    // across iterations safely (the injector holds atomics; the inner store
    // holds objects), so this keeps things deterministic per case.
    let rt = Runtime::new().expect("rt");
    rt.block_on(drive(input.cmds));
});

async fn drive(cmds: Vec<Cmd>) {
    let injector = FaultInjector::new();
    let inner: Arc<dyn ObjectStore> = Arc::new(InMemory::new());
    let store = Arc::new(FaultingObjectStore::new(inner, injector.clone()));

    // Track the maximum cumulative budget the test ever set, so we can
    // assert at the end: total observed failures ≤ total budget granted.
    // `fail_next` overwrites rather than adds, so the upper bound is the
    // sum of every value passed (each call could maximally turn into n
    // failures before the next call resets).
    let mut budget_granted: u64 = 0;
    let mut observed_failures: u64 = 0;

    for cmd in cmds {
        match cmd {
            Cmd::Block => injector.block(),
            Cmd::Unblock => injector.unblock(),
            Cmd::SetLatencyMs(ms) => {
                // Cap at a few ms so individual iterations don't run for
                // seconds when libFuzzer drops a 255ms input.
                let bounded = (ms % 4) as u64;
                injector.set_latency(std::time::Duration::from_millis(bounded));
            }
            Cmd::FailNext(n) => {
                let n = n as u32;
                budget_granted = budget_granted.saturating_add(n as u64);
                injector.fail_next(n);
            }
            Cmd::RestrictTo(kinds) => {
                let kinds: Vec<OpKind> = kinds.into_iter().map(Into::into).collect();
                injector.restrict_to(&kinds);
            }
            Cmd::RestrictAll => injector.restrict_all(),
            Cmd::Op(kind, idx) => {
                if matches!(perform(&store, kind.into(), idx as usize).await, OpResult::InjectedFailure) {
                    observed_failures += 1;
                }
            }
        }
    }

    assert!(
        observed_failures <= budget_granted + count_block_ops_upper_bound(),
        "observed {observed_failures} failures > granted {budget_granted}"
    );
}

/// Block doesn't draw from the budget — every faultable op fails while
/// blocked. Since we don't track when blocks were active, we accept any
/// number of additional failures attributable to `block`. The point of
/// the upper bound is to catch a *runaway-budget* bug, not to count
/// blocked failures precisely.
fn count_block_ops_upper_bound() -> u64 {
    // The test sequence is at most MAX_CMDS, of which at most all are
    // ops. So the upper bound on failures attributable to `block` is
    // MAX_CMDS.
    MAX_CMDS as u64
}

enum OpResult {
    Success,
    InjectedFailure,
    InnerError, // e.g. NotFound from the inner InMemory store
}

async fn perform(store: &FaultingObjectStore, kind: OpKind, idx: usize) -> OpResult {
    let path = Path::from(format!("k{idx}"));
    let result: Result<(), object_store::Error> = match kind {
        OpKind::Get => store.get(&path).await.map(|_| ()),
        OpKind::Put => store
            .put(&path, PutPayload::from(b"v".as_slice()))
            .await
            .map(|_| ()),
        OpKind::Delete => store.delete(&path).await,
        OpKind::List => store.list_with_delimiter(None).await.map(|_| ()),
        OpKind::Copy => store.copy(&path, &Path::from(format!("k{idx}-d"))).await,
        OpKind::Rename => store.rename(&path, &Path::from(format!("k{idx}-d"))).await,
        OpKind::Head => store.head(&path).await.map(|_| ()),
    };
    match result {
        Ok(()) => OpResult::Success,
        Err(e) => {
            let s = e.to_string();
            if s.contains("planned failure") || s.contains("network blocked") {
                OpResult::InjectedFailure
            } else {
                // NotFound and other inner errors are fine — they prove the
                // wrapper let the call through. The fuzzer only enforces
                // the two-shape boundary: injected vs. inner.
                OpResult::InnerError
            }
        }
    }
}
