#![no_main]

//! Postcard decode fuzzer for the Raft state-machine snapshots.
//!
//! Snapshots are persisted via each state machine's `snapshot()` and
//! reloaded via `deserialize_state()` — the same decode path is also
//! reachable from the wire when a leader streams an `InstallSnapshot`
//! payload and the follower writes the bytes to disk before validating
//! them. A panic in the decoder is therefore remotely-triggerable: a
//! hostile leader (or a corrupted on-disk snapshot) could crash every
//! follower it streams to.
//!
//! `deserialize_state` documents the contract: it never panics — it returns
//! `io::Error(InvalidData)` on garbage. The legacy-fallback decode path is
//! the higher-risk surface; we exercise it implicitly because every input
//! that fails the current layout falls through to it.
//!
//! Both state machines are driven: the single `CoordinationStateMachine`
//! was split into a control-group and a per-shard machine, and each one
//! decodes snapshots streamed by its own group's leader, so both are in
//! the threat model. The same input is fed to both — a byte string is
//! whatever the receiving group decides to read it as.
//!
//! Property: the decoder either returns Ok(state) — in which case
//! re-encoding and re-decoding must reach a fixed point — or returns Err.
//! It never panics or aborts. The canonical-roundtrip assertion
//! (`first == second`) is the state-machine determinism contract: every
//! replica must encode the same state to the same bytes, otherwise replay
//! across a snapshot install diverges.

use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::raft::{ControlStateMachine, ShardStateMachine};

const MAX_INPUT: usize = 1024 * 1024;

/// Assert the encode/decode fixed point for one decoded snapshot state.
fn assert_canonical_roundtrip<T, F>(label: &str, decoded: &T, redecode: F)
where
    T: serde::Serialize,
    F: Fn(&[u8]) -> std::io::Result<T>,
{
    let first = match postcard::to_stdvec(decoded) {
        Ok(v) => v,
        Err(e) => panic!("decoded {label} failed to re-encode: {e:?}"),
    };
    let again = match redecode(&first) {
        Ok(v) => v,
        Err(e) => panic!("{label}: canonical snapshot bytes must decode: {e:?}"),
    };
    let second = match postcard::to_stdvec(&again) {
        Ok(v) => v,
        Err(e) => panic!("re-decoded {label} failed to re-encode: {e:?}"),
    };
    assert_eq!(
        first, second,
        "{label} snapshot postcard canonical roundtrip drift"
    );
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    if let Ok(decoded) = ControlStateMachine::deserialize_state(data) {
        assert_canonical_roundtrip(
            "ControlState",
            &decoded,
            ControlStateMachine::deserialize_state,
        );
    }
    if let Ok(decoded) = ShardStateMachine::deserialize_state(data) {
        assert_canonical_roundtrip("ShardState", &decoded, ShardStateMachine::deserialize_state);
    }
});
