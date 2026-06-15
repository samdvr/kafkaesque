#![no_main]

//! Postcard decode fuzzer for the Raft state-machine snapshot.
//!
//! Snapshots are persisted via `CoordinationStateMachine::snapshot()` and
//! reloaded via `CoordinationStateMachine::deserialize_state()` — the same
//! decode path is also reachable from the wire when a leader streams an
//! `InstallSnapshot` payload and the follower writes the bytes to disk
//! before validating them. A panic in the decoder is therefore
//! remotely-triggerable: a hostile leader (or a corrupted on-disk snapshot)
//! could crash every follower it streams to.
//!
//! `deserialize_state` documents the contract: it never panics — it returns
//! `io::Error(InvalidData)` on garbage. The legacy-fallback decode path is
//! the higher-risk surface; we exercise it implicitly because every input
//! that fails the current layout falls through to it.
//!
//! Property: the decoder either returns Ok(state) — in which case re-encoding
//! and re-decoding must reach a fixed point — or returns Err. It never
//! panics or aborts. The canonical-roundtrip assertion (`first == second`)
//! is the state-machine determinism contract: every replica must encode the
//! same state to the same bytes, otherwise replay across a snapshot install
//! diverges.

use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::raft::CoordinationStateMachine;

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    if let Ok(decoded) = CoordinationStateMachine::deserialize_state(data) {
        let first = match postcard::to_stdvec(&decoded) {
            Ok(v) => v,
            Err(e) => panic!("decoded CoordinationState failed to re-encode: {e:?}"),
        };
        let again = CoordinationStateMachine::deserialize_state(&first)
            .expect("canonical snapshot bytes must decode");
        let second = match postcard::to_stdvec(&again) {
            Ok(v) => v,
            Err(e) => panic!("re-decoded CoordinationState failed to re-encode: {e:?}"),
        };
        assert_eq!(first, second, "snapshot postcard canonical roundtrip drift");
    }
});
