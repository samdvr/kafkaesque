#![no_main]

//! P0-4: SCRAM-SHA-256 client-final message parser fuzzer.
//!
//! Seeds the SCRAM state machine with a valid `AwaitingClientFinal` produced
//! by [`handle_client_first`], then feeds arbitrary bytes to
//! [`handle_client_final`]. Property: never panics; verification failure
//! returns `Failed(_)`, never crashes.

use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::scram::{
    handle_client_final, handle_client_first, ScramCredentials, ScramServerState,
};

const MAX_INPUT: usize = 64 * 1024;

/// Build a valid `AwaitingClientFinal` state once and clone it for each
/// fuzz iteration. The state is created by running `handle_client_first`
/// against a well-formed client-first message — exactly the input shape the
/// production code expects upstream of `handle_client_final`.
fn seed_state() -> ScramServerState {
    let creds = ScramCredentials::derive(b"fuzz-password", b"fuzz-salt-16-byt", 4096);
    let lookup = |_user: &str| Some(creds.clone());
    // n,, gs2 header + bare: n=fuzzuser,r=clientnonce
    let client_first = b"n,,n=fuzzuser,r=clientnonce-16ch";
    let (state, _server_first) = handle_client_first(client_first, lookup);
    state
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }

    // `handle_client_final` consumes `state` by value, so we can't share a
    // single seed across iterations. Building a fresh one per call costs a
    // PBKDF2 round (4096 iters), which is OK for the iteration rate
    // libFuzzer hits on a parser-level target.
    let state = seed_state();
    let _ = handle_client_final(data, state);

    // Also drive the "wrong-state" branch: feeding a final message when the
    // server hasn't seen a first message must return Failed, not panic.
    let dummy_creds = ScramCredentials::derive(b"x", b"x", 4096);
    let wrong = ScramServerState::Authenticated("User:x".to_string());
    let _ = handle_client_final(data, wrong);
    drop(dummy_creds);
});
