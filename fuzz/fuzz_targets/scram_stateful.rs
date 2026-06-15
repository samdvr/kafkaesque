#![no_main]

//! Stateful SCRAM-SHA-256 fuzzer — drives `handle_client_first` and
//! `handle_client_final` back-to-back with attacker bytes for BOTH steps.
//!
//! The single-step targets (`scram_client_first`, `scram_client_final`)
//! exercise each parser in isolation against a clean upstream state. They
//! cannot reach the bug class where step 2 panics on inputs that survive
//! step 1's parser — the very class most likely to slip past CI, because
//! a stage-1 panic alone is loud while a stage-2 panic only triggers when
//! the prior message satisfied step 1's invariants.
//!
//! Property: regardless of what the attacker sends for either message, the
//! state machine never panics. The only legitimate terminal states are
//! `Authenticated` (everything checks out) or `Failed` (any reason).

use arbitrary::{Arbitrary, Unstructured};
use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::scram::{
    ScramCredentials, ScramServerState, handle_client_final, handle_client_first,
};

/// Cap each individual message at 64 KiB to keep iterations fast.
const MAX_PER_MESSAGE: usize = 64 * 1024;

#[derive(Arbitrary, Debug)]
struct StatefulInput<'a> {
    /// Whether the credentials lookup for `client_first` returns Some(creds)
    /// (the "user known" path) or None (the "user missing" anti-enumeration
    /// dummy-creds branch). Half the corpus exercises each.
    user_found: bool,
    /// Bytes for the client-first message.
    client_first: &'a [u8],
    /// Bytes for the client-final message that follows it.
    client_final: &'a [u8],
}

fuzz_target!(|data: &[u8]| {
    let mut u = Unstructured::new(data);
    let Ok(input) = StatefulInput::arbitrary(&mut u) else {
        return;
    };
    if input.client_first.len() > MAX_PER_MESSAGE || input.client_final.len() > MAX_PER_MESSAGE {
        return;
    }

    let lookup = |_username: &str| -> Option<ScramCredentials> {
        if input.user_found {
            Some(ScramCredentials::derive(
                b"fuzz-password",
                b"fuzz-salt-16-byt",
                4096,
            ))
        } else {
            None
        }
    };

    // Step 1: parse client-first. Output is either AwaitingClientFinal
    // (the parser accepted and we have a server-first to send) or Failed
    // (the parser rejected). Either way, no panic.
    let (state, _server_first_bytes) = handle_client_first(input.client_first, lookup);

    // Step 2: feed client-final into whatever state we ended up in. The
    // interesting case is `state == AwaitingClientFinal`, but we also
    // exercise feeding a final message into a `Failed` state — that
    // transition must also not panic (the production dispatcher relies on
    // this when rejecting malformed sequences without dropping the
    // connection).
    let (final_state, _server_final_bytes) = handle_client_final(input.client_final, state);

    // Sink the result so the optimizer can't elide the work; no real
    // assertions — libFuzzer treats any panic as a crash.
    match final_state {
        ScramServerState::Authenticated(_) | ScramServerState::Failed(_) => {}
        // The state machine is fully driven by step 2 — anything else here
        // would mean the API contract was violated.
        _ => panic!("unexpected non-terminal SCRAM state after client_final"),
    }
});
