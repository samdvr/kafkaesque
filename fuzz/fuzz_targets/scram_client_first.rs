#![no_main]

//! P0-4: SCRAM-SHA-256 client-first message parser fuzzer.
//!
//! Drives [`handle_client_first`] with arbitrary attacker-supplied bytes.
//! The function MUST never panic; the worst outcome on malformed input is a
//! `ScramServerState::Failed`. SASL is the broker's auth boundary — a panic
//! here lets unauthenticated clients crash the broker.
//!
//! The credentials lookup closure returns `Some(derived)` for any username,
//! so the user-exists path is exercised; `None` is also reachable through
//! the dummy-creds branch in `handle_client_first` (anti-enumeration).

use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::scram::{handle_client_first, ScramCredentials};

const MAX_INPUT: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }

    // Half the runs hit "user found" (returns derived creds), half hit the
    // anti-enumeration "user missing" path (returns dummy creds inside the
    // function). The discriminator is the first byte's parity — keeps the
    // partition stable across libFuzzer's mutations so coverage compounds.
    let user_found = data.first().copied().unwrap_or(0) & 1 == 1;
    let lookup = |_username: &str| -> Option<ScramCredentials> {
        if user_found {
            Some(ScramCredentials::derive(b"fuzz-password", b"fuzz-salt-16-byt", 4096))
        } else {
            None
        }
    };

    let _ = handle_client_first(data, lookup);
});
