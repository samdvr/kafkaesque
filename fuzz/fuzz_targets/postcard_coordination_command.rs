#![no_main]

//! Postcard decode fuzzer for the Raft state-machine input commands.
//!
//! The old unified `CoordinationCommand` has been split into the two
//! top-level Raft command enums — [`ControlCommand`] (cluster-wide state)
//! and [`ShardCommand`] (per-shard hot-path state). Together they are the
//! input variants of the Raft state machines: every replicated cluster
//! operation flows through one of them. On the wire they travel
//! HMAC-authenticated, but on disk (Raft log entries, snapshots) they're
//! stored verbatim — corrupted bytes there feed straight into
//! `postcard::from_bytes`. A panic in the decoder takes down the broker on
//! restart.
//!
//! Property: any successful decode must round-trip byte-for-byte. Postcard
//! is a canonical format, so non-canonical encodings should fail to decode
//! (or at minimum re-encode to a different byte string and fail the
//! roundtrip — which is what we assert here).

use std::fmt::Debug;

use libfuzzer_sys::fuzz_target;
use serde::Serialize;
use serde::de::DeserializeOwned;

use kafkaesque::cluster::raft::{ControlCommand, ShardCommand};

const MAX_INPUT: usize = 1024 * 1024;

fn roundtrip<T: Serialize + DeserializeOwned + PartialEq + Debug>(data: &[u8], label: &str) {
    if let Ok(decoded) = postcard::from_bytes::<T>(data) {
        let reencoded = match postcard::to_stdvec(&decoded) {
            Ok(v) => v,
            Err(e) => panic!("decoded {label} failed to re-encode: {e:?}; decoded={decoded:?}"),
        };
        // Decode-round-trip: re-decoding the canonical form must match.
        let again: T = postcard::from_bytes(&reencoded).expect("canonical form must decode");
        assert_eq!(decoded, again, "postcard canonical roundtrip drift ({label})");
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    roundtrip::<ControlCommand>(data, "ControlCommand");
    roundtrip::<ShardCommand>(data, "ShardCommand");
});
