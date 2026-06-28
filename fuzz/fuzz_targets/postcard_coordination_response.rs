#![no_main]

//! Postcard decode fuzzer for the Raft state-machine responses.
//!
//! The old unified `CoordinationResponse` has been split into the two
//! top-level Raft response enums — [`ControlResponse`] and
//! [`ShardResponse`]. Responses flow back over the same Raft channels and
//! end up on disk in some retry/replay paths. Same threat model as the
//! command decoders.

use std::fmt::Debug;

use libfuzzer_sys::fuzz_target;
use serde::Serialize;
use serde::de::DeserializeOwned;

use kafkaesque::cluster::raft::{ControlResponse, ShardResponse};

const MAX_INPUT: usize = 1024 * 1024;

fn roundtrip<T: Serialize + DeserializeOwned + PartialEq + Debug>(data: &[u8], label: &str) {
    if let Ok(decoded) = postcard::from_bytes::<T>(data) {
        let reencoded = match postcard::to_stdvec(&decoded) {
            Ok(v) => v,
            Err(e) => panic!("decoded {label} failed to re-encode: {e:?}; decoded={decoded:?}"),
        };
        let again: T = postcard::from_bytes(&reencoded).expect("canonical form must decode");
        assert_eq!(decoded, again, "postcard canonical roundtrip drift ({label})");
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    roundtrip::<ControlResponse>(data, "ControlResponse");
    roundtrip::<ShardResponse>(data, "ShardResponse");
});
