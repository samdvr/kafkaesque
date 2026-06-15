#![no_main]

//! Postcard decode fuzzer for [`CoordinationResponse`].
//!
//! Responses flow back over the same Raft channels and end up on disk in
//! some retry/replay paths. Same threat model as `CoordinationCommand`.

use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::raft::CoordinationResponse;

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    if let Ok(decoded) = postcard::from_bytes::<CoordinationResponse>(data) {
        let reencoded = match postcard::to_stdvec(&decoded) {
            Ok(v) => v,
            Err(e) => panic!(
                "decoded CoordinationResponse failed to re-encode: {e:?}; decoded={decoded:?}"
            ),
        };
        let again: CoordinationResponse =
            postcard::from_bytes(&reencoded).expect("canonical form must decode");
        assert_eq!(decoded, again, "postcard canonical roundtrip drift");
    }
});
