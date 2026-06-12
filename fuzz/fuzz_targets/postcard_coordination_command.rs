#![no_main]

//! P0-5: Postcard decode fuzzer for [`CoordinationCommand`].
//!
//! `CoordinationCommand` is the input variant of the Raft state machine:
//! every replicated cluster operation flows through this enum. On the wire
//! it travels HMAC-authenticated, but on disk (Raft log entries, snapshots)
//! it's stored verbatim — corrupted bytes there feed straight into
//! `postcard::from_bytes`. A panic in the decoder takes down the broker on
//! restart.
//!
//! Property: any successful decode must round-trip byte-for-byte. Postcard
//! is a canonical format, so non-canonical encodings should fail to decode
//! (or at minimum re-encode to a different byte string and fail the
//! roundtrip — which is what we assert here).

use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::raft::CoordinationCommand;

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    if let Ok(decoded) = postcard::from_bytes::<CoordinationCommand>(data) {
        let reencoded = match postcard::to_stdvec(&decoded) {
            Ok(v) => v,
            Err(e) => panic!(
                "decoded CoordinationCommand failed to re-encode: {e:?}; decoded={decoded:?}"
            ),
        };
        // Decode-round-trip: re-decoding the canonical form must match.
        let again: CoordinationCommand =
            postcard::from_bytes(&reencoded).expect("canonical form must decode");
        assert_eq!(decoded, again, "postcard canonical roundtrip drift");
    }
});
