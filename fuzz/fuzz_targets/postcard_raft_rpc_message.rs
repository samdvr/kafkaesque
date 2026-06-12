#![no_main]

//! P0-5: Postcard decode fuzzer for [`RaftRpcMessage`].
//!
//! Raft RPC frames are HMAC-authenticated on the wire (see
//! `cluster::raft::auth`), but the postcard decode runs *before* HMAC
//! verification on the response path, and on the request side a stale
//! cached frame can reach the decoder if a peer is misbehaving. A panic
//! here is a remote DoS against the cluster control plane.
//!
//! Property: a successful decode must re-encode to a byte sequence that
//! itself decodes back. We assert byte-equality of the re-encoded form
//! after a second encode/decode cycle (postcard's canonical-form check
//! without relying on `PartialEq`, which is not derived for the openraft
//! payloads).

use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::raft::RaftRpcMessage;

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    if let Ok(decoded) = postcard::from_bytes::<RaftRpcMessage>(data) {
        let first = match postcard::to_stdvec(&decoded) {
            Ok(v) => v,
            Err(e) => panic!("decoded RaftRpcMessage failed to re-encode: {e:?}"),
        };
        let again = postcard::from_bytes::<RaftRpcMessage>(&first)
            .expect("canonical form must decode");
        let second = match postcard::to_stdvec(&again) {
            Ok(v) => v,
            Err(e) => panic!("re-decoded RaftRpcMessage failed to re-encode: {e:?}"),
        };
        // Encoding is deterministic, so the second pass must match the first.
        assert_eq!(first, second, "postcard canonical roundtrip drift");
    }
});
