#![no_main]

//! Postcard decode fuzzer for [`RaftRpcResponse`].
//!
//! Mirror of the `RaftRpcMessage` target; same threat model. See that
//! target's module docs for context.

use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::raft::RaftRpcResponse;

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    if let Ok(decoded) = postcard::from_bytes::<RaftRpcResponse>(data) {
        let first = match postcard::to_stdvec(&decoded) {
            Ok(v) => v,
            Err(e) => panic!("decoded RaftRpcResponse failed to re-encode: {e:?}"),
        };
        let again = postcard::from_bytes::<RaftRpcResponse>(&first)
            .expect("canonical form must decode");
        let second = match postcard::to_stdvec(&again) {
            Ok(v) => v,
            Err(e) => panic!("re-decoded RaftRpcResponse failed to re-encode: {e:?}"),
        };
        assert_eq!(first, second, "postcard canonical roundtrip drift");
    }
});
