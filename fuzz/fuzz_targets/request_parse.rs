#![no_main]

use libfuzzer_sys::fuzz_target;
use bytes::Bytes;
use kafkaesque::server::request::Request;

fuzz_target!(|data: &[u8]| {
    let _ = Request::parse(Bytes::copy_from_slice(data));
});
