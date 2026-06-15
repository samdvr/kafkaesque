#![no_main]

use bytes::Bytes;
use kafkaesque::server::request::Request;
use libfuzzer_sys::fuzz_target;

fuzz_target!(|data: &[u8]| {
    let _ = Request::parse(Bytes::copy_from_slice(data));
});
