#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;

use kafkaesque::server::request::Request;

// Structured fuzz target: first 4 bytes select `(api_key, api_version)`,
// remainder is treated as the request body after a minimal header.
//
// This exercises the version gate and per-API parsers with arbitrary
// payloads for advertised version boundaries — higher yield than raw
// `Request::parse` on random bytes alone.
fuzz_target!(|data: &[u8]| {
    if data.len() < 4 {
        return;
    }

    let api_key = i16::from_be_bytes([data[0], data[1]]);
    let api_version = i16::from_be_bytes([data[2], data[3]]);
    let body = &data[4..];

    let mut frame = Vec::with_capacity(12 + body.len());
    frame.extend_from_slice(&api_key.to_be_bytes());
    frame.extend_from_slice(&api_version.to_be_bytes());
    frame.extend_from_slice(&0i32.to_be_bytes()); // correlation_id
    frame.extend_from_slice(&(-1i16).to_be_bytes()); // null client_id
    frame.extend_from_slice(body);

    let _ = Request::parse(Bytes::from(frame));
});
