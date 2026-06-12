#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use nombytes::NomBytes;

use kafkaesque::server::request::parse_list_offsets_request;
use kafkaesque::server::request::ApiKey;
use kafkaesque::server::versions::find_version;

const MAX_INPUT: usize = 1024 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() < 2 || data.len() > MAX_INPUT {
        return;
    }
    let v = find_version(ApiKey::ListOffsets).expect("ListOffsets is advertised");
    let span = (v.max_version - v.min_version + 1).max(1) as i16;
    let raw_v = i16::from_be_bytes([data[0], data[1]]);
    let version = v.min_version + raw_v.rem_euclid(span);
    let body = Bytes::copy_from_slice(&data[2..]);
    let _ = parse_list_offsets_request(NomBytes::new(body), version);
});
