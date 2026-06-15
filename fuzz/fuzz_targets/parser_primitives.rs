#![no_main]

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use nombytes::NomBytes;

use kafkaesque::parser::{
    parse_array, parse_compact_nullable_string, parse_kafka_string, parse_kafka_string_opt,
    parse_nullable_string, parse_string, parse_unsigned_varint, skip_tagged_fields,
};

// Cap input length so libFuzzer doesn't waste cycles on payloads bigger than
// any real Kafka frame body. The 64 KiB ceiling is well above the largest
// header / array we'd parse here.
const MAX_INPUT: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    let bytes = Bytes::copy_from_slice(data);

    // Each parser must return cleanly (Ok or Err) on any input — no panics,
    // no unbounded allocations. We feed the same bytes to every primitive so
    // libFuzzer's coverage signal compounds across them.
    let _ = parse_string(NomBytes::new(bytes.clone()));
    let _ = parse_nullable_string(NomBytes::new(bytes.clone()));
    let _ = parse_kafka_string(NomBytes::new(bytes.clone()));
    let _ = parse_kafka_string_opt(NomBytes::new(bytes.clone()));
    let _ = parse_unsigned_varint(NomBytes::new(bytes.clone()));
    let _ = parse_compact_nullable_string(NomBytes::new(bytes.clone()));
    let _ = skip_tagged_fields(NomBytes::new(bytes.clone()));
    let _ = parse_array(parse_string)(NomBytes::new(bytes));
});
