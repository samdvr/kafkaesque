#![no_main]

use libfuzzer_sys::fuzz_target;

use kafkaesque::protocol::parse_record_count_checked;

fuzz_target!(|data: &[u8]| {
    let _ = parse_record_count_checked(data);
});
