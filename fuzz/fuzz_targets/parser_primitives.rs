#![no_main]

use libfuzzer_sys::fuzz_target;
use nombytes::NomBytes;

use kafkaesque::parser::{parse_array, parse_nullable_string, parse_string, parse_unsigned_varint};

fuzz_target!(|data: &[u8]| {
    let input = NomBytes::new(data);
    let _ = parse_string(input);

    let input = NomBytes::new(data);
    let _ = parse_nullable_string(input);

    let input = NomBytes::new(data);
    let _ = parse_unsigned_varint(input);

    let input = NomBytes::new(data);
    let _ = parse_array(parse_string)(input);
});
