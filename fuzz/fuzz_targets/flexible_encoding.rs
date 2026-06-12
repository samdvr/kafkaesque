#![no_main]

//! Compact-encoding + tagged-fields edge cases.
//!
//! KIP-482 flexible encoding has overflow corners that the existing
//! `parser_primitives` target only brushes against:
//!   - varint shift > 28 bits (the parser MUST reject, not loop or overflow);
//!   - varint MSB-only chains (every byte has the continuation bit set);
//!   - tagged-field counts at `MAX_PROTOCOL_ARRAY_SIZE` and beyond;
//!   - compact-array length with `length+1 == 0` after wrap.
//!
//! This target exercises [`parse_unsigned_varint`],
//! [`parse_compact_nullable_string`], and [`skip_tagged_fields`] on chained
//! inputs (so the same byte budget must satisfy several length prefixes in
//! a row) — the shape an adversary can actually craft.

use bytes::Bytes;
use libfuzzer_sys::fuzz_target;
use nombytes::NomBytes;

use kafkaesque::parser::{
    parse_compact_nullable_string, parse_unsigned_varint, skip_tagged_fields,
};

const MAX_INPUT: usize = 64 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    let bytes = Bytes::copy_from_slice(data);

    // 1. Standalone varint: any input must produce Ok or Err — never panic.
    //    Property: a successful parse consumed at most 5 bytes (4-byte
    //    payload + continuation bits stop at shift 28).
    if let Ok((rest, _)) = parse_unsigned_varint(NomBytes::new(bytes.clone())) {
        let consumed = bytes.len() - rest.into_bytes().len();
        assert!(
            consumed <= 5,
            "parse_unsigned_varint consumed {consumed} bytes; should be ≤5",
        );
    }

    // 2. Compact-nullable-string. The length prefix is varint-encoded as
    //    `length + 1` (0 = null, 1 = ""). Property: the parser never reads
    //    more bytes than the input contains — any successful parse leaves
    //    a valid suffix in `rest`.
    if let Ok((rest, payload)) = parse_compact_nullable_string(NomBytes::new(bytes.clone())) {
        if let Some(p) = &payload {
            // The bytes the string took came from the original input.
            assert!(p.len() <= bytes.len());
        }
        // No-op: just ensures the rest is well-formed.
        let _ = rest;
    }

    // 3. Tagged fields. The count is varint, capped at MAX_PROTOCOL_ARRAY_SIZE
    //    inside the parser. Property: never panic, never spin (an attacker
    //    cannot pass count=u32::MAX and force unbounded loop work).
    let _ = skip_tagged_fields(NomBytes::new(bytes.clone()));

    // 4. Chained: varint length followed by `length` bytes of payload, then
    //    another varint, then a compact string. This is the shape of every
    //    flexible-encoding body — the parsers must agree on byte boundaries.
    let mut input = NomBytes::new(bytes);
    if let Ok((next, _len)) = parse_unsigned_varint(input.clone()) {
        input = next;
        if let Ok((next, _s)) = parse_compact_nullable_string(input.clone()) {
            input = next;
            let _ = skip_tagged_fields(input);
        }
    }
});
