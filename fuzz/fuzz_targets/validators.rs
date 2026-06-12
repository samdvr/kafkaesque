#![no_main]

//! Identifier-validator fuzzers.
//!
//! `validate_topic_name` and `validate_group_id` flow into filesystem paths
//! under SlateDB. Any string the validator accepts MUST satisfy the documented
//! rules — anything else is a path-traversal vector. We fuzz with arbitrary
//! UTF-8 (libFuzzer mutates raw bytes; we lossily decode) and assert every
//! `Ok` answer matches the spec.

use libfuzzer_sys::fuzz_target;

use kafkaesque::cluster::{validate_group_id, validate_topic_name};

// Mirrors `cluster::validation::MAX_TOPIC_NAME_LENGTH` / `MAX_GROUP_ID_LENGTH`.
// They aren't re-exported; the values are part of the public contract
// (documented in module-level rustdoc) and a change here would also be a
// behavior change worth catching in this property.
const MAX_TOPIC_NAME_LENGTH: usize = 249;
const MAX_GROUP_ID_LENGTH: usize = 255;

const MAX_INPUT: usize = 4 * 1024;

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }

    // Decode lossily so libFuzzer's byte-level mutations still produce
    // valid `&str` inputs to the validators (which take `&str`, not bytes).
    let s = String::from_utf8_lossy(data);

    if validate_topic_name(&s).is_ok() {
        assert_post_conditions(&s, MAX_TOPIC_NAME_LENGTH);
    }
    if validate_group_id(&s).is_ok() {
        assert_post_conditions(&s, MAX_GROUP_ID_LENGTH);
    }
});

fn assert_post_conditions(s: &str, max_len: usize) {
    // Length cap.
    assert!(
        !s.is_empty() && s.len() <= max_len,
        "validator accepted out-of-range length: {}",
        s.len()
    );

    // Reserved filesystem names.
    assert!(s != "." && s != "..", "validator accepted reserved name");

    // Must not start with a hyphen (CLI-flag confusion).
    assert!(
        !s.starts_with('-'),
        "validator accepted leading-hyphen identifier"
    );

    // Documented charset: ASCII alphanumeric, '.', '_', '-'.
    for c in s.chars() {
        assert!(
            c.is_ascii_alphanumeric() || c == '.' || c == '_' || c == '-',
            "validator accepted disallowed char: {:?}",
            c
        );
        assert!(!c.is_ascii_control(), "validator accepted control char");
    }
}
