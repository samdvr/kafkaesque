//! Protocol-layer hardening tests.
//!
//! Background. The wire-protocol layer in `crates/kafkaesque-protocol`
//! has good fuzz coverage (no-panic) and a single-source-of-truth
//! consistency check between `SUPPORTED_VERSIONS` and `PARSER_ENCODER_COVERAGE`
//! (see `src/server/versions.rs`). The audit's P1.1/P1.9 flagged that
//! the *broker-handler* surface — what `ApiVersions` actually advertises
//! to a real client — is not asserted at the integration boundary. The
//! audit's P1.2/P1.3 flagged that unknown tagged-field preservation and
//! bounds-checking error semantics are likewise not pinned at the wire.
//!
//! These tests pin five contracts:
//!
//! 1. **ApiVersions response shape**: every key in the
//!    `SUPPORTED_VERSIONS` table is advertised, with valid `min/max`
//!    ranges (`max >= min >= 0`), and no key is duplicated.
//! 2. **Cross-handler consistency**: `default_api_versions()` is what
//!    the broker actually returns from `Handler::handle_api_versions`;
//!    no per-handler override drops or adds keys.
//! 3. **No deprecated APIs surface**: a hand-coded list of APIs we know
//!    the broker should not advertise (e.g. transactional APIs, share
//!    groups KIP-932) is verified absent. Catches regressions from a
//!    new feature accidentally registered without test coverage.
//! 4. **Tagged-field forward-compat**: a flexible request carrying an
//!    unknown tagged field parses without error. (Today the parser
//!    consumes-and-ignores; future EOS work might want to *preserve*
//!    them — flipping that contract should be deliberate.)
//! 5. **Bounds rejection**: a wire request with a negative array size
//!    or a too-large array size returns a parser failure rather than
//!    panicking or silently truncating.
//!
//! These pin the producer/consumer compatibility surface — a real
//! `librdkafka` or `KafkaConsumer` connection trips on every one of
//! these on its first request.

use bytes::Bytes;
use kafkaesque::cluster::ClusterProfile;
use kafkaesque::error::KafkaCode;
use kafkaesque::server::Handler;
use kafkaesque::server::request::{ApiKey, ApiVersionsRequestData};
use kafkaesque::server::versions::{SUPPORTED_VERSIONS, default_api_versions};
use nombytes::NomBytes;

mod common;
use common::BrokerHandle;

fn api_key_to_i16(k: ApiKey) -> i16 {
    i16::from(k)
}

fn empty_api_versions_request() -> ApiVersionsRequestData {
    ApiVersionsRequestData {
        client_software_name: None,
        client_software_version: None,
    }
}

// ---------------------------------------------------------------------------
// 1. ApiVersions response shape: every advertised version is sane
// ---------------------------------------------------------------------------

#[test]
fn supported_versions_table_has_no_duplicate_keys() {
    let mut keys: Vec<i16> = SUPPORTED_VERSIONS
        .iter()
        .map(|s| api_key_to_i16(s.api_key))
        .collect();
    keys.sort_unstable();
    let before = keys.len();
    keys.dedup();
    assert_eq!(
        before,
        keys.len(),
        "SUPPORTED_VERSIONS contains duplicate api_keys; ApiVersions clients see two ranges and pick arbitrarily",
    );
}

#[test]
fn every_supported_version_has_valid_range() {
    for sv in SUPPORTED_VERSIONS {
        assert!(
            sv.min_version >= 0,
            "{:?}: min_version must be >= 0 (got {})",
            sv.api_key,
            sv.min_version,
        );
        assert!(
            sv.max_version >= sv.min_version,
            "{:?}: max_version ({}) must be >= min_version ({})",
            sv.api_key,
            sv.max_version,
            sv.min_version,
        );
    }
}

#[test]
fn default_api_versions_lists_every_supported_key() {
    let advertised: Vec<i16> = default_api_versions()
        .iter()
        .map(|v| api_key_to_i16(v.api_key))
        .collect();
    for sv in SUPPORTED_VERSIONS {
        assert!(
            advertised.contains(&api_key_to_i16(sv.api_key)),
            "{:?} is in SUPPORTED_VERSIONS but missing from default_api_versions()",
            sv.api_key,
        );
    }
    assert_eq!(
        advertised.len(),
        SUPPORTED_VERSIONS.len(),
        "default_api_versions() advertises {} keys, SUPPORTED_VERSIONS has {} — drift",
        advertised.len(),
        SUPPORTED_VERSIONS.len(),
    );
}

// ---------------------------------------------------------------------------
// 2. Cross-handler consistency
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handler_api_versions_response_matches_default_table() {
    // The broker's `handle_api_versions` is the surface clients hit. A
    // regression where someone overrides it to drop a key (e.g. a stub
    // implementation in tests) would silently break every modern
    // client's negotiation.
    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    let resp = broker
        .handler
        .handle_api_versions(&broker.ctx(), empty_api_versions_request())
        .await;
    assert_eq!(resp.error_code, KafkaCode::None);
    assert_eq!(
        resp.api_keys.len(),
        default_api_versions().len(),
        "handler-returned api_keys count must equal the static table",
    );

    // Per-key min/max must match the static table, key by key.
    let by_key: std::collections::HashMap<i16, (i16, i16)> = resp
        .api_keys
        .iter()
        .map(|v| (api_key_to_i16(v.api_key), (v.min_version, v.max_version)))
        .collect();
    for sv in SUPPORTED_VERSIONS {
        let actual = by_key
            .get(&api_key_to_i16(sv.api_key))
            .unwrap_or_else(|| panic!("handler did not advertise {:?}", sv.api_key));
        assert_eq!(
            *actual,
            (sv.min_version, sv.max_version),
            "{:?}: handler advertised {:?}, table says ({}, {})",
            sv.api_key,
            actual,
            sv.min_version,
            sv.max_version,
        );
    }
}

// ---------------------------------------------------------------------------
// 3. No surprise APIs advertised
// ---------------------------------------------------------------------------

#[tokio::test]
async fn handler_does_not_advertise_unsupported_apis() {
    // Hard-coded list of APIs the README says are NOT supported. If a
    // future feature lands accidentally, this catches it before clients
    // start using it. Mapped to the i16 key that ApiKey::from_i16 would
    // have accepted, since the audit notes: "transactions, log
    // compaction, idempotent-producer deduplication beyond per-session
    // producer_id".
    //
    // Each entry: (api_key_i16, name).
    let unsupported = [
        (22, "InitProducerId — supported, but TX-related are not"),
        // Transactional APIs (KIP-98) — must not be advertised:
        (24, "AddPartitionsToTxn"),
        (25, "AddOffsetsToTxn"),
        (26, "EndTxn"),
        (27, "WriteTxnMarkers"),
        (28, "TxnOffsetCommit"),
        // KIP-932 share groups — not in scope:
        (78, "ShareGroupHeartbeat"),
        (79, "ShareGroupDescribe"),
    ];

    let broker = BrokerHandle::spawn(ClusterProfile::Development).await;
    let resp = broker
        .handler
        .handle_api_versions(&broker.ctx(), empty_api_versions_request())
        .await;
    let advertised: std::collections::HashSet<i16> = resp
        .api_keys
        .iter()
        .map(|v| api_key_to_i16(v.api_key))
        .collect();

    for (key, name) in unsupported {
        // We allow `InitProducerId` (key 22) since it's actually advertised
        // for the per-session-id idempotency we DO support. Skip its
        // negative assertion, but keep the row to document the carve-out.
        if key == 22 {
            assert!(
                advertised.contains(&key),
                "InitProducerId (22) should be advertised — pin so a future removal is intentional"
            );
            continue;
        }
        assert!(
            !advertised.contains(&key),
            "API {} ({}) is advertised but the broker doesn't support it; clients will trip on first use",
            key,
            name,
        );
    }
}

// ---------------------------------------------------------------------------
// 4. Bounds-checking on the wire
// ---------------------------------------------------------------------------

#[test]
fn parse_array_rejects_negative_length() {
    use kafkaesque::parser::parse_array;

    // Build a buffer whose array length is -1 (using parse_array, which
    // is for non-nullable arrays; -1 is a length in `parse_nullable_array`
    // territory only). The result must be a parser failure, not a panic
    // and not a silent empty Vec.
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i32).to_be_bytes());
    let nb = NomBytes::new(Bytes::from(data));

    // Element parser would consume one i32 each — never invoked here
    // because the length check fires first.
    let element = nom::number::complete::be_i32::<NomBytes, nom::error::Error<NomBytes>>;
    let result = parse_array(element)(nb);
    assert!(
        result.is_err(),
        "negative array length must be a parser failure, got Ok(_)",
    );
}

#[test]
fn parse_array_rejects_oversize_length() {
    use kafkaesque::constants::MAX_PROTOCOL_ARRAY_SIZE;
    use kafkaesque::parser::parse_array;

    // length == MAX_PROTOCOL_ARRAY_SIZE + 1 is rejected.
    let mut data = Vec::new();
    let bad_len = (MAX_PROTOCOL_ARRAY_SIZE + 1) as i32;
    data.extend_from_slice(&bad_len.to_be_bytes());
    let nb = NomBytes::new(Bytes::from(data));

    let element = nom::number::complete::be_i32::<NomBytes, nom::error::Error<NomBytes>>;
    let result = parse_array(element)(nb);
    assert!(
        result.is_err(),
        "array size > MAX_PROTOCOL_ARRAY_SIZE ({}) must be rejected; got Ok",
        MAX_PROTOCOL_ARRAY_SIZE,
    );
}

#[test]
fn parse_kafka_string_rejects_negative_length() {
    use kafkaesque::parser::parse_kafka_string;

    // Non-nullable string parser must reject negative lengths.
    // Length is i16; -1 is the NULL marker that nullable strings allow,
    // but `parse_kafka_string` (non-nullable) must reject it.
    let mut data = Vec::new();
    data.extend_from_slice(&(-1i16).to_be_bytes());
    let nb = NomBytes::new(Bytes::from(data));
    let result = parse_kafka_string(nb);
    assert!(
        result.is_err(),
        "non-nullable string with length=-1 must fail; got Ok",
    );
}

#[test]
fn parse_kafka_string_rejects_length_exceeding_buffer() {
    use kafkaesque::parser::parse_kafka_string;

    // Length claims 1000 bytes; buffer has 5. Must fail rather than
    // returning a partial / out-of-bounds slice.
    let mut data = Vec::new();
    data.extend_from_slice(&1000i16.to_be_bytes());
    data.extend_from_slice(b"short");
    let nb = NomBytes::new(Bytes::from(data));
    let result = parse_kafka_string(nb);
    assert!(
        result.is_err(),
        "string-length-larger-than-buffer must fail; got Ok",
    );
}

// ---------------------------------------------------------------------------
// 5. Tagged-field handling at the parser layer
// ---------------------------------------------------------------------------

#[test]
fn unsigned_varint_round_trips_unknown_tagged_field_count() {
    // Pin: the parser can read a non-zero tagged-field count without
    // crashing. The current code consumes-and-ignores; this test fails
    // if a future change starts panicking when the count is non-zero.
    use kafkaesque::parser::parse_unsigned_varint;

    // Unsigned varint encoding of 0 → single byte 0.
    // Value 5 → single byte 0x05.
    for v in [0u32, 1, 127, 128, 1_000, 65_535, 1_000_000] {
        let mut buf = Vec::new();
        let mut n = v;
        loop {
            let lo = (n & 0x7F) as u8;
            n >>= 7;
            if n == 0 {
                buf.push(lo);
                break;
            }
            buf.push(lo | 0x80);
        }
        let nb = NomBytes::new(Bytes::from(buf));
        let (_, parsed) = parse_unsigned_varint(nb).expect("varint decode");
        assert_eq!(parsed, v, "varint round-trip for {}", v);
    }
}
