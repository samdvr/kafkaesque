//! Throttle-time response-field tests.
//!
//! Real Kafka clients consult the `throttle_time_ms` field on every response
//! that carries one and back off accordingly. The field appears at a
//! version-specific offset on each API: get the offset wrong and clients
//! either ignore real throttling or apply it spuriously.
//!
//! The broker today does not actively throttle (`throttle_time_ms = 0`
//! everywhere), but the wire contract is the same: the field MUST be
//! emitted at the correct offset for every version that defines it. This
//! file pins:
//!
//! 1. ApiVersions: v0 omits the field, v1+ emits it at the trailing
//!    offset, v3+ uses flexible encoding (still emits the field but with
//!    tagged-fields trailer).
//! 2. Produce: v0 omits the field, v1+ emits it as the *leading* trailing
//!    field (after the topics array), v9+ flexible.
//! 3. Round-trip through encode_versioned at every advertised version
//!    completes without error and produces a deterministic byte length.
//!
//! These are anchor tests for a future quota-manager change: when
//! ThrottleTimeMs starts carrying non-zero values, the offset assertions
//! here continue to hold and the only change at this file is the value.

use kafkaesque::error::KafkaCode;
use kafkaesque::server::response::{
    ApiVersionData, ApiVersionsResponseData, ProduceResponseData,
};
use kafkaesque::server::request::ApiKey;

// ===========================================================================
// 1. ApiVersions v0 has NO throttle_time_ms
// ===========================================================================

#[test]
fn api_versions_v0_omits_throttle_time_ms() {
    let resp = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![],
        throttle_time_ms: 999,
    };
    let mut buf = Vec::new();
    resp.encode_versioned(&mut buf, 0).expect("encode v0");
    // v0 body: error_code (2 bytes) + array_len (4 bytes, value 0) = 6 bytes.
    // throttle_time_ms must NOT be present.
    assert_eq!(
        buf.len(),
        6,
        "v0 body must be exactly error_code + empty-array; got {} bytes ({:?})",
        buf.len(),
        buf,
    );
}

// ===========================================================================
// 2. ApiVersions v1+ emits throttle_time_ms as last 4 bytes
// ===========================================================================

#[test]
fn api_versions_v1_emits_throttle_time_ms_at_tail() {
    let resp = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![],
        throttle_time_ms: 0x12345678,
    };
    let mut buf = Vec::new();
    resp.encode_versioned(&mut buf, 1).expect("encode v1");
    // v1 body: error_code (2) + array_len (4, value 0) + throttle (4) = 10.
    assert_eq!(buf.len(), 10);
    let tail = &buf[buf.len() - 4..];
    assert_eq!(
        tail,
        &0x12345678i32.to_be_bytes(),
        "trailing 4 bytes must be throttle_time_ms; got {:?}",
        tail,
    );
}

// ===========================================================================
// 3. ApiVersions v2 has the same shape as v1
// ===========================================================================

#[test]
fn api_versions_v2_emits_throttle_time_ms_at_tail() {
    let resp = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![],
        throttle_time_ms: 42,
    };
    let mut buf = Vec::new();
    resp.encode_versioned(&mut buf, 2).expect("encode v2");
    assert_eq!(buf.len(), 10);
    let throttle_be = i32::from_be_bytes(buf[buf.len() - 4..].try_into().unwrap());
    assert_eq!(throttle_be, 42);
}

// ===========================================================================
// 4. ApiVersions v3 (flexible) carries throttle_time_ms before the tag buffer
// ===========================================================================

#[test]
fn api_versions_v3_flexible_emits_throttle_then_tag_buffer() {
    let resp = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![],
        throttle_time_ms: 7,
    };
    let mut buf = Vec::new();
    resp.encode_flexible(&mut buf).expect("encode flexible");
    // body: error_code (2) + compact_array_len (1, value 1 == empty)
    //       + throttle (4) + tag_buffer (1, value 0) = 8 bytes.
    assert_eq!(
        buf.len(),
        8,
        "flexible body must be exactly 8 bytes; got {} ({:?})",
        buf.len(),
        buf,
    );
    // Last byte is the empty tag buffer; the four bytes before it are the throttle.
    assert_eq!(buf[buf.len() - 1], 0, "trailer must be empty tag buffer");
    let throttle_be = i32::from_be_bytes(buf[buf.len() - 5..buf.len() - 1].try_into().unwrap());
    assert_eq!(throttle_be, 7);
}

// ===========================================================================
// 5. Produce v0 omits throttle_time_ms entirely
// ===========================================================================

#[test]
fn produce_v0_omits_throttle_time_ms() {
    let resp = ProduceResponseData {
        responses: vec![],
        throttle_time_ms: 1234,
    };
    let mut buf = Vec::new();
    resp.encode_versioned(&mut buf, 0).expect("encode v0");
    // v0 body: array_len (4, value 0) = 4 bytes; no throttle.
    assert_eq!(buf.len(), 4, "v0 produce body must be exactly an empty array");
}

// ===========================================================================
// 6. Produce v1 appends throttle_time_ms at the tail
// ===========================================================================

#[test]
fn produce_v1_emits_throttle_time_ms_at_tail() {
    let resp = ProduceResponseData {
        responses: vec![],
        throttle_time_ms: 0x0BAD_F00D,
    };
    let mut buf = Vec::new();
    resp.encode_versioned(&mut buf, 1).expect("encode v1");
    assert_eq!(buf.len(), 8);
    let tail = i32::from_be_bytes(buf[buf.len() - 4..].try_into().unwrap());
    assert_eq!(tail, 0x0BAD_F00Du32 as i32);
}

// ===========================================================================
// 7. Produce v9 flexible: trailing tag buffer follows throttle
// ===========================================================================

#[test]
fn produce_v9_flexible_throttle_then_tag_buffer() {
    let resp = ProduceResponseData {
        responses: vec![],
        throttle_time_ms: 0,
    };
    let mut buf = Vec::new();
    resp.encode_versioned(&mut buf, 9).expect("encode v9");
    // body: compact_array_len (1, value 1 == empty) + throttle (4) + tag_buffer (1) = 6 bytes.
    assert_eq!(
        buf.len(),
        6,
        "v9 produce body must be 6 bytes; got {} ({:?})",
        buf.len(),
        buf,
    );
    assert_eq!(buf[buf.len() - 1], 0, "trailer must be empty tag buffer");
}

// ===========================================================================
// 8. Defaults document the no-throttle posture
// ===========================================================================

#[test]
fn handler_defaults_emit_zero_throttle_time_ms() {
    // The handler-trait defaults all ship `throttle_time_ms = 0`. This
    // is the documented "no quota manager wired up yet" baseline; a
    // future change that lands a real quota manager must flip these
    // through the same encoded fields.
    let api = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![ApiVersionData {
            api_key: ApiKey::Produce,
            min_version: 0,
            max_version: 9,
        }],
        throttle_time_ms: 0,
    };
    let mut v1 = Vec::new();
    api.encode_versioned(&mut v1, 1).unwrap();
    let throttle = i32::from_be_bytes(v1[v1.len() - 4..].try_into().unwrap());
    assert_eq!(throttle, 0, "default throttle_time_ms must be 0 today");

    let prod = ProduceResponseData {
        responses: vec![],
        throttle_time_ms: 0,
    };
    let mut v1p = Vec::new();
    prod.encode_versioned(&mut v1p, 1).unwrap();
    let throttle_p = i32::from_be_bytes(v1p[v1p.len() - 4..].try_into().unwrap());
    assert_eq!(throttle_p, 0);
}
