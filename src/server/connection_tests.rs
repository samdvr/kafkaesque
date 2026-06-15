//! Inline tests previously embedded in `connection.rs`. Moved to a sibling
//! file so the implementation isn't gated on scrolling past the test
//! block. `super::*` re-exports private helpers the tests rely on.

#![allow(clippy::module_inception)]
#![allow(unused_imports)]

use super::*;

use super::*;
use crate::error::KafkaCode;
use crate::server::request::ApiKey;
use crate::server::response::{ApiVersionData, ApiVersionsResponseData};

#[test]
fn test_encode_api_versions_standard_v0() {
    // Test v0 encoding: no throttle_time_ms, standard header (no tagged fields)
    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![ApiVersionData {
            api_key: ApiKey::Produce,
            min_version: 0,
            max_version: 9,
        }],
        throttle_time_ms: 100, // Should be ignored in v0
    };

    let result = encode_api_versions_standard(42, 0, &response).unwrap();

    // Wire format:
    // size (4) + correlation_id (4) + error_code (2) + array_len (4) + 1 item (6)
    // = 4 + 4 + 2 + 4 + 6 = 20 bytes total
    // Message size = 16 (excludes size prefix)
    assert_eq!(result.len(), 20);

    // Size prefix = 16 (0x00000010)
    assert_eq!(&result[0..4], &[0x00, 0x00, 0x00, 0x10]);

    // correlation_id = 42 (0x0000002A)
    assert_eq!(&result[4..8], &[0x00, 0x00, 0x00, 0x2A]);

    // error_code = 0
    assert_eq!(&result[8..10], &[0x00, 0x00]);

    // array_length = 1
    assert_eq!(&result[10..14], &[0x00, 0x00, 0x00, 0x01]);

    // Verify NO throttle_time_ms at end (message ends at byte 20)
}

#[test]
fn test_encode_api_versions_standard_v1() {
    // Test v1 encoding: includes throttle_time_ms
    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![ApiVersionData {
            api_key: ApiKey::Fetch,
            min_version: 0,
            max_version: 11,
        }],
        throttle_time_ms: 500,
    };

    let result = encode_api_versions_standard(100, 1, &response).unwrap();

    // Wire format:
    // size (4) + correlation_id (4) + error_code (2) + array_len (4) + 1 item (6) + throttle_time_ms (4)
    // = 4 + 4 + 2 + 4 + 6 + 4 = 24 bytes total
    // Message size = 20 (excludes size prefix)
    assert_eq!(result.len(), 24);

    // Size prefix = 20 (0x00000014)
    assert_eq!(&result[0..4], &[0x00, 0x00, 0x00, 0x14]);

    // correlation_id = 100 (0x00000064)
    assert_eq!(&result[4..8], &[0x00, 0x00, 0x00, 0x64]);

    // throttle_time_ms = 500 (0x000001F4) at end
    assert_eq!(&result[20..24], &[0x00, 0x00, 0x01, 0xF4]);
}

#[test]
fn test_encode_api_versions_standard_v2() {
    // Test v2 encoding: same as v1
    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![],
        throttle_time_ms: 0,
    };

    let result_v1 = encode_api_versions_standard(1, 1, &response).unwrap();
    let result_v2 = encode_api_versions_standard(1, 2, &response).unwrap();

    // v1 and v2 should be identical
    assert_eq!(result_v1, result_v2);
}

#[test]
fn test_encode_api_versions_flexible_v3() {
    // Test v3 flexible encoding with OLD header format per KIP-511
    // ApiVersions is special - even v3 uses old header format for compatibility
    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![ApiVersionData {
            api_key: ApiKey::Produce,
            min_version: 0,
            max_version: 9,
        }],
        throttle_time_ms: 0,
    };

    let result = encode_api_versions_flexible(123, &response).unwrap();

    // Wire format (ApiVersions v3 per KIP-511):
    // Header (OLD format, NO tagged fields):
    //   size (4) + correlation_id (4) = 8 bytes
    // Body (flexible format):
    //   error_code (2) + compact_array_len (1) + 1 item (7) + throttle_time_ms (4) + tagged_fields (1) = 15 bytes
    // Total: 23 bytes

    // Size prefix (first 4 bytes)
    let size = i32::from_be_bytes([result[0], result[1], result[2], result[3]]);
    assert_eq!(size as usize, result.len() - 4);

    // correlation_id = 123 (0x0000007B)
    assert_eq!(&result[4..8], &[0x00, 0x00, 0x00, 0x7B]);

    // NO header_tagged_fields byte per KIP-511!
    // error_code = 0 starts immediately at byte 8
    assert_eq!(&result[8..10], &[0x00, 0x00]);

    // compact_array_len = 2 (1 item + 1) at byte 10
    assert_eq!(result[10], 0x02);
}

#[test]
fn test_encode_api_versions_flexible_uses_old_header_format() {
    // Per KIP-511: ApiVersions is special - even v3 uses OLD response header format
    // (just correlation_id, NO tagged fields) so clients can parse it before negotiation.
    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![],
        throttle_time_ms: 0,
    };

    let result = encode_api_versions_flexible(1, &response).unwrap();

    // Header should be OLD format: size (4) + correlation_id (4) = 8 bytes
    // Body starts at byte 8 with error_code
    // Byte 8 should be error_code high byte (0x00), NOT a tagged fields byte
    assert_eq!(result[8], 0x00, "Byte 8 should be error_code high byte");
    // Byte 9 should be error_code low byte (0x00)
    assert_eq!(result[9], 0x00, "Byte 9 should be error_code low byte");
    // Byte 10 should be compact array length (0x01 for empty array)
    assert_eq!(
        result[10], 0x01,
        "Byte 10 should be compact array length (1 = 0 items)"
    );
}

#[test]
fn test_encode_api_versions_flexible_correct_format() {
    // Verify v3 response has correct format per KIP-511:
    // Header (OLD format): size + correlation_id (NO tagged fields!)
    // Body: error_code, api_keys, throttle_time_ms, tagged_fields
    // Note: SupportedFeatures, FinalizedFeaturesEpoch, FinalizedFeatures are OPTIONAL
    // tagged fields (tag 0, 1, 2) that we omit by using empty tagged fields.
    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![],
        throttle_time_ms: 0,
    };

    let result = encode_api_versions_flexible(1, &response).unwrap();

    // Calculate positions (KIP-511: OLD header format):
    // size (4) + correlation_id (4) = 8 bytes header (NO tagged fields in header!)
    // error_code (2) + compact_array_len (1) + throttle_time_ms (4) + tagged_fields (1) = 8 bytes body
    // Total: 16 bytes

    assert_eq!(
        result.len(),
        16,
        "Expected 16 bytes for minimal v3 response (old header format per KIP-511)"
    );

    // After throttle_time_ms (at byte 15), we should have empty tagged_fields in body
    assert_eq!(result[15], 0x00, "Body should have empty tagged fields");
}

#[test]
fn test_encode_api_versions_v0_vs_v1_size_difference() {
    // v0 should be exactly 4 bytes smaller than v1 (no throttle_time_ms)
    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![ApiVersionData {
            api_key: ApiKey::Metadata,
            min_version: 0,
            max_version: 12,
        }],
        throttle_time_ms: 1000,
    };

    let v0_result = encode_api_versions_standard(1, 0, &response).unwrap();
    let v1_result = encode_api_versions_standard(1, 1, &response).unwrap();

    assert_eq!(
        v1_result.len() - v0_result.len(),
        4,
        "v1 should be 4 bytes larger than v0"
    );
}

#[test]
fn test_encode_response_style_standard() {
    // Test the encode_response_with_style helper for standard
    use crate::server::response::ApiVersionsResponseData;

    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![],
        throttle_time_ms: 0,
    };

    let result = encode_response_with_style(1, &response, ResponseStyle::Standard).unwrap();

    // Standard response shouldn't have tagged fields byte in header
    // Size prefix (4) + correlation_id (4) = 8, then body starts
    // Body: error_code (2) + array_len (4) + throttle_time_ms (4) = 10
    assert_eq!(result.len(), 18);
}

#[test]
fn test_encode_response_style_flexible() {
    // Test the encode_response_with_style helper for flexible
    use crate::server::response::ApiVersionsResponseData;

    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![],
        throttle_time_ms: 0,
    };

    let result = encode_response_with_style(1, &response, ResponseStyle::Flexible).unwrap();

    // Flexible response has tagged fields byte in header
    // Header includes 1 extra byte for tagged fields
    assert!(
        result.len() > 18,
        "Flexible encoding should be larger due to tagged fields"
    );

    // Byte 8 should be empty tagged fields (0x00)
    assert_eq!(result[8], 0x00);
}

#[test]
fn test_encode_response_helper_functions() {
    // Test the convenience helper against the style-parameterized form
    use crate::server::response::ApiVersionsResponseData;

    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![],
        throttle_time_ms: 0,
    };

    let standard = encode_response(1, &response).unwrap();
    let flexible = encode_response_with_style(1, &response, ResponseStyle::Flexible).unwrap();

    // Flexible should be larger due to header tagged fields
    assert!(flexible.len() > standard.len());
}

#[test]
fn test_response_style_enum() {
    // Test ResponseStyle enum equality
    assert_eq!(ResponseStyle::Standard, ResponseStyle::Standard);
    assert_eq!(ResponseStyle::Flexible, ResponseStyle::Flexible);
    assert_ne!(ResponseStyle::Standard, ResponseStyle::Flexible);

    // Test Copy trait
    let style = ResponseStyle::Flexible;
    let copied = style;
    assert_eq!(style, copied);
}

// ========================================================================
// AuthResult Tests
// ========================================================================

#[test]
fn test_auth_result_enum() {
    // Test AuthResult variants
    let not_auth = AuthResult::NotAuth;
    let success = AuthResult::Success;
    let failure = AuthResult::Failure;

    assert_eq!(not_auth, AuthResult::NotAuth);
    assert_eq!(success, AuthResult::Success);
    assert_eq!(failure, AuthResult::Failure);

    assert_ne!(not_auth, success);
    assert_ne!(success, failure);
    assert_ne!(not_auth, failure);
}

#[test]
fn test_auth_result_debug() {
    assert!(format!("{:?}", AuthResult::NotAuth).contains("NotAuth"));
    assert!(format!("{:?}", AuthResult::Success).contains("Success"));
    assert!(format!("{:?}", AuthResult::Failure).contains("Failure"));
}

#[test]
fn test_auth_result_copy() {
    let result = AuthResult::Success;
    let copied = result;
    assert_eq!(result, copied);
}

#[test]
fn test_auth_result_clone() {
    let result = AuthResult::Failure;
    let cloned = result;
    assert_eq!(result, cloned);
}

// ========================================================================
// ResponseStyle Tests
// ========================================================================

#[test]
fn test_response_style_debug() {
    assert!(format!("{:?}", ResponseStyle::Standard).contains("Standard"));
    assert!(format!("{:?}", ResponseStyle::Flexible).contains("Flexible"));
}

#[test]
fn test_response_style_clone() {
    let style = ResponseStyle::Standard;
    let cloned = style;
    assert_eq!(style, cloned);
}

// ========================================================================
// Metric Tracking Tests
// ========================================================================

#[test]
fn test_track_connection_metrics() {
    // Just verify the functions don't panic
    // (actual metric values are tested elsewhere)
    track_connection_open();
    track_connection_close();
}

// ========================================================================
// Encoding with Error Tests
// ========================================================================

#[test]
fn test_encode_api_versions_with_error_code() {
    let response = ApiVersionsResponseData {
        error_code: KafkaCode::UnsupportedVersion,
        api_keys: vec![],
        throttle_time_ms: 0,
    };

    let result = encode_api_versions_standard(1, 1, &response).unwrap();

    // Verify error code is encoded
    // After size prefix (4) and correlation_id (4), error_code is at byte 8-9
    // UnsupportedVersion = 35 (0x0023)
    assert_eq!(&result[8..10], &[0x00, 0x23]);
}

#[test]
fn test_encode_api_versions_flexible_with_error() {
    let response = ApiVersionsResponseData {
        error_code: KafkaCode::UnsupportedVersion,
        api_keys: vec![],
        throttle_time_ms: 0,
    };

    let result = encode_api_versions_flexible(1, &response).unwrap();

    // After size (4) and correlation_id (4), error_code is at byte 8-9
    // (no tagged fields in header per KIP-511)
    assert_eq!(&result[8..10], &[0x00, 0x23]);
}

// ========================================================================
// Constants Tests
// ========================================================================

#[test]
fn test_default_max_message_size() {
    // 100 MB limit
    assert_eq!(DEFAULT_MAX_MESSAGE_SIZE, 100 * 1024 * 1024);
}

// ========================================================================
// InitProducerId versioned response framing Tests
// ========================================================================

#[test]
fn test_init_producer_id_response_v1_classic_framing() {
    // v0-v1: response header v0 (no tagged fields) + classic body
    use crate::server::response::InitProducerIdResponseData;
    let resp = InitProducerIdResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        producer_id: 9000,
        producer_epoch: 1,
    };

    let mut body = Vec::new();
    resp.encode_versioned(&mut body, 1).unwrap();
    // throttle (4) + error (2) + producer_id (8) + epoch (2) = 16, NO tag byte
    assert_eq!(body.len(), 16);

    let framed = Response::new_raw(7, body)
        .unwrap()
        .encode_with_size()
        .unwrap();
    // size (4) + correlation (4) + body (16) = 24
    assert_eq!(framed.len(), 24);
    assert_eq!(&framed[4..8], &7i32.to_be_bytes());
    // Body starts immediately at byte 8 (no header tagged fields):
    assert_eq!(&framed[8..12], &0i32.to_be_bytes()); // throttle_time_ms
    assert_eq!(&framed[12..14], &[0x00, 0x00]); // error_code
    assert_eq!(&framed[14..22], &9000i64.to_be_bytes()); // producer_id
    assert_eq!(&framed[22..24], &1i16.to_be_bytes()); // producer_epoch
}

#[test]
fn test_init_producer_id_response_v2_flexible_framing() {
    // v2+: response header v1 (tagged fields) + flexible body
    use crate::server::response::InitProducerIdResponseData;
    let resp = InitProducerIdResponseData {
        throttle_time_ms: 0,
        error_code: KafkaCode::None,
        producer_id: 9000,
        producer_epoch: 1,
    };

    let mut body = Vec::new();
    resp.encode_versioned(&mut body, 2).unwrap();
    // 16 fixed bytes + trailing tagged-fields byte
    assert_eq!(body.len(), 17);
    assert_eq!(*body.last().unwrap(), 0x00);

    let framed = Response::new_raw_flexible(7, body)
        .unwrap()
        .encode_with_size()
        .unwrap();
    // size (4) + correlation (4) + header tag (1) + body (17) = 26
    assert_eq!(framed.len(), 26);
    assert_eq!(&framed[4..8], &7i32.to_be_bytes());
    assert_eq!(framed[8], 0x00, "header v1 must carry empty tagged fields");
    assert_eq!(&framed[9..13], &0i32.to_be_bytes()); // throttle_time_ms
    assert_eq!(&framed[15..23], &9000i64.to_be_bytes()); // producer_id
}

// ========================================================================
// UnsupportedVersion response Tests
// ========================================================================

#[test]
fn test_encode_unsupported_version_response_generic_api() {
    // Produce v0 (unadvertised) must produce: size prefix, the request's
    // correlation id, and the INT16 error code 35 — nothing else, and
    // definitely not a closed connection.
    let result = encode_unsupported_version_response(ApiKey::Produce, 7777).unwrap();

    // size (4) + correlation_id (4) + error_code (2) = 10 bytes
    assert_eq!(result.len(), 10);
    // Size prefix = 6
    assert_eq!(&result[0..4], &[0x00, 0x00, 0x00, 0x06]);
    // correlation_id = 7777 (0x00001E61)
    assert_eq!(&result[4..8], &7777i32.to_be_bytes());
    // error_code = 35 (UnsupportedVersion)
    assert_eq!(&result[8..10], &[0x00, 0x23]);
}

#[test]
fn test_encode_unsupported_version_response_correlation_id_passthrough() {
    for correlation_id in [0i32, -1, i32::MAX, i32::MIN, 123456] {
        let result = encode_unsupported_version_response(ApiKey::Fetch, correlation_id).unwrap();
        assert_eq!(&result[4..8], &correlation_id.to_be_bytes());
        assert_eq!(&result[8..10], &[0x00, 0x23]);
    }
}

#[test]
fn test_encode_unsupported_version_response_api_versions() {
    // ApiVersions gets the richer treatment: a v0-encoded ApiVersions
    // response with error 35 AND the advertised ranges, so the client
    // can downgrade and retry.
    let result = encode_unsupported_version_response(ApiKey::ApiVersions, 555).unwrap();

    // correlation_id passthrough
    assert_eq!(&result[4..8], &555i32.to_be_bytes());
    // error_code = 35 right after the header (v0 body layout)
    assert_eq!(&result[8..10], &[0x00, 0x23]);
    // api_keys array length must match the advertised table
    let count = i32::from_be_bytes(result[10..14].try_into().unwrap());
    assert_eq!(
        count as usize,
        crate::server::versions::SUPPORTED_VERSIONS.len()
    );
    // v0 layout: no throttle_time_ms after the array.
    // size (4) + correlation (4) + error (2) + len (4) + count * 6
    assert_eq!(result.len(), 14 + count as usize * 6);
}

#[test]
fn test_peek_correlation_id_from_request_body() {
    let mut body = Vec::new();
    // api_key Produce = 0, api_version 3, correlation_id 4242
    body.extend_from_slice(&0i16.to_be_bytes());
    body.extend_from_slice(&3i16.to_be_bytes());
    body.extend_from_slice(&4242i32.to_be_bytes());
    let data = Bytes::from(body);
    assert_eq!(peek_correlation_id(&data), Some(4242));
}

#[test]
fn test_peek_correlation_id_too_short() {
    assert_eq!(
        peek_correlation_id(&Bytes::from_static(&[0, 1, 2, 3])),
        None
    );
}

#[test]
fn test_encode_wire_error_response_carries_correlation_id() {
    let result = encode_wire_error_response(9001, KafkaCode::InvalidRequest).unwrap();
    assert_eq!(result.len(), 10);
    assert_eq!(&result[4..8], &9001i32.to_be_bytes());
    assert_eq!(&result[8..10], &42i16.to_be_bytes()); // InvalidRequest
}

#[test]
fn test_response_style_for_init_producer_id_versions() {
    use crate::server::request::ApiKey;
    assert_eq!(
        response_style_for(ApiKey::InitProducerId, 1),
        ResponseStyle::Standard
    );
    assert_eq!(
        response_style_for(ApiKey::InitProducerId, 2),
        ResponseStyle::Flexible
    );
}

// ========================================================================
// Global inflight budget Tests
// ========================================================================

#[test]
fn test_inflight_budget_reserve_and_release() {
    // Reserving must succeed for a small frame and permits must come
    // back when the guard drops.
    let before = GLOBAL_INFLIGHT.available_permits();
    {
        let permit = try_reserve_inflight_bytes(1024).expect("reservation should succeed");
        assert_eq!(permit.bytes, 1024);
        assert_eq!(GLOBAL_INFLIGHT.available_permits(), before - 1024);
    }
    assert_eq!(GLOBAL_INFLIGHT.available_permits(), before);
}

#[test]
fn test_set_global_inflight_byte_budget_is_one_shot() {
    // Force initialization (any earlier test or frame may already have);
    // from then on the budget is frozen and the setter must say so.
    let effective = global_inflight_byte_budget();
    assert!(effective > 0);
    assert!(
        !set_global_inflight_byte_budget(123),
        "setter must refuse once the budget is frozen"
    );
    // The effective budget is unchanged by the refused call.
    assert_eq!(global_inflight_byte_budget(), effective);
}

// ========================================================================
// Multiple API Keys Tests
// ========================================================================

#[test]
fn test_encode_api_versions_multiple_keys() {
    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![
            ApiVersionData {
                api_key: ApiKey::Produce,
                min_version: 0,
                max_version: 9,
            },
            ApiVersionData {
                api_key: ApiKey::Fetch,
                min_version: 0,
                max_version: 13,
            },
            ApiVersionData {
                api_key: ApiKey::Metadata,
                min_version: 0,
                max_version: 12,
            },
        ],
        throttle_time_ms: 0,
    };

    let result = encode_api_versions_standard(1, 1, &response).unwrap();

    // Wire format:
    // size (4) + correlation_id (4) + error_code (2) + array_len (4) + 3 items (18) + throttle (4)
    // = 4 + 4 + 2 + 4 + 18 + 4 = 36 bytes
    assert_eq!(result.len(), 36);

    // Array length = 3
    assert_eq!(&result[10..14], &[0x00, 0x00, 0x00, 0x03]);
}

#[test]
fn test_encode_api_versions_flexible_multiple_keys() {
    let response = ApiVersionsResponseData {
        error_code: KafkaCode::None,
        api_keys: vec![
            ApiVersionData {
                api_key: ApiKey::Produce,
                min_version: 0,
                max_version: 9,
            },
            ApiVersionData {
                api_key: ApiKey::Fetch,
                min_version: 0,
                max_version: 13,
            },
        ],
        throttle_time_ms: 100,
    };

    let result = encode_api_versions_flexible(42, &response).unwrap();

    // Verify correlation_id
    assert_eq!(&result[4..8], &[0x00, 0x00, 0x00, 0x2A]);

    // Verify compact array length = 3 (2 items + 1)
    assert_eq!(result[10], 0x03);
}

// ========================================================================
// Duration/Timeout Constants Tests
// ========================================================================

#[test]
fn test_timeout_duration_constants() {
    use crate::constants::{
        DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS, DEFAULT_REQUEST_READ_TIMEOUT_SECS,
    };

    let read_timeout = Duration::from_secs(DEFAULT_REQUEST_READ_TIMEOUT_SECS);
    let handler_timeout = Duration::from_secs(DEFAULT_REQUEST_HANDLER_TIMEOUT_SECS);

    // Both should be reasonable durations (at least 1 second)
    assert!(read_timeout.as_secs() >= 1);
    assert!(handler_timeout.as_secs() >= 1);
}

#[tokio::test]
async fn read_kafka_frame_rejects_body_shorter_than_minimum() {
    use tokio::io::AsyncWriteExt;

    let (mut client, mut server) = tokio::io::duplex(64);
    client.write_all(&4i32.to_be_bytes()).await.unwrap();
    client.write_all(&[0u8; 4]).await.unwrap();
    client.shutdown().await.unwrap();

    let result = read_kafka_frame_for_fuzz(&mut server, 1024).await;
    assert!(
        matches!(
            result,
            Err(Error::FrameRejected {
                reason: crate::error::FrameRejectReason::MalformedSize,
                ..
            })
        ),
        "frames shorter than 8 bytes should be rejected before allocation"
    );
}

#[tokio::test]
async fn read_kafka_frame_accepts_minimum_valid_body() {
    use tokio::io::AsyncWriteExt;

    let (mut client, mut server) = tokio::io::duplex(64);
    client.write_all(&8i32.to_be_bytes()).await.unwrap();
    client.write_all(&[0u8; 8]).await.unwrap();

    let frame = read_kafka_frame_for_fuzz(&mut server, 1024)
        .await
        .expect("minimum-size frame should parse");
    assert_eq!(frame.len(), 8);
}

#[tokio::test]
async fn read_kafka_frame_rejects_oversized_frame() {
    use tokio::io::AsyncWriteExt;

    let max = 1024usize;
    let (mut client, mut server) = tokio::io::duplex(64);
    client
        .write_all(&((max + 1) as i32).to_be_bytes())
        .await
        .unwrap();

    let result = read_kafka_frame_for_fuzz(&mut server, max).await;
    assert!(
        matches!(
            result,
            Err(Error::FrameRejected {
                reason: crate::error::FrameRejectReason::FrameTooLarge,
                ..
            })
        ),
        "frames above max_message_size should be rejected before allocation"
    );
}
