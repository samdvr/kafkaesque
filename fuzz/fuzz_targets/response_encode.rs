#![no_main]

//! P1-4: Encoder safety on adversarial response data.
//!
//! Builds representative response types — Metadata (nested vecs +
//! strings), Produce (vecs of partition responses), ApiVersions (varint-
//! adjacent paths via flexible v3), Heartbeat (smallest, version-aware) —
//! with attacker-shaped fields drawn from the fuzz input, then drives the
//! version-aware encoders for every advertised version.
//!
//! Property: errors propagate as `Error::Config` (the bounds-check path),
//! never panics, never `Vec::reserve` overflow.
//!
//! `arbitrary` lets us mutate field lengths and counts directly, which is
//! more productive than feeding raw bytes through `Bytes::copy_from_slice`
//! at this layer — fuzzing the encoder is fundamentally about field-level
//! shape, not byte-level wire shape.

use arbitrary::{Arbitrary, Unstructured};
use bytes::BytesMut;
use kafkaesque::encode::ToByte;
use libfuzzer_sys::fuzz_target;

use kafkaesque::error::KafkaCode;
use kafkaesque::server::request::ApiKey;
use kafkaesque::server::response::{
    ApiVersionData, ApiVersionsResponseData, BrokerData, HeartbeatResponseData,
    MetadataResponseData, PartitionMetadata, ProducePartitionResponse, ProduceResponseData,
    ProduceTopicResponse, TopicMetadata,
};

const MAX_INPUT: usize = 64 * 1024;

/// Cap on individual string lengths so we exercise the boundary-rejection
/// path frequently. Without this, libFuzzer rarely produces a 32_768-byte
/// field that trips `encode_string_len`.
const MAX_STR: usize = i16::MAX as usize + 16;
/// Cap on collection sizes — keep individual fuzz iterations bounded.
const MAX_ITEMS: usize = 64;

/// Draw a `String` whose length is biased toward the `i16::MAX` boundary
/// where `encode_string_len` flips from Ok → Err.
fn arb_string(u: &mut Unstructured<'_>) -> arbitrary::Result<String> {
    let raw = u8::arbitrary(u)?;
    let len = match raw % 4 {
        0 => 0usize,
        1 => 1usize,
        2 => i16::MAX as usize,
        _ => MAX_STR.min(usize::from(raw) * 64),
    };
    Ok("a".repeat(len))
}

fn arb_kafka_code(u: &mut Unstructured<'_>) -> arbitrary::Result<KafkaCode> {
    // Any i16 → KafkaCode via num_traits; if the value isn't a known code
    // we fall back to `KafkaCode::None` so the encoder still gets exercised.
    let raw = i16::arbitrary(u)?;
    Ok(num_traits::FromPrimitive::from_i16(raw).unwrap_or(KafkaCode::None))
}

fn arb_api_key(u: &mut Unstructured<'_>) -> arbitrary::Result<ApiKey> {
    Ok(ApiKey::from(i16::arbitrary(u)?))
}

fn arb_partition(u: &mut Unstructured<'_>) -> arbitrary::Result<PartitionMetadata> {
    Ok(PartitionMetadata {
        error_code: arb_kafka_code(u)?,
        partition_index: i32::arbitrary(u)?,
        leader_id: i32::arbitrary(u)?,
        replica_nodes: Vec::<i32>::arbitrary_take_rest(Unstructured::new(u.peek_bytes(64).unwrap_or(&[])))
            .unwrap_or_default(),
        isr_nodes: Vec::<i32>::arbitrary_take_rest(Unstructured::new(u.peek_bytes(64).unwrap_or(&[])))
            .unwrap_or_default(),
    })
}

fn arb_topic_meta(u: &mut Unstructured<'_>) -> arbitrary::Result<TopicMetadata> {
    let n = (u8::arbitrary(u)? as usize) % MAX_ITEMS;
    let mut partitions = Vec::with_capacity(n);
    for _ in 0..n {
        partitions.push(arb_partition(u)?);
    }
    Ok(TopicMetadata {
        error_code: arb_kafka_code(u)?,
        name: arb_string(u)?,
        is_internal: bool::arbitrary(u)?,
        partitions,
    })
}

fn arb_metadata_response(
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<MetadataResponseData> {
    let nb = (u8::arbitrary(u)? as usize) % MAX_ITEMS;
    let mut brokers = Vec::with_capacity(nb);
    for _ in 0..nb {
        let rack = if bool::arbitrary(u)? {
            Some(arb_string(u)?)
        } else {
            None
        };
        brokers.push(BrokerData {
            node_id: i32::arbitrary(u)?,
            host: arb_string(u)?,
            port: i32::arbitrary(u)?,
            rack,
        });
    }

    let nt = (u8::arbitrary(u)? as usize) % MAX_ITEMS;
    let mut topics = Vec::with_capacity(nt);
    for _ in 0..nt {
        topics.push(arb_topic_meta(u)?);
    }

    Ok(MetadataResponseData {
        brokers,
        controller_id: i32::arbitrary(u)?,
        topics,
    })
}

fn arb_produce_response(
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<ProduceResponseData> {
    let nt = (u8::arbitrary(u)? as usize) % MAX_ITEMS;
    let mut responses = Vec::with_capacity(nt);
    for _ in 0..nt {
        let np = (u8::arbitrary(u)? as usize) % MAX_ITEMS;
        let mut partitions = Vec::with_capacity(np);
        for _ in 0..np {
            partitions.push(ProducePartitionResponse {
                partition_index: i32::arbitrary(u)?,
                error_code: arb_kafka_code(u)?,
                base_offset: i64::arbitrary(u)?,
                log_append_time: i64::arbitrary(u)?,
            });
        }
        responses.push(ProduceTopicResponse {
            name: arb_string(u)?,
            partitions,
        });
    }
    Ok(ProduceResponseData {
        responses,
        throttle_time_ms: i32::arbitrary(u)?,
    })
}

fn arb_api_versions_response(
    u: &mut Unstructured<'_>,
) -> arbitrary::Result<ApiVersionsResponseData> {
    let n = (u8::arbitrary(u)? as usize) % MAX_ITEMS;
    let mut api_keys = Vec::with_capacity(n);
    for _ in 0..n {
        api_keys.push(ApiVersionData {
            api_key: arb_api_key(u)?,
            min_version: i16::arbitrary(u)?,
            max_version: i16::arbitrary(u)?,
        });
    }
    Ok(ApiVersionsResponseData {
        error_code: arb_kafka_code(u)?,
        api_keys,
        throttle_time_ms: i32::arbitrary(u)?,
    })
}

/// Drive a single encode call and convert the outcome to a tag we can assert
/// on. Any panic blows up the fuzz run (libFuzzer treats it as a finding).
fn encode_no_panic<F>(f: F)
where
    F: FnOnce(&mut BytesMut) -> kafkaesque::error::Result<()>,
{
    let mut buf = BytesMut::with_capacity(256);
    match f(&mut buf) {
        Ok(()) | Err(kafkaesque::error::Error::Config(_)) => {}
        Err(other) => panic!(
            "encoder produced a non-Config error on adversarial input: {other:?}"
        ),
    }
}

fuzz_target!(|data: &[u8]| {
    if data.len() > MAX_INPUT {
        return;
    }
    let mut u = Unstructured::new(data);

    if let Ok(resp) = arb_metadata_response(&mut u) {
        // MetadataResponseData uses the single-version `ToByte::encode` path.
        encode_no_panic(|buf| resp.encode(buf));
    }
    if let Ok(resp) = arb_produce_response(&mut u) {
        // ProduceResponseData also uses `ToByte::encode`.
        encode_no_panic(|buf| resp.encode(buf));
    }
    if let Ok(resp) = arb_api_versions_response(&mut u) {
        for v in 0i16..=3i16 {
            encode_no_panic(|buf| resp.encode_versioned(buf, v));
        }
    }
    if let (Ok(throttle_time_ms), Ok(error_code)) = (i32::arbitrary(&mut u), arb_kafka_code(&mut u))
    {
        let resp = HeartbeatResponseData {
            throttle_time_ms,
            error_code,
        };
        for v in 0i16..=1i16 {
            encode_no_panic(|buf| resp.encode_versioned(buf, v));
        }
    }
});
