//! Deserialize data from the bytecode protocol.
use bytes::Bytes;
use nom::{
    IResult,
    bytes::complete::take,
    number::complete::{be_i16, be_i32},
};
use nombytes::NomBytes;

use crate::constants::MAX_PROTOCOL_ARRAY_SIZE;

fn utf8_verify_err(input: NomBytes) -> nom::Err<nom::error::Error<NomBytes>> {
    nom::Err::Failure(nom::error::Error::new(input, nom::error::ErrorKind::Verify))
}

/// Parse a Kafka `STRING` directly into a validated UTF-8 [`String`].
///
/// Unlike `parse_string` followed by `bytes_to_string`, this avoids the
/// intermediate [`Bytes`] allocation on the hot path where every topic and
/// group name is decoded.
pub fn parse_kafka_string(s: NomBytes) -> IResult<NomBytes, String> {
    let (s, length) = be_i16(s)?;

    if length < 0 {
        return Err(utf8_verify_err(s));
    }

    let (s, raw) = take(length as usize)(s)?;
    let bytes = raw.into_bytes();
    match std::str::from_utf8(&bytes) {
        Ok(text) => Ok((s, text.to_string())),
        Err(_) => Err(utf8_verify_err(s)),
    }
}

/// Parse a Kafka `NULLABLE_STRING` directly into `Option<String>`.
pub fn parse_kafka_string_opt(s: NomBytes) -> IResult<NomBytes, Option<String>> {
    let (s, length) = be_i16(s)?;

    if length == -1 {
        return Ok((s, None));
    }

    if length < 0 {
        return Err(utf8_verify_err(s));
    }

    let (s, raw) = take(length as usize)(s)?;
    let bytes = raw.into_bytes();
    match std::str::from_utf8(&bytes) {
        Ok(text) => Ok((s, Some(text.to_string()))),
        Err(_) => Err(utf8_verify_err(s)),
    }
}

/// Convert bytes to a validated UTF-8 string.
/// Returns an error if the bytes are not valid UTF-8.
pub fn bytes_to_string(bytes: &Bytes) -> Result<String, nom::Err<nom::error::Error<NomBytes>>> {
    std::str::from_utf8(bytes)
        .map(|s| s.to_string())
        .map_err(|_| {
            nom::Err::Failure(nom::error::Error::new(
                NomBytes::from(bytes.as_ref()),
                nom::error::ErrorKind::Verify,
            ))
        })
}

/// Convert optional bytes to a validated UTF-8 string.
/// Returns None for None input, error for invalid UTF-8.
pub fn bytes_to_string_opt(
    bytes: Option<Bytes>,
) -> Result<Option<String>, nom::Err<nom::error::Error<NomBytes>>> {
    match bytes {
        Some(b) => bytes_to_string(&b).map(Some),
        None => Ok(None),
    }
}

/// Parse a Kafka `STRING`: signed INT16 length followed by that many bytes.
///
/// Per the Kafka protocol spec the length is a *signed* INT16. A negative
/// length (including -1, which is only legal for NULLABLE_STRING) is invalid
/// for a non-nullable STRING and is rejected. Reading the length as unsigned
/// would misinterpret 0xFFFF as 65535, mis-aligning every subsequent field.
pub fn parse_string(s: NomBytes) -> IResult<NomBytes, Bytes> {
    let (s, length) = be_i16(s)?;

    if length < 0 {
        return Err(nom::Err::Failure(nom::error::Error::new(
            s,
            nom::error::ErrorKind::Verify,
        )));
    }

    let (s, string) = take(length as usize)(s)?;
    Ok((s, string.into_bytes()))
}

pub fn parse_array<O, E, F>(mut f: F) -> impl FnMut(NomBytes) -> IResult<NomBytes, Vec<O>, E>
where
    F: FnMut(NomBytes) -> IResult<NomBytes, O, E>,
    E: nom::error::ParseError<NomBytes>,
{
    move |input: NomBytes| {
        let (mut rest, length) = be_i32(input)?;

        // Null array
        if length == -1 {
            return Ok((rest, vec![]));
        }

        // Validate array size bounds
        if !(0..=MAX_PROTOCOL_ARRAY_SIZE).contains(&length) {
            return Err(nom::Err::Failure(E::from_error_kind(
                rest,
                nom::error::ErrorKind::TooLarge,
            )));
        }

        let mut out = Vec::with_capacity(length as usize);
        for _ in 0..length {
            let (r, item) = f(rest)?;
            out.push(item);
            rest = r;
        }
        Ok((rest, out))
    }
}

pub fn parse_nullable_string(s: NomBytes) -> IResult<NomBytes, Option<Bytes>> {
    let (s, length) = be_i16(s)?;

    // Null string
    if length == -1 {
        return Ok((s, None));
    }

    // Validate string length bounds (only check lower bound since MAX_STRING_SIZE is i16::MAX)
    if length < 0 {
        return Err(nom::Err::Failure(nom::error::Error::new(
            s,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    let (s, string) = take(length as u16)(s)?;
    Ok((s, Some(string.into_bytes())))
}

/// Parse an unsigned varint (variable-length integer) used in flexible encoding.
/// Returns the parsed value and remaining bytes.
pub fn parse_unsigned_varint(s: NomBytes) -> IResult<NomBytes, u32> {
    let mut result: u32 = 0;
    let mut shift = 0;
    let mut remaining = s;

    loop {
        let (s, byte) = take(1usize)(remaining)?;
        let b = byte.into_bytes()[0];
        remaining = s;

        result |= ((b & 0x7F) as u32) << shift;

        if (b & 0x80) == 0 {
            break;
        }

        shift += 7;
        if shift > 28 {
            // Overflow protection
            return Err(nom::Err::Failure(nom::error::Error::new(
                remaining,
                nom::error::ErrorKind::TooLarge,
            )));
        }
    }

    Ok((remaining, result))
}

/// Parse a COMPACT_NULLABLE_STRING used in flexible encoding.
/// Format: unsigned varint length where:
/// - 0 = null
/// - 1 = empty string ""
/// - n+1 = string of length n
pub fn parse_compact_nullable_string(s: NomBytes) -> IResult<NomBytes, Option<Bytes>> {
    let (s, length) = parse_unsigned_varint(s)?;

    // 0 means null
    if length == 0 {
        return Ok((s, None));
    }

    // length - 1 is the actual string length
    let actual_length = length - 1;

    if actual_length == 0 {
        return Ok((s, Some(Bytes::new())));
    }

    let (s, string) = take(actual_length as usize)(s)?;
    Ok((s, Some(string.into_bytes())))
}

/// Skip tagged fields in flexible encoding.
/// Format: unsigned varint count, then for each: varint tag, varint size, bytes
pub fn skip_tagged_fields(s: NomBytes) -> IResult<NomBytes, ()> {
    // Cap iteration count to MAX_PROTOCOL_ARRAY_SIZE so an attacker-supplied
    // count of u32::MAX can't loop the parser. The varint decoder is happy
    // to return very large counts; the cap is the same one we apply to
    // every other length-prefixed array on the wire.
    let (mut s, count) = parse_unsigned_varint(s)?;
    if count as i64 > MAX_PROTOCOL_ARRAY_SIZE as i64 {
        return Err(nom::Err::Error(nom::error::Error::new(
            s,
            nom::error::ErrorKind::TooLarge,
        )));
    }

    for _ in 0..count {
        // Skip tag
        let (remaining, _tag) = parse_unsigned_varint(s)?;
        // Skip size and data; the size itself is also attacker-controlled,
        // but `take` will fail cleanly if the buffer is too short.
        let (remaining, size) = parse_unsigned_varint(remaining)?;
        let (remaining, _) = take(size as usize)(remaining)?;
        s = remaining;
    }

    Ok((s, ()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use nom::number::complete::be_i32;

    #[test]
    fn test_parse_kafka_string_valid() {
        let mut data = Vec::new();
        data.extend_from_slice(&5i16.to_be_bytes());
        data.extend_from_slice(b"hello");

        let input = NomBytes::new(Bytes::from(data));
        let (_, parsed) = parse_kafka_string(input).unwrap();
        assert_eq!(parsed, "hello");
    }

    #[test]
    fn test_parse_kafka_string_rejects_invalid_utf8() {
        let mut data = Vec::new();
        data.extend_from_slice(&2i16.to_be_bytes());
        data.extend_from_slice(&[0xFF, 0xFE]);

        let input = NomBytes::new(Bytes::from(data));
        assert!(parse_kafka_string(input).is_err());
    }

    #[test]
    fn test_parse_kafka_string_opt_null() {
        let data = (-1i16).to_be_bytes().to_vec();
        let input = NomBytes::new(Bytes::from(data));
        let (_, parsed) = parse_kafka_string_opt(input).unwrap();
        assert_eq!(parsed, None);
    }

    #[test]
    fn test_bytes_to_string_valid_utf8() {
        let bytes = Bytes::from("hello");
        let result = bytes_to_string(&bytes);
        assert_eq!(result.unwrap(), "hello");
    }

    #[test]
    fn test_bytes_to_string_invalid_utf8() {
        let bytes = Bytes::from(vec![0xff, 0xfe]);
        let result = bytes_to_string(&bytes);
        assert!(result.is_err());
    }

    #[test]
    fn test_bytes_to_string_opt_some() {
        let bytes = Some(Bytes::from("test"));
        let result = bytes_to_string_opt(bytes);
        assert_eq!(result.unwrap(), Some("test".to_string()));
    }

    #[test]
    fn test_bytes_to_string_opt_none() {
        let result = bytes_to_string_opt(None);
        assert_eq!(result.unwrap(), None);
    }

    #[test]
    fn test_parse_string() {
        // String format: u16 length + bytes
        let mut data = Vec::new();
        data.extend_from_slice(&5u16.to_be_bytes()); // length = 5
        data.extend_from_slice(b"hello");
        data.extend_from_slice(b"extra"); // extra bytes after

        let input = NomBytes::new(Bytes::from(data));
        let (remaining, parsed) = parse_string(input).unwrap();

        assert_eq!(parsed, Bytes::from("hello"));
        assert_eq!(remaining.into_bytes(), Bytes::from("extra"));
    }

    #[test]
    fn test_parse_string_rejects_negative_length() {
        // 0xFFFF is -1 as INT16: legal only for NULLABLE_STRING, never STRING.
        // A u16 reading would treat this as length 65535.
        let mut data = Vec::new();
        data.extend_from_slice(&(-1i16).to_be_bytes());
        data.extend_from_slice(b"payload");

        let input = NomBytes::new(Bytes::from(data));
        assert!(parse_string(input).is_err());

        // Any other negative length is equally invalid.
        let mut data = Vec::new();
        data.extend_from_slice(&(-2i16).to_be_bytes());
        let input = NomBytes::new(Bytes::from(data));
        assert!(parse_string(input).is_err());
    }

    #[test]
    fn test_parse_string_empty() {
        let data = 0i16.to_be_bytes().to_vec();
        let input = NomBytes::new(Bytes::from(data));
        let (_, parsed) = parse_string(input).unwrap();
        assert_eq!(parsed, Bytes::new());
    }

    #[test]
    fn test_parse_nullable_string_some() {
        // Nullable string format: i16 length + bytes
        let mut data = Vec::new();
        data.extend_from_slice(&4i16.to_be_bytes()); // length = 4
        data.extend_from_slice(b"test");

        let input = NomBytes::new(Bytes::from(data));
        let (_, parsed) = parse_nullable_string(input).unwrap();

        assert_eq!(parsed, Some(Bytes::from("test")));
    }

    #[test]
    fn test_parse_nullable_string_null() {
        // Null string indicated by length = -1
        let data = (-1i16).to_be_bytes();
        let input = NomBytes::new(Bytes::from(data.to_vec()));
        let (_, parsed) = parse_nullable_string(input).unwrap();

        assert_eq!(parsed, None);
    }

    #[test]
    fn test_parse_nullable_string_invalid_length() {
        // Invalid negative length (not -1)
        let data = (-2i16).to_be_bytes();
        let input = NomBytes::new(Bytes::from(data.to_vec()));
        let result = parse_nullable_string(input);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_array_empty() {
        // Empty array (length = 0)
        let data = 0i32.to_be_bytes();
        let input = NomBytes::new(Bytes::from(data.to_vec()));

        let mut parser = parse_array(be_i32::<_, nom::error::Error<NomBytes>>);
        let (_, parsed): (_, Vec<i32>) = parser(input).unwrap();

        assert!(parsed.is_empty());
    }

    #[test]
    fn test_parse_array_null() {
        // Null array (length = -1)
        let data = (-1i32).to_be_bytes();
        let input = NomBytes::new(Bytes::from(data.to_vec()));

        let mut parser = parse_array(be_i32::<_, nom::error::Error<NomBytes>>);
        let (_, parsed): (_, Vec<i32>) = parser(input).unwrap();

        assert!(parsed.is_empty());
    }

    #[test]
    fn test_parse_array_with_elements() {
        // Array with 3 i32 elements
        let mut data = Vec::new();
        data.extend_from_slice(&3i32.to_be_bytes()); // length = 3
        data.extend_from_slice(&10i32.to_be_bytes());
        data.extend_from_slice(&20i32.to_be_bytes());
        data.extend_from_slice(&30i32.to_be_bytes());

        let input = NomBytes::new(Bytes::from(data));

        let mut parser = parse_array(be_i32::<_, nom::error::Error<NomBytes>>);
        let (_, parsed): (_, Vec<i32>) = parser(input).unwrap();

        assert_eq!(parsed, vec![10, 20, 30]);
    }

    #[test]
    fn test_parse_array_too_large() {
        // Array size exceeds MAX_PROTOCOL_ARRAY_SIZE
        let data = (MAX_PROTOCOL_ARRAY_SIZE + 1).to_be_bytes();
        let input = NomBytes::new(Bytes::from(data.to_vec()));

        let mut parser = parse_array(be_i32::<_, nom::error::Error<NomBytes>>);
        let result: IResult<_, Vec<i32>, nom::error::Error<NomBytes>> = parser(input);

        assert!(result.is_err());
    }

    #[test]
    fn test_parse_unsigned_varint_single_byte() {
        // Single byte varint: 0x7F = 127
        let data = vec![0x7F];
        let input = NomBytes::new(Bytes::from(data));
        let (_, value) = parse_unsigned_varint(input).unwrap();
        assert_eq!(value, 127);
    }

    #[test]
    fn test_parse_unsigned_varint_multi_byte() {
        // Two byte varint: 0x80 0x01 = 128
        let data = vec![0x80, 0x01];
        let input = NomBytes::new(Bytes::from(data));
        let (_, value) = parse_unsigned_varint(input).unwrap();
        assert_eq!(value, 128);
    }

    #[test]
    fn test_parse_unsigned_varint_zero() {
        let data = vec![0x00];
        let input = NomBytes::new(Bytes::from(data));
        let (_, value) = parse_unsigned_varint(input).unwrap();
        assert_eq!(value, 0);
    }

    #[test]
    fn test_parse_unsigned_varint_300() {
        // 300 = 0xAC 0x02
        let data = vec![0xAC, 0x02];
        let input = NomBytes::new(Bytes::from(data));
        let (_, value) = parse_unsigned_varint(input).unwrap();
        assert_eq!(value, 300);
    }

    #[test]
    fn test_parse_compact_nullable_string_null() {
        // 0 = null
        let data = vec![0x00];
        let input = NomBytes::new(Bytes::from(data));
        let (_, result) = parse_compact_nullable_string(input).unwrap();
        assert_eq!(result, None);
    }

    #[test]
    fn test_parse_compact_nullable_string_empty() {
        // 1 = empty string
        let data = vec![0x01];
        let input = NomBytes::new(Bytes::from(data));
        let (_, result) = parse_compact_nullable_string(input).unwrap();
        assert_eq!(result, Some(Bytes::new()));
    }

    #[test]
    fn test_parse_compact_nullable_string_with_content() {
        // 6 = length 5, then "hello"
        let mut data = vec![0x06]; // length + 1 = 6, so actual length = 5
        data.extend_from_slice(b"hello");
        let input = NomBytes::new(Bytes::from(data));
        let (_, result) = parse_compact_nullable_string(input).unwrap();
        assert_eq!(result, Some(Bytes::from("hello")));
    }

    #[test]
    fn test_skip_tagged_fields_empty() {
        // 0 = no tagged fields
        let data = vec![0x00];
        let input = NomBytes::new(Bytes::from(data));
        let (remaining, _) = skip_tagged_fields(input).unwrap();
        assert!(remaining.into_bytes().is_empty());
    }

    #[test]
    fn test_skip_tagged_fields_with_fields() {
        // 1 field: tag=0, size=3, data="abc"
        let mut data = vec![
            0x01, // 1 tagged field
            0x00, // tag = 0
            0x03, // size = 3
        ];
        data.extend_from_slice(b"abc");
        data.extend_from_slice(b"remaining");
        let input = NomBytes::new(Bytes::from(data));
        let (remaining, _) = skip_tagged_fields(input).unwrap();
        assert_eq!(remaining.into_bytes(), Bytes::from("remaining"));
    }
}
