//! Serialize data into the bytecode protocol.
use bytes::{BufMut, Bytes};

use crate::error::{Error, Result};

#[inline]
fn encode_string_len<T: BufMut>(buffer: &mut T, len: usize) -> Result<()> {
    if len > i16::MAX as usize {
        return Err(Error::Config(format!(
            "string length {} exceeds wire field maximum {}",
            len,
            i16::MAX
        )));
    }
    buffer.put_i16(len as i16);
    Ok(())
}

#[inline]
fn encode_bytes_len<T: BufMut>(buffer: &mut T, len: usize) -> Result<()> {
    if len > i32::MAX as usize {
        return Err(Error::Config(format!(
            "byte length {} exceeds wire field maximum {}",
            len,
            i32::MAX
        )));
    }
    buffer.put_i32(len as i32);
    Ok(())
}

#[inline]
fn encode_array_len<T: BufMut>(buffer: &mut T, len: usize) -> Result<()> {
    if len > i32::MAX as usize {
        return Err(Error::Config(format!(
            "array length {} exceeds wire field maximum {}",
            len,
            i32::MAX
        )));
    }
    buffer.put_i32(len as i32);
    Ok(())
}

pub trait ToByte {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()>;
}

impl<'a, T: ToByte + 'a + ?Sized> ToByte for &'a T {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        (*self).encode(buffer)
    }
}

impl ToByte for bool {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        buffer.put_i8(*self as i8);
        Ok(())
    }
}

impl ToByte for i8 {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        buffer.put_i8(*self);
        Ok(())
    }
}

impl ToByte for i16 {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        buffer.put_i16(*self);
        Ok(())
    }
}

impl ToByte for i32 {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        buffer.put_i32(*self);
        Ok(())
    }
}

impl ToByte for u32 {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        buffer.put_u32(*self);
        Ok(())
    }
}

impl ToByte for i64 {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        buffer.put_i64(*self);
        Ok(())
    }
}

impl ToByte for str {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        encode_string_len(buffer, self.len())?;
        buffer.put(self.as_bytes());
        Ok(())
    }
}

impl ToByte for String {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        encode_string_len(buffer, self.len())?;
        buffer.put(self.as_bytes());
        Ok(())
    }
}

impl<V: ToByte> ToByte for [V] {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        encode_as_array(buffer, self, |buffer, x| x.encode(buffer))
    }
}

impl ToByte for [u8] {
    fn encode<T: BufMut>(&self, buffer: &mut T) -> Result<()> {
        encode_bytes_len(buffer, self.len())?;
        buffer.put(self);
        Ok(())
    }
}

/// Renders the length of `xs` to `buffer` as the start of a
/// protocol array and then for each element of `xs` invokes `f`
/// assuming that function will render the element to the buffer.
pub fn encode_as_array<T, F, W>(buffer: &mut W, xs: &[T], mut f: F) -> Result<()>
where
    F: FnMut(&mut W, &T) -> Result<()>,
    W: BufMut,
{
    encode_array_len(buffer, xs.len())?;
    for x in xs {
        f(buffer, x)?;
    }
    Ok(())
}

/// Encode a slice of ToByte items as a Kafka protocol array.
/// This is a convenience wrapper around `encode_as_array` for the common case.
pub fn encode_array<T: ToByte, W: BufMut>(buffer: &mut W, items: &[T]) -> Result<()> {
    encode_array_len(buffer, items.len())?;
    for item in items {
        item.encode(buffer)?;
    }
    Ok(())
}

/// Encode an unsigned varint (variable-length integer) to the buffer.
/// Used by flexible encoding formats (KIP-482).
pub fn encode_unsigned_varint<W: BufMut>(buffer: &mut W, mut value: u32) {
    loop {
        let mut byte = (value & 0x7F) as u8;
        value >>= 7;
        if value != 0 {
            byte |= 0x80;
        }
        buffer.put_u8(byte);
        if value == 0 {
            break;
        }
    }
}

/// Encode a compact array (used in flexible encoding).
/// Compact arrays encode length as unsigned varint of (length + 1).
/// A length of 0 means null array.
pub fn encode_compact_array<T, F, W>(buffer: &mut W, items: &[T], mut f: F) -> Result<()>
where
    F: FnMut(&mut W, &T) -> Result<()>,
    W: BufMut,
{
    // Compact array: length + 1 as unsigned varint
    encode_unsigned_varint(buffer, (items.len() + 1) as u32);
    for item in items {
        f(buffer, item)?;
    }
    Ok(())
}

/// Encode empty tagged fields (used at end of flexible-format messages).
/// An empty tagged field section is just a single 0 byte (varint for 0 fields).
pub fn encode_empty_tagged_fields<W: BufMut>(buffer: &mut W) {
    buffer.put_u8(0);
}

impl ToByte for Option<&[u8]> {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        match *self {
            Some(xs) => xs.encode(buffer),
            None => (-1i32).encode(buffer),
        }
    }
}

impl ToByte for Option<Bytes> {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        match self {
            Some(xs) => xs.encode(buffer),
            None => (-1i32).encode(buffer),
        }
    }
}

impl ToByte for Option<&str> {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        match *self {
            Some(xs) => xs.encode(buffer),
            None => (-1i16).encode(buffer), // NULLABLE_STRING uses i16 length prefix
        }
    }
}

impl ToByte for Option<String> {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        match self {
            Some(xs) => xs.encode(buffer),
            None => (-1i16).encode(buffer), // NULLABLE_STRING uses i16 length prefix
        }
    }
}

impl ToByte for Bytes {
    fn encode<W: BufMut>(&self, buffer: &mut W) -> Result<()> {
        encode_bytes_len(buffer, self.len())?;
        buffer.put_slice(self);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_bool_true() {
        let mut buf = Vec::new();
        true.encode(&mut buf).unwrap();
        assert_eq!(buf, vec![1]);
    }

    #[test]
    fn test_encode_bool_false() {
        let mut buf = Vec::new();
        false.encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0]);
    }

    #[test]
    fn test_encode_i8() {
        let mut buf = Vec::new();
        (-42i8).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0xD6]); // -42 in two's complement
    }

    #[test]
    fn test_encode_i16() {
        let mut buf = Vec::new();
        (0x1234i16).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x12, 0x34]); // big-endian
    }

    #[test]
    fn test_encode_i32() {
        let mut buf = Vec::new();
        (0x12345678i32).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x12, 0x34, 0x56, 0x78]);
    }

    #[test]
    fn test_encode_u32() {
        let mut buf = Vec::new();
        (0xDEADBEEFu32).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0xDE, 0xAD, 0xBE, 0xEF]);
    }

    #[test]
    fn test_encode_i64() {
        let mut buf = Vec::new();
        (0x123456789ABCDEF0i64).encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x12, 0x34, 0x56, 0x78, 0x9A, 0xBC, 0xDE, 0xF0]);
    }

    #[test]
    fn test_encode_str() {
        let mut buf = Vec::new();
        "hi".encode(&mut buf).unwrap();
        // i16 length prefix (2) + bytes
        assert_eq!(buf, vec![0x00, 0x02, b'h', b'i']);
    }

    #[test]
    fn test_encode_string() {
        let mut buf = Vec::new();
        "hello".to_string().encode(&mut buf).unwrap();
        // i16 length prefix (5) + bytes
        assert_eq!(buf, vec![0x00, 0x05, b'h', b'e', b'l', b'l', b'o']);
    }

    #[test]
    fn test_encode_bytes() {
        let mut buf = Vec::new();
        Bytes::from(vec![1, 2, 3]).encode(&mut buf).unwrap();
        // i32 length prefix (3) + bytes
        assert_eq!(buf, vec![0x00, 0x00, 0x00, 0x03, 1, 2, 3]);
    }

    #[test]
    fn test_encode_byte_slice() {
        let mut buf = Vec::new();
        let data: &[u8] = &[0xAB, 0xCD];
        data.encode(&mut buf).unwrap();
        // i32 length prefix (2) + bytes
        assert_eq!(buf, vec![0x00, 0x00, 0x00, 0x02, 0xAB, 0xCD]);
    }

    #[test]
    fn test_encode_array_i32() {
        let mut buf = Vec::new();
        let arr: &[i32] = &[1, 2];
        arr.encode(&mut buf).unwrap();
        // i32 array length (2) + two i32 values
        let expected = vec![
            0x00, 0x00, 0x00, 0x02, // length = 2
            0x00, 0x00, 0x00, 0x01, // 1
            0x00, 0x00, 0x00, 0x02, // 2
        ];
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_encode_as_array() {
        let mut buf = Vec::new();
        let items = vec![10i16, 20i16];
        encode_as_array(&mut buf, &items, |b, x| x.encode(b)).unwrap();
        let expected = vec![
            0x00, 0x00, 0x00, 0x02, // length = 2
            0x00, 0x0A, // 10
            0x00, 0x14, // 20
        ];
        assert_eq!(buf, expected);
    }

    #[test]
    fn test_encode_option_bytes_some() {
        let mut buf = Vec::new();
        let opt: Option<&[u8]> = Some(&[1, 2, 3]);
        opt.encode(&mut buf).unwrap();
        // i32 length prefix (3) + bytes
        assert_eq!(buf, vec![0x00, 0x00, 0x00, 0x03, 1, 2, 3]);
    }

    #[test]
    fn test_encode_option_bytes_none() {
        let mut buf = Vec::new();
        let opt: Option<&[u8]> = None;
        opt.encode(&mut buf).unwrap();
        // -1 as i32
        assert_eq!(buf, vec![0xFF, 0xFF, 0xFF, 0xFF]);
    }

    #[test]
    fn test_encode_option_str_some() {
        let mut buf = Vec::new();
        let opt: Option<&str> = Some("ok");
        opt.encode(&mut buf).unwrap();
        // i16 length prefix (2) + bytes
        assert_eq!(buf, vec![0x00, 0x02, b'o', b'k']);
    }

    #[test]
    fn test_encode_option_str_none() {
        let mut buf = Vec::new();
        let opt: Option<&str> = None;
        opt.encode(&mut buf).unwrap();
        // -1 as i16 (nullable string)
        assert_eq!(buf, vec![0xFF, 0xFF]);
    }

    #[test]
    fn test_encode_option_string_some() {
        let mut buf = Vec::new();
        let opt: Option<String> = Some("yo".to_string());
        opt.encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x00, 0x02, b'y', b'o']);
    }

    #[test]
    fn test_encode_option_string_none() {
        let mut buf = Vec::new();
        let opt: Option<String> = None;
        opt.encode(&mut buf).unwrap();
        // -1 as i16 (nullable string)
        assert_eq!(buf, vec![0xFF, 0xFF]);
    }

    #[test]
    fn test_encode_reference() {
        // Test that encoding via reference works
        let mut buf = Vec::new();
        let val = 42i32;
        val.encode(&mut buf).unwrap();
        assert_eq!(buf, vec![0x00, 0x00, 0x00, 0x2A]);
    }

    #[test]
    fn test_encode_string_overflow_returns_error() {
        let mut buf = Vec::new();
        let oversize: String = "a".repeat(i16::MAX as usize + 1);
        let err = oversize.encode(&mut buf).unwrap_err();
        match err {
            Error::Config(msg) => {
                assert!(msg.contains("exceeds wire field maximum"));
                assert!(msg.contains(&i16::MAX.to_string()));
            }
            other => panic!("expected Error::Config, got {:?}", other),
        }
    }

    #[test]
    fn test_encode_str_at_limit_succeeds() {
        let mut buf = Vec::new();
        let at_limit: String = "x".repeat(i16::MAX as usize);
        at_limit.encode(&mut buf).unwrap();
        assert_eq!(buf.len(), 2 + i16::MAX as usize);
    }
}
