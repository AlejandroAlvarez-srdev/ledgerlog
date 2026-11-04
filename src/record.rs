//! The record frame — identical on disk and on the wire.
//!
//! ```text
//! +---------+---------+----------+-------------+---------+-----+-----------+-------+
//! | len u32 | crc u32 | offset   | timestamp   | key_len | key | value_len | value |
//! |         |         | u64      | i64 (ms)    | i32     |     | u32       |       |
//! +---------+---------+----------+-------------+---------+-----+-----------+-------+
//!           |<------------------------- len bytes ---------------------------------->|
//!                     |<------------------- covered by crc32 ------------------------>|
//! ```
//!
//! All integers are big-endian. `key_len == -1` encodes a null key. Because a
//! fetch can ship raw segment bytes to clients, the frame carries its own
//! absolute offset.

use crate::error::{Error, Result};
use bytes::Bytes;

/// Size of the `len` prefix.
pub const LEN_BYTES: usize = 4;
/// Bytes covered by `len` that precede the key: crc + offset + timestamp + key_len.
const FIXED_AFTER_LEN: usize = 4 + 8 + 8 + 4;
/// Fixed per-record overhead: len + crc + offset + timestamp + key_len + value_len.
pub const HEADER_BYTES: usize = LEN_BYTES + FIXED_AFTER_LEN + 4;
/// Smallest legal value of the `len` field (empty key and value).
pub const MIN_LEN_FIELD: u32 = (HEADER_BYTES - LEN_BYTES) as u32;

/// A single log record.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Record {
    pub offset: u64,
    /// Milliseconds since the Unix epoch, assigned by the broker on append.
    pub timestamp: i64,
    pub key: Option<Bytes>,
    pub value: Bytes,
}

impl Record {
    /// Total encoded size of a record with the given key/value lengths.
    pub fn encoded_len_for(key_len: Option<usize>, value_len: usize) -> usize {
        HEADER_BYTES + key_len.unwrap_or(0) + value_len
    }

    pub fn encoded_len(&self) -> usize {
        Self::encoded_len_for(self.key.as_ref().map(|k| k.len()), self.value.len())
    }

    /// Append the encoded frame to `buf`.
    pub fn encode_into(&self, buf: &mut Vec<u8>) {
        encode_parts(
            buf,
            self.offset,
            self.timestamp,
            self.key.as_deref(),
            &self.value,
        );
    }

    pub fn encode(&self) -> Vec<u8> {
        let mut buf = Vec::with_capacity(self.encoded_len());
        self.encode_into(&mut buf);
        buf
    }
}

/// Encode a frame from borrowed parts (avoids building a [`Record`]).
pub fn encode_parts(
    buf: &mut Vec<u8>,
    offset: u64,
    timestamp: i64,
    key: Option<&[u8]>,
    value: &[u8],
) {
    let total = Record::encoded_len_for(key.map(<[u8]>::len), value.len());
    let start = buf.len();
    buf.reserve(total);
    buf.extend_from_slice(&((total - LEN_BYTES) as u32).to_be_bytes());
    buf.extend_from_slice(&[0u8; 4]); // crc placeholder
    buf.extend_from_slice(&offset.to_be_bytes());
    buf.extend_from_slice(&timestamp.to_be_bytes());
    match key {
        Some(k) => {
            buf.extend_from_slice(&(k.len() as i32).to_be_bytes());
            buf.extend_from_slice(k);
        }
        None => buf.extend_from_slice(&(-1i32).to_be_bytes()),
    }
    buf.extend_from_slice(&(value.len() as u32).to_be_bytes());
    buf.extend_from_slice(value);
    let crc = crc32fast::hash(&buf[start + 8..]);
    buf[start + 4..start + 8].copy_from_slice(&crc.to_be_bytes());
}

/// Outcome of trying to decode one frame from the front of a buffer.
#[derive(Debug, PartialEq, Eq)]
pub enum Decoded {
    /// A valid record and the number of bytes it occupied.
    Record(Record, usize),
    /// The buffer ends before the frame does (a torn write, or a short read).
    Incomplete,
}

fn be_u32(b: &[u8]) -> u32 {
    u32::from_be_bytes(b[..4].try_into().unwrap())
}
fn be_u64(b: &[u8]) -> u64 {
    u64::from_be_bytes(b[..8].try_into().unwrap())
}

/// Read only the `len` field: returns the full frame size if it is sane.
pub fn peek_frame_len(buf: &[u8], max_record_bytes: usize) -> Result<Option<usize>> {
    if buf.len() < LEN_BYTES {
        return Ok(None);
    }
    let len = be_u32(buf);
    if len < MIN_LEN_FIELD || len as usize + LEN_BYTES > max_record_bytes {
        return Err(Error::Corrupt {
            position: 0,
            reason: format!("implausible frame length {len}"),
        });
    }
    Ok(Some(len as usize + LEN_BYTES))
}

/// Decode one frame from the front of `buf`, verifying its CRC and internal
/// lengths. `max_record_bytes` bounds the `len` field so garbage cannot make
/// us wait for (or allocate) gigabytes.
pub fn decode(buf: &[u8], max_record_bytes: usize) -> Result<Decoded> {
    let total = match peek_frame_len(buf, max_record_bytes)? {
        Some(t) => t,
        None => return Ok(Decoded::Incomplete),
    };
    if buf.len() < total {
        return Ok(Decoded::Incomplete);
    }
    let frame = &buf[..total];
    let stored_crc = be_u32(&frame[4..]);
    let actual_crc = crc32fast::hash(&frame[8..]);
    if stored_crc != actual_crc {
        return Err(corrupt(format!(
            "crc mismatch (stored {stored_crc:#010x}, computed {actual_crc:#010x})"
        )));
    }
    let offset = be_u64(&frame[8..]);
    let timestamp = be_u64(&frame[16..]) as i64;
    let key_len = be_u32(&frame[24..]) as i32;
    let mut pos = 28;
    let key = if key_len == -1 {
        None
    } else if key_len < 0 {
        return Err(corrupt(format!("negative key length {key_len}")));
    } else {
        let k = key_len as usize;
        if pos + k + 4 > total {
            return Err(corrupt("key length overflows frame".into()));
        }
        let key = Bytes::copy_from_slice(&frame[pos..pos + k]);
        pos += k;
        Some(key)
    };
    if pos + 4 > total {
        return Err(corrupt("frame too short for value length".into()));
    }
    let value_len = be_u32(&frame[pos..]) as usize;
    pos += 4;
    if pos + value_len != total {
        return Err(corrupt("value length does not match frame length".into()));
    }
    let value = Bytes::copy_from_slice(&frame[pos..total]);
    Ok(Decoded::Record(
        Record {
            offset,
            timestamp,
            key,
            value,
        },
        total,
    ))
}

fn corrupt(reason: String) -> Error {
    Error::Corrupt {
        position: 0,
        reason,
    }
}

/// Decode a buffer of back-to-back frames. A trailing incomplete frame is an
/// error here: fetch responses always contain whole frames.
pub fn decode_all(mut buf: &[u8], max_record_bytes: usize) -> Result<Vec<Record>> {
    let mut out = Vec::new();
    let mut consumed = 0u64;
    while !buf.is_empty() {
        match decode(buf, max_record_bytes).map_err(|e| with_position(e, consumed))? {
            Decoded::Record(r, n) => {
                out.push(r);
                buf = &buf[n..];
                consumed += n as u64;
            }
            Decoded::Incomplete => {
                return Err(Error::Corrupt {
                    position: consumed,
                    reason: "truncated frame".into(),
                })
            }
        }
    }
    Ok(out)
}

/// Header fields readable without touching the payload.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct FrameHeader {
    /// Full frame size including the `len` prefix.
    pub size: usize,
    pub offset: u64,
    pub timestamp: i64,
}

/// Number of leading bytes needed by [`parse_header`].
pub const PEEK_HEADER_BYTES: usize = 24;

/// Parse `len`, `offset` and `timestamp` from the first 24 bytes of a frame
/// (no CRC check — used to skip through already-validated segments).
pub fn parse_header(buf: &[u8; PEEK_HEADER_BYTES]) -> FrameHeader {
    FrameHeader {
        size: be_u32(buf) as usize + LEN_BYTES,
        offset: be_u64(&buf[8..]),
        timestamp: be_u64(&buf[16..]) as i64,
    }
}

pub(crate) fn with_position(err: Error, base: u64) -> Error {
    match err {
        Error::Corrupt { position, reason } => Error::Corrupt {
            position: base + position,
            reason,
        },
        other => other,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn rec(offset: u64, key: Option<&str>, value: &str) -> Record {
        Record {
            offset,
            timestamp: 1_700_000_000_000,
            key: key.map(|k| Bytes::copy_from_slice(k.as_bytes())),
            value: Bytes::copy_from_slice(value.as_bytes()),
        }
    }

    #[test]
    fn roundtrip_with_and_without_key() {
        for r in [rec(7, Some("user-1"), "hello"), rec(8, None, "")] {
            let bytes = r.encode();
            assert_eq!(bytes.len(), r.encoded_len());
            assert_eq!(
                decode(&bytes, 1 << 20).unwrap(),
                Decoded::Record(r.clone(), bytes.len())
            );
        }
    }

    #[test]
    fn empty_key_is_distinct_from_null_key() {
        let r = rec(1, Some(""), "v");
        let Decoded::Record(back, _) = decode(&r.encode(), 1 << 20).unwrap() else {
            panic!("expected record")
        };
        assert_eq!(back.key.as_deref(), Some(&b""[..]));
    }

    #[test]
    fn truncated_frame_is_incomplete() {
        let bytes = rec(1, Some("k"), "value").encode();
        for cut in 0..bytes.len() {
            assert_eq!(decode(&bytes[..cut], 1 << 20).unwrap(), Decoded::Incomplete);
        }
    }

    #[test]
    fn flipped_payload_bit_fails_crc() {
        let mut bytes = rec(1, Some("k"), "value").encode();
        let last = bytes.len() - 1;
        bytes[last] ^= 0x01;
        assert!(matches!(
            decode(&bytes, 1 << 20),
            Err(Error::Corrupt { .. })
        ));
    }

    #[test]
    fn absurd_length_is_rejected() {
        let mut bytes = rec(1, None, "v").encode();
        bytes[..4].copy_from_slice(&u32::MAX.to_be_bytes());
        assert!(decode(&bytes, 1 << 20).is_err());
    }

    #[test]
    fn header_peek_matches_full_decode() {
        let r = rec(42, Some("key"), "value");
        let bytes = r.encode();
        let h = parse_header(bytes[..PEEK_HEADER_BYTES].try_into().unwrap());
        assert_eq!(h.size, bytes.len());
        assert_eq!(h.offset, 42);
        assert_eq!(h.timestamp, r.timestamp);
    }

    #[test]
    fn decode_all_reports_position_of_bad_frame() {
        let mut buf = rec(0, None, "a").encode();
        let second = buf.len();
        rec(1, None, "b").encode_into(&mut buf);
        let last = buf.len() - 1;
        buf[last] ^= 0xff;
        match decode_all(&buf, 1 << 20) {
            Err(Error::Corrupt { position, .. }) => assert_eq!(position, second as u64),
            other => panic!("unexpected {other:?}"),
        }
    }
}
