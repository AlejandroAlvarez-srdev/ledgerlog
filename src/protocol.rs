//! Compact, length-prefixed binary wire protocol.
//!
//! ```text
//! request  := size:u32 | api:u8  | correlation_id:u32 | body
//! response := size:u32 | correlation_id:u32 | status:u8 | body
//! ```
//!
//! `size` counts the bytes after itself. Integers are big-endian.
//! `string := len:u16 | utf8`, `bytes := len:u32 | raw`,
//! `nullable_bytes := len:i32 (-1 = null) | raw`.
//!
//! | api | request body | response body (status 0) |
//! |-----|--------------|--------------------------|
//! | 0 Produce | topic:string, partition:u32, count:u32, count × (key:nullable_bytes, value:bytes) | base_offset:u64, last_offset:u64 |
//! | 1 Fetch | topic:string, partition:u32, offset:u64, max_bytes:u32, max_wait_ms:u32 | start_offset:u64, high_watermark:u64, records:bytes (raw frames) |
//! | 2 CommitOffset | group:string, topic:string, partition:u32, offset:u64 | — |
//! | 3 FetchOffset | group:string, topic:string, partition:u32 | offset:i64 (-1 = none) |
//! | 4 CreateTopic | topic:string, partitions:u32 | — |
//! | 5 Metadata | topic:string (empty = all) | count:u32, count × (name:string, n:u32, n × (partition:u32, start:u64, end:u64, size:u64, segments:u32)) |
//!
//! A non-zero status carries `message:string` (and for
//! `OFFSET_OUT_OF_RANGE`, `requested:u64, start:u64, end:u64` first).

use crate::broker::{PartitionMeta, TopicMeta};
use crate::error::{Error, Result};
use bytes::{BufMut, Bytes, BytesMut};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

/// Largest accepted protocol frame.
pub const MAX_FRAME_BYTES: usize = 64 * 1024 * 1024;

pub mod api {
    pub const PRODUCE: u8 = 0;
    pub const FETCH: u8 = 1;
    pub const COMMIT_OFFSET: u8 = 2;
    pub const FETCH_OFFSET: u8 = 3;
    pub const CREATE_TOPIC: u8 = 4;
    pub const METADATA: u8 = 5;
}

pub mod status {
    pub const OK: u8 = 0;
    pub const INTERNAL: u8 = 1;
    pub const OFFSET_OUT_OF_RANGE: u8 = 2;
    pub const UNKNOWN_TOPIC_OR_PARTITION: u8 = 3;
    pub const TOPIC_EXISTS: u8 = 4;
    pub const INVALID_REQUEST: u8 = 5;
    pub const RECORD_TOO_LARGE: u8 = 6;
    pub const CORRUPT: u8 = 7;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Request {
    Produce {
        topic: String,
        partition: u32,
        records: Vec<(Option<Bytes>, Bytes)>,
    },
    Fetch {
        topic: String,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        max_wait_ms: u32,
    },
    CommitOffset {
        group: String,
        topic: String,
        partition: u32,
        offset: u64,
    },
    FetchOffset {
        group: String,
        topic: String,
        partition: u32,
    },
    CreateTopic {
        topic: String,
        partitions: u32,
    },
    Metadata {
        topic: Option<String>,
    },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Response {
    Produce {
        base_offset: u64,
        last_offset: u64,
    },
    Fetch {
        start_offset: u64,
        high_watermark: u64,
        records: Bytes,
    },
    CommitOffset,
    FetchOffset {
        offset: Option<u64>,
    },
    CreateTopic,
    Metadata {
        topics: Vec<TopicMeta>,
    },
}

// ---------------------------------------------------------------- encoding

fn put_str(buf: &mut BytesMut, s: &str) {
    debug_assert!(s.len() <= u16::MAX as usize);
    buf.put_u16(s.len() as u16);
    buf.put_slice(s.as_bytes());
}

fn put_bytes(buf: &mut BytesMut, b: &[u8]) {
    buf.put_u32(b.len() as u32);
    buf.put_slice(b);
}

/// Append a produce request to `buf` from borrowed records, without copying
/// them into a [`Request`] first (used by the client hot path).
pub fn encode_produce<K: AsRef<[u8]>, V: AsRef<[u8]>>(
    buf: &mut BytesMut,
    correlation_id: u32,
    topic: &str,
    partition: u32,
    records: &[(Option<K>, V)],
) {
    let start = buf.len();
    buf.put_u32(0);
    buf.put_u8(api::PRODUCE);
    buf.put_u32(correlation_id);
    put_str(buf, topic);
    buf.put_u32(partition);
    buf.put_u32(records.len() as u32);
    for (k, v) in records {
        match k {
            Some(k) => {
                buf.put_i32(k.as_ref().len() as i32);
                buf.put_slice(k.as_ref());
            }
            None => buf.put_i32(-1),
        }
        put_bytes(buf, v.as_ref());
    }
    let size = (buf.len() - start - 4) as u32;
    buf[start..start + 4].copy_from_slice(&size.to_be_bytes());
}

impl Request {
    pub fn api_key(&self) -> u8 {
        match self {
            Request::Produce { .. } => api::PRODUCE,
            Request::Fetch { .. } => api::FETCH,
            Request::CommitOffset { .. } => api::COMMIT_OFFSET,
            Request::FetchOffset { .. } => api::FETCH_OFFSET,
            Request::CreateTopic { .. } => api::CREATE_TOPIC,
            Request::Metadata { .. } => api::METADATA,
        }
    }

    /// Encode as a complete frame (size prefix included).
    pub fn encode(&self, correlation_id: u32, buf: &mut BytesMut) {
        if let Request::Produce {
            topic,
            partition,
            records,
        } = self
        {
            return encode_produce(buf, correlation_id, topic, *partition, records);
        }
        let start = buf.len();
        buf.put_u32(0);
        buf.put_u8(self.api_key());
        buf.put_u32(correlation_id);
        match self {
            Request::Produce { .. } => unreachable!(),
            Request::Fetch {
                topic,
                partition,
                offset,
                max_bytes,
                max_wait_ms,
            } => {
                put_str(buf, topic);
                buf.put_u32(*partition);
                buf.put_u64(*offset);
                buf.put_u32(*max_bytes);
                buf.put_u32(*max_wait_ms);
            }
            Request::CommitOffset {
                group,
                topic,
                partition,
                offset,
            } => {
                put_str(buf, group);
                put_str(buf, topic);
                buf.put_u32(*partition);
                buf.put_u64(*offset);
            }
            Request::FetchOffset {
                group,
                topic,
                partition,
            } => {
                put_str(buf, group);
                put_str(buf, topic);
                buf.put_u32(*partition);
            }
            Request::CreateTopic { topic, partitions } => {
                put_str(buf, topic);
                buf.put_u32(*partitions);
            }
            Request::Metadata { topic } => put_str(buf, topic.as_deref().unwrap_or("")),
        }
        let size = (buf.len() - start - 4) as u32;
        buf[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Decode a request body (the frame minus its size prefix).
    /// Returns `(correlation_id, request)`.
    pub fn decode(frame: Bytes) -> Result<(u32, Request)> {
        let mut r = Reader::new(frame);
        let api_key = r.u8()?;
        let correlation_id = r.u32()?;
        let req = match api_key {
            api::PRODUCE => {
                let topic = r.string()?;
                let partition = r.u32()?;
                let count = r.u32()? as usize;
                // Each record needs at least 8 bytes; reject absurd counts early.
                if count > r.remaining() / 8 {
                    return Err(Error::Protocol("record count exceeds frame".into()));
                }
                let mut records = Vec::with_capacity(count);
                for _ in 0..count {
                    let key = r.nullable_bytes()?;
                    let value = r.bytes()?;
                    records.push((key, value));
                }
                Request::Produce {
                    topic,
                    partition,
                    records,
                }
            }
            api::FETCH => Request::Fetch {
                topic: r.string()?,
                partition: r.u32()?,
                offset: r.u64()?,
                max_bytes: r.u32()?,
                max_wait_ms: r.u32()?,
            },
            api::COMMIT_OFFSET => Request::CommitOffset {
                group: r.string()?,
                topic: r.string()?,
                partition: r.u32()?,
                offset: r.u64()?,
            },
            api::FETCH_OFFSET => Request::FetchOffset {
                group: r.string()?,
                topic: r.string()?,
                partition: r.u32()?,
            },
            api::CREATE_TOPIC => Request::CreateTopic {
                topic: r.string()?,
                partitions: r.u32()?,
            },
            api::METADATA => {
                let t = r.string()?;
                Request::Metadata {
                    topic: (!t.is_empty()).then_some(t),
                }
            }
            other => return Err(Error::Protocol(format!("unknown api key {other}"))),
        };
        r.finish()?;
        Ok((correlation_id, req))
    }
}

impl Response {
    pub fn encode(&self, correlation_id: u32, buf: &mut BytesMut) {
        let start = buf.len();
        buf.put_u32(0);
        buf.put_u32(correlation_id);
        buf.put_u8(status::OK);
        match self {
            Response::Produce {
                base_offset,
                last_offset,
            } => {
                buf.put_u64(*base_offset);
                buf.put_u64(*last_offset);
            }
            Response::Fetch {
                start_offset,
                high_watermark,
                records,
            } => {
                buf.put_u64(*start_offset);
                buf.put_u64(*high_watermark);
                put_bytes(buf, records);
            }
            Response::CommitOffset | Response::CreateTopic => {}
            Response::FetchOffset { offset } => {
                buf.put_i64(offset.map(|o| o as i64).unwrap_or(-1));
            }
            Response::Metadata { topics } => {
                buf.put_u32(topics.len() as u32);
                for t in topics {
                    put_str(buf, &t.name);
                    buf.put_u32(t.partitions.len() as u32);
                    for p in &t.partitions {
                        buf.put_u32(p.partition);
                        buf.put_u64(p.start_offset);
                        buf.put_u64(p.end_offset);
                        buf.put_u64(p.size_bytes);
                        buf.put_u32(p.segments);
                    }
                }
            }
        }
        let size = (buf.len() - start - 4) as u32;
        buf[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Encode an error response.
    pub fn encode_error(err: &Error, correlation_id: u32, buf: &mut BytesMut) {
        let start = buf.len();
        buf.put_u32(0);
        buf.put_u32(correlation_id);
        let code = match err {
            Error::OffsetOutOfRange { .. } => status::OFFSET_OUT_OF_RANGE,
            Error::UnknownTopicOrPartition { .. } => status::UNKNOWN_TOPIC_OR_PARTITION,
            Error::TopicExists(_) => status::TOPIC_EXISTS,
            Error::InvalidRequest(_) | Error::Protocol(_) => status::INVALID_REQUEST,
            Error::RecordTooLarge { .. } => status::RECORD_TOO_LARGE,
            Error::Corrupt { .. } => status::CORRUPT,
            Error::Io(_) | Error::Server { .. } => status::INTERNAL,
        };
        buf.put_u8(code);
        if let Error::OffsetOutOfRange {
            requested,
            start: s,
            end,
        } = err
        {
            buf.put_u64(*requested);
            buf.put_u64(*s);
            buf.put_u64(*end);
        }
        let msg = err.to_string();
        let msg = &msg[..msg.len().min(1024)];
        put_str(buf, msg);
        let size = (buf.len() - start - 4) as u32;
        buf[start..start + 4].copy_from_slice(&size.to_be_bytes());
    }

    /// Decode a response body for a request with the given api key.
    /// Returns `(correlation_id, result)`; server-side errors become `Err`.
    pub fn decode(api_key: u8, frame: Bytes) -> Result<(u32, Result<Response>)> {
        let mut r = Reader::new(frame);
        let correlation_id = r.u32()?;
        let code = r.u8()?;
        if code != status::OK {
            let err = if code == status::OFFSET_OUT_OF_RANGE {
                let requested = r.u64()?;
                let start = r.u64()?;
                let end = r.u64()?;
                Error::OffsetOutOfRange {
                    requested,
                    start,
                    end,
                }
            } else {
                Error::Server {
                    code,
                    message: r.string()?,
                }
            };
            return Ok((correlation_id, Err(err)));
        }
        let resp = match api_key {
            api::PRODUCE => Response::Produce {
                base_offset: r.u64()?,
                last_offset: r.u64()?,
            },
            api::FETCH => Response::Fetch {
                start_offset: r.u64()?,
                high_watermark: r.u64()?,
                records: r.bytes()?,
            },
            api::COMMIT_OFFSET => Response::CommitOffset,
            api::FETCH_OFFSET => {
                let o = r.i64()?;
                Response::FetchOffset {
                    offset: (o >= 0).then_some(o as u64),
                }
            }
            api::CREATE_TOPIC => Response::CreateTopic,
            api::METADATA => {
                let n = r.u32()? as usize;
                let mut topics = Vec::with_capacity(n.min(1024));
                for _ in 0..n {
                    let name = r.string()?;
                    let np = r.u32()? as usize;
                    let mut partitions = Vec::with_capacity(np.min(1024));
                    for _ in 0..np {
                        partitions.push(PartitionMeta {
                            partition: r.u32()?,
                            start_offset: r.u64()?,
                            end_offset: r.u64()?,
                            size_bytes: r.u64()?,
                            segments: r.u32()?,
                        });
                    }
                    topics.push(TopicMeta { name, partitions });
                }
                Response::Metadata { topics }
            }
            other => return Err(Error::Protocol(format!("unknown api key {other}"))),
        };
        r.finish()?;
        Ok((correlation_id, Ok(resp)))
    }
}

// ---------------------------------------------------------------- decoding

/// Bounds-checked big-endian reader over a frame.
struct Reader {
    buf: Bytes,
    pos: usize,
}

impl Reader {
    fn new(buf: Bytes) -> Self {
        Self { buf, pos: 0 }
    }

    fn remaining(&self) -> usize {
        self.buf.len() - self.pos
    }

    fn take(&mut self, n: usize) -> Result<&[u8]> {
        if self.remaining() < n {
            return Err(Error::Protocol(format!(
                "frame truncated: need {n} bytes at {}, have {}",
                self.pos,
                self.remaining()
            )));
        }
        let s = &self.buf[self.pos..self.pos + n];
        self.pos += n;
        Ok(s)
    }

    fn u8(&mut self) -> Result<u8> {
        Ok(self.take(1)?[0])
    }
    fn u16(&mut self) -> Result<u16> {
        Ok(u16::from_be_bytes(self.take(2)?.try_into().unwrap()))
    }
    fn u32(&mut self) -> Result<u32> {
        Ok(u32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }
    fn u64(&mut self) -> Result<u64> {
        Ok(u64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }
    fn i64(&mut self) -> Result<i64> {
        Ok(self.u64()? as i64)
    }

    fn slice(&mut self, n: usize) -> Result<Bytes> {
        if self.remaining() < n {
            return Err(Error::Protocol("frame truncated".into()));
        }
        let b = self.buf.slice(self.pos..self.pos + n);
        self.pos += n;
        Ok(b)
    }

    fn string(&mut self) -> Result<String> {
        let n = self.u16()? as usize;
        let raw = self.take(n)?;
        String::from_utf8(raw.to_vec()).map_err(|_| Error::Protocol("invalid utf-8".into()))
    }

    fn bytes(&mut self) -> Result<Bytes> {
        let n = self.u32()? as usize;
        self.slice(n)
    }

    fn nullable_bytes(&mut self) -> Result<Option<Bytes>> {
        let n = self.u32()? as i32;
        match n {
            -1 => Ok(None),
            n if n < 0 => Err(Error::Protocol(format!("negative length {n}"))),
            n => self.slice(n as usize).map(Some),
        }
    }

    fn finish(&self) -> Result<()> {
        if self.remaining() != 0 {
            return Err(Error::Protocol(format!(
                "{} trailing bytes in frame",
                self.remaining()
            )));
        }
        Ok(())
    }
}

/// Read one size-prefixed frame. Returns `None` on a clean EOF between frames.
pub async fn read_frame<R: AsyncRead + Unpin>(r: &mut R) -> Result<Option<Bytes>> {
    let mut len = [0u8; 4];
    match r.read_exact(&mut len).await {
        Ok(_) => {}
        Err(e) if e.kind() == std::io::ErrorKind::UnexpectedEof => return Ok(None),
        Err(e) => return Err(e.into()),
    }
    let len = u32::from_be_bytes(len) as usize;
    if len > MAX_FRAME_BYTES {
        return Err(Error::Protocol(format!(
            "frame of {len} bytes exceeds limit"
        )));
    }
    let mut buf = BytesMut::zeroed(len);
    r.read_exact(&mut buf).await?;
    Ok(Some(buf.freeze()))
}

pub async fn write_all<W: AsyncWrite + Unpin>(w: &mut W, buf: &[u8]) -> Result<()> {
    w.write_all(buf).await?;
    w.flush().await?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn roundtrip_request(req: Request) {
        let mut buf = BytesMut::new();
        req.encode(42, &mut buf);
        let frame = buf.freeze().slice(4..);
        let (cid, back) = Request::decode(frame).unwrap();
        assert_eq!(cid, 42);
        assert_eq!(back, req);
    }

    fn roundtrip_response(api_key: u8, resp: Response) {
        let mut buf = BytesMut::new();
        resp.encode(7, &mut buf);
        let (cid, back) = Response::decode(api_key, buf.freeze().slice(4..)).unwrap();
        assert_eq!(cid, 7);
        assert_eq!(back.unwrap(), resp);
    }

    #[test]
    fn requests_roundtrip() {
        roundtrip_request(Request::Produce {
            topic: "t".into(),
            partition: 3,
            records: vec![
                (None, Bytes::from_static(b"v1")),
                (Some(Bytes::from_static(b"k")), Bytes::from_static(b"")),
            ],
        });
        roundtrip_request(Request::Fetch {
            topic: "t".into(),
            partition: 0,
            offset: 99,
            max_bytes: 1024,
            max_wait_ms: 500,
        });
        roundtrip_request(Request::CommitOffset {
            group: "g".into(),
            topic: "t".into(),
            partition: 1,
            offset: 5,
        });
        roundtrip_request(Request::FetchOffset {
            group: "g".into(),
            topic: "t".into(),
            partition: 1,
        });
        roundtrip_request(Request::CreateTopic {
            topic: "t".into(),
            partitions: 4,
        });
        roundtrip_request(Request::Metadata { topic: None });
        roundtrip_request(Request::Metadata {
            topic: Some("t".into()),
        });
    }

    #[test]
    fn responses_roundtrip() {
        roundtrip_response(
            api::PRODUCE,
            Response::Produce {
                base_offset: 1,
                last_offset: 9,
            },
        );
        roundtrip_response(
            api::FETCH,
            Response::Fetch {
                start_offset: 0,
                high_watermark: 10,
                records: Bytes::from_static(b"raw"),
            },
        );
        roundtrip_response(api::FETCH_OFFSET, Response::FetchOffset { offset: None });
        roundtrip_response(api::FETCH_OFFSET, Response::FetchOffset { offset: Some(3) });
        roundtrip_response(
            api::METADATA,
            Response::Metadata {
                topics: vec![TopicMeta {
                    name: "t".into(),
                    partitions: vec![PartitionMeta {
                        partition: 0,
                        start_offset: 1,
                        end_offset: 2,
                        size_bytes: 3,
                        segments: 4,
                    }],
                }],
            },
        );
    }

    #[test]
    fn errors_roundtrip() {
        let mut buf = BytesMut::new();
        Response::encode_error(
            &Error::OffsetOutOfRange {
                requested: 1,
                start: 5,
                end: 9,
            },
            3,
            &mut buf,
        );
        let (_, r) = Response::decode(api::FETCH, buf.freeze().slice(4..)).unwrap();
        assert!(matches!(
            r,
            Err(Error::OffsetOutOfRange {
                requested: 1,
                start: 5,
                end: 9
            })
        ));
    }

    #[test]
    fn malformed_frames_are_rejected() {
        assert!(Request::decode(Bytes::from_static(&[9, 0, 0, 0, 1])).is_err());
        assert!(Request::decode(Bytes::from_static(&[api::FETCH, 0, 0])).is_err());
        // Produce claiming a billion records in a tiny frame.
        let mut b = BytesMut::new();
        b.put_u8(api::PRODUCE);
        b.put_u32(1);
        put_str(&mut b, "t");
        b.put_u32(0);
        b.put_u32(1_000_000_000);
        assert!(Request::decode(b.freeze()).is_err());
    }
}
