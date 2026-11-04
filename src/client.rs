//! Async client and a simple single-partition consumer.

use crate::broker::TopicMeta;
use crate::error::{Error, Result};
use crate::protocol::{self, api, Request, Response};
use crate::record::{decode_all, Record};
use bytes::BytesMut;
use std::time::Duration;
use tokio::net::tcp::{OwnedReadHalf, OwnedWriteHalf};
use tokio::net::{TcpStream, ToSocketAddrs};

/// Records returned by [`Client::fetch`].
#[derive(Debug, Clone)]
pub struct FetchedBatch {
    pub start_offset: u64,
    pub high_watermark: u64,
    pub records: Vec<Record>,
}

/// One TCP connection to a ledgerlog server. Requests are issued one at a
/// time; open several clients for concurrency.
pub struct Client {
    rd: OwnedReadHalf,
    wr: OwnedWriteHalf,
    buf: BytesMut,
    next_id: u32,
    max_record_bytes: usize,
}

impl Client {
    pub async fn connect(addr: impl ToSocketAddrs) -> Result<Self> {
        let stream = TcpStream::connect(addr).await?;
        stream.set_nodelay(true)?;
        let (rd, wr) = stream.into_split();
        Ok(Self {
            rd,
            wr,
            buf: BytesMut::with_capacity(64 * 1024),
            next_id: 1,
            max_record_bytes: protocol::MAX_FRAME_BYTES,
        })
    }

    async fn roundtrip(&mut self, api_key: u8) -> Result<Response> {
        let cid = self.next_id.wrapping_sub(1);
        protocol::write_all(&mut self.wr, &self.buf).await?;
        let frame = protocol::read_frame(&mut self.rd)
            .await?
            .ok_or_else(|| Error::Protocol("server closed the connection".into()))?;
        let (got, result) = Response::decode(api_key, frame)?;
        if got != cid {
            return Err(Error::Protocol(format!(
                "correlation id mismatch: sent {cid}, got {got}"
            )));
        }
        result
    }

    async fn call(&mut self, req: Request) -> Result<Response> {
        self.buf.clear();
        let cid = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        req.encode(cid, &mut self.buf);
        self.roundtrip(req.api_key()).await
    }

    /// Append a batch; returns `(base_offset, last_offset)`.
    pub async fn produce<K, V>(
        &mut self,
        topic: &str,
        partition: u32,
        records: &[(Option<K>, V)],
    ) -> Result<(u64, u64)>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.buf.clear();
        let cid = self.next_id;
        self.next_id = self.next_id.wrapping_add(1);
        protocol::encode_produce(&mut self.buf, cid, topic, partition, records);
        match self.roundtrip(api::PRODUCE).await? {
            Response::Produce {
                base_offset,
                last_offset,
            } => Ok((base_offset, last_offset)),
            other => Err(unexpected(other)),
        }
    }

    /// Fetch whole records starting at `offset`. If nothing is available the
    /// server waits up to `max_wait` for new data (long poll).
    pub async fn fetch(
        &mut self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: u32,
        max_wait: Duration,
    ) -> Result<FetchedBatch> {
        let resp = self
            .call(Request::Fetch {
                topic: topic.to_string(),
                partition,
                offset,
                max_bytes,
                max_wait_ms: max_wait.as_millis().min(u128::from(u32::MAX)) as u32,
            })
            .await?;
        match resp {
            Response::Fetch {
                start_offset,
                high_watermark,
                records,
            } => Ok(FetchedBatch {
                start_offset,
                high_watermark,
                records: decode_all(&records, self.max_record_bytes)?,
            }),
            other => Err(unexpected(other)),
        }
    }

    pub async fn commit_offset(
        &mut self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        match self
            .call(Request::CommitOffset {
                group: group.into(),
                topic: topic.into(),
                partition,
                offset,
            })
            .await?
        {
            Response::CommitOffset => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    pub async fn fetch_offset(
        &mut self,
        group: &str,
        topic: &str,
        partition: u32,
    ) -> Result<Option<u64>> {
        match self
            .call(Request::FetchOffset {
                group: group.into(),
                topic: topic.into(),
                partition,
            })
            .await?
        {
            Response::FetchOffset { offset } => Ok(offset),
            other => Err(unexpected(other)),
        }
    }

    pub async fn create_topic(&mut self, topic: &str, partitions: u32) -> Result<()> {
        match self
            .call(Request::CreateTopic {
                topic: topic.into(),
                partitions,
            })
            .await?
        {
            Response::CreateTopic => Ok(()),
            other => Err(unexpected(other)),
        }
    }

    pub async fn metadata(&mut self, topic: Option<&str>) -> Result<Vec<TopicMeta>> {
        match self
            .call(Request::Metadata {
                topic: topic.map(str::to_string),
            })
            .await?
        {
            Response::Metadata { topics } => Ok(topics),
            other => Err(unexpected(other)),
        }
    }
}

fn unexpected(resp: Response) -> Error {
    Error::Protocol(format!("unexpected response {resp:?}"))
}

/// Where a consumer without a committed offset starts reading.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StartFrom {
    Earliest,
    Latest,
    Offset(u64),
}

impl std::str::FromStr for StartFrom {
    type Err = String;
    fn from_str(s: &str) -> std::result::Result<Self, Self::Err> {
        match s {
            "earliest" => Ok(StartFrom::Earliest),
            "latest" => Ok(StartFrom::Latest),
            n => n
                .parse()
                .map(StartFrom::Offset)
                .map_err(|_| format!("expected earliest|latest|<offset>, got {n:?}")),
        }
    }
}

/// A consumer for one partition, optionally tracking its position in a
/// consumer group (committed offset = next offset to read).
pub struct Consumer {
    client: Client,
    group: Option<String>,
    topic: String,
    partition: u32,
    position: u64,
    pub max_bytes: u32,
    pub max_wait: Duration,
}

impl Consumer {
    /// Resolve the starting position: the group's committed offset if there
    /// is one, otherwise `start`.
    pub async fn new(
        mut client: Client,
        group: Option<&str>,
        topic: &str,
        partition: u32,
        start: StartFrom,
    ) -> Result<Self> {
        let committed = match group {
            Some(g) => client.fetch_offset(g, topic, partition).await?,
            None => None,
        };
        let position = match committed {
            Some(o) => o,
            None => resolve(&mut client, topic, partition, start).await?,
        };
        Ok(Self {
            client,
            group: group.map(str::to_string),
            topic: topic.to_string(),
            partition,
            position,
            max_bytes: 1024 * 1024,
            max_wait: Duration::from_millis(500),
        })
    }

    pub fn position(&self) -> u64 {
        self.position
    }

    /// Fetch the next records, long-polling up to `max_wait`. If the position
    /// fell behind retention, it is reset to the earliest available offset.
    pub async fn poll(&mut self) -> Result<Vec<Record>> {
        let batch = match self
            .client
            .fetch(
                &self.topic,
                self.partition,
                self.position,
                self.max_bytes,
                self.max_wait,
            )
            .await
        {
            Ok(b) => b,
            Err(Error::OffsetOutOfRange { start, end, .. }) => {
                let reset = if self.position < start { start } else { end };
                tracing::warn!(
                    from = self.position,
                    to = reset,
                    "offset out of range, resetting"
                );
                self.position = reset;
                return Ok(Vec::new());
            }
            Err(e) => return Err(e),
        };
        if let Some(last) = batch.records.last() {
            self.position = last.offset + 1;
        }
        Ok(batch.records)
    }

    /// Commit the current position for the consumer group (no-op without a group).
    pub async fn commit(&mut self) -> Result<()> {
        if let Some(g) = &self.group {
            self.client
                .commit_offset(g, &self.topic, self.partition, self.position)
                .await?;
        }
        Ok(())
    }

    pub fn into_client(self) -> Client {
        self.client
    }
}

async fn resolve(
    client: &mut Client,
    topic: &str,
    partition: u32,
    start: StartFrom,
) -> Result<u64> {
    if let StartFrom::Offset(o) = start {
        return Ok(o);
    }
    let meta = client.metadata(Some(topic)).await?;
    let p = meta
        .first()
        .and_then(|t| t.partitions.get(partition as usize))
        .ok_or_else(|| Error::UnknownTopicOrPartition {
            topic: topic.to_string(),
            partition,
        })?;
    Ok(match start {
        StartFrom::Earliest => p.start_offset,
        _ => p.end_offset,
    })
}
