//! The broker: topics → partitions → [`Log`]s, plus consumer-group offsets.
//!
//! On-disk layout:
//!
//! ```text
//! <data_dir>/
//! ├── consumer_offsets.log
//! └── topics/
//!     └── <topic>/
//!         ├── 0/   00000000000000000000.log  00000000000000000000.index ...
//!         └── 1/   ...
//! ```
//!
//! All methods are synchronous (they do file I/O); the server calls them from
//! `spawn_blocking`. Each partition has its own `RwLock`, so producers to
//! different partitions never contend, and readers only block writers for the
//! duration of a positional read. A `tokio::sync::watch` channel per partition
//! publishes the high watermark so fetches can long-poll.

use crate::config::{FsyncPolicy, LogConfig};
use crate::error::{Error, Result};
use crate::log::{now_ms, AppendInfo, Log};
use crate::offsets::OffsetStore;
use parking_lot::{Mutex, RwLock};
use std::collections::BTreeMap;
use std::fs;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use tokio::sync::watch;

#[derive(Debug, Clone)]
pub struct BrokerConfig {
    pub data_dir: PathBuf,
    pub log: LogConfig,
    /// Partitions for topics created implicitly by a produce request.
    pub default_partitions: u32,
    pub auto_create_topics: bool,
}

impl BrokerConfig {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            data_dir: data_dir.into(),
            log: LogConfig::default(),
            default_partitions: 1,
            auto_create_topics: true,
        }
    }
}

pub struct Partition {
    log: RwLock<Log>,
    high_watermark: watch::Sender<u64>,
}

impl Partition {
    fn new(log: Log) -> Self {
        let (tx, _) = watch::channel(log.next_offset());
        Self {
            log: RwLock::new(log),
            high_watermark: tx,
        }
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.high_watermark.subscribe()
    }

    pub fn log(&self) -> &RwLock<Log> {
        &self.log
    }
}

pub struct Topic {
    pub name: String,
    pub partitions: Vec<Arc<Partition>>,
}

/// Data returned by a fetch.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct FetchResult {
    pub start_offset: u64,
    pub high_watermark: u64,
    /// Raw, whole record frames.
    pub records: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PartitionMeta {
    pub partition: u32,
    pub start_offset: u64,
    pub end_offset: u64,
    pub size_bytes: u64,
    pub segments: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TopicMeta {
    pub name: String,
    pub partitions: Vec<PartitionMeta>,
}

pub struct Broker {
    config: BrokerConfig,
    topics: RwLock<BTreeMap<String, Arc<Topic>>>,
    offsets: Mutex<OffsetStore>,
}

pub fn validate_topic_name(name: &str) -> Result<()> {
    let ok = !name.is_empty()
        && name.len() <= 200
        && name != "."
        && name != ".."
        && name
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'.' || b == b'_' || b == b'-');
    if ok {
        Ok(())
    } else {
        Err(Error::InvalidRequest(format!(
            "invalid topic name {name:?} (use 1-200 chars of [A-Za-z0-9._-])"
        )))
    }
}

impl Broker {
    /// Open the broker, recovering every partition found under `data_dir`.
    pub fn open(config: BrokerConfig) -> Result<Self> {
        let topics_dir = config.data_dir.join("topics");
        fs::create_dir_all(&topics_dir)?;
        let mut topics = BTreeMap::new();
        for entry in fs::read_dir(&topics_dir)? {
            let entry = entry?;
            if !entry.file_type()?.is_dir() {
                continue;
            }
            let name = entry.file_name().to_string_lossy().into_owned();
            if validate_topic_name(&name).is_err() {
                continue;
            }
            let mut ids: Vec<u32> = fs::read_dir(entry.path())?
                .filter_map(|e| e.ok())
                .filter_map(|e| e.file_name().to_str().and_then(|s| s.parse().ok()))
                .collect();
            ids.sort_unstable();
            if ids.is_empty() || ids != (0..ids.len() as u32).collect::<Vec<_>>() {
                return Err(Error::Corrupt {
                    position: 0,
                    reason: format!("topic {name}: partition directories are not 0..n"),
                });
            }
            let mut partitions = Vec::with_capacity(ids.len());
            for id in ids {
                let log = Log::open(entry.path().join(id.to_string()), config.log.clone())?;
                let rec = log.recovery();
                if rec.active.truncated_bytes > 0 {
                    tracing::warn!(
                        topic = %name,
                        partition = id,
                        truncated_bytes = rec.active.truncated_bytes,
                        "recovered partition"
                    );
                }
                partitions.push(Arc::new(Partition::new(log)));
            }
            topics.insert(name.clone(), Arc::new(Topic { name, partitions }));
        }
        let offsets = OffsetStore::open(&config.data_dir, config.log.fsync != FsyncPolicy::Never)?;
        Ok(Self {
            config,
            topics: RwLock::new(topics),
            offsets: Mutex::new(offsets),
        })
    }

    pub fn config(&self) -> &BrokerConfig {
        &self.config
    }

    pub fn data_dir(&self) -> &Path {
        &self.config.data_dir
    }

    pub fn create_topic(&self, name: &str, partitions: u32) -> Result<()> {
        validate_topic_name(name)?;
        if partitions == 0 || partitions > 1024 {
            return Err(Error::InvalidRequest(
                "partitions must be in 1..=1024".into(),
            ));
        }
        let mut topics = self.topics.write();
        if topics.contains_key(name) {
            return Err(Error::TopicExists(name.to_string()));
        }
        let dir = self.config.data_dir.join("topics").join(name);
        let mut parts = Vec::with_capacity(partitions as usize);
        for id in 0..partitions {
            let log = Log::open(dir.join(id.to_string()), self.config.log.clone())?;
            parts.push(Arc::new(Partition::new(log)));
        }
        crate::log::sync_dir(&self.config.data_dir.join("topics"), self.config.log.fsync)?;
        topics.insert(
            name.to_string(),
            Arc::new(Topic {
                name: name.to_string(),
                partitions: parts,
            }),
        );
        tracing::info!(topic = name, partitions, "created topic");
        Ok(())
    }

    pub fn topic(&self, name: &str) -> Option<Arc<Topic>> {
        self.topics.read().get(name).cloned()
    }

    pub fn partition(&self, topic: &str, partition: u32) -> Result<Arc<Partition>> {
        self.topic(topic)
            .and_then(|t| t.partitions.get(partition as usize).cloned())
            .ok_or_else(|| Error::UnknownTopicOrPartition {
                topic: topic.to_string(),
                partition,
            })
    }

    /// Append a batch to `topic/partition`, creating the topic if allowed.
    pub fn produce<K, V>(
        &self,
        topic: &str,
        partition: u32,
        records: &[(Option<K>, V)],
    ) -> Result<AppendInfo>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        if self.config.auto_create_topics && self.topic(topic).is_none() {
            match self.create_topic(topic, self.config.default_partitions) {
                Ok(()) | Err(Error::TopicExists(_)) => {}
                Err(e) => return Err(e),
            }
        }
        let p = self.partition(topic, partition)?;
        let info = {
            let mut log = p.log.write();
            let info = log.append(records)?;
            // Publish while still holding the lock so watermarks are monotonic.
            p.high_watermark.send_replace(info.last_offset + 1);
            info
        };
        Ok(info)
    }

    pub fn fetch(
        &self,
        topic: &str,
        partition: u32,
        offset: u64,
        max_bytes: usize,
    ) -> Result<FetchResult> {
        let p = self.partition(topic, partition)?;
        let log = p.log.read();
        let records = log.read(offset, max_bytes)?;
        Ok(FetchResult {
            start_offset: log.start_offset(),
            high_watermark: log.next_offset(),
            records,
        })
    }

    pub fn commit_offset(
        &self,
        group: &str,
        topic: &str,
        partition: u32,
        offset: u64,
    ) -> Result<()> {
        // Committing for an unknown partition is almost certainly a bug.
        self.partition(topic, partition)?;
        self.offsets.lock().commit(group, topic, partition, offset)
    }

    pub fn fetch_offset(&self, group: &str, topic: &str, partition: u32) -> Result<Option<u64>> {
        Ok(self.offsets.lock().get(group, topic, partition))
    }

    /// Metadata for one topic, or for all topics when `topic` is `None`.
    pub fn metadata(&self, topic: Option<&str>) -> Result<Vec<TopicMeta>> {
        let topics: Vec<Arc<Topic>> = match topic {
            Some(name) => vec![self
                .topic(name)
                .ok_or_else(|| Error::UnknownTopicOrPartition {
                    topic: name.to_string(),
                    partition: 0,
                })?],
            None => self.topics.read().values().cloned().collect(),
        };
        Ok(topics
            .iter()
            .map(|t| TopicMeta {
                name: t.name.clone(),
                partitions: t
                    .partitions
                    .iter()
                    .enumerate()
                    .map(|(i, p)| {
                        let log = p.log.read();
                        PartitionMeta {
                            partition: i as u32,
                            start_offset: log.start_offset(),
                            end_offset: log.next_offset(),
                            size_bytes: log.size_bytes(),
                            segments: log.segment_count() as u32,
                        }
                    })
                    .collect(),
            })
            .collect())
    }

    fn all_partitions(&self) -> Vec<Arc<Partition>> {
        self.topics
            .read()
            .values()
            .flat_map(|t| t.partitions.iter().cloned())
            .collect()
    }

    /// fsync partitions whose interval has elapsed (background flusher).
    pub fn sync_due(&self) -> Result<()> {
        for p in self.all_partitions() {
            p.log.write().sync_if_due()?;
        }
        Ok(())
    }

    /// fsync every partition with unsynced writes (used on shutdown).
    pub fn sync_all(&self) -> Result<()> {
        for p in self.all_partitions() {
            p.log.write().sync()?;
        }
        Ok(())
    }

    /// Apply retention to every partition. Returns segments deleted.
    pub fn enforce_retention(&self) -> Result<usize> {
        let now = now_ms();
        let mut removed = 0;
        for p in self.all_partitions() {
            removed += p.log.write().enforce_retention(now)?;
        }
        Ok(removed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::decode_all;

    fn broker(dir: &Path) -> Broker {
        let mut cfg = BrokerConfig::new(dir);
        cfg.log.fsync = FsyncPolicy::Never;
        cfg.default_partitions = 2;
        Broker::open(cfg).unwrap()
    }

    #[test]
    fn auto_creates_topics_and_reopens() {
        let dir = tempfile::tempdir().unwrap();
        {
            let b = broker(dir.path());
            let info = b
                .produce("events", 1, &[(None::<&[u8]>, b"a"), (None, b"b")])
                .unwrap();
            assert_eq!((info.base_offset, info.last_offset), (0, 1));
            b.commit_offset("g", "events", 1, 2).unwrap();
        }
        let b = broker(dir.path());
        let meta = b.metadata(Some("events")).unwrap();
        assert_eq!(meta[0].partitions.len(), 2);
        assert_eq!(meta[0].partitions[1].end_offset, 2);
        assert_eq!(b.fetch_offset("g", "events", 1).unwrap(), Some(2));
        let f = b.fetch("events", 1, 0, 1 << 20).unwrap();
        let recs = decode_all(&f.records, 1 << 20).unwrap();
        assert_eq!(recs[1].value.as_ref(), b"b");
    }

    #[test]
    fn rejects_bad_topics_and_partitions() {
        let dir = tempfile::tempdir().unwrap();
        let b = broker(dir.path());
        assert!(b.create_topic("../etc", 1).is_err());
        assert!(b.create_topic("ok", 0).is_err());
        b.create_topic("ok", 1).unwrap();
        assert!(matches!(
            b.create_topic("ok", 1),
            Err(Error::TopicExists(_))
        ));
        assert!(matches!(
            b.fetch("ok", 5, 0, 10),
            Err(Error::UnknownTopicOrPartition { .. })
        ));
        assert!(b.commit_offset("g", "missing", 0, 1).is_err());
    }

    #[test]
    fn high_watermark_is_published() {
        let dir = tempfile::tempdir().unwrap();
        let b = broker(dir.path());
        b.create_topic("t", 1).unwrap();
        let rx = b.partition("t", 0).unwrap().subscribe();
        assert_eq!(*rx.borrow(), 0);
        b.produce("t", 0, &[(None::<&[u8]>, b"x")]).unwrap();
        assert_eq!(*rx.borrow(), 1);
    }
}
