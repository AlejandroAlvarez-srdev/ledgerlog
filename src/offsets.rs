//! Durable consumer-group offsets.
//!
//! Commits are appended to `consumer_offsets.log` using the same CRC-framed
//! record format as partitions (key = `group \0 topic \0 partition`,
//! value = committed offset as `u64`), and fsynced before the commit is
//! acknowledged. On startup the file is replayed into memory, stopping at the
//! first torn or corrupt frame. When the file holds many superseded commits it
//! is compacted into a snapshot written to a temp file and atomically renamed.

use crate::error::{Error, Result};
use crate::record::{self, encode_parts, Decoded};
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::Write;
use std::path::{Path, PathBuf};

const FILE_NAME: &str = "consumer_offsets.log";
const MAX_FRAME: usize = 64 * 1024;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct GroupKey {
    pub group: String,
    pub topic: String,
    pub partition: u32,
}

impl GroupKey {
    fn encode(&self) -> Vec<u8> {
        format!("{}\0{}\0{}", self.group, self.topic, self.partition).into_bytes()
    }

    fn decode(raw: &[u8]) -> Option<Self> {
        let s = std::str::from_utf8(raw).ok()?;
        let mut it = s.split('\0');
        let group = it.next()?.to_string();
        let topic = it.next()?.to_string();
        let partition = it.next()?.parse().ok()?;
        if it.next().is_some() {
            return None;
        }
        Some(Self {
            group,
            topic,
            partition,
        })
    }
}

pub struct OffsetStore {
    dir: PathBuf,
    file: File,
    offsets: HashMap<GroupKey, u64>,
    /// Frames currently in the file (including superseded ones).
    frames: u64,
    seq: u64,
    sync: bool,
}

impl OffsetStore {
    /// Open the store in `dir`. `sync` controls whether commits are fsynced.
    pub fn open(dir: impl AsRef<Path>, sync: bool) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;
        let path = dir.join(FILE_NAME);
        let data = match fs::read(&path) {
            Ok(d) => d,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Vec::new(),
            Err(e) => return Err(e.into()),
        };

        let mut offsets = HashMap::new();
        let mut pos = 0usize;
        let mut frames = 0u64;
        while pos < data.len() {
            match record::decode(&data[pos..], MAX_FRAME) {
                Ok(Decoded::Record(r, n)) => {
                    let key = r.key.as_deref().and_then(GroupKey::decode);
                    let value: Option<[u8; 8]> = r.value.as_ref().try_into().ok();
                    match (key, value) {
                        (Some(k), Some(v)) => {
                            offsets.insert(k, u64::from_be_bytes(v));
                        }
                        _ => break,
                    }
                    pos += n;
                    frames += 1;
                }
                _ => break,
            }
        }

        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .read(true)
            .open(&path)?;
        if pos < data.len() {
            tracing::warn!(
                truncated_bytes = data.len() - pos,
                "consumer offsets: discarding torn tail"
            );
            file.set_len(pos as u64)?;
            file.sync_all()?;
        }
        Ok(Self {
            dir,
            file,
            offsets,
            frames,
            seq: frames,
            sync,
        })
    }

    pub fn get(&self, group: &str, topic: &str, partition: u32) -> Option<u64> {
        self.offsets
            .get(&GroupKey {
                group: group.to_string(),
                topic: topic.to_string(),
                partition,
            })
            .copied()
    }

    /// Durably record that `group` has consumed `topic/partition` up to
    /// (but not including) `offset`.
    pub fn commit(&mut self, group: &str, topic: &str, partition: u32, offset: u64) -> Result<()> {
        if group.is_empty() || group.len() > 255 || group.contains('\0') {
            return Err(Error::InvalidRequest(format!("invalid group id {group:?}")));
        }
        let key = GroupKey {
            group: group.to_string(),
            topic: topic.to_string(),
            partition,
        };
        let mut buf = Vec::new();
        encode_parts(
            &mut buf,
            self.seq,
            crate::log::now_ms(),
            Some(&key.encode()),
            &offset.to_be_bytes(),
        );
        self.file.write_all(&buf)?;
        if self.sync {
            self.file.sync_data()?;
        }
        self.seq += 1;
        self.frames += 1;
        self.offsets.insert(key, offset);
        if self.frames > 1024 && self.frames > 4 * self.offsets.len() as u64 {
            self.compact()?;
        }
        Ok(())
    }

    /// Rewrite the file with one frame per key (atomic via rename).
    pub fn compact(&mut self) -> Result<()> {
        let tmp = self.dir.join(format!("{FILE_NAME}.tmp"));
        let mut entries: Vec<_> = self.offsets.iter().collect();
        entries.sort();
        let mut buf = Vec::new();
        for (i, (k, v)) in entries.iter().enumerate() {
            encode_parts(
                &mut buf,
                i as u64,
                crate::log::now_ms(),
                Some(&k.encode()),
                &v.to_be_bytes(),
            );
        }
        {
            let mut f = File::create(&tmp)?;
            f.write_all(&buf)?;
            f.sync_all()?;
        }
        let path = self.dir.join(FILE_NAME);
        fs::rename(&tmp, &path)?;
        #[cfg(unix)]
        File::open(&self.dir)?.sync_all()?;
        self.file = OpenOptions::new().append(true).read(true).open(&path)?;
        self.frames = entries.len() as u64;
        self.seq = self.frames;
        Ok(())
    }

    pub fn len(&self) -> usize {
        self.offsets.len()
    }

    pub fn is_empty(&self) -> bool {
        self.offsets.is_empty()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn commits_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = OffsetStore::open(dir.path(), true).unwrap();
            s.commit("g1", "orders", 0, 10).unwrap();
            s.commit("g1", "orders", 0, 25).unwrap();
            s.commit("g2", "orders", 3, 7).unwrap();
        }
        let s = OffsetStore::open(dir.path(), true).unwrap();
        assert_eq!(s.get("g1", "orders", 0), Some(25));
        assert_eq!(s.get("g2", "orders", 3), Some(7));
        assert_eq!(s.get("g2", "orders", 0), None);
    }

    #[test]
    fn torn_tail_is_ignored() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = OffsetStore::open(dir.path(), false).unwrap();
            s.commit("g", "t", 0, 1).unwrap();
            s.commit("g", "t", 0, 2).unwrap();
        }
        let path = dir.path().join(FILE_NAME);
        let len = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(len - 3)
            .unwrap();
        let mut s = OffsetStore::open(dir.path(), false).unwrap();
        assert_eq!(s.get("g", "t", 0), Some(1));
        s.commit("g", "t", 0, 3).unwrap();
        drop(s);
        let s = OffsetStore::open(dir.path(), false).unwrap();
        assert_eq!(s.get("g", "t", 0), Some(3));
    }

    #[test]
    fn compaction_keeps_latest_values() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut s = OffsetStore::open(dir.path(), false).unwrap();
            for i in 0..5000u64 {
                s.commit("g", "t", (i % 3) as u32, i).unwrap();
            }
            assert!(s.frames < 5000, "compaction should have run");
        }
        let path = dir.path().join(FILE_NAME);
        assert!(fs::metadata(&path).unwrap().len() < 1024 * 64);
        let s = OffsetStore::open(dir.path(), false).unwrap();
        assert_eq!(s.get("g", "t", 0), Some(4998));
        assert_eq!(s.get("g", "t", 1), Some(4999));
        assert_eq!(s.get("g", "t", 2), Some(4997));
    }
}
