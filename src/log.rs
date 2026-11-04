//! A partition log: an ordered set of segments where only the newest
//! ("active") segment is written to.

use crate::config::{FsyncPolicy, LogConfig};
use crate::error::{Error, Result};
use crate::record::{encode_parts, Record};
use crate::segment::{self, BatchEntry, RecoveryReport, Segment};
use std::collections::BTreeMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::time::{Instant, SystemTime, UNIX_EPOCH};

/// Milliseconds since the Unix epoch.
pub fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Result of an append.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct AppendInfo {
    pub base_offset: u64,
    pub last_offset: u64,
    pub timestamp: i64,
}

/// Summary of what happened when the log was opened.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct LogRecovery {
    pub segments: usize,
    /// Report for the active segment, which is always fully re-validated.
    pub active: RecoveryReport,
}

pub struct Log {
    dir: PathBuf,
    config: LogConfig,
    segments: BTreeMap<u64, Segment>,
    last_sync: Instant,
    dirty: bool,
    recovery: LogRecovery,
}

impl Log {
    /// Open (or create) the log stored in `dir`, recovering the active segment.
    pub fn open(dir: impl AsRef<Path>, config: LogConfig) -> Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        if config.segment_bytes == 0 || config.segment_bytes > u64::from(u32::MAX) / 2 {
            return Err(Error::InvalidRequest(format!(
                "segment_bytes must be in 1..={}",
                u32::MAX / 2
            )));
        }
        fs::create_dir_all(&dir)?;

        let mut bases = Vec::new();
        for entry in fs::read_dir(&dir)? {
            let path = entry?.path();
            if path.extension().and_then(|e| e.to_str()) != Some(segment::LOG_SUFFIX) {
                continue;
            }
            if let Some(base) = path
                .file_stem()
                .and_then(|s| s.to_str())
                .and_then(|s| s.parse::<u64>().ok())
            {
                bases.push(base);
            }
        }
        bases.sort_unstable();

        let mut segments = BTreeMap::new();
        let mut recovery = LogRecovery::default();
        let last = bases.len().saturating_sub(1);
        for (i, &base) in bases.iter().enumerate() {
            let is_active = i == last;
            let (seg, report) = Segment::open(
                &dir,
                base,
                config.index_interval_bytes,
                config.max_record_bytes,
                is_active,
            )?;
            if is_active {
                recovery.active = report;
            }
            segments.insert(base, seg);
        }
        if segments.is_empty() {
            let seg = Segment::create(
                &dir,
                0,
                config.index_interval_bytes,
                config.max_record_bytes,
            )?;
            segments.insert(0, seg);
            sync_dir(&dir, config.fsync)?;
        }
        recovery.segments = segments.len();

        let log = Self {
            dir,
            config,
            segments,
            last_sync: Instant::now(),
            dirty: false,
            recovery,
        };
        log.check_contiguous()?;
        Ok(log)
    }

    fn check_contiguous(&self) -> Result<()> {
        let mut prev: Option<&Segment> = None;
        for seg in self.segments.values() {
            if let Some(p) = prev {
                if p.next_offset() != seg.base_offset() {
                    return Err(Error::Corrupt {
                        position: 0,
                        reason: format!(
                            "gap between segments: {} ends at {}, next starts at {}",
                            p.log_path().display(),
                            p.next_offset(),
                            seg.base_offset()
                        ),
                    });
                }
            }
            prev = Some(seg);
        }
        Ok(())
    }

    fn active(&self) -> &Segment {
        self.segments
            .values()
            .next_back()
            .expect("log has a segment")
    }

    fn active_mut(&mut self) -> &mut Segment {
        self.segments
            .values_mut()
            .next_back()
            .expect("log has a segment")
    }

    /// First offset still stored.
    pub fn start_offset(&self) -> u64 {
        self.segments.values().next().unwrap().base_offset()
    }

    /// Offset the next appended record will receive (the high watermark).
    pub fn next_offset(&self) -> u64 {
        self.active().next_offset()
    }

    pub fn size_bytes(&self) -> u64 {
        self.segments.values().map(Segment::size).sum()
    }

    pub fn segment_count(&self) -> usize {
        self.segments.len()
    }

    pub fn segment_base_offsets(&self) -> Vec<u64> {
        self.segments.keys().copied().collect()
    }

    pub fn recovery(&self) -> LogRecovery {
        self.recovery
    }

    pub fn config(&self) -> &LogConfig {
        &self.config
    }

    pub fn dir(&self) -> &Path {
        &self.dir
    }

    /// Append a batch of `(key, value)` records stamped with the current time.
    pub fn append<K, V>(&mut self, records: &[(Option<K>, V)]) -> Result<AppendInfo>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        self.append_at(records, now_ms())
    }

    /// Append with an explicit timestamp (used by tests and tools).
    pub fn append_at<K, V>(
        &mut self,
        records: &[(Option<K>, V)],
        timestamp: i64,
    ) -> Result<AppendInfo>
    where
        K: AsRef<[u8]>,
        V: AsRef<[u8]>,
    {
        if records.is_empty() {
            return Err(Error::InvalidRequest("empty batch".into()));
        }
        let mut batch_len = 0usize;
        for (k, v) in records {
            let len =
                Record::encoded_len_for(k.as_ref().map(|k| k.as_ref().len()), v.as_ref().len());
            if len > self.config.max_record_bytes {
                return Err(Error::RecordTooLarge {
                    size: len,
                    max: self.config.max_record_bytes,
                });
            }
            batch_len += len;
        }
        // Index positions are u32, so a single batch must fit well inside 4 GiB.
        if batch_len as u64 > u64::from(u32::MAX) / 2 {
            return Err(Error::InvalidRequest("batch too large".into()));
        }

        if self.should_roll(batch_len as u64, timestamp) {
            self.roll()?;
        }

        let base = self.next_offset();
        let mut buf = Vec::with_capacity(batch_len);
        let mut entries = Vec::with_capacity(records.len());
        for (i, (k, v)) in records.iter().enumerate() {
            let offset = base + i as u64;
            entries.push(BatchEntry {
                offset,
                position: buf.len() as u32,
            });
            encode_parts(
                &mut buf,
                offset,
                timestamp,
                k.as_ref().map(|k| k.as_ref()),
                v.as_ref(),
            );
        }
        self.active_mut().append(&buf, &entries, timestamp)?;
        self.dirty = true;
        self.maybe_sync()?;
        Ok(AppendInfo {
            base_offset: base,
            last_offset: base + records.len() as u64 - 1,
            timestamp,
        })
    }

    fn should_roll(&self, incoming: u64, now: i64) -> bool {
        let active = self.active();
        if active.is_empty() {
            return false;
        }
        if active.size() + incoming > self.config.segment_bytes {
            return true;
        }
        match (self.config.segment_ms, active.first_timestamp()) {
            (Some(ms), Some(first)) => now - first >= ms as i64,
            _ => false,
        }
    }

    /// Seal the active segment and start a new one at the current next offset.
    pub fn roll(&mut self) -> Result<()> {
        let next = self.next_offset();
        if self.config.fsync != FsyncPolicy::Never {
            self.active().sync_all()?;
        }
        let seg = Segment::create(
            &self.dir,
            next,
            self.config.index_interval_bytes,
            self.config.max_record_bytes,
        )?;
        self.segments.insert(next, seg);
        sync_dir(&self.dir, self.config.fsync)?;
        tracing::debug!(dir = %self.dir.display(), base_offset = next, "rolled segment");
        Ok(())
    }

    fn maybe_sync(&mut self) -> Result<()> {
        match self.config.fsync {
            FsyncPolicy::Always => self.sync(),
            // Normally the server's background flusher syncs off the write
            // path (see `take_due_sync`); this is only a safety net in case
            // nobody is driving it.
            FsyncPolicy::Interval(d) if self.last_sync.elapsed() >= d * 2 => self.sync(),
            _ => Ok(()),
        }
    }

    /// fsync the active segment if there are unsynced writes.
    pub fn sync(&mut self) -> Result<()> {
        if self.dirty {
            self.active().sync()?;
            self.dirty = false;
        }
        self.last_sync = Instant::now();
        Ok(())
    }

    /// For [`FsyncPolicy::Interval`]: if a sync is due, mark the log clean
    /// and return a duplicated handle of the active segment so the caller can
    /// fsync it *after* releasing the partition lock. fsync applies to the
    /// file, not the descriptor, so this covers every write made so far;
    /// writes racing with it set `dirty` again and are synced next round.
    pub fn take_due_sync(&mut self) -> Result<Option<File>> {
        if let FsyncPolicy::Interval(d) = self.config.fsync {
            if self.dirty && self.last_sync.elapsed() >= d {
                self.dirty = false;
                self.last_sync = Instant::now();
                return Ok(Some(self.active().duplicate_log_handle()?));
            }
        }
        Ok(None)
    }

    /// Read whole frames starting at `offset`, up to roughly `max_bytes`
    /// (always at least one frame when data is available).
    ///
    /// `offset == next_offset()` returns an empty buffer (caught up);
    /// anything outside `[start_offset, next_offset]` is out of range.
    pub fn read(&self, offset: u64, max_bytes: usize) -> Result<Vec<u8>> {
        let start = self.start_offset();
        let end = self.next_offset();
        if offset < start || offset > end {
            return Err(Error::OffsetOutOfRange {
                requested: offset,
                start,
                end,
            });
        }
        if offset == end {
            return Ok(Vec::new());
        }
        let (_, seg) = self
            .segments
            .range(..=offset)
            .next_back()
            .expect("offset >= start offset");
        seg.read(offset, max_bytes)
    }

    /// Delete sealed segments that violate the time or size retention limits.
    /// The active segment is never deleted. Returns the number removed.
    pub fn enforce_retention(&mut self, now: i64) -> Result<usize> {
        let mut removed = 0;
        let mut total = self.size_bytes();
        while self.segments.len() > 1 {
            let oldest = self.segments.values().next().unwrap();
            let expired = match (self.config.retention_ms, oldest.max_timestamp()) {
                (Some(ms), Some(ts)) => now - ts > ms as i64,
                _ => false,
            };
            let oversized = self.config.retention_bytes.is_some_and(|max| total > max);
            if !(expired || oversized) {
                break;
            }
            let base = oldest.base_offset();
            let seg = self.segments.remove(&base).unwrap();
            total -= seg.size();
            tracing::info!(
                dir = %self.dir.display(),
                base_offset = base,
                expired,
                oversized,
                "retention: deleting segment"
            );
            seg.delete()?;
            removed += 1;
        }
        Ok(removed)
    }
}

/// Persist directory entries (new/renamed files) on platforms that allow it.
pub(crate) fn sync_dir(dir: &Path, policy: FsyncPolicy) -> Result<()> {
    if policy == FsyncPolicy::Never {
        return Ok(());
    }
    #[cfg(unix)]
    File::open(dir)?.sync_all()?;
    #[cfg(not(unix))]
    let _ = dir;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::decode_all;

    fn cfg() -> LogConfig {
        LogConfig {
            segment_bytes: 4096,
            index_interval_bytes: 512,
            retention_ms: None,
            retention_bytes: None,
            fsync: FsyncPolicy::Never,
            ..LogConfig::default()
        }
    }

    fn values(n: usize, start: usize) -> Vec<(Option<Vec<u8>>, Vec<u8>)> {
        (start..start + n)
            .map(|i| {
                (
                    Some(format!("k{i}").into_bytes()),
                    format!("value-{i:05}").into_bytes(),
                )
            })
            .collect()
    }

    fn read_all(log: &Log) -> Vec<Record> {
        let mut out = Vec::new();
        let mut off = log.start_offset();
        while off < log.next_offset() {
            let recs = decode_all(&log.read(off, 1000).unwrap(), 1 << 20).unwrap();
            off = recs.last().unwrap().offset + 1;
            out.extend(recs);
        }
        out
    }

    #[test]
    fn append_assigns_contiguous_offsets() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), cfg()).unwrap();
        let a = log.append(&values(3, 0)).unwrap();
        let b = log.append(&values(2, 3)).unwrap();
        assert_eq!((a.base_offset, a.last_offset), (0, 2));
        assert_eq!((b.base_offset, b.last_offset), (3, 4));
        assert_eq!(log.next_offset(), 5);
    }

    #[test]
    fn rolls_segments_by_size_and_reads_across_them() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), cfg()).unwrap();
        for i in 0..100 {
            log.append(&values(5, i * 5)).unwrap();
        }
        assert!(
            log.segment_count() > 5,
            "got {} segments",
            log.segment_count()
        );
        let all = read_all(&log);
        assert_eq!(all.len(), 500);
        for (i, r) in all.iter().enumerate() {
            assert_eq!(r.offset, i as u64);
            assert_eq!(r.value, format!("value-{i:05}").as_bytes());
        }
    }

    #[test]
    fn rolls_segments_by_time() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                segment_ms: Some(1000),
                ..cfg()
            },
        )
        .unwrap();
        log.append_at(&values(1, 0), 10_000).unwrap();
        log.append_at(&values(1, 1), 10_500).unwrap();
        assert_eq!(log.segment_count(), 1);
        log.append_at(&values(1, 2), 11_000).unwrap();
        assert_eq!(log.segment_base_offsets(), vec![0, 2]);
    }

    #[test]
    fn reopen_preserves_everything() {
        let dir = tempfile::tempdir().unwrap();
        {
            let mut log = Log::open(dir.path(), cfg()).unwrap();
            for i in 0..50 {
                log.append(&values(4, i * 4)).unwrap();
            }
        }
        let log = Log::open(dir.path(), cfg()).unwrap();
        assert_eq!(log.next_offset(), 200);
        assert_eq!(log.recovery().active.truncated_bytes, 0);
        assert_eq!(read_all(&log).len(), 200);
    }

    #[test]
    fn out_of_range_and_caught_up_reads() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(dir.path(), cfg()).unwrap();
        log.append(&values(3, 0)).unwrap();
        assert!(log.read(3, 100).unwrap().is_empty());
        assert!(matches!(
            log.read(4, 100),
            Err(Error::OffsetOutOfRange {
                start: 0,
                end: 3,
                ..
            })
        ));
    }

    #[test]
    fn size_retention_deletes_oldest_sealed_segments() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                retention_bytes: Some(10_000),
                ..cfg()
            },
        )
        .unwrap();
        for i in 0..200 {
            log.append(&values(2, i * 2)).unwrap();
        }
        let before = log.segment_count();
        let removed = log.enforce_retention(now_ms()).unwrap();
        assert!(removed > 0);
        assert_eq!(log.segment_count(), before - removed);
        assert!(log.size_bytes() <= 10_000 + 4096);
        assert!(log.start_offset() > 0);
        assert!(matches!(
            log.read(0, 100),
            Err(Error::OffsetOutOfRange { .. })
        ));
        // Everything that is left is still readable.
        assert_eq!(
            read_all(&log).len() as u64,
            log.next_offset() - log.start_offset()
        );
    }

    #[test]
    fn time_retention_never_deletes_active_segment() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                retention_ms: Some(1_000),
                ..cfg()
            },
        )
        .unwrap();
        for i in 0..100 {
            log.append_at(&values(2, i * 2), 1_000).unwrap();
        }
        let segs = log.segment_count();
        assert!(segs > 1);
        assert_eq!(log.enforce_retention(1_500).unwrap(), 0);
        assert_eq!(log.enforce_retention(10_000).unwrap(), segs - 1);
        assert_eq!(log.segment_count(), 1);
        log.append(&values(1, 999)).unwrap();
        assert_eq!(log.next_offset(), 201);
    }

    #[test]
    fn rejects_oversized_records_and_empty_batches() {
        let dir = tempfile::tempdir().unwrap();
        let mut log = Log::open(
            dir.path(),
            LogConfig {
                max_record_bytes: 128,
                ..cfg()
            },
        )
        .unwrap();
        let big: Vec<(Option<&[u8]>, Vec<u8>)> = vec![(None, vec![0; 200])];
        assert!(matches!(
            log.append(&big),
            Err(Error::RecordTooLarge { .. })
        ));
        let empty: Vec<(Option<&[u8]>, &[u8])> = vec![];
        assert!(log.append(&empty).is_err());
    }
}
