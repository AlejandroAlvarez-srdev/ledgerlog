//! A segment: one `.log` file of back-to-back record frames plus a sparse
//! `.index` file mapping relative offsets to byte positions.
//!
//! Files are named after the segment's base offset, zero-padded to 20 digits
//! so that lexicographic order equals numeric order:
//!
//! ```text
//! 00000000000000000000.log    00000000000000000000.index
//! 00000000000000052113.log    00000000000000052113.index
//! ```
//!
//! Index entries are 8 bytes: `relative_offset u32 | position u32` (big-endian).
//! One entry is written whenever at least `index_interval_bytes` of log data
//! were appended since the previous entry, so a lookup is a binary search over
//! the in-memory copy of the index followed by a short forward scan.

use crate::error::{Error, Result};
use crate::io::read_exact_at;
use crate::record::{self, Decoded, PEEK_HEADER_BYTES};
use std::fs::{self, File, OpenOptions};
use std::io::{BufReader, Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};

pub const LOG_SUFFIX: &str = "log";
pub const INDEX_SUFFIX: &str = "index";
const INDEX_ENTRY_BYTES: usize = 8;

/// One sparse index entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct IndexEntry {
    pub relative_offset: u32,
    pub position: u32,
}

/// What recovery found when opening a segment.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct RecoveryReport {
    /// Complete, CRC-valid records kept.
    pub records: u64,
    /// Bytes chopped off the end of the log file (torn or corrupt tail).
    pub truncated_bytes: u64,
}

/// A record boundary inside a batch being appended.
#[derive(Debug, Clone, Copy)]
pub struct BatchEntry {
    pub offset: u64,
    /// Byte position of the frame inside the batch buffer.
    pub position: u32,
}

pub struct Segment {
    base_offset: u64,
    log_path: PathBuf,
    index_path: PathBuf,
    log: File,
    index_file: File,
    index: Vec<IndexEntry>,
    size: u64,
    next_offset: u64,
    first_timestamp: Option<i64>,
    max_timestamp: Option<i64>,
    bytes_since_index: u64,
    index_interval_bytes: u64,
    max_record_bytes: usize,
}

pub fn file_name(base_offset: u64, suffix: &str) -> String {
    format!("{base_offset:020}.{suffix}")
}

impl Segment {
    /// Create a brand-new, empty segment.
    pub fn create(
        dir: &Path,
        base_offset: u64,
        index_interval_bytes: u64,
        max_record_bytes: usize,
    ) -> Result<Self> {
        let log_path = dir.join(file_name(base_offset, LOG_SUFFIX));
        let index_path = dir.join(file_name(base_offset, INDEX_SUFFIX));
        let log = open_rw(&log_path, true)?;
        let index_file = open_rw(&index_path, true)?;
        Ok(Self {
            base_offset,
            log_path,
            index_path,
            log,
            index_file,
            index: Vec::new(),
            size: 0,
            next_offset: base_offset,
            first_timestamp: None,
            max_timestamp: None,
            bytes_since_index: 0,
            index_interval_bytes,
            max_record_bytes,
        })
    }

    /// Open an existing segment.
    ///
    /// With `recover == true` (used for the active segment) every frame is
    /// read and CRC-checked from the start of the file; the file is truncated
    /// at the first torn or corrupt frame and the index is rebuilt.
    ///
    /// With `recover == false` (sealed segments) the on-disk index is trusted
    /// if it is structurally sound, and only the frames after the last index
    /// entry are walked (headers only) to find the end of the segment.
    pub fn open(
        dir: &Path,
        base_offset: u64,
        index_interval_bytes: u64,
        max_record_bytes: usize,
        recover: bool,
    ) -> Result<(Self, RecoveryReport)> {
        let log_path = dir.join(file_name(base_offset, LOG_SUFFIX));
        let index_path = dir.join(file_name(base_offset, INDEX_SUFFIX));
        let log = open_rw(&log_path, false)?;
        let index_file = open_rw(&index_path, true)?;
        let size = log.metadata()?.len();
        let mut seg = Self {
            base_offset,
            log_path,
            index_path,
            log,
            index_file,
            index: Vec::new(),
            size,
            next_offset: base_offset,
            first_timestamp: None,
            max_timestamp: None,
            bytes_since_index: 0,
            index_interval_bytes,
            max_record_bytes,
        };

        let loaded = if recover { None } else { seg.load_index()? };
        let report = match loaded {
            Some(index) => {
                seg.index = index;
                seg.scan_tail()?
            }
            None => seg.recover()?,
        };
        if seg.size > 0 {
            let mut hdr = [0u8; PEEK_HEADER_BYTES];
            read_exact_at(&seg.log, &mut hdr, 0)?;
            seg.first_timestamp = Some(record::parse_header(&hdr).timestamp);
        }
        Ok((seg, report))
    }

    /// Read the index file and check it is structurally plausible.
    /// Returns `None` if it must be rebuilt.
    fn load_index(&self) -> Result<Option<Vec<IndexEntry>>> {
        let mut raw = Vec::new();
        (&self.index_file).seek(SeekFrom::Start(0))?;
        (&self.index_file).read_to_end(&mut raw)?;
        if raw.len() % INDEX_ENTRY_BYTES != 0 {
            return Ok(None);
        }
        let mut entries = Vec::with_capacity(raw.len() / INDEX_ENTRY_BYTES);
        for chunk in raw.chunks_exact(INDEX_ENTRY_BYTES) {
            let e = IndexEntry {
                relative_offset: u32::from_be_bytes(chunk[..4].try_into().unwrap()),
                position: u32::from_be_bytes(chunk[4..].try_into().unwrap()),
            };
            let monotonic = entries.last().is_none_or(|p: &IndexEntry| {
                e.relative_offset > p.relative_offset && e.position > p.position
            });
            if !monotonic || u64::from(e.position) >= self.size {
                return Ok(None);
            }
            entries.push(e);
        }
        Ok(Some(entries))
    }

    /// Walk frame headers from the last index entry to EOF to learn the next
    /// offset and newest timestamp of a sealed segment.
    fn scan_tail(&mut self) -> Result<RecoveryReport> {
        let (mut pos, mut expected) = match self.index.last() {
            Some(e) => (
                u64::from(e.position),
                self.base_offset + u64::from(e.relative_offset),
            ),
            None => (0, self.base_offset),
        };
        let mut hdr = [0u8; PEEK_HEADER_BYTES];
        while pos + PEEK_HEADER_BYTES as u64 <= self.size {
            read_exact_at(&self.log, &mut hdr, pos)?;
            let h = record::parse_header(&hdr);
            if h.offset != expected || pos + h.size as u64 > self.size {
                // Index disagrees with the data: fall back to a full scan.
                self.index.clear();
                return self.recover();
            }
            self.max_timestamp = Some(h.timestamp);
            pos += h.size as u64;
            expected += 1;
        }
        if pos != self.size {
            self.index.clear();
            return self.recover();
        }
        self.next_offset = expected;
        self.bytes_since_index = 0;
        Ok(RecoveryReport {
            records: expected - self.base_offset,
            truncated_bytes: 0,
        })
    }

    /// Full CRC-validating scan; truncates the log after the last good frame
    /// and rewrites the index from scratch.
    fn recover(&mut self) -> Result<RecoveryReport> {
        let mut reader = BufReader::with_capacity(256 * 1024, &self.log);
        reader.seek(SeekFrom::Start(0))?;
        let mut buf: Vec<u8> = Vec::with_capacity(64 * 1024);
        let mut pos = 0u64;
        let mut expected = self.base_offset;
        let mut index = Vec::new();
        let mut since_index = 0u64;
        let mut max_ts = None;

        loop {
            buf.clear();
            buf.resize(record::LEN_BYTES, 0);
            if read_full(&mut reader, &mut buf)? < record::LEN_BYTES {
                break; // EOF or torn length prefix
            }
            let total = match record::peek_frame_len(&buf, self.max_record_bytes) {
                Ok(Some(t)) => t,
                _ => break, // implausible length => garbage tail
            };
            buf.resize(total, 0);
            if read_full(&mut reader, &mut buf[record::LEN_BYTES..])? < total - record::LEN_BYTES {
                break; // torn frame body
            }
            let rec = match record::decode(&buf, self.max_record_bytes) {
                Ok(Decoded::Record(r, _)) => r,
                _ => break, // crc mismatch / inconsistent lengths
            };
            if rec.offset != expected {
                break;
            }
            if since_index >= self.index_interval_bytes {
                index.push(IndexEntry {
                    relative_offset: (expected - self.base_offset) as u32,
                    position: pos as u32,
                });
                since_index = 0;
            }
            since_index += total as u64;
            max_ts = Some(rec.timestamp);
            pos += total as u64;
            expected += 1;
        }
        drop(reader);

        let truncated = self.size - pos;
        if truncated > 0 {
            tracing::warn!(
                segment = %self.log_path.display(),
                valid_bytes = pos,
                truncated_bytes = truncated,
                "truncating torn/corrupt tail"
            );
            self.log.set_len(pos)?;
            self.log.sync_all()?;
        }
        self.size = pos;
        self.next_offset = expected;
        self.max_timestamp = max_ts;
        self.bytes_since_index = since_index;
        self.index = index;
        self.rewrite_index_file()?;
        Ok(RecoveryReport {
            records: expected - self.base_offset,
            truncated_bytes: truncated,
        })
    }

    fn rewrite_index_file(&mut self) -> Result<()> {
        self.index_file.set_len(0)?;
        let mut raw = Vec::with_capacity(self.index.len() * INDEX_ENTRY_BYTES);
        for e in &self.index {
            raw.extend_from_slice(&e.relative_offset.to_be_bytes());
            raw.extend_from_slice(&e.position.to_be_bytes());
        }
        // The index file is opened in append mode, so this lands at offset 0.
        self.index_file.write_all(&raw)?;
        Ok(())
    }

    /// Append an already-encoded batch of frames. `entries` lists the offset
    /// and in-batch position of every frame, in order.
    pub fn append(&mut self, batch: &[u8], entries: &[BatchEntry], max_ts: i64) -> Result<()> {
        debug_assert!(!entries.is_empty());
        debug_assert_eq!(entries[0].offset, self.next_offset);
        let mut new_index = Vec::new();
        let mut prev_pos = 0u32;
        for e in entries {
            self.bytes_since_index += u64::from(e.position - prev_pos);
            prev_pos = e.position;
            let abs = self.size + u64::from(e.position);
            if self.bytes_since_index >= self.index_interval_bytes {
                new_index.push(IndexEntry {
                    relative_offset: (e.offset - self.base_offset) as u32,
                    position: abs as u32,
                });
                self.bytes_since_index = 0;
            }
        }
        self.bytes_since_index += batch.len() as u64 - u64::from(prev_pos);

        self.log.write_all(batch)?;
        if !new_index.is_empty() {
            let mut raw = Vec::with_capacity(new_index.len() * INDEX_ENTRY_BYTES);
            for e in &new_index {
                raw.extend_from_slice(&e.relative_offset.to_be_bytes());
                raw.extend_from_slice(&e.position.to_be_bytes());
            }
            self.index_file.write_all(&raw)?;
            self.index.extend(new_index);
        }
        if self.first_timestamp.is_none() {
            self.first_timestamp = Some(max_ts);
        }
        self.size += batch.len() as u64;
        self.next_offset = entries.last().unwrap().offset + 1;
        self.max_timestamp = Some(max_ts);
        Ok(())
    }

    /// Byte position of the frame holding `offset` (which must be in range).
    fn position_of(&self, offset: u64) -> Result<u64> {
        let rel = (offset - self.base_offset) as u32;
        // Largest index entry with relative_offset <= rel.
        let idx = self.index.partition_point(|e| e.relative_offset <= rel);
        let (mut pos, mut cur) = if idx == 0 {
            (0u64, self.base_offset)
        } else {
            let e = self.index[idx - 1];
            (
                u64::from(e.position),
                self.base_offset + u64::from(e.relative_offset),
            )
        };
        let mut hdr = [0u8; PEEK_HEADER_BYTES];
        loop {
            read_exact_at(&self.log, &mut hdr, pos)?;
            let h = record::parse_header(&hdr);
            if h.offset != cur {
                return Err(Error::Corrupt {
                    position: pos,
                    reason: format!("expected offset {cur}, found {}", h.offset),
                });
            }
            if cur == offset {
                return Ok(pos);
            }
            pos += h.size as u64;
            cur += 1;
        }
    }

    /// Read whole frames starting at `offset`, up to `max_bytes`. At least
    /// one frame is returned if `offset` is in this segment, even if it is
    /// larger than `max_bytes`, so a consumer can never get stuck.
    pub fn read(&self, offset: u64, max_bytes: usize) -> Result<Vec<u8>> {
        if offset < self.base_offset || offset >= self.next_offset {
            return Ok(Vec::new());
        }
        let start = self.position_of(offset)?;
        let available = (self.size - start) as usize;
        let mut want = available.min(max_bytes.max(PEEK_HEADER_BYTES));
        let mut buf = vec![0u8; want];
        read_exact_at(&self.log, &mut buf, start)?;

        // Trim to the last complete frame.
        let mut end = 0usize;
        while end + record::LEN_BYTES <= buf.len() {
            let size = u32::from_be_bytes(buf[end..end + 4].try_into().unwrap()) as usize
                + record::LEN_BYTES;
            if end + size > buf.len() {
                break;
            }
            end += size;
        }
        if end == 0 {
            // First frame alone exceeds max_bytes: return it whole.
            let size =
                u32::from_be_bytes(buf[..4].try_into().unwrap()) as usize + record::LEN_BYTES;
            want = size;
            buf.resize(want, 0);
            read_exact_at(&self.log, &mut buf, start)?;
            end = want;
        }
        buf.truncate(end);
        Ok(buf)
    }

    /// Flush file data to stable storage.
    pub fn sync(&self) -> Result<()> {
        self.log.sync_data()?;
        Ok(())
    }

    /// Sync both the log and the index (used when sealing a segment).
    pub fn sync_all(&self) -> Result<()> {
        self.log.sync_data()?;
        self.index_file.sync_data()?;
        Ok(())
    }

    /// Remove the segment's files from disk.
    pub fn delete(self) -> Result<()> {
        let Segment {
            log_path,
            index_path,
            log,
            index_file,
            ..
        } = self;
        drop(log);
        drop(index_file);
        fs::remove_file(&log_path)?;
        match fs::remove_file(&index_path) {
            Ok(()) => Ok(()),
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => Ok(()),
            Err(e) => Err(e.into()),
        }
    }

    pub fn base_offset(&self) -> u64 {
        self.base_offset
    }
    pub fn next_offset(&self) -> u64 {
        self.next_offset
    }
    pub fn size(&self) -> u64 {
        self.size
    }
    pub fn is_empty(&self) -> bool {
        self.size == 0
    }
    pub fn first_timestamp(&self) -> Option<i64> {
        self.first_timestamp
    }
    pub fn max_timestamp(&self) -> Option<i64> {
        self.max_timestamp
    }
    pub fn index_entries(&self) -> &[IndexEntry] {
        &self.index
    }
    pub fn log_path(&self) -> &Path {
        &self.log_path
    }
}

fn open_rw(path: &Path, create: bool) -> Result<File> {
    Ok(OpenOptions::new()
        .read(true)
        .append(true)
        .create(create)
        .open(path)?)
}

/// Like `read_exact`, but returns how many bytes were read before EOF.
fn read_full<R: Read>(r: &mut R, buf: &mut [u8]) -> Result<usize> {
    let mut done = 0;
    while done < buf.len() {
        match r.read(&mut buf[done..]) {
            Ok(0) => break,
            Ok(n) => done += n,
            Err(e) if e.kind() == std::io::ErrorKind::Interrupted => {}
            Err(e) => return Err(e.into()),
        }
    }
    Ok(done)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::record::{decode_all, encode_parts};

    fn append_n(seg: &mut Segment, n: u64, value_len: usize) {
        let mut buf = Vec::new();
        let mut entries = Vec::new();
        for i in 0..n {
            let off = seg.next_offset() + i;
            entries.push(BatchEntry {
                offset: off,
                position: buf.len() as u32,
            });
            encode_parts(
                &mut buf,
                off,
                1000 + off as i64,
                None,
                &vec![b'x'; value_len],
            );
        }
        let ts = 1000 + entries.last().unwrap().offset as i64;
        seg.append(&buf, &entries, ts).unwrap();
    }

    #[test]
    fn sparse_index_is_built_and_lookups_work() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 100, 256, 1 << 20).unwrap();
        for _ in 0..10 {
            append_n(&mut seg, 10, 50);
        }
        assert_eq!(seg.next_offset(), 200);
        assert!(
            seg.index_entries().len() > 5,
            "index should be sparse but present"
        );
        for off in 100..200 {
            let bytes = seg.read(off, 1).unwrap();
            let recs = decode_all(&bytes, 1 << 20).unwrap();
            assert_eq!(recs.len(), 1);
            assert_eq!(recs[0].offset, off);
        }
    }

    #[test]
    fn read_respects_max_bytes_on_frame_boundaries() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0, 4096, 1 << 20).unwrap();
        append_n(&mut seg, 100, 100);
        let frame = 32 + 100;
        let bytes = seg.read(10, frame * 3 + 7).unwrap();
        let recs = decode_all(&bytes, 1 << 20).unwrap();
        assert_eq!(
            recs.iter().map(|r| r.offset).collect::<Vec<_>>(),
            [10, 11, 12]
        );
    }

    #[test]
    fn reopen_sealed_segment_uses_index() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0, 512, 1 << 20).unwrap();
        append_n(&mut seg, 500, 40);
        let entries = seg.index_entries().to_vec();
        drop(seg);
        let (seg, report) = Segment::open(dir.path(), 0, 512, 1 << 20, false).unwrap();
        assert_eq!(seg.index_entries(), &entries[..]);
        assert_eq!(seg.next_offset(), 500);
        assert_eq!(report.records, 500);
        assert_eq!(seg.max_timestamp(), Some(1000 + 499));
    }

    #[test]
    fn corrupt_index_is_rebuilt() {
        let dir = tempfile::tempdir().unwrap();
        let mut seg = Segment::create(dir.path(), 0, 512, 1 << 20).unwrap();
        append_n(&mut seg, 300, 40);
        let entries = seg.index_entries().to_vec();
        drop(seg);
        let idx = dir.path().join(file_name(0, INDEX_SUFFIX));
        fs::write(&idx, [1, 2, 3]).unwrap();
        let (seg, _) = Segment::open(dir.path(), 0, 512, 1 << 20, false).unwrap();
        assert_eq!(seg.index_entries(), &entries[..]);
        assert_eq!(seg.next_offset(), 300);
    }
}
