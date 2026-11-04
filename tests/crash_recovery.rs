//! Crash-safety tests: simulate a crash by tearing the active segment
//! mid-record (or leaving garbage behind it) and check that reopening keeps
//! every complete record, drops the partial one, and keeps accepting writes.

use ledgerlog::log::Log;
use ledgerlog::record::decode_all;
use ledgerlog::segment::{file_name, INDEX_SUFFIX, LOG_SUFFIX};
use ledgerlog::{FsyncPolicy, LogConfig, Record};
use std::fs::{self, OpenOptions};
use std::io::Write;
use std::path::Path;

fn config() -> LogConfig {
    LogConfig {
        segment_bytes: 64 * 1024,
        index_interval_bytes: 256,
        retention_ms: None,
        retention_bytes: None,
        // The crash is simulated by editing the file, so fsync would only slow
        // the test down without changing what it verifies.
        fsync: FsyncPolicy::Never,
        ..LogConfig::default()
    }
}

fn value(i: u64) -> Vec<u8> {
    format!("payload-{i:06}-{}", "x".repeat((i % 37) as usize)).into_bytes()
}

fn write_records(dir: &Path, n: u64) {
    let mut log = Log::open(dir, config()).unwrap();
    for i in 0..n {
        let key = format!("key-{i}");
        log.append(&[(Some(key.as_bytes()), value(i))]).unwrap();
    }
}

fn read_all(log: &Log) -> Vec<Record> {
    let mut out = Vec::new();
    let mut off = log.start_offset();
    while off < log.next_offset() {
        let recs = decode_all(&log.read(off, 4096).unwrap(), 1 << 20).unwrap();
        off = recs.last().unwrap().offset + 1;
        out.extend(recs);
    }
    out
}

fn assert_intact(log: &Log, expected: u64) {
    let recs = read_all(log);
    assert_eq!(recs.len() as u64, expected);
    for (i, r) in recs.iter().enumerate() {
        let i = i as u64;
        assert_eq!(r.offset, i);
        assert_eq!(r.key.as_deref(), Some(format!("key-{i}").as_bytes()));
        assert_eq!(r.value, value(i));
    }
}

fn active_log_path(dir: &Path) -> std::path::PathBuf {
    let log = Log::open(dir, config()).unwrap();
    let base = *log.segment_base_offsets().last().unwrap();
    dir.join(file_name(base, LOG_SUFFIX))
}

/// Byte length of the last frame in a single-segment log of `n` records.
fn last_frame_len(n: u64) -> u64 {
    let i = n - 1;
    Record::encoded_len_for(Some(format!("key-{i}").len()), value(i).len()) as u64
}

#[test]
fn truncation_mid_record_keeps_all_complete_records() {
    const N: u64 = 200;
    let last = last_frame_len(N);
    // Cut the final record at every possible byte: in the length prefix,
    // in the header, in the key and in the value.
    for cut in 1..last {
        let dir = tempfile::tempdir().unwrap();
        write_records(dir.path(), N);
        let path = active_log_path(dir.path());
        let full = fs::metadata(&path).unwrap().len();
        OpenOptions::new()
            .write(true)
            .open(&path)
            .unwrap()
            .set_len(full - cut)
            .unwrap();

        let mut log = Log::open(dir.path(), config()).unwrap();
        let rec = log.recovery().active;
        assert_eq!(rec.truncated_bytes, last - cut, "cut={cut}");
        assert_eq!(log.next_offset(), N - 1, "cut={cut}");
        assert_intact(&log, N - 1);
        assert_eq!(fs::metadata(&path).unwrap().len(), full - last);

        // The log keeps working and reuses the lost offset.
        let info = log
            .append(&[(Some(b"key-199".as_slice()), value(199))])
            .unwrap();
        assert_eq!(info.base_offset, N - 1);
        drop(log);
        let log = Log::open(dir.path(), config()).unwrap();
        assert_intact(&log, N);
    }
}

#[test]
fn zero_filled_tail_is_discarded() {
    // Some filesystems extend the file before the data lands, leaving zeros.
    let dir = tempfile::tempdir().unwrap();
    write_records(dir.path(), 50);
    let path = active_log_path(dir.path());
    OpenOptions::new()
        .append(true)
        .open(&path)
        .unwrap()
        .write_all(&[0u8; 777])
        .unwrap();
    let log = Log::open(dir.path(), config()).unwrap();
    assert_eq!(log.recovery().active.truncated_bytes, 777);
    assert_intact(&log, 50);
}

#[test]
fn corrupted_record_truncates_from_that_point() {
    let dir = tempfile::tempdir().unwrap();
    write_records(dir.path(), 100);
    let path = active_log_path(dir.path());
    let mut bytes = fs::read(&path).unwrap();
    // Find the start of record 60 by walking frames and flip a value byte.
    let mut pos = 0usize;
    for _ in 0..60 {
        let len = u32::from_be_bytes(bytes[pos..pos + 4].try_into().unwrap()) as usize;
        pos += 4 + len;
    }
    bytes[pos + 40] ^= 0xAA;
    fs::write(&path, &bytes).unwrap();

    let log = Log::open(dir.path(), config()).unwrap();
    assert_eq!(log.next_offset(), 60);
    assert_eq!(
        log.recovery().active.truncated_bytes,
        (bytes.len() - pos) as u64
    );
    assert_intact(&log, 60);
}

#[test]
fn stale_index_entries_beyond_truncation_are_dropped() {
    let dir = tempfile::tempdir().unwrap();
    write_records(dir.path(), 150);
    let path = active_log_path(dir.path());
    let full = fs::metadata(&path).unwrap().len();
    let index_path = path.with_extension(INDEX_SUFFIX);
    let index_before = fs::read(&index_path).unwrap();
    assert!(!index_before.is_empty());

    // Lose the last ~40% of the file; the index still points past the end.
    let keep = full * 6 / 10;
    OpenOptions::new()
        .write(true)
        .open(&path)
        .unwrap()
        .set_len(keep)
        .unwrap();
    let log = Log::open(dir.path(), config()).unwrap();
    let kept = log.next_offset();
    assert!(kept > 0 && kept < 150);
    assert_intact(&log, kept);
    let index_after = fs::read(&index_path).unwrap();
    assert!(index_after.len() < index_before.len());
    for e in index_after.chunks_exact(8) {
        let pos = u32::from_be_bytes(e[4..].try_into().unwrap()) as u64;
        assert!(pos < fs::metadata(&path).unwrap().len());
    }
    // Every offset is still seekable through the rebuilt index.
    for off in 0..kept {
        let recs = decode_all(&log.read(off, 1).unwrap(), 1 << 20).unwrap();
        assert_eq!(recs[0].offset, off);
    }
}

#[test]
fn crash_only_affects_the_active_segment() {
    let dir = tempfile::tempdir().unwrap();
    let cfg = LogConfig {
        segment_bytes: 2048,
        ..config()
    };
    {
        let mut log = Log::open(dir.path(), cfg.clone()).unwrap();
        for i in 0..300u64 {
            let key = format!("key-{i}");
            log.append(&[(Some(key.as_bytes()), value(i))]).unwrap();
        }
        assert!(log.segment_count() > 3);
    }
    let segs: Vec<u64> = Log::open(dir.path(), cfg.clone())
        .unwrap()
        .segment_base_offsets();
    let active = dir
        .path()
        .join(file_name(*segs.last().unwrap(), LOG_SUFFIX));
    let len = fs::metadata(&active).unwrap().len();
    OpenOptions::new()
        .write(true)
        .open(&active)
        .unwrap()
        .set_len(len - 5)
        .unwrap();

    let log = Log::open(dir.path(), cfg).unwrap();
    assert_eq!(log.segment_base_offsets(), segs);
    assert_eq!(log.next_offset(), 299);
    assert_intact(&log, 299);
}
