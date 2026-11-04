//! Storage configuration.

use std::str::FromStr;
use std::time::Duration;

/// When the log calls `fsync` (`File::sync_data`) on the active segment.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FsyncPolicy {
    /// Sync after every append. An acknowledged write survives power loss.
    Always,
    /// Sync at most once per interval (checked on append and by a background
    /// flusher). Bounded data loss on power failure, near-`Never` throughput.
    Interval(Duration),
    /// Never sync explicitly; rely on the OS page cache. Survives process
    /// crashes, not power loss.
    Never,
}

impl FromStr for FsyncPolicy {
    type Err = String;

    /// Parses `always`, `never`, or `<n>ms` / `interval:<n>` (milliseconds).
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let s = s.trim().to_ascii_lowercase();
        match s.as_str() {
            "always" => Ok(FsyncPolicy::Always),
            "never" => Ok(FsyncPolicy::Never),
            other => {
                let ms = other
                    .strip_prefix("interval:")
                    .or_else(|| other.strip_suffix("ms"))
                    .ok_or_else(|| {
                        format!("invalid fsync policy {other:?} (expected always|never|<n>ms)")
                    })?;
                let ms: u64 = ms
                    .trim()
                    .parse()
                    .map_err(|e| format!("invalid fsync interval {ms:?}: {e}"))?;
                Ok(FsyncPolicy::Interval(Duration::from_millis(ms)))
            }
        }
    }
}

/// Per-partition log configuration.
#[derive(Debug, Clone)]
pub struct LogConfig {
    /// Roll to a new segment once the active one would exceed this size.
    pub segment_bytes: u64,
    /// Roll to a new segment once the active one is older than this
    /// (measured from its first record). `None` disables time-based rolling.
    pub segment_ms: Option<u64>,
    /// Add a sparse index entry every time this many log bytes were written
    /// since the previous entry.
    pub index_interval_bytes: u64,
    /// Delete sealed segments whose newest record is older than this.
    pub retention_ms: Option<u64>,
    /// Delete the oldest sealed segments while the partition is larger than this.
    pub retention_bytes: Option<u64>,
    /// Durability policy for appends.
    pub fsync: FsyncPolicy,
    /// Largest accepted single record frame.
    pub max_record_bytes: usize,
}

impl Default for LogConfig {
    fn default() -> Self {
        Self {
            segment_bytes: 128 * 1024 * 1024,
            segment_ms: None,
            index_interval_bytes: 4096,
            retention_ms: Some(7 * 24 * 3600 * 1000),
            retention_bytes: None,
            fsync: FsyncPolicy::Interval(Duration::from_millis(100)),
            max_record_bytes: 8 * 1024 * 1024,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_fsync_policies() {
        assert_eq!("always".parse::<FsyncPolicy>(), Ok(FsyncPolicy::Always));
        assert_eq!("Never".parse::<FsyncPolicy>(), Ok(FsyncPolicy::Never));
        assert_eq!(
            "250ms".parse::<FsyncPolicy>(),
            Ok(FsyncPolicy::Interval(Duration::from_millis(250)))
        );
        assert_eq!(
            "interval:5".parse::<FsyncPolicy>(),
            Ok(FsyncPolicy::Interval(Duration::from_millis(5)))
        );
        assert!("sometimes".parse::<FsyncPolicy>().is_err());
    }
}
