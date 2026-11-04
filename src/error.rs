use std::io;

/// Errors produced by the storage engine, broker and client.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("I/O error: {0}")]
    Io(#[from] io::Error),

    #[error("corrupt record at byte {position}: {reason}")]
    Corrupt { position: u64, reason: String },

    #[error("offset {requested} is out of range [{start}, {end})")]
    OffsetOutOfRange {
        requested: u64,
        start: u64,
        end: u64,
    },

    #[error("unknown topic or partition: {topic}/{partition}")]
    UnknownTopicOrPartition { topic: String, partition: u32 },

    #[error("topic already exists: {0}")]
    TopicExists(String),

    #[error("invalid request: {0}")]
    InvalidRequest(String),

    #[error("record too large: {size} bytes (max {max})")]
    RecordTooLarge { size: usize, max: usize },

    #[error("protocol error: {0}")]
    Protocol(String),

    #[error("server error ({code}): {message}")]
    Server { code: u8, message: String },
}

pub type Result<T> = std::result::Result<T, Error>;
