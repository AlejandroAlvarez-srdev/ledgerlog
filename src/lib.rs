//! # ledgerlog
//!
//! A durable, segmented, append-only commit log — a small Kafka written from
//! scratch. The crate is layered bottom-up:
//!
//! * [`record`] — the on-disk / on-wire record frame
//!   (`len | crc32 | offset | timestamp | key | value`).
//! * [`segment`] — one `.log` file plus its sparse `.index` file.
//! * [`log`] — a partition: an ordered set of segments with rolling,
//!   retention and crash recovery.
//! * [`broker`] — topics → partitions, plus persisted consumer-group offsets
//!   ([`offsets`]).
//! * [`protocol`], [`server`], [`client`] — a compact length-prefixed binary
//!   protocol over TCP, a tokio server and an async client.

pub mod config;
pub mod error;
pub mod io;
pub mod record;

pub use config::{FsyncPolicy, LogConfig};
pub use error::{Error, Result};
pub use record::Record;
