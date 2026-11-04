//! End-to-end tests: a real server on an ephemeral port and real TCP clients,
//! all in-process.

use ledgerlog::server::{self, ServerConfig, ServerHandle};
use ledgerlog::{Broker, BrokerConfig, Client, Consumer, Error, FsyncPolicy, StartFrom};
use std::path::Path;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

async fn start(dir: &Path) -> ServerHandle {
    let mut cfg = BrokerConfig::new(dir);
    cfg.log.fsync = FsyncPolicy::Interval(Duration::from_millis(50));
    cfg.log.segment_bytes = 16 * 1024;
    cfg.default_partitions = 2;
    let broker = Arc::new(Broker::open(cfg).unwrap());
    server::spawn(broker, "127.0.0.1:0", ServerConfig::default())
        .await
        .unwrap()
}

fn batch(from: usize, n: usize) -> Vec<(Option<String>, String)> {
    (from..from + n)
        .map(|i| (Some(format!("k{i}")), format!("message number {i}")))
        .collect()
}

#[tokio::test]
async fn produce_then_fetch_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start(dir.path()).await;
    let mut c = Client::connect(srv.local_addr()).await.unwrap();

    assert_eq!(c.produce("orders", 0, &batch(0, 10)).await.unwrap(), (0, 9));
    assert_eq!(
        c.produce("orders", 0, &batch(10, 5)).await.unwrap(),
        (10, 14)
    );
    assert_eq!(c.produce("orders", 1, &batch(0, 3)).await.unwrap(), (0, 2));

    let got = c
        .fetch("orders", 0, 3, 1 << 20, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(got.high_watermark, 15);
    assert_eq!(got.records.len(), 12);
    for (i, r) in got.records.iter().enumerate() {
        let n = i + 3;
        assert_eq!(r.offset, n as u64);
        assert_eq!(r.key.as_deref(), Some(format!("k{n}").as_bytes()));
        assert_eq!(r.value, format!("message number {n}").as_bytes());
    }

    let meta = c.metadata(Some("orders")).await.unwrap();
    assert_eq!(meta[0].partitions.len(), 2);
    assert_eq!(meta[0].partitions[1].end_offset, 3);
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn fetch_respects_max_bytes_but_always_returns_one_record() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start(dir.path()).await;
    let mut c = Client::connect(srv.local_addr()).await.unwrap();
    let big = vec![(None::<&[u8]>, vec![7u8; 10_000]); 4];
    c.produce("blobs", 0, &big).await.unwrap();
    let got = c.fetch("blobs", 0, 0, 100, Duration::ZERO).await.unwrap();
    assert_eq!(got.records.len(), 1);
    let got = c
        .fetch("blobs", 0, 1, 25_000, Duration::ZERO)
        .await
        .unwrap();
    assert_eq!(got.records.len(), 2);
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn long_poll_wakes_up_on_produce() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start(dir.path()).await;
    let addr = srv.local_addr();
    let mut c = Client::connect(addr).await.unwrap();
    c.create_topic("live", 1).await.unwrap();

    let waiter = tokio::spawn(async move {
        let mut c = Client::connect(addr).await.unwrap();
        let t0 = Instant::now();
        let got = c
            .fetch("live", 0, 0, 1 << 20, Duration::from_secs(10))
            .await
            .unwrap();
        (got, t0.elapsed())
    });
    tokio::time::sleep(Duration::from_millis(150)).await;
    c.produce("live", 0, &[(None::<&[u8]>, b"wake up")])
        .await
        .unwrap();

    let (got, elapsed) = waiter.await.unwrap();
    assert_eq!(got.records.len(), 1);
    assert_eq!(got.records[0].value.as_ref(), b"wake up");
    assert!(elapsed >= Duration::from_millis(100), "{elapsed:?}");
    assert!(elapsed < Duration::from_secs(5), "{elapsed:?}");
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn long_poll_times_out_empty() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start(dir.path()).await;
    let mut c = Client::connect(srv.local_addr()).await.unwrap();
    c.create_topic("quiet", 1).await.unwrap();
    let t0 = Instant::now();
    let got = c
        .fetch("quiet", 0, 0, 1024, Duration::from_millis(200))
        .await
        .unwrap();
    assert!(got.records.is_empty());
    assert!(t0.elapsed() >= Duration::from_millis(190));
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn consumer_group_resumes_from_committed_offset_across_restart() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start(dir.path()).await;
    let addr = srv.local_addr();
    let mut p = Client::connect(addr).await.unwrap();
    p.produce("jobs", 0, &batch(0, 100)).await.unwrap();

    let mut consumer = Consumer::new(
        Client::connect(addr).await.unwrap(),
        Some("workers"),
        "jobs",
        0,
        StartFrom::Earliest,
    )
    .await
    .unwrap();
    consumer.max_bytes = 2_000;
    let mut seen = Vec::new();
    while seen.len() < 40 {
        seen.extend(consumer.poll().await.unwrap());
    }
    let committed = consumer.position();
    consumer.commit().await.unwrap();
    drop(consumer);
    drop(p);
    srv.shutdown().await.unwrap();

    // Restart on the same data directory.
    let srv = start(dir.path()).await;
    let addr = srv.local_addr();
    let mut c = Client::connect(addr).await.unwrap();
    assert_eq!(
        c.fetch_offset("workers", "jobs", 0).await.unwrap(),
        Some(committed)
    );
    let mut consumer = Consumer::new(c, Some("workers"), "jobs", 0, StartFrom::Earliest)
        .await
        .unwrap();
    assert_eq!(consumer.position(), committed);
    let mut rest = Vec::new();
    while consumer.position() < 100 {
        rest.extend(consumer.poll().await.unwrap());
    }
    assert_eq!(rest.first().unwrap().offset, committed);
    assert_eq!(seen.len() + rest.len(), 100);

    // A fresh group with `latest` starts at the end.
    let latest = Consumer::new(
        Client::connect(addr).await.unwrap(),
        Some("new-group"),
        "jobs",
        0,
        StartFrom::Latest,
    )
    .await
    .unwrap();
    assert_eq!(latest.position(), 100);
    srv.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_producers_get_unique_contiguous_offsets() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start(dir.path()).await;
    let addr = srv.local_addr();
    let mut tasks = Vec::new();
    for w in 0..8 {
        tasks.push(tokio::spawn(async move {
            let mut c = Client::connect(addr).await.unwrap();
            let mut ranges = Vec::new();
            for b in 0..50 {
                let recs = vec![(None::<String>, format!("w{w}-b{b}")); 7];
                ranges.push(c.produce("shared", 0, &recs).await.unwrap());
            }
            ranges
        }));
    }
    let mut ranges = Vec::new();
    for t in tasks {
        ranges.extend(t.await.unwrap());
    }
    ranges.sort();
    let mut expected = 0;
    for (base, last) in ranges {
        assert_eq!(base, expected);
        assert_eq!(last, base + 6);
        expected = last + 1;
    }
    assert_eq!(expected, 8 * 50 * 7);

    // Every batch is stored contiguously (batches are atomic appends).
    let mut c = Client::connect(addr).await.unwrap();
    let mut off = 0;
    let mut all = Vec::new();
    while off < expected {
        let got = c
            .fetch("shared", 0, off, 1 << 20, Duration::ZERO)
            .await
            .unwrap();
        off = got.records.last().unwrap().offset + 1;
        all.extend(got.records);
    }
    for chunk in all.chunks(7) {
        assert!(chunk.iter().all(|r| r.value == chunk[0].value));
    }
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn errors_are_reported_and_connection_survives() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start(dir.path()).await;
    let mut c = Client::connect(srv.local_addr()).await.unwrap();
    c.produce("t", 0, &batch(0, 3)).await.unwrap();

    assert!(matches!(
        c.fetch("t", 0, 50, 1024, Duration::ZERO).await,
        Err(Error::OffsetOutOfRange {
            requested: 50,
            start: 0,
            end: 3
        })
    ));
    assert!(matches!(
        c.fetch("t", 9, 0, 1024, Duration::ZERO).await,
        Err(Error::Server { code: 3, .. })
    ));
    assert!(matches!(
        c.create_topic("t", 1).await,
        Err(Error::Server { code: 4, .. })
    ));
    assert!(c.produce("bad/name", 0, &batch(0, 1)).await.is_err());
    // Same connection still works.
    assert_eq!(c.produce("t", 0, &batch(3, 1)).await.unwrap(), (3, 3));
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn garbage_frame_closes_only_that_connection() {
    let dir = tempfile::tempdir().unwrap();
    let srv = start(dir.path()).await;
    let mut raw = tokio::net::TcpStream::connect(srv.local_addr())
        .await
        .unwrap();
    raw.write_all(&[0, 0, 0, 5, 99, 0, 0, 0, 1]).await.unwrap();
    let mut buf = Vec::new();
    raw.read_to_end(&mut buf).await.unwrap(); // server replies with an error, then closes
    assert!(buf.len() > 9);

    let mut c = Client::connect(srv.local_addr()).await.unwrap();
    assert_eq!(c.produce("ok", 0, &batch(0, 1)).await.unwrap(), (0, 0));
    srv.shutdown().await.unwrap();
}

#[tokio::test]
async fn data_survives_restart_and_segments_roll() {
    let dir = tempfile::tempdir().unwrap();
    {
        let srv = start(dir.path()).await;
        let mut c = Client::connect(srv.local_addr()).await.unwrap();
        for i in 0..50 {
            c.produce("log", 0, &batch(i * 20, 20)).await.unwrap();
        }
        srv.shutdown().await.unwrap();
    }
    let srv = start(dir.path()).await;
    let mut c = Client::connect(srv.local_addr()).await.unwrap();
    let meta = c.metadata(Some("log")).await.unwrap();
    let p0 = &meta[0].partitions[0];
    assert_eq!(p0.end_offset, 1000);
    assert!(
        p0.segments > 1,
        "expected rolled segments, got {}",
        p0.segments
    );
    let mut off = 0;
    while off < 1000 {
        let got = c
            .fetch("log", 0, off, 64 * 1024, Duration::ZERO)
            .await
            .unwrap();
        for r in &got.records {
            assert_eq!(r.offset, off);
            assert_eq!(r.value, format!("message number {off}").as_bytes());
            off += 1;
        }
    }
    srv.shutdown().await.unwrap();
}
