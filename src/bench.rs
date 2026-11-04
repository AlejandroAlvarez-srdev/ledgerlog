//! `ledgerlog bench`: produce throughput per batch size, produce/fetch
//! request latency, sequential consume throughput and end-to-end long-poll
//! delivery latency. All numbers are measured on the machine it runs on.

use anyhow::{Context, Result};
use clap::Args;
use ledgerlog::server::{self, ServerConfig, ServerHandle};
use ledgerlog::{Broker, BrokerConfig, Client, FsyncPolicy};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Args, Clone)]
pub struct BenchArgs {
    /// Benchmark an already running server instead of an embedded one.
    #[arg(long)]
    addr: Option<String>,
    /// Data directory for the embedded server (default: a fresh temp dir,
    /// removed afterwards).
    #[arg(long)]
    data_dir: Option<PathBuf>,
    /// fsync policy of the embedded server: always | never | <n>ms.
    #[arg(long, default_value = "100ms")]
    fsync: FsyncPolicy,
    /// Messages produced per batch-size run (capped by --max-requests).
    #[arg(long, default_value_t = 1_000_000)]
    messages: usize,
    /// Upper bound on produce requests per run (keeps batch=1 runs short).
    #[arg(long, default_value_t = 100_000)]
    max_requests: usize,
    /// Value size in bytes.
    #[arg(long, default_value_t = 100)]
    size: usize,
    /// Comma-separated batch sizes to test.
    #[arg(long, default_value = "1,10,100,1000", value_delimiter = ',')]
    batch_sizes: Vec<usize>,
    /// Concurrent producer connections (each writes its own partition).
    #[arg(long, default_value_t = 1)]
    producers: usize,
    /// Random single-record fetches used to measure fetch latency.
    #[arg(long, default_value_t = 20_000)]
    fetch_samples: usize,
    /// Produce→consume round trips used to measure long-poll delivery latency.
    #[arg(long, default_value_t = 2_000)]
    e2e_samples: usize,
}

struct Lat(Vec<Duration>);

impl Lat {
    fn pct(&mut self, p: f64) -> Duration {
        if self.0.is_empty() {
            return Duration::ZERO;
        }
        self.0.sort_unstable();
        let i = ((self.0.len() as f64 - 1.0) * p).round() as usize;
        self.0[i]
    }
    fn fmt(&mut self) -> String {
        format!(
            "{} / {} / {}",
            us(self.pct(0.50)),
            us(self.pct(0.99)),
            us(self.pct(0.999))
        )
    }
}

fn us(d: Duration) -> String {
    let us = d.as_secs_f64() * 1e6;
    if us >= 1000.0 {
        format!("{:.2} ms", us / 1000.0)
    } else {
        format!("{us:.0} µs")
    }
}

/// Simple xorshift so payloads are not all zeros and offsets look random.
struct Rng(u64);
impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }
}

pub async fn run(args: BenchArgs) -> Result<()> {
    let mut embedded: Option<(ServerHandle, PathBuf, bool)> = None;
    let addr = match &args.addr {
        Some(a) => a.clone(),
        None => {
            let (dir, remove) = match &args.data_dir {
                Some(d) => (d.clone(), false),
                None => (
                    std::env::temp_dir().join(format!(
                        "ledgerlog-bench-{}-{}",
                        std::process::id(),
                        ledgerlog::log::now_ms()
                    )),
                    true,
                ),
            };
            let mut cfg = BrokerConfig::new(&dir);
            cfg.log.fsync = args.fsync;
            cfg.log.retention_ms = None;
            cfg.default_partitions = args.producers.max(1) as u32;
            let broker = Arc::new(Broker::open(cfg).context("opening bench broker")?);
            let h = server::spawn(broker, "127.0.0.1:0", ServerConfig::default()).await?;
            let a = h.local_addr().to_string();
            embedded = Some((h, dir, remove));
            a
        }
    };

    println!("# ledgerlog bench");
    println!();
    println!(
        "server: {} | fsync: {} | value size: {} B | producers: {}",
        if args.addr.is_some() {
            "external"
        } else {
            "embedded (same process)"
        },
        if args.addr.is_some() {
            "(server's setting)".to_string()
        } else {
            format!("{:?}", args.fsync)
        },
        args.size,
        args.producers
    );
    println!();
    println!("## Produce throughput");
    println!();
    println!("| batch | messages | msgs/s | MB/s (payload) | request p50 / p99 / p99.9 |");
    println!("|------:|---------:|-------:|---------------:|---------------------------|");

    let run_id = ledgerlog::log::now_ms();
    let mut last_topic = String::new();
    for &batch in &args.batch_sizes {
        let batch = batch.max(1);
        let per_producer = (args.messages / args.producers.max(1))
            .min(batch * args.max_requests / args.producers.max(1))
            .max(batch);
        let topic = format!("bench-{run_id}-b{batch}");
        {
            let mut c = Client::connect(&addr).await?;
            c.create_topic(&topic, args.producers.max(1) as u32).await?;
        }
        let start = Instant::now();
        let mut tasks = Vec::new();
        for p in 0..args.producers.max(1) {
            let addr = addr.clone();
            let topic = topic.clone();
            let size = args.size;
            tasks.push(tokio::spawn(async move {
                let mut c = Client::connect(&addr).await?;
                let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (p as u64 + 1));
                let value: Vec<u8> = (0..size).map(|_| rng.next() as u8).collect();
                let recs: Vec<(Option<&[u8]>, &[u8])> = vec![(None, &value[..]); batch];
                let mut lat = Vec::with_capacity(per_producer / batch + 1);
                let mut sent = 0;
                while sent < per_producer {
                    let t = Instant::now();
                    c.produce(&topic, p as u32, &recs).await?;
                    lat.push(t.elapsed());
                    sent += batch;
                }
                anyhow::Ok((sent, lat))
            }));
        }
        let mut total = 0usize;
        let mut lat = Vec::new();
        for t in tasks {
            let (n, l) = t.await??;
            total += n;
            lat.extend(l);
        }
        let secs = start.elapsed().as_secs_f64();
        let mut lat = Lat(lat);
        println!(
            "| {batch} | {total} | {:.0} | {:.1} | {} |",
            total as f64 / secs,
            (total * args.size) as f64 / secs / 1e6,
            lat.fmt()
        );
        last_topic = topic;
    }

    // Fetch latency: random single-record reads from the last (largest) topic.
    let mut c = Client::connect(&addr).await?;
    let meta = c.metadata(Some(&last_topic)).await?;
    let end = meta[0].partitions[0].end_offset;
    let mut rng = Rng(42);
    let mut lat = Vec::with_capacity(args.fetch_samples);
    for _ in 0..args.fetch_samples {
        let off = rng.next() % end;
        let t = Instant::now();
        let got = c.fetch(&last_topic, 0, off, 1, Duration::ZERO).await?;
        lat.push(t.elapsed());
        debug_assert_eq!(got.records[0].offset, off);
    }
    let mut fetch_lat = Lat(lat);

    // Sequential consume throughput with 1 MiB fetches.
    let t = Instant::now();
    let mut off = 0u64;
    let mut consumed = 0usize;
    let mut fetch_1m = Vec::new();
    while off < end {
        let t1 = Instant::now();
        let got = c
            .fetch(&last_topic, 0, off, 1 << 20, Duration::ZERO)
            .await?;
        fetch_1m.push(t1.elapsed());
        off = got.records.last().map(|r| r.offset + 1).unwrap_or(end);
        consumed += got.records.len();
    }
    let consume_secs = t.elapsed().as_secs_f64();
    let mut fetch_1m = Lat(fetch_1m);

    // End-to-end long-poll delivery latency.
    let e2e_topic = format!("bench-{run_id}-e2e");
    c.create_topic(&e2e_topic, 1).await?;
    let (tx, mut rx) = tokio::sync::mpsc::unbounded_channel::<Instant>();
    let consumer = {
        let addr = addr.clone();
        let topic = e2e_topic.clone();
        let n = args.e2e_samples;
        tokio::spawn(async move {
            let mut c = Client::connect(&addr).await?;
            let mut off = 0u64;
            while (off as usize) < n {
                let got = c
                    .fetch(&topic, 0, off, 1 << 16, Duration::from_secs(5))
                    .await?;
                let now = Instant::now();
                for r in &got.records {
                    off = r.offset + 1;
                    let _ = tx.send(now);
                }
            }
            anyhow::Ok(())
        })
    };
    let mut e2e = Vec::with_capacity(args.e2e_samples);
    let payload = vec![1u8; args.size];
    for _ in 0..args.e2e_samples {
        // Give the consumer time to park in its long poll.
        tokio::time::sleep(Duration::from_millis(1)).await;
        let sent = Instant::now();
        c.produce(&e2e_topic, 0, &[(None::<&[u8]>, &payload[..])])
            .await?;
        let received = rx.recv().await.context("consumer stopped")?;
        e2e.push(received.saturating_duration_since(sent));
    }
    consumer.await??;
    let mut e2e = Lat(e2e);

    println!();
    println!("## Fetch / consume");
    println!();
    println!("| measurement | result |");
    println!("|-------------|--------|");
    println!(
        "| random single-record fetch, p50 / p99 / p99.9 ({} samples over {end} records) | {} |",
        args.fetch_samples,
        fetch_lat.fmt()
    );
    println!(
        "| sequential consume, 1 MiB fetches | {:.0} msgs/s, {:.1} MB/s |",
        consumed as f64 / consume_secs,
        (consumed * args.size) as f64 / consume_secs / 1e6
    );
    println!(
        "| 1 MiB fetch request, p50 / p99 / p99.9 | {} |",
        fetch_1m.fmt()
    );
    println!(
        "| produce → long-poll consumer delivery, p50 / p99 / p99.9 ({} samples) | {} |",
        args.e2e_samples,
        e2e.fmt()
    );

    if let Some((h, dir, remove)) = embedded {
        h.shutdown().await?;
        if remove {
            let _ = std::fs::remove_dir_all(dir);
        }
    }
    Ok(())
}
