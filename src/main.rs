use anyhow::{Context, Result};
use clap::{Args, Parser, Subcommand};
use ledgerlog::server::{self, ServerConfig};
use ledgerlog::{Broker, BrokerConfig, Client, Consumer, FsyncPolicy, StartFrom};
use std::io::{BufRead, Write};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

mod bench;

#[derive(Parser)]
#[command(
    name = "ledgerlog",
    version,
    about = "A durable, segmented commit log (a mini-Kafka)"
)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Run a broker.
    Serve(ServeArgs),
    /// Create a topic.
    CreateTopic {
        #[arg(long, default_value = "127.0.0.1:9092")]
        addr: String,
        #[arg(long)]
        topic: String,
        #[arg(long, default_value_t = 1)]
        partitions: u32,
    },
    /// List topics, partitions and offsets.
    Topics {
        #[arg(long, default_value = "127.0.0.1:9092")]
        addr: String,
    },
    /// Produce messages from --message flags or from stdin (one per line).
    Produce {
        #[arg(long, default_value = "127.0.0.1:9092")]
        addr: String,
        #[arg(long)]
        topic: String,
        #[arg(long, default_value_t = 0)]
        partition: u32,
        /// Key attached to every message.
        #[arg(long)]
        key: Option<String>,
        /// Message value (repeatable). If absent, lines are read from stdin.
        #[arg(long = "message", short = 'm')]
        messages: Vec<String>,
        /// Lines per produce request when reading stdin.
        #[arg(long, default_value_t = 500)]
        batch_size: usize,
    },
    /// Consume messages from one partition.
    Consume(ConsumeArgs),
    /// Measure produce throughput and fetch latency.
    Bench(bench::BenchArgs),
}

#[derive(Args)]
struct ServeArgs {
    #[arg(long, default_value = "127.0.0.1:9092")]
    addr: String,
    #[arg(long, default_value = "./data")]
    data_dir: PathBuf,
    /// Maximum segment size in bytes before rolling.
    #[arg(long, default_value_t = 128 * 1024 * 1024)]
    segment_bytes: u64,
    /// Roll the active segment once it is this old (ms).
    #[arg(long)]
    segment_ms: Option<u64>,
    /// Delete sealed segments older than this (ms). 0 disables.
    #[arg(long, default_value_t = 7 * 24 * 3600 * 1000)]
    retention_ms: u64,
    /// Keep each partition under this many bytes (sealed segments only).
    #[arg(long)]
    retention_bytes: Option<u64>,
    /// always | never | <n>ms
    #[arg(long, default_value = "100ms")]
    fsync: FsyncPolicy,
    /// Partitions for auto-created topics.
    #[arg(long, default_value_t = 1)]
    default_partitions: u32,
    /// Log bytes between sparse index entries.
    #[arg(long, default_value_t = 4096)]
    index_interval_bytes: u64,
    /// Seconds between retention passes.
    #[arg(long, default_value_t = 30)]
    retention_check_secs: u64,
}

#[derive(Args)]
struct ConsumeArgs {
    #[arg(long, default_value = "127.0.0.1:9092")]
    addr: String,
    #[arg(long)]
    topic: String,
    #[arg(long, default_value_t = 0)]
    partition: u32,
    /// Consumer group; its committed offset is used and advanced.
    #[arg(long)]
    group: Option<String>,
    /// Start position when the group has no committed offset:
    /// earliest | latest | <offset>.
    #[arg(long, default_value = "earliest")]
    from: StartFrom,
    /// Keep long-polling for new messages instead of exiting when caught up.
    #[arg(long, short = 'f')]
    follow: bool,
    /// Stop after this many messages.
    #[arg(long, short = 'n')]
    max_messages: Option<u64>,
    /// Print only values (no offset/key columns).
    #[arg(long)]
    values_only: bool,
}

fn init_tracing(default: &str) {
    let filter = tracing_subscriber::EnvFilter::try_from_default_env()
        .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new(default));
    tracing_subscriber::fmt()
        .with_env_filter(filter)
        .with_writer(std::io::stderr)
        .init();
}

#[tokio::main]
async fn main() -> Result<()> {
    let cli = Cli::parse();
    init_tracing(match cli.command {
        Command::Serve(_) => "info",
        _ => "warn",
    });
    match cli.command {
        Command::Serve(args) => serve(args).await,
        Command::CreateTopic {
            addr,
            topic,
            partitions,
        } => {
            let mut c = Client::connect(&addr).await?;
            c.create_topic(&topic, partitions).await?;
            eprintln!("created topic {topic} with {partitions} partition(s)");
            Ok(())
        }
        Command::Topics { addr } => topics(&addr).await,
        Command::Produce {
            addr,
            topic,
            partition,
            key,
            messages,
            batch_size,
        } => produce(&addr, &topic, partition, key, messages, batch_size.max(1)).await,
        Command::Consume(args) => consume(args).await,
        Command::Bench(args) => bench::run(args).await,
    }
}

async fn serve(args: ServeArgs) -> Result<()> {
    let mut cfg = BrokerConfig::new(&args.data_dir);
    cfg.default_partitions = args.default_partitions;
    cfg.log.segment_bytes = args.segment_bytes;
    cfg.log.segment_ms = args.segment_ms;
    cfg.log.retention_ms = (args.retention_ms > 0).then_some(args.retention_ms);
    cfg.log.retention_bytes = args.retention_bytes;
    cfg.log.fsync = args.fsync;
    cfg.log.index_interval_bytes = args.index_interval_bytes;

    let t0 = Instant::now();
    let broker = Arc::new(
        Broker::open(cfg).with_context(|| format!("opening {}", args.data_dir.display()))?,
    );
    tracing::info!(
        data_dir = %args.data_dir.display(),
        fsync = ?args.fsync,
        recovered_in_ms = t0.elapsed().as_millis() as u64,
        "broker opened"
    );
    let srv_cfg = ServerConfig {
        retention_check_interval: Duration::from_secs(args.retention_check_secs.max(1)),
        ..ServerConfig::default()
    };
    let handle = server::spawn(broker, &args.addr, srv_cfg).await?;
    tokio::signal::ctrl_c().await?;
    tracing::info!("shutting down");
    handle.shutdown().await?;
    Ok(())
}

async fn topics(addr: &str) -> Result<()> {
    let mut c = Client::connect(addr).await?;
    println!(
        "{:<24} {:>9} {:>14} {:>14} {:>12} {:>8}",
        "TOPIC", "PARTITION", "START", "END", "BYTES", "SEGMENTS"
    );
    for t in c.metadata(None).await? {
        for p in t.partitions {
            println!(
                "{:<24} {:>9} {:>14} {:>14} {:>12} {:>8}",
                t.name, p.partition, p.start_offset, p.end_offset, p.size_bytes, p.segments
            );
        }
    }
    Ok(())
}

async fn produce(
    addr: &str,
    topic: &str,
    partition: u32,
    key: Option<String>,
    messages: Vec<String>,
    batch_size: usize,
) -> Result<()> {
    let mut c = Client::connect(addr).await?;
    let with_key = |batch: Vec<String>| -> Vec<(Option<String>, String)> {
        batch.into_iter().map(|v| (key.clone(), v)).collect()
    };
    if !messages.is_empty() {
        let recs = with_key(messages);
        let (base, last) = c.produce(topic, partition, &recs).await?;
        eprintln!(
            "produced {} message(s) at offsets {base}..={last}",
            recs.len()
        );
        return Ok(());
    }
    let mut total = 0usize;
    let mut batch = Vec::with_capacity(batch_size);
    let mut lines = std::io::stdin().lock().lines();
    loop {
        let line = lines.next().transpose()?;
        let done = line.is_none();
        if let Some(l) = line {
            batch.push(l);
        }
        if batch.len() == batch_size || (done && !batch.is_empty()) {
            let recs = with_key(std::mem::take(&mut batch));
            c.produce(topic, partition, &recs).await?;
            total += recs.len();
        }
        if done {
            break;
        }
    }
    eprintln!("produced {total} message(s)");
    Ok(())
}

async fn consume(args: ConsumeArgs) -> Result<()> {
    let client = Client::connect(&args.addr).await?;
    let mut consumer = Consumer::new(
        client,
        args.group.as_deref(),
        &args.topic,
        args.partition,
        args.from,
    )
    .await?;
    consumer.max_wait = if args.follow {
        Duration::from_secs(5)
    } else {
        Duration::from_millis(100)
    };
    let limit = args.max_messages.unwrap_or(u64::MAX);
    let mut seen = 0u64;
    while seen < limit {
        let records = consumer.poll().await?;
        if records.is_empty() {
            if args.follow {
                continue;
            }
            break;
        }
        let mut out = std::io::stdout().lock();
        for r in &records {
            let value = String::from_utf8_lossy(&r.value);
            if args.values_only {
                writeln!(out, "{value}")?;
            } else {
                let key = r
                    .key
                    .as_ref()
                    .map(|k| String::from_utf8_lossy(k).into_owned())
                    .unwrap_or_else(|| "-".into());
                writeln!(out, "{}\t{}\t{}", r.offset, key, value)?;
            }
            seen += 1;
            if seen == limit {
                // Commit only what was actually printed.
                consumer.seek(r.offset + 1);
                break;
            }
        }
        drop(out);
        consumer.commit().await?;
    }
    if let Some(g) = &args.group {
        eprintln!(
            "consumed {seen} message(s); group {g} committed offset {}",
            consumer.position()
        );
    } else {
        eprintln!("consumed {seen} message(s)");
    }
    Ok(())
}
