//! Tokio TCP server.
//!
//! One task per connection reads frames, dispatches them and writes responses
//! in order. Broker calls do blocking file I/O and run on tokio's blocking
//! pool. Fetches long-poll: if no data is available the handler waits on the
//! partition's high-watermark channel until data arrives or `max_wait_ms`
//! elapses. Two background tasks drive interval fsyncs and retention.

use crate::broker::Broker;
use crate::config::FsyncPolicy;
use crate::error::{Error, Result};
use crate::protocol::{self, Request, Response};
use bytes::{Bytes, BytesMut};
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::watch;
use tokio::task::JoinHandle;
use tokio::time::Instant;

#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// How often retention is enforced.
    pub retention_check_interval: Duration,
    /// Upper bound for a client-requested long-poll wait.
    pub max_fetch_wait: Duration,
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            retention_check_interval: Duration::from_secs(30),
            max_fetch_wait: Duration::from_secs(30),
        }
    }
}

/// A running server; dropping it does not stop it, call [`ServerHandle::shutdown`].
pub struct ServerHandle {
    addr: SocketAddr,
    shutdown: watch::Sender<bool>,
    task: JoinHandle<Result<()>>,
}

impl ServerHandle {
    pub fn local_addr(&self) -> SocketAddr {
        self.addr
    }

    /// Stop accepting, let connections finish their current request, and
    /// fsync every partition.
    pub async fn shutdown(self) -> Result<()> {
        let _ = self.shutdown.send(true);
        self.task
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e)))?
    }
}

/// Bind `addr` and serve `broker` in a background task.
pub async fn spawn(broker: Arc<Broker>, addr: &str, config: ServerConfig) -> Result<ServerHandle> {
    let listener = TcpListener::bind(addr).await?;
    let addr = listener.local_addr()?;
    let (tx, rx) = watch::channel(false);
    let task = tokio::spawn(run(listener, broker, config, rx));
    Ok(ServerHandle {
        addr,
        shutdown: tx,
        task,
    })
}

/// Serve until `shutdown` flips to `true`.
pub async fn run(
    listener: TcpListener,
    broker: Arc<Broker>,
    config: ServerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let mut background = Vec::new();
    if let FsyncPolicy::Interval(every) = broker.config().log.fsync {
        let b = broker.clone();
        let mut stop = shutdown.clone();
        background.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let b = b.clone();
                        if let Ok(Err(e)) = tokio::task::spawn_blocking(move || b.sync_due()).await {
                            tracing::error!(error = %e, "background fsync failed");
                        }
                    }
                    _ = stop.changed() => break,
                }
            }
        }));
    }
    {
        let b = broker.clone();
        let mut stop = shutdown.clone();
        let every = config.retention_check_interval;
        background.push(tokio::spawn(async move {
            let mut tick = tokio::time::interval(every);
            loop {
                tokio::select! {
                    _ = tick.tick() => {
                        let b = b.clone();
                        match tokio::task::spawn_blocking(move || b.enforce_retention()).await {
                            Ok(Ok(n)) if n > 0 => tracing::info!(segments = n, "retention removed segments"),
                            Ok(Err(e)) => tracing::error!(error = %e, "retention failed"),
                            _ => {}
                        }
                    }
                    _ = stop.changed() => break,
                }
            }
        }));
    }

    tracing::info!(addr = %listener.local_addr()?, "ledgerlog listening");
    let mut connections = tokio::task::JoinSet::new();
    loop {
        tokio::select! {
            accepted = listener.accept() => {
                let (stream, peer) = match accepted {
                    Ok(x) => x,
                    Err(e) => {
                        tracing::warn!(error = %e, "accept failed");
                        continue;
                    }
                };
                let _ = stream.set_nodelay(true);
                let broker = broker.clone();
                let stop = shutdown.clone();
                let cfg = config.clone();
                connections.spawn(async move {
                    if let Err(e) = handle_connection(stream, broker, cfg, stop).await {
                        tracing::debug!(%peer, error = %e, "connection closed with error");
                    }
                });
            }
            Some(_) = connections.join_next(), if !connections.is_empty() => {}
            _ = shutdown.changed() => break,
        }
    }
    drop(listener);
    while connections.join_next().await.is_some() {}
    for t in background {
        let _ = t.await;
    }
    let b = broker.clone();
    tokio::task::spawn_blocking(move || b.sync_all())
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e)))??;
    tracing::info!("ledgerlog stopped");
    Ok(())
}

async fn handle_connection(
    stream: TcpStream,
    broker: Arc<Broker>,
    config: ServerConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let (mut rd, mut wr) = stream.into_split();
    let mut out = BytesMut::with_capacity(64 * 1024);
    loop {
        let frame = tokio::select! {
            f = protocol::read_frame(&mut rd) => f?,
            _ = shutdown.changed() => return Ok(()),
        };
        let Some(frame) = frame else { return Ok(()) };
        out.clear();
        match Request::decode(frame) {
            Ok((cid, req)) => match dispatch(&broker, &config, req).await {
                Ok(resp) => resp.encode(cid, &mut out),
                Err(e) => Response::encode_error(&e, cid, &mut out),
            },
            Err(e) => {
                // We cannot trust the stream position any more: reply and close.
                Response::encode_error(&e, 0, &mut out);
                protocol::write_all(&mut wr, &out).await?;
                return Err(e);
            }
        }
        protocol::write_all(&mut wr, &out).await?;
    }
}

async fn blocking<T, F>(f: F) -> Result<T>
where
    F: FnOnce() -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e)))?
}

async fn dispatch(broker: &Arc<Broker>, config: &ServerConfig, req: Request) -> Result<Response> {
    let b = broker.clone();
    match req {
        Request::Produce {
            topic,
            partition,
            records,
        } => {
            let info = blocking(move || b.produce(&topic, partition, &records)).await?;
            Ok(Response::Produce {
                base_offset: info.base_offset,
                last_offset: info.last_offset,
            })
        }
        Request::Fetch {
            topic,
            partition,
            offset,
            max_bytes,
            max_wait_ms,
        } => {
            let wait = Duration::from_millis(u64::from(max_wait_ms)).min(config.max_fetch_wait);
            long_poll_fetch(b, topic, partition, offset, max_bytes as usize, wait).await
        }
        Request::CommitOffset {
            group,
            topic,
            partition,
            offset,
        } => {
            blocking(move || b.commit_offset(&group, &topic, partition, offset)).await?;
            Ok(Response::CommitOffset)
        }
        Request::FetchOffset {
            group,
            topic,
            partition,
        } => Ok(Response::FetchOffset {
            offset: b.fetch_offset(&group, &topic, partition)?,
        }),
        Request::CreateTopic { topic, partitions } => {
            blocking(move || b.create_topic(&topic, partitions)).await?;
            Ok(Response::CreateTopic)
        }
        Request::Metadata { topic } => Ok(Response::Metadata {
            topics: blocking(move || b.metadata(topic.as_deref())).await?,
        }),
    }
}

async fn long_poll_fetch(
    broker: Arc<Broker>,
    topic: String,
    partition: u32,
    offset: u64,
    max_bytes: usize,
    wait: Duration,
) -> Result<Response> {
    let deadline = Instant::now() + wait;
    let part = broker.partition(&topic, partition)?;
    loop {
        // Subscribe *before* reading so an append between the read and the
        // wait cannot be missed.
        let mut rx = part.subscribe();
        let b = broker.clone();
        let t = topic.clone();
        let res = blocking(move || b.fetch(&t, partition, offset, max_bytes)).await?;
        if !res.records.is_empty() || wait.is_zero() || Instant::now() >= deadline {
            return Ok(Response::Fetch {
                start_offset: res.start_offset,
                high_watermark: res.high_watermark,
                records: Bytes::from(res.records),
            });
        }
        let woke = tokio::time::timeout_at(deadline, rx.wait_for(|hw| *hw > offset)).await;
        if !matches!(woke, Ok(Ok(_))) {
            return Ok(Response::Fetch {
                start_offset: res.start_offset,
                high_watermark: res.high_watermark,
                records: Bytes::new(),
            });
        }
    }
}
