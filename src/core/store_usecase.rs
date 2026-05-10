//! Store-command orchestration (pure domain).
//!
//! ADR-003: this module owns the `CoreKafkaConsumer` port, the
//! `StoreKafkaCommand` use-case input, the `PumpTask` pipeline, filtering,
//! limits, and the rdkafka-free domain types (`TopicMetadata`,
//! `TopicPartitionsAssignment`, `DomainOffset`). It must not import
//! `rdkafka` — the adapter lives in `crate::kafka::consumer`.

use std::collections::HashMap;
use std::fmt;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use regex::Regex;
use tokio::signal;
use tokio::sync::mpsc;
use tracing::{debug, error, info};

use crate::core::format::MessageFormat;
use crate::core::models::KafkaMessage;
use crate::storage::files::{DirectoryStorage, DirectoryStorageConfig};
use crate::storage::StorageBackend;

// ---------------------------------------------------------------------------
// Use-case input (CLI translator → here)
// ---------------------------------------------------------------------------

pub struct StoreKafkaCommand {
    pub(crate) bootstrap_servers: String,
    pub(crate) topic: String,
    pub(crate) group_id: String,
    pub(crate) from: StoreKafkaFrom,
    pub(crate) to: StoreKafkaTo,
    pub(crate) store_to_storage: StoreKafkaToStorageBackend,
    pub(crate) format: Arc<dyn MessageFormat + Send + Sync>,
    pub(crate) key_regex: Option<String>,
    pub(crate) headers: Option<HashMap<String, String>>,
    pub(crate) partitions: Option<Vec<i32>>,
    pub(crate) limit_count: Option<u64>,
    pub(crate) limit_until_offset: Option<u64>,
    pub(crate) limit_until_timestamp: Option<i64>,
}

impl fmt::Debug for StoreKafkaCommand {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("StoreKafkaCommand")
            .field("bootstrap_servers", &self.bootstrap_servers)
            .field("topic", &self.topic)
            .field("group_id", &self.group_id)
            .field("from", &self.from)
            .field("to", &self.to)
            .field("store_to_storage", &self.store_to_storage)
            .field("format", &self.format.format_name())
            .field("key_regex", &self.key_regex)
            .field("headers", &self.headers)
            .field("partitions", &self.partitions)
            .field("limit_count", &self.limit_count)
            .field("limit_until_offset", &self.limit_until_offset)
            .field("limit_until_timestamp", &self.limit_until_timestamp)
            .finish()
    }
}

#[derive(Debug)]
pub enum StoreKafkaFrom {
    FromBegining,
    FromEnd,
    FromTimestamp(i64),
    FromOffset(u64),
}

#[derive(Debug)]
pub enum StoreKafkaToStorageBackend {
    Directory(String),
}

#[derive(Debug)]
pub enum StoreKafkaTo {
    Live,
}

// ---------------------------------------------------------------------------
// Domain types (rdkafka-free)
// ---------------------------------------------------------------------------

/// Snapshot of a topic's partitions, owned and detached from any wire-library
/// lifetime.
#[derive(Debug, Clone)]
pub struct TopicMetadata {
    pub name: String,
    pub partitions: Vec<PartitionMetadata>,
}

#[derive(Debug, Clone)]
pub struct PartitionMetadata {
    pub id: i32,
    pub leader: i32,
    pub replicas: Vec<i32>,
    pub isr: Vec<i32>,
}

/// Offset specifier in domain terms.
///
/// Matches the semantics of rdkafka's `Offset` enum but without the
/// dependency. `Offset(i64)` carries either a literal Kafka offset (for
/// `assign`) or a millisecond timestamp (when used as input to
/// `offsets_for_times`). The adapter handles the encoding asymmetry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DomainOffset {
    Beginning,
    End,
    Offset(i64),
}

#[derive(Debug, Clone)]
pub struct PartitionOffset {
    pub topic: String,
    pub partition: i32,
    pub offset: DomainOffset,
}

#[derive(Debug, Clone)]
pub struct TopicPartitionsAssignment {
    pub entries: Vec<PartitionOffset>,
}

// ---------------------------------------------------------------------------
// Port: Kafka consumer (the adapter — `RdKafkaConsumer` — lives in
// `crate::kafka::consumer`).
// ---------------------------------------------------------------------------

#[async_trait]
pub trait CoreKafkaConsumer: Send + Sync {
    async fn initialize(&self) -> Result<()>;
    async fn fetch_metadata(&self) -> Result<TopicMetadata>;
    async fn fetch_offset_positions(&self) -> Result<TopicPartitionsAssignment>;
    async fn assign(&self, tpl: TopicPartitionsAssignment) -> Result<()>;
    /// Receive next KafkaMessage within the given timeout (milliseconds).
    /// Returns `Ok(None)` on timeout without a message.
    async fn recv_next(&self, timeout_ms: u64) -> Result<Option<KafkaMessage>>;
    /// Stop the consumer and clean up resources.
    async fn stop(&self) -> Result<()>;
    /// Resolve a TPL whose `Offset(i64)` entries are millisecond timestamps
    /// into a TPL whose entries are real offsets.
    async fn offsets_for_times(
        &self,
        tpl: TopicPartitionsAssignment,
        timeout_ms: u64,
    ) -> Result<TopicPartitionsAssignment>;
}

// ---------------------------------------------------------------------------
// Use-case entry point
// ---------------------------------------------------------------------------

type CommandExecutionResult = ();

pub async fn store(command: StoreKafkaCommand) -> Result<CommandExecutionResult> {
    debug!("store command: {:?}", command);

    let consumer = crate::kafka::consumer::create_rdkafka_consumer(
        command.bootstrap_servers.clone(),
        command.topic.clone(),
        command.group_id.clone(),
    )?;
    consumer.initialize().await?;

    let topic_metadata = consumer.fetch_metadata().await?;
    print_metadata(&topic_metadata);

    let mut topic_assignment = parse_topic_assignment(&command, &topic_metadata);

    if matches!(command.from, StoreKafkaFrom::FromTimestamp(_)) {
        topic_assignment = consumer.offsets_for_times(topic_assignment, 10_000).await?;
    }

    consumer.assign(topic_assignment).await?;

    let topic_offset_positions = consumer.fetch_offset_positions().await?;
    print_topic_offset_positions(&topic_offset_positions);

    let consumer = Arc::new(consumer);

    let storage = Arc::new(create_storage(&command)?);
    storage
        .initialize()
        .await
        .map_err(|e| anyhow::anyhow!("Storage init error: {}", e))?;

    let pump_task = create_task(consumer.clone(), storage.clone(), &command)?;

    // Set up signal handling for graceful shutdown
    let ctrl_c = async {
        signal::ctrl_c()
            .await
            .expect("Failed to listen for ctrl-c signal");
    };

    let term_signal = async {
        signal::unix::signal(signal::unix::SignalKind::terminate())
            .expect("Failed to listen for terminate signal")
            .recv()
            .await;
    };

    let processed = tokio::select! {
        _ = ctrl_c => {
            info!("Received interrupt signal, shutting down gracefully...");
            return Err(anyhow::anyhow!("Interrupted by user"));
        }
        _ = term_signal => {
            info!("Received termination signal, shutting down gracefully...");
            return Err(anyhow::anyhow!("Terminated by signal"));
        }
        result = pump_task.run() => {
            result?
        }
    };

    info!("Stored {} messages", processed);

    storage
        .close()
        .await
        .map_err(|e| anyhow::anyhow!("Storage close error: {}", e))?;

    consumer.stop().await?;
    info!("Finished storing messages");
    drop(consumer);

    Ok(())
}

// ---------------------------------------------------------------------------
// Domain helpers
// ---------------------------------------------------------------------------

pub fn parse_topic_assignment(
    cmd: &StoreKafkaCommand,
    metadata: &TopicMetadata,
) -> TopicPartitionsAssignment {
    let domain_offset = match cmd.from {
        StoreKafkaFrom::FromBegining => DomainOffset::Beginning,
        StoreKafkaFrom::FromEnd => DomainOffset::End,
        // For FromTimestamp the value is carried in the offset slot until
        // `offsets_for_times` resolves it. See `DomainOffset` rustdoc.
        StoreKafkaFrom::FromTimestamp(ts) => DomainOffset::Offset(ts),
        StoreKafkaFrom::FromOffset(off) => DomainOffset::Offset(off as i64),
    };
    let entries = metadata
        .partitions
        .iter()
        .map(|p| PartitionOffset {
            topic: cmd.topic.clone(),
            partition: p.id,
            offset: domain_offset,
        })
        .collect();
    TopicPartitionsAssignment { entries }
}

fn print_metadata(metadata: &TopicMetadata) {
    info!("Topic metadata:");
    info!("  Topic: {}", metadata.name);
    for p in &metadata.partitions {
        info!(
            "    Partition {}: leader: {}, replicas: {:?}, isr: {:?}",
            p.id, p.leader, p.replicas, p.isr
        );
    }
}

fn print_topic_offset_positions(tpl: &TopicPartitionsAssignment) {
    info!("Topic partition list:");
    for entry in &tpl.entries {
        info!(
            "  Topic: {}, Partition: {}, Offset: {:?}",
            entry.topic, entry.partition, entry.offset
        );
    }
}

fn create_storage(command: &StoreKafkaCommand) -> Result<impl StorageBackend> {
    match &command.store_to_storage {
        StoreKafkaToStorageBackend::Directory(path) => {
            let cfg = DirectoryStorageConfig {
                base_dir: PathBuf::from(path),
                format: command.format.clone(),
                ..Default::default()
            };
            Ok(DirectoryStorage::new(cfg))
        }
    }
}

// ---------------------------------------------------------------------------
// Pump task — bounded mpsc producer/writer pipeline with idle-timeout shutdown
// (ADR-005).
// ---------------------------------------------------------------------------

struct PumpTask<S, D>
where
    S: CoreKafkaConsumer,
    D: StorageBackend,
{
    consumer: Arc<S>,
    storage: Arc<D>,
    channel_capacity: usize,
    filter: MessageFilter,
    limits: MessageLimits,
}

impl<S, D> PumpTask<S, D>
where
    S: CoreKafkaConsumer + 'static,
    D: StorageBackend + 'static,
{
    /// Run the pump task, consuming self to ensure Arc references are dropped
    /// after completion.
    pub async fn run(self) -> Result<u64> {
        let (tx, mut rx) = mpsc::channel::<KafkaMessage>(self.channel_capacity);
        let consumer = self.consumer;
        let storage = self.storage;
        let filter = self.filter;
        let limits = self.limits;
        info!("Starting PumpTask");

        // Producer task: read from Kafka and send to channel
        let producer_handle = tokio::spawn(async move {
            let mut produced: u64 = 0;
            let idle_shutdown_after = Duration::from_millis(500);
            let mut last_message_at = Instant::now();
            loop {
                match consumer.recv_next(200).await {
                    Ok(Some(msg)) => {
                        if filter.matches(&msg) {
                            if limits.should_stop_before(&msg) {
                                info!("Limit reached (before processing), stopping consumer");
                                break;
                            }

                            if tx.send(msg.clone()).await.is_err() {
                                break;
                            }
                            produced += 1;

                            if limits.should_stop_after(&msg, produced) {
                                info!("Limit reached (after processing), stopping consumer");
                                break;
                            }
                        }
                        last_message_at = Instant::now();
                    }
                    Ok(None) => {
                        if last_message_at.elapsed() >= idle_shutdown_after {
                            info!(
                                "No new messages for {}ms, stopping consumer",
                                idle_shutdown_after.as_millis()
                            );
                            break;
                        }
                        continue;
                    }
                    Err(e) => {
                        error!("Error receiving from Kafka: {}", e);
                        break;
                    }
                }
            }
            produced
        });

        // Writer task: write to storage
        let writer_handle = tokio::spawn(async move {
            let mut consumed: u64 = 0;
            while let Some(msg) = rx.recv().await {
                if let Err(e) = storage.store_message(msg).await {
                    error!("Error storing message: {}", e);
                } else {
                    consumed += 1;
                }
            }
            info!("Finished storing messages: {consumed}");
            let _ = storage.flush().await;
            consumed
        });

        let produced = producer_handle
            .await
            .map_err(|e| anyhow::anyhow!("Producer task join error: {}", e))?;
        let consumed = writer_handle
            .await
            .map_err(|e| anyhow::anyhow!("Writer task join error: {}", e))?;

        info!(
            "PumpTask finished. produced={}, consumed={}",
            produced, consumed
        );
        Ok(consumed)
    }
}

fn create_task<S, D>(
    consumer: Arc<S>,
    storage: Arc<D>,
    command: &StoreKafkaCommand,
) -> Result<PumpTask<S, D>>
where
    S: CoreKafkaConsumer,
    D: StorageBackend,
{
    let channel_capacity = 100;
    let filter = MessageFilter::new(command)?;
    let limits = MessageLimits::new(command);

    Ok(PumpTask {
        consumer,
        storage,
        channel_capacity,
        filter,
        limits,
    })
}

// ---------------------------------------------------------------------------
// Filtering & limits (pure domain)
// ---------------------------------------------------------------------------

#[derive(Clone, Copy)]
struct MessageLimits {
    count: Option<u64>,
    until_offset: Option<u64>,
    until_timestamp: Option<i64>,
}

impl MessageLimits {
    fn new(command: &StoreKafkaCommand) -> Self {
        Self {
            count: command.limit_count,
            until_offset: command.limit_until_offset,
            until_timestamp: command.limit_until_timestamp,
        }
    }

    fn should_stop_before(&self, message: &KafkaMessage) -> bool {
        if let Some(timestamp_limit) = self.until_timestamp {
            if let Some(msg_ts) = message.timestamp {
                if msg_ts >= timestamp_limit {
                    return true;
                }
            }
        }
        if let Some(offset_limit) = self.until_offset {
            if message.offset >= offset_limit as i64 {
                return true;
            }
        }
        false
    }

    fn should_stop_after(&self, _message: &KafkaMessage, produced_count: u64) -> bool {
        if let Some(count_limit) = self.count {
            if produced_count >= count_limit {
                return true;
            }
        }
        false
    }
}

struct MessageFilter {
    key_regex: Option<Regex>,
    headers: Option<HashMap<String, String>>,
    partitions: Option<Vec<i32>>,
}

impl MessageFilter {
    fn new(command: &StoreKafkaCommand) -> Result<Self> {
        let key_regex = if let Some(pattern) = &command.key_regex {
            Some(
                Regex::new(pattern)
                    .map_err(|e| anyhow::anyhow!("Invalid key regex: '{}': {}", pattern, e))?,
            )
        } else {
            None
        };

        Ok(Self {
            key_regex,
            headers: command.headers.clone(),
            partitions: command.partitions.clone(),
        })
    }

    fn matches(&self, message: &KafkaMessage) -> bool {
        if let Some(partitions) = &self.partitions {
            if !partitions.contains(&message.partition) {
                return false;
            }
        }

        if let Some(regex) = &self.key_regex {
            if let Some(key) = &message.key {
                if let Ok(key_str) = std::str::from_utf8(key) {
                    if !regex.is_match(key_str) {
                        return false;
                    }
                } else {
                    return false;
                }
            } else {
                return false;
            }
        }

        if let Some(filter_headers) = &self.headers {
            let msg_headers = &message.headers;
            for (k, v) in filter_headers {
                if let Some(msg_val) = msg_headers.get(k) {
                    if msg_val != v {
                        return false;
                    }
                } else {
                    return false;
                }
            }
        }

        true
    }
}
