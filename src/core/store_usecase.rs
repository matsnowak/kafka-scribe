use std::collections::HashMap;
use std::ops::Deref;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH, Instant};
use std::vec;

use anyhow::Result;
use async_trait::async_trait;
use clap::Command;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedHeaders, BorrowedMessage, Headers, Message, OwnedHeaders, OwnedMessage};
use rdkafka::metadata::{Metadata, MetadataTopic};
use rdkafka::topic_partition_list::{self, Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use rdkafka::Timestamp;
use regex::Regex;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::core::models::KafkaMessage;
use crate::storage::files::DirectoryStorage;
use crate::storage::files::DirectoryStorageConfig;
use crate::storage::{self, StorageBackend};

#[derive(Debug)]
pub struct StoreKafkaCommand {
    pub(crate) bootstrap_servers: String,
    pub(crate) topic: String,
    pub(crate) group_id: String,
    pub(crate) from: StoreKafkaFrom,
    pub(crate) to: StoreKafkaTo,
    pub(crate) store_to_storage: StoreKafkaToStorageBackend,
}

#[derive(Debug)]
pub enum StoreKafkaFrom {
    FromBegining,
    FromEnd,
}

#[derive(Debug)]
pub enum StoreKafkaToStorageBackend {
    Directory(String),
}

#[derive(Debug)]
pub enum StoreKafkaTo {
    Live,
}

#[async_trait]
pub trait StoreUsecase {
    async fn execute(&self, command: StoreKafkaCommand) -> anyhow::Result<()>;
}

struct StoreUsecaseImpl {}

impl StoreUsecaseImpl {
    pub(crate) fn create_consumer(
        &self,
        command: &StoreKafkaCommand,
    ) -> Result<impl CoreKafkaConsumer> {
        Ok(RdKafkaConsumer::new(
            command.bootstrap_servers.clone(),
            command.topic.clone(),
            command.group_id.clone(),
        )?)
    }
}

// TODO: could be dependency
fn create_consumer(command: &StoreKafkaCommand) -> Result<impl CoreKafkaConsumer> {
    Ok(RdKafkaConsumer::new(
        command.bootstrap_servers.clone(),
        command.topic.clone(),
        command.group_id.clone(),
    )?)
}

type CommandExecutionResult = ();
pub async fn store(command: StoreKafkaCommand) -> Result<CommandExecutionResult> {
    debug!("store command: {:?}", command);
    let consumer = create_consumer(&command)?;
    consumer.initialize().await?;

    let topic_metadata = consumer.fetch_metadata().await?;
    print_metadata(&topic_metadata);

    let topic_assigment = parse_topic_assignment(&command, &topic_metadata);
    // let topic_assigment = parse_topic_assigment(&StoreKafkaCommand, &topic_metadata)?;
    consumer.assign(topic_assigment).await?;

    let topic_offset_positions = consumer.fetch_offset_positions().await?;
    print_topic_offset_positions(&topic_offset_positions.inner);

    let consumer = Arc::new(consumer);

    let storage = Arc::new(create_storage(&command)?);
    storage.initialize().await.map_err(|e| anyhow::anyhow!("Storage init error: {}", e))?;
    let pump_task = create_task(consumer.clone(), storage.clone());
    let processed = pump_task.run().await?;
    info!("Stored {} messages", processed);
    storage.close().await.map_err(|e| anyhow::anyhow!("Storage close error: {}", e))?;

    info!("Finished storing messages");
    Ok(())
}

struct PumpTask<S, D>
where
    S: CoreKafkaConsumer,
    D: StorageBackend,
{
    consumer: Arc<S>,
    storage: Arc<D>,
    channel_capacity: usize,
}

impl<S, D> PumpTask<S, D>
where
    S: CoreKafkaConsumer + 'static,
    D: StorageBackend + 'static,
{
    pub fn new(consumer: Arc<S>, storage: Arc<D>, channel_capacity: usize) -> Self {
        Self {
            consumer,
            storage,
            channel_capacity,
        }
    }

    pub async fn run(&self) -> Result<u64> {
        let (tx, mut rx) = mpsc::channel::<KafkaMessage>(self.channel_capacity);
        let consumer = self.consumer.clone();
        let storage = self.storage.clone();
        info!("Starting PumpTask");

        // Producer task: read from Kafka and send to channel
        let producer_handle = tokio::spawn(async move {
            let mut produced: u64 = 0;
            let idle_shutdown_after = Duration::from_secs(5);
            let mut last_message_at = Instant::now();
            loop {
                match consumer.recv_next(500).await {
                    Ok(Some(msg)) => {
                        if tx.send(msg).await.is_err() {
                            // Receiver dropped, stop producing
                            break;
                        }
                        produced += 1;
                        last_message_at = Instant::now();
                    }
                    Ok(None) => {
                        // No message in this poll. If we've been idle long enough, stop.
                        if last_message_at.elapsed() >= idle_shutdown_after {
                            info!("No new messages for {:?}, stopping consumer", idle_shutdown_after);
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

        // Consumer task: write to storage
        let writer_handle = tokio::spawn(async move {
            let mut consumed: u64 = 0;
            while let Some(msg) = rx.recv().await {
                if let Err(e) = storage.store_message(msg).await {
                    error!("Error storing message: {}", e);
                    // Decide whether to stop on error; for now continue
                } else {
                    consumed += 1;
                }
            }
            info!("Finished storing messages: {consumed}");
            // Flush at the end
            let _ = storage.flush().await;
            consumed
        });

        let produced = producer_handle.await.map_err(|e| anyhow::anyhow!("Producer task join error: {}", e))?;
        // tx is dropped when producer task ends; channel closes and writer finishes
        let consumed = writer_handle.await.map_err(|e| anyhow::anyhow!("Writer task join error: {}", e))?;

        info!("PumpTask finished. produced={}, consumed={}", produced, consumed);
        Ok(consumed)
    }
}
fn create_task<S, D>(consumer: Arc<S>, storage: Arc<D>) -> PumpTask<S, D>
where
    S: CoreKafkaConsumer,
    D: StorageBackend,
{
    let channel_capacity = 100;
    let pump_task = PumpTask {
        consumer: consumer.clone(),
        storage: storage.clone(),
        channel_capacity,
    };

    pump_task
}

use std::path::PathBuf;

fn create_storage(command: &StoreKafkaCommand) -> Result<impl StorageBackend> {
    match &command.store_to_storage {
        StoreKafkaToStorageBackend::Directory(path) => {
            let mut cfg = DirectoryStorageConfig::default();
            cfg.base_dir = PathBuf::from(path);
            let storage = DirectoryStorage::new(cfg);
            Ok(storage)
        }
    }
}

pub fn parse_topic_assignment(
    store_kafka_command: &StoreKafkaCommand,
    metadata: &TopicMetadata,
) -> TopicPartitionsAssignment {
    match store_kafka_command.from {
        StoreKafkaFrom::FromBegining => {
            let mut topic_partition_list = TopicPartitionList::new();
            if let Some(topic_meta) = metadata.topic_metadata() {
                for partition in topic_meta.partitions() {
                    topic_partition_list.add_partition_offset(
                        &store_kafka_command.topic,
                        partition.id(),
                        Offset::Beginning,
                    );
                }
            }
            TopicPartitionsAssignment::from_rdkafka(topic_partition_list)
        }
        StoreKafkaFrom::FromEnd => {
            let mut topic_partition_list = TopicPartitionList::new();
            if let Some(topic_meta) = metadata.topic_metadata() {
                for partition in topic_meta.partitions() {
                    topic_partition_list.add_partition_offset(
                        &store_kafka_command.topic,
                        partition.id(),
                        Offset::End,
                    );
                }
            }
            TopicPartitionsAssignment::from_rdkafka(topic_partition_list)
        }
    }
}

fn print_topic_offset_positions(topic_offset_positions: &TopicPartitionList) {
    info!("Topic partition list:");
    for partition in topic_offset_positions.elements() {
        info!(
            "  Topic: {}, Partition: {}, Offset: {:?}",
            partition.topic(),
            partition.partition(),
            partition.offset()
        );
    }
}

fn print_metadata(metadata: &TopicMetadata) {
    info!("Topic metadata:");
    if let Some(topic) = metadata.topic_metadata() {
        info!("  Topic: {}", topic.name());
        for partition in topic.partitions() {
            info!(
                "    Partition {}: leader: {}, replicas: {:?}, isr: {:?}",
                partition.id(),
                partition.leader(),
                partition.replicas(),
                partition.isr()
            );
        }
    } else {
        info!("  Topic not found in metadata: {}", metadata.topic);
    }
}

// impl StoreUsecase for StoreUsecaseImpl {
//     async fn execute(&self, command: StoreKafkaCommand) -> anyhow::Result<()> {
//         let consumer = self.create_consumer(&command);
//         let topic_metadata = consumer.fetch_metadata()?;
//
//         let kafka_start_offsets: = parse_start_offsets(&command.from)?;
//         let kafka_end_offsets = parse_end_offsets(&command.to)?;
//
//         info!("kafka_start_offsets: {:?}", kafka_start_offsets);
//         info!("kafka_end_offsets: {:?}", kafka_end_offsets);
//
//         // TODO: extract to dependency
//         let consumer = self.create_consumer(&command);
//         consumer.initialize().await?;
//
//
//         Ok(())
//     }
// }

// kafka-consumer
// BLOCK begin
#[async_trait]
pub trait CoreKafkaConsumer: Send + Sync {
    async fn initialize(&self) -> Result<()>;
    async fn fetch_metadata(&self) -> Result<TopicMetadata>;
    async fn fetch_offset_positions(&self) -> Result<TopicPartitionsAssignment>;
    async fn assign(&self, tpl: TopicPartitionsAssignment) -> Result<()>;
    /// Receive next KafkaMessage within the given timeout (milliseconds).
    /// Returns Ok(None) on timeout without a message.
    async fn recv_next(&self, timeout_ms: u64) -> Result<Option<KafkaMessage>>;
}

struct TopicMetadata {
    inner: Metadata,
    topic: String,
}
impl TopicMetadata {
    pub fn from_rdkafka(metadata: Metadata, topic: String) -> TopicMetadata {
        TopicMetadata {
            inner: metadata,
            topic,
        }
    }

    fn topic_metadata(&self) -> Option<&MetadataTopic> {
        self.inner
            .topics()
            .iter()
            .find(|t| t.name() == self.topic)
            .or_else(|| self.inner.topics().first())
    }

    /// Create an owned snapshot of the topic metadata, detached from the
    /// underlying rdkafka::metadata::Metadata lifetime.
    /// Returns None if the topic is not present in the metadata.
    pub fn owned(&self) -> Option<OwnedTopicMetadata> {
        self.topic_metadata().map(|t| {
            let partitions = t
                .partitions()
                .iter()
                .map(|p| OwnedPartitionMetadata {
                    id: p.id(),
                    leader: p.leader(),
                    replicas: p.replicas().to_vec(),
                    isr: p.isr().to_vec(),
                })
                .collect();
            OwnedTopicMetadata {
                name: t.name().to_string(),
                partitions,
            }
        })
    }
}

#[derive(Debug, Clone)]
struct OwnedTopicMetadata {
    name: String,
    partitions: Vec<OwnedPartitionMetadata>,
}

#[derive(Debug, Clone)]
struct OwnedPartitionMetadata {
    id: i32,
    leader: i32,
    replicas: Vec<i32>,
    isr: Vec<i32>,
}

struct TopicPartitionsAssignment {
    inner: TopicPartitionList,
}

impl TopicPartitionsAssignment {
    pub fn from_rdkafka(tpl: TopicPartitionList) -> TopicPartitionsAssignment {
        TopicPartitionsAssignment { inner: tpl }
    }
}

fn convert_to_kafka_message(message: &OwnedMessage) -> KafkaMessage {
    let key = message.key().map(|k| k.to_vec());
    let payload = message.payload().map(|p| p.to_vec());
    let topic = message.topic().to_string();
    let partition = message.partition();
    let offset = message.offset();
    let timestamp = message.timestamp().to_millis();

    let mut kafka_message = KafkaMessage::new(key, payload, topic, partition, offset);

    if let Some(ts) = timestamp {
        kafka_message = kafka_message.with_timestamp(ts);
    }

    if let Some(headers) = message.headers() {
        for i in 0..headers.count() {
            let header = headers.get(i);
            if let Some(value_bytes) = header.value {
                if let Ok(value_str) = std::str::from_utf8(value_bytes) {
                    kafka_message = kafka_message.with_header(header.key, value_str);
                }
            }
        }
    }

    kafka_message
}

struct RdKafkaConsumer {
    bootstrap_servers: String,
    topic: String,
    group_id: String,
    inner_consumer: LoggedConsumer,
}

impl RdKafkaConsumer {
    pub fn new(bootstrap_servers: String, topic: String, group_id: String) -> Result<Self> {
        let context = KafkaConsumerContext;
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest") // Always start from earliest by default
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000")
            .set_log_level(RDKafkaLogLevel::Debug);

        let consumer: LoggedConsumer = client_config.create_with_context(context)?;
        Ok(Self {
            bootstrap_servers,
            topic,
            group_id,
            inner_consumer: consumer,
        })
    }
}

#[async_trait]
impl CoreKafkaConsumer for RdKafkaConsumer {
    // TODO: decide what to do with this
    async fn initialize(&self) -> Result<()> {
        let metadata = self
            .inner_consumer
            .fetch_metadata(Some(&self.topic), Timeout::After(Duration::from_secs(10)))?;

        if metadata.topics().is_empty() || metadata.topics().first().unwrap().name() != self.topic {
            return Err(anyhow::anyhow!("Topic '{}' not found", self.topic));
        }
        Ok(())
    }

    async fn fetch_metadata(&self) -> Result<TopicMetadata> {
        let metadata = self.inner_consumer.fetch_metadata(
            Some(&self.topic),
            // TODO: extract to config
            Timeout::After(Duration::from_secs(10)),
        )?;
        if let Some(first_topic_metadata) = metadata.topics().first() {
            if first_topic_metadata.name() != self.topic {
                return Err(anyhow::anyhow!("Topic '{}' not found", self.topic));
            }
            if first_topic_metadata.partitions().is_empty() {
                return Err(anyhow::anyhow!("Topic '{}' has no partitions", self.topic));
            }
            Ok(TopicMetadata::from_rdkafka(metadata, self.topic.clone()))
        } else {
            Err(anyhow::anyhow!("Topic '{}' not found", self.topic))
        }
    }

    async fn fetch_offset_positions(&self) -> Result<TopicPartitionsAssignment> {
        self.inner_consumer
            .assignment()
            .map(TopicPartitionsAssignment::from_rdkafka)
            .map_err(|e| anyhow::anyhow!("Error fetching offset positions: {}", e))
    }

    async fn assign(&self, tpl: TopicPartitionsAssignment) -> Result<()> {
        self.inner_consumer
            .assign(&tpl.inner)
            .map_err(|e| anyhow::anyhow!("Error assigning partitions: {}", e))
    }

    async fn recv_next(&self, timeout_ms: u64) -> Result<Option<KafkaMessage>> {
        // Use StreamConsumer::recv with timeout
        match tokio::time::timeout(
            Duration::from_millis(timeout_ms),
            self.inner_consumer.recv(),
        )
        .await
        {
            Err(_elapsed) => Ok(None),
            Ok(Err(e)) => Err(anyhow::anyhow!("Kafka receive error: {}", e)),
            Ok(Ok(bmsg)) => {
                // Convert BorrowedMessage to OwnedMessage to detach from librdkafka lifetime
                let owned: OwnedMessage = bmsg.detach();
                let msg = convert_to_kafka_message(&owned);
                Ok(Some(msg))
            }
        }
    }
}

/// Custom context for the Kafka consumer
struct KafkaConsumerContext;

impl ClientContext for KafkaConsumerContext {}

impl ConsumerContext for KafkaConsumerContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        debug!("Pre-rebalance: {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        debug!("Post-rebalance: {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            std::prelude::rust_2015::Ok(_) => debug!("Offsets committed successfully"),
            Err(e) => warn!("Error committing offsets: {:?}", e),
        }
    }
}

type LoggedConsumer = StreamConsumer<KafkaConsumerContext>;
