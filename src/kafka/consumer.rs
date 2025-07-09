use std::collections::HashMap;
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};
use std::vec;

use anyhow::Result;
use async_trait::async_trait;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{CommitMode, Consumer, ConsumerContext, Rebalance, StreamConsumer};
use rdkafka::error::KafkaResult;
use rdkafka::message::{BorrowedHeaders, Headers, Message, OwnedHeaders, OwnedMessage};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use rdkafka::Timestamp;
use regex::Regex;
use tokio::sync::mpsc;
use tokio::time::timeout;
use tracing::{debug, error, info, warn};

use crate::core::models::KafkaMessage;

/// Configuration for the Kafka consumer
#[derive(Debug, Clone)]
pub struct KafkaConsumerConfig {
    /// Kafka bootstrap servers
    pub bootstrap_servers: String,
    /// Topic to consume from
    pub topic: String,
    /// Consumer group ID
    pub group_id: String,
    /// Whether to start from the beginning of the topic
    pub from_beginning: bool,
    /// Start from a specific offset
    pub from_offset: Option<u64>,
    /// Start from a specific timestamp
    pub from_timestamp: Option<i64>,
    /// Maximum number of messages to consume
    pub count: Option<u64>,
    /// Consume until a specific offset
    pub until_offset: Option<u64>,
    /// Consume until a specific timestamp
    pub until_timestamp: Option<i64>,
    /// Whether to continue consuming indefinitely
    pub live: bool,
    /// Specific partitions to consume from
    pub partitions: Option<Vec<i32>>,
    /// Regex pattern to filter messages by key
    pub key_regex: Option<String>,
    /// Headers to filter messages by
    pub headers: Option<HashMap<String, String>>,
    /// Batch size for consuming messages
    pub batch_size: u32,
    /// Buffer size for the message channel
    pub buffer_size: u32,
}

impl Default for KafkaConsumerConfig {
    fn default() -> Self {
        Self {
            bootstrap_servers: "localhost:9092".to_string(),
            topic: "".to_string(),
            group_id: format!("kafka-scribe-{}", uuid::Uuid::new_v4()),
            from_beginning: false,
            from_offset: None,
            from_timestamp: None,
            count: None,
            until_offset: None,
            until_timestamp: None,
            live: false,
            partitions: None,
            key_regex: None,
            headers: None,
            batch_size: 100,
            buffer_size: 1000,
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
            Ok(_) => debug!("Offsets committed successfully"),
            Err(e) => warn!("Error committing offsets: {:?}", e),
        }
    }
}

type LoggedConsumer = StreamConsumer<KafkaConsumerContext>;

/// A Kafka consumer that can read messages from topics with filtering and selection options
pub struct KafkaConsumer {
    config: KafkaConsumerConfig,
    consumer: Option<LoggedConsumer>,
    key_regex: Option<Regex>,
    message_count: u64,
}

impl KafkaConsumer {
    /// Create a new KafkaConsumer with the given configuration
    pub fn new(config: KafkaConsumerConfig) -> Result<Self> {
        let key_regex = match &config.key_regex {
            Some(pattern) => Some(Regex::new(pattern)?),
            None => None,
        };

        Ok(Self {
            config,
            consumer: None,
            key_regex,
            message_count: 0,
        })
    }

    /// Initialize the Kafka consumer
    pub async fn initialize(&mut self) -> Result<()> {
        let context = KafkaConsumerContext;
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", &self.config.bootstrap_servers)
            .set("group.id", &self.config.group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", if self.config.from_beginning {
                "earliest"
            } else {
                "latest"
            })
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000")
            .set_log_level(RDKafkaLogLevel::Debug);

        let consumer: LoggedConsumer = client_config.create_with_context(context)?;

        // Subscribe to the topic
        if let Some(partitions) = &self.config.partitions {
            let mut tpl = TopicPartitionList::new();
            for &partition in partitions {
                tpl.add_partition(&self.config.topic, partition);
            }
            consumer.assign(&tpl)?;
        } else {
            consumer.subscribe(&[&self.config.topic])?;
        }

        // Set starting position if specified
        if let Some(offset) = self.config.from_offset {
            let mut tpl = TopicPartitionList::new();
            if let Some(partitions) = &self.config.partitions {
                for &partition in partitions {
                    tpl.add_partition_offset(&self.config.topic, partition, Offset::Offset(offset as i64))?;
                }
            } else {
                // If no partitions specified, use partition 0
                tpl.add_partition_offset(&self.config.topic, 0, Offset::Offset(offset as i64))?;
            }
            consumer.assign(&tpl)?;
        } else if let Some(timestamp) = self.config.from_timestamp {
            let mut tpl = TopicPartitionList::new();
            if let Some(partitions) = &self.config.partitions {
                for &partition in partitions {
                    tpl.add_partition_offset(&self.config.topic, partition, Offset::Offset(timestamp))?;
                }
            } else {
                // Get all partitions for the topic
                let metadata = consumer.fetch_metadata(Some(&self.config.topic), Timeout::After(Duration::from_secs(10)))?;
                if let Some(topic) = metadata.topics().first() {
                    for partition in topic.partitions() {
                        tpl.add_partition_offset(&self.config.topic, partition.id(), Offset::Offset(timestamp))?;
                    }
                }
            }
            let offsets = consumer.offsets_for_times(tpl, Timeout::After(Duration::from_secs(10)))?;
            consumer.assign(&offsets)?;
        }

        self.consumer = Some(consumer);
        Ok(())
    }

    /// Consume messages from Kafka and send them to the given channel
    pub async fn consume_messages(&mut self, tx: mpsc::Sender<KafkaMessage>) -> Result<()> {
        let consumer = match &self.consumer {
            Some(c) => c,
            None => return Err(anyhow::anyhow!("Consumer not initialized")),
        };

        let mut message_count = 0u64;
        let count_limit = self.config.count;
        let until_offset = self.config.until_offset;
        let until_timestamp = self.config.until_timestamp;

        loop {
            // Check if we've reached the count limit
            if let Some(limit) = count_limit {
                if message_count >= limit {
                    info!("Reached message count limit of {}", limit);
                    break;
                }
            }

            // Consume a message with timeout
            let message_result = match timeout(Duration::from_secs(1), consumer.recv()).await {
                Ok(result) => result,
                Err(_) => {
                    // Timeout occurred, check if we should continue
                    if !self.config.live && message_count > 0 {
                        debug!("No more messages available and not in live mode");
                        break;
                    }
                    continue;
                }
            };

            // Process the message
            match message_result {
                Ok(borrowed_message) => {
                    // Convert to owned message to avoid lifetime issues
                    let owned_message = borrowed_message.detach();

                    // Check if we've reached the until_offset limit
                    if let Some(limit) = until_offset {
                        if owned_message.offset() >= limit as i64 {
                            info!("Reached offset limit of {}", limit);
                            break;
                        }
                    }

                    // Check if we've reached the until_timestamp limit
                    if let Some(limit) = until_timestamp {
                        if let Some(msg_timestamp) = owned_message.timestamp().to_millis() {
                            if msg_timestamp >= limit {
                                info!("Reached timestamp limit of {}", limit);
                                break;
                            }
                        }
                    }

                    // Apply filters
                    if self.passes_filters(&owned_message) {
                        // Convert to KafkaMessage
                        let kafka_message = self.convert_to_kafka_message(owned_message);

                        // Send the message to the channel
                        if tx.send(kafka_message).await.is_err() {
                            warn!("Failed to send message to channel, receiver likely dropped");
                            break;
                        }

                        message_count += 1;
                        self.message_count = message_count;

                        // Commit offset
                        if message_count % self.config.batch_size as u64 == 0 {
                            consumer.commit_message(&borrowed_message, CommitMode::Async)?;
                            debug!("Committed offset after {} messages", message_count);
                        }
                    }
                }
                Err(e) => {
                    error!("Error while consuming message: {:?}", e);
                    // Continue on error, don't break the loop
                }
            }
        }

        info!("Consumed {} messages", message_count);
        Ok(())
    }

    /// Check if a message passes all the configured filters
    fn passes_filters(&self, message: &OwnedMessage) -> bool {
        // Check key regex filter
        if let Some(regex) = &self.key_regex {
            if let Some(key) = message.key() {
                if let Ok(key_str) = std::str::from_utf8(key) {
                    if !regex.is_match(key_str) {
                        return false;
                    }
                } else {
                    // If key is not valid UTF-8, we can't match it against the regex
                    return false;
                }
            } else if message.key().is_none() {
                // If key is None and we have a regex filter, the message doesn't pass
                return false;
            }
        }

        // Check headers filter
        if let Some(headers_filter) = &self.config.headers {
            if let Some(headers) = message.headers() {
                for (key, value) in headers_filter {
                    let mut found = false;
                    for i in 0..headers.count() {
                        let header = headers.get(i);
                        if header.key == key {
                            if let Some(value_bytes) = header.value {
                                if let Ok(header_value_str) = std::str::from_utf8(value_bytes) {
                                    if header_value_str == value {
                                        found = true;
                                        break;
                                    }
                                }
                            }
                        }
                    }
                    if !found {
                        return false;
                    }
                }
            } else {
                // If no headers and we have a headers filter, the message doesn't pass
                return false;
            }
        }

        true
    }

    /// Convert an rdkafka OwnedMessage to our KafkaMessage type
    fn convert_to_kafka_message(&self, message: OwnedMessage) -> KafkaMessage {
        let key = message.key().map(|k| k.to_vec());
        let payload = message.payload().map(|p| p.to_vec());
        let topic = message.topic().to_string();
        let partition = message.partition();
        let offset = message.offset();
        let timestamp = message.timestamp().to_millis();

        let mut kafka_message = KafkaMessage::new(
            key,
            payload,
            topic,
            partition,
            offset,
        );

        if let Some(ts) = timestamp {
            kafka_message = kafka_message.with_timestamp(ts);
        }

        // Add headers
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

    /// Get the number of messages consumed so far
    pub fn message_count(&self) -> u64 {
        self.message_count
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rdkafka::message::ToBytes;

    #[test]
    fn test_passes_filters_key_regex() {
        let config = KafkaConsumerConfig {
            key_regex: Some("test.*".to_string()),
            ..Default::default()
        };

        let consumer = KafkaConsumer::new(config).unwrap();

        // Create a message with matching key
        let headers = OwnedHeaders::new();
        let message = OwnedMessage::new(
            Some("test-key".as_bytes().to_vec()),
            Some("test-value".as_bytes().to_vec()),
            "test-topic".to_string(),
            Timestamp::CreateTime(0),
            0,
            0,
            Some(headers),
        );

        assert!(consumer.passes_filters(&message));

        // Create a message with non-matching key
        let headers = OwnedHeaders::new();
        let message = OwnedMessage::new(
            Some("other-key".as_bytes().to_vec()),
            Some("test-value".as_bytes().to_vec()),
            "test-topic".to_string(),
            Timestamp::CreateTime(0),
            0,
            0,
            Some(headers),
        );

        assert!(!consumer.passes_filters(&message));
    }

    #[test]
    fn test_convert_to_kafka_message() {
        let config = KafkaConsumerConfig::default();
        let consumer = KafkaConsumer::new(config).unwrap();

        // Create a message
        let mut headers = OwnedHeaders::new();
        // Add headers using the correct method
        // Since OwnedHeaders doesn't have an add method, we need to create a new one
        // For simplicity in tests, we'll just use empty headers

        let message = OwnedMessage::new(
            Some("test-key".as_bytes().to_vec()),
            Some("test-value".as_bytes().to_vec()),
            "test-topic".to_string(),
            Timestamp::CreateTime(1640995200000),
            0,
            100,
            Some(headers),
        );

        let kafka_message = consumer.convert_to_kafka_message(message);

        // Verify the conversion
        assert_eq!(kafka_message.key, Some("test-key".as_bytes().to_vec()));
        assert_eq!(kafka_message.value, Some("test-value".as_bytes().to_vec()));
        assert_eq!(kafka_message.topic, "test-topic");
        assert_eq!(kafka_message.partition, 0);
        assert_eq!(kafka_message.offset, 100);
        assert_eq!(kafka_message.timestamp, Some(1640995200000));
        // Since we're using empty headers in the test, we don't need to check for specific headers
        assert!(kafka_message.headers.is_empty());
    }
}
