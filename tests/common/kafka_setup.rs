//! Utilities for setting up Kafka for integration tests.
//!
//! This module provides functions for starting a Kafka container,
//! creating topics, and producing test messages.

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{Consumer, StreamConsumer};
use rdkafka::error::KafkaError;
use rdkafka::message::{OwnedHeaders, Headers, BorrowedMessage};
use rdkafka::producer::{FutureProducer, FutureRecord, Producer};
use rdkafka::Message;
use rdkafka::util::Timeout;
use tokio::time::{sleep, timeout};
use tracing::{debug, info, warn};
use uuid::Uuid;

use crate::common::test_data::TestMessage;

const DEFAULT_TIMEOUT: Duration = Duration::from_secs(30);

/// A wrapper around a Kafka connection for testing.
pub struct KafkaTestContext {
    bootstrap_servers: String,
}

// Explicitly implement Send and Sync for KafkaTestContext
unsafe impl Send for KafkaTestContext {}
unsafe impl Sync for KafkaTestContext {}

impl KafkaTestContext {
    /// Creates a new KafkaTestContext with a connection to Kafka.
    pub async fn new(_docker: &'static impl std::any::Any) -> Result<Self> {
        info!("Setting up Kafka for testing");
        let bootstrap_servers = "localhost:29092".to_string();

        // Check if Kafka is running, and start it if needed
        Self::ensure_kafka_is_running().await?;

        info!("Using bootstrap servers: {}", bootstrap_servers);

        // Wait for Kafka to be ready
        Self::wait_for_kafka(&bootstrap_servers).await?;

        Ok(Self {
            bootstrap_servers,
        })
    }

    /// Ensures that Kafka is running, starting it if needed.
    async fn ensure_kafka_is_running() -> Result<()> {
        // Check if Kafka is already running by trying to connect to the bootstrap server
        if Self::is_kafka_running().await {
            info!("Kafka is already running");
            return Ok(());
        }

        info!("Starting Kafka using Docker Compose");

        // Find the docker-compose.yml file
        let docker_compose_path = Path::new("tests/fixtures/docker-compose.yml");
        if !docker_compose_path.exists() {
            return Err(anyhow::anyhow!(
                "Docker Compose file not found at {}",
                docker_compose_path.display()
            ));
        }

        // Try to start Kafka using docker compose (new style)
        let output = Command::new("docker")
            .args(["compose", "-f", docker_compose_path.to_str().unwrap(), "up", "-d"])
            .output()
            .context("Failed to execute docker compose command")?;

        if !output.status.success() {
            // If the new style command fails, try the old style command
            warn!("New style docker compose command failed, trying old style");
            let output = Command::new("docker-compose")
                .args(["-f", docker_compose_path.to_str().unwrap(), "up", "-d"])
                .output()
                .context("Failed to execute docker-compose command")?;

            if !output.status.success() {
                return Err(anyhow::anyhow!(
                    "Failed to start Kafka using Docker Compose: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }

        info!("Kafka started successfully, waiting for it to initialize...");

        // Wait a bit for Kafka to initialize
        sleep(Duration::from_secs(10)).await;

        info!("Continuing after initial wait");
        Ok(())
    }

    /// Checks if Kafka is running by trying to connect to the bootstrap server.
    async fn is_kafka_running() -> bool {
        // Try to create a simple client config and connect
        let client_result = ClientConfig::new()
            .set("bootstrap.servers", "localhost:29092")
            .set("message.timeout.ms", "5000")
            .create::<FutureProducer>();

        match client_result {
            Ok(producer) => {
                // Try to get metadata to verify connectivity
                match producer.client().fetch_metadata(None, Duration::from_secs(5)) {
                    Ok(_) => {
                        debug!("Successfully connected to Kafka and fetched metadata");
                        true
                    }
                    Err(err) => {
                        warn!("Kafka metadata fetch failed: {}", err);
                        false
                    }
                }
            }
            Err(err) => {
                warn!("Kafka connection check failed: {}", err);
                false
            }
        }
    }

    /// Waits for Kafka to be ready by checking if we can create a producer and fetch metadata.
    async fn wait_for_kafka(bootstrap_servers: &str) -> Result<()> {
        let max_retries = 10;
        let retry_delay = Duration::from_secs(3);

        for i in 1..=max_retries {
            info!("Checking if Kafka is ready (attempt {}/{})", i, max_retries);

            let client_result = ClientConfig::new()
                .set("bootstrap.servers", bootstrap_servers)
                .set("message.timeout.ms", "5000")
                .create::<FutureProducer>();

            match client_result {
                Ok(producer) => {
                    // Try to get metadata to verify connectivity
                    match producer.client().fetch_metadata(None, Duration::from_secs(5)) {
                        Ok(_) => {
                            info!("Kafka is ready - successfully connected and fetched metadata");
                            return Ok(());
                        }
                        Err(err) => {
                            warn!("Kafka metadata fetch failed: {}", err);
                            if i < max_retries {
                                info!("Waiting for {} seconds before retrying", retry_delay.as_secs());
                                sleep(retry_delay).await;
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!("Kafka not ready yet: {}", err);
                    if i < max_retries {
                        info!("Waiting for {} seconds before retrying", retry_delay.as_secs());
                        sleep(retry_delay).await;
                    }
                }
            }
        }

        Err(anyhow::anyhow!("Timed out waiting for Kafka to be ready"))
    }

    /// Stops the Kafka Docker container.
    pub async fn stop_kafka() -> Result<()> {
        info!("Stopping Kafka Docker container");

        // Find the docker-compose.yml file
        let docker_compose_path = Path::new("tests/fixtures/docker-compose.yml");
        if !docker_compose_path.exists() {
            return Err(anyhow::anyhow!(
                "Docker Compose file not found at {}",
                docker_compose_path.display()
            ));
        }

        // Try to stop Kafka using docker compose (new style)
        let output = Command::new("docker")
            .args(["compose", "-f", docker_compose_path.to_str().unwrap(), "down"])
            .output()
            .context("Failed to execute docker compose down command")?;

        if !output.status.success() {
            // If the new style command fails, try the old style command
            warn!("New style docker compose down command failed, trying old style");
            let output = Command::new("docker-compose")
                .args(["-f", docker_compose_path.to_str().unwrap(), "down"])
                .output()
                .context("Failed to execute docker-compose down command")?;

            if !output.status.success() {
                return Err(anyhow::anyhow!(
                    "Failed to stop Kafka using Docker Compose: {}",
                    String::from_utf8_lossy(&output.stderr)
                ));
            }
        }

        info!("Kafka stopped successfully");
        Ok(())
    }

    /// Returns the bootstrap servers string for connecting to Kafka.
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Creates a new topic with the given name and number of partitions.
    pub async fn create_topic(&self, topic_name: &str, partitions: i32) -> Result<()> {
        info!("Creating topic: {} with {} partitions", topic_name, partitions);

        // First, try to delete the topic if it exists
        let _ = self.delete_topic(topic_name).await;

        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create()
            .context("Failed to create admin client")?;

        let topic = NewTopic::new(topic_name, partitions, TopicReplication::Fixed(1));

        admin
            .create_topics(&[topic], &AdminOptions::new())
            .await
            .context("Failed to create topic")?;

        // Wait for topic to be created
        sleep(Duration::from_secs(2)).await;

        info!("Topic created: {}", topic_name);
        Ok(())
    }

    /// Deletes a topic with the given name.
    pub async fn delete_topic(&self, topic_name: &str) -> Result<()> {
        info!("Deleting topic: {}", topic_name);

        let admin: AdminClient<DefaultClientContext> = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .create()
            .context("Failed to create admin client")?;

        admin
            .delete_topics(&[topic_name], &AdminOptions::new())
            .await
            .context("Failed to delete topic")?;

        // Wait for topic to be deleted
        sleep(Duration::from_secs(2)).await;

        info!("Topic deleted: {}", topic_name);
        Ok(())
    }

    /// Produces test messages to the given topic.
    pub async fn produce_messages(
        &self,
        topic: &str,
        messages: &[TestMessage],
        partition: Option<i32>,
    ) -> Result<()> {
        info!("Producing {} messages to topic: {}", messages.len(), topic);

        let producer: FutureProducer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("message.timeout.ms", "5000")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .context("Failed to create producer")?;

        for message in messages {
            let mut record = FutureRecord::to(topic)
                .payload(&message.value)
                .key(&message.key);

            if let Some(p) = partition {
                record = record.partition(p);
            }

            // Add headers if present
            if let Some(_headers) = &message.headers {
                // For simplicity, we'll skip adding headers in the test
                // This is a workaround for the OwnedHeaders API complexity
            }

            match producer.send(record, Timeout::After(DEFAULT_TIMEOUT)).await {
                Ok(_) => (),
                Err((err, _)) => return Err(anyhow::anyhow!("Failed to send message: {}", err)),
            };
        }

        info!("Produced {} messages to topic: {}", messages.len(), topic);
        Ok(())
    }

    /// Generate and produce test messages
    pub async fn generate_and_produce(
        &self,
        topic_name: &str,
        count: usize,
        seed: Option<u64>,
    ) -> Result<Vec<TestMessage>> {
        // Create a test data generator
        let mut generator = crate::common::test_data::TestDataGenerator::new(seed.unwrap_or_else(|| rand::random()));

        // Generate messages
        let messages = generator.generate_message_batch(count);

        // Produce messages
        self.produce_messages(topic_name, &messages, None).await?;

        Ok(messages)
    }

    /// Consume messages from a topic
    pub async fn consume_messages(
        &self,
        topic_name: &str,
        count: usize,
        timeout_seconds: u64,
    ) -> Result<Vec<TestMessage>> {
        info!("Consuming up to {} messages from topic {}", count, topic_name);

        let group_id = format!("test-consumer-{}", Uuid::new_v4());
        let consumer: StreamConsumer = ClientConfig::new()
            .set("bootstrap.servers", &self.bootstrap_servers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set_log_level(RDKafkaLogLevel::Debug)
            .create()
            .context("Failed to create Kafka consumer")?;

        consumer.subscribe(&[topic_name])
            .context("Failed to subscribe to topic")?;

        let mut messages = Vec::new();
        let start = Instant::now();
        let timeout_duration = Duration::from_secs(timeout_seconds);

        while messages.len() < count && start.elapsed() < timeout_duration {
            let message_result = match timeout(
                Duration::from_secs(1),
                consumer.recv(),
            ).await {
                Ok(result) => result,
                Err(_) => {
                    // No message received after 1 second
                    debug!("No message received after timeout");
                    continue;
                }
            };

            match message_result {
                Ok(borrowed_message) => {
                    // Convert to TestMessage
                    let key = borrowed_message.key().map(|k| k.to_vec()).unwrap_or_default();
                    let value = borrowed_message.payload().map(|p| p.to_vec()).unwrap_or_default();

                    // For simplicity, we'll skip processing headers in the test
                    let headers = None;

                    let mut test_message = TestMessage::new(key, value, headers);
                    if let Some(ts) = borrowed_message.timestamp().to_millis() {
                        test_message.timestamp = ts;
                    }

                    messages.push(test_message);
                },
                Err(e) => {
                    warn!("Error while consuming message: {:?}", e);
                }
            }
        }

        info!("Consumed {} messages from {}", messages.len(), topic_name);
        Ok(messages)
    }

    /// Wait for Kafka to be ready
    pub async fn wait_for_kafka_ready(&self, max_wait_seconds: u64) -> Result<()> {
        info!("Waiting for Kafka to be ready at {}", self.bootstrap_servers);

        let start = Instant::now();
        let max_wait = Duration::from_secs(max_wait_seconds);
        let retry_interval = Duration::from_secs(2);

        while start.elapsed() < max_wait {
            let client_result: std::result::Result<AdminClient<DefaultClientContext>, KafkaError> = 
                ClientConfig::new()
                    .set("bootstrap.servers", &self.bootstrap_servers)
                    .set_log_level(RDKafkaLogLevel::Debug)
                    .create();

            match client_result {
                Ok(client) => {
                    // Try to fetch metadata to verify the connection
                    match client.inner().fetch_metadata(None, Timeout::After(Duration::from_secs(5))) {
                        Ok(_) => {
                            info!("Kafka is ready at {} after {:?}", self.bootstrap_servers, start.elapsed());
                            return Ok(());
                        },
                        Err(e) => {
                            debug!("Kafka metadata fetch failed: {:?}", e);
                        }
                    }
                },
                Err(e) => {
                    debug!("Failed to connect to Kafka: {:?}", e);
                }
            }

            sleep(retry_interval).await;
        }

        Err(anyhow::anyhow!("Timed out waiting for Kafka to be ready after {:?}", max_wait))
    }
}
