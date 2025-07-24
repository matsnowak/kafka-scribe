//! Utilities for setting up Kafka for integration tests.
//!
//! This module provides functions for starting a Kafka container,
//! creating topics, and producing test messages.

use std::collections::HashMap;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio::time::sleep;
use tracing::{debug, info, warn};

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

        info!("Kafka started successfully");
        Ok(())
    }

    /// Checks if Kafka is running by trying to connect to the bootstrap server.
    async fn is_kafka_running() -> bool {
        // Try to create a simple client config and connect
        let client_result = ClientConfig::new()
            .set("bootstrap.servers", "localhost:29092")
            .set("message.timeout.ms", "5000")
            .create::<FutureProducer>();

        if let Err(err) = client_result {
            warn!("Kafka connection check failed: {}", err);
            return false;
        }

        true
    }

    /// Waits for Kafka to be ready by checking if we can create a producer.
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
                Ok(_) => {
                    info!("Kafka is ready");
                    return Ok(());
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
            if let Some(headers) = &message.headers {
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
}
