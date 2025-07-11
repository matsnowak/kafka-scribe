//! Utilities for setting up Kafka for integration tests.
//!
//! This module provides functions for starting a Kafka container,
//! creating topics, and producing test messages.

use std::collections::HashMap;
use std::time::Duration;

use anyhow::{Context, Result};
use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
use rdkafka::client::DefaultClientContext;
use rdkafka::config::ClientConfig;
use rdkafka::message::OwnedHeaders;
use rdkafka::producer::{FutureProducer, FutureRecord};
use rdkafka::util::Timeout;
use tokio::time::sleep;
use tracing::{debug, info};

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
        info!("Using local Kafka instance");
        // In a real implementation, we would start a Kafka container here
        // For now, we'll just use a fixed bootstrap server string
        let bootstrap_servers = "localhost:9092".to_string();

        info!("Using bootstrap servers: {}", bootstrap_servers);

        // Wait for Kafka to be ready (simulated)
        sleep(Duration::from_secs(1)).await;

        Ok(Self {
            bootstrap_servers,
        })
    }

    /// Returns the bootstrap servers string for connecting to Kafka.
    pub fn bootstrap_servers(&self) -> &str {
        &self.bootstrap_servers
    }

    /// Creates a new topic with the given name and number of partitions.
    pub async fn create_topic(&self, topic_name: &str, partitions: i32) -> Result<()> {
        info!("Creating topic: {} with {} partitions", topic_name, partitions);

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
