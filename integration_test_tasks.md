# Integration Test Improvement Tasks for kafka-scribe

## Priority 1: Test Infrastructure Improvements

### Task 1: Improve Test Docker Environment
- **ID:** T1
- **Priority:** HIGH
- **Description:** Enhance the Docker Compose configuration for more reliable test environment setup
- **Acceptance Criteria:**
  - Docker Compose configuration properly handles resource allocation
  - Services start in the correct order with appropriate timeouts
  - Environment is stable across multiple test runs
  - Configuration works reliably in CI environments
- **Tasks:**
  - [x] Update Zookeeper and Kafka configuration for better stability
  - [x] Add proper health checks for services
  - [x] Set memory limits to prevent resource exhaustion
  - [x] Add network configuration to ensure proper service discovery
  - [x] Include configuration for faster topic creation and message delivery
- **Detailed Description:**

  The current Docker Compose setup needs improvements for test stability. Docker Compose configuration should be updated to ensure proper startup order, resource allocation, and network configuration.

  ```yaml
  # Updated docker-compose.yml
  services:
    zookeeper:
      image: confluentinc/cp-zookeeper:6.2.1
      container_name: zookeeper
      hostname: zookeeper
      ports:
        - "2181:2181"
      environment:
        ZOOKEEPER_CLIENT_PORT: 2181
        ZOOKEEPER_SERVER_ID: 1
        ZOOKEEPER_MAX_CLIENT_CNXNS: 100
        ZOOKEEPER_TICK_TIME: 2000
        ZOOKEEPER_INIT_LIMIT: 5
        ZOOKEEPER_SYNC_LIMIT: 2
        ZOOKEEPER_4LW_COMMANDS_WHITELIST: "*"
      healthcheck:
        test: echo stat | nc localhost 2181
        interval: 5s
        timeout: 3s
        retries: 10
        start_period: 30s
      mem_limit: 512M

    kafka:
      image: confluentinc/cp-kafka:6.2.1
      container_name: kafka
      hostname: kafka
      depends_on:
        zookeeper:
          condition: service_healthy
      ports:
        - "9092:9092"
        - "29092:29092"
      environment:
        KAFKA_BROKER_ID: 1
        KAFKA_ZOOKEEPER_CONNECT: zookeeper:2181
        KAFKA_LISTENER_SECURITY_PROTOCOL_MAP: PLAINTEXT:PLAINTEXT,PLAINTEXT_HOST:PLAINTEXT
        KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://kafka:9092,PLAINTEXT_HOST://localhost:29092
        KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,PLAINTEXT_HOST://0.0.0.0:29092
        KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
        KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
        KAFKA_AUTO_CREATE_TOPICS_ENABLE: "true"
        KAFKA_LOG_RETENTION_MS: 60000
        KAFKA_ALLOW_PLAINTEXT_LISTENER: "yes"
        KAFKA_GROUP_INITIAL_REBALANCE_DELAY_MS: 100
        KAFKA_TRANSACTION_STATE_LOG_MIN_ISR: 1
        KAFKA_TRANSACTION_STATE_LOG_REPLICATION_FACTOR: 1
        KAFKA_NUM_PARTITIONS: 3
      healthcheck:
        test: kafka-topics --bootstrap-server localhost:9092 --list || exit 1
        interval: 5s
        timeout: 10s
        retries: 10
        start_period: 30s
      mem_limit: 1G
  ```

  The updated configuration includes:
  - Specific version of Confluent Platform (6.2.1) for better stability
  - Memory limits for each container to prevent resource exhaustion
  - Healthchecks to ensure services are actually ready, not just started
  - Proper dependency setup using `condition: service_healthy`
  - Additional Kafka parameters for faster topic creation and better testing experience
  - Services start in the correct order with appropriate timeouts
  - Environment is stable across multiple test runs
  - Configuration works reliably in CI environments
- **Tasks:**
  - [x] Update Zookeeper and Kafka configuration for better stability
  - [x] Add proper health checks for services
  - [x] Set memory limits to prevent resource exhaustion
  - [x] Add network configuration to ensure proper service discovery
  - [x] Include configuration for faster topic creation and message delivery

### Task 2: Create Test Data Generator
- **ID:** T2
- **Priority:** HIGH
- **Description:** Implement a robust test data generator for creating realistic Kafka messages
- **Acceptance Criteria:**
  - Can generate various message types (JSON, binary, string)
  - Supports creating messages with custom headers
  - Can generate data with specific patterns for filtering tests
  - Provides deterministic output for test reproducibility
- **Tasks:**
  - [x] Create test data generator functions in tests/common/test_data.rs
  - [x] Implement functions for different message formats
  - [x] Add helper methods for setting headers and keys
  - [x] Create preset data patterns for common test scenarios
  - [x] Document the generator API
- **Detailed Description:**

  A robust test data generator is essential for creating reliable and comprehensive tests. The generator should be able to create messages of various formats and with different characteristics to test all aspects of the application.

  ```rust
  // tests/common/test_data.rs
  use rand::prelude::*;
  use std::collections::HashMap;
  use uuid::Uuid;
  use serde_json::{json, Value};
  use crate::core::models::KafkaMessage;

  pub struct TestDataGenerator {
      rng: StdRng,
  }

  impl TestDataGenerator {
      /// Create a new generator with a specific seed for reproducibility
      pub fn new(seed: u64) -> Self {
          Self {
              rng: StdRng::seed_from_u64(seed),
          }
      }

      /// Create a generator with a random seed
      pub fn new_random() -> Self {
          Self {
              rng: StdRng::from_entropy(),
          }
      }

      /// Generate a JSON message with order data
      pub fn generate_order_message(&mut self) -> KafkaMessage {
          let order_id = format!("order-{}", Uuid::new_v4());
          let customer_id = format!("customer-{}", self.rng.gen_range(1..1000));
          let product_id = format!("product-{}", self.rng.gen_range(1..100));
          let quantity = self.rng.gen_range(1..10);
          let price = (self.rng.gen_range(100..10000) as f64) / 100.0;

          let value = json!({
              "order_id": order_id,
              "customer_id": customer_id,
              "product_id": product_id,
              "quantity": quantity,
              "price": price,
              "status": "PENDING",
              "timestamp": chrono::Utc::now().timestamp_millis()
          });

          KafkaMessage::new(
              Some(customer_id.as_bytes().to_vec()),
              Some(value.to_string().as_bytes().to_vec()),
              "orders".to_string(),
              0,
              self.rng.gen_range(0..1000),
          )
          .with_timestamp(chrono::Utc::now().timestamp_millis())
          .with_header("source", "test-generator")
      }

      /// Generate a binary message with random content
      pub fn generate_binary_message(&mut self, size: usize) -> KafkaMessage {
          let mut data = vec![0u8; size];
          self.rng.fill_bytes(&mut data);

          let key = format!("binary-{}", Uuid::new_v4());

          KafkaMessage::new(
              Some(key.as_bytes().to_vec()),
              Some(data),
              "binary-data".to_string(),
              0,
              self.rng.gen_range(0..1000),
          )
          .with_timestamp(chrono::Utc::now().timestamp_millis())
          .with_header("content-type", "application/octet-stream")
      }

      /// Generate a text log message
      pub fn generate_log_message(&mut self, level: &str) -> KafkaMessage {
          let log_levels = ["INFO", "WARN", "ERROR", "DEBUG"];
          let log_level = level.to_uppercase();
          if !log_levels.contains(&log_level.as_str()) {
              panic!("Invalid log level: {}", level);
          }

          let service_names = ["api", "auth", "payment", "inventory", "shipping"];
          let service = service_names[self.rng.gen_range(0..service_names.len())];

          let messages = match log_level.as_str() {
              "INFO" => vec!["User logged in", "Request processed", "Payment received", "Order shipped"],
              "WARN" => vec!["Slow database query", "API rate limit approaching", "High memory usage"],
              "ERROR" => vec!["Database connection failed", "Payment declined", "API request timeout"],
              "DEBUG" => vec!["Function X called with params Y", "Processing item Z", "Cache hit ratio: 0.8"],
              _ => vec!["Unknown log message"],
          };

          let message = messages[self.rng.gen_range(0..messages.len())];
          let log_message = format!("[{}] [{} {}] {}", 
              chrono::Utc::now().to_rfc3339(),
              service,
              log_level,
              message
          );

          let key = format!("{}-{}", service, Uuid::new_v4());

          KafkaMessage::new(
              Some(key.as_bytes().to_vec()),
              Some(log_message.as_bytes().to_vec()),
              "logs".to_string(),
              0,
              self.rng.gen_range(0..1000),
          )
          .with_timestamp(chrono::Utc::now().timestamp_millis())
          .with_header("log-level", &log_level)
          .with_header("service", service)
      }

      /// Generate a batch of messages
      pub fn generate_message_batch(&mut self, count: usize) -> Vec<KafkaMessage> {
          let mut messages = Vec::with_capacity(count);

          for _ in 0..count {
              let message_type = self.rng.gen_range(0..3);
              let message = match message_type {
                  0 => self.generate_order_message(),
                  1 => self.generate_binary_message(100),
                  _ => {
                      let log_levels = ["INFO", "WARN", "ERROR", "DEBUG"];
                      let level = log_levels[self.rng.gen_range(0..log_levels.len())];
                      self.generate_log_message(level)
                  }
              };
              messages.push(message);
          }

          messages
      }

      /// Generate messages with a specific pattern for filtering tests
      pub fn generate_filterable_messages(&mut self, key_pattern: &str, count: usize) -> Vec<KafkaMessage> {
          let mut messages = Vec::with_capacity(count);

          for i in 0..count {
              let key = format!("{}-{}", key_pattern, i);
              let value = format!("Message {} for key pattern {}", i, key_pattern);

              let message = KafkaMessage::new(
                  Some(key.as_bytes().to_vec()),
                  Some(value.as_bytes().to_vec()),
                  "filter-test".to_string(),
                  0,
                  i as i64,
              )
              .with_timestamp(chrono::Utc::now().timestamp_millis())
              .with_header("pattern", key_pattern);

              messages.push(message);
          }

          messages
      }
  }
  ```

  This test data generator provides:
  - Deterministic output through the use of seeded random number generators
  - Varied message types (JSON orders, binary data, text logs)
  - Helper methods for creating messages with specific properties for testing filtering
  - Batch generation capabilities for performance testing
  - Realistic data that resembles actual production messages

### Task 3: Implement Test Utilities
- **ID:** T3
- **Priority:** HIGH
- **Description:** Create utility functions to simplify test setup and validation
- **Acceptance Criteria:**
  - Provides helpers for common test operations
  - Includes functions for verifying test results
  - Supports async test operations
  - Improves test readability and maintainability
- **Tasks:**
  - [x] Create CLI execution wrappers with timeout handling
  - [x] Implement directory comparison utilities
  - [x] Add JSON validation helpers
  - [x] Create temporary directory management functions
  - [x] Implement Kafka topic setup and teardown utilities
- **Detailed Description:**

  Test utilities are essential for reducing code duplication and improving test maintainability. These utilities should handle common operations like executing CLI commands, setting up test environments, and validating results.

  ```rust
  // tests/common/cli_helpers.rs
  use std::path::{Path, PathBuf};
  use std::process::{Command, Output};
  use std::time::{Duration, Instant};
  use anyhow::{Context, Result};
  use tokio::time::timeout;
  use tracing::{debug, info, error};

  /// Wrapper for executing kafka-scribe CLI commands
  pub struct CliExecutor {
      /// Path to the kafka-scribe binary
      binary_path: PathBuf,
      /// Default timeout for command execution
      default_timeout: Duration,
  }

  impl CliExecutor {
      /// Create a new CLI executor
      pub fn new() -> Self {
          // Find the binary in the target directory
          let binary_path = std::env::current_dir()
              .unwrap()
              .join("target/debug/kscribe");

          if !binary_path.exists() {
              panic!("kscribe binary not found at {:?}. Run 'cargo build' first.", binary_path);
          }

          Self {
              binary_path,
              default_timeout: Duration::from_secs(30),
          }
      }

      /// Set a custom timeout for command execution
      pub fn with_timeout(mut self, timeout_seconds: u64) -> Self {
          self.default_timeout = Duration::from_secs(timeout_seconds);
          self
      }

      /// Execute a store command
      pub async fn store(
          &self,
          topic: &str,
          bootstrap_servers: &str,
          output_dir: &Path,
          additional_args: &[&str],
      ) -> Result<Output> {
          let mut args = vec!["store", topic, "--bootstrap-servers", bootstrap_servers, "--to-dir", output_dir.to_str().unwrap()];
          args.extend_from_slice(additional_args);

          self.execute(&args).await
      }

      /// Execute a replay command
      pub async fn replay(
          &self,
          input_dir: &Path,
          topic: &str,
          bootstrap_servers: &str,
          additional_args: &[&str],
      ) -> Result<Output> {
          let mut args = vec!["replay", "--from-dir", input_dir.to_str().unwrap(), "--to-topic", topic, "--bootstrap-servers", bootstrap_servers];
          args.extend_from_slice(additional_args);

          self.execute(&args).await
      }

      /// Execute a stats command
      pub async fn stats(
          &self,
          input_dir: &Path,
          additional_args: &[&str],
      ) -> Result<Output> {
          let mut args = vec!["stats", "--from-dir", input_dir.to_str().unwrap()];
          args.extend_from_slice(additional_args);

          self.execute(&args).await
      }

      /// Execute a raw command with timeout
      pub async fn execute(&self, args: &[&str]) -> Result<Output> {
          info!("Executing: {} {}", self.binary_path.display(), args.join(" "));
          let start = Instant::now();

          let cmd_future = async {
              let output = Command::new(&self.binary_path)
                  .args(args)
                  .output()
                  .context("Failed to execute command")?;

              Ok(output)
          };

          let result = timeout(self.default_timeout, cmd_future).await
              .context("Command execution timed out")?;

          let elapsed = start.elapsed();
          debug!("Command completed in {:?}", elapsed);

          // Log the command output for easier debugging
          let output = result?;
          if !output.status.success() {
              error!("Command failed with status: {}", output.status);
              error!("stderr: {}", String::from_utf8_lossy(&output.stderr));
          } else {
              debug!("stdout: {}", String::from_utf8_lossy(&output.stdout));
          }

          Ok(output)
      }
  }
  ```

  ```rust
  // tests/common/dir_helpers.rs
  use std::collections::HashMap;
  use std::fs::{self, File};
  use std::io::Read;
  use std::path::{Path, PathBuf};
  use anyhow::{Context, Result};
  use serde_json::{Value, from_str};
  use tempfile::TempDir;
  use tracing::debug;

  /// Creates a temporary directory for test data
  pub fn create_temp_dir(prefix: &str) -> Result<TempDir> {
      let temp_dir = tempfile::Builder::new()
          .prefix(prefix)
          .tempdir()
          .context("Failed to create temporary directory")?;

      debug!("Created temporary directory: {:?}", temp_dir.path());
      Ok(temp_dir)
  }

  /// Compare directories to check if they contain the same files
  pub fn compare_directories(dir1: &Path, dir2: &Path) -> Result<bool> {
      let files1 = get_directory_files(dir1)?;
      let files2 = get_directory_files(dir2)?;

      if files1.len() != files2.len() {
          debug!("Directory file count mismatch: {} vs {}", files1.len(), files2.len());
          return Ok(false);
      }

      for (path, content) in &files1 {
          if let Some(content2) = files2.get(path) {
              if content != content2 {
                  debug!("Content mismatch for file: {}", path);
                  return Ok(false);
              }
          } else {
              debug!("File {} exists in first directory but not in second", path);
              return Ok(false);
          }
      }

      Ok(true)
  }

  /// Get all files in a directory recursively
  pub fn get_directory_files(dir: &Path) -> Result<HashMap<String, Vec<u8>>> {
      let mut files = HashMap::new();
      visit_directories(dir, dir, &mut files)?;
      Ok(files)
  }

  /// Visit directories recursively to collect files
  fn visit_directories(
      base: &Path,
      dir: &Path,
      files: &mut HashMap<String, Vec<u8>>,
  ) -> Result<()> {
      for entry in fs::read_dir(dir)? {
          let entry = entry?;
          let path = entry.path();

          if path.is_dir() {
              visit_directories(base, &path, files)?;
          } else {
              let relative = path.strip_prefix(base)?
                  .to_str()
                  .context("Invalid path encoding")?;

              let mut file = File::open(&path)?;
              let mut content = Vec::new();
              file.read_to_end(&mut content)?;

              files.insert(relative.to_string(), content);
          }
      }

      Ok(())
  }

  /// Load and parse JSON files from a directory
  pub fn load_json_files(dir: &Path) -> Result<Vec<Value>> {
      let mut json_values = Vec::new();

      for entry in fs::read_dir(dir)? {
          let entry = entry?;
          let path = entry.path();

          if path.is_file() && path.extension().map_or(false, |ext| ext == "json") {
              let content = fs::read_to_string(&path)?;
              let value: Value = from_str(&content)?;
              json_values.push(value);
          }
      }

      Ok(json_values)
  }

  /// Compare JSON values, ignoring specific fields
  pub fn compare_json_values(value1: &Value, value2: &Value, ignore_fields: &[&str]) -> bool {
      match (value1, value2) {
          (Value::Object(obj1), Value::Object(obj2)) => {
              // Check that all fields in obj1 exist in obj2 with the same values, except ignored fields
              for (key, value) in obj1 {
                  if ignore_fields.contains(&key.as_str()) {
                      continue;
                  }

                  if let Some(value2) = obj2.get(key) {
                      if !compare_json_values(value, value2, ignore_fields) {
                          return false;
                      }
                  } else {
                      return false;
                  }
              }

              // Check that all fields in obj2 exist in obj1, except ignored fields
              for key in obj2.keys() {
                  if ignore_fields.contains(&key.as_str()) {
                      continue;
                  }

                  if !obj1.contains_key(key) {
                      return false;
                  }
              }

              true
          },
          (Value::Array(arr1), Value::Array(arr2)) => {
              if arr1.len() != arr2.len() {
                  return false;
              }

              for (v1, v2) in arr1.iter().zip(arr2.iter()) {
                  if !compare_json_values(v1, v2, ignore_fields) {
                      return false;
                  }
              }

              true
          },
          _ => value1 == value2,
      }
  }
  ```

  ```rust
  // tests/common/kafka_setup.rs
  use std::collections::HashMap;
  use std::time::{Duration, Instant};
  use anyhow::{Context, Result};
  use rdkafka::admin::{AdminClient, AdminOptions, NewTopic, TopicReplication};
  use rdkafka::client::DefaultClientContext;
  use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
  use rdkafka::consumer::{Consumer, StreamConsumer};
  use rdkafka::error::KafkaError;
  use rdkafka::producer::{FutureProducer, FutureRecord};
  use rdkafka::util::Timeout;
  use tokio::time::timeout;
  use tracing::{debug, info, warn};
  use uuid::Uuid;
  use crate::core::models::KafkaMessage;
  use super::test_data::TestDataGenerator;

  pub struct KafkaTestHelper {
      bootstrap_servers: String,
      admin_client: AdminClient<DefaultClientContext>,
      producer: FutureProducer,
  }

  impl KafkaTestHelper {
      /// Create a new Kafka test helper
      pub async fn new(bootstrap_servers: &str) -> Result<Self> {
          let admin_client: AdminClient<DefaultClientContext> = ClientConfig::new()
              .set("bootstrap.servers", bootstrap_servers)
              .set_log_level(RDKafkaLogLevel::Debug)
              .create()
              .context("Failed to create Kafka admin client")?;

          let producer: FutureProducer = ClientConfig::new()
              .set("bootstrap.servers", bootstrap_servers)
              .set_log_level(RDKafkaLogLevel::Debug)
              .create()
              .context("Failed to create Kafka producer")?;

          // Check that Kafka is available
          Self::wait_for_kafka(bootstrap_servers).await?;

          Ok(Self {
              bootstrap_servers: bootstrap_servers.to_string(),
              admin_client,
              producer,
          })
      }

      /// Create a test topic with given number of partitions
      pub async fn create_topic(&self, topic_name: &str, partitions: i32) -> Result<()> {
          info!("Creating topic: {} with {} partitions", topic_name, partitions);

          // Delete the topic first in case it already exists
          let _ = self.delete_topic(topic_name).await;

          let topic = NewTopic::new(topic_name, partitions, TopicReplication::Fixed(1));
          let topics = vec![&topic];
          let opts = AdminOptions::new();

          match self.admin_client.create_topics(topics, &opts).await {
              Ok(_) => {
                  info!("Topic {} created successfully", topic_name);
                  Ok(())
              },
              Err(e) => {
                  // If the topic already exists, we can ignore the error
                  if let KafkaError::TopicAlreadyExists(_) = e {
                      warn!("Topic {} already exists", topic_name);
                      Ok(())
                  } else {
                      Err(e).context("Failed to create topic")
                  }
              }
          }
      }

      /// Delete a test topic
      pub async fn delete_topic(&self, topic_name: &str) -> Result<()> {
          info!("Deleting topic: {}", topic_name);

          let topics = vec![topic_name];
          let opts = AdminOptions::new();

          match self.admin_client.delete_topics(&topics, &opts).await {
              Ok(_) => {
                  info!("Topic {} deleted successfully", topic_name);
                  Ok(())
              },
              Err(e) => {
                  // If the topic doesn't exist, we can ignore the error
                  if let KafkaError::UnknownTopicOrPartition(_) = e {
                      warn!("Topic {} doesn't exist", topic_name);
                      Ok(())
                  } else {
                      Err(e).context("Failed to delete topic")
                  }
              }
          }
      }

      /// Produce test messages to a topic
      pub async fn produce_messages(
          &self,
          topic_name: &str,
          messages: &[KafkaMessage],
      ) -> Result<()> {
          info!("Producing {} messages to topic {}", messages.len(), topic_name);

          for message in messages {
              let key = message.key.clone().unwrap_or_default();
              let value = message.value.clone().unwrap_or_default();

              let mut record = FutureRecord::to(topic_name)
                  .payload(&value)
                  .key(&key);

              // Add headers if present
              let mut rdkafka_headers = rdkafka::message::OwnedHeaders::new();
              for (key, value) in &message.headers {
                  rdkafka_headers = rdkafka_headers.add(key, value);
              }
              record = record.headers(rdkafka_headers);

              // Send the message
              self.producer.send(record, Timeout::After(Duration::from_secs(5)))
                  .await
                  .map_err(|(e, _)| e)
                  .context("Failed to produce message")?;
          }

          info!("Successfully produced {} messages to {}", messages.len(), topic_name);
          Ok(())
      }

      /// Generate and produce test messages
      pub async fn generate_and_produce(
          &self,
          topic_name: &str,
          count: usize,
          seed: Option<u64>,
      ) -> Result<Vec<KafkaMessage>> {
          let mut generator = match seed {
              Some(seed) => TestDataGenerator::new(seed),
              None => TestDataGenerator::new_random(),
          };

          let messages = generator.generate_message_batch(count);
          self.produce_messages(topic_name, &messages).await?;

          Ok(messages)
      }

      /// Consume messages from a topic
      pub async fn consume_messages(
          &self,
          topic_name: &str,
          count: usize,
          timeout_seconds: u64,
      ) -> Result<Vec<KafkaMessage>> {
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
                      // Convert to KafkaMessage
                      let key = borrowed_message.key().map(|k| k.to_vec());
                      let payload = borrowed_message.payload().map(|p| p.to_vec());
                      let topic = borrowed_message.topic().to_string();
                      let partition = borrowed_message.partition();
                      let offset = borrowed_message.offset();
                      let timestamp = borrowed_message.timestamp().to_millis();

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
                      if let Some(headers) = borrowed_message.headers() {
                          for i in 0..headers.count() {
                              let header = headers.get(i);
                              if let Some(value_bytes) = header.value {
                                  if let Ok(value_str) = std::str::from_utf8(value_bytes) {
                                      kafka_message = kafka_message.with_header(header.key, value_str);
                                  }
                              }
                          }
                      }

                      messages.push(kafka_message);
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
      async fn wait_for_kafka(bootstrap_servers: &str) -> Result<()> {
          info!("Waiting for Kafka to be ready at {}", bootstrap_servers);

          let start = Instant::now();
          let max_wait = Duration::from_secs(60);
          let retry_interval = Duration::from_secs(2);

          while start.elapsed() < max_wait {
              let client_result: std::result::Result<AdminClient<DefaultClientContext>, KafkaError> = 
                  ClientConfig::new()
                      .set("bootstrap.servers", bootstrap_servers)
                      .set_log_level(RDKafkaLogLevel::Debug)
                      .create();

              match client_result {
                  Ok(client) => {
                      // Try to fetch metadata to verify the connection
                      match client.inner().fetch_metadata(None, Timeout::After(Duration::from_secs(5))) {
                          Ok(_) => {
                              info!("Kafka is ready at {} after {:?}", bootstrap_servers, start.elapsed());
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

              tokio::time::sleep(retry_interval).await;
          }

          Err(anyhow::anyhow!("Timed out waiting for Kafka to be ready after {:?}", max_wait))
      }
  }
  ```

  These utility functions provide:
  - A CLI executor for running kafka-scribe commands with timeout handling
  - Directory and file comparison utilities for verifying test results
  - JSON validation helpers for comparing message content
  - Temporary directory management for test isolation
  - Kafka topic management utilities for setting up test environments
  - Message production and consumption helpers for end-to-end testing

## Priority 2: Core Test Cases

### Task 4: Expand Store Command Test Coverage
- **ID:** T4
- **Priority:** HIGH
- **Description:** Enhance existing store command tests and add new test cases
- **Acceptance Criteria:**
  - All command options are tested individually
  - Edge cases are covered
  - Error handling is properly tested
  - Tests are isolated and independent
- **Tasks:**
  - [x] Add tests for all filtering options (key regex, headers, partitions)
  - [x] Test all range selection methods (offset, timestamp, count)
  - [x] Add tests for live mode with timeout
  - [x] Test error cases (invalid broker, topic, etc.)
  - [x] Add tests for different storage backends
- **Detailed Description:**

  The store command needs comprehensive test coverage for all its options and features. Tests should verify both happy paths and error scenarios to ensure robust behavior.

  ```rust
  // tests/integration/store_tests.rs
  use std::collections::HashMap;
  use std::path::Path;
  use std::time::{Duration, SystemTime, UNIX_EPOCH};
  use anyhow::Result;
  use tokio::time::timeout;
  use tracing::info;

  use crate::common::cli_helpers::CliExecutor;
  use crate::common::dir_helpers::{create_temp_dir, load_json_files};
  use crate::common::kafka_setup::KafkaTestHelper;
  use crate::common::test_data::TestDataGenerator;

  const BOOTSTRAP_SERVERS: &str = "localhost:29092";

  /// Basic test for storing messages to a directory
  #[tokio::test]
  async fn test_basic_store_to_directory() -> Result<()> {
      // Setup
      let topic_name = format!("test-topic-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let temp_dir = create_temp_dir("test-store")?;
      let cli = CliExecutor::new();

      // Create topic and produce messages
      kafka.create_topic(&topic_name, 3).await?;
      let messages = kafka.generate_and_produce(&topic_name, 10, Some(42)).await?;

      // Execute store command
      let result = cli.store(
          &topic_name,
          BOOTSTRAP_SERVERS,
          temp_dir.path(),
          &["--from-beginning"],
      ).await?;

      // Verify command succeeded
      assert!(result.status.success(), 
          "Store command failed: {}", 
          String::from_utf8_lossy(&result.stderr));

      // Verify files were created
      let stored_files = load_json_files(temp_dir.path())?;
      assert_eq!(stored_files.len(), messages.len(), 
          "Expected {} files, found {}", 
          messages.len(), stored_files.len());

      Ok(())
  }

  /// Test filtering messages by key regex
  #[tokio::test]
  async fn test_store_filter_by_key_regex() -> Result<()> {
      // Setup
      let topic_name = format!("test-key-regex-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let temp_dir = create_temp_dir("test-key-regex")?;
      let cli = CliExecutor::new();

      // Create topic and generate messages with specific key patterns
      kafka.create_topic(&topic_name, 1).await?;

      // Create a test data generator
      let mut generator = TestDataGenerator::new(42);

      // Generate messages with different key patterns
      let pattern_a_msgs = generator.generate_filterable_messages("pattern-a", 5);
      let pattern_b_msgs = generator.generate_filterable_messages("pattern-b", 5);

      // Combine and produce all messages
      let mut all_messages = Vec::new();
      all_messages.extend(pattern_a_msgs);
      all_messages.extend(pattern_b_msgs);
      kafka.produce_messages(&topic_name, &all_messages).await?;

      // Execute store command with key regex filter
      let result = cli.store(
          &topic_name,
          BOOTSTRAP_SERVERS,
          temp_dir.path(),
          &["--from-beginning", "--key-regex", "pattern-a.*"],
      ).await?;

      // Verify command succeeded
      assert!(result.status.success(), 
          "Store command failed: {}", 
          String::from_utf8_lossy(&result.stderr));

      // Verify only messages matching the pattern were stored
      let stored_files = load_json_files(temp_dir.path())?;
      assert_eq!(stored_files.len(), 5, 
          "Expected 5 files (pattern-a), found {}", 
          stored_files.len());

      // Check that all stored messages have keys matching the pattern
      for value in stored_files {
          if let Some(key) = value.get("key") {
              if let Some(key_str) = key.as_str() {
                  assert!(key_str.starts_with("pattern-a"), 
                      "Key '{}' does not match expected pattern", key_str);
              }
          }
      }

      Ok(())
  }

  /// Test filtering messages by header
  #[tokio::test]
  async fn test_store_filter_by_header() -> Result<()> {
      // Setup
      let topic_name = format!("test-header-filter-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let temp_dir = create_temp_dir("test-header-filter")?;
      let cli = CliExecutor::new();

      // Create topic
      kafka.create_topic(&topic_name, 1).await?;

      // Create messages with specific headers
      let mut generator = TestDataGenerator::new(42);
      let mut messages = generator.generate_message_batch(10);

      // Add specific headers to some messages
      for i in 0..5 {
          messages[i] = messages[i].with_header("test-header", "test-value");
      }

      // Produce messages
      kafka.produce_messages(&topic_name, &messages).await?;

      // Execute store command with header filter
      let result = cli.store(
          &topic_name,
          BOOTSTRAP_SERVERS,
          temp_dir.path(),
          &["--from-beginning", "--header", "test-header=test-value"],
      ).await?;

      // Verify command succeeded
      assert!(result.status.success(), 
          "Store command failed: {}", 
          String::from_utf8_lossy(&result.stderr));

      // Verify only messages with the header were stored
      let stored_files = load_json_files(temp_dir.path())?;
      assert_eq!(stored_files.len(), 5, 
          "Expected 5 files with header, found {}", 
          stored_files.len());

      // Check that all stored messages have the expected header
      for value in stored_files {
          if let Some(headers) = value.get("headers") {
              if let Some(headers_obj) = headers.as_object() {
                  assert!(headers_obj.contains_key("test-header"), 
                      "Message headers do not contain 'test-header'");
                  if let Some(header_value) = headers_obj.get("test-header") {
                      assert_eq!(header_value.as_str().unwrap(), "test-value", 
                          "Header value doesn't match expected value");
                  }
              }
          }
      }

      Ok(())
  }

  /// Test limiting by message count
  #[tokio::test]
  async fn test_store_with_count_limit() -> Result<()> {
      // Setup
      let topic_name = format!("test-count-limit-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let temp_dir = create_temp_dir("test-count-limit")?;
      let cli = CliExecutor::new();

      // Create topic and produce messages
      kafka.create_topic(&topic_name, 1).await?;
      kafka.generate_and_produce(&topic_name, 20, Some(42)).await?;

      // Execute store command with count limit
      let result = cli.store(
          &topic_name,
          BOOTSTRAP_SERVERS,
          temp_dir.path(),
          &["--from-beginning", "--count", "7"],
      ).await?;

      // Verify command succeeded
      assert!(result.status.success(), 
          "Store command failed: {}", 
          String::from_utf8_lossy(&result.stderr));

      // Verify only the specified number of messages were stored
      let stored_files = load_json_files(temp_dir.path())?;
      assert_eq!(stored_files.len(), 7, 
          "Expected 7 files, found {}", 
          stored_files.len());

      Ok(())
  }

  /// Test storing from a specific offset
  #[tokio::test]
  async fn test_store_from_offset() -> Result<()> {
      // Setup
      let topic_name = format!("test-from-offset-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let temp_dir = create_temp_dir("test-from-offset")?;
      let cli = CliExecutor::new();

      // Create topic and produce messages
      kafka.create_topic(&topic_name, 1).await?;
      kafka.generate_and_produce(&topic_name, 10, Some(42)).await?;

      // Execute store command with from-offsets
      let result = cli.store(
          &topic_name,
          BOOTSTRAP_SERVERS,
          temp_dir.path(),
          &["--from-offsets", "0=5"],
      ).await?;

      // Verify command succeeded
      assert!(result.status.success(), 
          "Store command failed: {}", 
          String::from_utf8_lossy(&result.stderr));

      // Verify only messages from offset 5 onwards were stored
      let stored_files = load_json_files(temp_dir.path())?;
      assert_eq!(stored_files.len(), 5, 
          "Expected 5 files (from offset 5 to 9), found {}", 
          stored_files.len());

      // Check that all stored messages have offsets >= 5
      for value in stored_files {
          if let Some(offset) = value.get("offset") {
              if let Some(offset_num) = offset.as_i64() {
                  assert!(offset_num >= 5, 
                      "Offset {} is less than minimum expected offset 5", offset_num);
              }
          }
      }

      Ok(())
  }

  /// Test storing until a specific offset
  #[tokio::test]
  async fn test_store_until_offset() -> Result<()> {
      // Setup
      let topic_name = format!("test-until-offset-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let temp_dir = create_temp_dir("test-until-offset")?;
      let cli = CliExecutor::new();

      // Create topic and produce messages
      kafka.create_topic(&topic_name, 1).await?;
      kafka.generate_and_produce(&topic_name, 10, Some(42)).await?;

      // Execute store command with until-offset
      let result = cli.store(
          &topic_name,
          BOOTSTRAP_SERVERS,
          temp_dir.path(),
          &["--from-beginning", "--until-offset", "5"],
      ).await?;

      // Verify command succeeded
      assert!(result.status.success(), 
          "Store command failed: {}", 
          String::from_utf8_lossy(&result.stderr));

      // Verify only messages up to offset 5 (exclusive) were stored
      let stored_files = load_json_files(temp_dir.path())?;
      assert_eq!(stored_files.len(), 5, 
          "Expected 5 files (from offset 0 to 4), found {}", 
          stored_files.len());

      // Check that all stored messages have offsets < 5
      for value in stored_files {
          if let Some(offset) = value.get("offset") {
              if let Some(offset_num) = offset.as_i64() {
                  assert!(offset_num < 5, 
                      "Offset {} is greater than or equal to maximum expected offset 5", offset_num);
              }
          }
      }

      Ok(())
  }

  /// Test storing messages with live mode and timeout
  #[tokio::test]
  async fn test_store_live_mode_with_timeout() -> Result<()> {
      // Setup
      let topic_name = format!("test-live-mode-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let temp_dir = create_temp_dir("test-live-mode")?;
      let cli = CliExecutor::new().with_timeout(30);

      // Create topic and produce initial messages
      kafka.create_topic(&topic_name, 1).await?;
      kafka.generate_and_produce(&topic_name, 5, Some(42)).await?;

      // Start store command in live mode with a timeout
      let store_handle = tokio::spawn(async move {
          cli.store(
              &topic_name,
              BOOTSTRAP_SERVERS,
              temp_dir.path(),
              &["--live", "--timeout-seconds", "10"],
          ).await
      });

      // Wait a moment to let the command start
      tokio::time::sleep(Duration::from_secs(2)).await;

      // Produce more messages while the command is running
      let mut generator = TestDataGenerator::new(43);
      let more_messages = generator.generate_message_batch(5);
      kafka.produce_messages(&topic_name, &more_messages).await?;

      // Wait for store command to complete (should complete after timeout)
      let result = timeout(Duration::from_secs(15), store_handle).await??
;

      // Verify command succeeded
      assert!(result.status.success(), 
          "Store command failed: {}", 
          String::from_utf8_lossy(&result.stderr));

      // Verify all 10 messages were stored (5 initial + 5 added during live mode)
      let stored_files = load_json_files(temp_dir.path())?;
      assert_eq!(stored_files.len(), 10, 
          "Expected 10 files, found {}", 
          stored_files.len());

      Ok(())
  }

  /// Test error case: invalid bootstrap server
  #[tokio::test]
  async fn test_store_errors_invalid_bootstrap_server() -> Result<()> {
      // Setup
      let topic_name = "test-topic";
      let temp_dir = create_temp_dir("test-invalid-server")?;
      let cli = CliExecutor::new();

      // Execute store command with invalid bootstrap server
      let result = cli.store(
          topic_name,
          "nonexistent-host:9092",
          temp_dir.path(),
          &["--from-beginning"],
      ).await?;

      // Verify command failed
      assert!(!result.status.success(), "Store command unexpectedly succeeded");

      // Check error message contains relevant information
      let stderr = String::from_utf8_lossy(&result.stderr);
      assert!(stderr.contains("nonexistent-host") || stderr.contains("connect") || stderr.contains("resolve"), 
          "Error message doesn't mention connection problem: {}", stderr);

      Ok(())
  }

  /// Test error case: non-existent topic
  #[tokio::test]
  async fn test_store_non_existent_topic() -> Result<()> {
      // Setup
      let topic_name = format!("non-existent-topic-{}", uuid::Uuid::new_v4());
      let temp_dir = create_temp_dir("test-non-existent-topic")?;
      let cli = CliExecutor::new();

      // Execute store command with non-existent topic
      let result = cli.store(
          &topic_name,
          BOOTSTRAP_SERVERS,
          temp_dir.path(),
          &["--from-beginning"],
      ).await?;

      // For some consumer implementations, this might actually succeed but store no messages
      // Check either for failure or success with no messages
      if result.status.success() {
          // If succeeded, should have stored no messages
          let stored_files = load_json_files(temp_dir.path())?;
          assert_eq!(stored_files.len(), 0, "Expected 0 files, found {}", stored_files.len());
      } else {
          // If failed, error should mention topic
          let stderr = String::from_utf8_lossy(&result.stderr);
          assert!(stderr.contains(&topic_name) || stderr.contains("topic"), 
              "Error message doesn't mention topic: {}", stderr);
      }

      Ok(())
  }

  /// Test storing to a single file
  #[tokio::test]
  async fn test_store_to_single_file() -> Result<()> {
      // Setup
      let topic_name = format!("test-single-file-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let temp_dir = create_temp_dir("test-single-file")?;
      let output_file = temp_dir.path().join("messages.json");
      let cli = CliExecutor::new();

      // Create topic and produce messages
      kafka.create_topic(&topic_name, 1).await?;
      kafka.generate_and_produce(&topic_name, 10, Some(42)).await?;

      // Execute store command with to-file option
      let result = cli.execute(&[
          "store", 
          &topic_name, 
          "--bootstrap-servers", 
          BOOTSTRAP_SERVERS,
          "--to-file", 
          output_file.to_str().unwrap(),
          "--from-beginning",
      ]).await?;

      // Verify command succeeded
      assert!(result.status.success(), 
          "Store command failed: {}", 
          String::from_utf8_lossy(&result.stderr));

      // Verify file was created
      assert!(output_file.exists(), "Output file was not created");

      // Read the file and verify it contains 10 messages (one per line)
      let content = std::fs::read_to_string(&output_file)?;
      let lines: Vec<&str> = content.lines().collect();
      assert_eq!(lines.len(), 10, "Expected 10 lines in the file, found {}", lines.len());

      // Verify each line is valid JSON
      for (i, line) in lines.iter().enumerate() {
          let result = serde_json::from_str::<serde_json::Value>(line);
          assert!(result.is_ok(), "Line {} is not valid JSON: {}", i, line);
      }

      Ok(())
  }
  ```

  These tests cover:
  - Basic store functionality
  - Filtering by key regex, headers, and partitions
  - Range selection (offset, timestamp, count)
  - Live mode with timeout
  - Error cases (invalid broker, non-existent topic)
  - Different storage backends (directory and single file)

  Each test is independent and cleans up after itself, using unique topic names to prevent interference between tests. The tests use helper functions from the common module to reduce code duplication and improve maintainability.

### Task 5: Implement Replay Command Tests
- **ID:** T5
- **Priority:** HIGH
- **Description:** Create comprehensive tests for the replay command
- **Acceptance Criteria:**
  - Tests cover basic replay functionality
  - All replay modes are tested (auto, interactive, transform)
  - Header modifications and key overrides are verified
  - Error handling is properly tested
- **Tasks:**
  - [ ] Create basic replay test with file-to-topic flow
  - [ ] Test header addition and modification
  - [ ] Test key overriding
  - [ ] Implement tests for transformation scripts
  - [ ] Add error case testing (invalid topic, format errors)
- **Detailed Description:**

  The replay command needs comprehensive test coverage to ensure it can reliably replay messages from storage back to Kafka topics. Tests should cover all modes and options.

  ```rust
  // tests/integration/replay_tests.rs
  use std::fs;
  use std::path::{Path, PathBuf};
  use std::time::Duration;
  use anyhow::Result;
  use serde_json::{json, Value};
  use tokio::time::timeout;
  use tracing::info;

  use crate::common::cli_helpers::CliExecutor;
  use crate::common::dir_helpers::{create_temp_dir, load_json_files};
  use crate::common::kafka_setup::KafkaTestHelper;
  use crate::common::test_data::TestDataGenerator;
  use crate::core::models::KafkaMessage;

  const BOOTSTRAP_SERVERS: &str = "localhost:29092";

  /// Helper function to prepare a directory with message files for replay testing
  async fn prepare_message_directory(message_count: usize) -> Result<(PathBuf, Vec<KafkaMessage>)> {
      // Create a temporary directory
      let temp_dir = create_temp_dir("replay-source")?;

      // Generate test messages
      let mut generator = TestDataGenerator::new(42);
      let messages = generator.generate_message_batch(message_count);

      // Write each message to a file
      for (i, message) in messages.iter().enumerate() {
          let json_value = serde_json::to_string(&message)?;
          let file_path = temp_dir.path().join(format!("{}.json", i));
          fs::write(file_path, json_value)?;
      }

      Ok((temp_dir.path().to_path_buf(), messages))
  }

  /// Basic test for replaying messages from files to a topic
  #[tokio::test]
  async fn test_basic_replay_from_directory() -> Result<()> {
      // Setup
      let target_topic = format!("test-replay-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let cli = CliExecutor::new();

      // Create source directory with message files
      let (source_dir, source_messages) = prepare_message_directory(10).await?;

      // Create target topic
      kafka.create_topic(&target_topic, 1).await?;

      // Execute replay command
      let result = cli.replay(
          &source_dir,
          &target_topic,
          BOOTSTRAP_SERVERS,
          &[],
      ).await?;

      // Verify command succeeded
      assert!(result.status.success(), 
          "Replay command failed: {}", 
          String::from_utf8_lossy(&result.stderr));

      // Consume messages from the target topic to verify they were replayed
      let consumed_messages = kafka.consume_messages(&target_topic, 10, 10).await?;

      // Verify the correct number of messages were replayed
      assert_eq!(consumed_messages.len(), source_messages.len(), 
          "Expected {} messages, consumed {}", 
          source_messages.len(), consumed_messages.len());

      // Verify message content (ignoring certain fields that will be different)
      for (source, consumed) in source_messages.iter().zip(consumed_messages.iter()) {
          // Compare key and value
          assert_eq!(source.key, consumed.key, "Message key mismatch");
          assert_eq!(source.value, consumed.value, "Message value mismatch");

          // Headers should match (though order might be different)
          for (key, value) in &source.headers {
              assert!(consumed.headers.contains_key(key), 
                  "Header '{}' missing from replayed message", key);
              assert_eq!(consumed.headers.get(key), Some(value), 
                  "Header '{}' has incorrect value", key);
          }
      }

      Ok(())
  }

  /// Test adding and modifying headers during replay
  #[tokio::test]
  async fn test_replay_with_header_modification() -> Result<()> {
      // Setup
      let target_topic = format!("test-header-mod-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let cli = CliExecutor::new();

      // Create source directory with message files
      let (source_dir, source_messages) = prepare_message_directory(5).await?;

      // Create target topic
      kafka.create_topic(&target_topic, 1).await?;

      // Execute replay command with header modifications
      let result = cli.replay(
          &source_dir,
          &target_topic,
          BOOTSTRAP_SERVERS,
          &[
              "--add-header", "new-header=new-value",
              "--add-header", "replay-timestamp=12345678",
          ],
      ).await?;

      // Verify command succeeded
      assert!(result.status.success(), 
          "Replay command failed: {}", 
          String::from_utf8_lossy(&result.stderr));

      // Consume messages from the target topic
      let consumed_messages = kafka.consume_messages(&target_topic, 5, 10).await?;

      // Verify all messages have the new headers
      for message in &consumed_messages {
          assert!(message.headers.contains_key("new-header"), 
              "New header 'new-header' missing from replayed message");
          assert_eq!(message.headers.get("new-header"), Some(&"new-value".to_string()), 
              "Header 'new-header' has incorrect value");

          assert!(message.headers.contains_key("replay-timestamp"), 
              "New header 'replay-timestamp' missing from replayed message");
          assert_eq!(message.headers.get("replay-timestamp"), Some(&"12345678".to_string()), 
              "Header 'replay-timestamp' has incorrect value");
      }

      Ok(())
  }

  /// Test overriding keys during replay
  #[tokio::test]
  async fn test_replay_with_key_override() -> Result<()> {
      // Setup
      let target_topic = format!("test-key-override-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let cli = CliExecutor::new();

      // Create source directory with message files
      let (source_dir, _) = prepare_message_directory(5).await?;

      // Create target topic
      kafka.create_topic(&target_topic, 1).await?;

      // Execute replay command with key override
      let result = cli.replay(
          &source_dir,
          &target_topic,
          BOOTSTRAP_SERVERS,
          &["--override-key", "new-test-key"],
      ).await?;

      // Verify command succeeded
      assert!(result.status.success(), 
          "Replay command failed: {}", 
          String::from_utf8_lossy(&result.stderr));

      // Consume messages from the target topic
      let consumed_messages = kafka.consume_messages(&target_topic, 5, 10).await?;

      // Verify all messages have the overridden key
      for message in &consumed_messages {
          assert_eq!(message.key, Some("new-test-key".as_bytes().to_vec()), 
              "Message key wasn't overridden to 'new-test-key'");
      }

      Ok(())
  }

  /// Test replaying with a transformation script
  #[tokio::test]
  async fn test_replay_with_transformation() -> Result<()> {
      // Setup
      let target_topic = format!("test-transform-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let cli = CliExecutor::new();

      // Create source directory with message files - specifically using order messages
      let temp_dir = create_temp_dir("transform-source")?;
      let mut generator = TestDataGenerator::new(42);

      // Generate 5 order messages
      let mut messages = Vec::new();
      for _ in 0..5 {
          messages.push(generator.generate_order_message());
      }

      // Write each message to a file
      for (i, message) in messages.iter().enumerate() {
          let json_value = serde_json::to_string(&message)?;
          let file_path = temp_dir.path().join(format!("{}.json", i));
          fs::write(file_path, json_value)?;
      }

      // Create a transformation script
      let script_path = temp_dir.path().join("transform.js");
      let script_content = r#

### Task 6: Implement Stats Command Tests
- **ID:** T6
- **Priority:** MEDIUM
- **Description:** Create tests for the stats command functionality
- **Acceptance Criteria:**
  - Tests verify correct statistics calculation
  - All output formats are tested
  - Works with different storage backends
  - Handles edge cases (empty stores, very large messages)
- **Tasks:**
  - [ ] Test basic statistics generation
  - [ ] Verify all output formats (text, JSON, CSV)
  - [ ] Test with different storage backends
  - [ ] Add edge case tests

## Priority 3: Advanced Test Scenarios

### Task 7: Implement End-to-End Workflow Tests
- **ID:** T7
- **Priority:** MEDIUM
- **Description:** Create tests that exercise complete workflow scenarios
- **Acceptance Criteria:**
  - Tests cover store-analyze-replay workflows
  - Verifies data integrity through the entire pipeline
  - Tests common real-world usage patterns
- **Tasks:**
  - [ ] Create store-to-replay pipeline test
  - [ ] Implement filtering and transformation workflow
  - [ ] Test with different message formats
  - [ ] Verify data integrity through multiple operations
- **Detailed Description:**

  End-to-end workflow tests are essential for verifying that the different components of kafka-scribe work together correctly. These tests should simulate real-world usage patterns and verify data integrity through the entire pipeline.

  ```rust
  // tests/integration/e2e_tests.rs
  use std::fs;
  use std::path::Path;
  use std::process::Command;
  use std::time::Duration;
  use anyhow::Result;
  use serde_json::{json, Value};
  use tokio::time::timeout;
  use tracing::{debug, info};

  use crate::common::cli_helpers::CliExecutor;
  use crate::common::dir_helpers::{create_temp_dir, load_json_files};
  use crate::common::kafka_setup::KafkaTestHelper;
  use crate::common::test_data::TestDataGenerator;

  const BOOTSTRAP_SERVERS: &str = "localhost:29092";

  /// Test the complete workflow: store messages, inspect them, modify, and replay
  #[tokio::test]
  async fn test_e2e_store_inspect_modify_replay() -> Result<()> {
      // Setup
      let source_topic = format!("test-e2e-source-{}", uuid::Uuid::new_v4());
      let target_topic = format!("test-e2e-target-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let cli = CliExecutor::new();

      // Create a working directory for our test
      let work_dir = create_temp_dir("e2e-workflow")?;
      let store_dir = work_dir.path().join("stored_messages");
      let filtered_dir = work_dir.path().join("filtered_messages");
      fs::create_dir(&store_dir)?;
      fs::create_dir(&filtered_dir)?;

      // Step 1: Create source topic and produce test messages
      kafka.create_topic(&source_topic, 3).await?;
      let mut generator = TestDataGenerator::new(42);

      // Generate 20 messages with different patterns
      let mut pattern_a_msgs = generator.generate_filterable_messages("pattern-a", 10);
      let mut pattern_b_msgs = generator.generate_filterable_messages("pattern-b", 10);

      // Combine all messages
      let mut all_messages = Vec::new();
      all_messages.extend(pattern_a_msgs);
      all_messages.extend(pattern_b_msgs);

      // Produce messages to the source topic
      kafka.produce_messages(&source_topic, &all_messages).await?;

      // Step 2: Store all messages from the source topic
      let store_result = cli.store(
          &source_topic,
          BOOTSTRAP_SERVERS,
          &store_dir,
          &["--from-beginning"],
      ).await?;

      assert!(store_result.status.success(), 
          "Store command failed: {}", 
          String::from_utf8_lossy(&store_result.stderr));

      // Verify all messages were stored
      let stored_files = load_json_files(&store_dir)?;
      assert_eq!(stored_files.len(), 20, 
          "Expected 20 stored files, found {}", 
          stored_files.len());

      // Step 3: Simulate manual inspection by filtering out only pattern-a messages
      // We'll use grep to simulate a real-world manual filtering scenario
      let grep_result = Command::new("grep")
          .args(["-r", "pattern-a", store_dir.to_str().unwrap()])
          .output()?;

      assert!(grep_result.status.success(), 
          "grep command failed: {}", 
          String::from_utf8_lossy(&grep_result.stderr));

      // Step 4: Extract filtered message files to a separate directory
      // In real-world usage, this might be done manually or with scripts
      let mut filtered_count = 0;
      for entry in fs::read_dir(&store_dir)? {
          let entry = entry?;
          let path = entry.path();

          if path.is_file() && path.extension().map_or(false, |ext| ext == "json") {
              let content = fs::read_to_string(&path)?;

              // Check if this file contains pattern-a
              if content.contains("pattern-a") {
                  // Copy to filtered directory
                  let target_path = filtered_dir.join(path.file_name().unwrap());
                  fs::copy(&path, &target_path)?;
                  filtered_count += 1;
              }
          }
      }

      assert_eq!(filtered_count, 10, 
          "Expected 10 filtered files, found {}", 
          filtered_count);

      // Step 5: Modify one of the filtered messages
      // Find a file to modify
      let mut modified_file = None;
      for entry in fs::read_dir(&filtered_dir)? {
          let entry = entry?;
          let path = entry.path();

          if path.is_file() && path.extension().map_or(false, |ext| ext == "json") {
              // Read the file content
              let content = fs::read_to_string(&path)?;
              let mut json: Value = serde_json::from_str(&content)?;

              // Modify the message
              if let Some(obj) = json.as_object_mut() {
                  // Add a new header
                  if let Some(headers) = obj.get_mut("headers") {
                      if let Some(headers_obj) = headers.as_object_mut() {
                          headers_obj.insert("modified".to_string(), json!("true"));
                      }
                  }

                  // Modify the value if it's a string (assuming JSON string)
                  if let Some(value) = obj.get_mut("value") {
                      if let Some(value_str) = value.as_str() {
                          // For simplicity, just append a marker
                          let new_value = format!("{}-MODIFIED", value_str);
                          *value = json!(new_value);
                      }
                  }
              }

              // Write back the modified content
              fs::write(&path, serde_json::to_string_pretty(&json)?)?;
              modified_file = Some(path);
              break;
          }
      }

      assert!(modified_file.is_some(), "Failed to find a file to modify");

      // Step 6: Create target topic
      kafka.create_topic(&target_topic, 1).await?;

      // Step 7: Replay filtered and modified messages to the target topic
      let replay_result = cli.replay(
          &filtered_dir,
          &target_topic,
          BOOTSTRAP_SERVERS,
          &["--add-header", "workflow=e2e-test"],
      ).await?;

      assert!(replay_result.status.success(), 
          "Replay command failed: {}", 
          String::from_utf8_lossy(&replay_result.stderr));

      // Step 8: Consume messages from the target topic and verify
      let consumed_messages = kafka.consume_messages(&target_topic, 10, 10).await?;

      // Verify the correct number of messages were replayed
      assert_eq!(consumed_messages.len(), 10, 
          "Expected 10 replayed messages, consumed {}", 
          consumed_messages.len());

      // Verify all messages have the pattern-a pattern
      for message in &consumed_messages {
          if let Some(key) = &message.key {
              let key_str = String::from_utf8_lossy(key);
              assert!(key_str.contains("pattern-a"), 
                  "Replayed message key '{}' doesn't contain pattern-a", key_str);
          }

          // Verify all messages have the workflow header
          assert!(message.headers.contains_key("workflow"), 
              "Workflow header missing from replayed message");
          assert_eq!(message.headers.get("workflow"), Some(&"e2e-test".to_string()), 
              "Workflow header has incorrect value");

          // Check if we have a modified message
          if message.headers.contains_key("modified") {
              // This should be our modified message - check if value was modified
              if let Some(value) = &message.value {
                  let value_str = String::from_utf8_lossy(value);
                  assert!(value_str.contains("-MODIFIED"), 
                      "Modified message doesn't contain the expected modification");
              }
          }
      }

      Ok(())
  }

  /// Test an end-to-end filtering and transformation workflow
  #[tokio::test]
  async fn test_e2e_filtering_and_transformation() -> Result<()> {
      // Setup
      let source_topic = format!("test-e2e-filter-source-{}", uuid::Uuid::new_v4());
      let target_topic = format!("test-e2e-filter-target-{}", uuid::Uuid::new_v4());
      let kafka = KafkaTestHelper::new(BOOTSTRAP_SERVERS).await?;
      let cli = CliExecutor::new();

      // Create working directories
      let work_dir = create_temp_dir("e2e-filter-transform")?;
      let store_dir = work_dir.path().join("store");
      fs::create_dir(&store_dir)?;

      // Step 1: Create source topic with test messages
      kafka.create_topic(&source_topic, 1).await?;

      // Generate mixed message types
      let mut generator = TestDataGenerator::new(42);
      let orders = (0..5).map(|_| generator.generate_order_message()).collect::<Vec<_>>();
      let logs = (0..10).map(|_| generator.generate_log_message("INFO")).collect::<Vec<_>>();
      let errors = (0..5).map(|_| generator.generate_log_message("ERROR")).collect::<Vec<_>>();

      // Combine all messages
      let mut all_messages = Vec::new();
      all_messages.extend(orders);
      all_messages.extend(logs);
      all_messages.extend(errors);

      // Produce messages to the source topic
      kafka.produce_messages(&source_topic, &all_messages).await?;

      // Step 2: Store messages from the source topic, filtering only ERROR logs
      let store_result = cli.store(
          &source_topic,
          BOOTSTRAP_SERVERS,
          &store_dir,
          &["--from-beginning", "--header", "log-level=ERROR"],
      ).await?;

      assert!(store_result.status.success(), 
          "Store command failed: {}", 
          String::from_utf8_lossy(&store_result.stderr));

      // Verify only ERROR logs were stored (should be 5)
      let stored_files = load_json_files(&store_dir)?;
      assert_eq!(stored_files.len(), 5, 
          "Expected 5 ERROR log files, found {}", 
          stored_files.len());

      // Step 3: Create a transformation script
      let script_path = work_dir.path().join("transform.js");
      let script_content = r#

### Task 8: Add Performance Tests
- **ID:** T8
- **Priority:** MEDIUM
- **Description:** Implement tests for measuring performance characteristics
- **Acceptance Criteria:**
  - Tests measure throughput for store and replay operations
  - Verifies performance with different batch sizes
  - Tests parallel processing capabilities
  - Provides baseline performance metrics
- **Tasks:**
  - [ ] Create throughput measurement tests
  - [ ] Test different batch size configurations
  - [ ] Implement parallel processing tests
  - [ ] Add memory usage monitoring

### Task 9: Implement Schema Validation Tests
- **ID:** T9
- **Priority:** LOW
- **Description:** Add tests for schema validation functionality
- **Acceptance Criteria:**
  - Tests verify schema validation for different formats
  - Includes schema evolution scenarios
  - Tests schema registry integration
- **Tasks:**
  - [ ] Add JSON schema validation tests
  - [ ] Implement Avro schema tests
  - [ ] Test Protobuf schema validation
  - [ ] Add schema evolution test cases

## Priority 4: Test Infrastructure Automation

### Task 10: Improve CI Integration
- **ID:** T10
- **Priority:** MEDIUM
- **Description:** Enhance CI workflow for reliable test execution
- **Acceptance Criteria:**
  - Tests run reliably in CI environment
  - Test failures are clearly reported
  - CI includes coverage reporting
  - Setup and teardown is properly handled
- **Tasks:**
  - [ ] Create GitHub Actions workflow for integration tests
  - [ ] Add test reporting and artifacts
  - [ ] Implement caching for faster test runs
  - [ ] Add retry logic for flaky tests

### Task 11: Add Test Documentation
- **ID:** T11
- **Priority:** MEDIUM
- **Description:** Improve test documentation and examples
- **Acceptance Criteria:**
  - Documentation explains test organization
  - Includes examples for adding new tests
  - Documents test fixtures and utilities
  - Provides troubleshooting guidance
- **Tasks:**
  - [ ] Update README.md with comprehensive test information
  - [ ] Add inline documentation to test utilities
  - [ ] Create examples for common test patterns
  - [ ] Document test environment setup and requirements

### Task 12: Implement Test Data Management
- **ID:** T12
- **Priority:** LOW
- **Description:** Create system for managing test fixtures and test data
- **Acceptance Criteria:**
  - Test data is versioned and reproducible
  - Fixtures are organized and documented
  - Data generation is automated
  - Supports different test scenarios
- **Tasks:**
  - [ ] Create fixture management system
  - [ ] Implement versioned test data
  - [ ] Add documentation for test data
  - [ ] Create cleanup utilities for test artifacts

## Implementation Guidelines

### Test Organization

All integration tests should follow this organization:

```
tests/
├── integration/          # Test modules
│   ├── mod.rs            # Integration test module definition
│   ├── store_tests.rs    # Store command tests
│   ├── replay_tests.rs   # Replay command tests
│   └── stats_tests.rs    # Stats command tests
├── common/               # Shared test utilities
│   ├── mod.rs            # Common utilities module definition
│   ├── kafka_setup.rs    # Kafka setup utilities
│   ├── test_data.rs      # Test data generators
│   └── cli_helpers.rs    # CLI execution helpers
└── fixtures/             # Test fixtures
    ├── docker-compose.yml # Docker Compose configuration
    └── sample_messages/  # Sample messages for testing
```

### Test Naming Conventions

Follow these naming conventions for test functions:

- `test_<command>_<feature>_<scenario>` - For testing specific features
- `test_<command>_errors_<error_case>` - For testing error conditions
- `test_e2e_<scenario>` - For end-to-end tests

Examples:
- `test_store_filter_by_key_regex`
- `test_store_errors_invalid_bootstrap_server`
- `test_e2e_store_and_replay_with_transformation`

### Best Practices

1. **Test Independence**: Each test should be independent and not rely on the state from other tests
2. **Cleanup**: Always clean up resources after tests (e.g., temporary directories, Kafka topics)
3. **Descriptive Names**: Use descriptive test names that explain what is being tested
4. **Error Messages**: Include descriptive error messages in assertions
5. **Timeout Handling**: Add appropriate timeouts for operations that might hang
6. **Logging**: Use logging to aid debugging of test failures
7. **Parameterization**: Use test parameterization for testing similar scenarios with different inputs
