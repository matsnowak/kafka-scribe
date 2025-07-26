//! Utilities for generating test data for integration tests.
//!
//! This module provides functions and a TestDataGenerator for creating
//! test messages with different characteristics for use in integration tests.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use rand::{distributions::Alphanumeric, Rng, RngCore, SeedableRng};
use rand::rngs::StdRng;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use uuid::Uuid;

/// A test message for use in integration tests.
#[derive(Debug, Clone)]
pub struct TestMessage {
    /// The message key.
    pub key: Vec<u8>,
    /// The message value.
    pub value: Vec<u8>,
    /// The message headers.
    pub headers: Option<HashMap<String, Vec<u8>>>,
    /// The message timestamp.
    pub timestamp: i64,
}

/// A JSON representation of a Kafka message for validation.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JsonMessage {
    pub key: Option<String>,
    pub value: serde_json::Value,
    pub headers: Option<HashMap<String, String>>,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: i64,
}

impl TestMessage {
    /// Creates a new test message with the given key, value, and headers.
    pub fn new(
        key: impl AsRef<[u8]>,
        value: impl AsRef<[u8]>,
        headers: Option<HashMap<String, Vec<u8>>>,
    ) -> Self {
        Self {
            key: key.as_ref().to_vec(),
            value: value.as_ref().to_vec(),
            headers,
            timestamp: SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64,
        }
    }

    /// Creates a new test message with a JSON value.
    pub fn new_json(
        key: impl AsRef<str>,
        value: serde_json::Value,
        headers: Option<HashMap<String, String>>,
    ) -> Self {
        let headers_bytes = headers.map(|h| {
            h.into_iter()
                .map(|(k, v)| (k, v.as_bytes().to_vec()))
                .collect()
        });

        Self::new(
            key.as_ref().as_bytes(),
            serde_json::to_vec(&value).unwrap(),
            headers_bytes,
        )
    }
}

/// Generates a random string of the given length.
pub fn random_string(length: usize) -> String {
    rand::thread_rng()
        .sample_iter(&Alphanumeric)
        .take(length)
        .map(char::from)
        .collect()
}

/// Generates a set of test messages with different characteristics.
pub fn generate_test_messages(count: usize) -> Vec<TestMessage> {
    let mut messages = Vec::with_capacity(count);

    for i in 0..count {
        let key = format!("key-{}", i);
        let value = json!({
            "id": i,
            "name": format!("Test Message {}", i),
            "timestamp": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis(),
            "data": {
                "field1": random_string(10),
                "field2": i % 5,
                "field3": i % 2 == 0,
            }
        });

        let mut headers = HashMap::new();
        headers.insert("message-type".to_string(), "test".to_string());
        headers.insert("sequence".to_string(), i.to_string());
        headers.insert("even".to_string(), (i % 2 == 0).to_string());

        messages.push(TestMessage::new_json(key, value, Some(headers)));
    }

    messages
}

/// Generates a set of test messages with binary data.
pub fn generate_binary_test_messages(count: usize) -> Vec<TestMessage> {
    let mut messages = Vec::with_capacity(count);

    for i in 0..count {
        let key = format!("binary-key-{}", i).into_bytes();
        let mut value = Vec::with_capacity(100);
        for _ in 0..100 {
            value.push(rand::thread_rng().gen::<u8>());
        }

        let mut headers = HashMap::new();
        headers.insert(
            "message-type".to_string(),
            "binary".as_bytes().to_vec(),
        );
        headers.insert(
            "sequence".to_string(),
            i.to_string().as_bytes().to_vec(),
        );

        messages.push(TestMessage::new(key, value, Some(headers)));
    }

    messages
}

/// Generates a set of test messages with specific keys for testing key filtering.
pub fn generate_key_filtered_test_messages() -> Vec<TestMessage> {
    let mut messages = Vec::new();

    // Messages with keys that match the pattern "user-*"
    for i in 0..5 {
        let key = format!("user-{}", i);
        let value = json!({
            "id": i,
            "type": "user",
            "name": format!("User {}", i),
        });

        messages.push(TestMessage::new_json(key, value, None));
    }

    // Messages with keys that match the pattern "order-*"
    for i in 0..5 {
        let key = format!("order-{}", i);
        let value = json!({
            "id": i,
            "type": "order",
            "amount": i * 10,
        });

        messages.push(TestMessage::new_json(key, value, None));
    }

    // Messages with keys that don't match either pattern
    for i in 0..5 {
        let key = format!("other-{}", i);
        let value = json!({
            "id": i,
            "type": "other",
            "data": random_string(10),
        });

        messages.push(TestMessage::new_json(key, value, None));
    }

    messages
}

/// Generates a set of test messages with specific headers for testing header filtering.
pub fn generate_header_filtered_test_messages() -> Vec<TestMessage> {
    let mut messages = Vec::new();

    // Messages with header "region=us"
    for i in 0..5 {
        let key = format!("key-{}", i);
        let value = json!({
            "id": i,
            "region": "us",
            "data": random_string(10),
        });

        let mut headers = HashMap::new();
        headers.insert("region".to_string(), "us".to_string());

        messages.push(TestMessage::new_json(key, value, Some(headers)));
    }

    // Messages with header "region=eu"
    for i in 5..10 {
        let key = format!("key-{}", i);
        let value = json!({
            "id": i,
            "region": "eu",
            "data": random_string(10),
        });

        let mut headers = HashMap::new();
        headers.insert("region".to_string(), "eu".to_string());

        messages.push(TestMessage::new_json(key, value, Some(headers)));
    }

    // Messages with no region header
    for i in 10..15 {
        let key = format!("key-{}", i);
        let value = json!({
            "id": i,
            "data": random_string(10),
        });

        messages.push(TestMessage::new_json(key, value, None));
    }

    messages
}

/// Generates a large set of test messages for testing performance.
pub fn generate_large_test_message_set(count: usize) -> Vec<TestMessage> {
    generate_test_messages(count)
}

/// Generates a set of test messages with timestamps for testing timestamp filtering.
pub fn generate_timestamped_test_messages() -> Vec<TestMessage> {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_millis() as i64;

    let mut messages = Vec::new();

    // Messages from 1 hour ago
    for i in 0..5 {
        let key = format!("past-{}", i);
        let value = json!({
            "id": i,
            "time": "past",
            "data": random_string(10),
        });

        let mut msg = TestMessage::new_json(key, value, None);
        msg.timestamp = now - 3600000; // 1 hour ago
        messages.push(msg);
    }

    // Recent messages
    for i in 5..10 {
        let key = format!("recent-{}", i);
        let value = json!({
            "id": i,
            "time": "recent",
            "data": random_string(10),
        });

        messages.push(TestMessage::new_json(key, value, None));
    }

    messages
}

/// A generator for creating test data with deterministic output.
pub struct TestDataGenerator {
    /// Random number generator with seed for reproducibility
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
    pub fn generate_order_message(&mut self) -> TestMessage {
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
            "timestamp": SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64
        });

        let mut headers = HashMap::new();
        headers.insert("source".to_string(), "test-generator".to_string());

        let mut msg = TestMessage::new_json(customer_id, value, Some(headers));
        msg.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        msg
    }

    /// Generate a binary message with random content
    pub fn generate_binary_message(&mut self, size: usize) -> TestMessage {
        let mut data = vec![0u8; size];
        self.rng.fill_bytes(&mut data);

        let key = format!("binary-{}", Uuid::new_v4());

        let mut headers = HashMap::new();
        headers.insert(
            "content-type".to_string(),
            "application/octet-stream".to_string(),
        );

        let headers_bytes = headers.into_iter()
            .map(|(k, v)| (k, v.as_bytes().to_vec()))
            .collect();

        let mut msg = TestMessage::new(key.as_bytes().to_vec(), data, Some(headers_bytes));
        msg.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        msg
    }

    /// Generate a text log message
    pub fn generate_log_message(&mut self, level: &str) -> TestMessage {
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
        let timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis().to_string();
        let log_message = format!("[{}] [{} {}] {}", 
            timestamp,
            service,
            log_level,
            message
        );

        let key = format!("{}-{}", service, Uuid::new_v4());

        let mut headers = HashMap::new();
        headers.insert("log-level".to_string(), log_level.clone());
        headers.insert("service".to_string(), service.to_string());

        let headers_bytes = headers.into_iter()
            .map(|(k, v)| (k, v.as_bytes().to_vec()))
            .collect();

        let mut msg = TestMessage::new(
            key.as_bytes().to_vec(),
            log_message.as_bytes().to_vec(),
            Some(headers_bytes),
        );
        msg.timestamp = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_millis() as i64;
        msg
    }

    /// Generate a batch of messages
    pub fn generate_message_batch(&mut self, count: usize) -> Vec<TestMessage> {
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
    pub fn generate_filterable_messages(&mut self, key_pattern: &str, count: usize) -> Vec<TestMessage> {
        let mut messages = Vec::with_capacity(count);

        for i in 0..count {
            let key = format!("{}-{}", key_pattern, i);
            let value = format!("Message {} for key pattern {}", i, key_pattern);

            let mut headers = HashMap::new();
            headers.insert("pattern".to_string(), key_pattern.to_string());

            let headers_bytes = headers.into_iter()
                .map(|(k, v)| (k, v.as_bytes().to_vec()))
                .collect();

            let mut msg = TestMessage::new(
                key.as_bytes().to_vec(),
                value.as_bytes().to_vec(),
                Some(headers_bytes),
            );
            msg.timestamp = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as i64;

            messages.push(msg);
        }

        messages
    }
}
