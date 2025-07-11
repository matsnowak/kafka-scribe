//! Utilities for generating test data for integration tests.
//!
//! This module provides functions for generating test messages with
//! different characteristics for use in integration tests.

use std::collections::HashMap;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::Result;
use rand::{distributions::Alphanumeric, Rng};
use serde::{Deserialize, Serialize};
use serde_json::json;

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