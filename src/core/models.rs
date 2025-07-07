use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a Kafka message with all its metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaMessage {
    /// Message key
    pub key: Option<String>,
    /// Message value/payload
    pub value: Option<String>,
    /// Message headers
    pub headers: HashMap<String, String>,
    /// Topic name
    pub topic: String,
    /// Partition number
    pub partition: i32,
    /// Message offset
    pub offset: i64,
    /// Message timestamp
    pub timestamp: Option<i64>,
}

impl KafkaMessage {
    /// Create a new KafkaMessage
    pub fn new(
        key: Option<String>,
        value: Option<String>,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Self {
        Self {
            key,
            value,
            headers: HashMap::new(),
            topic,
            partition,
            offset,
            timestamp: None,
        }
    }

    /// Add a header to the message
    pub fn with_header<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set the timestamp
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Convert to JSON string
    pub fn to_json(&self) -> serde_json::Result<String> {
        serde_json::to_string_pretty(self)
    }

    /// Create from JSON string
    pub fn from_json(json: &str) -> serde_json::Result<Self> {
        serde_json::from_str(json)
    }
}

/// Statistics about a message store
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StorageStats {
    /// Total number of messages
    pub message_count: u64,
    /// Total size in bytes
    pub total_size: u64,
    /// Earliest message timestamp
    pub earliest_timestamp: Option<i64>,
    /// Latest message timestamp
    pub latest_timestamp: Option<i64>,
    /// Number of unique partitions
    pub partition_count: u32,
    /// Number of unique topics
    pub topic_count: u32,
}

impl Default for StorageStats {
    fn default() -> Self {
        Self {
            message_count: 0,
            total_size: 0,
            earliest_timestamp: None,
            latest_timestamp: None,
            partition_count: 0,
            topic_count: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_message_creation() {
        let msg = KafkaMessage::new(
            Some("test-key".to_string()),
            Some("test-value".to_string()),
            "test-topic".to_string(),
            0,
            100,
        );

        assert_eq!(msg.key, Some("test-key".to_string()));
        assert_eq!(msg.value, Some("test-value".to_string()));
        assert_eq!(msg.topic, "test-topic");
        assert_eq!(msg.partition, 0);
        assert_eq!(msg.offset, 100);
    }

    #[test]
    fn test_kafka_message_with_header() {
        let msg = KafkaMessage::new(
            None,
            Some("test-value".to_string()),
            "test-topic".to_string(),
            0,
            100,
        )
        .with_header("correlation-id", "abc123")
        .with_timestamp(1640995200000);

        assert_eq!(msg.headers.get("correlation-id"), Some(&"abc123".to_string()));
        assert_eq!(msg.timestamp, Some(1640995200000));
    }

    #[test]
    fn test_kafka_message_json_serialization() {
        let msg = KafkaMessage::new(
            Some("test-key".to_string()),
            Some("test-value".to_string()),
            "test-topic".to_string(),
            0,
            100,
        );

        let json = msg.to_json().unwrap();
        let deserialized = KafkaMessage::from_json(&json).unwrap();
        assert_eq!(msg, deserialized);
    }
}