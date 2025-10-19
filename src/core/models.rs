use crate::core::errors::{FormatError, FormatResult};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Represents a Kafka message with all its metadata
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct KafkaMessage {
    /// Message key (binary data)
    pub key: Option<Vec<u8>>,

    /// Message value/payload (binary data)
    pub value: Option<Vec<u8>>,

    /// Message headers
    pub headers: HashMap<String, String>,

    /// Topic name
    pub topic: String,

    /// Partition number
    pub partition: i32,

    /// Message offset
    pub offset: i64,

    /// Message timestamp (milliseconds since epoch)
    pub timestamp: Option<i64>,
}

impl KafkaMessage {
    /// Create a new KafkaMessage
    ///
    /// # Arguments
    ///
    /// * `key` - Optional message key
    /// * `value` - Optional message value/payload
    /// * `topic` - Topic name
    /// * `partition` - Partition number
    /// * `offset` - Message offset
    ///
    /// # Returns
    ///
    /// A new KafkaMessage instance
    pub fn new(
        key: Option<Vec<u8>>,
        value: Option<Vec<u8>>,
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

    /// Create a new KafkaMessage from string key and value
    ///
    /// # Arguments
    ///
    /// * `key` - Optional message key as string
    /// * `value` - Optional message value/payload as string
    /// * `topic` - Topic name
    /// * `partition` - Partition number
    /// * `offset` - Message offset
    ///
    /// # Returns
    ///
    /// A new KafkaMessage instance
    pub fn new_from_strings(
        key: Option<String>,
        value: Option<String>,
        topic: String,
        partition: i32,
        offset: i64,
    ) -> Self {
        Self {
            key: key.map(|k| k.into_bytes()),
            value: value.map(|v| v.into_bytes()),
            headers: HashMap::new(),
            topic,
            partition,
            offset,
            timestamp: None,
        }
    }

    /// Add a header to the message
    ///
    /// # Arguments
    ///
    /// * `key` - Header key
    /// * `value` - Header value
    ///
    /// # Returns
    ///
    /// Self with the header added
    pub fn with_header<K, V>(mut self, key: K, value: V) -> Self
    where
        K: Into<String>,
        V: Into<String>,
    {
        self.headers.insert(key.into(), value.into());
        self
    }

    /// Set the timestamp
    ///
    /// # Arguments
    ///
    /// * `timestamp` - Timestamp in milliseconds since epoch
    ///
    /// # Returns
    ///
    /// Self with the timestamp set
    pub fn with_timestamp(mut self, timestamp: i64) -> Self {
        self.timestamp = Some(timestamp);
        self
    }

    /// Get the key as a UTF-8 string, if it exists and is valid UTF-8
    ///
    /// # Returns
    ///
    /// Option containing the key as a string, or None if the key doesn't exist or isn't valid UTF-8
    pub fn key_as_string(&self) -> Option<Result<String, std::string::FromUtf8Error>> {
        self.key.as_ref().map(|k| String::from_utf8(k.clone()))
    }

    /// Get the value as a UTF-8 string, if it exists and is valid UTF-8
    ///
    /// # Returns
    ///
    /// Option containing the value as a string, or None if the value doesn't exist or isn't valid UTF-8
    pub fn value_as_string(&self) -> Option<Result<String, std::string::FromUtf8Error>> {
        self.value.as_ref().map(|v| String::from_utf8(v.clone()))
    }

    /// Convert to JSON string
    ///
    /// # Returns
    ///
    /// A Result containing the JSON string or a FormatError
    pub fn to_json(&self) -> FormatResult<String> {
        serde_json::to_string_pretty(self)
            .map_err(|e| FormatError::Encoding(format!("Failed to serialize to JSON: {}", e)))
    }

    /// Create from JSON string
    ///
    /// # Arguments
    ///
    /// * `json` - JSON string to parse
    ///
    /// # Returns
    ///
    /// A Result containing the KafkaMessage or a FormatError
    pub fn from_json(json: &str) -> FormatResult<Self> {
        serde_json::from_str(json)
            .map_err(|e| FormatError::Decoding(format!("Failed to deserialize from JSON: {}", e)))
    }

    /// Get a hexadecimal representation of the key, if it exists
    ///
    /// # Returns
    ///
    /// Option containing the key as a hex string, or None if the key doesn't exist
    pub fn key_as_hex(&self) -> Option<String> {
        self.key
            .as_ref()
            .map(|k| k.iter().map(|b| format!("{:02x}", b)).collect::<String>())
    }

    /// Get a hexadecimal representation of the value, if it exists
    ///
    /// # Returns
    ///
    /// Option containing the value as a hex string, or None if the value doesn't exist
    pub fn value_as_hex(&self) -> Option<String> {
        self.value
            .as_ref()
            .map(|v| v.iter().map(|b| format!("{:02x}", b)).collect::<String>())
    }
}

/// Statistics about a message store
#[derive(Debug, Clone, Serialize, Deserialize, Default)]
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_kafka_message_creation() {
        let msg = KafkaMessage::new(
            Some("test-key".as_bytes().to_vec()),
            Some("test-value".as_bytes().to_vec()),
            "test-topic".to_string(),
            0,
            100,
        );

        assert_eq!(msg.key, Some("test-key".as_bytes().to_vec()));
        assert_eq!(msg.value, Some("test-value".as_bytes().to_vec()));
        assert_eq!(msg.topic, "test-topic");
        assert_eq!(msg.partition, 0);
        assert_eq!(msg.offset, 100);
    }

    #[test]
    fn test_kafka_message_with_header() {
        let msg = KafkaMessage::new(
            None,
            Some("test-value".as_bytes().to_vec()),
            "test-topic".to_string(),
            0,
            100,
        )
        .with_header("correlation-id", "abc123")
        .with_timestamp(1640995200000);

        assert_eq!(
            msg.headers.get("correlation-id"),
            Some(&"abc123".to_string())
        );
        assert_eq!(msg.timestamp, Some(1640995200000));
    }

    #[test]
    fn test_kafka_message_json_serialization() {
        let msg = KafkaMessage::new(
            Some("test-key".as_bytes().to_vec()),
            Some("test-value".as_bytes().to_vec()),
            "test-topic".to_string(),
            0,
            100,
        );

        let json = msg.to_json().unwrap();
        let deserialized = KafkaMessage::from_json(&json).unwrap();
        assert_eq!(msg, deserialized);
    }

    #[test]
    fn test_kafka_message_string_conversion() {
        let msg = KafkaMessage::new_from_strings(
            Some("test-key".to_string()),
            Some("test-value".to_string()),
            "test-topic".to_string(),
            0,
            100,
        );

        // Check that the strings were properly converted to bytes
        assert_eq!(msg.key, Some("test-key".as_bytes().to_vec()));
        assert_eq!(msg.value, Some("test-value".as_bytes().to_vec()));

        // Check that we can convert back to strings
        assert_eq!(msg.key_as_string().unwrap().unwrap(), "test-key");
        assert_eq!(msg.value_as_string().unwrap().unwrap(), "test-value");
    }

    #[test]
    fn test_kafka_message_hex_encoding() {
        let msg = KafkaMessage::new(
            Some(vec![0x01, 0x02, 0x03, 0x04]),
            Some(vec![0xA1, 0xB2, 0xC3, 0xD4]),
            "test-topic".to_string(),
            0,
            100,
        );

        assert_eq!(msg.key_as_hex().unwrap(), "01020304");
        assert_eq!(msg.value_as_hex().unwrap(), "a1b2c3d4");
    }
}
