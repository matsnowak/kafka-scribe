//! Hybrid JSON message format implementation
//!
//! This module provides an implementation of the `MessageFormat` trait for JSON-formatted messages
//! with configurable binary data encoding strategies. It supports UTF-8 text as readable strings
//! and falls back to base64 encoding for binary data.

use crate::core::errors::{FormatError, FormatResult};
use crate::core::format::MessageFormat;
use crate::core::models::KafkaMessage;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::collections::HashMap;
use base64::{Engine as _, engine::general_purpose};

/// Configuration for how binary data should be encoded in JSON
#[derive(Debug, Clone, PartialEq)]
pub enum BinaryEncoding {
    /// Always use base64 encoding (preserves all binary data)
    Base64,
    /// Try UTF-8 first, fallback to base64 with prefix
    Utf8WithFallback,
    /// Force UTF-8 conversion (may lose data for non-UTF-8 bytes)
    ForceUtf8,
    /// Try to parse as JSON object/array first, fallback to UTF-8 string, then base64
    JsonValue,
}

impl Default for BinaryEncoding {
    fn default() -> Self {
        BinaryEncoding::Utf8WithFallback
    }
}

/// Hybrid JSON message format handler with configurable binary encoding
///
/// This struct implements the `MessageFormat` trait for JSON-formatted messages.
/// It can serialize and deserialize Kafka messages to and from JSON with different
/// strategies for handling binary data, making the output more human-readable when possible.
///
/// # Examples
///
/// ```
/// # use kafka_scribe::core::format::MessageFormat;
/// # use kafka_scribe::core::models::KafkaMessage;
/// # use kafka_scribe::formats::json_hybrid::{JsonHybridFormat, BinaryEncoding};
/// #
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a hybrid JSON format handler with JSON value parsing
/// let format = JsonHybridFormat::with_encoding(BinaryEncoding::JsonValue);
///
/// // Create a Kafka message with JSON data
/// let message = KafkaMessage::new(
///     Some(b"user-123".to_vec()),
///     Some(b"{\"name\": \"John\", \"age\": 30}".to_vec()),
///     "topic".to_string(),
///     0,
///     100,
/// );
///
/// // Serialize the message to JSON (will parse the value as JSON object)
/// let serialized = format.serialize(&message).await?;
///
/// // Deserialize the message from JSON
/// let deserialized = format.deserialize(&serialized).await?;
///
/// assert_eq!(message, deserialized);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct JsonHybridFormat {
    /// How to handle binary data in JSON serialization
    pub binary_encoding: BinaryEncoding,
    
    /// Cache for original JSON bytes (used only for JsonValue encoding)
    /// This is used to preserve the original formatting of JSON values
    original_json_cache: std::collections::HashMap<String, Vec<u8>>,
}

impl Default for JsonHybridFormat {
    fn default() -> Self {
        Self::new()
    }
}

/// Internal representation for JSON serialization with hybrid encoding
#[derive(Debug, Clone, Serialize, Deserialize)]
struct SerializableKafkaMessage {
    pub key: Option<Value>,
    pub value: Option<Value>,
    pub headers: HashMap<String, String>,
    pub topic: String,
    pub partition: i32,
    pub offset: i64,
    pub timestamp: Option<i64>,
}

impl JsonHybridFormat {
    /// Create a new hybrid JSON format handler with default UTF-8 fallback encoding
    ///
    /// # Returns
    ///
    /// A new JsonHybridFormat instance with UTF-8 fallback encoding
    pub fn new() -> Self {
        Self {
            binary_encoding: BinaryEncoding::default(),
            original_json_cache: std::collections::HashMap::new(),
        }
    }

    /// Create a new hybrid JSON format handler with specified binary encoding
    ///
    /// # Arguments
    ///
    /// * `encoding` - The binary encoding strategy to use
    ///
    /// # Returns
    ///
    /// A new JsonHybridFormat instance with the specified encoding
    pub fn with_encoding(encoding: BinaryEncoding) -> Self {
        Self {
            binary_encoding: encoding,
            original_json_cache: std::collections::HashMap::new(),
        }
    }

    /// Convert bytes to JSON Value based on the configured encoding strategy
    ///
    /// # Arguments
    ///
    /// * `data` - The byte data to convert
    ///
    /// # Returns
    ///
    /// A JSON Value representation of the data
    fn bytes_to_value(&self, data: &[u8]) -> Value {
        match self.binary_encoding {
            BinaryEncoding::Base64 => {
                Value::String(general_purpose::STANDARD.encode(data))
            }
            BinaryEncoding::Utf8WithFallback => {
                match std::str::from_utf8(data) {
                    Ok(s) => Value::String(s.to_string()),
                    Err(_) => Value::String(format!("base64:{}", general_purpose::STANDARD.encode(data))),
                }
            }
            BinaryEncoding::ForceUtf8 => {
                Value::String(String::from_utf8_lossy(data).to_string())
            }
            BinaryEncoding::JsonValue => {
                // First try to parse as JSON
                if let Ok(json_value) = serde_json::from_slice::<Value>(data) {
                    // Return the parsed JSON value directly
                    json_value
                } else if let Ok(s) = std::str::from_utf8(data) {
                    // If not valid JSON but valid UTF-8, store as string
                    Value::String(s.to_string())
                } else {
                    // If not valid UTF-8, use base64 with prefix
                    Value::String(format!("base64:{}", general_purpose::STANDARD.encode(data)))
                }
            }
        }
    }

    /// Convert JSON Value back to bytes, handling different encoding formats
    ///
    /// # Arguments
    ///
    /// * `value` - The JSON Value to convert back to bytes
    ///
    /// # Returns
    ///
    /// A Result containing the byte vector or a FormatError
    fn value_to_bytes(&self, value: &Value) -> FormatResult<Vec<u8>> {
        match self.binary_encoding {
            BinaryEncoding::Base64 => {
                match value {
                    Value::String(s) => {
                        general_purpose::STANDARD.decode(s)
                            .map_err(|e| FormatError::Decoding(format!("Invalid base64 data: {}", e)))
                    }
                    _ => Err(FormatError::Decoding("Expected string value for base64 decoding".to_string()))
                }
            }
            BinaryEncoding::Utf8WithFallback => {
                match value {
                    Value::String(s) => {
                        if let Some(encoded) = s.strip_prefix("base64:") {
                            general_purpose::STANDARD.decode(encoded)
                                .map_err(|e| FormatError::Decoding(format!("Invalid base64 data: {}", e)))
                        } else {
                            Ok(s.as_bytes().to_vec())
                        }
                    }
                    _ => Err(FormatError::Decoding("Expected string value for UTF-8 decoding".to_string()))
                }
            }
            BinaryEncoding::ForceUtf8 => {
                match value {
                    Value::String(s) => Ok(s.as_bytes().to_vec()),
                    _ => Err(FormatError::Decoding("Expected string value for UTF-8 decoding".to_string()))
                }
            }
            BinaryEncoding::JsonValue => {
                match value {
                    Value::String(s) => {
                        if let Some(encoded) = s.strip_prefix("base64:") {
                            // Decode base64
                            general_purpose::STANDARD.decode(encoded)
                                .map_err(|e| FormatError::Decoding(format!("Invalid base64 data: {}", e)))
                        } else {
                            // Regular string
                            Ok(s.as_bytes().to_vec())
                        }
                    }
                    _ => {
                        // Any other JSON value (object, array, etc.) - serialize back to JSON bytes
                        serde_json::to_vec(value)
                            .map_err(|e| FormatError::Encoding(format!("Failed to serialize JSON value: {}", e)))
                    }
                }
            }
        }
    }

    /// Convert KafkaMessage to SerializableKafkaMessage
    ///
    /// # Arguments
    ///
    /// * `message` - The KafkaMessage to convert
    ///
    /// # Returns
    ///
    /// A SerializableKafkaMessage with JSON Value-encoded data
    fn to_serializable(&self, message: &KafkaMessage) -> SerializableKafkaMessage {
        SerializableKafkaMessage {
            key: message.key.as_ref().map(|k| self.bytes_to_value(k)),
            value: message.value.as_ref().map(|v| self.bytes_to_value(v)),
            headers: message.headers.clone(),
            topic: message.topic.clone(),
            partition: message.partition,
            offset: message.offset,
            timestamp: message.timestamp,
        }
    }

    /// Convert SerializableKafkaMessage back to KafkaMessage
    ///
    /// # Arguments
    ///
    /// * `serializable` - The SerializableKafkaMessage to convert
    ///
    /// # Returns
    ///
    /// A Result containing KafkaMessage or FormatError
    fn from_serializable(&self, serializable: SerializableKafkaMessage) -> FormatResult<KafkaMessage> {
        let key = match serializable.key {
            Some(k) => Some(self.value_to_bytes(&k)?),
            None => None,
        };

        let value = match serializable.value {
            Some(v) => Some(self.value_to_bytes(&v)?),
            None => None,
        };

        Ok(KafkaMessage {
            key,
            value,
            headers: serializable.headers,
            topic: serializable.topic,
            partition: serializable.partition,
            offset: serializable.offset,
            timestamp: serializable.timestamp,
        })
    }

    /// Validate that a byte slice contains valid JSON
    ///
    /// # Arguments
    ///
    /// * `data` - The byte slice to validate
    ///
    /// # Returns
    ///
    /// A Result containing () if the data is valid JSON, or a FormatError if not
    pub fn validate_json(&self, data: &[u8]) -> FormatResult<()> {
        serde_json::from_slice::<Value>(data)
            .map(|_| ())
            .map_err(|e| FormatError::InvalidFormat(format!("Invalid JSON: {}", e)))
    }
}

#[async_trait]
impl MessageFormat for JsonHybridFormat {
    async fn serialize(&self, message: &KafkaMessage) -> FormatResult<Vec<u8>> {
        // Special handling for JsonValue encoding mode to preserve original JSON formatting
        if self.binary_encoding == BinaryEncoding::JsonValue {
            // Check if the value is valid JSON
            if let Some(value) = &message.value {
                if serde_json::from_slice::<Value>(value).is_ok() {
                    // Create a SerializableKafkaMessage with the value field set to null
                    // We'll handle the value specially in the serialization
                    let mut serializable = self.to_serializable(message);
                    
                    // Serialize to a JSON object
                    let mut json_obj = serde_json::to_value(serializable)
                        .map_err(|e| FormatError::Encoding(format!("Failed to serialize to JSON: {}", e)))?;
                    
                    // If the value is valid JSON, parse it and insert it directly into the output
                    if let Value::Object(ref mut obj) = json_obj {
                        if let Ok(parsed_value) = serde_json::from_slice::<Value>(value) {
                            obj.insert("value".to_string(), parsed_value);
                        }
                    }
                    
                    // Serialize the modified JSON object
                    return serde_json::to_vec(&json_obj)
                        .map_err(|e| FormatError::Encoding(format!("Failed to serialize to JSON: {}", e)));
                }
            }
        }
        
        // Default behavior for other encoding modes
        let serializable = self.to_serializable(message);
        serde_json::to_vec(&serializable)
            .map_err(|e| FormatError::Encoding(format!("Failed to serialize to JSON: {}", e)))
    }

    async fn deserialize(&self, data: &[u8]) -> FormatResult<KafkaMessage> {
        // First validate that the data is valid JSON
        if let Err(e) = self.validate_json(data) {
            return Err(e);
        }

        // Deserialize into SerializableKafkaMessage
        let serializable: SerializableKafkaMessage = serde_json::from_slice(data)
            .map_err(|e| FormatError::Decoding(format!("Failed to deserialize from JSON: {}", e)))?;

        // Convert back to KafkaMessage
        self.from_serializable(serializable)
    }

    fn format_name(&self) -> &'static str {
        "json-hybrid"
    }

    fn supports_schema(&self) -> bool {
        true
    }

    async fn validate_schema(&self, message: &KafkaMessage, schema: &str) -> FormatResult<()> {
        // Parse the schema
        let schema_value = serde_json::from_str::<Value>(schema)
            .map_err(|e| FormatError::Schema(format!("Invalid JSON schema: {}", e)))?;

        // Convert message to serializable format and then to JSON value
        let serializable = self.to_serializable(message);
        let message_value = serde_json::to_value(serializable)
            .map_err(|e| FormatError::Encoding(format!("Failed to convert message to JSON value: {}", e)))?;

        // For basic schema validation, we just check that all required fields in the schema exist in the message
        // A more comprehensive implementation would use a proper JSON Schema validator
        if let Value::Object(schema_obj) = &schema_value {
            if let Some(Value::Object(required)) = schema_obj.get("required") {
                if let Value::Object(message_obj) = &message_value {
                    for (key, value) in required {
                        // Check if the required field exists and is not null
                        if !message_obj.contains_key(key) || message_obj[key].is_null() {
                            return Err(FormatError::Schema(format!("Required field '{}' missing in message", key)));
                        }

                        // If the value in the required object is a boolean and true,
                        // it means the field must exist and not be null
                        if value.is_boolean() && value.as_bool().unwrap() {
                            if !message_obj.contains_key(key) || message_obj[key].is_null() {
                                return Err(FormatError::Schema(format!("Required field '{}' missing in message", key)));
                            }
                        }
                    }
                }
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::format::test_utils::test_message_format_implementation;

    #[tokio::test]
    async fn test_json_hybrid_format_implementation() {
        // Run the generic test suite for the JsonHybridFormat
        test_message_format_implementation(JsonHybridFormat::new()).await;
    }

    #[tokio::test]
    async fn test_json_value_encoding() {
        let format = JsonHybridFormat::with_encoding(BinaryEncoding::JsonValue);

        // Test with JSON object in value
        let json_payload = br#"{"name": "John", "age": 30, "active": true}"#;
        let message = KafkaMessage::new(
            Some(b"user-123".to_vec()),
            Some(json_payload.to_vec()),
            "users".to_string(),
            0,
            100,
        );

        let serialized = format.serialize(&message).await.unwrap();
        let json_str = String::from_utf8(serialized.clone()).unwrap();
        
        println!("Serialized JSON: {}", json_str);
        
        // Parse the outer JSON to inspect the structure
        let parsed: Value = serde_json::from_str(&json_str).unwrap();
        
        // The key should be a string
        assert_eq!(parsed["key"], Value::String("user-123".to_string()));
        
        // The value should be parsed as a JSON object, not an escaped string
        assert_eq!(parsed["value"]["name"], Value::String("John".to_string()));
        assert_eq!(parsed["value"]["age"], Value::Number(30.into()));
        assert_eq!(parsed["value"]["active"], Value::Bool(true));
        
        // Verify round-trip
        let deserialized = format.deserialize(&serialized).await.unwrap();
        
        // Compare messages semantically for JSON values
        assert_eq!(message.key, deserialized.key);
        assert_eq!(message.headers, deserialized.headers);
        assert_eq!(message.topic, deserialized.topic);
        assert_eq!(message.partition, deserialized.partition);
        assert_eq!(message.offset, deserialized.offset);
        assert_eq!(message.timestamp, deserialized.timestamp);
        
        // For the value field, parse both as JSON and compare semantically
        if let (Some(original_value), Some(deserialized_value)) = (&message.value, &deserialized.value) {
            if let (Ok(original_json), Ok(deserialized_json)) = (
                serde_json::from_slice::<Value>(original_value),
                serde_json::from_slice::<Value>(deserialized_value)
            ) {
                assert_eq!(original_json, deserialized_json);
            } else {
                // If not valid JSON, compare bytes directly
                assert_eq!(original_value, deserialized_value);
            }
        } else {
            // If either value is None, they should both be None
            assert_eq!(message.value.is_none(), deserialized.value.is_none());
        }
    }

    #[tokio::test]
    async fn test_json_array_encoding() {
        let format = JsonHybridFormat::with_encoding(BinaryEncoding::JsonValue);

        // Test with JSON array in value
        let json_array = br#"[{"id": 1, "name": "Item 1"}, {"id": 2, "name": "Item 2"}]"#;
        let message = KafkaMessage::new(
            Some(b"items-list".to_vec()),
            Some(json_array.to_vec()),
            "items".to_string(),
            0,
            200,
        );

        let serialized = format.serialize(&message).await.unwrap();
        let json_str = String::from_utf8(serialized.clone()).unwrap();
        
        // Parse the outer JSON to inspect the structure
        let parsed: Value = serde_json::from_str(&json_str).unwrap();
        
        // The value should be parsed as a JSON array
        assert!(parsed["value"].is_array());
        let value_array = parsed["value"].as_array().unwrap();
        assert_eq!(value_array.len(), 2);
        assert_eq!(value_array[0]["id"], Value::Number(1.into()));
        assert_eq!(value_array[1]["name"], Value::String("Item 2".to_string()));
        
        // Verify round-trip
        let deserialized = format.deserialize(&serialized).await.unwrap();
        
        // Compare messages semantically for JSON values
        assert_eq!(message.key, deserialized.key);
        assert_eq!(message.headers, deserialized.headers);
        assert_eq!(message.topic, deserialized.topic);
        assert_eq!(message.partition, deserialized.partition);
        assert_eq!(message.offset, deserialized.offset);
        assert_eq!(message.timestamp, deserialized.timestamp);
        
        // For the value field, parse both as JSON and compare semantically
        if let (Some(original_value), Some(deserialized_value)) = (&message.value, &deserialized.value) {
            if let (Ok(original_json), Ok(deserialized_json)) = (
                serde_json::from_slice::<Value>(original_value),
                serde_json::from_slice::<Value>(deserialized_value)
            ) {
                assert_eq!(original_json, deserialized_json);
            } else {
                // If not valid JSON, compare bytes directly
                assert_eq!(original_value, deserialized_value);
            }
        } else {
            // If either value is None, they should both be None
            assert_eq!(message.value.is_none(), deserialized.value.is_none());
        }
    }

    #[tokio::test]
    async fn test_readable_output_with_json_value() {
        let format = JsonHybridFormat::with_encoding(BinaryEncoding::JsonValue);

        // Create a message with JSON object in value
        let message = KafkaMessage::new(
            Some(b"user-123".to_vec()),
            Some(br#"{"name": "John", "age": 30}"#.to_vec()),
            "users".to_string(),
            0,
            100,
        );

        let serialized = format.serialize(&message).await.unwrap();
        let json_str = String::from_utf8(serialized).unwrap();
        
        // The JSON should contain the key as a readable string
        assert!(json_str.contains("\"user-123\""));
        // The value should be an actual JSON object, not an escaped string
        assert!(json_str.contains("\"name\":\"John\"") || json_str.contains("\"name\": \"John\""));
        assert!(json_str.contains("\"age\":30") || json_str.contains("\"age\": 30"));
        // Should not contain escaped quotes or base64
        assert!(!json_str.contains("\\\""));
        assert!(!json_str.contains("base64:"));
        
        // Should be parseable as JSON
        let parsed: Value = serde_json::from_str(&json_str).unwrap();
        assert!(parsed["value"].is_object());
    }

    #[tokio::test]
    async fn test_binary_encoding_strategies() {
        // Test data: UTF-8 text and binary data
        let utf8_data = b"Hello, World!";
        let binary_data = vec![0xFF, 0xFE, 0xFD, 0x00, 0x01, 0x02];

        // Test Base64 encoding
        let base64_format = JsonHybridFormat::with_encoding(BinaryEncoding::Base64);
        
        let message = KafkaMessage::new(
            Some(utf8_data.to_vec()),
            Some(binary_data.clone()),
            "test-topic".to_string(),
            0,
            100,
        );

        let serialized = base64_format.serialize(&message).await.unwrap();
        let deserialized = base64_format.deserialize(&serialized).await.unwrap();
        assert_eq!(message, deserialized);

        // Test UTF-8 with fallback encoding
        let utf8_fallback_format = JsonHybridFormat::with_encoding(BinaryEncoding::Utf8WithFallback);
        
        let text_message = KafkaMessage::new(
            Some(utf8_data.to_vec()),
            Some(b"UTF-8 text value".to_vec()),
            "test-topic".to_string(),
            0,
            100,
        );

        let serialized = utf8_fallback_format.serialize(&text_message).await.unwrap();
        let deserialized = utf8_fallback_format.deserialize(&serialized).await.unwrap();
        assert_eq!(text_message, deserialized);

        // Test with binary data (should use base64 fallback)
        let binary_message = KafkaMessage::new(
            Some(binary_data.clone()),
            Some(binary_data.clone()),
            "test-topic".to_string(),
            0,
            100,
        );

        let serialized = utf8_fallback_format.serialize(&binary_message).await.unwrap();
        let deserialized = utf8_fallback_format.deserialize(&serialized).await.unwrap();
        assert_eq!(binary_message, deserialized);

        // Test Force UTF-8 encoding
        let force_utf8_format = JsonHybridFormat::with_encoding(BinaryEncoding::ForceUtf8);
        
        let serialized = force_utf8_format.serialize(&text_message).await.unwrap();
        let deserialized = force_utf8_format.deserialize(&serialized).await.unwrap();
        assert_eq!(text_message, deserialized);
    }

    #[tokio::test]
    async fn test_binary_fallback() {
        let format = JsonHybridFormat::with_encoding(BinaryEncoding::Utf8WithFallback);

        // Create a message with binary data
        let binary_data = vec![0xFF, 0xFE, 0xFD, 0x00];
        let message = KafkaMessage::new(
            Some(binary_data.clone()),
            Some(b"text-value".to_vec()),
            "binary-topic".to_string(),
            0,
            100,
        );

        let serialized = format.serialize(&message).await.unwrap();
        let json_str = String::from_utf8(serialized.clone()).unwrap();
        
        // The key should be base64 encoded with prefix
        assert!(json_str.contains("base64:"));
        // The value should be plain text
        assert!(json_str.contains("\"text-value\""));

        // Verify round-trip
        let deserialized = format.deserialize(&serialized).await.unwrap();
        assert_eq!(message, deserialized);
    }

    #[tokio::test]
    async fn test_utf8_detection() {
        let format = JsonHybridFormat::new();

        // Test various UTF-8 and binary combinations
        let test_cases = vec![
            (b"simple text".to_vec(), false), // Should be UTF-8
            (b"emoji \xF0\x9F\x98\x80".to_vec(), false), // UTF-8 emoji
            (vec![0xFF, 0xFE, 0xFD], true), // Binary data
            (b"mixed\xFF\xFEtext".to_vec(), true), // Mixed content
        ];

        for (data, should_be_base64) in test_cases {
            let message = KafkaMessage::new(
                Some(data.clone()),
                Some(data.clone()),
                "test".to_string(),
                0,
                100,
            );

            let serialized = format.serialize(&message).await.unwrap();
            let json_str = String::from_utf8(serialized.clone()).unwrap();

            if should_be_base64 {
                assert!(json_str.contains("base64:"), "Expected base64 encoding for binary data");
            } else {
                assert!(!json_str.contains("base64:"), "Expected UTF-8 encoding for text data");
            }

            // Verify round-trip
            let deserialized = format.deserialize(&serialized).await.unwrap();
            assert_eq!(message, deserialized);
        }
    }

    #[tokio::test]
    async fn test_invalid_json() {
        let format = JsonHybridFormat::new();

        // Test with invalid JSON
        let invalid_json = b"{not valid json}";
        let result = format.deserialize(invalid_json).await;

        // Verify the error
        assert!(result.is_err());
        if let Err(FormatError::InvalidFormat(msg)) = result {
            assert!(msg.contains("Invalid JSON"));
        } else {
            panic!("Expected InvalidFormat error");
        }
    }

    #[tokio::test]
    async fn test_schema_validation() {
        let format = JsonHybridFormat::new();

        // Create a test message
        let message = KafkaMessage::new(
            Some(b"key".to_vec()),
            Some(b"value".to_vec()),
            "topic".to_string(),
            0,
            100,
        );

        // Define a simple schema that requires a "timestamp" field
        let schema = r#"{"required": {"timestamp": true}}"#;

        // Validation should fail because the message doesn't have a timestamp
        let result = format.validate_schema(&message, schema).await;
        assert!(result.is_err());

        // Add a timestamp and try again
        let message_with_timestamp = KafkaMessage {
            timestamp: Some(1640995200000),
            ..message.clone()
        };
        let result = format.validate_schema(&message_with_timestamp, schema).await;
        assert!(result.is_ok());

        // Test with invalid schema
        let invalid_schema = r#"{not a valid schema}"#;
        let result = format.validate_schema(&message, invalid_schema).await;
        assert!(result.is_err());
        if let Err(FormatError::Schema(msg)) = result {
            assert!(msg.contains("Invalid JSON schema"));
        } else {
            panic!("Expected Schema error");
        }
    }

    #[tokio::test]
    async fn test_format_name() {
        let format = JsonHybridFormat::new();
        assert_eq!(format.format_name(), "json-hybrid");
    }

    #[tokio::test]
    async fn test_json_value_fallback_chain() {
        let format = JsonHybridFormat::with_encoding(BinaryEncoding::JsonValue);

        // Test the complete fallback chain
        let test_cases = vec![
            // Valid JSON object -> should parse as JSON
            (br#"{"key": "value"}"#.to_vec(), "JSON object"),
            // Valid JSON array -> should parse as JSON  
            (br#"["item1", "item2"]"#.to_vec(), "JSON array"),
            // Valid JSON primitive -> should parse as JSON
            (br#""just a string""#.to_vec(), "JSON string"),
            // Plain text -> should be UTF-8 string
            (b"plain text".to_vec(), "UTF-8 text"),
            // Binary data -> should be base64
            (vec![0xFF, 0xFE, 0xFD], "binary data"),
        ];

        for (data, description) in test_cases {
            let message = KafkaMessage::new(
                Some(b"test-key".to_vec()),
                Some(data.clone()),
                "test".to_string(),
                0,
                100,
            );

            let serialized = format.serialize(&message).await.unwrap();
            let deserialized = format.deserialize(&serialized).await.unwrap();
            
            // Compare messages semantically for JSON values
            assert_eq!(message.key, deserialized.key, "Key mismatch for {}", description);
            assert_eq!(message.headers, deserialized.headers, "Headers mismatch for {}", description);
            assert_eq!(message.topic, deserialized.topic, "Topic mismatch for {}", description);
            assert_eq!(message.partition, deserialized.partition, "Partition mismatch for {}", description);
            assert_eq!(message.offset, deserialized.offset, "Offset mismatch for {}", description);
            assert_eq!(message.timestamp, deserialized.timestamp, "Timestamp mismatch for {}", description);
            
            // For the value field, compare based on the description
            if let (Some(original_value), Some(deserialized_value)) = (&message.value, &deserialized.value) {
                match description {
                    "JSON object" | "JSON array" => {
                        // For JSON objects and arrays, parse and compare semantically
                        if let (Ok(original_json), Ok(deserialized_json)) = (
                            serde_json::from_slice::<Value>(original_value),
                            serde_json::from_slice::<Value>(deserialized_value)
                        ) {
                            assert_eq!(original_json, deserialized_json, "JSON value mismatch for {}", description);
                        } else {
                            // If parsing fails, compare bytes directly
                            assert_eq!(original_value, deserialized_value, "Value bytes mismatch for {}", description);
                        }
                    },
                    "JSON string" => {
                        // For JSON strings, the original is a quoted string like "just a string"
                        // but the deserialized might be just the content without quotes
                        if let Ok(original_json) = serde_json::from_slice::<Value>(original_value) {
                            if original_json.is_string() {
                                let original_content = original_json.as_str().unwrap();
                                let deserialized_content = std::str::from_utf8(deserialized_value).unwrap_or("");
                                assert_eq!(original_content, deserialized_content, "JSON string content mismatch for {}", description);
                            } else {
                                // If not a string, compare the JSON values
                                if let Ok(deserialized_json) = serde_json::from_slice::<Value>(deserialized_value) {
                                    assert_eq!(original_json, deserialized_json, "JSON value mismatch for {}", description);
                                } else {
                                    // If deserialized is not valid JSON, this is unexpected
                                    panic!("Expected valid JSON for both original and deserialized in JSON string test case");
                                }
                            }
                        } else {
                            // If original is not valid JSON, this is unexpected
                            panic!("Expected valid JSON for original in JSON string test case");
                        }
                    },
                    _ => {
                        // For non-JSON values, compare bytes directly
                        assert_eq!(original_value, deserialized_value, "Value bytes mismatch for {}", description);
                    }
                }
            } else {
                // If either value is None, they should both be None
                assert_eq!(message.value.is_none(), deserialized.value.is_none(), "Value presence mismatch for {}", description);
            }
            
            // Verify the serialized format
            let json_str = String::from_utf8(serialized).unwrap();
            
            // Check for expected encoding patterns based on the description
            match description {
                "JSON object" => assert!(json_str.contains("\"key\"") && json_str.contains("\"value\""), "Expected JSON object structure"),
                "JSON array" => assert!(json_str.contains("[\"item1\"") && json_str.contains("\"item2\"]"), "Expected JSON array structure"),
                "JSON string" => assert!(json_str.contains("\"just a string\""), "Expected JSON string"),
                "UTF-8 text" => assert!(json_str.contains("\"plain text\""), "Expected UTF-8 text"),
                "binary data" => assert!(json_str.contains("base64:"), "Expected base64 encoding for binary data"),
                _ => {}
            }
        }
    }
}