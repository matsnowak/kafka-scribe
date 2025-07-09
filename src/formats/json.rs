//! JSON message format implementation
//!
//! This module provides an implementation of the `MessageFormat` trait for JSON-formatted messages.
//! It uses serde_json for serialization and deserialization.

use crate::core::errors::{FormatError, FormatResult};
use crate::core::format::MessageFormat;
use crate::core::models::KafkaMessage;
use async_trait::async_trait;
use serde_json::{Value, json};

/// JSON message format handler
///
/// This struct implements the `MessageFormat` trait for JSON-formatted messages.
/// It can serialize and deserialize Kafka messages to and from JSON.
///
/// # Examples
///
/// ```
/// # use kafka_scribe::core::format::MessageFormat;
/// # use kafka_scribe::core::models::KafkaMessage;
/// # use kafka_scribe::formats::JsonFormat;
/// #
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// // Create a JSON format handler
/// let format = JsonFormat::new();
///
/// // Create a Kafka message
/// let message = KafkaMessage::new(
///     Some(b"key".to_vec()),
///     Some(b"value".to_vec()),
///     "topic".to_string(),
///     0,
///     100,
/// );
///
/// // Serialize the message to JSON
/// let serialized = format.serialize(&message).await?;
///
/// // Deserialize the message from JSON
/// let deserialized = format.deserialize(&serialized).await?;
///
/// assert_eq!(message, deserialized);
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone, Default)]
pub struct JsonFormat {
    // Configuration options could be added here in the future
}

impl JsonFormat {
    /// Create a new JSON format handler
    ///
    /// # Returns
    ///
    /// A new JsonFormat instance
    pub fn new() -> Self {
        Self {}
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
impl MessageFormat for JsonFormat {
    async fn serialize(&self, message: &KafkaMessage) -> FormatResult<Vec<u8>> {
        serde_json::to_vec(message)
            .map_err(|e| FormatError::Encoding(format!("Failed to serialize to JSON: {}", e)))
    }

    async fn deserialize(&self, data: &[u8]) -> FormatResult<KafkaMessage> {
        // First validate that the data is valid JSON
        if let Err(e) = self.validate_json(data) {
            return Err(e);
        }

        // Then attempt to deserialize into a KafkaMessage
        serde_json::from_slice(data)
            .map_err(|e| FormatError::Decoding(format!("Failed to deserialize from JSON: {}", e)))
    }

    fn format_name(&self) -> &'static str {
        "json"
    }

    fn supports_schema(&self) -> bool {
        true
    }

    async fn validate_schema(&self, message: &KafkaMessage, schema: &str) -> FormatResult<()> {
        // Parse the schema
        let schema_value = serde_json::from_str::<Value>(schema)
            .map_err(|e| FormatError::Schema(format!("Invalid JSON schema: {}", e)))?;

        // Serialize the message to a JSON value
        let message_value = serde_json::to_value(message)
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
    async fn test_json_format_implementation() {
        // Run the generic test suite for the JsonFormat
        test_message_format_implementation(JsonFormat::new()).await;
    }

    #[tokio::test]
    async fn test_json_format_specific() {
        let format = JsonFormat::new();

        // Test with a message containing various data types
        let message = KafkaMessage::new(
            Some(b"binary-key".to_vec()),
            Some(json!({
                "string_field": "value",
                "number_field": 42,
                "boolean_field": true,
                "array_field": [1, 2, 3],
                "object_field": {
                    "nested": "value"
                }
            }).to_string().as_bytes().to_vec()),
            "test-topic".to_string(),
            0,
            100,
        )
        .with_header("content-type", "application/json")
        .with_timestamp(1640995200000);

        // Serialize and deserialize
        let serialized = format.serialize(&message).await.unwrap();
        let deserialized = format.deserialize(&serialized).await.unwrap();

        // Verify the result matches the original
        assert_eq!(message, deserialized);
    }

    #[tokio::test]
    async fn test_invalid_json() {
        let format = JsonFormat::new();

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
        let format = JsonFormat::new();

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
        let message_with_timestamp = message.clone().with_timestamp(1640995200000);
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
}
