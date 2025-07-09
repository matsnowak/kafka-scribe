use crate::core::errors::FormatResult;
use crate::core::models::KafkaMessage;
use async_trait::async_trait;

/// Trait for message format handlers
///
/// This trait defines the interface for different message format handlers that can be used
/// to serialize and deserialize Kafka messages. Implementations of this trait can handle
/// various formats such as JSON, Avro, Protobuf, Binary, and String.
///
/// # Examples
///
/// ```
/// # use async_trait::async_trait;
/// # use kafka_scribe::core::errors::FormatResult;
/// # use kafka_scribe::core::models::KafkaMessage;
/// # use kafka_scribe::core::format::MessageFormat;
/// #
/// # struct JsonFormat;
/// #
/// # #[async_trait]
/// # impl MessageFormat for JsonFormat {
/// #     async fn serialize(&self, message: &KafkaMessage) -> FormatResult<Vec<u8>> {
/// #         // Serialize the message to JSON
/// #         serde_json::to_vec(message)
/// #             .map_err(|e| crate::core::errors::FormatError::Encoding(format!("Failed to serialize to JSON: {}", e)))
/// #     }
/// #
/// #     async fn deserialize(&self, data: &[u8]) -> FormatResult<KafkaMessage> {
/// #         // Deserialize the message from JSON
/// #         serde_json::from_slice(data)
/// #             .map_err(|e| crate::core::errors::FormatError::Decoding(format!("Failed to deserialize from JSON: {}", e)))
/// #     }
/// #
/// #     fn format_name(&self) -> &'static str {
/// #         "json"
/// #     }
/// # }
/// #
/// # async fn example() -> FormatResult<()> {
/// // Create a JSON format handler
/// let format = JsonFormat;
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
/// // Serialize the message
/// let serialized = format.serialize(&message).await?;
///
/// // Deserialize the message
/// let deserialized = format.deserialize(&serialized).await?;
///
/// assert_eq!(message, deserialized);
/// # Ok(())
/// # }
/// ```
#[async_trait]
pub trait MessageFormat: Send + Sync {
    /// Serialize a Kafka message to bytes
    ///
    /// This method converts a KafkaMessage to a byte array in the format
    /// implemented by this handler.
    ///
    /// # Arguments
    ///
    /// * `message` - The Kafka message to serialize
    ///
    /// # Returns
    ///
    /// A Result containing the serialized message as bytes or a FormatError
    async fn serialize(&self, message: &KafkaMessage) -> FormatResult<Vec<u8>>;

    /// Deserialize bytes to a Kafka message
    ///
    /// This method converts a byte array in the format implemented by this handler
    /// to a KafkaMessage.
    ///
    /// # Arguments
    ///
    /// * `data` - The bytes to deserialize
    ///
    /// # Returns
    ///
    /// A Result containing the deserialized KafkaMessage or a FormatError
    async fn deserialize(&self, data: &[u8]) -> FormatResult<KafkaMessage>;

    /// Get the name of the format
    ///
    /// This method returns a string identifier for the format, such as "json", "avro", etc.
    ///
    /// # Returns
    ///
    /// A static string representing the format name
    fn format_name(&self) -> &'static str;

    /// Check if the format supports schema validation
    ///
    /// This method returns true if the format supports schema validation, such as Avro or Protobuf.
    ///
    /// # Returns
    ///
    /// A boolean indicating whether schema validation is supported
    fn supports_schema(&self) -> bool {
        false
    }

    /// Validate a message against a schema
    ///
    /// This method validates a KafkaMessage against a schema. It's only relevant for formats
    /// that support schemas, such as Avro or Protobuf.
    ///
    /// # Arguments
    ///
    /// * `message` - The Kafka message to validate
    /// * `schema` - The schema to validate against, as a string
    ///
    /// # Returns
    ///
    /// A Result containing () if validation succeeds or a FormatError if validation fails
    async fn validate_schema(&self, _message: &KafkaMessage, _schema: &str) -> FormatResult<()> {
        Ok(())
    }

    /// Batch serialize multiple Kafka messages
    ///
    /// This method provides a default implementation that serializes each message individually.
    /// Implementations can override this method to provide more efficient batch serialization.
    ///
    /// # Arguments
    ///
    /// * `messages` - The Kafka messages to serialize
    ///
    /// # Returns
    ///
    /// A Result containing a vector of serialized messages as bytes or a FormatError
    async fn batch_serialize(&self, messages: &[KafkaMessage]) -> FormatResult<Vec<Vec<u8>>> {
        let mut result = Vec::with_capacity(messages.len());
        for message in messages {
            result.push(self.serialize(message).await?);
        }
        Ok(result)
    }

    /// Batch deserialize multiple byte arrays to Kafka messages
    ///
    /// This method provides a default implementation that deserializes each byte array individually.
    /// Implementations can override this method to provide more efficient batch deserialization.
    ///
    /// # Arguments
    ///
    /// * `data` - The byte arrays to deserialize
    ///
    /// # Returns
    ///
    /// A Result containing a vector of deserialized KafkaMessages or a FormatError
    async fn batch_deserialize(&self, data: &[Vec<u8>]) -> FormatResult<Vec<KafkaMessage>> {
        let mut result = Vec::with_capacity(data.len());
        for bytes in data {
            result.push(self.deserialize(bytes).await?);
        }
        Ok(result)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::errors::FormatError;

    struct MockFormat {
        should_fail: bool,
    }

    #[async_trait]
    impl MessageFormat for MockFormat {
        async fn serialize(&self, message: &KafkaMessage) -> FormatResult<Vec<u8>> {
            if self.should_fail {
                return Err(FormatError::Encoding("Mock serialization failure".to_string()));
            }
            
            // Simple serialization for testing: just use JSON
            serde_json::to_vec(message)
                .map_err(|e| FormatError::Encoding(format!("Failed to serialize: {}", e)))
        }

        async fn deserialize(&self, data: &[u8]) -> FormatResult<KafkaMessage> {
            if self.should_fail {
                return Err(FormatError::Decoding("Mock deserialization failure".to_string()));
            }
            
            // Simple deserialization for testing: just use JSON
            serde_json::from_slice(data)
                .map_err(|e| FormatError::Decoding(format!("Failed to deserialize: {}", e)))
        }

        fn format_name(&self) -> &'static str {
            "mock"
        }
    }

    #[tokio::test]
    async fn test_message_format_success() {
        let format = MockFormat { should_fail: false };
        
        let message = KafkaMessage::new(
            Some(b"test-key".to_vec()),
            Some(b"test-value".to_vec()),
            "test-topic".to_string(),
            0,
            100,
        );
        
        let serialized = format.serialize(&message).await.unwrap();
        let deserialized = format.deserialize(&serialized).await.unwrap();
        
        assert_eq!(message, deserialized);
        assert_eq!(format.format_name(), "mock");
        assert_eq!(format.supports_schema(), false);
    }

    #[tokio::test]
    async fn test_message_format_failure() {
        let format = MockFormat { should_fail: true };
        
        let message = KafkaMessage::new(
            Some(b"test-key".to_vec()),
            Some(b"test-value".to_vec()),
            "test-topic".to_string(),
            0,
            100,
        );
        
        let serialized = format.serialize(&message).await;
        assert!(serialized.is_err());
        
        let dummy_data = vec![1, 2, 3, 4];
        let deserialized = format.deserialize(&dummy_data).await;
        assert!(deserialized.is_err());
    }

    #[tokio::test]
    async fn test_batch_operations() {
        let format = MockFormat { should_fail: false };
        
        let messages = vec![
            KafkaMessage::new(
                Some(b"key1".to_vec()),
                Some(b"value1".to_vec()),
                "topic".to_string(),
                0,
                100,
            ),
            KafkaMessage::new(
                Some(b"key2".to_vec()),
                Some(b"value2".to_vec()),
                "topic".to_string(),
                0,
                101,
            ),
        ];
        
        let serialized = format.batch_serialize(&messages).await.unwrap();
        assert_eq!(serialized.len(), 2);
        
        let deserialized = format.batch_deserialize(&serialized).await.unwrap();
        assert_eq!(deserialized.len(), 2);
        assert_eq!(messages, deserialized);
    }
}