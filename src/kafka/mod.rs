// `consumer` module hosts the rdkafka adapter for the `CoreKafkaConsumer`
// port (ADR-003). Domain types live in `crate::core::store_usecase`.
pub mod consumer;
pub mod producer;

#[cfg(test)]
pub mod mock;
