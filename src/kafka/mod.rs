// `consumer` module hosts the rdkafka adapter for the `CoreKafkaConsumer`
// port (ADR-003). Domain types live in `crate::core::store_usecase`.
//
// `producer` will return as a real impl in Task 44 (replay use-case).
pub mod consumer;
