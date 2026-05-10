// `consumer` module removed in Task 38 — first-gen `KafkaConsumer` was
// superseded by `CoreKafkaConsumer` trait + `RdKafkaConsumer` adapter
// in `src/core/store_usecase.rs`. Task 39 will lift the adapter back
// into this module under a clean API.
pub mod producer;

#[cfg(test)]
pub mod mock;
