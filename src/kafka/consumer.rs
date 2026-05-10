//! Rdkafka adapter for the `CoreKafkaConsumer` port.
//!
//! ADR-003: this is the only place that imports `rdkafka::*`. The domain
//! types (`TopicMetadata`, `TopicPartitionsAssignment`, `DomainOffset`)
//! are owned by `crate::core::store_usecase`; this module provides the
//! conversions and the live `RdKafkaConsumer` implementation.

use std::sync::Mutex;
use std::time::{Duration, Instant};

use anyhow::Result;
use async_trait::async_trait;
use rdkafka::client::ClientContext;
use rdkafka::config::{ClientConfig, RDKafkaLogLevel};
use rdkafka::consumer::{BaseConsumer, Consumer, ConsumerContext, Rebalance};
use rdkafka::error::KafkaResult;
use rdkafka::message::{Headers, Message, OwnedMessage};
use rdkafka::topic_partition_list::{Offset, TopicPartitionList};
use rdkafka::util::Timeout;
use tracing::{debug, warn};

use crate::core::models::KafkaMessage;
use crate::core::store_usecase::{
    CoreKafkaConsumer, DomainOffset, PartitionMetadata, PartitionOffset, TopicMetadata,
    TopicPartitionsAssignment,
};

// ---------------------------------------------------------------------------
// Public constructor (used by `core::store_usecase::store`)
// ---------------------------------------------------------------------------

pub fn create_rdkafka_consumer(
    bootstrap_servers: String,
    topic: String,
    group_id: String,
) -> Result<RdKafkaConsumer> {
    RdKafkaConsumer::new(bootstrap_servers, topic, group_id)
}

// ---------------------------------------------------------------------------
// Adapter
// ---------------------------------------------------------------------------

pub struct RdKafkaConsumer {
    topic: String,
    inner_consumer: Mutex<Option<LoggedConsumer>>,
}

impl RdKafkaConsumer {
    fn new(bootstrap_servers: String, topic: String, group_id: String) -> Result<Self> {
        let context = KafkaConsumerContext;
        let mut client_config = ClientConfig::new();

        client_config
            .set("bootstrap.servers", &bootstrap_servers)
            .set("group.id", &group_id)
            .set("enable.auto.commit", "false")
            .set("auto.offset.reset", "earliest")
            .set("enable.partition.eof", "false")
            .set("session.timeout.ms", "6000")
            .set("max.poll.interval.ms", "300000")
            .set_log_level(RDKafkaLogLevel::Debug);

        let consumer: LoggedConsumer = client_config.create_with_context(context)?;
        Ok(Self {
            topic,
            inner_consumer: Mutex::new(Some(consumer)),
        })
    }

    fn with_consumer<T>(&self, f: impl FnOnce(&LoggedConsumer) -> Result<T>) -> Result<T> {
        let guard = self
            .inner_consumer
            .lock()
            .map_err(|_| anyhow::anyhow!("Kafka consumer lock poisoned"))?;
        let consumer = guard
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Kafka consumer already stopped"))?;
        f(consumer)
    }

    fn take_consumer(&self) -> Result<Option<LoggedConsumer>> {
        let mut guard = self
            .inner_consumer
            .lock()
            .map_err(|_| anyhow::anyhow!("Kafka consumer lock poisoned"))?;
        Ok(guard.take())
    }
}

#[async_trait]
impl CoreKafkaConsumer for RdKafkaConsumer {
    async fn initialize(&self) -> Result<()> {
        let metadata = self.with_consumer(|consumer| {
            consumer
                .fetch_metadata(Some(&self.topic), Timeout::After(Duration::from_secs(10)))
                .map_err(|e| anyhow::anyhow!("Error fetching metadata: {}", e))
        })?;

        if metadata.topics().is_empty() || metadata.topics().first().unwrap().name() != self.topic {
            return Err(anyhow::anyhow!("Topic '{}' not found", self.topic));
        }
        Ok(())
    }

    async fn fetch_metadata(&self) -> Result<TopicMetadata> {
        let metadata = self.with_consumer(|consumer| {
            consumer
                .fetch_metadata(Some(&self.topic), Timeout::After(Duration::from_secs(10)))
                .map_err(|e| anyhow::anyhow!("Error fetching metadata: {}", e))
        })?;
        let topic_meta = metadata
            .topics()
            .iter()
            .find(|t| t.name() == self.topic)
            .or_else(|| metadata.topics().first())
            .ok_or_else(|| anyhow::anyhow!("Topic '{}' not found", self.topic))?;

        if topic_meta.name() != self.topic {
            return Err(anyhow::anyhow!("Topic '{}' not found", self.topic));
        }
        if topic_meta.partitions().is_empty() {
            return Err(anyhow::anyhow!("Topic '{}' has no partitions", self.topic));
        }

        let partitions = topic_meta
            .partitions()
            .iter()
            .map(|p| PartitionMetadata {
                id: p.id(),
                leader: p.leader(),
                replicas: p.replicas().to_vec(),
                isr: p.isr().to_vec(),
            })
            .collect();

        Ok(TopicMetadata {
            name: topic_meta.name().to_string(),
            partitions,
        })
    }

    async fn fetch_offset_positions(&self) -> Result<TopicPartitionsAssignment> {
        self.with_consumer(|consumer| {
            consumer
                .assignment()
                .map(tpl_from_rdkafka)
                .map_err(|e| anyhow::anyhow!("Error fetching offset positions: {}", e))
        })
    }

    async fn assign(&self, tpl: TopicPartitionsAssignment) -> Result<()> {
        let rdkafka_tpl = tpl_to_rdkafka(&tpl)?;
        self.with_consumer(|consumer| {
            consumer
                .assign(&rdkafka_tpl)
                .map_err(|e| anyhow::anyhow!("Error assigning partitions: {}", e))
        })
    }

    async fn recv_next(&self, timeout_ms: u64) -> Result<Option<KafkaMessage>> {
        let timeout = Duration::from_millis(timeout_ms);
        let owned = tokio::task::block_in_place(|| {
            self.with_consumer(|consumer| match consumer.poll(timeout) {
                None => Ok(None),
                Some(Err(e)) => Err(anyhow::anyhow!("Kafka receive error: {}", e)),
                Some(Ok(bmsg)) => Ok(Some(bmsg.detach())),
            })
        })?;
        Ok(owned.map(|msg| convert_to_kafka_message(&msg)))
    }

    async fn stop(&self) -> Result<()> {
        let consumer = self.take_consumer()?;
        if let Some(consumer) = consumer {
            if let Err(e) = consumer.unassign() {
                warn!("Kafka consumer unassign error: {}", e);
            }
            consumer.unsubscribe();

            // Close cleanly and poll until drained.
            use rdkafka::bindings as rdkafka_sys;
            use rdkafka::bindings::rd_kafka_resp_err_t;
            let native_ptr = consumer.client().native_ptr();
            let close_err = unsafe { rdkafka_sys::rd_kafka_consumer_close(native_ptr) };
            if close_err != rd_kafka_resp_err_t::RD_KAFKA_RESP_ERR_NO_ERROR {
                warn!("Kafka consumer close error: {:?}", close_err);
            }

            let close_deadline = Instant::now() + Duration::from_secs(5);
            while Instant::now() < close_deadline {
                let msg_ptr = unsafe { rdkafka_sys::rd_kafka_consumer_poll(native_ptr, 100) };
                if !msg_ptr.is_null() {
                    unsafe { rdkafka_sys::rd_kafka_message_destroy(msg_ptr) };
                }
                let closed = unsafe { rdkafka_sys::rd_kafka_consumer_closed(native_ptr) == 1 };
                if closed {
                    break;
                }
            }

            let closed = unsafe { rdkafka_sys::rd_kafka_consumer_closed(native_ptr) == 1 };
            if !closed {
                warn!("Kafka consumer did not close within timeout");
            }

            // Leak the consumer handle to avoid hanging in rd_kafka_destroy.
            std::mem::forget(consumer);
        }
        Ok(())
    }

    async fn offsets_for_times(
        &self,
        tpl: TopicPartitionsAssignment,
        timeout_ms: u64,
    ) -> Result<TopicPartitionsAssignment> {
        // For `offsets_for_times` rdkafka expects the per-partition `offset`
        // to actually carry a millisecond timestamp. `tpl_to_rdkafka` builds
        // that mapping verbatim.
        let rdkafka_tpl = tpl_to_rdkafka(&tpl)?;
        let result = self.with_consumer(|consumer| {
            consumer
                .offsets_for_times(rdkafka_tpl, Timeout::After(Duration::from_millis(timeout_ms)))
                .map_err(|e| anyhow::anyhow!("Error fetching offsets for times: {}", e))
        })?;
        Ok(tpl_from_rdkafka(result))
    }
}

// ---------------------------------------------------------------------------
// Conversions
// ---------------------------------------------------------------------------

fn tpl_to_rdkafka(tpl: &TopicPartitionsAssignment) -> Result<TopicPartitionList> {
    let mut out = TopicPartitionList::new();
    for entry in &tpl.entries {
        let offset = match entry.offset {
            DomainOffset::Beginning => Offset::Beginning,
            DomainOffset::End => Offset::End,
            DomainOffset::Offset(n) => Offset::Offset(n),
        };
        out.add_partition_offset(&entry.topic, entry.partition, offset)
            .map_err(|e| {
                anyhow::anyhow!(
                    "Failed to add partition offset (topic={}, partition={}): {}",
                    entry.topic,
                    entry.partition,
                    e
                )
            })?;
    }
    Ok(out)
}

fn tpl_from_rdkafka(tpl: TopicPartitionList) -> TopicPartitionsAssignment {
    let entries = tpl
        .elements()
        .into_iter()
        .map(|p| PartitionOffset {
            topic: p.topic().to_string(),
            partition: p.partition(),
            offset: match p.offset() {
                Offset::Beginning => DomainOffset::Beginning,
                Offset::End => DomainOffset::End,
                Offset::Offset(n) => DomainOffset::Offset(n),
                Offset::Invalid => DomainOffset::Offset(-1001),
                Offset::Stored => DomainOffset::Offset(-1000),
                Offset::OffsetTail(n) => DomainOffset::Offset(-2000 - n),
            },
        })
        .collect();
    TopicPartitionsAssignment { entries }
}

fn convert_to_kafka_message(message: &OwnedMessage) -> KafkaMessage {
    let key = message.key().map(|k| k.to_vec());
    let payload = message.payload().map(|p| p.to_vec());
    let topic = message.topic().to_string();
    let partition = message.partition();
    let offset = message.offset();
    let timestamp = message.timestamp().to_millis();

    let mut kafka_message = KafkaMessage::new(key, payload, topic, partition, offset);

    if let Some(ts) = timestamp {
        kafka_message = kafka_message.with_timestamp(ts);
    }

    if let Some(headers) = message.headers() {
        for i in 0..headers.count() {
            let header = headers.get(i);
            if let Some(value_bytes) = header.value {
                if let Ok(value_str) = std::str::from_utf8(value_bytes) {
                    kafka_message = kafka_message.with_header(header.key, value_str);
                }
            }
        }
    }

    kafka_message
}

// ---------------------------------------------------------------------------
// rdkafka client context
// ---------------------------------------------------------------------------

struct KafkaConsumerContext;

impl ClientContext for KafkaConsumerContext {}

impl ConsumerContext for KafkaConsumerContext {
    fn pre_rebalance(&self, rebalance: &Rebalance) {
        debug!("Pre-rebalance: {:?}", rebalance);
    }

    fn post_rebalance(&self, rebalance: &Rebalance) {
        debug!("Post-rebalance: {:?}", rebalance);
    }

    fn commit_callback(&self, result: KafkaResult<()>, _offsets: &TopicPartitionList) {
        match result {
            Ok(_) => debug!("Offsets committed successfully"),
            Err(e) => warn!("Error committing offsets: {:?}", e),
        }
    }
}

type LoggedConsumer = BaseConsumer<KafkaConsumerContext>;
