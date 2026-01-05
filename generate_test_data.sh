#!/usr/bin/env bash
# generate_test_data.sh
#
# Creates a Kafka topic and produces sample messages for local testing.
# - Topic: generated-scripted (default)
# - Partitions: 10 (default)
# - Messages: 2000 (default)
# - Keys: 50 rotating keys so each key repeats across partitions
# - Each message includes a few Kafka headers and a JSON payload
#
# Requirements (one of):
# - kcat (preferred) OR
# - Apache Kafka's kafka-console-producer.sh with parse.key + parse.headers support
# Additionally for topic creation:
# - Apache Kafka's kafka-topics.sh
#
# You can override defaults using environment variables, e.g.:
#   TOPIC=my-topic PARTITIONS=10 NUM_MESSAGES=500 BROKER=localhost:29092 ./generate_test_data.sh

set -euo pipefail

# Defaults
BROKER=${BROKER:-localhost:29092}
TOPIC=${TOPIC:-generated-scripted}
PARTITIONS=${PARTITIONS:-10}
NUM_MESSAGES=${NUM_MESSAGES:-2000}
NUM_KEYS=${NUM_KEYS:-50}  # must be > 10 per requirements
REPLICATION_FACTOR=${REPLICATION_FACTOR:-1}

# Utilities: find Kafka CLI tools if available
have() { command -v "$1" >/dev/null 2>&1; }

KT_TOPICS=""
if have kafka-topics.sh; then
  KT_TOPICS="kafka-topics.sh"
elif have kafka-topics; then
  KT_TOPICS="kafka-topics"
fi

KCAT=""
if have kcat; then
  KCAT="kcat"
elif have kafkacat; then
  KCAT="kafkacat"
fi

KAFKA_CONSOLE_PRODUCER=""
if have kafka-console-producer.sh; then
  KAFKA_CONSOLE_PRODUCER="kafka-console-producer.sh"
elif have kafka-console-producer; then
  KAFKA_CONSOLE_PRODUCER="kafka-console-producer"
fi

log() { echo "[generate-test-data] $*"; }
err() { echo "[generate-test-data][ERROR] $*" >&2; }

if [[ -z "$KT_TOPICS" ]]; then
  err "kafka-topics(.sh) not found in PATH. Please install Apache Kafka CLI or add it to PATH."
  exit 1
fi

# Create topic if it doesn't exist
create_topic() {
  if $KT_TOPICS --bootstrap-server "$BROKER" --describe --topic "$TOPIC" >/dev/null 2>&1; then
    log "Topic '$TOPIC' already exists on $BROKER"
  else
    log "Creating topic '$TOPIC' with $PARTITIONS partitions, RF=$REPLICATION_FACTOR"
    $KT_TOPICS \
      --bootstrap-server "$BROKER" \
      --create \
      --topic "$TOPIC" \
      --partitions "$PARTITIONS" \
      --replication-factor "$REPLICATION_FACTOR"
    log "Topic '$TOPIC' created."
  fi
}

# Generate one JSON payload line for a given sequence and key
json_payload() {
  local seq="$1"; local key="$2";
  local ts_ms
  # GNU date supports %3N, fallback to seconds if unavailable
  ts_ms=$(date +%s%3N 2>/dev/null || echo "$(date +%s)000")
  local rand
  rand=$(( (RANDOM % 100000) ))
  cat <<EOF
{"seq":$seq,"key":"$key","message":"hello from kafka-scribe test data","random":$rand,"timestamp_ms":"$ts_ms"}
EOF
}

produce_with_kcat() {
  log "Producing $NUM_MESSAGES messages to '$TOPIC' via kcat on $BROKER"
  # kcat: -H applies to all messages; we include static headers and include seq in JSON
  # -K '|' sets key|value separator; -k to provide key from input
  local sep='|'
  local i key value
  {
    for ((i=0; i<NUM_MESSAGES; i++)); do
      key="key-$(( i % NUM_KEYS ))"
      value=$(json_payload "$i" "$key")
      printf '%s%s%s\n' "$key" "$sep" "$value"
    done
  } | $KCAT \
        -P \
        -b "$BROKER" \
        -t "$TOPIC" \
        -K "$sep" \
        -H "source=generate_test_data.sh" \
        -H "content-type=application/json"
}

produce_with_console_producer() {
  if [[ -z "$KAFKA_CONSOLE_PRODUCER" ]]; then
    return 1
  fi
  log "Producing $NUM_MESSAGES messages to '$TOPIC' via kafka-console-producer on $BROKER"
  # kafka-console-producer supports key and headers per message when configured:
  # Format: key<TAB>header1=val,header2=val<TAB>value
  # Properties: parse.key=true, key.separator=\t, parse.headers=true, headers.separator=\t
  local key value headers
  {
    for ((i=0; i<NUM_MESSAGES; i++)); do
      key="key-$(( i % NUM_KEYS ))"
      value=$(json_payload "$i" "$key")
      headers="source=generate_test_data.sh,content-type=application/json"
      printf '%s\t%s\t%s\n' "$key" "$headers" "$value"
    done
  } | "$KAFKA_CONSOLE_PRODUCER" \
        --bootstrap-server "$BROKER" \
        --topic "$TOPIC" \
        --property parse.key=true \
        --property key.separator=$'\t' \
        --property parse.headers=true \
        --property headers.separator=$'\t'
}

main() {
  if (( NUM_KEYS <= 10 )); then
    err "NUM_KEYS must be > 10 to satisfy requirements (current: $NUM_KEYS)"
    exit 1
  fi
  if (( PARTITIONS != 10 )); then
    log "Warning: PARTITIONS is $PARTITIONS, requirement asks for 10. Overriding to 10."
    PARTITIONS=10
  fi

  create_topic

  if [[ -n "$KCAT" ]]; then
    produce_with_kcat && { log "Done."; return 0; }
  fi

  if produce_with_console_producer; then
    log "Done."
    return 0
  fi

  err "No suitable producer found. Install 'kcat' or ensure 'kafka-console-producer(.sh)' supports parse.headers."
  return 2
}

main "$@"
