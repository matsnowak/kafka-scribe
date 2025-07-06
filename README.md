# kafka-scribe

A high-performance CLI tool for Kafka message capture, analysis, and replay.

## Overview

**kafka-scribe** enables developers and operators to:

1. **Store** messages from Kafka topics to files or databases
2. **Analyze** messages using familiar tools (grep, jq, SQL)
3. **Replay** messages back to Kafka with optional transformations

## Key Features

- **Flexible Storage**: Store messages in files or databases
- **Powerful Filtering**: Select messages by offset, timestamp, key pattern, or headers
- **Interactive Replay**: Review and modify messages before replaying
- **Transformation Support**: Apply JavaScript transformations to messages
- **Performance Optimized**: Parallel processing and configurable batching

## Quick Start

```bash
# Install
cargo install kafka-scribe

# Store 1000 messages from a topic to a directory
kscribe store orders --bootstrap-servers kafka:9092 --count 1000 --to-dir ./orders_data

# Analyze with standard tools
grep -r "error" ./orders_data/
cat ./orders_data/*.json | jq -r '.payload.product_id' | sort | uniq -c

# Replay messages to another topic
kscribe replay --from-dir ./orders_data/ --to-topic orders-test --bootstrap-servers kafka:9092
```

## Documentation

For detailed documentation, see the [Design Document](docs/design-document.md).

## License

MIT
