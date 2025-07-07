# kafka-scribe

A high-performance CLI tool for Kafka message capture, analysis, and replay.

## Project Status

🚧 **Currently in Development - MVP Phase** 🚧

This project is in active development. The basic project scaffolding is complete, but core functionality is still being implemented. See the [task list](kafka_scribe_task_list.md) for current progress.

## Overview

**kafka-scribe** will enable developers and operators to:

1. **Store** messages from Kafka topics to files or databases
2. **Analyze** messages using familiar tools (grep, jq, SQL)
3. **Replay** messages back to Kafka with optional transformations

## Architecture

The tool follows a three-stage workflow:
- **Store** → **Analyze** → **Replay**

Built with a modular Rust architecture featuring pluggable storage backends and message format handlers.

## Current Implementation

### ✅ Completed
- Project scaffolding with full module structure
- CLI interface framework with clap
- Core data structures (`KafkaMessage`, error handling, configuration)
- Storage backend trait definitions
- Comprehensive test setup

### 🚧 In Progress
- Command implementations (store, replay, stats)
- Kafka integration (consumer/producer)
- File-based storage backends

### 📋 Planned Features
- Database storage (SQLite, PostgreSQL)
- Message format support (JSON, Avro, Protobuf)
- Interactive replay modes
- JavaScript transformations
- Performance optimizations

## Development

```bash
# Build the project
cargo build

# Run tests
cargo test

# Check CLI help (currently shows placeholder commands)
cargo run -- --help

# View available subcommands
cargo run -- store --help
cargo run -- replay --help
cargo run -- stats --help
```

## Documentation

- [Design Document](docs/design-document.md) - Complete technical specification
- [Architecture Decisions](docs/ARCHITECTURE_DECISIONS.md) - Key design choices
- [Implementation Guide](docs/IMPLEMENTATION_GUIDE.md) - Development guidelines
- [Task List](kafka_scribe_task_list.md) - Current development progress

## Contributing

This project follows the task-driven development approach outlined in the task list. See `kafka_scribe_task_list.md` for current priorities and implementation status.

## License

MIT
