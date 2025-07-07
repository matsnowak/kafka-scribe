`````````# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

kafka-scribe is a high-performance CLI tool for Kafka message capture, analysis, and replay. It follows a three-stage workflow:

1. **Store**: Capture messages from Kafka topics to files or databases
2. **Analyze**: Use standard tools (grep, jq, SQL) to investigate stored messages
3. **Replay**: Send messages back to Kafka with optional transformations

## Development Commands

### Build & Test
```bash
# Build the project
cargo build

# Run tests
cargo test

# Build for release
cargo build --release

# Run with debug output
RUST_LOG=debug cargo run -- --help
```

### Code Quality
```bash
# Format code
cargo fmt

# Run linter
cargo clippy

# Run clippy with all targets
cargo clippy --all-targets --all-features
```

## Architecture

The project follows a modular Rust architecture with these key components:

### Core Traits
- `StorageBackend`: Trait for different storage systems (files, databases)
- `MessageFormat`: Trait for message serialization formats (JSON, Avro, Protobuf, etc.)

### Module Structure
- `src/cli/`: Command-line interface and subcommands
- `src/core/`: Core data structures (`KafkaMessage`, errors, config)
- `src/kafka/`: Kafka consumer/producer implementations
- `src/storage/`: Storage backend implementations
- `src/utils/`: Logging, progress reporting, validation utilities

### Key Data Structures
- `KafkaMessage`: Represents a Kafka message with key, value, headers, partition, offset, timestamp
- `StorageBackend`: Abstract interface for storage systems
- `MessageFormat`: Abstract interface for message serialization

## Implementation Guidelines

### Current Status
The project is in early development with only basic scaffolding. The main implementation follows the detailed task list in `kafka_scribe_task_list.md`.

### Task Priority
1. **CRITICAL**: Basic project structure, core data types, file storage
2. **HIGH**: Kafka integration, basic commands, JSON format support
3. **MEDIUM**: Database storage, advanced features, analysis tools
4. **LOW**: Schema registry, object storage, CloudEvents support

### Architecture Decisions
- File-based JSON storage for MVP (ADR-001)
- Leverage external tools (grep, jq, SQL) for analysis rather than building custom analysis commands (ADR-002)
- Stateless operation with all parameters passed as CLI arguments
- Pluggable architecture for storage backends and message formats

### Testing Strategy
- Unit tests for all core components
- Integration tests using mock Kafka servers
- Property-based testing for complex transformations
- Test coverage target: >80%

## Key Files

### Documentation
- `README.md`: Project overview and quick start
- `docs/design-document.md`: Comprehensive design specification
- `docs/IMPLEMENTATION_GUIDE.md`: Implementation guidance for AI agents
- `kafka_scribe_task_list.md`: Detailed task breakdown with priorities and dependencies

### Configuration
- `Cargo.toml`: Rust project configuration with minimal dependencies
- `src/main.rs`: Entry point with basic "Hello, world!" placeholder

## MVP Scope

The initial release focuses on:
- File-based storage with JSON format
- Basic Kafka consumer/producer integration
- Core CLI commands: `store`, `replay`, `stats`
- Essential filtering and selection options
- Comprehensive error handling and testing

Database storage, advanced transformations, and schema registry integration are deferred to Phase 2.

## AI Agent Coordination

### Task Selection Protocol
1. **Check Dependencies**: Only select tasks marked as TODO with all dependencies COMPLETED
2. **Follow Priority Order**: CRITICAL → HIGH → MEDIUM → LOW
3. **Update Status**: Change task status to IN_PROGRESS when starting work
4. **Single Agent Rule**: Only one agent should work on a task at a time

### Status Updates Required
- **Daily**: Current task, progress percentage, blockers encountered
- **On Completion**: Update task status to COMPLETED, provide implementation summary
- **On Blocking**: Update status to BLOCKED, describe the specific blocker

### Handoff Procedures
- **Code Review**: All implementations require review before marking COMPLETED
- **Integration Testing**: Verify new code works with existing components
- **Documentation**: Update relevant docs when adding new features


## Quality Gates

### Definition of Done
Before marking any task as COMPLETED:
- [ ] All acceptance criteria met
- [ ] Unit tests written and passing
- [ ] Code follows Rust best practices
- [ ] Documentation updated if needed
- [ ] Integration with existing code verified
- [ ] No breaking changes to existing functionality

### Code Quality Standards
- Use comprehensive error handling with `thiserror`/`anyhow`
- Write idiomatic, safe Rust code
- Document all public APIs with rustdoc comments
- Follow the Rust API guidelines
- Maintain >80% test coverage for core modules


## Communication Protocols

### Escalation Triggers
Immediately escalate when:
- Task acceptance criteria are unclear or contradictory
- Technical blockers cannot be resolved within 4 hours
- Architecture decisions need clarification
- Scope creep is identified in task requirements

### Context Preservation
- **Reference Key Files**: Always check design-document.md and task list before starting
- **Maintain Decision History**: Document important implementation decisions
- **Update Task Notes**: Add relevant information to task notes section

### Current Status (Updated)
The project is in **Phase 1: Foundation** with these immediate priorities:

**Week 1-2 Focus**:
- Task #1: Project scaffolding (CRITICAL)
- Task #3: CLI argument parsing (CRITICAL)  
- Task #4: KafkaMessage data structure (CRITICAL)
- Task #5: StorageBackend trait (CRITICAL)

**Next Steps**:
- Task #7: File-based storage implementation
- Task #15: Kafka consumer integration
- Task #17: Store command implementation


`````````