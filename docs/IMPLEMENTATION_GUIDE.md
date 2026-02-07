# Implementation Guide for AI Agents

## AI Engineering Principles 🤖

> **CRITICAL**: Before writing any code, you **MUST** read and understand the [AI Engineering Principles](AI_ENGINEERING_PRINCIPLES.md).
> These principles cover Security, Modularity, Testing, Performance, and Documentation standards.

## Task Selection Priority
1. CRITICAL tasks first
2. Check dependencies are COMPLETED
3. Follow the dependency graph

## Code Quality Standards
- Comprehensive error handling
- Unit tests for all components
- Documentation for public APIs

## Local Development & Sanity Checks 🛠️

These scripts are YOUR source of truth for verifying the environment is working.

### 1. Infrastructure
The Kafka environment is defined in `tests/fixtures/docker-compose.yml`.
Ensure it is running before any integration tests:
```bash
cd tests/fixtures && docker-compose up -d
```

### 2. Generate Data
To populate Kafka with test messages (Sanity Check 1):
```bash
# Generates 2000 messages to topic 'generated-scripted'
./scripts/generate_test_data.sh
```

### 3. Run & Consume
To run the app and consume messages (Sanity Check 2):
```bash
# Consumes from 'generated-scripted' and writes to 'stores/generated-scripted'
./run_local.sh
```

### 4. Cleanup
To reset storage state:
```bash
./scripts/clean-storage.sh
```
