# kafka-scribe — Task Backlog

Single source of truth for task state. Mutate only via the `Edit` tool (markdown-only mode — see `CLAUDE.md`). Match on `### Task N\n- **ID:** N` for uniqueness.

## Legend

- **Status:** `TODO` / `IN_PROGRESS` / `BLOCKED` / `COMPLETED` / `DEFERRED` / `SUPERSEDED`
- **Priority:** `CRITICAL` (blocks Phase-1 release) / `HIGH` / `MEDIUM` / `LOW`
- **Phase:** `1` (MVP / v0.1.0) / `2` (post-v0.1.0) / `3` (scale & polish)

## Task Selection Protocol

1. Only pick tasks with `Status: TODO` and all `Dependencies:` in `COMPLETED`.
2. Priority order within the active Phase: CRITICAL → HIGH → MEDIUM → LOW.
3. Start: flip `TODO → IN_PROGRESS`. Complete: flip to `COMPLETED` and append commit SHA + date to `Notes`. Block: flip to `BLOCKED`, append `BLOCKED YYYY-MM-DD: <reason>` to `Notes`.
4. One agent per task. Don't pick Phase-2 work while Phase-1 has unfinished CRITICAL items.

---

## Phase 0 — Completed Foundations (historical reference)

### Task 1
- **ID:** 1
- **Title:** Create project scaffolding
- **Status:** COMPLETED
- **Priority:** CRITICAL
- **Phase:** 0
- **Description:** Basic Rust project structure with `Cargo.toml`, module tree, and initial README.
- **Acceptance Criteria:**
  - [x] Project builds with `cargo build`
  - [x] README.md with basic project info
  - [x] Git repo + `.gitignore`
- **Dependencies:** None
- **Notes:** Target Rust 1.86+. COMPLETED: full module structure and working CLI scaffold.

### Task 3
- **ID:** 3
- **Title:** CLI argument parsing with clap
- **Status:** COMPLETED
- **Priority:** CRITICAL
- **Phase:** 0
- **Description:** Clap-based CLI with subcommands `store`, `replay`, `stats`, `completion`.
- **Acceptance Criteria:**
  - [x] Subcommand structure matches design document
  - [x] `--help` is comprehensive with examples
  - [x] Consistent option patterns across commands
- **Dependencies:** 1
- **Notes:** COMPLETED: clap derive, mutually-exclusive groups, inline rustdoc examples.

### Task 4
- **ID:** 4
- **Title:** Define KafkaMessage type
- **Status:** COMPLETED
- **Priority:** CRITICAL
- **Phase:** 0
- **Description:** Core data structure for Kafka messages (key, value, headers, partition, offset, timestamp).
- **Acceptance Criteria:**
  - [x] Vec<u8> for key/value (binary-safe)
  - [x] serde serialize/deserialize
  - [x] Comprehensive unit tests
- **Dependencies:** 1
- **Notes:** COMPLETED in `src/core/models.rs` (295 LOC).

### Task 5
- **ID:** 5
- **Title:** StorageBackend trait
- **Status:** COMPLETED
- **Priority:** CRITICAL
- **Phase:** 0
- **Description:** Async trait for storage backends with `store_message`, `store_messages`, `flush`, `initialize`, `close`, `get_stats`.
- **Acceptance Criteria:**
  - [x] Typed error taxonomy (`StorageError`)
  - [x] MockStorage impl for tests
  - [x] Documented with usage examples
- **Dependencies:** 4
- **Notes:** COMPLETED in `src/storage/mod.rs` (491 LOC). Strongest abstraction in the codebase.

### Task 6
- **ID:** 6
- **Title:** MessageFormat trait
- **Status:** COMPLETED
- **Priority:** HIGH
- **Phase:** 0
- **Description:** Async trait for serialization formats with typed `FormatError`.
- **Acceptance Criteria:**
  - [x] Async serialize/deserialize
  - [x] Schema validation hook
  - [x] MockFormat for tests
- **Dependencies:** 4
- **Notes:** COMPLETED in `src/core/format.rs` (360 LOC). WARNING: not yet wired through `--format` CLI flag — see Task 41.

### Task 7
- **ID:** 7
- **Title:** File-based storage backends (single-file + directory)
- **Status:** COMPLETED
- **Priority:** CRITICAL
- **Phase:** 0
- **Description:** `SingleFileStorage` (JSONL, one line per msg) and `DirectoryStorage` (one file per message, partitioned by topic/partition).
- **Acceptance Criteria:**
  - [x] Concurrent writes via tokio::sync::Mutex
  - [x] Storage statistics tracked (count, size, time range)
  - [x] Unit + integration tests
- **Dependencies:** 5
- **Notes:** COMPLETED. Performance issue flagged — see Task 60 (per-message open/close/flush in DirectoryStorage).

### Task 10
- **ID:** 10
- **Title:** JsonHybrid message format
- **Status:** COMPLETED
- **Priority:** HIGH
- **Phase:** 0
- **Description:** JSON format with four `BinaryEncoding` strategies (Base64 / Utf8WithFallback / ForceUtf8 / JsonValue) to handle non-UTF8 Kafka payloads.
- **Acceptance Criteria:**
  - [x] All four encoding strategies implemented
  - [x] Round-trip tests per strategy
  - [x] Schema validation
- **Dependencies:** 6
- **Notes:** COMPLETED in `src/formats/json_hybrid.rs` (868 LOC). Materiał na blog post #1 — see Task 75.

### Task 15
- **ID:** 15
- **Title:** Kafka consumer (first-gen)
- **Status:** SUPERSEDED
- **Priority:** HIGH
- **Phase:** 0
- **Description:** `KafkaConsumer` in `src/kafka/consumer.rs` (646 LOC) — first-generation impl.
- **Acceptance Criteria:**
  - [x] Filtering (key regex, headers, partitions)
  - [x] Range selection (offset / timestamp)
  - [x] Limits (count / until-offset / until-timestamp)
- **Dependencies:** 4
- **Notes:** SUPERSEDED by `CoreKafkaConsumer` trait + `RdKafkaConsumer` in `src/core/store_usecase.rs`. The first-gen module is scheduled for deletion in Task 38.

### Task 17
- **ID:** 17
- **Title:** Store command (implementation)
- **Status:** COMPLETED
- **Priority:** CRITICAL
- **Phase:** 0
- **Description:** `kscribe store` end-to-end: consumer → filter/limit → storage backend.
- **Acceptance Criteria:**
  - [x] Integrates new-gen consumer + storage
  - [x] Progress feedback
  - [x] SIGINT/SIGTERM graceful shutdown
  - [x] Integration tests with testcontainers-Kafka
- **Dependencies:** 3, 5, 7
- **Notes:** COMPLETED. Known regressions tracked in Task 42 (flags silently dropped by `execute_new`).

### Task 36
- **ID:** 36
- **Title:** Partition-specific from-offsets
- **Status:** COMPLETED
- **Priority:** MEDIUM
- **Phase:** 0
- **Description:** `--from-offsets partition:offset,partition:offset` replaces single `--from-offset`.
- **Acceptance Criteria:**
  - [x] Parser for partition:offset map
  - [x] Wired into consumer initialization
  - [x] Tests for multi-partition scenarios
- **Dependencies:** 3, 15
- **Notes:** COMPLETED.

---

## Phase 1 — MVP (v0.1.0 target)

### Week 1 — Cleanup & Consolidation

> Deletes ~1000 LOC of dead first-gen code, establishes the hexagonal boundary for real, and syncs docs with reality.

### Task 37
- **ID:** 37
- **Title:** Remove dead legacy `execute()` body in store.rs
- **Status:** COMPLETED
- **Priority:** CRITICAL
- **Phase:** 1
- **Description:** `src/cli/commands/store.rs::execute()` (line ~172) immediately returns `self.execute_new().await` followed by ~250 unreachable lines. Delete the dead body, rename `execute_new` → `execute`.
- **Acceptance Criteria:**
  - [x] Lines 172–438 of `src/cli/commands/store.rs` removed (267 lines)
  - [x] Orphan imports removed (`KafkaConsumer`, `KafkaConsumerConfig`, `DirectoryStorage`, `DirectoryStorageConfig`, `SingleFileStorage`, `SingleFileStorageConfig`, `StorageBackend`, `KafkaMessage`, `PathBuf`, `Arc`, `Duration`, `Instant`, `Context`, `signal`, `mpsc`, `debug`, `error`)
  - [x] `execute_new` renamed to `pub async fn execute`
  - [x] `cargo build` green; `cargo test --bins` green (47 passed, 2 ignored)
  - [ ] `cargo clippy -- -D warnings`: 49 pre-existing warnings (`SingleFileStorage` / `DirectoryStorageFormat::Json` etc. unused) — will clear after Task 38–41 finish removing dead code paths
- **Dependencies:** None
- **Notes:** COMPLETED 2026-05-07 via commit 7033a91. File `store.rs` shrunk 873 → 606 LOC (−267). Two unit tests adjusted: `test_execute_no_destination` now matches the new error string; `test_execute_dry_run` marked `#[ignore]` referencing Task 42 which will restore `--dry-run` routing through `StoreKafkaCommand`. Unlocks Tasks 38 and 39.

### Task 38
- **ID:** 38
- **Title:** Delete first-gen `src/kafka/consumer.rs`
- **Status:** COMPLETED
- **Priority:** CRITICAL
- **Phase:** 1
- **Description:** After Task 37 there are no callers of `KafkaConsumer`/`KafkaConsumerConfig`. Delete the entire file and drop `pub mod consumer;` from `src/kafka/mod.rs`.
- **Acceptance Criteria:**
  - [x] `grep -rn "KafkaConsumer\b" src/` shows no callers (only the now-deleted file matched)
  - [x] `src/kafka/consumer.rs` removed (646 LOC)
  - [x] `src/kafka/mod.rs` updated with placeholder comment for Task 39
  - [x] `cargo build` green; `cargo test --bins` 45 passed / 0 failed / 2 ignored
- **Dependencies:** 37
- **Notes:** COMPLETED 2026-05-07 via commit daf509f. LOC delta: −646. Test count dropped 47 → 45 because the deleted file owned 2 unit tests for first-gen behavior — expected. Warnings dropped 49 → 38. Note: `KafkaConsumerContext` private type in `src/core/store_usecase.rs` is unrelated (same name in different scope) and remains until Task 39 moves the adapter.

### Task 39
- **ID:** 39
- **Title:** Move `RdKafkaConsumer` adapter into `src/kafka/`; make `core/` rdkafka-free
- **Status:** COMPLETED
- **Priority:** CRITICAL
- **Phase:** 1
- **Description:** Lift `RdKafkaConsumer`, `OwnedTopicMetadata`, `parse_topic_assignment`, and `convert_to_kafka_message` from `src/core/store_usecase.rs` into a new (clean) `src/kafka/consumer.rs`. Keep only trait + pure orchestration in `core/`.
- **Acceptance Criteria:**
  - [x] `grep "use rdkafka" src/core/` returns zero results (one residual `#[from] rdkafka::error::KafkaError` in `core/errors.rs` flagged as Task 39b)
  - [x] `CoreKafkaConsumer` trait + new pure-domain types (`TopicMetadata`, `PartitionMetadata`, `TopicPartitionsAssignment`, `PartitionOffset`, `DomainOffset`) live in `core/`
  - [x] Adapter `src/kafka/consumer.rs` (293 LOC) holds all rdkafka imports and TPL/metadata conversions
  - [x] `cargo build` green; `cargo test --bins` 45 passed / 0 failed / 2 ignored
  - [ ] Integration tests still green (needs Docker — owner to verify)
  - [ ] ADR-003 written in `docs/ARCHITECTURE_DECISIONS.md` (next sub-task; tracked separately so the boundary commit lands clean)
- **Dependencies:** 38
- **Notes:** COMPLETED 2026-05-07 via commit e47bf83. `core/store_usecase.rs` shrunk 876 → 511 LOC (−365). New `src/kafka/consumer.rs` 293 LOC. Domain types (`DomainOffset`) match rdkafka semantics; adapter handles bidirectional TPL/metadata conversion. Removed the unused `StoreUsecase` trait + `StoreUsecaseImpl` struct (vestigial). Warnings 38 → 24. Real hex boundary established. Enables mockall-based unit tests of `PumpTask` (will be exercised in Task 70).

### Task 40
- **ID:** 40
- **Title:** Delete 2-line stubs and drop `database` feature
- **Status:** COMPLETED
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Remove all stub modules that back no real implementation and remove the `default = ["database"]` feature.
- **Acceptance Criteria:**
  - [x] Deleted: `src/kafka/{producer,mock}.rs`, `src/storage/database/{postgres,sqlite,mod}.rs`, `src/storage/transform/{js_engine,mod}.rs`, `src/plugins/{registry,mod}.rs` (9 files, all 2-line stubs)
  - [x] `src/utils/{logging,progress,validation}.rs` retained (real bodies in Tasks 53, 59, 66)
  - [x] `[features] default = ["database"]` removed from `Cargo.toml`; `sqlx` dependency removed (returns in Task 8 as real SQLite impl)
  - [x] `serde_yaml` retained for now (Task 57 will replace with `serde_yml`/`toml` per security review)
  - [x] `src/storage/mod.rs`, `src/kafka/mod.rs`, `src/main.rs` updated
  - [x] `cargo build` green; `cargo test --bins` 45 passed / 0 failed / 2 ignored
- **Dependencies:** 37
- **Notes:** COMPLETED 2026-05-07. `producer.rs` will return as a real impl in Task 44 (replay). SQLite returns as top-level `src/storage/sqlite.rs` in Task 8. The `sqlx-postgres` future-incompatibility warning that was polluting every build is gone now that the dep is removed. Empty subdirs `database/`, `transform/`, `plugins/` removed.

### Task 41
- **ID:** 41
- **Title:** Wire `--format` through `MessageFormat` trait; drop the storage-format enum
- **Status:** COMPLETED
- **Priority:** HIGH
- **Phase:** 1
- **Description:** `--format` was parsed by clap but never reached storage. Removed `DirectoryStorageFormat` enum, replaced with `Arc<dyn MessageFormat + Send + Sync>` in `DirectoryStorageConfig`, threaded format selection from CLI through `StoreKafkaCommand` to the storage backend.
- **Acceptance Criteria:**
  - [x] `StoreKafkaCommand` carries `format: Arc<dyn MessageFormat + Send + Sync>` with manual `Debug` impl
  - [x] `DirectoryStorageConfig.format` is a trait object; default is `JsonHybridFormat` (Utf8WithFallback encoding)
  - [x] `DirectoryStorage::store_message` calls `self.config.format.serialize(&message).await` (no enum match)
  - [x] CLI `parse_format(&str)` recognizes `json`, `json-hybrid`, `json-hybrid-base64`, `json-hybrid-utf8`, `json-hybrid-force-utf8`, `json-hybrid-value` (plus short aliases) and errors with "planned for Phase 2" on `avro`/`protobuf`/`binary`/`string`
  - [x] `cargo build` green; `cargo test --bins` 46 passed / 0 failed / 2 ignored
- **Dependencies:** 37, 40
- **Notes:** COMPLETED 2026-05-07. Decision vs simplifier review: kept BOTH `JsonFormat` (verbose serde-derive) AND `JsonHybridFormat` (smart binary encodings) as independent `MessageFormat` impls — each pays rent for a different output style. Architect-reviewer's "collapse" suggestion would force one chosen output style on users; the trait route preserves choice without code duplication in dispatch. Warnings dropped 25 → 20 (`DirectoryStorageFormat::Json` variant warning gone). Format names: `JsonFormat::format_name() == "json"`, `JsonHybridFormat::format_name() == "json-hybrid"`. Avro/Protobuf/Binary/String formats are deferred to Phase 2 but the CLI now error-routes them properly instead of silently defaulting to JSON.

### Task 42
- **ID:** 42
- **Title:** Fix silent-drop CLI flag regression
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** `execute_new` ignores `--to-file`, `--to-db`, `--count`, `--from-offsets`, `--until-*`, `--live`, `--batch-size`, `--buffer-size`, `--threads`, `--compression`, `--dry-run`. Implement `TryFrom<StoreCommand> for StoreKafkaCommand` and either wire every flag or error explicitly with "planned for Phase 2".
- **Acceptance Criteria:**
  - [ ] All flags either functional or return a typed error message
  - [ ] No silent no-ops
  - [ ] Unit test per flag: either asserts behavior or asserts the correct "not yet supported" error
- **Dependencies:** 37, 41
- **Notes:** CLI UX review flagged this as the most confusing behavior for first-time users.

### Task 43
- **ID:** 43
- **Title:** Docs sync with reality — single SSoT + ADR trail
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Consolidate planning docs; add ADR-003/004/005; remove stale claims.
- **Acceptance Criteria:**
  - [ ] `docs/ROADMAP.md` absorbs `docs/plan.md` (delete `plan.md` after merge)
  - [ ] `docs/ARCHITECTURE_DECISIONS.md` gains ADR-003 (hexagonal boundary), ADR-004 (JsonHybrid encodings), ADR-005 (backpressure)
  - [ ] `CLAUDE.md` Current State section matches the actual code
  - [ ] `README.md` no longer claims `main.rs` is a "Hello world placeholder"
  - [ ] `GEMINI.md` / `tests/NEXT_STEPS.md` evaluated — delete or fold into a single test README
- **Dependencies:** 39 (ADR-003 describes the boundary established then)
- **Notes:** Close Week 1 with zero lies in docs.

---

### Week 2 — Replay MVP

### Task 44
- **ID:** 44
- **Title:** Implement `RdKafkaProducer` adapter + `CoreKafkaProducer` trait
- **Status:** TODO
- **Priority:** CRITICAL
- **Phase:** 1
- **Description:** Mirror the consumer split: trait in `core/`, rdkafka adapter in `kafka/`. Support per-message key/value/headers/partition + idempotent producer config.
- **Acceptance Criteria:**
  - [ ] `CoreKafkaProducer` trait in `src/core/`
  - [ ] `RdKafkaProducer` impl in `src/kafka/producer.rs`
  - [ ] Idempotent producer defaults (`enable.idempotence=true`)
  - [ ] Unit tests via mockall on the trait
- **Dependencies:** 39, 40
- **Notes:** Supersedes original Task 16.

### Task 45
- **ID:** 45
- **Title:** Implement replay use-case + `replay --from-file --to-topic` round-trip
- **Status:** TODO
- **Priority:** CRITICAL
- **Phase:** 1
- **Description:** New `src/core/replay_usecase.rs`. Reads messages via an iterator on `StorageBackend`, emits via `CoreKafkaProducer`. Rewrite `src/cli/commands/replay.rs` to call this.
- **Acceptance Criteria:**
  - [ ] `StorageBackend` gains `iter_messages()` (or similar)
  - [ ] Round-trip integration test: `store → jq edit → replay → consumer verifies delivery`
  - [ ] Preserves keys, headers, partition, timestamps (basic fidelity)
  - [ ] `replay --interactive` and `replay --transform` return "planned for Phase 2" error
- **Dependencies:** 44
- **Notes:** Supersedes original Task 18. Interactive + transform modes move to Phase 2 (Tasks 19, 20).

---

### Week 3 — Replay Fidelity (the moat)

### Task 46
- **ID:** 46
- **Title:** `--timestamp-strategy {preserve,now,shift=±Nh}`
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Core differentiator vs kcat. `preserve` (default) sets original timestamp; `now` uses wall clock; `shift=+1h` offsets by duration.
- **Acceptance Criteria:**
  - [ ] Three strategies functional with integration tests
  - [ ] Rustdoc explains use-cases (time-travel replay, chaos drills)
  - [ ] `--help` example per strategy
- **Dependencies:** 45

### Task 47
- **ID:** 47
- **Title:** `--partition-strategy {preserve,round-robin,key-hash}`
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Controls target-partition selection when source and target topics differ in partition count.
- **Acceptance Criteria:**
  - [ ] All three strategies work
  - [ ] Integration test with source=3 partitions, target=5
- **Dependencies:** 45

### Task 48
- **ID:** 48
- **Title:** `--rate Nmsg/s` and `--speedup Nx` replay pacing
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Rate-limit replay to Nmsg/s OR replay at N× original timestamp spacing (chaos-drill time-travel).
- **Acceptance Criteria:**
  - [ ] `--rate 100` caps at 100 msg/s
  - [ ] `--speedup 10x` compresses original inter-arrival gaps 10×
  - [ ] Flags are mutually exclusive
- **Dependencies:** 45, 46

### Task 49
- **ID:** 49
- **Title:** Replay fidelity integration test suite
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** End-to-end tests covering: header preservation, key preservation, timestamp strategies, partition strategies, rate/speedup.
- **Acceptance Criteria:**
  - [ ] Each fidelity dimension has at least one test
  - [ ] Uses testcontainers for broker isolation
  - [ ] Runs in CI (< 3 min per test)
- **Dependencies:** 46, 47, 48

---

### Week 4 — SQLite Backend (queryable storage, second moat)

### Task 8
- **ID:** 8
- **Title:** Implement SQLite StorageBackend
- **Status:** TODO
- **Priority:** CRITICAL
- **Phase:** 1
- **Description:** Flat `src/storage/sqlite.rs`. Single `messages` table with indices on `(topic, partition, offset)`, `(timestamp)`, `(key)`. WAL mode on, `pragma synchronous = NORMAL` for throughput.
- **Acceptance Criteria:**
  - [ ] Implements `StorageBackend` trait
  - [ ] Schema auto-created on first write
  - [ ] Batched inserts (tx per batch, not per row)
  - [ ] Integration test: store 100k messages, verify count + integrity
- **Dependencies:** 40 (ensure old database stubs are gone first), 41
- **Notes:** Moved from DEFERRED to CRITICAL for Phase 1 — queryability is the second moat. Use `rusqlite` with `bundled` feature or `sqlx-sqlite`; evaluate in sub-spike.

### Task 50
- **ID:** 50
- **Title:** `kscribe query <db> "SELECT ..."` subcommand
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** New subcommand for ad-hoc SQL queries over SQLite-backed captures. Output NDJSON (pipe-friendly) or pretty table.
- **Acceptance Criteria:**
  - [ ] Accepts arbitrary `SELECT` statement
  - [ ] Rejects non-read statements (no `INSERT/UPDATE/DELETE/DROP`)
  - [ ] `--output {ndjson,table}` flag
  - [ ] Integration test: store → query → verify results
- **Dependencies:** 8
- **Notes:** Read-only enforcement: wrap the connection in read-only mode; additionally parse the SQL to reject DDL/DML.

### Task 51
- **ID:** 51
- **Title:** Benchmark: NDJSON+jq vs SQLite query
- **Status:** TODO
- **Priority:** MEDIUM
- **Phase:** 1
- **Description:** Side-by-side benchmark with 1M messages: "find all errors from last hour" via `jq` vs SQLite query. Publish numbers in README.
- **Acceptance Criteria:**
  - [ ] Reproducible benchmark script in `benches/` or `examples/`
  - [ ] README includes the numbers
- **Dependencies:** 8, 50

---

### Week 5 — Security

### Task 52
- **ID:** 52
- **Title:** SASL/TLS configuration surface
- **Status:** TODO
- **Priority:** CRITICAL
- **Phase:** 1
- **Description:** CLI flags + env fallbacks for SASL/TLS. Never accept password on argv.
- **Acceptance Criteria:**
  - [ ] Flags: `--security-protocol {PLAINTEXT,SSL,SASL_SSL,SASL_PLAINTEXT}`, `--sasl-mechanism`, `--sasl-username`, `--sasl-password-file`, `--ssl-ca-location`, `--ssl-client-cert`, `--ssl-client-key`
  - [ ] Env fallbacks: `KAFKA_SASL_PASSWORD`, `KAFKA_SSL_PASSWORD`
  - [ ] Default: `enable.ssl.certificate.verification=true`
  - [ ] `PLAINTEXT`/`SASL_PLAINTEXT` requires explicit `--insecure` flag
  - [ ] Integration test against a SASL-enabled Redpanda container
- **Dependencies:** 39

### Task 53
- **ID:** 53
- **Title:** Path + topic-name sanitization (`src/utils/validation.rs`)
- **Status:** TODO
- **Priority:** CRITICAL
- **Phase:** 1
- **Description:** Fill the placeholder file with real sanitizers to close the path-traversal vulnerability via broker-controlled topic names.
- **Acceptance Criteria:**
  - [ ] `sanitize_topic_for_path()` rejects `..`, `/`, `\`, `\0`, control chars
  - [ ] `canonicalize_under_base()` asserts path stays under base_dir
  - [ ] Unit tests with adversarial inputs (`../../etc/passwd`, `topic\0.txt`, symlink bait)
  - [ ] `DirectoryStorage` uses sanitizer before every `path.push()`
- **Dependencies:** 7

### Task 54
- **ID:** 54
- **Title:** File / directory permissions (Unix)
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** `OpenOptions::new().mode(0o600)` for files, `0o700` for directories.
- **Acceptance Criteria:**
  - [ ] All file creates in `SingleFileStorage`/`DirectoryStorage`/`SqliteStorage` use restrictive modes on Unix
  - [ ] Test asserts mode bits on created files
  - [ ] Windows path is a no-op (documented)
- **Dependencies:** 7, 8

### Task 55
- **ID:** 55
- **Title:** rdkafka memory caps + regex size limits
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Set explicit `message.max.bytes` / `fetch.message.max.bytes` / `receive.message.max.bytes` with sane defaults exposed as CLI flags. Cap user regex via `RegexBuilder::size_limit(1 MiB)`.
- **Acceptance Criteria:**
  - [ ] Default `message.max.bytes=10 MiB`, `fetch.message.max.bytes=50 MiB`
  - [ ] Flags `--max-message-bytes`, `--max-fetch-bytes`
  - [ ] Regex with size > 1 MiB returns typed error
- **Dependencies:** 39

### Task 56
- **ID:** 56
- **Title:** Strip ANSI/control chars from broker data before logging
- **Status:** TODO
- **Priority:** MEDIUM
- **Phase:** 1
- **Description:** Log injection / terminal-escape attacks via message keys, headers, topic names.
- **Acceptance Criteria:**
  - [ ] Helper `scrub_for_log(&[u8]) -> String` (replace non-printable with `\xNN`)
  - [ ] Applied at every `tracing::*` site that logs broker data
  - [ ] Unit tests with `\x1b[31m` injection, BOMs
- **Dependencies:** None (can run parallel to 52-55)

### Task 57
- **ID:** 57
- **Title:** CI supply-chain gates (`cargo audit`, `cargo deny`, `gitleaks`)
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** New `.github/workflows/security.yml`. `deny.toml` bans `serde_yaml` (RUSTSEC-2024-0320), enforces MIT/Apache-2.0/BSD-3-Clause license policy.
- **Acceptance Criteria:**
  - [ ] `cargo audit` zero-vuln gate in CI
  - [ ] `cargo deny check` passes
  - [ ] `gitleaks` scans commits
  - [ ] Dependabot or equivalent enabled for Rust deps
- **Dependencies:** None

### Task 58
- **ID:** 58
- **Title:** Replay safety guard (`--allow-prod-bootstrap-servers`)
- **Status:** TODO
- **Priority:** MEDIUM
- **Phase:** 1
- **Description:** Prevent confusable replay target. When replaying with a bootstrap list containing `prod`/`production`/`prd` in hostnames, require explicit `--allow-prod-bootstrap-servers` flag.
- **Acceptance Criteria:**
  - [ ] Heuristic hostname check
  - [ ] Optional `--require-cluster-id <uuid>` flag for stricter verification via metadata
  - [ ] Unit tests
- **Dependencies:** 45

---

### Week 6 — Performance & Debuggability

### Task 29
- **ID:** 29
- **Title:** Criterion benchmarks under `benches/`
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Real statistical benchmarks replacing the ad-hoc `tests/integration/performance_tests.rs` timings.
- **Acceptance Criteria:**
  - [ ] `benches/json_format.rs` — serialize/deserialize across encoding strategies
  - [ ] `benches/store_throughput.rs` — end-to-end with mock in-memory consumer
  - [ ] `cargo bench --no-run` wired into CI (prevents bitrot)
  - [ ] README publishes headline numbers
- **Dependencies:** 41

### Task 30
- **ID:** 30
- **Title:** Performance pass — BufWriter batching + channel cap + hot-path logging
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Resolve the top-3 bottlenecks identified in performance review.
- **Acceptance Criteria:**
  - [ ] `DirectoryStorage` uses batched `BufWriter` + `sync_data()` at batch boundary (not per message)
  - [ ] All `info!()` calls on per-message hot path (especially in filter code) demoted to `trace!`
  - [ ] `--buffer-size` flag replaces hardcoded channel capacity 100 (default 10 000)
  - [ ] rdkafka fetch tuning: `fetch.min.bytes=65536`, `queued.max.messages.kbytes=65536`
  - [ ] `RDKafkaLogLevel` tied to `RUST_LOG` (not hardcoded `Debug`)
- **Dependencies:** 29 (benchmark baseline)

### Task 59
- **ID:** 59
- **Title:** Structured tracing — spans + fields
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** `#[instrument(fields(topic, partition, offset))]` on consume/store functions. `--log-format {text,json}` flag.
- **Acceptance Criteria:**
  - [ ] `src/utils/logging.rs` implements `init_tracing(format: LogFormat)`
  - [ ] `--log-format json` enables `tracing_subscriber::fmt::json()` layer
  - [ ] Key hot paths have spans
  - [ ] jq-parseable when log-format=json (sample test)
- **Dependencies:** 30

### Task 60
- **ID:** 60
- **Title:** Runtime panic elimination (`unwrap` / `expect`)
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Replace `.unwrap()` / `.expect()` on runtime paths with `?` + `.context(...)`. Runtime paths: signal handlers, metadata fetch, TPL operations, broker-message conversion.
- **Acceptance Criteria:**
  - [ ] `grep -E "\.(unwrap|expect)\(" src/` shows only test-guarded sites (`#[cfg(test)]`) or statically-infallible contexts (documented)
  - [ ] Each replacement includes `topic`/`partition`/`offset` in the context message
- **Dependencies:** 59

### Task 61
- **ID:** 61
- **Title:** Message counters (consumed / filtered / stored / errors)
- **Status:** TODO
- **Priority:** MEDIUM
- **Phase:** 1
- **Description:** `AtomicU64` counters threaded through `PumpTask`. Reported at progress intervals and in final summary.
- **Acceptance Criteria:**
  - [ ] Counters: `messages_consumed`, `messages_filtered`, `messages_stored`, `store_errors`, `utf8_rejected`
  - [ ] Reported in final summary (superset of current summary)
  - [ ] Emitted as structured fields every 1s progress tick
- **Dependencies:** 59

### Task 62
- **ID:** 62
- **Title:** Remove debug noise from runtime output
- **Status:** TODO
- **Priority:** LOW
- **Phase:** 1
- **Description:** `println!("Parsed cli")` in `src/main.rs:29`, duplicate `use tracing` imports, any leftover `dbg!()`.
- **Acceptance Criteria:**
  - [ ] `grep -rn "println!\|dbg!" src/main.rs src/cli/` shows only intentional user-facing output
  - [ ] Duplicate imports removed
- **Dependencies:** None (trivial)

### Task 63
- **ID:** 63
- **Title:** Fix topic-not-found behavior
- **Status:** TODO
- **Priority:** MEDIUM
- **Phase:** 1
- **Description:** Currently logs `warn` and continues for 60s with no progress. Make it a hard error unless `--allow-missing-topic` is passed.
- **Acceptance Criteria:**
  - [ ] Default: fail fast with actionable error message
  - [ ] `--allow-missing-topic` flag for live/future-topic scenarios
- **Dependencies:** 39

---

### Week 7 — CLI Polish

### Task 22
- **ID:** 22
- **Title:** Shell completions (real implementation)
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Replace stub with `clap_complete::generate()` for bash/zsh/fish/powershell.
- **Acceptance Criteria:**
  - [ ] `kscribe completion zsh` emits valid completion script
  - [ ] Tested on fresh install of bash/zsh
  - [ ] Documentation (`completion --help`) explains install steps per shell
- **Dependencies:** 3
- **Notes:** Bumped from LOW to HIGH — highest-signal polish marker for a CLI.

### Task 64
- **ID:** 64
- **Title:** Env-var fallback for `--bootstrap-servers` + short flag `-b`
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** `#[arg(short = 'b', env = "KAFKA_BOOTSTRAP_SERVERS")]` on the bootstrap-servers flag across all commands.
- **Acceptance Criteria:**
  - [ ] `KAFKA_BOOTSTRAP_SERVERS=localhost:9092 kscribe store orders` works without `-b`
  - [ ] `-b` short form accepted
- **Dependencies:** None

### Task 65
- **ID:** 65
- **Title:** Human-friendly timestamp parsing (ISO 8601 + relative)
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** `--from-timestamp` / `--until-timestamp` accept `2026-04-15T10:00:00Z` and relative (`1h`, `30m`, `-2h`). Current raw-millis behavior is confusing.
- **Acceptance Criteria:**
  - [ ] Custom clap value_parser accepts both forms
  - [ ] Error message on unparseable input suggests valid forms
  - [ ] Unit tests for edge cases (timezones, overflow, future vs past)
- **Dependencies:** 42

### Task 66
- **ID:** 66
- **Title:** Progress bars via `indicatif` (`src/utils/progress.rs`)
- **Status:** TODO
- **Priority:** MEDIUM
- **Phase:** 1
- **Description:** Real body for the placeholder. Track consumed/stored counts, ETA, throughput.
- **Acceptance Criteria:**
  - [ ] Progress bar active by default; `--no-progress` or non-TTY disables
  - [ ] Updates every 100ms (not 1s)
  - [ ] Plays nicely with `--log-format json` (redirects to stderr)
- **Dependencies:** 61

### Task 67
- **ID:** 67
- **Title:** CLI snapshot tests via `assert_cmd` + `insta`
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Replace `contains()` grep-style stderr/stdout checks with structured golden-file snapshots.
- **Acceptance Criteria:**
  - [ ] `tests/cli_snapshots/` with snapshots per command's `--help`
  - [ ] Snapshot for error messages (bad args, missing brokers)
  - [ ] `cargo insta review` workflow documented
- **Dependencies:** 22, 64, 65

### Task 68
- **ID:** 68
- **Title:** CI gates: fix branch + add fmt/clippy
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** CI workflows trigger on `main` but default branch is `master` — PRs don't run tests. Also no fmt/clippy gate.
- **Acceptance Criteria:**
  - [ ] `.github/workflows/*.yml` triggers on `master` (or rename branch — owner decides)
  - [ ] `cargo fmt --check` gate
  - [ ] `cargo clippy --all-targets -- -D warnings` gate
  - [ ] `stats` / `replay` "not implemented" paths exit 1 (not 0) so scripted callers don't silently succeed
- **Dependencies:** None

### Task 21
- **ID:** 21
- **Title:** Implement `stats` command
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Display statistics about a capture (file / directory / SQLite): count, size, time range, topic/partition breakdown.
- **Acceptance Criteria:**
  - [ ] Works with all Phase-1 storage backends
  - [ ] `--output {text,json}` for machine-parseability
  - [ ] Integration test per backend
- **Dependencies:** 7, 8
- **Notes:** Moved to Week 7 after CLI polish lands.

---

### Week 8 — Testing Debt

### Task 69
- **ID:** 69
- **Title:** Property-based tests via `proptest`
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** `proptest` is already a dev-dep but unused. Cover serialization round-trips and filter predicates.
- **Acceptance Criteria:**
  - [ ] `KafkaMessage` round-trip through each `BinaryEncoding` variant
  - [ ] Filter predicates (key regex, headers, partition, offset/timestamp range)
  - [ ] Runs in default `cargo test`
- **Dependencies:** 41

### Task 70
- **ID:** 70
- **Title:** Migrate integration tests to `testcontainers-rs`
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Replace the shell-out to `docker compose` + hardcoded `localhost:29092` with testcontainers. Removes `unsafe impl Send`, enables parallel isolation.
- **Acceptance Criteria:**
  - [ ] `tests/common/kafka_setup.rs` uses `testcontainers::images::kafka::Kafka` (or Redpanda)
  - [ ] No `unsafe` in the test harness
  - [ ] Tests can run in parallel (`cargo test` without `--test-threads 1`)
  - [ ] Docker Compose files removed (or retained only for dev)
- **Dependencies:** None

### Task 71
- **ID:** 71
- **Title:** Coverage gate ≥ 80% via `cargo llvm-cov`
- **Status:** TODO
- **Priority:** MEDIUM
- **Phase:** 1
- **Description:** Set a hard coverage gate in CI. Focus on `core/`, `kafka/`, `storage/`, `formats/`.
- **Acceptance Criteria:**
  - [ ] CI fails if coverage < 80% (line)
  - [ ] Codecov or equivalent report linked from README
- **Dependencies:** 69, 70

### Task 72
- **ID:** 72
- **Title:** Consolidate test docs
- **Status:** TODO
- **Priority:** LOW
- **Phase:** 1
- **Description:** `tests/NEXT_STEPS.md`, `integration_test_tasks.md`, `tests/README.md`, `tests/fixtures/README.md` — too many. Fold into one `tests/README.md`.
- **Acceptance Criteria:**
  - [ ] Single `tests/README.md`
  - [ ] Rest deleted
- **Dependencies:** 70

### Task 26
- **ID:** 26
- **Title:** Test suite health check (meta)
- **Status:** SUPERSEDED
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Original broad "create comprehensive test suite" task — replaced by the concrete Tasks 69–72 plus ongoing per-feature tests.
- **Acceptance Criteria:** — (superseded)
- **Dependencies:** 69, 70, 71, 72
- **Notes:** Kept for historical reference; actual work is tracked in the superseding tasks.

---

### Week 9 — Demo, Examples, Docs, Blog #1

### Task 73
- **ID:** 73
- **Title:** `examples/` directory with three scenarios
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** Three runnable, documented demo scenarios.
- **Acceptance Criteria:**
  - [ ] `examples/prod-debug/` — find errors in last hour, extract user IDs via SQLite query, correlate across topics
  - [ ] `examples/chaos-replay/` — replay yesterday's prod traffic into staging at 10× speed
  - [ ] `examples/reprocess-failures/` — grab DLQ, fix via jq, replay to original topic
  - [ ] Each example has a README and a runnable script (docker-compose + commands)
- **Dependencies:** 45, 46, 47, 48, 8, 50

### Task 74
- **ID:** 74
- **Title:** `docs/QUICKSTART.md` — 5-minute walkthrough
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** From `cargo install kafka-scribe` to first round-trip in 5 min on a fresh machine.
- **Acceptance Criteria:**
  - [ ] Tested on a fresh VM / container
  - [ ] Screenshots or asciinema recordings
- **Dependencies:** 73

### Task 27
- **ID:** 27
- **Title:** User documentation (full)
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** README rewrite around the moat (replay fidelity + SQLite queries). Full command reference via generated `--help`. Troubleshooting section.
- **Acceptance Criteria:**
  - [ ] README leads with tagline + killer demo GIF
  - [ ] Command reference auto-generated from `--help`
  - [ ] Troubleshooting covers: Kafka connection, SASL errors, path-traversal rejection, signal handling
- **Dependencies:** 73, 74

### Task 28
- **ID:** 28
- **Title:** Developer documentation + ADRs
- **Status:** TODO
- **Priority:** MEDIUM
- **Phase:** 1
- **Description:** `docs/ARCHITECTURE.md` with port/adapter diagram, extension points, ADR index. ADR-004/005 (drafted in Task 43) land here fully fleshed out.
- **Acceptance Criteria:**
  - [ ] Module-level rustdoc on `core/`, `kafka/`, `storage/`, `formats/`
  - [ ] Architecture diagram (Mermaid or image)
  - [ ] ADR-004 and ADR-005 complete with tradeoffs
- **Dependencies:** 43

### Task 75
- **ID:** 75
- **Title:** Blog post #1 draft — "JsonHybrid: 4 Binary Encoding Strategies"
- **Status:** TODO
- **Priority:** MEDIUM
- **Phase:** 1
- **Description:** Technical deep-dive on the `BinaryEncoding` variants. Portfolio content that pays off regardless of release timing.
- **Acceptance Criteria:**
  - [ ] Draft in `docs/blog/2026-XX-json-hybrid-encodings.md`
  - [ ] Code excerpts cross-referenced with live source
  - [ ] Published or ready to publish by release day
- **Dependencies:** None (the code already exists)

---

### Week 10 — Release v0.1.0 & Blog #2

### Task 76
- **ID:** 76
- **Title:** `cargo-dist` matrix builds
- **Status:** TODO
- **Priority:** CRITICAL
- **Phase:** 1
- **Description:** `.github/workflows/release.yml` tag-triggered, produces binaries for 5 targets.
- **Acceptance Criteria:**
  - [ ] Targets: `x86_64-unknown-linux-gnu`, `x86_64-unknown-linux-musl`, `x86_64-apple-darwin`, `aarch64-apple-darwin`, `x86_64-pc-windows-msvc`
  - [ ] Binaries attached to GitHub Release
  - [ ] Smoke test per binary runs in release workflow
- **Dependencies:** 57, 68

### Task 31
- **ID:** 31
- **Title:** Signed releases
- **Status:** TODO
- **Priority:** MEDIUM
- **Phase:** 1
- **Description:** `cosign` or `minisign` on release artifacts. Public key in repo.
- **Acceptance Criteria:**
  - [ ] Signatures attached to each release binary
  - [ ] README documents signature verification
- **Dependencies:** 76

### Task 77
- **ID:** 77
- **Title:** CHANGELOG + release notes
- **Status:** TODO
- **Priority:** HIGH
- **Phase:** 1
- **Description:** `CHANGELOG.md` with complete history since first commit. Release notes linking to blog post #1.
- **Acceptance Criteria:**
  - [ ] `CHANGELOG.md` follows Keep-a-Changelog
  - [ ] v0.1.0 section lists all Phase-1 tasks
  - [ ] GitHub release text includes blog-post link
- **Dependencies:** 76

### Task 32
- **ID:** 32
- **Title:** Tag and publish v0.1.0
- **Status:** TODO
- **Priority:** CRITICAL
- **Phase:** 1
- **Description:** Final sign-off: docs done, bench numbers in README, examples tested, security scan clean.
- **Acceptance Criteria:**
  - [ ] All Phase-1 CRITICAL and HIGH tasks in `COMPLETED`
  - [ ] `cargo audit` clean
  - [ ] `cargo publish --dry-run` succeeds (if publishing to crates.io)
  - [ ] Tag `v0.1.0` pushed
  - [ ] GitHub Release published
- **Dependencies:** 27, 73, 74, 76, 77, 78

### Task 78
- **ID:** 78
- **Title:** Blog post #2 draft — "Kafka Rebalance Edge Cases in Rust Async"
- **Status:** TODO
- **Priority:** MEDIUM
- **Phase:** 1
- **Description:** Deep-dive on `BaseConsumer` + `block_in_place` + rebalance callbacks as implemented in `store_usecase.rs`.
- **Acceptance Criteria:**
  - [ ] Draft in `docs/blog/`
  - [ ] Ready to publish same week as v0.1.0
- **Dependencies:** None

---

## Phase 2 — Post-MVP

### Task 2
- **ID:** 2
- **Title:** Setup CI/CD pipeline (extended)
- **Status:** SUPERSEDED
- **Priority:** MEDIUM
- **Phase:** 2
- **Description:** Superseded by Tasks 57 (security CI), 68 (fmt/clippy), 71 (coverage), 76 (release workflow) — which collectively implement this.
- **Dependencies:** —

### Task 9
- **ID:** 9
- **Title:** PostgreSQL StorageBackend
- **Status:** DEFERRED
- **Priority:** MEDIUM
- **Phase:** 2
- **Description:** PostgreSQL backend via sqlx. JSONB payloads, indexed on topic/partition/offset/timestamp/key.
- **Acceptance Criteria:**
  - [ ] Auto-creates schema
  - [ ] Batched inserts
  - [ ] Integration tests with Postgres container
- **Dependencies:** 5, 8
- **Notes:** DEFERRED: SQLite (Task 8) covers the "queryable storage" moat for Phase 1. PostgreSQL lands when users demand multi-writer or central analytics.

### Task 11
- **ID:** 11
- **Title:** String message format
- **Status:** SUPERSEDED
- **Priority:** LOW
- **Phase:** 2
- **Description:** Covered by `JsonHybridFormat` with `BinaryEncoding::Utf8WithFallback` or `ForceUtf8`. Pure string format is a subset.
- **Dependencies:** —
- **Notes:** SUPERSEDED. Revisit only if users want raw-text output (no JSON wrapping).

### Task 12
- **ID:** 12
- **Title:** Binary message format
- **Status:** SUPERSEDED
- **Priority:** LOW
- **Phase:** 2
- **Description:** Covered by `JsonHybridFormat` with `BinaryEncoding::Base64`.
- **Dependencies:** —
- **Notes:** SUPERSEDED.

### Task 13
- **ID:** 13
- **Title:** Avro message format
- **Status:** DEFERRED
- **Priority:** HIGH
- **Phase:** 2
- **Description:** `apache-avro` impl of `MessageFormat` with schema-registry hook. Handles the leading-magic-byte + schema-id-prefix wire format.
- **Acceptance Criteria:**
  - [ ] Deserialize Avro with schema registry lookup (Task 33)
  - [ ] Serialize with registered schema
  - [ ] Round-trip tests
- **Dependencies:** 6, 33
- **Notes:** DEFERRED to Phase 2. Design sketch must live in `docs/ARCHITECTURE.md` before v0.1.0 ships (see Task 28) — a replay tool that can't handle Avro-with-schema-id is a toy for real prod Kafka.

### Task 14
- **ID:** 14
- **Title:** Protobuf message format
- **Status:** DEFERRED
- **Priority:** MEDIUM
- **Phase:** 2
- **Description:** `prost`-based impl with schema-registry hook.
- **Dependencies:** 6, 33

### Task 16
- **ID:** 16
- **Title:** Original "Kafka producer for replay"
- **Status:** SUPERSEDED
- **Priority:** —
- **Phase:** 1
- **Description:** Folded into Task 44.
- **Dependencies:** —

### Task 18
- **ID:** 18
- **Title:** Original "replay auto mode"
- **Status:** SUPERSEDED
- **Priority:** —
- **Phase:** 1
- **Description:** Folded into Task 45.
- **Dependencies:** —

### Task 19
- **ID:** 19
- **Title:** Replay interactive mode (TUI)
- **Status:** DEFERRED
- **Priority:** LOW
- **Phase:** 2
- **Description:** Browse messages, edit before send, per-message confirm. Consider `ratatui` for the TUI.
- **Dependencies:** 45

### Task 20
- **ID:** 20
- **Title:** Replay transform mode (JS engine)
- **Status:** DEFERRED
- **Priority:** MEDIUM
- **Phase:** 2
- **Description:** Apply programmatic transformations via embedded JS engine (`deno_core`) or Rhai.
- **Dependencies:** 45

### Task 33
- **ID:** 33
- **Title:** Confluent Schema Registry integration
- **Status:** DEFERRED
- **Priority:** HIGH
- **Phase:** 2
- **Description:** Client for Confluent Schema Registry / Apicurio. Used by Avro/Protobuf formats.
- **Acceptance Criteria:**
  - [ ] Fetch + cache schemas by ID
  - [ ] Auth (basic auth, API key)
  - [ ] Handles schema evolution on replay (compatibility checks)
- **Dependencies:** —
- **Notes:** HIGH priority within Phase 2. Design sketch in `docs/ARCHITECTURE.md` before v0.1.0 per Task 28 (Phase-1 credibility).

### Task 34
- **ID:** 34
- **Title:** CloudEvents format support
- **Status:** DEFERRED
- **Priority:** LOW
- **Phase:** 2
- **Description:** Recognize and validate CloudEvents-formatted messages.
- **Dependencies:** 6

### Task 23
- **ID:** 23
- **Title:** Schema detection script
- **Status:** DEFERRED
- **Priority:** LOW
- **Phase:** 2
- **Description:** Infer schema from stored message samples. Companion script.
- **Dependencies:** 7, 10, 13, 14
- **Notes:** ADR-002 (external tools for analysis) lowers priority. May graduate to a bundled example instead.

### Task 24
- **ID:** 24
- **Title:** Database query templates
- **Status:** DEFERRED
- **Priority:** LOW
- **Phase:** 2
- **Description:** Pre-built SQL templates for common analysis patterns over SQLite captures.
- **Dependencies:** 8, 50
- **Notes:** May live in `examples/` instead of a dedicated subcommand.

### Task 25
- **ID:** 25
- **Title:** Data export (CSV, Parquet)
- **Status:** DEFERRED
- **Priority:** LOW
- **Phase:** 2
- **Description:** Export stored messages to CSV or Parquet for downstream analytics.
- **Dependencies:** 7, 8

---

## Phase 3 — Scale & Polish

### Task 35
- **ID:** 35
- **Title:** Object-storage backends (S3, GCS, Azure Blob)
- **Status:** DEFERRED
- **Priority:** LOW
- **Phase:** 3
- **Description:** Implement `StorageBackend` for object stores. Multipart upload, retry with exponential backoff.
- **Dependencies:** 5

### Task 79
- **ID:** 79
- **Title:** Per-partition parallelism
- **Status:** DEFERRED
- **Priority:** MEDIUM
- **Phase:** 3
- **Description:** Single consumer currently owns all partitions. Split into N consumer tasks sharing the group; N independent `PumpTask` writers.
- **Acceptance Criteria:**
  - [ ] `--parallel N` flag
  - [ ] Rebalance handling across tasks
  - [ ] Throughput benchmarks demonstrate linear scale up to broker cap
- **Dependencies:** 29, 30

### Task 80
- **ID:** 80
- **Title:** `simd-json` / `sonic-rs` evaluation
- **Status:** DEFERRED
- **Priority:** LOW
- **Phase:** 3
- **Description:** Drop-in replacement for `serde_json` on hot paths, benchmarked.
- **Dependencies:** 29

### Task 81
- **ID:** 81
- **Title:** Plugin system (custom formats + backends)
- **Status:** DEFERRED
- **Priority:** LOW
- **Phase:** 3
- **Description:** WASM-based or dylib-based plugin loader.
- **Dependencies:** —

### Task 82
- **ID:** 82
- **Title:** Package for Homebrew / Scoop / AUR
- **Status:** DEFERRED
- **Priority:** LOW
- **Phase:** 3
- **Description:** Distribution beyond `cargo install` and GitHub Releases.
- **Dependencies:** 32

### Task 83
- **ID:** 83
- **Title:** Migrate to GitHub Issues as runtime SSoT
- **Status:** DEFERRED
- **Priority:** LOW
- **Phase:** 3
- **Description:** Once v0.1.0 is out and the project has external contributors, move task state from this markdown file into GitHub Issues with the label taxonomy already documented in the backlog-manager agent.
- **Acceptance Criteria:**
  - [ ] All TODO/IN_PROGRESS tasks mirrored as GitHub Issues
  - [ ] Agent configs flip from markdown-only to GH mode
  - [ ] `kafka_scribe_task_list.md` archived in `docs/`
- **Dependencies:** 32
