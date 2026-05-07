# CLAUDE.md

This file provides guidance to Claude Code and other AI coding agents working in this repository. `.junie/guidelines.md` delegates to this file — update here, not there.

## Project Overview

**kafka-scribe** is a Rust CLI for capturing Kafka messages to disk (files or SQLite), analyzing them with standard tools (grep/jq/SQL), and replaying them back with **deterministic fidelity** (preserved keys, headers, partition, timestamps).

### Positioning

Not a generic viewer (kcat/rpk do that). The moat is:
1. **Replay fidelity** — preserve headers/keys/partitions and offer `--timestamp-strategy {preserve,now,shift=±Nh}` for chaos drills and time-travel replays (e.g. "replay yesterday's 14:00–14:05 prod traffic into staging at 10× speed").
2. **Queryable SQLite-backed captures** — indexed `(topic, partition, offset, timestamp, key)` turns 50 GB of captures from `jq`-scan territory into sub-second SQL.

Target personas: backend/SRE during a prod incident; chaos engineer running replay drills.

## Current State (keep honest, update as reality changes)

**Live code (~6400 LOC Rust):**
- `store` command works end-to-end (file + directory storage via `StorageBackend` trait).
- `MessageFormat` trait with `JsonHybridFormat` (four binary-encoding strategies: Base64 / Utf8WithFallback / ForceUtf8 / JsonValue).
- Integration tests use testcontainers-rs (dev-dep) + docker-compose harness with `insta` snapshot assertions.
- Backpressure: `PumpTask` in `src/core/store_usecase.rs` — bounded `mpsc` + two-task producer/writer pipeline + idle-timeout shutdown.

**Dead / legacy code to prune in Week 1 of level-up (see `docs/ROADMAP.md`):**
- `src/cli/commands/store.rs::execute()` returns `execute_new()` on line ~172, then ~250 lines of unreachable legacy body — remove.
- `src/kafka/consumer.rs` (646 LOC) is the **first-generation** consumer. The second (cleaner, domain-oriented) is `CoreKafkaConsumer` trait + `RdKafkaConsumer` in `src/core/store_usecase.rs`. Keep the new one; delete the old.
- 2-line stubs to delete: `src/kafka/{producer,mock}.rs`, `src/storage/database/{postgres,sqlite}.rs`, `src/storage/transform/js_engine.rs`, `src/plugins/registry.rs`. `producer.rs` will return in Week 2 as a real impl for `replay`.
- `src/utils/{logging,progress,validation}.rs` are 2-line placeholders; they will get real bodies in weeks 5–7, keep the files.
- `[features] default = ["database"]` in `Cargo.toml` backs nothing real — remove.

**Stub / not yet implemented:**
- `replay` command (prints "not yet implemented" — real impl in Week 2).
- `stats` command (placeholder).
- Shell completions (`src/cli/commands/completion.rs` is a stub).

## Phases

| Phase | Scope | Target |
|-------|-------|--------|
| **Phase 1 — MVP (v0.1.0)** | Working `store` + `replay` + `stats` + SQLite backend + security (SASL/TLS) + criterion benchmarks + polished CLI + matrix-built binaries | End of 10-week level-up |
| **Phase 2 — Post-MVP** | Avro / Protobuf / schema-registry integration, PostgreSQL backend, JS transforms via `deno_core`, plugin system, object storage | After v0.1.0 |
| **Phase 3 — Scale & polish** | Per-partition parallelism, simd-json, advanced filter DSLs, Homebrew / scoop / AUR packaging | Post-v0.2.0 |

**Explicit Phase-1 cuts** (don't implement in MVP even if "almost free"): Avro, Protobuf, PostgreSQL, JS transforms, schema registry, plugin system, object storage, interactive replay mode. Phase 2 — not started.

## Development Commands

```bash
cargo build                                            # Debug build
cargo test                                             # Unit + integration (needs Docker for Kafka)
cargo build --release                                  # Release build
cargo fmt --check                                      # Formatting gate
cargo clippy --all-targets --all-features -- -D warnings  # Lint gate
RUST_LOG=debug cargo run -- <subcommand> --help        # Debug CLI
```

## Architecture

Aspiring ports-and-adapters boundary (formalized in **ADR-003**, to be written in Week 1 after the consumer move):
- **`core/`** — pure orchestration. Owns `KafkaMessage`, errors, config, use-cases (`store_usecase`, future `replay_usecase`), and trait ports (`CoreKafkaConsumer`, `CoreKafkaProducer` when added). **Must not import `rdkafka`.**
- **`kafka/`** — rdkafka adapter. Owns `RdKafkaConsumer` + future `RdKafkaProducer`, metadata-owning helpers, offset assignment translation.
- **`storage/`** — storage adapters. Trait `StorageBackend` + impls: `SingleFileStorage`, `DirectoryStorage`, `SqliteStorage` (Week 4).
- **`formats/`** — serialization adapters. Trait `MessageFormat` + `JsonHybridFormat`. Must be wired through `--format` CLI flag (currently bypassed — fix in Week 1 cleanup).
- **`cli/`** — clap command structs and thin translators `TryFrom<CliCommand> for UsecaseInput`.
- **`utils/`** — logging init (`tracing`), progress (`indicatif`), validation (path/topic sanitizers).

### ADRs (`docs/ARCHITECTURE_DECISIONS.md`)

- **ADR-001:** File-based storage with JSONL for MVP.
- **ADR-002:** Leverage external tools (grep/jq/SQL) for analysis; don't build custom analysis commands.
- **ADR-003** (to add, Week 1): Hexagonal boundary — `core/` owns orchestration and traits; `kafka/`, `storage/`, `formats/` are adapters. `core/` does not import third-party wire libraries.
- **ADR-004** (to add, Week 9): `JsonHybridFormat` binary-encoding strategies — rationale and tradeoffs.
- **ADR-005** (to add, Week 9): Backpressure via bounded `mpsc` + idle-timeout shutdown.

## Task Tracking — markdown-only mode

**Single source of truth:** `kafka_scribe_task_list.md`. Top-level structure: `## Phase 1 — MVP`, `## Phase 2`, `## Phase 3`. Each task has:

```markdown
### Task N
- **ID:** N
- **Title:** Brief description
- **Status:** TODO | IN_PROGRESS | BLOCKED | COMPLETED | DEFERRED
- **Priority:** CRITICAL | HIGH | MEDIUM | LOW
- **Description:** ...
- **Acceptance Criteria:** - [ ] AC 1 ...
- **Dependencies:** <comma-separated task IDs, or None>
- **Notes:** ...
```

Agents mutate Status via the `Edit` tool, matching on `### Task N\n- **ID:** N` for uniqueness. GitHub Issues are **not** the runtime state store in this mode — they are deferred until after v0.1.0.

### Task Selection Protocol
1. Only pick tasks with `Status: TODO` and all `Dependencies` in `COMPLETED`.
2. Follow priority order: CRITICAL → HIGH → MEDIUM → LOW.
3. On start: flip `Status: TODO` → `IN_PROGRESS` via `Edit`.
4. On completion: flip to `COMPLETED` and append an outcome note (commit SHA, date).
5. On block: flip to `BLOCKED`, append `BLOCKED YYYY-MM-DD: <reason>` to Notes.
6. Single agent per task at a time.

## Quality Gates

### Definition of Done
- [ ] All Acceptance Criteria met
- [ ] Unit tests written and passing
- [ ] Integration tests cover the new path (when applicable)
- [ ] `cargo fmt --check` clean
- [ ] `cargo clippy --all-targets -- -D warnings` clean
- [ ] `cargo audit` clean (from Week 5 onward)
- [ ] Rustdoc on public APIs
- [ ] CLAUDE.md / task list updated if behavior or status changed
- [ ] No regression in existing snapshots (or snapshots intentionally updated with justification)

### Code Quality Standards
- Typed errors (`thiserror`) at module boundaries; `anyhow::Context` for CLI-layer chaining — do not stringify typed errors into `anyhow!` strings.
- No `.unwrap()` / `.expect()` on runtime paths (tests are fine). Runtime paths include signal handlers, broker data conversion, TPL ops, metadata lookups.
- No `info!()` on the per-message hot path — use `trace!` for per-message logs, `info!` for summaries.
- `#[instrument(fields(topic, partition, offset))]` on consume/store functions; prefer structured fields over string interpolation.
- Target >80% line coverage (`cargo llvm-cov nextest`).
- All public APIs documented with rustdoc and at least one example in module-level docs when non-trivial.

## Testing Strategy
- Unit tests for core logic (formats, filters, message conversions).
- Integration tests against real Kafka via testcontainers-rs (preferred) or docker-compose.
- `proptest` for serialization round-trips across all `BinaryEncoding` variants and filter predicates (not yet used — Week 8 task).
- `insta` snapshots for CLI output and NDJSON shape.
- `criterion` benchmarks under `benches/` for store throughput and JSON ser/de (Week 6).
- CLI tests via `assert_cmd` + `insta` (Week 7).

## Security Baseline (Week 5 hardening)
- Fail-closed TLS: `enable.ssl.certificate.verification=true` by default; `PLAINTEXT`/`SASL_PLAINTEXT` requires `--insecure` flag.
- Never accept SASL password on argv; read from env var or `--sasl-password-file`.
- Sanitize broker-supplied topic names before using them in filesystem paths (reject `..`, `/`, `\0`, control chars; canonicalize & assert under base_dir).
- File perms `0o600`, directory perms `0o700` on Unix.
- Regex: `RegexBuilder::size_limit(1 MiB)` + `dfa_size_limit(1 MiB)`.
- rdkafka: explicit `message.max.bytes` / `fetch.message.max.bytes` / `receive.message.max.bytes` caps.
- Strip ANSI / control chars from broker data before any log site.
- CI gates: `cargo audit`, `cargo deny check`, `gitleaks`.
- `serde_yaml` (RUSTSEC-2024-0320) is banned in `deny.toml`; use `serde_yml` or `toml`.

## Key Files

### Code
- `src/main.rs` — entry; tracing init lives here (will move to `src/utils/logging.rs` in Week 6).
- `src/cli/commands/store.rs` — CLI clap structs + translator to `StoreKafkaCommand`.
- `src/core/store_usecase.rs` — orchestration, `PumpTask`, `MessageFilter`, `MessageLimits`, `CoreKafkaConsumer` trait. **Infrastructure import of `rdkafka` is temporary — moves to `kafka/` in Week 1 cleanup.**
- `src/kafka/consumer.rs` (after Week 1 cleanup) — `RdKafkaConsumer` adapter.
- `src/storage/files/{single_file,directory}.rs` — file backends.
- `src/formats/json_hybrid.rs` — canonical format impl.

### Docs
- `README.md` — user-facing overview (to be rewritten around moat in Week 9).
- `docs/design-document.md` — full design spec.
- `docs/ARCHITECTURE_DECISIONS.md` — ADR trail.
- `docs/ROADMAP.md` — 10-week level-up plan + Phase 2/3 ideas (will consolidate `plan.md` into this file during Week 1 docs sync).
- `docs/IMPLEMENTATION_GUIDE.md` — implementation guidance for AI agents.
- `kafka_scribe_task_list.md` — runtime task SSoT.

## Communication Protocols

### Escalation Triggers
Immediately escalate when:
- Task Acceptance Criteria are unclear or contradictory.
- Technical blocker cannot be resolved within 4 hours.
- Architecture decision needs clarification (missing or ambiguous ADR).
- Scope creep would push Phase-2 work into Phase-1 MVP.

### Context Preservation
- Read `kafka_scribe_task_list.md` for Task N and its Dependencies before starting work.
- Check `docs/ARCHITECTURE_DECISIONS.md` for relevant ADRs before proposing architectural changes.
- Record non-obvious implementation decisions in the task's `Notes:` field.

## Immediate Focus (Week 1 of level-up — see `docs/ROADMAP.md` for full 10-week plan)

1. Remove dead legacy `execute()` body in `src/cli/commands/store.rs`.
2. Delete first-gen `src/kafka/consumer.rs` (646 LOC); `CoreKafkaConsumer` is the sole path.
3. Move `RdKafkaConsumer` from `core/store_usecase.rs` to `src/kafka/consumer.rs`; make `core/` rdkafka-free. Write ADR-003.
4. Delete 2-line stubs (`producer`, `mock`, `database/*`, `transform/*`, `plugins/*`); drop the `database` feature.
5. Unify `json.rs` + `json_hybrid.rs`; route `--format` through the `MessageFormat` trait.
6. Fix silent-drop regression: all CLI flags either work or error with "planned for Phase 2" — no silent no-ops.
7. Sync CLAUDE.md + task list + ADRs with reality.
