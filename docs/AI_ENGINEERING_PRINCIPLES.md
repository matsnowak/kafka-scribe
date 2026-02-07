# AI Engineering Principles

This document serves as the **AUTHORITATIVE GUIDE** for all AI agents (Gemini, Claude, etc.) contributing to this repository. You **MUST** adhere to these principles.

## 1. Security First 🛡️

**"Secure by Design" is our mantra.**

*   **Vulnerability Checks**: Before adding any dependency, you MUST verify it is maintained and free of known critical vulnerabilities.
*   **Input Validation**: ALL external input (CLI args, file contents, network data) MUST be validated strictly. Use strong typing (e.g., `NewType` pattern) to enforce constraints, not just raw strings or integers.
*   **Safe Rust**: Avoid `unsafe` code unless absolutely necessary and proven safe with extensive comments and tests.
*   **Secrets Management**: NEVER commit secrets, keys, or credentials. Use environment variables or secure configuration management.
*   **Error Handling**: Do not leak sensitive system information in error messages.

## 2. Simplicity & Modularity 🧩

**Build with blocks. Keep it clean.**

*   **Hexagonal / Clean Architecture**:
    *   **Core**: Pure business logic and domain entities. NO external dependencies (no DB, no HTTP, no CLI).
    *   **Ports**: Traits defining interfaces for driving (CLI, API) and driven (DB, Filesystem) adapters.
    *   **Adapters**: Implementations of ports. Keep them separate from core logic.
*   **Small Function & Modules**: Functions should do one thing well. Modules should have clear responsibilities.
*   **Dependencies**: Use established, high-quality crates (e.g., `serde`, `tokio`, `thiserror`, `clap`). Avoid adding heavy dependencies for trivial utility.
*   **Idiomatic Rust**: Use Rust patterns (Option, Result, Iterators, Traits) effectively. Follow standard formatting (`cargo fmt`) and linting (`cargo clippy`).

## 3. Testability 🧪

**If it's not tested, it doesn't work.**

*   **Unit Tests**:
    *   Every core domain logic MUST have unit tests.
    *   Test edge cases, not just happy paths.
    *   Co-locate unit tests with code (`mod tests` inside the file).
*   **Integration Tests**:
    *   Test interaction between adapters and core logic.
    *   Use `tests/` directory for integration tests.
    *   Mock external dependencies (databases, Kafka) where appropriate, or use Docker containers (Testcontainers) for real integration testing.
*   **End-to-End (E2E) Tests**:
    *   Validate the application flow from CLI input to final output.
    *   Ensure the "user journey" is broken-free.
*   **Benchmarks**:
    *   For performance-critical code (parsing, serialization, I/O), write benchmarks using `criterion`.

## 4. Performance 🚀

**Make it fly.**

*   **Zero-Cost Abstractions**: Leverage Rust's ability to abstract without runtime penalty.
*   **Memory Management**: Minimize cloning (`.clone()`). Use references and Cow (Clone-on-Write) where possible.
*   **Async/Await**: Use async I/O for network and file operations to keep the runtime responsive.
*   **Profiling**: If you suspect a bottleneck, MEASURE it. Do not guess.
*   **Data Structures**: Choose the right data structure for the job (e.g., `HashMap` vs `BTreeMap`, `Vec` vs `LinkedList`).

## 5. Documentation 📚

**Code tells you how, documentation tells you why.**

*   **Rustdoc**: Document all public structs, enums, functions, and modules. Use examples in doc comments.
*   **Architecture Decision Records (ADRs)**: When making a significant design choice (e.g., choosing a database, changing core architecture), document it in `docs/adr/`.
*   **User Guides**: Keep `README.md` and `docs/` updated with usage instructions.
*   **Developer Guides**: Explain the "why" behind complex implementation details for future contributors (human or AI).

## Implementation Checklist for Agents

When implementing a feature, verify:
- [ ] Is it secure? (Input validated, no unsafe blocks without justification)
- [ ] Is it simple? (Clean architecture, modular)
- [ ] Is it tested? (Unit + Integration tests added/passed)
- [ ] Is it fast? (No obvious performance pitfalls)
- [ ] Is it documented? (Rustdocs + Markdown updates)
