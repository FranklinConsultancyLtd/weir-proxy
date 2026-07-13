# Weir Policy Enforcement + Telemetry Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add local-config policy enforcement (block disallowed models at admission, block disallowed tools mid-response) and a bounded per-request event log (`/events`), reusing and extending Weir's existing budget-enforcement pipeline rather than building a parallel mechanism.

**Architecture:** Policy (`blocked_models`, `blocked_tools`) is parsed from the same `weir.toml` as budgets, into a new parallel `TenantPolicies` map that hot-reloads alongside `TenantLimits` under one shared, atomically-swapped `ParsedConfig`. Model blocking happens in the gateway before any upstream call (same shape as the existing budget admission check). Tool blocking reuses the adapters' existing tool-call parsing (already built for token counting) — adapters now also report tool *names* (never arguments) in their cost results, and the enforcer/gateway check those names against policy the same way they already check token totals against budget. A new bounded, mutex-guarded ring buffer (`EventLog`) records one `UsageEvent` per completed request (tenant, model, tools called, tokens, blocked-or-not) and is exposed via a new `GET /events?since=<id>&limit=<n>` endpoint.

**Tech Stack:** Same as the existing Weir codebase — Rust, Axum, Tokio, serde/toml, tiktoken-rs.

## Global Constraints

- Policy is local-config-only for this MVP — no remote/SaaS-managed policy push-down. The dashboard (a separate, future project) will read policy via `/stats`/`/events`, not manage it.
- Privacy line: only tool **names** are ever captured or exposed, never tool call arguments. Prompt/response content is never touched by this work, same as before.
- `EventLog` is explicitly NOT a hot-path structure in the sense `SlidingWindowCounter` is — it receives one push per *completed request*, not per chunk, so a plain `Mutex` is the correct, honest choice; do not over-engineer lock-free machinery here.
- This work touches already-reviewed, already-hardened code (`BudgetRegistry`, both provider adapters, `enforcer.rs`, `gateway.rs`). Every task below states exactly what ripples into existing code and why. If you hit a type mismatch against the actual current file contents that isn't explained by a task's stated ripple, stop and report NEEDS_CONTEXT rather than improvising a fix — this codebase has a history of subtle concurrency and reconciliation bugs that were only caught by careful review, and silent workarounds have bitten this project before.
- No new production dependency is needed — everything here builds on `serde`, `toml`, `dashmap`/`arc-swap` (unchanged), and `std::collections::VecDeque`/`std::sync::Mutex` (new use, both already effectively available via std).

---

## File Structure

```
weir-proxy/
├── src/
│   ├── config.rs           (extended: PolicyConfig, ParsedConfig, TenantPolicies, SharedConfig)
│   ├── error.rs             (extended: WeirError::PolicyViolation)
│   ├── budget/
│   │   └── mod.rs           (extended: BudgetRegistry::policy_for, adapts to ParsedConfig)
│   ├── provider/
│   │   ├── mod.rs           (extended: ChunkCost.tool_calls, NonStreamingCost)
│   │   ├── openai.rs        (extended: tool_calls in both cost methods)
│   │   └── anthropic.rs     (extended: content_block_start handling, tool_calls in both cost methods)
│   ├── telemetry.rs         (NEW: UsageEvent, EventLog)
│   ├── enforcer.rs          (extended: policy check + UsageEvent emission, streaming path)
│   ├── gateway.rs           (extended: model blocking, non-streaming tool blocking, /events route)
│   └── main.rs              (extended: EventLog construction)
├── weir.example.toml        (extended: policy example)
└── tests/
    └── proxy_flow_test.rs   (extended: policy blocking + /events integration tests)
```

---

### Task 1: Policy config parsing

**Files:**
- Modify: `src/config.rs`

**Interfaces:**
- Consumes: nothing new.
- Produces: `PolicyConfig { blocked_models: Vec<String>, blocked_tools: Vec<String> }` (Clone, Default), `ParsedConfig { limits: TenantLimits, policies: TenantPolicies }`, `TenantPolicies = HashMap<String, PolicyConfig>`, `SharedConfig = Arc<ArcSwap<ParsedConfig>>` (type changes from `Arc<ArcSwap<TenantLimits>>`), `parse(contents: &str) -> Result<ParsedConfig, WeirError>` (return type changes from `Result<TenantLimits, WeirError>`), `load_from_file(path) -> Result<ParsedConfig, WeirError>`, `load_shared(path) -> Result<SharedConfig, WeirError>`, `watch(path, shared: SharedConfig) -> notify::Result<notify::RecommendedWatcher>`. `BudgetLimit`/`TenantLimits` themselves are UNCHANGED — only what wraps them changes.

**Ripple note:** `parse`/`load_from_file`/`load_shared`/`SharedConfig`'s return/generic type all change shape. This ripples into `src/budget/mod.rs` (Task 3), `src/main.rs` (Task 10), and this file's own existing tests (updated in this task) — but does **not** touch `BudgetLimit`, `SlidingWindowCounter`, or `BudgetRegistry`'s counter/CAS internals at all.

- [ ] **Step 1: Write the failing tests**

Read the current `src/config.rs` first (`Read` it) so you can see the exact existing `RawConfig`/`RawTenantLimit`/`parse`/`load_from_file`/`SharedConfig`/`load_shared`/`watch` code and tests you're modifying — this task edits an existing file, it does not start from a blank slate.

Replace the non-test portion of `src/config.rs` (everything above `#[cfg(test)]`) with:

```rust
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;
use arc_swap::ArcSwap;
use serde::Deserialize;

use crate::error::WeirError;

#[derive(Debug, Clone, Copy)]
pub struct BudgetLimit {
    pub max_tokens: u64,
    pub window: Duration,
}

#[derive(Debug, Clone, Default)]
pub struct PolicyConfig {
    pub blocked_models: Vec<String>,
    pub blocked_tools: Vec<String>,
}

pub type TenantLimits = HashMap<String, BudgetLimit>;
pub type TenantPolicies = HashMap<String, PolicyConfig>;

#[derive(Debug, Default)]
pub struct ParsedConfig {
    pub limits: TenantLimits,
    pub policies: TenantPolicies,
}

#[derive(Debug, Deserialize)]
struct RawConfig {
    tenants: HashMap<String, RawTenantLimit>,
}

#[derive(Debug, Deserialize)]
struct RawTenantLimit {
    max_tokens: u64,
    window_seconds: u64,
    #[serde(default)]
    policy: RawPolicy,
}

#[derive(Debug, Deserialize, Default)]
struct RawPolicy {
    #[serde(default)]
    blocked_models: Vec<String>,
    #[serde(default)]
    blocked_tools: Vec<String>,
}

pub fn parse(contents: &str) -> Result<ParsedConfig, WeirError> {
    let raw: RawConfig =
        toml::from_str(contents).map_err(|e| WeirError::Config(e.to_string()))?;

    let mut limits = TenantLimits::new();
    let mut policies = TenantPolicies::new();

    for (id, t) in raw.tenants {
        limits.insert(
            id.clone(),
            BudgetLimit {
                max_tokens: t.max_tokens,
                window: Duration::from_secs(t.window_seconds),
            },
        );
        policies.insert(
            id,
            PolicyConfig {
                blocked_models: t.policy.blocked_models,
                blocked_tools: t.policy.blocked_tools,
            },
        );
    }

    Ok(ParsedConfig { limits, policies })
}

pub fn load_from_file(path: &Path) -> Result<ParsedConfig, WeirError> {
    let contents = std::fs::read_to_string(path)
        .map_err(|e| WeirError::Config(format!("reading {}: {e}", path.display())))?;
    parse(&contents)
}

pub type SharedConfig = Arc<ArcSwap<ParsedConfig>>;

pub fn load_shared(path: &Path) -> Result<SharedConfig, WeirError> {
    let parsed = load_from_file(path)?;
    Ok(Arc::new(ArcSwap::from_pointee(parsed)))
}

pub fn watch(
    path: PathBuf,
    shared: SharedConfig,
) -> notify::Result<notify::RecommendedWatcher> {
    use notify::{RecursiveMode, Watcher};

    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if res.is_err() {
            return;
        }
        match load_from_file(&path) {
            Ok(parsed) => {
                shared.store(Arc::new(parsed));
                tracing::info!("reloaded config from {}", path.display());
            }
            Err(e) => {
                tracing::warn!("ignoring invalid config reload: {e}");
            }
        }
    })?;
    watcher.watch(&path, RecursiveMode::NonRecursive)?;
    Ok(watcher)
}
```

Update the existing `#[cfg(test)] mod tests` block: everywhere it currently does
`let limits = parse(toml).unwrap(); let limit = limits.get("acct_123").unwrap();`,
change to `let parsed = parse(toml).unwrap(); let limit = parsed.limits.get("acct_123").unwrap();`.
Keep the `rejects_malformed_toml` test's assertion shape (`matches!(result, Err(WeirError::Config(_)))`)
unchanged — `parse`'s error path is unaffected by this task.

Add two new tests to the same module:

```rust
    #[test]
    fn parses_tenant_policy() {
        let toml = r#"
            [tenants.acct_123]
            max_tokens = 50000
            window_seconds = 60

            [tenants.acct_123.policy]
            blocked_models = ["gpt-3.5-turbo"]
            blocked_tools = ["send_email"]
        "#;
        let parsed = parse(toml).unwrap();
        let policy = parsed.policies.get("acct_123").unwrap();
        assert_eq!(policy.blocked_models, vec!["gpt-3.5-turbo".to_string()]);
        assert_eq!(policy.blocked_tools, vec!["send_email".to_string()]);
    }

    #[test]
    fn tenant_without_policy_block_gets_empty_policy() {
        let toml = r#"
            [tenants.acct_123]
            max_tokens = 50000
            window_seconds = 60
        "#;
        let parsed = parse(toml).unwrap();
        let policy = parsed.policies.get("acct_123").unwrap();
        assert!(policy.blocked_models.is_empty());
        assert!(policy.blocked_tools.is_empty());
    }
```

Also update `src/config.rs`'s hot-reload test (`watch_reloads_on_file_change`) — it currently does
`let shared = load_shared(file.path()).unwrap(); assert_eq!(shared.load().get("acct_1").unwrap().max_tokens, 100);`
— change both occurrences of `shared.load().get(...)` to `shared.load().limits.get(...)`.

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib config`
Expected: FAIL to compile — the whole file changed shape; every existing caller of the old `parse`/`load_shared` signatures needs updating (that's the rest of this task, plus Tasks 3 and 10).

- [ ] **Step 3: Run tests to verify config.rs's own tests pass**

Run: `cargo test --lib config::tests`
Expected: this specific test module should now compile and pass on its own. The crate as a whole will still fail to build until Task 3 (budget/mod.rs) and Task 10 (main.rs) catch up to the new types — that's expected and handled by those tasks, not a sign this task is wrong.

- [ ] **Step 4: Commit**

```bash
git add src/config.rs
git commit -m "feat: parse tenant policy (blocked models/tools) alongside budget config"
```

Note: this commit will not compile the whole crate by itself (`budget/mod.rs` and `main.rs` still reference the old types) — that's expected for this step-by-step plan; the crate compiles again after Task 3.

---

### Task 2: WeirError::PolicyViolation

**Files:**
- Modify: `src/error.rs`

**Interfaces:**
- Produces: new `WeirError::PolicyViolation { tenant: String, reason: String }` variant, mapped to `403 Forbidden` / `"policy_violation"` in `IntoResponse`.

- [ ] **Step 1: Write the failing test**

Read the current `src/error.rs` first. Add the new variant to the existing `WeirError` enum (do not remove or reorder existing variants):

```rust
    #[error("tenant '{tenant}' violated policy: {reason}")]
    PolicyViolation { tenant: String, reason: String },
```

Add a new arm to the existing `match &self { ... }` inside `impl IntoResponse for WeirError`:

```rust
            WeirError::PolicyViolation { .. } => (StatusCode::FORBIDDEN, "policy_violation"),
```

Add a new test to the existing `#[cfg(test)] mod tests` block:

```rust
    #[test]
    fn policy_violation_maps_to_403() {
        let response = WeirError::PolicyViolation {
            tenant: "acct_1".into(),
            reason: "blocked_tool: send_email".into(),
        }
        .into_response();
        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cargo test --lib error::tests`
Expected: FAIL to compile — `PolicyViolation` variant doesn't exist yet (before applying the above).

- [ ] **Step 3: Run test to verify it passes**

Run: `cargo test --lib error::tests`
Expected: PASS (3 tests: the 2 existing plus the new one)

- [ ] **Step 4: Commit**

```bash
git add src/error.rs
git commit -m "feat: add WeirError::PolicyViolation mapped to 403"
```

---

### Task 3: BudgetRegistry adapts to ParsedConfig, gains policy_for

**Files:**
- Modify: `src/budget/mod.rs`

**Interfaces:**
- Consumes: `ParsedConfig`, `SharedConfig`, `PolicyConfig` (Task 1), `WeirError::PolicyViolation` is NOT used here (policy_for returns `WeirError::UnknownTenant` for an unknown tenant, same as `limit_for` — the *violation itself* is decided by callers in Tasks 8/9, not by this lookup).
- Produces: `BudgetRegistry::new(config: SharedConfig) -> Self` (parameter type changes from `Arc<ArcSwap<TenantLimits>>`), `pub fn policy_for(&self, tenant: &str) -> Result<PolicyConfig, WeirError>` (new). `is_within_budget`, `record`, `counter_for`'s internal CAS/counter logic are **UNCHANGED** — only the config-lookup layer (`limit_for`) changes.

**Ripple note:** `BudgetRegistry` now does double duty (budget AND policy lookups) rather than being renamed — this is a deliberate, accepted small naming imprecision to avoid renaming a type used throughout the whole codebase (`AppState.budget: Arc<BudgetRegistry>` etc.) for a two-field addition. Do not rename `BudgetRegistry` as part of this task.

- [ ] **Step 1: Write the failing tests**

Read the current `src/budget/mod.rs` first. Replace the top of the file (the `use` statements through the end of `impl BudgetRegistry`, i.e. everything before `#[cfg(test)] mod registry_tests`) with:

```rust
pub mod sliding_window;

use std::sync::Arc;
use dashmap::DashMap;

use crate::config::{BudgetLimit, ParsedConfig, PolicyConfig, SharedConfig};
use crate::error::WeirError;
use sliding_window::SlidingWindowCounter;

pub struct BudgetRegistry {
    config: SharedConfig,
    // Tracks the window each tenant's counter was created with, so a
    // config reload that changes a tenant's window recreates the counter
    // instead of silently running it against a stale window forever.
    counters: DashMap<String, (std::time::Duration, Arc<SlidingWindowCounter>)>,
}

impl BudgetRegistry {
    pub fn new(config: SharedConfig) -> Self {
        Self { config, counters: DashMap::new() }
    }

    fn limit_for(&self, tenant: &str) -> Result<BudgetLimit, WeirError> {
        self.config
            .load()
            .limits
            .get(tenant)
            .copied()
            .ok_or(WeirError::UnknownTenant)
    }

    /// Returns the tenant's configured policy (blocked models/tools). An
    /// unknown tenant is the same error `limit_for` returns for the same
    /// reason — both are reading the same underlying config snapshot.
    pub fn policy_for(&self, tenant: &str) -> Result<PolicyConfig, WeirError> {
        self.config
            .load()
            .policies
            .get(tenant)
            .cloned()
            .ok_or(WeirError::UnknownTenant)
    }

    // Fast path: try a borrowed lookup first to avoid allocating an
    // owned `String` key on every call — this runs on the hot path,
    // once per SSE event, not once per request.
    fn counter_for(&self, tenant: &str, limit: BudgetLimit) -> Arc<SlidingWindowCounter> {
        if let Some(existing) = self.counters.get(tenant) {
            if existing.0 == limit.window {
                return existing.1.clone();
            }
        }

        // Cache miss, or the tenant's configured window changed: fall back
        // to the owning `entry` API, which needs an owned key to insert.
        let mut entry = self
            .counters
            .entry(tenant.to_string())
            .or_insert_with(|| (limit.window, Arc::new(SlidingWindowCounter::new(limit.window))));

        if entry.0 != limit.window {
            // The tenant's configured window changed since this counter was
            // created. Old accumulated usage doesn't cleanly map onto a
            // differently-sized window, so start fresh rather than keep
            // enforcing against a stale window.
            *entry = (limit.window, Arc::new(SlidingWindowCounter::new(limit.window)));
        }

        entry.1.clone()
    }

    pub fn is_within_budget(&self, tenant: &str, now_ms: i64) -> Result<bool, WeirError> {
        let limit = self.limit_for(tenant)?;
        let counter = self.counter_for(tenant, limit);
        Ok(counter.estimate(now_ms) < limit.max_tokens)
    }

    /// A chunk that brings the tenant's usage to exactly `max_tokens` is
    /// still allowed through (`<=`); the tenant is blocked starting with
    /// the *next* admission check once genuinely at or over the ceiling
    /// (`is_within_budget` uses strict `<`). This lets a tenant consume its
    /// full budget rather than being cut off one token short of it.
    pub fn record(&self, tenant: &str, amount: u64, now_ms: i64) -> Result<bool, WeirError> {
        let limit = self.limit_for(tenant)?;
        let counter = self.counter_for(tenant, limit);
        let total = counter.add(amount, now_ms);
        Ok(total <= limit.max_tokens)
    }
}
```

(`ParsedConfig` is imported but only used via `self.config.load().limits`/`.policies` — the import is needed for the `SharedConfig = Arc<ArcSwap<ParsedConfig>>` type to resolve through `self.config.load()`'s return type; if your Rust toolchain flags `ParsedConfig` as an unused direct import because it's only referenced through inference, remove it from the `use` line — keep `BudgetLimit, PolicyConfig, SharedConfig`.)

Now update `#[cfg(test)] mod registry_tests`: read the existing module first. Change the `registry_with` helper from:
```rust
    fn registry_with(tenant: &str, max_tokens: u64, window_secs: u64) -> BudgetRegistry {
        let mut limits = HashMap::new();
        limits.insert(
            tenant.to_string(),
            BudgetLimit { max_tokens, window: Duration::from_secs(window_secs) },
        );
        BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(limits)))
    }
```
to:
```rust
    fn registry_with(tenant: &str, max_tokens: u64, window_secs: u64) -> BudgetRegistry {
        let mut limits = HashMap::new();
        limits.insert(
            tenant.to_string(),
            BudgetLimit { max_tokens, window: Duration::from_secs(window_secs) },
        );
        let parsed = ParsedConfig { limits, policies: HashMap::new() };
        BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(parsed)))
    }
```
Apply the same `ParsedConfig { limits, policies: HashMap::new() }` wrapping to every other place in this test module that currently does `BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(limits)))` directly (e.g. inside `changing_window_recreates_counter_and_resets_usage`, for both the initial and the swapped-in `new_limits`) — wrap each raw `limits`/`new_limits` HashMap in `ParsedConfig { limits: ..., policies: HashMap::new() }` before passing to `ArcSwap::from_pointee`/`.store`.

Add one new test:

```rust
    #[test]
    fn policy_for_returns_configured_policy() {
        let mut limits = HashMap::new();
        limits.insert(
            "acct_1".to_string(),
            BudgetLimit { max_tokens: 100, window: Duration::from_secs(60) },
        );
        let mut policies = HashMap::new();
        policies.insert(
            "acct_1".to_string(),
            PolicyConfig {
                blocked_models: vec!["gpt-3.5-turbo".to_string()],
                blocked_tools: vec!["send_email".to_string()],
            },
        );
        let parsed = ParsedConfig { limits, policies };
        let registry = BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(parsed)));

        let policy = registry.policy_for("acct_1").unwrap();
        assert_eq!(policy.blocked_models, vec!["gpt-3.5-turbo".to_string()]);
        assert_eq!(policy.blocked_tools, vec!["send_email".to_string()]);

        assert!(matches!(registry.policy_for("acct_unknown"), Err(WeirError::UnknownTenant)));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib budget`
Expected: FAIL to compile — `registry_tests` helpers still construct the old `Arc<ArcSwap<TenantLimits>>` shape directly (before applying the updates above).

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test --lib budget`
Expected: PASS. This crosses back into a fully-compiling `src/budget/` module tree; the crate as a whole still won't build until Tasks 4-10 catch up other callers (enforcer.rs, gateway.rs, main.rs, and their own test modules) — expected at this point in the plan.

- [ ] **Step 4: Commit**

```bash
git add src/budget/mod.rs
git commit -m "feat: BudgetRegistry reads from ParsedConfig, exposes policy_for"
```

---

### Task 4: ChunkCost and NonStreamingCost gain tool_calls

**Files:**
- Modify: `src/provider/mod.rs`

**Interfaces:**
- Produces: `ChunkCost { estimated_tokens: u64, authoritative_total: Option<u64>, tool_calls: Vec<String> }` (new field), `NonStreamingCost { total_tokens: Option<u64>, tool_calls: Vec<String> }` (NEW struct, replaces the trait method's old `Option<u64>` return), `ProviderAdapter::non_streaming_cost(&self, body: &Bytes) -> NonStreamingCost` (return type changes from `Option<u64>`).

**Ripple note:** this is a trait-level change. Tasks 5 and 6 update both real adapter implementations to match. `src/enforcer.rs`'s two test-only adapter stubs (`FixedCostAdapter`, `AuthoritativeCostAdapter`) and `src/gateway.rs`'s callers of `non_streaming_cost` also need updating — those are handled in Tasks 8 and 9 respectively, not this task.

- [ ] **Step 1: Make the change**

Read the current `src/provider/mod.rs` first. Replace the `ChunkCost` struct, the `ProviderAdapter` trait, and add `NonStreamingCost`:

```rust
pub struct ChunkCost {
    pub estimated_tokens: u64,
    pub authoritative_total: Option<u64>,
    /// Names of any tools invoked in this chunk/event — never call
    /// arguments, only the tool's name, per the project's privacy line.
    pub tool_calls: Vec<String>,
}

/// The result of parsing a complete (non-streaming) JSON response body.
pub struct NonStreamingCost {
    pub total_tokens: Option<u64>,
    /// Names of any tools invoked anywhere in the response — never call
    /// arguments.
    pub tool_calls: Vec<String>,
}

pub trait ProviderAdapter: Send {
    fn chunk_cost(&mut self, raw: &Bytes) -> ChunkCost;

    /// Parses a complete (non-streaming) JSON response body and returns
    /// its authoritative total token count (if present) and any tool
    /// calls found. Non-streaming responses always carry their own
    /// authoritative usage (no interim estimation needed, unlike the
    /// streaming `chunk_cost` path).
    fn non_streaming_cost(&self, body: &Bytes) -> NonStreamingCost;
}
```

This is a type-only change in this file; there is no new test to write here (the trait itself has no behavior of its own) — Tasks 5 and 6 write the tests that exercise the new fields against real parsing logic.

- [ ] **Step 2: Confirm this file compiles standalone**

Run: `cargo build --lib 2>&1 | grep "provider/mod.rs"`
Expected: no errors specifically attributed to `src/provider/mod.rs` itself (errors from `openai.rs`/`anthropic.rs`/`enforcer.rs`/`gateway.rs` not yet matching the new trait shape are expected and are NOT this task's concern — those are Tasks 5, 6, 8, 9).

- [ ] **Step 3: Commit**

```bash
git add src/provider/mod.rs
git commit -m "feat: ChunkCost and NonStreamingCost report tool call names"
```

---

### Task 5: OpenAI adapter reports tool_calls

**Files:**
- Modify: `src/provider/openai.rs`

**Interfaces:**
- Consumes: `ChunkCost`, `NonStreamingCost` (Task 4).
- Produces: `OpenAiAdapter`'s `chunk_cost` and `non_streaming_cost` both populate `tool_calls: Vec<String>` from `delta.tool_calls[].function.name` (streaming) / `choices[].message.tool_calls[].function.name` (non-streaming).

- [ ] **Step 1: Write the failing tests**

Read the current `src/provider/openai.rs` first. In `chunk_cost`, the existing tool-call loop already does:
```rust
                for tool_call in &choice.delta.tool_calls {
                    let Some(function) = &tool_call.function else { continue };
                    if let Some(name) = &function.name {
                        estimated_tokens += self.tokenizer.encode_ordinary(name).len() as u64;
                    }
                    if let Some(arguments) = &function.arguments {
                        estimated_tokens += self.tokenizer.encode_ordinary(arguments).len() as u64;
                    }
                }
```
Add a `tool_calls: Vec<String>` local accumulator at the top of `chunk_cost` (alongside `estimated_tokens`/`authoritative_total`), and inside this same loop, push the name when present:
```rust
                    if let Some(name) = &function.name {
                        estimated_tokens += self.tokenizer.encode_ordinary(name).len() as u64;
                        tool_calls.push(name.clone());
                    }
```
Update the function's final return: `ChunkCost { estimated_tokens, authoritative_total, tool_calls }`.

Now replace `non_streaming_cost` in full (it currently only returns `Option<u64>` via an inline `NonStreamingResponse { usage: Option<OpenAiUsage> }` struct):

```rust
    fn non_streaming_cost(&self, body: &Bytes) -> NonStreamingCost {
        #[derive(Deserialize, Default)]
        struct NonStreamingMessage {
            #[serde(default)]
            tool_calls: Vec<OpenAiToolCallDelta>,
        }
        #[derive(Deserialize, Default)]
        struct NonStreamingChoice {
            #[serde(default)]
            message: NonStreamingMessage,
        }
        #[derive(Deserialize)]
        struct NonStreamingResponse {
            #[serde(default)]
            choices: Vec<NonStreamingChoice>,
            usage: Option<OpenAiUsage>,
        }

        let Ok(parsed) = serde_json::from_slice::<NonStreamingResponse>(body) else {
            return NonStreamingCost { total_tokens: None, tool_calls: Vec::new() };
        };

        let mut tool_calls = Vec::new();
        for choice in &parsed.choices {
            for tool_call in &choice.message.tool_calls {
                if let Some(function) = &tool_call.function {
                    if let Some(name) = &function.name {
                        tool_calls.push(name.clone());
                    }
                }
            }
        }

        NonStreamingCost { total_tokens: parsed.usage.map(|u| u.total_tokens), tool_calls }
    }
```

This reuses the existing `OpenAiToolCallDelta`/`OpenAiFunctionDelta` structs (already defined in this file for the streaming path) since OpenAI's non-streaming `message.tool_calls[]` shares the same `{id, type, function: {name, arguments}}` shape as the streaming `delta.tool_calls[]`.

Update the existing tests that call `non_streaming_cost` and check its return directly — both `non_streaming_cost_extracts_total_tokens` and `non_streaming_cost_returns_none_for_unparseable_body` currently do e.g. `assert_eq!(adapter.non_streaming_cost(&body), Some(7));` — change to `assert_eq!(adapter.non_streaming_cost(&body).total_tokens, Some(7));` (and `.total_tokens, None` for the unparseable-body test).

Add two new tests:

```rust
    #[test]
    fn chunk_cost_reports_tool_call_names() {
        let mut adapter = OpenAiAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"get_weather\",\"arguments\":\"{}\"}}]}}]}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert_eq!(cost.tool_calls, vec!["get_weather".to_string()]);
    }

    #[test]
    fn non_streaming_cost_reports_tool_call_names() {
        let adapter = OpenAiAdapter::new(tokenizer());
        let body = Bytes::from_static(
            b"{\"choices\":[{\"message\":{\"content\":null,\"tool_calls\":[{\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"get_weather\",\"arguments\":\"{}\"}}]}}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}",
        );
        let cost = adapter.non_streaming_cost(&body);
        assert_eq!(cost.tool_calls, vec!["get_weather".to_string()]);
        assert_eq!(cost.total_tokens, Some(7));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib provider::openai`
Expected: FAIL to compile (return type mismatches, before applying the above).

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test --lib provider::openai`
Expected: PASS (9 tests: 7 existing, updated where noted, plus 2 new).

- [ ] **Step 4: Commit**

```bash
git add src/provider/openai.rs
git commit -m "feat: OpenAI adapter reports tool call names in both cost paths"
```

---

### Task 6: Anthropic adapter reports tool_calls

**Files:**
- Modify: `src/provider/anthropic.rs`

**Interfaces:**
- Consumes: `ChunkCost`, `NonStreamingCost` (Task 4).
- Produces: `AnthropicAdapter`'s `chunk_cost` newly handles `content_block_start` (currently routed to the catch-all `Other` variant) to capture a tool_use block's name; `non_streaming_cost` extracts tool_use names from the response body's `content` array.

- [ ] **Step 1: Write the failing tests**

Read the current `src/provider/anthropic.rs` first. Add a new variant to `AnthropicEvent` (do not remove `Other` — it still catches `message_stop`, `content_block_stop`, `ping`, etc.):

```rust
    #[serde(rename = "content_block_start")]
    ContentBlockStart { content_block: AnthropicContentBlockStart },
```

Add the new nested type:

```rust
#[derive(Deserialize)]
#[serde(tag = "type")]
enum AnthropicContentBlockStart {
    #[serde(rename = "tool_use")]
    ToolUse { name: String },
    #[serde(other)]
    Other,
}
```

In `chunk_cost`, add a `tool_calls: Vec<String>` local accumulator (alongside `estimated_tokens`/`authoritative_total`), and add a new match arm:

```rust
                AnthropicEvent::ContentBlockStart { content_block } => {
                    if let AnthropicContentBlockStart::ToolUse { name } = content_block {
                        tool_calls.push(name);
                    }
                }
```

Update the function's final return: `ChunkCost { estimated_tokens, authoritative_total, tool_calls }`.

Now replace `non_streaming_cost` in full (currently a two-struct parse returning `Option<u64>`):

```rust
    fn non_streaming_cost(&self, body: &Bytes) -> NonStreamingCost {
        #[derive(Deserialize)]
        #[serde(tag = "type")]
        enum NonStreamingContentBlock {
            #[serde(rename = "tool_use")]
            ToolUse { name: String },
            #[serde(other)]
            Other,
        }
        #[derive(Deserialize)]
        struct NonStreamingUsage {
            input_tokens: u64,
            output_tokens: u64,
        }
        #[derive(Deserialize)]
        struct NonStreamingResponse {
            #[serde(default)]
            content: Vec<NonStreamingContentBlock>,
            usage: NonStreamingUsage,
        }

        let Ok(parsed) = serde_json::from_slice::<NonStreamingResponse>(body) else {
            return NonStreamingCost { total_tokens: None, tool_calls: Vec::new() };
        };

        let tool_calls = parsed
            .content
            .into_iter()
            .filter_map(|block| match block {
                NonStreamingContentBlock::ToolUse { name } => Some(name),
                NonStreamingContentBlock::Other => None,
            })
            .collect();

        NonStreamingCost {
            total_tokens: Some(parsed.usage.input_tokens + parsed.usage.output_tokens),
            tool_calls,
        }
    }
```

Add two new tests:

```rust
    #[test]
    fn chunk_cost_reports_tool_use_name_from_content_block_start() {
        let mut adapter = AnthropicAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":1,\"content_block\":{\"type\":\"tool_use\",\"id\":\"toolu_01\",\"name\":\"get_weather\",\"input\":{}}}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert_eq!(cost.tool_calls, vec!["get_weather".to_string()]);
    }

    #[test]
    fn non_streaming_cost_reports_tool_use_names() {
        let adapter = AnthropicAdapter::new(tokenizer());
        let body = Bytes::from_static(
            b"{\"content\":[{\"type\":\"text\",\"text\":\"Hi\"},{\"type\":\"tool_use\",\"id\":\"toolu_01\",\"name\":\"get_weather\",\"input\":{}}],\"usage\":{\"input_tokens\":25,\"output_tokens\":15}}",
        );
        let cost = adapter.non_streaming_cost(&body);
        assert_eq!(cost.tool_calls, vec!["get_weather".to_string()]);
        assert_eq!(cost.total_tokens, Some(40));
    }
```

Also add a test confirming a plain `content_block_start` for a **text** block (not tool_use) contributes nothing to `tool_calls` and doesn't panic — this is the common case and must stay a silent no-op:

```rust
    #[test]
    fn content_block_start_for_text_block_is_ignored() {
        let mut adapter = AnthropicAdapter::new(tokenizer());
        let raw = Bytes::from_static(
            b"event: content_block_start\ndata: {\"type\":\"content_block_start\",\"index\":0,\"content_block\":{\"type\":\"text\",\"text\":\"\"}}\n\n",
        );
        let cost = adapter.chunk_cost(&raw);
        assert!(cost.tool_calls.is_empty());
        assert_eq!(cost.estimated_tokens, 0);
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib provider::anthropic`
Expected: FAIL to compile (before applying the above).

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test --lib provider::anthropic`
Expected: PASS (7 tests: 4 existing, updated where noted, plus 3 new).

- [ ] **Step 4: Commit**

```bash
git add src/provider/anthropic.rs
git commit -m "feat: Anthropic adapter reports tool_use names via content_block_start"
```

---

### Task 7: EventLog and UsageEvent

**Files:**
- Create: `src/telemetry.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `Provider` (existing, from `crate::provider`) — this task also adds `Serialize`/`Deserialize` derives to it (see Step 1), since `UsageEvent` embeds it and needs to round-trip through JSON for the `/events` endpoint (Task 9) and its tests (Tasks 9, 12).
- Produces: `UsageEvent { id: u64, tenant: String, provider: Provider, model: Option<String>, tools_called: Vec<String>, tokens: u64, blocked: bool, block_reason: Option<String>, timestamp_ms: i64 }` (Clone, Debug, Serialize, Deserialize), `EventLog::new(capacity: usize) -> Self`, `EventLog::push(&self, event: UsageEvent)` (assigns and returns nothing; the log itself owns `id` assignment — callers pass `id: 0` as a placeholder, it's overwritten), `EventLog::since(&self, since: u64, limit: usize) -> Vec<UsageEvent>`. Used by Task 8 (`enforcer.rs`) and Task 9 (`gateway.rs`).

- [ ] **Step 1: Add Serialize/Deserialize to Provider**

Read the current `src/provider/mod.rs` first. `Provider` currently derives `Clone, Copy, Debug, PartialEq, Eq`. Change its derive line to also include `serde::Serialize, serde::Deserialize`:

```rust
#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
pub enum Provider {
    OpenAi,
    Anthropic,
}
```

This is the only change to `src/provider/mod.rs` in this task (the `ChunkCost`/`NonStreamingCost`/trait changes are Task 4, not this one — this task only touches `Provider`'s derive line).

- [ ] **Step 2: Write the failing tests**

Create `src/telemetry.rs`:

```rust
use std::collections::VecDeque;
use std::sync::Mutex;

use crate::provider::Provider;

/// One completed request's outcome, kept for external telemetry only —
/// never prompt/response content, never tool call arguments, only names.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
pub struct UsageEvent {
    pub id: u64,
    pub tenant: String,
    pub provider: Provider,
    pub model: Option<String>,
    pub tools_called: Vec<String>,
    pub tokens: u64,
    pub blocked: bool,
    pub block_reason: Option<String>,
    pub timestamp_ms: i64,
}

/// A bounded, mutex-guarded ring buffer of recent `UsageEvent`s. This is
/// NOT a hot-path structure in the sense `SlidingWindowCounter` is — it
/// receives one push per completed request, not per chunk, so a plain
/// `Mutex` is the correct, honest choice here.
pub struct EventLog {
    inner: Mutex<EventLogInner>,
    capacity: usize,
}

struct EventLogInner {
    events: VecDeque<UsageEvent>,
    next_id: u64,
}

impl EventLog {
    pub fn new(capacity: usize) -> Self {
        Self {
            inner: Mutex::new(EventLogInner { events: VecDeque::new(), next_id: 1 }),
            capacity: capacity.max(1),
        }
    }

    /// Assigns the event a fresh monotonic id (overwriting whatever `id`
    /// the caller passed in) and appends it, evicting the oldest event(s)
    /// if the buffer is now over capacity.
    pub fn push(&self, mut event: UsageEvent) {
        let mut inner = self.inner.lock().unwrap();
        event.id = inner.next_id;
        inner.next_id += 1;
        inner.events.push_back(event);
        while inner.events.len() > self.capacity {
            inner.events.pop_front();
        }
    }

    /// Returns events with `id > since`, oldest first, capped at `limit`.
    pub fn since(&self, since: u64, limit: usize) -> Vec<UsageEvent> {
        let inner = self.inner.lock().unwrap();
        inner.events.iter().filter(|e| e.id > since).take(limit).cloned().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sample_event(tenant: &str) -> UsageEvent {
        UsageEvent {
            id: 0, // overwritten by push()
            tenant: tenant.to_string(),
            provider: Provider::OpenAi,
            model: Some("gpt-4o-mini".to_string()),
            tools_called: vec![],
            tokens: 10,
            blocked: false,
            block_reason: None,
            timestamp_ms: 0,
        }
    }

    #[test]
    fn push_assigns_monotonic_ids() {
        let log = EventLog::new(10);
        log.push(sample_event("acct_1"));
        log.push(sample_event("acct_2"));

        let events = log.since(0, 10);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].id, 1);
        assert_eq!(events[1].id, 2);
    }

    #[test]
    fn since_filters_and_limits() {
        let log = EventLog::new(10);
        for i in 0..5 {
            log.push(sample_event(&format!("acct_{i}")));
        }
        let events = log.since(2, 10);
        assert_eq!(events.len(), 3); // ids 3, 4, 5
        assert_eq!(events[0].id, 3);

        let limited = log.since(0, 2);
        assert_eq!(limited.len(), 2);
        assert_eq!(limited[0].id, 1);
        assert_eq!(limited[1].id, 2);
    }

    #[test]
    fn evicts_oldest_when_over_capacity() {
        let log = EventLog::new(3);
        for i in 0..5 {
            log.push(sample_event(&format!("acct_{i}")));
        }
        // Only the last 3 pushed (ids 3, 4, 5) should remain.
        let events = log.since(0, 10);
        assert_eq!(events.len(), 3);
        assert_eq!(events.iter().map(|e| e.id).collect::<Vec<_>>(), vec![3, 4, 5]);
    }
}
```

- [ ] **Step 3: Run tests to verify they fail**

Run: `cargo test --lib telemetry`
Expected: FAIL to compile — `src/telemetry.rs` isn't wired into `lib.rs` yet.

- [ ] **Step 4: Wire the module in**

Modify `src/lib.rs` (add the new line, keep the existing ones — check current alphabetical ordering and insert consistently):

```rust
pub mod telemetry;
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `cargo test --lib telemetry`
Expected: PASS (3 tests)

- [ ] **Step 6: Run provider tests to confirm the Provider derive change didn't break anything**

Run: `cargo test --lib provider`
Expected: PASS — adding derives is additive and shouldn't change any existing behavior, this is a safety check, not expected to find anything.

- [ ] **Step 7: Commit**

```bash
git add src/telemetry.rs src/lib.rs src/provider/mod.rs
git commit -m "feat: add bounded UsageEvent ring buffer (EventLog)"
```

---

### Task 8: Enforcer checks tool policy and emits UsageEvents (streaming path)

**Files:**
- Modify: `src/enforcer.rs`

**Interfaces:**
- Consumes: `NonStreamingCost`/`ChunkCost.tool_calls` (Tasks 4-6), `EventLog`/`UsageEvent` (Task 7), `Provider` (existing).
- Produces: `enforce()`'s signature gains `provider: Provider`, `model: Option<String>`, `blocked_tools: Vec<String>`, `event_log: Arc<EventLog>` parameters. On completion (normal end, budget trip, policy trip, or upstream error) exactly one `UsageEvent` is pushed to `event_log` reflecting the final outcome. Used by Task 9 (`gateway.rs`).

**Ripple note:** this task also updates the two test-only adapters in this file's own test module (`FixedCostAdapter`, `AuthoritativeCostAdapter`) to match `ProviderAdapter`'s new `non_streaming_cost` return type from Task 4 — they can return an empty/default `NonStreamingCost` since neither is ever exercised via that method in this file's existing tests.

- [ ] **Step 1: Write the failing tests**

Read the current `src/enforcer.rs` first — this task rewrites the production code section (everything above `#[cfg(test)]`) and extends the test module; it does not touch the SSE reassembly logic (`SseFrameBuffer`, `find_event_boundary`) at all.

Replace the production code section with:

```rust
use std::sync::Arc;
use bytes::Bytes;
use futures::{Stream, StreamExt};

use crate::budget::BudgetRegistry;
use crate::error::WeirError;
use crate::provider::{Provider, ProviderAdapter};
use crate::telemetry::{EventLog, UsageEvent};

const BUDGET_EXCEEDED_EVENT: &[u8] =
    b"event: error\ndata: {\"error\":\"budget_exceeded\"}\n\n";

fn policy_violation_event(tool: &str) -> Bytes {
    Bytes::from(format!(
        "event: error\ndata: {{\"error\":\"policy_violation\",\"tool\":\"{tool}\"}}\n\n"
    ))
}

/// Buffers raw upstream bytes and yields only complete SSE events (each
/// terminated by a blank line, `"\n\n"`), retaining any trailing partial
/// event across chunk boundaries. `reqwest`'s `bytes_stream()` yields
/// TCP/TLS-sized reads that are not aligned to SSE event boundaries — a
/// `data: {...}` line can be split across two reads under ordinary network
/// conditions. Without reassembly, both halves fail to parse and are
/// silently dropped, undercounting token usage and potentially letting an
/// over-budget chunk through. This buffer guarantees every adapter call
/// receives one complete event, regardless of how the bytes arrived on the
/// wire.
struct SseFrameBuffer {
    buf: Vec<u8>,
}

impl SseFrameBuffer {
    fn new() -> Self {
        Self { buf: Vec::new() }
    }

    fn push(&mut self, chunk: &[u8]) -> Vec<Bytes> {
        self.buf.extend_from_slice(chunk);
        let mut events = Vec::new();
        while let Some(end) = find_event_boundary(&self.buf) {
            let event: Vec<u8> = self.buf.drain(..end).collect();
            events.push(Bytes::from(event));
        }
        events
    }

    fn flush(&mut self) -> Option<Bytes> {
        if self.buf.is_empty() {
            None
        } else {
            Some(Bytes::from(std::mem::take(&mut self.buf)))
        }
    }
}

fn find_event_boundary(buf: &[u8]) -> Option<usize> {
    buf.windows(2).position(|w| w == b"\n\n").map(|i| i + 2)
}

enum EventOutcome {
    Forward(Bytes),
    BudgetTrip,
    PolicyTrip(String),
}

struct EventAccounting<'a> {
    adapter: &'a mut dyn ProviderAdapter,
    budget: &'a BudgetRegistry,
    tenant: &'a str,
    blocked_tools: &'a [String],
    recorded_so_far: &'a mut u64,
    tools_seen: &'a mut Vec<String>,
}

fn process_event(acc: &mut EventAccounting, event: &Bytes, now_ms: i64) -> Result<EventOutcome, WeirError> {
    let cost = acc.adapter.chunk_cost(event);

    for tool in &cost.tool_calls {
        if !acc.tools_seen.contains(tool) {
            acc.tools_seen.push(tool.clone());
        }
        if acc.blocked_tools.contains(tool) {
            return Ok(EventOutcome::PolicyTrip(tool.clone()));
        }
    }

    let delta = match cost.authoritative_total {
        Some(total) => {
            let delta = total.saturating_sub(*acc.recorded_so_far);
            *acc.recorded_so_far = total;
            delta
        }
        None => {
            *acc.recorded_so_far += cost.estimated_tokens;
            cost.estimated_tokens
        }
    };

    let within_budget = acc.budget.record(acc.tenant, delta, now_ms)?;

    Ok(if within_budget {
        EventOutcome::Forward(event.clone())
    } else {
        EventOutcome::BudgetTrip
    })
}

/// Wraps an upstream SSE byte stream, enforcing the tenant's token budget
/// and tool policy event by event. Raw upstream reads are first
/// reassembled into complete SSE events (see `SseFrameBuffer`), so
/// accounting and forwarding decisions are never made against a partial
/// frame. Each event's cost is recorded against the budget, and its tool
/// calls checked against policy, BEFORE it is yielded; an event that would
/// breach the budget or invoke a blocked tool is never forwarded — a
/// terminal SSE error event is yielded instead and the stream ends. On
/// completion (however it ends) exactly one `UsageEvent` is pushed to
/// `event_log`.
#[allow(clippy::too_many_arguments)]
pub fn enforce(
    tenant: String,
    provider: Provider,
    model: Option<String>,
    mut upstream: impl Stream<Item = reqwest::Result<Bytes>> + Unpin + Send + 'static,
    mut adapter: Box<dyn ProviderAdapter>,
    budget: Arc<BudgetRegistry>,
    blocked_tools: Vec<String>,
    event_log: Arc<EventLog>,
    now_ms: impl Fn() -> i64 + Send + 'static,
) -> impl Stream<Item = Result<Bytes, WeirError>> {
    async_stream::stream! {
        let mut recorded_so_far: u64 = 0;
        let mut tools_seen: Vec<String> = Vec::new();
        let mut frames = SseFrameBuffer::new();

        macro_rules! emit_and_return {
            ($blocked:expr, $reason:expr) => {{
                event_log.push(UsageEvent {
                    id: 0,
                    tenant: tenant.clone(),
                    provider,
                    model: model.clone(),
                    tools_called: tools_seen.clone(),
                    tokens: recorded_so_far,
                    blocked: $blocked,
                    block_reason: $reason,
                    timestamp_ms: now_ms(),
                });
                return;
            }};
        }

        while let Some(chunk_res) = upstream.next().await {
            let raw = match chunk_res {
                Ok(raw) => raw,
                Err(e) => {
                    yield Err(WeirError::Upstream(e));
                    emit_and_return!(true, Some("upstream_error".to_string()));
                }
            };

            for event in frames.push(&raw) {
                let mut acc = EventAccounting {
                    adapter: adapter.as_mut(),
                    budget: &budget,
                    tenant: &tenant,
                    blocked_tools: &blocked_tools,
                    recorded_so_far: &mut recorded_so_far,
                    tools_seen: &mut tools_seen,
                };
                match process_event(&mut acc, &event, now_ms()) {
                    Ok(EventOutcome::Forward(bytes)) => yield Ok(bytes),
                    Ok(EventOutcome::BudgetTrip) => {
                        yield Ok(Bytes::from_static(BUDGET_EXCEEDED_EVENT));
                        emit_and_return!(true, Some("budget_exceeded".to_string()));
                    }
                    Ok(EventOutcome::PolicyTrip(tool)) => {
                        yield Ok(policy_violation_event(&tool));
                        emit_and_return!(true, Some(format!("blocked_tool:{tool}")));
                    }
                    Err(e) => {
                        yield Err(e);
                        emit_and_return!(true, Some("error".to_string()));
                    }
                }
            }
        }

        if let Some(event) = frames.flush() {
            let mut acc = EventAccounting {
                adapter: adapter.as_mut(),
                budget: &budget,
                tenant: &tenant,
                blocked_tools: &blocked_tools,
                recorded_so_far: &mut recorded_so_far,
                tools_seen: &mut tools_seen,
            };
            match process_event(&mut acc, &event, now_ms()) {
                Ok(EventOutcome::Forward(bytes)) => yield Ok(bytes),
                Ok(EventOutcome::BudgetTrip) => yield Ok(Bytes::from_static(BUDGET_EXCEEDED_EVENT)),
                Ok(EventOutcome::PolicyTrip(tool)) => yield Ok(policy_violation_event(&tool)),
                Err(e) => yield Err(e),
            }
        }

        emit_and_return!(false, None);
    }
}
```

Now update the test module. Read the current test module first. The two test-only adapters need a `non_streaming_cost` stub added to satisfy the trait (Task 4 changed its return type):

```rust
    impl ProviderAdapter for FixedCostAdapter {
        fn chunk_cost(&mut self, _raw: &Bytes) -> ChunkCost {
            ChunkCost { estimated_tokens: self.cost_per_chunk, authoritative_total: None, tool_calls: Vec::new() }
        }

        fn non_streaming_cost(&self, _body: &Bytes) -> crate::provider::NonStreamingCost {
            crate::provider::NonStreamingCost { total_tokens: None, tool_calls: Vec::new() }
        }
    }
```

```rust
    impl ProviderAdapter for AuthoritativeCostAdapter {
        fn chunk_cost(&mut self, _raw: &Bytes) -> ChunkCost {
            let total = self.totals.pop_front().unwrap_or(0);
            ChunkCost { estimated_tokens: 0, authoritative_total: Some(total), tool_calls: Vec::new() }
        }

        fn non_streaming_cost(&self, _body: &Bytes) -> crate::provider::NonStreamingCost {
            crate::provider::NonStreamingCost { total_tokens: None, tool_calls: Vec::new() }
        }
    }
```

Every existing call to `enforce(...)` in this test module needs 4 new arguments inserted: `Provider::OpenAi` (or whichever), `None` (no model asserted in these tests), `Vec::new()` (no blocked tools), and `Arc::new(EventLog::new(100))` (a fresh log per test). For example, the existing:
```rust
        let out: Vec<_> = enforce("acct_1".into(), upstream, adapter, budget, || 0)
            .collect()
            .await;
```
becomes:
```rust
        let out: Vec<_> = enforce(
            "acct_1".into(),
            Provider::OpenAi,
            None,
            upstream,
            adapter,
            budget,
            Vec::new(),
            Arc::new(EventLog::new(100)),
            || 0,
        )
        .collect()
        .await;
```
Apply this to all 5 existing tests (`forwards_chunks_within_budget`, `trips_before_forwarding_over_budget_chunk`, `authoritative_total_reconciles_via_delta_not_double_count`, `reassembles_sse_event_split_across_multiple_raw_chunks`, `multiple_events_in_one_raw_chunk_are_processed_independently`). Add `use crate::telemetry::EventLog;` to the test module's imports.

Add two new tests exercising policy tripping and event emission:

```rust
    #[tokio::test]
    async fn trips_on_blocked_tool_and_never_forwards_it() {
        struct ToolCallAdapter;
        impl ProviderAdapter for ToolCallAdapter {
            fn chunk_cost(&mut self, _raw: &Bytes) -> ChunkCost {
                ChunkCost {
                    estimated_tokens: 1,
                    authoritative_total: None,
                    tool_calls: vec!["send_email".to_string()],
                }
            }
            fn non_streaming_cost(&self, _body: &Bytes) -> crate::provider::NonStreamingCost {
                crate::provider::NonStreamingCost { total_tokens: None, tool_calls: Vec::new() }
            }
        }

        let upstream = futures::stream::iter(vec![Ok(Bytes::from_static(b"chunk1\n\n"))]);
        let adapter: Box<dyn ProviderAdapter> = Box::new(ToolCallAdapter);
        let budget = budget_with("acct_1", 1000); // plenty of budget — this must trip on policy, not budget
        let event_log = Arc::new(EventLog::new(100));

        let out: Vec<_> = enforce(
            "acct_1".into(),
            Provider::OpenAi,
            Some("gpt-4o-mini".to_string()),
            upstream,
            adapter,
            budget,
            vec!["send_email".to_string()],
            event_log.clone(),
            || 0,
        )
        .collect()
        .await;

        assert_eq!(out.len(), 1);
        let event = out[0].as_ref().unwrap();
        assert!(String::from_utf8_lossy(event).contains("policy_violation"));

        let events = event_log.since(0, 10);
        assert_eq!(events.len(), 1);
        assert!(events[0].blocked);
        assert_eq!(events[0].block_reason.as_deref(), Some("blocked_tool:send_email"));
        assert_eq!(events[0].tools_called, vec!["send_email".to_string()]);
    }

    #[tokio::test]
    async fn successful_stream_emits_one_unblocked_usage_event() {
        let upstream = futures::stream::iter(vec![
            Ok(Bytes::from_static(b"chunk1\n\n")),
            Ok(Bytes::from_static(b"chunk2\n\n")),
        ]);
        let adapter: Box<dyn ProviderAdapter> = Box::new(FixedCostAdapter { cost_per_chunk: 10 });
        let budget = budget_with("acct_1", 1000);
        let event_log = Arc::new(EventLog::new(100));

        let _: Vec<_> = enforce(
            "acct_1".into(),
            Provider::OpenAi,
            Some("gpt-4o-mini".to_string()),
            upstream,
            adapter,
            budget,
            Vec::new(),
            event_log.clone(),
            || 0,
        )
        .collect()
        .await;

        let events = event_log.since(0, 10);
        assert_eq!(events.len(), 1, "exactly one UsageEvent per completed stream, not one per chunk");
        assert!(!events[0].blocked);
        assert_eq!(events[0].tokens, 20);
        assert_eq!(events[0].model.as_deref(), Some("gpt-4o-mini"));
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib enforcer`
Expected: FAIL to compile (signature mismatches throughout, before applying the above).

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test --lib enforcer`
Expected: PASS (7 tests: 5 existing updated + 2 new)

- [ ] **Step 4: Commit**

```bash
git add src/enforcer.rs
git commit -m "feat: enforcer checks tool policy and emits UsageEvents"
```

---

### Task 9: Gateway — model blocking, non-streaming tool blocking, /events route

**Files:**
- Modify: `src/gateway.rs`

**Interfaces:**
- Consumes: `enforce()`'s new signature (Task 8), `BudgetRegistry::policy_for` (Task 3), `NonStreamingCost` (Task 4), `EventLog`/`UsageEvent` (Task 7).
- Produces: `AppState` gains `pub events: Arc<EventLog>`. New route `GET /events`. Model-blocking check before any upstream call. Non-streaming responses check `tool_calls` against policy in addition to budget. Used by Task 10 (`main.rs`).

- [ ] **Step 1: Write the failing tests**

Read the current `src/gateway.rs` first — this task modifies `router()`, `proxy()`, and adds new helpers; it does not touch `is_hop_by_hop`/`should_forward_request_header`/`with_connection_close` at all.

Add `pub events: Arc<EventLog>` to the `AppState` struct, and add the imports it needs:
```rust
use crate::telemetry::{EventLog, UsageEvent};
```

Add the new route to `router()` — insert alongside the existing `/healthz`/`/openai/*rest`/`/anthropic/*rest` routes, before `.layer(DefaultBodyLimit::max(...))`:
```rust
        .route("/events", get(events_handler))
```
(`get` is already imported from `axum::routing`.)

Add the handler function (near `proxy`/`with_connection_close`):
```rust
#[derive(serde::Deserialize)]
struct EventsQuery {
    since: Option<u64>,
    limit: Option<usize>,
}

async fn events_handler(
    State(state): State<AppState>,
    axum::extract::Query(query): axum::extract::Query<EventsQuery>,
) -> axum::Json<Vec<UsageEvent>> {
    let since = query.since.unwrap_or(0);
    let limit = query.limit.unwrap_or(100).min(1000);
    axum::Json(state.events.since(since, limit))
}
```
`UsageEvent` already derives `Serialize`/`Deserialize` from Task 7 — no further change needed there. Add `axum::extract::Query` to the top-level `use axum::extract::{...}` import list in `gateway.rs`.

Add a model-name extraction helper:
```rust
fn extract_model_name(body: &Bytes) -> Option<String> {
    #[derive(serde::Deserialize)]
    struct ModelField {
        model: Option<String>,
    }
    serde_json::from_slice::<ModelField>(body).ok()?.model
}
```

Now rewrite `proxy()`. Read the current full function first — this replaces it in its entirety:

```rust
async fn proxy(
    State(state): State<AppState>,
    headers: HeaderMap,
    method: Method,
    Path(rest): Path<String>,
    body: Bytes,
    provider: Provider,
) -> Response {
    let tenant = match headers.get(TENANT_HEADER).and_then(|v| v.to_str().ok()) {
        Some(t) => t.to_string(),
        None => return WeirError::UnknownTenant.into_response(),
    };

    let now = now_ms();
    match state.budget.is_within_budget(&tenant, now) {
        Ok(true) => {}
        Ok(false) => {
            return with_connection_close(WeirError::BudgetExceeded(tenant).into_response())
        }
        Err(e) => return with_connection_close(e.into_response()),
    }

    let policy = match state.budget.policy_for(&tenant) {
        Ok(p) => p,
        Err(e) => return with_connection_close(e.into_response()),
    };

    let model = extract_model_name(&body);
    if let Some(model_name) = &model {
        if policy.blocked_models.contains(model_name) {
            state.events.push(UsageEvent {
                id: 0,
                tenant: tenant.clone(),
                provider,
                model: model.clone(),
                tools_called: Vec::new(),
                tokens: 0,
                blocked: true,
                block_reason: Some(format!("blocked_model:{model_name}")),
                timestamp_ms: now_ms(),
            });
            return with_connection_close(
                WeirError::PolicyViolation {
                    tenant,
                    reason: format!("blocked_model:{model_name}"),
                }
                .into_response(),
            );
        }
    }

    let base = match provider {
        Provider::OpenAi => &state.openai_base,
        Provider::Anthropic => &state.anthropic_base,
    };
    let url = format!("{base}/{rest}");

    let mut upstream_req = state.http.request(method, &url).body(body);
    for (name, value) in headers.iter() {
        if should_forward_request_header(name) {
            upstream_req = upstream_req.header(name, value);
        }
    }

    let upstream_res = match upstream_req.send().await {
        Ok(res) => res,
        Err(e) => return with_connection_close(WeirError::Upstream(e).into_response()),
    };

    let status = upstream_res.status();
    let upstream_headers = upstream_res.headers().clone();
    // Real OpenAI/Anthropic responses always set Content-Type, so this
    // check is reliable in practice. A response with no Content-Type at
    // all falls to the non-streaming path below; if it were actually a
    // stream, that response is buffered whole and forwarded correctly but
    // without incremental delivery or enforcement for that one response,
    // since non_streaming_cost can't parse SSE text as JSON. This is a
    // known, low-likelihood edge against compliant providers, not a case
    // worth adding stream-sniffing complexity for.
    let is_streaming = upstream_headers
        .get(axum::http::header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .map(|ct| ct.starts_with("text/event-stream"))
        .unwrap_or(false);

    if is_streaming {
        let adapter = state.tokenizer.new_adapter(provider);
        let stream = enforcer::enforce(
            tenant,
            provider,
            model,
            upstream_res.bytes_stream(),
            adapter,
            state.budget.clone(),
            policy.blocked_tools,
            state.events.clone(),
            now_ms,
        );

        let mut response_builder = Response::builder().status(status);
        for (name, value) in upstream_headers.iter() {
            if !is_hop_by_hop(name) {
                response_builder = response_builder.header(name, value);
            }
        }

        let mut response = response_builder.body(Body::from_stream(stream)).unwrap();
        response.headers_mut().insert(
            HeaderName::from_static("connection"),
            HeaderValue::from_static("close"),
        );
        return response;
    }

    // Non-streaming: the whole response is one atomic unit, and it always
    // carries its own authoritative usage — no bytes have reached the
    // client yet, so we buffer the full body, check its tool calls against
    // policy and record its token usage, and only forward it if both
    // checks pass. A response that violates policy or exceeds budget is
    // rejected outright (a real error status, not a mid-stream trip)
    // rather than delivered to the client.
    let body_bytes = match upstream_res.bytes().await {
        Ok(b) => b,
        Err(e) => return with_connection_close(WeirError::Upstream(e).into_response()),
    };

    let adapter = state.tokenizer.new_adapter(provider);
    let cost = adapter.non_streaming_cost(&body_bytes);

    for tool in &cost.tool_calls {
        if policy.blocked_tools.contains(tool) {
            state.events.push(UsageEvent {
                id: 0,
                tenant: tenant.clone(),
                provider,
                model: model.clone(),
                tools_called: cost.tool_calls.clone(),
                tokens: cost.total_tokens.unwrap_or(0),
                blocked: true,
                block_reason: Some(format!("blocked_tool:{tool}")),
                timestamp_ms: now_ms(),
            });
            return with_connection_close(
                WeirError::PolicyViolation {
                    tenant,
                    reason: format!("blocked_tool:{tool}"),
                }
                .into_response(),
            );
        }
    }

    if let Some(total) = cost.total_tokens {
        match state.budget.record(&tenant, total, now_ms()) {
            Ok(true) => {}
            Ok(false) => {
                state.events.push(UsageEvent {
                    id: 0,
                    tenant: tenant.clone(),
                    provider,
                    model: model.clone(),
                    tools_called: cost.tool_calls.clone(),
                    tokens: total,
                    blocked: true,
                    block_reason: Some("budget_exceeded".to_string()),
                    timestamp_ms: now_ms(),
                });
                return with_connection_close(WeirError::BudgetExceeded(tenant).into_response());
            }
            Err(e) => return with_connection_close(e.into_response()),
        }
    }

    state.events.push(UsageEvent {
        id: 0,
        tenant,
        provider,
        model,
        tools_called: cost.tool_calls,
        tokens: cost.total_tokens.unwrap_or(0),
        blocked: false,
        block_reason: None,
        timestamp_ms: now_ms(),
    });

    let mut response_builder = Response::builder().status(status);
    for (name, value) in upstream_headers.iter() {
        if !is_hop_by_hop(name) {
            response_builder = response_builder.header(name, value);
        }
    }
    let mut response = response_builder.body(Body::from(body_bytes)).unwrap();
    response.headers_mut().insert(
        HeaderName::from_static("connection"),
        HeaderValue::from_static("close"),
    );
    response
}
```

Now update the test module. Read it first. Every `state_with_tenant` call site is unaffected in shape, but the helper itself must now also populate `events`:
```rust
    fn state_with_tenant(tenant: &str, max_tokens: u64) -> AppState {
        let mut limits: TenantLimits = HashMap::new();
        limits.insert(
            tenant.to_string(),
            BudgetLimit { max_tokens, window: Duration::from_secs(60) },
        );
        let parsed = crate::config::ParsedConfig { limits, policies: HashMap::new() };
        AppState {
            budget: Arc::new(BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(parsed)))),
            tokenizer: Arc::new(Tokenizer::load()),
            http: reqwest::Client::new(),
            openai_base: "http://127.0.0.1:1".into(),
            anthropic_base: "http://127.0.0.1:1".into(),
            events: Arc::new(EventLog::new(1000)),
        }
    }
```
Add `use crate::telemetry::EventLog;` to the test module's imports. Every existing test that already compiled against the old `state_with_tenant` shape needs no other change — they all go through this one helper.

Add three new tests:

```rust
    #[tokio::test]
    async fn blocked_model_is_rejected_before_any_upstream_call() {
        let mut state = state_with_tenant("acct_1", 1000);
        // Point at an address nothing is listening on — if the request
        // ever reached this point, the test would hang/error on connect,
        // proving the block happened before any upstream call.
        state.openai_base = "http://127.0.0.1:1".into();

        let mut limits = HashMap::new();
        limits.insert(
            "acct_1".to_string(),
            BudgetLimit { max_tokens: 1000, window: Duration::from_secs(60) },
        );
        let mut policies = HashMap::new();
        policies.insert(
            "acct_1".to_string(),
            crate::config::PolicyConfig {
                blocked_models: vec!["gpt-3.5-turbo".to_string()],
                blocked_tools: Vec::new(),
            },
        );
        let parsed = crate::config::ParsedConfig { limits, policies };
        state.budget = Arc::new(BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(parsed))));

        let app = router(state);
        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openai/v1/chat/completions")
                    .method("POST")
                    .header(TENANT_HEADER, "acct_1")
                    .body(AxumBody::from(r#"{"model":"gpt-3.5-turbo"}"#))
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn events_endpoint_returns_pushed_events() {
        let state = state_with_tenant("acct_1", 1000);
        state.events.push(UsageEvent {
            id: 0,
            tenant: "acct_1".to_string(),
            provider: Provider::OpenAi,
            model: Some("gpt-4o-mini".to_string()),
            tools_called: Vec::new(),
            tokens: 10,
            blocked: false,
            block_reason: None,
            timestamp_ms: 0,
        });
        let app = router(state);

        let response = app
            .oneshot(Request::builder().uri("/events").body(AxumBody::empty()).unwrap())
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        let events: Vec<UsageEvent> = serde_json::from_slice(&body).unwrap();
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].tenant, "acct_1");
    }

    #[tokio::test]
    async fn non_streaming_blocked_tool_is_rejected_not_forwarded() {
        use wiremock::matchers::{method, path};
        use wiremock::{Mock, MockServer, ResponseTemplate};

        let mock = MockServer::start().await;
        Mock::given(method("POST"))
            .and(path("/v1/chat/completions"))
            .respond_with(ResponseTemplate::new(200).set_body_raw(
                "{\"choices\":[{\"message\":{\"content\":null,\"tool_calls\":[{\"id\":\"call_1\",\"type\":\"function\",\"function\":{\"name\":\"send_email\",\"arguments\":\"{}\"}}]}}],\"usage\":{\"prompt_tokens\":5,\"completion_tokens\":2,\"total_tokens\":7}}",
                "application/json",
            ))
            .mount(&mock)
            .await;

        let mut limits = HashMap::new();
        limits.insert(
            "acct_1".to_string(),
            BudgetLimit { max_tokens: 1000, window: Duration::from_secs(60) },
        );
        let mut policies = HashMap::new();
        policies.insert(
            "acct_1".to_string(),
            crate::config::PolicyConfig {
                blocked_models: Vec::new(),
                blocked_tools: vec!["send_email".to_string()],
            },
        );
        let parsed = crate::config::ParsedConfig { limits, policies };

        let mut state = state_with_tenant("acct_1", 1000);
        state.budget = Arc::new(BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(parsed))));
        state.openai_base = mock.uri();
        let app = router(state);

        let response = app
            .oneshot(
                Request::builder()
                    .uri("/openai/v1/chat/completions")
                    .method("POST")
                    .header(TENANT_HEADER, "acct_1")
                    .body(AxumBody::empty())
                    .unwrap(),
            )
            .await
            .unwrap();

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert!(
            !String::from_utf8_lossy(&body).contains("send_email"),
            "the blocked tool's presence must not leak the underlying response content to the client"
        );
    }
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --lib gateway`
Expected: FAIL to compile (before applying the above).

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test --lib gateway`
Expected: PASS (10 tests: 7 existing + 3 new)

- [ ] **Step 4: Commit**

```bash
git add src/gateway.rs src/telemetry.rs
git commit -m "feat: gateway enforces model/tool policy and exposes GET /events"
```

---

### Task 10: Main wiring — EventLog construction

**Files:**
- Modify: `src/main.rs`

**Interfaces:**
- Consumes: `EventLog` (Task 7), `AppState.events` (Task 9), `SharedConfig`/`ParsedConfig` (Task 1).

- [ ] **Step 1: Apply the change**

Read the current `src/main.rs` first (it already has the graceful-shutdown addition from a previous fix — do not remove `shutdown_signal`/the `.with_graceful_shutdown(...)` call). Add the import:
```rust
use weir::telemetry::EventLog;
```
Add a new env-configurable capacity right after the existing `config_path` resolution:
```rust
    let event_log_capacity: usize = env::var("WEIR_EVENT_LOG_CAPACITY")
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(10_000);
```
Add `events: Arc::new(EventLog::new(event_log_capacity)),` as a new field in the `AppState { ... }` construction (alongside the existing `budget`, `tokenizer`, `http`, `openai_base`, `anthropic_base` fields).

- [ ] **Step 2: Verify it builds**

Run: `cargo build`
Expected: builds with no errors. This is the point in the plan where the WHOLE crate compiles again after Tasks 1-9's incremental, individually-non-compiling steps.

- [ ] **Step 3: Verify the full test suite passes**

Run: `cargo test`
Expected: all lib tests, `proxy_flow_test`, and `budget_concurrency_test` pass. This is the first point since Task 1 where the full suite can run — if anything unrelated to this plan's changes fails, stop and report NEEDS_CONTEXT rather than guessing at a fix.

- [ ] **Step 4: Commit**

```bash
git add src/main.rs
git commit -m "feat: wire EventLog into AppState with configurable capacity"
```

---

### Task 11: Example config and README updates

**Files:**
- Modify: `weir.example.toml`
- Modify: `README.md`

**Interfaces:** none — documentation/example only.

- [ ] **Step 1: Update the example config**

Read the current `weir.example.toml`. Add a policy example to one of the two existing tenants:

```toml
[tenants.acct_123]
max_tokens = 50000
window_seconds = 60

[tenants.acct_123.policy]
blocked_models = ["gpt-3.5-turbo"]
blocked_tools = ["send_email", "execute_shell"]

[tenants.acct_456]
max_tokens = 200000
window_seconds = 3600
```

- [ ] **Step 2: Update the README**

Read the current `README.md`. In the "Configuration" section, after the existing `weir.toml` example, add a short paragraph and example covering the new `policy` block — mention that `blocked_models` rejects a request before any upstream call, `blocked_tools` trips mid-stream (or rejects a non-streaming response) the same way a budget overrun does, and that omitting `policy` entirely means no restrictions beyond budget. Also add a one-line mention of the new `GET /events?since=&limit=` endpoint alongside the existing description of `/stats`-equivalent behavior (note: `/stats` itself does not exist as of this plan — only add documentation for what this plan actually built: `/events`. Do not invent or reference a `/stats` endpoint that doesn't exist in the codebase.).

- [ ] **Step 3: Commit**

```bash
git add weir.example.toml README.md
git commit -m "docs: document policy config and /events endpoint"
```

---

### Task 12: End-to-end integration tests

**Files:**
- Modify: `tests/proxy_flow_test.rs`

**Interfaces:**
- Consumes: everything built in Tasks 1-10, exercised through the real `router()`/`AppState` stack against a wiremock upstream (matching this file's existing pattern).

- [ ] **Step 1: Write the failing tests**

Read the current `tests/proxy_flow_test.rs` in full first. Add two new tests to the file (do not modify the existing three):

```rust
#[tokio::test]
async fn streaming_response_with_blocked_tool_trips_and_is_never_forwarded() {
    let mock = MockServer::start().await;
    let sse_body = "data: {\"choices\":[{\"delta\":{\"tool_calls\":[{\"index\":0,\"function\":{\"name\":\"send_email\",\"arguments\":\"{}\"}}]}}]}\n\n\
data: {\"choices\":[{\"delta\":{\"content\":\"should never be forwarded\"}}]}\n\n";
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(sse_body, "text/event-stream"))
        .mount(&mock)
        .await;

    let mut limits = HashMap::new();
    limits.insert(
        "acct_1".to_string(),
        BudgetLimit { max_tokens: 1000, window: Duration::from_secs(60) },
    );
    let mut policies = HashMap::new();
    policies.insert(
        "acct_1".to_string(),
        weir::config::PolicyConfig {
            blocked_models: Vec::new(),
            blocked_tools: vec!["send_email".to_string()],
        },
    );
    let parsed = weir::config::ParsedConfig { limits, policies };

    let state = AppState {
        budget: Arc::new(BudgetRegistry::new(Arc::new(arc_swap::ArcSwap::from_pointee(parsed)))),
        tokenizer: Arc::new(Tokenizer::load()),
        http: reqwest::Client::new(),
        openai_base: mock.uri(),
        anthropic_base: mock.uri(),
        events: Arc::new(weir::telemetry::EventLog::new(100)),
    };
    let app = router(state);

    let response = app
        .oneshot(
            Request::builder()
                .uri("/openai/v1/chat/completions")
                .method("POST")
                .header("x-weir-tenant", "acct_1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    assert_eq!(response.status(), StatusCode::OK); // headers already committed before the trip
    let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
    let body = String::from_utf8_lossy(&body);
    assert!(body.contains("policy_violation"));
    assert!(!body.contains("should never be forwarded"));
}

#[tokio::test]
async fn events_endpoint_reflects_a_completed_request() {
    let mock = MockServer::start().await;
    let sse_body = "data: {\"choices\":[{\"delta\":{\"content\":\"Hi\"}}]}\n\n\
data: {\"choices\":[{\"delta\":{}}],\"usage\":{\"prompt_tokens\":1,\"completion_tokens\":1,\"total_tokens\":2}}\n\n";
    Mock::given(method("POST"))
        .and(path("/v1/chat/completions"))
        .respond_with(ResponseTemplate::new(200).set_body_raw(sse_body, "text/event-stream"))
        .mount(&mock)
        .await;

    let mut limits = HashMap::new();
    limits.insert(
        "acct_1".to_string(),
        BudgetLimit { max_tokens: 1000, window: Duration::from_secs(60) },
    );
    let parsed = weir::config::ParsedConfig { limits, policies: HashMap::new() };

    let state = AppState {
        budget: Arc::new(BudgetRegistry::new(Arc::new(arc_swap::ArcSwap::from_pointee(parsed)))),
        tokenizer: Arc::new(Tokenizer::load()),
        http: reqwest::Client::new(),
        openai_base: mock.uri(),
        anthropic_base: mock.uri(),
        events: Arc::new(weir::telemetry::EventLog::new(100)),
    };
    let app = router(state);

    let _ = app
        .clone()
        .oneshot(
            Request::builder()
                .uri("/openai/v1/chat/completions")
                .method("POST")
                .header("x-weir-tenant", "acct_1")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();

    let events_response = app
        .oneshot(Request::builder().uri("/events").body(Body::empty()).unwrap())
        .await
        .unwrap();
    assert_eq!(events_response.status(), StatusCode::OK);
    let body = to_bytes(events_response.into_body(), usize::MAX).await.unwrap();
    let events: Vec<weir::telemetry::UsageEvent> = serde_json::from_slice(&body).unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].tenant, "acct_1");
    assert!(!events[0].blocked);
}
```

Check the top of the file for existing `use` statements (`weir::budget::BudgetRegistry`, `weir::config::{BudgetLimit, TenantLimits}`, `weir::gateway::{router, AppState}`, `weir::provider::Tokenizer`, `std::collections::HashMap`, `std::time::Duration`, `std::sync::Arc`, wiremock imports, `axum::body::{to_bytes, Body}`, `axum::http::{Request, StatusCode}`, `tower::ServiceExt`) and add only what's missing (`weir::config::PolicyConfig`/`ParsedConfig` and `weir::telemetry::{EventLog, UsageEvent}` are referenced above via fully-qualified paths in most spots to avoid import-list churn — keep it that way rather than adding more top-level `use` lines, to minimize risk of colliding with an existing name).

- [ ] **Step 2: Run tests to verify they fail**

Run: `cargo test --test proxy_flow_test`
Expected: FAIL — either compile errors if a type path is wrong, or (once compiling) a failing assertion if the policy-trip/event-recording behavior has a gap. Investigate and fix precisely per which failure mode you hit.

- [ ] **Step 3: Run tests to verify they pass**

Run: `cargo test --test proxy_flow_test`
Expected: PASS (5 tests: 3 existing + 2 new)

- [ ] **Step 4: Run the full suite one more time**

Run: `cargo test`
Expected: all pass — this is the final confirmation the whole plan's work is internally consistent.

- [ ] **Step 5: Commit**

```bash
git add tests/proxy_flow_test.rs
git commit -m "test: add end-to-end policy blocking and /events integration tests"
```

---

## Self-Review Notes

- **Spec coverage:** local-config policy parsing (Task 1), model blocking at admission (Task 9), tool blocking via the existing adapter/enforcer pipeline for both streaming (Task 8) and non-streaming (Task 9) paths, bounded event log with `/events` (Tasks 7, 9), privacy line (only tool *names* ever captured — checked in Tasks 4-6's struct designs, never an argument field). ROI tracking is explicitly out of scope per the design spec and has no task here.
- **Type consistency:** `ChunkCost`/`NonStreamingCost` (Task 4) are defined once and consumed identically by both adapters (Tasks 5, 6) and the enforcer/gateway (Tasks 8, 9). `EventLog`/`UsageEvent` (Task 7) are defined once and used identically by the enforcer (Task 8) and gateway (Task 9) — checked field-by-field while writing this plan.
- **Known, accepted scope boundary:** policy is local-config-only, matching the design spec's explicit MVP cut-line — no task here builds remote/SaaS-managed policy push-down.
- **Known ripple, stated explicitly:** this plan changes the return type of `parse`/`load_from_file`/`load_shared` (Task 1), `SharedConfig`'s generic parameter (Task 1), `BudgetRegistry::new`'s parameter type (Task 3), and `ProviderAdapter::non_streaming_cost`'s return type (Task 4) — all previously-shipped, already-reviewed interfaces. Every task that depends on one of these ripples states exactly what it needs to update and why, rather than leaving it implicit.
