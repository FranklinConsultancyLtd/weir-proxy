pub mod sliding_window;

use std::sync::Arc;
use dashmap::DashMap;

use crate::config::{BudgetLimit, PolicyConfig, SharedConfig};
use crate::error::SymfynityError;
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

    fn limit_for(&self, tenant: &str) -> Result<BudgetLimit, SymfynityError> {
        self.config
            .load()
            .limits
            .get(tenant)
            .copied()
            .ok_or(SymfynityError::UnknownTenant)
    }

    /// Returns the tenant's configured policy (blocked models/tools). An
    /// unknown tenant is the same error `limit_for` returns for the same
    /// reason — both are reading the same underlying config snapshot.
    pub fn policy_for(&self, tenant: &str) -> Result<PolicyConfig, SymfynityError> {
        self.config
            .load()
            .policies
            .get(tenant)
            .cloned()
            .ok_or(SymfynityError::UnknownTenant)
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

    pub fn is_within_budget(&self, tenant: &str, now_ms: i64) -> Result<bool, SymfynityError> {
        let limit = self.limit_for(tenant)?;
        let counter = self.counter_for(tenant, limit);
        Ok(counter.estimate(now_ms) < limit.max_tokens)
    }

    /// A chunk that brings the tenant's usage to exactly `max_tokens` is
    /// still allowed through (`<=`); the tenant is blocked starting with
    /// the *next* admission check once genuinely at or over the ceiling
    /// (`is_within_budget` uses strict `<`). This lets a tenant consume its
    /// full budget rather than being cut off one token short of it.
    pub fn record(&self, tenant: &str, amount: u64, now_ms: i64) -> Result<bool, SymfynityError> {
        let limit = self.limit_for(tenant)?;
        let counter = self.counter_for(tenant, limit);
        let total = counter.add(amount, now_ms);
        Ok(total <= limit.max_tokens)
    }
}

#[cfg(test)]
mod registry_tests {
    use super::*;
    use std::collections::HashMap;
    use std::time::Duration;
    use arc_swap::ArcSwap;
    use crate::config::ParsedConfig;

    fn registry_with(tenant: &str, max_tokens: u64, window_secs: u64) -> BudgetRegistry {
        let mut limits = HashMap::new();
        limits.insert(
            tenant.to_string(),
            BudgetLimit { max_tokens, window: Duration::from_secs(window_secs) },
        );
        let parsed = ParsedConfig { limits, policies: HashMap::new() };
        BudgetRegistry::new(Arc::new(ArcSwap::from_pointee(parsed)))
    }

    #[test]
    fn unknown_tenant_is_rejected() {
        let registry = registry_with("acct_1", 1000, 60);
        let result = registry.is_within_budget("acct_unknown", 0);
        assert!(matches!(result, Err(SymfynityError::UnknownTenant)));
    }

    #[test]
    fn records_and_trips_at_ceiling() {
        let registry = registry_with("acct_1", 100, 60);
        assert!(registry.record("acct_1", 60, 0).unwrap());
        assert!(registry.record("acct_1", 30, 0).unwrap()); // 90, still within
        assert!(!registry.record("acct_1", 20, 0).unwrap()); // 110, over
    }

    #[test]
    fn record_allows_exact_ceiling_then_blocks_next_admission() {
        let registry = registry_with("acct_1", 100, 60);
        assert!(registry.record("acct_1", 100, 0).unwrap()); // lands exactly at ceiling: allowed
        assert!(!registry.is_within_budget("acct_1", 0).unwrap()); // now at ceiling: blocked
    }

    #[test]
    fn changing_window_recreates_counter_and_resets_usage() {
        let tenant = "acct_1";
        let mut limits = HashMap::new();
        limits.insert(
            tenant.to_string(),
            BudgetLimit { max_tokens: 100, window: Duration::from_secs(60) },
        );
        let parsed = ParsedConfig { limits, policies: HashMap::new() };
        let shared = Arc::new(ArcSwap::from_pointee(parsed));
        let registry = BudgetRegistry::new(shared.clone());

        registry.record(tenant, 50, 0).unwrap();
        assert!(registry.is_within_budget(tenant, 0).unwrap());

        // Change the tenant's window — the stale counter must be
        // recreated, not silently kept alive against its old window.
        let mut new_limits = HashMap::new();
        new_limits.insert(
            tenant.to_string(),
            BudgetLimit { max_tokens: 100, window: Duration::from_secs(120) },
        );
        let new_parsed = ParsedConfig { limits: new_limits, policies: HashMap::new() };
        shared.store(Arc::new(new_parsed));

        // Usage resets because the window changed — old accumulated state
        // doesn't cleanly carry over to a differently-sized window.
        assert!(registry.record(tenant, 100, 0).unwrap()); // fresh counter: 100 <= 100 ok
    }

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

        assert!(matches!(registry.policy_for("acct_unknown"), Err(SymfynityError::UnknownTenant)));
    }
}
