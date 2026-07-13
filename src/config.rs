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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_tenant_limits() {
        let toml = r#"
            [tenants.acct_123]
            max_tokens = 50000
            window_seconds = 60
        "#;
        let parsed = parse(toml).unwrap();
        let limit = parsed.limits.get("acct_123").unwrap();
        assert_eq!(limit.max_tokens, 50_000);
        assert_eq!(limit.window, Duration::from_secs(60));
    }

    #[test]
    fn rejects_malformed_toml() {
        let result = parse("not valid toml {{{");
        assert!(matches!(result, Err(WeirError::Config(_))));
    }

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

    let path_clone = path.clone();
    let mut watcher = notify::recommended_watcher(move |res: notify::Result<notify::Event>| {
        if res.is_err() {
            return;
        }
        match load_from_file(&path_clone) {
            Ok(parsed) => {
                shared.store(Arc::new(parsed));
                tracing::info!("reloaded config from {}", path_clone.display());
            }
            Err(e) => {
                tracing::warn!("ignoring invalid config reload: {e}");
            }
        }
    })?;
    watcher.watch(&path, RecursiveMode::NonRecursive)?;
    Ok(watcher)
}

#[cfg(test)]
mod hot_reload_tests {
    use super::*;
    use std::io::Write;
    use std::time::Duration as StdDuration;

    #[test]
    fn watch_reloads_on_file_change() {
        let file = tempfile_toml(
            r#"
            [tenants.acct_1]
            max_tokens = 100
            window_seconds = 60
        "#,
        );
        let path = file.path().to_path_buf();
        let shared = load_shared(&path).unwrap();
        assert_eq!(shared.load().limits.get("acct_1").unwrap().max_tokens, 100);

        let _watcher = watch(path.clone(), shared.clone()).unwrap();

        // Give the watcher time to initialize
        std::thread::sleep(StdDuration::from_millis(100));

        // Use std::fs::write for more reliable file modification detection
        std::fs::write(
            &path,
            r#"
            [tenants.acct_1]
            max_tokens = 999
            window_seconds = 60
        "#,
        )
        .unwrap();

        std::thread::sleep(StdDuration::from_millis(1000));
        assert_eq!(shared.load().limits.get("acct_1").unwrap().max_tokens, 999);
    }

    fn tempfile_toml(contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
        write!(f, "{contents}").unwrap();
        f.flush().unwrap();
        f
    }
}
