use std::collections::HashMap;
use std::path::Path;
use std::time::Duration;
use serde::Deserialize;

use crate::error::WeirError;

#[derive(Debug, Clone, Copy)]
pub struct BudgetLimit {
    pub max_tokens: u64,
    pub window: Duration,
}

pub type TenantLimits = HashMap<String, BudgetLimit>;

#[derive(Debug, Deserialize)]
struct RawConfig {
    tenants: HashMap<String, RawTenantLimit>,
}

#[derive(Debug, Deserialize)]
struct RawTenantLimit {
    max_tokens: u64,
    window_seconds: u64,
}

pub fn parse(contents: &str) -> Result<TenantLimits, WeirError> {
    let raw: RawConfig =
        toml::from_str(contents).map_err(|e| WeirError::Config(e.to_string()))?;
    Ok(raw
        .tenants
        .into_iter()
        .map(|(id, t)| {
            (
                id,
                BudgetLimit {
                    max_tokens: t.max_tokens,
                    window: Duration::from_secs(t.window_seconds),
                },
            )
        })
        .collect())
}

pub fn load_from_file(path: &Path) -> Result<TenantLimits, WeirError> {
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
        let limits = parse(toml).unwrap();
        let limit = limits.get("acct_123").unwrap();
        assert_eq!(limit.max_tokens, 50_000);
        assert_eq!(limit.window, Duration::from_secs(60));
    }

    #[test]
    fn rejects_malformed_toml() {
        let result = parse("not valid toml {{{");
        assert!(matches!(result, Err(WeirError::Config(_))));
    }
}

use std::path::PathBuf;
use std::sync::Arc;
use arc_swap::ArcSwap;

pub type SharedConfig = Arc<ArcSwap<TenantLimits>>;

pub fn load_shared(path: &Path) -> Result<SharedConfig, WeirError> {
    let limits = load_from_file(path)?;
    Ok(Arc::new(ArcSwap::from_pointee(limits)))
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
            Ok(limits) => {
                shared.store(Arc::new(limits));
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
        assert_eq!(shared.load().get("acct_1").unwrap().max_tokens, 100);

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
        assert_eq!(shared.load().get("acct_1").unwrap().max_tokens, 999);
    }

    fn tempfile_toml(contents: &str) -> tempfile::NamedTempFile {
        let mut f = tempfile::Builder::new().suffix(".toml").tempfile().unwrap();
        write!(f, "{contents}").unwrap();
        f.flush().unwrap();
        f
    }
}
