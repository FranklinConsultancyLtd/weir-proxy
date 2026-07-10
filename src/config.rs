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
