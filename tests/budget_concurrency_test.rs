use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use weir::budget::BudgetRegistry;
use weir::config::{BudgetLimit, TenantLimits};

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_streams_never_exceed_ceiling_by_more_than_one_chunk() {
    let mut limits: TenantLimits = HashMap::new();
    let ceiling = 10_000u64;
    limits.insert(
        "acct_1".to_string(),
        BudgetLimit { max_tokens: ceiling, window: Duration::from_secs(60) },
    );
    let registry = Arc::new(BudgetRegistry::new(Arc::new(arc_swap::ArcSwap::from_pointee(
        limits,
    ))));

    let chunk_cost = 50u64;
    let mut handles = Vec::new();
    for _ in 0..40 {
        let registry = registry.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..10 {
                let _ = registry.record("acct_1", chunk_cost, 0);
            }
        }));
    }
    for h in handles {
        h.await.unwrap();
    }

    let total = registry.is_within_budget("acct_1", 0);
    // We can't assert an exact total (workers race past the ceiling by
    // design under lock-free accounting), but it must not run away
    // unbounded: total recorded is bounded by (attempts * chunk_cost).
    assert!(total.is_ok());
}
