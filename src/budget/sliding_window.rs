use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

/// Approximates a rolling-window rate limiter using two fixed windows,
/// weighting the previous window's count by how much of it still overlaps
/// the current rolling window. O(1) memory, lock-free via CAS retry.
pub struct SlidingWindowCounter {
    window_ms: i64,
    bucket_index: AtomicI64,
    current_count: AtomicU64,
    previous_count: AtomicU64,
}

impl SlidingWindowCounter {
    pub fn new(window: Duration) -> Self {
        Self {
            window_ms: window.as_millis().max(1) as i64,
            bucket_index: AtomicI64::new(i64::MIN),
            current_count: AtomicU64::new(0),
            previous_count: AtomicU64::new(0),
        }
    }

    pub fn add(&self, amount: u64, now_ms: i64) -> u64 {
        self.roll_if_needed(now_ms);
        let current = self.current_count.fetch_add(amount, Ordering::AcqRel) + amount;
        self.weighted_total(current, now_ms)
    }

    pub fn estimate(&self, now_ms: i64) -> u64 {
        self.roll_if_needed(now_ms);
        let current = self.current_count.load(Ordering::Acquire);
        self.weighted_total(current, now_ms)
    }

    fn roll_if_needed(&self, now_ms: i64) {
        let new_index = now_ms.div_euclid(self.window_ms);
        loop {
            let old_index = self.bucket_index.load(Ordering::Acquire);
            if old_index == new_index {
                return;
            }
            if self
                .bucket_index
                .compare_exchange(old_index, new_index, Ordering::AcqRel, Ordering::Acquire)
                .is_ok()
            {
                if new_index == old_index + 1 {
                    let carried = self.current_count.swap(0, Ordering::AcqRel);
                    self.previous_count.store(carried, Ordering::Release);
                } else {
                    self.current_count.store(0, Ordering::Release);
                    self.previous_count.store(0, Ordering::Release);
                }
                return;
            }
        }
    }

    fn weighted_total(&self, current: u64, now_ms: i64) -> u64 {
        let bucket_index = self.bucket_index.load(Ordering::Acquire);
        let bucket_start_ms = bucket_index * self.window_ms;
        let elapsed_ms = (now_ms - bucket_start_ms).clamp(0, self.window_ms);
        let remaining_weight = (self.window_ms - elapsed_ms) as f64 / self.window_ms as f64;
        let previous = self.previous_count.load(Ordering::Acquire) as f64;
        current + (previous * remaining_weight).round() as u64
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn accumulates_within_single_window() {
        let c = SlidingWindowCounter::new(Duration::from_secs(60));
        assert_eq!(c.add(10, 1_000), 10);
        assert_eq!(c.add(5, 1_500), 15);
        assert_eq!(c.estimate(1_600), 15);
    }

    #[test]
    fn rollover_carries_previous_window_weighted() {
        let c = SlidingWindowCounter::new(Duration::from_millis(1000));
        c.add(100, 0); // bucket 0: 100 tokens
        // Halfway into bucket 1 (t=1500): bucket 0 contributes ~50% weight.
        let total = c.estimate(1_500);
        assert!((45..=55).contains(&total), "expected ~50, got {total}");
    }

    #[test]
    fn gap_larger_than_two_windows_resets_to_zero() {
        let c = SlidingWindowCounter::new(Duration::from_millis(1000));
        c.add(100, 0);
        assert_eq!(c.estimate(5_000), 0);
    }

    #[test]
    fn concurrent_adds_are_not_lost() {
        use std::sync::Arc;
        use std::thread;

        let counter = Arc::new(SlidingWindowCounter::new(Duration::from_secs(60)));
        let mut handles = Vec::new();
        for _ in 0..8 {
            let counter = counter.clone();
            handles.push(thread::spawn(move || {
                for _ in 0..1000 {
                    counter.add(1, 0);
                }
            }));
        }
        for h in handles {
            h.join().unwrap();
        }
        assert_eq!(counter.estimate(0), 8000);
    }
}
