use dashmap::DashMap;
use std::time::Instant;
use uuid::Uuid;
use zradar_models::WorkspaceId;

#[derive(Debug, Clone)]
struct TokenBucket {
    tokens: f64,
    last_refill: Instant,
}

#[derive(Debug, Default)]
pub struct ProjectRateLimiter {
    buckets: DashMap<Uuid, TokenBucket>,
}

impl ProjectRateLimiter {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn check_and_record(
        &self,
        workspace_id: WorkspaceId,
        limit_per_second: u64,
        records: u64,
    ) -> bool {
        if limit_per_second == 0 {
            return false;
        }

        let now = Instant::now();
        let capacity = limit_per_second.saturating_mul(2) as f64;
        let requested = records as f64;
        let mut bucket = self
            .buckets
            .entry(workspace_id.into())
            .or_insert(TokenBucket {
                tokens: capacity,
                last_refill: now,
            });

        let elapsed_secs = now.duration_since(bucket.last_refill).as_secs_f64();
        let refill = elapsed_secs * limit_per_second as f64;
        bucket.tokens = (bucket.tokens + refill).min(capacity);
        bucket.last_refill = now;

        if bucket.tokens < requested {
            return false;
        }

        bucket.tokens -= requested;
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_burst_up_to_twice_rate() {
        let limiter = ProjectRateLimiter::new();
        assert!(limiter.check_and_record(Uuid::new_v4().into(), 10, 20));
    }

    #[test]
    fn rejects_when_bucket_empty() {
        let limiter = ProjectRateLimiter::new();
        let workspace_id = Uuid::new_v4();
        assert!(limiter.check_and_record(workspace_id.into(), 10, 20));
        assert!(!limiter.check_and_record(workspace_id.into(), 10, 1));
    }

    #[test]
    fn zero_rate_rejects() {
        let limiter = ProjectRateLimiter::new();
        assert!(!limiter.check_and_record(Uuid::new_v4().into(), 0, 1));
    }
}
