use std::path::PathBuf;
use std::process;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio::process::Command;
use tonic::Status;

#[derive(Debug)]
pub struct CircuitBreaker {
    data_dir: PathBuf,
    max_disk_usage_percent: u8,
    max_memory_usage_percent: u8,
    max_queue_depth: u64,
    queue_depth: AtomicU64,
}

impl CircuitBreaker {
    pub fn new(
        data_dir: PathBuf,
        max_disk_usage_percent: u8,
        max_memory_usage_percent: u8,
        max_queue_depth: u64,
    ) -> Self {
        Self {
            data_dir,
            max_disk_usage_percent,
            max_memory_usage_percent,
            max_queue_depth,
            queue_depth: AtomicU64::new(0),
        }
    }

    pub fn set_queue_depth(&self, depth: u64) {
        self.queue_depth.store(depth, Ordering::Relaxed);
    }

    pub fn queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    pub fn max_queue_depth(&self) -> u64 {
        self.max_queue_depth
    }

    pub async fn check(&self) -> Result<(), String> {
        if let Some(disk_usage) = disk_usage_percent(&self.data_dir).await {
            if disk_usage > self.max_disk_usage_percent {
                return Err(format!("disk usage {}% exceeds threshold", disk_usage));
            }
        }

        if let Some(memory_usage) = memory_usage_percent().await {
            if memory_usage > self.max_memory_usage_percent {
                return Err(format!("memory usage {}% exceeds threshold", memory_usage));
            }
        }

        let queue_depth = self.queue_depth.load(Ordering::Relaxed);
        if queue_depth > self.max_queue_depth {
            return Err(format!("queue depth {} exceeds threshold", queue_depth));
        }

        Ok(())
    }

    pub async fn check_status(&self) -> Result<(), Status> {
        self.check().await.map_err(|reason| {
            Status::resource_exhausted(format!("Ingestion backpressure: {}", reason))
        })
    }
}

async fn disk_usage_percent(path: &PathBuf) -> Option<u8> {
    let output = Command::new("df")
        .arg("-Pk")
        .arg(path)
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8(output.stdout).ok()?;
    let line = stdout.lines().nth(1)?;
    let usage = line.split_whitespace().nth(4)?.trim_end_matches('%');
    usage.parse::<u8>().ok()
}

async fn memory_usage_percent() -> Option<u8> {
    let pid = process::id().to_string();
    let output = Command::new("ps")
        .args(["-o", "%mem=", "-p", &pid])
        .output()
        .await
        .ok()?;
    if !output.status.success() {
        return None;
    }

    let stdout = String::from_utf8(output.stdout).ok()?;
    let percent = stdout.trim().parse::<f64>().ok()?;
    if !percent.is_finite() || percent.is_sign_negative() {
        return None;
    }

    Some(percent.ceil().min(u8::MAX as f64) as u8)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn closed_when_thresholds_not_exceeded() {
        let breaker = CircuitBreaker::new(PathBuf::from("."), 100, 100, 10);
        breaker.set_queue_depth(10);
        assert!(breaker.check().await.is_ok());
    }

    #[tokio::test]
    async fn open_when_queue_depth_exceeds_threshold() {
        let breaker = CircuitBreaker::new(PathBuf::from("."), 100, 100, 1);
        breaker.set_queue_depth(2);
        assert!(breaker.check().await.is_err());
    }

    #[tokio::test]
    async fn exposes_queue_depth_snapshot() {
        let breaker = CircuitBreaker::new(PathBuf::from("."), 100, 100, 10);
        breaker.set_queue_depth(7);
        assert_eq!(breaker.queue_depth(), 7);
        assert_eq!(breaker.max_queue_depth(), 10);
    }
}
