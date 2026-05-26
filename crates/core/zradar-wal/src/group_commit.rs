/// Group-commit fsync coalescing.
///
/// A single background task wakes every `group_commit_window_ms`, issues one fsync
/// on the active segment, then notifies all pending callers that their data is durable.
use std::sync::Arc;

use tokio::sync::{Mutex, Notify, oneshot};
use tokio_util::sync::CancellationToken;

/// Handle returned to callers of `Wal::append`. Awaiting `durable()` blocks until the
/// group-commit fsync covers this record.
pub struct AppendHandle {
    rx: oneshot::Receiver<()>,
}

impl AppendHandle {
    pub(crate) fn new(rx: oneshot::Receiver<()>) -> Self {
        Self { rx }
    }

    /// Wait until the fsync covering this record completes.
    pub async fn durable(self) -> Result<(), AppendError> {
        self.rx.await.map_err(|_| AppendError::FsyncDropped)
    }
}

/// Errors from append/sync operations.
#[derive(Debug, thiserror::Error)]
pub enum AppendError {
    #[error("WAL segment write failed: {0}")]
    Io(#[from] std::io::Error),

    #[error("fsync notification dropped (WAL shutting down)")]
    FsyncDropped,

    #[error("WAL resource exhausted (backpressure reject)")]
    ResourceExhausted,
}

/// Tracks pending fsync notifications for the group-commit window.
pub(crate) struct FsyncQueue {
    pending: Mutex<Vec<oneshot::Sender<()>>>,
    notify: Notify,
    fsync_count: std::sync::atomic::AtomicU64,
}

impl FsyncQueue {
    pub fn new() -> Self {
        Self {
            pending: Mutex::new(Vec::with_capacity(256)),
            notify: Notify::new(),
            fsync_count: std::sync::atomic::AtomicU64::new(0),
        }
    }

    /// Register a pending write that needs fsync. Returns the oneshot receiver.
    pub async fn register(&self) -> oneshot::Receiver<()> {
        let (tx, rx) = oneshot::channel();
        self.pending.lock().await.push(tx);
        self.notify.notify_one();
        rx
    }

    /// Drain all pending senders and notify them.
    pub async fn drain_and_notify(&self) {
        let mut pending = self.pending.lock().await;
        for tx in pending.drain(..) {
            let _ = tx.send(());
        }
    }

    /// Returns true if there are pending writes waiting for fsync.
    pub async fn has_pending(&self) -> bool {
        !self.pending.lock().await.is_empty()
    }

    /// Wait for a notification that new work has arrived.
    pub async fn wait_for_work(&self) {
        self.notify.notified().await;
    }

    /// Increment the fsync counter and return the new value.
    pub fn inc_fsync_count(&self) -> u64 {
        self.fsync_count
            .fetch_add(1, std::sync::atomic::Ordering::Relaxed)
            + 1
    }

    /// Current fsync count (for testing).
    pub fn fsync_count(&self) -> u64 {
        self.fsync_count.load(std::sync::atomic::Ordering::Relaxed)
    }
}

/// The group-commit fsync background task.
///
/// Wakes at `window_ms` intervals (or when notified), issues one fsync, and
/// resolves all pending `AppendHandle` futures.
pub(crate) async fn group_commit_loop(
    queue: Arc<FsyncQueue>,
    fsync_fn: Arc<dyn Fn() -> Result<(), std::io::Error> + Send + Sync>,
    window_ms: u64,
    cancel: CancellationToken,
) {
    let interval_dur = std::time::Duration::from_millis(window_ms);

    loop {
        tokio::select! {
            _ = cancel.cancelled() => {
                if queue.has_pending().await && fsync_fn().is_ok() {
                    queue.inc_fsync_count();
                    queue.drain_and_notify().await;
                }
                break;
            }
            _ = tokio::time::sleep(interval_dur) => {}
            _ = queue.wait_for_work() => {
                // Small extra coalescing window after first notification
                tokio::time::sleep(std::time::Duration::from_millis(window_ms.min(1))).await;
            }
        }

        if !queue.has_pending().await {
            continue;
        }

        match fsync_fn() {
            Ok(()) => {
                queue.inc_fsync_count();
                queue.drain_and_notify().await;
            }
            Err(e) => {
                tracing::error!(error = %e, "group-commit fsync failed");
                // Don't notify — callers will timeout or retry
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicU64, Ordering};

    #[tokio::test]
    async fn test_group_commit_coalesces_fsyncs() {
        let queue = Arc::new(FsyncQueue::new());
        let fsync_count = Arc::new(AtomicU64::new(0));
        let fsync_count_clone = fsync_count.clone();

        let fsync_fn: Arc<dyn Fn() -> Result<(), std::io::Error> + Send + Sync> =
            Arc::new(move || {
                fsync_count_clone.fetch_add(1, Ordering::Relaxed);
                Ok(())
            });

        let cancel = CancellationToken::new();
        let queue_clone = queue.clone();
        let cancel_clone = cancel.clone();

        let handle = tokio::spawn(group_commit_loop(
            queue_clone,
            fsync_fn,
            5, // 5ms window
            cancel_clone,
        ));

        // Register 100 pending writes rapidly
        let mut receivers = Vec::new();
        for _ in 0..100 {
            receivers.push(queue.register().await);
        }

        // Wait for all to resolve
        for rx in receivers {
            tokio::time::timeout(std::time::Duration::from_secs(2), async {
                rx.await.unwrap();
            })
            .await
            .unwrap();
        }

        cancel.cancel();
        handle.await.unwrap();

        // Should have far fewer fsyncs than writes (coalesced)
        let actual_fsyncs = fsync_count.load(Ordering::Relaxed);
        assert!(
            actual_fsyncs <= 10,
            "expected <=10 fsyncs for 100 writes, got {actual_fsyncs}"
        );
        assert!(actual_fsyncs >= 1);
    }

    #[tokio::test]
    async fn test_fsync_queue_drain_notifies_all() {
        let queue = FsyncQueue::new();

        let rx1 = queue.register().await;
        let rx2 = queue.register().await;
        let rx3 = queue.register().await;

        assert!(queue.has_pending().await);

        queue.drain_and_notify().await;

        assert!(!queue.has_pending().await);
        rx1.await.unwrap();
        rx2.await.unwrap();
        rx3.await.unwrap();
    }
}
