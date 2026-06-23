//! 協調シャットダウン。Ctrl+C や明示トリガで立つ共有フラグ。

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use tokio::sync::Notify;

/// クローン可能な共有シャットダウンシグナル。
#[derive(Clone, Default)]
pub(crate) struct Shutdown {
    inner: Arc<Inner>,
}

#[derive(Default)]
struct Inner {
    flag: AtomicBool,
    notify: Notify,
}

impl Shutdown {
    /// 未トリガの新規シグナルを生成。
    pub(crate) fn new() -> Self {
        Self::default()
    }

    /// シャットダウンを要求する（冪等）。
    pub(crate) fn trigger(&self) {
        self.inner.flag.store(true, Ordering::SeqCst);
        self.inner.notify.notify_waiters();
    }

    /// トリガ済みかをポーリングで確認する。
    pub(crate) fn is_triggered(&self) -> bool {
        self.inner.flag.load(Ordering::SeqCst)
    }

    /// トリガまで非同期に待つ（tokio コンポーネント用）。
    pub(crate) async fn wait(&self) {
        loop {
            if self.is_triggered() {
                return;
            }
            // notified() を作ってから再チェックし、trigger との取りこぼしを防ぐ。
            let notified = self.inner.notify.notified();
            if self.is_triggered() {
                return;
            }
            notified.await;
        }
    }
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)] // テストは panic で失敗を表現してよい
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn starts_untriggered_then_triggers() {
        let s = Shutdown::new();
        assert!(!s.is_triggered());
        let clone = s.clone();
        clone.trigger();
        assert!(s.is_triggered());
    }

    #[tokio::test]
    async fn wait_returns_after_trigger() {
        let s = Shutdown::new();
        let s2 = s.clone();
        tokio::spawn(async move {
            tokio::time::sleep(Duration::from_millis(10)).await;
            s2.trigger();
        });
        tokio::time::timeout(Duration::from_secs(1), s.wait())
            .await
            .expect("wait should return promptly after trigger");
    }
}
