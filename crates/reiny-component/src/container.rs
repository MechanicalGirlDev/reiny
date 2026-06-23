//! `Container` — 1プロセス内で複数コンポーネントをスレッド起動する最小ホスト。
//!
//! `Component + Send + 'static` のみ受け付ける。GUI のようにメインスレッドを
//! 要求するコンポーネントは（`!Send` にしておくことで）型レベルで弾かれる。

use std::thread::JoinHandle;

use crate::{Component, Shutdown};

/// 複数コンポーネントを束ねるスレッドホスト。
pub struct Container {
    shutdown: Shutdown,
    handles: Vec<JoinHandle<()>>,
}

impl Container {
    /// 空のコンテナを生成。
    pub fn new() -> Self {
        Self {
            shutdown: Shutdown::new(),
            handles: Vec::new(),
        }
    }

    /// 全コンポーネントで共有する `Shutdown` のクローンを返す。
    /// 呼び出し側がこれを Ctrl+C 等に配線してトリガする。
    pub fn handle(&self) -> Shutdown {
        self.shutdown.clone()
    }

    /// コンポーネントを専用スレッドで起動する。
    pub fn spawn<C: Component + Send + 'static>(&mut self, component: C) {
        let shutdown = self.shutdown.clone();
        let handle = std::thread::spawn(move || {
            if let Err(e) = component.run(shutdown) {
                tracing::error!("component '{}' exited with error: {e:#}", C::KIND);
            }
        });
        self.handles.push(handle);
    }

    /// 全コンポーネントスレッドが終了するまでブロックして join する。
    /// （各スレッドは共有 `Shutdown` がトリガされると終了する想定。）
    pub fn run(self) -> anyhow::Result<()> {
        for handle in self.handles {
            let _ = handle.join();
        }
        Ok(())
    }
}

impl Default for Container {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::CommonArgs;
    use clap::Args;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    #[derive(Args, Debug)]
    struct NoArgs;

    /// shutdown まで回り、終了時にカウンタを増やすダミー。
    struct Counting {
        done: Arc<AtomicUsize>,
    }
    impl Component for Counting {
        const KIND: &'static str = "counting";
        type Args = NoArgs;
        fn build(_c: &CommonArgs, _a: Self::Args) -> anyhow::Result<Self> {
            Ok(Counting {
                done: Arc::new(AtomicUsize::new(0)),
            })
        }
        fn run(self, shutdown: Shutdown) -> anyhow::Result<()> {
            while !shutdown.is_triggered() {
                std::thread::sleep(Duration::from_millis(5));
            }
            self.done.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    #[test]
    fn runs_components_and_joins_on_shutdown() {
        let done = Arc::new(AtomicUsize::new(0));
        let mut container = Container::new();
        container.spawn(Counting { done: done.clone() });
        container.spawn(Counting { done: done.clone() });

        let trigger = container.handle();
        std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            trigger.trigger();
        });

        container.run().unwrap();
        assert_eq!(done.load(Ordering::SeqCst), 2);
    }
}
