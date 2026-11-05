//! Event-driven observation and control of the actual SDK host processes.

use std::collections::BTreeMap;

use anyhow::{Context, Result, ensure};
use serde_json::Value;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::net::tcp::OwnedWriteHalf;
use tokio::sync::mpsc;
use tokio::task::{JoinHandle, JoinSet};

use super::DEADLINE;

struct Event {
    value: Value,
    command: Option<OwnedWriteHalf>,
}

pub(crate) struct Events {
    pub(crate) address: String,
    receiver: mpsc::UnboundedReceiver<Result<Event>>,
    commands: BTreeMap<String, OwnedWriteHalf>,
    observed: Vec<Value>,
    task: JoinHandle<()>,
}

impl Events {
    pub(crate) async fn listen() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?.to_string();
        let (sender, receiver) = mpsc::unbounded_channel();
        let task = tokio::spawn(async move {
            let mut readers = JoinSet::new();
            loop {
                tokio::select! {
                    connection = listener.accept() => {
                        let (socket, _) = match connection {
                            Ok(connection) => connection,
                            Err(error) => {
                                let _sent = sender.send(Err(error.into()));
                                return;
                            }
                        };
                        let sender = sender.clone();
                        readers.spawn(async move {
                            let (reader, writer) = socket.into_split();
                            let mut command = Some(writer);
                            let mut lines = BufReader::new(reader).lines();
                            loop {
                                match lines.next_line().await {
                                    Ok(Some(line)) => {
                                        let event = serde_json::from_str(&line).map(|value| Event {
                                            value,
                                            command: command.take(),
                                        }).map_err(Into::into);
                                        if sender.send(event).is_err() {
                                            return;
                                        }
                                    }
                                    Ok(None) => return,
                                    Err(error) => {
                                        let _sent = sender.send(Err(error.into()));
                                        return;
                                    }
                                }
                            }
                        });
                    }
                    _joined = readers.join_next(), if !readers.is_empty() => {}
                }
            }
        });
        Ok(Self {
            address,
            receiver,
            commands: BTreeMap::new(),
            observed: Vec::new(),
            task,
        })
    }

    pub(crate) async fn wait(&mut self, role: &str, kind: &str) -> Result<Value> {
        tokio::time::timeout(DEADLINE, async {
            loop {
                if let Some(event) = self
                    .observed
                    .iter()
                    .find(|event| event["role"] == role && event["event"] == kind)
                {
                    return Ok(event.clone());
                }
                let event = self
                    .receiver
                    .recv()
                    .await
                    .context("event listener ended")??;
                if let Some(command) = event.command {
                    self.commands.insert(
                        event.value["role"]
                            .as_str()
                            .context("event role")?
                            .to_owned(),
                        command,
                    );
                }
                self.observed.push(event.value);
            }
        })
        .await
        .with_context(|| format!("waiting for {role} {kind}; observed {:?}", self.observed))?
    }

    pub(crate) async fn exit(&mut self, role: &str) -> Result<()> {
        self.commands
            .get_mut(role)
            .context("running host command socket")?
            .write_all(b"exit\n")
            .await?;
        Ok(())
    }

    pub(crate) async fn release_prepare(&mut self, role: &str) -> Result<()> {
        self.commands
            .get_mut(role)
            .context("prepared host command socket")?
            .write_all(b"prepare\n")
            .await?;
        Ok(())
    }

    pub(crate) fn assert_no_run(&mut self) -> Result<()> {
        while let Ok(event) = self.receiver.try_recv() {
            self.observed.push(event?.value);
        }
        ensure!(
            !self.observed.iter().any(|event| event["event"] == "run"),
            "rejected graph ran a component: {:?}",
            self.observed
        );
        Ok(())
    }
}

impl Drop for Events {
    fn drop(&mut self) {
        self.task.abort();
    }
}
