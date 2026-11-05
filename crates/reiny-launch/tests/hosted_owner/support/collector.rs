use super::{DEADLINE, Event, EventKind};
use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::io::{BufRead, BufReader};
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, mpsc};

pub(super) struct Collector {
    pub(super) listener: TcpListener,
    pub(super) stop: Arc<AtomicBool>,
    pub(super) sockets: Arc<Mutex<BTreeMap<String, TcpStream>>>,
    pub(super) sender: mpsc::SyncSender<Event>,
}

impl Collector {
    pub(super) fn run(self) -> Result<()> {
        let Self {
            listener,
            stop,
            sockets,
            sender,
        } = self;
        let mut readers = Vec::new();
        for _ in 0..2 {
            let (stream, _) = listener.accept()?;
            if stop.load(Ordering::Acquire) {
                break;
            }
            stream.set_read_timeout(Some(DEADLINE))?;
            let sockets = Arc::clone(&sockets);
            let sender = sender.clone();
            readers.push(std::thread::spawn(move || -> Result<()> {
                let mut reader = BufReader::new(stream);
                loop {
                    let mut line = String::new();
                    if reader.read_line(&mut line)? == 0 {
                        break;
                    }
                    let event: Event = serde_json::from_str(&line)?;
                    if event.kind == EventKind::Connected {
                        sockets
                            .lock()
                            .map_err(|_| anyhow::anyhow!("fixture sockets poisoned"))?
                            .insert(event.host.clone(), reader.get_ref().try_clone()?);
                    }
                    sender.send(event).context("event receiver closed")?;
                }
                Ok(())
            }));
        }
        for reader in readers {
            reader
                .join()
                .map_err(|_| anyhow::anyhow!("event reader panicked"))??;
        }
        Ok(())
    }
}
