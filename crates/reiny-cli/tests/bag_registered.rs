//! Native recorder registration before a real typed SDK publisher process exists.

use std::io::{BufRead, Write};
use std::process::Stdio;
use std::time::Duration;

use anyhow::{Context, Result, ensure};
use prost::Message;
use reiny::zenoh::{self, Wait};
use reiny::{Descriptor, MessageType};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const BIN: &str = env!("CARGO_BIN_EXE_reiny");
const DEADLINE: Duration = Duration::from_secs(15);
const DESCRIPTOR: &[u8] = b"\x0a\x2e\x0a\x09bag.proto\x12\x03bag\x22\x14\x0a\x05Probe\x12\x0b\x0a\x03seq\x18\x01\x20\x01\x28\x0d\x62\x06proto3";

#[derive(Clone, PartialEq, Message)]
struct Probe {
    #[prost(uint32, tag = "1")]
    seq: u32,
}

impl MessageType for Probe {
    const TYPE_NAME: &'static str = "bag.Probe";
    const SCHEMA_FINGERPRINT: u64 = 0x6be7_59d3_e446_4911;
    const DESCRIPTOR: Option<Descriptor> = Some(Descriptor {
        message: "bag.Probe",
        file_set: DESCRIPTOR,
    });
}

fn publisher_bindings() -> Result<reiny::bindings::ModuleBindings> {
    Ok(serde_yaml::from_str(
        "version: 3\nnamespace: producer\nendpoint_namespace: producer\ninputs: {}\noutputs:\n  first:\n    contract: {type: bag.Probe}\n  wrong:\n    contract: {type: bag.Probe}\n",
    )?)
}

#[test]
#[ignore = "subprocess fixture, invoked only after recorder readiness"]
fn bag_sdk_publisher_fixture() -> Result<()> {
    let endpoint = std::env::var("BAG_TEST_ENDPOINT")?;
    let domain = std::env::var("BAG_TEST_DOMAIN")?;
    let mut control = std::net::TcpStream::connect(std::env::var("BAG_TEST_CONTROL")?)?;
    control.set_read_timeout(Some(DEADLINE))?;
    let runtime = tokio::runtime::Runtime::new()?;
    runtime.block_on(async {
        let mut options = reiny::RuntimeOptions::new("producer");
        options.domain = domain;
        options.module_bindings = Some(publisher_bindings()?);
        options.install_tracing = false;
        options.zenoh_overrides = vec![
            ("connect/endpoints".into(), format!("[\"{endpoint}\"]")),
            ("scouting/multicast/enabled".into(), "false".into()),
            ("mode".into(), "\"client\"".into()),
        ];
        let context = reiny::ModuleContext::open(options).await?;
        let first = context.output::<Probe>("first")?;
        let wrong = context.output::<Probe>("wrong")?;
        context.ready()?;
        // One live sample, never repeated and never retained. No timing retry can rescue it.
        first.send(Probe { seq: 7 }).await?;
        wrong.send(Probe { seq: 99 }).await?;
        writeln!(control, "{}", std::process::id())?;
        let mut byte = [0];
        std::io::Read::read_exact(&mut control, &mut byte)?;
        Ok(())
    })
}

struct Recorder {
    child: tokio::process::Child,
    events: tokio::sync::mpsc::UnboundedReceiver<Result<serde_json::Value>>,
    reader: Option<std::thread::JoinHandle<()>>,
}

impl Recorder {
    fn spawn(
        bindings: &std::path::Path,
        bag: &std::path::Path,
        endpoint: &str,
        domain: &str,
    ) -> Result<Self> {
        let (pipe, writer) = std::io::pipe()?;
        let (sender, events) = tokio::sync::mpsc::unbounded_channel();
        let reader = std::thread::spawn(move || {
            for line in std::io::BufReader::new(pipe).lines() {
                let event = line
                    .map_err(anyhow::Error::from)
                    .and_then(|line| serde_json::from_str(&line).map_err(Into::into));
                if sender.send(event).is_err() {
                    break;
                }
            }
        });
        let child = tokio::process::Command::new(BIN)
            .args(["bag", "record", "--ready-json", "--module-bindings"])
            .arg(bindings)
            .args([
                "--domain",
                domain,
                "--connect",
                endpoint,
                "--zenoh-mode",
                "client",
                "--out",
            ])
            .arg(bag)
            .stdout(writer)
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()?;
        println!(
            "BAG_NATIVE_RECORDER_PID {}",
            child.id().context("recorder PID")?
        );
        Ok(Self {
            child,
            events,
            reader: Some(reader),
        })
    }

    async fn event(&mut self) -> Result<serde_json::Value> {
        tokio::time::timeout(DEADLINE, self.events.recv())
            .await?
            .context("recorder EOF")?
    }

    async fn cleanup(&mut self) -> Result<()> {
        self.child.start_kill()?;
        let status = tokio::time::timeout(DEADLINE, self.child.wait()).await??;
        println!("BAG_NATIVE_REAPED {status}");
        if let Some(reader) = self.reader.take() {
            reader
                .join()
                .map_err(|_| anyhow::anyhow!("reader panicked"))?;
        }
        Ok(())
    }
}

async fn fabric() -> Result<(zenoh::Session, String)> {
    let mut config = zenoh::Config::default();
    config
        .insert_json5("listen/endpoints", "[\"tcp/127.0.0.1:0\"]")
        .map_err(anyhow::Error::msg)?;
    config
        .insert_json5("scouting/multicast/enabled", "false")
        .map_err(anyhow::Error::msg)?;
    let session = zenoh::open(config).await.map_err(anyhow::Error::msg)?;
    let endpoint = session
        .info()
        .locators()
        .await
        .into_iter()
        .next()
        .context("fabric locator")?
        .to_string();
    Ok((session, endpoint))
}

fn recording(path: &std::path::Path) -> Result<()> {
    let binding: reiny::bindings::ModuleBindings = serde_yaml::from_str(
        "version: 3\nnamespace: recorder\nendpoint_namespace: recorder\ninputs:\n  sample:\n    contract: {type: bag.Probe}\n    sources: [{namespace: producer, port: first}]\noutputs: {}\n",
    )?;
    std::fs::write(path, serde_json::to_vec(&binding)?)?;
    Ok(())
}

async fn stop(session: &zenoh::Session, domain: &str) -> Result<()> {
    let replies = session
        .get(format!("reiny/v3/{domain}/recorder/@stop"))
        .timeout(DEADLINE)
        .await
        .map_err(anyhow::Error::msg)?;
    let ack = tokio::time::timeout(DEADLINE, replies.recv_async())
        .await?
        .map_err(anyhow::Error::msg)?;
    ensure!(ack.result().is_ok(), "recorder rejected stop");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bag_ready_precedes_typed_publisher_and_preserves_first_sample() -> Result<()> {
    ensure!(
        reiny_build::message_fingerprint(DESCRIPTOR, Probe::TYPE_NAME)?
            == Some(Probe::SCHEMA_FINGERPRINT),
        "typed fixture fingerprint drift"
    );
    let directory = tempfile::tempdir()?;
    let bindings = directory.path().join("bindings.json");
    let bag = directory.path().join("first.mcap");
    recording(&bindings)?;
    let (session, endpoint) = fabric().await?;
    let domain = "bag-first";
    let (on_schema, mut observations) = tokio::sync::mpsc::unbounded_channel();
    // This observer supplies no descriptor. Only the later SDK publisher can supply one.
    let _observer = session
        .declare_queryable(format!(
            "reiny/v3/{domain}/producer/@out/first/bag.Probe/@schema/bag.Probe"
        ))
        .callback(move |_| {
            let _ = on_schema.send(());
        })
        .wait()
        .map_err(anyhow::Error::msg)?;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
    let mut recorder = Recorder::spawn(&bindings, &bag, &endpoint, domain)?;
    let mut publisher = None;
    let result = async {
        ensure!(
            recorder.event().await?
                == serde_json::json!({
                    "v": 1, "event": "bag_ready", "domain": domain, "inputs": 1, "sources": 1
                }),
            "wrong readiness"
        );
        session
            .put(
                "reiny/v3/other-domain/producer/@out/first/bag.Probe",
                Probe { seq: 88 }.encode_to_vec(),
            )
            .attachment(Probe::SCHEMA_FINGERPRINT.to_le_bytes().to_vec())
            .wait()
            .map_err(anyhow::Error::msg)?;
        let child = tokio::process::Command::new(std::env::current_exe()?)
            .args([
                "--exact",
                "bag_sdk_publisher_fixture",
                "--ignored",
                "--nocapture",
            ])
            .env("BAG_TEST_ENDPOINT", &endpoint)
            .env("BAG_TEST_DOMAIN", domain)
            .env("BAG_TEST_CONTROL", listener.local_addr()?.to_string())
            .kill_on_drop(true)
            .spawn()?;
        println!(
            "BAG_NATIVE_PUBLISHER_PID {}",
            child.id().context("publisher PID")?
        );
        publisher = Some(child);
        let (mut control, _) = tokio::time::timeout(DEADLINE, listener.accept()).await??;
        let mut announced = [0; 32];
        let count = tokio::time::timeout(DEADLINE, control.read(&mut announced)).await??;
        ensure!(count > 0, "publisher did not announce");
        tokio::time::timeout(DEADLINE, observations.recv())
            .await?
            .context("schema query missing")?;
        stop(&session, domain).await?;
        ensure!(
            recorder.event().await?
                == serde_json::json!({"v":1,"event":"bag_finished","messages":1}),
            "recorder did not finalize exactly the first live sample"
        );
        ensure!(
            tokio::time::timeout(DEADLINE, recorder.child.wait())
                .await??
                .success(),
            "recorder exit"
        );
        control.write_all(b"x").await?;
        ensure!(
            tokio::time::timeout(
                DEADLINE,
                publisher.as_mut().context("publisher child")?.wait()
            )
            .await??
            .success(),
            "SDK publisher fixture failed"
        );
        verify_first_mcap(&bag, domain)?;
        Ok::<_, anyhow::Error>(())
    }
    .await;
    if let Some(mut publisher) = publisher {
        let _ = publisher.start_kill();
        let status = tokio::time::timeout(DEADLINE, publisher.wait()).await??;
        println!("BAG_NATIVE_PUBLISHER_REAPED {status}");
    }
    recorder.cleanup().await?;
    result
}

fn verify_first_mcap(bag: &std::path::Path, domain: &str) -> Result<()> {
    let bytes = std::fs::read(bag)?;
    let summary = mcap::Summary::read(&bytes)?.context("MCAP was not finalized")?;
    ensure!(summary.channels.len() == 1, "wrong-port payload admitted");
    let messages = mcap::MessageStream::new(&bytes)?.collect::<std::result::Result<Vec<_>, _>>()?;
    ensure!(
        messages.len() == 1 && Probe::decode(messages[0].data.as_ref())?.seq == 7,
        "first sample lost"
    );
    ensure!(
        messages[0].channel.metadata["reiny.source"] == "producer"
            && messages[0].channel.metadata["reiny.port"] == "first",
        "source identity lost"
    );
    ensure!(
        messages[0].channel.metadata["reiny.domain"] == domain,
        "domain lost"
    );
    println!("BAG_NATIVE_MCAP first=7 messages=1 source=producer port=first summary=valid");
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bag_registered_rejects_mismatched_schema_before_writing() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bindings = directory.path().join("bindings.json");
    let bag = directory.path().join("mismatch.mcap");
    recording(&bindings)?;
    let (session, endpoint) = fabric().await?;
    let key = "reiny/v3/bag-mismatch/producer/@out/first/bag.Probe";
    let mut recorder = Recorder::spawn(&bindings, &bag, &endpoint, "bag-mismatch")?;
    let result = async {
        ensure!(
            recorder.event().await?["event"] == "bag_ready",
            "not registered"
        );
        let schema_key = format!("{key}/@schema/bag.Probe");
        let reply_key = schema_key.clone();
        let _descriptor = session
            .declare_queryable(schema_key)
            .callback(move |query| {
                let _ = query.reply(reply_key.clone(), DESCRIPTOR).wait();
            })
            .wait()
            .map_err(anyhow::Error::msg)?;
        let publisher = session
            .declare_publisher(key)
            .await
            .map_err(anyhow::Error::msg)?;
        let matching = publisher
            .matching_listener()
            .await
            .map_err(anyhow::Error::msg)?;
        tokio::time::timeout(DEADLINE, async {
            if !publisher
                .matching_status()
                .await
                .map_err(anyhow::Error::msg)?
                .matching()
            {
                loop {
                    if matching
                        .recv_async()
                        .await
                        .map_err(anyhow::Error::msg)?
                        .matching()
                    {
                        break;
                    }
                }
            }
            Ok::<_, anyhow::Error>(())
        })
        .await??;
        publisher
            .put(Probe { seq: 11 }.encode_to_vec())
            .attachment((Probe::SCHEMA_FINGERPRINT ^ 1).to_le_bytes().to_vec())
            .wait()
            .map_err(anyhow::Error::msg)?;
        let status = tokio::time::timeout(DEADLINE, recorder.child.wait()).await??;
        ensure!(!status.success(), "mismatched schema accepted");
        let bytes = std::fs::read(&bag)?;
        ensure!(
            mcap::Summary::read(&bytes)?.is_some(),
            "failed recorder did not finalize"
        );
        ensure!(
            mcap::MessageStream::new(&bytes)?.next().is_none(),
            "unverified payload written"
        );
        println!("BAG_NATIVE_MISMATCH exit={status} messages=0 summary=valid");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    recorder.cleanup().await?;
    result
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn bag_registered_replays_exact_retained_source_on_late_arrival() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bindings = directory.path().join("bindings.json");
    let bag = directory.path().join("snapshot.mcap");
    let binding: reiny::bindings::ModuleBindings = serde_yaml::from_str(
        "version: 3\nnamespace: recorder\nendpoint_namespace: recorder\ninputs:\n  sample:\n    contract: {type: bag.Probe, replay: last}\n    sources: [{namespace: producer, port: first}]\noutputs: {}\n",
    )?;
    std::fs::write(&bindings, serde_json::to_vec(&binding)?)?;
    let (session, endpoint) = fabric().await?;
    let domain = "bag-snapshot";
    let key = format!("reiny/v3/{domain}/producer/@out/first/bag.Probe");
    let mut recorder = Recorder::spawn(&bindings, &bag, &endpoint, domain)?;
    let result = async {
        ensure!(
            recorder.event().await?["event"] == "bag_ready",
            "not registered"
        );
        let reply_key = key.clone();
        let _retained = session
            .declare_queryable(key.clone())
            .callback(move |query| {
                let _ = query
                    .reply(reply_key.clone(), Probe { seq: 44 }.encode_to_vec())
                    .attachment(Probe::SCHEMA_FINGERPRINT.to_le_bytes().to_vec())
                    .wait();
            })
            .wait()
            .map_err(anyhow::Error::msg)?;
        let schema_key = format!("{key}/@schema/bag.Probe");
        let reply_key = schema_key.clone();
        let (on_schema, mut observations) = tokio::sync::mpsc::unbounded_channel();
        let _schema = session
            .declare_queryable(schema_key)
            .callback(move |query| {
                let _ = query.reply(reply_key.clone(), DESCRIPTOR).wait();
                let _ = on_schema.send(());
            })
            .wait()
            .map_err(anyhow::Error::msg)?;
        let _presence = session
            .liveliness()
            .declare_token(key.clone())
            .wait()
            .map_err(anyhow::Error::msg)?;
        tokio::time::timeout(DEADLINE, observations.recv())
            .await?
            .context("snapshot was not verified")?;
        stop(&session, domain).await?;
        ensure!(recorder.event().await?["messages"] == 1, "snapshot count");
        ensure!(
            tokio::time::timeout(DEADLINE, recorder.child.wait())
                .await??
                .success(),
            "snapshot recorder exit"
        );
        let bytes = std::fs::read(&bag)?;
        let messages =
            mcap::MessageStream::new(&bytes)?.collect::<std::result::Result<Vec<_>, _>>()?;
        ensure!(
            messages.len() == 1 && Probe::decode(messages[0].data.as_ref())?.seq == 44,
            "snapshot payload"
        );
        ensure!(
            messages[0].channel.metadata["reiny.latched"] == "true",
            "replay policy lost"
        );
        println!("BAG_NATIVE_SNAPSHOT late_source=true messages=1 value=44");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    recorder.cleanup().await?;
    result
}

#[tokio::test]
async fn bag_failed_initialization_never_reports_ready() -> Result<()> {
    let directory = tempfile::tempdir()?;
    let bindings = directory.path().join("bindings.json");
    let bag = directory.path().join("invalid.mcap");
    std::fs::write(&bindings, b"{}")?;
    let mut recorder = Recorder::spawn(&bindings, &bag, "tcp/127.0.0.1:1", "invalid")?;
    let result = async {
        ensure!(
            !tokio::time::timeout(DEADLINE, recorder.child.wait())
                .await??
                .success(),
            "invalid init accepted"
        );
        ensure!(
            tokio::time::timeout(DEADLINE, recorder.events.recv())
                .await?
                .is_none(),
            "fake readiness after invalid init"
        );
        ensure!(!bag.exists(), "invalid initialization created a recording");
        Ok::<_, anyhow::Error>(())
    }
    .await;
    recorder.cleanup().await?;
    result
}
