//! A deliberately small single-host fixture owner, not deployment integration.

use std::io::Read;
use std::process::Stdio;
use std::time::Duration;

use reiny::component::process::protocol::{
    GraphGeneration, HostGeneration, HostId, Nonce, OwnerGeneration, PreparedDigest,
    SessionIdentity, frame_header,
};
use reiny::component::process::{Bootstrap, encode_payload};
use tokio::io::{AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;
use tokio::process::Command;
use tokio::time::{Instant, timeout_at};

#[path = "peer.rs"]
mod peer;
use peer::Peer;

pub(super) fn exercise() -> reiny::Result<()> {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?
        .block_on(run())
}

async fn run() -> reiny::Result<()> {
    let deadline = Instant::now() + Duration::from_secs(15);
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let identity = SessionIdentity {
        owner_generation: OwnerGeneration(1),
        graph_generation: GraphGeneration(1),
        host_id: HostId("example".into()),
        host_generation: HostGeneration(1),
        graph_digest: PreparedDigest("fixture-graph".into()),
        bundle_digest: PreparedDigest("fixture-binary".into()),
    };
    // This runnable Unix fixture uses OS entropy. Production owners supply their
    // verified bundle/graph digests instead of these explicit fixture labels.
    let mut secret = [0; 32];
    std::fs::File::open("/dev/urandom")?.read_exact(&mut secret)?;
    let nonce = Nonce::new(secret);
    let bootstrap = Bootstrap {
        address: listener.local_addr()?,
        identity: identity.clone(),
        nonce: nonce.clone(),
    };
    let mut child = Command::new(std::env::current_exe()?)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .kill_on_drop(true)
        .spawn()?;
    let stdout = child
        .stdout
        .take()
        .ok_or_else(|| anyhow::anyhow!("missing stdout"))?;
    let mut stdout = BufReader::new(stdout);
    let exchange = timeout_at(deadline, async {
        let mut stdin = child
            .stdin
            .take()
            .ok_or_else(|| anyhow::anyhow!("missing stdin"))?;
        let payload = encode_payload(&bootstrap)?;
        stdin.write_all(&frame_header(&payload)?).await?;
        stdin.write_all(&payload).await?;
        drop(stdin);
        let (stream, _) = listener.accept().await?;
        let mut peer = Peer::new(stream, identity, deadline);
        let implementation = peer.authenticate(&nonce).await?;
        peer.admit(implementation, &mut stdout).await
    })
    .await;
    match exchange {
        Ok(Ok(())) => {}
        Ok(Err(error)) => {
            child.kill().await?;
            return Err(error);
        }
        Err(error) => {
            child.kill().await?;
            return Err(error.into());
        }
    }
    let status = match timeout_at(deadline, child.wait()).await {
        Ok(status) => status?,
        Err(error) => {
            child.kill().await?;
            return Err(error.into());
        }
    };
    anyhow::ensure!(status.success(), "host process failed");
    let mut text = String::new();
    timeout_at(deadline, stdout.read_to_string(&mut text)).await??;
    anyhow::ensure!(
        text.lines().collect::<Vec<_>>() == ["stop"],
        "unexpected component cleanup"
    );
    println!("validated prepare -> commit -> run -> stop; exact child reaped");
    Ok(())
}
