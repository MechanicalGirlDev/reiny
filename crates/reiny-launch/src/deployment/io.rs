use super::{
    Connection, Context, Control, DeploymentStatus, ErrorKind, FileLock, IO_TIMEOUT, Instant,
    MAX_FRAME, ModuleReport, OpenOptions, Path, PreparedModule, Read, Request, Response, Result,
    Serialize, TcpStream, Write, bail, digest, ensure,
};

pub(super) fn validate_report(node: &PreparedModule, report: &ModuleReport) -> Result<()> {
    for binding in node.bindings.inputs.values() {
        crate::modules::validate_contract(&node.namespace, &binding.contract, true)?;
    }
    for binding in node.bindings.outputs.values() {
        crate::modules::validate_contract(&node.namespace, &binding.contract, false)?;
    }
    reiny_core::bindings::validate_report(&node.bindings, report)
        .with_context(|| format!("invalid readiness report for {}", node.namespace))
}

pub(super) fn private_directory(path: &Path) -> Result<()> {
    std::fs::create_dir_all(path)?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))?;
    }
    Ok(())
}

pub(super) fn lock_file(path: &Path) -> Result<FileLock> {
    let file = OpenOptions::new()
        .create(true)
        .truncate(false)
        .read(true)
        .write(true)
        .open(path)?;
    FileLock::new(file)
        .with_context(|| format!("deployment already has an owner ({})", path.display()))
}

pub(super) fn nonce() -> Result<String> {
    let mut bytes = [0_u8; 32];
    getrandom::fill(&mut bytes)
        .map_err(|error| anyhow::anyhow!("generating owner credential: {error}"))?;
    Ok(digest(&bytes))
}

pub(super) fn atomic_json(path: &Path, value: &impl Serialize) -> Result<()> {
    let parent = path.parent().context("state path has no parent")?;
    let mut temporary = tempfile::NamedTempFile::new_in(parent)?;
    serde_json::to_writer(temporary.as_file_mut(), value)?;
    temporary.as_file().sync_all()?;
    temporary
        .persist(path)
        .map_err(|error| error.error)
        .with_context(|| format!("persisting {}", path.display()))?;
    Ok(())
}

pub(super) fn write_frame(stream: &mut TcpStream, value: &impl Serialize) -> Result<()> {
    let bytes = serde_json::to_vec(value)?;
    ensure!(bytes.len() <= MAX_FRAME, "owner frame exceeds size limit");
    stream.write_all(&u32::try_from(bytes.len())?.to_be_bytes())?;
    stream.write_all(&bytes)?;
    Ok(())
}

pub(super) fn read_frame<T: serde::de::DeserializeOwned>(stream: &mut TcpStream) -> Result<T> {
    let mut length = [0_u8; 4];
    stream.read_exact(&mut length)?;
    let length = usize::try_from(u32::from_be_bytes(length))?;
    ensure!(length <= MAX_FRAME, "owner frame exceeds size limit");
    let mut bytes = vec![0; length];
    stream.read_exact(&mut bytes)?;
    Ok(serde_json::from_slice(&bytes)?)
}

pub(super) fn receive(connection: &mut Connection) -> Result<Option<Request>> {
    ensure!(
        Instant::now() < connection.deadline,
        "owner request timed out"
    );
    let mut buffer = [0_u8; 8192];
    loop {
        match connection.stream.read(&mut buffer) {
            Ok(0) => bail!("client disconnected"),
            Ok(count) => {
                connection.bytes.extend_from_slice(&buffer[..count]);
                ensure!(
                    connection.bytes.len() <= MAX_FRAME + 4,
                    "owner frame exceeds size limit"
                );
                if let Some(prefix) = connection.bytes.get(..4) {
                    let length = usize::try_from(u32::from_be_bytes(prefix.try_into()?))?;
                    ensure!(length <= MAX_FRAME, "owner frame exceeds size limit");
                    if connection.bytes.len() >= length + 4 {
                        return Ok(Some(serde_json::from_slice(
                            &connection.bytes[4..length + 4],
                        )?));
                    }
                }
            }
            Err(error) if error.kind() == ErrorKind::WouldBlock => return Ok(None),
            Err(error) => return Err(error.into()),
        }
    }
}

pub(super) fn authenticated(control: &Control, request: &Request) -> bool {
    let difference = control
        .token
        .bytes()
        .zip(request.token.bytes())
        .fold(0_u8, |difference, (left, right)| {
            difference | (left ^ right)
        });
    control.token.len() == request.token.len()
        && difference == 0
        && control.owner_generation == request.owner_generation
}

pub(super) fn reply(mut stream: TcpStream, result: Result<DeploymentStatus>) {
    let response = match result {
        Ok(status) => Response {
            status: Some(status),
            error: None,
        },
        Err(error) => Response {
            status: None,
            error: Some(format!("{error:#}")),
        },
    };
    let result = (|| {
        stream.set_nonblocking(false)?;
        stream.set_write_timeout(Some(IO_TIMEOUT))?;
        write_frame(&mut stream, &response)
    })();
    if let Err(error) = result {
        tracing::debug!(%error, "owner client disconnected before response");
    }
}
