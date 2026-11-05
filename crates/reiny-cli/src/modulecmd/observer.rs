use super::{
    BTreeMap, Context, Duration, KEY_ROOT, ModuleObserver, PreparedDeployment, Result,
    RuntimeOptions, RwLock, Wait, ZenohSource, zenoh,
};

pub(super) struct BusObserver {
    domain: String,
    sessions: RwLock<BTreeMap<String, (String, zenoh::Session)>>,
}

impl BusObserver {
    pub(super) fn open(prepared: &PreparedDeployment) -> Result<Self> {
        let observer = Self {
            domain: prepared.domain.clone(),
            sessions: RwLock::new(BTreeMap::new()),
        };
        observer.configure(prepared)?;
        Ok(observer)
    }

    fn replace_sessions(&self, prepared: &PreparedDeployment) -> Result<()> {
        anyhow::ensure!(prepared.domain == self.domain, "deployment domain changed");
        let previous = self
            .sessions
            .read()
            .map_err(|_| anyhow::anyhow!("observer registry poisoned"))?;
        let mut sessions = BTreeMap::new();
        for node in &prepared.nodes {
            let signature = serde_json::to_string(&node.provider)?;
            if let Some((existing, session)) = previous.get(&node.namespace)
                && existing == &signature
            {
                sessions.insert(node.namespace.clone(), (signature, session.clone()));
                continue;
            }
            let mut options = RuntimeOptions::new("module-owner");
            options.domain.clone_from(&prepared.domain);
            if let Some(config) = &node.provider.zenoh_config {
                options.zenoh = ZenohSource::File(config.clone());
            }
            if !node.provider.connect.is_empty() {
                options.zenoh_overrides.push((
                    "connect/endpoints".into(),
                    serde_json::to_string(&node.provider.connect)?,
                ));
            }
            let session = zenoh::open(options.zenoh_config()?)
                .wait()
                .map_err(anyhow::Error::msg)?;
            sessions.insert(node.namespace.clone(), (signature, session));
        }
        drop(previous);
        *self
            .sessions
            .write()
            .map_err(|_| anyhow::anyhow!("observer registry poisoned"))? = sessions;
        Ok(())
    }

    fn session(&self, namespace: &str) -> Result<zenoh::Session> {
        self.sessions
            .read()
            .map_err(|_| anyhow::anyhow!("observer registry poisoned"))?
            .get(namespace)
            .map(|(_, session)| session.clone())
            .with_context(|| format!("unknown managed namespace {namespace}"))
    }
}

impl ModuleObserver for BusObserver {
    fn configure(&self, prepared: &PreparedDeployment) -> Result<()> {
        self.replace_sessions(prepared)
    }

    fn ready(&self, namespace: &str) -> Result<bool> {
        let key = format!("{KEY_ROOT}/{}/{namespace}/@ready", self.domain);
        let replies = self
            .session(namespace)?
            .liveliness()
            .get(&key)
            .timeout(Duration::from_millis(200))
            .wait()
            .map_err(anyhow::Error::msg)?;
        Ok(replies.iter().any(|reply| {
            reply
                .result()
                .is_ok_and(|sample| sample.key_expr().as_str() == key)
        }))
    }

    fn request_stop(&self, namespace: &str) -> Result<()> {
        let key = format!("{KEY_ROOT}/{}/{namespace}/@stop", self.domain);
        let replies = self
            .session(namespace)?
            .get(&key)
            .payload(Vec::<u8>::new())
            .timeout(Duration::from_secs(2))
            .wait()
            .map_err(anyhow::Error::msg)?;
        let reply = replies
            .recv()
            .map_err(anyhow::Error::msg)
            .context("managed module did not acknowledge stop")?;
        reply
            .result()
            .map_err(|error| anyhow::anyhow!("managed stop rejected: {error:?}"))?;
        Ok(())
    }
}
