use super::{
    Arc, BTreeMap, BTreeSet, Context, Control, DeploymentPhase, DeploymentStatus, ModuleObserver,
    ModuleStatus, Owner, PreparedDeployment, Result, STATUS_VERSION, TcpListener, atomic_json,
    digest, hosted, lock_file, nonce, private_directory, validate_prepared,
};

impl<'a> Owner<'a> {
    pub(super) fn new(
        mut desired: PreparedDeployment,
        observer: &'a dyn ModuleObserver,
    ) -> Result<Self> {
        desired.root = std::fs::canonicalize(&desired.root)?;
        validate_prepared(&desired)?;
        let cache = desired.root.join(".reiny");
        private_directory(&cache)?;
        let root_lock = lock_file(&cache.join("owner.lock"))?;
        super::remote::refuse_unresolved(&desired.root)?;
        let global = std::env::temp_dir().join("reiny-deployment-owners-v1");
        private_directory(&global)?;
        let identity = digest(&serde_json::to_vec(&(
            &desired.domain,
            &desired.deployment,
        ))?);
        let global_lock = lock_file(&global.join(format!("{identity}.lock")))?;
        let generation = nonce()?;
        let listener = TcpListener::bind(("127.0.0.1", 0))?;
        listener.set_nonblocking(true)?;
        let control = Control {
            version: 1,
            address: listener.local_addr()?,
            token: nonce()?,
            owner_generation: generation.clone(),
        };
        let status = DeploymentStatus {
            version: STATUS_VERSION,
            root: desired.root.clone(),
            deployment: desired.deployment.clone(),
            domain: desired.domain.clone(),
            owner_generation: generation,
            owner_pid: std::process::id(),
            owner_alive: true,
            revision: 0,
            phase: DeploymentPhase::Starting,
            modules: BTreeMap::new(),
            component_hosts: BTreeMap::new(),
            components: BTreeMap::new(),
            resources: desired.resources.clone(),
            error: None,
        };
        let mut owner = Self {
            desired,
            observer,
            status,
            published: None,
            control,
            cache,
            listener,
            locks: vec![root_lock, global_lock],
            children: BTreeMap::new(),
            retries: BTreeMap::new(),
            attempts: BTreeMap::new(),
            next_generation: 0,
            graph_generation: 0,
            hosted: None,
            remote: BTreeMap::new(),
            waiters: Vec::new(),
            runtime: Arc::new(
                tokio::runtime::Builder::new_current_thread()
                    .enable_all()
                    .build()?,
            ),
        };
        owner.install_desired_status();
        owner.publish()?;
        atomic_json(&owner.cache.join("prepared.json"), &owner.desired)?;
        atomic_json(&owner.cache.join("control.json"), &owner.control)?;
        Ok(owner)
    }

    pub(super) fn install_desired_status(&mut self) {
        self.status.resources.clone_from(&self.desired.resources);
        if !self.desired.components.is_empty() {
            self.status.version = 3;
            self.status.modules.clear();
            hosted::install(self);
            return;
        }
        self.status.version = STATUS_VERSION;
        self.status.components.clear();
        self.status.component_hosts.clear();
        let names: BTreeSet<_> = self
            .desired
            .nodes
            .iter()
            .map(|node| node.namespace.clone())
            .collect();
        self.status.modules.retain(|name, _| names.contains(name));
        for node in &self.desired.nodes {
            self.status
                .modules
                .entry(node.namespace.clone())
                .and_modify(|status| {
                    if status.desired_fingerprint != node.fingerprint
                        && status.phase == DeploymentPhase::Completed
                    {
                        status.phase = DeploymentPhase::Starting;
                    }
                    status.desired_fingerprint.clone_from(&node.fingerprint);
                })
                .or_insert_with(|| ModuleStatus {
                    namespace: node.namespace.clone(),
                    desired_fingerprint: node.fingerprint.clone(),
                    actual_fingerprint: None,
                    pid: None,
                    generation: 0,
                    phase: DeploymentPhase::Starting,
                    error: None,
                    report: None,
                });
        }
    }

    pub(super) fn publish(&mut self) -> Result<()> {
        if self.published.as_ref() == Some(&self.status) {
            return Ok(());
        }
        self.status.revision = self
            .status
            .revision
            .checked_add(1)
            .context("state revision exhausted")?;
        atomic_json(&self.cache.join("state.json"), &self.status)?;
        self.published = Some(self.status.clone());
        self.notify_waiters();
        Ok(())
    }
}
