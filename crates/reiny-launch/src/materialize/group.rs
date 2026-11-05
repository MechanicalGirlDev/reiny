use super::{
    BTreeMap, Built, Context, DeploymentPlan, Events, Group, GroupState, Member, Output, Path,
    PathBuf, PrepareEvent, PrepareOptions, Result, Work, artifacts, ensure, prepared,
};

impl Group {
    pub(super) fn event(&self, plan: &DeploymentPlan, events: &Events<'_>, state: GroupState) {
        events.emit(PrepareEvent::Group {
            id: self.id.clone(),
            kind: self.kind,
            state,
            nodes: self
                .members
                .iter()
                .map(|member| match member {
                    Member::Module(index) => plan.nodes[*index].namespace.clone(),
                    Member::Host(name) => name.clone(),
                    Member::Component(index) => plan.components[*index].namespace.clone(),
                })
                .collect(),
            bins: self.bins.iter().cloned().collect(),
        });
    }

    pub(super) fn execute(
        &self,
        plan: &DeploymentPlan,
        cache: &Path,
        options: &PrepareOptions<'_>,
        events: &Events<'_>,
    ) -> Result<Output> {
        let cancel = &options.cancel;
        cancel.check()?;
        match &self.work {
            Work::Build(build) => Ok(Output::Build(build.execute(
                &self.bins,
                &cache.join("build"),
                cancel,
                events,
                &self.id,
            )?)),
            Work::Prebuilt(directory) => {
                let executables: Vec<_> = self
                    .bins
                    .iter()
                    .map(|bin| directory.join(artifacts::executable_name(bin)))
                    .collect();
                let first = executables.first().context("empty prebuilt group")?;
                let staged = artifacts::stage_prebuilt_bundle(
                    first,
                    &executables[1..],
                    &cache.join("build"),
                    cancel,
                )?;
                cancel.check()?;
                let directory = staged.parent().context("prebuilt snapshot directory")?;
                Ok(Output::Build(Built {
                    executables: self
                        .bins
                        .iter()
                        .map(|bin| (bin.clone(), directory.join(artifacts::executable_name(bin))))
                        .collect(),
                    fresh: true,
                }))
            }
            Work::Config { files, entry, .. } => {
                let (path, bundle) =
                    prepared::freeze_config(files, entry, &cache.join("config"), cancel)?;
                Ok(Output::Config(path, bundle))
            }
            Work::Resource(index) => {
                let resource = &plan.resources[*index];
                let name = PathBuf::from(resource.source.file_name().context("resource filename")?);
                let files = BTreeMap::from([(name.clone(), resource.source.clone())]);
                let path =
                    artifacts::stage_files_with(&files, &cache.join("artifacts"), &name, cancel)?;
                if let Some(expected) = &resource.sha256 {
                    let found = prepared::digest_file(&path)?;
                    ensure!(
                        &found == expected,
                        "artifact {} SHA-256 mismatch: expected {expected}, found {found}",
                        resource.address,
                    );
                }
                Ok(Output::Resource(path))
            }
        }
    }
}
