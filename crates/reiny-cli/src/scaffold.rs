//! Create executable modules and add local schema dependencies in `main.yaml`.

use std::path::{Path, PathBuf};

use anyhow::{Context, Result, bail};
use serde_yaml::{Mapping, Value};

pub(crate) fn new(path: &Path, publish: Option<&str>, name: Option<&str>) -> Result<()> {
    if path.exists() {
        bail!("{} already exists; use `reiny init`", path.display());
    }
    std::fs::create_dir_all(path).with_context(|| format!("creating {}", path.display()))?;
    scaffold(path, publish, name)?;
    println!("created module at {}", path.display());
    Ok(())
}

pub(crate) fn init(path: Option<&Path>, publish: Option<&str>, name: Option<&str>) -> Result<()> {
    let dir = match path {
        Some(path) => path.to_path_buf(),
        None => std::env::current_dir().context("resolving current directory")?,
    };
    std::fs::create_dir_all(&dir)?;
    scaffold(&dir, publish, name)?;
    println!("initialized module in {}", dir.display());
    Ok(())
}

fn scaffold(dir: &Path, publish: Option<&str>, name: Option<&str>) -> Result<()> {
    let proj = project_name(dir, name)?;
    write_cargo_toml(dir, &proj)?;
    // An existing package's binary defaults to its package name, not the directory name.
    let cargo: toml::Table = toml::from_str(&std::fs::read_to_string(dir.join("Cargo.toml"))?)?;
    let bin = cargo
        .get("package")
        .and_then(|package| package.get("name"))
        .and_then(toml::Value::as_str)
        .context("Cargo.toml requires package.name")?;
    write_if_absent(
        &dir.join("main.yaml"),
        &main_yaml(&proj, bin, publish, dir.join("Cargo.lock").is_file())?,
    )?;
    write_if_absent(&dir.join("build.rs"), BUILD_RS)?;
    if let Some(ty) = publish {
        std::fs::create_dir_all(dir.join("proto"))?;
        write_if_absent(
            &dir.join("proto")
                .join(format!("{}.proto", ty.to_lowercase())),
            &proto(ty),
        )?;
    }
    std::fs::create_dir_all(dir.join("src"))?;
    write_if_absent(&dir.join("src/main.rs"), &main_rs(publish))?;
    Ok(())
}

/// Add only `schema.dependencies`; YAML comments/formatting are normalized on insertion.
pub(crate) fn add(dep_path: &Path) -> Result<()> {
    add_in(&std::env::current_dir()?, dep_path)
}

fn add_in(dir: &Path, dep_path: &Path) -> Result<()> {
    let manifest = dir.join("main.yaml");
    let dep_manifest = dir.join(dep_path).join("main.yaml");
    let dependency: DepManifest = serde_yaml::from_str(
        &std::fs::read_to_string(&dep_manifest)
            .with_context(|| format!("reading {}", dep_manifest.display()))?,
    )
    .with_context(|| format!("parsing {}", dep_manifest.display()))?;
    let project = dependency
        .schema
        .and_then(|schema| schema.project)
        .with_context(|| format!("{} requires schema.project", dep_manifest.display()))?;
    let version = project
        .version
        .as_deref()
        .map_or_else(|| "0.1".to_owned(), major_minor);
    let path = dep_path.to_string_lossy().replace('\\', "/");
    let text = std::fs::read_to_string(&manifest)
        .with_context(|| format!("reading {}", manifest.display()))?;
    let mut doc: Value =
        serde_yaml::from_str(&text).with_context(|| format!("parsing {}", manifest.display()))?;
    if insert_dependency(&mut doc, &project.name, &version, &path)? {
        std::fs::write(&manifest, serde_yaml::to_string(&doc)?)?;
        println!(
            "added schema dependency '{}' (YAML formatting normalized)",
            project.name
        );
    } else {
        println!("schema dependency '{}' already declared", project.name);
    }
    Ok(())
}

fn insert_dependency(doc: &mut Value, name: &str, version: &str, path: &str) -> Result<bool> {
    let root = doc
        .as_mapping_mut()
        .context("main.yaml must be a mapping")?;
    let schema = root
        .entry(Value::from("schema"))
        .or_insert_with(|| Value::Mapping(Mapping::new()))
        .as_mapping_mut()
        .context("schema must be a mapping")?;
    let dependencies = schema
        .entry(Value::from("dependencies"))
        .or_insert_with(|| Value::Mapping(Mapping::new()))
        .as_mapping_mut()
        .context("schema.dependencies must be a mapping")?;
    let key = Value::from(name);
    if dependencies.contains_key(&key) {
        return Ok(false);
    }
    let entry = Mapping::from_iter([
        (Value::from("version"), Value::from(version)),
        (Value::from("path"), Value::from(path)),
    ]);
    dependencies.insert(key, Value::Mapping(entry));
    Ok(true)
}

fn write_cargo_toml(dir: &Path, proj: &str) -> Result<()> {
    let path = dir.join("Cargo.toml");
    let existing = path.is_file();
    let mut doc: toml::Table = if existing {
        toml::from_str(&std::fs::read_to_string(&path)?)
            .with_context(|| format!("parsing {}", path.display()))?
    } else {
        toml::Table::new()
    };
    let package = doc
        .entry("package")
        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        .as_table_mut()
        .context("Cargo.toml package must be a table")?;
    for (key, value) in [("name", proj), ("version", "0.1.0"), ("edition", "2021")] {
        package.entry(key).or_insert_with(|| value.into());
    }
    if !existing {
        // New modules are independently buildable even inside the SDK workspace.
        doc.insert("workspace".into(), toml::Value::Table(toml::Table::new()));
    }
    let crates = locate_reiny_crates(dir);
    for (section, name, local) in [
        ("dependencies", "reiny", crates.as_ref().map(|(sdk, _)| sdk)),
        (
            "build-dependencies",
            "reiny-build",
            crates.as_ref().map(|(_, build)| build),
        ),
    ] {
        let dependency = match local {
            Some(path) => toml::Value::Table(toml::Table::from_iter([(
                "path".into(),
                path.to_string_lossy().replace('\\', "/").into(),
            )])),
            None => env!("CARGO_PKG_VERSION").into(),
        };
        insert_dep(&mut doc, section, name, dependency)?;
    }
    let simple: toml::Table = toml::from_str(
        "prost = \"0.14\"\ntracing = \"0.1\"\ntokio = { version = \"1\", features = [\"full\"] }\ntracing-subscriber = { version = \"0.3\", features = [\"env-filter\"] }\n",
    )?;
    for (name, value) in simple {
        insert_dep(&mut doc, "dependencies", &name, value)?;
    }
    let rendered = toml::to_string_pretty(&doc)?;
    if existing {
        println!(
            "merging missing Cargo.toml entries; existing values retained, formatting normalized"
        );
    }
    std::fs::write(&path, rendered).with_context(|| format!("writing {}", path.display()))
}

fn insert_dep(doc: &mut toml::Table, section: &str, name: &str, value: toml::Value) -> Result<()> {
    doc.entry(section)
        .or_insert_with(|| toml::Value::Table(toml::Table::new()))
        .as_table_mut()
        .with_context(|| format!("Cargo.toml {section} must be a table"))?
        .entry(name)
        .or_insert(value);
    Ok(())
}

const BUILD_RS: &str = "//! main.yaml schema code generation.\nfn main() {\n    reiny_build::compile().expect(\"reiny schema code generation\");\n}\n";

fn main_yaml(proj: &str, bin: &str, publish: Option<&str>, locked: bool) -> Result<String> {
    let mut doc = Mapping::from_iter([
        (Value::from("version"), Value::from(2)),
        (Value::from("deployment"), Value::from(proj)),
        (
            Value::from("providers"),
            serde_yaml::from_str("{process: {type: process}}")?,
        ),
        (Value::from("build"), serde_yaml::from_str("{type: cargo}")?),
        (
            Value::from("run"),
            Value::Mapping(Mapping::from_iter([
                (Value::from("provider"), Value::from("process")),
                (Value::from("bin"), Value::from(bin)),
            ])),
        ),
        (Value::from("in"), Value::Mapping(Mapping::new())),
        (Value::from("out"), Value::Mapping(Mapping::new())),
    ]);
    if let Some(build) = doc
        .get_mut(Value::from("build"))
        .and_then(Value::as_mapping_mut)
    {
        build.insert(Value::from("locked"), Value::from(locked));
    }
    let mut publications = Mapping::new();
    if let Some(ty) = publish {
        let lower = ty.to_lowercase();
        let message = format!("{lower}.{ty}");
        publications.insert(
            Value::from(ty),
            Value::Mapping(Mapping::from_iter([
                (
                    Value::from("proto"),
                    Value::from(format!("proto/{lower}.proto")),
                ),
                (Value::from("message"), Value::from(message.clone())),
            ])),
        );
        doc.insert(
            Value::from("out"),
            Value::Mapping(Mapping::from_iter([(
                Value::from(lower),
                Value::Mapping(Mapping::from_iter([(
                    Value::from("type"),
                    Value::from(message),
                )])),
            )])),
        );
    }
    doc.insert(
        Value::from("schema"),
        Value::Mapping(Mapping::from_iter([
            (
                Value::from("project"),
                Value::Mapping(Mapping::from_iter([
                    (Value::from("name"), Value::from(proj)),
                    (Value::from("version"), Value::from("0.1.0")),
                ])),
            ),
            (Value::from("publications"), Value::Mapping(publications)),
            (Value::from("dependencies"), Value::Mapping(Mapping::new())),
        ])),
    );
    Ok(serde_yaml::to_string(&doc)?)
}

fn proto(ty: &str) -> String {
    format!(
        "syntax = \"proto3\";\n\npackage {};\n\nmessage {ty} {{\n  uint64 seq = 1;\n  int64 sent_unix = 2;\n}}\n",
        ty.to_lowercase()
    )
}

fn main_rs(publish: Option<&str>) -> String {
    match publish {
        Some(ty) => format!(
            "use std::time::Duration;\nuse reiny::prelude::*;\nuse crate::publications::{ty};\n\n\
             #[reiny::main]\nasync fn main(cloudy: Cloudy) -> reiny::Result<()> {{\n\
             \x20   let out = cloudy.output::<{ty}>(\"{}\")?;\n\
             \x20   cloudy.ready()?;\n\
             \x20   let mut tick = tokio::time::interval(Duration::from_secs(1));\n\
             \x20   let mut seq = 0;\n\
             \x20   loop {{\n\
             \x20       tokio::select! {{\n\
             \x20           _ = tick.tick() => {{\n\
             \x20               out.send({ty} {{ seq, sent_unix: cloudy.now_unix() }}).await?;\n\
             \x20               seq += 1;\n\
             \x20           }}\n\
             \x20           _ = cloudy.shutdown() => break,\n\
             \x20       }}\n\
             \x20   }}\n\
             \x20   Ok(())\n}}\n",
            ty.to_lowercase()
        ),
        None => "use reiny::prelude::*;\n\n#[reiny::main]\nasync fn main(cloudy: Cloudy) -> reiny::Result<()> {\n    cloudy.ready()?;\n    cloudy.shutdown().await;\n    Ok(())\n}\n".to_owned(),
    }
}

fn project_name(dir: &Path, name: Option<&str>) -> Result<String> {
    match name {
        Some(name) => Ok(name.to_owned()),
        None => std::fs::canonicalize(dir)?
            .file_name()
            .map(|name| name.to_string_lossy().into_owned())
            .with_context(|| format!("cannot derive a name from {}", dir.display())),
    }
}

fn locate_reiny_crates(dir: &Path) -> Option<(PathBuf, PathBuf)> {
    let dir = std::fs::canonicalize(dir).unwrap_or_else(|_| dir.to_path_buf());
    let executable = std::env::current_exe().ok();
    for start in std::iter::once(dir.as_path()).chain(executable.as_deref()) {
        for ancestor in start.ancestors() {
            let sdk = ancestor.join("crates/reiny");
            let build = ancestor.join("crates/reiny-build");
            if sdk.join("Cargo.toml").is_file() && build.join("Cargo.toml").is_file() {
                return Some((sdk, build));
            }
        }
    }
    None
}

fn major_minor(version: &str) -> String {
    let mut parts = version.split('.');
    match (parts.next(), parts.next()) {
        (Some(major), Some(minor)) => format!("{major}.{minor}"),
        _ => version.to_owned(),
    }
}

fn write_if_absent(path: &Path, contents: &str) -> Result<()> {
    use std::io::Write;
    match std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .open(path)
    {
        Ok(mut file) => file
            .write_all(contents.as_bytes())
            .with_context(|| format!("writing {}", path.display())),
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => Ok(()),
        Err(error) => Err(error).with_context(|| format!("creating {}", path.display())),
    }
}

#[derive(serde::Deserialize)]
struct DepManifest {
    schema: Option<DepSchema>,
}

#[derive(serde::Deserialize)]
struct DepSchema {
    project: Option<DepProject>,
}

#[derive(serde::Deserialize)]
struct DepProject {
    name: String,
    version: Option<String>,
}

#[cfg(test)]
#[allow(clippy::expect_used, clippy::unwrap_used)]
mod tests {
    use super::*;

    #[test]
    fn dependencies_preserve_runtime_when_schema_dependency_is_added() {
        // Given
        let mut doc: Value = serde_yaml::from_str(
            "version: 2\ndeployment: demo\nrun: {provider: process, bin: demo}\nschema:\n  project: {name: demo}\n  publications: {Ping: {proto: ping.proto}}\n",
        ).unwrap();
        let before = doc.clone();
        // When
        assert!(insert_dependency(&mut doc, "ping", "0.1", "../ping").unwrap());
        // Then
        assert_eq!(doc["run"], before["run"]);
        assert_eq!(
            doc["schema"]["publications"],
            before["schema"]["publications"]
        );
        assert_eq!(doc["schema"]["dependencies"]["ping"]["path"], "../ping");
        assert_eq!(doc["schema"]["dependencies"]["ping"]["version"], "0.1");
    }

    #[test]
    fn dependency_remains_unchanged_when_already_present() {
        // Given
        let mut doc: Value =
            serde_yaml::from_str("schema: {dependencies: {ping: {version: '0.1', path: ../ping}}}")
                .unwrap();
        let before = doc.clone();
        // When
        let added = insert_dependency(&mut doc, "ping", "0.2", "../other").unwrap();
        // Then
        assert!(!added);
        assert_eq!(doc, before);
    }

    #[test]
    fn dependencies_are_created_when_schema_is_absent() {
        // Given
        let mut doc: Value = serde_yaml::from_str("version: 2\ndeployment: demo").unwrap();
        // When
        insert_dependency(&mut doc, "ping", "0.1", "../ping").unwrap();
        // Then
        assert_eq!(doc["schema"]["dependencies"]["ping"]["path"], "../ping");
        assert_eq!(doc["deployment"], "demo");
    }

    #[test]
    fn executable_manifest_declares_named_output_when_publishing() {
        // Given / When
        let text = main_yaml("demo", "demo", Some("Ping"), false).unwrap();
        // Then
        let module: reiny_launch::ModuleManifest = serde_yaml::from_str(&text).unwrap();
        assert_eq!(module.version, 2);
        assert_eq!(module.outputs["ping"].type_name, "ping.Ping");
        assert!(!module.build.unwrap().locked);
        assert_eq!(module.run.unwrap().bin, "demo");
        let schema = module.schema.unwrap();
        assert_eq!(schema["publications"]["Ping"]["message"], "ping.Ping");
    }

    #[test]
    fn executable_manifest_has_no_ports_when_empty() {
        // Given / When
        let text = main_yaml("demo", "demo", None, true).unwrap();
        // Then
        let module: reiny_launch::ModuleManifest = serde_yaml::from_str(&text).unwrap();
        assert_eq!(module.version, 2);
        assert!(module.inputs.is_empty());
        assert!(module.outputs.is_empty());
        assert!(module.build.unwrap().locked);
    }

    #[test]
    fn major_minor_trims() {
        assert_eq!(major_minor("0.1.0"), "0.1");
        assert_eq!(major_minor("1.2.3-rc1"), "1.2");
        assert_eq!(major_minor("7"), "7");
    }
}
