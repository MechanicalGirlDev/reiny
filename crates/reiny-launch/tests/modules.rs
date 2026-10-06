//! Module connections are resolved independently of process startup order.

#![allow(clippy::expect_used)]

use reiny_launch::{DeploymentPlan, ModuleError};

fn write(dir: &std::path::Path, name: &str, text: &str) {
    let path = dir.join(name);
    std::fs::create_dir_all(path.parent().expect("fixture directory")).expect("create fixture");
    std::fs::write(path, text).expect("write fixture");
}

const PROVIDER: &str = "version: 1\ndeployment: test\nproviders:\n  process: {type: process}\n";
const PRODUCER: &str =
    "version: 1\nrun: {provider: process, bin: producer}\nout:\n  state: {type: test.State}\n";
const CONSUMER: &str =
    "version: 1\nrun: {provider: process, bin: consumer}\nin:\n  state: {type: test.State}\n";

#[test]
fn sibling_connection_resolves_exact_namespace_when_types_match() {
    // Given two same-type publishers, only one is explicitly connected.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        &format!(
            "{PROVIDER}modules:\n  a:\n    source: ./a\n    out: {{state: {{type: test.State}}}}\n  b:\n    source: ./b\n    out: {{state: {{type: test.State}}}}\n  c:\n    source: ./c\n    in: {{state: {{type: test.State, from: a.state}}}}\n"
        ),
    );
    write(fixture.path(), "a/main.yaml", PRODUCER);
    write(fixture.path(), "b/main.yaml", PRODUCER);
    write(fixture.path(), "c/main.yaml", CONSUMER);
    // When the deployment is planned.
    let plan = DeploymentPlan::load(fixture.path(), false).expect("valid plan");
    // Then the input is bound to a, never a wildcard or b.
    let consumer = plan
        .nodes
        .iter()
        .find(|node| node.namespace == "test/c")
        .expect("consumer");
    assert_eq!(consumer.bindings.inputs["state"].source, "test/a");
    assert_eq!(plan.nodes.len(), 3);
}

#[test]
fn parent_forwarding_resolves_original_publisher_without_relay() {
    // Given exports and parent inputs cross two nested module boundaries.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        &format!(
            "{PROVIDER}modules:\n  branch:\n    source: ./branch\n    out: {{state: {{type: test.State}}}}\n  c:\n    source: ./c\n    in: {{state: {{type: test.State, from: branch.state}}}}\n"
        ),
    );
    write(
        fixture.path(),
        "branch/main.yaml",
        "version: 1\nmodules:\n  producer:\n    source: ./producer\n    out: {state: {type: test.State}}\nout:\n  state: {type: test.State, from: producer.state}\n",
    );
    write(fixture.path(), "branch/producer/main.yaml", PRODUCER);
    write(
        fixture.path(),
        "c/main.yaml",
        "version: 1\nin:\n  state: {type: test.State}\nmodules:\n  consumer:\n    source: ./consumer\n    in: {state: {type: test.State, from: in.state}}\n",
    );
    write(fixture.path(), "c/consumer/main.yaml", CONSUMER);
    // When aliases are resolved.
    let plan = DeploymentPlan::load(fixture.path(), false).expect("valid plan");
    // Then no intermediate module has become a publisher process.
    assert_eq!(plan.nodes.len(), 2);
    assert_eq!(
        plan.nodes[1].bindings.inputs["state"].source,
        "test/branch/producer"
    );
}

#[test]
fn mismatched_connection_fails_before_any_process_is_started() {
    // Given a caller declares the child's input with the wrong type.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        &format!(
            "{PROVIDER}modules:\n  c:\n    source: ./c\n    in: {{state: {{type: test.Other, from: absent.state}}}}\n"
        ),
    );
    write(fixture.path(), "c/main.yaml", CONSUMER);
    // When the declaration is loaded.
    let result = DeploymentPlan::load(fixture.path(), false);
    // Then type mismatch is reported instead of selecting another publisher.
    assert!(matches!(result, Err(ModuleError::TypeMismatch { .. })));
}

#[test]
fn undefined_output_fails_when_reference_is_not_public() {
    // Given a producer's output is not exposed by its caller.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        &format!(
            "{PROVIDER}modules:\n  a: {{source: ./a}}\n  c:\n    source: ./c\n    in: {{state: {{type: test.State, from: a.state}}}}\n"
        ),
    );
    write(fixture.path(), "a/main.yaml", PRODUCER);
    write(fixture.path(), "c/main.yaml", CONSUMER);
    // When the connection is resolved.
    let result = DeploymentPlan::load(fixture.path(), false);
    // Then internal outputs are inaccessible.
    assert!(matches!(result, Err(ModuleError::Reference { .. })));
}

#[test]
fn feedback_between_executable_modules_is_not_an_alias_cycle() {
    // Given two processes exchange the same message type in both directions.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        &format!(
            "{PROVIDER}modules:\n  a:\n    source: ./a\n    in: {{state: {{type: test.State, from: b.state}}}}\n    out: {{state: {{type: test.State}}}}\n  b:\n    source: ./b\n    in: {{state: {{type: test.State, from: a.state}}}}\n    out: {{state: {{type: test.State}}}}\n"
        ),
    );
    let both = "version: 1\nrun: {provider: process, bin: peer}\nin:\n  state: {type: test.State}\nout:\n  state: {type: test.State}\n";
    write(fixture.path(), "a/main.yaml", both);
    write(fixture.path(), "b/main.yaml", both);
    // When the communication graph is resolved.
    let plan = DeploymentPlan::load(fixture.path(), false).expect("feedback is valid");
    // Then both exact-source connections exist without a startup order.
    assert_eq!(plan.nodes[0].bindings.inputs["state"].source, "test/b");
    assert_eq!(plan.nodes[1].bindings.inputs["state"].source, "test/a");
}

#[test]
fn pure_alias_cycle_fails_when_no_executable_can_publish() {
    // Given two composites forward each other's input indefinitely.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        &format!(
            "{PROVIDER}modules:\n  a:\n    source: ./a\n    in: {{state: {{type: test.State, from: b.state}}}}\n    out: {{state: {{type: test.State}}}}\n  b:\n    source: ./b\n    in: {{state: {{type: test.State, from: a.state}}}}\n    out: {{state: {{type: test.State}}}}\n  producer: {{source: ./producer}}\n"
        ),
    );
    let alias = "version: 1\nin:\n  state: {type: test.State}\nout:\n  state: {type: test.State, from: in.state}\n";
    write(fixture.path(), "a/main.yaml", alias);
    write(fixture.path(), "b/main.yaml", alias);
    write(fixture.path(), "producer/main.yaml", PRODUCER);
    // When aliases are followed.
    let result = DeploymentPlan::load(fixture.path(), false);
    // Then the cycle is rejected.
    assert!(matches!(result, Err(ModuleError::Cycle(_))));
}

#[test]
fn recursive_local_module_source_is_rejected() {
    // Given a module tries to instantiate its own source recursively.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        &format!("{PROVIDER}modules:\n  again: {{source: .}}\n"),
    );
    // When source traversal begins.
    let result = DeploymentPlan::load(fixture.path(), false);
    // Then the recursion is rejected before expanding forever.
    assert!(matches!(result, Err(ModuleError::Cycle(_))));
}

#[test]
fn unknown_fields_and_old_launch_tables_are_rejected() {
    // Given a retired launch table is present.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        &format!("{PROVIDER}launch: {{old: {{bin: old}}}}\n"),
    );
    // When strict YAML parsing runs.
    let result = DeploymentPlan::load(fixture.path(), false);
    // Then it cannot silently become an empty deployment.
    assert!(matches!(result, Err(ModuleError::Yaml { .. })));
}

#[test]
fn run_and_modules_cannot_share_one_instance() {
    // Given a module ambiguously declares both a process and child modules.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        &format!(
            "{PROVIDER}run: {{provider: process, bin: root}}\nmodules:\n  child: {{source: ./child}}\n"
        ),
    );
    // When the module invariant is checked.
    let result = DeploymentPlan::load(fixture.path(), false);
    // Then it fails without looking for the nonexistent child.
    assert!(matches!(result, Err(ModuleError::Invalid { .. })));
}

#[test]
fn provider_mapping_selects_parent_configuration_without_child_redefinition() {
    // Given the caller supplies another process configuration under the child's slot.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        "version: 1\ndeployment: test\nproviders:\n  remote: {type: process, bin_dir: remote-bin}\nmodules:\n  child:\n    source: ./child\n    providers: {process: remote}\n    out: {state: {type: test.State}}\n",
    );
    write(fixture.path(), "child/main.yaml", PRODUCER);
    // When provider slots are resolved.
    let plan = DeploymentPlan::load(fixture.path(), false).expect("mapped provider");
    // Then the child uses the caller's absolute binary directory.
    assert!(plan.nodes[0].provider.bin_dir.ends_with("remote-bin"));
    assert!(plan.nodes[0].provider.bin_dir.is_absolute());
}

#[test]
fn companion_library_changes_replace_prepared_fingerprint_without_overwriting_old_bundle() {
    // Given a prebuilt module whose native runtime library changes independently.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        "version: 1\ndeployment: test\nproviders:\n  process: {type: process, bin_dir: bin}\nrun: {provider: process, bin: producer}\n",
    );
    let binary = format!("bin/producer{}", std::env::consts::EXE_SUFFIX);
    write(fixture.path(), &binary, "binary content");
    write(fixture.path(), "bin/companion.dll", "first library");
    let plan = DeploymentPlan::load(fixture.path(), false).expect("plan");
    let first = plan.prepare().expect("prepare first");
    write(fixture.path(), "bin/companion.dll", "second library");
    // When the same declaration is prepared after the library update.
    let second = plan.prepare().expect("prepare updated library");
    // Then the immutable address and reconciliation fingerprint change.
    assert_ne!(first.nodes[0].fingerprint, second.nodes[0].fingerprint);
    assert_ne!(first.nodes[0].executable, second.nodes[0].executable);
    let first_dir = first.nodes[0].executable.parent().expect("first bundle");
    assert_eq!(
        std::fs::read_to_string(first_dir.join("companion.dll")).expect("old library"),
        "first library"
    );
}

#[test]
fn working_directory_changes_replace_prepared_fingerprint_with_identical_binary() {
    // Given identical binaries and bindings with a different module source directory.
    let fixture = tempfile::tempdir().expect("fixture");
    write(
        fixture.path(),
        "main.yaml",
        "version: 1\ndeployment: test\nproviders:\n  process: {type: process, bin_dir: bin}\nrun: {provider: process, bin: producer}\n",
    );
    let binary = format!("bin/producer{}", std::env::consts::EXE_SUFFIX);
    write(fixture.path(), &binary, "binary content");
    let mut plan = DeploymentPlan::load(fixture.path(), false).expect("plan");
    assert!(!fixture.path().join("lock.yaml").exists());
    let first = plan.prepare().expect("prepare original directory");
    let moved = fixture.path().join("moved");
    std::fs::create_dir(&moved).expect("new working directory");
    plan.nodes[0].module_dir = moved;

    // When the execution directory moves, then reconciliation must replace the process.
    let second = plan.prepare().expect("prepare moved directory");
    assert_eq!(first.nodes[0].executable, second.nodes[0].executable);
    assert_ne!(first.nodes[0].fingerprint, second.nodes[0].fingerprint);
}
