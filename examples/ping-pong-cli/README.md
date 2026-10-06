# ping-pong-cli

A shell walkthrough creating two independent Cargo projects using the reiny CLI.
Build `reiny-cli` at the repository root and put `target/debug` on PATH first.

```sh
./run-all.sh
./05-run.sh
# Alternatively package the built deployment:
./06-compress.sh
```

The numbered steps clean generated projects, run `reiny new` and `reiny init`,
add reciprocal schema dependencies, and build each project. Adding a dependency
does not write behavior: `03-add.sh` installs checked-in `templates/` with complete
named-port loops and matching leaf manifests. The root `main.yaml` composes them.

`04-build.sh` shares `CARGO_TARGET_DIR` between independent projects so the root
process provider finds both binaries in `target/debug`. Cargo generates the locks.
Run starts the composition with `reiny run main.yaml`; compress packages that
same manifest and reachable artifacts.

Generated `ping/`, `pong/`, and `target/` remain disposable. Checked-in files are
the root manifest, templates, scripts, and this walkthrough.
