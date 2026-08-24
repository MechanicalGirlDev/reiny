# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Maintaining this file

Capture only what reading the code won't tell you: cross-file architecture, the few non-obvious constraints, and project conventions. Do **not** restate things a single file or `grep` already reveals — crate `description` fields (see each `Cargo.toml`), API signatures, struct fields, doc-comment examples, generic `cargo` usage, or per-crate one-liners. Such content goes stale and bloats context. If a fact is verifiable by reading one file, leave it out.

## Orientation

reiny is a Rust SDK for distributed "grains" (processes) that communicate over [Zenoh](https://zenoh.io) pub/sub. Organizing principle: **type = topic**. A grain publishes and subscribes *by Rust type*, never by topic string — publishing `T` goes to `reiny/<domain>/<id>/T`, subscribing to `T` receives `reiny/<domain>/*/T` (the same type from every publisher; `<domain>` is a logical namespace from `--domain` / `REINY_DOMAIN`, default `"default"`). The type→topic mapping is generated at build time from a `Reiny.toml`, so user code only ever names types.

## Two build worlds — the root workspace excludes `examples/`

- **`crates/` is the core workspace.** Root `cargo build`/`test`/`clippy`/`fmt` operate here. Each crate's purpose is in its `Cargo.toml` `description`.
- **`examples/ping-pong-*` are each an independent cargo workspace** (own `Cargo.lock`), excluded from the root. To build or run one, `cd` into it. CI runs them as a matrix, excluding `ping-pong-cli` (shell-script walkthrough, no Rust).

## Build-time codegen pipeline (`reiny-build` + `reiny-macros` + `reiny`)

The cross-cutting flow to understand before touching any of these three:

1. Each grain's `build.rs` calls `reiny_build::compile()`.
2. `compile()` searches *upward* for the nearest `Reiny.toml` and resolves it in one of two layouts (`reiny_build::Mode`):
   - **per-project** (`[project]`): own `[publications]` + each `[dependencies]` project's public types → `publications::*` and `dependencies::<project>::*`.
   - **workspace** (`[internals]` / `[projects.*]`): the shared `[internals]` catalog → `internals::*`; the current package must appear as `[projects.<pkg-name>]`.
     - With an optional `[schema] crate = "<pkg>"`, workspace splits by the *current package*: the package whose name equals `[schema].crate` compiles `[internals]` **once** (full prost + `impl Topic`, exposed via `reiny::schema!()` in its `lib.rs`); every other package becomes a **consumer** that recompiles no protos and just re-exports `::<schema_crate>::internals::*` as `crate::internals`. This removes per-grain duplicate proto compilation. The shared `Topic`/`Message` impls live only in the schema crate (coherence makes them global). See `examples/ping-pong-schema`.
3. It compiles the protos with prost and writes `$OUT_DIR/reiny_generated.rs`: prost output under `__pb`, the re-export modules above, one `impl ::reiny::Topic` per type (where the topic string is embedded), and — if `[config]` is present — a typed `config::Config` plus a `cloudy.config()` extension trait.
4. `#[reiny::main]` `include!`s `reiny_generated.rs` into the crate root — which is why user code says `crate::publications::Ping`, not `reiny::...` — and wraps the async `main` in `reiny::__rt::run`.

Because the key is the Rust type, two crates sharing a type resolve to the same topic regardless of which "owns" it.

## Three manifest files (don't conflate)

- **`Cargo.toml`** — Rust build.
- **`Reiny.toml`** — reiny's manifest: identity, published/dependency types, optional `[schema] crate` (workspace shared-schema crate), `[config]` schema + defaults. Read at build time by `reiny-build` (schema: `crates/reiny-build/src/lib.rs`, `Manifest`).
- **launch config** (e.g. `ping-pong.toml`) — *deployment*: a `[grain]` table read by the launcher saying which grains to spawn together (`depends_on`, `on_exit`, etc.). Every key is an equal grain (no privileged component types); key = instance name = default bin name.

## The `reiny` CLI (`reiny-cli`) — one binary, several non-obvious behaviors

The `reiny` binary lives in **`reiny-cli`** (it depends on `reiny-launch` as a library; `reiny-launch` is now lib-only). Subcommands `new`/`init`/`add`/`check`/`build`/`run`/`compress` plus two non-subcommand entry paths handled *before* clap in `main`:

- **Backward-compat launch:** `reiny <launch>.toml` (bare positional) and `reiny --config <launch>.toml` both mean `reiny run`.
- **Renamed-launcher self mode:** if `argv[0]`'s basename isn't `reiny` (i.e. a `reiny compress --launcher <name>` artifact), it reads `<name>.toml` next to itself and launches with no args.

Facts that span files:
- `reiny check [path]` resolves the nearest `Reiny.toml` and prints its layout mode + type→topic table **without compiling protos**. It uses `reiny-build` with `default-features = false` (the `compile` feature, which pulls `prost-build`/`protoc`, is off), calling `reiny_build::describe()` (the catalog view, not package-bound — contrast `compile()`'s package-scoped `resolve_for`). Manifest validation (identifier checks, topic-collision) runs here too, so misconfig surfaces at `check` time, not just at build.
- `reiny build` is just `cargo build` in the cwd — codegen happens via the grain's `build.rs` (`reiny_build::compile()`), so the wrapper stays thin.
- `reiny new`/`init` scaffold **standalone** cargo projects (not workspace members); their `Cargo.toml` gets **path deps** to the reiny crates found by searching upward for `crates/reiny` + `crates/reiny-build` (so generated projects build inside this repo). `reiny add` only edits the target's `Reiny.toml` `[dependencies]` (textual insert, preserving comments) — never `src`.
- `reiny run` must find grain bins that may live in **separate `target/` dirs** (each scaffolded grain is its own project). It builds a search-dir list (`<launch>/<grain>/target/{debug,release}`, `<launch>/target/...`, the launcher's own dir) and hands it to `reiny_launch::run_launch_dirs` (the multi-dir generalization of `run_launch`).
- `reiny compress` bundles only what's needed (reachable grain bins + non-system `.so`s via `ldd` + launch config + the launcher itself) into one self-contained dir.

## Conventions & gotchas

- **Comments and rustdoc are in Japanese.** Match that, and read them for design intent before changing behavior.
- **`reiny-build` validates identifiers up front.** `[dependencies]` keys, `[publications]`/`[internals]` aliases, and `[schema].crate` all become Rust identifiers in generated code, so a hyphen (or keyword) is rejected with an actionable error pointing at the section — instead of a downstream `rustc` syntax error in `reiny_generated.rs`. Topic-segment collisions (two distinct types mapping to the same type name) are likewise rejected (`validate_no_topic_collision`).
- **`REINY_VERBOSE=1`** (a build-time env var) makes `reiny_build::compile()` print the resolved mode, every type→topic, and the deduped proto set via `cargo:warning=` — handy when an ownership/dependency assignment isn't compiling the type you expect.
- **`reiny-build` is feature-split.** The default `compile` feature pulls `prost-build`/`protoc`; `default-features = false` leaves only manifest resolution + topic derivation (used by `reiny check`). Keep prost-touching code under `#[cfg(feature = "compile")]`.
- `protoc` is **vendored** via `protoc-bin-vendored` — a bare `cargo build` needs no system protoc.
- **`crates/reiny/src/e2e.rs` is a live-zenoh test, not a unit test.** It opens three real sessions over a fixed loopback port (`tcp/127.0.0.1:37447`, multicast off) to check presence / latched / domain isolation together. It lives in `src/` rather than `tests/` because it needs the private `Cloudy::new` — adding a public constructor just to test would be the wrong trade. If it starts failing everywhere at once, suspect that port.
- **zenoh is a public dependency** (`pub use zenoh` + `Cloudy::session()`). Bumping zenoh's major version is a reiny breaking change; say so in the CHANGELOG when it happens.
- **Do not import zenoh's `QoSBuilderTrait` / `SampleBuilderTrait`.** Those traits live behind zenoh's `internal` + `unstable` features, but zenoh's `#[internal_trait]` macro *also* emits every one of their methods as an inherent method — so `.priority()`, `.congestion_control()`, `.express()` and `.attachment()` are callable on stable zenoh with no features enabled. Reaching for the trait import is what makes them look unavailable.
- **Multi-crate schemas ride cargo's `links` metadata, not `Reiny.toml`.** A `[schema.<name>]` crate emits `cargo:proto_include=` / `cargo:proto_types=`; dependents read `DEP_<LINKS>_PROTO_INCLUDE` / `_PROTO_TYPES` and convert them to prost `extern_path` entries. That indirection is deliberate: `Reiny.toml` is not shipped inside a published crate, so a schema crate consumed from crates.io has no other way to say where its protos live. Consequences: every schema crate needs `links = "<package name>"` hand-written in `Cargo.toml`, and `depends` must be transitively closed with matching Cargo dep edges (cargo only gives `DEP_*` to direct dependents). `examples/ping-pong-schema-split` is the worked example and the CI coverage for this path.
- **`prost-build` generates code for imported files too** (it passes `--include_imports`), which is exactly why the extern plumbing above is needed — without it a shared leaf type is generated separately in every crate that imports it, and the two are different Rust types.
- CI gate (mirror locally before pushing): `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, `cargo build --all-targets`. Run a single crate/test with `cargo test -p <crate>` / `cargo test <name>`.
