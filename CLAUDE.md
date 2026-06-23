# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Maintaining this file

Capture only what reading the code won't tell you: cross-file architecture, the few non-obvious constraints, and project conventions. Do **not** restate things a single file or `grep` already reveals — crate `description` fields (see each `Cargo.toml`), API signatures, struct fields, doc-comment examples, generic `cargo` usage, or per-crate one-liners. Such content goes stale and bloats context. If a fact is verifiable by reading one file, leave it out.

## Orientation

reiny is a Rust SDK for distributed "grains" (processes) that communicate over [Zenoh](https://zenoh.io) pub/sub. Organizing principle: **type = topic**. A grain publishes and subscribes *by Rust type*, never by topic string — publishing `T` goes to `reiny/<id>/T`, subscribing to `T` receives `reiny/*/T` (the same type from every publisher). The type→topic mapping is generated at build time from a `Reiny.toml`, so user code only ever names types.

## Two build worlds — the root workspace excludes `examples/`

- **`crates/` is the core workspace.** Root `cargo build`/`test`/`clippy`/`fmt` operate here. Each crate's purpose is in its `Cargo.toml` `description`.
- **`examples/ping-pong-*` are each an independent cargo workspace** (own `Cargo.lock`), excluded from the root. To build or run one, `cd` into it. CI runs them as a matrix, excluding `ping-pong-cli` (shell-script walkthrough, no Rust).
- `reiny-transport` (Zenoh + UDP, presence, `MessageRouter`) is lower-level and predates the SDK; the `reiny` SDK does **not** depend on it.

## Build-time codegen pipeline (`reiny-build` + `reiny-macros` + `reiny`)

The cross-cutting flow to understand before touching any of these three:

1. Each grain's `build.rs` calls `reiny_build::compile()`.
2. `compile()` searches *upward* for the nearest `Reiny.toml` and resolves it in one of two modes:
   - **per-project** (`[project]`): own `[publications]` + each `[dependencies]` project's public types → `publications::*` and `dependencies::<project>::*`.
   - **workspace** (`[internals]` / `[projects.*]`): the shared `[internals]` catalog → `internals::*`; the current package must appear as `[projects.<pkg-name>]`.
3. It compiles the protos with prost and writes `$OUT_DIR/reiny_generated.rs`: prost output under `__pb`, the re-export modules above, one `impl ::reiny::Topic` per type (where the topic string is embedded), and — if `[config]` is present — a typed `config::Config` plus a `cloudy.config()` extension trait.
4. `#[reiny::main]` `include!`s `reiny_generated.rs` into the crate root — which is why user code says `crate::publications::Ping`, not `reiny::...` — and wraps the async `main` in `reiny::__rt::run`.

Because the key is the Rust type, two crates sharing a type resolve to the same topic regardless of which "owns" it.

## Three manifest files (don't conflate)

- **`Cargo.toml`** — Rust build.
- **`Reiny.toml`** — reiny's manifest: identity, published/dependency types, `[config]` schema + defaults. Read at build time by `reiny-build` (schema: `crates/reiny-build/src/lib.rs`, `Manifest`).
- **launch config** (e.g. `ping-pong.toml`) — *deployment*: a `[grain]` table read by the `reiny-launch` CLI saying which grains to spawn together (`depends_on`, `on_exit`, etc.). Every key is an equal grain (no privileged component types); key = instance name = default bin name.

## Conventions & gotchas

- **Comments and rustdoc are in Japanese.** Match that, and read them for design intent before changing behavior.
- `protoc` is **vendored** via `protoc-bin-vendored` — a bare `cargo build` needs no system protoc.
- CI gate (mirror locally before pushing): `cargo fmt --all --check`, `cargo clippy --all-targets -- -D warnings`, `cargo build --all-targets`. Run a single crate/test with `cargo test -p <crate>` / `cargo test <name>`.
