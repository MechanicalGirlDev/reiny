# Changelog

All notable changes to the reiny workspace crates (`reiny`, `reiny-build`,
`reiny-macros`, `reiny-launch`, `reiny-cli`). Versions are kept in lockstep via
`[workspace.package].version`.

## 0.3.0 — unreleased

Escape-hatch and schema-scaling pass. Design record: `docs/design/0.3.0.md`.

**BREAKING (wire):** topic keys gain a namespace segment — `reiny/<ns>/<id>/<TYPE>`
(publish) and `reiny/<ns>/*/<TYPE>` (subscribe), with `<ns>` defaulting to
`"default"`. 0.2 and 0.3 grains cannot talk to each other; upgrade every grain
together. No compatibility shim is provided. User code is otherwise
source-compatible: `publish::<T>()`, `subscribe::<T>()` and `recv()` keep both
their signatures and their meaning.

### Added

- **`Cloudy::session()` + `pub use zenoh`** — the escape hatch. Everything reiny
  does not wrap (queryable, liveliness, attachments, scouting) becomes reachable
  directly. This makes `zenoh` a *public* dependency of `reiny`: a zenoh major
  bump is from now on a reiny breaking change.
- **Session configuration.** `RuntimeOptions` + `run_with()`, plus
  `--zenoh-config <path>`, `--domain <ns>`, `--connect <endpoint>`,
  `--zenoh-mode peer|client` (and `REINY_DOMAIN`). `reiny-launch` gains `domain`
  and `zenoh_config` per `[grain]`. reiny does **not** define a TOML schema for
  zenoh configuration — the JSON5 file is passed straight through.
- **Presence.** `Cloudy::publish::<T>()` now also declares a liveliness token on
  the same key, so `Cloudy::publishers::<T>()` (snapshot) and
  `watch_publishers::<T>()` (`Joined` / `Left` stream) report who is currently
  publishing a type. Replaces the hand-rolled heartbeat + timeout liveness that
  downstreams had to write after the session was closed off.
- **Sender identity.** `Subscriber::recv_envelope()` returns `Envelope<T>`
  (`value` + `source` + zenoh `timestamp`), and
  `Cloudy::subscriber::<T>().from(id)` subscribes to a single publisher instead
  of all of them.
- **Latched pub/sub.** `.latched()` on the publisher / subscriber builders serves
  and fetches the last value, so a late-joining grain gets current state without
  a periodic re-publish. Implemented with a queryable plus one `get()` (no
  `zenoh-ext` dependency); a query reply is dropped once a live sample from that
  source has already been delivered.
- **QoS builder.** `Cloudy::publisher::<T>()` exposes `priority`, `congestion`
  and `express`.
- **`Topic::SCHEMA: Option<u64>`** — schema fingerprint carried in the zenoh
  attachment; a mismatch is warned about once per source and the sample dropped.
  It is a defaulted associated const, so hand-written `impl Topic` stays valid
  unchanged.
- **`Cloudy::shutdown_now()`** and **`Cloudy::extra_args()`**.
- **`reiny_build::compile_with(|cfg| …)`** — direct access to
  `prost_build::Config` (`type_attribute` for serde derives,
  `file_descriptor_set_path` for dynamic decoding, `bytes`, …). `compile()`
  becomes `compile_with(|_| {})`.
- **Multi-crate `[schema]`.** A workspace `Reiny.toml` may declare several
  `[schema.<name>]` entries (`crate`, `protos`, `depends`), splitting the shared
  schema across independently publishable crates. reiny-build compiles only the
  current crate's protos, auto-generates `extern_path` for the others (so leaf
  types are never generated twice), emits `impl Topic` only for its own types,
  and hands the proto include dir + owned FQN list to dependents through cargo
  `links` metadata (`DEP_*_PROTO_INCLUDE` / `DEP_*_PROTO_TYPES`) so published
  crates resolve from crates.io. The 0.2 single `[schema] crate = "…"` form keeps
  working as sugar for one entry.

### Changed

- **The runtime no longer takes process globals unconditionally.**
  `RuntimeOptions.install_tracing` (default `true`; also
  `#[reiny::main(tracing = false)]`) lets a grain install its own `tracing`
  subscriber, and `worker_threads` configures the tokio runtime. On unix,
  `SIGTERM` triggers shutdown in addition to Ctrl+C.
- `reiny-build` selects `protoc` via `Config::protoc_executable()` instead of
  writing the `PROTOC` process environment variable from the build script.

### Notes

- Unknown CLI arguments are still ignored rather than rejected (grain-specific
  flags are none of reiny's business), but are now retrievable through
  `Cloudy::extra_args()`.
- Deliberately **not** added: typed request/response, `subscribe_raw`, topic
  remapping, QoS in `Reiny.toml`, `[projects.*]` enforcement, and latched history
  beyond the latest value. Rationale in `docs/design/0.3.0.md` §4.

## 0.2.0

Developer-experience and build-scaling pass. All changes are backward compatible:
existing per-project / workspace manifests build unchanged; the new behavior is
either opt-in (`[schema]`, `REINY_VERBOSE`) or only rejects already-broken configs.

### Added

- **`[schema]` shared-schema crate (workspace mode).** A workspace `Reiny.toml`
  may declare `[schema] crate = "<pkg>"`. That package compiles `[internals]`
  **once** (full prost + `impl Topic`) and exposes it from its `lib.rs` via the
  new `reiny::schema!()` macro; every other grain becomes a *consumer* that
  recompiles no protos and re-exports `::<schema_crate>::internals::*` as
  `crate::internals`. Removes per-grain duplicate proto compilation as a workspace
  grows. Consumers no longer need a direct `prost` dependency. See
  `examples/ping-pong-schema`. (`reiny::schema!`, `reiny_build::Mode::Schema` /
  `Mode::SchemaConsumer`.)
- **`reiny check [path]`** — resolve the nearest `Reiny.toml` and print its layout
  mode + type→topic table **without compiling protos**. Runs manifest validation,
  so misconfig surfaces here, not just at build time.
- **Manifest validation in `reiny-build`.** `[dependencies]` keys,
  `[publications]`/`[internals]` aliases, and `[schema].crate` are validated as
  Rust identifiers up front — a hyphen or keyword now yields an actionable error
  pointing at the section (e.g. suggesting `control_app` for `control-app`)
  instead of a downstream `rustc` syntax error in generated code. Topic-segment
  collisions (two distinct types mapping to the same type name) are rejected too.
- **`REINY_VERBOSE=1`** build-time env var: `reiny_build::compile()` prints the
  resolved mode, every type→topic, and the deduped proto set via `cargo:warning=`.
- **Public introspection API** in `reiny-build`: `describe()`, `Resolution`,
  `Mode`, `TypeInfo` (used by `reiny check`).

### Changed

- **`reiny-build` is feature-split.** The default `compile` feature pulls
  `prost-build` / `protoc`; `default-features = false` leaves only manifest
  resolution + topic derivation (used by `reiny check`, so the CLI doesn't pull
  protoc).

## 0.1.0

Initial release: type-addressed pub/sub over Zenoh, build-time codegen from
`Reiny.toml` (`reiny-build` + `reiny-macros` + `reiny`), the `reiny` CLI
(`new`/`init`/`add`/`build`/`run`/`compress`), and the launcher.
