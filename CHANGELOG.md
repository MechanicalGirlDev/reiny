# Changelog

All notable changes to the reiny workspace crates (`reiny`, `reiny-build`,
`reiny-macros`, `reiny-launch`, `reiny-cli`). Versions are kept in lockstep via
`[workspace.package].version`.

## 0.3.0 — unreleased

Escape-hatch and schema-scaling pass. Design record: `docs/design/0.3.0.md`
(§8 records what shipped and where the implementation departed from the design).

**BREAKING (wire):** topic keys gain a namespace segment — `reiny/<domain>/<id>/<TYPE>`
(publish) and `reiny/<domain>/*/<TYPE>` (subscribe), with `<domain>` defaulting to
`"default"`. 0.2 and 0.3 grains cannot talk to each other; upgrade every grain
together. No compatibility shim is provided. User code is otherwise
source-compatible: `publish::<T>()`, `subscribe::<T>()` and `recv()` keep both
their signatures and their meaning.

### Added

- **`Cloudy::session()` + `pub use zenoh`** — the escape hatch. Everything reiny
  does not wrap (queryable, attachments, scouting, arbitrary key expressions)
  becomes reachable directly. This makes `zenoh` a *public* dependency of
  `reiny`: a zenoh major bump is from now on a reiny breaking change.
- **Session configuration.** `RuntimeOptions` + `run_with()`, plus
  `--domain <ns>` (or `REINY_DOMAIN`), `--zenoh-config <path>`,
  `--connect <endpoint>` (repeatable) and `--zenoh-mode peer|client`.
  `reiny-launch` gains a launch-wide `domain` key and per-`[grain]` `domain` /
  `zenoh_config`. reiny does **not** define a TOML schema for zenoh
  configuration — the JSON5 file is passed straight through, and the two CLI
  shorthands become `Config::insert_json5` overrides.
- **Presence.** `Cloudy::publish::<T>()` now also declares a liveliness token on
  the same key, so `Cloudy::publishers::<T>()` (snapshot) and
  `watch_publishers::<T>()` (`Joined` / `Left` stream) report who is currently
  publishing a type. Replaces the hand-rolled heartbeat + timeout liveness that
  downstreams had to write after the session was closed off. There is no
  opt-out: a publisher without a token would make the answers untrustworthy.
- **Sender identity.** `Subscriber::recv_envelope()` returns `Envelope<T>`
  (`value` + `source` + zenoh `timestamp`), and
  `Cloudy::subscriber::<T>().from(id)` subscribes to a single publisher instead
  of all of them.
- **Latched pub/sub.** `.latched()` on the publisher / subscriber builders serves
  and fetches the last value, so a late-joining grain gets current state without
  a periodic re-publish. Implemented with a queryable plus one `get()` (no
  `zenoh-ext` dependency); a query reply is dropped once a live sample from that
  source has already been delivered.
- **`Cloudy::shutdown_now()`**, **`Cloudy::extra_args()`**, **`Cloudy::domain()`**.
- **`reiny_build::compile_with(|cfg| …)`** — direct access to
  `prost_build::Config` (`type_attribute` for serde derives,
  `file_descriptor_set_path` for dynamic decoding, `bytes`, …). `compile()`
  becomes `compile_with(|_| {})`, and `prost_build` is re-exported so build
  scripts name the same version reiny compiled against.
- **QoS builder.** `Cloudy::publisher::<T>()` takes `.priority()`,
  `.congestion()` and `.express()`.
- **Multi-crate `[schema]`.** A workspace `Reiny.toml` may declare several
  `[schema.<name>]` entries (`crate`, `protos`, `depends`), splitting the shared
  schema across independently publishable crates. Ownership of an `[internals]`
  type follows its proto path — nothing extra to declare. Each schema crate
  compiles only its own protos and publishes its proto include dir plus the FQNs
  it defines through cargo `links` metadata (`cargo:proto_include=` /
  `cargo:proto_types=`); dependents read those back as `DEP_<LINKS>_*` and turn
  them into prost `extern_path` entries, so a leaf type shared by two schema
  crates is generated exactly once and both sides see the *same* Rust type.
  Since the metadata travels through `links`, this keeps working for schema
  crates published to crates.io, where `Reiny.toml` is not available. The 0.2
  single `[schema] crate = "…"` form keeps working as sugar for one entry.
  Two rules are enforced with actionable errors: each schema crate needs
  `links = "<package name>"` in its `Cargo.toml` (reiny cannot write that
  itself), and `depends` must be transitively closed with a matching Cargo
  dependency edge (cargo only hands `DEP_*` to *direct* dependents).
- **`Topic::SCHEMA: Option<u64>`** — a schema fingerprint derived from the proto
  descriptor, carried in the zenoh attachment on publish and checked on
  subscribe. On mismatch the sample is dropped and the source is warned about
  once. `TYPE` is the bare type name, so the topic namespace is flat and two
  projects can land different types on one topic; protobuf is permissive enough
  to decode such a sample into silent garbage, and this is the guard against
  that. It is a defaulted associated const, so hand-written `impl Topic` stays
  valid unchanged.

### Changed

- **The runtime no longer takes process globals unconditionally.**
  `RuntimeOptions.install_tracing` (default `true`; also
  `#[reiny::main(tracing = false)]`) lets a grain install its own `tracing`
  subscriber, and `worker_threads` configures the tokio runtime. On unix,
  `SIGTERM` triggers shutdown in addition to Ctrl+C, and a signal handler that
  cannot be installed now waits forever instead of shutting the grain down.
- `--id` / `--domain` are validated as single key segments at startup. A value
  containing `/`, `*`, `?`, `#`, `$`, `@` or whitespace used to silently corrupt
  every key the grain touched.
- `reiny-build` selects `protoc` via `Config::protoc_executable()` instead of
  writing the `PROTOC` process environment variable from the build script.

### Notes

- Unknown CLI arguments are still ignored rather than rejected (grain-specific
  flags are none of reiny's business), but are now retrievable through
  `Cloudy::extra_args()`.
- The fingerprint covers a message's **own** declared fields (number, name,
  type, label, referenced type name, oneof membership) and its fully-qualified
  name — not the contents of the messages it references. It exists to separate
  same-named types from different projects, which the top-level shape already
  distinguishes; a leaf type that needs versioning of its own should be a topic
  type of its own.
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
