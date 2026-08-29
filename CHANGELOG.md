# Changelog

All notable changes to the reiny workspace crates (`reiny`, `reiny-core`,
`reiny-link`, `reiny-iceoryx2`, `reiny-ros2`, `reiny-build`, `reiny-macros`,
`reiny-launch`, `reiny-cli`).
Versions are kept in lockstep via `[workspace.package].version`.

## Unreleased

First slice of 0.5.0 (design record: `docs/design/0.5.0.md`, §11 records what
shipped). The `Engine` abstraction itself is not in yet; what landed is the
piece that does not depend on it.

### Added

- **`reiny-core`** — the type vocabulary (`Topic`, `Descriptor`, `Service`) as a
  `#![no_std]` crate (needs `alloc` through `prost`). `reiny` re-exports it, so
  downstream paths are unchanged. MCU firmware depends on it directly under the
  alias `reiny = { package = "reiny-core" }`, which makes the
  `impl ::reiny::Topic` that `reiny-build` generates resolve with no codegen
  change.
- **`reiny-link`** — reiny over **anything that moves bytes**. The core is a
  `#![no_std]` sans-I/O state machine, `Link`: `feed` it received bytes,
  `drain` / `drain_frame` what it wants sent, `tick` it with a millisecond
  clock; publish / subscribe / request by `Topic` / `Service` type, with the
  type name carried on the wire as a 32-bit FNV-1a hash and exchanged by name
  once in a `Hello`. Wire = COBS-framed
  `[kind][hash][seq][payload][crc16]`. With the default `std` feature it adds
  the host side: a `Transport` trait ("send bytes / receive bytes"),
  `Stream<T>` for any `AsyncRead + AsyncWrite` (serial ports via
  `transport::serial::open`, TCP, pty, `tokio::io::duplex`), `Udp`
  (point-to-point; learns the peer from the first datagram), and `Host`, which
  drives a `Link` on tokio and offers `send` / `call` / `recv`. CI builds
  `reiny-core` / `reiny-link` for `thumbv7em-none-eabihf` with
  `--no-default-features`.

- **QoS in reiny's own vocabulary** — `Qos { reliability, priority, history,
  durability, express }` with the enums `Reliability` / `Priority` / `History`
  / `Durability` live in `reiny-core` and are re-exported by `reiny` (and its
  prelude). `PublisherBuilder::qos(Qos)` takes a whole profile —
  `Qos::SENSOR` (best-effort, keep-last 1), `Qos::COMMAND` (the default:
  reliable, keep-all) or `Qos::STATE` (reliable, keep-last 1,
  transient-local = latched) — and `.reliability()` joins the per-field sugar.
  `.latched()` is now spelled `durability: TransientLocal` underneath. The
  engine mapping is documented in `docs/design/0.5.0.md` §2.3.

- **The `Engine` trait** (`reiny::engine`) — the five primitives reiny asks
  of a bus (publisher / subscribe / liveliness: declare, list, watch /
  respond + query) as one object-safe trait; `Cloudy` now holds an
  `Arc<dyn Engine>`. Keys are a struct (`engine::Key`, rendered per engine),
  samples are `engine::Sample` (bytes + attachment + unix-ns timestamp).
  Everything above the trait — encode / decode, fingerprint checks, latched
  ("presence, then query"), latest-wins, presence streams, services with
  `NoReply` / `Timeout` — is reiny's own code and runs unchanged on every
  engine; the receive buffers (Fifo 256, Ring for `latest(n)`) are reiny's
  now too. Design: `docs/design/0.5.0.md` §1.
- **`engine::Zenoh`** (feature `zenoh`, on by default) — the 0.4 behavior
  moved behind the trait; the wire (key shape, attachment, verbatim chunks)
  is unchanged except for the presence token renamed `@grain` → `@launch`
  (see *Changed*), so 0.4 and 0.5 interoperate for pub/sub and services but
  not for presence.
- **`engine::Local`** — an in-process bus with no network, ports or config.
  Clone one into several `Cloudy::open` calls and a launch's publish /
  subscribe / call runs inside a `#[tokio::test]`. It is also the reference
  implementation the conformance test is written against.
- **`Cloudy::open(RuntimeOptions)`** — the async entry (no tokio runtime, no
  signal handling; `run_with` is now runtime + signals + `open`).
  `RuntimeOptions::engine` picks the engine (`None` = zenoh);
  `Cloudy::engine()` exposes it, with `as_any()` for downcasts.
- A conformance test (`crates/reiny/src/engine/conformance.rs`) runs one
  scenario — pub/sub with source, cancel-safety, `latest(n)`, presence with
  history and leave, latched, fingerprints, services incl. `reply_err` /
  `NoReply` / `Timeout`, shutdown — against `Local` and against `Zenoh`
  (loopback port 37453). It is public behind reiny's `conformance` feature
  (`reiny::engine::conformance::{cloudy, exercise}`), so an engine crate
  passes by adding one test that calls it.
- **`reiny-iceoryx2`** — the `Engine` on
  [iceoryx2](https://github.com/eclipse-iceoryx/iceoryx2) 0.9: launches on one
  host over shared memory. One pub-sub service per type (`reiny/<d>/<T>`,
  the source id in a fixed user header), one request-response service per
  type for latched / services / `@schema`, presence as a `reiny-alive/<key>`
  service held open and polled every 200 ms, one engine thread on a
  `WaitSet`. `Reliability` maps to `BackpressureStrategy`
  (`RetryUntilDelivered` / `DiscardData`). Passes the same conformance test
  with two engines (two nodes) in one process. Creates iceoryx2's root
  directory (`/tmp/iceoryx2/`, `C:\Temp\iceoryx2\`) if missing. **Build note:** iceoryx2
  needs libclang (bindgen) on Windows / macOS — set `LIBCLANG_PATH`; on
  Linux it binds libc directly. Because of that the crate is not in the
  workspace's `default-members`: `cargo test` at the root skips it, CI runs
  `cargo test --workspace`, and locally it is `cargo test -p reiny-iceoryx2`.

- **`reiny::bridge::forward(a, b)`** — a raw bridge between two engines
  held by two `Cloudy`s with the same id / domain (`Cloudy::with_engine`
  builds the second). It mirrors presence tokens with the *original* source,
  forwards samples of every type it has seen a token for (attachment =
  fingerprint included), and relays queries — latched and services alike — by
  registering a responder per mirrored key and re-issuing each query to the
  *exact* source on the other side, which is what keeps `*` queries from
  bouncing. One echo rule: anything seen on a side that came from a source
  the bridge itself injected there is dropped. Tested `Local` ↔ `Local`.
- **`reiny-link` bridge mode and `LinkEngine`.** `Link::as_bridge()` sends a
  Hello with the new `HELLO_BRIDGE` flag: the peer treats a bridge as
  subscribing to, publishing and serving everything, and the bridge accepts
  any Data / Request without declaring types. `Link::calls::<S>()` (flag
  `CALLS`) lets an MCU name the request types it calls, which a bridge needs
  to route them. Raw (`hash` + bytes) variants `send_raw` / `request_raw` /
  `reply_raw` / `reply_err_raw` on `Link` and `Host`. `LinkEngine::spawn(host,
  domain)` (feature `engine`, default on) is the `Engine` over a `Host`: the
  peer's Hello is the presence, its LATCHED types are cached to answer
  latched queries, its requests reach the responder of that type, and a
  request dropped unanswered is turned into an error reply (links have no
  finalize). `Cloudy::open` with `engine = LinkEngine` puts a launch directly
  on a serial / UDP link — covered by `tests/engine.rs`.
- **`reiny bridge serial <port> | udp <bind> | iceoryx2`** — the CLI's only
  tokio subcommand: a zenoh `Cloudy` plus the other engine, joined by
  `bridge::forward`, so MCU and iceoryx2 launches show up in `reiny node list`
  / `topic hz` / `bag record`. `iceoryx2` is behind the CLI feature of the
  same name (libclang on Windows / macOS).

- **`reiny-ros2`** — a ROS 2 bridge *library* on pure-Rust DDS
  (`ros2-client` 0.10 / RustDDS; no ROS installation). A bridge launch builds
  a `Ros` (one ROS node, spinner on tokio; `ROS_DOMAIN_ID` and
  `ROS_LOCALHOST_ONLY=1` honoured) and adds routes per type with closures:
  `export::<T, R>` (reiny → ROS topic), `import::<R, T>` (ROS → reiny, source
  = the bridge's id), `export_service` (ROS clients → a reiny service) and
  `import_service` (reiny callers → a ROS service, failures become
  `reply_err`). `export_auto` / `import_auto` map same-shaped types by proto
  field name through `Topic::DESCRIPTOR` (prost-reflect + serde). QoS maps
  reliability / durability / history 1:1 onto DDS; `priority` / `express` are
  dropped. The ROS distribution is a feature (`jazzy` default). Covered by an
  in-process e2e against a ros2-client node over RustDDS loopback.

- **`reiny run` draws the topic flow at startup** — before spawning, the
  launcher resolves each launch's `Reiny.toml` (per-project and workspace
  layouts) and renders a sequence-diagram-style banner: one boxed column per
  launch, one horizontal arrow per type (`●─── Ping ──▶│`, `┼` where a line
  crosses an uninvolved rail), types colored per row on a TTY (`NO_COLOR`
  honoured). `[services]` request types draw as a double line caller
  `●══▶` server, labeled `Req => Reply`. Best-effort — a launch whose
  manifest cannot be resolved is skipped, and a dist layout with no
  `Reiny.toml` prints nothing. `reiny-build`'s `Resolution` gained
  `projects()` (the `[projects.*]` declarations) to feed it.

### Changed

- **Breaking (naming):** the word **grain** is gone — a reiny process is a
  **launch**. This renames three surfaces at once, with no compatibility
  shim: the launch config table `[grain]` → **`[launch]`**, the zenoh
  presence token `reiny/<domain>/<id>/@grain` → **`@launch`**, and the Rust
  names `GrainSpec` / `GrainEntry` / `ResolvedGrain` / `LaunchPlan::grains` /
  `LaunchConfig::grain` / `engine::GRAIN_CHUNK` / `engine::Key::grain` →
  `LaunchSpec` / `LaunchEntry` / `ResolvedLaunch` / `LaunchPlan::launches` /
  `LaunchConfig::launch` / `engine::LAUNCH_CHUNK` / `engine::Key::launch`.
  Pub/sub and services still interoperate with 0.4 (the data key shape is
  unchanged); presence does not — a 0.4 process holds `@grain` and shows up
  in `reiny node list` only through its publisher / server tokens.
- `reiny::{Topic, Descriptor, Service}` are now re-exports of `reiny-core`
  (same paths; no source change downstream).
- `reiny_link::Host::recv` takes `&self` (the receiver sits behind a tokio
  mutex) so a `Host` can be shared in an `Arc`; `wire::hello_write` gained
  the `bridge` argument and `wire::Hello` the `bridge` field.
- `engine::Key` renders a launch token as `ty = "@launch"` (verbatim in the
  type slot; `Key::launch`, `Key::all`, `Key::is_verbatim_type`), so
  `reiny/<d>/*/*/@service` and `reiny/<d>/*/*` are expressible patterns and
  `Key::matches` never lets `*` match a verbatim type.
- **Breaking:** `Cloudy::session()` returns `Option<&zenoh::Session>` (`None`
  when the engine is not zenoh). `pub use zenoh`, `ZenohSource`,
  `RuntimeOptions::{zenoh, zenoh_overrides, zenoh_config}` and `session()`
  itself live behind the `zenoh` feature (default on); with
  `default-features = false` reiny is the trait + `Local` only.
- **Breaking:** `Envelope.timestamp` is `Option<u64>` (unix ns) instead of
  `Option<zenoh::time::Timestamp>`; `CallError::Zenoh` is now
  `CallError::Engine`.
- `Publisher::send` / `Request::reply` / `Request::reply_err` keep their
  `async fn` signatures but no longer await anything: the engine methods are
  synchronous, because a zenoh 1.x builder's `.await` is `ready(wait())`.
- **Breaking:** `PublisherBuilder::priority` takes `reiny::Priority` (five
  levels: `RealTime` / `High` / `Normal` / `Low` / `Background`, mapped onto
  zenoh's) instead of `zenoh::qos::Priority`, and `.congestion()` is gone —
  `.reliability(Reliability::BestEffort)` is the `CongestionControl::Drop` it
  used to set. zenoh's own `reliability()` is still not used (it does not
  retransmit and is `unstable` in 1.10).
- **Behavior change:** publishers now default to `Reliability::Reliable`,
  i.e. zenoh `congestion_control(Block)` — a full link makes `send` wait
  instead of dropping. 0.4 left zenoh's default (`Drop`); use `Qos::SENSOR` or
  `.reliability(Reliability::BestEffort)` for that behavior.
- A publisher built with `history: KeepLast(n > 1)` is rejected at `build()`
  (a publisher keeps at most the one latched value; rings belong to the
  subscriber's `.latest(n)`).

### Fixed

- **`reiny-link`** — `Link::decode` / `decode_reply` through a stale `Frame`
  handle returned a default-valued message instead of `None`. `payload()`
  answers a stale handle with an empty slice, and an empty slice is a valid
  encoding of `T::default()`, so a caller holding an old handle silently read
  zeros. Both now check the handle's generation before decoding.
- **`reiny-link`** — `LinkEngine` did not retract the peer's presence when the
  transport ended (EOF or error); only the link's own silence timeout
  (`Disconnected`) did. After the far end went away, `publishers()` /
  `servers()` kept naming a peer nothing could reach.

### Notes

- An `embedded-io-async` adapter was left out; `feed` / `drain` is four
  lines. The raw bridge subscribes on a side to every type it has seen a
  token for (all sources), not only the types the other side wants —
  narrowing that needs an engine-side "who wants this type" query that does
  not exist yet.
- `reiny-ros2` on **Windows debug builds**: rustdds 0.14 still uses mio 0.6,
  whose Windows UDP code trips Rust 1.96's null-pointer UB check and aborts
  the DDS event loop. The crate builds; its e2e is `#[ignore]`d there and
  runs with `--release --include-ignored`. Linux (CI) is unaffected.
- ROS 2 actions / parameters are not bridged (0.4.0 §2.6: services + topics
  + latched express them; parameters are not a bridge's job).
- Publishers now default to `Reliable` (see *Changed*); a launch that relied on
  zenoh's `Drop` default for high-rate data should say `Qos::SENSOR`.

## 0.4.0 — 2026-08-28

Services, subscriber-side QoS and bus introspection. Design record:
`docs/design/0.4.0.md` (§11 records what shipped and where the implementation
departed from the design). **No wire break**: 0.3 and 0.4 grains interoperate;
the only new keys are verbatim chunks (`@service`, `@grain`) that `*` / `**`
never match.

### Added

- **Typed request/response (services).** The request type is the address:
  `cloudy.serve::<Req>()` declares a queryable at `reiny/<domain>/<id>/<Req>`
  (+ a liveliness token at `…/<Req>/@service`), `Server::recv()` yields a
  `Request<Req>` that is consumed by `.reply(resp)` / `.reply_err(msg)`, and
  `cloudy.call::<Req>(req)` / `cloudy.caller::<Req>().to(id).timeout(d).build()`
  return `Req::Response`. `CallError` separates `NoReply` (no server, or the
  server dropped the request), `Timeout`, `Remote(String)` (`reply_err`),
  `Schema` (response fingerprint mismatch), `Decode` and `Zenoh`. Fingerprints
  ride both directions; a request whose fingerprint does not match is answered
  with `reply_err`, not dropped, so the caller cannot mistake it for an absent
  server. `servers::<Req>()` / `watch_servers::<Req>()` mirror the publisher
  presence API. A type may be latched-published *and* served by the same grain:
  the two queryables tell each other's queries apart by payload presence.
  `trait Service { type Response; }` is one line to hand-write for third-party
  types, and `Reiny.toml` gains a `[services]` table
  (`Name = { request = "Req", response = "Resp" }`, aliases from
  `[publications]` / `[internals]`, or `<dep>::<Alias>` for per-project
  dependencies) from which `reiny-build` generates `impl reiny::Service` in the
  crate that owns the request type. `reiny check` prints a `services:` table.
  Example: `examples/ping-pong-service`.
- **`SubscriberBuilder::latest(n)`** — keep only the newest `n` samples and drop
  the oldest when full (zenoh `RingChannel`; ROS 2 `KEEP_LAST`). The default
  stays zenoh's `FifoChannel`, which **blocks the zenoh receive thread when
  full** — a subscriber that reads a 100 Hz state at frame rate should use
  `latest(1)`. `Subscriber::recv` / `recv_envelope` are documented as
  cancel-safe and pinned by the e2e, so a receive deadline is
  `tokio::time::timeout(d, sub.recv())` — no deadline API was added.
- **Grain presence.** `Cloudy::new` declares a liveliness token at
  `reiny/<domain>/<id>/@grain`, so `reiny node list` shows grains that publish
  nothing, and a grain whose id is already live on the bus is warned about at
  startup (not refused — `bag play --as` is a legitimate impersonation).
- **`reiny topic list | hz | bw | echo`, `reiny node list | info`,
  `reiny service list | call`** — `ros2 topic / node / service` equivalents.
  `list` / `node` / `service list` only query liveliness; `hz` / `bw` use one raw
  subscriber and report **per source**; `echo` and `service call` decode /
  encode through the descriptor a running grain serves at `@schema`
  (`prost-reflect`, confined to `reiny-cli/src/codec.rs`; types without a
  `DESCRIPTOR` are shown as hex). `service call Req '{json}' [--to id]` puts
  calibration / reset style services within reach of a shell. The bus
  vocabulary shared with `bag` moved to `reiny-cli/src/bus.rs`.

### Changed

- A latched publisher's queryable now ignores queries that carry a payload
  (those are service calls). Invisible to 0.3 subscribers, whose latched `get`
  carries none.
- `reiny-cli` depends on `prost`, `prost-reflect 0.14` and `serde_json`.

### Notes

- Deliberately **not** added: a receive deadline API (`tokio::time::timeout`),
  `.reliability()` (zenoh 1.9 keeps it `unstable` and it does not retransmit),
  an actions API (service + feedback topic + latched result is the convention),
  streaming replies, request sender identity (`Query::zid` is unstable),
  subscriber presence, `reiny topic pub`, `reiny node kill`. Rationale in
  `docs/design/0.4.0.md` §5.

## 0.3.0 — 2026-08-27

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
- **`reiny bag record | play | info`** — record the bus to an
  [MCAP](https://mcap.dev) file and replay it, `ros2 bag`-style. `record`
  subscribes to `reiny/<domain>/*/*` with one raw subscriber, captures the
  pre-start latched values with a single `get()` (snapshot), and writes each
  key's `(domain, source, type, fingerprint, latched)` into the channel
  metadata. `play` restores those keys (rewritable with `--domain` / `--as`),
  re-declares a publisher + liveliness token per channel (so presence and
  `.from()` see the replay), serves latched channels through a queryable, puts
  the fingerprint back on the attachment, and sleeps to absolute deadlines so
  the rate does not drift; replaying into a domain that already has a live
  publisher of that type is refused unless `--force`. `info` prints per-channel
  source / type / count / rate / latched / schema. Filtering, merging, splitting
  and conversion are delegated to the `mcap` CLI, and JSON dumping to
  `mcap cat --json`. Slicing is available with `--start` / `--duration` /
  `--rate` / `--loop` / `--type` / `--from`.
- **`Topic::DESCRIPTOR: Option<Descriptor>`** — a `reiny-build`-generated pointer
  to the crate's proto `FileDescriptorSet`. A `Some` publisher answers a
  queryable at `reiny/<domain>/<id>/<TYPE>/@schema/<message>` with that set, so a
  running grain describes its own types **on the bus**; `reiny bag record` picks
  them up and embeds a pruned, protobuf-encoded schema per channel, giving a bag
  that Foxglove and `mcap cat --json` decode with no reiny-specific code. The
  `@schema` chunk is a zenoh *verbatim* segment — invisible to `reiny/<domain>/**`
  subscribers and to `record`'s own `*/*` capture. Also a defaulted const;
  hand-written `impl Topic` opts out by leaving it `None`. `reiny-build` gains a
  `descriptors` feature (decode + fingerprint of an encoded set, no `protoc`)
  that `reiny bag` consumes; `compile` builds on top of it.

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
