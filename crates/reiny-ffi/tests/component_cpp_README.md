# C++ executable component adapter

`../include/reiny_component.hpp` is a C++17, header-only adapter in
`reiny::component`. It can coexist with the legacy `reiny.hpp`.
Install or copy the complete `include/` directory: this entrypoint includes
`reiny_component_ports.hpp` and `reiny_component_base.hpp`. They are internal
header-only implementation pieces, not optional standalone dependencies.

Implement `Component::run`, optionally override `prepare` and `stop`, and register
a factory returning `std::unique_ptr<Component>` with `Registry::add(name,
revision, factory)`. Seal the registry explicitly. Import the strict, owner-resolved
graph JSON with `Graph::import`, then call `Host::launch_local` and
`Host::wait_running`. This does not load code from factory names.

During `prepare`, use `ContextView::input`, `output`, `rpc_client`, or `rpc_server`
with exact declared names and explicit request/response schemas. Bytes remain
opaque; use your application's protobuf codec and the matching generated
fingerprint artifacts. Schema equality does not validate payloads.

Ownership and execution:

- All owned handles are move-only. `close()` reports release errors; destructors
  release without throwing. `ContextView` is borrowed for one callback only;
  `retain()` produces an owned context claim.
- Factory captures must own their dependencies and support concurrent creates.
  Each instance's create, prepare, run, stop, and destructor execute serially
  on one ordinary native thread. Exceptions from create/prepare/run/stop become callback
  errors and copied diagnostics. Component destructors and factory-capture
  destructors must not throw. Factory finalization runs synchronously on the
  thread releasing its last native owner, not necessarily the instance lane.
  Callable/capture destructors must neither block nor unwind; the `noexcept`
  finalizer does not make an invalid destructor safe.
- Subscribe before the triggering action. `Host::request_stop` requests
  cooperative stop. `wait_terminal` returns a JSON outcome even for failed or
  unresponsive cleanup; inspect it and `phase()` rather than treating successful
  waiting as successful execution.
- `wait_joined` separately proves callback teardown. A terminal unresponsive
  host may still be executing callbacks. Release requests stop, but never joins
  implicitly. Keep callback machine code loaded until every host has joined
  and all registry/factory claims are released. Release retained contexts and
  endpoints to reclaim transport resources. Independently, keep the native ABI
  library loaded for the entire process lifetime because its shared runtime is
  process-scoped.
- Stop closes new endpoint admissions, not previously admitted effects.
  Context retention preserves resources, not authority. Untracked detached
  native threads cannot be certified by the host.
- RPC call and wait budgets are explicit positive `std::chrono::nanoseconds`.
  A wait timeout or last-observer release does not undo server execution.
  There is no automatic retry. Reply is one-shot; destroying the last unanswered
  request finalizes NoReply. `Error::status()` preserves the C ABI category.
- `Registry::run_process` reads the inherited private bootstrap and runs the
  actual service process-host protocol. The app-owned executable must exit
  after this call returns or throws. It is not a spawn/reap or deployment API.

This ABI has no step callbacks, encoded step graph/value API, jobs, deployment
ownership, distributed scheduling, or zero-copy foreign payloads. The adapter
does not synthesize those operations. Host outcomes and metadata remain JSON
buffers because the C ABI exposes documents, not typed accessors.

## Real integration client

`component_cpp_client.cpp` uses real factories and a closed Local graph. It
checks binary and empty payloads, exact stream provenance, successful/remote-error/
NoReply RPC, retained operation results, one-shot replies, forbidden prepare and
post-stop sends, factory ownership on failed registration, release of public
registry handles during execution, standard/unknown/factory exceptions, serialized
destruction, and unresponsive-to-joined cleanup with an explicit future gate.
It does not use sleeps or mock the native runtime.

From the repository root, point `LIBDIR` at the freshly verified ABI artifact
directory, and use a separate output directory:

```sh
OUT=$(mktemp -d /tmp/reiny-cpp-check-XXXXXX)
g++ -std=c++17 -Wall -Wextra -Werror -pedantic -pthread \
  -I crates/reiny-ffi/include \
  crates/reiny-ffi/tests/component_cpp_client.cpp \
  -L "$LIBDIR" -Wl,-rpath,"$LIBDIR" -lreiny_ffi \
  -o "$OUT/component_cpp_client"
"$OUT/component_cpp_client"
```

Header-only compilation uses the same compiler flags with `-fsyntax-only` and
without linker arguments. These commands do not test a foreign process-host
deployment; that requires an actual owned host executable and owner receipts.
