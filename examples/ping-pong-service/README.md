# ping-pong-service

Typed SDK request/response services remain supported inside namespaced modules.
The root `schema.internals` defines Add and Sum; `schema.services.Adder` maps
the request alias to the response alias and generates `impl reiny::Service for Add`.
Runtime input and output contracts are empty because these applications use SDK
service calls rather than pub/sub ports.

Calc declares `cloudy.serve::<Add>()` before readiness and returns Sum or an
overflow error. Asker uses a two-second caller timeout and retries once per second.
The caller selects calc within its own composition namespace rather than guessing
a global process ID. Service errors distinguish `NoReply`, `Timeout`, and
`Remote`; `servers::<Add>()` and `watch_servers::<Add>()` expose presence.

## Run

From this independent Cargo workspace:

```sh
cargo build --locked
reiny run main.yaml
```

The root process provider resolves binaries in `target/debug`. Each runtime leaf has its own
`main.yaml`, explicit `in`/`out` contracts, and a Cargo build declaration. Runtime module
paths determine identities under the deployment namespace. Ports are created synchronously;
`cloudy.ready()?` is called only after every named port has been created.
