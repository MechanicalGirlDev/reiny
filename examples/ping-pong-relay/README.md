# ping-pong-relay

A source/transform/sink pipeline: `ping.ping` feeds relay, and `relay.relayed`
feeds pong. Ping produces once per second. Relay records its canonical identity
and a hop count. Pong consumes and logs elapsed time without declaring an output.

Each leaf has its own build-time public catalog and schema dependencies; runtime
connections are declared by the root composition. Empty `in` or `out` mappings
are explicit contracts, so caller and child still agree for the source and sink.

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
