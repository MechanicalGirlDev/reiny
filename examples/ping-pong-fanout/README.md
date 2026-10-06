# ping-pong-fanout

One Ping output feeds three instances of the same Pong module. Their names
`pong-1`, `pong-2`, and `pong-3` are explicit composition names, not automatically
allocated process IDs. Each response carries its canonical module identity.

Ping declares three named Pong inputs (`pong_1`, `pong_2`, `pong_3`). The root
wires each to its exact sibling output and sends Ping to every Pong instance.
The receive loop selects across all three inputs; there is no global wildcard
subscription or accidental fan-in from another deployment.

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
