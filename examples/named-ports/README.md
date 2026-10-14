# Named ports

Run the real SDK proof on the ordered Local engine:

```sh
cargo run --locked --manifest-path examples/named-ports/Cargo.toml -- --local
```

The executable opens two outputs of `named.Value` and two RPC servers of
`named.Request`. Exact input endpoint pairs select independent values and replies.
It prints `NAMED_STREAM_RPC_OK` only after verifying stream provenance, payloads,
delivery counts, and both RPC responses under bounded deadlines.

The adjacent `main.yaml` runs the same proof through the deployment launcher and
Zenoh. In managed mode the process stays ready until stopped; read the marker from
the deployment generation's `stdout.log`.
