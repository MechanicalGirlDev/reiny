# Hosted component placement

Two application-owned `ProcessHost` executables register factories from the same
library. The root places the sender and receiver in separate processes; named
`data` and `acknowledgement` ports exchange a typed protobuf value over Zenoh
without changing its source namespace or output port. Retention and replay make
the initial value available when the remote subscriber becomes visible.

Build this independent workspace, then use the repository's `reiny` CLI:

```sh
cargo build --manifest-path examples/component-placement/Cargo.toml
reiny plan examples/component-placement
reiny apply examples/component-placement --detach
reiny status examples/component-placement --json
reiny stop examples/component-placement
```

The commands assume the repository root is the working directory. The sender
listens at `tcp/127.0.0.1:37458` and the receiver explicitly connects to it.
Change both provider JSON files to select another loopback port. Each JSON file must
contain `connect.endpoints`, `listen.endpoints`, and
`scouting.multicast.enabled`, with no other settings in host protocol v1.

Each process prints `prepared`, `run`, `delivered`, and `stop` JSON records in the owner log.
The integration test supplies a private event listener through per-instance
TOML configuration instead. That listener observes real lifecycle and payload
events and can request an abrupt exit with `exit\n`; it does not replace the
SDK host protocol or Zenoh. `wrong_type = true` selects a deliberately
incompatible compiled schema to exercise whole-graph rejection before run.
`prepare_gate = true` waits for `prepare\n` from the event controller before
returning from the prepare hook. An optional absolute `run_marker` path records
the first run poll synchronously, so rejection checks do not race event readers.

Run the real CLI regression with:

```sh
cargo test -p reiny-cli --test component_process -- --nocapture
```

The test copies this example and its SDK dependencies into a private source
tree and uses a separate private Cargo target. It builds two actual SDK host
binaries, opens an ephemeral loopback Zenoh router with explicit peer links on
individually allocated loopback ports, and checks plan, prepare,
apply, status, delivery provenance, abnormal exit, and bounded owner cleanup.
The explicit host-to-host links form the peer clique; the test does not rely on
a router forwarding between peers in the same region or on loopback gossip.
