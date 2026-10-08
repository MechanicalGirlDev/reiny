# owned-child

The app owns a host and its driver. The driver implements a retained state
publication and an RPC server under the host's public endpoint namespace.
Its separate readiness identity and compiled report are verified before
the host becomes ready. The driver is an explicit immutable companion.

From this independent workspace:

```sh
cargo build --bins
reiny apply main.yaml --detach
cargo run --bin probe -- --domain owned-child-example
reiny stop main.yaml
```

The standalone probe must print `OWNED_CHILD_QA_OK` after receiving state 42
from `owned-child-example/host` and an RPC result of 43 from that same source.
No parent/child process uses an undeclared standalone bus. The host waits for
child reaping during shutdown; stop acknowledgement alone is not completion.
