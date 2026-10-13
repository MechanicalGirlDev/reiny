# owned-child

The app owns a host and its driver. The driver implements a retained state
publication and an RPC server under the host's public endpoint namespace.
Its separate readiness identity and compiled report are verified before
the host becomes ready. The driver is an explicit immutable companion.

From this independent workspace:

```sh
cargo build --bins
reiny apply main.yaml --detach
reiny stop main.yaml
```

The root composition wires a finite probe task to the host state and RPC outputs.
The probe must print `OWNED_CHILD_QA_OK` after receiving state 42
from `owned-child-example/host` and an RPC result of 43 from that same source.
No process uses an undeclared communication endpoint. The host waits for
child reaping during shutdown; stop acknowledgement alone is not completion.
