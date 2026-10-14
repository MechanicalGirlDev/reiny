# reiny-cli

The `reiny` command for
[reiny](https://github.com/MechanicalGirlDev/reiny).

```sh
cargo install reiny-cli
```

Deployments (every command takes a root directory or its `main.yaml`, default `.`):

- `reiny plan` - resolve module sources and show modules and connections, without building
- `reiny apply` - prepare artifacts, start or reconcile managed processes and wait for readiness (`--detach` returns after readiness)
- `reiny run` - the same contract as `apply`, in the foreground
- `reiny status` / `reiny stop` - read live state, or stop cooperatively and reap owned processes
- `reiny update` - move Git module refs and rewrite `lock.yaml`
- `reiny bundle` - build and bundle a self-contained deployment

Modules and schema:

- `reiny new` / `reiny init` - scaffold a leaf module with `main.yaml` and a Cargo build declaration
- `reiny add` - add a local schema dependency to `main.yaml`
- `reiny check` - validate composition or describe the schema without compiling
- `reiny build` - prepare declared build artifacts without starting processes

Bus tools: `bag`, `topic`, `node`, `service`, `bridge`.

Data commands require prepared version-3 `--module-bindings`. Input sources
are exact `{namespace, port}` addresses. Discovery displays named endpoint
ports separately from complete protobuf type names.

Replay maps every selected MCAP endpoint channel explicitly:

```sh
reiny bag play recording.mcap --module-bindings replay.json \
  --map 'reiny/v3/lab/controller/@out/state/robot.State=state'
```

Repeat `--map recorded-key=output` for other channels. A type match alone never
selects a replay output. Missing, repeated, ambiguous, or incompatible mappings
are errors. Bridges likewise pair identical declared input/output port names
and validate request and response fingerprints.

The `main.yaml` format is documented in `docs/modules.md` in the repository.

License: MIT
