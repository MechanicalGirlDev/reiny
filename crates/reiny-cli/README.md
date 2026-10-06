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
- `reiny compress` - build and bundle a self-contained deployment

Modules and schema:

- `reiny new` / `reiny init` - scaffold a leaf module with `main.yaml` and a Cargo build declaration
- `reiny add` - add a local schema dependency to `main.yaml`
- `reiny check` - validate composition or describe the schema without compiling
- `reiny build` - prepare declared build artifacts without starting processes

Bus tools: `bag`, `topic`, `node`, `service`, `bridge`.

The `main.yaml` format is documented in `docs/modules.md` in the repository.

License: MIT
