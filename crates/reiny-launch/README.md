# reiny-launch

The deployment library for
[reiny](https://github.com/MechanicalGirlDev/reiny). It resolves a tree of
`main.yaml` modules (local or revision-pinned Git sources), checks their port
wiring, prepares declared Cargo builds and file artifacts, and supervises the
resulting managed processes.

You normally use it through `reiny plan` / `apply` / `run` from
[`reiny-cli`](https://crates.io/crates/reiny-cli); this crate provides the
underlying functionality as a library and links no communication bus. The
`main.yaml` format is documented in `docs/modules.md` in the repository.

License: MIT
