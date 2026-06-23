# reiny-cli

The `reiny` command for
[reiny](https://github.com/MechanicalGirlDev/reiny).

```sh
cargo install reiny-cli
```

Subcommands:

- `reiny new` / `reiny init` — scaffold a grain project (a standalone cargo project)
- `reiny add` — append a dependency type to the target's `Reiny.toml` `[dependencies]`
- `reiny build` — run `cargo build` in the cwd (code generation is handled by the grain's `build.rs`)
- `reiny run` — read a launch config and spawn the grain processes
- `reiny compress` — bundle the reachable grain binaries, `.so`s, launch config, and launcher into one self-contained directory

`reiny <launch>.toml` (a bare positional argument) is shorthand for `reiny run`.

License: MIT
