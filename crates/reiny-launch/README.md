# reiny-launch

The launcher library for
[reiny](https://github.com/MechanicalGirlDev/reiny). It reads the `[grain]`
section of a launch config (e.g. `ping-pong.toml`) and spawns/supervises the
grain processes together (`depends_on`, `on_exit`, and so on).

You normally use it via `reiny run` from
[`reiny-cli`](https://crates.io/crates/reiny-cli); this crate provides the
underlying functionality as a library.

License: MIT
