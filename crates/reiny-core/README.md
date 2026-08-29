# reiny-core

The type vocabulary of [reiny](https://github.com/MechanicalGirlDev/reiny):
the `Topic` / `Service` traits, the `Descriptor` struct and the QoS vocabulary
(`Qos` and its enums), in a `#![no_std]` crate (needs `alloc` through `prost`).

You normally get these through the [`reiny`](https://crates.io/crates/reiny)
crate, which re-exports them. Depend on `reiny-core` directly only where
`reiny` cannot go — an MCU firmware talking over
[`reiny-link`](https://crates.io/crates/reiny-link). There, alias it so the
code `reiny-build` generates (`impl ::reiny::Topic for …`) resolves unchanged:

```toml
[dependencies]
reiny = { package = "reiny-core", version = "0.5" }
```

License: MIT
