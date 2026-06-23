# reiny-macros

The `#[reiny::main]` proc-macro implementation for
[reiny](https://github.com/MechanicalGirlDev/reiny).

You normally do not depend on this crate directly — use `#[reiny::main]` via the
[`reiny`](https://crates.io/crates/reiny) crate. The macro wraps an async `main`
in the `reiny` runtime, constructing and passing a `Cloudy`.

License: MIT
