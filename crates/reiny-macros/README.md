# reiny-macros

The `#[reiny::main]` proc-macro implementation for
[reiny](https://github.com/MechanicalGirlDev/reiny).

You normally do not depend on this crate directly — use `#[reiny::main]` via the
[`reiny`](https://crates.io/crates/reiny) crate. The macro wraps an async `main`
in the `reiny` runtime, constructing and passing a `Cloudy`.

The macro includes types generated from the build-time `schema` block in
`main.yaml`. Runtime composition and provider selection remain runtime concerns.
Create all named `cloudy.input::<T>("port")` and `cloudy.output::<T>("port")`
handles before calling `cloudy.ready()?` in the async entry point.

License: MIT
