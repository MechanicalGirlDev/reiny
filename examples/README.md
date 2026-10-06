# reiny examples

Every module uses one `main.yaml` for runtime composition and optional build-time schema.
Rust examples are independent Cargo workspaces with their own locks.

| Example | Focus |
| --- | --- |
| [ping-pong-ring](ping-pong-ring) | Per-project schemas and a communication cycle |
| [ping-pong-workspace](ping-pong-workspace) | Shared catalog inherited by runtime leaves |
| [ping-pong-schema](ping-pong-schema) | One shared schema crate |
| [ping-pong-schema-split](ping-pong-schema-split) | Split crates and imported message ownership |
| [ping-pong-fanout](ping-pong-fanout) | One Ping output wired to three Pong instances |
| [ping-pong-config](ping-pong-config) | Typed defaults and runtime overrides |
| [ping-pong-relay](ping-pong-relay) | Source, transform, and sink |
| [ping-pong-service](ping-pong-service) | Typed SDK request/response services |
| [ping-pong-cli](ping-pong-cli) | Scaffolding, dependencies, building, and packaging |

## Contracts

The root `modules` mapping names instances and imports relative directories. Caller
contracts match child `in` and `out` types. An input selects one exact output with
`from: sibling.port`; composition outputs forward child outputs with `from`.
Communication cycles do not imply startup dependencies.

Applications use `cloudy.input::<T>("port")` and `cloudy.output::<T>("port")`, then
`cloudy.ready()?`. Types identify payload schemas; named ports identify wiring.
The `schema` block is build-time only: codegen searches upward for the nearest
`main.yaml` containing it, skipping runtime-only leaves and ignoring providers.

Build and run from the example directory with `cargo build --locked` and
`reiny run main.yaml`. Start with the ring example, then compare the shared catalogs.
