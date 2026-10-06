# ping-pong-schema-split

The shared schema is split into `pingpong-geometry` (Point) and `pingpong-msg`
(Ping/Pong, which refer to Point). The root `main.yaml` declares parts under
`schema.schema.geometry` and `schema.schema.msg`. Type ownership follows proto paths.

Geometry exports include paths and owned descriptor names through Cargo `links`
metadata. Cargo passes them to directly dependent build scripts as `DEP_*`;
reiny-build converts them to prost `extern_path` mappings. Point is therefore
referenced from geometry rather than generated again in msg. The application
assigns a geometry Point directly to `Ping.at`, proving shared type ownership
at compile time.

Each schema crate needs `links = "<package-name>"`. Schema `depends` lists must
be transitively closed and have matching direct Cargo dependencies, because
Cargo forwards build metadata only across direct dependency edges.

## Run

From this independent Cargo workspace:

```sh
cargo build --locked
reiny run main.yaml
```

The root process provider resolves binaries in `target/debug`. Each runtime leaf has its own
`main.yaml`, explicit `in`/`out` contracts, and a Cargo build declaration. Runtime module
paths determine identities under the deployment namespace. Ports are created synchronously;
`cloudy.ready()?` is called only after every named port has been created.
