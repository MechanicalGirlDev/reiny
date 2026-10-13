# reiny-link

[reiny](https://github.com/MechanicalGirlDev/reiny) over **anything that moves
bytes**: a UART, USB CDC, RS-485, a UDP socket, a TCP stream, a pty, an
in-memory pipe.

The core is a `#![no_std]` **sans-I/O** state machine, `Link`. It never touches
I/O: you feed it the bytes you received (`feed`) and copy out the bytes it wants
sent (`drain` / `drain_frame`). Wire it to whatever your board or OS gives you
in four lines, then publish / subscribe / call through **named static ports** — the same
`Topic` / `Service` traits as the rest of reiny, with the type name carried on
the wire as a 32-bit hash.

```rust,ignore
// build.rs: reiny_build::compile()?; reiny_build::compile_ports("ports.yaml")?;
include!(concat!(env!("OUT_DIR"), "/reiny_ports.rs"));
let mut link = Link::<_, 8>::new("motor-board", PORTS, [0u8; 512], [0u8; 512])?;
link.publishes::<MotorState>("state")?;
link.subscribes::<MotorCommand>("command")?;

link.feed(&bytes_from_uart);                                   // Rx, any chunking
while let n @ 1.. = link.drain(&mut out) { uart.write(&out[..n]); } // Tx
link.send("state", &MotorState { .. })?;
while let Some(ev) = link.next() {
    if let Event::Data(f) = ev
        && let Some(cmd) = link.decode::<MotorCommand>(&f) { /* … */ }
}
```

With the `std` feature (default) the crate adds the host side: a `Transport`
trait ("send bytes / receive bytes"), `Stream<T>` for anything
`AsyncRead + AsyncWrite` (serial ports via `transport::serial::open`, TCP, pty),
`Udp`, and `Host`, which drives a `Link` over a transport on tokio and gives
you `send` / `call` / `recv`.

Wire format and the reasoning: `docs/design/0.5.0.md` §3 in the repository.

Current typed traffic requires schema fingerprints. Hello entries without a
fingerprint are rejected. Reply payloads begin with the response fingerprint
(8 bytes, little-endian), followed by the prost response; requests retain their
request-type hash. Missing or mismatched response metadata is rejected before
decoding. `compile_ports` reads an explicit YAML contract: `in` and `out` map
port names to message types, and `uses` and `provides` map names to request and
response types. The schema catalog alone grants no ports. Every declaration
and typed send, request, or reply checks the selected name, direction, type,
and fingerprint. Client ports must be declared with `calls` before I/O starts.
Hello also advertises RPC response fingerprints as metadata-only type entries;
these consume type-table capacity but grant no publish, subscribe, or service permission.

Bridge raw operations are internal transport implementation. Transport tools
construct them with `Host::bridge`; a bridge host has no typed application ports.

License: MIT
