# reiny-link

[reiny](https://github.com/MechanicalGirlDev/reiny) over **anything that moves
bytes**: a UART, USB CDC, RS-485, a UDP socket, a TCP stream, a pty, an
in-memory pipe.

The core is a `#![no_std]` **sans-I/O** state machine, `Link`. It never touches
I/O: you feed it the bytes you received (`feed`) and copy out the bytes it wants
sent (`drain` / `drain_frame`). Wire it to whatever your board or OS gives you
in four lines, then publish / subscribe / call through **named static ports** — the same
`MessageType` / `Service` traits as the rest of reiny, with the type name carried on
Hello alongside fingerprints; frames carry the hash of namespace and output port.

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

Every frame uses COBS around `RY`, epoch 2, kind, endpoint hash, sequence,
payload, and CRC. The endpoint hash is FNV-1a32 over two u32 little-endian
length-prefixed UTF-8 strings: namespace and output port. Data identifies the
sending output; requests and replies identify the destination RPC server.

Hello carries fingerprint version 2 and each declared port's name, direction,
request/message FQN and fingerprint, response contract, and exact source pairs.
Missing, malformed, duplicate, colliding, or incompatible contracts are rejected
atomically. A changed Hello disconnects a live contract rather than replacing it.
Reply payloads begin with the response fingerprint (8 bytes, little-endian).

`compile_ports` reads an explicit version-3 YAML contract, separate from the
schema catalog. For a motor-board application:

```yaml
version: 3
out:
  state: { type: motor.MotorState }
in:
  command:
    type: motor.MotorCommand
    from:
      - { namespace: host, port: command }
rpc_servers:
  calibrate:
    type: motor.Calibrate
    response: motor.Calibration
rpc_clients:
  clock:
    type: clock.ReadClock
    response: clock.Clock
    from:
      - { namespace: host, port: clock }
```

Generated `StaticPort.sources` preserves these exact addresses. Inputs require
nonempty source lists; RPC clients require exactly one source. Output/server
ports have none. Every typed declaration and operation checks its explicit name,
direction, type, and fingerprint. RPC response metadata belongs to its named
port and grants no stream permission. Pending calls and retained values are
keyed by endpoint hash, so two ports carrying the same type remain independent.
The bounded pending-call table refuses new calls when full and never replaces
a live call when its sequence wraps.

Bridge raw operations are internal transport implementation. Transport tools
construct them with `Host::bridge`; a bridge host has no typed application ports.

License: MIT
