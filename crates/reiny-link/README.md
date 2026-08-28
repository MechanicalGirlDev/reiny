# reiny-link

[reiny](https://github.com/MechanicalGirlDev/reiny) over **anything that moves
bytes**: a UART, USB CDC, RS-485, a UDP socket, a TCP stream, a pty, an
in-memory pipe.

The core is a `#![no_std]` **sans-I/O** state machine, `Link`. It never touches
I/O: you feed it the bytes you received (`feed`) and copy out the bytes it wants
sent (`drain` / `drain_frame`). Wire it to whatever your board or OS gives you
in four lines, then publish / subscribe / call by **Rust type** — the same
`Topic` / `Service` traits as the rest of reiny, with the type name carried on
the wire as a 32-bit hash.

```rust,ignore
let mut link = Link::<_, 8>::new("motor-board", [0u8; 512], [0u8; 512])?;
link.publishes::<MotorState>()?;
link.subscribes::<MotorCommand>()?;

link.feed(&bytes_from_uart);                                   // Rx, any chunking
while let Some(n) = link.drain(&mut out) { uart.write(&out[..n]); } // Tx
link.send(&MotorState { .. })?;
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

License: MIT
