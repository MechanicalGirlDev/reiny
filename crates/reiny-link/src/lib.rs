//! reiny on top of **anything that can carry bytes** — a point-to-point link.
//!
//! The core is [`Link`]: a `#![no_std]` **sans-I/O** state machine. It owns neither I/O nor a clock;
//! it eats the bytes you received through [`Link::feed`], emits the bytes to send through
//! [`Link::drain`] / [`Link::drain_frame`], and advances time through [`Link::tick`]. UART / USB CDC
//! / RS-485 / UDP / TCP / pty / `tokio::io::duplex` — which one it sits on is a few lines on the
//! caller's side, and the same `Link` runs on an MCU (embassy / RTIC / interrupts) and on a host.
//!
//! The types are the same [`Topic`] / [`Service`] reiny proper uses (`reiny-core`). Only a 32-bit
//! hash of `Topic::TYPE` rides on the wire ([`wire::type_hash`]); type names and fingerprints are
//! exchanged once, in the Hello, at connection time.
//!
//! ```ignore
//! // On the MCU (#![no_std], with alloc)
//! let mut link = Link::<_, 8>::new("motor-board", [0u8; 512], [0u8; 512])?;
//! link.publishes::<MotorState>()?;
//! link.subscribes::<MotorCommand>()?;
//! link.serves::<Calibrate>()?;
//!
//! loop {
//!     link.feed(&uart_rx_bytes());                         // whatever arrived, as it arrived
//!     while let Some(ev) = link.next() {
//!         match ev {
//!             Event::Data(f) => if let Some(cmd) = link.decode::<MotorCommand>(&f) { apply(cmd) },
//!             Event::Request(f) => if let Some(req) = link.decode::<Calibrate>(&f) {
//!                 link.reply::<Calibrate>(f.seq, &calibrate(req))?;
//!             },
//!             Event::Connected(_) => { /* re-send latched types here */ }
//!             _ => {}
//!         }
//!     }
//!     link.tick(millis());
//!     link.send(&MotorState { .. })?;
//!     while let n @ 1.. = link.drain(&mut out) { uart_tx(&out[..n]); }
//! }
//! ```
//!
//! Feature `std` (on by default) adds the host side: the "can send bytes" trait
//! [`transport::Transport`], [`transport::Stream`] wrapping anything `AsyncRead + AsyncWrite`,
//! [`transport::Udp`], [`transport::serial::open`] for serial ports (feature `serial`), and [`Host`],
//! which drives a `Link` over any of them.
//!
//! ```ignore
//! // On the host
//! let mut link = HostLink::host("pc")?;
//! link.subscribes::<MotorState>()?;
//! link.publishes::<MotorCommand>()?;
//! let mut host = Host::spawn(link, transport::serial::open("/dev/ttyACM0", 921_600)?);
//! // let mut host = Host::spawn(link, transport::Udp::bind("0.0.0.0:7000").await?.with_peer(mcu));
//! while let Some(ev) = host.recv().await {
//!     if let Some(state) = ev.decode::<MotorState>() { … }
//! }
//! ```
//!
//! Feature `engine` (on by default) adds [`LinkEngine`], which turns a `Host` into a reiny `Engine`
//! and so puts a `Cloudy` on top of a link (the innards of `reiny bridge serial …`).
//!
//! The design and the reasoning behind the wire format are in `docs/design/0.5.0.md` §3.

#![cfg_attr(not(feature = "std"), no_std)]

mod link;
pub mod wire;

pub use link::{Error, Event, Frame, ID_MAX, Link, LinkConfig, RemoteType, Stats};
pub use reiny_core::{Descriptor, Service, Topic};

#[cfg(feature = "engine")]
mod engine;
#[cfg(feature = "std")]
mod host;
#[cfg(feature = "std")]
pub mod transport;

#[cfg(feature = "engine")]
pub use engine::LinkEngine;
#[cfg(feature = "std")]
pub use host::{CallError, Host, HostEvent, HostLink, PeerType};
