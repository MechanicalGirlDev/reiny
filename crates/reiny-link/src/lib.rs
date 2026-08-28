//! reiny を **バイトを運べるものなら何でも**の上に載せる —— 点対点リンク。
//!
//! 芯は [`Link`]: `#![no_std]` の **sans-I/O** 状態機械。I/O も時計も持たず、受け取った
//! バイトを [`Link::feed`] で食い、送るべきバイトを [`Link::drain`] / [`Link::drain_frame`]
//! で吐き、[`Link::tick`] で時を進める。UART / USB CDC / RS-485 / UDP / TCP / pty /
//! `tokio::io::duplex` —— どれに繋ぐかは呼び出し側の数行で決まり、MCU(embassy / RTIC /
//! 割り込み)でもホストでも同じ `Link` を使う。
//!
//! 型は reiny 本体と同じ [`Topic`] / [`Service`](`reiny-core`)。ワイヤには
//! `Topic::TYPE` の 32 bit ハッシュだけが載り([`wire::type_hash`])、型名と指紋は接続時の
//! Hello で 1 回交換する。
//!
//! ```ignore
//! // MCU 側(#![no_std]、alloc あり)
//! let mut link = Link::<_, 8>::new("motor-board", [0u8; 512], [0u8; 512])?;
//! link.publishes::<MotorState>()?;
//! link.subscribes::<MotorCommand>()?;
//! link.serves::<Calibrate>()?;
//!
//! loop {
//!     link.feed(&uart_rx_bytes());                         // 届いた分をそのまま
//!     while let Some(ev) = link.next() {
//!         match ev {
//!             Event::Data(f) => if let Some(cmd) = link.decode::<MotorCommand>(&f) { apply(cmd) },
//!             Event::Request(f) => if let Some(req) = link.decode::<Calibrate>(&f) {
//!                 link.reply::<Calibrate>(f.seq, &calibrate(req))?;
//!             },
//!             Event::Connected(_) => { /* latched な型はここで送り直す */ }
//!             _ => {}
//!         }
//!     }
//!     link.tick(millis());
//!     link.send(&MotorState { .. })?;
//!     while let n @ 1.. = link.drain(&mut out) { uart_tx(&out[..n]); }
//! }
//! ```
//!
//! feature `std`(既定)でホスト側が付く: 「バイトを送れる者」の trait [`transport::Transport`]、
//! `AsyncRead + AsyncWrite` を包む [`transport::Stream`]、[`transport::Udp`]、シリアルの
//! [`transport::serial::open`](feature `serial`)、そしてそれらの上で `Link` を回す [`Host`]。
//!
//! ```ignore
//! // ホスト側
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
//! 設計と wire 形式の理由は `docs/design/0.5.0.md` §3。

#![cfg_attr(not(feature = "std"), no_std)]

mod link;
pub mod wire;

pub use link::{Error, Event, Frame, ID_MAX, Link, LinkConfig, RemoteType, Stats};
pub use reiny_core::{Descriptor, Service, Topic};

#[cfg(feature = "std")]
mod host;
#[cfg(feature = "std")]
pub mod transport;

#[cfg(feature = "std")]
pub use host::{CallError, Host, HostEvent, HostLink, PeerType};
