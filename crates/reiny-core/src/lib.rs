//! reiny の型語彙 —— `#![no_std]`。
//!
//! reiny の組織原理は **type = topic**: launch は型を渡すだけで publish / subscribe する。
//! その「型」を名乗るための trait([`Topic`] / [`Service`])と descriptor([`Descriptor`])、
//! それに `QoS` の語彙([`Qos`])だけをここに置く。`reiny` 本体(tokio / zenoh)は std 前提なので、zenoh が走らない場所
//! (MCU 上の `reiny-link`)でも型 = トピックを共有できるよう、語彙を下に切り出した。
//!
//! 通常は `reiny` が再エクスポートするものを使う。このクレートを直接引くのは MCU 側だけで、
//! そのときは `Cargo.toml` で `reiny = { package = "reiny-core" }` と別名にしておくと、
//! `reiny-build` が生成する `impl ::reiny::Topic for …` がそのまま解決する。
//!
//! `prost` を `no_std` で引くので **alloc は要る**(`Vec` / `String` フィールド)。
// ponytail: alloc 必須。alloc-free は micropb + heapless に乗り換える段で trait を切る。

#![no_std]

mod qos;

pub use qos::{Durability, History, Priority, Qos, Reliability};

use prost::Message;

/// 型 → トピックの対応。`reiny-build` が `Reiny.toml` を読んで各メッセージ型に impl する。
///
/// トピックは **型でアドレスする**。型 `Ping` は `reiny/<domain>/<id>/Ping` へ publish され、
/// `reiny/<domain>/*/Ping`(同じ domain の全 publisher の同じ型)で subscribe される。
/// `<id>` は実行時のインスタンス id、`<domain>` は論理名前空間、`TYPE` がキーの型セグメント
/// (例 `Ping`)。発行側・購読側のどちらの crate でも同じ型は同じ `TYPE` になる。
///
/// zenoh 以外のエンジンはこのキー形をそのまま描くとは限らない —— `reiny-link` は `TYPE` の
/// 32 bit ハッシュをワイヤに載せ、`<domain>` を持たない。共通なのは「`TYPE` が住所」という
/// 1 点だけ。
pub trait Topic {
    /// トピックキーの型セグメント(例 `Ping`)。publish は `reiny/<domain>/<id>/<TYPE>`、
    /// subscribe は `reiny/<domain>/*/<TYPE>`。
    const TYPE: &'static str;

    /// スキーマ指紋。`reiny-build` 生成型は proto の descriptor から算出した値が入る。
    ///
    /// **なぜ要るか。** `TYPE` は proto パッケージを剥がした素の型名なので、トピックの
    /// 名前空間は平坦なままで、別プロジェクトの同名型が同じトピックに乗りうる。protobuf は
    /// 寛容なので、フィールド番号がたまたま噛み合うと **decode が成功して黙って化ける**。
    /// 指紋はその平坦さに対する唯一の安全弁で、publish 時に zenoh の attachment へ載り、
    /// subscribe 時に照合される(両側が `Some` で不一致なら、送信元ごとに 1 度だけ警告して
    /// そのサンプルを捨てる。片方でも `None` なら素通し)。
    ///
    /// 既定値付きなので、**手書きの `impl Topic` は無改造で通る** —— 「第三者は自分の型に
    /// `impl Topic` を書くだけで参加できる」という reiny の売りを壊さないため。
    const SCHEMA: Option<u64> = None;

    /// proto の descriptor。`reiny-build` 生成型は自クレートの descriptor set を指す。
    ///
    /// `Some` の型を publish すると、publisher は自分のキーの脇
    /// `reiny/<domain>/<id>/<TYPE>/@schema/<message>` に queryable を 1 本立て、問い合わせに
    /// この descriptor set を返す。走っている launch が自分の型を**バス上で名乗る**ための口で、
    /// `reiny bag record` はこれを拾って MCAP にスキーマを同梱し、Foxglove がそのまま decode
    /// できる bag を作る(`docs/design/bag.md` §5)。
    ///
    /// `@schema` は zenoh の verbatim チャンクなので、`reiny/<domain>/**` の購読者にも
    /// `reiny/<domain>/*/*` の記録にも見えない —— 型のトピックは汚れない。
    const DESCRIPTOR: Option<Descriptor> = None;
}

/// [`Topic::DESCRIPTOR`] の中身 —— proto の完全メッセージ名と、それを含む
/// `google.protobuf.FileDescriptorSet` の encode 済みバイト列。
#[derive(Debug, Clone, Copy)]
pub struct Descriptor {
    /// 完全メッセージ名(例 `hs.RobotState`)。MCAP のスキーマ名にそのまま使われる。
    pub message: &'static str,
    /// `FileDescriptorSet` の encode 済みバイト列。`message` のファイルと、その推移 import を含む。
    pub file_set: &'static [u8],
}

/// request 型 → response 型。request 型の [`Topic::TYPE`] がそのままキーの型セグメントになる。
///
/// `reiny-build` は `Reiny.toml` の `[services]` から impl を生成する。手書きも 1 行:
///
/// ```ignore
/// impl reiny::Service for Add { type Response = Sum; }
/// ```
///
/// `Response: Topic` を要求するのは、応答にも `SCHEMA` 指紋を載せるため。
pub trait Service: Topic + Message + Default {
    /// 応答の型。
    type Response: Topic + Message + Default;
}
