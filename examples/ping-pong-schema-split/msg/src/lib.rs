//! Ping / Pong のスキーマクレート。`Point` は再生成せず geometry のものを使う。

reiny::schema!();

/// `Point` を素通しで再エクスポートしておく(利用側が両方のクレートを名指ししなくて済む)。
pub use pingpong_geometry::internals::Point;
