//! リーフのスキーマクレート。`Point` はここでだけ生成される。
//!
//! `msg` 側の proto も `geometry.proto` を import するが、reiny-build がこのクレートの
//! FQN 一覧を prost の `extern_path` に流し込むので、あちらでは生成されず
//! `::pingpong_geometry::__pb::pingpong::Point` への参照になる。

reiny::schema!();
