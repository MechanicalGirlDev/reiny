//! 消費 grain の codegen(ping/build.rs と同じ)。パッケージ名 `pong` は `[projects.pong]`
//! 側なので消費モードで動き、proto を再コンパイルせず pingpong-schema を再エクスポートする。
fn main() {
    reiny_build::compile().expect("reiny consumer codegen from workspace Reiny.toml");
}
