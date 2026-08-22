//! `#[reiny::main]` の実装。
//!
//! `reiny` umbrella クレートから `pub use reiny_macros::main;` で再エクスポートされ、利用側は
//! `#[reiny::main]` として使う。展開は 2 つの仕事をする:
//!
//! 1. **生成型の取り込み** — `reiny-build`(各 grain の `build.rs`)が `$OUT_DIR/reiny_generated.rs`
//!    に書き出した `publications` / `dependencies` / `internals` モジュールを crate ルートへ
//!    取り込む。これで利用側コードの `use crate::publications::Ping;` 等が解決する。
//!    (外部クレート `reiny::` の名前空間には利用側固有の生成型を後入れできないため、
//!    `reiny::publications` ではなく `crate::publications` になる。)
//! 2. **ランタイム起動** — `async fn main(cloudy: Cloudy) -> reiny::Result<()>` を実行する
//!    同期 `fn main` を生成し、tokio ランタイム・Zenoh セッション・シグナルシャットダウンを
//!    `reiny::run_with` に肩代わりさせる。
//!
//! 唯一のオプションは `#[reiny::main(tracing = false)]` で、reiny に
//! `tracing_subscriber` をグローバル登録させない(自前の subscriber を持つ grain 用)。

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, LitBool, parse_macro_input};

/// grain のエントリポイント。`async fn main(cloudy: Cloudy) -> reiny::Result<()>` に付ける。
///
/// `#[reiny::main(tracing = false)]` で reiny の `tracing_subscriber` 登録を止められる。
/// `tracing_subscriber::try_init` は**後勝ちしない**ので、自前の subscriber を持つ grain が
/// 「reiny より先に入れる」順序依存を抱えずに済む唯一の方法がこれ。
#[proc_macro_attribute]
pub fn main(attr: TokenStream, item: TokenStream) -> TokenStream {
    let mut install_tracing = true;
    let attr_parser = syn::meta::parser(|meta| {
        if meta.path.is_ident("tracing") {
            install_tracing = meta.value()?.parse::<LitBool>()?.value();
            Ok(())
        } else {
            Err(meta.error("unknown #[reiny::main] option (only `tracing = false` is supported)"))
        }
    });
    parse_macro_input!(attr with attr_parser);

    let user_fn = parse_macro_input!(item as ItemFn);

    // async であることを要求する(reiny の main は async)。
    if user_fn.sig.asyncness.is_none() {
        return syn::Error::new_spanned(
            user_fn.sig.fn_token,
            "#[reiny::main] requires an `async fn`",
        )
        .to_compile_error()
        .into();
    }

    // 利用側 fn を別名へ退避(name は問わない。慣例では `main`)。シグネチャ・本体はそのまま使う。
    let attrs = &user_fn.attrs;
    let inputs = &user_fn.sig.inputs;
    let output = &user_fn.sig.output;
    let body = &user_fn.block;

    let expanded = quote! {
        // 1. reiny-build が生成した型を crate ルートへ。`crate::publications::*` 等で参照される。
        #[doc(hidden)]
        mod __reiny_generated {
            include!(concat!(env!("OUT_DIR"), "/reiny_generated.rs"));
        }
        #[allow(unused_imports)]
        pub use __reiny_generated::*;

        // 2. 同期エントリ。ランタイム構築・Zenoh セッション・シグナルは reiny に任せる。
        fn main() -> ::reiny::Result<()> {
            #(#attrs)*
            async fn __reiny_user_main(#inputs) #output #body

            let mut __opts = ::reiny::RuntimeOptions::from_args(env!("CARGO_PKG_NAME"));
            __opts.install_tracing = #install_tracing;
            ::reiny::run_with(__opts, __reiny_user_main)
        }
    };

    expanded.into()
}
