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
//!    同期 `fn main` を生成し、tokio ランタイム・Zenoh セッション・Ctrl+C シャットダウンを
//!    [`reiny::__rt::run`] に肩代わりさせる。

use proc_macro::TokenStream;
use quote::quote;
use syn::{ItemFn, parse_macro_input};

/// grain のエントリポイント。`async fn main(cloudy: Cloudy) -> reiny::Result<()>` に付ける。
#[proc_macro_attribute]
pub fn main(_attr: TokenStream, item: TokenStream) -> TokenStream {
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

        // 2. 同期エントリ。ランタイム構築・Zenoh セッション・Ctrl+C は reiny に任せる。
        fn main() -> ::reiny::Result<()> {
            #(#attrs)*
            async fn __reiny_user_main(#inputs) #output #body

            ::reiny::__rt::run(env!("CARGO_PKG_NAME"), __reiny_user_main)
        }
    };

    expanded.into()
}
