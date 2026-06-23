use std::env;
use std::io::Result;
use std::path::PathBuf;

fn main() -> Result<()> {
    // Get the directory containing Cargo.toml for this crate
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR").unwrap());
    // Proto files are in the workspace root's sibling directory
    let proto_dir = manifest_dir
        .parent()
        .unwrap()
        .parent()
        .unwrap()
        .join("proto");
    let proto_dir_str = proto_dir.to_str().unwrap();

    println!("cargo:rerun-if-changed={}", proto_dir_str);

    prost_build::Config::new().compile_protos(
        &[
            proto_dir.join("types/geometry.proto"),
            proto_dir.join("types/time.proto"),
            proto_dir.join("types/tf.proto"),
            proto_dir.join("command.proto"),
            proto_dir.join("state.proto"),
            proto_dir.join("config.proto"),
            proto_dir.join("control.proto"),
            proto_dir.join("sim.proto"),
            proto_dir.join("scene.proto"),
            proto_dir.join("plugin.proto"),
        ],
        &[&proto_dir],
    )?;

    Ok(())
}
