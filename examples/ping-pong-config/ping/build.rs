//! Compile the nearest ancestor schema.yaml build-time catalog.
fn main() {
    reiny_build::compile().expect("reiny codegen from schema.yaml");
}
