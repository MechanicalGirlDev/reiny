//! Compile the nearest ancestor main.yaml build-time schema.
fn main() {
    reiny_build::compile().expect("reiny codegen from main.yaml schema");
}
