fn main() {
    reiny_build::compile().expect("fixture schema");
    reiny_build::compile_ports("app.yaml").expect("fixture static ports");
}
