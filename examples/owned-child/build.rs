fn main() -> Result<(), Box<dyn std::error::Error>> {
    reiny_build::compile()?;
    Ok(())
}
