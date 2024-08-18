use std::{io::Write, path::Path};

extern crate tonic_build;

fn prepend_package_name_and_build(
    path: impl AsRef<Path>,
    package_name: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let mut source_file = std::fs::File::open(path)?;
    let mut temp_file = tempfile::NamedTempFile::new()?;

    std::io::copy(&mut source_file, &mut temp_file)?;
    writeln!(temp_file)?;
    writeln!(temp_file, "package {package_name};")?;

    let target_path = temp_file.into_temp_path();
    tonic_build::compile_protos(target_path)?;
    Ok(())
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
    prepend_package_name_and_build(
        "hatchet/api-contracts/dispatcher/dispatcher.proto",
        "dispatcher",
    )?;
    prepend_package_name_and_build("hatchet/api-contracts/events/events.proto", "events")?;
    prepend_package_name_and_build(
        "hatchet/api-contracts/workflows/workflows.proto",
        "workflows",
    )?;
    Ok(())
}
