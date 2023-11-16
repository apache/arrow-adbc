use bindgen::Builder;
use std::{env, error::Error, path::PathBuf, process::Command};

fn main() -> Result<(), Box<dyn Error>> {
    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let go_dir = manifest_dir.ancestors().nth(3).unwrap().join("go");
    let go_pkg = go_dir.join("adbc/pkg/snowflake");

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let archive = out_dir.join("libsnowflake.a");
    let header = out_dir.join("libsnowflake.h");
    let bindings = out_dir.join("snowflake.rs");

    // Build the Go driver
    let status = Command::new("go")
        .current_dir(go_pkg.as_path())
        .arg("build")
        .arg("-tags")
        .arg("driverlib")
        .arg("-buildmode=c-archive")
        .arg("-o")
        .arg(&archive)
        .arg(".")
        .status()?;
    assert!(status.success(), "Go build failed");

    // Generate the bindings based on the generated header
    Builder::default()
        .header(header.to_str().unwrap())
        .clang_arg(format!("-I{}", go_pkg.display()))
        .derive_default(true)
        .allowlist_item("Snowflake.*")
        .generate()?
        .write_to_file(bindings)?;

    // Retrigger the build when the Go pkg changes
    println!("cargo:rerun-if-changed={}", go_pkg.display());

    // Link the driver statically
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=snowflake");

    // Link other dependencies
    println!("cargo:rustc-link-lib=resolv");
    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
        println!("cargo:rustc-link-lib=framework=Security");
    }

    Ok(())
}
