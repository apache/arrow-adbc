// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::error::Error;

/// Build and link the Go driver statically.
#[cfg(feature = "bundled")]
fn bundled() -> Result<(), Box<dyn Error>> {
    use std::{env, path::PathBuf, process::Command};

    let manifest_dir = PathBuf::from(env::var("CARGO_MANIFEST_DIR")?);
    let go_dir = manifest_dir.ancestors().nth(3).unwrap().join("go");
    let go_pkg = go_dir.join("adbc/pkg/snowflake");

    let out_dir = PathBuf::from(env::var("OUT_DIR")?);
    let archive = out_dir.join("libsnowflake.a");

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

    // Rebuild when the Go pkg changes.
    println!("cargo:rerun-if-changed={}", go_pkg.display());

    // Link the driver statically.
    println!("cargo:rustc-link-search=native={}", out_dir.display());
    println!("cargo:rustc-link-lib=static=snowflake");

    // Link other dependencies.
    println!("cargo:rustc-link-lib=resolv");
    #[cfg(target_os = "macos")]
    {
        println!("cargo:rustc-link-lib=framework=CoreFoundation");
        println!("cargo:rustc-link-lib=framework=Security");
    }

    Ok(())
}

/// Link the Go driver.
#[cfg(feature = "linked")]
fn linked() -> Result<(), Box<dyn Error>> {
    // Rebuild when config var changes.
    println!("cargo:rerun-if-env-changed=ADBC_SNOWFLAKE_GO_LIB_DIR");

    // Add search path if requested.
    if let Some(path) = option_env!("ADBC_SNOWFLAKE_GO_LIB_DIR") {
        println!("cargo:rustc-link-search={path}");
    }

    // Link the driver.
    println!("cargo:rustc-link-lib=adbc_driver_snowflake");

    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    // Bundle the Go driver.
    #[cfg(feature = "bundled")]
    bundled()?;

    // Link the Go driver.
    #[cfg(feature = "linked")]
    linked()?;

    // Rebuild when tests are enabled.
    println!("cargo:rerun-if-env-changed=ADBC_SNOWFLAKE_TESTS");

    Ok(())
}
