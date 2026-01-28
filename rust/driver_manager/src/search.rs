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

use std::env;
use std::ffi::OsStr;
use std::fs;
use std::path::{Path, PathBuf};

use toml::de::DeTable;

use adbc_core::{
    error::{Error, Result, Status},
    LoadFlags, LOAD_FLAG_SEARCH_ENV, LOAD_FLAG_SEARCH_SYSTEM, LOAD_FLAG_SEARCH_USER,
};

#[derive(Debug, Default)]
pub(crate) struct DriverInfo {
    pub(crate) lib_path: PathBuf,
    pub(crate) entrypoint: Option<Vec<u8>>,
    // TODO: until we add more logging these are unused so we'll leave
    // them out until such time that we do so.
    // driver_name: String,
    // version: String,
    // source: String,
}

impl DriverInfo {
    pub fn load_driver_manifest(manifest_file: &Path, entrypoint: Option<&[u8]>) -> Result<Self> {
        let contents = fs::read_to_string(manifest_file).map_err(|e| {
            Error::with_message_and_status(
                format!("Could not read manifest '{}': {e}", manifest_file.display()),
                Status::InvalidArguments,
            )
        })?;

        let manifest = DeTable::parse(&contents)
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::InvalidArguments))?;

        let manifest_version = manifest
            .get_ref()
            .get("manifest_version")
            .and_then(|v| v.get_ref().as_integer())
            .map(|v| v.as_str())
            .unwrap_or("1");

        if manifest_version != "1" {
            return Err(Error::with_message_and_status(
                format!("Unsupported manifest version: {manifest_version}"),
                Status::InvalidArguments,
            ));
        }

        // leave these out until we add logging that would actually utilize them
        // let driver_name = get_optional_key(&manifest, "name");
        // let version = get_optional_key(&manifest, "version");
        // let source = get_optional_key(&manifest, "source");
        let (os, arch, extra) = arch_triplet();

        let lib_path = match manifest
            .get_ref()
            .get("Driver")
            .and_then(|v| v.get_ref().get("shared"))
            .map(|v| v.get_ref())
        {
            Some(driver) => {
                if driver.is_str() {
                    // If the value is a string, we assume it is the path to the shared library.
                    Ok(PathBuf::from(driver.as_str().unwrap_or_default()))
                } else if driver.is_table() {
                    // If the value is a table, we look for the specific key for this OS and architecture.
                    Ok(PathBuf::from(
                        driver
                            .get(format!("{os}_{arch}{extra}"))
                            .and_then(|v| v.get_ref().as_str())
                            .unwrap_or_default(),
                    ))
                } else {
                    Err(Error::with_message_and_status(
                        format!(
                            "Driver.shared must be a string or a table, found '{}'",
                            driver.type_str()
                        ),
                        Status::InvalidArguments,
                    ))
                }
            }
            None => Err(Error::with_message_and_status(
                format!(
                    "Manifest '{}' missing Driver.shared key",
                    manifest_file.display()
                ),
                Status::InvalidArguments,
            )),
        }?;

        if lib_path.as_os_str().is_empty() {
            return Err(Error::with_message_and_status(
                format!(
                    "Manifest '{}' found empty string for library path of Driver.shared.{os}_{arch}{extra}",
                    lib_path.display()
                ),
                Status::InvalidArguments,
            ));
        }

        let entrypoint_val = manifest
            .get_ref()
            .get("Driver")
            .and_then(|v| v.get_ref().as_table())
            .and_then(|t| t.get("entrypoint"))
            .map(|v| v.get_ref());

        if let Some(entry) = entrypoint_val {
            if !entry.is_str() {
                return Err(Error::with_message_and_status(
                    "Driver entrypoint must be a string".to_string(),
                    Status::InvalidArguments,
                ));
            }
        }

        let entrypoint = match entrypoint_val.and_then(|v| v.as_str()) {
            Some(s) => Some(s.as_bytes().to_vec()),
            None => entrypoint.map(|s| s.to_vec()),
        };

        Ok(DriverInfo {
            lib_path,
            entrypoint,
        })
    }
}

// construct default entrypoint from the library name
pub(crate) fn get_default_entrypoint(driver_path: impl AsRef<OsStr>) -> String {
    // - libadbc_driver_sqlite.so.2.0.0 -> AdbcDriverSqliteInit
    // - adbc_driver_sqlite.dll -> AdbcDriverSqliteInit
    // - proprietary_driver.dll -> AdbcProprietaryDriverInit

    // potential path -> filename
    let mut filename = driver_path.as_ref().to_str().unwrap_or_default();
    if let Some(pos) = filename.rfind(['/', '\\']) {
        filename = &filename[pos + 1..];
    }

    // remove all extensions
    filename = filename
        .find('.')
        .map_or_else(|| filename, |pos| &filename[..pos]);

    let mut entrypoint = filename
        .to_string()
        .strip_prefix(env::consts::DLL_PREFIX)
        .unwrap_or(filename)
        .split(&['-', '_'][..])
        .map(|s| {
            // uppercase first character of a string
            // https://stackoverflow.com/questions/38406793/why-is-capitalizing-the-first-letter-of-a-string-so-convoluted-in-rust
            let mut c = s.chars();
            match c.next() {
                None => String::new(),
                Some(f) => f.to_uppercase().collect::<String>() + c.as_str(),
            }
        })
        .collect::<Vec<_>>()
        .join("");

    if !entrypoint.starts_with("Adbc") {
        entrypoint = format!("Adbc{entrypoint}");
    }

    entrypoint.push_str("Init");
    entrypoint
}

pub(crate) const fn arch_triplet() -> (&'static str, &'static str, &'static str) {
    #[cfg(target_arch = "x86_64")]
    const ARCH: &str = "amd64";
    #[cfg(all(target_arch = "aarch64", target_endian = "big"))]
    const ARCH: &str = "arm64be";
    #[cfg(all(target_arch = "aarch64", target_endian = "little"))]
    const ARCH: &str = "arm64";
    #[cfg(all(target_arch = "powerpc64", target_endian = "little"))]
    const ARCH: &str = "powerpc64le";
    #[cfg(all(target_arch = "powerpc64", target_endian = "big"))]
    const ARCH: &str = "powerpc64";
    #[cfg(target_arch = "riscv32")]
    const ARCH: &str = "riscv";
    #[cfg(not(any(
        target_arch = "x86_64",
        target_arch = "aarch64",
        target_arch = "powerpc64",
        target_arch = "riscv32",
    )))]
    const ARCH: &str = std::env::consts::ARCH;

    const OS: &str = std::env::consts::OS;

    #[cfg(target_env = "musl")]
    const EXTRA: &str = "_musl";
    #[cfg(all(target_os = "windows", target_env = "gnu"))]
    const EXTRA: &str = "_mingw";
    #[cfg(not(any(target_env = "musl", all(target_os = "windows", target_env = "gnu"))))]
    const EXTRA: &str = "";

    (OS, ARCH, EXTRA)
}

#[cfg(target_os = "windows")]
pub(crate) mod target_windows {
    use windows_sys as windows;

    use std::ffi::c_void;
    use std::ffi::OsString;
    use std::os::windows::ffi::OsStringExt;
    use std::path::PathBuf;
    use std::slice;

    use windows::Win32::UI::Shell;

    // adapted from https://github.com/dirs-dev/dirs-sys-rs/blob/main/src/lib.rs#L150
    pub fn user_config_dir() -> Option<PathBuf> {
        unsafe {
            let mut path_ptr: windows::core::PWSTR = std::ptr::null_mut();
            let result = Shell::SHGetKnownFolderPath(
                &Shell::FOLDERID_LocalAppData,
                0,
                std::ptr::null_mut(),
                &mut path_ptr,
            );

            if result == 0 {
                let len = windows::Win32::Globalization::lstrlenW(path_ptr) as usize;
                let path = slice::from_raw_parts(path_ptr, len);
                let ostr: OsString = OsStringExt::from_wide(path);
                windows::Win32::System::Com::CoTaskMemFree(path_ptr as *const c_void);
                Some(PathBuf::from(ostr))
            } else {
                windows::Win32::System::Com::CoTaskMemFree(path_ptr as *const c_void);
                None
            }
        }
    }
}

pub(crate) fn user_config_dir() -> Option<PathBuf> {
    #[cfg(target_os = "windows")]
    {
        use target_windows::user_config_dir;
        user_config_dir().map(|mut path| {
            path.push("ADBC");
            path.push("Drivers");
            path
        })
    }

    #[cfg(target_os = "macos")]
    {
        env::var_os("HOME").map(PathBuf::from).map(|mut path| {
            path.push("Library");
            path.push("Application Support");
            path.push("ADBC");
            path.push("Drivers");
            path
        })
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        env::var_os("XDG_CONFIG_HOME")
            .map(PathBuf::from)
            .or_else(|| {
                env::var_os("HOME").map(|home| {
                    let mut path = PathBuf::from(home);
                    path.push(".config");
                    path
                })
            })
            .map(|mut path| {
                path.push("adbc");
                path.push("drivers");
                path
            })
    }
}

pub(crate) fn system_config_dir() -> Option<PathBuf> {
    #[cfg(target_os = "macos")]
    {
        Some(PathBuf::from("/Library/Application Support/ADBC/Drivers"))
    }

    #[cfg(all(unix, not(target_os = "macos")))]
    {
        Some(PathBuf::from("/etc/adbc/drivers"))
    }

    #[cfg(not(unix))]
    {
        None
    }
}

pub(crate) fn get_search_paths(lvls: LoadFlags) -> Vec<PathBuf> {
    let mut result = Vec::new();
    if lvls & LOAD_FLAG_SEARCH_ENV != 0 {
        if let Some(paths) = env::var_os("ADBC_DRIVER_PATH") {
            for p in env::split_paths(&paths) {
                result.push(p);
            }
        }
    }

    if lvls & LOAD_FLAG_SEARCH_USER != 0 {
        if let Some(path) = user_config_dir() {
            if path.exists() {
                result.push(path);
            }
        }
    }

    // system level for windows is to search the registry keys
    #[cfg(not(windows))]
    if lvls & LOAD_FLAG_SEARCH_SYSTEM != 0 {
        if let Some(path) = system_config_dir() {
            if path.exists() {
                result.push(path);
            }
        }
    }

    result
}

pub(crate) fn parse_driver_uri(uri: &str) -> Result<(&str, &str)> {
    let idx = uri.find(":").ok_or(Error::with_message_and_status(
        format!("Invalid URI: {uri}"),
        Status::InvalidArguments,
    ))?;

    let driver = &uri[..idx];
    if uri.len() <= idx + 2 {
        return Ok((driver, uri));
    }

    #[cfg(target_os = "windows")]
    if let Ok(true) = std::fs::exists(uri) {
        return Ok((uri, ""));
    }

    if &uri[idx..idx + 2] == ":/" {
        // scheme is also driver
        return Ok((driver, uri));
    }

    Ok((driver, &uri[idx + 1..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[cfg_attr(target_os = "windows", ignore)] // TODO: remove this line after fixing
    #[test]
    fn test_default_entrypoint() {
        for driver in [
            "adbc_driver_sqlite",
            "adbc_driver_sqlite.dll",
            "driver_sqlite",
            "libadbc_driver_sqlite",
            "libadbc_driver_sqlite.so",
            "libadbc_driver_sqlite.so.6.0.0",
            "/usr/lib/libadbc_driver_sqlite.so",
            "/usr/lib/libadbc_driver_sqlite.so.6.0.0",
            "C:\\System32\\adbc_driver_sqlite.dll",
        ] {
            assert_eq!(get_default_entrypoint(driver), "AdbcDriverSqliteInit");
        }

        for driver in [
            "adbc_sqlite",
            "sqlite",
            "/usr/lib/sqlite.so",
            "C:\\System32\\sqlite.dll",
        ] {
            assert_eq!(get_default_entrypoint(driver), "AdbcSqliteInit");
        }

        for driver in [
            "proprietary_engine",
            "libproprietary_engine.so.6.0.0",
            "/usr/lib/proprietary_engine.so",
            "C:\\System32\\proprietary_engine.dll",
        ] {
            assert_eq!(get_default_entrypoint(driver), "AdbcProprietaryEngineInit");
        }

        for driver in ["driver_example", "libdriver_example.so"] {
            assert_eq!(get_default_entrypoint(driver), "AdbcDriverExampleInit");
        }
    }

    /// Regression test for https://github.com/apache/arrow-adbc/pull/3693
    /// Ensures driver manager tests for Windows pull in Windows crates. This
    /// can be removed/replace when more complete tests are added.
    #[test]
    fn test_user_config_dir() {
        let _ = user_config_dir().unwrap();
    }

    #[test]
    #[cfg_attr(not(windows), ignore)]
    fn test_get_search_paths() {
        #[cfg(target_os = "macos")]
        let system_path = PathBuf::from("/Library/Application Support/ADBC/Drivers");
        #[cfg(not(target_os = "macos"))]
        let system_path = PathBuf::from("/etc/adbc/drivers");

        let search_paths = get_search_paths(LOAD_FLAG_SEARCH_SYSTEM);
        if system_path.exists() {
            assert_eq!(search_paths, vec![system_path]);
        } else {
            assert_eq!(search_paths, Vec::<PathBuf>::new());
        }
    }

    #[test]
    fn test_parse_driver_uri() {
        let cases = vec![
            ("sqlite", Err(Status::InvalidArguments)),
            ("sqlite:", Ok(("sqlite", "sqlite:"))),
            ("sqlite:file::memory:", Ok(("sqlite", "file::memory:"))),
            (
                "sqlite:file::memory:?cache=shared",
                Ok(("sqlite", "file::memory:?cache=shared")),
            ),
            (
                "postgresql://a:b@localhost:9999/nonexistent",
                Ok(("postgresql", "postgresql://a:b@localhost:9999/nonexistent")),
            ),
        ];

        for (input, expected) in cases {
            let result = parse_driver_uri(input);
            match expected {
                Ok((exp_driver, exp_conn)) => {
                    let (driver, conn) = result.expect("Expected Ok result");
                    assert_eq!(driver, exp_driver);
                    assert_eq!(conn, exp_conn);
                }
                Err(exp_status) => {
                    let err = result.expect_err("Expected Err result");
                    assert_eq!(err.status, exp_status);
                }
            }
        }
    }

    #[cfg(target_os = "windows")]
    #[test]
    fn test_parse_driver_uri_windows_file() {
        let tmp_dir = Builder::new()
            .prefix("adbc_tests")
            .tempdir()
            .expect("Failed to create temporary directory for driver manager manifest test");

        let temp_db_path = tmp_dir.path().join("test.dll");
        if let Some(parent) = temp_db_path.parent() {
            std::fs::create_dir_all(parent)
                .expect("Failed to create parent directory for manifest");
        }
        std::fs::write(&temp_db_path, b"").expect("Failed to create temporary database file");

        let (driver, conn) = parse_driver_uri(temp_db_path.to_str().unwrap()).unwrap();

        assert_eq!(driver, temp_db_path.to_str().unwrap());
        assert_eq!(conn, "");

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }
}
