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

use std::path::PathBuf;

use adbc_core::options::{AdbcVersion, OptionDatabase, OptionValue};
use adbc_core::{error::Status, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::profile::{
    ConnectionProfile, ConnectionProfileProvider, FilesystemProfileProvider,
};
use adbc_driver_manager::ManagedDatabase;
use serial_test::serial;
use std::env;

mod common;

fn write_profile_to_tempfile(profile_name: &str, content: &str) -> (tempfile::TempDir, PathBuf) {
    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_profile_tests")
        .tempdir()
        .expect("Failed to create temporary directory for profile tests");

    let profile_path = tmp_dir.path().join(format!("{}.toml", profile_name));
    std::fs::write(&profile_path, content).expect("Failed to write profile to temporary file");

    (tmp_dir, profile_path)
}

fn simple_profile() -> String {
    r#"
profile_version = 1
driver = "adbc_driver_sqlite"

[Options]
uri = ":memory:"
"#
    .to_string()
}

fn profile_with_nested_options() -> String {
    r#"
profile_version = 1
driver = "adbc_driver_sqlite"

[Options]
uri = ":memory:"
[Options.connection]
timeout = 30
retry = true
[Options.connection.pool]
max_size = 10
min_size = 2
idle_timeout = 300.5
"#
    .to_string()
}

fn profile_with_all_types() -> String {
    r#"
profile_version = 1
driver = "adbc_driver_sqlite"

[Options]
uri = ":memory:"
string_opt = "test_value"
int_opt = 42
float_opt = 12.34
bool_opt = true
"#
    .to_string()
}

fn profile_without_driver() -> String {
    r#"
profile_version = 1

[Options]
uri = ":memory:"
"#
    .to_string()
}

fn profile_without_options() -> String {
    r#"
profile_version = 1
driver = "adbc_driver_sqlite"
"#
    .to_string()
}

fn profile_with_unsupported_version() -> String {
    r#"
profile_version = 2
driver = "adbc_driver_sqlite"

[Options]
uri = ":memory:"
"#
    .to_string()
}

fn invalid_toml() -> &'static str {
    r#"
profile_version = 1
driver = "adbc_driver_sqlite"
[Options
uri = ":memory:"
"#
}

#[test]
fn test_filesystem_profile_load_simple() {
    let (tmp_dir, profile_path) = write_profile_to_tempfile("simple", &simple_profile());

    let search_paths = Some(vec![tmp_dir.path().to_path_buf()]);
    let provider = FilesystemProfileProvider::new_with_search_paths(search_paths);
    let profile = provider
        .get_profile(profile_path.to_str().unwrap())
        .unwrap();

    let (driver_name, init_func) = profile.get_driver_name().unwrap();
    assert_eq!(driver_name, "adbc_driver_sqlite");
    assert!(init_func.is_none());

    let options: Vec<_> = profile.get_options().unwrap().into_iter().collect();
    assert_eq!(options.len(), 1);

    let (key, value) = &options[0];
    assert_eq!(key, &OptionDatabase::Uri);
    match value {
        OptionValue::String(s) => assert_eq!(s, ":memory:"),
        _ => panic!("Expected string value for uri option"),
    }

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
fn test_filesystem_profile_nested_options() {
    let (tmp_dir, profile_path) =
        write_profile_to_tempfile("nested", &profile_with_nested_options());

    let search_paths = Some(vec![tmp_dir.path().to_path_buf()]);
    let provider = FilesystemProfileProvider::new_with_search_paths(search_paths);
    let profile = provider
        .get_profile(profile_path.to_str().unwrap())
        .unwrap();

    let options: Vec<_> = profile.get_options().unwrap().into_iter().collect();

    // Should have uri, connection.timeout, connection.retry, connection.pool.max_size,
    // connection.pool.min_size, connection.pool.idle_timeout
    assert_eq!(options.len(), 6);

    // Verify the nested key construction
    let option_keys: Vec<String> = options
        .iter()
        .map(|(k, _)| k.as_ref().to_string())
        .collect();

    assert!(option_keys.contains(&"uri".to_string()));
    assert!(option_keys.contains(&"connection.timeout".to_string()));
    assert!(option_keys.contains(&"connection.retry".to_string()));
    assert!(option_keys.contains(&"connection.pool.max_size".to_string()));
    assert!(option_keys.contains(&"connection.pool.min_size".to_string()));
    assert!(option_keys.contains(&"connection.pool.idle_timeout".to_string()));

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
fn test_filesystem_profile_all_option_types() {
    let (tmp_dir, profile_path) = write_profile_to_tempfile("all_types", &profile_with_all_types());

    let provider =
        FilesystemProfileProvider::new_with_search_paths(Some(vec![tmp_dir.path().to_path_buf()]));
    let profile = provider
        .get_profile(profile_path.to_str().unwrap())
        .unwrap();

    let options: Vec<_> = profile.get_options().unwrap().into_iter().collect();
    assert_eq!(options.len(), 5);

    for (key, value) in options {
        match key.as_ref() {
            "uri" => match value {
                OptionValue::String(s) => assert_eq!(s, ":memory:"),
                _ => panic!("Expected string value for uri"),
            },
            "string_opt" => match value {
                OptionValue::String(s) => assert_eq!(s, "test_value"),
                _ => panic!("Expected string value for string_opt"),
            },
            "int_opt" => match value {
                OptionValue::Int(i) => assert_eq!(i, 42),
                _ => panic!("Expected int value for int_opt"),
            },
            "float_opt" => match value {
                OptionValue::Double(f) => assert!((f - 12.34).abs() < 1e-10),
                _ => panic!("Expected double value for float_opt"),
            },
            "bool_opt" => match value {
                OptionValue::String(s) => assert_eq!(s, "true"),
                _ => panic!("Expected string value for bool_opt"),
            },
            _ => panic!("Unexpected option key: {}", key.as_ref()),
        }
    }

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
fn test_filesystem_profile_error_cases() {
    let test_cases = vec![
        (
            "without options",
            profile_without_options(),
            Status::InvalidArguments,
            "missing or invalid 'Options' table in profile",
        ),
        (
            "unsupported version",
            profile_with_unsupported_version(),
            Status::InvalidArguments,
            "unsupported profile version",
        ),
        (
            "no version",
            r#"
driver = "adbc_driver_sqlite"
[Options]
"#
            .to_string(),
            Status::InvalidArguments,
            "missing 'profile_version' in profile",
        ),
        (
            "bad version",
            r#"
profile_version = "1"
driver = "adbc_driver_sqlite"
[Options]
"#
            .to_string(),
            Status::InvalidArguments,
            "invalid 'profile_version' in profile",
        ),
        (
            "invalid toml",
            invalid_toml().to_string(),
            Status::InvalidArguments,
            "TOML parse error",
        ),
        (
            "no driver",
            r#"
profile_version = 1
[Options]
"#
            .to_string(),
            Status::InvalidArguments,
            "missing or invalid 'driver' field in profile",
        ),
        (
            "numeric driver",
            r#"
profile_version = 1
driver = 2
[Options]
"#
            .to_string(),
            Status::InvalidArguments,
            "missing or invalid 'driver' field in profile",
        ),
        (
            "table driver",
            r#"
profile_version = 1
[driver]
foo = "bar"
[Options]
"#
            .to_string(),
            Status::InvalidArguments,
            "missing or invalid 'driver' field in profile",
        ),
        (
            "no options",
            r#"
profile_version = 1
driver = "foo"
"#
            .to_string(),
            Status::InvalidArguments,
            "missing or invalid 'Options' table in profile",
        ),
    ];

    for (name, profile_content, expected_status, expected_msg_fragment) in test_cases {
        let (tmp_dir, profile_path) = write_profile_to_tempfile(name, &profile_content);

        let provider = FilesystemProfileProvider::new_with_search_paths(Some(vec![tmp_dir
            .path()
            .to_path_buf()]));
        let result = provider.get_profile(profile_path.to_str().unwrap());

        assert!(result.is_err(), "Test case '{}': expected error", name);
        let err = result.unwrap_err();
        assert_eq!(
            err.status, expected_status,
            "Test case '{}': wrong status",
            name
        );
        if !expected_msg_fragment.is_empty() {
            assert!(
                err.message.contains(expected_msg_fragment),
                "Test case '{}': expected '{}' in error message, got '{}'",
                name,
                expected_msg_fragment,
                err.message
            );
        }

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }
}

#[test]
fn test_filesystem_profile_not_found() {
    let provider = FilesystemProfileProvider::default();
    let result = provider.get_profile("nonexistent_profile");

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.status, Status::NotFound);
    assert!(err.message.contains("Profile not found"));
}

#[test]
#[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
fn test_database_from_uri_with_profile() {
    let (tmp_dir, profile_path) = write_profile_to_tempfile("test_db", &simple_profile());

    let uri = format!("profile://{}", profile_path.display());
    let database =
        ManagedDatabase::from_uri(&uri, None, AdbcVersion::V100, LOAD_FLAG_DEFAULT, None).unwrap();

    common::test_database(&database);

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
#[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
fn test_database_from_uri_with_profile_additional_options() {
    let (tmp_dir, profile_path) = write_profile_to_tempfile("test_opts", &simple_profile());

    let uri = format!("profile://{}", profile_path.display());

    // Additional options should override profile options
    let additional_opts = vec![(
        OptionDatabase::Uri,
        OptionValue::String("file::memory:".to_string()),
    )];

    let database = ManagedDatabase::from_uri_with_opts(
        &uri,
        None,
        AdbcVersion::V100,
        LOAD_FLAG_DEFAULT,
        None,
        additional_opts,
    )
    .unwrap();

    common::test_database(&database);

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
fn test_profile_loading_scenarios() {
    let test_cases = vec![
        (
            "search with additional paths",
            "searchable",
            simple_profile(),
            true,
            true,
        ),
        ("absolute path", "absolute", simple_profile(), false, true),
    ];

    for (name, profile_name, profile_content, use_search_path, use_absolute) in test_cases {
        let (tmp_dir, profile_path) = write_profile_to_tempfile(profile_name, &profile_content);

        let search_paths = if use_search_path {
            Some(vec![tmp_dir.path().to_path_buf()])
        } else {
            None
        };
        let provider = FilesystemProfileProvider::new_with_search_paths(search_paths);

        let profile_arg = if use_absolute {
            profile_path.to_str().unwrap()
        } else {
            profile_name
        };

        let profile = provider
            .get_profile(profile_arg)
            .unwrap_or_else(|e| panic!("Test case '{}' failed: {:?}", name, e));

        let (driver_name, _) = profile.get_driver_name().unwrap();
        assert_eq!(
            driver_name, "adbc_driver_sqlite",
            "Test case '{}': driver mismatch",
            name
        );

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }
}

#[test]
fn test_profile_display() {
    let (tmp_dir, profile_path) = write_profile_to_tempfile("display", &simple_profile());

    let provider = FilesystemProfileProvider::default();
    let profile = provider
        .get_profile(profile_path.to_str().unwrap())
        .unwrap();

    let display_str = format!("{}", profile);
    assert!(display_str.contains("FilesystemProfile"));
    assert!(display_str.contains(profile_path.to_str().unwrap()));

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
#[serial]
fn test_profile_hierarchical_path_via_env_var() {
    use std::env;

    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_profile_env_test")
        .tempdir()
        .expect("Failed to create temporary directory");

    // Create a hierarchical directory structure: databases/postgres/
    let databases_dir = tmp_dir.path().join("databases");
    let postgres_dir = databases_dir.join("postgres");
    std::fs::create_dir_all(&postgres_dir).expect("Failed to create subdirectories");

    // Create a profile in the nested directory
    let profile_path = postgres_dir.join("production.toml");
    std::fs::write(&profile_path, simple_profile()).expect("Failed to write profile");

    // Verify the file was created
    assert!(
        profile_path.is_file(),
        "Profile file should exist at {}",
        profile_path.display()
    );

    // Set ADBC_PROFILE_PATH to the parent directory
    let _guard = common::SetEnv::new("ADBC_PROFILE_PATH", tmp_dir.path());

    // Verify the environment variable is set correctly
    assert_eq!(
        env::var_os("ADBC_PROFILE_PATH").as_deref(),
        Some(tmp_dir.path().as_os_str())
    );

    // Try to load the profile using hierarchical relative path
    let provider = FilesystemProfileProvider::default();
    let result = provider.get_profile("databases/postgres/production");

    // Verify the profile was loaded successfully
    let profile = result.expect("Failed to load profile from hierarchical path");
    let (driver_name, _) = profile.get_driver_name().unwrap();
    assert_eq!(driver_name, "adbc_driver_sqlite");

    // Verify it loaded from the correct path
    let display_str = format!("{}", profile);
    assert!(
        display_str.contains(&profile_path.to_string_lossy().to_string()),
        "display: {}, profile_path: {}",
        display_str,
        profile_path.to_string_lossy()
    );

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
#[serial]
fn test_profile_hierarchical_path_with_extension_via_env_var() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_profile_env_test2")
        .tempdir()
        .expect("Failed to create temporary directory");

    // Create a hierarchical directory structure: configs/dev/
    let configs_dir = tmp_dir.path().join("configs");
    let dev_dir = configs_dir.join("dev");
    std::fs::create_dir_all(&dev_dir).expect("Failed to create subdirectories");

    // Create a profile in the nested directory
    let profile_path = dev_dir.join("database.toml");
    std::fs::write(&profile_path, simple_profile()).expect("Failed to write profile");

    // Set ADBC_PROFILE_PATH to the parent directory
    let _guard = common::SetEnv::new("ADBC_PROFILE_PATH", tmp_dir.path());

    // Try to load the profile using hierarchical relative path with .toml extension
    let provider = FilesystemProfileProvider::default();
    let result = provider.get_profile("configs/dev/database.toml");

    // Verify the profile was loaded successfully
    let profile = result.expect("Failed to load profile from hierarchical path with extension");
    let (driver_name, _) = profile.get_driver_name().unwrap();
    assert_eq!(driver_name, "adbc_driver_sqlite");

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
fn test_profile_hierarchical_path_additional_search_paths() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_profile_hier_test")
        .tempdir()
        .expect("Failed to create temporary directory");

    // Create a hierarchical directory structure: projects/myapp/
    let projects_dir = tmp_dir.path().join("projects");
    let myapp_dir = projects_dir.join("myapp");
    std::fs::create_dir_all(&myapp_dir).expect("Failed to create subdirectories");

    // Create a profile in the nested directory
    let profile_path = myapp_dir.join("local.toml");
    std::fs::write(&profile_path, simple_profile()).expect("Failed to write profile");

    // Load profile using hierarchical path via additional_search_paths
    let provider =
        FilesystemProfileProvider::new_with_search_paths(Some(vec![tmp_dir.path().to_path_buf()]));
    let result = provider.get_profile("projects/myapp/local");

    // Verify the profile was loaded successfully
    let profile = result.expect("Failed to load profile from hierarchical path");
    let (driver_name, _) = profile.get_driver_name().unwrap();
    assert_eq!(driver_name, "adbc_driver_sqlite");

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
fn test_profile_conda_prefix() {
    #[cfg(conda_build)]
    let is_conda_build = true;
    #[cfg(not(conda_build))]
    let is_conda_build = false;

    eprintln!(
        "Is conda build: {}",
        if is_conda_build {
            "defined"
        } else {
            "not defined"
        }
    );
    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_profile_conda_prefix_test")
        .tempdir()
        .expect("Failed to create temporary directory");

    let filepath = tmp_dir
        .path()
        .join("etc")
        .join("adbc")
        .join("profiles")
        .join("sqlite-profile.toml");

    std::fs::create_dir_all(filepath.parent().unwrap())
        .expect("Failed to create directories for conda prefix test");
    std::fs::write(&filepath, simple_profile()).expect("Failed to write profile");

    // Set CONDA_PREFIX environment variable
    let _guard = common::SetEnv::new("CONDA_PREFIX", tmp_dir.path());

    let uri = "profile://sqlite-profile";
    let result = ManagedDatabase::from_uri(uri, None, AdbcVersion::V100, LOAD_FLAG_DEFAULT, None);

    if is_conda_build {
        assert!(result.is_ok(), "Expected success for conda build");
    } else {
        assert!(result.is_err(), "Expected error for non-conda build");
        if let Err(err) = result {
            assert!(
                err.message.contains("Profile not found: sqlite-profile"),
                "Expected 'Profile file does not exist' error, got: {}",
                err.message
            );
        }
    }

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory")
}

#[test]
#[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
fn test_profile_load_manifest() {
    let driver_path = PathBuf::from(
        env::var_os("ADBC_DRIVER_MANAGER_TEST_LIB")
            .expect("ADBC_DRIVER_MANAGER_TEST_LIB must be set for driver manager manifest tests"),
    )
    .to_string_lossy()
    .to_string()
    .replace("\\", "\\\\");
    let manifest_dir = tempfile::Builder::new()
        .prefix("adbc-test-manifest")
        .tempdir()
        .unwrap();
    let profile_dir = tempfile::Builder::new()
        .prefix("adbc-test-profile")
        .tempdir()
        .unwrap();

    let manifest_contents = format!(
        r#"
manifest_version = 1
[Driver]
shared = "{driver_path}"
"#
    );

    let manifest_path = manifest_dir.path().join("sqlite.toml");
    std::fs::write(&manifest_path, &manifest_contents).unwrap();

    let manifest_path = profile_dir.path().join("sqlitemani.toml");
    std::fs::write(&manifest_path, &manifest_contents).unwrap();

    let profile_contents = r#"
profile_version = 1
driver = "sqlite"
[Options]
uri = ":memory:"
"#;

    let profile_path = profile_dir.path().join("sqlitedev.toml");
    std::fs::write(&profile_path, profile_contents).unwrap();
    let profile_path = manifest_dir.path().join("sqliteprof.toml");
    std::fs::write(&profile_path, profile_contents).unwrap();

    let provider = FilesystemProfileProvider::new_with_search_paths(Some(vec![profile_dir
        .path()
        .to_path_buf()]));
    let database = ManagedDatabase::from_uri_with_profile_provider(
        "profile://sqlitedev",
        None,
        AdbcVersion::V100,
        LOAD_FLAG_DEFAULT,
        Some(vec![manifest_dir.path().to_path_buf()]),
        provider.clone(),
        std::iter::empty(),
    )
    .unwrap();

    common::test_database(&database);

    // should not be able to load a profile from manifest dir or vice versa
    let result = ManagedDatabase::from_uri_with_profile_provider(
        "profile://sqliteprof",
        None,
        AdbcVersion::V100,
        LOAD_FLAG_DEFAULT,
        Some(vec![manifest_dir.path().to_path_buf()]),
        provider.clone(),
        std::iter::empty(),
    );
    assert!(result.is_err());
    let err = result.err().unwrap();
    assert!(
        err.message.contains("Profile not found: sqliteprof"),
        "{}",
        err.message
    );

    let result = ManagedDatabase::from_uri_with_profile_provider(
        "sqlitemani://",
        None,
        AdbcVersion::V100,
        LOAD_FLAG_DEFAULT,
        Some(vec![manifest_dir.path().to_path_buf()]),
        provider.clone(),
        std::iter::empty(),
    );
    assert!(result.is_err());
    #[cfg(not(windows))]
    {
        // The Windows error just says 'LoadLibraryExW failed'
        let err = result.err().unwrap();
        assert!(err.message.contains("sqlitemani"), "{}", err.message);
    }

    manifest_dir.close().unwrap();
    profile_dir.close().unwrap();
}
