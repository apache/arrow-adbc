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
use adbc_driver_manager::connection_profiles::{
    ConnectionProfile, ConnectionProfileProvider, FilesystemProfileProvider,
};
use adbc_driver_manager::ManagedDatabase;

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
version = 1
driver = "adbc_driver_sqlite"

[options]
uri = ":memory:"
"#
    .to_string()
}

fn profile_with_nested_options() -> String {
    r#"
version = 1
driver = "adbc_driver_sqlite"

[options]
uri = ":memory:"
[options.connection]
timeout = 30
retry = true
[options.connection.pool]
max_size = 10
min_size = 2
idle_timeout = 300.5
"#
    .to_string()
}

fn profile_with_all_types() -> String {
    r#"
version = 1
driver = "adbc_driver_sqlite"

[options]
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
version = 1

[options]
uri = ":memory:"
"#
    .to_string()
}

fn profile_without_options() -> String {
    r#"
version = 1
driver = "adbc_driver_sqlite"
"#
    .to_string()
}

fn profile_with_unsupported_version() -> String {
    r#"
version = 2
driver = "adbc_driver_sqlite"

[options]
uri = ":memory:"
"#
    .to_string()
}

fn invalid_toml() -> &'static str {
    r#"
version = 1
driver = "adbc_driver_sqlite"
[options
uri = ":memory:"
"#
}

#[test]
fn test_filesystem_profile_load_simple() {
    let (tmp_dir, profile_path) = write_profile_to_tempfile("simple", &simple_profile());

    let provider = FilesystemProfileProvider;
    let profile = provider
        .get_profile(
            profile_path.to_str().unwrap(),
            Some(vec![tmp_dir.path().to_path_buf()]),
        )
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

    let provider = FilesystemProfileProvider;
    let profile = provider
        .get_profile(
            profile_path.to_str().unwrap(),
            Some(vec![tmp_dir.path().to_path_buf()]),
        )
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

    let provider = FilesystemProfileProvider;
    let profile = provider
        .get_profile(
            profile_path.to_str().unwrap(),
            Some(vec![tmp_dir.path().to_path_buf()]),
        )
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
            "missing or invalid 'options' table in profile",
        ),
        (
            "unsupported version",
            profile_with_unsupported_version(),
            Status::InvalidArguments,
            "unsupported profile version",
        ),
        (
            "invalid toml",
            invalid_toml().to_string(),
            Status::InvalidArguments,
            "",
        ),
    ];

    for (name, profile_content, expected_status, expected_msg_fragment) in test_cases {
        let (tmp_dir, profile_path) = write_profile_to_tempfile(name, &profile_content);

        let provider = FilesystemProfileProvider;
        let result = provider.get_profile(
            profile_path.to_str().unwrap(),
            Some(vec![tmp_dir.path().to_path_buf()]),
        );

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
    let provider = FilesystemProfileProvider;
    let result = provider.get_profile("nonexistent_profile", None);

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert_eq!(err.status, Status::NotFound);
    assert!(err.message.contains("Profile not found"));
}

#[test]
fn test_filesystem_profile_without_driver() {
    let (tmp_dir, profile_path) = write_profile_to_tempfile("no_driver", &profile_without_driver());

    let provider = FilesystemProfileProvider;
    let profile = provider
        .get_profile(
            profile_path.to_str().unwrap(),
            Some(vec![tmp_dir.path().to_path_buf()]),
        )
        .unwrap();

    let (driver_name, _) = profile.get_driver_name().unwrap();
    // Should get empty string for missing driver
    assert_eq!(driver_name, "");

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
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

        let provider = FilesystemProfileProvider;
        let search_paths = if use_search_path {
            Some(vec![tmp_dir.path().to_path_buf()])
        } else {
            None
        };

        let profile_arg = if use_absolute {
            profile_path.to_str().unwrap()
        } else {
            profile_name
        };

        let profile = provider
            .get_profile(profile_arg, search_paths)
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

    let provider = FilesystemProfileProvider;
    let profile = provider
        .get_profile(profile_path.to_str().unwrap(), None)
        .unwrap();

    let display_str = format!("{}", profile);
    assert!(display_str.contains("FilesystemProfile"));
    assert!(display_str.contains(profile_path.to_str().unwrap()));

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
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
    let prev_value = env::var_os("ADBC_PROFILE_PATH");
    env::set_var("ADBC_PROFILE_PATH", tmp_dir.path());

    // Verify the environment variable is set correctly
    assert_eq!(
        env::var_os("ADBC_PROFILE_PATH").as_deref(),
        Some(tmp_dir.path().as_os_str())
    );

    // Try to load the profile using hierarchical relative path
    let provider = FilesystemProfileProvider;
    let result = provider.get_profile("databases/postgres/production", None);

    // Restore the original environment variable
    match prev_value {
        Some(val) => env::set_var("ADBC_PROFILE_PATH", val),
        None => env::remove_var("ADBC_PROFILE_PATH"),
    }

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
fn test_profile_hierarchical_path_with_extension_via_env_var() {
    use std::env;

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
    let prev_value = env::var_os("ADBC_PROFILE_PATH");
    env::set_var("ADBC_PROFILE_PATH", tmp_dir.path());

    // Try to load the profile using hierarchical relative path with .toml extension
    let provider = FilesystemProfileProvider;
    let result = provider.get_profile("configs/dev/database.toml", None);

    // Restore the original environment variable
    match prev_value {
        Some(val) => env::set_var("ADBC_PROFILE_PATH", val),
        None => env::remove_var("ADBC_PROFILE_PATH"),
    }

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
    let provider = FilesystemProfileProvider;
    let result = provider.get_profile(
        "projects/myapp/local",
        Some(vec![tmp_dir.path().to_path_buf()]),
    );

    // Verify the profile was loaded successfully
    let profile = result.expect("Failed to load profile from hierarchical path");
    let (driver_name, _) = profile.get_driver_name().unwrap();
    assert_eq!(driver_name, "adbc_driver_sqlite");

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}
