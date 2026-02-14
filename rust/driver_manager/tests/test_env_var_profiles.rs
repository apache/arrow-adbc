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
//   Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::env;
use std::path::PathBuf;

use adbc_core::options::AdbcVersion;
use adbc_core::{error::Status, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::ManagedDatabase;

fn write_profile_to_tempfile(tmp_dir: &tempfile::TempDir, name: &str, content: &str) -> PathBuf {
    let profile_path = tmp_dir.path().join(format!("{}.toml", name));
    std::fs::write(&profile_path, content).expect("Failed to write profile");
    profile_path
}

#[test]
#[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
fn test_env_var_replacement_basic() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_env_var_test")
        .tempdir()
        .expect("Failed to create temporary directory");

    // Set a test environment variable
    let prev_value = env::var_os("ADBC_TEST_ENV_VAR");
    env::set_var("ADBC_TEST_ENV_VAR", ":memory:");

    let profile_content = r#"
version = 1
driver = "adbc_driver_sqlite"

[options]
uri = "{{ env_var(ADBC_TEST_ENV_VAR) }}"
"#;

    let profile_path = write_profile_to_tempfile(&tmp_dir, "test", profile_content);
    let uri = format!("profile://{}", profile_path.display());

    let result = ManagedDatabase::from_uri(&uri, None, AdbcVersion::V100, LOAD_FLAG_DEFAULT, None);

    // Restore environment variable
    match prev_value {
        Some(val) => env::set_var("ADBC_TEST_ENV_VAR", val),
        None => env::remove_var("ADBC_TEST_ENV_VAR"),
    }

    match result {
        Ok(_db) => {
            // Successfully created database with env_var replacement
        }
        Err(e) => {
            panic!(
                "Failed to create database with env_var replacement: {}",
                e.message
            );
        }
    }

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
#[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
fn test_env_var_replacement_empty() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_env_var_test")
        .tempdir()
        .expect("Failed to create temporary directory");

    // Make sure the env var doesn't exist
    env::remove_var("ADBC_NONEXISTENT_VAR_12345");

    let profile_content = r#"
version = 1
driver = "adbc_driver_sqlite"

[options]
uri = ":memory:"
test_option = "{{ env_var(ADBC_NONEXISTENT_VAR_12345) }}"
"#;

    let profile_path = write_profile_to_tempfile(&tmp_dir, "test", profile_content);
    let uri = format!("profile://{}", profile_path.display());

    // Should succeed with empty string for undefined env var
    let result = ManagedDatabase::from_uri(&uri, None, AdbcVersion::V100, LOAD_FLAG_DEFAULT, None);
    assert!(result.is_err(), "Expected error for malformed env_var");
    if let Err(err) = result {
        assert_eq!(
            err.message,
            "[SQLite] Unknown database option test_option=''"
        )
    }

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
#[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
fn test_env_var_replacement_missing_closing_paren() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_env_var_test")
        .tempdir()
        .expect("Failed to create temporary directory");

    let profile_content = r#"
version = 1
driver = "adbc_driver_sqlite"

[options]
uri = ":memory:"
test_option = "{{ env_var(SOME_VAR }}"
"#;

    let profile_path = write_profile_to_tempfile(&tmp_dir, "test", profile_content);
    let uri = format!("profile://{}", profile_path.display());

    let result = ManagedDatabase::from_uri(&uri, None, AdbcVersion::V100, LOAD_FLAG_DEFAULT, None);

    assert!(result.is_err(), "Expected error for malformed env_var");
    if let Err(err) = result {
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(
            err.message
                .contains("invalid profile replacement expression"),
            "Error message should mention invalid expression, got: {}",
            err.message
        );
    }

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
#[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
fn test_env_var_replacement_missing_arg() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_env_var_test")
        .tempdir()
        .expect("Failed to create temporary directory");

    let profile_content = r#"
version = 1
driver = "adbc_driver_sqlite"

[options]
uri = ":memory:"
test_option = "{{ env_var() }}"
"#;

    let profile_path = write_profile_to_tempfile(&tmp_dir, "test", profile_content);
    let uri = format!("profile://{}", profile_path.display());

    let result = ManagedDatabase::from_uri(&uri, None, AdbcVersion::V100, LOAD_FLAG_DEFAULT, None);

    assert!(result.is_err(), "Expected error for empty env_var argument");
    if let Err(err) = result {
        assert_eq!(err.status, Status::InvalidArguments);
        assert!(
            err.message.contains("empty environment variable name"),
            "Error message should mention empty variable name, got: {}",
            err.message
        );
    }

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
#[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
fn test_env_var_replacement_interpolation() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_env_var_test")
        .tempdir()
        .expect("Failed to create temporary directory");

    // Set a test environment variable
    let prev_value = env::var_os("ADBC_TEST_INTERPOLATE");
    env::set_var("ADBC_TEST_INTERPOLATE", "middle_value");

    let profile_content = r#"
version = 1
driver = "adbc_driver_sqlite"

[options]
uri = ":memory:"
test_option = "prefix_{{ env_var(ADBC_TEST_INTERPOLATE) }}_suffix"
"#;

    let profile_path = write_profile_to_tempfile(&tmp_dir, "test", profile_content);
    let uri = format!("profile://{}", profile_path.display());

    let result = ManagedDatabase::from_uri(&uri, None, AdbcVersion::V100, LOAD_FLAG_DEFAULT, None);

    // Restore environment variable
    match prev_value {
        Some(val) => env::set_var("ADBC_TEST_INTERPOLATE", val),
        None => env::remove_var("ADBC_TEST_INTERPOLATE"),
    }

    assert!(result.is_err(), "Expected error for malformed env_var");
    if let Err(err) = result {
        assert_eq!(
            err.message,
            "[SQLite] Unknown database option test_option='prefix_middle_value_suffix'"
        );
    }

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
#[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
fn test_env_var_replacement_multiple() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_env_var_test")
        .tempdir()
        .expect("Failed to create temporary directory");

    // Set test environment variables
    let prev_var1 = env::var_os("ADBC_TEST_VAR1");
    let prev_var2 = env::var_os("ADBC_TEST_VAR2");
    env::set_var("ADBC_TEST_VAR1", "first");
    env::set_var("ADBC_TEST_VAR2", "second");

    let profile_content = r#"
version = 1
driver = "adbc_driver_sqlite"

[options]
uri = ":memory:"
test_option = "{{ env_var(ADBC_TEST_VAR1) }}_and_{{ env_var(ADBC_TEST_VAR2) }}"
"#;

    let profile_path = write_profile_to_tempfile(&tmp_dir, "test", profile_content);
    let uri = format!("profile://{}", profile_path.display());

    let result = ManagedDatabase::from_uri(&uri, None, AdbcVersion::V100, LOAD_FLAG_DEFAULT, None);

    // Restore environment variables
    match prev_var1 {
        Some(val) => env::set_var("ADBC_TEST_VAR1", val),
        None => env::remove_var("ADBC_TEST_VAR1"),
    }
    match prev_var2 {
        Some(val) => env::set_var("ADBC_TEST_VAR2", val),
        None => env::remove_var("ADBC_TEST_VAR2"),
    }

    assert!(result.is_err(), "Expected error for malformed env_var");
    if let Err(err) = result {
        assert_eq!(
            err.message,
            "[SQLite] Unknown database option test_option='first_and_second'"
        );
    }

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}

#[test]
#[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
fn test_env_var_replacement_whitespace() {
    let tmp_dir = tempfile::Builder::new()
        .prefix("adbc_env_var_test")
        .tempdir()
        .expect("Failed to create temporary directory");

    // Set a test environment variable
    let prev_value = env::var_os("ADBC_TEST_WHITESPACE");
    env::set_var("ADBC_TEST_WHITESPACE", "value");

    let profile_content = r#"
version = 1
driver = "adbc_driver_sqlite"

[options]
uri = ":memory:"
test_option = "{{   env_var(  ADBC_TEST_WHITESPACE  )   }}"
"#;

    let profile_path = write_profile_to_tempfile(&tmp_dir, "test", profile_content);
    let uri = format!("profile://{}", profile_path.display());

    let result = ManagedDatabase::from_uri(&uri, None, AdbcVersion::V100, LOAD_FLAG_DEFAULT, None);

    // Restore environment variable
    match prev_value {
        Some(val) => env::set_var("ADBC_TEST_WHITESPACE", val),
        None => env::remove_var("ADBC_TEST_WHITESPACE"),
    }

    assert!(result.is_err(), "Expected error for malformed env_var");
    if let Err(err) = result {
        assert_eq!(
            err.message,
            "[SQLite] Unknown database option test_option='value'"
        );
    }

    tmp_dir
        .close()
        .expect("Failed to close/remove temporary directory");
}
