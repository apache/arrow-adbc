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

use crate::search::find_filesystem_profile;
use crate::ManagedDatabase;
use adbc_core::{
    error::{Error, Result, Status},
    options::{OptionDatabase, OptionValue},
    Optionable,
};
use adbc_ffi::FFI_AdbcDriverInitFunc;
use regex::{Captures, Regex};
use std::path::PathBuf;
use std::{collections::HashMap, fs};
use std::{env, fmt, sync::OnceLock};
use toml::de::{DeTable, DeValue};

/// A connection profile that provides configuration for creating ADBC database connections.
///
/// Profiles contain the driver name, optional initialization function, and database options
/// that can be used to create a configured database connection without needing to specify
/// all connection details programmatically.
pub trait ConnectionProfile {
    /// Returns the driver name and an optional static initialization function.
    ///
    /// # Returns
    ///
    /// A tuple containing:
    /// - A string slice with the driver name (e.g., "adbc_driver_sqlite")
    /// - An optional reference to a statically-linked driver initialization function
    ///
    /// # Errors
    ///
    /// Returns an error if the profile is malformed or cannot be read.
    fn get_driver_name(&self) -> Result<(&str, Option<&FFI_AdbcDriverInitFunc>)>;

    /// Returns an iterator of database options to apply when creating a connection.
    ///
    /// # Returns
    ///
    /// An iterator yielding `(OptionDatabase, OptionValue)` tuples that should be
    /// applied to the database before initialization.
    ///
    /// # Errors
    ///
    /// Returns an error if the options cannot be retrieved or parsed.
    fn get_options(
        &self,
    ) -> Result<impl IntoIterator<Item = (<ManagedDatabase as Optionable>::Option, OptionValue)>>;
}

/// Provides access to connection profiles from a specific storage backend.
///
/// Implementations of this trait define how profiles are located and loaded,
/// such as from the filesystem, a configuration service, or other sources.
pub trait ConnectionProfileProvider {
    /// The concrete profile type returned by this provider.
    type Profile: ConnectionProfile;

    /// Retrieves a connection profile by name.
    ///
    /// # Arguments
    ///
    /// * `name` - The profile name or path to locate
    ///
    /// # Returns
    ///
    /// The loaded connection profile.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The profile cannot be found
    /// - The profile file is malformed
    /// - The profile version is unsupported
    fn get_profile(&self, name: &str) -> Result<Self::Profile>;
}

/// Provides connection profiles from TOML files on the filesystem.
///
/// This provider searches for profile files with a `.toml` extension in standard
/// configuration directories and any additional paths provided. Profile files must
/// conform to the ADBC profile specification version 1.
///
/// # Search Order
///
/// Profiles are searched in the following order:
/// 1. Additional paths provided via `new_with_search_paths`
/// 2. `ADBC_PROFILE_PATH` environment variable paths
/// 3. User configuration directory (`~/.config/adbc/profiles` on Linux,
///    `~/Library/Application Support/ADBC/Profiles` on macOS,
///    `%LOCALAPPDATA%\ADBC\Profiles` on Windows)
///
/// # Example
///
/// ```no_run
/// use adbc_driver_manager::profile::{
///     ConnectionProfileProvider, FilesystemProfileProvider
/// };
///
/// let provider = FilesystemProfileProvider::default();
/// let profile = provider.get_profile("my_database")?;
/// # Ok::<(), adbc_core::error::Error>(())
/// ```
#[derive(Clone, Default)]
pub struct FilesystemProfileProvider {
    additional_paths: Option<Vec<PathBuf>>,
}

impl FilesystemProfileProvider {
    /// Search the given paths (if any) for profiles.
    pub fn new_with_search_paths(additional_paths: Option<Vec<PathBuf>>) -> Self {
        Self { additional_paths }
    }
}

impl ConnectionProfileProvider for FilesystemProfileProvider {
    type Profile = FilesystemProfile;

    fn get_profile(&self, name: &str) -> Result<Self::Profile> {
        let profile_path = find_filesystem_profile(name, &self.additional_paths)?;
        FilesystemProfile::from_path(profile_path)
    }
}

/// Recursively processes TOML table entries into database options.
///
/// This function flattens nested TOML tables into dot-separated option keys
/// and converts TOML values into appropriate `OptionValue` types.
///
/// # Arguments
///
/// * `opts` - Map to populate with parsed options
/// * `prefix` - Current key prefix for nested tables (e.g., "connection." for nested options)
/// * `table` - TOML table to process
///
/// # Supported Types
///
/// - String values → `OptionValue::String`
/// - Integer values → `OptionValue::Int`
/// - Float values → `OptionValue::Double`
/// - Boolean values → `OptionValue::String` (converted to "true" or "false")
/// - Nested tables → Recursively processed with dot-separated keys
///
/// # Errors
///
/// Returns an error if:
/// - An integer value cannot be parsed as `i64`
/// - A float value cannot be parsed as `f64`
/// - An unsupported TOML type is encountered (e.g., arrays, inline tables)
fn process_options(
    opts: &mut HashMap<OptionDatabase, OptionValue>,
    prefix: &str,
    table: &DeTable,
) -> Result<()> {
    for (key, value) in table.iter() {
        let full_key = format!("{}{}", prefix, key.get_ref());
        match value.get_ref() {
            DeValue::String(s) => {
                opts.insert(full_key.as_str().into(), OptionValue::String(s.to_string()));
            }
            DeValue::Integer(i) => {
                let val: i64 = i.as_str().parse().map_err(|e| {
                    Error::with_message_and_status(
                        format!("invalid integer value for key '{}': {e}", full_key),
                        Status::InvalidArguments,
                    )
                })?;
                opts.insert(full_key.as_str().into(), OptionValue::Int(val));
            }
            DeValue::Float(f) => {
                let val: f64 = f.as_str().parse().map_err(|e| {
                    Error::with_message_and_status(
                        format!("invalid float value for key '{}': {e}", full_key),
                        Status::InvalidArguments,
                    )
                })?;
                opts.insert(full_key.as_str().into(), OptionValue::Double(val));
            }
            DeValue::Boolean(b) => {
                opts.insert(full_key.as_str().into(), OptionValue::String(b.to_string()));
            }
            DeValue::Table(t) => {
                let nested_prefix = format!("{}.", full_key);
                process_options(opts, &nested_prefix, t)?;
            }
            _ => {
                return Err(Error::with_message_and_status(
                    format!("unsupported option type for key '{}'", full_key),
                    Status::InvalidArguments,
                ));
            }
        }
    }
    Ok(())
}

/// A connection profile loaded from a TOML file on the filesystem.
///
/// This profile contains:
/// - The path to the profile file
/// - The driver name specified in the profile
/// - A map of database options parsed from the profile
///
/// # Profile Format
///
/// Profile files must be valid TOML with the following structure:
///
/// ```toml
/// profile_version = 1
/// driver = "driver_name"
///
/// [Options]
/// option_key = "option_value"
/// nested.key = "nested_value"
/// ```
///
/// Currently, only profile_version 1 profiles are supported.
#[derive(Debug)]
pub struct FilesystemProfile {
    profile_path: PathBuf,
    driver: String,
    opts: HashMap<OptionDatabase, OptionValue>,
}

impl FilesystemProfile {
    /// Loads a profile from the specified filesystem path.
    ///
    /// # Arguments
    ///
    /// * `profile_path` - Path to the TOML profile file
    ///
    /// # Returns
    ///
    /// A loaded `FilesystemProfile` with parsed configuration.
    ///
    /// # Errors
    ///
    /// Returns an error if:
    /// - The file cannot be read
    /// - The TOML is malformed
    /// - The profile version is not "1"
    /// - The `options` table is missing or invalid
    /// - Any option values cannot be parsed
    fn from_path(profile_path: PathBuf) -> Result<Self> {
        let contents = fs::read_to_string(&profile_path).map_err(|e| {
            Error::with_message_and_status(
                format!("could not read profile '{}': {e}", profile_path.display()),
                Status::InvalidArguments,
            )
        })?;

        let profile = DeTable::parse(&contents)
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::InvalidArguments))?;

        let raw_profile_version = profile.get_ref().get("profile_version").ok_or_else(|| {
            Error::with_message_and_status(
                "missing 'profile_version' in profile".to_string(),
                Status::InvalidArguments,
            )
        })?;

        let profile_version = raw_profile_version
            .as_ref()
            .as_integer()
            .and_then(|i| i64::from_str_radix(i.as_str(), i.radix()).ok())
            .ok_or_else(|| {
                Error::with_message_and_status(
                    format!(
                        "invalid 'profile_version' in profile: {:?}",
                        raw_profile_version.as_ref()
                    ),
                    Status::InvalidArguments,
                )
            })?;

        if profile_version != 1 {
            return Err(Error::with_message_and_status(
                format!(
                    "unsupported profile version '{}', expected '1'",
                    profile_version
                ),
                Status::InvalidArguments,
            ));
        }

        let driver = profile
            .get_ref()
            .get("driver")
            .and_then(|v| v.get_ref().as_str())
            .ok_or_else(|| {
                Error::with_message_and_status(
                    "missing or invalid 'driver' field in profile".to_string(),
                    Status::InvalidArguments,
                )
            })?
            .to_string();

        let options_table = profile
            .get_ref()
            .get("Options")
            .and_then(|v| v.get_ref().as_table())
            .ok_or_else(|| {
                Error::with_message_and_status(
                    "missing or invalid 'Options' table in profile".to_string(),
                    Status::InvalidArguments,
                )
            })?;

        let mut opts = HashMap::new();
        process_options(&mut opts, "", options_table)?;

        Ok(FilesystemProfile {
            profile_path,
            driver,
            opts,
        })
    }
}

impl ConnectionProfile for FilesystemProfile {
    fn get_driver_name(&self) -> Result<(&str, Option<&FFI_AdbcDriverInitFunc>)> {
        Ok((&self.driver, None))
    }

    fn get_options(
        &self,
    ) -> Result<impl IntoIterator<Item = (<ManagedDatabase as Optionable>::Option, OptionValue)>>
    {
        Ok(self.opts.clone().into_iter())
    }
}

impl fmt::Display for FilesystemProfile {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "FilesystemProfile({})", self.profile_path.display())
    }
}

fn profile_value_regex() -> &'static Regex {
    static RE: OnceLock<Regex> = OnceLock::new();
    RE.get_or_init(|| Regex::new(r"\{\{\s*([^{}]*?)\s*\}\}").unwrap())
}

pub fn process_profile_value(value: &str) -> Result<OptionValue> {
    let re = profile_value_regex();

    let replacer = |caps: &Captures| -> Result<String> {
        let content = caps.get(1).unwrap().as_str();
        if !content.starts_with("env_var(") || !content.ends_with(")") {
            return Err(Error::with_message_and_status(
                format!(
                    "invalid profile replacement expression '{{{{ {} }}}}'",
                    content
                ),
                Status::InvalidArguments,
            ));
        }

        let env_var_name = content[8..content.len() - 1].trim();
        if env_var_name.is_empty() {
            return Err(Error::with_message_and_status(
                format!("empty environment variable name in profile replacement expression '{{{{ {} }}}}'", content),
                Status::InvalidArguments,
            ));
        }

        match env::var(env_var_name) {
            Ok(val) => Ok(val),
            Err(env::VarError::NotPresent) => Ok("".to_string()),
            Err(e) => Err(Error::with_message_and_status(
                format!("error retrieving environment variable '{}' for profile replacement expression '{{{{ {} }}}}': {}", env_var_name, content, e),
                Status::InvalidArguments,
            )),
        }
    };

    let mut new = String::with_capacity(value.len());
    let mut last_match = 0;
    for caps in re.captures_iter(value) {
        let m = caps.get(0).unwrap();
        new.push_str(&value[last_match..m.start()]);
        new.push_str(&replacer(&caps)?);
        last_match = m.end();
    }
    new.push_str(&value[last_match..]);
    Ok(OptionValue::String(new))
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::HashMap;
    use toml::de::DeTable;

    #[test]
    fn test_process_options_basic_types() {
        let test_cases = vec![
            (
                "string value",
                r#"key = "value""#,
                vec![("key", OptionValue::String("value".to_string()))],
            ),
            (
                "integer value",
                r#"port = 5432"#,
                vec![("port", OptionValue::Int(5432))],
            ),
            (
                "float value",
                r#"timeout = 30.5"#,
                vec![("timeout", OptionValue::Double(30.5))],
            ),
            (
                "boolean values",
                r#"enabled = true
disabled = false"#,
                vec![
                    ("enabled", OptionValue::String("true".to_string())),
                    ("disabled", OptionValue::String("false".to_string())),
                ],
            ),
            (
                "multiple types",
                r#"str = "text"
num = 42
flt = 1.5
flag = true"#,
                vec![
                    ("str", OptionValue::String("text".to_string())),
                    ("num", OptionValue::Int(42)),
                    ("flt", OptionValue::Double(1.5)),
                    ("flag", OptionValue::String("true".to_string())),
                ],
            ),
        ];

        for (name, toml_str, expected_opts) in test_cases {
            let table = DeTable::parse(toml_str).unwrap();
            let mut opts = HashMap::new();
            process_options(&mut opts, "", table.get_ref())
                .unwrap_or_else(|_| panic!("Failed to process options for test case: {}", name));

            assert_eq!(
                opts.len(),
                expected_opts.len(),
                "Test case '{}': expected {} options, got {}",
                name,
                expected_opts.len(),
                opts.len()
            );

            for (key_str, expected_value) in expected_opts {
                let key = OptionDatabase::from(key_str);
                let actual_value = opts
                    .get(&key)
                    .unwrap_or_else(|| panic!("Test case '{}': missing key '{}'", name, key_str));

                match (actual_value, &expected_value) {
                    (OptionValue::String(a), OptionValue::String(e)) => {
                        assert_eq!(
                            a, e,
                            "Test case '{}': string mismatch for key '{}'",
                            name, key_str
                        );
                    }
                    (OptionValue::Int(a), OptionValue::Int(e)) => {
                        assert_eq!(
                            a, e,
                            "Test case '{}': int mismatch for key '{}'",
                            name, key_str
                        );
                    }
                    (OptionValue::Double(a), OptionValue::Double(e)) => {
                        assert!(
                            (a - e).abs() < 1e-10,
                            "Test case '{}': float mismatch for key '{}'",
                            name,
                            key_str
                        );
                    }
                    _ => panic!("Test case '{}': type mismatch for key '{}'", name, key_str),
                }
            }
        }
    }

    #[test]
    fn test_process_options_nested_table() {
        let toml_str = r#"
key = "value"
[nested]
subkey = "subvalue"
number = 42
"#;
        let table = DeTable::parse(toml_str).unwrap();
        let mut opts = HashMap::new();
        process_options(&mut opts, "", table.get_ref()).unwrap();

        assert_eq!(opts.len(), 3);

        let key1 = OptionDatabase::from("key");
        match opts.get(&key1) {
            Some(OptionValue::String(s)) => assert_eq!(s, "value"),
            _ => panic!("Expected string value"),
        }

        let key2 = OptionDatabase::from("nested.subkey");
        match opts.get(&key2) {
            Some(OptionValue::String(s)) => assert_eq!(s, "subvalue"),
            _ => panic!("Expected string value for nested.subkey"),
        }

        let key3 = OptionDatabase::from("nested.number");
        match opts.get(&key3) {
            Some(OptionValue::Int(i)) => assert_eq!(*i, 42),
            _ => panic!("Expected int value for nested.number"),
        }
    }

    #[test]
    fn test_process_options_deeply_nested() {
        let toml_str = r#"
[level1]
[level1.level2]
[level1.level2.level3]
deep = "value"
"#;
        let table = DeTable::parse(toml_str).unwrap();
        let mut opts = HashMap::new();
        process_options(&mut opts, "", table.get_ref()).unwrap();

        assert_eq!(opts.len(), 1);

        let key = OptionDatabase::from("level1.level2.level3.deep");
        match opts.get(&key) {
            Some(OptionValue::String(s)) => assert_eq!(s, "value"),
            _ => panic!("Expected string value"),
        }
    }

    #[test]
    fn test_process_options_error_cases() {
        let test_cases = vec![
            (
                "invalid integer (too large)",
                r#"bad_int = 999999999999999999999999999999"#,
                "invalid integer value",
            ),
            (
                "unsupported type (array)",
                r#"array = [1, 2, 3]"#,
                "unsupported option type",
            ),
        ];

        for (name, toml_str, expected_error_msg) in test_cases {
            let table = DeTable::parse(toml_str).unwrap();
            let mut opts = HashMap::new();
            let result = process_options(&mut opts, "", table.get_ref());

            assert!(
                result.is_err(),
                "Test case '{}': expected error but got Ok",
                name
            );
            let err = result.unwrap_err();
            assert_eq!(
                err.status,
                Status::InvalidArguments,
                "Test case '{}': wrong status",
                name
            );
            assert!(
                err.message.contains(expected_error_msg),
                "Test case '{}': expected '{}' in error message, got '{}'",
                name,
                expected_error_msg,
                err.message
            );
        }
    }

    #[test]
    fn test_filesystem_profile_from_path_errors() {
        let test_cases = vec![
            (
                "missing file",
                "/nonexistent/path/to/profile.toml",
                None,
                "could not read profile",
            ),
            (
                "invalid version (too high)",
                "invalid_version_high.toml",
                Some(
                    r#"
profile_version = 99
driver = "test_driver"

[Options]
key = "value"
"#,
                ),
                "unsupported profile version '99', expected '1'",
            ),
            (
                "version 0",
                "version_zero.toml",
                Some(
                    r#"
profile_version = 0
driver = "test_driver"

[Options]
key = "value"
"#,
                ),
                "unsupported profile version '0', expected '1'",
            ),
            (
                "version 2",
                "version_two.toml",
                Some(
                    r#"
profile_version = 2
driver = "test_driver"

[Options]
key = "value"
"#,
                ),
                "unsupported profile version '2', expected '1'",
            ),
        ];

        for (name, filename, content_opt, expected_error_msg) in test_cases {
            let profile_path = if let Some(content) = content_opt {
                let tmp_dir = tempfile::Builder::new()
                    .prefix("adbc_profile_test")
                    .tempdir()
                    .unwrap();
                let path = tmp_dir.path().join(filename);
                std::fs::write(&path, content).unwrap();
                // Keep tmp_dir alive until after the test
                let result = FilesystemProfile::from_path(path.clone());

                assert!(
                    result.is_err(),
                    "Test case '{}': expected error but got Ok",
                    name
                );
                let err = result.unwrap_err();
                assert_eq!(
                    err.status,
                    Status::InvalidArguments,
                    "Test case '{}': wrong status",
                    name
                );
                assert!(
                    err.message.contains(expected_error_msg),
                    "Test case '{}': expected '{}' in error message, got '{}'",
                    name,
                    expected_error_msg,
                    err.message
                );

                tmp_dir.close().unwrap();
                continue;
            } else {
                PathBuf::from(filename)
            };

            let result = FilesystemProfile::from_path(profile_path);

            assert!(
                result.is_err(),
                "Test case '{}': expected error but got Ok",
                name
            );
            let err = result.unwrap_err();
            assert_eq!(
                err.status,
                Status::InvalidArguments,
                "Test case '{}': wrong status",
                name
            );
            assert!(
                err.message.contains(expected_error_msg),
                "Test case '{}': expected '{}' in error message, got '{}'",
                name,
                expected_error_msg,
                err.message
            );
        }
    }

    #[test]
    fn test_process_profile_value() {
        // (name, env_vars_to_set, input, expected_ok / expected_err_fragment)
        struct TestCase<'a>(
            &'a str,
            Vec<(&'a str, &'a str)>,
            &'a str,
            std::result::Result<&'a str, &'a str>,
        );

        let test_cases: Vec<TestCase> = vec![
            TestCase("empty string", vec![], "", Ok("")),
            TestCase(
                "plain string no templates",
                vec![],
                "just a plain string",
                Ok("just a plain string"),
            ),
            TestCase(
                "not actually a substitution",
                vec![],
                "{{ env_var(NONEXISTENT)",
                Ok("{{ env_var(NONEXISTENT)"),
            ),
            TestCase(
                "not actually a substitution (2)",
                vec![],
                "{{ env_var(NONEXISTENT) }",
                Ok("{{ env_var(NONEXISTENT) }"),
            ),
            TestCase(
                "not actually a substitution (3)",
                vec![],
                "{ env_var(NONEXISTENT) }",
                Ok("{ env_var(NONEXISTENT) }"),
            ),
            TestCase(
                "string with special chars but no templates",
                vec![],
                "host=localhost port=5432",
                Ok("host=localhost port=5432"),
            ),
            TestCase(
                "env var present",
                vec![("ADBC_TEST_PPV_HOST", "myhost.example.com")],
                "{{ env_var(ADBC_TEST_PPV_HOST) }}",
                Ok("myhost.example.com"),
            ),
            TestCase(
                "env var not set returns empty string",
                vec![],
                "{{ env_var(ADBC_TEST_PPV_NONEXISTENT_XYZ) }}",
                Ok(""),
            ),
            TestCase(
                "env var not set interpolates the empty string",
                vec![],
                "foo{{ env_var(ADBC_TEST_PPV_NONEXISTENT_XYZ) }}bar",
                Ok("foobar"),
            ),
            TestCase(
                "env var not set interpolates the empty string (2)",
                vec![],
                "foo{{ env_var(ADBC_TEST_PPV_NONEXISTENT_XYZ) }}",
                Ok("foo"),
            ),
            TestCase(
                "env var not set interpolates the empty string (3)",
                vec![],
                "{{ env_var(ADBC_TEST_PPV_NONEXISTENT_XYZ) }}bar",
                Ok("bar"),
            ),
            TestCase(
                "env var not set interpolates the empty string (4)",
                vec![],
                "foo{{ env_var(ADBC_TEST_PPV_NONEXISTENT_XYZ) }}bar{{ env_var(ADBC_TEST_PPV_NONEXISTENT_XYZ2) }}baz",
                Ok("foobarbaz"),
            ),
            TestCase(
                "env var not set interpolates the empty string (5)",
                vec![],
                "{{ env_var(ADBC_TEST_PPV_NONEXISTENT_XYZ) }}foobarbaz{{ env_var(ADBC_TEST_PPV_NONEXISTENT_XYZ2) }}",
                Ok("foobarbaz"),
            ),
            TestCase(
                "mixed literal text and env var",
                vec![("ADBC_TEST_PPV_PORT", "5432")],
                "host=localhost port={{ env_var(ADBC_TEST_PPV_PORT) }}",
                Ok("host=localhost port=5432"),
            ),
            TestCase(
                "multiple env var replacements",
                vec![
                    ("ADBC_TEST_PPV_USER", "alice"),
                    ("ADBC_TEST_PPV_PASS", "secret"),
                ],
                "{{ env_var(ADBC_TEST_PPV_USER) }}:{{ env_var(ADBC_TEST_PPV_PASS) }}",
                Ok("alice:secret"),
            ),
            TestCase(
                "extra whitespace inside braces",
                vec![("ADBC_TEST_PPV_DB", "mydb")],
                "{{  env_var(ADBC_TEST_PPV_DB)  }}",
                Ok("mydb"),
            ),
            TestCase(
                "no whitespace inside braces",
                vec![("ADBC_TEST_PPV_DB", "mydb")],
                "{{env_var(ADBC_TEST_PPV_DB)}}",
                Ok("mydb"),
            ),
            TestCase(
                "invalid expression not env_var",
                vec![],
                "{{ something_invalid }}",
                Err("invalid profile replacement expression"),
            ),
            TestCase(
                "empty env var name",
                vec![],
                "{{ env_var() }}",
                Err("empty environment variable name"),
            ),
            TestCase(
                "empty env var name with whitespace",
                vec![],
                "{{ env_var(   ) }}",
                Err("empty environment variable name"),
            ),
        ];

        for TestCase(name, env_vars, input, expected) in test_cases {
            for (k, v) in &env_vars {
                std::env::set_var(k, v);
            }

            let result = process_profile_value(input);

            match expected {
                Ok(expected_str) => match result.unwrap_or_else(|e| {
                    panic!("Test case '{}': expected Ok but got Err: {:?}", name, e)
                }) {
                    OptionValue::String(s) => {
                        assert_eq!(s, expected_str, "Test case '{}': string mismatch", name)
                    }
                    other => panic!(
                        "Test case '{}': expected OptionValue::String, got {:?}",
                        name, other
                    ),
                },
                Err(err_fragment) => {
                    assert!(
                        result.is_err(),
                        "Test case '{}': expected Err but got Ok",
                        name
                    );
                    let err = result.unwrap_err();
                    assert_eq!(
                        err.status,
                        Status::InvalidArguments,
                        "Test case '{}': wrong status",
                        name
                    );
                    assert!(
                        err.message.contains(err_fragment),
                        "Test case '{}': expected {:?} in error message, got {:?}",
                        name,
                        err_fragment,
                        err.message
                    );
                }
            }

            for (k, _) in &env_vars {
                std::env::remove_var(k);
            }
        }
    }

    #[test]
    fn test_filesystem_profile_provider() {
        let profile_content = r#"
profile_version = 1
driver = "test_driver"

[Options]
test_key = "test_value"
"#;

        let test_cases = vec![
            ("absolute path", "absolute_test.toml", None, true),
            (
                "search path with name only",
                "search_test",
                Some(vec![]),
                true,
            ),
            (
                "search path with .toml extension",
                "search_test.toml",
                Some(vec![]),
                true,
            ),
        ];

        for (name, profile_name, search_paths_opt, should_succeed) in test_cases {
            let tmp_dir = tempfile::Builder::new()
                .prefix("adbc_profile_test")
                .tempdir()
                .unwrap();

            let profile_path = tmp_dir.path().join(if profile_name.ends_with(".toml") {
                profile_name.to_string()
            } else {
                format!("{}.toml", profile_name)
            });
            std::fs::write(&profile_path, profile_content).unwrap();

            let search_paths = search_paths_opt.map(|mut paths| {
                paths.push(tmp_dir.path().to_path_buf());
                paths
            });
            let provider = FilesystemProfileProvider::new_with_search_paths(search_paths);

            let profile_arg = if name.contains("absolute") {
                profile_path.to_str().unwrap().to_string()
            } else {
                profile_name.to_string()
            };

            let result = provider.get_profile(&profile_arg);

            if should_succeed {
                let profile =
                    result.unwrap_or_else(|e| panic!("Test case '{}' failed: {:?}", name, e));
                let (driver, _) = profile.get_driver_name().unwrap();
                assert_eq!(
                    driver, "test_driver",
                    "Test case '{}': driver mismatch",
                    name
                );
            } else {
                assert!(result.is_err(), "Test case '{}': expected error", name);
            }

            tmp_dir.close().unwrap();
        }
    }
}
