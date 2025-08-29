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

//! Load and use ADBC drivers.
//!
//! The driver manager provides an implementation of the ADBC interface which
//! uses FFI to wrap an object file implementation of
//! [`adbc.h`](https://github.com/apache/arrow-adbc/blob/main/c/include/arrow-adbc/adbc.h).
//!
//! There are two ways that drivers can be used:
//! 1. By linking (either statically or dynamically) the driver implementation
//!    at link-time and then using [ManagedDriver::load_static].
//! 2. By loading the driver implementation at run-time (with `dlopen/LoadLibrary`)
//!    using [ManagedDriver::load_dynamic_from_name] or
//!    [ManagedDriver::load_dynamic_from_filename].
//!
//! Drivers are initialized using a function provided by the driver as a main
//! entrypoint, canonically called `AdbcDriverInit`. Although many will use a
//! different name to support statically linking multiple drivers within the
//! same program.
//!
//! ## Using across threads
//!
//! [ManagedDriver], [ManagedDatabase], [ManagedConnection] and [ManagedStatement]
//! can be used across threads though all of their operations are serialized
//! under the hood. They hold their inner implementations within [std::sync::Arc],
//! so they are cheaply clonable.
//!
//! ## Example
//!
//! ```rust
//! # use std::sync::Arc;
//! # use arrow_array::{Array, StringArray, Int64Array, Float64Array};
//! # use arrow_array::{RecordBatch, RecordBatchReader};
//! # use arrow_schema::{Field, Schema, DataType};
//! # use arrow_select::concat::concat_batches;
//! # use adbc_core::{
//! #     options::{AdbcVersion, OptionDatabase, OptionStatement},
//! #     Connection, Database, Driver, Statement, Optionable
//! # };
//! # use adbc_driver_manager::ManagedDriver;
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let opts = [(OptionDatabase::Uri, ":memory:".into())];
//! let mut driver = ManagedDriver::load_dynamic_from_name("adbc_driver_sqlite", None, AdbcVersion::V100)?;
//! let database = driver.new_database_with_opts(opts)?;
//! let mut connection = database.new_connection()?;
//! let mut statement = connection.new_statement()?;
//!
//! // Define some data.
//! let columns: Vec<Arc<dyn Array>> = vec![
//!     Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
//!     Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
//!     Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
//! ];
//! let schema = Schema::new(vec![
//!     Field::new("a", DataType::Int64, true),
//!     Field::new("b", DataType::Float64, true),
//!     Field::new("c", DataType::Utf8, true),
//! ]);
//! let input: RecordBatch = RecordBatch::try_new(Arc::new(schema), columns)?;
//!
//! // Ingest data.
//! statement.set_option(OptionStatement::TargetTable, "my_table".into())?;
//! statement.bind(input.clone())?;
//! statement.execute_update()?;
//!
//! // Extract data.
//! statement.set_sql_query("select * from my_table")?;
//! let output = statement.execute()?;
//! let schema = output.schema();
//! let output: Result<Vec<RecordBatch>, _> = output.collect();
//! let output = concat_batches(&schema, &output?)?;
//! assert_eq!(input, output);
//!
//! # Ok(())
//! # }
//! ```

// According to the ADBC specification, objects allow serialized access from
// multiple threads: one thread may make a call, and once finished, another
// thread may make a call. They do not allow concurrent access from multiple
// threads.
//
// In order to implement these semantics, all mutable FFI objects are wrapped
// in `Mutex`es. `FFI_Driver` is not wrapped in a `Mutex` because it is
// an immutable struct of function pointers. Wrapping the driver in a `Mutex`
// would prevent any parallelism between driver calls, which is not desirable.

pub mod error;

use std::collections::HashSet;
use std::env;
use std::ffi::{CStr, CString, OsStr};
use std::fs;
use std::ops::DerefMut;
use std::os::raw::{c_char, c_void};
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::ptr::{null, null_mut};
use std::sync::{Arc, Mutex};

use arrow_array::ffi::{to_ffi, FFI_ArrowSchema};
use arrow_array::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{Array, RecordBatch, RecordBatchReader, StructArray};
use toml::de::DeTable;

use adbc_core::{
    error::{Error, Result, Status},
    options::{self, AdbcVersion, InfoCode, OptionValue},
    LoadFlags, PartitionedResult, LOAD_FLAG_ALLOW_RELATIVE_PATHS, LOAD_FLAG_SEARCH_ENV,
    LOAD_FLAG_SEARCH_SYSTEM, LOAD_FLAG_SEARCH_USER,
};
use adbc_core::{ffi, ffi::driver_method, Optionable};
use adbc_core::{Connection, Database, Driver, Statement};

use crate::error::libloading_error_to_adbc_error;

const ERR_ONLY_STRING_OPT: &str = "Only string option value are supported with ADBC 1.0.0";
const ERR_CANCEL_UNSUPPORTED: &str =
    "Canceling connection or statement is not supported with ADBC 1.0.0";
const ERR_STATISTICS_UNSUPPORTED: &str = "Statistics are not supported with ADBC 1.0.0";

fn check_status(status: ffi::FFI_AdbcStatusCode, error: ffi::FFI_AdbcError) -> Result<()> {
    match status {
        ffi::constants::ADBC_STATUS_OK => Ok(()),
        _ => {
            let mut error: Error = error.try_into()?;
            error.status = status.try_into()?;
            Err(error)
        }
    }
}

#[derive(Debug)]
struct ManagedDriverInner {
    driver: ffi::FFI_AdbcDriver,
    version: AdbcVersion, // Driver version
    // The dynamic library must be kept loaded for the entire lifetime of the driver.
    // To avoid complex lifetimes we prefer to store it as part of this struct.
    // Besides, the `library` field must always appear after `driver` because of drop order:
    // `driver` has an implicit dependency on `library` and so it must be dropped
    // before `library` because otherwise `driver` would be full of dangling
    // function pointers.
    // See: https://doc.rust-lang.org/std/ops/trait.Drop.html#drop-order
    _library: Option<libloading::Library>,
}

#[derive(Default)]
struct DriverInfo {
    lib_path: std::path::PathBuf,
    entrypoint: Option<Vec<u8>>,
    // TODO: until we add more logging these are unused so we'll leave
    // them out until such time that we do so.
    // driver_name: String,
    // version: String,
    // source: String,
}

impl DriverInfo {
    fn load_driver_manifest(manifest_file: &Path, entrypoint: Option<&[u8]>) -> Result<Self> {
        let contents = fs::read_to_string(manifest_file).map_err(|e| {
            Error::with_message_and_status(
                format!("Could not read manifest '{}': {e}", manifest_file.display()),
                Status::InvalidArguments,
            )
        })?;

        let manifest = DeTable::parse(&contents)
            .map_err(|e| Error::with_message_and_status(e.to_string(), Status::InvalidArguments))?;

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

/// Implementation of [Driver].
#[derive(Clone, Debug)]
pub struct ManagedDriver {
    inner: Pin<Arc<ManagedDriverInner>>,
}

impl ManagedDriver {
    /// Returns the [`AdbcVersion`] of this driver.
    pub fn version(&self) -> AdbcVersion {
        self.inner.version
    }

    /// Load a driver from an initialization function.
    pub fn load_static(init: &ffi::FFI_AdbcDriverInitFunc, version: AdbcVersion) -> Result<Self> {
        let driver = Self::load_impl(init, version)?;
        let inner = Arc::pin(ManagedDriverInner {
            driver,
            version,
            _library: None,
        });
        Ok(ManagedDriver { inner })
    }

    /// Load a driver either by name, filename, path, or via locating a toml manifest file.
    /// The `load_flags` control what directories are searched to locate a manifest.
    /// The `entrypoint` allows customizing the name of the driver initialization function
    /// if it is not the default `AdbcDriverInit` and isn't described in the loaded manifest.
    /// If not provided, an entrypoint will be searched for based on the driver's name.
    /// The `version` defines the ADBC revision to attempt to initialize.
    ///
    /// The full logic used here is as follows:
    ///  - if `name` has an extension: it is treated as a filename. If the load_flags does not
    ///    contain `LOAD_FLAG_ALLOW_RELATIVE_PATHS`, then relative paths will be rejected.
    ///    - if the extension is `toml` then we attempt to load the Driver Manifest, otherwise
    ///      we defer to the previous logic in [`Self::load_dynamic_from_filename`] which will attempt to
    ///      load the library
    ///  - if `name` does not have an extension but is an absolute path: we first check to see
    ///    if there is an existing file with the same name that *does* have a "toml" extension,
    ///    attempting to load that if it exists. Otherwise we just pass it to load_dynamic_from_filename.
    ///  - Finally, if there's no extension and it is not an absolute path, we will search through
    ///    the relevant directories (based on the set load flags) for a manifest file with this name, and
    ///    if one is not found we see if the name refers to a library on the LD_LIBRARY_PATH etc.
    pub fn load_from_name(
        name: impl AsRef<OsStr>,
        entrypoint: Option<&[u8]>,
        version: AdbcVersion,
        load_flags: LoadFlags,
        additional_search_paths: Option<Vec<PathBuf>>,
    ) -> Result<Self> {
        let driver_path = Path::new(name.as_ref());
        let allow_relative = load_flags & LOAD_FLAG_ALLOW_RELATIVE_PATHS != 0;
        if let Some(ext) = driver_path.extension() {
            if !allow_relative && driver_path.is_relative() {
                return Err(Error::with_message_and_status(
                    "Relative paths are not allowed",
                    Status::InvalidArguments,
                ));
            }

            if ext == "toml" {
                Self::load_from_manifest(driver_path, entrypoint, version)
            } else {
                Self::load_dynamic_from_filename(driver_path, entrypoint, version)
            }
        } else if driver_path.is_absolute() {
            let toml_path = driver_path.with_extension("toml");
            if toml_path.is_file() {
                return Self::load_from_manifest(&toml_path, entrypoint, version);
            }

            Self::load_dynamic_from_filename(driver_path, entrypoint, version)
        } else {
            Self::find_driver(
                driver_path,
                entrypoint,
                version,
                load_flags,
                additional_search_paths,
            )
        }
    }

    /// Load a driver from a dynamic library filename.
    ///
    /// Will attempt to load the dynamic library located at `filename`, find the
    /// symbol with name `entrypoint` (defaults to `AdbcDriverInit` if `None`),
    /// and then create the driver using the resolved function.
    ///
    /// The `filename` argument may be either:
    /// - A library filename;
    /// - The absolute path to the library;
    /// - A relative (to the current working directory) path to the library.
    pub fn load_dynamic_from_filename(
        filename: impl AsRef<OsStr>,
        entrypoint: Option<&[u8]>,
        version: AdbcVersion,
    ) -> Result<Self> {
        let default_entrypoint = get_default_entrypoint(filename.as_ref());

        let entrypoint = entrypoint.unwrap_or(default_entrypoint.as_bytes());
        let library = unsafe {
            libloading::Library::new(filename.as_ref()).map_err(libloading_error_to_adbc_error)?
        };
        let init: libloading::Symbol<ffi::FFI_AdbcDriverInitFunc> = unsafe {
            library
                .get(entrypoint)
                .or_else(|_| library.get(b"AdbcDriverInit"))
                .map_err(libloading_error_to_adbc_error)?
        };
        let driver = Self::load_impl(&init, version)?;
        let inner = Arc::pin(ManagedDriverInner {
            driver,
            version,
            _library: Some(library),
        });
        Ok(ManagedDriver { inner })
    }

    /// Load a driver from a dynamic library name.
    ///
    /// Will attempt to load the dynamic library with the given `name`, find the
    /// symbol with name `entrypoint` (defaults to `AdbcDriverInit` if `None`),
    /// and then create the driver using the resolved function.
    ///
    /// The `name` should not include any platform-specific prefixes or suffixes.
    /// For example, use `adbc_driver_sqlite` rather than `libadbc_driver_sqlite.so`.
    pub fn load_dynamic_from_name(
        name: impl AsRef<str>,
        entrypoint: Option<&[u8]>,
        version: AdbcVersion,
    ) -> Result<Self> {
        let filename = libloading::library_filename(name.as_ref());
        Self::load_dynamic_from_filename(filename, entrypoint, version)
    }

    fn load_impl(
        init: &ffi::FFI_AdbcDriverInitFunc,
        version: AdbcVersion,
    ) -> Result<ffi::FFI_AdbcDriver> {
        let mut error = ffi::FFI_AdbcError::default();
        let mut driver = ffi::FFI_AdbcDriver::default();
        let status = unsafe {
            init(
                version.into(),
                &mut driver as *mut ffi::FFI_AdbcDriver as *mut c_void,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok(driver)
    }

    fn inner_ffi_driver(&self) -> &ffi::FFI_AdbcDriver {
        &self.inner.driver
    }

    /// Returns a new database using the loaded driver.
    fn database_new(&self) -> Result<ffi::FFI_AdbcDatabase> {
        let driver = self.inner_ffi_driver();
        let mut database = ffi::FFI_AdbcDatabase::default();

        // DatabaseNew
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(*driver, DatabaseNew);
        let status = unsafe { method(&mut database, &mut error) };
        check_status(status, error)?;

        Ok(database)
    }

    /// Initialize the given database using the loaded driver.
    fn database_init(&self, mut database: ffi::FFI_AdbcDatabase) -> Result<ffi::FFI_AdbcDatabase> {
        let driver = self.inner_ffi_driver();

        // DatabaseInit
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, DatabaseInit);
        let status = unsafe { method(&mut database, &mut error) };
        check_status(status, error)?;

        Ok(database)
    }

    fn load_from_manifest(
        driver_path: &Path,
        entrypoint: Option<&[u8]>,
        version: AdbcVersion,
    ) -> Result<Self> {
        let info = DriverInfo::load_driver_manifest(driver_path, entrypoint)?;
        Self::load_dynamic_from_filename(info.lib_path, info.entrypoint.as_deref(), version)
    }

    fn search_path_list(
        driver_path: &Path,
        path_list: Vec<PathBuf>,
        entrypoint: Option<&[u8]>,
        version: AdbcVersion,
    ) -> Result<Self> {
        for path in path_list {
            let mut full_path = path.join(driver_path);
            full_path.set_extension("toml");
            if full_path.is_file() {
                if let Ok(result) = Self::load_from_manifest(&full_path, entrypoint, version) {
                    return Ok(result);
                }
            }

            full_path.set_extension(""); // Remove the extension to try loading as a dynamic library.
            if let Ok(result) = Self::load_dynamic_from_filename(full_path, entrypoint, version) {
                return Ok(result);
            }
        }

        Err(Error::with_message_and_status(
            format!("Driver not found: {}", driver_path.display()),
            Status::NotFound,
        ))
    }

    #[cfg(target_os = "windows")]
    fn find_driver(
        driver_path: &Path,
        entrypoint: Option<&[u8]>,
        version: AdbcVersion,
        load_flags: LoadFlags,
        additional_search_paths: Option<Vec<PathBuf>>,
    ) -> Result<Self> {
        if load_flags & LOAD_FLAG_SEARCH_ENV != 0 {
            if let Ok(result) = Self::search_path_list(
                driver_path,
                get_search_paths(LOAD_FLAG_SEARCH_ENV),
                entrypoint,
                version,
            ) {
                return Ok(result);
            }
        }

        // the logic we want is that we first search ADBC_CONFIG_PATH if set,
        // then we search the additional search paths if they exist. Finally,
        // we will search CONDA_PREFIX if built with conda_build before moving on.
        if let Some(additional_search_paths) = additional_search_paths {
            if let Ok(result) =
                Self::search_path_list(driver_path, additional_search_paths, entrypoint, version)
            {
                return Ok(result);
            }
        }

        #[cfg(conda_build)]
        if load_flags & LOAD_FLAG_SEARCH_ENV != 0 {
            if let Some(conda_prefix) = env::var_os("CONDA_PREFIX") {
                let conda_path = PathBuf::from(conda_prefix)
                    .join("etc")
                    .join("adbc")
                    .join("drivers");
                if let Ok(result) =
                    Self::search_path_list(driver_path, vec![conda_path], entrypoint, version)
                {
                    return Ok(result);
                }
            }
        }

        if load_flags & LOAD_FLAG_SEARCH_USER != 0 {
            // first check registry for the driver, then check the user config path
            if let Ok(result) = load_driver_from_registry(
                windows_registry::CURRENT_USER,
                driver_path.as_os_str(),
                entrypoint,
            ) {
                return Self::load_dynamic_from_filename(result.lib_path, entrypoint, version);
            }

            if let Ok(result) = Self::search_path_list(
                driver_path,
                get_search_paths(LOAD_FLAG_SEARCH_USER),
                entrypoint,
                version,
            ) {
                return Ok(result);
            }
        }

        if load_flags & LOAD_FLAG_SEARCH_SYSTEM != 0 {
            if let Ok(result) = load_driver_from_registry(
                windows_registry::LOCAL_MACHINE,
                driver_path.as_os_str(),
                entrypoint,
            ) {
                return Self::load_dynamic_from_filename(result.lib_path, entrypoint, version);
            }

            if let Ok(result) = Self::search_path_list(
                driver_path,
                get_search_paths(LOAD_FLAG_SEARCH_SYSTEM),
                entrypoint,
                version,
            ) {
                return Ok(result);
            }
        }

        let driver_name = driver_path.as_os_str().to_string_lossy().into_owned();
        Self::load_dynamic_from_name(driver_name, entrypoint, version)
    }

    #[cfg(not(windows))]
    fn find_driver(
        driver_path: &Path,
        entrypoint: Option<&[u8]>,
        version: AdbcVersion,
        load_flags: LoadFlags,
        additional_search_paths: Option<Vec<PathBuf>>,
    ) -> Result<Self> {
        let mut path_list = get_search_paths(load_flags & LOAD_FLAG_SEARCH_ENV);

        if let Some(additional_search_paths) = additional_search_paths {
            path_list.extend(additional_search_paths);
        }

        #[cfg(conda_build)]
        if load_flags & LOAD_FLAG_SEARCH_ENV != 0 {
            if let Some(conda_prefix) = env::var_os("CONDA_PREFIX") {
                let conda_path = PathBuf::from(conda_prefix)
                    .join("etc")
                    .join("adbc")
                    .join("drivers");
                path_list.push(conda_path);
            }
        }

        path_list.extend(get_search_paths(load_flags & !LOAD_FLAG_SEARCH_ENV));
        if let Ok(result) = Self::search_path_list(driver_path, path_list, entrypoint, version) {
            return Ok(result);
        }

        // Convert OsStr to String before passing to load_dynamic_from_name
        let driver_name = driver_path.as_os_str().to_string_lossy().into_owned();
        if let Ok(driver) = Self::load_dynamic_from_name(driver_name, entrypoint, version) {
            return Ok(driver);
        }

        Err(Error::with_message_and_status(
            format!("Driver not found: {}", driver_path.display()),
            Status::NotFound,
        ))
    }
}

impl Driver for ManagedDriver {
    type DatabaseType = ManagedDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        // Construct a new database.
        let database = self.database_new()?;
        // Initialize the database.
        let database = self.database_init(database)?;
        let inner = Arc::new(ManagedDatabaseInner {
            database: Mutex::new(database),
            driver: self.inner.clone(),
        });
        Ok(Self::DatabaseType { inner })
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (<Self::DatabaseType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        // Construct a new database.
        let mut database = self.database_new()?;
        // Set the options.
        {
            let driver = self.inner_ffi_driver();
            for (key, value) in opts {
                set_option_database(driver, &mut database, self.inner.version, key, value)?;
            }
        }
        // Initialize the database.
        let database = self.database_init(database)?;
        let inner = Arc::new(ManagedDatabaseInner {
            database: Mutex::new(database),
            driver: self.inner.clone(),
        });
        Ok(Self::DatabaseType { inner })
    }
}

#[cfg(target_os = "windows")]
fn load_driver_from_registry(
    root: &windows_registry::Key,
    driver_name: &OsStr,
    entrypoint: Option<&[u8]>,
) -> Result<DriverInfo> {
    const ADBC_DRIVER_REGISTRY: &str = "SOFTWARE\\ADBC\\Drivers";
    let drivers_key = root
        .open(ADBC_DRIVER_REGISTRY)
        .and_then(|k| k.open(driver_name.to_str().unwrap_or_default()))
        .map_err(|e| {
            Error::with_message_and_status(
                format!("Failed to open registry key: {e}"),
                Status::NotFound,
            )
        })?;

    let entrypoint_val = drivers_key
        .get_string("entrypoint")
        .ok()
        .map(|s| s.into_bytes());

    Ok(DriverInfo {
        lib_path: PathBuf::from(drivers_key.get_string("driver").unwrap_or_default()),
        entrypoint: entrypoint_val.or_else(|| entrypoint.map(|s| s.to_vec())),
    })
}

// construct default entrypoint from the library name
fn get_default_entrypoint(driver_path: impl AsRef<OsStr>) -> String {
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

fn set_option_database(
    driver: &ffi::FFI_AdbcDriver,
    database: &mut ffi::FFI_AdbcDatabase,
    version: AdbcVersion,
    key: impl AsRef<str>,
    value: OptionValue,
) -> Result<()> {
    let key = CString::new(key.as_ref())?;
    let mut error = ffi::FFI_AdbcError::with_driver(driver);
    #[allow(unknown_lints)]
    #[warn(non_exhaustive_omitted_patterns)]
    let status = match (version, value) {
        (_, OptionValue::String(value)) => {
            let value = CString::new(value)?;
            let method = driver_method!(driver, DatabaseSetOption);
            unsafe { method(database, key.as_ptr(), value.as_ptr(), &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Bytes(value)) => {
            let method = driver_method!(driver, DatabaseSetOptionBytes);
            unsafe {
                method(
                    database,
                    key.as_ptr(),
                    value.as_ptr(),
                    value.len(),
                    &mut error,
                )
            }
        }
        (AdbcVersion::V110, OptionValue::Int(value)) => {
            let method = driver_method!(driver, DatabaseSetOptionInt);
            unsafe { method(database, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Double(value)) => {
            let method = driver_method!(driver, DatabaseSetOptionDouble);
            unsafe { method(database, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V100, _) => Err(Error::with_message_and_status(
            ERR_ONLY_STRING_OPT,
            Status::NotImplemented,
        ))?,
        (_, _) => unreachable!(),
    };
    check_status(status, error)
}

// Utility function to implement `*GetOption` and `*GetOptionBytes`. Basically,
// it allocates a fixed-sized buffer to store the option's value, call the driver's
// `*GetOption`/`*GetOptionBytes` method that will fill this buffer and finally
// we return the option's value as a `Vec`. Note that if the fixed-size buffer
// is too small, we retry the same operation with a bigger buffer (the size of
// which is obtained via the out parameter `length` of `*GetOption`/`*GetOptionBytes`).
fn get_option_buffer<F, T>(
    key: impl AsRef<str>,
    mut populate: F,
    driver: &ffi::FFI_AdbcDriver,
) -> Result<Vec<T>>
where
    F: FnMut(*const c_char, *mut T, *mut usize, *mut ffi::FFI_AdbcError) -> ffi::FFI_AdbcStatusCode,
    T: Default + Clone,
{
    const DEFAULT_LENGTH: usize = 128;
    let key = CString::new(key.as_ref())?;
    let mut run = |length| {
        let mut value = vec![T::default(); length];
        let mut length: usize = core::mem::size_of::<T>() * value.len();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        (
            populate(key.as_ptr(), value.as_mut_ptr(), &mut length, &mut error),
            length,
            value,
            error,
        )
    };

    let (status, length, value, error) = run(DEFAULT_LENGTH);
    check_status(status, error)?;

    if length <= DEFAULT_LENGTH {
        Ok(value[..length].to_vec())
    } else {
        let (status, _, value, error) = run(length);
        check_status(status, error)?;
        Ok(value)
    }
}

fn get_option_bytes<F>(
    key: impl AsRef<str>,
    populate: F,
    driver: &ffi::FFI_AdbcDriver,
) -> Result<Vec<u8>>
where
    F: FnMut(
        *const c_char,
        *mut u8,
        *mut usize,
        *mut ffi::FFI_AdbcError,
    ) -> ffi::FFI_AdbcStatusCode,
{
    get_option_buffer(key, populate, driver)
}

fn get_option_string<F>(
    key: impl AsRef<str>,
    populate: F,
    driver: &ffi::FFI_AdbcDriver,
) -> Result<String>
where
    F: FnMut(
        *const c_char,
        *mut c_char,
        *mut usize,
        *mut ffi::FFI_AdbcError,
    ) -> ffi::FFI_AdbcStatusCode,
{
    let value = get_option_buffer(key, populate, driver)?;
    let value = unsafe { CStr::from_ptr(value.as_ptr()) };
    Ok(value.to_string_lossy().to_string())
}

struct ManagedDatabaseInner {
    database: Mutex<ffi::FFI_AdbcDatabase>,
    driver: Pin<Arc<ManagedDriverInner>>,
}

impl Drop for ManagedDatabaseInner {
    fn drop(&mut self) {
        let driver = &self.driver.driver;
        let mut database = self.database.lock().unwrap();
        let method = driver_method!(driver, DatabaseRelease);
        // TODO(alexandreyc): how should we handle `DatabaseRelease` failing?
        // See: https://github.com/apache/arrow-adbc/pull/1742#discussion_r1574388409
        unsafe { method(database.deref_mut(), null_mut()) };
    }
}

/// Implementation of [Database].
#[derive(Clone)]
pub struct ManagedDatabase {
    inner: Arc<ManagedDatabaseInner>,
}

impl ManagedDatabase {
    fn ffi_driver(&self) -> &ffi::FFI_AdbcDriver {
        &self.inner.driver.driver
    }

    fn driver_version(&self) -> AdbcVersion {
        self.inner.driver.version
    }

    /// Returns a new connection using the loaded driver.
    fn connection_new(&self) -> Result<ffi::FFI_AdbcConnection> {
        let driver = self.ffi_driver();
        let mut connection = ffi::FFI_AdbcConnection::default();

        // ConnectionNew
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(*driver, ConnectionNew);
        let status = unsafe { method(&mut connection, &mut error) };
        check_status(status, error)?;

        Ok(connection)
    }

    /// Initialize the given connection using the loaded driver.
    fn connection_init(
        &self,
        mut connection: ffi::FFI_AdbcConnection,
    ) -> Result<ffi::FFI_AdbcConnection> {
        let driver = self.ffi_driver();
        let mut database = self.inner.database.lock().unwrap();

        // ConnectionInit
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionInit);
        let status = unsafe { method(&mut connection, &mut *database, &mut error) };
        check_status(status, error)?;

        Ok(connection)
    }
}

impl Optionable for ManagedDatabase {
    type Option = options::OptionDatabase;

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        let driver = self.ffi_driver();
        let database = &mut self.inner.database.lock().unwrap();
        let method = driver_method!(driver, DatabaseGetOptionBytes);
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(database.deref_mut(), key, value, length, error)
        };
        get_option_bytes(key, populate, driver)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        let driver = self.ffi_driver();
        let mut database = self.inner.database.lock().unwrap();
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let mut value: f64 = f64::default();
        let method = driver_method!(driver, DatabaseGetOptionDouble);
        let status = unsafe { method(database.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        let driver = self.ffi_driver();
        let mut database = self.inner.database.lock().unwrap();
        let key = CString::new(key.as_ref())?;
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let mut value: i64 = 0;
        let method = driver_method!(driver, DatabaseGetOptionInt);
        let status = unsafe { method(database.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let driver = self.ffi_driver();
        let mut database = self.inner.database.lock().unwrap();
        let method = driver_method!(driver, DatabaseGetOption);
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(database.deref_mut(), key, value, length, error)
        };
        get_option_string(key, populate, driver)
    }

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let driver = self.ffi_driver();
        let mut database = self.inner.database.lock().unwrap();
        set_option_database(
            driver,
            database.deref_mut(),
            self.driver_version(),
            key,
            value,
        )
    }
}

impl Database for ManagedDatabase {
    type ConnectionType = ManagedConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        // Construct a new connection.
        let connection = self.connection_new()?;
        // Initialize the connection.
        let connection = self.connection_init(connection)?;
        let inner = ManagedConnectionInner {
            connection: Mutex::new(connection),
            database: self.inner.clone(),
        };
        Ok(Self::ConnectionType {
            inner: Arc::new(inner),
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (<Self::ConnectionType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        // Construct a new connection.
        let mut connection = self.connection_new()?;
        // Set the options.
        {
            let driver = self.ffi_driver();
            for (key, value) in opts {
                set_option_connection(driver, &mut connection, self.driver_version(), key, value)?;
            }
        }
        // Initialize the connection.
        let connection = self.connection_init(connection)?;
        let inner = ManagedConnectionInner {
            connection: Mutex::new(connection),
            database: self.inner.clone(),
        };
        Ok(Self::ConnectionType {
            inner: Arc::new(inner),
        })
    }
}

fn set_option_connection(
    driver: &ffi::FFI_AdbcDriver,
    connection: &mut ffi::FFI_AdbcConnection,
    version: AdbcVersion,
    key: impl AsRef<str>,
    value: OptionValue,
) -> Result<()> {
    let key = CString::new(key.as_ref())?;
    let mut error = ffi::FFI_AdbcError::with_driver(driver);
    #[allow(unknown_lints)]
    #[warn(non_exhaustive_omitted_patterns)]
    let status = match (version, value) {
        (_, OptionValue::String(value)) => {
            let value = CString::new(value)?;
            let method = driver_method!(driver, ConnectionSetOption);
            unsafe { method(connection, key.as_ptr(), value.as_ptr(), &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Bytes(value)) => {
            let method = driver_method!(driver, ConnectionSetOptionBytes);
            unsafe {
                method(
                    connection,
                    key.as_ptr(),
                    value.as_ptr(),
                    value.len(),
                    &mut error,
                )
            }
        }
        (AdbcVersion::V110, OptionValue::Int(value)) => {
            let method = driver_method!(driver, ConnectionSetOptionInt);
            unsafe { method(connection, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Double(value)) => {
            let method = driver_method!(driver, ConnectionSetOptionDouble);
            unsafe { method(connection, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V100, _) => Err(Error::with_message_and_status(
            ERR_ONLY_STRING_OPT,
            Status::NotImplemented,
        ))?,
        (_, _) => unreachable!(),
    };
    check_status(status, error)
}

struct ManagedConnectionInner {
    connection: Mutex<ffi::FFI_AdbcConnection>,
    database: Arc<ManagedDatabaseInner>,
}

impl Drop for ManagedConnectionInner {
    fn drop(&mut self) {
        let driver = &self.database.driver.driver;
        let mut connection = self.connection.lock().unwrap();
        let method = driver_method!(driver, ConnectionRelease);
        // TODO(alexandreyc): how should we handle `ConnectionRelease` failing?
        // See: https://github.com/apache/arrow-adbc/pull/1742#discussion_r1574388409
        unsafe { method(connection.deref_mut(), null_mut()) };
    }
}

/// Implementation of [Connection].
#[derive(Clone)]
pub struct ManagedConnection {
    inner: Arc<ManagedConnectionInner>,
}

impl ManagedConnection {
    fn ffi_driver(&self) -> &ffi::FFI_AdbcDriver {
        &self.inner.database.driver.driver
    }

    fn driver_version(&self) -> AdbcVersion {
        self.inner.database.driver.version
    }
}

impl Optionable for ManagedConnection {
    type Option = options::OptionConnection;

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetOptionBytes);
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(connection.deref_mut(), key, value, length, error)
        };
        get_option_bytes(key, populate, driver)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut value: f64 = f64::default();
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionGetOptionDouble);
        let status =
            unsafe { method(connection.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut value: i64 = 0;
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionGetOptionInt);
        let status =
            unsafe { method(connection.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let method = driver_method!(driver, ConnectionGetOption);
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(connection.deref_mut(), key, value, length, error)
        };
        get_option_string(key, populate, driver)
    }

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        set_option_connection(
            driver,
            connection.deref_mut(),
            self.driver_version(),
            key,
            value,
        )
    }
}

impl Connection for ManagedConnection {
    type StatementType = ManagedStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut statement = ffi::FFI_AdbcStatement::default();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementNew);
        let status = unsafe { method(connection.deref_mut(), &mut statement, &mut error) };
        check_status(status, error)?;

        let inner = Arc::new(ManagedStatementInner {
            statement: Mutex::new(statement),
            connection: self.inner.clone(),
        });

        Ok(Self::StatementType { inner })
    }

    fn cancel(&mut self) -> Result<()> {
        if let AdbcVersion::V100 = self.driver_version() {
            return Err(Error::with_message_and_status(
                ERR_CANCEL_UNSUPPORTED,
                Status::NotImplemented,
            ));
        }
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionCancel);
        let status = unsafe { method(connection.deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn commit(&mut self) -> Result<()> {
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionCommit);
        let status = unsafe { method(connection.deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn rollback(&mut self) -> Result<()> {
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionRollback);
        let status = unsafe { method(connection.deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn get_info(&self, codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader> {
        let mut stream = FFI_ArrowArrayStream::empty();
        let codes: Option<Vec<u32>> =
            codes.map(|codes| codes.iter().map(|code| code.into()).collect());
        let (codes_ptr, codes_len) = codes
            .as_ref()
            .map(|c| (c.as_ptr(), c.len()))
            .unwrap_or((null(), 0));
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionGetInfo);
        let status = unsafe {
            method(
                connection.deref_mut(),
                codes_ptr,
                codes_len,
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_objects(
        &self,
        depth: crate::options::ObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<Vec<&str>>,
        column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader> {
        let catalog = catalog.map(CString::new).transpose()?;
        let db_schema = db_schema.map(CString::new).transpose()?;
        let table_name = table_name.map(CString::new).transpose()?;
        let column_name = column_name.map(CString::new).transpose()?;
        let table_type = table_type
            .map(|t| {
                t.iter()
                    .map(|x| CString::new(*x))
                    .collect::<std::result::Result<Vec<CString>, _>>()
            })
            .transpose()?;

        let catalog_ptr = catalog.as_ref().map(|c| c.as_ptr()).unwrap_or(null());
        let db_schema_ptr = db_schema.as_ref().map(|c| c.as_ptr()).unwrap_or(null());
        let table_name_ptr = table_name.as_ref().map(|c| c.as_ptr()).unwrap_or(null());
        let column_name_ptr = column_name.as_ref().map(|c| c.as_ptr()).unwrap_or(null());

        let mut table_type_ptrs = table_type
            .as_ref()
            .map(|v| v.iter().map(|c| c.as_ptr()))
            .map(|c| c.collect::<Vec<_>>());
        let table_type_ptr = match table_type_ptrs.as_mut() {
            None => null(),
            Some(t) => {
                t.push(null());
                t.as_ptr()
            }
        };

        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionGetObjects);
        let mut stream = FFI_ArrowArrayStream::empty();

        let status = unsafe {
            method(
                connection.deref_mut(),
                depth.into(),
                catalog_ptr,
                db_schema_ptr,
                table_name_ptr,
                table_type_ptr,
                column_name_ptr,
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;

        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_statistics(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        approximate: bool,
    ) -> Result<impl RecordBatchReader> {
        if let AdbcVersion::V100 = self.driver_version() {
            return Err(Error::with_message_and_status(
                ERR_STATISTICS_UNSUPPORTED,
                Status::NotImplemented,
            ));
        }

        let catalog = catalog.map(CString::new).transpose()?;
        let db_schema = db_schema.map(CString::new).transpose()?;
        let table_name = table_name.map(CString::new).transpose()?;

        let catalog_ptr = catalog.as_ref().map(|c| c.as_ptr()).unwrap_or(null());
        let db_schema_ptr = db_schema.as_ref().map(|c| c.as_ptr()).unwrap_or(null());
        let table_name_ptr = table_name.as_ref().map(|c| c.as_ptr()).unwrap_or(null());

        let mut stream = FFI_ArrowArrayStream::empty();
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionGetStatistics);
        let status = unsafe {
            method(
                connection.deref_mut(),
                catalog_ptr,
                db_schema_ptr,
                table_name_ptr,
                approximate as std::os::raw::c_char,
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader> {
        if let AdbcVersion::V100 = self.driver_version() {
            return Err(Error::with_message_and_status(
                ERR_STATISTICS_UNSUPPORTED,
                Status::NotImplemented,
            ));
        }
        let mut stream = FFI_ArrowArrayStream::empty();
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionGetStatisticNames);
        let status = unsafe { method(connection.deref_mut(), &mut stream, &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<arrow_schema::Schema> {
        let catalog = catalog.map(CString::new).transpose()?;
        let db_schema = db_schema.map(CString::new).transpose()?;
        let table_name = CString::new(table_name)?;

        let catalog_ptr = catalog.as_ref().map(|c| c.as_ptr()).unwrap_or(null());
        let db_schema_ptr = db_schema.as_ref().map(|c| c.as_ptr()).unwrap_or(null());
        let table_name_ptr = table_name.as_ptr();

        let mut schema = FFI_ArrowSchema::empty();
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionGetTableSchema);
        let status = unsafe {
            method(
                connection.deref_mut(),
                catalog_ptr,
                db_schema_ptr,
                table_name_ptr,
                &mut schema,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader> {
        let mut stream = FFI_ArrowArrayStream::empty();
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionGetTableTypes);
        let status = unsafe { method(connection.deref_mut(), &mut stream, &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn read_partition(&self, partition: impl AsRef<[u8]>) -> Result<impl RecordBatchReader> {
        let mut stream = FFI_ArrowArrayStream::empty();
        let driver = self.ffi_driver();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionReadPartition);
        let partition = partition.as_ref();
        let status = unsafe {
            method(
                connection.deref_mut(),
                partition.as_ptr(),
                partition.len(),
                &mut stream,
                &mut error,
            )
        };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }
}

fn set_option_statement(
    driver: &ffi::FFI_AdbcDriver,
    statement: &mut ffi::FFI_AdbcStatement,
    version: AdbcVersion,
    key: impl AsRef<str>,
    value: OptionValue,
) -> Result<()> {
    let key = CString::new(key.as_ref())?;
    let mut error = ffi::FFI_AdbcError::with_driver(driver);
    #[allow(unknown_lints)]
    #[warn(non_exhaustive_omitted_patterns)]
    let status = match (version, value) {
        (_, OptionValue::String(value)) => {
            let value = CString::new(value)?;
            let method = driver_method!(driver, StatementSetOption);
            unsafe { method(statement, key.as_ptr(), value.as_ptr(), &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Bytes(value)) => {
            let method = driver_method!(driver, StatementSetOptionBytes);
            unsafe {
                method(
                    statement,
                    key.as_ptr(),
                    value.as_ptr(),
                    value.len(),
                    &mut error,
                )
            }
        }
        (AdbcVersion::V110, OptionValue::Int(value)) => {
            let method = driver_method!(driver, StatementSetOptionInt);
            unsafe { method(statement, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V110, OptionValue::Double(value)) => {
            let method = driver_method!(driver, StatementSetOptionDouble);
            unsafe { method(statement, key.as_ptr(), value, &mut error) }
        }
        (AdbcVersion::V100, _) => Err(Error::with_message_and_status(
            ERR_ONLY_STRING_OPT,
            Status::NotImplemented,
        ))?,
        (_, _) => unreachable!(),
    };
    check_status(status, error)
}

struct ManagedStatementInner {
    statement: Mutex<ffi::FFI_AdbcStatement>,
    connection: Arc<ManagedConnectionInner>,
}
/// Implementation of [Statement].
#[derive(Clone)]
pub struct ManagedStatement {
    inner: Arc<ManagedStatementInner>,
}

impl ManagedStatement {
    fn driver_version(&self) -> AdbcVersion {
        self.inner.connection.database.driver.version
    }

    fn ffi_driver(&self) -> &ffi::FFI_AdbcDriver {
        &self.inner.connection.database.driver.driver
    }
}

impl Statement for ManagedStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementBind);
        let batch: StructArray = batch.into();
        let (mut array, mut schema) = to_ffi(&batch.to_data())?;
        let status = unsafe { method(statement.deref_mut(), &mut array, &mut schema, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementBindStream);
        let mut stream = FFI_ArrowArrayStream::new(reader);
        let status = unsafe { method(statement.deref_mut(), &mut stream, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn cancel(&mut self) -> Result<()> {
        if let AdbcVersion::V100 = self.driver_version() {
            return Err(Error::with_message_and_status(
                ERR_CANCEL_UNSUPPORTED,
                Status::NotImplemented,
            ));
        }
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementCancel);
        let status = unsafe { method(statement.deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementExecuteQuery);
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = unsafe { method(statement.deref_mut(), &mut stream, null_mut(), &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn execute_schema(&mut self) -> Result<arrow_schema::Schema> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementExecuteSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let status = unsafe { method(statement.deref_mut(), &mut schema, &mut error) };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementExecuteQuery);
        let mut rows_affected: i64 = -1;
        let status = unsafe {
            method(
                statement.deref_mut(),
                null_mut(),
                &mut rows_affected,
                &mut error,
            )
        };
        check_status(status, error)?;
        Ok((rows_affected != -1).then_some(rows_affected))
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementExecutePartitions);
        let mut schema = FFI_ArrowSchema::empty();
        let mut partitions = ffi::FFI_AdbcPartitions::default();
        let mut rows_affected: i64 = -1;
        let status = unsafe {
            method(
                statement.deref_mut(),
                &mut schema,
                &mut partitions,
                &mut rows_affected,
                &mut error,
            )
        };
        check_status(status, error)?;

        let result = PartitionedResult {
            partitions: partitions.into(),
            schema: (&schema).try_into()?,
            rows_affected,
        };

        Ok(result)
    }

    fn get_parameter_schema(&self) -> Result<arrow_schema::Schema> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementGetParameterSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let status = unsafe { method(statement.deref_mut(), &mut schema, &mut error) };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn prepare(&mut self) -> Result<()> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementPrepare);
        let status = unsafe { method(statement.deref_mut(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        let query = CString::new(query.as_ref())?;
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementSetSqlQuery);
        let status = unsafe { method(statement.deref_mut(), query.as_ptr(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> Result<()> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementSetSubstraitPlan);
        let plan = plan.as_ref();
        let status =
            unsafe { method(statement.deref_mut(), plan.as_ptr(), plan.len(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }
}

impl Optionable for ManagedStatement {
    type Option = options::OptionStatement;

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let method = driver_method!(driver, StatementGetOptionBytes);
        let populate = |key: *const c_char,
                        value: *mut u8,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(statement.deref_mut(), key, value, length, error)
        };
        get_option_bytes(key, populate, driver)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        let key = CString::new(key.as_ref())?;
        let mut value: f64 = f64::default();
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementGetOptionDouble);
        let status = unsafe { method(statement.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        let key = CString::new(key.as_ref())?;
        let mut value: i64 = 0;
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementGetOptionInt);
        let status = unsafe { method(statement.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let method = driver_method!(driver, StatementGetOption);
        let populate = |key: *const c_char,
                        value: *mut c_char,
                        length: *mut usize,
                        error: *mut ffi::FFI_AdbcError| unsafe {
            method(statement.deref_mut(), key, value, length, error)
        };
        get_option_string(key, populate, driver)
    }

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        set_option_statement(
            driver,
            statement.deref_mut(),
            self.driver_version(),
            key,
            value,
        )
    }
}

impl Drop for ManagedStatement {
    fn drop(&mut self) {
        let driver = self.ffi_driver();
        let mut statement = self.inner.statement.lock().unwrap();
        let method = driver_method!(driver, StatementRelease);
        // TODO(alexandreyc): how should we handle `StatementRelease` failing?
        // See: https://github.com/apache/arrow-adbc/pull/1742#discussion_r1574388409
        unsafe { method(statement.deref_mut(), null_mut()) };
    }
}

#[cfg(target_os = "windows")]
mod target_windows {
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

fn user_config_dir() -> Option<PathBuf> {
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

fn system_config_dir() -> Option<PathBuf> {
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

fn get_search_paths(lvls: LoadFlags) -> Vec<PathBuf> {
    let mut result = Vec::new();
    if lvls & LOAD_FLAG_SEARCH_ENV != 0 {
        if let Some(paths) = env::var_os("ADBC_CONFIG_PATH") {
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

const fn arch_triplet() -> (&'static str, &'static str, &'static str) {
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

#[cfg(test)]
mod tests {
    use super::*;

    use adbc_core::LOAD_FLAG_DEFAULT;
    use temp_env::{with_var, with_var_unset};
    use tempfile::Builder;

    fn manifest_without_driver() -> &'static str {
        r#"
        name = 'SQLite3'
        publisher = 'arrow-adbc'
        version = '1.0.0'

        [ADBC]
        version = '1.1.0'
        "#
    }

    fn simple_manifest() -> String {
        // if this test is enabled, we expect the env var ADBC_DRIVER_MANAGER_TEST_LIB
        // to be defined.
        let driver_path =
            PathBuf::from(env::var_os("ADBC_DRIVER_MANAGER_TEST_LIB").expect(
                "ADBC_DRIVER_MANAGER_TEST_LIB must be set for driver manager manifest tests",
            ));

        assert!(
            driver_path.exists(),
            "ADBC_DRIVER_MANAGER_TEST_LIB path does not exist: {}",
            driver_path.display()
        );

        let (os, arch, extra) = arch_triplet();
        format!(
            r#"
    {}

    [Driver]
    [Driver.shared]
    {os}_{arch}{extra} = {driver_path:?}
    "#,
            manifest_without_driver()
        )
    }

    fn write_manifest_to_tempfile(p: PathBuf, tbl: String) -> (tempfile::TempDir, PathBuf) {
        let tmp_dir = Builder::new()
            .prefix("adbc_tests")
            .tempdir()
            .expect("Failed to create temporary directory for driver manager manifest test");

        let manifest_path = tmp_dir.path().join(p);
        if let Some(parent) = manifest_path.parent() {
            std::fs::create_dir_all(parent)
                .expect("Failed to create parent directory for manifest");
        }

        std::fs::write(&manifest_path, tbl.as_str())
            .expect("Failed to write driver manager manifest to temporary file");

        (tmp_dir, manifest_path)
    }

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

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    #[cfg_attr(target_os = "windows", ignore)] // TODO: remove this line after fixing
    fn test_load_driver_env() {
        // ensure that we fail without the env var set
        with_var_unset("ADBC_CONFIG_PATH", || {
            let err = ManagedDriver::load_from_name(
                "sqlite",
                None,
                AdbcVersion::V100,
                LOAD_FLAG_SEARCH_ENV,
                None,
            )
            .unwrap_err();
            assert_eq!(err.status, Status::NotFound);
        });

        let (tmp_dir, manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), simple_manifest());

        with_var(
            "ADBC_CONFIG_PATH",
            Some(manifest_path.parent().unwrap().as_os_str()),
            || {
                ManagedDriver::load_from_name(
                    "sqlite",
                    None,
                    AdbcVersion::V100,
                    LOAD_FLAG_SEARCH_ENV,
                    None,
                )
                .unwrap();
            },
        );

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_driver_env_multiple_paths() {
        let (tmp_dir, manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), simple_manifest());

        let path_os_string = env::join_paths([
            Path::new("/home"),
            Path::new(""),
            manifest_path.parent().unwrap(),
        ])
        .unwrap();

        with_var("ADBC_CONFIG_PATH", Some(&path_os_string), || {
            ManagedDriver::load_from_name(
                "sqlite",
                None,
                AdbcVersion::V100,
                LOAD_FLAG_SEARCH_ENV,
                None,
            )
            .unwrap();
        });

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_non_ascii_path() {
        let p = PathBuf::from("majestik møøse/sqlite.toml");
        let (tmp_dir, manifest_path) = write_manifest_to_tempfile(p, simple_manifest());

        with_var(
            "ADBC_CONFIG_PATH",
            Some(manifest_path.parent().unwrap().as_os_str()),
            || {
                ManagedDriver::load_from_name(
                    "sqlite",
                    None,
                    AdbcVersion::V100,
                    LOAD_FLAG_SEARCH_ENV,
                    None,
                )
                .unwrap();
            },
        );

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    #[cfg_attr(target_os = "windows", ignore)] // TODO: remove this line after fixing
    fn test_disallow_env_config() {
        let (tmp_dir, manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), simple_manifest());

        with_var(
            "ADBC_CONFIG_PATH",
            Some(manifest_path.parent().unwrap().as_os_str()),
            || {
                let load_flags = LOAD_FLAG_DEFAULT & !LOAD_FLAG_SEARCH_ENV;
                let err = ManagedDriver::load_from_name(
                    "sqlite",
                    None,
                    AdbcVersion::V100,
                    load_flags,
                    None,
                )
                .unwrap_err();
                assert_eq!(err.status, Status::NotFound);
            },
        );

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_additional_path() {
        let p = PathBuf::from("majestik møøse/sqlite.toml");
        let (tmp_dir, manifest_path) = write_manifest_to_tempfile(p, simple_manifest());

        ManagedDriver::load_from_name(
            "sqlite",
            None,
            AdbcVersion::V100,
            LOAD_FLAG_SEARCH_ENV,
            Some(vec![manifest_path.parent().unwrap().to_path_buf()]),
        )
        .unwrap();

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_absolute_path() {
        let (tmp_dir, manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), simple_manifest());

        ManagedDriver::load_from_name(
            manifest_path,
            None,
            AdbcVersion::V100,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap();

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_absolute_path_no_ext() {
        let (tmp_dir, mut manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), simple_manifest());

        manifest_path.set_extension("");
        ManagedDriver::load_from_name(
            manifest_path,
            None,
            AdbcVersion::V100,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap();

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_load_relative_path() {
        std::fs::write(PathBuf::from("sqlite.toml"), simple_manifest())
            .expect("Failed to write driver manager manifest to file");

        let err = ManagedDriver::load_from_name("sqlite.toml", None, AdbcVersion::V100, 0, None)
            .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);

        ManagedDriver::load_from_name(
            "sqlite.toml",
            None,
            AdbcVersion::V100,
            LOAD_FLAG_ALLOW_RELATIVE_PATHS,
            None,
        )
        .unwrap();

        std::fs::remove_file("sqlite.toml")
            .expect("Failed to remove temporary driver manifest file");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_manifest_missing_driver() {
        let (tmp_dir, manifest_path) = write_manifest_to_tempfile(
            PathBuf::from("sqlite.toml"),
            manifest_without_driver().to_string(),
        );

        let err = ManagedDriver::load_from_name(
            manifest_path,
            None,
            AdbcVersion::V100,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(not(feature = "driver_manager_test_lib"), ignore)]
    fn test_manifest_wrong_arch() {
        let manifest_wrong_arch = format!(
            r#"
    {}

    [Driver]
    [Driver.shared]
    non-existing = 'path/to/bad/driver.so'
    "#,
            manifest_without_driver()
        );

        let (tmp_dir, manifest_path) =
            write_manifest_to_tempfile(PathBuf::from("sqlite.toml"), manifest_wrong_arch);

        let err = ManagedDriver::load_from_name(
            manifest_path,
            None,
            AdbcVersion::V100,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap_err();
        assert_eq!(err.status, Status::InvalidArguments);

        tmp_dir
            .close()
            .expect("Failed to close/remove temporary directory");
    }

    #[test]
    #[cfg_attr(
        not(all(
            feature = "driver_manager_test_lib",
            feature = "driver_manager_test_manifest_user"
        )),
        ignore
    )]
    #[cfg_attr(target_os = "windows", ignore)] // TODO: remove this line after fixing
    fn test_manifest_user_config() {
        let err = ManagedDriver::load_from_name(
            "adbc-test-sqlite",
            None,
            AdbcVersion::V110,
            LOAD_FLAG_DEFAULT,
            None,
        )
        .unwrap_err();
        assert_eq!(err.status, Status::NotFound);

        let usercfg_dir = user_config_dir().unwrap();
        let mut created = false;
        if !usercfg_dir.exists() {
            std::fs::create_dir_all(&usercfg_dir)
                .expect("Failed to create user config directory for driver manager test");
            created = true;
        }

        let manifest_path = usercfg_dir.join("adbc-test-sqlite.toml");
        std::fs::write(&manifest_path, simple_manifest())
            .expect("Failed to write driver manager manifest to user config directory");

        // fail to load if the load flag doesn't have LOAD_FLAG_SEARCH_USER
        let err = ManagedDriver::load_from_name(
            "adbc-test-sqlite",
            None,
            AdbcVersion::V110,
            LOAD_FLAG_DEFAULT & !LOAD_FLAG_SEARCH_USER,
            None,
        )
        .unwrap_err();
        assert_eq!(err.status, Status::NotFound);

        // succeed loading if LOAD_FLAG_SEARCH_USER flag is set
        ManagedDriver::load_from_name(
            "adbc-test-sqlite",
            None,
            AdbcVersion::V110,
            LOAD_FLAG_SEARCH_USER,
            None,
        )
        .unwrap();

        std::fs::remove_file(&manifest_path)
            .expect("Failed to remove driver manager manifest from user config directory");
        if created {
            std::fs::remove_dir(usercfg_dir).unwrap();
        }
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
}
