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
//! [`adbc.h`](https://github.com/apache/arrow-adbc/blob/main/adbc.h).
//!
//! There are two ways that drivers can be used:
//! 1. By linking (either statically or dynamically) the driver implementation
//! at link-time and then using [ManagedDriver::load_static].
//! 2. By loading the driver implementation at run-time (with `dlopen/LoadLibrary`)
//! using [ManagedDriver::load_dynamic_from_name] or
//! [ManagedDriver::load_dynamic_from_filename].
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
//! # use arrow::{
//! #     array::{Array, StringArray, Int64Array, Float64Array},
//! #     record_batch::{RecordBatch, RecordBatchReader},
//! #     datatypes::{Field, Schema, DataType},
//! #     compute::concat_batches,
//! # };
//! # use adbc_core::{
//! #     driver_manager::ManagedDriver,
//! #     options::{AdbcVersion, OptionDatabase, OptionStatement},
//! #     Connection, Database, Driver, Statement, Optionable
//! # };
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let opts = [(OptionDatabase::Uri, ":memory:".into())];
//! let mut driver = ManagedDriver::load_dynamic_from_name("adbc_driver_sqlite", None, AdbcVersion::V100)?;
//! let mut database = driver.new_database_with_opts(opts)?;
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
// In order to implement this semantics, all FFI objects are wrapped into
// `Mutex`. Hence, we need to deal with multiple locks at once, so care must
// be taken to avoid deadlock and in particular we must avoid "lock inversion".
// The general convention chosen here is to first acquire lock to the driver
// and then acquire lock to the specific object under implementation.

use std::collections::HashSet;
use std::ffi::{CStr, CString, OsStr};
use std::ops::DerefMut;
use std::os::raw::{c_char, c_void};
use std::ptr::{null, null_mut};
use std::sync::{Arc, Mutex};

use arrow::array::{Array, RecordBatch, RecordBatchReader, StructArray};
use arrow::ffi::{to_ffi, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use crate::{
    error::{Error, Status},
    options::{self, AdbcVersion, InfoCode, OptionValue},
    PartitionedResult, Result,
};
use crate::{ffi, ffi::types::driver_method, Optionable};
use crate::{Connection, Database, Driver, Statement};

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

impl From<libloading::Error> for Error {
    fn from(value: libloading::Error) -> Self {
        Self {
            message: format!("Error with dynamic library: {value}"),
            status: Status::Internal,
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
        }
    }
}

struct ManagedDriverInner {
    driver: Mutex<ffi::FFI_AdbcDriver>,
    version: AdbcVersion, // Driver version
    // The dynamic library must be kept loaded for the entire lifetime of the driver.
    // To avoid complex lifetimes we prefer to store it as part of this struct.
    // Besides, the `library` field must always appear after `driver` because of drop order:
    // `driver` has an implicit dependency on `library` and so it must be droped
    // before `library` because otherwise `driver` would be full of dangling
    // function pointers.
    // See: https://doc.rust-lang.org/std/ops/trait.Drop.html#drop-order
    _library: Option<libloading::Library>,
}

/// Implementation of [Driver].
#[derive(Clone)]
pub struct ManagedDriver {
    inner: Arc<ManagedDriverInner>,
}

impl ManagedDriver {
    /// Load a driver from an initialization function.
    pub fn load_static(init: &ffi::FFI_AdbcDriverInitFunc, version: AdbcVersion) -> Result<Self> {
        let driver = Self::load_impl(init, version)?;
        let inner = Arc::new(ManagedDriverInner {
            driver: Mutex::new(driver),
            version,
            _library: None,
        });
        Ok(ManagedDriver { inner })
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
        let entrypoint = entrypoint.unwrap_or(b"AdbcDriverInit");
        let library = unsafe { libloading::Library::new(filename.as_ref())? };
        let init: libloading::Symbol<ffi::FFI_AdbcDriverInitFunc> =
            unsafe { library.get(entrypoint)? };
        let driver = Self::load_impl(&init, version)?;
        let inner = Arc::new(ManagedDriverInner {
            driver: Mutex::new(driver),
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
}

impl Driver for ManagedDriver {
    type DatabaseType = ManagedDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        self.new_database_with_opts(None)
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (<Self::DatabaseType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let driver = &self.inner.driver.lock().unwrap();
        let mut database = ffi::FFI_AdbcDatabase::default();

        // DatabaseNew
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, DatabaseNew);
        let status = unsafe { method(&mut database, &mut error) };
        check_status(status, error)?;

        // DatabaseSetOption
        for (key, value) in opts {
            set_option_database(driver, &mut database, self.inner.version, key, value)?;
        }

        // DatabaseInit
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, DatabaseInit);
        let status = unsafe { method(&mut database, &mut error) };
        check_status(status, error)?;

        let inner = Arc::new(ManagedDatabaseInner {
            database: Mutex::new(database),
            driver: self.inner.clone(),
        });
        Ok(Self::DatabaseType { inner })
    }
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
    driver: Arc<ManagedDriverInner>,
}

impl Drop for ManagedDatabaseInner {
    fn drop(&mut self) {
        let driver = &self.driver.driver.lock().unwrap();
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
    fn driver_version(&self) -> AdbcVersion {
        self.inner.driver.version
    }
}

impl Optionable for ManagedDatabase {
    type Option = options::OptionDatabase;

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        let driver = &self.inner.driver.driver.lock().unwrap();
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
        let driver = &self.inner.driver.driver.lock().unwrap();
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
        let driver = &self.inner.driver.driver.lock().unwrap();
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
        let driver = &self.inner.driver.driver.lock().unwrap();
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
        let driver = &self.inner.driver.driver.lock().unwrap();
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

    fn new_connection(&mut self) -> Result<Self::ConnectionType> {
        self.new_connection_with_opts(None)
    }

    fn new_connection_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (<Self::ConnectionType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let driver = &self.inner.driver.driver.lock().unwrap();
        let mut database = self.inner.database.lock().unwrap();
        let mut connection = ffi::FFI_AdbcConnection::default();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionNew);
        let status = unsafe { method(&mut connection, &mut error) };
        check_status(status, error)?;

        for (key, value) in opts {
            set_option_connection(driver, &mut connection, self.driver_version(), key, value)?;
        }

        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionInit);
        let status = unsafe { method(&mut connection, database.deref_mut(), &mut error) };
        check_status(status, error)?;

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
    };
    check_status(status, error)
}

struct ManagedConnectionInner {
    connection: Mutex<ffi::FFI_AdbcConnection>,
    database: Arc<ManagedDatabaseInner>,
}

impl Drop for ManagedConnectionInner {
    fn drop(&mut self) {
        let driver = &self.database.driver.driver.lock().unwrap();
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
    fn driver_version(&self) -> AdbcVersion {
        self.inner.database.driver.version
    }
}

impl Optionable for ManagedConnection {
    type Option = options::OptionConnection;

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.database.driver.driver.lock().unwrap();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionGetOptionInt);
        let status =
            unsafe { method(connection.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.database.driver.driver.lock().unwrap();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionCancel);
        let status = unsafe { method(connection.deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn commit(&mut self) -> Result<()> {
        let driver = &self.inner.database.driver.driver.lock().unwrap();
        let mut connection = self.inner.connection.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, ConnectionCommit);
        let status = unsafe { method(connection.deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn rollback(&mut self) -> Result<()> {
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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

        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
    ) -> Result<arrow::datatypes::Schema> {
        let catalog = catalog.map(CString::new).transpose()?;
        let db_schema = db_schema.map(CString::new).transpose()?;
        let table_name = CString::new(table_name)?;

        let catalog_ptr = catalog.as_ref().map(|c| c.as_ptr()).unwrap_or(null());
        let db_schema_ptr = db_schema.as_ref().map(|c| c.as_ptr()).unwrap_or(null());
        let table_name_ptr = table_name.as_ptr();

        let mut schema = FFI_ArrowSchema::empty();
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.database.driver.driver.lock().unwrap();
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
}

impl Statement for ManagedStatement {
    fn bind(&mut self, batch: RecordBatch) -> Result<()> {
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementCancel);
        let status = unsafe { method(statement.deref_mut(), &mut error) };
        check_status(status, error)
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader> {
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementExecuteQuery);
        let mut stream = FFI_ArrowArrayStream::empty();
        let status = unsafe { method(statement.deref_mut(), &mut stream, null_mut(), &mut error) };
        check_status(status, error)?;
        let reader = ArrowArrayStreamReader::try_new(stream)?;
        Ok(reader)
    }

    fn execute_schema(&mut self) -> Result<arrow::datatypes::Schema> {
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementExecuteSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let status = unsafe { method(statement.deref_mut(), &mut schema, &mut error) };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
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

    fn get_parameter_schema(&self) -> Result<arrow::datatypes::Schema> {
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementGetParameterSchema);
        let mut schema = FFI_ArrowSchema::empty();
        let status = unsafe { method(statement.deref_mut(), &mut schema, &mut error) };
        check_status(status, error)?;
        Ok((&schema).try_into()?)
    }

    fn prepare(&mut self) -> Result<()> {
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementPrepare);
        let status = unsafe { method(statement.deref_mut(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> Result<()> {
        let query = CString::new(query.as_ref())?;
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementSetSqlQuery);
        let status = unsafe { method(statement.deref_mut(), query.as_ptr(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> Result<()> {
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
        let mut statement = self.inner.statement.lock().unwrap();
        let mut error = ffi::FFI_AdbcError::with_driver(driver);
        let method = driver_method!(driver, StatementGetOptionInt);
        let status = unsafe { method(statement.deref_mut(), key.as_ptr(), &mut value, &mut error) };
        check_status(status, error)?;
        Ok(value)
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
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
        let driver = &self.inner.connection.database.driver.driver.lock().unwrap();
        let mut statement = self.inner.statement.lock().unwrap();
        let method = driver_method!(driver, StatementRelease);
        // TODO(alexandreyc): how should we handle `StatementRelease` failing?
        // See: https://github.com/apache/arrow-adbc/pull/1742#discussion_r1574388409
        unsafe { method(statement.deref_mut(), null_mut()) };
    }
}
