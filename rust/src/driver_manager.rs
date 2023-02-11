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
//! ## Loading a driver
//!
//! Drivers are initialized using a function provided by the driver as a main
//! entrypoint, canonically called `AdbcDriverInit`. (Although many will use a
//! different name to support statically linking multiple drivers within the
//! same program.)
//!
//! To load from a function, use [AdbcDriver::load_from_init].
//!
//! To load from a dynamic library, use [AdbcDriver::load].
//!
//! ## Using across threads
//!
//! [AdbcDriver] and [AdbcDatabase] can be used across multiple threads. They
//! hold their inner implementations within [std::sync::Arc], so they are
//! cheaply copy-able.
//!
//! [AdbcConnection] should not be used across multiple threads. Driver
//! implementations do not guarantee connection APIs are safe to call from
//! multiple threads, unless calls are carefully sequenced. So instead of using
//! the same connection across multiple threads, create a connection for each
//! thread. [AdbcConnectionBuilder] is [core::marker::Send], so it can be moved
//! to a new thread before initialized into an [AdbcConnection]. [AdbcConnection]
//! holds it's inner data in a [std::rc::Rc], so it is also cheaply copyable.

use std::{
    cell::RefCell,
    ffi::{c_void, CString},
    ops::{Deref, DerefMut},
    ptr::{null, null_mut},
    rc::Rc,
    sync::{Arc, RwLock},
};

use arrow::{
    array::{export_array_into_raw, StringArray, StructArray},
    datatypes::{DataType, Field, Schema},
    error::ArrowError,
    ffi::{FFI_ArrowArray, FFI_ArrowSchema},
    ffi_stream::{export_reader_into_raw, ArrowArrayStreamReader, FFI_ArrowArrayStream},
    record_batch::{RecordBatch, RecordBatchReader},
};

use crate::{
    error::{AdbcError, AdbcStatusCode, FFI_AdbcError},
    ffi::{
        driver_function_stubs, FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver,
        FFI_AdbcPartitions, FFI_AdbcStatement,
    },
    interface::{ConnectionApi, PartitionedStatementResult, StatementApi, StatementResult},
};

/// An error from an ADBC driver.
#[derive(Debug, Clone)]
pub struct AdbcDriverManagerError {
    pub message: String,
    pub vendor_code: i32,
    pub sqlstate: [i8; 5usize],
    pub status_code: AdbcStatusCode,
}

impl std::fmt::Display for AdbcDriverManagerError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (sqlstate: {:?}, vendor_code: {})",
            self.status_code, self.message, self.sqlstate, self.vendor_code
        )
    }
}

impl std::error::Error for AdbcDriverManagerError {
    fn description(&self) -> &str {
        &self.message
    }
}

pub type Result<T> = std::result::Result<T, AdbcDriverManagerError>;

impl<T: AdbcError> From<T> for AdbcDriverManagerError {
    fn from(value: T) -> Self {
        Self {
            message: value.message().to_string(),
            vendor_code: value.vendor_code(),
            sqlstate: value.sqlstate(),
            status_code: value.status_code(),
        }
    }
}

impl From<libloading::Error> for AdbcDriverManagerError {
    fn from(value: libloading::Error) -> Self {
        match value {
            // Error from UNIX
            libloading::Error::DlOpen { desc } => Self {
                message: format!("{desc:?}"),
                vendor_code: -1,
                sqlstate: [0; 5],
                status_code: AdbcStatusCode::Internal,
            },
            // Error from Windows
            libloading::Error::LoadLibraryExW { source } => Self {
                message: format!("{source:?}"),
                vendor_code: -1,
                sqlstate: [0; 5],
                status_code: AdbcStatusCode::Internal,
            },
            // The remaining errors either shouldn't be relevant or are unknown and
            // have no additional context on them.
            _ => Self {
                message: "Unknown error while loading shared library".to_string(),
                vendor_code: -1,
                sqlstate: [0; 5],
                status_code: AdbcStatusCode::Unknown,
            },
        }
    }
}

/// Convert ADBC-style status & error into our Result type.
///
/// This function consumes the error and calls its release callback before dropping.
fn check_status(status: AdbcStatusCode, error: FFI_AdbcError) -> Result<()> {
    if status == AdbcStatusCode::Ok {
        Ok(())
    } else {
        let message = unsafe { error.get_message() }.unwrap_or_default();

        Err(AdbcDriverManagerError {
            message,
            vendor_code: error.vendor_code,
            sqlstate: error.sqlstate,
            status_code: status,
        })
    }
}

/// An internal safe wrapper around the driver
///
/// If applicable, keeps the loaded dynamic library in scope as long as the
/// FFI_AdbcDriver so that all it's function pointers remain valid.
#[derive(Debug)]
struct AdbcDriverInner {
    driver: FFI_AdbcDriver,
    _library: Option<libloading::Library>,
}

/// Call a ADBC driver method on a [AdbcDriverInner], or else a stub function.
macro_rules! driver_method {
    ($driver_inner:expr, $func_name:ident) => {
        $driver_inner
            .driver
            .$func_name
            .unwrap_or(driver_function_stubs::$func_name)
    };
}

/// Signature of an ADBC driver init function.
pub type AdbcDriverInitFunc = unsafe extern "C" fn(
    version: ::std::os::raw::c_int,
    driver: *mut ::std::os::raw::c_void,
    error: *mut crate::error::FFI_AdbcError,
) -> crate::error::AdbcStatusCode;

/// A handle to an ADBC driver.
///
/// The internal data is held behind a [std::sync::Arc], so it is cheaply-copyable.
#[derive(Clone, Debug)]
pub struct AdbcDriver {
    inner: Arc<AdbcDriverInner>,
}

impl AdbcDriver {
    /// Load a driver from a dynamic library.
    ///
    /// Will attempt to load the dynamic library with the given `name`, find the
    /// symbol with name `entrypoint` (defaults to "AdbcDriverInit"), and then
    /// call create the driver using the resolved function.
    ///
    /// `name` should **not** include any platform-specific prefixes or suffixes.
    /// For example, use `"adbc_driver_sqlite"` rather than `"libadbc_driver_sqlite.so"`.
    pub fn load(name: &str, entrypoint: Option<&[u8]>, version: i32) -> Result<Self> {
        // Safety: because the driver we are loading contains pointers to functions
        // in this library, we must keep it loaded as long as the driver is alive.
        let library = unsafe { libloading::Library::new(libloading::library_filename(name))? };

        let entrypoint = entrypoint.unwrap_or(b"AdbcDriverInit");
        let init_func: libloading::Symbol<AdbcDriverInitFunc> = unsafe { library.get(entrypoint)? };

        let driver = Self::load_impl(&init_func, version)?;
        Ok(Self {
            inner: Arc::new(AdbcDriverInner {
                driver,
                _library: Some(library),
            }),
        })
    }

    /// Load a driver from an initialization function.
    pub fn load_from_init(init_func: &AdbcDriverInitFunc, version: i32) -> Result<Self> {
        let driver = Self::load_impl(init_func, version)?;
        Ok(Self {
            inner: Arc::new(AdbcDriverInner {
                driver,
                _library: None,
            }),
        })
    }

    fn load_impl(init_func: &AdbcDriverInitFunc, version: i32) -> Result<FFI_AdbcDriver> {
        let mut error = FFI_AdbcError::empty();
        let mut driver = FFI_AdbcDriver::empty();

        let status = unsafe {
            init_func(
                version,
                &mut driver as *mut FFI_AdbcDriver as *mut c_void,
                &mut error,
            )
        };
        check_status(status, error)?;

        Ok(driver)
    }

    /// Create a new database builder to initialize a database.
    pub fn new_database(&self) -> Result<AdbcDatabaseBuilder> {
        let mut inner = FFI_AdbcDatabase::empty();

        let mut error = FFI_AdbcError::empty();
        let database_new = driver_method!(self.inner, database_new);
        let status = unsafe { database_new(&mut inner, &mut error) };

        check_status(status, error)?;

        Ok(AdbcDatabaseBuilder {
            inner,
            driver: self.inner.clone(),
        })
    }
}

fn str_to_cstring(value: &str) -> Result<CString> {
    match CString::new(value) {
        Ok(out) => Ok(out),
        Err(err) => Err(AdbcDriverManagerError {
            message: format!(
                "Null character in string at position {}",
                err.nul_position()
            ),
            vendor_code: -1,
            sqlstate: [0; 5],
            status_code: AdbcStatusCode::InvalidArguments,
        }),
    }
}

/// Builder for an [AdbcDatabase].
///
/// Use this to set options on a database. While some databases may allow setting
/// options after initialization, many do not.
#[derive(Debug)]
pub struct AdbcDatabaseBuilder {
    inner: FFI_AdbcDatabase,
    driver: Arc<AdbcDriverInner>,
}

impl AdbcDatabaseBuilder {
    pub fn set_option(mut self, key: &str, value: &str) -> Result<Self> {
        let mut error = FFI_AdbcError::empty();
        let key = str_to_cstring(key)?;
        let value = str_to_cstring(value)?;
        let set_option = driver_method!(self.driver, database_set_option);
        let status =
            unsafe { set_option(&mut self.inner, key.as_ptr(), value.as_ptr(), &mut error) };

        check_status(status, error)?;

        Ok(self)
    }

    pub fn init(self) -> Result<AdbcDatabase> {
        Ok(AdbcDatabase {
            inner: Arc::new(RwLock::new(self.inner)),
            driver: self.driver,
        })
    }
}

// Safety: the only thing in the builder that isn't Send + Sync is the
// FFI_AdbcDatabase within the AdbcDriverInner. But the builder has exclusive
// access to that value, since it was created when the builder was constructed
// and there is no public access to it.
unsafe impl Send for AdbcDatabaseBuilder {}
unsafe impl Sync for AdbcDatabaseBuilder {}

/// An ADBC database handle.
///
/// See [crate::interface::DatabaseApi] for more details.
#[derive(Clone, Debug)]
pub struct AdbcDatabase {
    // In general, ADBC objects allow serialized access from multiple threads,
    // but not concurrent access. Specific implementations may permit
    // multiple threads. To support safe access to all drivers, we wrap them in
    // RwLock.
    inner: Arc<RwLock<FFI_AdbcDatabase>>,
    driver: Arc<AdbcDriverInner>,
}

impl AdbcDatabase {
    /// Set an option on the database.
    ///
    /// Some drivers may not support setting options after initialization and
    /// instead return an error. So when possible prefer setting options on the
    /// builder.
    pub fn set_option(&self, key: &str, value: &str) -> Result<()> {
        let mut error = FFI_AdbcError::empty();
        let key = str_to_cstring(key)?;
        let value = str_to_cstring(value)?;

        let mut inner_mut = self
            .inner
            .write()
            .expect("Read-write lock of AdbcDatabase was poisoned.");
        let status = unsafe {
            let set_option = driver_method!(self.driver, database_set_option);
            set_option(
                inner_mut.deref_mut(),
                key.as_ptr(),
                value.as_ptr(),
                &mut error,
            )
        };

        check_status(status, error)?;

        Ok(())
    }

    /// Get a connection builder to create a [AdbcConnection].
    pub fn new_connection(&self) -> Result<AdbcConnectionBuilder> {
        let mut inner = FFI_AdbcConnection::empty();

        let mut error = FFI_AdbcError::empty();
        let status = unsafe {
            let connection_new = driver_method!(self.driver, connection_new);
            connection_new(&mut inner, &mut error)
        };

        check_status(status, error)?;

        Ok(AdbcConnectionBuilder {
            inner,
            database: self.inner.clone(),
            driver: self.driver.clone(),
        })
    }
}

// Safety: the only thing in the builder that isn't Send + Sync is the
// FFI_AdbcDatabase within the AdbcDriverInner. The builder ensures it doesn't
// have multiple references to that before this struct is created. And within
// this struct, the value is wrapped in a RwLock to manage access.
unsafe impl Send for AdbcDatabase {}
unsafe impl Sync for AdbcDatabase {}

/// Builder for an [AdbcConnection].
#[derive(Debug)]
pub struct AdbcConnectionBuilder {
    inner: FFI_AdbcConnection,
    database: Arc<RwLock<FFI_AdbcDatabase>>,
    driver: Arc<AdbcDriverInner>,
}

impl AdbcConnectionBuilder {
    /// Set an option on a connection.
    pub fn set_option(mut self, key: &str, value: &str) -> Result<Self> {
        let mut error = FFI_AdbcError::empty();
        let key = str_to_cstring(key)?;
        let value = str_to_cstring(value)?;
        let status = unsafe {
            let set_option = driver_method!(self.driver, connection_set_option);
            set_option(&mut self.inner, key.as_ptr(), value.as_ptr(), &mut error)
        };

        check_status(status, error)?;

        Ok(self)
    }

    /// Initialize the connection.
    ///
    /// [AdbcConnection] is not [core::marker::Send], so move the builder to
    /// the destination thread before initializing.
    pub fn init(mut self) -> Result<AdbcConnection> {
        let mut error = FFI_AdbcError::empty();

        let mut database_mut = self
            .database
            .write()
            .expect("Read-write lock of AdbcDatabase was poisoned.");

        let connection_init = driver_method!(self.driver, connection_init);
        let status =
            unsafe { connection_init(&mut self.inner, database_mut.deref_mut(), &mut error) };

        check_status(status, error)?;

        Ok(AdbcConnection {
            inner: Rc::new(RefCell::new(self.inner)),
            driver: self.driver,
        })
    }
}

// Safety: There are only two things within the struct that are not Sync + Send.
// FFI_AdbcDatabase is not thread-safe, but as wrapped is Sync + Send in the same
// way as described for AdbcDatabase. And the inner FFI_AdbcConnection is
// guaranteed to have no references to it outside the builder, since it was
// created at the same time as the builder and there is no way to get a reference
// to it from this struct.
unsafe impl Send for AdbcConnectionBuilder {}
unsafe impl Sync for AdbcConnectionBuilder {}

/// An ADBC Connection associated with the driver.
///
/// Connections should be used on a single thread. To use a driver from multiple
/// threads, create a connection for each thread.
///
/// See [ConnectionApi] for details of the methods.
#[derive(Debug)]
pub struct AdbcConnection {
    inner: Rc<RefCell<FFI_AdbcConnection>>,
    driver: Arc<AdbcDriverInner>,
}

impl ConnectionApi for AdbcConnection {
    type Error = AdbcDriverManagerError;

    fn set_option(&self, key: &str, value: &str) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();
        let key = str_to_cstring(key)?;
        let value = str_to_cstring(value)?;

        let set_option = driver_method!(self.driver, connection_set_option);
        let status = unsafe {
            set_option(
                self.inner.borrow_mut().deref_mut(),
                key.as_ptr(),
                value.as_ptr(),
                &mut error,
            )
        };

        check_status(status, error)?;

        Ok(())
    }

    /// Get the valid table types for the database.
    ///
    /// For example, in sqlite the table types are "view" and "table".
    ///
    /// This can error if not implemented by the driver.
    fn get_table_types(&self) -> std::result::Result<Vec<String>, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut reader = FFI_ArrowArrayStream::empty();

        let get_table_types = driver_method!(self.driver, connection_get_table_types);
        let status = unsafe {
            get_table_types(self.inner.borrow_mut().deref_mut(), &mut reader, &mut error)
        };
        check_status(status, error)?;

        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut reader)? };

        let expected_schema = Schema::new(vec![Field::new("table_type", DataType::Utf8, false)]);
        let schema_mismatch_error = |found_schema| AdbcDriverManagerError {
            message: format!("Driver returned unexpected schema: {found_schema:?}"),
            vendor_code: -1,
            sqlstate: [0; 5],
            status_code: AdbcStatusCode::Internal,
        };
        if reader.schema().deref() != &expected_schema {
            return Err(schema_mismatch_error(reader.schema()));
        }

        let batches: Vec<RecordBatch> = reader.collect::<std::result::Result<_, ArrowError>>()?;

        let mut out: Vec<String> =
            Vec::with_capacity(batches.iter().map(|batch| batch.num_rows()).sum());
        for batch in &batches {
            if batch.schema().deref() != &expected_schema {
                return Err(schema_mismatch_error(batch.schema()));
            }
            let column = batch
                .column(0)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(); // We just asserted above this is a StringArray.
            for value in column.into_iter().flatten() {
                out.push(value.to_string());
            }
        }

        Ok(out)
    }

    fn get_info(
        &self,
        info_codes: &[u32],
    ) -> std::result::Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error> {
        // TODO: Get this to return a more usable type
        let mut error = FFI_AdbcError::empty();

        let mut reader = FFI_ArrowArrayStream::empty();

        let get_info = driver_method!(self.driver, connection_get_info);
        let status = unsafe {
            get_info(
                self.inner.borrow_mut().deref_mut(),
                info_codes.as_ptr(),
                info_codes.len(),
                &mut reader,
                &mut error,
            )
        };
        check_status(status, error)?;

        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut reader)? };

        Ok(Box::new(reader))
    }

    fn get_objects(
        &self,
        depth: crate::ffi::AdbcObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&[&str]>,
        column_name: Option<&str>,
    ) -> std::result::Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut reader = FFI_ArrowArrayStream::empty();

        let catalog = catalog.map(str_to_cstring).transpose()?;
        let catalog_ptr = catalog.map(|s| s.as_ptr()).unwrap_or(null());

        let db_schema = db_schema.map(str_to_cstring).transpose()?;
        let db_schema_ptr = db_schema.map(|s| s.as_ptr()).unwrap_or(null());

        let table_name = table_name.map(str_to_cstring).transpose()?;
        let table_name_ptr = table_name.map(|s| s.as_ptr()).unwrap_or(null());

        let column_name = column_name.map(str_to_cstring).transpose()?;
        let column_name_ptr = column_name.map(|s| s.as_ptr()).unwrap_or(null());

        let table_type: Vec<CString> = match table_type {
            Some(table_type) => table_type
                .iter()
                .map(|&s| str_to_cstring(s))
                .collect::<Result<_>>()?,
            None => Vec::new(),
        };
        let mut table_type_ptrs: Vec<_> = table_type.iter().map(|s| s.as_ptr()).collect();
        // Make sure the array is null-terminated
        table_type_ptrs.push(null());

        let get_objects = driver_method!(self.driver, connection_get_objects);
        let status = unsafe {
            get_objects(
                self.inner.borrow_mut().deref_mut(),
                depth,
                catalog_ptr,
                db_schema_ptr,
                table_name_ptr,
                if table_type_ptrs.len() == 1 {
                    // Just null
                    null()
                } else {
                    table_type_ptrs.as_ptr()
                },
                column_name_ptr,
                &mut reader,
                &mut error,
            )
        };
        check_status(status, error)?;

        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut reader)? };

        Ok(Box::new(reader))
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> std::result::Result<arrow::datatypes::Schema, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let catalog = catalog.map(str_to_cstring).transpose()?;
        let catalog_ptr = catalog.map(|s| s.as_ptr()).unwrap_or(null());

        let db_schema = db_schema.map(str_to_cstring).transpose()?;
        let db_schema_ptr = db_schema.map(|s| s.as_ptr()).unwrap_or(null());

        let table_name = str_to_cstring(table_name)?;

        let mut schema = FFI_ArrowSchema::empty();

        let get_table_schema = driver_method!(self.driver, connection_get_table_schema);
        let status = unsafe {
            get_table_schema(
                self.inner.borrow_mut().deref_mut(),
                catalog_ptr,
                db_schema_ptr,
                table_name.as_ptr(),
                &mut schema,
                &mut error,
            )
        };
        check_status(status, error)?;

        Ok(Schema::try_from(&schema)?)
    }

    fn read_partition(
        &self,
        partition: &[u8],
    ) -> std::result::Result<Box<dyn arrow::record_batch::RecordBatchReader>, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut reader = FFI_ArrowArrayStream::empty();

        let read_partition = driver_method!(self.driver, connection_read_partition);
        let status = unsafe {
            read_partition(
                self.inner.borrow_mut().deref_mut(),
                partition.as_ptr(),
                partition.len(),
                &mut reader,
                &mut error,
            )
        };
        check_status(status, error)?;

        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut reader)? };

        Ok(Box::new(reader))
    }

    fn commit(&self) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let commit = driver_method!(self.driver, connection_commit);
        let status = unsafe { commit(self.inner.borrow_mut().deref_mut(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn rollback(&self) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let rollback = driver_method!(self.driver, connection_rollback);
        let status = unsafe { rollback(self.inner.borrow_mut().deref_mut(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }
}

impl AdbcConnection {
    /// Create a new statement.
    pub fn new_statement(&self) -> Result<AdbcStatement> {
        let mut inner = FFI_AdbcStatement::empty();
        let mut error = FFI_AdbcError::empty();

        let statement_new = driver_method!(self.driver, statement_new);
        let status =
            unsafe { statement_new(self.inner.borrow_mut().deref_mut(), &mut inner, &mut error) };
        check_status(status, error)?;

        Ok(AdbcStatement {
            inner,
            _connection: self.inner.clone(),
            driver: self.driver.clone(),
        })
    }
}

/// A handle to an ADBC statement.
///
/// See [StatementApi] for details.
#[derive(Debug)]
pub struct AdbcStatement {
    inner: FFI_AdbcStatement,
    // We hold onto the connection to make sure it is kept alive (and keep
    // lifetime semantics simple).
    _connection: Rc<RefCell<FFI_AdbcConnection>>,
    driver: Arc<AdbcDriverInner>,
}

impl StatementApi for AdbcStatement {
    type Error = AdbcDriverManagerError;

    fn prepare(&mut self) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let statement_prepare = driver_method!(self.driver, statement_prepare);
        let status = unsafe { statement_prepare(&mut self.inner, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_option(&mut self, key: &str, value: &str) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let key = str_to_cstring(key)?;
        let value = str_to_cstring(value)?;

        let set_option = driver_method!(self.driver, statement_set_option);
        let status =
            unsafe { set_option(&mut self.inner, key.as_ptr(), value.as_ptr(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: &str) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let query = str_to_cstring(query)?;

        let set_sql_query = driver_method!(self.driver, statement_set_sql_query);
        let status = unsafe { set_sql_query(&mut self.inner, query.as_ptr(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: &[u8]) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let set_substrait_plan = driver_method!(self.driver, statement_set_substrait_plan);
        let status =
            unsafe { set_substrait_plan(&mut self.inner, plan.as_ptr(), plan.len(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn get_param_schema(&mut self) -> std::result::Result<arrow::datatypes::Schema, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut schema = FFI_ArrowSchema::empty();

        let get_parameter_schema = driver_method!(self.driver, statement_get_parameter_schema);
        let status = unsafe { get_parameter_schema(&mut self.inner, &mut schema, &mut error) };
        check_status(status, error)?;

        Ok(Schema::try_from(&schema)?)
    }

    fn bind_data(&mut self, batch: RecordBatch) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let struct_arr = Arc::new(StructArray::from(batch));

        let mut schema = FFI_ArrowSchema::empty();
        let mut array = FFI_ArrowArray::empty();

        unsafe { export_array_into_raw(struct_arr, &mut array, &mut schema)? };

        let statement_bind = driver_method!(self.driver, statement_bind);
        let status =
            unsafe { statement_bind(&mut self.inner, &mut array, &mut schema, &mut error) };
        check_status(status, error)?;

        Ok(())
    }

    fn bind_stream(
        &mut self,
        reader: Box<dyn arrow::record_batch::RecordBatchReader>,
    ) -> std::result::Result<(), Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut stream = FFI_ArrowArrayStream::empty();

        unsafe { export_reader_into_raw(reader, &mut stream) };

        let statement_bind_stream = driver_method!(self.driver, statement_bind_stream);
        let status = unsafe { statement_bind_stream(&mut self.inner, &mut stream, &mut error) };
        check_status(status, error)?;

        Ok(())
    }

    fn execute(&mut self) -> std::result::Result<StatementResult, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut stream = FFI_ArrowArrayStream::empty();
        let mut rows_affected: i64 = -1;

        let execute_query = driver_method!(self.driver, statement_execute_query);
        let status =
            unsafe { execute_query(&mut self.inner, &mut stream, &mut rows_affected, &mut error) };
        check_status(status, error)?;

        let result: Option<Box<dyn RecordBatchReader>> = if stream.release.is_none() {
            // There was no result
            None
        } else {
            unsafe { Some(Box::new(ArrowArrayStreamReader::from_raw(&mut stream)?)) }
        };

        Ok(StatementResult {
            result,
            rows_affected,
        })
    }

    fn execute_update(&mut self) -> std::result::Result<i64, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let stream = null_mut();
        let mut rows_affected: i64 = -1;

        let execute_query = driver_method!(self.driver, statement_execute_query);
        let status =
            unsafe { execute_query(&mut self.inner, stream, &mut rows_affected, &mut error) };
        check_status(status, error)?;

        Ok(rows_affected)
    }

    fn execute_partitioned(
        &mut self,
    ) -> std::result::Result<PartitionedStatementResult, Self::Error> {
        let mut error = FFI_AdbcError::empty();

        let mut schema = FFI_ArrowSchema::empty();
        let mut partitions = FFI_AdbcPartitions::empty();
        let mut rows_affected: i64 = -1;

        let execute_partitions = driver_method!(self.driver, statement_execute_partitions);
        let status = unsafe {
            execute_partitions(
                &mut self.inner,
                &mut schema,
                &mut partitions,
                &mut rows_affected,
                &mut error,
            )
        };
        check_status(status, error)?;

        let schema = Schema::try_from(&schema)?;

        let partition_lengths = unsafe {
            std::slice::from_raw_parts(partitions.partition_lengths, partitions.num_partitions)
        };
        let partition_ptrs =
            unsafe { std::slice::from_raw_parts(partitions.partitions, partitions.num_partitions) };
        let partition_ids = partition_ptrs
            .iter()
            .zip(partition_lengths.iter())
            .map(|(&part_ptr, &len)| unsafe { std::slice::from_raw_parts(part_ptr, len).to_vec() })
            .collect();

        Ok(PartitionedStatementResult {
            schema,
            partition_ids,
            rows_affected,
        })
    }
}
