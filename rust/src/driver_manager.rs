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
//! [AdbcDriver] and [DriverDatabase] can be used across multiple threads. They
//! hold their inner implementations within [std::sync::Arc], so they are
//! cheaply copy-able.
//!
//! [DriverConnection] should not be used across multiple threads. Driver
//! implementations do not guarantee connection APIs are safe to call from
//! multiple threads, unless calls are carefully sequenced. So instead of using
//! the same connection across multiple threads, create a connection for each
//! thread. [DriverConnectionBuilder] is [core::marker::Send], so it can be moved
//! to a new thread before initialized into an [DriverConnection]. [DriverConnection]
//! holds it's inner data in a [std::rc::Rc], so it is also cheaply copyable.

use std::{
    cell::RefCell,
    collections::HashMap,
    ffi::{c_char, c_void, CString, NulError},
    ops::{Deref, DerefMut},
    ptr::{null, null_mut},
    rc::Rc,
    sync::{Arc, RwLock},
};

use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};
use arrow_array::{
    cast::{as_list_array, as_string_array, as_struct_array},
    types::Int16Type,
    types::Int32Type,
    Array, RecordBatch, RecordBatchReader, StructArray,
};
use arrow_data::ffi::FFI_ArrowArray;
use arrow_schema::{ffi::FFI_ArrowSchema, ArrowError, DataType, Field, Schema};

use crate::{
    error::{AdbcError, AdbcStatusCode},
    ffi::{
        driver_function_stubs, FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcError,
        FFI_AdbcPartitions, FFI_AdbcStatement,
    },
    info::{import_info_data, InfoCode, InfoData},
    objects::{
        CatalogArrayBuilder, ColumnSchemaRef, DatabaseCatalogCollection, DatabaseCatalogEntry,
        DatabaseSchemaEntry, DatabaseTableEntry, ForeignKeyUsageRef, TableConstraintRef,
        TableConstraintTypeRef,
    },
    AdbcConnection, AdbcStatement, PartitionedStatementResult, StatementResult,
};

use self::util::{NullableCString, RowReference, StructArraySlice};

pub(crate) mod util {
    use std::ffi::NulError;

    use super::*;

    /// Nullable CString wrapper type to avoid to avoid safety issues.
    ///
    /// This struct makes it obvious how to get a pointer to the data without
    /// *consuming* the struct. This is easily to accidentally do with methods
    /// directly on Option, such as map.
    pub(crate) struct NullableCString(Option<CString>);

    impl TryFrom<Option<&str>> for NullableCString {
        type Error = NulError;

        fn try_from(value: Option<&str>) -> std::result::Result<Self, Self::Error> {
            Ok(Self(value.map(CString::new).transpose()?))
        }
    }

    impl NullableCString {
        pub fn as_ptr(&self) -> *const c_char {
            // Note we are calling .as_ref() to make sure we aren't consuming the option.
            self.0.as_ref().map_or(null(), |s| s.as_ptr())
        }
    }

    use arrow::{
        array::{as_boolean_array, as_primitive_array, as_string_array, Array, StructArray},
        datatypes::ArrowPrimitiveType,
    };

    /// Represents a slice of a struct array as a reference.
    ///
    /// This is different tha the slices produced by [arrow::array::Array::slice] in
    /// that it is tied to the lifetime of the original array.
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct StructArraySlice<'a> {
        pub inner: &'a StructArray,
        pub offset: usize,
        pub len: usize,
    }

    impl<'a> StructArraySlice<'a> {
        pub fn iter_rows(&self) -> impl Iterator<Item = RowReference<'a>> {
            let end = self.offset + self.len;
            let inner = self.inner;
            (self.offset..end).map(|pos| RowReference { inner, pos })
        }
    }

    /// Represents a row in an [StructArray].
    ///
    /// Provides accessors to extract fields for a row.
    #[derive(Debug, Clone, Copy)]
    pub(crate) struct RowReference<'a> {
        pub inner: &'a StructArray,
        pub pos: usize,
    }

    impl<'a> RowReference<'a> {
        pub fn get_primitive<ArrowType: ArrowPrimitiveType>(
            &self,
            col_i: usize,
        ) -> Option<ArrowType::Native> {
            let column = as_primitive_array::<ArrowType>(self.inner.column(col_i));
            if column.is_null(self.pos) {
                None
            } else {
                Some(column.value(self.pos))
            }
        }

        pub fn get_str(&self, col_i: usize) -> Option<&'a str> {
            let column = as_string_array(self.inner.column(col_i));
            if column.is_null(self.pos) {
                None
            } else {
                Some(column.value(self.pos))
            }
        }

        pub fn get_bool(&self, col_i: usize) -> Option<bool> {
            let column = as_boolean_array(self.inner.column(col_i));
            if column.is_null(self.pos) {
                None
            } else {
                Some(column.value(self.pos))
            }
        }

        pub fn get_str_list(&self, col_i: usize) -> impl Iterator<Item = Option<&'a str>> + 'a {
            let list_column = as_list_array(self.inner.column(col_i));
            let start: usize = list_column.value_offsets()[self.pos].try_into().unwrap();
            let len: usize = list_column.value_length(self.pos).try_into().unwrap();
            let end = start + len;
            let values = as_string_array(list_column.values());
            (start..end).map(|i| {
                if values.is_null(i) {
                    None
                } else {
                    Some(values.value(i))
                }
            })
        }

        /// For columns of type List<Struct<...>>, get the struct array slice
        /// corresponding to this row.
        pub fn get_struct_slice(&self, col_i: usize) -> StructArraySlice<'a> {
            let column = as_list_array(self.inner.column(col_i));
            let offset: usize = column.value_offsets()[self.pos].try_into().unwrap();
            let len: usize = column.value_length(self.pos).try_into().unwrap();

            StructArraySlice {
                inner: as_struct_array(column.values()),
                offset,
                len,
            }
        }
    }
}

pub type Result<T> = std::result::Result<T, AdbcError>;

impl From<libloading::Error> for AdbcError {
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

        Err(AdbcError {
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
    error: *mut crate::ffi::FFI_AdbcError,
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
    pub fn new_database(&self) -> Result<DriverDatabaseBuilder> {
        let mut inner = FFI_AdbcDatabase::empty();

        let mut error = FFI_AdbcError::empty();
        let database_new = driver_method!(self.inner, database_new);
        let status = unsafe { database_new(&mut inner, &mut error) };

        check_status(status, error)?;

        Ok(DriverDatabaseBuilder {
            inner,
            driver: self.inner.clone(),
        })
    }
}

/// Builder for an [DriverDatabase].
///
/// Use this to set options on a database. While some databases may allow setting
/// options after initialization, many do not.
#[derive(Debug)]
pub struct DriverDatabaseBuilder {
    inner: FFI_AdbcDatabase,
    driver: Arc<AdbcDriverInner>,
}

impl DriverDatabaseBuilder {
    pub fn set_option(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Result<Self> {
        let mut error = FFI_AdbcError::empty();
        let key = CString::new(key.as_ref())?;
        let value = CString::new(value.as_ref())?;
        let set_option = driver_method!(self.driver, database_set_option);
        let status =
            unsafe { set_option(&mut self.inner, key.as_ptr(), value.as_ptr(), &mut error) };

        check_status(status, error)?;

        Ok(self)
    }

    pub fn init(self) -> DriverDatabase {
        DriverDatabase {
            inner: Arc::new(RwLock::new(self.inner)),
            driver: self.driver,
        }
    }
}

// Safety: the only thing in the builder that isn't Send + Sync is the
// FFI_AdbcDatabase. But the builder has exclusive access to that value,
// since it was created when the builder was constructed and there is
// no public access to it.
unsafe impl Send for DriverDatabaseBuilder {}
unsafe impl Sync for DriverDatabaseBuilder {}

/// An ADBC database handle.
///
/// See [crate::AdbcDatabase] for more details.
#[derive(Clone, Debug)]
pub struct DriverDatabase {
    // In general, ADBC objects allow serialized access from multiple threads,
    // but not concurrent access. Specific implementations may permit
    // multiple threads. To support safe access to all drivers, we wrap them in
    // RwLock.
    inner: Arc<RwLock<FFI_AdbcDatabase>>,
    driver: Arc<AdbcDriverInner>,
}

impl DriverDatabase {
    /// Set an option on the database.
    ///
    /// Some drivers may not support setting options after initialization and
    /// instead return an error. So when possible prefer setting options on the
    /// builder.
    pub fn set_option(&self, key: impl AsRef<str>, value: impl AsRef<str>) -> Result<()> {
        let mut error = FFI_AdbcError::empty();
        let key = CString::new(key.as_ref())?;
        let value = CString::new(value.as_ref())?;

        let mut inner_mut = self
            .inner
            .write()
            .expect("Read-write lock of AdbcDatabase was poisoned.");
        let set_option = driver_method!(self.driver, database_set_option);
        let status = unsafe {
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
    pub fn new_connection(&self) -> Result<DriverConnectionBuilder> {
        let mut inner = FFI_AdbcConnection::empty();

        let mut error = FFI_AdbcError::empty();
        let connection_new = driver_method!(self.driver, connection_new);
        let status = unsafe { connection_new(&mut inner, &mut error) };

        check_status(status, error)?;

        Ok(DriverConnectionBuilder {
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
unsafe impl Send for DriverDatabase {}
unsafe impl Sync for DriverDatabase {}

/// Builder for an [DriverConnection].
#[derive(Debug)]
pub struct DriverConnectionBuilder {
    inner: FFI_AdbcConnection,
    database: Arc<RwLock<FFI_AdbcDatabase>>,
    driver: Arc<AdbcDriverInner>,
}

impl DriverConnectionBuilder {
    /// Set an option on a connection.
    pub fn set_option(mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Result<Self> {
        let mut error = FFI_AdbcError::empty();
        let key = CString::new(key.as_ref())?;
        let value = CString::new(value.as_ref())?;
        let set_option = driver_method!(self.driver, connection_set_option);
        let status =
            unsafe { set_option(&mut self.inner, key.as_ptr(), value.as_ptr(), &mut error) };

        check_status(status, error)?;

        Ok(self)
    }

    /// Initialize the connection.
    ///
    /// [DriverConnection] is not [core::marker::Send], so move the builder to
    /// the destination thread before initializing.
    pub fn init(mut self) -> Result<DriverConnection> {
        let mut error = FFI_AdbcError::empty();

        let mut database_mut = self
            .database
            .write()
            .expect("Read-write lock of AdbcDatabase was poisoned.");

        let connection_init = driver_method!(self.driver, connection_init);
        let status =
            unsafe { connection_init(&mut self.inner, database_mut.deref_mut(), &mut error) };

        check_status(status, error)?;

        Ok(DriverConnection {
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
unsafe impl Send for DriverConnectionBuilder {}
unsafe impl Sync for DriverConnectionBuilder {}

/// An ADBC Connection associated with the driver.
///
/// Connections should be used on a single thread. To use a driver from multiple
/// threads, create a connection for each thread.
///
/// See [AdbcConnection] for details of the methods.
#[derive(Debug)]
pub struct DriverConnection {
    inner: Rc<RefCell<FFI_AdbcConnection>>,
    driver: Arc<AdbcDriverInner>,
}

impl AdbcConnection for DriverConnection {
    type StatementType = DriverStatement;
    type ObjectCollectionType = ImportedCatalogCollection;

    fn set_option(&self, key: impl AsRef<str>, value: impl AsRef<str>) -> Result<()> {
        let mut error = FFI_AdbcError::empty();
        let key = CString::new(key.as_ref())?;
        let value = CString::new(value.as_ref())?;

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

    fn new_statement(&self) -> Result<DriverStatement> {
        let mut inner = FFI_AdbcStatement::empty();
        let mut error = FFI_AdbcError::empty();

        let statement_new = driver_method!(self.driver, statement_new);
        let status =
            unsafe { statement_new(self.inner.borrow_mut().deref_mut(), &mut inner, &mut error) };
        check_status(status, error)?;

        Ok(DriverStatement {
            inner,
            _connection: self.inner.clone(),
            driver: self.driver.clone(),
        })
    }

    /// Get the valid table types for the database.
    ///
    /// For example, in sqlite the table types are "view" and "table".
    ///
    /// This can error if not implemented by the driver.
    fn get_table_types(&self) -> Result<Vec<String>> {
        let mut error = FFI_AdbcError::empty();

        let mut reader = FFI_ArrowArrayStream::empty();

        let get_table_types = driver_method!(self.driver, connection_get_table_types);
        let status = unsafe {
            get_table_types(self.inner.borrow_mut().deref_mut(), &mut reader, &mut error)
        };
        check_status(status, error)?;

        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut reader)? };

        let expected_schema = Schema::new(vec![Field::new("table_type", DataType::Utf8, false)]);
        let schema_mismatch_error = |found_schema| AdbcError {
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
            let column = as_string_array(batch.column(0));
            for value in column.into_iter().flatten() {
                out.push(value.to_string());
            }
        }

        Ok(out)
    }

    fn get_info(&self, info_codes: Option<&[InfoCode]>) -> Result<HashMap<InfoCode, InfoData>> {
        let mut error = FFI_AdbcError::empty();

        let mut reader = FFI_ArrowArrayStream::empty();

        let (info_codes_ptr, info_codes_len) = match &info_codes {
            Some(info) => (info.as_ptr(), info.len()),
            None => (null(), 0),
        };

        let get_info = driver_method!(self.driver, connection_get_info);
        let status = unsafe {
            get_info(
                self.inner.borrow_mut().deref_mut(),
                info_codes_ptr as *const u32,
                info_codes_len,
                &mut reader,
                &mut error,
            )
        };
        check_status(status, error)?;

        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut reader)? };

        Ok(import_info_data(reader)?)
    }

    fn get_objects(
        &self,
        depth: crate::AdbcObjectDepth,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: Option<&str>,
        table_type: Option<&[&str]>,
        column_name: Option<&str>,
    ) -> Result<Self::ObjectCollectionType> {
        let mut error = FFI_AdbcError::empty();

        let mut reader = FFI_ArrowArrayStream::empty();

        let catalog = NullableCString::try_from(catalog)?;
        let db_schema = NullableCString::try_from(db_schema)?;
        let table_name = NullableCString::try_from(table_name)?;
        let column_name = NullableCString::try_from(column_name)?;

        let table_type: Vec<CString> = match table_type {
            Some(table_type) => table_type
                .iter()
                .map(|&s| CString::new(s))
                .collect::<std::result::Result<_, NulError>>()?,
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
                catalog.as_ptr(),
                db_schema.as_ptr(),
                table_name.as_ptr(),
                if table_type_ptrs.len() == 1 {
                    // Just null
                    null()
                } else {
                    table_type_ptrs.as_ptr()
                },
                column_name.as_ptr(),
                &mut reader,
                &mut error,
            )
        };
        check_status(status, error)?;

        let reader = unsafe { ArrowArrayStreamReader::from_raw(&mut reader)? };

        let collection = ImportedCatalogCollection::try_new(reader)?;
        Ok(collection)
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<arrow::datatypes::Schema> {
        let mut error = FFI_AdbcError::empty();

        let catalog = NullableCString::try_from(catalog)?;
        let db_schema = NullableCString::try_from(db_schema)?;
        let table_name = CString::new(table_name)?;

        let mut schema = FFI_ArrowSchema::empty();

        let get_table_schema = driver_method!(self.driver, connection_get_table_schema);
        let status = unsafe {
            get_table_schema(
                self.inner.borrow_mut().deref_mut(),
                catalog.as_ptr(),
                db_schema.as_ptr(),
                table_name.as_ptr(),
                &mut schema,
                &mut error,
            )
        };
        check_status(status, error)?;

        Ok(Schema::try_from(&schema)?)
    }

    fn read_partition(&self, partition: &[u8]) -> Result<Box<dyn RecordBatchReader + Send>> {
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

    fn commit(&self) -> Result<()> {
        let mut error = FFI_AdbcError::empty();

        let commit = driver_method!(self.driver, connection_commit);
        let status = unsafe { commit(self.inner.borrow_mut().deref_mut(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn rollback(&self) -> Result<()> {
        let mut error = FFI_AdbcError::empty();

        let rollback = driver_method!(self.driver, connection_rollback);
        let status = unsafe { rollback(self.inner.borrow_mut().deref_mut(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }
}

/// A handle to an ADBC statement.
///
/// See [AdbcStatement] for details.
#[derive(Debug)]
pub struct DriverStatement {
    inner: FFI_AdbcStatement,
    // We hold onto the connection to make sure it is kept alive (and keep
    // lifetime semantics simple).
    _connection: Rc<RefCell<FFI_AdbcConnection>>,
    driver: Arc<AdbcDriverInner>,
}

impl AdbcStatement for DriverStatement {
    fn prepare(&mut self) -> Result<()> {
        let mut error = FFI_AdbcError::empty();

        let statement_prepare = driver_method!(self.driver, statement_prepare);
        let status = unsafe { statement_prepare(&mut self.inner, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_option(&mut self, key: impl AsRef<str>, value: impl AsRef<str>) -> Result<()> {
        let mut error = FFI_AdbcError::empty();

        let key = CString::new(key.as_ref())?;
        let value = CString::new(value.as_ref())?;

        let set_option = driver_method!(self.driver, statement_set_option);
        let status =
            unsafe { set_option(&mut self.inner, key.as_ptr(), value.as_ptr(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_sql_query(&mut self, query: &str) -> Result<()> {
        let mut error = FFI_AdbcError::empty();

        let query = CString::new(query)?;

        let set_sql_query = driver_method!(self.driver, statement_set_sql_query);
        let status = unsafe { set_sql_query(&mut self.inner, query.as_ptr(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: &[u8]) -> Result<()> {
        let mut error = FFI_AdbcError::empty();

        let set_substrait_plan = driver_method!(self.driver, statement_set_substrait_plan);
        let status =
            unsafe { set_substrait_plan(&mut self.inner, plan.as_ptr(), plan.len(), &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn get_param_schema(&mut self) -> Result<arrow::datatypes::Schema> {
        let mut error = FFI_AdbcError::empty();

        let mut schema = FFI_ArrowSchema::empty();

        let get_parameter_schema = driver_method!(self.driver, statement_get_parameter_schema);
        let status = unsafe { get_parameter_schema(&mut self.inner, &mut schema, &mut error) };
        check_status(status, error)?;

        Ok(Schema::try_from(&schema)?)
    }

    fn bind_data(&mut self, array: &StructArray) -> Result<()> {
        let mut schema = FFI_ArrowSchema::try_from(array.data_type())?;
        let mut array = FFI_ArrowArray::new(&array.to_data());
        let mut error = FFI_AdbcError::empty();
        let statement_bind = driver_method!(self.driver, statement_bind);
        let status =
            unsafe { statement_bind(&mut self.inner, &mut array, &mut schema, &mut error) };
        check_status(status, error)?;
        Ok(())
    }

    fn bind_stream(&mut self, reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        let mut error = FFI_AdbcError::empty();

        let mut stream = FFI_ArrowArrayStream::new(reader);

        let statement_bind_stream = driver_method!(self.driver, statement_bind_stream);
        let status = unsafe { statement_bind_stream(&mut self.inner, &mut stream, &mut error) };
        check_status(status, error)?;

        Ok(())
    }

    fn execute(&mut self) -> Result<StatementResult> {
        let mut error = FFI_AdbcError::empty();

        let mut stream = FFI_ArrowArrayStream::empty();
        let mut rows_affected: i64 = -1;

        let execute_query = driver_method!(self.driver, statement_execute_query);
        let status =
            unsafe { execute_query(&mut self.inner, &mut stream, &mut rows_affected, &mut error) };
        check_status(status, error)?;

        let result: Option<Box<dyn RecordBatchReader + Send>> = if stream.release.is_none() {
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

    fn execute_update(&mut self) -> Result<i64> {
        let mut error = FFI_AdbcError::empty();

        let stream = null_mut();
        let mut rows_affected: i64 = -1;

        let execute_query = driver_method!(self.driver, statement_execute_query);
        let status =
            unsafe { execute_query(&mut self.inner, stream, &mut rows_affected, &mut error) };
        check_status(status, error)?;

        Ok(rows_affected)
    }

    fn execute_partitioned(&mut self) -> Result<PartitionedStatementResult> {
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

pub struct ImportedCatalogCollection {
    batches: Vec<StructArray>,
}

impl ImportedCatalogCollection {
    pub(crate) fn try_new(reader: impl RecordBatchReader) -> Result<Self> {
        if reader.schema().as_ref() != &Schema::new(CatalogArrayBuilder::schema()) {
            return Err(AdbcError {
                message: format!(
                    "Received incorrect Arrow schema from GetObjects: {:?}",
                    reader.schema()
                ),
                sqlstate: [0; 5],
                vendor_code: -1,
                status_code: AdbcStatusCode::InvalidState,
            });
        }
        let batches = reader.collect::<std::result::Result<Vec<RecordBatch>, ArrowError>>()?;
        let batches = batches.into_iter().map(StructArray::from).collect();
        Ok(Self { batches })
    }
}

impl DatabaseCatalogCollection for ImportedCatalogCollection {
    type CatalogEntryType<'a> = ImportedCatalogEntry<'a>;
    type CatalogIterator<'a> = Box<dyn Iterator<Item = Self::CatalogEntryType<'a>> + 'a>;

    fn catalogs(&self) -> Self::CatalogIterator<'_> {
        Box::new(self.batches.iter().flat_map(move |batch| {
            (0..batch.len()).map(move |pos| ImportedCatalogEntry::new(batch, pos))
        }))
    }

    fn catalog<'a>(&'a self, name: Option<&str>) -> Option<Self::CatalogEntryType<'a>> {
        self.batches
            .iter()
            .flat_map(|batch| {
                as_string_array(batch.column(0))
                    .iter()
                    .enumerate()
                    .map(move |(pos, name)| (batch, pos, name))
            })
            .find(|(_, _, catalog_name)| catalog_name == &name)
            .map(|(batch, pos, _)| ImportedCatalogEntry::new(batch, pos))
    }
}

pub struct ImportedCatalogEntry<'a> {
    /// The row in the catalog record batch.
    row: RowReference<'a>,
    /// The schemas in this catalog
    schemas_array: StructArraySlice<'a>,
}

impl<'a> ImportedCatalogEntry<'a> {
    fn new(batch: &'a StructArray, pos: usize) -> Self {
        let row = RowReference { inner: batch, pos };
        let schemas_array = row.get_struct_slice(1);
        Self { row, schemas_array }
    }
}

impl<'a> DatabaseCatalogEntry<'a> for ImportedCatalogEntry<'a> {
    type SchemaEntryType = ImportedSchemaEntry<'a>;
    type SchemaIterator = Box<dyn Iterator<Item = Self::SchemaEntryType> + 'a>;

    fn name(&self) -> Option<&'a str> {
        self.row.get_str(0)
    }

    fn schemas(&self) -> Self::SchemaIterator {
        Box::new(
            self.schemas_array
                .iter_rows()
                .map(|row| ImportedSchemaEntry::new(row.inner, row.pos)),
        )
    }

    fn schema(&self, name: Option<&str>) -> Option<Self::SchemaEntryType> {
        self.schemas_array
            .iter_rows()
            .find(|row| row.get_str(0) == name)
            .map(|row| ImportedSchemaEntry::new(row.inner, row.pos))
    }
}

pub struct ImportedSchemaEntry<'a> {
    row: RowReference<'a>,
    tables_array: StructArraySlice<'a>,
}

impl<'a> ImportedSchemaEntry<'a> {
    fn new(struct_arr: &'a StructArray, pos: usize) -> Self {
        let row = RowReference {
            inner: struct_arr,
            pos,
        };
        let tables_array = row.get_struct_slice(1);
        Self { row, tables_array }
    }
}

impl<'a> DatabaseSchemaEntry<'a> for ImportedSchemaEntry<'a> {
    type TableEntryType = ImportedTableEntry<'a>;
    type TableIterator = Box<dyn Iterator<Item = Self::TableEntryType> + 'a>;

    fn name(&self) -> Option<&'a str> {
        self.row.get_str(0)
    }

    fn tables(&self) -> Self::TableIterator {
        Box::new(
            self.tables_array
                .iter_rows()
                .map(|row| ImportedTableEntry::new(row.inner, row.pos)),
        )
    }

    fn table(&self, name: &str) -> Option<Self::TableEntryType> {
        self.tables_array
            .iter_rows()
            .find(|row| row.get_str(0).expect("table_name was null") == name)
            .map(|row| ImportedTableEntry::new(row.inner, row.pos))
    }
}

pub struct ImportedTableEntry<'a> {
    row: RowReference<'a>,
    column_array: StructArraySlice<'a>,
    constraint_array: StructArraySlice<'a>,
}

impl<'a> ImportedTableEntry<'a> {
    fn new(struct_arr: &'a StructArray, pos: usize) -> ImportedTableEntry<'a> {
        let row = RowReference {
            inner: struct_arr,
            pos,
        };
        let column_array = row.get_struct_slice(2);
        let constraint_array = row.get_struct_slice(3);
        Self {
            row,
            column_array,
            constraint_array,
        }
    }
}

fn extract_column_schema(row: RowReference) -> ColumnSchemaRef {
    ColumnSchemaRef {
        name: row.get_str(0).expect("column name is null"),
        ordinal_position: row.get_primitive::<Int32Type>(1).unwrap(),
        remarks: row.get_str(2),
        xdbc_data_type: row.get_primitive::<Int16Type>(3),
        xdbc_type_name: row.get_str(4),
        xdbc_column_size: row.get_primitive::<Int32Type>(5),
        xdbc_decimal_digits: row.get_primitive::<Int16Type>(6),
        xdbc_num_prec_radix: row.get_primitive::<Int16Type>(7),
        xdbc_nullable: row.get_primitive::<Int16Type>(8),
        xdbc_column_def: row.get_str(9),
        xdbc_sql_data_type: row.get_primitive::<Int16Type>(10),
        xdbc_datetime_sub: row.get_primitive::<Int16Type>(11),
        xdbc_char_octet_length: row.get_primitive::<Int32Type>(12),
        xdbc_is_nullable: row.get_str(13),
        xdbc_scope_catalog: row.get_str(14),
        xdbc_scope_schema: row.get_str(15),
        xdbc_scope_table: row.get_str(16),
        xdbc_is_autoincrement: row.get_bool(17),
        xdbc_is_generatedcolumn: row.get_bool(18),
    }
}

impl<'a> DatabaseTableEntry<'a> for ImportedTableEntry<'a> {
    type ColumnIterator = Box<dyn Iterator<Item = ColumnSchemaRef<'a>> + 'a>;
    type ConstraintIterator = Box<dyn Iterator<Item = TableConstraintRef<'a>> + 'a>;

    fn name(&self) -> &'a str {
        self.row.get_str(0).expect("table_name was null")
    }

    fn table_type(&self) -> &'a str {
        self.row.get_str(1).expect("table_type was null")
    }

    fn columns(&self) -> Self::ColumnIterator {
        Box::new(self.column_array.iter_rows().map(extract_column_schema))
    }

    fn column(&self, i: i32) -> Option<ColumnSchemaRef<'a>> {
        self.column_array
            .iter_rows()
            .find(|row| {
                let position = row.get_primitive::<Int32Type>(1).unwrap();
                position == i
            })
            .map(extract_column_schema)
    }

    fn column_by_name(&self, name: &str) -> Option<ColumnSchemaRef<'a>> {
        self.column_array
            .iter_rows()
            .find(|row| {
                let column_name = row.get_str(0).expect("column name is null");
                column_name == name
            })
            .map(extract_column_schema)
    }

    fn constraints(&self) -> Self::ConstraintIterator {
        Box::new(self.constraint_array.iter_rows().map(|row| {
            let name = row.get_str(0);
            let constraint_type = row.get_str(1).expect("constraint_type is null");
            let columns = row
                .get_str_list(2)
                .map(|col| col.expect("column in constraint is null"))
                .collect();

            let constraint_type = match constraint_type {
                "CHECK" => TableConstraintTypeRef::Check,
                "PRIMARY KEY" => TableConstraintTypeRef::PrimaryKey,
                "UNIQUE" => TableConstraintTypeRef::Unique,
                "FOREIGN KEY" => {
                    let usage_array = row.get_struct_slice(3);
                    let usage: Vec<ForeignKeyUsageRef> = usage_array
                        .iter_rows()
                        .map(|row| ForeignKeyUsageRef {
                            catalog: row.get_str(0),
                            db_schema: row.get_str(1),
                            table: row
                                .get_str(2)
                                .expect("table_name is null in foreign key constraint"),
                            column_name: row
                                .get_str(3)
                                .expect("column_name is null in foreign key constraint"),
                        })
                        .collect();
                    TableConstraintTypeRef::ForeignKey { usage }
                }
                _ => panic!("Unknown constraint type: {constraint_type}"),
            };

            TableConstraintRef {
                name,
                columns,
                constraint_type,
            }
        }))
    }
}
