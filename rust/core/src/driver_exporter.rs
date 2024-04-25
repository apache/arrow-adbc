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

use std::collections::{HashMap, HashSet};
use std::ffi::{CStr, CString};
use std::hash::Hash;
use std::os::raw::{c_char, c_int, c_void};

use arrow::array::StructArray;
use arrow::datatypes::DataType;
use arrow::ffi::{from_ffi, FFI_ArrowArray, FFI_ArrowSchema};
use arrow::ffi_stream::{ArrowArrayStreamReader, FFI_ArrowArrayStream};

use crate::error::{Error, Result, Status};
use crate::ffi::constants::ADBC_STATUS_OK;
use crate::ffi::{
    types::ErrorPrivateData, FFI_AdbcConnection, FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcError,
    FFI_AdbcErrorDetail, FFI_AdbcPartitions, FFI_AdbcStatement, FFI_AdbcStatusCode,
};
use crate::options::{InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionValue};
use crate::{Connection, Database, Driver, Optionable, Statement};

// Invariant: options.is_none() XOR database.is_none()
struct ExportedDatabase<DriverType: Driver + Default> {
    options: Option<HashMap<OptionDatabase, OptionValue>>, // Pre-init options
    database: Option<DriverType::DatabaseType>,
}

// Invariant: options.is_none() XOR database.is_none()
struct ExportedConnection<DriverType: Driver + Default> {
    options: Option<HashMap<OptionConnection, OptionValue>>, // Pre-init options
    connection: Option<<DriverType::DatabaseType as Database>::ConnectionType>,
}

struct ExportedStatement<DriverType: Driver + Default> {
    statement:
        <<DriverType::DatabaseType as Database>::ConnectionType as Connection>::StatementType,
}

pub fn make_ffi_driver<DriverType: Driver + Default + 'static>() -> FFI_AdbcDriver {
    FFI_AdbcDriver {
        private_data: std::ptr::null_mut(),
        private_manager: std::ptr::null(),
        release: Some(release_ffi_driver),
        DatabaseInit: Some(database_init::<DriverType>),
        DatabaseNew: Some(database_new::<DriverType>),
        DatabaseSetOption: Some(database_set_option::<DriverType>),
        DatabaseRelease: Some(database_release::<DriverType>),
        ConnectionCommit: Some(connection_commit::<DriverType>),
        ConnectionGetInfo: Some(connection_get_info::<DriverType>),
        ConnectionGetObjects: Some(connection_get_objects::<DriverType>),
        ConnectionGetTableSchema: Some(connection_get_table_schema::<DriverType>),
        ConnectionGetTableTypes: Some(connection_get_table_types::<DriverType>),
        ConnectionInit: Some(connection_init::<DriverType>),
        ConnectionNew: Some(connection_new::<DriverType>),
        ConnectionSetOption: Some(connection_set_option::<DriverType>),
        ConnectionReadPartition: Some(connection_read_partition::<DriverType>),
        ConnectionRelease: Some(connection_release::<DriverType>),
        ConnectionRollback: Some(connection_rollback::<DriverType>),
        StatementBind: Some(statement_bind::<DriverType>),
        StatementBindStream: Some(statement_bind_stream::<DriverType>),
        StatementExecuteQuery: Some(statement_execute_query::<DriverType>),
        StatementExecutePartitions: Some(statement_execute_partitions::<DriverType>),
        StatementGetParameterSchema: Some(statement_get_parameter_schema::<DriverType>),
        StatementNew: Some(statement_new::<DriverType>),
        StatementPrepare: Some(statement_prepare::<DriverType>),
        StatementRelease: Some(statement_release::<DriverType>),
        StatementSetOption: Some(statement_set_option::<DriverType>),
        StatementSetSqlQuery: Some(statement_set_sql_query::<DriverType>),
        StatementSetSubstraitPlan: Some(statement_set_substrait_plan::<DriverType>),
        ErrorGetDetailCount: Some(error_get_detail_count),
        ErrorGetDetail: Some(error_get_detail),
        ErrorFromArrayStream: None, // TODO(alexandreyc): what to do with this?
        DatabaseGetOption: Some(database_get_option::<DriverType>),
        DatabaseGetOptionBytes: Some(database_get_option_bytes::<DriverType>),
        DatabaseGetOptionDouble: Some(database_get_option_double::<DriverType>),
        DatabaseGetOptionInt: Some(database_get_option_int::<DriverType>),
        DatabaseSetOptionBytes: Some(database_set_option_bytes::<DriverType>),
        DatabaseSetOptionDouble: Some(database_set_option_double::<DriverType>),
        DatabaseSetOptionInt: Some(database_set_option_int::<DriverType>),
        ConnectionCancel: Some(connection_cancel::<DriverType>),
        ConnectionGetOption: Some(connection_get_option::<DriverType>),
        ConnectionGetOptionBytes: Some(connection_get_option_bytes::<DriverType>),
        ConnectionGetOptionDouble: Some(connection_get_option_double::<DriverType>),
        ConnectionGetOptionInt: Some(connection_get_option_int::<DriverType>),
        ConnectionGetStatistics: Some(connection_get_statistics::<DriverType>),
        ConnectionGetStatisticNames: Some(connection_get_statistic_names::<DriverType>),
        ConnectionSetOptionBytes: Some(connection_set_option_bytes::<DriverType>),
        ConnectionSetOptionDouble: Some(connection_set_option_double::<DriverType>),
        ConnectionSetOptionInt: Some(connection_set_option_int::<DriverType>),
        StatementCancel: Some(statement_cancel::<DriverType>),
        StatementExecuteSchema: Some(statement_execute_schema::<DriverType>),
        StatementGetOption: Some(statement_get_option::<DriverType>),
        StatementGetOptionBytes: Some(statement_get_option_bytes::<DriverType>),
        StatementGetOptionDouble: Some(statement_get_option_double::<DriverType>),
        StatementGetOptionInt: Some(statement_get_option_int::<DriverType>),
        StatementSetOptionBytes: Some(statement_set_option_bytes::<DriverType>),
        StatementSetOptionDouble: Some(statement_set_option_double::<DriverType>),
        StatementSetOptionInt: Some(statement_set_option_int::<DriverType>),
    }
}

/// Export a Rust driver to a C driver.
///
/// The default name recommended is `AdbcDriverInit` or `<Prefix>DriverInit`.
///
/// The driver type must implement [Driver] and [Default].
#[macro_export]
macro_rules! export_driver {
    ($func_name:ident, $driver_type:ty) => {
        #[no_mangle]
        pub unsafe extern "C" fn $func_name(
            version: std::os::raw::c_int,
            driver: *mut std::os::raw::c_void,
            error: *mut $crate::ffi::FFI_AdbcError,
        ) -> $crate::ffi::FFI_AdbcStatusCode {
            if version != $crate::options::AdbcVersion::V110.into() {
                let err = $crate::error::Error::with_message_and_status(
                    format!("Unsupported ADBC version: {version}"),
                    $crate::error::Status::NotImplemented,
                );
                $crate::check_err!(Err(err), error);
            }

            if driver.is_null() {
                let err = $crate::error::Error::with_message_and_status(
                    "Passed null pointer to initialization function",
                    $crate::error::Status::NotImplemented,
                );
                $crate::check_err!(Err(err), error);
            }

            let ffi_driver = $crate::make_ffi_driver::<$driver_type>();
            unsafe {
                std::ptr::write_unaligned(driver as *mut $crate::ffi::FFI_AdbcDriver, ffi_driver);
            }
            $crate::ffi::constants::ADBC_STATUS_OK
        }
    };
}

/// Given a Result, either unwrap the value or handle the error in ADBC function.
///
/// This macro is for use when implementing ADBC methods that have an out
/// parameter for [FFI_AdbcError] and return [FFI_AdbcStatusCode]. If the result is
/// `Ok`, the expression resolves to the value. Otherwise, it will return early,
/// setting the error and status code appropriately. In order for this to work,
/// the error must be convertible to [crate::error::Error].
#[doc(hidden)]
#[macro_export]
macro_rules! check_err {
    ($res:expr, $err_out:expr) => {
        match $res {
            Ok(x) => x,
            Err(error) => {
                let error = $crate::error::Error::from(error);
                let status: $crate::ffi::FFI_AdbcStatusCode = error.status.into();
                if !$err_out.is_null() {
                    let mut ffi_error = $crate::ffi::FFI_AdbcError::try_from(error).unwrap();
                    ffi_error.private_driver = (*$err_out).private_driver;
                    unsafe { std::ptr::write_unaligned($err_out, ffi_error) };
                }
                return status;
            }
        }
    };
}

/// Check that the given raw pointer is not null.
///
/// If null, an error is returned from the enclosing function, otherwise this is
/// a no-op.
macro_rules! check_not_null {
    ($ptr:ident, $err_out:expr) => {
        let res = if $ptr.is_null() {
            Err(Error::with_message_and_status(
                format!("Passed null pointer for argument {:?}", stringify!($ptr)),
                Status::InvalidArguments,
            ))
        } else {
            Ok(())
        };
        check_err!(res, $err_out);
    };
}

unsafe extern "C" fn release_ffi_driver(
    driver: *mut FFI_AdbcDriver,
    _error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    if let Some(driver) = driver.as_mut() {
        driver.release = None;
    }
    ADBC_STATUS_OK
}

// Option helpers

unsafe fn copy_string(src: &str, dst: *mut c_char, length: *mut usize) -> Result<()> {
    let n = src.len() + 1; // +1 for nul terminator
    let src = CString::new(src)?;
    if n <= *length {
        std::ptr::copy(src.as_ptr(), dst, n);
    }
    *length = n;
    Ok::<(), Error>(())
}

unsafe fn copy_bytes(src: &[u8], dst: *mut u8, length: *mut usize) {
    let n = src.len();
    if n <= *length {
        std::ptr::copy(src.as_ptr(), dst, n);
    }
    *length = n;
}

unsafe fn get_option_int<'a, OptionType, Object>(
    object: Option<&Object>,
    options: &mut Option<HashMap<OptionType, OptionValue>>,
    key: *const c_char,
) -> Result<i64>
where
    OptionType: Hash + Eq + From<&'a str>,
    Object: Optionable<Option = OptionType>,
{
    let key = CStr::from_ptr(key).to_str()?;

    if let Some(options) = options.as_mut() {
        let optvalue = options
            .get(&key.into())
            .ok_or(Error::with_message_and_status(
                format!("Option key not found: {key:?}"),
                Status::NotFound,
            ))?;
        if let OptionValue::Int(optvalue) = optvalue {
            Ok(*optvalue)
        } else {
            let err = Error::with_message_and_status(
                format!("Option value for key {key:?} has wrong type"),
                Status::InvalidState,
            );
            Err(err)
        }
    } else {
        let object = object.expect("Broken invariant");
        let optvalue = object.get_option_int(key.into())?;
        Ok(optvalue)
    }
}

unsafe fn get_option_double<'a, OptionType, Object>(
    object: Option<&Object>,
    options: &mut Option<HashMap<OptionType, OptionValue>>,
    key: *const c_char,
) -> Result<f64>
where
    OptionType: Hash + Eq + From<&'a str>,
    Object: Optionable<Option = OptionType>,
{
    let key = CStr::from_ptr(key).to_str()?;

    if let Some(options) = options.as_mut() {
        let optvalue = options
            .get(&key.into())
            .ok_or(Error::with_message_and_status(
                format!("Option key not found: {key}"),
                Status::NotFound,
            ))?;
        if let OptionValue::Double(optvalue) = optvalue {
            Ok(*optvalue)
        } else {
            let err = Error::with_message_and_status(
                format!("Option value for key {:?} has wrong type", key),
                Status::InvalidState,
            );
            Err(err)
        }
    } else {
        let object = object.expect("Broken invariant");
        let optvalue = object.get_option_double(key.into())?;
        Ok(optvalue)
    }
}

unsafe fn get_option<'a, OptionType, Object>(
    object: Option<&Object>,
    options: &mut Option<HashMap<OptionType, OptionValue>>,
    key: *const c_char,
) -> Result<String>
where
    OptionType: Hash + Eq + From<&'a str>,
    Object: Optionable<Option = OptionType>,
{
    let key = CStr::from_ptr(key).to_str()?;

    if let Some(options) = options.as_ref() {
        let optvalue = options
            .get(&key.into())
            .ok_or(Error::with_message_and_status(
                format!("Option key not found: {key:?}"),
                Status::NotFound,
            ))?;
        if let OptionValue::String(optvalue) = optvalue {
            Ok(optvalue.clone())
        } else {
            let err = Error::with_message_and_status(
                format!("Option value for key {key:?} has wrong type"),
                Status::InvalidState,
            );
            Err(err)
        }
    } else {
        let database = object.as_ref().expect("Broken invariant");
        let optvalue = database.get_option_string(key.into())?;
        Ok(optvalue)
    }
}

unsafe fn get_option_bytes<'a, OptionType, Object>(
    object: Option<&Object>,
    options: &mut Option<HashMap<OptionType, OptionValue>>,
    key: *const c_char,
) -> Result<Vec<u8>>
where
    OptionType: Hash + Eq + From<&'a str>,
    Object: Optionable<Option = OptionType>,
{
    let key = CStr::from_ptr(key).to_str()?;

    if let Some(options) = options.as_ref() {
        let optvalue = options
            .get(&key.into())
            .ok_or(Error::with_message_and_status(
                format!("Option key not found: {key:?}"),
                Status::NotFound,
            ))?;
        if let OptionValue::Bytes(optvalue) = optvalue {
            Ok(optvalue.clone())
        } else {
            let err = Error::with_message_and_status(
                format!("Option value for key {key:?} has wrong type"),
                Status::InvalidState,
            );
            Err(err)
        }
    } else {
        let connection = object.as_ref().expect("Broken invariant");
        let optvalue = connection.get_option_bytes(key.into())?;
        Ok(optvalue)
    }
}

// Database

unsafe fn database_private_data<'a, DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
) -> Result<&'a mut ExportedDatabase<DriverType>> {
    let database = database.as_mut().ok_or(Error::with_message_and_status(
        "Passed null database pointer",
        Status::InvalidArguments,
    ))?;
    let exported = database.private_data as *mut ExportedDatabase<DriverType>;
    let exported = exported.as_mut().ok_or(Error::with_message_and_status(
        "Uninitialized database",
        Status::InvalidState,
    ));
    exported
}

unsafe fn database_set_option_impl<DriverType: Driver + Default, Value: Into<OptionValue>>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: Value,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());

    let key = check_err!(CStr::from_ptr(key).to_str(), error);

    if let Some(options) = exported.options.as_mut() {
        options.insert(key.into(), value.into());
    } else {
        let database = exported.database.as_mut().expect("Broken invariant");
        check_err!(database.set_option(key.into(), value.into()), error);
    }

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_new<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(database, error);

    let database = database.as_mut().unwrap();
    let exported = Box::new(ExportedDatabase::<DriverType> {
        options: Some(HashMap::new()),
        database: None::<DriverType::DatabaseType>,
    });
    database.private_data = Box::into_raw(exported) as *mut c_void;

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_init<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(database, error);

    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() && exported.database.is_none());

    let mut driver = DriverType::default();
    let options = exported.options.take().expect("Broken invariant");
    let database = driver.new_database_with_opts(options);
    let database = check_err!(database, error);
    exported.database = Some(database);

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_release<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(database, error);

    let database = database.as_mut().unwrap();
    let exported = Box::from_raw(database.private_data as *mut ExportedDatabase<DriverType>);
    drop(exported);
    database.private_data = std::ptr::null_mut();

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_set_option<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *const c_char,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(database, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let value = check_err!(CStr::from_ptr(value).to_str(), error);
    database_set_option_impl::<DriverType, &str>(database, key, value, error)
}

unsafe extern "C" fn database_set_option_int<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(database, error);
    check_not_null!(key, error);

    database_set_option_impl::<DriverType, i64>(database, key, value, error)
}

unsafe extern "C" fn database_set_option_double<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(database, error);
    check_not_null!(key, error);

    database_set_option_impl::<DriverType, f64>(database, key, value, error)
}

unsafe extern "C" fn database_set_option_bytes<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *const u8,
    length: usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(database, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let value = std::slice::from_raw_parts(value, length);
    database_set_option_impl::<DriverType, &[u8]>(database, key, value, error)
}

unsafe extern "C" fn database_get_option<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *mut c_char,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(database, error);
    check_not_null!(key, error);
    check_not_null!(value, error);
    check_not_null!(length, error);

    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());

    let optvalue = get_option(exported.database.as_ref(), &mut exported.options, key);
    let optvalue = check_err!(optvalue, error);
    check_err!(copy_string(&optvalue, value, length), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_get_option_int<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *mut i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(database, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());

    let optvalue = check_err!(
        get_option_int(exported.database.as_ref(), &mut exported.options, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_get_option_double<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *mut f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(database, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());

    let optvalue = check_err!(
        get_option_double(exported.database.as_ref(), &mut exported.options, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);

    ADBC_STATUS_OK
}

unsafe extern "C" fn database_get_option_bytes<DriverType: Driver + Default>(
    database: *mut FFI_AdbcDatabase,
    key: *const c_char,
    value: *mut u8,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(database, error);
    check_not_null!(key, error);
    check_not_null!(value, error);
    check_not_null!(length, error);

    let exported = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(exported.options.is_some() ^ exported.database.is_some());

    let optvalue = get_option_bytes(exported.database.as_ref(), &mut exported.options, key);
    let optvalue = check_err!(optvalue, error);
    copy_bytes(&optvalue, value, length);

    ADBC_STATUS_OK
}

// Connection

unsafe fn connection_private_data<'a, DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
) -> Result<&'a mut ExportedConnection<DriverType>> {
    let connection = connection.as_mut().ok_or(Error::with_message_and_status(
        "Passed null connection pointer",
        Status::InvalidArguments,
    ))?;
    let exported = connection.private_data as *mut ExportedConnection<DriverType>;
    let exported = exported.as_mut().ok_or(Error::with_message_and_status(
        "Uninitialized connection",
        Status::InvalidState,
    ));
    exported
}

unsafe fn connection_set_option_impl<DriverType: Driver + Default, Value: Into<OptionValue>>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: Value,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    debug_assert!(exported.options.is_some() ^ exported.connection.is_some());

    let key = check_err!(CStr::from_ptr(key).to_str(), error);

    if let Some(options) = exported.options.as_mut() {
        options.insert(key.into(), value.into());
    } else {
        let connection = exported.connection.as_mut().expect("Broken invariant");
        check_err!(connection.set_option(key.into(), value.into()), error);
    }

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_new<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);

    let connection = connection.as_mut().unwrap();
    let exported = Box::new(ExportedConnection::<DriverType> {
        options: Some(HashMap::new()),
        connection: None,
    });
    connection.private_data = Box::into_raw(exported) as *mut c_void;

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_init<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    database: *mut FFI_AdbcDatabase,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(database, error);

    let exported_connection = check_err!(connection_private_data::<DriverType>(connection), error);
    let exported_database = check_err!(database_private_data::<DriverType>(database), error);
    debug_assert!(
        exported_connection.options.is_some()
            && exported_connection.connection.is_none()
            && exported_database.database.is_some()
    );

    let options = exported_connection
        .options
        .take()
        .expect("Broken invariant");

    let connection = exported_database
        .database
        .as_mut()
        .expect("Broken invariant")
        .new_connection_with_opts(options);
    let connection = check_err!(connection, error);
    exported_connection.connection = Some(connection);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_release<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);

    let connection = connection.as_mut().unwrap();
    let exported = Box::from_raw(connection.private_data as *mut ExportedConnection<DriverType>);
    drop(exported);
    connection.private_data = std::ptr::null_mut();

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_set_option<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *const c_char,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let value = check_err!(CStr::from_ptr(value).to_str(), error);
    connection_set_option_impl::<DriverType, &str>(connection, key, value, error)
}

unsafe extern "C" fn connection_set_option_int<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(key, error);

    connection_set_option_impl::<DriverType, i64>(connection, key, value, error)
}

unsafe extern "C" fn connection_set_option_double<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(key, error);

    connection_set_option_impl::<DriverType, f64>(connection, key, value, error)
}

unsafe extern "C" fn connection_set_option_bytes<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *const u8,
    length: usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let value = std::slice::from_raw_parts(value, length);
    connection_set_option_impl::<DriverType, &[u8]>(connection, key, value, error)
}

unsafe extern "C" fn connection_get_option<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *mut c_char,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(key, error);
    check_not_null!(value, error);
    check_not_null!(length, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    debug_assert!(exported.options.is_some() ^ exported.connection.is_some());

    let optvalue = get_option(exported.connection.as_ref(), &mut exported.options, key);
    let optvalue = check_err!(optvalue, error);
    check_err!(copy_string(&optvalue, value, length), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_option_int<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *mut i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    debug_assert!(exported.options.is_some() ^ exported.connection.is_some());

    let optvalue = check_err!(
        get_option_int(exported.connection.as_ref(), &mut exported.options, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_option_double<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *mut f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    debug_assert!(exported.options.is_some() ^ exported.connection.is_some());

    let optvalue = check_err!(
        get_option_double(exported.connection.as_ref(), &mut exported.options, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_option_bytes<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    key: *const c_char,
    value: *mut u8,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(key, error);
    check_not_null!(value, error);
    check_not_null!(length, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    debug_assert!(exported.options.is_some() ^ exported.connection.is_some());

    let optvalue = get_option_bytes(exported.connection.as_ref(), &mut exported.options, key);
    let optvalue = check_err!(optvalue, error);
    copy_bytes(&optvalue, value, length);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_table_types<DriverType: Driver + Default + 'static>(
    connection: *mut FFI_AdbcConnection,
    out: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(out, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");

    let reader = check_err!(connection.get_table_types(), error);
    let reader = Box::new(reader);
    let reader = FFI_ArrowArrayStream::new(reader);
    std::ptr::write_unaligned(out, reader);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_table_schema<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    schema: *mut FFI_ArrowSchema,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(table_name, error);
    check_not_null!(schema, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");

    let catalog = catalog
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let catalog = check_err!(catalog, error);

    let db_schema = db_schema
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let db_schema = check_err!(db_schema, error);

    let table_name = table_name
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .unwrap();
    let table_name = check_err!(table_name, error);

    let schema_value = connection.get_table_schema(catalog, db_schema, table_name);
    let schema_value = check_err!(schema_value, error);
    let schema_value: FFI_ArrowSchema = check_err!(schema_value.try_into(), error);
    std::ptr::write_unaligned(schema, schema_value);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_info<DriverType: Driver + Default + 'static>(
    connection: *mut FFI_AdbcConnection,
    info_codes: *const u32,
    info_codes_length: usize,
    out: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");

    let info_codes = if info_codes.is_null() {
        None
    } else {
        let info_codes = std::slice::from_raw_parts(info_codes, info_codes_length);
        let info_codes: Result<HashSet<InfoCode>> =
            info_codes.iter().map(|c| InfoCode::try_from(*c)).collect();
        let info_codes = check_err!(info_codes, error);
        Some(info_codes)
    };

    let reader = check_err!(connection.get_info(info_codes), error);
    let reader = Box::new(reader);
    let reader = FFI_ArrowArrayStream::new(reader);
    std::ptr::write_unaligned(out, reader);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_commit<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_mut().expect("Broken invariant");
    check_err!(connection.commit(), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_rollback<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_mut().expect("Broken invariant");
    check_err!(connection.rollback(), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_cancel<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_mut().expect("Broken invariant");
    check_err!(connection.cancel(), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_statistic_names<DriverType: Driver + Default + 'static>(
    connection: *mut FFI_AdbcConnection,
    out: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(out, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");

    let reader = check_err!(connection.get_statistic_names(), error);
    let reader = Box::new(reader);
    let reader = FFI_ArrowArrayStream::new(reader);
    std::ptr::write_unaligned(out, reader);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_read_partition<DriverType: Driver + Default + 'static>(
    connection: *mut FFI_AdbcConnection,
    serialized_partition: *const u8,
    serialized_length: usize,
    out: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(serialized_partition, error);
    check_not_null!(out, error);

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");

    let partition = std::slice::from_raw_parts(serialized_partition, serialized_length);
    let reader = check_err!(connection.read_partition(partition), error);
    let reader = Box::new(reader);
    let reader = FFI_ArrowArrayStream::new(reader);
    std::ptr::write_unaligned(out, reader);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_statistics<DriverType: Driver + Default + 'static>(
    connection: *mut FFI_AdbcConnection,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    approximate: c_char,
    out: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(out, error);

    let catalog = catalog
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let catalog = check_err!(catalog, error);

    let db_schema = db_schema
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let db_schema = check_err!(db_schema, error);

    let table_name = table_name
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let table_name = check_err!(table_name, error);

    let approximate = approximate != 0;

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");

    let reader = connection.get_statistics(catalog, db_schema, table_name, approximate);
    let reader = check_err!(reader, error);
    let reader = Box::new(reader);
    let reader = FFI_ArrowArrayStream::new(reader);
    std::ptr::write_unaligned(out, reader);

    ADBC_STATUS_OK
}

unsafe extern "C" fn connection_get_objects<DriverType: Driver + Default + 'static>(
    connection: *mut FFI_AdbcConnection,
    depth: c_int,
    catalog: *const c_char,
    db_schema: *const c_char,
    table_name: *const c_char,
    table_type: *const *const c_char,
    column_name: *const c_char,
    out: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(out, error);

    let depth = check_err!(ObjectDepth::try_from(depth), error);

    let catalog = catalog
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let catalog = check_err!(catalog, error);

    let db_schema = db_schema
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let db_schema = check_err!(db_schema, error);

    let table_name = table_name
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let table_name = check_err!(table_name, error);

    let column_name = column_name
        .as_ref()
        .map(|c| CStr::from_ptr(c).to_str())
        .transpose();
    let column_name = check_err!(column_name, error);

    let table_type = if !table_type.is_null() {
        let mut strs = Vec::new();
        let mut ptr = table_type;
        while !(*ptr).is_null() {
            let str = check_err!(CStr::from_ptr(*ptr).to_str(), error);
            strs.push(str);
            ptr = ptr.add(1);
        }
        Some(strs)
    } else {
        None
    };

    let exported = check_err!(connection_private_data::<DriverType>(connection), error);
    let connection = exported.connection.as_ref().expect("Broken invariant");

    let reader = connection.get_objects(
        depth,
        catalog,
        db_schema,
        table_name,
        table_type,
        column_name,
    );
    let reader = check_err!(reader, error);
    let reader = Box::new(reader);
    let reader = FFI_ArrowArrayStream::new(reader);
    std::ptr::write_unaligned(out, reader);

    ADBC_STATUS_OK
}

// Statement

unsafe fn statement_private_data<'a, DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
) -> Result<&'a mut ExportedStatement<DriverType>> {
    let statement = statement.as_mut().ok_or(Error::with_message_and_status(
        "Passed null statement pointer",
        Status::InvalidArguments,
    ))?;
    let exported = statement.private_data as *mut ExportedStatement<DriverType>;
    let exported = exported.as_mut().ok_or(Error::with_message_and_status(
        "Uninitialized statement",
        Status::InvalidState,
    ));
    exported
}

unsafe fn statement_set_option_impl<DriverType: Driver + Default, Value: Into<OptionValue>>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: Value,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let key = check_err!(CStr::from_ptr(key).to_str(), error);
    check_err!(
        exported.statement.set_option(key.into(), value.into()),
        error
    );
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_new<DriverType: Driver + Default>(
    connection: *mut FFI_AdbcConnection,
    statement: *mut FFI_AdbcStatement,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(connection, error);
    check_not_null!(statement, error);

    let exported_connection = check_err!(connection_private_data::<DriverType>(connection), error);
    let inner_connection = exported_connection
        .connection
        .as_mut()
        .expect("Broken invariant");

    let statement = statement.as_mut().unwrap();
    let inner_statement = check_err!(inner_connection.new_statement(), error);

    let exported = Box::new(ExportedStatement::<DriverType> {
        statement: inner_statement,
    });
    statement.private_data = Box::into_raw(exported) as *mut c_void;

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_release<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);

    let statement = statement.as_mut().unwrap();
    let exported = Box::from_raw(statement.private_data as *mut ExportedStatement<DriverType>);

    drop(exported);
    statement.private_data = std::ptr::null_mut();

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_set_option<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *const c_char,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let value = check_err!(CStr::from_ptr(value).to_str(), error);
    statement_set_option_impl::<DriverType, &str>(statement, key, value, error)
}

unsafe extern "C" fn statement_set_option_int<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(key, error);

    statement_set_option_impl::<DriverType, i64>(statement, key, value, error)
}

unsafe extern "C" fn statement_set_option_double<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(key, error);

    statement_set_option_impl::<DriverType, f64>(statement, key, value, error)
}

unsafe extern "C" fn statement_set_option_bytes<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *const u8,
    length: usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let value = std::slice::from_raw_parts(value, length);
    statement_set_option_impl::<DriverType, &[u8]>(statement, key, value, error)
}

unsafe extern "C" fn statement_get_option<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *mut c_char,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(key, error);
    check_not_null!(value, error);
    check_not_null!(length, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let optvalue = get_option(Some(&exported.statement), &mut None, key);
    let optvalue = check_err!(optvalue, error);
    check_err!(copy_string(&optvalue, value, length), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_get_option_int<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *mut i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let optvalue = check_err!(
        get_option_int(Some(&exported.statement), &mut None, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_get_option_double<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *mut f64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(key, error);
    check_not_null!(value, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let optvalue = check_err!(
        get_option_double(Some(&exported.statement), &mut None, key),
        error
    );
    std::ptr::write_unaligned(value, optvalue);

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_get_option_bytes<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    key: *const c_char,
    value: *mut u8,
    length: *mut usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(key, error);
    check_not_null!(value, error);
    check_not_null!(length, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let optvalue = get_option_bytes(Some(&exported.statement), &mut None, key);
    let optvalue = check_err!(optvalue, error);
    copy_bytes(&optvalue, value, length);
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_bind<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    values: *mut FFI_ArrowArray,
    schema: *mut FFI_ArrowSchema,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(values, error);
    check_not_null!(schema, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &mut exported.statement;

    let schema = schema.as_ref().unwrap();
    let data = FFI_ArrowArray::from_raw(values);
    let array = check_err!(from_ffi(data, schema), error);

    if !matches!(array.data_type(), DataType::Struct(_)) {
        check_err!(
            Err(Error::with_message_and_status(
                "You must pass a struct array to StatementBind",
                Status::InvalidArguments
            )),
            error
        );
    }

    let array: StructArray = array.into();
    check_err!(statement.bind(array.into()), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_bind_stream<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    stream: *mut FFI_ArrowArrayStream,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(stream, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &mut exported.statement;

    let reader = check_err!(ArrowArrayStreamReader::from_raw(stream), error);
    let reader = Box::new(reader);
    check_err!(statement.bind_stream(reader), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_cancel<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &mut exported.statement;

    check_err!(statement.cancel(), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_execute_query<DriverType: Driver + Default + 'static>(
    statement: *mut FFI_AdbcStatement,
    out: *mut FFI_ArrowArrayStream,
    rows_affected: *mut i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &mut exported.statement;

    if !out.is_null() {
        let reader = check_err!(statement.execute(), error);
        let reader = Box::new(reader);
        let reader = FFI_ArrowArrayStream::new(reader);
        std::ptr::write_unaligned(out, reader);
    } else {
        let rows_affected_value = check_err!(statement.execute_update(), error).unwrap_or(-1);
        if !rows_affected.is_null() {
            std::ptr::write_unaligned(rows_affected, rows_affected_value);
        }
    }

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_execute_schema<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(schema, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &mut exported.statement;

    let schema_value = check_err!(statement.execute_schema(), error);
    let schema_value: FFI_ArrowSchema = check_err!(schema_value.try_into(), error);
    std::ptr::write_unaligned(schema, schema_value);

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_execute_partitions<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    partitions: *mut FFI_AdbcPartitions,
    rows_affected: *mut i64,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(schema, error);
    check_not_null!(partitions, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &mut exported.statement;

    let result = check_err!(statement.execute_partitions(), error);

    if !rows_affected.is_null() {
        std::ptr::write_unaligned(rows_affected, result.rows_affected);
    }

    let schema_value: FFI_ArrowSchema = check_err!((&result.schema).try_into(), error);
    std::ptr::write_unaligned(schema, schema_value);

    let partitions_value: FFI_AdbcPartitions = result.partitions.into();
    std::ptr::write_unaligned(partitions, partitions_value);

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_prepare<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &mut exported.statement;
    check_err!(statement.prepare(), error);
    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_set_sql_query<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    query: *const c_char,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(query, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &mut exported.statement;

    let query = check_err!(CStr::from_ptr(query).to_str(), error);
    check_err!(statement.set_sql_query(query), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_set_substrait_plan<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    plan: *const u8,
    length: usize,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(plan, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &mut exported.statement;

    let plan = std::slice::from_raw_parts(plan, length);
    check_err!(statement.set_substrait_plan(plan), error);

    ADBC_STATUS_OK
}

unsafe extern "C" fn statement_get_parameter_schema<DriverType: Driver + Default>(
    statement: *mut FFI_AdbcStatement,
    schema: *mut FFI_ArrowSchema,
    error: *mut FFI_AdbcError,
) -> FFI_AdbcStatusCode {
    check_not_null!(statement, error);
    check_not_null!(schema, error);

    let exported = check_err!(statement_private_data::<DriverType>(statement), error);
    let statement = &exported.statement;

    let schema_value = check_err!(statement.get_parameters_schema(), error);
    let schema_value: FFI_ArrowSchema = check_err!(schema_value.try_into(), error);
    std::ptr::write_unaligned(schema, schema_value);

    ADBC_STATUS_OK
}

// Error

unsafe extern "C" fn error_get_detail_count(error: *const FFI_AdbcError) -> c_int {
    match error.as_ref() {
        None => 0,
        Some(error) => {
            if !error.private_data.is_null() {
                let private_data = error.private_data as *const ErrorPrivateData;
                (*private_data)
                    .keys
                    .len()
                    .try_into()
                    .expect("Overflow with error detail count")
            } else {
                0
            }
        }
    }
}

unsafe extern "C" fn error_get_detail(
    error: *const FFI_AdbcError,
    index: c_int,
) -> FFI_AdbcErrorDetail {
    let default = FFI_AdbcErrorDetail::default();

    if index < 0 {
        return default;
    }

    match error.as_ref() {
        None => default,
        Some(error) => {
            let detail_count = error_get_detail_count(error);
            if index >= detail_count {
                return default;
            }
            let index = index as usize; // Cannot overflow since index >= 0 and index < detail_count

            let private_data = error.private_data as *const ErrorPrivateData;

            let key = (*private_data).keys[index].as_ptr();
            let value = (*private_data).values[index].as_ptr();
            let value_length = (*private_data).values[index].len();

            FFI_AdbcErrorDetail {
                key,
                value,
                value_length,
            }
        }
    }
}
