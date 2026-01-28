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

use std::ffi::{CStr, CString};
use std::os::raw::c_char;

use adbc_core::constants;
use adbc_core::error::{AdbcStatusCode, Error, Result, Status};
use adbc_core::options::{AdbcVersion, OptionValue};

use crate::{driver_method, FFI_AdbcConnection, FFI_AdbcStatement};
use crate::{FFI_AdbcDatabase, FFI_AdbcDriver, FFI_AdbcError};

const ERR_ONLY_STRING_OPT: &str = "Only string option value are supported with ADBC 1.0.0";

pub fn check_status(status: AdbcStatusCode, error: FFI_AdbcError) -> Result<()> {
    match status {
        constants::ADBC_STATUS_OK => Ok(()),
        _ => {
            let mut error: Error = error.try_into()?;
            error.status = status.try_into()?;
            Err(error)
        }
    }
}

#[allow(unknown_lints)]
#[warn(non_exhaustive_omitted_patterns)]
pub(crate) fn get_opt_name(value: &OptionValue) -> &str {
    match value {
        OptionValue::String(_) => "String",
        OptionValue::Bytes(_) => "Bytes",
        OptionValue::Int(_) => "Int",
        OptionValue::Double(_) => "Double",
        _ => unreachable!(),
    }
}

pub fn set_option_database(
    driver: &FFI_AdbcDriver,
    database: &mut FFI_AdbcDatabase,
    version: AdbcVersion,
    key: impl AsRef<str>,
    value: OptionValue,
) -> Result<()> {
    let key = CString::new(key.as_ref())?;
    let mut error = FFI_AdbcError::with_driver(driver);
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
pub fn get_option_buffer<F, T>(
    key: impl AsRef<str>,
    mut populate: F,
    driver: &FFI_AdbcDriver,
) -> Result<Vec<T>>
where
    F: FnMut(*const c_char, *mut T, *mut usize, *mut FFI_AdbcError) -> AdbcStatusCode,
    T: Default + Clone,
{
    const DEFAULT_LENGTH: usize = 128;
    let key = CString::new(key.as_ref())?;
    let mut run = |length| {
        let mut value = vec![T::default(); length];
        let mut length: usize = core::mem::size_of::<T>() * value.len();
        let mut error = FFI_AdbcError::with_driver(driver);
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

pub fn get_option_bytes<F>(
    key: impl AsRef<str>,
    populate: F,
    driver: &FFI_AdbcDriver,
) -> Result<Vec<u8>>
where
    F: FnMut(*const c_char, *mut u8, *mut usize, *mut FFI_AdbcError) -> AdbcStatusCode,
{
    get_option_buffer(key, populate, driver)
}

pub fn get_option_string<F>(
    key: impl AsRef<str>,
    populate: F,
    driver: &FFI_AdbcDriver,
) -> Result<String>
where
    F: FnMut(*const c_char, *mut c_char, *mut usize, *mut FFI_AdbcError) -> AdbcStatusCode,
{
    let value = get_option_buffer(key, populate, driver)?;
    let value = unsafe { CStr::from_ptr(value.as_ptr()) };
    Ok(value.to_string_lossy().to_string())
}

pub fn set_option_connection(
    driver: &FFI_AdbcDriver,
    connection: &mut FFI_AdbcConnection,
    version: AdbcVersion,
    key: impl AsRef<str>,
    value: OptionValue,
) -> Result<()> {
    let key = CString::new(key.as_ref())?;
    let mut error = FFI_AdbcError::with_driver(driver);
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

pub fn set_option_statement(
    driver: &FFI_AdbcDriver,
    statement: &mut FFI_AdbcStatement,
    version: AdbcVersion,
    key: impl AsRef<str>,
    value: OptionValue,
) -> Result<()> {
    let key = CString::new(key.as_ref())?;
    let mut error = FFI_AdbcError::with_driver(driver);
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
