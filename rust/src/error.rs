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

//! ADBC error handling utilities
//!
//! ADBC functions report errors in two ways at the same time: first, they
//! return a status code, [AdbcStatusCode], and second, they fill in an out pointer
//! to [FFI_AdbcError]. To easily convert between a Rust error enum and these
//! two types, implement the [AdbcError] trait. With that trait defined, you can
//! use the [check_err] macro to handle Rust errors within ADBC functions.
//!
//! # Examples
//!
//! In simple cases, you can use [FFI_AdbcError::set_message] and return an error
//! status code early. To handle error enums that implement [AdbcError], use [check_err].
//!
//! ```
//! use std::ffi::CStr;
//! use std::os::raw::c_char;
//! use arrow_adbc::error::{FFI_AdbcError, AdbcStatusCode, check_err, AdbcError};
//!
//! unsafe fn adbc_str_utf8_len(
//!     key: *const c_char,
//!     out: *mut usize,
//!     error: *mut FFI_AdbcError) -> AdbcStatusCode {
//!     if key.is_null() {
//!         FFI_AdbcError::set_message(error, "Passed a null pointer.");
//!         return AdbcStatusCode::InvalidArguments;
//!     } else {
//!         // AdbcError is implemented for Utf8Error
//!         let key: &str = check_err!(CStr::from_ptr(key).to_str(), error);
//!         let len: usize = key.chars().count();
//!         std::ptr::write_unaligned(out, len);
//!     }
//!    AdbcStatusCode::Ok
//! }
//!
//!
//! let msg: &[u8] = &[0x68, 0x65, 0x6c, 0x6c, 0x6f, 0x0]; // "hello"
//! let mut out: usize = 0;
//!
//! let mut error = FFI_AdbcError::empty();
//!
//! let status_code = unsafe { adbc_str_utf8_len(
//!   msg.as_ptr() as *const c_char,
//!   &mut out as *mut usize,
//!   &mut error
//! ) };
//!
//! assert_eq!(status_code, AdbcStatusCode::Ok);
//! assert_eq!(out, 5);
//!
//! let mut error = FFI_AdbcError::empty();
//! let mut msg: &[u8] = &[0xff, 0x0];
//! let status_code = unsafe { adbc_str_utf8_len(
//!   msg.as_ptr() as *const c_char,
//!   &mut out as *mut usize,
//!   &mut error
//! ) };
//!
//! assert_eq!(status_code, AdbcStatusCode::InvalidArguments);
//! let error_msg = unsafe { CStr::from_ptr(error.message).to_str().unwrap() };
//! assert_eq!(error_msg, "Invalid UTF-8 character");
//! assert_eq!(error.sqlstate, [2, 2, 0, 2, 1]);
//! ```
//!

use std::{
    ffi::{c_char, CStr, CString},
    ptr::null_mut,
};

#[derive(Debug, PartialEq, Copy, Clone)]
#[repr(u8)]
pub enum AdbcStatusCode {
    /// No error.
    Ok = 0,
    /// An unknown error occurred.
    ///
    /// May indicate a driver-side or database-side error.
    Unknown = 1,
    /// The operation is not implemented or supported.
    ///
    /// May indicate a driver-side or database-side error.
    NotImplemented = 2,
    /// A requested resource was not found.
    ///
    /// May indicate a driver-side or database-side error.
    NotFound = 3,
    /// A requested resource already exists.
    ///
    /// May indicate a driver-side or database-side error.
    AlreadyExists = 4,
    /// The arguments are invalid, likely a programming error.
    ///
    /// May indicate a driver-side or database-side error.
    InvalidArguments = 5,
    /// The preconditions for the operation are not met, likely a
    ///   programming error.
    ///
    /// For instance, the object may be uninitialized, or may have not
    /// been fully configured.
    ///
    /// May indicate a driver-side or database-side error.
    InvalidState = 6,
    /// Invalid data was processed (not a programming error).
    ///
    /// For instance, a division by zero may have occurred during query
    /// execution.
    ///
    /// May indicate a database-side error only.
    InvalidData = 7,
    /// The database's integrity was affected.
    ///
    /// For instance, a foreign key check may have failed, or a uniqueness
    /// constraint may have been violated.
    ///
    /// May indicate a database-side error only.
    Integrity = 8,
    /// An error internal to the driver or database occurred.
    ///
    /// May indicate a driver-side or database-side error.
    Internal = 9,
    /// An I/O error occurred.
    ///
    /// For instance, a remote service may be unavailable.
    ///
    /// May indicate a driver-side or database-side error.
    IO = 10,
    /// The operation was cancelled, not due to a timeout.
    ///
    /// May indicate a driver-side or database-side error.
    Cancelled = 11,
    /// The operation was cancelled due to a timeout.
    ///
    /// May indicate a driver-side or database-side error.
    Timeout = 12,
    /// Authentication failed.
    ///
    /// May indicate a database-side error only.
    Unauthenticated = 13,
    /// The client is not authorized to perform the given operation.
    ///
    /// May indicate a database-side error only.
    Unauthorized = 14,
}

impl std::fmt::Display for AdbcStatusCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            AdbcStatusCode::Ok => write!(f, "Ok"),
            AdbcStatusCode::Unknown => write!(f, "Unknown"),
            AdbcStatusCode::NotImplemented => write!(f, "Not Implemented"),
            AdbcStatusCode::NotFound => write!(f, "Not Found"),
            AdbcStatusCode::AlreadyExists => write!(f, "Already Exists"),
            AdbcStatusCode::InvalidArguments => write!(f, "Invalid Arguments"),
            AdbcStatusCode::InvalidState => write!(f, "Invalid State"),
            AdbcStatusCode::InvalidData => write!(f, "Invalid Data"),
            AdbcStatusCode::Integrity => write!(f, "Integrity"),
            AdbcStatusCode::Internal => write!(f, "Internal Error"),
            AdbcStatusCode::IO => write!(f, "IO Error"),
            AdbcStatusCode::Cancelled => write!(f, "Cancelled"),
            AdbcStatusCode::Timeout => write!(f, "Timeout"),
            AdbcStatusCode::Unauthenticated => write!(f, "Unauthenticated"),
            AdbcStatusCode::Unauthorized => write!(f, "Unauthorized"),
        }
    }
}

/// A detailed error message for an operation.
#[repr(C)]
#[derive(Debug, Clone)]
pub struct FFI_AdbcError {
    /// The error message.
    pub message: *mut c_char,
    /// A vendor-specific error code, if applicable.
    pub vendor_code: i32,
    /// A SQLSTATE error code, if provided, as defined by the
    /// SQL:2003 standard.  If not set, it should be set to
    /// "\\0\\0\\0\\0\\0".
    pub sqlstate: [c_char; 5usize],
    /// Release the contained error.
    ///
    /// Unlike other structures, this is an embedded callback to make it
    /// easier for the driver manager and driver to cooperate.
    pub release: Option<unsafe extern "C" fn(error: *mut Self)>,
}

impl Drop for FFI_AdbcError {
    fn drop(&mut self) {
        if let Some(release) = self.release {
            unsafe { release(self) };
        }
    }
}

impl FFI_AdbcError {
    /// Create an empty error
    pub fn empty() -> Self {
        Self {
            message: null_mut(),
            vendor_code: -1,
            sqlstate: ['\0' as c_char; 5],
            release: None,
        }
    }

    /// Create a new FFI_AdbcError.
    ///
    /// `vendor_code` defaults to -1 and `sql_state` defaults to zeros.
    pub fn new(message: &str, vendor_code: Option<i32>, sqlstate: Option<[c_char; 5]>) -> Self {
        Self {
            message: CString::new(message).unwrap().into_raw(),
            vendor_code: vendor_code.unwrap_or(-1),
            sqlstate: sqlstate.unwrap_or(['\0' as c_char; 5]),
            release: Some(drop_adbc_error),
        }
    }

    /// Set an error message.
    ///
    /// # Safety
    ///
    /// If `dest` is null, no error is written. If `dest` is non-null, it must
    /// be valid for writes.
    pub unsafe fn set_message(dest: *mut Self, message: &str) {
        if !dest.is_null() {
            let error = Self::new(message, None, None);
            unsafe { std::ptr::write_unaligned(dest, error) }
        }
    }

    /// Get message as a String.
    ///
    /// # Safety
    ///
    /// Underlying message null-terminated string must have a valid terminator
    /// and the buffer up to that terminator must be valid for reads.
    pub unsafe fn get_message(&self) -> Option<String> {
        if self.message.is_null() {
            None
        } else {
            let message = unsafe { CStr::from_ptr(self.message) }
                .to_string_lossy()
                .to_string();
            Some(message)
        }
    }
}

/// An error that can be converted into [FFI_AdbcError] and [AdbcStatusCode].
///
/// Can be used in combination with [check_err] when implementing ADBC FFI
/// functions.
pub trait AdbcError {
    /// The status code this error corresponds to.
    fn status_code(&self) -> AdbcStatusCode;

    /// The message associated with the error.
    fn message(&self) -> &str;

    /// A vendor-specific error code. Defaults to always returning `-1`.
    fn vendor_code(&self) -> i32 {
        -1
    }

    /// A SQLSTATE error code, if provided, as defined by the
    /// SQL:2003 standard.  By default, it is set to
    /// `"\0\0\0\0\0"`.
    fn sqlstate(&self) -> [i8; 5] {
        [0, 0, 0, 0, 0]
    }
}

impl<T: AdbcError> From<&T> for FFI_AdbcError {
    fn from(err: &T) -> Self {
        let message: *mut i8 = CString::new(err.message()).unwrap().into_raw();
        Self {
            message,
            vendor_code: err.vendor_code(),
            sqlstate: err.sqlstate(),
            release: Some(drop_adbc_error),
        }
    }
}

impl AdbcError for std::str::Utf8Error {
    fn message(&self) -> &str {
        "Invalid UTF-8 character"
    }

    fn sqlstate(&self) -> [i8; 5] {
        // A character is not in the coded character set or the conversion is not supported.
        [2, 2, 0, 2, 1]
    }

    fn status_code(&self) -> AdbcStatusCode {
        AdbcStatusCode::InvalidArguments
    }

    fn vendor_code(&self) -> i32 {
        -1
    }
}

impl AdbcError for std::ffi::NulError {
    fn message(&self) -> &str {
        "An input string contained an interior nul"
    }

    fn sqlstate(&self) -> [i8; 5] {
        [0; 5]
    }

    fn status_code(&self) -> AdbcStatusCode {
        AdbcStatusCode::InvalidArguments
    }

    fn vendor_code(&self) -> i32 {
        -1
    }
}

impl AdbcError for ArrowError {
    fn message(&self) -> &str {
        match self {
            ArrowError::CDataInterface(msg) => msg,
            ArrowError::SchemaError(msg) => msg,
            _ => "Arrow error", // TODO: Fill in remainder
        }
    }

    fn status_code(&self) -> AdbcStatusCode {
        AdbcStatusCode::Internal
    }
}

unsafe extern "C" fn drop_adbc_error(error: *mut FFI_AdbcError) {
    if let Some(error) = error.as_mut() {
        // Retake pointer so it will drop once out of scope.
        if !error.message.is_null() {
            let _ = CString::from_raw(error.message);
        }
        error.message = null_mut();
    }
}

/// Given a Result, either unwrap the value or handle the error in ADBC function.
///
/// This macro is for use when implementing ADBC methods that have an out
/// parameter for [FFI_AdbcError] and return [AdbcStatusCode]. If the result is
/// `Ok`, the expression resolves to the value. Otherwise, it will return early,
/// setting the error and status code appropriately. In order for this to work,
/// the error must implement [AdbcError].
#[macro_export]
macro_rules! check_err {
    ($res:expr, $err_out:expr) => {
        match $res {
            Ok(x) => x,
            Err(err) => {
                let error = FFI_AdbcError::from(&err);
                unsafe { std::ptr::write_unaligned($err_out, error) };
                return err.status_code();
            }
        }
    };
}

use arrow::error::ArrowError;
pub use check_err;

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use super::*;

    #[test]
    fn test_adbcerror() {
        let cases = vec![
            ("hello", None, None),
            ("", None, None),
            ("unicode 😅", None, None),
            ("msg", Some(20), None),
            ("msg", None, Some([3, 4, 5, 6, 7])),
        ];

        for (msg, vendor_code, sqlstate) in cases {
            let mut err = FFI_AdbcError::new(msg, vendor_code, sqlstate);
            assert_eq!(
                unsafe { CStr::from_ptr(err.message).to_str().unwrap() },
                msg
            );
            assert_eq!(err.vendor_code, vendor_code.unwrap_or(-1));
            assert_eq!(err.sqlstate, sqlstate.unwrap_or([0, 0, 0, 0, 0]));

            assert!(err.release.is_some());
            let release_func = err.release.unwrap();
            unsafe { release_func(&mut err) };

            assert!(err.message.is_null());
        }
    }

    #[test]
    fn test_adbcerror_set_message() {
        let mut error = FFI_AdbcError::empty();

        let msg = "Hello world!";
        unsafe { FFI_AdbcError::set_message(&mut error, msg) };

        assert_eq!(
            unsafe { CStr::from_ptr(error.message).to_str().unwrap() },
            msg
        );
        assert_eq!(error.vendor_code, -1);
        assert_eq!(error.sqlstate, [0, 0, 0, 0, 0]);

        assert!(error.release.is_some());
        let release_func = error.release.unwrap();
        unsafe { release_func(&mut error) };

        assert!(error.message.is_null());
    }
}
