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

//! ADBC error enums and structs

use arrow_schema::ArrowError;

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

/// An error from an ADBC driver.
#[derive(Debug, Clone)]
pub struct AdbcError {
    /// An error message
    pub message: String,
    /// A vendor-specific error code.
    pub vendor_code: i32,
    /// A SQLSTATE error code, if provided, as defined by the SQL:2003 standard.
    /// If not set, it is left as `[0; 5]`.
    pub sqlstate: [i8; 5usize],
    /// The status code indicating the type of error.
    pub status_code: AdbcStatusCode,
}

impl std::fmt::Display for AdbcError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (sqlstate: {:?}, vendor_code: {})",
            self.status_code, self.message, self.sqlstate, self.vendor_code
        )
    }
}

impl std::error::Error for AdbcError {
    fn description(&self) -> &str {
        &self.message
    }
}

impl From<std::str::Utf8Error> for AdbcError {
    fn from(value: std::str::Utf8Error) -> Self {
        Self {
            message: format!(
                "Invalid UTF-8 character at position {}",
                value.valid_up_to()
            ),
            vendor_code: -1,
            // A character is not in the coded character set or the conversion is not supported.
            sqlstate: [2, 2, 0, 2, 1],
            status_code: AdbcStatusCode::InvalidArguments,
        }
    }
}

impl From<std::ffi::NulError> for AdbcError {
    fn from(value: std::ffi::NulError) -> Self {
        Self {
            message: format!(
                "An input string contained an interior nul at position {}",
                value.nul_position()
            ),
            vendor_code: -1,
            sqlstate: [0; 5],
            status_code: AdbcStatusCode::InvalidArguments,
        }
    }
}

impl From<ArrowError> for AdbcError {
    fn from(value: ArrowError) -> Self {
        let message = format!("Arrow error: {}", value.to_string());
        Self {
            message,
            vendor_code: -1,
            sqlstate: [0; 5],
            status_code: AdbcStatusCode::Internal,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::ffi::CStr;

    use crate::ffi::FFI_AdbcError;

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
