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

use std::result;

use arrow_adbc_sys as adbc;
use arrow_schema::ArrowError;

/// An ADBC result type with [Error] as failure type.
pub type Result<T> = result::Result<T, Error>;

#[derive(Debug, PartialEq, Copy, Clone)]
#[repr(u8)]
pub enum StatusCode {
    /// No error.
    Ok = adbc::ADBC_STATUS_OK as adbc::AdbcStatusCode,
    /// An unknown error occurred.
    ///
    /// May indicate a driver-side or database-side error.
    Unknown = adbc::ADBC_STATUS_UNKNOWN as adbc::AdbcStatusCode,
    /// The operation is not implemented or supported.
    ///
    /// May indicate a driver-side or database-side error.
    NotImplemented = adbc::ADBC_STATUS_NOT_IMPLEMENTED as adbc::AdbcStatusCode,
    /// A requested resource was not found.
    ///
    /// May indicate a driver-side or database-side error.
    NotFound = adbc::ADBC_STATUS_NOT_FOUND as adbc::AdbcStatusCode,
    /// A requested resource already exists.
    ///
    /// May indicate a driver-side or database-side error.
    AlreadyExists = adbc::ADBC_STATUS_ALREADY_EXISTS as adbc::AdbcStatusCode,
    /// The arguments are invalid, likely a programming error.
    ///
    /// May indicate a driver-side or database-side error.
    InvalidArgument = adbc::ADBC_STATUS_INVALID_ARGUMENT as adbc::AdbcStatusCode,
    /// The preconditions for the operation are not met, likely a
    ///   programming error.
    ///
    /// For instance, the object may be uninitialized, or may have not
    /// been fully configured.
    ///
    /// May indicate a driver-side or database-side error.
    InvalidState = adbc::ADBC_STATUS_INVALID_STATE as adbc::AdbcStatusCode,
    /// Invalid data was processed (not a programming error).
    ///
    /// For instance, a division by zero may have occurred during query
    /// execution.
    ///
    /// May indicate a database-side error only.
    InvalidData = adbc::ADBC_STATUS_INVALID_DATA as adbc::AdbcStatusCode,
    /// The database's integrity was affected.
    ///
    /// For instance, a foreign key check may have failed, or a uniqueness
    /// constraint may have been violated.
    ///
    /// May indicate a database-side error only.
    Integrity = adbc::ADBC_STATUS_INTEGRITY as adbc::AdbcStatusCode,
    /// An error internal to the driver or database occurred.
    ///
    /// May indicate a driver-side or database-side error.
    Internal = adbc::ADBC_STATUS_INTERNAL as adbc::AdbcStatusCode,
    /// An I/O error occurred.
    ///
    /// For instance, a remote service may be unavailable.
    ///
    /// May indicate a driver-side or database-side error.
    IO = adbc::ADBC_STATUS_IO as adbc::AdbcStatusCode,
    /// The operation was cancelled, not due to a timeout.
    ///
    /// May indicate a driver-side or database-side error.
    Cancelled = adbc::ADBC_STATUS_CANCELLED as adbc::AdbcStatusCode,
    /// The operation was cancelled due to a timeout.
    ///
    /// May indicate a driver-side or database-side error.
    Timeout = adbc::ADBC_STATUS_TIMEOUT as adbc::AdbcStatusCode,
    /// Authentication failed.
    ///
    /// May indicate a database-side error only.
    Unauthenticated = adbc::ADBC_STATUS_UNAUTHENTICATED as adbc::AdbcStatusCode,
    /// The client is not authorized to perform the given operation.
    ///
    /// May indicate a database-side error only.
    Unauthorized = adbc::ADBC_STATUS_UNAUTHORIZED as adbc::AdbcStatusCode,
}

impl std::fmt::Display for StatusCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            StatusCode::Ok => write!(f, "Ok"),
            StatusCode::Unknown => write!(f, "Unknown"),
            StatusCode::NotImplemented => write!(f, "Not Implemented"),
            StatusCode::NotFound => write!(f, "Not Found"),
            StatusCode::AlreadyExists => write!(f, "Already Exists"),
            StatusCode::InvalidArgument => write!(f, "Invalid Argument"),
            StatusCode::InvalidState => write!(f, "Invalid State"),
            StatusCode::InvalidData => write!(f, "Invalid Data"),
            StatusCode::Integrity => write!(f, "Integrity"),
            StatusCode::Internal => write!(f, "Internal Error"),
            StatusCode::IO => write!(f, "IO Error"),
            StatusCode::Cancelled => write!(f, "Cancelled"),
            StatusCode::Timeout => write!(f, "Timeout"),
            StatusCode::Unauthenticated => write!(f, "Unauthenticated"),
            StatusCode::Unauthorized => write!(f, "Unauthorized"),
        }
    }
}

/// An error from an ADBC driver.
#[derive(Debug, Clone)]
pub struct Error {
    /// An error message
    pub message: String,
    /// A vendor-specific error code.
    pub vendor_code: i32,
    /// A SQLSTATE error code, if provided, as defined by the SQL:2003 standard.
    /// If not set, it is left as `[0; 5]`.
    pub sqlstate: [i8; 5usize],
    /// The status code indicating the type of error.
    pub status_code: StatusCode,
}

impl std::fmt::Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{}: {} (sqlstate: {:?}, vendor_code: {})",
            self.status_code, self.message, self.sqlstate, self.vendor_code
        )
    }
}

impl std::error::Error for Error {}

impl From<std::str::Utf8Error> for Error {
    fn from(value: std::str::Utf8Error) -> Self {
        Self {
            message: format!(
                "Invalid UTF-8 character at position {}",
                value.valid_up_to()
            ),
            vendor_code: -1,
            // A character is not in the coded character set or the conversion is not supported.
            sqlstate: [2, 2, 0, 2, 1],
            status_code: StatusCode::InvalidArgument,
        }
    }
}

impl From<std::ffi::NulError> for Error {
    fn from(value: std::ffi::NulError) -> Self {
        Self {
            message: format!(
                "An input string contained an interior nul at position {}",
                value.nul_position()
            ),
            vendor_code: -1,
            sqlstate: [0; 5],
            status_code: StatusCode::InvalidArgument,
        }
    }
}

impl From<ArrowError> for Error {
    fn from(value: ArrowError) -> Self {
        let message = match value {
            ArrowError::CDataInterface(msg) => msg,
            ArrowError::SchemaError(msg) => msg,
            _ => "Arrow error".to_string(), // TODO: Fill in remainder
        };

        Self {
            message,
            vendor_code: -1,
            sqlstate: [0; 5],
            status_code: StatusCode::Internal,
        }
    }
}
