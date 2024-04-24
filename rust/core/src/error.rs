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

//! Error, status and result types.

use std::os::raw::c_char;
use std::{ffi::NulError, fmt::Display};

use arrow::error::ArrowError;

/// Status of an operation.
#[derive(Debug, Copy, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub enum Status {
    /// No error.
    Ok,
    /// An unknown error occurred.
    Unknown,
    /// The operation is not implemented or supported.
    NotImplemented,
    /// A requested resource was not found.
    NotFound,
    /// A requested resource already exists.
    AlreadyExists,
    /// The arguments are invalid, likely a programming error.
    /// For instance, they may be of the wrong format, or out of range.
    InvalidArguments,
    /// The preconditions for the operation are not met, likely a programming error.
    /// For instance, the object may be uninitialized, or may have not
    /// been fully configured.
    InvalidState,
    /// Invalid data was processed (not a programming error).
    /// For instance, a division by zero may have occurred during query
    /// execution.
    InvalidData,
    /// The database's integrity was affected.
    /// For instance, a foreign key check may have failed, or a uniqueness
    /// constraint may have been violated.
    Integrity,
    /// An error internal to the driver or database occurred.
    Internal,
    /// An I/O error occurred.
    /// For instance, a remote service may be unavailable.
    IO,
    /// The operation was cancelled, not due to a timeout.
    Cancelled,
    /// The operation was cancelled due to a timeout.
    Timeout,
    /// Authentication failed.
    Unauthenticated,
    /// The client is not authorized to perform the given operation.
    Unauthorized,
}

/// An ADBC error.
#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Error {
    /// The error message.
    pub message: String,
    /// The status of the operation.
    pub status: Status,
    /// A vendor-specific error code, if applicable.
    pub vendor_code: i32,
    /// A SQLSTATE error code, if provided, as defined by the SQL:2003 standard.
    /// If not set, it should be set to `\0\0\0\0\0`.
    pub sqlstate: [c_char; 5], // TODO(alexandreyc): should we move to something else than c_char? (see https://github.com/apache/arrow-adbc/pull/1725#discussion_r1567531539)
    /// Additional metadata. Introduced in ADBC 1.1.0.
    pub details: Option<Vec<(String, Vec<u8>)>>,
}

/// Result type wrapping [Error].
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn with_message_and_status(message: impl Into<String>, status: Status) -> Self {
        Self {
            message: message.into(),
            status,
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
        }
    }
}

impl Display for Error {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{:?}: {} (sqlstate: {:?}, vendor_code: {})",
            self.status, self.message, self.sqlstate, self.vendor_code
        )
    }
}

impl std::error::Error for Error {}

impl From<ArrowError> for Error {
    fn from(value: ArrowError) -> Self {
        Self {
            message: value.to_string(),
            status: Status::Internal,
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
        }
    }
}

impl From<NulError> for Error {
    fn from(value: NulError) -> Self {
        Self {
            message: format!(
                "Interior null byte was found at position {}",
                value.nul_position()
            ),
            status: Status::InvalidData,
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
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

impl From<std::str::Utf8Error> for Error {
    fn from(value: std::str::Utf8Error) -> Self {
        Self {
            message: format!("Error while decoding UTF-8: {value}"),
            status: Status::Internal,
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
        }
    }
}

impl From<std::ffi::IntoStringError> for Error {
    fn from(value: std::ffi::IntoStringError) -> Self {
        let error = value.utf8_error();
        error.into()
    }
}
