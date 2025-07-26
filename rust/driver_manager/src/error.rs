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

use std::fmt;

use adbc_core::error::Status;

/// A Driver Manager error.
#[derive(Debug, PartialEq, Eq, Clone)]
pub enum Error {
    Adbc(adbc_core::error::Error),
    DynamicLibrary(String),
    Other(String),
}

/// Result type wrapping [Error].
pub type Result<T> = std::result::Result<T, Error>;

impl Error {
    pub fn with_message_and_status(message: impl Into<String>, status: Status) -> Self {
        (adbc_core::error::Error {
            message: message.into(),
            status,
            vendor_code: 0,
            sqlstate: [0; 5],
            details: None,
        })
        .into()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Adbc(e) => write!(f, "ADBC error: {e}"),
            Error::DynamicLibrary(s) => write!(f, "Error with dynamic library: {s}"),
            Error::Other(s) => write!(f, "Other error: {s}"),
        }
    }
}

impl std::error::Error for Error {}

impl From<adbc_core::error::Error> for Error {
    fn from(e: adbc_core::error::Error) -> Self {
        Error::Adbc(e)
    }
}

impl From<libloading::Error> for Error {
    fn from(e: libloading::Error) -> Self {
        Error::DynamicLibrary(e.to_string())
    }
}

impl From<Error> for adbc_core::error::Error {
    fn from(e: Error) -> Self {
        match e {
            Error::Adbc(e) => e,
            Error::DynamicLibrary(s) => adbc_core::error::Error {
                message: s,
                status: Status::Internal,
                vendor_code: 0,
                sqlstate: [0; 5],
                details: None,
            },
            Error::Other(s) => adbc_core::error::Error {
                message: s,
                status: Status::Internal,
                vendor_code: 0,
                sqlstate: [0; 5],
                details: None,
            },
        }
    }
}
