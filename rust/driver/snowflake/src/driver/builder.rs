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

//! A builder for [`Driver`]
//!
//!

use adbc_core::{
    error::{Error, Result},
    options::AdbcVersion,
};

#[cfg(feature = "env")]
use crate::builder::env_parse;
use crate::Driver;

/// A builder for [`Driver`].
///
/// The builder can be used to initialize a [`Driver`] with
/// [`Builder::try_load`].
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct Builder {
    /// The [`AdbcVersion`] version of the driver.
    pub adbc_version: Option<AdbcVersion>,
}

#[cfg(feature = "env")]
impl Builder {
    /// See [`Self::adbc_version`].
    pub const ADBC_VERSION_ENV: &str = "ADBC_SNOWFLAKE_ADBC_VERSION";

    /// Construct a builder, setting values based on values of the
    /// configuration environment variables.
    ///
    /// # Error
    ///
    /// Returns an error when environment variables are set but their values
    /// fail to parse.
    pub fn from_env() -> Result<Self> {
        #[cfg(feature = "dotenv")]
        let _ = dotenvy::dotenv();

        let adbc_version = env_parse(Self::ADBC_VERSION_ENV, str::parse)?;

        Ok(Self { adbc_version })
    }
}

impl Builder {
    /// Use the provided [`AdbcVersion`] when loading the driver.
    pub fn with_adbc_version(mut self, version: AdbcVersion) -> Self {
        self.adbc_version = Some(version);
        self
    }

    /// Try to load the [`Driver`] using the values provided to this builder.
    pub fn try_load(self) -> Result<Driver> {
        Driver::try_new(self.adbc_version.unwrap_or_default())
    }
}

impl TryFrom<Builder> for Driver {
    type Error = Error;

    fn try_from(value: Builder) -> Result<Self> {
        value.try_load()
    }
}

#[cfg(test)]
#[cfg(feature = "env")]
mod tests {
    use std::env;

    use adbc_core::error::Status;

    use super::*;

    #[test]
    fn from_env_parse_error() {
        // Set a value that fails to parse to an AdbcVersion
        env::set_var(Builder::ADBC_VERSION_ENV, "?");
        let result = Builder::from_env();
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err(),
            Error::with_message_and_status("Unknown ADBC version: ?", Status::InvalidArguments)
        );
    }
}
