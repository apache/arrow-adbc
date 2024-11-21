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

//! Snowflake ADBC Driver
//!
//!

#[cfg(any(feature = "bundled", feature = "linked"))]
use std::ffi::{c_int, c_void};
use std::{fmt, sync::LazyLock};

#[cfg(any(feature = "bundled", feature = "linked"))]
use adbc_core::ffi::{FFI_AdbcDriverInitFunc, FFI_AdbcError, FFI_AdbcStatusCode};
use adbc_core::{
    driver_manager::ManagedDriver,
    error::Result,
    options::{AdbcVersion, OptionDatabase, OptionValue},
};

use crate::Database;

mod builder;
pub use builder::*;

static DRIVER: LazyLock<Result<ManagedDriver>> = LazyLock::new(|| {
    ManagedDriver::load_dynamic_from_name(
        "adbc_driver_snowflake",
        Some(b"SnowflakeDriverInit"),
        Default::default(),
    )
});

/// Snowflake ADBC Driver.
#[derive(Clone)]
pub struct Driver(ManagedDriver);

/// Panics when the drivers fails to load.
impl Default for Driver {
    fn default() -> Self {
        Self::try_load().expect("driver init")
    }
}

impl fmt::Debug for Driver {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("SnowflakeDriver")
            .field("version", &self.0.version())
            .finish_non_exhaustive()
    }
}

#[cfg(any(feature = "bundled", feature = "linked"))]
extern "C" {
    #[link_name = "SnowflakeDriverInit"]
    fn init(version: c_int, raw_driver: *mut c_void, err: *mut FFI_AdbcError)
        -> FFI_AdbcStatusCode;
}

impl Driver {
    /// Returns a [`Driver`].
    ///
    /// Defaults to the loading the latest [`AdbcVersion`]. To load the
    /// [`Driver`] with a different version use [`Builder::with_adbc_version`],
    /// or to load the configuration from environment variables use
    /// [`Builder::from_env`].
    ///
    /// If the crate was built without the `bundled` and `linked` features this
    /// will attempt to dynamically load the driver.
    ///
    /// # Error
    ///
    /// Returns an error when the driver fails to load.
    ///
    /// # Example
    ///
    /// ## Using the default ADBC version
    ///
    /// ```rust
    /// # use adbc_core::error::Result;
    /// # use adbc_snowflake::Driver;
    /// # fn main() -> Result<()> {
    /// let mut driver = Driver::try_load()?;
    /// # Ok(()) }
    /// ```
    ///
    /// ## Using a different ADBC version
    ///
    /// ```rust
    /// # use adbc_core::{error::Result, options::AdbcVersion};
    /// # use adbc_snowflake::{driver::Builder, Driver};
    /// # fn main() -> Result<()> {
    /// let mut driver = Builder::default()
    ///    .with_adbc_version(AdbcVersion::V100)
    ///    .try_load()?;
    /// # Ok(()) }
    /// ```
    pub fn try_load() -> Result<Self> {
        Self::try_new(Default::default())
    }

    fn try_new(version: AdbcVersion) -> Result<Self> {
        // Load the bundled or linked driver.
        #[cfg(any(feature = "bundled", feature = "linked"))]
        {
            let driver_init: FFI_AdbcDriverInitFunc = init;
            ManagedDriver::load_static(&driver_init, version).map(Self)
        }
        // Fallback: attempt to dynamically load the driver.
        #[cfg(not(any(feature = "bundled", feature = "linked")))]
        {
            let _ = version;
            Self::try_new_dynamic()
        }
    }

    fn try_new_dynamic() -> Result<Self> {
        DRIVER.clone().map(Self)
    }

    /// Returns a dynamically loaded [`Driver`].
    ///
    /// This attempts to load the `adbc_driver_snowflake` library using the
    /// default `AdbcVersion`.
    ///
    /// # Error
    ///
    /// Returns an error when the driver fails to load.
    ///
    /// # Example
    ///
    /// ```rust,no_run
    /// # use adbc_core::error::Result;
    /// # use adbc_snowflake::Driver;
    /// # fn main() -> Result<()> {
    /// let mut driver = Driver::try_load_dynamic()?;
    /// # Ok(()) }
    /// ```
    pub fn try_load_dynamic() -> Result<Self> {
        Self::try_new_dynamic()
    }
}

impl adbc_core::Driver for Driver {
    type DatabaseType = Database;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        self.0.new_database().map(Database)
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        self.0.new_database_with_opts(opts).map(Database)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn try_load(version: AdbcVersion) -> Result<()> {
        Builder::default().with_adbc_version(version).try_load()?;
        Ok(())
    }

    #[test]
    fn load_v1_0_0() -> Result<()> {
        try_load(AdbcVersion::V100)
    }

    #[test]
    fn load_v1_1_0() -> Result<()> {
        try_load(AdbcVersion::V110)
    }

    #[test]
    #[cfg_attr(
        not(feature = "linked"),
        ignore = "because the `linked` feature is not enabled"
    )]
    fn dynamic() -> Result<()> {
        let _a = Driver::try_load_dynamic()?;
        let _b = Driver::try_load_dynamic()?;
        Ok(())
    }
}
