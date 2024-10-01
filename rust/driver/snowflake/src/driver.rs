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

use std::ffi::{c_int, c_void};

use adbc_core::{
    driver_manager::ManagedDriver,
    error::Result,
    ffi::{FFI_AdbcDriverInitFunc, FFI_AdbcError, FFI_AdbcStatusCode},
    options::{AdbcVersion, OptionDatabase, OptionValue},
    Driver,
};

use crate::SnowflakeDatabase;

/// Snowflake ADBC Driver.
pub struct SnowflakeDriver(ManagedDriver);

#[link(name = "snowflake", kind = "static")]
extern "C" {
    #[link_name = "SnowflakeDriverInit"]
    fn init(version: c_int, raw_driver: *mut c_void, err: *mut FFI_AdbcError)
        -> FFI_AdbcStatusCode;
}

impl SnowflakeDriver {
    /// Returns a [`SnowflakeDriver`] using the provided [`AdbcVersion`].
    pub fn try_new(version: AdbcVersion) -> Result<Self> {
        let driver_init: FFI_AdbcDriverInitFunc = init;
        ManagedDriver::load_static(&driver_init, version).map(Self)
    }
}

impl Driver for SnowflakeDriver {
    type DatabaseType = SnowflakeDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        self.0.new_database().map(SnowflakeDatabase)
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionDatabase, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        self.0.new_database_with_opts(opts).map(SnowflakeDatabase)
    }
}
