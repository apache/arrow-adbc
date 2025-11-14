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

//! Snowflake ADBC Database
//!
//!

use std::{collections::HashSet, ffi::c_int, sync::Arc};

use adbc_core::{
    error::{Error, Result, Status},
    options::{AdbcVersion, InfoCode, OptionConnection, OptionDatabase, OptionValue},
    Connection as _, Database as _, Optionable,
};
use adbc_driver_manager::ManagedDatabase;
use arrow_array::{
    cast::AsArray,
    types::{Int64Type, UInt32Type},
    Array,
};

use crate::Connection;

mod builder;
pub use builder::*;

/// Snowflake ADBC Database.
#[derive(Clone)]
pub struct Database(pub(crate) ManagedDatabase);

impl Database {
    fn get_info(&self, info_code: InfoCode) -> Result<Arc<dyn Array>> {
        self.new_connection()?
            .get_info(Some(HashSet::from_iter([info_code])))?
            .next()
            .ok_or(Error::with_message_and_status(
                "failed to get info",
                Status::Internal,
            ))?
            .map_err(Into::into)
            .and_then(|record_batch| {
                if InfoCode::try_from(record_batch.column(0).as_primitive::<UInt32Type>().value(0))?
                    == info_code
                {
                    Ok(record_batch.column(1).as_union().value(0))
                } else {
                    Err(Error::with_message_and_status(
                        "invalid get info reply",
                        Status::Internal,
                    ))
                }
            })
    }

    /// Returns the name of the vendor.
    pub fn vendor_name(&mut self) -> Result<String> {
        self.get_info(InfoCode::VendorName)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }

    /// Returns the version of the vendor.
    pub fn vendor_version(&mut self) -> Result<String> {
        self.get_info(InfoCode::VendorVersion)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }

    /// Returns the Arrow version of the vendor.
    pub fn vendor_arrow_version(&mut self) -> Result<String> {
        self.get_info(InfoCode::VendorArrowVersion)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }

    /// Returns true if SQL queries are supported.
    pub fn vendor_sql(&mut self) -> Result<bool> {
        self.get_info(InfoCode::VendorSql)
            .map(|array| array.as_boolean().value(0))
    }

    /// Returns true if Substrait queries are supported.
    pub fn vendor_substrait(&mut self) -> Result<bool> {
        self.get_info(InfoCode::VendorSubstrait)
            .map(|array| array.as_boolean().value(0))
    }

    /// Returns the name of the wrapped Go driver.
    pub fn driver_name(&mut self) -> Result<String> {
        self.get_info(InfoCode::DriverName)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }

    /// Returns the version of the wrapped Go driver.
    pub fn driver_version(&mut self) -> Result<String> {
        self.get_info(InfoCode::DriverVersion)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }

    /// Returns the Arrow version of the wrapped Go driver.
    pub fn driver_arrow_version(&mut self) -> Result<String> {
        self.get_info(InfoCode::DriverArrowVersion)
            .map(|array| array.as_string::<i32>().value(0).to_owned())
    }

    /// Returns the [`AdbcVersion`] reported by the driver.
    pub fn adbc_version(&self) -> Result<AdbcVersion> {
        self.new_connection()?
            .get_info(Some(HashSet::from_iter([InfoCode::DriverAdbcVersion])))?
            .next()
            .ok_or(Error::with_message_and_status(
                "failed to get info",
                Status::Internal,
            ))?
            .map_err(Into::into)
            .and_then(|record_batch| {
                assert_eq!(
                    record_batch.column(0).as_primitive::<UInt32Type>().value(0),
                    u32::from(&InfoCode::DriverAdbcVersion)
                );
                AdbcVersion::try_from(
                    record_batch
                        .column(1)
                        .as_union()
                        .value(0)
                        .as_primitive::<Int64Type>()
                        .value(0) as c_int,
                )
            })
    }
}

impl Optionable for Database {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        self.0.set_option(key, value)
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        self.0.get_option_string(key)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        self.0.get_option_bytes(key)
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        self.0.get_option_int(key)
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        self.0.get_option_double(key)
    }
}

impl adbc_core::Database for Database {
    type ConnectionType = Connection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        self.0.new_connection().map(Connection)
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        self.0.new_connection_with_opts(opts).map(Connection)
    }
}
