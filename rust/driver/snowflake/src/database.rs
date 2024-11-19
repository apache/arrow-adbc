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

use adbc_core::{
    driver_manager::ManagedDatabase,
    error::Result,
    options::{OptionConnection, OptionDatabase, OptionValue},
    Optionable,
};

use crate::Connection;

mod builder;
pub use builder::*;

/// Snowflake ADBC Database.
#[derive(Clone)]
pub struct Database(pub(crate) ManagedDatabase);

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

    fn new_connection(&mut self) -> Result<Self::ConnectionType> {
        self.0.new_connection().map(Connection)
    }

    fn new_connection_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (OptionConnection, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        self.0.new_connection_with_opts(opts).map(Connection)
    }
}
