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

//! Snowflake ADBC driver tests
//!
//! These tests are disabled by default because they require a Snowflake
//! account.
//!
//! To enable these tests set the `ADBC_SNOWFLAKE_TESTS` environment variable
//! when building these tests.
//!
//! These tests load the configuration from environment variables:
//! - Driver: [`driver::Builder::from_env`]
//! - Database: [`database::Builder::from_env`]
//! - Connection: ...
//! - Statement: ...
//!
//! These methods are available when the `env` crate feature is enabled.
//!

#[cfg(feature = "env")]
#[test_with::env(ADBC_SNOWFLAKE_TESTS)]
mod tests {
    use adbc_core::{error::Result, Database as _};
    use adbc_snowflake::{database, driver, Connection, Database, Driver};

    fn with_driver(func: impl FnOnce(Driver) -> Result<()>) -> Result<()> {
        driver::Builder::from_env().try_load().and_then(func)
    }

    fn with_database(func: impl FnOnce(Database) -> Result<()>) -> Result<()> {
        with_driver(|mut driver| {
            database::Builder::from_env()
                .build(&mut driver)
                .and_then(func)
        })
    }

    fn with_connection(func: impl FnOnce(Connection) -> Result<()>) -> Result<()> {
        with_database(|mut database| database.new_connection().and_then(func))
    }

    #[test]
    /// Test the configuration by constructing a connection.
    fn connection() -> Result<()> {
        with_connection(|_connection| Ok(()))?;
        Ok(())
    }
}
