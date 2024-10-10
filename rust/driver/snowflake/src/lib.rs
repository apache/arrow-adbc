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

//! Snowflake ADBC driver, based on the Go driver.

mod driver;
pub use driver::*;

mod database;
pub use database::*;

mod connection;
pub use connection::*;

mod statement;
pub use statement::*;

#[cfg(test)]
mod tests {
    use adbc_core::{error::Result, options::AdbcVersion};

    use super::*;

    #[test]
    fn load_driver() -> Result<()> {
        SnowflakeDriver::try_new(AdbcVersion::V100)?;
        SnowflakeDriver::try_new(AdbcVersion::V110)?;
        Ok(())
    }
}
