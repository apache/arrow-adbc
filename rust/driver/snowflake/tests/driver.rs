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
//! - Driver: [`adbc_snowflake::driver::Builder::from_env`]
//! - Database: [`adbc_snowflake::database::Builder::from_env`]
//! - Connection: [`adbc_snowflake::connection::Builder::from_env`]
//! - Statement: ...
//!
//! These methods are available when the `env` crate feature is enabled.
//!

#[cfg(feature = "env")]
mod tests {
    use std::{collections::HashSet, ops::Deref, sync::LazyLock};

    use adbc_core::{
        error::{Error, Result},
        options::AdbcVersion,
        Connection as _, Statement as _,
    };
    use adbc_snowflake::{connection, database, driver, Connection, Database, Driver, Statement};
    use arrow_array::{cast::AsArray, types::Decimal128Type};

    const ADBC_VERSION: AdbcVersion = AdbcVersion::V110;

    static DRIVER: LazyLock<Result<Driver>> = LazyLock::new(|| {
        driver::Builder::from_env()?
            .with_adbc_version(ADBC_VERSION)
            .try_load()
    });

    static DATABASE: LazyLock<Result<Database>> =
        LazyLock::new(|| database::Builder::from_env()?.build(&mut DRIVER.deref().clone()?));

    static CONNECTION: LazyLock<Result<Connection>> =
        LazyLock::new(|| connection::Builder::from_env()?.build(&mut DATABASE.deref().clone()?));

    fn with_database(func: impl FnOnce(Database) -> Result<()>) -> Result<()> {
        DATABASE.deref().clone().and_then(func)
    }

    fn with_connection(func: impl FnOnce(Connection) -> Result<()>) -> Result<()> {
        // This always clones the connection because connection methods require
        // exclusive access (&mut Connection). The alternative would be an
        // `Arc<Mutex<Connection>>` however any test failure is a panic and
        // would trigger mutex poisoning.
        //
        // TODO(mbrobbel): maybe force interior mutability via the core traits?
        CONNECTION.deref().clone().and_then(func)
    }

    fn with_empty_statement(func: impl FnOnce(Statement) -> Result<()>) -> Result<()> {
        with_connection(|mut connection| connection.new_statement().and_then(func))
    }

    #[test_with::env(ADBC_SNOWFLAKE_TESTS)]
    /// Check the returned info by the driver using the database methods.
    fn database_get_info() -> Result<()> {
        with_database(|mut database| {
            assert_eq!(database.vendor_name(), Ok("Snowflake".to_owned()));
            assert!(database
                .vendor_version()
                .is_ok_and(|version| version.starts_with("v")));
            assert!(database.vendor_arrow_version().is_ok());
            assert_eq!(database.vendor_sql(), Ok(true));
            assert_eq!(database.vendor_substrait(), Ok(false));
            assert_eq!(
                database.driver_name(),
                Ok("ADBC Snowflake Driver - Go".to_owned())
            );
            assert!(database.driver_version().is_ok());
            assert!(database
                .driver_arrow_version()
                .is_ok_and(|version| version.starts_with("v")));
            assert_eq!(database.adbc_version(), Ok(ADBC_VERSION));
            Ok(())
        })
    }

    #[test_with::env(ADBC_SNOWFLAKE_TESTS)]
    /// Check execute of statement with `SELECT 21 + 21` query.
    fn statement_execute() -> Result<()> {
        with_empty_statement(|mut statement| {
            statement.set_sql_query("SELECT 21 + 21")?;
            let batch = statement
                .execute()?
                .next()
                .expect("a record batch")
                .map_err(Error::from)?;
            assert_eq!(
                batch.column(0).as_primitive::<Decimal128Type>().value(0),
                42
            );
            Ok(())
        })
    }

    #[test_with::env(ADBC_SNOWFLAKE_TESTS)]
    /// Check execute schema of statement with `SHOW WAREHOUSES` query.
    fn statement_execute_schema() -> Result<()> {
        with_empty_statement(|mut statement| {
            statement.set_sql_query("SHOW WAREHOUSES")?;
            let schema = statement.execute_schema()?;
            let field_names = schema
                .fields()
                .into_iter()
                .map(|field| field.name().as_ref())
                .collect::<HashSet<_>>();
            let expected_field_names = [
                "name",
                "state",
                "type",
                "size",
                "running",
                "queued",
                "is_default",
                "is_current",
                "auto_suspend",
                "auto_resume",
                "available",
                "provisioning",
                "quiescing",
                "other",
                "created_on",
                "resumed_on",
                "updated_on",
                "owner",
                "comment",
                "resource_monitor",
                "actives",
                "pendings",
                "failed",
                "suspended",
                "uuid",
                "budget",
                "owner_role_type",
            ]
            .into_iter()
            .collect::<HashSet<_>>();
            assert_eq!(
                expected_field_names
                    .difference(&field_names)
                    .collect::<Vec<_>>(),
                Vec::<&&str>::default()
            );
            Ok(())
        })
    }
}
