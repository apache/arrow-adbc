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

use arrow_schema::{Field, Schema};

use adbc_core::options::{AdbcVersion, OptionConnection, OptionDatabase};
use adbc_core::{error::Status, Driver, Optionable};
use adbc_core::{Connection, Database, Statement, LOAD_FLAG_DEFAULT};
use adbc_driver_manager::{ManagedDatabase, ManagedDriver};

mod common;

// By passing in ":memory:" for URI, we create a distinct temporary database for
// each test, preventing noisy neighbor issues on tests.
const URI: &str = ":memory:";

fn get_driver() -> ManagedDriver {
    ManagedDriver::load_dynamic_from_name("adbc_driver_sqlite", None, AdbcVersion::V100).unwrap()
}

fn get_database(driver: &mut ManagedDriver) -> ManagedDatabase {
    let opts = [(OptionDatabase::Uri, URI.into())];
    driver.new_database_with_opts(opts).unwrap()
}

#[test]
fn test_driver() {
    let mut driver = get_driver();
    common::test_driver(&mut driver, URI);
}

#[test]
fn test_database() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    common::test_database(&database);
}

#[test]
fn test_database_implicit_uri() {
    let database = ManagedDatabase::from_uri(
        "adbc_driver_sqlite:file::memory:",
        None,
        AdbcVersion::V100,
        LOAD_FLAG_DEFAULT,
        None,
    )
    .unwrap();
    common::test_database(&database);
}

#[test]
fn test_database_get_option() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);

    let error = database
        .get_option_bytes(OptionDatabase::Username)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = database
        .get_option_string(OptionDatabase::Username)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = database
        .get_option_int(OptionDatabase::Username)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = database
        .get_option_double(OptionDatabase::Username)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_connection() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    common::test_connection(&mut connection);
}

#[test]
fn test_connection_get_option() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();

    let error = connection
        .get_option_bytes(OptionConnection::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = connection
        .get_option_string(OptionConnection::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = connection
        .get_option_int(OptionConnection::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);

    let error = connection
        .get_option_double(OptionConnection::AutoCommit)
        .unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_connection_cancel() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();

    let error = connection.cancel().unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_connection_commit_rollback() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    common::test_connection_commit_rollback(&mut connection);
}

#[test]
fn test_connection_read_partition() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_read_partition(&connection);
}

#[test]
fn test_connection_get_table_types() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_get_table_types(&connection, &["table", "view"]);
}

#[test]
fn test_connection_get_info() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_get_info(&connection, 6);
}

#[test]
fn test_connection_get_objects() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();
    common::test_connection_get_objects(&connection, 1, 1);
}

#[test]
fn test_connection_get_table_schema() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    common::test_connection_get_table_schema(&mut connection);
}

#[test]
fn test_connection_get_statistic_names() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();
    assert!(connection.get_statistic_names().is_err());
}

#[test]
fn test_connection_get_statistics() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let connection = database.new_connection().unwrap();
    assert!(connection.get_statistics(None, None, None, false).is_err());
}

#[test]
fn test_statement() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement(&mut statement);
}

#[test]
fn test_statement_prepare() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_prepare(&mut statement);
}

#[test]
fn test_statement_set_substrait_plan() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_set_substrait_plan(&mut statement);
}

#[test]
fn test_statement_get_parameter_schema() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    let error = statement.get_parameter_schema().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);

    statement.set_sql_query("select 42").unwrap();
    statement.prepare().unwrap();
    let got = statement.get_parameter_schema().unwrap();
    let fields: Vec<Field> = vec![];
    let actual = Schema::new(fields);
    assert_eq!(got, actual);
}

#[test]
fn test_statement_execute() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_execute(&mut statement);
}

#[test]
fn test_statement_execute_update() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    common::test_statement_execute_update(&mut connection);
}

#[test]
fn test_statement_execute_schema() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    let error = statement.execute_schema().unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_statement_execute_partitions() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_execute_partitions(&mut statement);
}

#[test]
fn test_statement_cancel() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();

    let error = statement.cancel().unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

#[test]
fn test_statement_bind() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_bind(&mut statement);
}

#[test]
fn test_statement_bind_stream() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    let mut statement = connection.new_statement().unwrap();
    common::test_statement_bind_stream(&mut statement);
}

#[test]
fn test_ingestion_roundtrip() {
    let mut driver = get_driver();
    let database = get_database(&mut driver);
    let mut connection = database.new_connection().unwrap();
    common::test_ingestion_roundtrip(&mut connection);
}
