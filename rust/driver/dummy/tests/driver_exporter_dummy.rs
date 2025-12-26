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

/// This integration test compares the output of the dummy driver when it's used
/// directly using the Rust API (native) and through the exported driver via the
/// driver manager (exported). That allows us to test that data correctly round-trip
/// between C and Rust.
use std::ops::Deref;
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, RecordBatchReader, StringArray};
use arrow_schema::{DataType, Field, Schema};
use arrow_select::concat::concat_batches;

use adbc_core::options::{
    AdbcVersion, InfoCode, IngestMode, IsolationLevel, ObjectDepth, OptionConnection,
    OptionDatabase, OptionStatement,
};
use adbc_core::Statement;
use adbc_core::{schemas, Connection, Database, Driver, Optionable};
use adbc_driver_manager::{ManagedConnection, ManagedDatabase, ManagedDriver, ManagedStatement};

use adbc_dummy::{DummyConnection, DummyDatabase, DummyDriver, DummyStatement, SingleBatchReader};

const OPTION_STRING_LONG: &str = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";
const OPTION_BYTES_LONG: &[u8] = b"AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA";

pub fn concat_reader(reader: impl RecordBatchReader) -> RecordBatch {
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&schema, &batches).unwrap()
}

pub fn sample_batch() -> RecordBatch {
    let columns: Vec<Arc<dyn Array>> = vec![
        Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
        Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0, 4.0])),
        Arc::new(StringArray::from(vec!["a", "b", "c", "d"])),
    ];
    let schema = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Float64, true),
        Field::new("c", DataType::Utf8, true),
    ]);
    RecordBatch::try_new(Arc::new(schema), columns).unwrap()
}

fn get_exported() -> (
    ManagedDriver,
    ManagedDatabase,
    ManagedConnection,
    ManagedStatement,
) {
    let mut driver = ManagedDriver::load_dynamic_from_name(
        "adbc_dummy",
        Some(b"AdbcDummyInit"),
        AdbcVersion::V110,
    )
    .unwrap();
    let database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    (driver, database, connection, statement)
}

fn get_native() -> (DummyDriver, DummyDatabase, DummyConnection, DummyStatement) {
    let mut driver = DummyDriver {};
    let database = driver.new_database().unwrap();
    let mut connection = database.new_connection().unwrap();
    let statement = connection.new_statement().unwrap();
    (driver, database, connection, statement)
}

// Database

#[test]
fn test_database_options() {
    let (mut driver, _, _, _) = get_exported();

    // Pre-init options.
    let options = [
        (OptionDatabase::Username, "Alice".into()),
        (OptionDatabase::Password, 42.into()),
        (OptionDatabase::Uri, std::f64::consts::PI.into()),
        (OptionDatabase::Other("pre.bytes".into()), b"Hello".into()),
        (
            OptionDatabase::Other("pre.string.long".into()),
            OPTION_STRING_LONG.into(),
        ),
        (
            OptionDatabase::Other("pre.bytes.long".into()),
            OPTION_BYTES_LONG.into(),
        ),
    ];

    let mut database = driver.new_database_with_opts(options).unwrap();

    let value = database
        .get_option_string(OptionDatabase::Username)
        .unwrap();
    assert_eq!(value, "Alice");

    let value = database.get_option_int(OptionDatabase::Password).unwrap();
    assert_eq!(value, 42);

    let value = database.get_option_double(OptionDatabase::Uri).unwrap();
    assert_eq!(value, std::f64::consts::PI);

    let value = database
        .get_option_bytes(OptionDatabase::Other("pre.bytes".into()))
        .unwrap();
    assert_eq!(value, b"Hello");

    let value = database
        .get_option_string(OptionDatabase::Other("pre.string.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_STRING_LONG);

    let value = database
        .get_option_bytes(OptionDatabase::Other("pre.bytes.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_BYTES_LONG);

    // Post-init options.
    database
        .set_option(OptionDatabase::Other("post.string".into()), "Bob".into())
        .unwrap();
    let value = database
        .get_option_string(OptionDatabase::Other("post.string".into()))
        .unwrap();
    assert_eq!(value, "Bob");

    database
        .set_option(OptionDatabase::Other("post.int".into()), 1337.into())
        .unwrap();
    let value = database
        .get_option_int(OptionDatabase::Other("post.int".into()))
        .unwrap();
    assert_eq!(value, 1337);

    database
        .set_option(OptionDatabase::Other("post.double".into()), 1.41.into())
        .unwrap();
    let value = database
        .get_option_double(OptionDatabase::Other("post.double".into()))
        .unwrap();
    assert_eq!(value, 1.41);

    database
        .set_option(OptionDatabase::Other("post.bytes".into()), b"Bye".into())
        .unwrap();
    let value = database
        .get_option_bytes(OptionDatabase::Other("post.bytes".into()))
        .unwrap();
    assert_eq!(value, b"Bye");

    database
        .set_option(
            OptionDatabase::Other("post.string.long".into()),
            OPTION_STRING_LONG.into(),
        )
        .unwrap();
    let value = database
        .get_option_string(OptionDatabase::Other("post.string.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_STRING_LONG);

    database
        .set_option(
            OptionDatabase::Other("post.bytes.long".into()),
            OPTION_BYTES_LONG.into(),
        )
        .unwrap();
    let value = database
        .get_option_bytes(OptionDatabase::Other("post.bytes.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_BYTES_LONG);
}

// Connection

#[test]
fn test_connection_options() {
    let (_, database, _, _) = get_exported();

    // Pre-init options
    let options = [
        (OptionConnection::CurrentCatalog, "Alice".into()),
        (OptionConnection::AutoCommit, 42.into()),
        (OptionConnection::CurrentSchema, std::f64::consts::PI.into()),
        (
            OptionConnection::IsolationLevel,
            IsolationLevel::Linearizable.into(),
        ),
        (OptionConnection::Other("pre.bytes".into()), b"Hello".into()),
        (OptionConnection::ReadOnly, OPTION_STRING_LONG.into()),
        (
            OptionConnection::Other("pre.bytes.long".into()),
            OPTION_BYTES_LONG.into(),
        ),
    ];
    let mut connection = database.new_connection_with_opts(options).unwrap();

    let value = connection
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();
    assert_eq!(value, "Alice");

    let value = connection
        .get_option_int(OptionConnection::AutoCommit)
        .unwrap();
    assert_eq!(value, 42);

    let value = connection
        .get_option_double(OptionConnection::CurrentSchema)
        .unwrap();
    assert_eq!(value, std::f64::consts::PI);

    let value = connection
        .get_option_string(OptionConnection::IsolationLevel)
        .unwrap();
    assert_eq!(value, Into::<String>::into(IsolationLevel::Linearizable));

    let value = connection
        .get_option_bytes(OptionConnection::Other("pre.bytes".into()))
        .unwrap();
    assert_eq!(value, b"Hello");

    let value = connection
        .get_option_string(OptionConnection::ReadOnly)
        .unwrap();
    assert_eq!(value, OPTION_STRING_LONG);

    let value = connection
        .get_option_bytes(OptionConnection::Other("pre.bytes.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_BYTES_LONG);

    // Post-init options
    connection
        .set_option(OptionConnection::AutoCommit, "true".into())
        .unwrap();
    let value = connection
        .get_option_string(OptionConnection::AutoCommit)
        .unwrap();
    assert_eq!(value, "true");

    connection
        .set_option(OptionConnection::CurrentCatalog, 1337.into())
        .unwrap();
    let value = connection
        .get_option_int(OptionConnection::CurrentCatalog)
        .unwrap();
    assert_eq!(value, 1337);

    connection
        .set_option(OptionConnection::CurrentSchema, 1.41.into())
        .unwrap();
    let value = connection
        .get_option_double(OptionConnection::CurrentSchema)
        .unwrap();
    assert_eq!(value, 1.41);

    connection
        .set_option(OptionConnection::Other("post.bytes".into()), b"Bye".into())
        .unwrap();
    let value = connection
        .get_option_bytes(OptionConnection::Other("post.bytes".into()))
        .unwrap();
    assert_eq!(value, b"Bye");

    connection
        .set_option(
            OptionConnection::Other("post.string.long".into()),
            OPTION_STRING_LONG.into(),
        )
        .unwrap();
    let value = connection
        .get_option_string(OptionConnection::Other("post.string.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_STRING_LONG);

    connection
        .set_option(
            OptionConnection::Other("post.bytes.long".into()),
            OPTION_BYTES_LONG.into(),
        )
        .unwrap();
    let value = connection
        .get_option_bytes(OptionConnection::Other("post.bytes.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_BYTES_LONG);
}

#[test]
fn test_connection_get_table_types() {
    let (_, _, exported_connection, _) = get_exported();
    let (_, _, native_connection, _) = get_native();

    let exported_table_types = concat_reader(exported_connection.get_table_types().unwrap());
    let native_table_types = concat_reader(native_connection.get_table_types().unwrap());

    assert_eq!(
        exported_table_types.schema(),
        *schemas::GET_TABLE_TYPES_SCHEMA.deref()
    );
    assert_eq!(exported_table_types, native_table_types);
}

#[test]
fn test_connection_get_table_schema() {
    let (_, _, exported_connection, _) = get_exported();
    let (_, _, native_connection, _) = get_native();

    let exported_schema = exported_connection
        .get_table_schema(Some("default"), Some("default"), "default")
        .unwrap();
    let native_schema = native_connection
        .get_table_schema(Some("default"), Some("default"), "default")
        .unwrap();

    assert_eq!(exported_schema, native_schema);
}

#[test]
fn test_connection_get_info() {
    let (_, _, exported_connection, _) = get_exported();
    let (_, _, native_connection, _) = get_native();

    let exported_info = concat_reader(exported_connection.get_info(None).unwrap());
    let native_info = concat_reader(native_connection.get_info(None).unwrap());
    assert_eq!(exported_info.schema(), *schemas::GET_INFO_SCHEMA.deref());
    assert_eq!(exported_info, native_info);

    let exported_info = concat_reader(
        exported_connection
            .get_info(Some(
                [InfoCode::DriverAdbcVersion, InfoCode::DriverName].into(),
            ))
            .unwrap(),
    );
    let native_info = concat_reader(
        native_connection
            .get_info(Some(
                [InfoCode::DriverAdbcVersion, InfoCode::DriverName].into(),
            ))
            .unwrap(),
    );
    assert_eq!(exported_info.schema(), *schemas::GET_INFO_SCHEMA.deref());
    assert_eq!(exported_info, native_info);
}

#[test]
fn test_connection_cancel() {
    let (_, _, mut exported_connection, _) = get_exported();
    let (_, _, mut native_connection, _) = get_native();

    let exported_error = exported_connection.cancel().unwrap_err();
    let native_error = native_connection.cancel().unwrap_err();

    assert_eq!(exported_error, native_error);
}

#[test]
fn test_connection_commit_rollback() {
    let (_, _, mut exported_connection, _) = get_exported();
    let (_, _, mut native_connection, _) = get_native();

    exported_connection.commit().unwrap();
    exported_connection.rollback().unwrap();

    native_connection.commit().unwrap();
    native_connection.rollback().unwrap();
}

#[test]
fn test_connection_get_statistic_names() {
    let (_, _, exported_connection, _) = get_exported();
    let (_, _, native_connection, _) = get_native();

    let exported_names = concat_reader(exported_connection.get_statistic_names().unwrap());
    let native_names = concat_reader(native_connection.get_statistic_names().unwrap());

    assert_eq!(
        exported_names.schema(),
        *schemas::GET_STATISTIC_NAMES_SCHEMA.deref()
    );
    assert_eq!(exported_names, native_names);
}

#[test]
fn test_connection_read_partition() {
    let (_, _, exported_connection, _) = get_exported();
    let (_, _, native_connection, _) = get_native();

    let exported_partition = concat_reader(exported_connection.read_partition(b"").unwrap());
    let native_partition = concat_reader(native_connection.read_partition(b"").unwrap());

    assert_eq!(
        exported_partition.schema(),
        exported_connection
            .get_table_schema(None, None, "default")
            .unwrap()
            .into()
    );
    assert_eq!(exported_partition, native_partition);
}

#[test]
fn test_connection_get_statistics() {
    let (_, _, exported_connection, _) = get_exported();
    let (_, _, native_connection, _) = get_native();

    let exported_statistics = concat_reader(
        exported_connection
            .get_statistics(None, None, None, false)
            .unwrap(),
    );
    let native_statistics = concat_reader(
        native_connection
            .get_statistics(None, None, None, false)
            .unwrap(),
    );

    assert_eq!(exported_statistics, native_statistics);
    assert_eq!(
        exported_statistics.schema(),
        schemas::GET_STATISTICS_SCHEMA.clone(),
    );
}

#[test]
fn test_connection_get_objects() {
    let (_, _, exported_connection, _) = get_exported();
    let (_, _, native_connection, _) = get_native();

    let exported_objects = concat_reader(
        exported_connection
            .get_objects(
                ObjectDepth::All,
                None,
                None,
                None,
                Some(vec!["table", "view"]),
                None,
            )
            .unwrap(),
    );
    let native_objects = concat_reader(
        native_connection
            .get_objects(ObjectDepth::All, None, None, None, None, None)
            .unwrap(),
    );

    assert_eq!(exported_objects, native_objects);
    assert_eq!(
        exported_objects.schema(),
        schemas::GET_OBJECTS_SCHEMA.clone(),
    );
}

// Statement

#[test]
fn test_statement_options() {
    let (_, _, _, mut statement) = get_exported();

    statement
        .set_option(OptionStatement::Incremental, "true".into())
        .unwrap();
    let value = statement
        .get_option_string(OptionStatement::Incremental)
        .unwrap();
    assert_eq!(value, "true");

    statement
        .set_option(OptionStatement::TargetTable, 42.into())
        .unwrap();
    let value = statement
        .get_option_int(OptionStatement::TargetTable)
        .unwrap();
    assert_eq!(value, 42);

    statement
        .set_option(OptionStatement::MaxProgress, std::f64::consts::PI.into())
        .unwrap();
    let value = statement
        .get_option_double(OptionStatement::MaxProgress)
        .unwrap();
    assert_eq!(value, std::f64::consts::PI);

    statement
        .set_option(OptionStatement::Other("bytes".into()), b"Hello".into())
        .unwrap();
    let value = statement
        .get_option_bytes(OptionStatement::Other("bytes".into()))
        .unwrap();
    assert_eq!(value, b"Hello");

    statement
        .set_option(OptionStatement::IngestMode, IngestMode::CreateAppend.into())
        .unwrap();
    let value = statement
        .get_option_string(OptionStatement::IngestMode)
        .unwrap();
    assert_eq!(value, Into::<String>::into(IngestMode::CreateAppend));

    statement
        .set_option(
            OptionStatement::Other("bytes.long".into()),
            OPTION_BYTES_LONG.into(),
        )
        .unwrap();
    let value = statement
        .get_option_bytes(OptionStatement::Other("bytes.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_BYTES_LONG);

    statement
        .set_option(
            OptionStatement::Other("string.long".into()),
            OPTION_STRING_LONG.into(),
        )
        .unwrap();
    let value = statement
        .get_option_string(OptionStatement::Other("string.long".into()))
        .unwrap();
    assert_eq!(value, OPTION_STRING_LONG);
}

#[test]
fn test_statement_bind() {
    let (_, _, _, mut exported_statement) = get_exported();
    let (_, _, _, mut native_statement) = get_native();

    let batch = sample_batch();

    exported_statement.bind(batch.clone()).unwrap();
    native_statement.bind(batch).unwrap();
}

#[test]
fn test_statement_bind_stream() {
    let (_, _, _, mut exported_statement) = get_exported();
    let (_, _, _, mut native_statement) = get_native();

    let batch = sample_batch();
    let reader = Box::new(SingleBatchReader::new(batch));
    exported_statement.bind_stream(reader).unwrap();

    let batch = sample_batch();
    let reader = Box::new(SingleBatchReader::new(batch));
    native_statement.bind_stream(reader).unwrap();
}

#[test]
fn test_statement_cancel() {
    let (_, _, _, mut exported_statement) = get_exported();
    let (_, _, _, mut native_statement) = get_native();

    exported_statement.cancel().unwrap();
    native_statement.cancel().unwrap();
}

#[test]
fn test_statement_execute_query() {
    let (_, _, _, mut exported_statement) = get_exported();
    let (_, _, _, mut native_statement) = get_native();

    let exported_data = concat_reader(exported_statement.execute().unwrap());
    let native_data = concat_reader(native_statement.execute().unwrap());
    assert_eq!(exported_data, native_data);

    let exported_data = exported_statement.execute_update().unwrap();
    let native_data = native_statement.execute_update().unwrap();
    assert_eq!(exported_data, native_data);
}

#[test]
fn test_statement_execute_schema() {
    let (_, _, _, mut exported_statement) = get_exported();
    let (_, _, _, mut native_statement) = get_native();

    let exported_schema = exported_statement.execute_schema().unwrap();
    let native_schema = native_statement.execute_schema().unwrap();
    assert_eq!(exported_schema, native_schema);
}

#[test]
fn test_statement_execute_partitions() {
    let (_, _, _, mut exported_statement) = get_exported();
    let (_, _, _, mut native_statement) = get_native();

    let exported_result = exported_statement.execute_partitions().unwrap();
    let native_result = native_statement.execute_partitions().unwrap();
    assert_eq!(exported_result, native_result);
}

#[test]
fn test_statement_prepare() {
    let (_, _, _, mut exported_statement) = get_exported();
    let (_, _, _, mut native_statement) = get_native();

    exported_statement.prepare().unwrap();
    native_statement.prepare().unwrap();
}

#[test]
fn test_statement_set_sql_query() {
    let (_, _, _, mut exported_statement) = get_exported();
    let (_, _, _, mut native_statement) = get_native();

    exported_statement
        .set_sql_query("select * from table")
        .unwrap();
    native_statement
        .set_sql_query("select * from table")
        .unwrap();
}

#[test]
fn test_statement_set_substrait_plan() {
    let (_, _, _, mut exported_statement) = get_exported();
    let (_, _, _, mut native_statement) = get_native();

    exported_statement.set_substrait_plan(b"SCAN").unwrap();
    native_statement.set_substrait_plan(b"SCAN").unwrap();
}

#[test]
fn test_statement_get_parameter_schema() {
    let (_, _, _, exported_statement) = get_exported();
    let (_, _, _, native_statement) = get_native();

    let exported_schema = exported_statement.get_parameter_schema().unwrap();
    let native_schema = native_statement.get_parameter_schema().unwrap();
    assert_eq!(exported_schema, native_schema);
}
