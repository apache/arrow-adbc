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

//! Test driver manager against SQLite implementations.

use std::collections::HashMap;
use std::sync::Arc;

use arrow::array::{Int64Array, StringArray};
use arrow::compute::concat_batches;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::error::ArrowError;
use arrow::record_batch::RecordBatch;
use arrow_adbc::driver_manager::{AdbcDriver, DriverDatabase, DriverStatement, Result};
use arrow_adbc::info::{InfoCode, InfoData};
use arrow_adbc::objects::{
    ColumnSchemaRef, DatabaseCatalogCollection, DatabaseCatalogEntry, DatabaseSchemaEntry,
    DatabaseTableEntry,
};
use arrow_adbc::AdbcObjectDepth;
use arrow_adbc::{AdbcConnection, AdbcStatement, ADBC_VERSION_1_0_0};

fn get_driver() -> Result<AdbcDriver> {
    AdbcDriver::load("adbc_driver_sqlite", None, ADBC_VERSION_1_0_0)
}

fn get_database() -> Result<DriverDatabase> {
    let driver = get_driver()?;
    // By passing in "" for uri, we create a distinct temporary database for each
    // test, preventing noisy neighbor issues on tests.
    driver.new_database()?.set_option("uri", "")?.init()
}

#[test]
fn test_database() {
    let driver = get_driver().unwrap();

    let builder = driver
        .new_database()
        .unwrap()
        .set_option("uri", "test.db")
        .unwrap()
        .init()
        .unwrap();

    builder.set_option("uri", "test2.db").unwrap();
}

#[test]
fn test_connection_info() {
    let database = get_database().unwrap();
    let connection = database.new_connection().unwrap().init().unwrap();

    let table_types = connection.get_table_types().unwrap();
    assert_eq!(table_types, vec!["table", "view"]);

    let info = connection
        .get_info(Some(&[InfoCode::DriverName, InfoCode::VendorName]))
        .unwrap();

    let info: HashMap<InfoCode, String> = info
        .into_iter()
        .map(|(code, info)| match info {
            InfoData::StringValue(val) => (code, val.into_owned()),
            _ => unreachable!(),
        })
        .collect();
    assert_eq!(info.len(), 2);
    assert_eq!(
        info.get(&InfoCode::DriverName),
        Some(&"ADBC SQLite Driver".to_string())
    );
    assert_eq!(info.get(&InfoCode::VendorName), Some(&"SQLite".to_string()));
}

fn get_example_data() -> RecordBatch {
    let ints_arr = Arc::new(Int64Array::from(vec![1, 2, 3, 4]));
    let str_arr = Arc::new(StringArray::from(vec!["a", "b", "c", "d"]));
    let schema1 = Schema::new(vec![
        Field::new("ints", DataType::Int64, true),
        Field::new("strs", DataType::Utf8, true),
    ]);
    RecordBatch::try_new(Arc::new(schema1), vec![ints_arr, str_arr]).unwrap()
}

fn upload_data(statement: &mut DriverStatement, data: RecordBatch, name: &str) {
    statement
        .set_option(arrow_adbc::options::AdbcOptionKey::IngestTargetTable, name)
        .unwrap();
    statement.bind_data(&data.into()).unwrap();
    statement.execute_update().unwrap();
}

#[test]
fn test_connection_get_objects() {
    let database = get_database().unwrap();
    let connection = database.new_connection().unwrap().init().unwrap();

    let record_batch = get_example_data();
    let mut statement = connection.new_statement().unwrap();
    upload_data(&mut statement, record_batch, "foo");

    let catalogs = connection
        .get_objects(AdbcObjectDepth::All, None, None, None, None, None)
        .unwrap();

    // There is only 1 catalog
    assert_eq!(catalogs.catalogs().count(), 1);
    let catalog = catalogs.catalogs().next().unwrap();
    assert_eq!(catalog.name(), Some("main"));

    // There is only 1 db_schema
    assert_eq!(catalog.schemas().count(), 1);
    let db_schema = catalog.schemas().next().unwrap();
    assert_eq!(db_schema.name(), None);

    // There is only 1 table
    assert_eq!(db_schema.tables().count(), 1);
    let table = db_schema.tables().next().unwrap();
    assert_eq!(table.name(), "foo");
    assert_eq!(table.table_type(), "table");

    // There are only 2 columns
    assert_eq!(table.columns().count(), 2);
    let columns: HashMap<&str, ColumnSchemaRef> =
        table.columns().map(|col| (col.name, col)).collect();
    assert!(columns.contains_key("ints"));
    assert!(columns.contains_key("strs"));
    assert_eq!(columns.get("ints").unwrap().ordinal_position, 1);
    assert_eq!(columns.get("strs").unwrap().ordinal_position, 2);
}

#[test]
fn test_connection_get_table_schema() {
    let database = get_database().unwrap();
    let connection = database.new_connection().unwrap().init().unwrap();

    let record_batch = get_example_data();
    let mut statement = connection.new_statement().unwrap();
    upload_data(&mut statement, record_batch.clone(), "bar");

    let schema = connection.get_table_schema(None, None, "bar").unwrap();

    assert_eq!(&schema, record_batch.schema().as_ref());
}

#[test]
fn test_prepared() {
    let database = get_database().unwrap();
    let connection = database.new_connection().unwrap().init().unwrap();

    let array = Arc::new(Int64Array::from_iter(vec![1, 2, 3, 4]));

    let mut statement = connection.new_statement().unwrap();
    statement.set_sql_query("SELECT ?").unwrap();
    statement.prepare().unwrap();
    let param_batch = RecordBatch::try_new(
        Arc::new(Schema::new(vec![Field::new("1", DataType::Int64, false)])),
        vec![array.clone()],
    )
    .unwrap();
    statement.bind_data(&param_batch.into()).unwrap();

    let result = statement.execute().unwrap();

    let expected_schema = Schema::new(vec![Field::new("?", DataType::Int64, true)]);
    let result_schema = result.result.as_ref().unwrap().schema();
    assert_eq!(result_schema.as_ref(), &expected_schema);

    let data: Vec<RecordBatch> = result
        .result
        .unwrap()
        .collect::<std::result::Result<_, ArrowError>>()
        .unwrap();
    let data: RecordBatch = concat_batches(&result_schema, data.iter()).unwrap();
    let expected = RecordBatch::try_new(Arc::new(expected_schema), vec![array]).unwrap();
    assert_eq!(data, expected);
}

#[test]
fn test_ingest() {
    let database = get_database().unwrap();
    let connection = database.new_connection().unwrap().init().unwrap();

    let record_batch = get_example_data();
    let mut statement = connection.new_statement().unwrap();
    upload_data(&mut statement, record_batch.clone(), "baz");

    statement.set_sql_query("SELECT * FROM baz").unwrap();
    let result = statement.execute().unwrap();
    let data: Vec<RecordBatch> = result
        .result
        .unwrap()
        .collect::<std::result::Result<_, ArrowError>>()
        .unwrap();
    let data: RecordBatch = concat_batches(&data[0].schema(), data.iter()).unwrap();

    assert_eq!(data, record_batch);
}
