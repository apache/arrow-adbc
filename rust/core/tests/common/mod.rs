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

use std::collections::HashSet;
use std::ops::Deref;
use std::sync::Arc;

use adbc_core::driver_manager::{
    ManagedConnection, ManagedDatabase, ManagedDriver, ManagedStatement,
};
use adbc_core::error::Status;
use adbc_core::options::{
    InfoCode, IngestMode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement,
};
use adbc_core::schemas;
use adbc_core::{Connection, Database, Driver, Optionable, Statement};

use arrow_array::{
    cast::as_string_array, Array, Float64Array, Int64Array, RecordBatch, RecordBatchReader,
    StringArray,
};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef};
use arrow_select::concat::concat_batches;

pub struct SingleBatchReader {
    batch: Option<RecordBatch>,
    schema: SchemaRef,
}

impl SingleBatchReader {
    pub fn new(batch: RecordBatch) -> Self {
        let schema = batch.schema();
        Self {
            batch: Some(batch),
            schema,
        }
    }
}

impl Iterator for SingleBatchReader {
    // `RecordBatchReader` requires item to be wrapped within `Result`.
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        Ok(self.batch.take()).transpose()
    }
}

impl RecordBatchReader for SingleBatchReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
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

pub fn concat_reader(reader: impl RecordBatchReader) -> RecordBatch {
    let schema = reader.schema();
    let batches: Vec<RecordBatch> = reader.map(|b| b.unwrap()).collect();
    concat_batches(&schema, &batches).unwrap()
}

pub fn test_driver(driver: &mut ManagedDriver, uri: &str) {
    let opts = [(OptionDatabase::Uri, uri.into())];
    driver.new_database_with_opts(opts).unwrap();

    // Unknown database option.
    let opts = [(OptionDatabase::Other("unknown".into()), "".into())];
    assert!(driver.new_database_with_opts(opts).is_err());
}

pub fn test_database(database: &ManagedDatabase) {
    assert!(database.new_connection().is_ok());

    let opts = [(OptionConnection::AutoCommit, "true".into())];
    database.new_connection_with_opts(opts).unwrap();

    // Unknown connection option.
    let opts = [(OptionConnection::Other("unknown".into()), "".into())];
    assert!(database.new_connection_with_opts(opts).is_err());
}

pub fn test_connection(connection: &mut ManagedConnection) {
    assert!(connection
        .set_option(OptionConnection::AutoCommit, "true".into())
        .is_ok());

    // Unknown connection option
    assert!(connection
        .set_option(OptionConnection::Other("unknown".into()), "".into())
        .is_err());

    assert!(connection.new_statement().is_ok());
}

pub fn test_connection_commit_rollback(connection: &mut ManagedConnection) {
    let error = connection.commit().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);

    let error = connection.rollback().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);

    connection
        .set_option(OptionConnection::AutoCommit, "false".into())
        .unwrap();

    connection.commit().unwrap();
    connection.rollback().unwrap();
}

pub fn test_connection_read_partition(connection: &ManagedConnection) {
    assert!(connection.read_partition(b"").is_err());
}

pub fn test_connection_get_table_types(connection: &ManagedConnection, actual: &[&str]) {
    let got = concat_reader(connection.get_table_types().unwrap());
    assert_eq!(got.num_columns(), 1);
    assert_eq!(got.schema(), *schemas::GET_TABLE_TYPES_SCHEMA.deref());

    let got: Vec<Option<&str>> = as_string_array(got.column(0)).iter().collect();
    assert!(got.iter().all(|x| x.is_some()));

    let got: HashSet<&str> = got.into_iter().map(|x| x.unwrap()).collect();
    let actual: HashSet<&str> = actual.iter().copied().collect();
    assert_eq!(got, actual);
}

pub fn test_connection_get_info(connection: &ManagedConnection, actual_num_info: usize) {
    let info = concat_reader(connection.get_info(None).unwrap());
    assert_eq!(info.num_columns(), 2);
    assert_eq!(info.num_rows(), actual_num_info);
    assert_eq!(info.schema(), *schemas::GET_INFO_SCHEMA.deref());

    let info = concat_reader(
        connection
            .get_info(Some(
                [
                    InfoCode::VendorName,
                    InfoCode::DriverVersion,
                    InfoCode::DriverName,
                    InfoCode::VendorVersion,
                ]
                .into(),
            ))
            .unwrap(),
    );
    assert_eq!(info.num_columns(), 2);
    assert_eq!(info.num_rows(), 4);
    assert_eq!(info.schema(), *schemas::GET_INFO_SCHEMA.deref());
}

pub fn test_connection_get_objects(
    connection: &ManagedConnection,
    actual_num_catalog: usize,
    actual_num_tables: usize,
) {
    let objects = concat_reader(
        connection
            .get_objects(ObjectDepth::All, None, None, None, None, None)
            .unwrap(),
    );
    assert_eq!(objects.num_columns(), 2);
    assert_eq!(objects.num_rows(), actual_num_catalog);

    let objects = connection
        .get_objects(
            ObjectDepth::All,
            None,
            None,
            None,
            Some(vec!["table", "view"]),
            None,
        )
        .unwrap();
    let objects = concat_reader(objects);
    assert_eq!(objects.num_columns(), 2);
    assert_eq!(objects.num_rows(), actual_num_tables);

    let objects = concat_reader(
        connection
            .get_objects(
                ObjectDepth::All,
                Some("my_catalog"),
                Some("my_schema"),
                Some("my_table"),
                Some(vec!["table", "view"]),
                Some("my_column"),
            )
            .unwrap(),
    );
    assert_eq!(objects.num_rows(), 0);
    assert_eq!(objects.num_columns(), 2);
}

pub fn test_connection_get_table_schema(connection: &mut ManagedConnection) {
    const TABLE_NAME: &str = "my_super_table";

    connection
        .set_option(OptionConnection::AutoCommit, "false".into())
        .unwrap();

    let mut statement = connection.new_statement().unwrap();
    statement
        .set_sql_query(format!("create table {TABLE_NAME}(a bigint, b bigint);"))
        .unwrap();
    statement.execute_update().unwrap();

    let got = connection.get_table_schema(None, None, TABLE_NAME).unwrap();
    let actual = Schema::new(vec![
        Field::new("a", DataType::Int64, true),
        Field::new("b", DataType::Int64, true),
    ]);
    assert_eq!(got, actual);

    connection.rollback().unwrap();

    assert!(connection
        .get_table_schema(None, None, "nonexistent_table")
        .is_err());
}

pub fn test_statement(statement: &mut ManagedStatement) {
    statement
        .set_option(OptionStatement::IngestMode, IngestMode::Create.into())
        .unwrap();

    statement
        .set_option(
            OptionStatement::Other("unknown".into()),
            "unknown.value".into(),
        )
        .unwrap_err();
}

pub fn test_statement_prepare(statement: &mut ManagedStatement) {
    let error = statement.prepare().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);

    statement.set_sql_query("select 42").unwrap();
    statement.prepare().unwrap();
}

pub fn test_statement_set_substrait_plan(statement: &mut ManagedStatement) {
    let error = statement.set_substrait_plan(b"").unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

pub fn test_statement_execute(statement: &mut ManagedStatement) {
    assert!(statement.execute().is_err());

    statement.set_sql_query("select 42").unwrap();
    let batch = concat_reader(statement.execute().unwrap());
    assert_eq!(batch.num_rows(), 1);
    assert_eq!(batch.num_columns(), 1);
}

pub fn test_statement_execute_update(connection: &mut ManagedConnection) {
    let mut statement = connection.new_statement().unwrap();

    let error = statement.execute_update().unwrap_err();
    assert_eq!(error.status, Status::InvalidState);

    connection
        .set_option(OptionConnection::AutoCommit, "false".into())
        .unwrap();

    statement.set_sql_query("create table t(a int)").unwrap();
    statement.execute_update().unwrap();

    statement.set_sql_query("insert into t values(42)").unwrap();
    statement.execute_update().unwrap();

    connection.rollback().unwrap();
}

pub fn test_statement_execute_partitions(statement: &mut ManagedStatement) {
    let error = statement.execute_partitions().unwrap_err();
    assert_eq!(error.status, Status::NotImplemented);
}

pub fn test_statement_bind(statement: &mut ManagedStatement) {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    statement.bind(batch).unwrap();
}

pub fn test_statement_bind_stream(statement: &mut ManagedStatement) {
    let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, true)]));
    let columns: Vec<Arc<dyn Array>> = vec![Arc::new(Int64Array::from(vec![1, 2, 3]))];
    let batch = RecordBatch::try_new(schema, columns).unwrap();
    let reader = SingleBatchReader::new(batch);
    statement.bind_stream(Box::new(reader)).unwrap();
}

pub fn test_ingestion_roundtrip(connection: &mut ManagedConnection) {
    let mut statement = connection.new_statement().unwrap();
    let batch = sample_batch();

    connection
        .set_option(OptionConnection::AutoCommit, "false".into())
        .unwrap();

    // Ingest
    statement
        .set_option(OptionStatement::TargetTable, "my_table".into())
        .unwrap();

    statement.bind(batch.clone()).unwrap();
    statement.execute_update().unwrap();

    // Read back
    statement.set_sql_query("select * from my_table").unwrap();
    let batch_got = concat_reader(statement.execute().unwrap());
    assert_eq!(batch, batch_got);

    connection.rollback().unwrap();
}
