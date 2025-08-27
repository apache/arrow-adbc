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

use adbc_core::{Connection, Database, Driver, Optionable, Statement};
use adbc_datafusion::{DataFusionConnection, DataFusionDriver};
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::compute::concat_batches;
use datafusion::prelude::*;

use adbc_core::options::{OptionConnection, OptionStatement, OptionValue};
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;

fn get_connection() -> DataFusionConnection {
    let mut driver = DataFusionDriver::default();
    let database = driver.new_database().unwrap();
    database.new_connection().unwrap()
}

fn get_objects(connection: &DataFusionConnection) -> RecordBatch {
    let objects = connection.get_objects(
        adbc_core::options::ObjectDepth::All,
        None,
        None,
        None,
        None,
        None,
    );

    let batches: Vec<RecordBatch> = objects.unwrap().map(|b| b.unwrap()).collect();

    let schema = batches.first().unwrap().schema();

    concat_batches(&schema, &batches).unwrap()
}

fn execute_update(connection: &mut DataFusionConnection, query: &str) {
    let mut statement = connection.new_statement().unwrap();
    let _ = statement.set_sql_query(query);
    let _ = statement.execute_update();
}

fn execute_sql_query(connection: &mut DataFusionConnection, query: &str) -> RecordBatch {
    let mut statement = connection.new_statement().unwrap();
    let _ = statement.set_sql_query(query);

    let batches: Vec<RecordBatch> = statement.execute().unwrap().map(|b| b.unwrap()).collect();

    let schema = batches.first().unwrap().schema();

    concat_batches(&schema, &batches).unwrap()
}

fn execute_substrait(connection: &mut DataFusionConnection, plan: Plan) -> RecordBatch {
    let mut statement = connection.new_statement().unwrap();

    let _ = statement.set_substrait_plan(plan.encode_to_vec());

    let batches: Vec<RecordBatch> = statement.execute().unwrap().map(|b| b.unwrap()).collect();

    let schema = batches.first().unwrap().schema();

    concat_batches(&schema, &batches).unwrap()
}

#[test]
fn test_connection_options() {
    let mut connection = get_connection();

    let current_catalog = connection
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();

    assert_eq!(current_catalog, "datafusion");

    let _ = connection.set_option(
        OptionConnection::CurrentCatalog,
        OptionValue::String("datafusion2".to_string()),
    );

    let current_catalog = connection
        .get_option_string(OptionConnection::CurrentCatalog)
        .unwrap();

    assert_eq!(current_catalog, "datafusion2");

    let current_schema = connection
        .get_option_string(OptionConnection::CurrentSchema)
        .unwrap();

    assert_eq!(current_schema, "public");

    let _ = connection.set_option(
        OptionConnection::CurrentSchema,
        OptionValue::String("public2".to_string()),
    );

    let current_schema = connection
        .get_option_string(OptionConnection::CurrentSchema)
        .unwrap();

    assert_eq!(current_schema, "public2");
}

#[test]
fn test_get_objects_database() {
    let mut connection = get_connection();

    let objects = get_objects(&connection);

    assert_eq!(objects.num_rows(), 1);

    execute_update(&mut connection, "CREATE DATABASE another");

    let objects = get_objects(&connection);

    assert_eq!(objects.num_rows(), 2);
}

#[test]
fn test_execute_sql() {
    let mut connection = get_connection();

    execute_update(&mut connection, "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')");

    let batch = execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example");

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
}

#[test]
fn test_ingest() {
    let mut connection = get_connection();

    execute_update(&mut connection, "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')");

    let batch = execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example");

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);

    let mut statement = connection.new_statement().unwrap();

    let _ = statement.set_option(
        OptionStatement::TargetTable,
        OptionValue::String("example".to_string()),
    );
    let _ = statement.bind(batch);

    let _ = statement.execute_update();

    let batch = execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example");

    assert_eq!(batch.num_rows(), 6);
}

#[test]
fn test_execute_substrait() {
    let mut connection = get_connection();

    execute_update(&mut connection, "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')");

    let ctx = SessionContext::new();

    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .unwrap();

    let plan = runtime.block_on(async {
        let _ = ctx.sql(
            "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')"
        ).await;

        let df = ctx.sql("SELECT c1, c2 FROM datafusion.public.example").await.unwrap();

        to_substrait_plan(df.logical_plan(), &ctx.state()).unwrap()
    });

    let batch = execute_substrait(&mut connection, *plan);

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
}
