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

use std::pin::Pin;

use adbc_core::non_blocking::RecordBatchStream;
use adbc_core::non_blocking::{
    AsyncConnection, AsyncDatabase, AsyncDriver, AsyncStatement, LocalAsyncOptionable,
};
use adbc_datafusion::{DataFusionConnection, DataFusionDriver};
use arrow_array::RecordBatch;
use datafusion::prelude::*;

use adbc_core::options::{OptionConnection, OptionStatement, OptionValue};
use arrow_select::concat::concat_batches;
use datafusion_substrait::logical_plan::producer::to_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use futures::StreamExt;
use prost::Message;

async fn read_all_record_batches(
    mut reader: Pin<Box<dyn RecordBatchStream + Send>>,
) -> adbc_core::error::Result<Vec<RecordBatch>> {
    let mut batches = Vec::new();
    while let Some(batch) = reader.next().await {
        batches.push(batch?);
    }
    Ok(batches)
}

async fn get_connection() -> DataFusionConnection {
    let mut driver = DataFusionDriver::default();
    let database = driver.new_database().await.unwrap();
    database.new_connection().await.unwrap()
}

async fn get_objects(connection: &DataFusionConnection) -> RecordBatch {
    let objects = connection
        .get_objects(
            adbc_core::options::ObjectDepth::All,
            None,
            None,
            None,
            None,
            None,
        )
        .await;

    let batches = read_all_record_batches(objects.unwrap()).await.unwrap();

    let schema = batches.first().unwrap().schema();

    concat_batches(&schema, &batches).unwrap()
}

async fn execute_update(connection: &mut DataFusionConnection, query: &str) {
    let mut statement = connection.new_statement().await.unwrap();
    let _ = statement.set_sql_query(query).await;
    let _ = statement.execute_update().await;
}

async fn execute_sql_query(connection: &mut DataFusionConnection, query: &str) -> RecordBatch {
    let mut statement = connection.new_statement().await.unwrap();
    let _ = statement.set_sql_query(query).await;

    let batches = read_all_record_batches(statement.execute().await.unwrap())
        .await
        .unwrap();

    let schema = batches.first().unwrap().schema();

    concat_batches(&schema, &batches).unwrap()
}

async fn execute_substrait(connection: &mut DataFusionConnection, plan: Plan) -> RecordBatch {
    let mut statement = connection.new_statement().await.unwrap();

    let _ = statement.set_substrait_plan(&plan.encode_to_vec()).await;

    let batches = read_all_record_batches(statement.execute().await.unwrap())
        .await
        .unwrap();

    let schema = batches.first().unwrap().schema();

    concat_batches(&schema, &batches).unwrap()
}

#[tokio::test]
async fn test_connection_options() {
    let mut connection = get_connection().await;

    let current_catalog = connection
        .get_option_string(OptionConnection::CurrentCatalog)
        .await
        .unwrap();

    assert_eq!(current_catalog, "datafusion");

    let _ = connection
        .set_option(
            OptionConnection::CurrentCatalog,
            OptionValue::String("datafusion2".to_string()),
        )
        .await;

    let current_catalog = connection
        .get_option_string(OptionConnection::CurrentCatalog)
        .await
        .unwrap();

    assert_eq!(current_catalog, "datafusion2");

    let current_schema = connection
        .get_option_string(OptionConnection::CurrentSchema)
        .await
        .unwrap();

    assert_eq!(current_schema, "public");

    let _ = connection
        .set_option(
            OptionConnection::CurrentSchema,
            OptionValue::String("public2".to_string()),
        )
        .await;

    let current_schema = connection
        .get_option_string(OptionConnection::CurrentSchema)
        .await
        .unwrap();

    assert_eq!(current_schema, "public2");
}

#[tokio::test]
async fn test_get_objects_database() {
    let mut connection = get_connection().await;

    let objects = get_objects(&connection).await;

    assert_eq!(objects.num_rows(), 1);

    execute_update(&mut connection, "CREATE DATABASE another").await;

    let objects = get_objects(&connection).await;

    assert_eq!(objects.num_rows(), 2);
}

#[tokio::test]
async fn test_execute_sql() {
    let mut connection = get_connection().await;

    execute_update(&mut connection, "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')").await;

    let batch = execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example").await;

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
}

#[tokio::test]
async fn test_ingest() {
    let mut connection = get_connection().await;

    execute_update(&mut connection, "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')").await;

    let batch = execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example").await;

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);

    let mut statement = connection.new_statement().await.unwrap();

    let _ = statement
        .set_option(
            OptionStatement::TargetTable,
            OptionValue::String("example".to_string()),
        )
        .await;
    let _ = statement.bind(batch).await;

    let _ = statement.execute_update().await;

    let batch = execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example").await;

    assert_eq!(batch.num_rows(), 6);
}

#[tokio::test]
async fn test_execute_substrait() {
    let mut connection = get_connection().await;

    execute_update(&mut connection, "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')").await;

    let ctx = SessionContext::new();

    let _ = ctx.sql(
        "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')"
    ).await;

    let df = ctx
        .sql("SELECT c1, c2 FROM datafusion.public.example")
        .await
        .unwrap();

    let plan = to_substrait_plan(df.logical_plan(), &ctx.state()).unwrap();

    let batch = execute_substrait(&mut connection, *plan).await;

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_running_in_async() {
    let mut connection = get_connection().await;

    execute_update(&mut connection, "CREATE TABLE IF NOT EXISTS datafusion.public.example (c1 INT, c2 VARCHAR) AS VALUES(1,'HELLO'),(2,'DATAFUSION'),(3,'!')").await;

    let batch = execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example").await;

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);

    let batch = tokio::spawn(async move {
        let batch =
            execute_sql_query(&mut connection, "SELECT * FROM datafusion.public.example").await;
        batch
    })
    .await
    .unwrap();

    assert_eq!(batch.num_rows(), 3);
    assert_eq!(batch.num_columns(), 2);
}
