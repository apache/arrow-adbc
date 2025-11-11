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

#![allow(refining_impl_trait)]

use adbc_core::constants;
use datafusion::dataframe::DataFrameWriteOptions;
use datafusion::datasource::TableType;
use datafusion::prelude::*;
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use datafusion_substrait::substrait::proto::Plan;
use prost::Message;
use std::fmt::Debug;
use std::future::Future;
use std::sync::Arc;
use std::vec::IntoIter;

use arrow_array::builder::{
    BooleanBuilder, Int32Builder, Int64Builder, ListBuilder, MapBuilder, MapFieldNames,
    StringBuilder, UInt32Builder,
};
use arrow_array::{
    ArrayRef, BooleanArray, Int16Array, Int32Array, ListArray, RecordBatch, RecordBatchReader,
    StringArray, StructArray, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, SchemaRef};

use adbc_core::{
    error::{Error, Result, Status},
    options::{
        InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
    },
    schemas, Connection, Database, Driver, Optionable, Statement,
};

pub enum Runtime {
    Handle(tokio::runtime::Handle),
    Tokio(tokio::runtime::Runtime),
}

impl Runtime {
    pub fn new() -> std::io::Result<Self> {
        if let Ok(handle) = tokio::runtime::Handle::try_current() {
            Ok(Self::Handle(handle))
        } else {
            let runtime = tokio::runtime::Builder::new_multi_thread()
                .enable_all()
                .build()?;
            Ok(Self::Tokio(runtime))
        }
    }

    pub fn block_on<F: Future>(&self, future: F) -> F::Output {
        match self {
            Runtime::Handle(handle) => tokio::task::block_in_place(|| handle.block_on(future)),
            Runtime::Tokio(runtime) => runtime.block_on(future),
        }
    }
}

#[derive(Debug)]
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

pub struct DataFusionReader {
    batches: IntoIter<RecordBatch>,
    schema: SchemaRef,
}

impl DataFusionReader {
    pub async fn new(df: DataFrame) -> Self {
        let schema = df.schema().as_arrow().clone();

        Self {
            batches: df.collect().await.unwrap().into_iter(),
            schema: schema.into(),
        }
    }
}

impl Iterator for DataFusionReader {
    type Item = std::result::Result<RecordBatch, ArrowError>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batches.next().map(Ok)
    }
}

impl RecordBatchReader for DataFusionReader {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

#[derive(Default)]
pub struct DataFusionDriver {}

impl Driver for DataFusionDriver {
    type DatabaseType = DataFusionDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(Self::DatabaseType {})
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<
            Item = (
                adbc_core::options::OptionDatabase,
                adbc_core::options::OptionValue,
            ),
        >,
    ) -> adbc_core::error::Result<Self::DatabaseType> {
        let mut database = Self::DatabaseType {};
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

pub struct DataFusionDatabase {}

impl Optionable for DataFusionDatabase {
    type Option = OptionDatabase;

    fn set_option(
        &mut self,
        key: Self::Option,
        _value: adbc_core::options::OptionValue,
    ) -> adbc_core::error::Result<()> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }
}

impl Database for DataFusionDatabase {
    type ConnectionType = DataFusionConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        let ctx = SessionContext::new();

        let runtime = Runtime::new().unwrap();

        Ok(DataFusionConnection {
            runtime: Arc::new(runtime),
            ctx: Arc::new(ctx),
        })
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<
            Item = (
                adbc_core::options::OptionConnection,
                adbc_core::options::OptionValue,
            ),
        >,
    ) -> adbc_core::error::Result<Self::ConnectionType> {
        let ctx = SessionContext::new();

        let runtime = Runtime::new().unwrap();

        let mut connection = DataFusionConnection {
            runtime: Arc::new(runtime),
            ctx: Arc::new(ctx),
        };

        for (key, value) in opts {
            connection.set_option(key, value)?;
        }

        Ok(connection)
    }
}

pub struct DataFusionConnection {
    runtime: Arc<Runtime>,
    ctx: Arc<SessionContext>,
}

impl Optionable for DataFusionConnection {
    type Option = OptionConnection;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: adbc_core::options::OptionValue,
    ) -> adbc_core::error::Result<()> {
        match key.as_ref() {
            constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG => match value {
                OptionValue::String(value) => {
                    self.runtime.block_on(async {
                        let query = format!("SET datafusion.catalog.default_catalog = {value}");
                        self.ctx.sql(query.as_str()).await.unwrap();
                    });
                    Ok(())
                }
                _ => Err(Error::with_message_and_status(
                    "CurrentCatalog value must be of type String",
                    Status::InvalidArguments,
                )),
            },
            constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA => match value {
                OptionValue::String(value) => {
                    self.runtime.block_on(async {
                        let query = format!("SET datafusion.catalog.default_schema = {value}");
                        self.ctx.sql(query.as_str()).await.unwrap();
                    });
                    Ok(())
                }
                _ => Err(Error::with_message_and_status(
                    "CurrentSchema value must be of type String",
                    Status::InvalidArguments,
                )),
            },
            _ => Err(Error::with_message_and_status(
                format!("Unrecognized option: {key:?}"),
                Status::NotFound,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key.as_ref() {
            constants::ADBC_CONNECTION_OPTION_CURRENT_CATALOG => Ok(self
                .ctx
                .state()
                .config_options()
                .catalog
                .default_catalog
                .clone()),
            constants::ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA => Ok(self
                .ctx
                .state()
                .config_options()
                .catalog
                .default_schema
                .clone()),
            _ => Err(Error::with_message_and_status(
                format!("Unrecognized option: {key:?}"),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }
}

struct GetInfoBuilder {
    name_builder: UInt32Builder,
    type_id_vec: Vec<i8>,
    offsets_vec: Vec<i32>,
    string_array_builder: StringBuilder,
    string_offset: i32,
    bool_array_builder: BooleanBuilder,
    bool_offset: i32,
    int64_array_builder: Int64Builder,
    // int64_offset: i32,
    int32_array_builder: Int32Builder,
    // int32_offset: i32,
    list_string_array_builder: ListBuilder<StringBuilder>,
    // list_string_offset: i32,
    map_builder: MapBuilder<Int32Builder, ListBuilder<Int32Builder>>,
    // map_offset: i32,
}

impl GetInfoBuilder {
    pub fn new() -> GetInfoBuilder {
        GetInfoBuilder {
            name_builder: UInt32Builder::new(),
            type_id_vec: vec![],
            offsets_vec: vec![],
            string_array_builder: StringBuilder::new(),
            string_offset: 0,
            bool_array_builder: BooleanBuilder::new(),
            bool_offset: 0,
            int64_array_builder: Int64Builder::new(),
            // int64_offset: 0,
            int32_array_builder: Int32Builder::new(),
            // int32_offset: 0,
            list_string_array_builder: ListBuilder::new(StringBuilder::new()),
            // list_string_offset: 0,
            map_builder: MapBuilder::new(
                Some(MapFieldNames {
                    entry: "entries".to_string(),
                    key: "key".to_string(),
                    value: "value".to_string(),
                }),
                Int32Builder::new(),
                ListBuilder::new(Int32Builder::new()),
            ),
            // map_offset: 0,
        }
    }

    pub fn set_string(&mut self, code: InfoCode, string: &str) {
        self.name_builder.append_value(Into::<u32>::into(&code));
        self.string_array_builder.append_value(string);
        self.type_id_vec.push(0);
        self.offsets_vec.push(self.string_offset);
        self.string_offset += 1;
    }

    pub fn set_bool(&mut self, code: InfoCode, bool: bool) {
        self.name_builder.append_value(Into::<u32>::into(&code));
        self.bool_array_builder.append_value(bool);
        self.type_id_vec.push(1);
        self.offsets_vec.push(self.bool_offset);
        self.bool_offset += 1;
    }

    pub fn finish(mut self) -> Result<RecordBatch> {
        let fields = match schemas::GET_INFO_SCHEMA
            .field_with_name("info_value")
            .unwrap()
            .data_type()
        {
            DataType::Union(fields, _) => Some(fields),
            _ => None,
        };

        let value_array = UnionArray::try_new(
            fields.unwrap().clone(),
            self.type_id_vec.into_iter().collect::<ScalarBuffer<i8>>(),
            Some(self.offsets_vec.into_iter().collect::<ScalarBuffer<i32>>()),
            vec![
                Arc::new(self.string_array_builder.finish()),
                Arc::new(self.bool_array_builder.finish()),
                Arc::new(self.int64_array_builder.finish()),
                Arc::new(self.int32_array_builder.finish()),
                Arc::new(self.list_string_array_builder.finish()),
                Arc::new(self.map_builder.finish()),
            ],
        )?;

        Ok(RecordBatch::try_new(
            schemas::GET_INFO_SCHEMA.clone(),
            vec![Arc::new(self.name_builder.finish()), Arc::new(value_array)],
        )?)
    }
}

struct GetObjectsBuilder {
    catalog_names: Vec<String>,
    catalog_db_schema_offsets: Vec<i32>,
    catalog_db_schema_names: Vec<String>,
    table_offsets: Vec<i32>,
    table_names: Vec<String>,
    table_types: Vec<String>,
    column_offsets: Vec<i32>,
    column_names: Vec<String>,
}

impl GetObjectsBuilder {
    pub fn new() -> GetObjectsBuilder {
        GetObjectsBuilder {
            catalog_names: vec![],
            catalog_db_schema_offsets: vec![0],
            catalog_db_schema_names: vec![],
            table_offsets: vec![0],
            table_names: vec![],
            table_types: vec![],
            column_offsets: vec![0],
            column_names: vec![],
        }
    }

    pub fn build(
        &mut self,
        runtime: &Runtime,
        ctx: &SessionContext,
        depth: &ObjectDepth,
    ) -> Result<RecordBatch> {
        let mut catalogs = ctx.catalog_names();
        self.catalog_names.append(&mut catalogs);

        self.catalog_names.iter().for_each(|cat| {
            let catalog_provider = ctx.catalog(cat).unwrap();
            let schema_names = catalog_provider.schema_names();
            self.catalog_db_schema_names
                .append(&mut schema_names.clone());

            self.catalog_db_schema_offsets
                .push(self.catalog_db_schema_offsets.last().unwrap() + schema_names.len() as i32);

            schema_names.iter().for_each(|schema| {
                let schema_provider = catalog_provider.schema(schema).unwrap();
                let table_names = schema_provider.table_names();
                self.table_names.append(&mut table_names.clone());
                self.table_offsets
                    .push(self.table_offsets.last().unwrap() + table_names.len() as i32);

                table_names.iter().for_each(|t| {
                    runtime.block_on(async {
                        let table_provider = schema_provider.table(t).await.unwrap().unwrap();
                        let table_type = match table_provider.table_type() {
                            TableType::Base => "Base",
                            TableType::View => "View",
                            TableType::Temporary => "Temporary",
                        };
                        self.table_types.push(table_type.to_string());

                        let schema = table_provider.schema();
                        let num_fields = schema.fields().len();

                        schema.fields().iter().for_each(|f| {
                            self.column_names.push(f.name().clone());
                        });
                        self.column_offsets
                            .push(self.column_offsets.last().unwrap() + num_fields as i32);
                    });
                });
            });
        });

        //////////////////////////////////////////////////////

        let table_columns_array = match depth {
            ObjectDepth::Columns | ObjectDepth::All => {
                let columns_struct_array = StructArray::from(vec![
                    (
                        Arc::new(Field::new("column_name", DataType::Utf8, false)),
                        Arc::new(StringArray::from(self.column_names.clone())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("ordinal_position", DataType::Int32, true)),
                        Arc::new(Int32Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("remarks", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_data_type", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_type_name", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_column_size", DataType::Int32, true)),
                        Arc::new(Int32Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_decimal_digits", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_num_prec_radix", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_nullable", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_column_def", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_sql_data_type", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_datetime_sub", DataType::Int16, true)),
                        Arc::new(Int16Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_char_octet_length", DataType::Int32, true)),
                        Arc::new(Int32Array::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_is_nullable", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_scope_catalog", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_scope_schema", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_scope_table", DataType::Utf8, true)),
                        Arc::new(StringArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("xdbc_is_autoincrement", DataType::Boolean, true)),
                        Arc::new(BooleanArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new(
                            "xdbc_is_generatedcolumn",
                            DataType::Boolean,
                            true,
                        )),
                        Arc::new(BooleanArray::new_null(self.column_names.len())) as ArrayRef,
                    ),
                ]);

                ListArray::new(
                    Arc::new(Field::new("item", schemas::COLUMN_SCHEMA.clone(), true)),
                    OffsetBuffer::new(ScalarBuffer::from(self.column_offsets.clone())),
                    Arc::new(columns_struct_array) as ArrayRef,
                    None,
                )
            }
            _ => ListArray::new_null(
                Arc::new(Field::new("item", schemas::COLUMN_SCHEMA.clone(), true)),
                self.table_names.len(),
            ),
        };

        let db_schema_tables_array = match depth {
            ObjectDepth::Tables | ObjectDepth::Columns | ObjectDepth::All => {
                let table_constraints_array = ListArray::new_null(
                    Arc::new(Field::new("item", schemas::CONSTRAINT_SCHEMA.clone(), true)),
                    self.table_names.len(),
                );

                let tables_struct_array = StructArray::from(vec![
                    (
                        Arc::new(Field::new("table_name", DataType::Utf8, false)),
                        Arc::new(StringArray::from(self.table_names.clone())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new("table_type", DataType::Utf8, false)),
                        Arc::new(StringArray::from(self.table_types.clone())) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new_list(
                            "table_columns",
                            Arc::new(Field::new("item", schemas::COLUMN_SCHEMA.clone(), true)),
                            true,
                        )),
                        Arc::new(table_columns_array) as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new_list(
                            "table_constraints",
                            Arc::new(Field::new("item", schemas::CONSTRAINT_SCHEMA.clone(), true)),
                            true,
                        )),
                        Arc::new(table_constraints_array) as ArrayRef,
                    ),
                ]);

                ListArray::new(
                    Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
                    OffsetBuffer::new(ScalarBuffer::from(self.table_offsets.clone())),
                    Arc::new(tables_struct_array) as ArrayRef,
                    None,
                )
            }
            _ => ListArray::new_null(
                Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
                self.catalog_db_schema_names.len(),
            ),
        };

        let catalog_db_schemas_array = match depth {
            ObjectDepth::Columns
            | ObjectDepth::Tables
            | ObjectDepth::Schemas
            | ObjectDepth::All => {
                let db_schemas_array = StructArray::from(vec![
                    (
                        Arc::new(Field::new("db_schema_name", DataType::Utf8, true)),
                        Arc::new(StringArray::from(self.catalog_db_schema_names.clone()))
                            as ArrayRef,
                    ),
                    (
                        Arc::new(Field::new_list(
                            "db_schema_tables",
                            Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
                            true,
                        )),
                        Arc::new(db_schema_tables_array) as ArrayRef,
                    ),
                ]);

                ListArray::new(
                    Arc::new(Field::new(
                        "item",
                        schemas::OBJECTS_DB_SCHEMA_SCHEMA.clone(),
                        true,
                    )),
                    OffsetBuffer::new(ScalarBuffer::from(self.catalog_db_schema_offsets.clone())),
                    Arc::new(db_schemas_array) as ArrayRef,
                    None,
                )
            }
            _ => ListArray::new_null(
                Arc::new(Field::new(
                    "item",
                    schemas::OBJECTS_DB_SCHEMA_SCHEMA.clone(),
                    true,
                )),
                self.catalog_names.len(),
            ),
        };

        let catalog_name_array = StringArray::from(self.catalog_names.clone());

        let batch = RecordBatch::try_new(
            schemas::GET_OBJECTS_SCHEMA.clone(),
            vec![
                Arc::new(catalog_name_array),
                Arc::new(catalog_db_schemas_array),
            ],
        )?;

        Ok(batch)
    }
}

impl Connection for DataFusionConnection {
    type StatementType = DataFusionStatement;

    fn new_statement(&mut self) -> adbc_core::error::Result<Self::StatementType> {
        Ok(DataFusionStatement {
            runtime: self.runtime.clone(),
            ctx: self.ctx.clone(),
            sql_query: None,
            substrait_plan: None,
            bound_record_batch: None,
            ingest_target_table: None,
        })
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        todo!()
    }

    fn get_info(
        &self,
        codes: Option<std::collections::HashSet<adbc_core::options::InfoCode>>,
    ) -> Result<impl RecordBatchReader + Send> {
        let mut get_info_builder = GetInfoBuilder::new();

        codes.unwrap().into_iter().for_each(|f| match f {
            InfoCode::DriverName => get_info_builder.set_string(f, "ADBCDataFusion"),
            InfoCode::VendorName => get_info_builder.set_string(f, "DataFusion"),
            InfoCode::VendorSql => get_info_builder.set_bool(f, true),
            InfoCode::VendorSubstrait => get_info_builder.set_bool(f, true),
            _ => {}
        });

        let batch = get_info_builder.finish()?;
        let reader = SingleBatchReader::new(batch);
        Ok(reader)
    }

    fn get_objects(
        &self,
        depth: adbc_core::options::ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader + Send> {
        let batch = GetObjectsBuilder::new().build(&self.runtime, &self.ctx, &depth)?;
        let reader = SingleBatchReader::new(batch);
        Ok(reader)
    }

    fn get_table_schema(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: &str,
    ) -> adbc_core::error::Result<arrow_schema::Schema> {
        todo!()
    }

    fn get_table_types(&self) -> Result<SingleBatchReader> {
        todo!()
    }

    fn get_statistic_names(&self) -> Result<SingleBatchReader> {
        todo!()
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<SingleBatchReader> {
        todo!()
    }

    fn commit(&mut self) -> adbc_core::error::Result<()> {
        todo!()
    }

    fn rollback(&mut self) -> adbc_core::error::Result<()> {
        todo!()
    }

    fn read_partition(&self, _partition: impl AsRef<[u8]>) -> Result<SingleBatchReader> {
        todo!()
    }
}

pub struct DataFusionStatement {
    runtime: Arc<Runtime>,
    ctx: Arc<SessionContext>,
    sql_query: Option<String>,
    substrait_plan: Option<Plan>,
    bound_record_batch: Option<RecordBatch>,
    ingest_target_table: Option<String>,
}

impl Optionable for DataFusionStatement {
    type Option = OptionStatement;

    fn set_option(
        &mut self,
        key: Self::Option,
        value: adbc_core::options::OptionValue,
    ) -> adbc_core::error::Result<()> {
        match key.as_ref() {
            constants::ADBC_INGEST_OPTION_TARGET_TABLE => match value {
                OptionValue::String(value) => {
                    self.ingest_target_table = Some(value);
                    Ok(())
                }
                _ => Err(Error::with_message_and_status(
                    "IngestOptionTargetTable value must be of type String",
                    Status::InvalidArguments,
                )),
            },
            _ => Err(Error::with_message_and_status(
                format!("Unrecognized option: {key:?}"),
                Status::NotFound,
            )),
        }
    }

    fn get_option_string(&self, key: Self::Option) -> adbc_core::error::Result<String> {
        match key.as_ref() {
            constants::ADBC_INGEST_OPTION_TARGET_TABLE => {
                let target_table = self.ingest_target_table.clone();
                match target_table {
                    Some(table) => Ok(table),
                    None => Err(Error::with_message_and_status(
                        format!("{key:?} has not been set"),
                        Status::NotFound,
                    )),
                }
            }
            _ => Err(Error::with_message_and_status(
                format!("Unrecognized option: {key:?}"),
                Status::NotFound,
            )),
        }
    }

    fn get_option_bytes(&self, key: Self::Option) -> adbc_core::error::Result<Vec<u8>> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_int(&self, key: Self::Option) -> adbc_core::error::Result<i64> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }

    fn get_option_double(&self, key: Self::Option) -> adbc_core::error::Result<f64> {
        Err(Error::with_message_and_status(
            format!("Unrecognized option: {key:?}"),
            Status::NotFound,
        ))
    }
}

impl Statement for DataFusionStatement {
    fn bind(&mut self, batch: arrow_array::RecordBatch) -> adbc_core::error::Result<()> {
        self.bound_record_batch.replace(batch);
        Ok(())
    }

    fn bind_stream(
        &mut self,
        _reader: Box<dyn arrow_array::RecordBatchReader + Send>,
    ) -> adbc_core::error::Result<()> {
        todo!()
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader + Send> {
        self.runtime.block_on(async {
            let df = if self.sql_query.is_some() {
                self.ctx
                    .sql(&self.sql_query.clone().unwrap())
                    .await
                    .unwrap()
            } else {
                let plan =
                    from_substrait_plan(&self.ctx.state(), &self.substrait_plan.clone().unwrap())
                        .await
                        .unwrap();
                self.ctx.execute_logical_plan(plan).await.unwrap()
            };

            Ok(DataFusionReader::new(df).await)
        })
    }

    fn execute_update(&mut self) -> adbc_core::error::Result<Option<i64>> {
        if self.sql_query.is_some() {
            self.runtime.block_on(async {
                let _ = self
                    .ctx
                    .sql(&self.sql_query.clone().unwrap())
                    .await
                    .unwrap();
            });
        } else if let Some(batch) = self.bound_record_batch.take() {
            self.runtime.block_on(async {
                let table = match self.ingest_target_table.clone() {
                    Some(table) => table,
                    None => todo!(),
                };

                self.ctx
                    .read_batch(batch)
                    .unwrap()
                    .write_table(table.as_str(), DataFrameWriteOptions::new())
                    .await
                    .unwrap();
            });
        }

        Ok(Some(0))
    }

    fn execute_schema(&mut self) -> adbc_core::error::Result<arrow_schema::Schema> {
        self.runtime.block_on(async {
            let df = self
                .ctx
                .sql(&self.sql_query.clone().unwrap())
                .await
                .unwrap();

            Ok(df.schema().as_arrow().clone())
        })
    }

    fn execute_partitions(&mut self) -> adbc_core::error::Result<adbc_core::PartitionedResult> {
        todo!()
    }

    fn get_parameter_schema(&self) -> adbc_core::error::Result<arrow_schema::Schema> {
        todo!()
    }

    fn prepare(&mut self) -> adbc_core::error::Result<()> {
        todo!()
    }

    fn set_sql_query(&mut self, query: impl AsRef<str>) -> adbc_core::error::Result<()> {
        self.sql_query = Some(query.as_ref().to_string());
        Ok(())
    }

    fn set_substrait_plan(&mut self, plan: impl AsRef<[u8]>) -> adbc_core::error::Result<()> {
        self.substrait_plan = Some(Plan::decode(plan.as_ref()).unwrap());
        Ok(())
    }

    fn cancel(&mut self) -> adbc_core::error::Result<()> {
        todo!()
    }
}

#[cfg(feature = "ffi")]
adbc_ffi::export_driver!(DataFusionDriverInit, DataFusionDriver);
