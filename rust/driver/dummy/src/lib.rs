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
use std::sync::Arc;
use std::{collections::HashMap, fmt::Debug, hash::Hash};

use adbc_core::options::Statistics;
use arrow_array::{
    Array, ArrayRef, BinaryArray, BooleanArray, Float64Array, Int16Array, Int32Array, Int64Array,
    ListArray, MapArray, RecordBatch, RecordBatchReader, StringArray, StructArray, UInt32Array,
    UInt64Array, UnionArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{ArrowError, DataType, Field, Schema, SchemaRef, UnionFields};

use adbc_core::{
    constants,
    error::{Error, Result, Status},
    options::{
        InfoCode, ObjectDepth, OptionConnection, OptionDatabase, OptionStatement, OptionValue,
    },
    schemas, Connection, Database, Driver, Optionable, PartitionedResult, Statement,
};

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

fn get_table_schema() -> Schema {
    Schema::new(vec![
        Field::new("a", DataType::UInt32, true),
        Field::new("b", DataType::Float64, false),
        Field::new("c", DataType::Utf8, true),
    ])
}

fn get_table_data() -> RecordBatch {
    RecordBatch::try_new(
        Arc::new(get_table_schema()),
        vec![
            Arc::new(UInt32Array::from(vec![1, 2, 3])),
            Arc::new(Float64Array::from(vec![1.5, 2.5, 3.5])),
            Arc::new(StringArray::from(vec!["A", "B", "C"])),
        ],
    )
    .unwrap()
}

fn set_option<T>(options: &mut HashMap<T, OptionValue>, key: T, value: OptionValue) -> Result<()>
where
    T: Eq + Hash,
{
    options.insert(key, value);
    Ok(())
}

fn get_option_bytes<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<Vec<u8>>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            format!("Unrecognized {kind} option: {key:?}"),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::Bytes(value) => Ok(value.clone()),
            _ => Err(Error::with_message_and_status(
                format!("Incorrect value for {kind} option: {key:?}"),
                Status::InvalidData,
            )),
        },
    }
}

fn get_option_double<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<f64>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            format!("Unrecognized {kind} option: {key:?}"),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::Double(value) => Ok(*value),
            _ => Err(Error::with_message_and_status(
                format!("Incorrect value for {kind} option: {key:?}"),
                Status::InvalidData,
            )),
        },
    }
}

fn get_option_int<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<i64>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            format!("Unrecognized {kind} option: {key:?}"),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::Int(value) => Ok(*value),
            _ => Err(Error::with_message_and_status(
                format!("Incorrect value for {kind} option: {key:?}"),
                Status::InvalidData,
            )),
        },
    }
}

fn get_option_string<T>(options: &HashMap<T, OptionValue>, key: T, kind: &str) -> Result<String>
where
    T: Eq + Hash + Debug,
{
    let value = options.get(&key);
    match value {
        None => Err(Error::with_message_and_status(
            format!("Unrecognized {kind} option: {key:?}"),
            Status::NotFound,
        )),
        Some(value) => match value {
            OptionValue::String(value) => Ok(value.clone()),
            _ => Err(Error::with_message_and_status(
                format!("Incorrect value for {kind} option: {key:?}"),
                Status::InvalidData,
            )),
        },
    }
}

fn maybe_panic(fnname: impl AsRef<str>) {
    if let Some(func) = std::env::var_os("PANICDUMMY_FUNC").map(|x| x.to_string_lossy().to_string())
    {
        if fnname.as_ref() == func {
            let message = std::env::var_os("PANICDUMMY_MESSAGE")
                .map(|x| x.to_string_lossy().to_string())
                .unwrap_or_else(|| format!("We panicked in {}!", fnname.as_ref()));
            panic!("{}", message);
        }
    }
}

/// A dummy driver used for testing purposes.
#[derive(Default)]
pub struct DummyDriver {}

impl Driver for DummyDriver {
    type DatabaseType = DummyDatabase;

    fn new_database(&mut self) -> Result<Self::DatabaseType> {
        Ok(Self::DatabaseType::default())
    }

    fn new_database_with_opts(
        &mut self,
        opts: impl IntoIterator<Item = (<Self::DatabaseType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::DatabaseType> {
        let mut database = Self::DatabaseType::default();
        for (key, value) in opts {
            database.set_option(key, value)?;
        }
        Ok(database)
    }
}

#[derive(Default)]
pub struct DummyDatabase {
    options: HashMap<OptionDatabase, OptionValue>,
}

impl Optionable for DummyDatabase {
    type Option = OptionDatabase;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        set_option(&mut self.options, key, value)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        get_option_bytes(&self.options, key, "database")
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        get_option_double(&self.options, key, "database")
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        get_option_int(&self.options, key, "database")
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_option_string(&self.options, key, "database")
    }
}

impl Database for DummyDatabase {
    type ConnectionType = DummyConnection;

    fn new_connection(&self) -> Result<Self::ConnectionType> {
        Ok(Self::ConnectionType::default())
    }

    fn new_connection_with_opts(
        &self,
        opts: impl IntoIterator<Item = (<Self::ConnectionType as Optionable>::Option, OptionValue)>,
    ) -> Result<Self::ConnectionType> {
        let mut connection = Self::ConnectionType::default();
        for (key, value) in opts {
            connection.set_option(key, value)?;
        }
        Ok(connection)
    }
}

#[derive(Default)]
pub struct DummyConnection {
    options: HashMap<OptionConnection, OptionValue>,
}

impl Optionable for DummyConnection {
    type Option = OptionConnection;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        set_option(&mut self.options, key, value)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        get_option_bytes(&self.options, key, "connection")
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        get_option_double(&self.options, key, "connection")
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        get_option_int(&self.options, key, "connection")
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_option_string(&self.options, key, "connection")
    }
}

impl Connection for DummyConnection {
    type StatementType = DummyStatement;

    fn new_statement(&mut self) -> Result<Self::StatementType> {
        Ok(Self::StatementType::default())
    }

    // This method is used to test that errors round-trip correctly.
    fn cancel(&mut self) -> Result<()> {
        let mut error = Error::with_message_and_status("message", Status::Cancelled);
        error.vendor_code = constants::ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
        error.sqlstate = [1, 2, 3, 4, 5];
        error.details = Some(vec![
            ("key1".into(), b"AAA".into()),
            ("key2".into(), b"ZZZZZ".into()),
        ]);
        Err(error)
    }

    fn commit(&mut self) -> Result<()> {
        Ok(())
    }

    fn get_info(&self, _codes: Option<HashSet<InfoCode>>) -> Result<impl RecordBatchReader> {
        let string_value_array = StringArray::from(vec!["MyVendorName"]);
        let bool_value_array = BooleanArray::from(vec![true]);
        let int64_value_array = Int64Array::from(vec![42]);
        let int32_bitmask_array = Int32Array::from(vec![1337]);
        let string_list_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            Arc::new(StringArray::from(vec!["Hello", "World"])),
            None,
        );
        let int32_to_int32_list_map_array = MapArray::try_new(
            Arc::new(Field::new_struct(
                "entries",
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new_list("value", Field::new_list_field(DataType::Int32, true), true),
                ],
                false,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 2])),
            StructArray::new(
                vec![
                    Field::new("key", DataType::Int32, false),
                    Field::new_list("value", Field::new_list_field(DataType::Int32, true), true),
                ]
                .into(),
                vec![
                    Arc::new(Int32Array::from(vec![42, 1337])),
                    Arc::new(ListArray::new(
                        Arc::new(Field::new("item", DataType::Int32, true)),
                        OffsetBuffer::new(ScalarBuffer::from(vec![0, 3, 6])),
                        Arc::new(Int32Array::from(vec![1, 2, 3, 1, 4, 9])),
                        None,
                    )),
                ],
                None,
            ),
            None,
            false,
        )?;

        let name_array = UInt32Array::from(vec![
            Into::<u32>::into(&InfoCode::VendorName),
            Into::<u32>::into(&InfoCode::VendorVersion),
            Into::<u32>::into(&InfoCode::VendorArrowVersion),
            Into::<u32>::into(&InfoCode::DriverName),
            Into::<u32>::into(&InfoCode::DriverVersion),
            Into::<u32>::into(&InfoCode::DriverArrowVersion),
        ]);

        let type_id_buffer = [0_i8, 1, 2, 3, 4, 5]
            .into_iter()
            .collect::<ScalarBuffer<i8>>();
        let value_offsets_buffer = [0_i32, 0, 0, 0, 0, 0]
            .into_iter()
            .collect::<ScalarBuffer<i32>>();

        let value_array = UnionArray::try_new(
            UnionFields::new(
                [0, 1, 2, 3, 4, 5],
                [
                    Field::new("string_value", string_value_array.data_type().clone(), true),
                    Field::new("bool_value", bool_value_array.data_type().clone(), true),
                    Field::new("int64_value", int64_value_array.data_type().clone(), true),
                    Field::new(
                        "int32_bitmask",
                        int32_bitmask_array.data_type().clone(),
                        true,
                    ),
                    Field::new("string_list", string_list_array.data_type().clone(), true),
                    Field::new(
                        "int32_to_int32_list_map",
                        int32_to_int32_list_map_array.data_type().clone(),
                        true,
                    ),
                ],
            ),
            type_id_buffer,
            Some(value_offsets_buffer),
            vec![
                Arc::new(string_value_array),
                Arc::new(bool_value_array),
                Arc::new(int64_value_array),
                Arc::new(int32_bitmask_array),
                Arc::new(string_list_array),
                Arc::new(int32_to_int32_list_map_array),
            ],
        )?;

        let batch = RecordBatch::try_new(
            schemas::GET_INFO_SCHEMA.clone(),
            vec![Arc::new(name_array), Arc::new(value_array)],
        )?;
        let reader = SingleBatchReader::new(batch);
        Ok(reader)
    }

    fn get_objects(
        &self,
        _depth: ObjectDepth,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _table_type: Option<Vec<&str>>,
        _column_name: Option<&str>,
    ) -> Result<impl RecordBatchReader> {
        let constraint_column_usage_array_inner = StructArray::from(vec![
            (
                Arc::new(Field::new("fk_catalog", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["my_catalog"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("fk_db_schema", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["my_db_schema"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("fk_table", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["my_table"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("fk_column_name", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["my_column"])) as ArrayRef,
            ),
        ]);

        let constraint_column_usage_array = ListArray::new(
            Arc::new(Field::new("item", schemas::USAGE_SCHEMA.clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(constraint_column_usage_array_inner),
            None,
        );

        let constraint_column_names_array = ListArray::new(
            Arc::new(Field::new("item", DataType::Utf8, true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(StringArray::from(vec!["my_other_column"])),
            None,
        );

        let table_constraints_array_inner = StructArray::from(vec![
            (
                Arc::new(Field::new("constraint_name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["my_constraint"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("constraint_type", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["FOREIGN KEY"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "constraint_column_names",
                    DataType::new_list(DataType::Utf8, true),
                    false,
                )),
                Arc::new(constraint_column_names_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "constraint_column_usage",
                    DataType::new_list(schemas::USAGE_SCHEMA.clone(), true),
                    true,
                )),
                Arc::new(constraint_column_usage_array) as ArrayRef,
            ),
        ]);

        let table_columns_array_inner = StructArray::from(vec![
            (
                Arc::new(Field::new("column_name", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["my_column"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("ordinal_position", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![0])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("remarks", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["Nice column!"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_data_type", DataType::Int16, true)),
                Arc::new(Int16Array::from(vec![0])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_type_name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["my_type"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_column_size", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![42])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_decimal_digits", DataType::Int16, true)),
                Arc::new(Int16Array::from(vec![42])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_num_prec_radix", DataType::Int16, true)),
                Arc::new(Int16Array::from(vec![42])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_nullable", DataType::Int16, true)),
                Arc::new(Int16Array::from(vec![42])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_column_def", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["column_def"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_sql_data_type", DataType::Int16, true)),
                Arc::new(Int16Array::from(vec![42])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_datetime_sub", DataType::Int16, true)),
                Arc::new(Int16Array::from(vec![42])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_char_octet_length", DataType::Int32, true)),
                Arc::new(Int32Array::from(vec![42])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_is_nullable", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["YES"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_scope_catalog", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["MyCatalog"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_scope_schema", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["MySchema"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_scope_table", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["MyTable"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("xdbc_is_autoincrement", DataType::Boolean, true)),
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "xdbc_is_generatedcolumn",
                    DataType::Boolean,
                    true,
                )),
                Arc::new(BooleanArray::from(vec![true])) as ArrayRef,
            ),
        ]);

        let table_columns_array = ListArray::new(
            Arc::new(Field::new("item", schemas::COLUMN_SCHEMA.clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(table_columns_array_inner),
            None,
        );

        let table_constraints_array = ListArray::new(
            Arc::new(Field::new("item", schemas::CONSTRAINT_SCHEMA.clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(table_constraints_array_inner),
            None,
        );

        let db_schema_tables_array_inner = StructArray::from(vec![
            (
                Arc::new(Field::new("table_name", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["default"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("table_type", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["table"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "table_columns",
                    DataType::new_list(schemas::COLUMN_SCHEMA.clone(), true),
                    true,
                )),
                Arc::new(table_columns_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "table_constraints",
                    DataType::new_list(schemas::CONSTRAINT_SCHEMA.clone(), true),
                    true,
                )),
                Arc::new(table_constraints_array) as ArrayRef,
            ),
        ]);

        let db_schema_tables_array = ListArray::new(
            Arc::new(Field::new("item", schemas::TABLE_SCHEMA.clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(db_schema_tables_array_inner),
            None,
        );

        let catalog_db_schemas_array_inner = StructArray::from(vec![
            (
                Arc::new(Field::new("db_schema_name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["default"])) as ArrayRef,
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

        let catalog_name_array = StringArray::from(vec!["default"]);
        let catalog_db_schemas_array = ListArray::new(
            Arc::new(Field::new(
                "item",
                schemas::OBJECTS_DB_SCHEMA_SCHEMA.clone(),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(catalog_db_schemas_array_inner),
            None,
        );

        let batch = RecordBatch::try_new(
            schemas::GET_OBJECTS_SCHEMA.clone(),
            vec![
                Arc::new(catalog_name_array),
                Arc::new(catalog_db_schemas_array),
            ],
        )?;
        let reader = SingleBatchReader::new(batch);
        Ok(reader)
    }

    fn get_statistics(
        &self,
        _catalog: Option<&str>,
        _db_schema: Option<&str>,
        _table_name: Option<&str>,
        _approximate: bool,
    ) -> Result<impl RecordBatchReader> {
        let statistic_value_int64_array = Int64Array::from(Vec::<i64>::new());
        let statistic_value_uint64_array = UInt64Array::from(vec![42]);
        let statistic_value_float64_array = Float64Array::from(Vec::<f64>::new());
        let statistic_value_binary_array = BinaryArray::from(Vec::<&[u8]>::new());
        let type_id_buffer = [1_i8].into_iter().collect::<ScalarBuffer<i8>>();
        let value_offsets_buffer = [0_i32].into_iter().collect::<ScalarBuffer<i32>>();
        let statistic_value_array = UnionArray::try_new(
            UnionFields::new(
                [0, 1, 2, 3],
                [
                    Field::new("int64", DataType::Int64, true),
                    Field::new("uint64", DataType::UInt64, true),
                    Field::new("float64", DataType::Float64, true),
                    Field::new("binary", DataType::Binary, true),
                ],
            ),
            type_id_buffer,
            Some(value_offsets_buffer),
            vec![
                Arc::new(statistic_value_int64_array),
                Arc::new(statistic_value_uint64_array),
                Arc::new(statistic_value_float64_array),
                Arc::new(statistic_value_binary_array),
            ],
        )?;

        let db_schema_statistics_array_inner = StructArray::from(vec![
            (
                Arc::new(Field::new("table_name", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["default"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("column_name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["my_column"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("statistic_key", DataType::Int16, false)),
                Arc::new(Int16Array::from(vec![Into::<i16>::into(
                    Statistics::AverageByteWidth,
                )])) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "statistic_value",
                    schemas::STATISTIC_VALUE_SCHEMA.clone(),
                    false,
                )),
                Arc::new(statistic_value_array) as ArrayRef,
            ),
            (
                Arc::new(Field::new(
                    "statistic_is_approximate",
                    DataType::Boolean,
                    false,
                )),
                Arc::new(BooleanArray::from(vec![false])) as ArrayRef,
            ),
        ]);

        let db_schema_statistics_array = ListArray::new(
            Arc::new(Field::new("item", schemas::STATISTICS_SCHEMA.clone(), true)),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(db_schema_statistics_array_inner),
            None,
        );

        let catalog_db_schemas_array_inner = StructArray::from(vec![
            (
                Arc::new(Field::new("db_schema_name", DataType::Utf8, true)),
                Arc::new(StringArray::from(vec!["default"])) as ArrayRef,
            ),
            (
                Arc::new(Field::new_list(
                    "db_schema_statistics",
                    Arc::new(Field::new("item", schemas::STATISTICS_SCHEMA.clone(), true)),
                    false,
                )),
                Arc::new(db_schema_statistics_array) as ArrayRef,
            ),
        ]);

        let catalog_name_array = StringArray::from(vec!["default"]);
        let catalog_db_schemas_array = ListArray::new(
            Arc::new(Field::new(
                "item",
                schemas::STATISTICS_DB_SCHEMA_SCHEMA.clone(),
                true,
            )),
            OffsetBuffer::new(ScalarBuffer::from(vec![0, 1])),
            Arc::new(catalog_db_schemas_array_inner),
            None,
        );

        let batch = RecordBatch::try_new(
            schemas::GET_STATISTICS_SCHEMA.clone(),
            vec![
                Arc::new(catalog_name_array),
                Arc::new(catalog_db_schemas_array),
            ],
        )?;

        let reader = SingleBatchReader::new(batch);
        Ok(reader)
    }

    fn get_statistic_names(&self) -> Result<impl RecordBatchReader> {
        let name_array = StringArray::from(vec!["sum", "min", "max"]);
        let key_array = Int16Array::from(vec![0, 1, 2]);
        let batch = RecordBatch::try_new(
            schemas::GET_STATISTIC_NAMES_SCHEMA.clone(),
            vec![Arc::new(name_array), Arc::new(key_array)],
        )?;
        let reader = SingleBatchReader::new(batch);
        Ok(reader)
    }

    fn get_table_schema(
        &self,
        catalog: Option<&str>,
        db_schema: Option<&str>,
        table_name: &str,
    ) -> Result<arrow_schema::Schema> {
        let catalog = catalog.unwrap_or("default");
        let db_schema = db_schema.unwrap_or("default");

        if catalog == "default" && db_schema == "default" && table_name == "default" {
            Ok(get_table_schema())
        } else {
            Err(Error::with_message_and_status(
                format!("Table {catalog}.{db_schema}.{table_name} does not exist"),
                Status::NotFound,
            ))
        }
    }

    fn get_table_types(&self) -> Result<impl RecordBatchReader> {
        let array = Arc::new(StringArray::from(vec!["table", "view"]));
        let batch = RecordBatch::try_new(schemas::GET_TABLE_TYPES_SCHEMA.clone(), vec![array])?;
        let reader = SingleBatchReader::new(batch);
        Ok(reader)
    }

    fn read_partition(&self, _partition: impl AsRef<[u8]>) -> Result<impl RecordBatchReader> {
        let batch = get_table_data();
        let reader = SingleBatchReader::new(batch);
        Ok(reader)
    }

    fn rollback(&mut self) -> Result<()> {
        Ok(())
    }
}

#[derive(Default)]
pub struct DummyStatement {
    options: HashMap<OptionStatement, OptionValue>,
}

impl Optionable for DummyStatement {
    type Option = OptionStatement;

    fn set_option(&mut self, key: Self::Option, value: OptionValue) -> Result<()> {
        set_option(&mut self.options, key, value)
    }

    fn get_option_bytes(&self, key: Self::Option) -> Result<Vec<u8>> {
        get_option_bytes(&self.options, key, "statement")
    }

    fn get_option_double(&self, key: Self::Option) -> Result<f64> {
        get_option_double(&self.options, key, "statement")
    }

    fn get_option_int(&self, key: Self::Option) -> Result<i64> {
        get_option_int(&self.options, key, "statement")
    }

    fn get_option_string(&self, key: Self::Option) -> Result<String> {
        get_option_string(&self.options, key, "statement")
    }
}

impl Statement for DummyStatement {
    fn bind(&mut self, _batch: RecordBatch) -> Result<()> {
        Ok(())
    }

    fn bind_stream(&mut self, _reader: Box<dyn RecordBatchReader + Send>) -> Result<()> {
        Ok(())
    }

    fn cancel(&mut self) -> Result<()> {
        Ok(())
    }

    fn execute(&mut self) -> Result<impl RecordBatchReader> {
        maybe_panic("StatementExecuteQuery");
        let batch = get_table_data();
        let reader = SingleBatchReader::new(batch);
        Ok(reader)
    }

    fn execute_partitions(&mut self) -> Result<PartitionedResult> {
        Ok(PartitionedResult {
            partitions: vec![b"AAA".into(), b"ZZZZZ".into()],
            schema: get_table_schema(),
            rows_affected: 0,
        })
    }

    fn execute_schema(&mut self) -> Result<Schema> {
        Ok(get_table_schema())
    }

    fn execute_update(&mut self) -> Result<Option<i64>> {
        Ok(Some(0))
    }

    fn get_parameter_schema(&self) -> Result<Schema> {
        Ok(get_table_schema())
    }

    fn prepare(&mut self) -> Result<()> {
        Ok(())
    }

    fn set_sql_query(&mut self, _query: impl AsRef<str>) -> Result<()> {
        Ok(())
    }

    fn set_substrait_plan(&mut self, _plan: impl AsRef<[u8]>) -> Result<()> {
        Ok(())
    }
}

impl Drop for DummyStatement {
    fn drop(&mut self) {
        maybe_panic("StatementClose");
    }
}

adbc_ffi::export_driver!(AdbcDummyInit, DummyDriver);
