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

//! Schemas and data types that appear through ADBC.

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef, UnionFields, UnionMode};
use once_cell::sync::Lazy;

/// Schema of the data returned by [get_table_types][crate::Connection::get_table_types].
pub static GET_TABLE_TYPES_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![Field::new(
        "table_type",
        DataType::Utf8,
        false,
    )]))
});

/// Schema of the data returned by [get_info][crate::Connection::get_info].
pub static GET_INFO_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    let info_schema = DataType::Union(
        UnionFields::new(
            vec![0, 1, 2, 3, 4, 5],
            vec![
                Field::new("string_value", DataType::Utf8, true),
                Field::new("bool_value", DataType::Boolean, true),
                Field::new("int64_value", DataType::Int64, true),
                Field::new("int32_bitmask", DataType::Int32, true),
                Field::new_list(
                    "string_list",
                    Field::new_list_field(DataType::Utf8, true),
                    true,
                ),
                Field::new_map(
                    "int32_to_int32_list_map",
                    "entries",
                    Field::new("key", DataType::Int32, false),
                    Field::new_list("value", Field::new_list_field(DataType::Int32, true), true),
                    false,
                    true,
                ),
            ],
        ),
        UnionMode::Dense,
    );

    Arc::new(Schema::new(vec![
        Field::new("info_name", DataType::UInt32, false),
        Field::new("info_value", info_schema, true),
    ]))
});

/// Schema of data returned by [get_statistic_names][crate::Connection::get_statistic_names].
pub static GET_STATISTIC_NAMES_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("statistic_name", DataType::Utf8, false),
        Field::new("statistic_key", DataType::Int16, false),
    ]))
});

pub static STATISTIC_VALUE_SCHEMA: Lazy<DataType> = Lazy::new(|| {
    DataType::Union(
        UnionFields::new(
            vec![0, 1, 2, 3],
            vec![
                Field::new("int64", DataType::Int64, true),
                Field::new("uint64", DataType::UInt64, true),
                Field::new("float64", DataType::Float64, true),
                Field::new("binary", DataType::Binary, true),
            ],
        ),
        UnionMode::Dense,
    )
});

pub static STATISTICS_SCHEMA: Lazy<DataType> = Lazy::new(|| {
    DataType::Struct(
        vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("column_name", DataType::Utf8, true),
            Field::new("statistic_key", DataType::Int16, false),
            Field::new("statistic_value", STATISTIC_VALUE_SCHEMA.clone(), false),
            Field::new("statistic_is_approximate", DataType::Boolean, false),
        ]
        .into(),
    )
});

pub static STATISTICS_DB_SCHEMA_SCHEMA: Lazy<DataType> = Lazy::new(|| {
    DataType::Struct(
        vec![
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new(
                "db_schema_statistics",
                DataType::new_list(STATISTICS_SCHEMA.clone(), true),
                false,
            ),
        ]
        .into(),
    )
});

/// Schema of data returned by [get_statistics][crate::Connection::get_statistics].
pub static GET_STATISTICS_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new(
            "catalog_db_schemas",
            DataType::new_list(STATISTICS_DB_SCHEMA_SCHEMA.clone(), true),
            false,
        ),
    ]))
});

pub static USAGE_SCHEMA: Lazy<DataType> = Lazy::new(|| {
    DataType::Struct(
        vec![
            Field::new("fk_catalog", DataType::Utf8, true),
            Field::new("fk_db_schema", DataType::Utf8, true),
            Field::new("fk_table", DataType::Utf8, false),
            Field::new("fk_column_name", DataType::Utf8, false),
        ]
        .into(),
    )
});

pub static CONSTRAINT_SCHEMA: Lazy<DataType> = Lazy::new(|| {
    DataType::Struct(
        vec![
            Field::new("constraint_name", DataType::Utf8, true),
            Field::new("constraint_type", DataType::Utf8, false),
            Field::new(
                "constraint_column_names",
                DataType::new_list(DataType::Utf8, true),
                false,
            ),
            Field::new(
                "constraint_column_usage",
                DataType::new_list(USAGE_SCHEMA.clone(), true),
                true,
            ),
        ]
        .into(),
    )
});

pub static COLUMN_SCHEMA: Lazy<DataType> = Lazy::new(|| {
    DataType::Struct(
        vec![
            Field::new("column_name", DataType::Utf8, false),
            Field::new("ordinal_position", DataType::Int32, true),
            Field::new("remarks", DataType::Utf8, true),
            Field::new("xdbc_data_type", DataType::Int16, true),
            Field::new("xdbc_type_name", DataType::Utf8, true),
            Field::new("xdbc_column_size", DataType::Int32, true),
            Field::new("xdbc_decimal_digits", DataType::Int16, true),
            Field::new("xdbc_num_prec_radix", DataType::Int16, true),
            Field::new("xdbc_nullable", DataType::Int16, true),
            Field::new("xdbc_column_def", DataType::Utf8, true),
            Field::new("xdbc_sql_data_type", DataType::Int16, true),
            Field::new("xdbc_datetime_sub", DataType::Int16, true),
            Field::new("xdbc_char_octet_length", DataType::Int32, true),
            Field::new("xdbc_is_nullable", DataType::Utf8, true),
            Field::new("xdbc_scope_catalog", DataType::Utf8, true),
            Field::new("xdbc_scope_schema", DataType::Utf8, true),
            Field::new("xdbc_scope_table", DataType::Utf8, true),
            Field::new("xdbc_is_autoincrement", DataType::Boolean, true),
            Field::new("xdbc_is_generatedcolumn", DataType::Boolean, true),
        ]
        .into(),
    )
});

pub static TABLE_SCHEMA: Lazy<DataType> = Lazy::new(|| {
    DataType::Struct(
        vec![
            Field::new("table_name", DataType::Utf8, false),
            Field::new("table_type", DataType::Utf8, false),
            Field::new(
                "table_columns",
                DataType::new_list(COLUMN_SCHEMA.clone(), true),
                true,
            ),
            Field::new(
                "table_constraints",
                DataType::new_list(CONSTRAINT_SCHEMA.clone(), true),
                true,
            ),
        ]
        .into(),
    )
});

pub static OBJECTS_DB_SCHEMA_SCHEMA: Lazy<DataType> = Lazy::new(|| {
    DataType::Struct(
        vec![
            Field::new("db_schema_name", DataType::Utf8, true),
            Field::new(
                "db_schema_tables",
                DataType::new_list(TABLE_SCHEMA.clone(), true),
                true,
            ),
        ]
        .into(),
    )
});

/// Schema of data returned by [get_objects][crate::Connection::get_objects].
pub static GET_OBJECTS_SCHEMA: Lazy<SchemaRef> = Lazy::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("catalog_name", DataType::Utf8, true),
        Field::new(
            "catalog_db_schemas",
            DataType::new_list(OBJECTS_DB_SCHEMA_SCHEMA.clone(), true),
            true,
        ),
    ]))
});
