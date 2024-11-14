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

package adbc

import "github.com/apache/arrow-go/v18/arrow"

var (
	GetInfoSchema = arrow.NewSchema([]arrow.Field{
		{Name: "info_name", Type: arrow.PrimitiveTypes.Uint32},
		{Name: "info_value", Type: arrow.DenseUnionOf(
			[]arrow.Field{
				{Name: "string_value", Type: arrow.BinaryTypes.String, Nullable: true},
				{Name: "bool_value", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
				{Name: "int64_value", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
				{Name: "int32_bitmask", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				{Name: "string_list", Type: arrow.ListOf(arrow.BinaryTypes.String), Nullable: true},
				{Name: "int32_to_int32_list_map",
					Type: arrow.MapOf(arrow.PrimitiveTypes.Int32,
						arrow.ListOf(arrow.PrimitiveTypes.Int32)), Nullable: true},
			},
			[]arrow.UnionTypeCode{0, 1, 2, 3, 4, 5},
		), Nullable: true},
	}, nil)

	TableTypesSchema = arrow.NewSchema([]arrow.Field{{Name: "table_type", Type: arrow.BinaryTypes.String}}, nil)

	UsageSchema = arrow.StructOf(
		arrow.Field{Name: "fk_catalog", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "fk_db_schema", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "fk_table", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "fk_column_name", Type: arrow.BinaryTypes.String},
	)

	ConstraintSchema = arrow.StructOf(
		arrow.Field{Name: "constraint_name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "constraint_type", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "constraint_column_names", Type: arrow.ListOf(arrow.BinaryTypes.String)},
		arrow.Field{Name: "constraint_column_usage", Type: arrow.ListOf(UsageSchema), Nullable: true},
	)

	ColumnSchema = arrow.StructOf(
		arrow.Field{Name: "column_name", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "ordinal_position", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "remarks", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_data_type", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_type_name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_column_size", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "xdbc_decimal_digits", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_num_prec_radix", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_nullable", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_column_def", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_sql_data_type", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_datetime_sub", Type: arrow.PrimitiveTypes.Int16, Nullable: true},
		arrow.Field{Name: "xdbc_char_octet_length", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
		arrow.Field{Name: "xdbc_is_nullable", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_scope_catalog", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_scope_schema", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_scope_table", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "xdbc_is_autoincrement", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
		arrow.Field{Name: "xdbc_is_generatedcolumn", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
	)

	TableSchema = arrow.StructOf(
		arrow.Field{Name: "table_name", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "table_type", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "table_columns", Type: arrow.ListOf(ColumnSchema), Nullable: true},
		arrow.Field{Name: "table_constraints", Type: arrow.ListOf(ConstraintSchema), Nullable: true},
	)

	DBSchemaSchema = arrow.StructOf(
		arrow.Field{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "db_schema_tables", Type: arrow.ListOf(TableSchema), Nullable: true},
	)

	GetObjectsSchema = arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "catalog_db_schemas", Type: arrow.ListOf(DBSchemaSchema), Nullable: true},
	}, nil)

	StatisticsSchema = arrow.StructOf(
		arrow.Field{Name: "table_name", Type: arrow.BinaryTypes.String, Nullable: false},
		arrow.Field{Name: "column_name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "statistic_key", Type: arrow.PrimitiveTypes.Int16, Nullable: false},
		arrow.Field{Name: "statistic_value", Type: arrow.DenseUnionOf([]arrow.Field{
			{Name: "int64", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "uint64", Type: arrow.PrimitiveTypes.Uint64, Nullable: true},
			{Name: "float64", Type: arrow.PrimitiveTypes.Float64, Nullable: true},
			{Name: "binary", Type: arrow.BinaryTypes.Binary, Nullable: true},
		}, []arrow.UnionTypeCode{0, 1, 2, 3}), Nullable: false},
		arrow.Field{Name: "statistic_is_approximate", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	)

	StatisticsDBSchemaSchema = arrow.StructOf(
		arrow.Field{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: true},
		arrow.Field{Name: "db_schema_statistics", Type: arrow.ListOf(StatisticsSchema), Nullable: false},
	)

	GetStatisticsSchema = arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "catalog_db_schemas", Type: arrow.ListOf(StatisticsDBSchemaSchema), Nullable: false},
	}, nil)

	GetStatisticNamesSchema = arrow.NewSchema([]arrow.Field{
		{Name: "statistic_name", Type: arrow.BinaryTypes.String, Nullable: false},
		{Name: "statistic_key", Type: arrow.PrimitiveTypes.Int16, Nullable: false},
	}, nil)

	GetTableSchemaSchema = arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "db_schema_name", Type: arrow.BinaryTypes.String, Nullable: true},
		{Name: "table_name", Type: arrow.BinaryTypes.String},
		{Name: "table_type", Type: arrow.BinaryTypes.String},
		{Name: "table_schema", Type: arrow.BinaryTypes.Binary},
	}, nil)
)
