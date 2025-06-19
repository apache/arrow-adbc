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

package databricks

import (
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/databricks/databricks-sdk-go/service/sql"
	"github.com/stretchr/testify/assert"
)

func TestGetArrowTypeFromColumnInfo(t *testing.T) {
	tests := []struct {
		name     string
		col      sql.ColumnInfo
		expected arrow.DataType
		wantErr  bool
	}{
		// Basic types
		{
			name: "byte type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameByte,
			},
			expected: arrow.PrimitiveTypes.Int8,
		},
		{
			name: "short type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameShort,
			},
			expected: arrow.PrimitiveTypes.Int16,
		},
		{
			name: "int type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameInt,
			},
			expected: arrow.PrimitiveTypes.Int32,
		},
		{
			name: "long type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameLong,
			},
			expected: arrow.PrimitiveTypes.Int64,
		},
		{
			name: "float type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameFloat,
			},
			expected: arrow.PrimitiveTypes.Float32,
		},
		{
			name: "double type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameDouble,
			},
			expected: arrow.PrimitiveTypes.Float64,
		},
		{
			name: "string type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameString,
			},
			expected: arrow.BinaryTypes.String,
		},
		{
			name: "binary type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameBinary,
			},
			expected: arrow.BinaryTypes.Binary,
		},
		{
			name: "boolean type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameBoolean,
			},
			expected: arrow.FixedWidthTypes.Boolean,
		},
		{
			name: "date type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameDate,
			},
			expected: arrow.FixedWidthTypes.Date32,
		},
		{
			name: "null type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameNull,
			},
			expected: arrow.Null,
		},

		// Decimal types
		{
			name: "decimal type with precision and scale",
			col: sql.ColumnInfo{
				TypeName:      sql.ColumnInfoTypeNameDecimal,
				TypePrecision: 10,
				TypeScale:     2,
			},
			expected: &arrow.Decimal128Type{Precision: 10, Scale: 2},
		},
		{
			name: "decimal type with precision > 38",
			col: sql.ColumnInfo{
				TypeName:      sql.ColumnInfoTypeNameDecimal,
				TypePrecision: 39,
				TypeScale:     2,
			},
			expected: &arrow.Decimal256Type{Precision: 39, Scale: 2},
		},
		{
			name: "decimal type from type text",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameDecimal,
				TypeText: "DECIMAL(10,2)",
			},
			expected: &arrow.Decimal128Type{Precision: 10, Scale: 2},
		},

		// Timestamp type
		{
			name: "timestamp type",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameTimestamp,
			},
			expected: &arrow.TimestampType{
				Unit:     arrow.Microsecond,
				TimeZone: "Etc/UTC",
			},
		},

		// Array type
		{
			name: "array of int",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameArray,
				TypeText: "ARRAY<INT>",
			},
			expected: arrow.ListOf(arrow.PrimitiveTypes.Int32),
		},
		{
			name: "array of string",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameArray,
				TypeText: "ARRAY<STRING>",
			},
			expected: arrow.ListOf(arrow.BinaryTypes.String),
		},

		// Map type
		{
			name: "map of string to int",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameMap,
				TypeText: "MAP<STRING,INT>",
			},
			expected: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32),
		},

		// Struct type
		{
			name: "struct with basic fields",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameStruct,
				TypeText: "STRUCT<id:INT,name:STRING,active:BOOLEAN>",
			},
			expected: arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			),
		},
		// Interval types
		{
			name: "year to month interval",
			col: sql.ColumnInfo{
				TypeName:         sql.ColumnInfoTypeNameInterval,
				TypeIntervalType: "YEAR TO MONTH",
				TypeText:         "INTERVAL YEAR TO MONTH",
			},
			expected: arrow.FixedWidthTypes.MonthInterval,
		},

		// Error cases
		{
			name: "invalid array type format",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameArray,
				TypeText: "INVALID",
			},
			wantErr: true,
		},
		{
			name: "invalid map type format",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameMap,
				TypeText: "INVALID",
			},
			wantErr: true,
		},
		{
			name: "invalid struct type format",
			col: sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeNameStruct,
				TypeText: "INVALID",
			},
			wantErr: true,
		},
		{
			name: "unsupported type",
			col: sql.ColumnInfo{
				TypeName: "UNSUPPORTED",
			},
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getArrowTypeFromColumnInfo(tt.col)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}

func TestGetArrowTypeFromStringType(t *testing.T) {
	tests := []struct {
		name     string
		typeText string
		expected arrow.DataType
		wantErr  bool
	}{
		// DECIMAL types
		{
			name:     "decimal type with precision and scale",
			typeText: "DECIMAL(5,2)",
			expected: &arrow.Decimal128Type{Precision: 5, Scale: 2},
		},
		{
			name:     "decimal type with precision only",
			typeText: "DECIMAL(10)",
			expected: &arrow.Decimal128Type{Precision: 10, Scale: 0},
		},
		{
			name:     "dec type with precision and scale",
			typeText: "DEC(8,3)",
			expected: &arrow.Decimal128Type{Precision: 8, Scale: 3},
		},
		{
			name:     "numeric type with precision and scale",
			typeText: "NUMERIC(15,4)",
			expected: &arrow.Decimal128Type{Precision: 15, Scale: 4},
		},

		// ARRAY types
		{
			name:     "array of int",
			typeText: "ARRAY<INT>",
			expected: arrow.ListOf(arrow.PrimitiveTypes.Int32),
		},
		{
			name:     "array of string",
			typeText: "ARRAY<STRING>",
			expected: arrow.ListOf(arrow.BinaryTypes.String),
		},
		{
			name:     "array of decimal",
			typeText: "ARRAY<DECIMAL(10,2)>",
			expected: arrow.ListOf(&arrow.Decimal128Type{Precision: 10, Scale: 2}),
		},
		{
			name:     "nested array",
			typeText: "ARRAY<ARRAY<INT>>",
			expected: arrow.ListOf(arrow.ListOf(arrow.PrimitiveTypes.Int32)),
		},

		// MAP types
		{
			name:     "map of string to int",
			typeText: "MAP<STRING,INT>",
			expected: arrow.MapOf(arrow.BinaryTypes.String, arrow.PrimitiveTypes.Int32),
		},
		{
			name:     "map of int to string",
			typeText: "MAP<INT,STRING>",
			expected: arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.BinaryTypes.String),
		},
		{
			name:     "map with complex key and value",
			typeText: "MAP<DECIMAL(5,2),ARRAY<STRING>>",
			expected: arrow.MapOf(&arrow.Decimal128Type{Precision: 5, Scale: 2}, arrow.ListOf(arrow.BinaryTypes.String)),
		},

		// STRUCT types
		{
			name:     "struct with basic fields",
			typeText: "STRUCT<id:INT,name:STRING,active:BOOLEAN>",
			expected: arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
				arrow.Field{Name: "active", Type: arrow.FixedWidthTypes.Boolean, Nullable: true},
			),
		},
		{
			name:     "struct with not null field",
			typeText: "STRUCT<id:INT NOT NULL,name:STRING>",
			expected: arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			),
		},
		{
			name:     "struct with extra whitespace",
			typeText: "STRUCT<id: INT  NOT  NULL,    name : STRING>",
			expected: arrow.StructOf(
				arrow.Field{Name: "id", Type: arrow.PrimitiveTypes.Int32, Nullable: false},
				arrow.Field{Name: "name", Type: arrow.BinaryTypes.String, Nullable: true},
			),
		},
		{
			name:     "struct with nested types",
			typeText: "STRUCT<data:ARRAY<INT>,metadata:MAP<STRING,STRING>>",
			expected: arrow.StructOf(
				arrow.Field{Name: "data", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
				arrow.Field{Name: "metadata", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
			),
		},
		{
			name:     "struct with comments",
			typeText: "STRUCT<data:ARRAY<INT> COMMENT \"blah blah blah\",metadata:MAP<STRING,STRING>>",
			expected: arrow.StructOf(
				arrow.Field{Name: "data", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
				arrow.Field{Name: "metadata", Type: arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String), Nullable: true},
			),
		},
		{
			name:     "struct with nested decimal",
			typeText: "STRUCT<data:ARRAY<INT>,metadata:MAP<DECIMAL(10,2),STRING>>",
			expected: arrow.StructOf(
				arrow.Field{Name: "data", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
				arrow.Field{Name: "metadata", Type: arrow.MapOf(&arrow.Decimal128Type{Precision: 10, Scale: 2}, arrow.BinaryTypes.String), Nullable: true},
			),
		},
		{
			name:     "struct with interval type",
			typeText: "STRUCT<data:ARRAY<INT>,metadata:MAP<DECIMAL(10,2),INTERVAL MONTH>>",
			expected: arrow.StructOf(
				arrow.Field{Name: "data", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
				arrow.Field{Name: "metadata", Type: arrow.MapOf(&arrow.Decimal128Type{Precision: 10, Scale: 2}, arrow.FixedWidthTypes.MonthInterval), Nullable: true},
			),
		},
		{
			name:     "struct with decimal and interval type",
			typeText: "STRUCT<data:ARRAY<INT>,dece:DECIMAL(8,8),inter:INTERVAL MONTH>>",
			expected: arrow.StructOf(
				arrow.Field{Name: "data", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true},
				arrow.Field{Name: "dece", Type: &arrow.Decimal128Type{Precision: 8, Scale: 8}, Nullable: true},
				arrow.Field{Name: "inter", Type: arrow.FixedWidthTypes.MonthInterval, Nullable: true},
			),
		},
		{
			name:     "struct with nested struct",
			typeText: "STRUCT<address:STRUCT<street:STRING,city:STRING>,age:INT>",
			expected: arrow.StructOf(
				arrow.Field{Name: "address", Type: arrow.StructOf(
					arrow.Field{Name: "street", Type: arrow.BinaryTypes.String, Nullable: true},
					arrow.Field{Name: "city", Type: arrow.BinaryTypes.String, Nullable: true},
				), Nullable: true},
				arrow.Field{Name: "age", Type: arrow.PrimitiveTypes.Int32, Nullable: true},
			),
		},

		// INTERVAL types
		{
			name:     "year to month interval",
			typeText: "INTERVAL YEAR TO MONTH",
			expected: arrow.FixedWidthTypes.MonthInterval,
		},
		{
			name:     "year interval",
			typeText: "INTERVAL YEAR",
			expected: arrow.FixedWidthTypes.MonthInterval,
		},
		{
			name:     "month interval",
			typeText: "INTERVAL MONTH",
			expected: arrow.FixedWidthTypes.MonthInterval,
		},
		{
			name:     "day to hour interval",
			typeText: "INTERVAL DAY TO HOUR",
			expected: arrow.FixedWidthTypes.Duration_s,
		},
		{
			name:     "day to minute interval",
			typeText: "INTERVAL DAY TO MINUTE",
			expected: arrow.FixedWidthTypes.Duration_s,
		},
		{
			name:     "day to second interval",
			typeText: "INTERVAL DAY TO SECOND",
			expected: arrow.FixedWidthTypes.Duration_s,
		},
		{
			name:     "hour to minute interval",
			typeText: "INTERVAL HOUR TO MINUTE",
			expected: arrow.FixedWidthTypes.Duration_s,
		},
		{
			name:     "hour to second interval",
			typeText: "INTERVAL HOUR TO SECOND",
			expected: arrow.FixedWidthTypes.Duration_s,
		},
		{
			name:     "minute to second interval",
			typeText: "INTERVAL MINUTE TO SECOND",
			expected: arrow.FixedWidthTypes.Duration_s,
		},
		{
			name:     "day interval",
			typeText: "INTERVAL DAY",
			expected: arrow.FixedWidthTypes.Duration_s,
		},
		{
			name:     "hour interval",
			typeText: "INTERVAL HOUR",
			expected: arrow.FixedWidthTypes.Duration_s,
		},
		{
			name:     "minute interval",
			typeText: "INTERVAL MINUTE",
			expected: arrow.FixedWidthTypes.Duration_s,
		},
		{
			name:     "second interval",
			typeText: "INTERVAL SECOND",
			expected: arrow.FixedWidthTypes.Duration_s,
		},

		// Error cases
		{
			name:     "invalid decimal format",
			typeText: "DECIMAL(abc,def)",
			wantErr:  true,
		},
		{
			name:     "invalid array format",
			typeText: "ARRAY<",
			wantErr:  true,
		},
		{
			name:     "invalid map format",
			typeText: "MAP<STRING>",
			wantErr:  true,
		},
		{
			name:     "invalid struct format",
			typeText: "STRUCT<id:INT,name>",
			wantErr:  true,
		},
		{
			name:     "invalid interval format",
			typeText: "INTERVAL INVALID",
			wantErr:  true,
		},
		{
			name:     "unrecognized type",
			typeText: "UNKNOWN_TYPE",
			wantErr:  true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := getArrowTypeFromStringType(tt.typeText)
			if tt.wantErr {
				assert.Error(t, err)
				return
			}
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, got)
		})
	}
}
