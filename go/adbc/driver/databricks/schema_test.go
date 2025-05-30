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
				TimeZone: "UTC",
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
			},
			expected: arrow.FixedWidthTypes.MonthInterval,
		},
		{
			name: "day to second interval",
			col: sql.ColumnInfo{
				TypeName:         sql.ColumnInfoTypeNameInterval,
				TypeIntervalType: "DAY TO SECOND",
			},
			expected: arrow.FixedWidthTypes.DayTimeInterval,
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
