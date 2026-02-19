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

package internal_test

import (
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/extensions"
	"github.com/stretchr/testify/require"
)

func TestToXdbcDataType(t *testing.T) {
	tests := []struct {
		name     string
		dataType arrow.DataType
		expected internal.XdbcDataType
	}{
		{
			name:     "Nil type",
			dataType: nil,
			expected: internal.XdbcDataType_XDBC_UNKNOWN_TYPE,
		},
		{
			name:     "INT8",
			dataType: arrow.PrimitiveTypes.Int8,
			expected: internal.XdbcDataType_XDBC_TINYINT,
		},
		{
			name:     "INT16",
			dataType: arrow.PrimitiveTypes.Int16,
			expected: internal.XdbcDataType_XDBC_SMALLINT,
		},
		{
			name:     "INT32",
			dataType: arrow.PrimitiveTypes.Int32,
			expected: internal.XdbcDataType_XDBC_INTEGER,
		},
		{
			name:     "INT64",
			dataType: arrow.PrimitiveTypes.Int64,
			expected: internal.XdbcDataType_XDBC_BIGINT,
		},
		{
			name:     "FLOAT32",
			dataType: arrow.PrimitiveTypes.Float32,
			expected: internal.XdbcDataType_XDBC_FLOAT,
		},
		{
			name:     "String",
			dataType: arrow.BinaryTypes.String,
			expected: internal.XdbcDataType_XDBC_VARCHAR,
		},
		{
			name:     "Binary",
			dataType: arrow.BinaryTypes.Binary,
			expected: internal.XdbcDataType_XDBC_BINARY,
		},
		{
			name:     "Boolean",
			dataType: arrow.FixedWidthTypes.Boolean,
			expected: internal.XdbcDataType_XDBC_BIT,
		},
		{
			name:     "Date32",
			dataType: arrow.FixedWidthTypes.Date32,
			expected: internal.XdbcDataType_XDBC_DATE,
		},
		{
			name:     "Timestamp",
			dataType: arrow.FixedWidthTypes.Timestamp_us,
			expected: internal.XdbcDataType_XDBC_TIMESTAMP,
		},
		{
			name:     "UUID Extension Type",
			dataType: extensions.NewUUIDType(),
			expected: internal.XdbcDataType_XDBC_GUID,
		},
		{
			name:     "Bool8 Extension Type",
			dataType: extensions.NewBool8Type(),
			expected: internal.XdbcDataType_XDBC_TINYINT,
		},
		{
			name:     "Opaque Extension Type",
			dataType: extensions.NewOpaqueType(arrow.BinaryTypes.String, "test.opaque", "test_vendor"),
			expected: internal.XdbcDataType_XDBC_VARCHAR,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := internal.ToXdbcDataType(tt.dataType)
			require.Equal(t, tt.expected, result, "Expected XDBC type %v for %s, got %v", tt.expected, tt.name, result)
		})
	}
}

func TestToXdbcDataType_ExtensionTypes(t *testing.T) {

	t.Run("JSON falls back to storage type", func(t *testing.T) {
		jsonType, err := extensions.NewJSONType(arrow.BinaryTypes.String)
		require.NoError(t, err)
		require.NotNil(t, jsonType)
		require.Equal(t, "arrow.json", jsonType.ExtensionName())

		// JSON storage type is String, which maps to VARCHAR
		xdbcType := internal.ToXdbcDataType(jsonType)
		require.Equal(t, internal.XdbcDataType_XDBC_VARCHAR, xdbcType)
	})

	t.Run("Opaque falls back to storage type", func(t *testing.T) {
		opaqueType := extensions.NewOpaqueType(arrow.BinaryTypes.Binary, "test.opaque", "test_vendor")
		require.NotNil(t, opaqueType)
		require.Equal(t, "arrow.opaque", opaqueType.ExtensionName())

		// Opaque storage type is Binary, which maps to BINARY
		xdbcType := internal.ToXdbcDataType(opaqueType)
		require.Equal(t, internal.XdbcDataType_XDBC_BINARY, xdbcType)
	})
}
