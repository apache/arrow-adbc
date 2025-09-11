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

package sqldriver

import (
	"database/sql/driver"
	"encoding/base64"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestParseConnectStr(t *testing.T) {
	const (
		scheme   = "grpc+tcp"
		host     = "host"
		port     = 443
		dbname   = "dbname"
		username = "username"
		password = "token=="
	)

	var (
		uri = fmt.Sprintf("%s://%s:%d/%s", scheme, host, port, dbname)
	)

	dsn := strings.Join([]string{
		fmt.Sprintf("%s=%s", adbc.OptionKeyURI, uri),
		fmt.Sprintf("%s=%s", adbc.OptionKeyUsername, username),
		fmt.Sprintf("%s=%s", adbc.OptionKeyPassword, password),
		fmt.Sprintf("%s=%s", adbc.OptionKeyReadOnly, adbc.OptionValueEnabled),
	}, " ; ")

	expectOpts := map[string]string{
		adbc.OptionKeyURI:      uri,
		adbc.OptionKeyUsername: username,
		adbc.OptionKeyPassword: password,
		adbc.OptionKeyReadOnly: adbc.OptionValueEnabled,
	}

	gotOpts, err := parseConnectStr(dsn)
	if assert.NoError(t, err) {
		assert.Equal(t, expectOpts, gotOpts)
	}
}

func TestColumnTypeDatabaseTypeName(t *testing.T) {
	tests := []struct {
		typ      arrow.DataType
		typeName string
	}{
		{
			typ:      &arrow.StringType{},
			typeName: "utf8",
		},
		{
			typ:      &arrow.Date32Type{},
			typeName: "date32",
		},
		{
			typ:      &arrow.Date64Type{},
			typeName: "date64",
		},
		{
			typ:      &arrow.TimestampType{Unit: arrow.Second, TimeZone: "utc"},
			typeName: "timestamp[s, tz=utc]",
		},
		{
			typ:      &arrow.TimestampType{Unit: arrow.Millisecond},
			typeName: "timestamp[ms]",
		},
		{
			typ:      &arrow.Time32Type{Unit: arrow.Second},
			typeName: "time32[s]",
		},
		{
			typ:      &arrow.Time32Type{Unit: arrow.Microsecond},
			typeName: "time32[us]",
		},
		{
			typ:      &arrow.Time64Type{Unit: arrow.Second},
			typeName: "time64[s]",
		},
		{
			typ:      &arrow.Time64Type{Unit: arrow.Nanosecond},
			typeName: "time64[ns]",
		},
		{
			typ:      &arrow.DurationType{Unit: arrow.Nanosecond},
			typeName: "duration[ns]",
		},
		{
			typ:      &arrow.Decimal128Type{Precision: 9, Scale: 2},
			typeName: "decimal(9, 2)",
		},
		{
			typ:      &arrow.Decimal256Type{Precision: 28, Scale: 4},
			typeName: "decimal256(28, 4)",
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d-%s", i, test.typeName), func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Type: test.typ}}, nil)
			reader, err := array.NewRecordReader(schema, nil)
			require.NoError(t, err)
			r := &rows{rdr: reader}
			assert.Equal(t, test.typeName, r.ColumnTypeDatabaseTypeName(0))
		})
	}
}

var (
	tz          = time.FixedZone("North Idaho", -int((8 * time.Hour).Seconds()))
	testTime    = time.Date(2023, time.January, 26, 15, 40, 39, 123456789, tz)
	stringField = arrow.Field{
		Name: "str",
		Type: arrow.BinaryTypes.String,
	}
	int32Field = arrow.Field{
		Name: "int",
		Type: arrow.PrimitiveTypes.Int32,
	}

	tstampSec, _   = arrow.TimestampFromTime(testTime, arrow.Second)
	tstampMilli, _ = arrow.TimestampFromTime(testTime, arrow.Millisecond)
	tstampMicro, _ = arrow.TimestampFromTime(testTime, arrow.Microsecond)
	tstampNano, _  = arrow.TimestampFromTime(testTime, arrow.Nanosecond)
)

func TestNextRowTypes(t *testing.T) {
	tests := []struct {
		arrowType      arrow.DataType
		arrowValueFunc func(*testing.T, array.Builder)
		golangValue    any
	}{
		{
			arrowType: &arrow.StringType{},
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				b.(*array.StringBuilder).Append("my-string")
			},
			golangValue: "my-string",
		},
		{
			arrowType: &arrow.Date32Type{},
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				b.(*array.Date32Builder).Append(arrow.Date32FromTime(testTime))
			},
			golangValue: testTime.UTC().Truncate(24 * time.Hour),
		},
		{
			arrowType: &arrow.Date64Type{},
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				b.(*array.Date64Builder).Append(arrow.Date64FromTime(testTime))
			},
			golangValue: testTime.UTC().Truncate(24 * time.Hour),
		},
		{
			arrowType: &arrow.TimestampType{Unit: arrow.Second, TimeZone: "North Idaho"},
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				s := testTime.Format("2006-01-02 15:04:05-07:00")
				timestamp, _, err := arrow.TimestampFromStringInLocation(s, arrow.Second, tz)
				require.NoError(t, err)
				b.(*array.TimestampBuilder).Append(timestamp)
			},
			golangValue: testTime.UTC().Truncate(time.Second),
		},
		{
			arrowType: &arrow.TimestampType{Unit: arrow.Millisecond},
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				s := testTime.Format("2006-01-02 15:04:05.999-07:00")
				timestamp, _, err := arrow.TimestampFromStringInLocation(s, arrow.Millisecond, tz)
				require.NoError(t, err)
				b.(*array.TimestampBuilder).Append(timestamp)
			},
			golangValue: testTime.UTC().Truncate(time.Millisecond),
		},
		{
			arrowType: &arrow.Time32Type{Unit: arrow.Second},
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				s := testTime.Format("15:04:05")
				t.Log(s)
				time32, err := arrow.Time32FromString(s, arrow.Second)
				require.NoError(t, err)
				b.(*array.Time32Builder).Append(time32)
				t.Log("end of avf")
			},
			golangValue: time.Date(1970, time.January, 1, testTime.Hour(), testTime.Minute(), testTime.Second(), 0, time.UTC),
		},
		{
			arrowType: &arrow.Time32Type{Unit: arrow.Millisecond},
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				s := testTime.Format("15:04:05.999")
				time32, err := arrow.Time32FromString(s, arrow.Millisecond)
				require.NoError(t, err)
				b.(*array.Time32Builder).Append(time32)
			},
			golangValue: time.Date(1970, time.January, 1, testTime.Hour(), testTime.Minute(), testTime.Second(), testTime.Nanosecond()-testTime.Nanosecond()%int(time.Millisecond), time.UTC),
		},
		{
			arrowType: &arrow.Time64Type{Unit: arrow.Microsecond},
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				s := testTime.Format("15:04:05.999999")
				time64, err := arrow.Time64FromString(s, arrow.Microsecond)
				require.NoError(t, err)
				b.(*array.Time64Builder).Append(time64)
			},
			golangValue: time.Date(1970, time.January, 1, testTime.Hour(), testTime.Minute(), testTime.Second(), testTime.Nanosecond()-testTime.Nanosecond()%int(time.Microsecond), time.UTC),
		},
		{
			arrowType: &arrow.Time64Type{Unit: arrow.Nanosecond},
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				s := testTime.Format("15:04:05.999999999")
				time64, err := arrow.Time64FromString(s, arrow.Nanosecond)
				require.NoError(t, err)
				b.(*array.Time64Builder).Append(time64)
			},
			golangValue: time.Date(1970, time.January, 1, testTime.Hour(), testTime.Minute(), testTime.Second(), testTime.Nanosecond(), time.UTC),
		},
		{
			arrowType: &arrow.Decimal128Type{Precision: 9, Scale: 2},
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				b.(*array.Decimal128Builder).Append(decimal128.FromU64(10))
			},
			golangValue: decimal128.FromU64(10),
		},
		{
			arrowType: &arrow.Decimal256Type{Precision: 10, Scale: 5},
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				b.(*array.Decimal256Builder).Append(decimal256.FromU64(10))
			},
			golangValue: decimal256.FromU64(10),
		},
		{
			arrowType: arrow.SparseUnionOf([]arrow.Field{stringField, int32Field}, []arrow.UnionTypeCode{0, 1}),
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				ub := b.(array.UnionBuilder)
				ub.Append(0)
				ub.Child(0).(*array.StringBuilder).Append("my-string")
				ub.Child(1).AppendEmptyValue()
			},
			golangValue: "my-string",
		},
		{
			arrowType: arrow.SparseUnionOf([]arrow.Field{stringField, int32Field}, []arrow.UnionTypeCode{0, 1}),
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				ub := b.(array.UnionBuilder)
				ub.Append(1)
				ub.Child(1).(*array.Int32Builder).Append(100)
				ub.Child(0).AppendEmptyValue()

			},
			golangValue: int32(100),
		},
		{
			arrowType: arrow.DenseUnionOf([]arrow.Field{int32Field, stringField}, []arrow.UnionTypeCode{10, 20}),
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				ub := b.(array.UnionBuilder)
				ub.Append(20)
				ub.Child(1).(*array.StringBuilder).Append("my-string")
			},
			golangValue: "my-string",
		},
		{
			arrowType: arrow.DenseUnionOf([]arrow.Field{int32Field, stringField}, []arrow.UnionTypeCode{10, 20}),
			arrowValueFunc: func(t *testing.T, b array.Builder) {
				t.Helper()
				b.(array.UnionBuilder).Append(10)
				b.(array.UnionBuilder).Child(0).(*array.Int32Builder).Append(100)
			},
			golangValue: int32(100),
		},
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d-%s", i, test.arrowType.String()), func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Type: test.arrowType}}, nil)
			recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			t.Cleanup(recordBuilder.Release)
			test.arrowValueFunc(t, recordBuilder.Field(0))
			record := recordBuilder.NewRecordBatch()
			t.Cleanup(record.Release)

			r := &rows{curRecord: record}
			dest := make([]driver.Value, 1)
			err := r.Next(dest)
			assert.NoError(t, err)
			assert.IsType(t, test.golangValue, dest[0])
			assert.Equal(t, test.golangValue, dest[0])
		})
	}
}

func TestArrFromVal(t *testing.T) {
	tests := []struct {
		value               any
		inputDataType       arrow.DataType
		expectedDataType    arrow.DataType
		expectedStringValue string
	}{
		{
			value:               true,
			expectedDataType:    arrow.FixedWidthTypes.Boolean,
			expectedStringValue: "true",
		},
		{
			value:               int8(1),
			expectedDataType:    arrow.PrimitiveTypes.Int8,
			expectedStringValue: "1",
		},
		{
			value:               uint8(1),
			expectedDataType:    arrow.PrimitiveTypes.Uint8,
			expectedStringValue: "1",
		},
		{
			value:               int16(1),
			expectedDataType:    arrow.PrimitiveTypes.Int16,
			expectedStringValue: "1",
		},
		{
			value:               uint16(1),
			expectedDataType:    arrow.PrimitiveTypes.Uint16,
			expectedStringValue: "1",
		},
		{
			value:               int32(1),
			expectedDataType:    arrow.PrimitiveTypes.Int32,
			expectedStringValue: "1",
		},
		{
			value:               uint32(1),
			expectedDataType:    arrow.PrimitiveTypes.Uint32,
			expectedStringValue: "1",
		},
		{
			value:               int64(1),
			expectedDataType:    arrow.PrimitiveTypes.Int64,
			expectedStringValue: "1",
		},
		{
			value:               uint64(1),
			expectedDataType:    arrow.PrimitiveTypes.Uint64,
			expectedStringValue: "1",
		},
		{
			value:               float32(1),
			expectedDataType:    arrow.PrimitiveTypes.Float32,
			expectedStringValue: "1",
		},
		{
			value:               float64(1),
			expectedDataType:    arrow.PrimitiveTypes.Float64,
			expectedStringValue: "1",
		},
		{
			value:               arrow.Date32FromTime(testTime),
			expectedDataType:    arrow.PrimitiveTypes.Date32,
			expectedStringValue: testTime.UTC().Truncate(24 * time.Hour).Format("2006-01-02"),
		},
		{
			value:               arrow.Date64FromTime(testTime),
			expectedDataType:    arrow.PrimitiveTypes.Date64,
			expectedStringValue: testTime.UTC().Truncate(24 * time.Hour).Format("2006-01-02"),
		},
		{
			value:               []byte("my-string"),
			expectedDataType:    arrow.BinaryTypes.Binary,
			expectedStringValue: base64.StdEncoding.EncodeToString([]byte("my-string")),
		},
		{
			value:               []byte("my-string"),
			inputDataType:       arrow.BinaryTypes.LargeBinary,
			expectedDataType:    arrow.BinaryTypes.LargeBinary,
			expectedStringValue: base64.StdEncoding.EncodeToString([]byte("my-string")),
		},
		{
			value:               "my-string",
			expectedDataType:    arrow.BinaryTypes.String,
			expectedStringValue: "my-string",
		},
		{
			value:               "my-string",
			inputDataType:       arrow.BinaryTypes.LargeString,
			expectedDataType:    arrow.BinaryTypes.LargeString,
			expectedStringValue: "my-string",
		},
		{
			value:               tstampSec,
			inputDataType:       &arrow.TimestampType{Unit: arrow.Second},
			expectedDataType:    &arrow.TimestampType{Unit: arrow.Second},
			expectedStringValue: testTime.UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z"),
		},
		{
			value:               tstampMilli,
			inputDataType:       &arrow.TimestampType{Unit: arrow.Millisecond},
			expectedDataType:    &arrow.TimestampType{Unit: arrow.Millisecond},
			expectedStringValue: testTime.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z"),
		},
		{
			value:               tstampMicro,
			inputDataType:       &arrow.TimestampType{Unit: arrow.Microsecond},
			expectedDataType:    &arrow.TimestampType{Unit: arrow.Microsecond},
			expectedStringValue: testTime.UTC().Truncate(time.Microsecond).Format("2006-01-02T15:04:05.000000Z"),
		},
		{
			value:               tstampNano,
			inputDataType:       &arrow.TimestampType{Unit: arrow.Nanosecond},
			expectedDataType:    &arrow.TimestampType{Unit: arrow.Nanosecond},
			expectedStringValue: testTime.UTC().Truncate(time.Nanosecond).Format("2006-01-02T15:04:05.000000000Z"),
		},
		{
			value:               testTime,
			inputDataType:       &arrow.TimestampType{Unit: arrow.Second},
			expectedDataType:    &arrow.TimestampType{Unit: arrow.Second},
			expectedStringValue: testTime.UTC().Truncate(time.Second).Format("2006-01-02T15:04:05Z"),
		},
		{
			value:               testTime,
			inputDataType:       &arrow.TimestampType{Unit: arrow.Millisecond},
			expectedDataType:    &arrow.TimestampType{Unit: arrow.Millisecond},
			expectedStringValue: testTime.UTC().Truncate(time.Millisecond).Format("2006-01-02T15:04:05.000Z"),
		},
		{
			value:               testTime,
			inputDataType:       &arrow.TimestampType{Unit: arrow.Microsecond},
			expectedDataType:    &arrow.TimestampType{Unit: arrow.Microsecond},
			expectedStringValue: testTime.UTC().Truncate(time.Microsecond).Format("2006-01-02T15:04:05.000000Z"),
		},
		{
			value:               testTime,
			inputDataType:       &arrow.TimestampType{Unit: arrow.Nanosecond},
			expectedDataType:    &arrow.TimestampType{Unit: arrow.Nanosecond},
			expectedStringValue: testTime.UTC().Truncate(time.Nanosecond).Format("2006-01-02T15:04:05.000000000Z"),
		},
	}
	for i, test := range tests {
		t.Run(fmt.Sprintf("%d-%T", i, test.value), func(t *testing.T) {
			arr, err := arrFromVal(test.value, test.inputDataType)
			require.NoError(t, err)

			assert.Equal(t, test.expectedDataType, arr.DataType())
			require.Equal(t, 1, arr.Len())
			assert.True(t, arr.IsValid(0))
			assert.Equal(t, test.expectedStringValue, arr.ValueStr(0))
		})
	}
}
