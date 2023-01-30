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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
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
	tz       = time.FixedZone("North Idaho", -int((8 * time.Hour).Seconds()))
	testTime = time.Date(2023, time.January, 26, 15, 40, 39, 123456789, tz)
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
	}

	for i, test := range tests {
		t.Run(fmt.Sprintf("%d-%s", i, test.arrowType.String()), func(t *testing.T) {
			schema := arrow.NewSchema([]arrow.Field{{Type: test.arrowType}}, nil)
			recordBuilder := array.NewRecordBuilder(memory.DefaultAllocator, schema)
			t.Cleanup(recordBuilder.Release)
			test.arrowValueFunc(t, recordBuilder.Field(0))
			record := recordBuilder.NewRecord()
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
