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

package sqldriver_test

import (
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/sqldriver"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/float16"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

var errNotImplemented = errors.New("demo; not implemented")

type typeDriver struct {
	payload arrow.RecordBatch
}

func (td *typeDriver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return &typeDatabase{td}, nil
}

type typeDatabase struct {
	driver *typeDriver
}

func (td *typeDatabase) SetOptions(map[string]string) error { return nil }
func (td *typeDatabase) Open(ctx context.Context) (adbc.Connection, error) {
	return &typeConnection{td.driver}, nil
}
func (td *typeDatabase) Close() error { return nil }

type typeConnection struct {
	driver *typeDriver
}

func (tc *typeConnection) NewStatement() (adbc.Statement, error) {
	return &typeStatement{tc.driver}, nil
}
func (tc *typeConnection) GetInfo(context.Context, []adbc.InfoCode) (array.RecordReader, error) {
	return nil, errNotImplemented
}
func (tc *typeConnection) GetObjects(context.Context, adbc.ObjectDepth, *string, *string, *string, *string, []string) (array.RecordReader, error) {
	return nil, errNotImplemented
}
func (tc *typeConnection) GetTableSchema(context.Context, *string, *string, string) (*arrow.Schema, error) {
	return nil, errNotImplemented
}
func (tc *typeConnection) GetTableTypes(context.Context) (array.RecordReader, error) {
	return nil, errNotImplemented
}
func (tc *typeConnection) Commit(context.Context) error   { return errNotImplemented }
func (tc *typeConnection) Rollback(context.Context) error { return errNotImplemented }
func (tc *typeConnection) Close() error                   { return nil }
func (tc *typeConnection) ReadPartition(context.Context, []byte) (array.RecordReader, error) {
	return nil, errNotImplemented
}

type typeStatement struct {
	driver *typeDriver
}

func (ts *typeStatement) ExecuteQuery(context.Context) (array.RecordReader, int64, error) {
	if ts.driver.payload == nil {
		return nil, -1, errors.New("no payload set")
	}
	rr, _ := array.NewRecordReader(ts.driver.payload.Schema(), []arrow.RecordBatch{ts.driver.payload})
	return rr, -1, nil
}
func (ts *typeStatement) Close() error                                         { return nil }
func (ts *typeStatement) SetOption(string, string) error                       { return nil }
func (ts *typeStatement) SetSqlQuery(string) error                             { return nil }
func (ts *typeStatement) ExecuteUpdate(context.Context) (int64, error)         { return -1, errNotImplemented }
func (ts *typeStatement) Prepare(context.Context) error                        { return nil }
func (ts *typeStatement) SetSubstraitPlan([]byte) error                        { return nil }
func (ts *typeStatement) Bind(context.Context, arrow.RecordBatch) error        { return nil }
func (ts *typeStatement) BindStream(context.Context, array.RecordReader) error { return nil }
func (ts *typeStatement) GetParameterSchema() (*arrow.Schema, error)           { return nil, errNotImplemented }
func (ts *typeStatement) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, errNotImplemented
}

type testCase struct {
	ty       arrow.DataType
	json     string
	expected []any
}

func ptr[T any](v T) *T { return &v }

func TestArrowTypes(t *testing.T) {
	date := time.Date(2026, time.June, 19, 0, 0, 0, 0, time.UTC)
	timestamp := time.Date(2026, time.June, 19, 1, 2, 3, 4_005_006, time.UTC)
	timeOfDay := time.Date(1970, time.January, 1, 1, 2, 3, 4_005_006, time.UTC)

	testCases := []testCase{
		{
			ty:       arrow.PrimitiveTypes.Int8,
			json:     `[{"a": 1}, {"a": null}]`,
			expected: []any{int8(1), nil},
		},
		{
			ty:       arrow.PrimitiveTypes.Int16,
			json:     `[{"a": 1}, {"a": null}]`,
			expected: []any{int16(1), nil},
		},
		{
			ty:       arrow.PrimitiveTypes.Int32,
			json:     `[{"a": 1}, {"a": null}]`,
			expected: []any{int32(1), nil},
		},
		{
			ty:       arrow.PrimitiveTypes.Int64,
			json:     `[{"a": 1}, {"a": null}]`,
			expected: []any{int64(1), nil},
		},
		{
			ty:       arrow.PrimitiveTypes.Uint8,
			json:     `[{"a": 1}, {"a": null}]`,
			expected: []any{uint8(1), nil},
		},
		{
			ty:       arrow.PrimitiveTypes.Uint16,
			json:     `[{"a": 1}, {"a": null}]`,
			expected: []any{uint16(1), nil},
		},
		{
			ty:       arrow.PrimitiveTypes.Uint32,
			json:     `[{"a": 1}, {"a": null}]`,
			expected: []any{uint32(1), nil},
		},
		{
			ty:       arrow.PrimitiveTypes.Uint64,
			json:     `[{"a": 1}, {"a": null}]`,
			expected: []any{uint64(1), nil},
		},
		{
			ty:       arrow.PrimitiveTypes.Float32,
			json:     `[{"a": 1.5}, {"a": null}]`,
			expected: []any{float32(1.5), nil},
		},
		{
			ty:       arrow.PrimitiveTypes.Float64,
			json:     `[{"a": 1.5}, {"a": null}]`,
			expected: []any{float64(1.5), nil},
		},
		{
			ty:       arrow.PrimitiveTypes.Date32,
			json:     `[{"a": "2026-06-19"}, {"a": null}]`,
			expected: []any{date, nil},
		},
		{
			ty:       arrow.PrimitiveTypes.Date64,
			json:     `[{"a": "2026-06-19"}, {"a": null}]`,
			expected: []any{date, nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Boolean,
			json:     `[{"a": true}, {"a": null}]`,
			expected: []any{true, nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Date32,
			json:     `[{"a": "2026-06-19"}, {"a": null}]`,
			expected: []any{date, nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Date64,
			json:     `[{"a": "2026-06-19"}, {"a": null}]`,
			expected: []any{date, nil},
		},
		{
			ty:       arrow.FixedWidthTypes.DayTimeInterval,
			json:     `[{"a": {"days": 1, "milliseconds": 2}}, {"a": null}]`,
			expected: []any{arrow.DayTimeInterval{Days: 1, Milliseconds: 2}, nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Duration_s,
			json:     `[{"a": "5s"}, {"a": null}]`,
			expected: []any{5 * time.Second, nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Duration_ms,
			json:     `[{"a": "5ms"}, {"a": null}]`,
			expected: []any{5 * time.Millisecond, nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Duration_us,
			json:     `[{"a": "5us"}, {"a": null}]`,
			expected: []any{5 * time.Microsecond, nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Duration_ns,
			json:     `[{"a": "5ns"}, {"a": null}]`,
			expected: []any{5 * time.Nanosecond, nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Float16,
			json:     `[{"a": 1.5}, {"a": null}]`,
			expected: []any{float16.New(1.5), nil},
		},
		{
			ty:       arrow.FixedWidthTypes.MonthInterval,
			json:     `[{"a": {"months": 3}}, {"a": null}]`,
			expected: []any{arrow.MonthInterval(3), nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Time32s,
			json:     `[{"a": "01:02:03"}, {"a": null}]`,
			expected: []any{timeOfDay.Truncate(time.Second), nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Time32ms,
			json:     `[{"a": "01:02:03.004"}, {"a": null}]`,
			expected: []any{timeOfDay.Truncate(time.Millisecond), nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Time64us,
			json:     `[{"a": "01:02:03.004005"}, {"a": null}]`,
			expected: []any{timeOfDay.Truncate(time.Microsecond), nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Time64ns,
			json:     `[{"a": "01:02:03.004005006"}, {"a": null}]`,
			expected: []any{timeOfDay, nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Timestamp_s,
			json:     `[{"a": "2026-06-19T01:02:03Z"}, {"a": null}]`,
			expected: []any{timestamp.Truncate(time.Second), nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Timestamp_ms,
			json:     `[{"a": "2026-06-19T01:02:03.004Z"}, {"a": null}]`,
			expected: []any{timestamp.Truncate(time.Millisecond), nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Timestamp_us,
			json:     `[{"a": "2026-06-19T01:02:03.004005Z"}, {"a": null}]`,
			expected: []any{timestamp.Truncate(time.Microsecond), nil},
		},
		{
			ty:       arrow.FixedWidthTypes.Timestamp_ns,
			json:     `[{"a": "2026-06-19T01:02:03.004005006Z"}, {"a": null}]`,
			expected: []any{timestamp, nil},
		},
		{
			ty:       arrow.FixedWidthTypes.MonthDayNanoInterval,
			json:     `[{"a": {"months": 1, "days": 2, "nanoseconds": 3}}, {"a": null}]`,
			expected: []any{arrow.MonthDayNanoInterval{Months: 1, Days: 2, Nanoseconds: 3}, nil},
		},
		{
			ty:       arrow.BinaryTypes.Binary,
			json:     `[{"a": "AQID"}, {"a": null}]`,
			expected: []any{[]byte{1, 2, 3}, nil},
		},
		{
			ty:       arrow.BinaryTypes.String,
			json:     `[{"a": "value"}, {"a": null}]`,
			expected: []any{"value", nil},
		},
		{
			ty:       arrow.BinaryTypes.LargeBinary,
			json:     `[{"a": "AQID"}, {"a": null}]`,
			expected: []any{[]byte{1, 2, 3}, nil},
		},
		{
			ty:       arrow.BinaryTypes.LargeString,
			json:     `[{"a": "value"}, {"a": null}]`,
			expected: []any{"value", nil},
		},
		{
			ty:       arrow.BinaryTypes.BinaryView,
			json:     `[{"a": "AQID"}, {"a": null}]`,
			expected: []any{[]byte{1, 2, 3}, nil},
		},
		{
			ty:       arrow.BinaryTypes.StringView,
			json:     `[{"a": "value"}, {"a": null}]`,
			expected: []any{"value", nil},
		},
		{
			// XXX: arreflect's fallback drops the null
			ty:       arrow.ListOf(arrow.PrimitiveTypes.Int32),
			json:     `[{"a": [1, null, 2]}, {"a": null}]`,
			expected: []any{[]int32{1, 0, 2}, nil},
		},
		{
			ty:   arrow.StructOf(arrow.Field{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}, arrow.Field{Name: "b", Type: arrow.BinaryTypes.String, Nullable: true}),
			json: `[{"a": {"a": 1, "b": "value"}}, {"a": null}]`,
			expected: []any{struct {
				A *int32  `arrow:"a"`
				B *string `arrow:"b"`
			}{A: ptr(int32(1)), B: ptr("value")}, nil},
		},
	}

	drv := &typeDriver{}
	const driverName = "adbc-arrow-types"
	sql.Register(driverName, sqldriver.Driver{drv})

	for _, tc := range testCases {
		t.Run(tc.ty.String(), func(t *testing.T) {
			mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
			defer mem.AssertSize(t, 0)
			schema := arrow.NewSchema([]arrow.Field{{Name: "a", Type: tc.ty, Nullable: true}}, nil)
			batch, _, err := array.RecordFromJSON(mem, schema, strings.NewReader(tc.json))
			require.NoError(t, err)
			defer batch.Release()

			drv.payload = batch

			db, err := sql.Open(driverName, "a=b")
			require.NoError(t, err)
			defer validation.CheckedClose(t, db)

			rows, err := db.Query("")
			require.NoError(t, err)
			defer validation.CheckedClose(t, rows)

			values := make([]any, 0, 2)
			for rows.Next() {
				var v any
				require.NoError(t, rows.Scan(&v))
				values = append(values, v)
			}
			require.NoError(t, rows.Err())
			require.Equal(t, tc.expected, values)
		})
	}
}
