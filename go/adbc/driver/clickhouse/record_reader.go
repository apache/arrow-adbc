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

package clickhouse

import (
	"context"
	"fmt"
	clickhouseDriver "github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"golang.org/x/sync/errgroup"
	"io"
	"reflect"
	"strings"
	"sync/atomic"
)

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.Record
	curChIndex int
	rec        arrow.Record
	err        error

	cancelFn context.CancelFunc
}

func checkContext(ctx context.Context, maybeErr error) error {
	if maybeErr != nil {
		return maybeErr
	} else if ctx.Err() == context.Canceled {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusCancelled}
	} else if ctx.Err() == context.DeadlineExceeded {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusTimeout}
	}
	return ctx.Err()
}

func appendNullableBool(fieldBuilder array.Builder, v **bool) {
	currentValues := *v
	b := fieldBuilder.(*array.BooleanBuilder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendNullableInt8(fieldBuilder array.Builder, v **int8) {
	currentValues := *v
	b := fieldBuilder.(*array.Int8Builder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendNullableUint8(fieldBuilder array.Builder, v **uint8) {
	currentValues := *v
	b := fieldBuilder.(*array.Uint8Builder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendNullableInt16(fieldBuilder array.Builder, v **int16) {
	currentValues := *v
	b := fieldBuilder.(*array.Int16Builder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendNullableUint16(fieldBuilder array.Builder, v **uint16) {
	currentValues := *v
	b := fieldBuilder.(*array.Uint16Builder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendNullableInt32(fieldBuilder array.Builder, v **int32) {
	currentValues := *v
	b := fieldBuilder.(*array.Int32Builder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendNullableUint32(fieldBuilder array.Builder, v **uint32) {
	currentValues := *v
	b := fieldBuilder.(*array.Uint32Builder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendNullableInt64(fieldBuilder array.Builder, v **int64) {
	currentValues := *v
	b := fieldBuilder.(*array.Int64Builder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendNullableUint64(fieldBuilder array.Builder, v **uint64) {
	currentValues := *v
	b := fieldBuilder.(*array.Uint64Builder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendNullableFloat32(fieldBuilder array.Builder, v **float32) {
	currentValues := *v
	b := fieldBuilder.(*array.Float32Builder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendFloat32(fieldBuilder array.Builder, v *float32) {
	b := fieldBuilder.(*array.Float32Builder)
	b.Append(*v)
}

func appendNullableFloat64(fieldBuilder array.Builder, v **float64) {
	currentValues := *v
	b := fieldBuilder.(*array.Float64Builder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendFloat64(fieldBuilder array.Builder, v *float64) {
	b := fieldBuilder.(*array.Float64Builder)
	b.Append(*v)
}

func appendNullableString(fieldBuilder array.Builder, v **string) {
	currentValues := *v
	b := fieldBuilder.(*array.StringBuilder)
	if currentValues == nil {
		b.AppendNull()
	} else {
		b.Append(*currentValues)
	}
}

func appendString(fieldBuilder array.Builder, v *string) {
	b := fieldBuilder.(*array.StringBuilder)
	b.Append(*v)
}

func sendBatch(schema *arrow.Schema, fieldBuilders []array.Builder, numRows int, ch chan arrow.Record) {
	fieldValues := make([]arrow.Array, len(fieldBuilders))
	for i := range fieldBuilders {
		fieldValues[i] = fieldBuilders[i].NewArray()
	}
	rec := array.NewRecord(schema, fieldValues, int64(numRows))
	ch <- rec
}

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, parameters array.RecordReader, conn clickhouseDriver.Conn, query string, alloc memory.Allocator, resultRecordBufferSize, prefetchConcurrency int) (rdr *reader, totalRows int64, err error) {
	rows, err := conn.Query(ctx, query)
	if err != nil {
		return nil, 0, err
	}

	// todo: handle bulk queries
	numChannels := 1
	chs := make([]chan arrow.Record, numChannels)
	ch := make(chan arrow.Record, resultRecordBufferSize)
	chs[0] = ch
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(prefetchConcurrency)
	ctx, cancelFn := context.WithCancel(ctx)
	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	rdr = &reader{
		refCount: 1,
		chs:      chs,
		err:      nil,
		cancelFn: cancelFn,
		schema:   nil,
	}

	fields := make([]arrow.Field, 0)
	columnTypes := rows.ColumnTypes()
	for _, c := range columnTypes {
		typeLen := len(c.DatabaseTypeName())
		field, err := buildSchemaField(c.Name(), c.DatabaseTypeName(), 0, typeLen)
		if err != nil {
			return nil, 0, err
		}
		fields = append(fields, field)
	}
	schema := arrow.NewSchema(fields, nil)
	rdr.schema = schema

	group.Go(func() error {
		var vars = make([]any, len(columnTypes))
		schemaFields := schema.Fields()
		fieldBuilders := make([]array.Builder, len(columnTypes))
		for i := range columnTypes {
			vars[i] = reflect.New(columnTypes[i].ScanType()).Interface()
			schemaField := schemaFields[i]
			fieldBuilders[i] = array.NewBuilder(alloc, schemaField.Type)
		}

		rowsProcessed := 0
		for rows.Next() && ctx.Err() == nil {
			if err := rows.Scan(vars...); err != nil {
				return err
			}
			for i, v := range vars {
				fieldBuilder := fieldBuilders[i]
				if v == nil {
					return adbc.Error{
						Code: adbc.StatusInternal,
					}
				}

				switch v := v.(type) {
				case **bool:
					appendNullableBool(fieldBuilder, v)
				case **int8:
					appendNullableInt8(fieldBuilder, v)
				case **uint8:
					appendNullableUint8(fieldBuilder, v)
				case **int16:
					appendNullableInt16(fieldBuilder, v)
				case **uint16:
					appendNullableUint16(fieldBuilder, v)
				case **int32:
					appendNullableInt32(fieldBuilder, v)
				case **uint32:
					appendNullableUint32(fieldBuilder, v)
				case **int64:
					appendNullableInt64(fieldBuilder, v)
				case **uint64:
					appendNullableUint64(fieldBuilder, v)
				case **float32:
					appendNullableFloat32(fieldBuilder, v)
				case *float32:
					appendFloat32(fieldBuilder, v)
				case **float64:
					appendNullableFloat64(fieldBuilder, v)
				case *float64:
					appendFloat64(fieldBuilder, v)
				case **string:
					appendNullableString(fieldBuilder, v)
				case *string:
					appendString(fieldBuilder, v)
				default:
					return adbc.Error{
						Code: adbc.StatusNotImplemented,
						Msg:  fmt.Sprintf("type `%T` is not supported yet", v),
					}
				}
			}
			rowsProcessed++
			if rowsProcessed == resultRecordBufferSize {
				sendBatch(schema, fieldBuilders, rowsProcessed, ch)
				rowsProcessed = 0
			}
		}
		if rowsProcessed > 0 {
			sendBatch(schema, fieldBuilders, rowsProcessed, ch)
		}
		if err = rows.Close(); err != nil {
			return err
		}
		if err = rows.Err(); err != nil {
			return err
		}
		return checkContext(ctx, rdr.Err())
	})

	lastChannelIndex := len(chs) - 1
	go func() {
		// place this here so that we always clean up, but they can't be in a
		// separate goroutine. Otherwise we'll have a race condition between
		// the call to wait and the calls to group.Go to kick off the jobs
		// to perform the pre-fetching (GH-1283).
		rdr.err = group.Wait()
		// don't close the last channel until after the group is finished,
		// so that Next() can only return after reader.err may have been set
		close(chs[lastChannelIndex])
	}()

	return rdr, totalRows, nil
}

func (r *reader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *reader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
		}
		r.cancelFn()
		for _, ch := range r.chs {
			for rec := range ch {
				rec.Release()
			}
		}
	}
}

func (r *reader) Err() error {
	return r.err
}

func (r *reader) Next() bool {
	if r.rec != nil {
		r.rec.Release()
		r.rec = nil
	}

	if r.curChIndex >= len(r.chs) {
		return false
	}
	var ok bool
	for r.curChIndex < len(r.chs) {
		if r.rec, ok = <-r.chs[r.curChIndex]; ok {
			break
		}
		r.curChIndex++
	}
	return r.rec != nil
}

func (r *reader) Schema() *arrow.Schema {
	return r.schema
}

func (r *reader) Record() arrow.Record {
	return r.rec
}

func parseType(typeString string, typeIndex, typeLen int) (string, int, error) {
	typeEnd := -2
	for i := typeIndex; i < typeLen; i++ {
		c := typeString[i]
		if (c >= '0' && c <= '9') || (c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || c == '_' {
			typeEnd = i
		} else {
			break
		}
	}
	typeEnd += 1
	if typeEnd == -1 {
		return "", 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("unknown type `%s`", typeString),
		}
	}

	return typeString[typeIndex:typeEnd], typeEnd, nil
}

func expectRightBracket(typeString string, typeIndex, typeLen int) (int, error) {
	if typeIndex == typeLen {
		return 0, io.EOF
	}

	for i := typeIndex; i < typeLen; i++ {
		if typeString[i] == ')' {
			return i + 1, nil
		} else if typeString[i] == ' ' || typeString[i] == '\t' || typeString[i] == '\n' {
			continue
		} else {
			return 0, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("expecting a `)` but got: `%c` at %d", typeString[i], i),
			}
		}
	}
	return typeLen, io.EOF
}

var (
	simpleDataType = map[string]arrow.DataType{
		"Int8":    arrow.PrimitiveTypes.Int8,
		"UInt8":   arrow.PrimitiveTypes.Uint8,
		"Int16":   arrow.PrimitiveTypes.Int16,
		"UInt16":  arrow.PrimitiveTypes.Uint16,
		"Int32":   arrow.PrimitiveTypes.Int32,
		"UInt32":  arrow.PrimitiveTypes.Uint32,
		"Int64":   arrow.PrimitiveTypes.Int64,
		"UInt64":  arrow.PrimitiveTypes.Uint64,
		"Float32": arrow.PrimitiveTypes.Float32,
		"Float64": arrow.PrimitiveTypes.Float64,
		"String":  arrow.BinaryTypes.String,
	}
)

func buildField(name, typeName string) (arrow.Field, error) {
	dataType, ok := simpleDataType[typeName]
	if !ok {
		return arrow.Field{}, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("unsupported type: `%s`", typeName),
		}
	}

	field := arrow.Field{
		Name:     name,
		Type:     dataType,
		Nullable: false,
	}
	return field, nil
}

func ensureConsumedTypeString(typeString string, typeIndex, typeLen int) error {
	if typeIndex == typeLen {
		return nil
	}
	for i := typeIndex; i < typeLen; i++ {
		if typeString[i] == ' ' || typeString[i] == '\t' || typeString[i] == '\n' {
			continue
		} else {
			return adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("cannot fully consume the type string `%s`, parsed up to index %d", typeString, i),
			}
		}
	}
	return nil
}

func buildSchemaField(name string, typeString string, typeIndex, typeLen int) (arrow.Field, error) {
	if strings.HasPrefix(typeString, "Nullable(") {
		typeName, typeIndex, err := parseType(typeString, typeIndex+9, typeLen)
		if err != nil {
			return arrow.Field{}, err
		}
		typeIndex, err = expectRightBracket(typeString, typeIndex, typeLen)
		if err != nil {
			return arrow.Field{}, err
		}
		if err = ensureConsumedTypeString(typeString, typeIndex, typeLen); err != nil {
			return arrow.Field{}, err
		}

		field, err := buildField(name, typeName)
		field.Nullable = true
		return field, err
	} else {
		typeName, typeIndex, err := parseType(typeString, typeIndex, typeLen)
		if err != nil {
			return arrow.Field{}, err
		}
		if err = ensureConsumedTypeString(typeString, typeIndex, typeLen); err != nil {
			return arrow.Field{}, err
		}
		return buildField(name, typeName)
	}
}
