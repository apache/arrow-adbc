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

package snowflake

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"testing"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestIngestBatchedParquetWithFileLimit(t *testing.T) {
	var buf bytes.Buffer
	ctx := context.Background()
	mem := memory.NewCheckedAllocator(memory.DefaultAllocator)
	defer mem.AssertSize(t, 0)

	ingestOpts := DefaultIngestOptions()
	parquetProps, arrowProps := newWriterProps(mem, ingestOpts)

	nCols := 3
	nRecs := 10
	nRows := 1000
	targetFileSize := 10000

	rec := makeRec(mem, nCols, nRows)
	defer rec.Release()

	// Create a temporary parquet writer and write a single row group so we know
	// approximately how many bytes it should take
	tempWriter, err := pqarrow.NewFileWriter(rec.Schema(), &buf, parquetProps, arrowProps)
	require.NoError(t, err)

	// Write 1 record and check the size before closing so footer bytes are not included
	require.NoError(t, tempWriter.Write(rec))
	expectedRowGroupSize := buf.Len()
	require.NoError(t, tempWriter.Close())

	recs := make([]arrow.Record, nRecs)
	for i := 0; i < nRecs; i++ {
		recs[i] = rec
	}

	rdr, err := array.NewRecordReader(rec.Schema(), recs)
	require.NoError(t, err)
	defer rdr.Release()

	records := make(chan arrow.Record)
	go func() { assert.NoError(t, readRecords(ctx, rdr, records)) }()

	buf.Reset()
	// Expected to read multiple records but then stop after targetFileSize, indicated by nil error
	require.NoError(t, writeParquet(rdr.Schema(), &buf, records, targetFileSize, parquetProps, arrowProps))

	// Expect to exceed the targetFileSize but by no more than the size of 1 row group
	assert.Greater(t, buf.Len(), targetFileSize)
	assert.Less(t, buf.Len(), targetFileSize+expectedRowGroupSize)

	// Drain the remaining records with no limit on file size, expect EOF
	require.ErrorIs(t, writeParquet(rdr.Schema(), &buf, records, -1, parquetProps, arrowProps), io.EOF)
}

func makeRec(mem memory.Allocator, nCols, nRows int) arrow.Record {
	vals := make([]int8, nRows)
	for val := 0; val < nRows; val++ {
		vals[val] = int8(val)
	}

	bldr := array.NewInt8Builder(mem)
	defer bldr.Release()

	bldr.AppendValues(vals, nil)
	arr := bldr.NewArray()
	defer arr.Release()

	fields := make([]arrow.Field, nCols)
	cols := make([]arrow.Array, nCols)
	for i := 0; i < nCols; i++ {
		fields[i] = arrow.Field{Name: fmt.Sprintf("field_%d", i), Type: arrow.PrimitiveTypes.Int8}
		cols[i] = arr // array.NewRecord will retain these
	}

	schema := arrow.NewSchema(fields, nil)
	return array.NewRecord(schema, cols, int64(nRows))
}
