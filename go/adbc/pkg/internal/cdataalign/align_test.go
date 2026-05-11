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

package cdataalign

import (
	"testing"
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal128"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
	"github.com/stretchr/testify/require"
)

func TestRecordBatchCopiesMisalignedDecimal128ValueBuffer(t *testing.T) {
	values := []decimal128.Num{
		decimal128.FromI64(123),
		decimal128.FromI64(-456),
	}
	valueBytes := misalignedBytes(t, len(values)*arrow.Decimal128SizeBytes)
	copy(valueBytes, arrow.Decimal128Traits.CastToBytes(values))
	require.NotZero(t, uintptr(unsafe.Pointer(&valueBytes[0]))%cDataBufferAlignment)

	dt := &arrow.Decimal128Type{Precision: 38, Scale: 0}
	data := array.NewData(dt, len(values), []*memory.Buffer{
		nil,
		memory.NewBufferBytes(valueBytes),
	}, nil, 0, 0)
	arr := array.MakeFromData(data).(*array.Decimal128)
	data.Release()
	defer arr.Release()

	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: dt}}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, int64(len(values)))
	defer rec.Release()

	aligned, release := RecordBatch(rec)
	require.True(t, release)
	defer aligned.Release()

	alignedArr := aligned.Column(0).(*array.Decimal128)
	alignedBytes := alignedArr.Data().Buffers()[1].Bytes()
	require.Zero(t, uintptr(unsafe.Pointer(&alignedBytes[0]))%cDataBufferAlignment)
	require.Equal(t, values, alignedArr.Values())
}

func TestRecordBatchLeavesAlignedBuffersUntouched(t *testing.T) {
	alloc := mallocator.NewMallocator()
	dt := &arrow.Decimal128Type{Precision: 38, Scale: 0}
	bldr := array.NewDecimal128Builder(alloc, dt)
	bldr.AppendValues([]decimal128.Num{
		decimal128.FromI64(123),
		decimal128.FromI64(456),
	}, nil)
	arr := bldr.NewDecimal128Array()
	bldr.Release()
	defer arr.Release()

	values := arr.Data().Buffers()[1].Bytes()
	require.Zero(t, uintptr(unsafe.Pointer(&values[0]))%cDataBufferAlignment)

	schema := arrow.NewSchema([]arrow.Field{{Name: "n", Type: dt}}, nil)
	rec := array.NewRecordBatch(schema, []arrow.Array{arr}, int64(arr.Len()))
	defer rec.Release()

	aligned, release := recordBatch(rec, alloc)
	require.False(t, release)
	require.Same(t, rec, aligned)
}

func misalignedBytes(t *testing.T, size int) []byte {
	t.Helper()
	raw := make([]byte, size+cDataBufferAlignment)
	for i := 0; i < cDataBufferAlignment; i++ {
		b := raw[i : i+size]
		if uintptr(unsafe.Pointer(&b[0]))%cDataBufferAlignment != 0 {
			return b
		}
	}
	t.Fatal("could not create misaligned byte slice")
	return nil
}
