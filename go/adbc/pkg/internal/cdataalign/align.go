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
	"unsafe"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/memory/mallocator"
)

const cDataBufferAlignment = 64

var exportAllocator memory.Allocator = mallocator.NewMallocatorWithAlignment(cDataBufferAlignment)

// RecordBatch returns rec or a copy whose buffers are aligned for Arrow C Data export.
func RecordBatch(rec arrow.RecordBatch) (arrow.RecordBatch, bool) {
	return recordBatch(rec, exportAllocator)
}

func recordBatch(rec arrow.RecordBatch, alloc memory.Allocator) (arrow.RecordBatch, bool) {
	cols := rec.Columns()
	exportCols := make([]arrow.Array, len(cols))
	releaseCols := make([]bool, len(cols))
	changed := false

	for i, col := range cols {
		exportCols[i], releaseCols[i] = arrayForCData(col, alloc)
		changed = changed || releaseCols[i]
	}
	if !changed {
		return rec, false
	}

	exportRec := array.NewRecordBatch(rec.Schema(), exportCols, rec.NumRows())
	for i, release := range releaseCols {
		if release {
			exportCols[i].Release()
		}
	}
	return exportRec, true
}

func arrayForCData(arr arrow.Array, alloc memory.Allocator) (arrow.Array, bool) {
	data, release := arrayDataForCData(arr.Data(), alloc)
	if !release {
		return arr, false
	}
	defer data.Release()
	return array.MakeFromData(data), true
}

func arrayDataForCData(data arrow.ArrayData, alloc memory.Allocator) (arrow.ArrayData, bool) {
	if !hasArrayData(data) {
		return data, false
	}

	buffers := data.Buffers()
	exportBuffers := make([]*memory.Buffer, len(buffers))
	releaseBuffers := make([]bool, len(buffers))
	changed := false

	for i, buffer := range buffers {
		exportBuffers[i], releaseBuffers[i] = bufferForCData(buffer, alloc)
		changed = changed || releaseBuffers[i]
	}

	children := data.Children()
	exportChildren := make([]arrow.ArrayData, len(children))
	releaseChildren := make([]bool, len(children))
	for i, child := range children {
		exportChildren[i], releaseChildren[i] = arrayDataForCData(child, alloc)
		changed = changed || releaseChildren[i]
	}

	var (
		exportDict  arrow.ArrayData
		releaseDict bool
	)
	if dict := data.Dictionary(); hasArrayData(dict) {
		exportDict, releaseDict = arrayDataForCData(dict, alloc)
		changed = changed || releaseDict
	}

	if !changed {
		return data, false
	}

	exportData := array.NewData(data.DataType(), data.Len(), exportBuffers, exportChildren, data.NullN(), data.Offset())
	if exportDict != nil {
		exportData.SetDictionary(exportDict)
	}

	for i, release := range releaseBuffers {
		if release {
			exportBuffers[i].Release()
		}
	}
	for i, release := range releaseChildren {
		if release {
			exportChildren[i].Release()
		}
	}
	if releaseDict {
		exportDict.Release()
	}

	return exportData, true
}

func hasArrayData(data arrow.ArrayData) bool {
	if data == nil {
		return false
	}
	if typedData, ok := data.(*array.Data); ok {
		return typedData != nil
	}
	return true
}

func bufferForCData(buffer *memory.Buffer, alloc memory.Allocator) (*memory.Buffer, bool) {
	if buffer == nil || buffer.Len() == 0 {
		return buffer, false
	}
	bytes := buffer.Bytes()
	if uintptr(unsafe.Pointer(&bytes[0]))%cDataBufferAlignment == 0 {
		return buffer, false
	}

	exportBuffer := memory.NewResizableBuffer(alloc)
	exportBuffer.Resize(buffer.Len())
	copy(exportBuffer.Bytes(), bytes)
	return exportBuffer, true
}
