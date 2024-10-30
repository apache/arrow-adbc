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
	"io"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

type readerIter interface {
	Release()

	Next() (array.RecordReader, error)
}

type concatReader struct {
	refCount      atomic.Int64
	readers       readerIter
	currentReader array.RecordReader
	schema        *arrow.Schema
	err           error
}

func (r *concatReader) nextReader() {
	if r.currentReader != nil {
		r.currentReader.Release()
		r.currentReader = nil
	}
	reader, err := r.readers.Next()
	if err == io.EOF {
		r.currentReader = nil
	} else if err != nil {
		r.err = err
	} else {
		// May be nil
		r.currentReader = reader
	}
}
func (r *concatReader) Init(readers readerIter) error {
	r.readers = readers
	r.refCount.Store(1)
	r.nextReader()
	if r.err != nil {
		r.Release()
		return r.err
	} else if r.currentReader == nil {
		r.Release()
		r.err = adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "[Snowflake] No data in this stream",
		}
		return r.err
	}
	r.schema = r.currentReader.Schema()
	return nil
}
func (r *concatReader) Retain() {
	r.refCount.Add(1)
}
func (r *concatReader) Release() {
	if r.refCount.Add(-1) == 0 {
		if r.currentReader != nil {
			r.currentReader.Release()
		}
		r.readers.Release()
	}
}
func (r *concatReader) Schema() *arrow.Schema {
	if r.schema == nil {
		panic("did not call concatReader.Init")
	}
	return r.schema
}
func (r *concatReader) Next() bool {
	for r.currentReader != nil && !r.currentReader.Next() {
		r.nextReader()
	}
	if r.currentReader == nil || r.err != nil {
		return false
	}
	return true
}
func (r *concatReader) Record() arrow.Record {
	return r.currentReader.Record()
}
func (r *concatReader) Err() error {
	return r.err
}
