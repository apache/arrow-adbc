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

package bigquery

import (
	"context"
	"github.com/apache/arrow/go/v17/arrow/array"
	"sync/atomic"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

type reader struct {
	refCount int64
	schema   *arrow.Schema
	ch       chan arrow.Record
	rec      arrow.Record
	err      error

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

func runQuery(ctx context.Context, query *bigquery.Query) (bigquery.ArrowIterator, int64, error) {
	job, err := query.Run(ctx)
	if err != nil {
		return nil, -1, err
	}
	iter, err := job.Read(ctx)
	if err != nil {
		return nil, -1, err
	}
	arrowIterator, err := iter.ArrowIterator()
	if err != nil {
		return nil, -1, err
	}
	totalRows := int64(iter.TotalRows)
	return arrowIterator, totalRows, nil
}

func ipcReaderFromArrowIterator(arrowIterator bigquery.ArrowIterator, alloc memory.Allocator) (*ipc.Reader, error) {
	arrowItReader := bigquery.NewArrowIteratorReader(arrowIterator)
	rdr, err := ipc.NewReader(arrowItReader, ipc.WithAllocator(alloc))
	if err != nil {
		return nil, err
	}
	return rdr, nil
}

func getQueryParameter(values arrow.Record, row int, parameterMode string) ([]bigquery.QueryParameter, error) {
	parameters := make([]bigquery.QueryParameter, values.NumCols())
	includeName := parameterMode == OptionValueQueryParameterModeNamed
	for i, v := range values.Columns() {
		pi, err := arrowValueToQueryParameterValue(v, row)
		if err != nil {
			return nil, err
		}
		parameters[i] = pi
		if includeName {
			parameters[i].Name = values.ColumnName(i)
		}
	}
	return parameters, nil
}

func runPlainQuery(ctx context.Context, query *bigquery.Query, alloc memory.Allocator, resultRecordBufferSize int) (bigqueryRdr *reader, totalRows int64, err error) {
	arrowIterator, totalRows, err := runQuery(ctx, query)
	if err != nil {
		return nil, -1, err
	}
	rdr, err := ipcReaderFromArrowIterator(arrowIterator, alloc)
	if err != nil {
		return nil, -1, err
	}

	ch := make(chan arrow.Record, resultRecordBufferSize)
	ctx, cancelFn := context.WithCancel(ctx)

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	bigqueryRdr = &reader{
		refCount: 1,
		ch:       ch,
		err:      nil,
		cancelFn: cancelFn,
		schema:   rdr.Schema(),
	}

	go func() {
		defer rdr.Release()
		for rdr.Next() && ctx.Err() == nil {
			rec := rdr.Record()
			rec.Retain()
			ch <- rec
		}

		err = checkContext(ctx, rdr.Err())
		defer close(ch)
	}()

	return bigqueryRdr, totalRows, nil
}

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, query *bigquery.Query, boundParameters array.RecordReader, parameterMode string, alloc memory.Allocator, resultRecordBufferSize int) (bigqueryRdr *reader, totalRows int64, err error) {
	if boundParameters == nil {
		return runPlainQuery(ctx, query, alloc, resultRecordBufferSize)
	} else {
		ch := make(chan arrow.Record, resultRecordBufferSize)
		ctx, cancelFn := context.WithCancel(ctx)

		defer func() {
			if err != nil {
				close(ch)
				cancelFn()
			}
		}()

		bigqueryRdr = &reader{
			refCount: 1,
			ch:       ch,
			err:      nil,
			cancelFn: cancelFn,
			schema:   nil,
		}

		go func() {
			for boundParameters.Next() {
				values := boundParameters.Record()
				for i := 0; i < int(values.NumRows()); i++ {
					parameters, err := getQueryParameter(values, i, parameterMode)
					if err != nil {
						return
					}
					if parameters != nil {
						query.QueryConfig.Parameters = parameters
					}

					arrowIterator, _, err := runQuery(ctx, query)
					if err != nil {
						return
					}
					rdr, err := ipcReaderFromArrowIterator(arrowIterator, alloc)
					if err != nil {
						return
					}

					// todo: possible race condition
					bigqueryRdr.schema = rdr.Schema()

					for rdr.Next() && ctx.Err() == nil {
						rec := rdr.Record()
						rec.Retain()
						ch <- rec
					}
					err = checkContext(ctx, rdr.Err())
					if err != nil {
						return
					}
				}
			}
			defer close(ch)
		}()

		return bigqueryRdr, totalRows, nil
	}
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
		for rec := range r.ch {
			rec.Release()
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

	r.rec = <-r.ch
	return r.rec != nil
}

func (r *reader) Schema() *arrow.Schema {
	return r.schema
}

func (r *reader) Record() arrow.Record {
	return r.rec
}
