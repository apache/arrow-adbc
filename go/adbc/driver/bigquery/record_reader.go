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
	"bytes"
	"context"
	"errors"
	"log"
	"sync/atomic"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/ipc"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
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
	} else if errors.Is(ctx.Err(), context.Canceled) {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusCancelled}
	} else if errors.Is(ctx.Err(), context.DeadlineExceeded) {
		return adbc.Error{Msg: ctx.Err().Error(), Code: adbc.StatusTimeout}
	}
	return ctx.Err()
}

func runQuery(ctx context.Context, query *bigquery.Query, executeUpdate bool) (bigquery.ArrowIterator, int64, error) {
	job, err := query.Run(ctx)
	if err != nil {
		return nil, -1, err
	}
	if executeUpdate {
		return nil, 0, nil
	}

	iter, err := job.Read(ctx)
	if err != nil {
		return nil, -1, err
	}

	var arrowIterator bigquery.ArrowIterator
	if iter.TotalRows > 0 {
		if arrowIterator, err = iter.ArrowIterator(); err != nil {
			return nil, -1, err
		}
	} else {
		arrowIterator = emptyArrowIterator{iter.Schema}
	}
	totalRows := int64(iter.TotalRows)
	return arrowIterator, totalRows, nil
}

func ipcReaderFromArrowIterator(arrowIterator bigquery.ArrowIterator, alloc memory.Allocator) (*ipc.Reader, error) {
	arrowItReader := bigquery.NewArrowIteratorReader(arrowIterator)
	return ipc.NewReader(arrowItReader, ipc.WithAllocator(alloc))
}

func getQueryParameter(values arrow.Record, row int, parameterMode string) ([]bigquery.QueryParameter, error) {
	parameters := make([]bigquery.QueryParameter, values.NumCols())
	includeName := parameterMode == OptionValueQueryParameterModeNamed
	schema := values.Schema()
	for i, v := range values.Columns() {
		pi, err := arrowValueToQueryParameterValue(schema.Field(i), v, row)
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
	arrowIterator, totalRows, err := runQuery(ctx, query, false)
	if err != nil {
		return nil, -1, err
	}
	rdr, err := ipcReaderFromArrowIterator(arrowIterator, alloc)
	if err != nil {
		return nil, -1, err
	}

	chs := make([]chan arrow.Record, 1)
	ctx, cancelFn := context.WithCancel(ctx)
	ch := make(chan arrow.Record, resultRecordBufferSize)
	chs[0] = ch

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	bigqueryRdr = &reader{
		refCount:   1,
		chs:        chs,
		curChIndex: 0,
		err:        nil,
		cancelFn:   cancelFn,
		schema:     rdr.Schema(),
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

func queryRecordWithSchemaCallback(ctx context.Context, group *errgroup.Group, query *bigquery.Query, rec arrow.Record, ch chan arrow.Record, parameterMode string, alloc memory.Allocator, rdrSchema func(schema *arrow.Schema)) (int64, error) {
	totalRows := int64(-1)
	for i := 0; i < int(rec.NumRows()); i++ {
		parameters, err := getQueryParameter(rec, i, parameterMode)
		if err != nil {
			return -1, err
		}
		if parameters != nil {
			query.Parameters = parameters
		}

		arrowIterator, rows, err := runQuery(ctx, query, false)
		if err != nil {
			return -1, err
		}
		totalRows = rows
		rdr, err := ipcReaderFromArrowIterator(arrowIterator, alloc)
		if err != nil {
			return -1, err
		}
		rdrSchema(rdr.Schema())
		group.Go(func() error {
			defer rdr.Release()
			for rdr.Next() && ctx.Err() == nil {
				rec := rdr.Record()
				rec.Retain()
				ch <- rec
			}
			return checkContext(ctx, rdr.Err())
		})
	}
	return totalRows, nil
}

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, query *bigquery.Query, boundParameters array.RecordReader, parameterMode string, alloc memory.Allocator, resultRecordBufferSize, prefetchConcurrency int) (bigqueryRdr *reader, totalRows int64, err error) {
	if boundParameters == nil {
		return runPlainQuery(ctx, query, alloc, resultRecordBufferSize)
	}
	defer boundParameters.Release()

	totalRows = 0
	// BigQuery can expose result sets as multiple streams when using certain APIs
	// for now lets keep this and set the number of channels to 1
	// when we need to adapt to multiple streams we can change the value here
	chs := make([]chan arrow.Record, 1)

	ch := make(chan arrow.Record, resultRecordBufferSize)
	group, ctx := errgroup.WithContext(ctx)
	group.SetLimit(prefetchConcurrency)
	ctx, cancelFn := context.WithCancel(ctx)
	chs[0] = ch

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	bigqueryRdr = &reader{
		refCount: 1,
		chs:      chs,
		err:      nil,
		cancelFn: cancelFn,
		schema:   nil,
	}

	for boundParameters.Next() {
		rec := boundParameters.Record()
		// Each call to Record() on the record reader is allowed to release the previous record
		// and since we're doing this sequentially
		// we don't need to call rec.Retain() here and call call rec.Release() in queryRecordWithSchemaCallback
		batchRows, err := queryRecordWithSchemaCallback(ctx, group, query, rec, ch, parameterMode, alloc, func(schema *arrow.Schema) {
			bigqueryRdr.schema = schema
		})
		if err != nil {
			return nil, -1, err
		}
		totalRows += batchRows
	}
	bigqueryRdr.err = group.Wait()
	defer close(ch)
	return bigqueryRdr, totalRows, nil
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

type emptyArrowIterator struct {
	schema bigquery.Schema
}

func (e emptyArrowIterator) Next() (*bigquery.ArrowRecordBatch, error) {
	return nil, iterator.Done
}

func (e emptyArrowIterator) Schema() bigquery.Schema {
	return e.schema
}

func (e emptyArrowIterator) SerializedArrowSchema() []byte {

	fields := make([]arrow.Field, len(e.schema))
	for i, field := range e.schema {
		f, err := buildField(field, 0)
		if err != nil {
			log.Fatalf("Error building field %s: %v", field.Name, err)
		}
		fields[i] = f
	}

	arrowSchema := arrow.NewSchema(fields, nil)

	var buf bytes.Buffer
	writer := ipc.NewWriter(&buf, ipc.WithSchema(arrowSchema))

	err := writer.Close()
	if err != nil {
		log.Fatalf("Error serializing an empty schema: %v", err)
	}

	return buf.Bytes()
}
