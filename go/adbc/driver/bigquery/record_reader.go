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
	"cloud.google.com/go/bigquery"
	"context"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/ipc"
	"github.com/apache/arrow/go/v17/arrow/memory"
	"golang.org/x/sync/errgroup"
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

func runQuery(ctx context.Context, query *bigquery.Query, executeUpdate bool) (bigquery.ArrowIterator, int64, error) {
	job, err := query.Run(ctx)
	if err != nil {
		return nil, -1, err
	}
	if executeUpdate {
		return nil, 0, nil
	} else {
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

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, query *bigquery.Query, boundParameters array.RecordReader, parameterMode string, alloc memory.Allocator, resultRecordBufferSize, prefetchConcurrency int) (bigqueryRdr *reader, totalRows int64, err error) {
	if boundParameters == nil {
		return runPlainQuery(ctx, query, alloc, resultRecordBufferSize)
	} else {
		recs := make([]arrow.Record, 0)
		for boundParameters.Next() {
			rec := boundParameters.Record()
			recs = append(recs, rec)
		}
		batches := int64(len(recs))
		chs := make([]chan arrow.Record, batches)

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

		rec := recs[0]
		for i := 0; i < int(rec.NumRows()); i++ {
			parameters, err := getQueryParameter(rec, i, parameterMode)
			if err != nil {
				return nil, -1, err
			}
			if parameters != nil {
				query.QueryConfig.Parameters = parameters
			}

			arrowIterator, rows, err := runQuery(ctx, query, false)
			if err != nil {
				return nil, -1, err
			}
			totalRows = rows
			rdr, err := ipcReaderFromArrowIterator(arrowIterator, alloc)
			if err != nil {
				return nil, -1, err
			}
			bigqueryRdr.schema = rdr.Schema()

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

		lastChannelIndex := len(chs) - 1
		go func() {
			for index, values := range recs[1:] {
				batchIndex := index + 1
				record := values
				chs[batchIndex] = make(chan arrow.Record, resultRecordBufferSize)
				group.Go(func() error {
					if batchIndex != lastChannelIndex {
						defer close(chs[batchIndex])
					}
					for i := 0; i < int(record.NumRows()); i++ {
						parameters, err := getQueryParameter(record, i, parameterMode)
						if err != nil {
							return err
						}
						if parameters != nil {
							query.QueryConfig.Parameters = parameters
						}

						arrowIterator, rows, err := runQuery(ctx, query, false)
						if err != nil {
							return err
						}
						totalRows = rows
						rdr, err := ipcReaderFromArrowIterator(arrowIterator, alloc)
						if err != nil {
							return err
						}
						defer rdr.Release()

						for rdr.Next() && ctx.Err() == nil {
							rec := rdr.Record()
							rec.Retain()
							chs[batchIndex] <- rec
						}
						err = checkContext(ctx, rdr.Err())
						if err != nil {
							return err
						}
					}
					return nil
				})
			}

			// place this here so that we always clean up, but they can't be in a
			// separate goroutine. Otherwise we'll have a race condition between
			// the call to wait and the calls to group.Go to kick off the jobs
			// to perform the pre-fetching (GH-1283).
			bigqueryRdr.err = group.Wait()
			// don't close the last channel until after the group is finished,
			// so that Next() can only return after reader.err may have been set
			close(chs[lastChannelIndex])
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
