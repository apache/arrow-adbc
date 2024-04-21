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
	"sync/atomic"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"golang.org/x/sync/errgroup"
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

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, query *bigquery.Query, alloc memory.Allocator) (rdr array.RecordReader, totalRows int64, err error) {
	job, err := query.Run(ctx)
	if err != nil {
		return nil, -1, err
	}
	iter, err := job.Read(ctx)
	if err != nil {
		return nil, -1, err
	}
	totalRows = int64(iter.TotalRows)
	arrowIterator, err := iter.ArrowIterator()
	if err != nil {
		return nil, -1, err
	}

	ch := make(chan arrow.Record, 4096)
	group, ctx := errgroup.WithContext(ctx)
	ctx, cancelFn := context.WithCancel(ctx)

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	reader := &reader{
		refCount: 1,
		ch:       ch,
		err:      nil,
		cancelFn: cancelFn,
		schema:   nil,
	}

	group.Go(func() error {
		arrowItReader := bigquery.NewArrowIteratorReader(arrowIterator)
		rdr, err := ipc.NewReader(arrowItReader, ipc.WithAllocator(alloc))
		if err != nil {
			return err
		}
		reader.schema = rdr.Schema()

		defer rdr.Release()
		for rdr.Next() && ctx.Err() == nil {
			rec := rdr.Record()
			rec.Retain()
			ch <- rec
		}
		if err := checkContext(ctx, rdr.Err()); err != nil {
			return err
		}
		return nil
	})

	go func() {
		reader.err = group.Wait()
		close(ch)
	}()
	return reader, totalRows, nil
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
