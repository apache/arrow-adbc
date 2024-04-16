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
	"cloud.google.com/go/bigquery"
	"context"
	"errors"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"golang.org/x/sync/errgroup"
	"google.golang.org/api/iterator"
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

func deserializeSchema(info []byte, mem memory.Allocator) (*arrow.Schema, error) {
	rdr, err := ipc.NewReader(bytes.NewReader(info), ipc.WithAllocator(mem))
	if err != nil {
		return nil, err
	}
	defer rdr.Release()
	return rdr.Schema(), nil
}

func checkContext(maybeErr error, ctx context.Context) error {
	if maybeErr != nil {
		return maybeErr
	} else if ctx.Err() == context.Canceled {
		return adbc.Error{Msg: "Cancelled by request", Code: adbc.StatusCancelled}
	} else if ctx.Err() == context.DeadlineExceeded {
		return adbc.Error{Msg: "Deadline exceeded", Code: adbc.StatusTimeout}
	}
	return ctx.Err()
}

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, cl *bigquery.Client, projectID, query string, queryConfig bigquery.QueryConfig, alloc memory.Allocator) (rdr array.RecordReader, err error) {
	q := cl.Query("")
	q.QueryConfig = queryConfig
	q.QueryConfig.Q = query
	job, err := q.Run(ctx)
	if err != nil {
		return nil, err
	}
	iter, err := job.Read(ctx)
	if err != nil {
		return nil, err
	}
	arrowIterator, err := iter.ArrowIterator()
	if err != nil {
		return nil, err
	}

	schema, err := deserializeSchema(arrowIterator.SerializedArrowSchema(), alloc)
	if err != nil {
		return nil, err
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

	chs := make([]chan arrow.Record, 1)
	chs[0] = ch
	reader := &reader{
		refCount:   1,
		chs:        chs,
		curChIndex: 0,
		err:        nil,
		cancelFn:   cancelFn,
		schema:     schema,
	}

	group.Go(func() error {
		for {
			batch, err := arrowIterator.Next()
			if err != nil {
				if errors.Is(err, iterator.Done) {
					return nil
				}
				return err
			}

			rdr, err := ipc.NewReader(bytes.NewReader(append(batch.Schema, batch.Data[:]...)), ipc.WithAllocator(alloc))
			if err != nil {
				return err
			}
			defer rdr.Release()
			for rdr.Next() && ctx.Err() == nil {
				rec := rdr.Record()
				rec.Retain()
				chs[0] <- rec
			}
			if err := checkContext(rdr.Err(), ctx); err != nil {
				return err
			}
		}
	})

	go func() {
		reader.err = group.Wait()
		// Don't close the channel until after the group is finished, so that
		// Next() can only return after reader.err may have been set
		close(chs[0])
	}()
	return reader, nil
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
