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
	"context"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/errgroup"
)

// func getLoaderSchema(ld gosnowflake.ArrowStreamLoader) *arrow.Schema {
// 	loc := ld.Location()
// 	types := ld.RowTypes()
// 	fields := make([]arrow.Field, len(types))
// 	for _, t := range types {

// 		switch strings.ToUpper(t.Type) {
// 		case "FIXED":
// 			if t.Scale == 0 {

// 			}
// 		}
// 	}
// }

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.Record
	curChIndex int
	rec        arrow.Record
	err        error

	cancelFn context.CancelFunc
}

func newRecordReader(ctx context.Context, alloc memory.Allocator, ld gosnowflake.ArrowStreamLoader, bufferSize int) (array.RecordReader, error) {
	batches, err := ld.GetBatches()
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusInternal, err)
	}

	ch := make(chan arrow.Record, bufferSize)
	r, err := batches[0].GetStream(ctx)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}

	rr, err := ipc.NewReader(r, ipc.WithAllocator(alloc))
	if err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInvalidState,
		}
	}

	schema := rr.Schema()
	group, ctx := errgroup.WithContext(ctx)
	ctx, cancelFn := context.WithCancel(ctx)

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	group.Go(func() error {
		defer rr.Release()
		defer r.Close()

		for rr.Next() && ctx.Err() == nil {
			rec := rr.Record()
			rec.Retain()
			ch <- rec
		}
		return rr.Err()
	})

	chs := make([]chan arrow.Record, len(batches))
	chs[0] = ch
	rdr := &reader{
		refCount: 1,
		chs:      chs,
		err:      nil,
		cancelFn: cancelFn,
		schema:   schema,
	}

	lastChannelIndex := len(chs) - 1
	for i, b := range batches[1:] {
		batch, batchIdx := b, i+1
		chs[batchIdx] = make(chan arrow.Record, bufferSize)
		group.Go(func() error {
			// close channels (except the last) so that Next can move on to the next channel properly
			if batchIdx != lastChannelIndex {
				defer close(chs[batchIdx])
			}

			rdr, err := batch.GetStream(ctx)
			if err != nil {
				return err
			}
			defer rdr.Close()

			rr, err := ipc.NewReader(rdr, ipc.WithAllocator(alloc))
			if err != nil {
				return err
			}
			defer rr.Release()

			for rr.Next() && ctx.Err() == nil {
				rec := rr.Record()
				rec.Retain()
				chs[batchIdx] <- rec
			}

			return rr.Err()
		})
	}

	go func() {
		rdr.err = group.Wait()
		// don't close the last channel until after the group is finished,
		// so that Next() can only return after reader.err may have been set
		close(chs[lastChannelIndex])
	}()

	return rdr, nil
}

func (r *reader) Schema() *arrow.Schema {
	return r.schema
}

func (r *reader) Record() arrow.Record {
	return r.rec
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
