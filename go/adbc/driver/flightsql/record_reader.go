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

package flightsql

import (
	"context"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/flight"
	"github.com/apache/arrow/go/v11/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/bluele/gcache"
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

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, alloc memory.Allocator, cl *flightsql.Client, info *flight.FlightInfo, clCache gcache.Cache) (rdr array.RecordReader, err error) {
	ch := make(chan arrow.Record, 5)
	group, ctx := errgroup.WithContext(ctx)
	ctx, cancelFn := context.WithCancel(ctx)

	defer func() {
		if err != nil {
			cancelFn()
			for rec := range ch {
				rec.Release()
			}
		}
	}()

	endpoints := info.Endpoint

	var schema *arrow.Schema
	if info.Schema != nil {
		schema, err = flight.DeserializeSchema(info.Schema, alloc)
		if err != nil {
			close(ch)
			return nil, adbc.Error{
				Msg:  err.Error(),
				Code: adbc.StatusInvalidState}
		}
	} else {
		rdr, err := doGet(ctx, cl, endpoints[0], clCache)
		if err != nil {
			return nil, adbcFromFlightStatus(err)
		}
		schema = rdr.Schema()
		group.Go(func() error {
			defer rdr.Release()
			for rdr.Next() && ctx.Err() == nil {
				rec := rdr.Record()
				rec.Retain()
				ch <- rec
			}
			return rdr.Err()
		})

		endpoints = endpoints[1:]
	}

	reader := &reader{
		refCount: 1,
		ch:       ch,
		err:      nil,
		cancelFn: cancelFn,
		schema:   schema,
	}

	go func() {
		reader.err = group.Wait()
		close(ch)
	}()

	for _, ep := range endpoints {
		endpoint := ep
		group.Go(func() error {
			rdr, err := doGet(ctx, cl, endpoint, clCache)
			if err != nil {
				return err
			}
			defer rdr.Release()

			for rdr.Next() && ctx.Err() == nil {
				rec := rdr.Record()
				rec.Retain()
				ch <- rec
			}
			return rdr.Err()
		})
	}

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
