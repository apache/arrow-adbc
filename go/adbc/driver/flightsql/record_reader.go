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
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/bluele/gcache"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
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

// kicks off a goroutine for each endpoint and returns a reader which
// gathers all of the records as they come in.
func newRecordReader(ctx context.Context, alloc memory.Allocator, cl *flightsql.Client, info *flight.FlightInfo, clCache gcache.Cache, bufferSize int, opts ...grpc.CallOption) (rdr array.RecordReader, err error) {
	endpoints := info.Endpoint
	var schema *arrow.Schema
	if len(endpoints) == 0 {
		if info.Schema == nil {
			return nil, adbc.Error{
				Msg:  "Server returned FlightInfo with no schema and no endpoints, cannot read stream",
				Code: adbc.StatusInternal,
			}
		}
		schema, err = flight.DeserializeSchema(info.Schema, alloc)
		if err != nil {
			return nil, adbc.Error{
				Msg:  "Server returned FlightInfo with invalid schema and no endpoints, cannot read stream",
				Code: adbc.StatusInternal,
			}
		}
		return array.NewRecordReader(schema, []arrow.Record{})
	}

	ch := make(chan arrow.Record, bufferSize)
	group, ctx := errgroup.WithContext(ctx)
	ctx, cancelFn := context.WithCancel(ctx)
	// We may mutate endpoints below
	numEndpoints := len(endpoints)

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	if info.Schema != nil {
		schema, err = flight.DeserializeSchema(info.Schema, alloc)
		if err != nil {
			return nil, adbc.Error{
				Msg:  err.Error(),
				Code: adbc.StatusInvalidState}
		}
	} else {
		rdr, err := doGet(ctx, cl, endpoints[0], clCache, opts...)
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

	chs := make([]chan arrow.Record, numEndpoints)
	chs[0] = ch
	reader := &reader{
		refCount: 1,
		chs:      chs,
		err:      nil,
		cancelFn: cancelFn,
		schema:   schema,
	}

	lastChannelIndex := len(chs) - 1

	for i, ep := range endpoints {
		endpoint := ep
		endpointIndex := i
		chs[endpointIndex] = make(chan arrow.Record, bufferSize)
		group.Go(func() error {
			// Close channels (except the last) so that Next can move on to the next channel properly
			if endpointIndex != lastChannelIndex {
				defer close(chs[endpointIndex])
			}

			rdr, err := doGet(ctx, cl, endpoint, clCache, opts...)
			if err != nil {
				return err
			}
			defer rdr.Release()

			for rdr.Next() && ctx.Err() == nil {
				rec := rdr.Record()
				rec.Retain()
				chs[endpointIndex] <- rec
			}

			return rdr.Err()
		})
	}

	go func() {
		reader.err = group.Wait()
		// Don't close the last channel until after the group is finished, so that
		// Next() can only return after reader.err may have been set
		close(chs[lastChannelIndex])
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
