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
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/utils"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/util"
	"github.com/bluele/gcache"
	"golang.org/x/sync/errgroup"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.RecordBatch
	curChIndex int
	rec        arrow.RecordBatch
	err        error

	cancelFn context.CancelFunc
}

// recordReaderConfig bundles the dependencies that newRecordReader
// needs to spin up its per-endpoint goroutines.
type recordReaderConfig struct {
	alloc       memory.Allocator
	cl          *flightsql.Client
	info        *flight.FlightInfo
	clientCache gcache.Cache
	bufferSize  int
	logger      *slog.Logger
}

// newRecordReader kicks off a goroutine for each endpoint and returns a
// reader which gathers all of the records as they come in. cfg.logger
// may be nil.
func newRecordReader(ctx context.Context, cfg recordReaderConfig, opts ...grpc.CallOption) (rdr array.RecordReader, err error) {
	log := safeLogger(cfg.logger)
	info := cfg.info
	endpoints := info.Endpoint
	var header, trailer metadata.MD
	opts = append(append([]grpc.CallOption{}, opts...), grpc.Header(&header), grpc.Trailer(&trailer))
	var schema *arrow.Schema
	if len(endpoints) == 0 {
		if info.Schema == nil {
			return nil, adbc.Error{
				Msg:  "Server returned FlightInfo with no schema and no endpoints, cannot read stream",
				Code: adbc.StatusInternal,
			}
		}
		schema, err = flight.DeserializeSchema(info.Schema, cfg.alloc)
		if err != nil {
			return nil, adbc.Error{
				Msg:  "Server returned FlightInfo with invalid schema and no endpoints, cannot read stream",
				Code: adbc.StatusInternal,
			}
		}
		return array.NewRecordReader(schema, []arrow.RecordBatch{})
	}

	ch := make(chan arrow.RecordBatch, cfg.bufferSize)
	group, ctx := errgroup.WithContext(ctx)
	ctx, cancelFn := context.WithCancel(ctx)
	// We may mutate endpoints below
	numEndpoints := len(endpoints)

	log.DebugContext(ctx, "FlightSQL newRecordReader start",
		append([]any{
			slog.Int("bufferSize", cfg.bufferSize),
		}, flightInfoLogAttrs(info)...)...,
	)

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	if info.Schema != nil {
		schema, err = flight.DeserializeSchema(info.Schema, cfg.alloc)
		if err != nil {
			return nil, adbc.Error{
				Msg:  err.Error(),
				Code: adbc.StatusInvalidState}
		}
	} else {
		firstEndpoint := endpoints[0]
		epAttrs := endpointLogAttrs(0, numEndpoints, firstEndpoint)
		log.DebugContext(ctx, "FlightSQL endpoint stream opening (schema discovery)", epAttrs...)
		startSchemaFetch := newStreamProgress()
		rdr, err := doGetWithLogger(ctx, cfg.cl, firstEndpoint, cfg.clientCache, log, opts...)
		if err != nil {
			log.ErrorContext(ctx, "FlightSQL endpoint DoGet failed (schema discovery)",
				append(append([]any{}, epAttrs...),
					"err", err,
					"elapsed", startSchemaFetch.summary(),
				)...,
			)
			return nil, adbcFromFlightStatusWithDetails(err, header, trailer,
				"DoGet: endpoint 0: remote: %s", firstEndpoint.Location)
		}
		schema = rdr.Schema()
		group.Go(func() error {
			defer rdr.Release()
			if numEndpoints > 1 {
				defer close(ch)
			}

			progress := newStreamProgress()
			for rdr.Next() && ctx.Err() == nil {
				rec := rdr.RecordBatch()
				progress.recordBatch(rec.NumRows(), util.TotalRecordSize(rec))
				rec.Retain()
				ch <- rec
			}
			if err := checkContext(rdr.Err(), ctx); err != nil {
				log.ErrorContext(ctx, "FlightSQL endpoint stream ended with error",
					append(append([]any{}, endpointLogAttrs(0, numEndpoints, firstEndpoint)...),
						append([]any{"err", err}, progress.logAttrs()...)...,
					)...,
				)
				return adbcFromFlightStatusWithDetails(err, header, trailer,
					"DoGet: endpoint 0: remote: %s", firstEndpoint.Location)
			}
			log.DebugContext(ctx, "FlightSQL endpoint stream completed",
				append(append([]any{}, endpointLogAttrs(0, numEndpoints, firstEndpoint)...),
					progress.logAttrs()...,
				)...,
			)
			return nil
		})

		endpoints = endpoints[1:]
	}

	chs := make([]chan arrow.RecordBatch, numEndpoints)
	chs[0] = ch
	reader := &reader{
		refCount: 1,
		chs:      chs,
		err:      nil,
		cancelFn: cancelFn,
		schema:   schema,
	}

	lastChannelIndex := len(chs) - 1

	referenceSchema := utils.RemoveSchemaMetadata(schema)
	for i, ep := range endpoints {
		endpoint := ep
		endpointIndex := i
		// Offset the endpoint index for the log records to account for endpoint 0
		// having been processed above when info.Schema was unset.
		logEndpointIndex := endpointIndex
		if info.Schema == nil {
			logEndpointIndex = endpointIndex + 1
		}
		chs[endpointIndex] = make(chan arrow.RecordBatch, cfg.bufferSize)
		group.Go(func() error {
			// Close channels (except the last) so that Next can move on to the next channel properly
			if endpointIndex != lastChannelIndex {
				defer close(chs[endpointIndex])
			}

			epAttrs := endpointLogAttrs(logEndpointIndex, numEndpoints, endpoint)
			log.DebugContext(ctx, "FlightSQL endpoint stream opening", epAttrs...)
			doGetStart := newStreamProgress()
			rdr, err := doGetWithLogger(ctx, cfg.cl, endpoint, cfg.clientCache, log, opts...)
			if err != nil {
				log.ErrorContext(ctx, "FlightSQL endpoint DoGet failed",
					append(append([]any{}, epAttrs...),
						"err", err,
						"elapsed", doGetStart.summary(),
					)...,
				)
				return adbcFromFlightStatusWithDetails(err, header, trailer,
					"DoGet: endpoint %d: %s", logEndpointIndex, endpoint.Location)
			}
			defer rdr.Release()

			streamSchema := utils.RemoveSchemaMetadata(rdr.Schema())
			if !streamSchema.Equal(referenceSchema) {
				log.ErrorContext(ctx, "FlightSQL endpoint returned inconsistent schema",
					append(append([]any{}, epAttrs...),
						"expectedSchema", referenceSchema.String(),
						"actualSchema", streamSchema.String(),
					)...,
				)
				return fmt.Errorf("endpoint %d returned inconsistent schema: expected %s but got %s", logEndpointIndex, referenceSchema.String(), streamSchema.String())
			}

			progress := newStreamProgress()
			for rdr.Next() && ctx.Err() == nil {
				rec := rdr.RecordBatch()
				progress.recordBatch(rec.NumRows(), util.TotalRecordSize(rec))
				rec.Retain()
				chs[endpointIndex] <- rec
			}

			if err := checkContext(rdr.Err(), ctx); err != nil {
				log.ErrorContext(ctx, "FlightSQL endpoint stream ended with error",
					append(append([]any{}, epAttrs...),
						append([]any{"err", err}, progress.logAttrs()...)...,
					)...,
				)
				return adbcFromFlightStatusWithDetails(err, header, trailer,
					"DoGet: endpoint %d: %s", logEndpointIndex, endpoint.Location)
			}
			log.DebugContext(ctx, "FlightSQL endpoint stream completed",
				append(append([]any{}, epAttrs...),
					progress.logAttrs()...,
				)...,
			)
			return nil
		})
	}

	go func() {
		err := group.Wait()
		reader.err = err
		if reader.err != nil {
			log.WarnContext(ctx, "FlightSQL record reader finished with error",
				"err", reader.err,
				"numEndpoints", numEndpoints,
			)
		} else {
			log.DebugContext(ctx, "FlightSQL record reader finished successfully",
				"numEndpoints", numEndpoints,
			)
		}
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

func (r *reader) Record() arrow.RecordBatch {
	return r.rec
}

func (r *reader) RecordBatch() arrow.RecordBatch {
	return r.rec
}

var _ array.RecordReader = (*reader)(nil)
