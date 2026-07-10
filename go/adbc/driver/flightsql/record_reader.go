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
	"sync/atomic"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-adbc/go/adbc/utils"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/util"
	"github.com/bluele/gcache"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
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
	tracing     adbc.OTelTracing
}

// newRecordReader kicks off a goroutine for each endpoint and returns a
// reader which gathers all of the records as they come in. cfg.logger
// may be nil.
func newRecordReader(ctx context.Context, cfg recordReaderConfig, opts ...grpc.CallOption) (rdr array.RecordReader, err error) {
	const prefix = "record_reader.newRecordReader"
	var span trace.Span
	ctx, span = internal.StartSpan(ctx, prefix, cfg.tracing)
	defer func() {
		internal.EndSpan(span, err)
	}()

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

	span.AddEvent(prefix+".start", trace.WithAttributes(
		append(flightInfoLogAttrs(info),
			attribute.Int("bufferSize", cfg.bufferSize),
		)...))

	defer func() {
		if err != nil {
			close(ch)
			cancelFn()
		}
	}()

	if info.Schema != nil {
		schema, err = flight.DeserializeSchema(info.Schema, cfg.alloc)
		if err != nil {
			err = adbc.Error{
				Msg:  err.Error(),
				Code: adbc.StatusInvalidState}
			return nil, err
		}
	} else {
		firstEndpoint := endpoints[0]
		epAttrs := endpointLogAttrs(0, numEndpoints, firstEndpoint)
		span.AddEvent("endpoint_stream.open.schema_discovery", trace.WithAttributes(epAttrs...))
		startSchemaFetch := newStreamProgress()
		rdr, err = doGetWithTracing(ctx, cfg.cl, firstEndpoint, cfg.clientCache, cfg.tracing, opts...)
		if err != nil {
			span.RecordError(err, trace.WithAttributes(
				append(
					epAttrs,
					attribute.String("context", "FlightSQL endpoint DoGet failed (schema discovery)"),
					attribute.String("elapsed", startSchemaFetch.summary()),
				)...))
			err = adbcFromFlightStatusWithDetails(err, header, trailer,
				"DoGet: endpoint 0: remote: %s", firstEndpoint.Location)
			return nil, err
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
			if err = checkContext(rdr.Err(), ctx); err != nil {
				attrs := append(
					endpointLogAttrs(0, numEndpoints, firstEndpoint),
					progress.logAttrs()...,
				)
				attrs = append(attrs,
					attribute.String("context", "FlightSQL endpoint stream ended with error"),
				)
				err = adbcFromFlightStatusWithDetails(err, header, trailer,
					"DoGet: endpoint 0: remote: %s", firstEndpoint.Location)

				span.RecordError(err, trace.WithAttributes(attrs...))
				return err
			}
			span.AddEvent("FlightSQL endpoint stream completed", trace.WithAttributes(
				append(append([]attribute.KeyValue{},
					endpointLogAttrs(0, numEndpoints, firstEndpoint)...,
				), progress.logAttrs()...,
				)...,
			))
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
			span.AddEvent(prefix+".endpoint_stream_open", trace.WithAttributes(epAttrs...))
			doGetStart := newStreamProgress()
			rdr, err := doGetWithTracing(ctx, cfg.cl, endpoint, cfg.clientCache, cfg.tracing, opts...)
			if err != nil {
				span.RecordError(err, trace.WithAttributes(
					append(epAttrs,
						attribute.String("context", prefix+".failed"),
						attribute.String("elapsed", doGetStart.summary()),
					)...,
				))
				err = adbcFromFlightStatusWithDetails(err, header, trailer,
					"DoGet: endpoint %d: %s", logEndpointIndex, endpoint.Location)
				return err
			}
			defer rdr.Release()

			streamSchema := utils.RemoveSchemaMetadata(rdr.Schema())
			if !streamSchema.Equal(referenceSchema) {
				err = fmt.Errorf("endpoint %d returned inconsistent schema: expected %s but got %s", logEndpointIndex, referenceSchema.String(), streamSchema.String())
				span.RecordError(err, trace.WithAttributes(
					append(epAttrs,
						attribute.String("context", prefix+".inconsistent_schema"),
						attribute.String("expectedSchema", fmt.Sprint(referenceSchema)),
						attribute.String("actualSchema", fmt.Sprint(streamSchema)),
					)...,
				))
				return err
			}

			progress := newStreamProgress()
			for rdr.Next() && ctx.Err() == nil {
				rec := rdr.RecordBatch()
				progress.recordBatch(rec.NumRows(), util.TotalRecordSize(rec))
				rec.Retain()
				chs[endpointIndex] <- rec
			}

			if err = checkContext(rdr.Err(), ctx); err != nil {
				span.RecordError(err, trace.WithAttributes(
					append(epAttrs, append(progress.logAttrs(),
						attribute.String("context", prefix+".ended_with_error"),
					)...)...,
				))
				err = adbcFromFlightStatusWithDetails(err, header, trailer,
					"DoGet: endpoint %d: %s", logEndpointIndex, endpoint.Location)
				return err
			}
			return nil
		})
	}

	go func(span trace.Span) {
		err := group.Wait()
		reader.err = err
		if reader.err != nil {
			span.RecordError(reader.err, trace.WithAttributes(
				attribute.String("context", prefix+".finished_with_error"),
				attribute.Int("numEndpoints", numEndpoints),
			))
		} else {
			span.AddEvent(prefix+".success", trace.WithAttributes(
				attribute.Int("numEndpoints", numEndpoints),
			))
		}
		// Don't close the last channel until after the group is finished, so that
		// Next() can only return after reader.err may have been set
		close(chs[lastChannelIndex])
	}(span)

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
