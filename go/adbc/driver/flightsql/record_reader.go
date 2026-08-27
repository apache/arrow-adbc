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
	"errors"
	"fmt"
	"sync/atomic"
	"time"

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
)

type reader struct {
	refCount   int64
	schema     *arrow.Schema
	chs        []chan arrow.RecordBatch
	curChIndex int
	rec        arrow.RecordBatch
	err        error

	cancelFn context.CancelCauseFunc
}

var errReaderReleased = errors.New("record reader released")

type recordReaderCallerContextKey struct{}

// isRecordReaderSiblingCancellation reports whether the record reader's
// derived context was canceled while its original caller context remains
// active, indicating that another endpoint goroutine triggered cancellation.
func isRecordReaderSiblingCancellation(ctx context.Context) bool {
	callerCtx, ok := ctx.Value(recordReaderCallerContextKey{}).(context.Context)
	return ok && ctx.Err() == context.Canceled && callerCtx.Err() == nil
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
// reader which gathers all of the records as they come in.
func newRecordReader(ctx context.Context, cfg recordReaderConfig, opts ...grpc.CallOption) (rdr array.RecordReader, err error) {
	const spanName = "FlightSQL.RecordReader.newRecordReader"
	startTime := time.Now()
	ctx, span, endSpanHelper := internal.StartSpanWithEndSpanHelper(ctx, spanName, cfg.tracing)
	spanOwnedByReader := false
	errorRecorded := false
	defer func() {
		if !spanOwnedByReader {
			endSpanHelper.
				WithStartTime(startTime).
				WithError(err).
				WithRecordedError(errorRecorded).
				EndSpan()
		}
	}()

	info := cfg.info
	endpoints := info.Endpoint
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
	callerCtx := ctx
	group, ctx := errgroup.WithContext(ctx)
	ctx, cancelFn := context.WithCancelCause(ctx)
	ctx = context.WithValue(ctx, recordReaderCallerContextKey{}, callerCtx)
	goEndpoint := func(endpointFn func() error) {
		group.Go(func() error {
			err := endpointFn()
			if err != nil {
				cancelFn(err)
			}
			return err
		})
	}
	// We may mutate endpoints below
	numEndpoints := len(endpoints)

	span.AddEvent("endpoint_stream.starting", trace.WithAttributes(
		append([]attribute.KeyValue{
			attribute.Int("bufferSize", cfg.bufferSize),
		}, flightInfoTracingKeyValues(info)...)...,
	))

	defer func() {
		if err != nil {
			close(ch)
			cancelFn(err)
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
		epAttrs := endpointTraceKeyValues(0, numEndpoints, firstEndpoint)
		span.AddEvent("endpoint_stream.opening_schema_discovery", trace.WithAttributes(epAttrs...))
		startSchemaFetch := newStreamProgress()
		endpointCtx, responseMetadata := withResponseMetadata(ctx)
		var rdr array.RecordReader
		rdr, err = doGetWithTracer(endpointCtx, cfg.cl, firstEndpoint, cfg.clientCache, cfg.tracing, opts...)
		if err != nil {
			span.RecordError(err, trace.WithAttributes(
				append(append([]attribute.KeyValue{}, epAttrs...),
					attribute.String("elapsed", startSchemaFetch.summary()),
					attribute.String("flight.stage", "schema_discovery"),
				)...,
			))
			errorRecorded = true
			return nil, adbcFromFlightStatusWithDetails(err, responseMetadata.snapshot(), nil,
				"DoGet: endpoint 0: remote: %s", firstEndpoint.Location)
		}
		schema = rdr.Schema()
		goEndpoint(func() error {
			span := trace.SpanFromContext(ctx)
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
			if err := checkRecordReaderContext(rdr.Err(), ctx, callerCtx); err != nil {
				attrs := endpointTraceKeyValues(0, numEndpoints, firstEndpoint)
				attrs = append(attrs, progress.logKeyValues()...)
				span.RecordError(err,
					/*"FlightSQL endpoint stream ended with error",*/
					trace.WithAttributes(attrs...),
				)
				return adbcFromFlightStatusWithDetails(err, responseMetadata.snapshot(), nil,
					"DoGet: endpoint 0: remote: %s", firstEndpoint.Location)
			}
			span.AddEvent("endpoint_stream.completed", trace.WithAttributes(
				append(
					append(
						[]attribute.KeyValue{},
						endpointTraceKeyValues(0, numEndpoints, firstEndpoint)...),
					progress.logKeyValues()...,
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
		goEndpoint(func() error {
			// Close channels (except the last) so that Next can move on to the next channel properly
			if endpointIndex != lastChannelIndex {
				defer close(chs[endpointIndex])
			}

			epAttrs := endpointTraceKeyValues(logEndpointIndex, numEndpoints, endpoint)
			span.AddEvent("endpoint_stream.opening", trace.WithAttributes(epAttrs...))
			doGetStart := newStreamProgress()
			endpointCtx, responseMetadata := withResponseMetadata(ctx)
			rdr, err := doGetWithTracer(endpointCtx, cfg.cl, endpoint, cfg.clientCache, cfg.tracing, opts...)
			if err != nil {
				if checkRecordReaderContext(err, ctx, callerCtx) == nil {
					return nil
				}
				span.RecordError(err, trace.WithAttributes(
					append(
						append([]attribute.KeyValue{}, epAttrs...),
						attribute.String("err", err.Error()),
						attribute.String("elapsed", doGetStart.summary()),
						attribute.String("flight.stage", "do_get"),
					)...,
				))
				return adbcFromFlightStatusWithDetails(err, responseMetadata.snapshot(), nil,
					"DoGet: endpoint %d: %s", logEndpointIndex, endpoint.Location)
			}
			defer rdr.Release()

			streamSchema := utils.RemoveSchemaMetadata(rdr.Schema())
			if !streamSchema.Equal(referenceSchema) {
				err = fmt.Errorf("endpoint %d returned inconsistent schema: expected %s but got %s", logEndpointIndex, referenceSchema.String(), streamSchema.String())
				span.RecordError(err, trace.WithAttributes(
					append(
						append([]attribute.KeyValue{}, epAttrs...),
						attribute.String("expectedSchema", referenceSchema.String()),
						attribute.String("actualSchema", streamSchema.String()),
						attribute.String("stage", "FlightSQL endpoint returned inconsistent schema"),
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

			if err := checkRecordReaderContext(rdr.Err(), ctx, callerCtx); err != nil {
				span.RecordError(err, trace.WithAttributes(
					append(append([]attribute.KeyValue{}, epAttrs...),
						append([]attribute.KeyValue{
							attribute.String("err", err.Error()),
							attribute.String("stage", "FlightSQL endpoint stream ended with error"),
						}, progress.logKeyValues()...)...,
					)...,
				))
				return adbcFromFlightStatusWithDetails(err, responseMetadata.snapshot(), nil,
					"DoGet: endpoint %d: %s", logEndpointIndex, endpoint.Location)
			}
			span.AddEvent("endpoint_stream.completed", trace.WithAttributes(
				append(append([]attribute.KeyValue{}, epAttrs...),
					progress.logKeyValues()...,
				)...,
			))
			return nil
		})
	}

	spanOwnedByReader = true
	go func() {
		err := group.Wait()
		reader.err = err
		if reader.err != nil {
			span.AddEvent("record_reader.failed", trace.WithAttributes(
				attribute.Int("numEndpoints", numEndpoints),
			))
		} else {
			span.AddEvent("record_reader.completed", trace.WithAttributes(
				attribute.Int("numEndpoints", numEndpoints),
			))
		}
		errorRecorded := reader.err != nil
		endSpanHelper.
			WithStartTime(startTime).
			WithError(reader.err).
			WithRecordedError(errorRecorded).
			EndSpan()
		// Don't close the last channel until after the group is finished, so that
		// Next() can only return after reader.err and tracing have been finalized.
		close(chs[lastChannelIndex])
	}()

	return reader, nil
}

func checkRecordReaderContext(maybeErr error, ctx, callerCtx context.Context) error {
	if errors.Is(context.Cause(ctx), errReaderReleased) {
		return nil
	}
	if ctx.Err() == context.Canceled && callerCtx.Err() == nil {
		return nil
	}
	return checkContext(maybeErr, ctx)
}

func (r *reader) Retain() {
	atomic.AddInt64(&r.refCount, 1)
}

func (r *reader) Release() {
	if atomic.AddInt64(&r.refCount, -1) == 0 {
		if r.rec != nil {
			r.rec.Release()
		}
		r.cancelFn(errReaderReleased)
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
