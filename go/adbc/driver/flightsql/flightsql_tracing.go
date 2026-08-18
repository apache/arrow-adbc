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
	"encoding/hex"
	"fmt"
	"sync"
	"time"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"go.opentelemetry.io/otel/attribute"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

type responseMetadataKey struct{}

type responseMetadataCollector struct {
	mutex sync.RWMutex
	value metadata.MD
}

func withResponseMetadata(ctx context.Context) (context.Context, *responseMetadataCollector) {
	collector := &responseMetadataCollector{}
	return context.WithValue(ctx, responseMetadataKey{}, collector), collector
}

func captureResponseMetadata(ctx context.Context, value metadata.MD) {
	collector, ok := responseMetadataFromContext(ctx)
	if !ok {
		return
	}
	collector.mutex.Lock()
	collector.value = value.Copy()
	defer collector.mutex.Unlock()
}

func responseMetadataFromContext(ctx context.Context) (*responseMetadataCollector, bool) {
	collector, ok := ctx.Value(responseMetadataKey{}).(*responseMetadataCollector)
	return collector, ok
}

func (c *responseMetadataCollector) snapshot() metadata.MD {
	c.mutex.RLock()
	defer c.mutex.RUnlock()
	return c.value.Copy()
}

func responseMetadataStreamInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	stream, err := streamer(ctx, desc, cc, method, opts...)
	if err != nil {
		return stream, err
	}
	if _, ok := responseMetadataFromContext(ctx); !ok {
		return stream, nil
	}
	return &responseMetadataClientStream{ClientStream: stream, ctx: ctx}, nil
}

type responseMetadataClientStream struct {
	grpc.ClientStream
	ctx context.Context
}

func (s *responseMetadataClientStream) RecvMsg(message interface{}) error {
	err := s.ClientStream.RecvMsg(message)
	if err != nil {
		header, _ := s.Header()
		captureResponseMetadata(s.ctx, metadata.Join(header, s.Trailer()))
	}
	return err
}

// endpointTraceKeyValues builds OpenTelemetry attributes describing a Flight
// endpoint. Ticket contents are intentionally never recorded.
func endpointTraceKeyValues(endpointIndex, numEndpoints int, endpoint *flight.FlightEndpoint) []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.Int("endpointIndex", endpointIndex),
		attribute.Int("numEndpoints", numEndpoints),
	}
	if endpoint == nil {
		return attrs
	}
	if endpoint.Ticket != nil {
		attrs = append(attrs, attribute.Int("ticketBytes", len(endpoint.Ticket.Ticket)))
	}
	if len(endpoint.Location) == 0 {
		attrs = append(attrs, attribute.String("locations", "<empty: using default client connection>"))
	} else {
		uris := make([]string, 0, len(endpoint.Location))
		for _, loc := range endpoint.Location {
			uris = append(uris, loc.Uri)
		}
		attrs = append(attrs, attribute.StringSlice("locations", uris))
	}
	if endpoint.ExpirationTime != nil {
		attrs = append(attrs, attribute.String("expirationTime", endpoint.ExpirationTime.AsTime().String()))
	}
	return attrs
}

// logKeyValues returns OpenTelemetry attributes summarizing stream progress.
func (p *streamProgress) logKeyValues() []attribute.KeyValue {
	attrs := []attribute.KeyValue{
		attribute.Int64("batchesRead", p.batchesRead),
		attribute.Int64("recordsRead", p.recordsRead),
		attribute.Int64("approxBytesRead", p.bytesEstimate),
		attribute.String("elapsed", time.Since(p.start).String()),
	}
	if !p.firstBatchAt.IsZero() {
		attrs = append(attrs, attribute.String("timeToFirstBatch", p.firstBatchAt.Sub(p.start).String()))
	} else {
		attrs = append(attrs, attribute.String("timeToFirstBatch", "never"))
	}
	if !p.lastBatchAt.IsZero() {
		attrs = append(attrs, attribute.String("timeSinceLastBatch", time.Since(p.lastBatchAt).String()))
	}
	return attrs
}

// headerKeyValuesWithPrefix is the shared implementation behind
// correlationHeaderAttrs (incoming) and outgoingCallHeaderAttrs
// (outbound). Only headers in wellKnownCorrelationHeaders are emitted;
// returns nil when none are present.
func headerKeyValuesWithPrefix(md metadata.MD, prefix string) []attribute.KeyValue {
	if len(md) == 0 {
		return nil
	}
	out := make([]attribute.KeyValue, 0, 4)
	for _, k := range wellKnownCorrelationHeaders {
		if vals := md.Get(k); len(vals) > 0 {
			out = append(out, attribute.StringSlice(prefix+k, vals))
		}
	}
	return out
}

// correlationHeaderKeyValues returns OpenTelemetry attributes for well-known
// correlation headers present in md (typically incoming headers/trailers). Uses the
// "hdr_" prefix; only allow-listed headers are emitted.
func correlationHeaderKeyValues(md metadata.MD) []attribute.KeyValue {
	return headerKeyValuesWithPrefix(md, "hdr_")
}

// grpcStatusKeyValues returns OpenTelemetry attributes for the gRPC status
// embedded in err, or nil if err has no status.
func grpcStatusKeyValues(err error) []attribute.KeyValue {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	return []attribute.KeyValue{
		attribute.String("grpc_code", st.Code().String()),
		attribute.String("grpc_message", st.Message()),
	}
}

// flightInfoTracingKeyValues returns OpenTelemetry attributes describing a FlightInfo:
// descriptor type and command prefix, AppMetadata prefix (some backends
// embed a server-side query handle there), and advisory record/byte
// counts. Returns nil for a nil info.
func flightInfoTracingKeyValues(info *flight.FlightInfo) []attribute.KeyValue {
	if info == nil {
		return nil
	}
	attrs := []attribute.KeyValue{
		attribute.Int("numEndpoints", len(info.Endpoint)),
		attribute.Int64("totalRecords", info.TotalRecords),
		attribute.Int64("totalBytes", info.TotalBytes),
		attribute.Bool("haveSchemaInFlightInfo", len(info.Schema) > 0),
	}
	if desc := info.FlightDescriptor; desc != nil {
		attrs = append(attrs, attribute.String("descriptorType", desc.Type.String()))
		if len(desc.Cmd) > 0 {
			limit := len(desc.Cmd)
			if limit > maxLoggedBlobBytes {
				limit = maxLoggedBlobBytes
			}
			attrs = append(attrs,
				attribute.Int("descriptorCmdBytes", len(desc.Cmd)),
				attribute.String("descriptorCmdPrefixHex", hex.EncodeToString(desc.Cmd[:limit])),
			)
		}
		if len(desc.Path) > 0 {
			attrs = append(attrs, attribute.String("descriptorPath", fmt.Sprint(desc.Path)))
		}
	}
	if len(info.AppMetadata) > 0 {
		limit := len(info.AppMetadata)
		if limit > maxLoggedBlobBytes {
			limit = maxLoggedBlobBytes
		}
		attrs = append(attrs,
			attribute.Int("appMetadataBytes", len(info.AppMetadata)),
			attribute.String("appMetadataPrefixHex", hex.EncodeToString(info.AppMetadata[:limit])),
		)
	}
	return attrs
}
