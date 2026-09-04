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
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/driverbase"
	"go.opentelemetry.io/otel/attribute"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/sdk/trace/tracetest"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

func TestResponseMetadataUnaryInterceptorDoesNotEndParentSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Fatalf("TracerProvider shutdown failed: %v", err)
		}
	})

	conn, err := grpc.NewClient(
		"passthrough:///test",
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient() failed: %v", err)
	}
	t.Cleanup(func() {
		if err := conn.Close(); err != nil {
			t.Fatalf("ClientConn.Close() failed: %v", err)
		}
	})

	ctx, span := tp.Tracer("test.flightsql").Start(context.Background(), "operation")
	err = responseMetadataUnaryInterceptor(
		ctx,
		"/test.Service/Method",
		nil,
		nil,
		conn,
		func(context.Context, string, any, any, *grpc.ClientConn, ...grpc.CallOption) error {
			return nil
		},
	)
	if err != nil {
		t.Fatalf("responseMetadataUnaryInterceptor() failed: %v", err)
	}
	if !span.IsRecording() {
		t.Fatal("responseMetadataUnaryInterceptor() ended the parent span")
	}
	if got := len(recorder.Ended()); got != 0 {
		t.Fatalf("ended spans before parent Span.End() = %d, want 0", got)
	}

	span.End()
	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans len = %d, want 1", len(spans))
	}
	for _, event := range spans[0].Events() {
		if event.Name == "Metadata.Unary.Interceptor./test.Service/Method" {
			return
		}
	}
	t.Fatal("parent span does not contain the unary metadata event")
}

func TestTraceHeaderAttrsWithPrefix_AllowAndDeny(t *testing.T) {
	md := metadata.New(map[string]string{
		"x-request-id":        "req-1",
		"activityid":          "act-1",
		"x-pbi-activity-id":   "pbi-act-1",
		"x-vendor-request-id": "vreq-1",
		"authorization":       "Bearer SECRET",
		"x-random-header":     "noise",
	})

	got := otelAttrsToMap(traceHeaderAttrsWithPrefix(md, traceRequestMetadataPrefix))

	if v := got["rpc.request.metadata.x-request-id"]; len(v) != 1 || v[0] != "req-1" {
		t.Fatalf("rpc.request.metadata.x-request-id = %v, want [req-1]", v)
	}
	if v := got["rpc.request.metadata.activityid"]; len(v) != 1 || v[0] != "act-1" {
		t.Fatalf("rpc.request.metadata.activityid = %v, want [act-1]", v)
	}
	if v := got["rpc.request.metadata.x-pbi-activity-id"]; len(v) != 1 || v[0] != "pbi-act-1" {
		t.Fatalf("rpc.request.metadata.x-pbi-activity-id = %v, want [pbi-act-1]", v)
	}
	if _, ok := got["rpc.request.metadata.authorization"]; ok {
		t.Fatalf("authorization header must not be promoted into tracing attrs: %v", got)
	}
	if _, ok := got["rpc.request.metadata.x-random-header"]; ok {
		t.Fatalf("x-random-header must not be promoted into tracing attrs: %v", got)
	}
	if _, ok := got["rpc.request.metadata.x-vendor-request-id"]; ok {
		t.Fatalf("x-vendor-request-id must not be promoted into tracing attrs: %v", got)
	}
}

func TestTraceHeaderAttrsWithPrefix_EmptyMetadata(t *testing.T) {
	if got := traceHeaderAttrsWithPrefix(nil, traceRequestMetadataPrefix); got != nil {
		t.Fatalf("traceHeaderAttrsWithPrefix(nil, _) = %v, want nil", got)
	}
	if got := traceHeaderAttrsWithPrefix(metadata.MD{}, traceRequestMetadataPrefix); got != nil {
		t.Fatalf("traceHeaderAttrsWithPrefix(empty, _) = %v, want nil", got)
	}
}

func TestTraceHeaderAttrsWithPrefix_AppliedToSpan(t *testing.T) {
	recorder := tracetest.NewSpanRecorder()
	tp := sdktrace.NewTracerProvider(sdktrace.WithSpanProcessor(recorder))
	t.Cleanup(func() {
		if err := tp.Shutdown(context.Background()); err != nil {
			t.Fatalf("TracerProvider shutdown failed: %v", err)
		}
	})

	tracing := stubTracing{
		tracer: tp.Tracer("test.flightsql"),
		attrs: []attribute.KeyValue{
			attribute.Key("db.system.name").String("flight_sql"),
		},
	}

	ctx, _, endSpanHelper := driverbase.StartSpanWithEndSpanHelper(
		context.Background(),
		"FlightSQL.Statement.ExecuteQuery",
		tracing,
		trace.WithAttributes(traceHeaderAttrsWithPrefix(metadata.New(map[string]string{
			"x-request-id":    "req-123",
			"authorization":   "Bearer SECRET",
			"x-random-header": "noise",
		}), traceRequestMetadataPrefix)...),
	)
	_ = ctx
	endSpanHelper.EndSpan()

	spans := recorder.Ended()
	if len(spans) != 1 {
		t.Fatalf("ended spans len = %d, want 1", len(spans))
	}

	got := otelAttrsToMap(spans[0].Attributes())
	if v := got["rpc.request.metadata.x-request-id"]; len(v) != 1 || v[0] != "req-123" {
		t.Fatalf("rpc.request.metadata.x-request-id = %v, want [req-123]", v)
	}
	if _, ok := got["rpc.request.metadata.authorization"]; ok {
		t.Fatalf("authorization header leaked into span attrs: %v", got)
	}
	if _, ok := got["rpc.request.metadata.x-random-header"]; ok {
		t.Fatalf("x-random-header leaked into span attrs: %v", got)
	}
	if v := got["db.operation.name"]; len(v) != 1 || v[0] != "FlightSQL.Statement.ExecuteQuery" {
		t.Fatalf("db.operation.name = %v, want [FlightSQLStatement.ExecuteQuery]", v)
	}
}

type stubTracing struct {
	tracer trace.Tracer
	attrs  []attribute.KeyValue
}

func (s stubTracing) SetTraceParent(string)  {}
func (s stubTracing) GetTraceParent() string { return "" }
func (s stubTracing) StartSpan(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	return s.tracer.Start(ctx, spanName, opts...)
}
func (s stubTracing) GetInitialSpanAttributes() []attribute.KeyValue { return s.attrs }

func otelAttrsToMap(attrs []attribute.KeyValue) map[string][]string {
	out := make(map[string][]string, len(attrs))
	for _, attr := range attrs {
		switch attr.Value.Type() {
		case attribute.STRING:
			out[string(attr.Key)] = []string{attr.Value.AsString()}
		case attribute.STRINGSLICE:
			out[string(attr.Key)] = attr.Value.AsStringSlice()
		}
	}
	return out
}
