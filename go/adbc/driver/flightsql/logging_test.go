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
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// TestHeaderAttrsWithPrefix_AllowAndDeny exercises the curated allow-list.
// The function is the engine behind both correlationHeaderAttrs ("hdr_"
// prefix, used on received headers / trailers) and outgoingCallHeaderAttrs
// ("out_hdr_" prefix, used on call-time outgoing metadata). Only headers
// in wellKnownCorrelationHeaders are emitted; everything else is dropped.
func TestHeaderAttrsWithPrefix_AllowAndDeny(t *testing.T) {
	md := metadata.New(map[string]string{
		// Allow-listed exact match.
		"x-request-id": "req-1",
		// Microsoft / PBI allow-listed exact matches.
		"activityid":        "act-1",
		"x-pbi-activity-id": "pbi-act-1",
		// Not on the allow-list (no suffix-match fallback).
		"x-vendor-request-id": "vreq-1",
		// Credential header.
		"authorization": "Bearer SECRET",
		// Random header.
		"x-random-header": "noise",
	})

	got := headerAttrsWithPrefix(md, "hdr_")

	// Convert []any of alternating key/value pairs into a map for
	// stable assertions. Each slog.Any takes the form (string, slice).
	gotMap := slogAttrsToMap(t, got)

	wantPresent := []string{
		"hdr_x-request-id",
		"hdr_activityid",
		"hdr_x-pbi-activity-id",
	}
	for _, k := range wantPresent {
		if _, ok := gotMap[k]; !ok {
			t.Errorf("expected attribute %q in headerAttrsWithPrefix result, got keys=%v",
				k, sortedKeys(gotMap))
		}
	}

	wantAbsent := []string{
		"hdr_x-vendor-request-id",
		"hdr_authorization",
		"hdr_x-random-header",
	}
	for _, k := range wantAbsent {
		if _, ok := gotMap[k]; ok {
			t.Errorf("unexpected attribute %q in headerAttrsWithPrefix result "+
				"(must be filtered)", k)
		}
	}
}

// TestHeaderAttrsWithPrefix_EmptyMetadata verifies the function returns
// nil (not an empty slice) when there is nothing to log, so callers can
// safely use append(...) without producing an empty placeholder entry.
func TestHeaderAttrsWithPrefix_EmptyMetadata(t *testing.T) {
	if got := headerAttrsWithPrefix(nil, "hdr_"); got != nil {
		t.Fatalf("headerAttrsWithPrefix(nil, _) = %v, want nil", got)
	}
	if got := headerAttrsWithPrefix(metadata.MD{}, "hdr_"); got != nil {
		t.Fatalf("headerAttrsWithPrefix(empty, _) = %v, want nil", got)
	}
}

// TestCorrelationVsOutgoingPrefix asserts the two public wrappers use
// distinct prefixes so received and sent headers never collide in a
// single log line.
func TestCorrelationVsOutgoingPrefix(t *testing.T) {
	md := metadata.New(map[string]string{
		"activityid": "act-99",
	})

	in := slogAttrsToMap(t, correlationHeaderAttrs(md))
	if _, ok := in["hdr_activityid"]; !ok {
		t.Errorf("correlationHeaderAttrs did not emit hdr_activityid; got %v",
			sortedKeys(in))
	}

	ctx := metadata.NewOutgoingContext(context.Background(), md)
	out := slogAttrsToMap(t, outgoingCallHeaderAttrs(ctx))
	if _, ok := out["out_hdr_activityid"]; !ok {
		t.Errorf("outgoingCallHeaderAttrs did not emit out_hdr_activityid; got %v",
			sortedKeys(out))
	}
}

// TestOutgoingCallHeaderAttrs_NilOrMissingContext covers the safety paths
// (nil context, context without outbound metadata) so that the call
// sites in the unary/stream interceptors do not need their own
// nil-guards.
func TestOutgoingCallHeaderAttrs_NilOrMissingContext(t *testing.T) {
	// Use a typed nil context.Context variable rather than the untyped
	// `nil` literal: staticcheck SA1012 flags passing the literal `nil`
	// to a context.Context parameter (and offers two conflicting
	// auto-fixes that collide in the linter). The function under test
	// *does* have a `ctx == nil` guard that we want to exercise, and a
	// typed nil interface value still compares equal to nil, so this
	// covers the same branch without tripping the lint rule.
	var nilCtx context.Context
	if got := outgoingCallHeaderAttrs(nilCtx); got != nil {
		t.Fatalf("outgoingCallHeaderAttrs(nil) = %v, want nil", got)
	}
	if got := outgoingCallHeaderAttrs(context.Background()); got != nil {
		t.Fatalf("outgoingCallHeaderAttrs(context.Background()) = %v, want nil "+
			"(no outbound metadata set)", got)
	}
}

// TestGrpcStatusAttrs covers the helper that promotes a gRPC status to
// its own structured "grpc_code"/"grpc_message" log fields. The helper
// must handle nil errors, plain Go errors, real gRPC status errors,
// and gRPC errors that have been wrapped via fmt.Errorf("%w", ...).
func TestGrpcStatusAttrs(t *testing.T) {
	t.Run("nil_error", func(t *testing.T) {
		if got := grpcStatusAttrs(nil); got != nil {
			t.Fatalf("grpcStatusAttrs(nil) = %v, want nil", got)
		}
	})

	t.Run("plain_error", func(t *testing.T) {
		// errors.New does not carry a GRPCStatus()/Unwrap chain, so
		// status.FromError returns ok=false and the helper returns
		// nil rather than synthesizing a fake code.
		if got := grpcStatusAttrs(errors.New("boom")); got != nil {
			t.Fatalf("grpcStatusAttrs(errors.New) = %v, want nil", got)
		}
	})

	t.Run("grpc_status_error", func(t *testing.T) {
		err := status.Error(codes.Unavailable, "DoGet: endpoint 0")
		got := slogAttrsToMap(t, grpcStatusAttrs(err))
		if v := got["grpc_code"]; v != "Unavailable" {
			t.Errorf("grpc_code = %q, want %q", v, "Unavailable")
		}
		if v := got["grpc_message"]; v != "DoGet: endpoint 0" {
			t.Errorf("grpc_message = %q, want %q", v, "DoGet: endpoint 0")
		}
	})

	t.Run("wrapped_grpc_status_error", func(t *testing.T) {
		inner := status.Error(codes.DeadlineExceeded, "timeout")
		wrapped := fmt.Errorf("outer: %w", inner)
		got := slogAttrsToMap(t, grpcStatusAttrs(wrapped))
		if v := got["grpc_code"]; v != "DeadlineExceeded" {
			t.Errorf("grpc_code = %q, want %q", v, "DeadlineExceeded")
		}
	})
}

// TestOtelTraceHandler_InjectsTraceIDs creates an slog handler chain
// "JSON -> otelTraceHandler -> buffer", emits a record with a context
// carrying a known SpanContext, and verifies that the handler stamped
// "trace_id" and "span_id" attributes onto the resulting record. This
// is the bridge between the driver's slog stream and any external
// OpenTelemetry traces the host application is producing.
func TestOtelTraceHandler_InjectsTraceIDs(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(&otelTraceHandler{inner: base})

	tidHex := "4bf92f3577b34da6a3ce929d0e0e4736"
	sidHex := "00f067aa0ba902b7"
	tid, err := trace.TraceIDFromHex(tidHex)
	if err != nil {
		t.Fatalf("TraceIDFromHex: %v", err)
	}
	sid, err := trace.SpanIDFromHex(sidHex)
	if err != nil {
		t.Fatalf("SpanIDFromHex: %v", err)
	}
	sc := trace.NewSpanContext(trace.SpanContextConfig{
		TraceID:    tid,
		SpanID:     sid,
		TraceFlags: trace.FlagsSampled,
		Remote:     true,
	})
	ctx := trace.ContextWithSpanContext(context.Background(), sc)

	logger.InfoContext(ctx, "test event")

	rec := decodeFirstLogLine(t, buf.Bytes())
	if got := rec["trace_id"]; got != tidHex {
		t.Errorf("trace_id = %q, want %q (full record: %v)", got, tidHex, rec)
	}
	if got := rec["span_id"]; got != sidHex {
		t.Errorf("span_id = %q, want %q (full record: %v)", got, sidHex, rec)
	}
}

// TestOtelTraceHandler_NoSpanLeavesRecordUnchanged ensures the handler
// is a no-op when the context does not carry a valid SpanContext. We
// must not invent placeholder trace/span IDs just to fill the slot —
// otherwise downstream log search would match unrelated records.
func TestOtelTraceHandler_NoSpanLeavesRecordUnchanged(t *testing.T) {
	var buf bytes.Buffer
	base := slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	logger := slog.New(&otelTraceHandler{inner: base})

	logger.InfoContext(context.Background(), "no span")

	rec := decodeFirstLogLine(t, buf.Bytes())
	if _, ok := rec["trace_id"]; ok {
		t.Errorf("unexpected trace_id in record without active span: %v", rec)
	}
	if _, ok := rec["span_id"]; ok {
		t.Errorf("unexpected span_id in record without active span: %v", rec)
	}
}

// TestWithOtelTraceContext_Idempotent verifies the wrap helper does not
// stack handlers on repeated calls. Without this guard every
// derivation step (NewConnection, NewStatement, ...) would add another
// wrapper, slowing logging and bloating handler chains over time.
func TestWithOtelTraceContext_Idempotent(t *testing.T) {
	if got := withOtelTraceContext(nil); got != nil {
		t.Fatalf("withOtelTraceContext(nil) = %v, want nil", got)
	}

	base := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
	wrapped1 := withOtelTraceContext(base)
	wrapped2 := withOtelTraceContext(wrapped1)
	if _, ok := wrapped2.Handler().(*otelTraceHandler); !ok {
		t.Fatalf("expected outer handler to be *otelTraceHandler after double-wrap")
	}
	// Drill one level into the inner handler — it must not itself be
	// another *otelTraceHandler (which would mean the helper stacked
	// instead of de-duplicating).
	outer := wrapped2.Handler().(*otelTraceHandler)
	if _, doubled := outer.inner.(*otelTraceHandler); doubled {
		t.Fatalf("withOtelTraceContext stacked handlers on repeated calls")
	}
}

// TestSafeLogger_AlwaysWrapsOtel guarantees that every logger going
// through the central safe wrapper carries the OTEL trace bridge, so
// individual callers do not have to remember to add it themselves.
func TestSafeLogger_AlwaysWrapsOtel(t *testing.T) {
	t.Run("nil_input", func(t *testing.T) {
		l := safeLogger(nil)
		if l == nil {
			t.Fatal("safeLogger(nil) returned nil")
		}
		if _, ok := l.Handler().(*otelTraceHandler); !ok {
			t.Errorf("safeLogger(nil) did not wrap with otelTraceHandler; got %T",
				l.Handler())
		}
	})
	t.Run("real_input", func(t *testing.T) {
		base := slog.New(slog.NewJSONHandler(&bytes.Buffer{}, nil))
		l := safeLogger(base)
		if _, ok := l.Handler().(*otelTraceHandler); !ok {
			t.Errorf("safeLogger(base) did not wrap with otelTraceHandler; got %T",
				l.Handler())
		}
	})
}

// ---------- test helpers ----------

// slogAttrsToMap converts the slog.Attr slice returned by the various
// "...Attrs" helpers into a map[string]string keyed by attribute name.
// Each element of the input is expected to be a single slog.Attr value
// (which is what slog.Any / slog.String produce). The map's values are
// taken from the slog.Value's String() representation so callers can do
// straightforward equality assertions without unwrapping Value kinds.
func slogAttrsToMap(t *testing.T, attrs []attribute.KeyValue) map[string]string {
	t.Helper()
	out := make(map[string]string, len(attrs))
	for i, a := range attrs {
		var iface interface{} = a
		attr, ok := iface.(attribute.KeyValue)
		if !ok {
			t.Fatalf("attrs[%d] is %T, want attribute.KeyValue (value=%v)", i, a, a)
		}
		out[string(attr.Key)] = attr.Value.String()
	}
	return out
}

func sortedKeys(m map[string]string) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func decodeFirstLogLine(t *testing.T, b []byte) map[string]any {
	t.Helper()
	line := bytes.TrimSpace(b)
	if i := bytes.IndexByte(line, '\n'); i >= 0 {
		line = line[:i]
	}
	if len(line) == 0 {
		t.Fatalf("no log lines captured")
	}
	rec := map[string]any{}
	if err := json.Unmarshal(line, &rec); err != nil {
		t.Fatalf("failed to decode log line %q: %v", line, err)
	}
	return rec
}
