package snowflake

import (
	"context"
	"fmt"
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/trace"
)

func TestContextWithTraceParentPrecedence(t *testing.T) {
	ctx := context.Background()

	dbTP := mockTraceparent(1)
	ctx = contextWithTraceParent(ctx, dbTP, traceParentScopeDatabase)
	requireTraceParentFromContext(t, ctx, dbTP)

	connTP := mockTraceparent(2)
	ctx = contextWithTraceParent(ctx, connTP, traceParentScopeConnection)
	requireTraceParentFromContext(t, ctx, connTP)

	connReplacement := mockTraceparent(3)
	ctx = contextWithTraceParent(ctx, connReplacement, traceParentScopeConnection)
	requireTraceParentFromContext(t, ctx, connReplacement)

	stmtTP := mockTraceparent(4)
	ctx = contextWithTraceParent(ctx, stmtTP, traceParentScopeStatement)
	requireTraceParentFromContext(t, ctx, stmtTP)

	ctx = contextWithTraceParent(ctx, connTP, traceParentScopeConnection)
	requireTraceParentFromContext(t, ctx, stmtTP)
}

func requireTraceParentFromContext(t *testing.T, ctx context.Context, expected string) {
	t.Helper()

	want := spanContextFromTraceParent(t, expected)
	got := trace.SpanContextFromContext(ctx)
	require.True(t, got.IsValid(), "span context must be valid")
	require.Equal(t, want.TraceID(), got.TraceID())
	require.Equal(t, want.SpanID(), got.SpanID())
	require.Equal(t, want.TraceFlags(), got.TraceFlags())
}

func spanContextFromTraceParent(t *testing.T, traceParent string) trace.SpanContext {
	t.Helper()

	carrier := propagation.MapCarrier{"traceparent": traceParent}
	ctx := propagation.TraceContext{}.Extract(context.Background(), carrier)
	sc := trace.SpanContextFromContext(ctx)
	require.True(t, sc.IsValid(), "traceparent must encode a valid span context")
	return sc
}

func mockTraceparent(seed uint64) string {
	return fmt.Sprintf("00-%032x-%016x-01", seed, seed+1)
}
