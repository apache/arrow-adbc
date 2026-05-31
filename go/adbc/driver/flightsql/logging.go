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
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"log/slog"
	"strconv"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"go.opentelemetry.io/otel/trace"
	"golang.org/x/exp/maps"
	"golang.org/x/exp/slices"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// safeLogger returns a non-nil *slog.Logger wrapped with otelTraceHandler
// so records carry trace/span IDs when their context has an active span.
// A nil logger becomes a discard logger; the wrap is idempotent.
func safeLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return withOtelTraceContext(logger)
}

// maxLoggedBlobBytes caps how many bytes of opaque server-defined blobs
// (descriptor commands, AppMetadata) are emitted in log records. Flight
// tickets are not logged at all because they may carry sensitive data.
const maxLoggedBlobBytes = 32

// endpointLogAttrs builds slog attributes describing a Flight endpoint
// (index, ticket length, locations) for per-endpoint log records. Ticket
// contents are intentionally never logged.
func endpointLogAttrs(endpointIndex, numEndpoints int, endpoint *flight.FlightEndpoint) []any {
	attrs := []any{
		slog.Int("endpointIndex", endpointIndex),
		slog.Int("numEndpoints", numEndpoints),
	}
	if endpoint == nil {
		return attrs
	}
	if endpoint.Ticket != nil {
		attrs = append(attrs, slog.Int("ticketBytes", len(endpoint.Ticket.Ticket)))
	}
	if len(endpoint.Location) == 0 {
		attrs = append(attrs, slog.String("locations", "<empty: using default client connection>"))
	} else {
		uris := make([]string, 0, len(endpoint.Location))
		for _, loc := range endpoint.Location {
			uris = append(uris, loc.Uri)
		}
		attrs = append(attrs, slog.Any("locations", uris))
	}
	if endpoint.ExpirationTime != nil {
		attrs = append(attrs, slog.Time("expirationTime", endpoint.ExpirationTime.AsTime()))
	}
	return attrs
}

// streamProgress tracks per-endpoint streaming statistics for log records
// and error messages emitted when a stream ends. Not safe for concurrent
// use; intended to be owned by the goroutine driving one endpoint.
type streamProgress struct {
	start         time.Time
	firstBatchAt  time.Time
	lastBatchAt   time.Time
	batchesRead   int64
	recordsRead   int64
	bytesEstimate int64
}

func newStreamProgress() *streamProgress {
	return &streamProgress{start: time.Now()}
}

// recordBatch updates the tracker after one Arrow record batch was received.
func (p *streamProgress) recordBatch(rows int64, bytes int64) {
	now := time.Now()
	if p.batchesRead == 0 {
		p.firstBatchAt = now
	}
	p.lastBatchAt = now
	p.batchesRead++
	p.recordsRead += rows
	p.bytesEstimate += bytes
}

// logAttrs returns slog attributes summarizing this stream's progress.
func (p *streamProgress) logAttrs() []any {
	attrs := []any{
		slog.Int64("batchesRead", p.batchesRead),
		slog.Int64("recordsRead", p.recordsRead),
		slog.Int64("approxBytesRead", p.bytesEstimate),
		slog.Duration("elapsed", time.Since(p.start)),
	}
	if !p.firstBatchAt.IsZero() {
		attrs = append(attrs, slog.Duration("timeToFirstBatch", p.firstBatchAt.Sub(p.start)))
	} else {
		attrs = append(attrs, slog.String("timeToFirstBatch", "never"))
	}
	if !p.lastBatchAt.IsZero() {
		attrs = append(attrs, slog.Duration("timeSinceLastBatch", time.Since(p.lastBatchAt)))
	}
	return attrs
}

// summary returns a compact human-readable summary of the stream's progress
// suitable for embedding into wrapped error messages.
func (p *streamProgress) summary() string {
	if p.batchesRead == 0 {
		return "no batches received before failure; elapsed=" + time.Since(p.start).String()
	}
	return "received " + formatInt(p.batchesRead) + " batch(es), " +
		formatInt(p.recordsRead) + " row(s) before failure; elapsed=" + time.Since(p.start).String() +
		"; timeSinceLastBatch=" + time.Since(p.lastBatchAt).String()
}

// formatInt formats an int64 without pulling in fmt.
func formatInt(n int64) string {
	return strconv.FormatInt(n, 10)
}

func makeUnaryLoggingInterceptor(logger *slog.Logger) grpc.UnaryClientInterceptor {
	interceptor := func(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
		start := time.Now()
		// Ignore errors
		outgoing, _ := metadata.FromOutgoingContext(ctx)
		err := invoker(ctx, method, req, reply, cc, opts...)
		if logger.Enabled(ctx, slog.LevelDebug) {
			args := []any{"target", cc.Target(), "duration", time.Since(start), "err", err, "metadata", outgoing}
			args = append(args, outgoingCallHeaderAttrs(ctx)...)
			args = append(args, grpcStatusAttrs(err)...)
			logger.DebugContext(ctx, method, args...)
		} else {
			keys := maps.Keys(outgoing)
			slices.Sort(keys)
			args := []any{"target", cc.Target(), "duration", time.Since(start), "err", err, "metadata", keys}
			// Surface curated outbound correlation IDs regardless of level.
			args = append(args, outgoingCallHeaderAttrs(ctx)...)
			args = append(args, grpcStatusAttrs(err)...)
			logger.InfoContext(ctx, method, args...)
		}
		return err
	}
	return interceptor
}

func makeStreamLoggingInterceptor(logger *slog.Logger) grpc.StreamClientInterceptor {
	interceptor := func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
		start := time.Now()
		// Ignore errors
		outgoing, _ := metadata.FromOutgoingContext(ctx)
		stream, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			args := []any{"target", cc.Target(), "duration", time.Since(start), "err", err}
			args = append(args, outgoingCallHeaderAttrs(ctx)...)
			args = append(args, grpcStatusAttrs(err)...)
			logger.InfoContext(ctx, method, args...)
			return stream, err
		}

		return &loggedStream{ClientStream: stream, logger: logger, ctx: ctx, method: method, start: start, target: cc.Target(), outgoing: outgoing}, err
	}
	return interceptor
}

type loggedStream struct {
	grpc.ClientStream

	logger   *slog.Logger
	ctx      context.Context
	method   string
	start    time.Time
	target   string
	outgoing metadata.MD

	// recvCount tracks how many messages were received before the stream
	// ended; logged on termination so EOFs on empty streams are distinguishable
	// from mid-stream failures.
	recvCount int64
}

func (stream *loggedStream) RecvMsg(m any) error {
	err := stream.ClientStream.RecvMsg(m)
	if err == nil {
		stream.recvCount++
		return nil
	}

	loggedErr := err
	if loggedErr == io.EOF {
		loggedErr = nil
	}

	// Capture trailers from the terminated stream; they often carry
	// server-side diagnostic information for failure triage.
	trailer := stream.Trailer()

	if stream.logger.Enabled(stream.ctx, slog.LevelDebug) {
		stream.logger.DebugContext(stream.ctx, stream.method,
			"target", stream.target,
			"duration", time.Since(stream.start),
			"err", loggedErr,
			"recvMessages", stream.recvCount,
			"metadata", stream.outgoing,
			"trailer", trailer,
		)
	} else {
		keys := maps.Keys(stream.outgoing)
		slices.Sort(keys)
		trailerKeys := maps.Keys(trailer)
		slices.Sort(trailerKeys)
		args := []any{
			"target", stream.target,
			"duration", time.Since(stream.start),
			"err", loggedErr,
			"recvMessages", stream.recvCount,
			"metadata", keys,
			"trailer", trailerKeys,
		}
		// Promote curated correlation headers from the trailer.
		args = append(args, correlationHeaderAttrs(trailer)...)
		// Promote the outbound correlation IDs the caller supplied.
		args = append(args, outgoingCallHeaderAttrs(stream.ctx)...)
		// EOF is a clean close in Flight, so loggedErr was nil-ed above;
		// only attach status attrs for real errors.
		if loggedErr != nil {
			args = append(args, grpcStatusAttrs(loggedErr)...)
		}
		stream.logger.InfoContext(stream.ctx, stream.method, args...)
	}
	return err
}

// wellKnownCorrelationHeaders is the curated allow-list of inbound gRPC
// header/trailer keys that are surfaced verbatim into log records, for
// cross-referencing client-side logs with server-side traces. Includes
// the Microsoft / Power BI / Power Query family of correlation IDs.
var wellKnownCorrelationHeaders = []string{
	"x-request-id",
	"x-correlation-id",
	"x-trace-id",
	"x-amzn-trace-id",
	"x-b3-traceid",
	"x-b3-spanid",
	"traceparent",
	"tracestate",
	"x-arrow-flight-session-id",
	"x-dremio-request-id",
	"x-dremio-query-id",
	"x-server-version",
	"server",
	// Microsoft / Power BI / Power Query family. gRPC's metadata package
	// normalizes header names to lower case; both unprefixed and "x-ms-"
	// variants are listed because Mashup's diagnostics record the former.
	"activityid",
	"activity-id",
	"x-ms-activity-id",
	"x-ms-client-request-id",
	"x-ms-request-id",
	"requestid",
	"x-pbi-activity-id",
}

// headerAttrsWithPrefix is the shared implementation behind
// correlationHeaderAttrs (incoming) and outgoingCallHeaderAttrs
// (outbound). Only headers in wellKnownCorrelationHeaders are emitted;
// returns nil when none are present.
func headerAttrsWithPrefix(md metadata.MD, prefix string) []any {
	if len(md) == 0 {
		return nil
	}
	out := make([]any, 0, 4)
	for _, k := range wellKnownCorrelationHeaders {
		if vals := md.Get(k); len(vals) > 0 {
			out = append(out, slog.Any(prefix+k, vals))
		}
	}
	return out
}

// correlationHeaderAttrs returns slog attributes for well-known correlation
// headers present in md (typically incoming headers/trailers). Uses the
// "hdr_" prefix; only allow-listed headers are emitted.
func correlationHeaderAttrs(md metadata.MD) []any {
	return headerAttrsWithPrefix(md, "hdr_")
}

// outgoingCallHeaderAttrs returns slog attributes for well-known correlation
// headers on ctx's outbound gRPC metadata. Uses the "out_hdr_" prefix.
func outgoingCallHeaderAttrs(ctx context.Context) []any {
	if ctx == nil {
		return nil
	}
	md, ok := metadata.FromOutgoingContext(ctx)
	if !ok {
		return nil
	}
	return headerAttrsWithPrefix(md, "out_hdr_")
}

// grpcStatusAttrs returns "grpc_code" and "grpc_message" slog attributes
// for the gRPC status embedded in err, or nil if err has no status.
func grpcStatusAttrs(err error) []any {
	if err == nil {
		return nil
	}
	st, ok := status.FromError(err)
	if !ok {
		return nil
	}
	return []any{
		slog.String("grpc_code", st.Code().String()),
		slog.String("grpc_message", st.Message()),
	}
}

// otelTraceHandler wraps an slog.Handler so records are stamped with the
// current OpenTelemetry "trace_id" and "span_id" when the record's context
// carries an active span.
type otelTraceHandler struct {
	inner slog.Handler
}

func (h *otelTraceHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *otelTraceHandler) Handle(ctx context.Context, r slog.Record) error {
	if ctx != nil {
		sc := trace.SpanFromContext(ctx).SpanContext()
		if sc.IsValid() {
			r.AddAttrs(
				slog.String("trace_id", sc.TraceID().String()),
				slog.String("span_id", sc.SpanID().String()),
			)
		}
	}
	return h.inner.Handle(ctx, r)
}

func (h *otelTraceHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &otelTraceHandler{inner: h.inner.WithAttrs(attrs)}
}

func (h *otelTraceHandler) WithGroup(name string) slog.Handler {
	return &otelTraceHandler{inner: h.inner.WithGroup(name)}
}

// withOtelTraceContext wraps logger so records carry "trace_id" and
// "span_id" attributes from any OpenTelemetry span on the record's
// context. Idempotent; a nil logger is returned unchanged.
func withOtelTraceContext(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return logger
	}
	if _, alreadyWrapped := logger.Handler().(*otelTraceHandler); alreadyWrapped {
		return logger
	}
	return slog.New(&otelTraceHandler{inner: logger.Handler()})
}

// newRandomID returns a short "<prefix>-<hex>" identifier for tagging log
// records and error details. Falls back to a nanosecond timestamp if
// crypto/rand is unavailable.
func newRandomID(prefix string) string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

// queryFingerprintAttrs builds slog attributes identifying a SQL query
// without exposing it: length and a SHA-256 prefix. The query text itself
// is never logged because it can embed end-user PII as literals.
func queryFingerprintAttrs(query string) []any {
	if query == "" {
		return []any{slog.String("query_type", "empty")}
	}
	h := sha256.Sum256([]byte(query))
	return []any{
		slog.String("query_type", "sql"),
		slog.Int("query_length", len(query)),
		slog.String("query_sha256_prefix", hex.EncodeToString(h[:8])),
	}
}

// substraitFingerprintAttrs builds slog attributes identifying a Substrait
// plan: length, SHA-256 prefix, and protocol version. Plan bytes are never
// logged.
func substraitFingerprintAttrs(plan []byte, version string) []any {
	if len(plan) == 0 {
		return []any{slog.String("query_type", "substrait_empty")}
	}
	h := sha256.Sum256(plan)
	attrs := []any{
		slog.String("query_type", "substrait"),
		slog.Int("substrait_plan_bytes", len(plan)),
		slog.String("substrait_plan_sha256_prefix", hex.EncodeToString(h[:8])),
	}
	if version != "" {
		attrs = append(attrs, slog.String("substrait_version", version))
	}
	return attrs
}

// flightInfoLogAttrs returns slog attributes describing a FlightInfo:
// descriptor type and command prefix, AppMetadata prefix (some backends
// embed a server-side query handle there), and advisory record/byte
// counts. Returns nil for a nil info.
func flightInfoLogAttrs(info *flight.FlightInfo) []any {
	if info == nil {
		return nil
	}
	attrs := []any{
		slog.Int("numEndpoints", len(info.Endpoint)),
		slog.Int64("totalRecords", info.TotalRecords),
		slog.Int64("totalBytes", info.TotalBytes),
		slog.Bool("haveSchemaInFlightInfo", len(info.Schema) > 0),
	}
	if desc := info.FlightDescriptor; desc != nil {
		attrs = append(attrs, slog.String("descriptorType", desc.Type.String()))
		if len(desc.Cmd) > 0 {
			limit := len(desc.Cmd)
			if limit > maxLoggedBlobBytes {
				limit = maxLoggedBlobBytes
			}
			attrs = append(attrs,
				slog.Int("descriptorCmdBytes", len(desc.Cmd)),
				slog.String("descriptorCmdPrefixHex", hex.EncodeToString(desc.Cmd[:limit])),
			)
		}
		if len(desc.Path) > 0 {
			attrs = append(attrs, slog.Any("descriptorPath", desc.Path))
		}
	}
	if len(info.AppMetadata) > 0 {
		limit := len(info.AppMetadata)
		if limit > maxLoggedBlobBytes {
			limit = maxLoggedBlobBytes
		}
		attrs = append(attrs,
			slog.Int("appMetadataBytes", len(info.AppMetadata)),
			slog.String("appMetadataPrefixHex", hex.EncodeToString(info.AppMetadata[:limit])),
		)
	}
	return attrs
}

// withOperationIDs appends "statement_id" and "connection_id" text error
// details to err when err is an adbc.Error. Non-adbc.Error values are
// returned unchanged.
func withOperationIDs(err error, statementID, connectionID string) error {
	if err == nil {
		return err
	}
	adbcErr, ok := err.(adbc.Error)
	if !ok {
		return err
	}
	if statementID != "" {
		adbcErr.Details = append(adbcErr.Details,
			&adbc.TextErrorDetail{Name: "statement_id", Detail: statementID})
	}
	if connectionID != "" {
		adbcErr.Details = append(adbcErr.Details,
			&adbc.TextErrorDetail{Name: "connection_id", Detail: connectionID})
	}
	return adbcErr
}

// operationIDsCtxKey is the context.Value key used to propagate
// statement_id and connection_id into async code paths (e.g. the
// per-endpoint goroutines in newRecordReader) so errors surfaced via
// reader.Err() carry the same correlation IDs as synchronous errors.
type operationIDsCtxKey struct{}

type operationIDs struct {
	statementID  string
	connectionID string
}

// withOperationIDsCtx attaches the given statement and connection IDs to
// ctx for retrieval by operationIDsFromCtx. Returns ctx unchanged when
// both identifiers are empty.
func withOperationIDsCtx(ctx context.Context, statementID, connectionID string) context.Context {
	if statementID == "" && connectionID == "" {
		return ctx
	}
	return context.WithValue(ctx, operationIDsCtxKey{}, operationIDs{
		statementID:  statementID,
		connectionID: connectionID,
	})
}

// operationIDsFromCtx returns the statement and connection identifiers
// previously stored on ctx by withOperationIDsCtx, or empty strings if
// none were set. Safe to call on any context.
func operationIDsFromCtx(ctx context.Context) (statementID, connectionID string) {
	if ctx == nil {
		return "", ""
	}
	if v, ok := ctx.Value(operationIDsCtxKey{}).(operationIDs); ok {
		return v.statementID, v.connectionID
	}
	return "", ""
}
