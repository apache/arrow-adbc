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
	"strings"
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

// safeLogger returns a non-nil *slog.Logger. If the provided logger is nil
// a discard logger is returned so that callers can always safely log without
// nil-checks. This is important because the streaming code paths can be
// reached by callers (such as tests) that do not have a Logger initialized.
//
// The returned logger is always wrapped with otelTraceHandler so that any
// record emitted through it is automatically stamped with "trace_id" and
// "span_id" attributes when its context carries an active OpenTelemetry
// span. The wrap is idempotent, so wrapping an already-wrapped logger is
// a no-op (see withOtelTraceContext).
func safeLogger(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return withOtelTraceContext(logger)
}

// maxLoggedTicketBytes limits how many bytes of a Flight ticket are emitted in
// log records. Tickets can be opaque server-defined blobs of arbitrary size
// (sometimes embedding large query plans), so we cap how much we include
// to keep log records reasonably sized while still being useful for
// correlation against server-side logs.
const maxLoggedTicketBytes = 32

// endpointLogAttrs builds a slice of slog.Attr describing a Flight endpoint.
// These attributes are intended to be attached to every log record emitted on
// behalf of a per-endpoint stream so an operator can correlate a failure with
// the specific endpoint (URI, ticket prefix, etc.) that produced it. This is
// particularly important when diagnosing errors like
// "[FlightSQL] error reading from server: EOF (Unavailable; DoGet: endpoint N: [])"
// where the empty "[]" indicates the FlightInfo's Location list was empty and
// the default client connection was used as a fallback.
func endpointLogAttrs(endpointIndex, numEndpoints int, endpoint *flight.FlightEndpoint) []any {
	attrs := []any{
		slog.Int("endpointIndex", endpointIndex),
		slog.Int("numEndpoints", numEndpoints),
	}
	if endpoint == nil {
		return attrs
	}
	if endpoint.Ticket != nil {
		ticket := endpoint.Ticket.Ticket
		attrs = append(attrs, slog.Int("ticketBytes", len(ticket)))
		if len(ticket) > 0 {
			limit := len(ticket)
			if limit > maxLoggedTicketBytes {
				limit = maxLoggedTicketBytes
			}
			attrs = append(attrs, slog.String("ticketPrefixHex", hex.EncodeToString(ticket[:limit])))
		}
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

// streamProgress tracks per-endpoint streaming statistics so that informative
// log records and error messages can be emitted when a stream ends (either
// successfully or with an error such as a mid-stream EOF). It is safe to be
// used by a single goroutine that owns one endpoint's stream.
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

// recordBatch updates the progress tracker after a single Arrow record batch
// has been successfully received from the server.
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

// logAttrs returns slog attributes summarizing the progress of this stream.
// These attributes are intended to be appended to per-endpoint log records or
// embedded into error messages produced when a stream ends unexpectedly.
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
// that can be embedded into wrapped error messages so the diagnostic
// information survives when only the error string is preserved (for example
// when the error is exported through a C-language client).
func (p *streamProgress) summary() string {
	if p.batchesRead == 0 {
		return "no batches received before failure; elapsed=" + time.Since(p.start).String()
	}
	return "received " + formatInt(p.batchesRead) + " batch(es), " +
		formatInt(p.recordsRead) + " row(s) before failure; elapsed=" + time.Since(p.start).String() +
		"; timeSinceLastBatch=" + time.Since(p.lastBatchAt).String()
}

// formatInt formats an int64 without pulling in the heavier fmt machinery for
// what would otherwise be a hot, allocation-sensitive path in error messages.
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
			// Surface curated outbound correlation IDs (e.g. PBI
			// ActivityId, x-ms-client-request-id) regardless of log
			// level so they are always available for cross-log joins.
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

	// recvCount tracks how many messages were successfully received from the
	// server before the stream ended. This is logged on every termination
	// (success or failure) so an operator can tell whether a stream that
	// failed with "EOF/Unavailable" never received any data or died mid-way
	// through a large result set.
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

	// Attempt to capture trailer metadata from the underlying stream. Trailers
	// are only valid once the stream has terminated, which is the case here
	// because RecvMsg returned a non-nil error. Trailers frequently carry
	// server-side diagnostic information (e.g., grpc-message, custom error
	// detail headers) that is invaluable when triaging "[FlightSQL] error
	// reading from server: EOF (Unavailable; ...)" reports.
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
		// Promote curated correlation/tracing header values from the trailer
		// to first-class log attributes so an operator can cross-reference
		// this failure with the corresponding server-side trace without
		// enabling Debug-level logging.
		args = append(args, correlationHeaderAttrs(trailer)...)
		// Also promote the outbound correlation IDs that the caller
		// supplied (PBI ActivityId, x-ms-client-request-id, ...) so a
		// single log line carries both sides of the join.
		args = append(args, outgoingCallHeaderAttrs(stream.ctx)...)
		// Emit gRPC code/message as separate structured fields. EOF
		// is treated as a clean close by Flight so loggedErr was
		// nil-ed out above; only attach status attrs for real errors.
		if loggedErr != nil {
			args = append(args, grpcStatusAttrs(loggedErr)...)
		}
		stream.logger.InfoContext(stream.ctx, stream.method, args...)
	}
	return err
}

// wellKnownCorrelationHeaders is the curated allow-list of inbound gRPC
// header/trailer keys that are surfaced verbatim into log records. These
// headers are commonly emitted by FlightSQL servers, gateways, and tracing
// frameworks to allow operators to cross-reference a client-side log entry
// against the corresponding server-side trace or query history record.
//
// The allow-list also intentionally covers the Microsoft / Power BI / Power
// Query family of correlation identifiers ("ActivityId",
// "x-ms-client-request-id", etc.). The driver is frequently invoked from
// Power BI Desktop / Mashup, which records every step under a per-step
// ActivityId GUID; capturing that value on the ADBC side is the single
// most useful join column when triaging an issue against a Power BI
// diagnostic trace bundle.
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
	// Microsoft / Power BI / Power Query family. The exact casing varies
	// by host (PBI Desktop emits "ActivityId" as a property in its trace
	// file but transmits it as a gRPC header that gRPC's metadata
	// package normalizes to lower case), so the allow-list uses the
	// canonical lower-case form throughout. Both the unprefixed and
	// "x-ms-" prefixed variants are listed because the prefix-less form
	// is what Mashup's own diagnostics record.
	"activityid",
	"activity-id",
	"x-ms-activity-id",
	"x-ms-client-request-id",
	"x-ms-request-id",
	"requestid",
	"x-pbi-activity-id",
}

// sensitiveHeaderTokens lists case-insensitive substrings whose presence
// in a header name marks the header as carrying credential material that
// must never be surfaced in driver logs. The list is consulted by
// isSensitiveHeader, which in turn gates correlationHeaderAttrs's
// allow-list and suffix-match paths. The substrings are deliberately
// coarse — "token" rather than a specific header name — because the
// cost of a false positive (a useful correlation header is skipped) is
// much lower than the cost of a false negative (a bearer token is
// written to disk). Header names known to be tracking-only and that
// happen to contain a denylist substring should be added directly to
// wellKnownCorrelationHeaders only after confirming they do not carry
// credentials on the target server.
var sensitiveHeaderTokens = []string{
	"authorization",
	"cookie",
	"password",
	"secret",
	"private",
	"credential",
	"token",
	"apikey",
	"api-key",
}

// isSensitiveHeader reports whether name (compared case-insensitively)
// matches any of the substrings in sensitiveHeaderTokens. It is the only
// place the driver guards against accidentally logging credential
// material lifted out of gRPC metadata; correlationHeaderAttrs and
// outgoingCallHeaderAttrs both consult it before promoting a header.
func isSensitiveHeader(name string) bool {
	lower := strings.ToLower(name)
	for _, tok := range sensitiveHeaderTokens {
		if strings.Contains(lower, tok) {
			return true
		}
	}
	return false
}

// headerAttrsWithPrefix is the shared implementation behind
// correlationHeaderAttrs (incoming headers/trailers) and
// outgoingCallHeaderAttrs (call-time outbound metadata). Both surfaces
// use the same allow-list and the same sensitive-header denylist; only
// the slog attribute prefix differs so an operator can tell at a glance
// whether the value was something the driver sent or something the
// server returned. Returns nil when no allow-listed header is present
// so callers can use append(...).
func headerAttrsWithPrefix(md metadata.MD, prefix string) []any {
	if len(md) == 0 {
		return nil
	}
	out := make([]any, 0, 4)
	seen := make(map[string]bool, len(wellKnownCorrelationHeaders))
	for _, k := range wellKnownCorrelationHeaders {
		seen[k] = true
		if isSensitiveHeader(k) {
			continue
		}
		if vals := md.Get(k); len(vals) > 0 {
			out = append(out, slog.Any(prefix+k, vals))
		}
	}
	for k, vals := range md {
		lk := strings.ToLower(k)
		if seen[lk] {
			continue
		}
		if isSensitiveHeader(lk) {
			continue
		}
		if strings.HasSuffix(lk, "-request-id") ||
			strings.HasSuffix(lk, "-trace-id") ||
			strings.HasSuffix(lk, "-query-id") ||
			strings.HasSuffix(lk, "-session-id") ||
			strings.HasSuffix(lk, "-activity-id") {
			out = append(out, slog.Any(prefix+lk, vals))
		}
	}
	return out
}

// correlationHeaderAttrs returns slog attributes for the well-known
// correlation/tracing headers present in md (typically headers or
// trailers received from the server). Attribute names use the "hdr_"
// prefix to distinguish them from outbound headers, which use
// "out_hdr_" (see outgoingCallHeaderAttrs). Sensitive headers (matched
// by isSensitiveHeader) are always skipped.
func correlationHeaderAttrs(md metadata.MD) []any {
	return headerAttrsWithPrefix(md, "hdr_")
}

// outgoingCallHeaderAttrs returns slog attributes for the well-known
// correlation headers present on ctx's outbound gRPC metadata (the
// values the caller — for example Power Query — set before invoking
// the driver). Attribute names use the "out_hdr_" prefix so they can
// be visually separated from incoming response headers in a single
// log line. This is the join column that pairs an ADBC log record with
// the PBI / Mashup trace entry that triggered it.
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

// grpcStatusAttrs returns slog attributes describing the gRPC status
// embedded in err, or nil when err is nil or carries no gRPC status.
// The attributes ("grpc_code" and "grpc_message") are emitted as
// first-class structured fields rather than left inside the formatted
// error string so an operator can filter on them directly when
// triaging a batch of incidents — "Unavailable", "DeadlineExceeded"
// and "Unauthenticated" require very different remediations.
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

// otelTraceHandler wraps an slog.Handler so that every record produced
// through it is automatically stamped with the current OpenTelemetry
// "trace_id" and "span_id" when the record's context carries an active
// span. This bridges the driver's structured log stream to its
// OpenTelemetry traces (and, transitively, to server-side traces that
// follow the same trace context via the W3C "traceparent" header), so
// a single identifier can be used to join all three views of a single
// failing operation.
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

// withOtelTraceContext wraps logger so that every record it produces
// carries "trace_id" and "span_id" attributes derived from the
// OpenTelemetry span (if any) attached to the record's context. The
// wrap is idempotent: calling it twice on the same logger does not
// produce duplicate handlers. A nil logger is returned unchanged so
// callers can chain it freely.
func withOtelTraceContext(logger *slog.Logger) *slog.Logger {
	if logger == nil {
		return logger
	}
	if _, alreadyWrapped := logger.Handler().(*otelTraceHandler); alreadyWrapped {
		return logger
	}
	return slog.New(&otelTraceHandler{inner: logger.Handler()})
}

// newRandomID returns a short random identifier suitable for tagging log
// records and ADBC error details. The result has the form "<prefix>-<hex>"
// where hex is 12 lower-case hex characters (48 bits of entropy) — enough
// to disambiguate concurrent operations within a single process without
// inflating log line widths. Falls back to a nanosecond timestamp if the
// crypto/rand source is unavailable.
func newRandomID(prefix string) string {
	var b [6]byte
	if _, err := rand.Read(b[:]); err != nil {
		return prefix + "-" + strconv.FormatInt(time.Now().UnixNano(), 16)
	}
	return prefix + "-" + hex.EncodeToString(b[:])
}

// queryFingerprintAttrs builds slog attributes identifying a SQL query in a
// way that is safe to log even when the query may contain sensitive literal
// values from end-user inputs. Always emits the query length and the first
// 16 hex characters of its SHA-256 digest so two log lines reporting the
// "same" query can be matched without exposing the text. When previewLimit
// is greater than zero a "query_preview" attribute is also emitted holding
// the first previewLimit characters of the query verbatim — this is opt-in
// because Power Query / Mashup workflows can embed user data in SQL.
func queryFingerprintAttrs(query string, previewLimit int) []any {
	if query == "" {
		return []any{slog.String("query_type", "empty")}
	}
	h := sha256.Sum256([]byte(query))
	attrs := []any{
		slog.String("query_type", "sql"),
		slog.Int("query_length", len(query)),
		slog.String("query_sha256_prefix", hex.EncodeToString(h[:8])),
	}
	if previewLimit > 0 {
		preview := query
		truncated := false
		if len(preview) > previewLimit {
			preview = preview[:previewLimit]
			truncated = true
		}
		attrs = append(attrs, slog.String("query_preview", preview))
		if truncated {
			attrs = append(attrs, slog.Bool("query_preview_truncated", true))
		}
	}
	return attrs
}

// substraitFingerprintAttrs builds slog attributes identifying a Substrait
// plan. The plan bytes themselves are never logged; only the length, the
// SHA-256 prefix, and the declared protocol version are emitted so an
// operator can fingerprint the plan without exposing its contents.
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

// flightInfoLogAttrs returns slog attributes describing a FlightInfo
// response. The attributes include correlation hooks (descriptor command
// type and a hex prefix of the descriptor command bytes plus a hex prefix
// of FlightInfo.AppMetadata — many backends, including Dremio and Spice,
// embed an opaque server-side query handle in AppMetadata) and capacity
// information (number of endpoints, advertised total records/bytes). The
// total records and bytes are advisory only as servers are not required to
// populate them.
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
			if limit > maxLoggedTicketBytes {
				limit = maxLoggedTicketBytes
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
		if limit > maxLoggedTicketBytes {
			limit = maxLoggedTicketBytes
		}
		attrs = append(attrs,
			slog.Int("appMetadataBytes", len(info.AppMetadata)),
			slog.String("appMetadataPrefixHex", hex.EncodeToString(info.AppMetadata[:limit])),
		)
	}
	return attrs
}

// withOperationIDs returns err with "statement_id" and "connection_id"
// text error details appended when err is an adbc.Error. Otherwise the
// original error is returned unchanged. Recording these identifiers in
// ADBC's error details surfaces them all the way to the host application
// (for example Power Query / Mashup), enabling a single failing operation
// to be traced through both client-side logs and the host's own logs.
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

// operationIDsCtxKey is the context.Value key used to propagate the
// statement_id and connection_id identifiers from a statement's Execute
// method into asynchronous code paths (such as the per-endpoint goroutines
// inside newRecordReader) so that errors surfaced later through
// reader.Err() can be stamped with the same correlation IDs that the
// synchronous error path uses.
type operationIDsCtxKey struct{}

type operationIDs struct {
	statementID  string
	connectionID string
}

// withOperationIDsCtx attaches the given statement and connection
// identifiers to ctx so they can be retrieved by operationIDsFromCtx in
// downstream code such as newRecordReader. Returns ctx unchanged if both
// identifiers are empty.
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
