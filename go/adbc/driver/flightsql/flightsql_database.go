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
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"log/slog"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/bluele/gcache"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/peer"
)

type dbDialOpts struct {
	opts       []grpc.DialOption
	maxMsgSize int
	authority  string
}

func (d *dbDialOpts) rebuild() {
	d.opts = []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(d.maxMsgSize),
			grpc.MaxCallSendMsgSize(d.maxMsgSize)),
	}
	if d.authority != "" {
		d.opts = append(d.opts, grpc.WithAuthority(d.authority))
	}
}

type databaseImpl struct {
	driverbase.DatabaseImplBase

	uri           *url.URL
	creds         credentials.TransportCredentials
	user, pass    string
	hdrs          metadata.MD
	timeout       timeoutOption
	dialOpts      dbDialOpts
	enableCookies bool
	options       map[string]string
	userDialOpts  []grpc.DialOption
	oauthToken    credentials.PerRPCCredentials
	// tracesExporter records the value of
	// adbc.OptionKeyTelemetryTracesExporter that was supplied (if any)
	// at database construction time. The tracer itself is already
	// initialized by driverbase by the time SetOptions runs, so this
	// field is retained purely so GetOption can echo back the
	// configured value (callers expect "get returns what was set") and
	// so SetOption can return a precise diagnostic when a caller tries
	// to change the exporter after the database has been opened.
	tracesExporter string
	// tracesFolderPath records the value of
	// adbc.OptionKeyTelemetryTracesFolderPath that was supplied at
	// construction time. The on-disk RotatingFileWriter behind the
	// "adbcfile" exporter is created during NewDatabase*, so this field
	// is retained for symmetry with tracesExporter: GetOption can echo
	// the configured value back, and SetOption can surface a precise
	// diagnostic when a caller tries to retarget the folder after the
	// writer is already running.
	tracesFolderPath string
}

func (d *databaseImpl) SetOptions(cnOptions map[string]string) error {
	var tlsConfig tls.Config

	for k, v := range cnOptions {
		d.options[k] = v
	}

	if authority, ok := cnOptions[OptionAuthority]; ok {
		d.dialOpts.authority = authority
		delete(cnOptions, OptionAuthority)
	}

	mtlsCert := cnOptions[OptionMTLSCertChain]
	mtlsKey := cnOptions[OptionMTLSPrivateKey]
	switch {
	case mtlsCert != "" && mtlsKey != "":
		cert, err := tls.X509KeyPair([]byte(mtlsCert), []byte(mtlsKey))
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid mTLS certificate: %#v", err),
				Code: adbc.StatusInvalidArgument,
			}
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		delete(cnOptions, OptionMTLSCertChain)
		delete(cnOptions, OptionMTLSPrivateKey)
	case mtlsCert != "":
		return adbc.Error{
			Msg:  fmt.Sprintf("Must provide both '%s' and '%s', only provided '%s'", OptionMTLSCertChain, OptionMTLSPrivateKey, OptionMTLSCertChain),
			Code: adbc.StatusInvalidArgument,
		}
	case mtlsKey != "":
		return adbc.Error{
			Msg:  fmt.Sprintf("Must provide both '%s' and '%s', only provided '%s'", OptionMTLSCertChain, OptionMTLSPrivateKey, OptionMTLSPrivateKey),
			Code: adbc.StatusInvalidArgument,
		}
	}

	if hostname, ok := cnOptions[OptionSSLOverrideHostname]; ok {
		tlsConfig.ServerName = hostname
		delete(cnOptions, OptionSSLOverrideHostname)
	}

	if val, ok := cnOptions[OptionSSLSkipVerify]; ok {
		switch val {
		case adbc.OptionValueEnabled:
			tlsConfig.InsecureSkipVerify = true
		case adbc.OptionValueDisabled:
			tlsConfig.InsecureSkipVerify = false
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionSSLSkipVerify, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
		delete(cnOptions, OptionSSLSkipVerify)
	}

	if cert, ok := cnOptions[OptionSSLRootCerts]; ok {
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM([]byte(cert)) {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': failed to append certificates", OptionSSLRootCerts),
				Code: adbc.StatusInvalidArgument,
			}
		}
		tlsConfig.RootCAs = cp
		delete(cnOptions, OptionSSLRootCerts)
	}

	d.creds = credentials.NewTLS(&tlsConfig)

	if auth, ok := cnOptions[OptionAuthorizationHeader]; ok {
		d.hdrs.Set("authorization", auth)
		delete(cnOptions, OptionAuthorizationHeader)
	}

	const authConflictError = "Authentication conflict: Use either Authorization header OR username/password parameter"

	if u, ok := cnOptions[adbc.OptionKeyUsername]; ok {
		if d.hdrs.Len() > 0 {
			return adbc.Error{
				Msg:  authConflictError,
				Code: adbc.StatusInvalidArgument,
			}
		}
		d.user = u
		delete(cnOptions, adbc.OptionKeyUsername)
	}

	if p, ok := cnOptions[adbc.OptionKeyPassword]; ok {
		if d.hdrs.Len() > 0 {
			return adbc.Error{
				Msg:  authConflictError,
				Code: adbc.StatusInvalidArgument,
			}
		}
		d.pass = p
		delete(cnOptions, adbc.OptionKeyPassword)
	}

	if flow, ok := cnOptions[OptionKeyOauthFlow]; ok {
		if d.hdrs.Len() > 0 {
			return adbc.Error{
				Msg:  authConflictError,
				Code: adbc.StatusInvalidArgument,
			}
		}

		var err error
		switch flow {
		case ClientCredentials:
			d.oauthToken, err = newClientCredentials(cnOptions, &tlsConfig)
		case TokenExchange:
			d.oauthToken, err = newTokenExchangeFlow(cnOptions, &tlsConfig)
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("oauth flow not implemented: %s", flow),
				Code: adbc.StatusNotImplemented,
			}
		}

		if err != nil {
			return err
		}
		delete(cnOptions, OptionKeyOauthFlow)
	}

	var err error
	if tv, ok := cnOptions[OptionTimeoutFetch]; ok {
		if err = d.timeout.setTimeoutString(OptionTimeoutFetch, tv); err != nil {
			return err
		}
		delete(cnOptions, OptionTimeoutFetch)
	}

	if tv, ok := cnOptions[OptionTimeoutQuery]; ok {
		if err = d.timeout.setTimeoutString(OptionTimeoutQuery, tv); err != nil {
			return err
		}
		delete(cnOptions, OptionTimeoutQuery)
	}

	if tv, ok := cnOptions[OptionTimeoutUpdate]; ok {
		if err = d.timeout.setTimeoutString(OptionTimeoutUpdate, tv); err != nil {
			return err
		}
		delete(cnOptions, OptionTimeoutUpdate)
	}

	if tv, ok := cnOptions[OptionTimeoutConnect]; ok {
		if err = d.timeout.setTimeoutString(OptionTimeoutConnect, tv); err != nil {
			return err
		}
		delete(cnOptions, OptionTimeoutConnect)
	}

	// gRPC deprecated this and explicitly recommends against it
	delete(cnOptions, OptionWithBlock)

	if val, ok := cnOptions[OptionWithMaxMsgSize]; ok {
		var err error
		var size int
		if size, err = strconv.Atoi(val); err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s' is not a positive integer", OptionWithMaxMsgSize, val),
				Code: adbc.StatusInvalidArgument,
			}
		} else if size <= 0 {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s' is not a positive integer", OptionWithMaxMsgSize, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
		d.dialOpts.maxMsgSize = size
		delete(cnOptions, OptionWithMaxMsgSize)
	}
	d.dialOpts.rebuild()

	if val, ok := cnOptions[OptionCookieMiddleware]; ok {
		switch val {
		case adbc.OptionValueEnabled:
			d.enableCookies = true
		case adbc.OptionValueDisabled:
			d.enableCookies = false
		default:
			return d.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "Invalid value for database option '%s': '%s'", OptionCookieMiddleware, val)
		}
		delete(cnOptions, OptionCookieMiddleware)
	}

	for key, val := range cnOptions {
		if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
			d.hdrs.Append(strings.TrimPrefix(key, OptionRPCCallHeaderPrefix), val)
			continue
		}
		return d.ErrorHelper.Errorf(adbc.StatusInvalidArgument, "[Flight SQL] Unknown database option '%s'", key)
	}

	return nil
}

func (d *databaseImpl) GetOption(key string) (string, error) {
	switch key {
	case OptionTimeoutFetch:
		return d.timeout.fetchTimeout.String(), nil
	case OptionTimeoutQuery:
		return d.timeout.queryTimeout.String(), nil
	case OptionTimeoutUpdate:
		return d.timeout.updateTimeout.String(), nil
	case OptionTimeoutConnect:
		return d.timeout.connectTimeout.String(), nil
	case adbc.OptionKeyTelemetryTracesExporter:
		return d.tracesExporter, nil
	case adbc.OptionKeyTelemetryTracesFolderPath:
		return d.tracesFolderPath, nil
	}
	if val, ok := d.options[key]; ok {
		return val, nil
	}
	return d.DatabaseImplBase.GetOption(key)
}

func (d *databaseImpl) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		fallthrough
	case OptionTimeoutConnect:
		val, err := d.GetOptionDouble(key)
		if err != nil {
			return 0, err
		}
		return int64(val), nil
	}

	return d.DatabaseImplBase.GetOptionInt(key)
}

func (d *databaseImpl) GetOptionDouble(key string) (float64, error) {
	switch key {
	case OptionTimeoutFetch:
		return d.timeout.fetchTimeout.Seconds(), nil
	case OptionTimeoutQuery:
		return d.timeout.queryTimeout.Seconds(), nil
	case OptionTimeoutUpdate:
		return d.timeout.updateTimeout.Seconds(), nil
	case OptionTimeoutConnect:
		return d.timeout.connectTimeout.Seconds(), nil
	}

	return d.DatabaseImplBase.GetOptionDouble(key)
}

func (d *databaseImpl) SetOption(key, value string) error {
	// We can't change most options post-init
	switch key {
	case OptionTimeoutFetch, OptionTimeoutQuery, OptionTimeoutUpdate, OptionTimeoutConnect:
		return d.timeout.setTimeoutString(key, value)
	case adbc.OptionKeyTelemetryTracesExporter:
		// The OpenTelemetry tracer is built during NewDatabase*, so any
		// value supplied through SetOption after that point cannot
		// retroactively rewire the exporter. Surface a precise error
		// rather than silently dropping the change, but still record the
		// new value so GetOption can echo it back to introspection
		// tooling that immediately follows up with a Get.
		d.tracesExporter = value
		return adbc.Error{
			Msg:  fmt.Sprintf("Option '%s' must be set at database construction time; tracer is already initialized", key),
			Code: adbc.StatusInvalidState,
		}
	case adbc.OptionKeyTelemetryTracesFolderPath:
		// The on-disk file writer behind the "adbcfile" exporter is
		// created during NewDatabase*; once it is running its target
		// folder cannot be changed without tearing the writer down. The
		// new value is recorded for GetOption echo-back, mirroring the
		// behavior of OptionKeyTelemetryTracesExporter above.
		d.tracesFolderPath = value
		return adbc.Error{
			Msg:  fmt.Sprintf("Option '%s' must be set at database construction time; trace file writer is already initialized", key),
			Code: adbc.StatusInvalidState,
		}
	}
	if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
		d.hdrs.Set(strings.TrimPrefix(key, OptionRPCCallHeaderPrefix), value)
	}
	return d.DatabaseImplBase.SetOption(key, value)
}

func (d *databaseImpl) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		fallthrough
	case OptionTimeoutConnect:
		return d.timeout.setTimeout(key, float64(value))
	}

	return d.DatabaseImplBase.SetOptionInt(key, value)
}

func (d *databaseImpl) SetOptionDouble(key string, value float64) error {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		fallthrough
	case OptionTimeoutConnect:
		return d.timeout.setTimeout(key, value)
	}

	return d.DatabaseImplBase.SetOptionDouble(key, value)
}

func (d *databaseImpl) Close() error {
	// Emit a structured log line on close so that connection / database
	// lifetime events can be reconstructed end-to-end from the driver's
	// log stream alone. This is a no-op release otherwise; resources
	// owned by individual connections are released by connectionImpl.Close.
	if d.Logger != nil {
		d.Logger.Info("FlightSQL database closed",
			"target", d.uri.String(),
		)
	}
	return nil
}

func getFlightClient(ctx context.Context, loc string, d *databaseImpl, authMiddle *bearerAuthMiddleware, cookies flight.CookieMiddleware) (*flightsql.Client, error) {
	middleware := []flight.ClientMiddleware{
		{
			Unary:  makeUnaryLoggingInterceptor(d.Logger),
			Stream: makeStreamLoggingInterceptor(d.Logger),
		},
		flight.CreateClientMiddleware(authMiddle),
		{
			Unary:  unaryTimeoutInterceptor,
			Stream: streamTimeoutInterceptor,
		},
	}

	if d.enableCookies {
		middleware = append(middleware, flight.CreateClientMiddleware(cookies))
	}

	uri, err := url.Parse(loc)
	if err != nil {
		return nil, adbc.Error{Msg: fmt.Sprintf("Invalid URI '%s': %s", loc, err), Code: adbc.StatusInvalidArgument}
	}
	creds := d.creds

	target := uri.Host
	switch uri.Scheme {
	case "grpc", "grpc+tcp":
		creds = insecure.NewCredentials()
	case "grpc+unix":
		creds = insecure.NewCredentials()
		target = "unix:" + uri.Path
	}

	dv, _ := d.DriverInfo.GetInfoForInfoCode(adbc.InfoDriverVersion)
	driverVersion := dv.(string)
	dialOpts := append(d.dialOpts.opts, grpc.WithConnectParams(d.timeout.connectParams()), grpc.WithTransportCredentials(creds), grpc.WithUserAgent("ADBC Flight SQL Driver "+driverVersion))
	dialOpts = append(dialOpts, d.userDialOpts...)

	if d.oauthToken != nil {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(d.oauthToken))
	}

	d.Logger.DebugContext(ctx, "new client", "location", loc)
	cl, err := flightsql.NewClient(target, nil, middleware, dialOpts...)
	if err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusIO,
		}
	}

	cl.Alloc = d.Alloc
	// Authorization header is already set, continue
	if len(authMiddle.hdrs.Get("authorization")) > 0 {
		d.Logger.DebugContext(ctx, "reusing auth token", "location", loc)
		return cl, nil
	}

	var authValue string

	if d.user != "" || d.pass != "" {
		// Emit start/finish logs around basic auth so an operator can
		// distinguish "the driver never tried to authenticate" from
		// "authentication started but the server never responded" — the
		// two have very different root causes (option not threaded vs
		// network/TLS/server fault).
		authStart := time.Now()
		d.Logger.InfoContext(ctx, "FlightSQL basic auth started",
			"target", loc,
			"user", d.user,
		)
		var header, trailer metadata.MD
		ctx, err = cl.Client.AuthenticateBasicToken(ctx, d.user, d.pass, grpc.Header(&header), grpc.Trailer(&trailer), d.timeout)
		if err != nil {
			args := []any{
				"target", loc,
				"user", d.user,
				"duration", time.Since(authStart),
				"err", err,
			}
			args = append(args, correlationHeaderAttrs(header)...)
			args = append(args, correlationHeaderAttrs(trailer)...)
			args = append(args, grpcStatusAttrs(err)...)
			d.Logger.InfoContext(ctx, "FlightSQL basic auth failed", args...)
			return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "AuthenticateBasicToken")
		}

		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			authValue = md.Get("Authorization")[0]
		}

		// Log only the fact + token length, never the token value. The
		// length is useful for quick sanity checks (e.g. a zero-length
		// token would indicate the server returned an empty
		// Authorization header even though AuthenticateBasicToken
		// returned no error).
		d.Logger.InfoContext(ctx, "FlightSQL basic auth succeeded",
			"target", loc,
			"user", d.user,
			"duration", time.Since(authStart),
			"token_length", len(authValue),
		)
	}

	if authValue != "" {
		authMiddle.SetHeader(authValue)
	}

	return cl, nil
}

type support struct {
	transactions bool
}

func (d *databaseImpl) Open(ctx context.Context) (adbc.Connection, error) {
	authMiddle := &bearerAuthMiddleware{hdrs: d.hdrs.Copy(), logger: safeLogger(d.Logger)}
	var cookies flight.CookieMiddleware
	if d.enableCookies {
		cookies = flight.NewCookieMiddleware()
	}

	cl, err := getFlightClient(ctx, d.uri.String(), d, authMiddle, cookies)
	if err != nil {
		return nil, err
	}

	cache := gcache.New(20).LRU().
		Expiration(5 * time.Minute).
		LoaderFunc(func(loc interface{}) (interface{}, error) {
			uri, ok := loc.(string)
			if !ok {
				return nil, adbc.Error{Msg: fmt.Sprintf("Location must be a string, got %#v",
					uri), Code: adbc.StatusInternal}
			}

			var cookieMiddleware flight.CookieMiddleware
			// if cookies are enabled, start by cloning the existing cookies
			if d.enableCookies {
				cookieMiddleware = cookies.Clone()
			}
			// use the existing auth token if there is one
			cl, err := getFlightClient(context.Background(), uri, d,
				&bearerAuthMiddleware{hdrs: authMiddle.hdrs.Copy(), logger: safeLogger(d.Logger)}, cookieMiddleware)
			if err != nil {
				return nil, err
			}

			cl.Alloc = d.Alloc
			return cl, nil
		}).
		EvictedFunc(func(_, client interface{}) {
			conn := client.(*flightsql.Client)
			err := conn.Close()
			if err != nil {
				d.Logger.Debug("failed to close client", "error", err.Error())
			}
		}).PurgeVisitorFunc(func(_ interface{}, client interface{}) {
		conn := client.(*flightsql.Client)
		err := conn.Close()
		if err != nil {
			d.Logger.Debug("failed to close client", "error", err.Error())
		}
	}).Build()

	var cnxnSupport support

	// Capture the resolved gRPC peer address from the first call so the
	// connection can stamp it onto every subsequent log line (and so an
	// operator can see at a glance which backend the gRPC name resolver
	// selected, which frequently differs from the user-supplied URI when
	// an L7 proxy is in front of the server). The peer field is read
	// after the call returns regardless of success — even a failed call
	// usually has a resolved peer attached at that point — but is only
	// logged when it is non-empty.
	var sqlInfoPeer peer.Peer
	info, err := cl.GetSqlInfo(ctx, []flightsql.SqlInfo{flightsql.SqlInfoFlightSqlServerTransaction}, d.timeout, grpc.Peer(&sqlInfoPeer))
	// ignore this if it fails
	if err == nil {
		const int32code = 3

		for _, endpoint := range info.Endpoint {
			rdr, err := doGetWithLogger(ctx, cl, endpoint, cache, d.Logger, d.timeout)
			if err != nil {
				continue
			}
			defer rdr.Release()

			for rdr.Next() {
				rec := rdr.RecordBatch()
				codes := rec.Column(0).(*array.Uint32)
				values := rec.Column(1).(*array.DenseUnion)
				int32Value := values.Field(int32code).(*array.Int32)

				for i := 0; i < int(rec.NumRows()); i++ {
					switch codes.Value(i) {
					case uint32(flightsql.SqlInfoFlightSqlServerTransaction):
						if values.TypeCode(i) != int32code {
							continue
						}

						idx := values.ValueOffset(i)
						if !int32Value.IsValid(int(idx)) {
							continue
						}

						value := int32Value.Value(int(idx))
						cnxnSupport.transactions =
							value == int32(flightsql.SqlTransactionTransaction) ||
								value == int32(flightsql.SqlTransactionSavepoint)
					}
				}
			}
		}
	}

	conn := &connectionImpl{
		cl: cl, db: d, clientCache: cache,
		hdrs: make(metadata.MD), timeouts: d.timeout, supportInfo: cnxnSupport,
		ConnectionImplBase: driverbase.NewConnectionImplBase(&d.DatabaseImplBase),
	}
	// Mint a stable per-connection identifier and wrap the inherited
	// logger so that every subsequent log line (and every statement
	// created through this connection) carries connection_id. This is the
	// primary correlation hook between FlightSQL driver logs and any host
	// application (such as Power Query / Mashup) that records its own
	// per-connection identifier.
	conn.id = newRandomID("conn")
	conn.openedAt = time.Now()
	conn.Logger = safeLogger(conn.Logger).With("connection_id", conn.id)
	// Record + emit the resolved gRPC peer address. This runs even when
	// GetSqlInfo failed because gRPC populates Peer on the call context
	// before the RPC body executes; capturing it here means an operator
	// can answer "which backend was the driver actually talking to?"
	// from a single log line without enabling Debug logging.
	conn.recordPeer(&sqlInfoPeer)
	conn.Logger.InfoContext(ctx, "FlightSQL connection opened",
		"target", d.uri.String(),
		"peer_addr", conn.peerAddr,
		"transactionsSupported", cnxnSupport.transactions,
		"driver", infoDriverName,
	)

	return driverbase.NewConnectionBuilder(conn).
		WithDriverInfoPreparer(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		Connection(), nil
}

type bearerAuthMiddleware struct {
	mutex sync.RWMutex
	hdrs  metadata.MD
	// logger, when non-nil, receives an Info-level event every time the
	// bearer token is rotated (either because the server returned a new
	// Authorization header on a response, or because the driver itself
	// explicitly set one via SetHeader). The token value is never
	// logged; only the fact of the rotation plus the new token length
	// is recorded so an operator can confirm that token-refresh logic
	// is firing without exposing credential material in the log stream.
	logger *slog.Logger
}

func (b *bearerAuthMiddleware) StartCall(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return metadata.NewOutgoingContext(ctx, metadata.Join(md, b.hdrs))
}

// rotateAuth atomically replaces the stored Authorization metadata and
// returns the previous value plus a snapshot of the logger pointer.
// Splitting this out lets HeadersReceived and SetHeader manage the
// mutex with defer-style discipline while still performing the logger
// call outside of the critical section: log handlers are user-supplied
// and may be slow, so holding b.mutex across them would penalize every
// concurrent StartCall reader.
func (b *bearerAuthMiddleware) rotateAuth(headers ...string) (previous []string, logger *slog.Logger) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	previous = b.hdrs.Get("authorization")
	b.hdrs.Set("authorization", headers...)
	return previous, b.logger
}

func (b *bearerAuthMiddleware) HeadersReceived(ctx context.Context, md metadata.MD) {
	// apache/arrow-adbc#584
	headers := md.Get("authorization")
	if len(headers) == 0 {
		return
	}
	previous, logger := b.rotateAuth(headers...)
	if logger == nil {
		return
	}
	// Compare lengths rather than values so that we never touch the
	// token contents in the log path. Equal lengths can still indicate
	// a fresh token (a server might issue tokens of the same shape),
	// but for the no-op case (server echoed the same header) the
	// reflected length is what an operator wants to see anyway.
	var prevLen int
	if len(previous) > 0 {
		prevLen = len(previous[0])
	}
	logger.InfoContext(ctx, "FlightSQL bearer token rotated by server",
		"previous_token_length", prevLen,
		"new_token_length", len(headers[0]),
		"source", "HeadersReceived",
	)
}

func (b *bearerAuthMiddleware) SetHeader(authValue string) {
	previous, logger := b.rotateAuth(authValue)
	if logger == nil {
		return
	}
	var prevLen int
	if len(previous) > 0 {
		prevLen = len(previous[0])
	}
	logger.Info("FlightSQL bearer token rotated by client",
		"previous_token_length", prevLen,
		"new_token_length", len(authValue),
		"source", "SetHeader",
	)
}
