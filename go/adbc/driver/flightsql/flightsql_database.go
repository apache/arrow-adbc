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
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/bluele/gcache"
	"go.opentelemetry.io/contrib/instrumentation/google.golang.org/grpc/otelgrpc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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

func (d *databaseImpl) Close() (err error) {
	const spanName = "FlightSQL.Database.Close"
	startTime := time.Now()
	var span trace.Span
	_, span = internal.StartSpan(context.Background(), spanName, d)

	span.AddEvent("closing", trace.WithAttributes(attribute.String("target", d.uri.String())))
	return closeTracing(context.Background(), &d.DatabaseImplBase, func(flushErr error) {
		internal.NewEndSpanHelper(span).
			WithError(flushErr).
			WithStartTime(startTime).
			EndSpan()
	})
}

type tracingLifecycle interface {
	ForceFlushTracing(context.Context) error
	Close() error
}

func closeTracing(ctx context.Context, lifecycle tracingLifecycle, finishSpan func(error)) error {
	flushErr := lifecycle.ForceFlushTracing(ctx)
	finishSpan(flushErr)
	shutdownErr := lifecycle.Close()
	return errors.Join(flushErr, shutdownErr)
}

func getFlightClient(ctx context.Context, loc string, d *databaseImpl, authMiddle *bearerAuthMiddleware, cookies flight.CookieMiddleware, span trace.Span) (client *flightsql.Client, err error) {
	middleware := []flight.ClientMiddleware{
		flight.CreateClientMiddleware(authMiddle),
		{
			Unary:  unaryTimeoutInterceptor,
			Stream: streamTimeoutInterceptor,
		},
		{Stream: responseMetadataStreamInterceptor},
	}

	if d.enableCookies {
		middleware = append(middleware, flight.CreateClientMiddleware(cookies))
	}

	var uri *url.URL
	uri, err = url.Parse(loc)
	if err != nil {
		err = adbc.Error{Msg: fmt.Sprintf("Invalid URI '%s': %s", loc, err), Code: adbc.StatusInvalidArgument}
		return nil, err
	}
	creds := d.creds

	target := uri.Host
	switch uri.Scheme {
	case "grpc", "grpc+tcp":
		creds = insecure.NewCredentials()
	case "grpc+unix":
		creds = insecure.NewCredentials()
		target = "unix:" + uri.Path
	case "flightsql":
		transport := strings.ToLower(uri.Query().Get("transport"))
		switch transport {
		case "", "tls":
			if uri.Path != "" {
				return nil, adbc.Error{Msg: fmt.Sprintf("Invalid URI '%s': a socket path is only valid with transport=unix", loc), Code: adbc.StatusInvalidArgument}
			}
		case "tcp":
			if uri.Path != "" {
				return nil, adbc.Error{Msg: fmt.Sprintf("Invalid URI '%s': a socket path is only valid with transport=unix", loc), Code: adbc.StatusInvalidArgument}
			}
			creds = insecure.NewCredentials()
		case "unix":
			if uri.Host != "" {
				return nil, adbc.Error{Msg: fmt.Sprintf("Invalid URI '%s': a host is not valid with transport=unix", loc), Code: adbc.StatusInvalidArgument}
			}
			if uri.Path == "" {
				return nil, adbc.Error{Msg: fmt.Sprintf("Invalid URI '%s': transport=unix requires a socket path", loc), Code: adbc.StatusInvalidArgument}
			}
			creds = insecure.NewCredentials()
			target = "unix:" + uri.Path
		default:
			return nil, adbc.Error{Msg: fmt.Sprintf("Invalid URI '%s': unrecognized transport '%s' (expected 'tls', 'tcp', or 'unix')", loc, transport), Code: adbc.StatusInvalidArgument}
		}
	}

	dv, _ := d.DriverInfo.GetInfoForInfoCode(adbc.InfoDriverVersion)
	driverVersion := dv.(string)
	dialOpts := append(d.dialOpts.opts,
		grpc.WithStatsHandler(otelgrpc.NewClientHandler(
			otelgrpc.WithTracerProvider(d.GetTracerProvider()),
		)),
		grpc.WithConnectParams(d.timeout.connectParams()),
		grpc.WithTransportCredentials(creds),
		grpc.WithUserAgent("ADBC Flight SQL Driver "+driverVersion),
	)
	dialOpts = append(dialOpts, d.userDialOpts...)

	if d.oauthToken != nil {
		dialOpts = append(dialOpts, grpc.WithPerRPCCredentials(d.oauthToken))
	}

	span.AddEvent("flight.client.connecting", trace.WithAttributes(
		attribute.String("flight.location", loc),
	))
	client, err = flightsql.NewClient(target, nil, middleware, dialOpts...)
	if err != nil {
		err = adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusIO,
		}
		return nil, err
	}

	client.Alloc = d.Alloc
	// Authorization header is already set, continue
	if len(authMiddle.hdrs.Get("authorization")) > 0 {
		span.AddEvent("flight.auth.token.reused", trace.WithAttributes(
			attribute.String("flight.location", loc),
		))
		return client, nil
	}

	var authValue string

	if d.user != "" || d.pass != "" {
		authStart := time.Now()
		span.AddEvent("flight.auth.basic.started", trace.WithAttributes(
			attribute.String("target", loc),
			attribute.String("user", d.user),
		))
		var header, trailer metadata.MD
		ctx, err = client.Client.AuthenticateBasicToken(ctx, d.user, d.pass, grpc.Header(&header), grpc.Trailer(&trailer), d.timeout)
		if err != nil {
			args := []attribute.KeyValue{
				attribute.String("target", loc),
				attribute.String("user", d.user),
				attribute.Float64("duration_s", time.Since(authStart).Seconds()),
			}
			args = append(args, correlationHeaderKeyValues(header)...)
			args = append(args, correlationHeaderKeyValues(trailer)...)
			args = append(args, grpcStatusKeyValues(err)...)
			span.SetAttributes(args...)
			err = adbcFromFlightStatusWithDetails(err, header, trailer, "AuthenticateBasicToken")
			return nil, err
		}

		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			authValue = md.Get("Authorization")[0]
		}

		span.AddEvent("flight.auth.basic.completed", trace.WithAttributes(
			attribute.String("target", loc),
			attribute.String("user", d.user),
			attribute.String("duration_s", time.Since(authStart).String()),
			attribute.Int("token_length", len(authValue)),
		))
	}

	if authValue != "" {
		authMiddle.SetHeader(authValue, span)
	}

	return client, nil
}

type support struct {
	transactions bool
}

func closeCachedFlightClient(d *databaseImpl, location, client interface{}, reason string) {
	startTime := time.Now()
	var err error
	_, span := internal.StartSpan(context.Background(), "FlightSQL.Database.CloseCachedClient", d,
		trace.WithAttributes(
			attribute.String("flight.location", fmt.Sprint(location)),
			attribute.String("flight.cache.reason", reason),
		))
	defer func() {
		internal.NewEndSpanHelper(span).
			WithError(err).
			WithStartTime(startTime).
			EndSpan()
	}()

	err = client.(*flightsql.Client).Close()
}

func (d *databaseImpl) Open(ctx context.Context) (_ adbc.Connection, err error) {
	ctx, span := internal.StartSpan(
		ctx,
		"FlightSQL.Database.Open",
		d,
		trace.WithAttributes(traceHeaderAttrsWithPrefix(d.hdrs, traceRequestMetadataPrefix)...),
	)
	defer func() {
		internal.NewEndSpanHelper(span).
			WithError(err).
			EndSpan()
	}()

	authMiddle := &bearerAuthMiddleware{
		hdrs: d.hdrs.Copy(),
	}
	var cookies flight.CookieMiddleware
	if d.enableCookies {
		cookies = flight.NewCookieMiddleware()
	}

	cl, err := getFlightClient(ctx, d.uri.String(), d, authMiddle, cookies, span)
	if err != nil {
		return nil, err
	}

	cache := gcache.New(20).LRU().
		Expiration(5 * time.Minute).
		LoaderFunc(func(loc interface{}) (_ interface{}, err error) {
			startTime := time.Now()
			ctx, cacheSpan := internal.StartSpan(context.Background(), "FlightSQL.Database.LoadCachedClient", d)
			defer func() {
				internal.NewEndSpanHelper(cacheSpan).
					WithError(err).
					WithStartTime(startTime).
					EndSpan()
			}()

			uri, ok := loc.(string)
			if !ok {
				return nil, adbc.Error{Msg: fmt.Sprintf("Location must be a string, got %#v",
					uri), Code: adbc.StatusInternal}
			}
			cacheSpan.SetAttributes(attribute.String("flight.location", uri))

			var cookieMiddleware flight.CookieMiddleware
			// if cookies are enabled, start by cloning the existing cookies
			if d.enableCookies {
				cookieMiddleware = cookies.Clone()
			}
			// use the existing auth token if there is one
			cl, err := getFlightClient(
				ctx,
				uri,
				d,
				&bearerAuthMiddleware{hdrs: authMiddle.hdrs.Copy()},
				cookieMiddleware,
				cacheSpan)
			if err != nil {
				return nil, err
			}

			cl.Alloc = d.Alloc
			return cl, nil
		}).
		EvictedFunc(func(location, client interface{}) {
			closeCachedFlightClient(d, location, client, "evicted")
		}).PurgeVisitorFunc(func(location, client interface{}) {
		closeCachedFlightClient(d, location, client, "purged")
	}).Build()

	var cnxnSupport support

	info, err := cl.GetSqlInfo(ctx, []flightsql.SqlInfo{flightsql.SqlInfoFlightSqlServerTransaction}, d.timeout)
	// ignore this if it fails
	if err == nil {
		const int32code = 3

		for _, endpoint := range info.Endpoint {
			rdr, err := doGetWithTracer(ctx, cl, endpoint, cache, d, d.timeout)
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
	// Stamp a stable per-connection ID onto every log line emitted by
	// this connection (and any statements derived from it).
	conn.id = newRandomID("conn")
	conn.openedAt = time.Now()
	span.AddEvent("finished", trace.WithAttributes(
		attribute.String("target", d.uri.String()),
		attribute.Bool("transactionsSupported", cnxnSupport.transactions),
		attribute.String("driver", infoDriverName),
	))

	return driverbase.NewConnectionBuilder(conn).
		WithDriverInfoPreparer(conn).
		WithAutocommitSetter(conn).
		WithCurrentNamespacer(conn).
		Connection(), nil
}

type bearerAuthMiddleware struct {
	mutex sync.RWMutex
	hdrs  metadata.MD
}

func (b *bearerAuthMiddleware) StartCall(ctx context.Context) context.Context {
	md, _ := metadata.FromOutgoingContext(ctx)
	b.mutex.RLock()
	defer b.mutex.RUnlock()
	return metadata.NewOutgoingContext(ctx, metadata.Join(md, b.hdrs))
}

// rotateAuth atomically replaces the stored Authorization metadata and
// returns the previous value.
func (b *bearerAuthMiddleware) rotateAuth(headers ...string) (previous []string) {
	b.mutex.Lock()
	defer b.mutex.Unlock()
	previous = b.hdrs.Get("authorization")
	b.hdrs.Set("authorization", headers...)
	return previous
}

func (b *bearerAuthMiddleware) HeadersReceived(ctx context.Context, md metadata.MD) {
	captureResponseMetadata(ctx, md)
	// apache/arrow-adbc#584
	headers := md.Get("authorization")
	if len(headers) == 0 {
		return
	}
	previous := b.rotateAuth(headers...)
	// Log lengths, never values, so credentials never reach the log path.
	var prevLen int
	if len(previous) > 0 {
		prevLen = len(previous[0])
	}
	if span := trace.SpanFromContext(ctx); span != nil && span.IsRecording() {
		span.AddEvent("auth_token.rotated_by_server", trace.WithAttributes(
			attribute.Int("previous_token_length", prevLen),
			attribute.Int("new_token_length", len(headers[0])),
			attribute.String("source", "HeadersReceived"),
		))
	}
}

func (b *bearerAuthMiddleware) SetHeader(authValue string, span trace.Span) {
	previous := b.rotateAuth(authValue)
	var prevLen int
	if len(previous) > 0 {
		prevLen = len(previous[0])
	}
	span.AddEvent("auth_token.rotated_by_client", trace.WithAttributes(
		attribute.Int("previous_token_length", prevLen),
		attribute.Int("new_token_length", len(authValue)),
		attribute.String("source", "SetHeader"),
	))
}
