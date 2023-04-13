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

// Package flightsql is an ADBC Driver Implementation for Flight SQL
// natively in go.
//
// It can be used to register a driver for database/sql by importing
// github.com/apache/arrow-adbc/go/adbc/sqldriver and running:
//
//	sql.Register("flightsql", sqldriver.Driver{flightsql.Driver{}})
//
// You can then open a flightsql connection with the database/sql
// standard package by using:
//
//	db, err := sql.Open("flightsql", "uri=<flight sql db url>")
//
// The URI passed *must* contain a scheme, most likely "grpc+tcp://"
package flightsql

import (
	"bytes"
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io"
	"math"
	"net/url"
	"runtime/debug"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow/go/v12/arrow/ipc"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/bluele/gcache"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

const (
	OptionMTLSCertChain       = "adbc.flight.sql.client_option.mtls_cert_chain"
	OptionMTLSPrivateKey      = "adbc.flight.sql.client_option.mtls_private_key"
	OptionSSLOverrideHostname = "adbc.flight.sql.client_option.tls_override_hostname"
	OptionSSLSkipVerify       = "adbc.flight.sql.client_option.tls_skip_verify"
	OptionSSLRootCerts        = "adbc.flight.sql.client_option.tls_root_certs"
	OptionWithBlock           = "adbc.flight.sql.client_option.with_block"
	OptionWithMaxMsgSize      = "adbc.flight.sql.client_option.with_max_msg_size"
	OptionAuthorizationHeader = "adbc.flight.sql.authorization_header"
	OptionTimeoutFetch        = "adbc.flight.sql.rpc.timeout_seconds.fetch"
	OptionTimeoutQuery        = "adbc.flight.sql.rpc.timeout_seconds.query"
	OptionTimeoutUpdate       = "adbc.flight.sql.rpc.timeout_seconds.update"
	OptionRPCCallHeaderPrefix = "adbc.flight.sql.rpc.call_header."
	infoDriverName            = "ADBC Flight SQL Driver - Go"
)

var (
	infoDriverVersion      string
	infoDriverArrowVersion string
	infoSupportedCodes     []adbc.InfoCode
)

var errNoTransactionSupport = adbc.Error{
	Msg:  "[Flight SQL] server does not report transaction support",
	Code: adbc.StatusNotImplemented,
}

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/apache/arrow-adbc/go/adbc/driver/flightsql":
				infoDriverVersion = dep.Version
			case strings.HasPrefix(dep.Path, "github.com/apache/arrow/go/"):
				infoDriverArrowVersion = dep.Version
			}
		}
	}
	// XXX: Deps not populated in tests
	// https://github.com/golang/go/issues/33976
	if infoDriverVersion == "" {
		infoDriverVersion = "(unknown or development build)"
	}
	if infoDriverArrowVersion == "" {
		infoDriverArrowVersion = "(unknown or development build)"
	}

	infoSupportedCodes = []adbc.InfoCode{
		adbc.InfoDriverName,
		adbc.InfoDriverVersion,
		adbc.InfoDriverArrowVersion,
		adbc.InfoVendorName,
		adbc.InfoVendorVersion,
		adbc.InfoVendorArrowVersion,
	}
}

func getTimeoutOptionValue(v string) (time.Duration, error) {
	timeout, err := strconv.ParseFloat(v, 64)
	if math.IsNaN(timeout) || math.IsInf(timeout, 0) || timeout < 0 {
		return 0, errors.New("timeout must be positive and finite")
	}
	return time.Duration(timeout * float64(time.Second)), err
}

type Driver struct {
	Alloc memory.Allocator
}

func (d Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	opts = maps.Clone(opts)
	uri, ok := opts[adbc.OptionKeyURI]
	if !ok {
		return nil, adbc.Error{
			Msg:  "URI required for a FlightSQL DB",
			Code: adbc.StatusInvalidArgument,
		}
	}
	delete(opts, adbc.OptionKeyURI)

	db := &database{alloc: d.Alloc, hdrs: make(metadata.MD)}
	if db.alloc == nil {
		db.alloc = memory.DefaultAllocator
	}

	var err error
	if db.uri, err = url.Parse(uri); err != nil {
		return nil, adbc.Error{Msg: err.Error(), Code: adbc.StatusInvalidArgument}
	}

	// Do not set WithBlock since it converts some types of connection
	// errors to infinite hangs
	// Use WithMaxMsgSize(16 MiB) since Flight services tend to send large messages
	db.dialOpts.block = false
	db.dialOpts.maxMsgSize = 16 * 1024 * 1024

	return db, db.SetOptions(opts)
}

type dbDialOpts struct {
	opts       []grpc.DialOption
	block      bool
	maxMsgSize int
}

func (d *dbDialOpts) rebuild() {
	d.opts = []grpc.DialOption{
		grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(d.maxMsgSize),
			grpc.MaxCallSendMsgSize(d.maxMsgSize)),
		grpc.WithUserAgent("ADBC Flight SQL Driver " + infoDriverVersion),
	}
	if d.block {
		d.opts = append(d.opts, grpc.WithBlock())
	}
}

type database struct {
	uri        *url.URL
	creds      credentials.TransportCredentials
	user, pass string
	hdrs       metadata.MD
	timeout    timeoutOption
	dialOpts   dbDialOpts

	alloc memory.Allocator
}

func (d *database) SetOptions(cnOptions map[string]string) error {
	var tlsConfig tls.Config

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
		if val == adbc.OptionValueEnabled {
			tlsConfig.InsecureSkipVerify = true
		} else if val == adbc.OptionValueDisabled {
			tlsConfig.InsecureSkipVerify = false
		} else {
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

	if u, ok := cnOptions[adbc.OptionKeyUsername]; ok {
		if d.hdrs.Len() > 0 {
			return adbc.Error{
				Msg:  "Authorization header already provided, do not provide user/pass also",
				Code: adbc.StatusInvalidArgument,
			}
		}
		d.user = u
		delete(cnOptions, adbc.OptionKeyUsername)
	}

	if p, ok := cnOptions[adbc.OptionKeyPassword]; ok {
		if d.hdrs.Len() > 0 {
			return adbc.Error{
				Msg:  "Authorization header already provided, do not provide user/pass also",
				Code: adbc.StatusInvalidArgument,
			}
		}
		d.pass = p
		delete(cnOptions, adbc.OptionKeyPassword)
	}

	var err error
	if tv, ok := cnOptions[OptionTimeoutFetch]; ok {
		if d.timeout.fetchTimeout, err = getTimeoutOptionValue(tv); err != nil {
			return adbc.Error{
				Msg: fmt.Sprintf("invalid timeout option value %s = %s : %s",
					OptionTimeoutFetch, tv, err.Error()),
				Code: adbc.StatusInvalidArgument,
			}
		}
	}

	if tv, ok := cnOptions[OptionTimeoutQuery]; ok {
		if d.timeout.queryTimeout, err = getTimeoutOptionValue(tv); err != nil {
			return adbc.Error{
				Msg: fmt.Sprintf("invalid timeout option value %s = %s : %s",
					OptionTimeoutQuery, tv, err.Error()),
				Code: adbc.StatusInvalidArgument,
			}
		}
	}

	if tv, ok := cnOptions[OptionTimeoutUpdate]; ok {
		if d.timeout.updateTimeout, err = getTimeoutOptionValue(tv); err != nil {
			return adbc.Error{
				Msg: fmt.Sprintf("invalid timeout option value %s = %s : %s",
					OptionTimeoutUpdate, tv, err.Error()),
				Code: adbc.StatusInvalidArgument,
			}
		}
	}

	if val, ok := cnOptions[OptionWithBlock]; ok {
		if val == adbc.OptionValueEnabled {
			d.dialOpts.block = true
		} else if val == adbc.OptionValueDisabled {
			d.dialOpts.block = false
		} else {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionWithBlock, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
		delete(cnOptions, OptionWithBlock)
	}
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

	for key, val := range cnOptions {
		if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
			d.hdrs.Append(strings.TrimPrefix(key, OptionRPCCallHeaderPrefix), val)
			continue
		}
		return adbc.Error{
			Msg:  fmt.Sprintf("Unknown database option '%s'", key),
			Code: adbc.StatusInvalidArgument,
		}
	}

	return nil
}

type timeoutOption struct {
	grpc.EmptyCallOption

	// timeout for DoGet requests
	fetchTimeout time.Duration
	// timeout for GetFlightInfo requests
	queryTimeout time.Duration
	// timeout for DoPut or DoAction requests
	updateTimeout time.Duration
}

func getTimeout(method string, callOptions []grpc.CallOption) (time.Duration, bool) {
	for _, opt := range callOptions {
		if to, ok := opt.(timeoutOption); ok {
			var tm time.Duration
			switch {
			case strings.HasSuffix(method, "DoGet"):
				tm = to.fetchTimeout
			case strings.HasSuffix(method, "GetFlightInfo"):
				tm = to.queryTimeout
			case strings.HasSuffix(method, "DoPut") || strings.HasSuffix(method, "DoAction"):
				tm = to.updateTimeout
			}

			return tm, tm > 0
		}
	}

	return 0, false
}

func unaryTimeoutInterceptor(ctx context.Context, method string, req, reply any, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	if tm, ok := getTimeout(method, opts); ok {
		ctx, cancel := context.WithTimeout(ctx, tm)
		defer cancel()
		return invoker(ctx, method, req, reply, cc, opts...)
	}

	return invoker(ctx, method, req, reply, cc, opts...)
}

type streamEventType int

const (
	receiveEndEvent streamEventType = iota
	errorEvent
)

type streamEvent struct {
	Type streamEventType
	Err  error
}

type wrappedClientStream struct {
	grpc.ClientStream

	desc       *grpc.StreamDesc
	events     chan streamEvent
	eventsDone chan struct{}
}

func (w *wrappedClientStream) RecvMsg(m any) error {
	err := w.ClientStream.RecvMsg(m)

	switch {
	case err == nil && !w.desc.ServerStreams:
		w.sendStreamEvent(receiveEndEvent, nil)
	case err == io.EOF:
		w.sendStreamEvent(receiveEndEvent, nil)
	case err != nil:
		w.sendStreamEvent(errorEvent, err)
	}

	return err
}

func (w *wrappedClientStream) SendMsg(m any) error {
	err := w.ClientStream.SendMsg(m)
	if err != nil {
		w.sendStreamEvent(errorEvent, err)
	}
	return err
}

func (w *wrappedClientStream) Header() (metadata.MD, error) {
	md, err := w.ClientStream.Header()
	if err != nil {
		w.sendStreamEvent(errorEvent, err)
	}
	return md, err
}

func (w *wrappedClientStream) CloseSend() error {
	err := w.ClientStream.CloseSend()
	if err != nil {
		w.sendStreamEvent(errorEvent, err)
	}
	return err
}

func (w *wrappedClientStream) sendStreamEvent(eventType streamEventType, err error) {
	select {
	case <-w.eventsDone:
	case w.events <- streamEvent{Type: eventType, Err: err}:
	}
}

func streamTimeoutInterceptor(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
	if tm, ok := getTimeout(method, opts); ok {
		ctx, cancel := context.WithTimeout(ctx, tm)
		s, err := streamer(ctx, desc, cc, method, opts...)
		if err != nil {
			defer cancel()
			return s, err
		}

		events, eventsDone := make(chan streamEvent), make(chan struct{})
		go func() {
			defer close(eventsDone)
			defer cancel()

			for {
				select {
				case event := <-events:
					// split by event type in case we want to add more logging
					// or even adding in some telemetry in the future.
					// Errors will already be propagated by the RecvMsg, SendMsg
					// methods.
					switch event.Type {
					case receiveEndEvent:
						return
					case errorEvent:
						return
					}
				case <-ctx.Done():
					return
				}
			}
		}()

		stream := &wrappedClientStream{
			ClientStream: s,
			desc:         desc,
			events:       events,
			eventsDone:   eventsDone,
		}
		return stream, nil
	}

	return streamer(ctx, desc, cc, method, opts...)
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

func (b *bearerAuthMiddleware) HeadersReceived(ctx context.Context, md metadata.MD) {
	// apache/arrow-adbc#584
	headers := md.Get("authorization")
	if len(headers) > 0 {
		b.mutex.Lock()
		defer b.mutex.Unlock()
		b.hdrs.Set("authorization", headers...)
	}
}

func getFlightClient(ctx context.Context, loc string, d *database) (*flightsql.Client, error) {
	authMiddle := &bearerAuthMiddleware{hdrs: d.hdrs.Copy()}
	middleware := []flight.ClientMiddleware{
		flight.CreateClientMiddleware(authMiddle),
		{
			Unary:  unaryTimeoutInterceptor,
			Stream: streamTimeoutInterceptor,
		},
	}

	uri, err := url.Parse(loc)
	if err != nil {
		return nil, adbc.Error{Msg: fmt.Sprintf("Invalid URI '%s': %s", loc, err), Code: adbc.StatusInvalidArgument}
	}
	creds := d.creds

	target := uri.Host
	if uri.Scheme == "grpc" || uri.Scheme == "grpc+tcp" {
		creds = insecure.NewCredentials()
	} else if uri.Scheme == "grpc+unix" {
		creds = insecure.NewCredentials()
		target = "unix:" + uri.Path
	}
	dialOpts := append(d.dialOpts.opts, grpc.WithTransportCredentials(creds))

	cl, err := flightsql.NewClient(target, nil, middleware, dialOpts...)
	if err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusIO,
		}
	}

	cl.Alloc = d.alloc
	if d.user != "" || d.pass != "" {
		ctx, err = cl.Client.AuthenticateBasicToken(ctx, d.user, d.pass)
		if err != nil {
			return nil, adbc.Error{
				Msg:  err.Error(),
				Code: adbc.StatusUnauthenticated,
			}
		}

		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			// No need to worry about lock here since we are sole owner
			authMiddle.hdrs.Set("authorization", md.Get("Authorization")[0])
		}
	}

	return cl, nil
}

type support struct {
	transactions bool
}

func (d *database) Open(ctx context.Context) (adbc.Connection, error) {
	cl, err := getFlightClient(ctx, d.uri.String(), d)
	if err != nil {
		return nil, err
	}

	cache := gcache.New(20).LRU().
		Expiration(5 * time.Minute).
		LoaderFunc(func(loc interface{}) (interface{}, error) {
			uri, ok := loc.(string)
			if !ok {
				return nil, adbc.Error{Msg: fmt.Sprintf("Location must be a string, got %#v", uri), Code: adbc.StatusInternal}
			}

			cl, err := getFlightClient(context.Background(), uri, d)
			if err != nil {
				return nil, err
			}

			cl.Alloc = d.alloc
			return cl, nil
		}).
		EvictedFunc(func(_, client interface{}) {
			conn := client.(*flightsql.Client)
			conn.Close()
		}).Build()

	var cnxnSupport support

	info, err := cl.GetSqlInfo(ctx, []flightsql.SqlInfo{flightsql.SqlInfoFlightSqlServerTransaction}, d.timeout)
	// ignore this if it fails
	if err == nil {
		const int32code = 3

		for _, endpoint := range info.Endpoint {
			rdr, err := doGet(ctx, cl, endpoint, cache, d.timeout)
			if err != nil {
				continue
			}
			defer rdr.Release()

			for rdr.Next() {
				rec := rdr.Record()
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

	return &cnxn{cl: cl, db: d, clientCache: cache,
		hdrs: make(metadata.MD), timeouts: d.timeout,
		supportInfo: cnxnSupport}, nil
}

type cnxn struct {
	cl *flightsql.Client

	db          *database
	clientCache gcache.Cache
	hdrs        metadata.MD
	timeouts    timeoutOption
	txn         *flightsql.Txn
	supportInfo support
}

var adbcToFlightSQLInfo = map[adbc.InfoCode]flightsql.SqlInfo{
	adbc.InfoVendorName:         flightsql.SqlInfoFlightSqlServerName,
	adbc.InfoVendorVersion:      flightsql.SqlInfoFlightSqlServerVersion,
	adbc.InfoVendorArrowVersion: flightsql.SqlInfoFlightSqlServerArrowVersion,
}

func doGet(ctx context.Context, cl *flightsql.Client, endpoint *flight.FlightEndpoint, clientCache gcache.Cache, opts ...grpc.CallOption) (rdr *flight.Reader, err error) {
	if len(endpoint.Location) == 0 {
		return cl.DoGet(ctx, endpoint.Ticket, opts...)
	}

	var (
		cc interface{}
	)

	for _, loc := range endpoint.Location {
		cc, err = clientCache.Get(loc.Uri)
		if err != nil {
			continue
		}

		conn := cc.(*flightsql.Client)
		rdr, err = conn.DoGet(ctx, endpoint.Ticket, opts...)
		if err != nil {
			continue
		}

		return
	}

	return nil, err
}

func (c *cnxn) SetOption(key, value string) error {
	if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
		name := strings.TrimPrefix(key, OptionRPCCallHeaderPrefix)
		if value == "" {
			c.hdrs.Delete(name)
		} else {
			c.hdrs.Append(name, value)
		}
		return nil
	}

	switch key {
	case OptionTimeoutFetch:
		timeout, err := getTimeoutOptionValue(value)
		if err != nil {
			return adbc.Error{
				Msg: fmt.Sprintf("invalid timeout option value %s = %s : %s",
					OptionTimeoutFetch, value, err.Error()),
				Code: adbc.StatusInvalidArgument,
			}
		}
		c.timeouts.fetchTimeout = timeout
	case OptionTimeoutQuery:
		timeout, err := getTimeoutOptionValue(value)
		if err != nil {
			return adbc.Error{
				Msg: fmt.Sprintf("invalid timeout option value %s = %s : %s",
					OptionTimeoutFetch, value, err.Error()),
				Code: adbc.StatusInvalidArgument,
			}
		}
		c.timeouts.queryTimeout = timeout
	case OptionTimeoutUpdate:
		timeout, err := getTimeoutOptionValue(value)
		if err != nil {
			return adbc.Error{
				Msg: fmt.Sprintf("invalid timeout option value %s = %s : %s",
					OptionTimeoutFetch, value, err.Error()),
				Code: adbc.StatusInvalidArgument,
			}
		}
		c.timeouts.updateTimeout = timeout
	case adbc.OptionKeyAutoCommit:
		autocommit := true
		switch value {
		case adbc.OptionValueEnabled:
		case adbc.OptionValueDisabled:
			autocommit = false
		default:
			return adbc.Error{
				Msg:  "[Flight SQL] invalid value for option " + key + ": " + value,
				Code: adbc.StatusInvalidArgument,
			}
		}

		if autocommit && c.txn == nil {
			// no-op don't even error if the server didn't support transactions
			return nil
		}

		if !c.supportInfo.transactions {
			return errNoTransactionSupport
		}

		ctx := metadata.NewOutgoingContext(context.Background(), c.hdrs)
		var err error
		if c.txn != nil {
			if err = c.txn.Commit(ctx, c.timeouts); err != nil {
				return adbc.Error{
					Msg:  "[Flight SQL] failed to update autocommit: " + err.Error(),
					Code: adbc.StatusIO,
				}
			}
		}

		if autocommit {
			c.txn = nil
			return nil
		}

		if c.txn, err = c.cl.BeginTransaction(ctx, c.timeouts); err != nil {
			return adbc.Error{
				Msg:  "[Flight SQL] failed to update autocommit: " + err.Error(),
				Code: adbc.StatusIO,
			}
		}
		return nil

	default:
		return adbc.Error{
			Msg:  "[Flight SQL] unknown connection option",
			Code: adbc.StatusNotImplemented,
		}
	}

	return nil
}

// GetInfo returns metadata about the database/driver.
//
// The result is an Arrow dataset with the following schema:
//
//	Field Name									| Field Type
//	----------------------------|-----------------------------
//	info_name					   				| uint32 not null
//	info_value									| INFO_SCHEMA
//
// INFO_SCHEMA is a dense union with members:
//
//	Field Name (Type Code)			| Field Type
//	----------------------------|-----------------------------
//	string_value (0)						| utf8
//	bool_value (1)							| bool
//	int64_value (2)							| int64
//	int32_bitmask (3)						| int32
//	string_list (4)							| list<utf8>
//	int32_to_int32_list_map (5)	| map<int32, list<int32>>
//
// Each metadatum is identified by an integer code. The recognized
// codes are defined as constants. Codes [0, 10_000) are reserved
// for ADBC usage. Drivers/vendors will ignore requests for unrecognized
// codes (the row will be omitted from the result).
func (c *cnxn) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	const strValTypeID arrow.UnionTypeCode = 0

	if len(infoCodes) == 0 {
		infoCodes = infoSupportedCodes
	}

	bldr := array.NewRecordBuilder(c.cl.Alloc, adbc.GetInfoSchema)
	defer bldr.Release()
	bldr.Reserve(len(infoCodes))

	infoNameBldr := bldr.Field(0).(*array.Uint32Builder)
	infoValueBldr := bldr.Field(1).(*array.DenseUnionBuilder)
	strInfoBldr := infoValueBldr.Child(0).(*array.StringBuilder)

	translated := make([]flightsql.SqlInfo, 0, len(infoCodes))
	for _, code := range infoCodes {
		if t, ok := adbcToFlightSQLInfo[code]; ok {
			translated = append(translated, t)
			continue
		}

		switch code {
		case adbc.InfoDriverName:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(infoDriverName)
		case adbc.InfoDriverVersion:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(infoDriverVersion)
		case adbc.InfoDriverArrowVersion:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(infoDriverArrowVersion)
		}
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	info, err := c.cl.GetSqlInfo(ctx, translated, c.timeouts)
	if err == nil {
		for _, endpoint := range info.Endpoint {
			rdr, err := doGet(ctx, c.cl, endpoint, c.clientCache, c.timeouts)
			if err != nil {
				return nil, adbcFromFlightStatus(err)
			}

			for rdr.Next() {
				rec := rdr.Record()
				field := rec.Column(0).(*array.Uint32)
				info := rec.Column(1).(*array.DenseUnion)

				for i := 0; i < int(rec.NumRows()); i++ {
					switch flightsql.SqlInfo(field.Value(i)) {
					case flightsql.SqlInfoFlightSqlServerName:
						infoNameBldr.Append(uint32(adbc.InfoVendorName))
					case flightsql.SqlInfoFlightSqlServerVersion:
						infoNameBldr.Append(uint32(adbc.InfoVendorVersion))
					case flightsql.SqlInfoFlightSqlServerArrowVersion:
						infoNameBldr.Append(uint32(adbc.InfoVendorArrowVersion))
					}

					infoValueBldr.Append(info.TypeCode(i))
					// we know we're only doing string fields here right now
					v := info.Field(info.ChildID(i)).(*array.String).
						Value(int(info.ValueOffset(i)))
					strInfoBldr.Append(v)
				}
			}

			if rdr.Err() != nil {
				return nil, adbcFromFlightStatus(rdr.Err())
			}
		}
	} else if grpcstatus.Code(err) != grpccodes.Unimplemented {
		return nil, adbcFromFlightStatus(err)
	}

	final := bldr.NewRecord()
	defer final.Release()
	return array.NewRecordReader(adbc.GetInfoSchema, []arrow.Record{final})
}

// GetObjects gets a hierarchical view of all catalogs, database schemas,
// tables, and columns.
//
// The result is an Arrow Dataset with the following schema:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	catalog_name								| utf8
//	catalog_db_schemas					| list<DB_SCHEMA_SCHEMA>
//
// DB_SCHEMA_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	db_schema_name							| utf8
//	db_schema_tables						|	list<TABLE_SCHEMA>
//
// TABLE_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	table_name									| utf8 not null
//	table_type									|	utf8 not null
//	table_columns								| list<COLUMN_SCHEMA>
//	table_constraints						| list<CONSTRAINT_SCHEMA>
//
// COLUMN_SCHEMA is a Struct with the fields:
//
//		Field Name 									| Field Type					| Comments
//		----------------------------|---------------------|---------
//		column_name									| utf8 not null				|
//		ordinal_position						| int32								| (1)
//		remarks											| utf8								| (2)
//		xdbc_data_type							| int16								| (3)
//		xdbc_type_name							| utf8								| (3)
//		xdbc_column_size						| int32								| (3)
//		xdbc_decimal_digits					| int16								| (3)
//		xdbc_num_prec_radix					| int16								| (3)
//		xdbc_nullable								| int16								| (3)
//		xdbc_column_def							| utf8								| (3)
//		xdbc_sql_data_type					| int16								| (3)
//		xdbc_datetime_sub						| int16								| (3)
//		xdbc_char_octet_length			| int32								| (3)
//		xdbc_is_nullable						| utf8								| (3)
//		xdbc_scope_catalog					| utf8								| (3)
//		xdbc_scope_schema						| utf8								| (3)
//		xdbc_scope_table						| utf8								| (3)
//		xdbc_is_autoincrement				| bool								| (3)
//		xdbc_is_generatedcolumn			| bool								| (3)
//
//	 1. The column's ordinal position in the table (starting from 1).
//	 2. Database-specific description of the column.
//	 3. Optional Value. Should be null if not supported by the driver.
//	    xdbc_values are meant to provide JDBC/ODBC-compatible metadata
//	    in an agnostic manner.
//
// CONSTRAINT_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type					| Comments
//	----------------------------|---------------------|---------
//	constraint_name							| utf8								|
//	constraint_type							| utf8 not null				| (1)
//	constraint_column_names			| list<utf8> not null | (2)
//	constraint_column_usage			| list<USAGE_SCHEMA>	| (3)
//
// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
// 2. The columns on the current table that are constrained, in order.
// 3. For FOREIGN KEY only, the referenced table and columns.
//
// USAGE_SCHEMA is a Struct with fields:
//
//	Field Name									|	Field Type
//	----------------------------|----------------------------
//	fk_catalog									| utf8
//	fk_db_schema								| utf8
//	fk_table										| utf8 not null
//	fk_column_name							| utf8 not null
//
// For the parameters: If nil is passed, then that parameter will not
// be filtered by at all. If an empty string, then only objects without
// that property (ie: catalog or db schema) will be returned.
//
// tableName and columnName must be either nil (do not filter by
// table name or column name) or non-empty.
//
// All non-empty, non-nil strings should be a search pattern (as described
// earlier).
func (c *cnxn) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	g := internal.GetObjects{Ctx: ctx, Depth: depth, Catalog: catalog, DbSchema: dbSchema, TableName: tableName, ColumnName: columnName, TableType: tableType}
	if err := g.Init(c.db.alloc, c.getObjectsDbSchemas, c.getObjectsTables); err != nil {
		return nil, err
	}
	defer g.Release()

	// To avoid an N+1 query problem, we assume result sets here will fit in memory and build up a single response.
	info, err := c.cl.GetCatalogs(ctx)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}

	rdr, err := c.readInfo(ctx, schema_ref.Catalogs, info)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}
	defer rdr.Release()

	foundCatalog := false
	for rdr.Next() {
		arr := rdr.Record().Column(0).(*array.String)
		for i := 0; i < arr.Len(); i++ {
			// XXX: force copy since accessor is unsafe
			catalogName := string([]byte(arr.Value(i)))
			g.AppendCatalog(catalogName)
			foundCatalog = true
		}
	}

	// Implementations like Dremio report no catalogs, but still have schemas
	if !foundCatalog && depth != adbc.ObjectDepthCatalogs {
		g.AppendCatalog("")
	}

	if err = rdr.Err(); err != nil {
		return nil, adbcFromFlightStatus(err)
	}

	return g.Finish()
}

// Helper function to read and validate a metadata stream
func (c *cnxn) readInfo(ctx context.Context, expectedSchema *arrow.Schema, info *flight.FlightInfo) (array.RecordReader, error) {
	// use a default queueSize for the reader
	rdr, err := newRecordReader(ctx, c.db.alloc, c.cl, info, c.clientCache, 5)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}

	if !rdr.Schema().Equal(expectedSchema) {
		rdr.Release()
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("Invalid schema returned for: expected %s, got %s", expectedSchema.String(), rdr.Schema().String()),
			Code: adbc.StatusInternal,
		}
	}
	return rdr, nil
}

// Helper function to build up a map of catalogs to DB schemas
func (c *cnxn) getObjectsDbSchemas(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string) (result map[string][]string, err error) {
	if depth == adbc.ObjectDepthCatalogs {
		return
	}
	result = make(map[string][]string)
	// Pre-populate the map of which schemas are in which catalogs
	info, err := c.cl.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{DbSchemaFilterPattern: dbSchema})
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}

	rdr, err := c.readInfo(ctx, schema_ref.DBSchemas, info)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}
	defer rdr.Release()

	for rdr.Next() {
		// Nullable
		catalog := rdr.Record().Column(0).(*array.String)
		// Non-nullable
		dbSchema := rdr.Record().Column(1).(*array.String)

		for i := 0; i < catalog.Len(); i++ {
			catalogName := ""
			if !catalog.IsNull(i) {
				catalogName = string([]byte(catalog.Value(i)))
			}
			result[catalogName] = append(result[catalogName], string([]byte(dbSchema.Value(i))))
		}
	}

	if rdr.Err() != nil {
		result = nil
		err = adbcFromFlightStatus(rdr.Err())
	}
	return
}

func (c *cnxn) getObjectsTables(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (result internal.SchemaToTableInfo, err error) {
	if depth == adbc.ObjectDepthCatalogs || depth == adbc.ObjectDepthDBSchemas {
		return
	}
	result = make(map[internal.CatalogAndSchema][]internal.TableInfo)

	// Pre-populate the map of which schemas are in which catalogs
	includeSchema := depth == adbc.ObjectDepthAll || depth == adbc.ObjectDepthColumns
	info, err := c.cl.GetTables(ctx, &flightsql.GetTablesOpts{
		TableNameFilterPattern: tableName,
		TableTypes:             tableType,
		IncludeSchema:          includeSchema,
	})
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}

	expectedSchema := schema_ref.Tables
	if includeSchema {
		expectedSchema = schema_ref.TablesWithIncludedSchema
	}
	rdr, err := c.readInfo(ctx, expectedSchema, info)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}
	defer rdr.Release()

	for rdr.Next() {
		// Nullable
		catalog := rdr.Record().Column(0).(*array.String)
		dbSchema := rdr.Record().Column(1).(*array.String)
		// Non-nullable
		tableName := rdr.Record().Column(2).(*array.String)
		tableType := rdr.Record().Column(3).(*array.String)

		for i := 0; i < catalog.Len(); i++ {
			catalogName := ""
			dbSchemaName := ""
			if !catalog.IsNull(i) {
				catalogName = string([]byte(catalog.Value(i)))
			}
			if !dbSchema.IsNull(i) {
				dbSchemaName = string([]byte(dbSchema.Value(i)))
			}
			key := internal.CatalogAndSchema{
				Catalog: catalogName,
				Schema:  dbSchemaName,
			}

			var schema *arrow.Schema
			if includeSchema {
				reader, err := ipc.NewReader(bytes.NewReader(rdr.Record().Column(4).(*array.Binary).Value(i)))
				if err != nil {
					return nil, adbc.Error{
						Msg:  err.Error(),
						Code: adbc.StatusInternal,
					}
				}
				schema = reader.Schema()
				reader.Release()
			}

			result[key] = append(result[key], internal.TableInfo{
				Name:      string([]byte(tableName.Value(i))),
				TableType: string([]byte(tableType.Value(i))),
				Schema:    schema,
			})
		}
	}

	if rdr.Err() != nil {
		result = nil
		err = adbcFromFlightStatus(rdr.Err())
	}
	return
}

func (c *cnxn) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	opts := &flightsql.GetTablesOpts{
		Catalog:                catalog,
		DbSchemaFilterPattern:  dbSchema,
		TableNameFilterPattern: &tableName,
		IncludeSchema:          true,
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	info, err := c.cl.GetTables(ctx, opts, c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}

	rdr, err := doGet(ctx, c.cl, info.Endpoint[0], c.clientCache, c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}
	defer rdr.Release()

	rec, err := rdr.Read()
	if err != nil {
		if err == io.EOF {
			return nil, adbc.Error{
				Msg:  "No table found",
				Code: adbc.StatusNotFound,
			}
		}
		return nil, adbcFromFlightStatus(err)
	}

	if rec.NumRows() == 0 {
		return nil, adbc.Error{
			Code: adbc.StatusNotFound,
		}
	}

	// returned schema should be
	//    0: catalog_name: utf8
	//    1: db_schema_name: utf8
	//    2: table_name: utf8 not null
	//    3: table_type: utf8 not null
	//    4: table_schema: bytes not null
	schemaBytes := rec.Column(4).(*array.Binary).Value(0)
	s, err := flight.DeserializeSchema(schemaBytes, c.db.alloc)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}
	return s, nil
}

// GetTableTypes returns a list of the table types in the database.
//
// The result is an arrow dataset with the following schema:
//
//	Field Name			| Field Type
//	----------------|--------------
//	table_type			| utf8 not null
func (c *cnxn) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	info, err := c.cl.GetTableTypes(ctx, c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}

	return newRecordReader(ctx, c.db.alloc, c.cl, info, c.clientCache, 5)
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
// When not supported, the convention is that it should act as if autocommit
// is enabled and return INVALID_STATE errors.
func (c *cnxn) Commit(ctx context.Context) error {
	if c.txn == nil {
		return adbc.Error{
			Msg:  "[Flight SQL] Cannot commit when autocommit is enabled",
			Code: adbc.StatusInvalidState,
		}
	}

	if !c.supportInfo.transactions {
		return errNoTransactionSupport
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	err := c.txn.Commit(ctx, c.timeouts)
	if err != nil {
		return adbcFromFlightStatus(err)
	}

	c.txn, err = c.cl.BeginTransaction(ctx, c.timeouts)
	if err != nil {
		return adbcFromFlightStatus(err)
	}
	return nil
}

// Rollback rolls back any pending transactions. Only used if autocommit
// is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
// When not supported, the convention is that it should act as if autocommit
// is enabled and return INVALID_STATE errors.
func (c *cnxn) Rollback(ctx context.Context) error {
	if c.txn == nil {
		return adbc.Error{
			Msg:  "[Flight SQL] Cannot rollback when autocommit is enabled",
			Code: adbc.StatusInvalidState,
		}
	}

	if !c.supportInfo.transactions {
		return errNoTransactionSupport
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	err := c.txn.Rollback(ctx, c.timeouts)
	if err != nil {
		return adbcFromFlightStatus(err)
	}

	c.txn, err = c.cl.BeginTransaction(ctx, c.timeouts)
	if err != nil {
		return adbcFromFlightStatus(err)
	}
	return nil
}

// NewStatement initializes a new statement object tied to this connection
func (c *cnxn) NewStatement() (adbc.Statement, error) {
	return &statement{
		alloc:       c.db.alloc,
		clientCache: c.clientCache,
		hdrs:        c.hdrs.Copy(),
		queueSize:   5,
		timeouts:    c.timeouts,
		cnxn:        c,
	}, nil
}

func (c *cnxn) execute(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	if c.txn != nil {
		return c.txn.Execute(ctx, query, opts...)
	}

	return c.cl.Execute(ctx, query, opts...)
}

func (c *cnxn) executeSubstrait(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	if c.txn != nil {
		return c.txn.ExecuteSubstrait(ctx, plan, opts...)
	}

	return c.cl.ExecuteSubstrait(ctx, plan, opts...)
}

func (c *cnxn) executeUpdate(ctx context.Context, query string, opts ...grpc.CallOption) (n int64, err error) {
	if c.txn != nil {
		return c.txn.ExecuteUpdate(ctx, query, opts...)
	}

	return c.cl.ExecuteUpdate(ctx, query, opts...)
}

func (c *cnxn) executeSubstraitUpdate(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (n int64, err error) {
	if c.txn != nil {
		return c.txn.ExecuteSubstraitUpdate(ctx, plan, opts...)
	}

	return c.cl.ExecuteSubstraitUpdate(ctx, plan, opts...)
}

func (c *cnxn) prepare(ctx context.Context, query string, opts ...grpc.CallOption) (*flightsql.PreparedStatement, error) {
	if c.txn != nil {
		return c.txn.Prepare(ctx, query, opts...)
	}

	return c.cl.Prepare(ctx, query, opts...)
}

func (c *cnxn) prepareSubstrait(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (*flightsql.PreparedStatement, error) {
	if c.txn != nil {
		return c.txn.PrepareSubstrait(ctx, plan, opts...)
	}

	return c.cl.PrepareSubstrait(ctx, plan, opts...)
}

// Close closes this connection and releases any associated resources.
func (c *cnxn) Close() error {
	if c.cl == nil {
		return adbc.Error{
			Msg:  "[Flight SQL Connection] trying to close already closed connection",
			Code: adbc.StatusInvalidState,
		}
	}

	err := c.cl.Close()
	c.cl = nil
	return err
}

// ReadPartition constructs a statement for a partition of a query. The
// results can then be read independently using the returned RecordReader.
//
// A partition can be retrieved by using ExecutePartitions on a statement.
func (c *cnxn) ReadPartition(ctx context.Context, serializedPartition []byte) (rdr array.RecordReader, err error) {
	var info flight.FlightInfo
	if err := proto.Unmarshal(serializedPartition, &info); err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInvalidArgument,
		}
	}

	// The driver only ever returns one endpoint.
	if len(info.Endpoint) != 1 {
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("Invalid partition: expected 1 endpoint, got %d", len(info.Endpoint)),
			Code: adbc.StatusInvalidArgument,
		}
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	rdr, err = doGet(ctx, c.cl, info.Endpoint[0], c.clientCache, c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}
	return rdr, nil
}

var (
	_ adbc.PostInitOptions = (*cnxn)(nil)
)
