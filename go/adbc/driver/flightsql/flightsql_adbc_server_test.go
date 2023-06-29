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

// Tests that use custom server implementations.

package flightsql_test

import (
	"context"
	"errors"
	"fmt"
	"net/textproto"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/apache/arrow/go/v13/arrow/flight"
	"github.com/apache/arrow/go/v13/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v13/arrow/memory"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

// ---- Common Infra --------------------

type ServerBasedTests struct {
	suite.Suite

	s    flight.Server
	db   adbc.Database
	cnxn adbc.Connection
}

func (suite *ServerBasedTests) DoSetupSuite(srv flightsql.Server, srvMiddleware []flight.ServerMiddleware, dbArgs map[string]string) {
	suite.s = flight.NewServerWithMiddleware(srvMiddleware)
	suite.s.RegisterFlightService(flightsql.NewFlightServer(srv))
	suite.Require().NoError(suite.s.Init("localhost:0"))
	suite.s.SetShutdownOnSignals(os.Interrupt, os.Kill)
	go func() {
		_ = suite.s.Serve()
	}()

	uri := "grpc+tcp://" + suite.s.Addr().String()
	var err error

	args := map[string]string{
		"uri": uri,
	}
	maps.Copy(args, dbArgs)
	suite.db, err = (driver.Driver{}).NewDatabase(args)
	suite.Require().NoError(err)
}

func (suite *ServerBasedTests) SetupTest() {
	var err error
	suite.cnxn, err = suite.db.Open(context.Background())
	suite.Require().NoError(err)
}

func (suite *ServerBasedTests) TearDownTest() {
	suite.Require().NoError(suite.cnxn.Close())
}

func (suite *ServerBasedTests) TearDownSuite() {
	suite.db = nil
	suite.s.Shutdown()
}

// ---- Tests --------------------

func TestAuthn(t *testing.T) {
	suite.Run(t, &AuthnTests{})
}

func TestTimeout(t *testing.T) {
	suite.Run(t, &TimeoutTests{})
}

func TestCookies(t *testing.T) {
	suite.Run(t, &CookieTests{})
}

// ---- AuthN Tests --------------------

type AuthnTestServer struct {
	flightsql.BaseServer
}

func (server *AuthnTestServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	md := metadata.MD{}
	md.Set("authorization", "Bearer final")
	if err := grpc.SendHeader(ctx, md); err != nil {
		return nil, err
	}
	tkt, _ := flightsql.CreateStatementQueryTicket([]byte{})
	info := &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: tkt}},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
	}
	return info, nil
}

func (server *AuthnTestServer) DoGetStatement(ctx context.Context, tkt flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	sc := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(`[{"a": 5}]`))
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: rec,
			Desc: nil,
			Err:  nil,
		}
	}()
	return sc, ch, nil
}

func authnTestUnary(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	md, ok := metadata.FromIncomingContext(ctx)
	if !ok {
		return nil, status.Error(codes.InvalidArgument, "Could not get metadata")
	}
	auth := md.Get("authorization")
	if len(auth) == 0 {
		return nil, status.Error(codes.Unauthenticated, "No token")
	} else if auth[0] != "Bearer initial" {
		return nil, status.Error(codes.Unauthenticated, "Invalid token for unary call: "+auth[0])
	}

	md.Set("authorization", "Bearer final")
	ctx = metadata.NewOutgoingContext(ctx, md)
	return handler(ctx, req)
}

func authnTestStream(srv interface{}, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	md, ok := metadata.FromIncomingContext(ss.Context())
	if !ok {
		return status.Error(codes.InvalidArgument, "Could not get metadata")
	}
	auth := md.Get("authorization")
	if len(auth) == 0 {
		return status.Error(codes.Unauthenticated, "No token")
	} else if auth[0] != "Bearer final" {
		return status.Error(codes.Unauthenticated, "Invalid token for stream call: "+auth[0])
	}

	return handler(srv, ss)
}

type AuthnTests struct {
	ServerBasedTests
}

func (suite *AuthnTests) SetupSuite() {
	suite.DoSetupSuite(&AuthnTestServer{}, []flight.ServerMiddleware{
		{Stream: authnTestStream, Unary: authnTestUnary},
	}, map[string]string{
		driver.OptionAuthorizationHeader: "Bearer initial",
	})
}

func (suite *AuthnTests) TestBearerTokenUpdated() {
	// apache/arrow-adbc#584: when setting the auth header directly, the client should use any updated token value from the server if given
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer stmt.Close()

	suite.Require().NoError(stmt.SetSqlQuery("timeout"))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	suite.NoError(err)
	defer reader.Release()
}

// ---- Timeout Tests --------------------

type TimeoutTestServer struct {
	flightsql.BaseServer
}

func (ts *TimeoutTestServer) DoGetStatement(ctx context.Context, tkt flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	if string(tkt.GetStatementHandle()) == "sleep and succeed" {
		time.Sleep(1 * time.Second)
		sc := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
		rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(`[{"a": 5}]`))
		if err != nil {
			return nil, nil, err
		}

		ch := make(chan flight.StreamChunk)
		go func() {
			defer close(ch)
			ch <- flight.StreamChunk{
				Data: rec,
				Desc: nil,
				Err:  nil,
			}
		}()
		return sc, ch, nil
	}

	// wait till the context is cancelled
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func (ts *TimeoutTestServer) DoPutCommandStatementUpdate(ctx context.Context, cmd flightsql.StatementUpdate) (int64, error) {
	if cmd.GetQuery() == "timeout" {
		<-ctx.Done()
		return -1, ctx.Err()
	}
	return -1, arrow.ErrNotImplemented
}

func (ts *TimeoutTestServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	switch cmd.GetQuery() {
	case "timeout":
		<-ctx.Done()
	case "fetch":
		tkt, _ := flightsql.CreateStatementQueryTicket([]byte("fetch"))
		info := &flight.FlightInfo{
			FlightDescriptor: desc,
			Endpoint: []*flight.FlightEndpoint{
				{Ticket: &flight.Ticket{Ticket: tkt}},
			},
			TotalRecords: -1,
			TotalBytes:   -1,
		}
		return info, nil
	case "notimeout":
		time.Sleep(1 * time.Second)
		tkt, _ := flightsql.CreateStatementQueryTicket([]byte("sleep and succeed"))
		info := &flight.FlightInfo{
			FlightDescriptor: desc,
			Endpoint: []*flight.FlightEndpoint{
				{Ticket: &flight.Ticket{Ticket: tkt}},
			},
			TotalRecords: -1,
			TotalBytes:   -1,
		}
		return info, nil
	}
	return nil, ctx.Err()
}

func (ts *TimeoutTestServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (result flightsql.ActionCreatePreparedStatementResult, err error) {
	<-ctx.Done()
	return result, ctx.Err()
}

type TimeoutTests struct {
	ServerBasedTests
}

func (suite *TimeoutTests) SetupSuite() {
	suite.DoSetupSuite(&TimeoutTestServer{}, nil, nil)
}

func (ts *TimeoutTests) TestInvalidValues() {
	keys := []string{
		"adbc.flight.sql.rpc.timeout_seconds.fetch",
		"adbc.flight.sql.rpc.timeout_seconds.query",
		"adbc.flight.sql.rpc.timeout_seconds.update",
	}
	values := []string{"1.1f", "asdf", "inf", "NaN", "-1"}

	for _, k := range keys {
		for _, v := range values {
			ts.Run("key="+k+",val="+v, func() {
				err := ts.cnxn.(adbc.PostInitOptions).SetOption(k, v)
				var adbcErr adbc.Error
				ts.ErrorAs(err, &adbcErr)
				ts.Equal(adbc.StatusInvalidArgument, adbcErr.Code)
				ts.ErrorContains(err, "invalid timeout option value")
			})
		}
	}
}

func (ts *TimeoutTests) TestRemoveTimeout() {
	keys := []string{
		"adbc.flight.sql.rpc.timeout_seconds.fetch",
		"adbc.flight.sql.rpc.timeout_seconds.query",
		"adbc.flight.sql.rpc.timeout_seconds.update",
	}
	for _, k := range keys {
		ts.Run(k, func() {
			ts.NoError(ts.cnxn.(adbc.PostInitOptions).SetOption(k, "1.0"))
			ts.NoError(ts.cnxn.(adbc.PostInitOptions).SetOption(k, "0"))
		})
	}
}

func (ts *TimeoutTests) TestDoActionTimeout() {
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.update", "0.1"))

	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer stmt.Close()

	ts.Require().NoError(stmt.SetSqlQuery("fetch"))
	var adbcErr adbc.Error
	ts.ErrorAs(stmt.Prepare(context.Background()), &adbcErr)
	ts.Equal(adbc.StatusTimeout, adbcErr.Code, adbcErr.Error())
	// Exact match - we don't want extra fluff in the message
	ts.Equal("context deadline exceeded", adbcErr.Msg)
}

func (ts *TimeoutTests) TestDoGetTimeout() {
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.fetch", "0.1"))

	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer stmt.Close()

	ts.Require().NoError(stmt.SetSqlQuery("fetch"))
	var adbcErr adbc.Error
	_, _, err = stmt.ExecuteQuery(context.Background())
	ts.ErrorAs(err, &adbcErr)
	ts.Equal(adbc.StatusTimeout, adbcErr.Code, adbcErr.Error())
}

func (ts *TimeoutTests) TestDoPutTimeout() {
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.update", "1.1"))

	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer stmt.Close()

	ts.Require().NoError(stmt.SetSqlQuery("timeout"))
	var adbcErr adbc.Error
	_, err = stmt.ExecuteUpdate(context.Background())
	ts.ErrorAs(err, &adbcErr)
	ts.Equal(adbc.StatusTimeout, adbcErr.Code, adbcErr.Error())
}

func (ts *TimeoutTests) TestGetFlightInfoTimeout() {
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.query", "0.1"))

	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer stmt.Close()

	ts.Require().NoError(stmt.SetSqlQuery("timeout"))
	var adbcErr adbc.Error
	_, _, err = stmt.ExecuteQuery(context.Background())
	ts.ErrorAs(err, &adbcErr)
	ts.NotEqual(adbc.StatusNotImplemented, adbcErr.Code, adbcErr.Error())
}

func (ts *TimeoutTests) TestDontTimeout() {
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.fetch", "2.0"))
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.query", "2.0"))

	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer stmt.Close()

	ts.Require().NoError(stmt.SetSqlQuery("notimeout"))
	// GetFlightInfo will sleep for one second and DoGet will also
	// sleep for one second. But our timeout is 2 seconds, which is
	// per-operation. So we shouldn't time out and all should succeed.
	rr, _, err := stmt.ExecuteQuery(context.Background())
	ts.Require().NoError(err)
	defer rr.Release()

	ts.True(rr.Next())
	rec := rr.Record()

	sc := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	expected, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(`[{"a": 5}]`))
	ts.Require().NoError(err)
	defer expected.Release()
	ts.Truef(array.RecordEqual(rec, expected), "expected: %s\nactual: %s", expected, rec)
}

// ---- Cookie Tests --------------------
type CookieTestServer struct {
	flightsql.BaseServer

	cur time.Time
}

func (server *CookieTestServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	md := metadata.MD{}
	md.Append("set-cookie", "foo=bar")
	md.Append("set-cookie", "bar=baz; Max-Age=1")
	server.cur = time.Now()

	if err := grpc.SendHeader(ctx, md); err != nil {
		return nil, err
	}

	tkt, _ := flightsql.CreateStatementQueryTicket([]byte{})
	info := &flight.FlightInfo{
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: tkt}},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
	}

	return info, nil
}

func (server *CookieTestServer) DoGetStatement(ctx context.Context, tkt flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var (
		foundFoo, foundBar bool
	)

	cookies := metadata.ValueFromIncomingContext(ctx, "cookie")
	for _, line := range cookies {
		line = textproto.TrimString(line)

		var part string
		for len(line) > 0 {
			part, line, _ = strings.Cut(line, ";")
			part = textproto.TrimString(part)
			if part == "" {
				continue
			}

			name, val, _ := strings.Cut(part, "=")
			name = textproto.TrimString(name)
			if len(val) > 1 && val[0] == '"' && val[len(val)-1] == '"' {
				val = val[1 : len(val)-1]
			}

			switch name {
			case "foo":
				if val == "bar" {
					foundFoo = true
				}
			case "bar":
				if val == "baz" {
					foundBar = true
				}
			default:
				return nil, nil, fmt.Errorf("found unexpected cookie '%s' = '%s'", name, val)
			}
		}
	}

	if !foundFoo {
		return nil, nil, errors.New("missing cookie 'foo'='bar'")
	}

	if !foundBar && time.Now().Before(server.cur.Add(1*time.Second)) {
		return nil, nil, errors.New("missing cookie 'bar'='baz'")
	}

	sc := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, sc, strings.NewReader(`[{"a": 5}]`))
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: rec,
			Desc: nil,
			Err:  nil,
		}
	}()
	return sc, ch, nil
}

type CookieTests struct {
	ServerBasedTests
}

func (suite *CookieTests) SetupSuite() {
	suite.DoSetupSuite(&CookieTestServer{}, nil, map[string]string{
		driver.OptionCookieMiddleware: adbc.OptionValueEnabled,
	})
}

func (suite *CookieTests) TestCookieUsage() {
	stmt, err := suite.cnxn.NewStatement()
	suite.Require().NoError(err)
	defer stmt.Close()

	suite.Require().NoError(stmt.SetSqlQuery("timeout"))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	suite.Require().NoError(err)
	defer reader.Release()
}
