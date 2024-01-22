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
	"github.com/google/uuid"
	"net/textproto"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
	"github.com/apache/arrow/go/v15/arrow/flight"
	"github.com/apache/arrow/go/v15/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v15/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow/go/v15/arrow/memory"
	"github.com/golang/protobuf/ptypes/wrappers"
	"github.com/stretchr/testify/suite"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/anypb"
	"google.golang.org/protobuf/types/known/wrapperspb"
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
	suite.db, err = (driver.NewDriver(memory.DefaultAllocator)).NewDatabase(args)
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
	suite.NoError(suite.db.Close())
	suite.db = nil
	suite.s.Shutdown()
}

// ---- Tests --------------------

func TestAuthn(t *testing.T) {
	suite.Run(t, &AuthnTests{})
}

func TestErrorDetails(t *testing.T) {
	suite.Run(t, &ErrorDetailsTests{})
}

func TestExecuteSchema(t *testing.T) {
	suite.Run(t, &ExecuteSchemaTests{})
}

func TestIncrementalPoll(t *testing.T) {
	suite.Run(t, &IncrementalPollTests{})
}

func TestTimeout(t *testing.T) {
	suite.Run(t, &TimeoutTests{})
}

func TestCookies(t *testing.T) {
	suite.Run(t, &CookieTests{})
}

func TestDataType(t *testing.T) {
	suite.Run(t, &DataTypeTests{})
}

func TestMultiTable(t *testing.T) {
	suite.Run(t, &MultiTableTests{})
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

// ---- Error Details Tests --------------------

type ErrorDetailsTestServer struct {
	flightsql.BaseServer
}

func (srv *ErrorDetailsTestServer) GetFlightInfoStatement(ctx context.Context, query flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	if query.GetQuery() == "details" {
		detail := wrapperspb.Int32Value{Value: 42}
		st, err := status.New(codes.Unknown, "details").WithDetails(&detail)
		if err != nil {
			return nil, err
		}
		return nil, st.Err()
	} else if query.GetQuery() == "query" {
		tkt, err := flightsql.CreateStatementQueryTicket([]byte("fetch"))
		if err != nil {
			panic(err)
		}
		return &flight.FlightInfo{Endpoint: []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: tkt}}}}, nil
	}
	return nil, status.Errorf(codes.Unimplemented, "GetSchemaStatement not implemented")
}

func (ts *ErrorDetailsTestServer) DoGetStatement(ctx context.Context, tkt flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	sc := arrow.NewSchema([]arrow.Field{}, nil)
	detail := wrapperspb.Int32Value{Value: 42}
	st, err := status.New(codes.Unknown, "details").WithDetails(&detail)
	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: nil,
			Desc: nil,
			Err:  st.Err(),
		}
	}()
	return sc, ch, nil
}

type ErrorDetailsTests struct {
	ServerBasedTests
}

func (suite *ErrorDetailsTests) SetupSuite() {
	srv := ErrorDetailsTestServer{}
	srv.Alloc = memory.DefaultAllocator
	suite.DoSetupSuite(&srv, nil, nil)
}

func (ts *ErrorDetailsTests) TestGetFlightInfo() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer stmt.Close()

	ts.NoError(stmt.SetSqlQuery("details"))

	_, _, err = stmt.ExecuteQuery(context.Background())
	var adbcErr adbc.Error
	ts.ErrorAs(err, &adbcErr)

	ts.Equal(1, len(adbcErr.Details))

	wrapper := adbcErr.Details[0]
	ts.Equal("grpc-status-details-bin", wrapper.Key())

	raw, err := wrapper.Serialize()
	ts.NoError(err)
	any := anypb.Any{}
	ts.NoError(proto.Unmarshal(raw, &any))
	message := wrappers.Int32Value{}
	ts.NoError(any.UnmarshalTo(&message))
	ts.Equal(int32(42), message.Value)
}

func (ts *ErrorDetailsTests) TestDoGet() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer stmt.Close()

	ts.NoError(stmt.SetSqlQuery("query"))

	reader, _, err := stmt.ExecuteQuery(context.Background())
	ts.NoError(err)

	defer reader.Release()

	for reader.Next() {
	}
	err = reader.Err()

	ts.Error(err)

	var adbcErr adbc.Error
	ts.ErrorAs(err, &adbcErr, "Error was: %#v", err)

	ts.Equal(1, len(adbcErr.Details))

	wrapper := adbcErr.Details[0]
	ts.Equal("grpc-status-details-bin", wrapper.Key())

	raw, err := wrapper.Serialize()
	ts.NoError(err)
	any := anypb.Any{}
	ts.NoError(proto.Unmarshal(raw, &any))
	message := wrappers.Int32Value{}
	ts.NoError(any.UnmarshalTo(&message))
	ts.Equal(int32(42), message.Value)
}

// ---- ExecuteSchema Tests --------------------

type ExecuteSchemaTestServer struct {
	flightsql.BaseServer
}

func (srv *ExecuteSchemaTestServer) GetSchemaStatement(ctx context.Context, query flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.SchemaResult, error) {
	if query.GetQuery() == "sample query" {
		return &flight.SchemaResult{
			Schema: flight.SerializeSchema(arrow.NewSchema([]arrow.Field{
				{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
			}, nil), srv.Alloc),
		}, nil
	}
	return nil, status.Errorf(codes.Unimplemented, "GetSchemaStatement not implemented")
}

func (srv *ExecuteSchemaTestServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (res flightsql.ActionCreatePreparedStatementResult, err error) {
	if req.GetQuery() == "sample query" {
		return flightsql.ActionCreatePreparedStatementResult{
			DatasetSchema: arrow.NewSchema([]arrow.Field{
				{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
			}, nil),
		}, nil
	}
	return flightsql.ActionCreatePreparedStatementResult{}, status.Error(codes.Unimplemented, "CreatePreparedStatement not implemented")
}

type ExecuteSchemaTests struct {
	ServerBasedTests
}

func (suite *ExecuteSchemaTests) SetupSuite() {
	srv := ExecuteSchemaTestServer{}
	srv.Alloc = memory.DefaultAllocator
	suite.DoSetupSuite(&srv, nil, nil)
}

func (ts *ExecuteSchemaTests) TestNoQuery() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer stmt.Close()

	es := stmt.(adbc.StatementExecuteSchema)
	_, err = es.ExecuteSchema(context.Background())

	var adbcErr adbc.Error
	ts.ErrorAs(err, &adbcErr)
	ts.Equal(adbc.StatusInvalidState, adbcErr.Code, adbcErr.Error())
}

func (ts *ExecuteSchemaTests) TestPreparedQuery() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer stmt.Close()

	ts.NoError(stmt.SetSqlQuery("sample query"))
	ts.NoError(stmt.Prepare(context.Background()))

	es := stmt.(adbc.StatementExecuteSchema)
	schema, err := es.ExecuteSchema(context.Background())
	ts.NoError(err)
	ts.NotNil(schema)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	ts.True(expectedSchema.Equal(schema), schema.String())
}

func (ts *ExecuteSchemaTests) TestQuery() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer stmt.Close()

	ts.NoError(stmt.SetSqlQuery("sample query"))

	es := stmt.(adbc.StatementExecuteSchema)
	schema, err := es.ExecuteSchema(context.Background())
	ts.NoError(err)
	ts.NotNil(schema)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
	}, nil)

	ts.True(expectedSchema.Equal(schema), schema.String())
}

// ---- IncrementalPoll Tests --------------------

type IncrementalQuery struct {
	query     string
	nextIndex int
}

type IncrementalPollTestServer struct {
	flightsql.BaseServer
	mu        sync.Mutex
	queries   map[string]*IncrementalQuery
	testCases map[string]IncrementalPollTestCase
}

func (srv *IncrementalPollTestServer) PollFlightInfo(ctx context.Context, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	srv.mu.Lock()
	defer srv.mu.Unlock()

	var val wrapperspb.StringValue
	var err error
	if err = proto.Unmarshal(desc.Cmd, &val); err != nil {
		return nil, err
	}
	queryId := val.Value
	progress := int64(0)
	if strings.Contains(queryId, ";") {
		parts := strings.SplitN(queryId, ";", 2)
		queryId = parts[0]
		progress, err = strconv.ParseInt(parts[1], 10, 32)
		if err != nil {
			return nil, err
		}
	}

	query, ok := srv.queries[queryId]
	if !ok {
		return nil, status.Errorf(codes.NotFound, "Query ID not found")
	}

	testCase, ok := srv.testCases[query.query]
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, fmt.Sprintf("Invalid case %s", query.query))
	}

	if testCase.differentRetryDescriptor && progress != int64(query.nextIndex) {
		return nil, status.Errorf(codes.InvalidArgument, fmt.Sprintf("Used wrong retry descriptor, expected %d but got %d", query.nextIndex, progress))
	}

	return srv.MakePollInfo(&testCase, query, queryId)
}

func (srv *IncrementalPollTestServer) PollFlightInfoStatement(ctx context.Context, query flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	queryId := uuid.New().String()

	testCase, ok := srv.testCases[query.GetQuery()]
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, fmt.Sprintf("Invalid case %s", query.GetQuery()))
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.queries[queryId] = &IncrementalQuery{
		query:     query.GetQuery(),
		nextIndex: 0,
	}

	return srv.MakePollInfo(&testCase, srv.queries[queryId], queryId)
}

func (srv *IncrementalPollTestServer) PollFlightInfoPreparedStatement(ctx context.Context, query flightsql.PreparedStatementQuery, desc *flight.FlightDescriptor) (*flight.PollInfo, error) {
	queryId := uuid.New().String()
	req := string(query.GetPreparedStatementHandle())

	testCase, ok := srv.testCases[req]
	if !ok {
		return nil, status.Errorf(codes.Unimplemented, fmt.Sprintf("Invalid case %s", req))
	}

	srv.mu.Lock()
	defer srv.mu.Unlock()

	srv.queries[queryId] = &IncrementalQuery{
		query:     req,
		nextIndex: 0,
	}

	return srv.MakePollInfo(&testCase, srv.queries[queryId], queryId)
}

func (srv *IncrementalPollTestServer) BeginTransaction(context.Context, flightsql.ActionBeginTransactionRequest) (id []byte, err error) {
	return []byte("txn"), nil
}

func (srv *IncrementalPollTestServer) EndTransaction(context.Context, flightsql.ActionEndTransactionRequest) error {
	return nil
}

func (srv *IncrementalPollTestServer) CreatePreparedStatement(ctx context.Context, req flightsql.ActionCreatePreparedStatementRequest) (res flightsql.ActionCreatePreparedStatementResult, err error) {
	return flightsql.ActionCreatePreparedStatementResult{
		Handle: []byte(req.GetQuery()),
		DatasetSchema: arrow.NewSchema([]arrow.Field{
			{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
		}, nil),
	}, nil
}

func (srv *IncrementalPollTestServer) ClosePreparedStatement(ctx context.Context, req flightsql.ActionClosePreparedStatementRequest) error {
	return nil
}

func (srv *IncrementalPollTestServer) MakePollInfo(testCase *IncrementalPollTestCase, query *IncrementalQuery, queryId string) (*flight.PollInfo, error) {
	schema := flight.SerializeSchema(arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int32},
	}, nil), srv.Alloc)

	pb := wrapperspb.StringValue{Value: queryId}
	if testCase.differentRetryDescriptor {
		pb.Value = queryId + ";" + strconv.Itoa(query.nextIndex+1)
	}
	descriptor, err := proto.Marshal(&pb)
	if err != nil {
		return nil, err
	}

	numEndpoints := 0
	for i := 0; i <= query.nextIndex; i++ {
		if i >= len(testCase.progress) {
			break
		}
		numEndpoints += testCase.progress[i]
	}
	endpoints := make([]*flight.FlightEndpoint, numEndpoints)
	for i := range endpoints {
		endpoints[i] = &flight.FlightEndpoint{
			Ticket: &flight.Ticket{
				Ticket: []byte{},
			},
		}
	}

	query.nextIndex++
	pollInfo := flight.PollInfo{
		Info: &flight.FlightInfo{
			Schema:   schema,
			Endpoint: endpoints,
		},
		FlightDescriptor: &flight.FlightDescriptor{
			Type: flight.DescriptorCMD,
			Cmd:  descriptor,
		},
		Progress: proto.Float64(float64(query.nextIndex) / float64(len(testCase.progress))),
	}

	if query.nextIndex >= len(testCase.progress) {
		if testCase.completeLazily {
			if query.nextIndex == len(testCase.progress) {
				// Make the client poll one more time
			} else {
				pollInfo.FlightDescriptor = nil
				delete(srv.queries, queryId)
			}

		} else {
			pollInfo.FlightDescriptor = nil
			delete(srv.queries, queryId)
		}
	}

	return &pollInfo, nil
}

type IncrementalPollTestCase struct {
	// on each poll (including the first), this many new endpoints complete
	// making 0 progress is allowed, but not recommended (allow clients to 'long poll')
	progress []int

	// use a different retry descriptor for each poll
	differentRetryDescriptor bool

	// require one extra poll to get completion (i.e. the last poll will have a nil FlightInfo)
	completeLazily bool
}

type IncrementalPollTests struct {
	ServerBasedTests
	testCases map[string]IncrementalPollTestCase
}

func (suite *IncrementalPollTests) SetupSuite() {
	suite.testCases = map[string]IncrementalPollTestCase{
		"basic": {
			progress: []int{1, 1, 1, 1},
		},
		"basic 2": {
			progress: []int{2, 3, 4, 5},
		},
		"basic 3": {
			progress: []int{2},
		},
		"descriptor changes": {
			progress:                 []int{1, 1, 1, 1},
			differentRetryDescriptor: true,
		},
		"lazy": {
			progress:       []int{1, 1, 1, 1},
			completeLazily: true,
		},
		"lazy 2": {
			progress:       []int{1, 1, 1, 0},
			completeLazily: true,
		},
		"no progress": {
			progress: []int{0, 1, 1, 1},
		},
		"no progress 2": {
			progress: []int{0, 0, 1, 1},
		},
		"no progress 3": {
			progress: []int{0, 0, 1, 0},
		},
	}

	srv := IncrementalPollTestServer{
		queries:   make(map[string]*IncrementalQuery),
		testCases: suite.testCases,
	}
	suite.NoError(srv.RegisterSqlInfo(flightsql.SqlInfoFlightSqlServerTransaction, int32(flightsql.SqlTransactionTransaction)))
	srv.Alloc = memory.DefaultAllocator
	suite.DoSetupSuite(&srv, nil, nil)
}

func (ts *IncrementalPollTests) TestMaxProgress() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer stmt.Close()
	opts := stmt.(adbc.GetSetOptions)

	val, err := opts.GetOptionDouble(adbc.OptionKeyMaxProgress)
	ts.NoError(err)
	ts.Equal(1.0, val)
}

func (ts *IncrementalPollTests) TestOptionValue() {
	stmt, err := ts.cnxn.NewStatement()
	ts.NoError(err)
	defer stmt.Close()
	opts := stmt.(adbc.GetSetOptions)

	val, err := opts.GetOption(adbc.OptionKeyIncremental)
	ts.NoError(err)
	ts.Equal(adbc.OptionValueDisabled, val)

	ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

	val, err = opts.GetOption(adbc.OptionKeyIncremental)
	ts.NoError(err)
	ts.Equal(adbc.OptionValueEnabled, val)

	var adbcErr adbc.Error
	ts.ErrorAs(stmt.SetOption(adbc.OptionKeyIncremental, "foobar"), &adbcErr)
	ts.Equal(adbc.StatusInvalidArgument, adbcErr.Code)
}

func (ts *IncrementalPollTests) RunOneTestCase(ctx context.Context, stmt adbc.Statement, name string, testCase *IncrementalPollTestCase) {
	opts := stmt.(adbc.GetSetOptions)

	for idx, progress := range testCase.progress {
		if progress == 0 {
			// the driver hides this from us
			continue
		}

		_, partitions, _, err := stmt.ExecutePartitions(ctx)
		ts.NoError(err)

		ts.Equal(uint64(progress), partitions.NumPartitions)

		val, err := opts.GetOptionDouble(adbc.OptionKeyProgress)
		ts.NoError(err)
		ts.Equal(float64(idx+1)/float64(len(testCase.progress)), val)
	}

	// Query completed, but we find out by getting no partitions in this call
	_, partitions, _, err := stmt.ExecutePartitions(ctx)
	ts.NoError(err)

	ts.Equal(uint64(0), partitions.NumPartitions)
}

func (ts *IncrementalPollTests) TestQuery() {
	ctx := context.Background()
	for name, testCase := range ts.testCases {
		ts.Run(name, func() {
			stmt, err := ts.cnxn.NewStatement()
			ts.NoError(err)
			defer stmt.Close()

			ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

			// Run the query multiple times (we should be able to reuse the statement)
			for i := 0; i < 2; i++ {
				ts.NoError(stmt.SetSqlQuery(name))
				ts.RunOneTestCase(ctx, stmt, name, &testCase)
			}
		})
	}
}

func (ts *IncrementalPollTests) TestQueryPrepared() {
	ctx := context.Background()
	for name, testCase := range ts.testCases {
		ts.Run(name, func() {
			stmt, err := ts.cnxn.NewStatement()
			ts.NoError(err)
			defer stmt.Close()

			ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

			// Run the query multiple times (we should be able to reuse the statement)
			for i := 0; i < 2; i++ {
				ts.NoError(stmt.SetSqlQuery(name))
				ts.NoError(stmt.Prepare(ctx))
				ts.RunOneTestCase(ctx, stmt, name, &testCase)
			}
		})
	}
}

func (ts *IncrementalPollTests) TestQueryPreparedTransaction() {
	ctx := context.Background()
	for name, testCase := range ts.testCases {
		ts.Run(name, func() {
			ts.NoError(ts.cnxn.(adbc.PostInitOptions).SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
			stmt, err := ts.cnxn.NewStatement()
			ts.NoError(err)
			defer stmt.Close()

			ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

			// Run the query multiple times (we should be able to reuse the statement)
			for i := 0; i < 2; i++ {
				ts.NoError(stmt.SetSqlQuery(name))
				ts.NoError(stmt.Prepare(ctx))
				ts.RunOneTestCase(ctx, stmt, name, &testCase)
			}
		})
	}
}

func (ts *IncrementalPollTests) TestQueryTransaction() {
	ctx := context.Background()
	for name, testCase := range ts.testCases {
		ts.Run(name, func() {
			ts.NoError(ts.cnxn.(adbc.PostInitOptions).SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
			stmt, err := ts.cnxn.NewStatement()
			ts.NoError(err)
			defer stmt.Close()

			ts.NoError(stmt.SetOption(adbc.OptionKeyIncremental, adbc.OptionValueEnabled))

			// Run the query multiple times (we should be able to reuse the statement)
			for i := 0; i < 2; i++ {
				ts.NoError(stmt.SetSqlQuery(name))
				ts.RunOneTestCase(ctx, stmt, name, &testCase)
			}
		})
	}
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

func (ts *TimeoutTests) TestGetSet() {
	keys := []string{
		"adbc.flight.sql.rpc.timeout_seconds.fetch",
		"adbc.flight.sql.rpc.timeout_seconds.query",
		"adbc.flight.sql.rpc.timeout_seconds.update",
	}
	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer stmt.Close()

	for _, v := range []interface{}{ts.db, ts.cnxn, stmt} {
		getset := v.(adbc.GetSetOptions)

		for _, k := range keys {
			strval, err := getset.GetOption(k)
			ts.NoError(err)
			ts.Equal("0s", strval)

			intval, err := getset.GetOptionInt(k)
			ts.NoError(err)
			ts.Equal(int64(0), intval)

			floatval, err := getset.GetOptionDouble(k)
			ts.NoError(err)
			ts.Equal(0.0, floatval)

			err = getset.SetOptionInt(k, 1)
			ts.NoError(err)

			strval, err = getset.GetOption(k)
			ts.NoError(err)
			ts.Equal("1s", strval)

			intval, err = getset.GetOptionInt(k)
			ts.NoError(err)
			ts.Equal(int64(1), intval)

			floatval, err = getset.GetOptionDouble(k)
			ts.NoError(err)
			ts.Equal(1.0, floatval)

			err = getset.SetOptionDouble(k, 0.1)
			ts.NoError(err)

			strval, err = getset.GetOption(k)
			ts.NoError(err)
			ts.Equal("100ms", strval)

			intval, err = getset.GetOptionInt(k)
			ts.NoError(err)
			// truncated
			ts.Equal(int64(0), intval)

			floatval, err = getset.GetOptionDouble(k)
			ts.NoError(err)
			ts.Equal(0.1, floatval)
		}
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
	ts.Equal("[FlightSQL] context deadline exceeded (DeadlineExceeded; Prepare)", adbcErr.Msg)
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

// ---- Data Type Tests --------------------
type DataTypeTestServer struct {
	flightsql.BaseServer
}

func (server *DataTypeTestServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	tkt, _ := flightsql.CreateStatementQueryTicket([]byte(cmd.GetQuery()))
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

var (
	SchemaListInt3     = arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.FixedSizeListOf(3, arrow.PrimitiveTypes.Int32), Nullable: true}}, nil)
	SchemaListInt      = arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.ListOf(arrow.PrimitiveTypes.Int32), Nullable: true}}, nil)
	SchemaLargeListInt = arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.LargeListOf(arrow.PrimitiveTypes.Int32), Nullable: true}}, nil)
	SchemaMapIntInt    = arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.MapOf(arrow.PrimitiveTypes.Int32, arrow.PrimitiveTypes.Int32), Nullable: true}}, nil)
)

func (server *DataTypeTestServer) DoGetStatement(ctx context.Context, tkt flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	var schema *arrow.Schema
	var record arrow.Record
	var err error

	cmd := string(tkt.GetStatementHandle())
	switch cmd {
	case "list[int, 3]":
		schema = SchemaListInt3
		record, _, err = array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"a": [1, 2, 3]}]`))
	case "list[int]":
		schema = SchemaListInt
		record, _, err = array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"a": [1]}]`))
	case "large_list[int]":
		schema = SchemaLargeListInt
		record, _, err = array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"a": [1]}]`))
	case "map[int]int":
		schema = SchemaMapIntInt
		record, _, err = array.RecordFromJSON(memory.DefaultAllocator, schema, strings.NewReader(`[{"a": null}]`))
	default:
		return nil, nil, fmt.Errorf("Unknown command: '%s'", cmd)
	}

	if err != nil {
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: record,
		}
	}()
	return schema, ch, nil
}

type DataTypeTests struct {
	ServerBasedTests
}

func (suite *DataTypeTests) SetupSuite() {
	suite.DoSetupSuite(&DataTypeTestServer{}, nil, map[string]string{})
}

func (suite *DataTypeTests) DoTestCase(name string, schema *arrow.Schema) {
	stmt, err := suite.cnxn.NewStatement()
	suite.NoError(err)
	defer stmt.Close()

	suite.NoError(stmt.SetSqlQuery(name))
	reader, _, err := stmt.ExecuteQuery(context.Background())
	suite.NoError(err)
	suite.Equal(reader.Schema(), schema)
	defer reader.Release()
}

func (suite *DataTypeTests) TestListInt3() {
	suite.DoTestCase("list[int, 3]", SchemaListInt3)
}

func (suite *DataTypeTests) TestLargeListInt() {
	suite.DoTestCase("large_list[int]", SchemaLargeListInt)
}

func (suite *DataTypeTests) TestListInt() {
	suite.DoTestCase("list[int]", SchemaListInt)
}

func (suite *DataTypeTests) TestMapIntInt() {
	suite.DoTestCase("map[int]int", SchemaMapIntInt)
}

// ---- Multi Table Tests --------------------

type MultiTableTestServer struct {
	flightsql.BaseServer
}

func (server *MultiTableTestServer) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	query := cmd.GetQuery()
	tkt, err := flightsql.CreateStatementQueryTicket([]byte(query))
	if err != nil {
		return nil, err
	}

	return &flight.FlightInfo{
		Endpoint:         []*flight.FlightEndpoint{{Ticket: &flight.Ticket{Ticket: tkt}}},
		FlightDescriptor: desc,
		TotalRecords:     -1,
		TotalBytes:       -1,
	}, nil
}

func (server *MultiTableTestServer) GetFlightInfoTables(ctx context.Context, cmd flightsql.GetTables, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}
	server.Alloc = memory.NewCheckedAllocator(memory.DefaultAllocator)
	info := &flight.FlightInfo{
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: desc.Cmd}},
		},
		FlightDescriptor: desc,
		Schema:           flight.SerializeSchema(schema, server.Alloc),
		TotalRecords:     -1,
		TotalBytes:       -1,
	}

	return info, nil
}

func (server *MultiTableTestServer) DoGetTables(ctx context.Context, cmd flightsql.GetTables) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	bldr := array.NewRecordBuilder(server.Alloc, adbc.GetTableSchemaSchema)

	bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"", ""}, nil)
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"", ""}, nil)
	bldr.Field(2).(*array.StringBuilder).AppendValues([]string{"tbl1", "tbl2"}, nil)
	bldr.Field(3).(*array.StringBuilder).AppendValues([]string{"", ""}, nil)

	sc1 := arrow.NewSchema([]arrow.Field{{Name: "a", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	sc2 := arrow.NewSchema([]arrow.Field{{Name: "b", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	buf1 := flight.SerializeSchema(sc1, server.Alloc)
	buf2 := flight.SerializeSchema(sc2, server.Alloc)

	bldr.Field(4).(*array.BinaryBuilder).AppendValues([][]byte{buf1, buf2}, nil)
	defer bldr.Release()

	rec := bldr.NewRecord()

	ch := make(chan flight.StreamChunk)
	go func() {
		defer close(ch)
		ch <- flight.StreamChunk{
			Data: rec,
			Desc: nil,
			Err:  nil,
		}
	}()
	return adbc.GetTableSchemaSchema, ch, nil
}

type MultiTableTests struct {
	ServerBasedTests
}

func (suite *MultiTableTests) SetupSuite() {
	suite.DoSetupSuite(&MultiTableTestServer{}, nil, map[string]string{})
}

// Regression test for https://github.com/apache/arrow-adbc/issues/934
func (suite *MultiTableTests) TestGetTableSchema() {
	actualSchema, err := suite.cnxn.GetTableSchema(context.Background(), nil, nil, "tbl2")
	suite.NoError(err)

	expectedSchema := arrow.NewSchema([]arrow.Field{{Name: "b", Type: arrow.PrimitiveTypes.Int32, Nullable: true}}, nil)
	suite.Equal(expectedSchema, actualSchema)
}
