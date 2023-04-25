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

package flightsql_test

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"database/sql"
	"encoding/pem"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql/example"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type HeaderServerMiddleware struct {
	recordedHeaders metadata.MD
}

func (hm *HeaderServerMiddleware) StartCall(ctx context.Context) context.Context {
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		hm.recordedHeaders = metadata.Join(md, hm.recordedHeaders)
	}
	return ctx
}

func (hm *HeaderServerMiddleware) CallCompleted(context.Context, error) {}

type FlightSQLQuirks struct {
	srv    *example.SQLiteFlightSQLServer
	s      flight.Server
	middle HeaderServerMiddleware
	opts   []grpc.ServerOption
	db     *sql.DB

	done chan bool
	mem  *memory.CheckedAllocator
}

func (s *FlightSQLQuirks) SetupDriver(t *testing.T) adbc.Driver {
	s.middle.recordedHeaders = make(metadata.MD)

	var err error
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.s = flight.NewServerWithMiddleware([]flight.ServerMiddleware{flight.CreateServerMiddleware(&s.middle)}, s.opts...)
	require.NoError(t, err)
	s.srv, err = example.NewSQLiteFlightSQLServer(s.db)
	require.NoError(t, err)
	s.srv.Alloc = s.mem

	s.s.RegisterFlightService(flightsql.NewFlightServer(s.srv))
	require.NoError(t, s.s.Init("localhost:0"))
	s.s.SetShutdownOnSignals(os.Interrupt, os.Kill)
	s.done = make(chan bool)
	go func() {
		defer close(s.done)
		_ = s.s.Serve()
	}()

	return driver.Driver{Alloc: s.mem}
}

func (s *FlightSQLQuirks) TearDownDriver(t *testing.T, _ adbc.Driver) {
	if s.done == nil {
		return
	}

	s.s.Shutdown()
	<-s.done
	s.srv = nil
	s.mem.AssertSize(t, 0)
	s.done = nil
}

func (s *FlightSQLQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		adbc.OptionKeyURI: "grpc+tcp://" + s.s.Addr().String(),
	}
}

func (s *FlightSQLQuirks) getSqlTypeFromArrowType(dt arrow.DataType) string {
	switch dt.ID() {
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64:
		return "integer"
	case arrow.FLOAT32:
		return "float"
	case arrow.FLOAT64:
		return "double"
	case arrow.STRING:
		return "text"
	default:
		return ""
	}
}

type srvQuery string

func (s srvQuery) GetQuery() string { return string(s) }

func (s srvQuery) GetTransactionId() []byte { return nil }

func writeTo(arr arrow.Array, idx int, w io.Writer) {
	switch arr := arr.(type) {
	case *array.Int8:
		fmt.Fprint(w, arr.Value(idx))
	case *array.Uint8:
		fmt.Fprint(w, arr.Value(idx))
	case *array.Int16:
		fmt.Fprint(w, arr.Value(idx))
	case *array.Uint16:
		fmt.Fprint(w, arr.Value(idx))
	case *array.Int32:
		fmt.Fprint(w, arr.Value(idx))
	case *array.Uint32:
		fmt.Fprint(w, arr.Value(idx))
	case *array.Int64:
		fmt.Fprint(w, arr.Value(idx))
	case *array.Uint64:
		fmt.Fprint(w, arr.Value(idx))
	case *array.Float32:
		fmt.Fprint(w, arr.Value(idx))
	case *array.Float64:
		fmt.Fprint(w, arr.Value(idx))
	case *array.String:
		fmt.Fprintf(w, `"%s"`, arr.Value(idx))
	}
}

func (s *FlightSQLQuirks) CreateSampleTable(tableName string, r arrow.Record) error {
	var b strings.Builder
	b.WriteString("CREATE TABLE ")
	b.WriteString(tableName)
	b.WriteString(" (")
	for i := 0; i < int(r.NumCols()); i++ {
		if i != 0 {
			b.WriteString(", ")
		}
		f := r.Schema().Field(i)
		b.WriteString(f.Name)
		b.WriteByte(' ')
		b.WriteString(s.getSqlTypeFromArrowType(f.Type))
	}
	b.WriteString(")")

	_, err := s.srv.DoPutCommandStatementUpdate(context.Background(), srvQuery(b.String()))
	if err != nil {
		return err
	}

	insertQueryPrefix := "INSERT INTO " + tableName + " VALUES ("
	for i := 0; i < int(r.NumRows()); i++ {
		b.Reset()
		b.WriteString(insertQueryPrefix)

		for j := 0; j < int(r.NumCols()); j++ {
			if j != 0 {
				b.WriteString(", ")
			}

			col := r.Column(j)
			writeTo(col, j, &b)
		}

		b.WriteString(")")
		_, err := s.srv.DoPutCommandStatementUpdate(context.Background(), srvQuery(b.String()))
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *FlightSQLQuirks) Alloc() memory.Allocator               { return s.mem }
func (s *FlightSQLQuirks) BindParameter(_ int) string            { return "?" }
func (s *FlightSQLQuirks) SupportsConcurrentStatements() bool    { return true }
func (s *FlightSQLQuirks) SupportsPartitionedData() bool         { return true }
func (s *FlightSQLQuirks) SupportsTransactions() bool            { return true }
func (s *FlightSQLQuirks) SupportsGetParameterSchema() bool      { return false }
func (s *FlightSQLQuirks) SupportsDynamicParameterBinding() bool { return true }
func (s *FlightSQLQuirks) GetMetadata(code adbc.InfoCode) interface{} {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Flight SQL Driver - Go"
	// runtime/debug.ReadBuildInfo doesn't currently work for tests
	// github.com/golang/go/issues/33976
	case adbc.InfoDriverVersion:
		return "(unknown or development build)"
	case adbc.InfoDriverArrowVersion:
		return "(unknown or development build)"
	case adbc.InfoVendorName:
		return "db_name"
	case adbc.InfoVendorVersion:
		return "sqlite 3"
	case adbc.InfoVendorArrowVersion:
		return "12.0.0-SNAPSHOT"
	}

	return nil
}

func TestADBCFlightSQL(t *testing.T) {
	db, err := example.CreateDB()
	require.NoError(t, err)
	defer db.Close()

	q := &FlightSQLQuirks{db: db}
	suite.Run(t, &validation.DatabaseTests{Quirks: q})
	suite.Run(t, &validation.ConnectionTests{Quirks: q})
	suite.Run(t, &validation.StatementTests{Quirks: q})

	suite.Run(t, &DefaultDialOptionsTests{Quirks: q})
	suite.Run(t, &HeaderTests{Quirks: q})
	suite.Run(t, &AuthnTests{})
	suite.Run(t, &OptionTests{Quirks: q})
	suite.Run(t, &PartitionTests{Quirks: q})
	suite.Run(t, &StatementTests{Quirks: q})
	suite.Run(t, &TimeoutTestSuite{})
	suite.Run(t, &TLSTests{Quirks: &FlightSQLQuirks{db: db}})
	suite.Run(t, &ConnectionTests{})
	suite.Run(t, &DomainSocketTests{db: db})
}

// Driver-specific tests

type DefaultDialOptionsTests struct {
	suite.Suite

	Driver adbc.Driver
	Quirks validation.DriverQuirks

	ctx context.Context
	DB  adbc.Database
}

func (suite *DefaultDialOptionsTests) SetupSuite() {
	suite.Driver = suite.Quirks.SetupDriver(suite.T())

	var err error
	suite.ctx = context.Background()
	suite.DB, err = suite.Driver.NewDatabase(suite.Quirks.DatabaseOptions())
	suite.NoError(err)

	cnxn, err := suite.DB.Open(suite.ctx)
	suite.NoError(err)
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	suite.NoError(err)
	defer stmt.Close()

	// Construct huge table
	suite.NoError(stmt.SetSqlQuery("CREATE TABLE huge (str)"))
	_, err = stmt.ExecuteUpdate(suite.ctx)
	suite.NoError(err)

	// 4 KiB
	suite.NoError(stmt.SetSqlQuery("INSERT INTO huge (str) VALUES (printf('%.*c', 4096, '!'))"))
	_, err = stmt.ExecuteUpdate(suite.ctx)
	suite.NoError(err)

	// 4 MiB
	suite.NoError(stmt.SetSqlQuery("INSERT INTO huge (str) SELECT * FROM huge"))
	for i := 0; i < 10; i++ {
		_, err = stmt.ExecuteUpdate(suite.ctx)
		suite.NoError(err)
	}
}

func (suite *DefaultDialOptionsTests) TearDownSuite() {
	suite.Quirks.TearDownDriver(suite.T(), suite.Driver)
	suite.DB = nil
	suite.Driver = nil
}

func (suite *DefaultDialOptionsTests) TestMaxIncomingMessageSizeDefault() {
	opts := suite.Quirks.DatabaseOptions()
	opts["adbc.flight.sql.client_option.with_max_msg_size"] = "1000000"
	db, err := suite.Driver.NewDatabase(opts)
	suite.NoError(err)

	cnxn, err := db.Open(suite.ctx)
	suite.NoError(err)
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	suite.NoError(err)
	defer stmt.Close()

	suite.NoError(stmt.SetSqlQuery("SELECT * FROM huge"))
	reader, _, err := stmt.ExecuteQuery(suite.ctx)
	suite.NoError(err)
	defer reader.Release()

	for reader.Next() {
	}
	suite.ErrorContains(reader.Err(), "received message larger than max")
}

func (suite *DefaultDialOptionsTests) TestMaxIncomingMessageSizeLow() {
	cnxn, err := suite.DB.Open(suite.ctx)
	suite.NoError(err)
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	suite.NoError(err)
	defer stmt.Close()

	suite.NoError(stmt.SetSqlQuery("SELECT * FROM huge"))
	reader, _, err := stmt.ExecuteQuery(suite.ctx)
	suite.NoError(err)
	defer reader.Release()

	for reader.Next() {
	}
	suite.NoError(reader.Err())
}

type OptionTests struct {
	suite.Suite

	Driver adbc.Driver
	Quirks validation.DriverQuirks
}

func (suite *OptionTests) SetupTest() {
	suite.Driver = suite.Quirks.SetupDriver(suite.T())
}

func (suite *OptionTests) TearDownTest() {
	suite.Quirks.TearDownDriver(suite.T(), suite.Driver)
	suite.Driver = nil
}

func (suite *OptionTests) TestMutualTLS() {
	// Just checks that the option is accepted - doesn't actually configure TLS
	options := suite.Quirks.DatabaseOptions()

	options["adbc.flight.sql.client_option.mtls_cert_chain"] = "certs"
	_, err := suite.Driver.NewDatabase(options)
	suite.Require().ErrorContains(err, "Must provide both")

	options["adbc.flight.sql.client_option.mtls_private_key"] = "key"
	_, err = suite.Driver.NewDatabase(options)
	suite.Require().ErrorContains(err, "Invalid mTLS certificate")

	delete(options, "adbc.flight.sql.client_option.mtls_cert_chain")
	_, err = suite.Driver.NewDatabase(options)
	suite.Require().ErrorContains(err, "Must provide both")
}

func (suite *OptionTests) TestOverrideHostname() {
	// Just checks that the option is accepted - doesn't actually configure TLS
	options := suite.Quirks.DatabaseOptions()
	options["adbc.flight.sql.client_option.tls_override_hostname"] = "hostname"
	_, err := suite.Driver.NewDatabase(options)
	suite.Require().NoError(err)
}

func (suite *OptionTests) TestRootCerts() {
	// Just checks that the option is accepted - doesn't actually configure TLS
	options := suite.Quirks.DatabaseOptions()
	options["adbc.flight.sql.client_option.tls_root_certs"] = "these are not valid certs"
	_, err := suite.Driver.NewDatabase(options)
	suite.Require().ErrorContains(err, "Invalid value for database option 'adbc.flight.sql.client_option.tls_root_certs': failed to append certificates")
}

func (suite *OptionTests) TestSkipVerify() {
	options := suite.Quirks.DatabaseOptions()
	options["adbc.flight.sql.client_option.tls_skip_verify"] = "true"
	_, err := suite.Driver.NewDatabase(options)
	suite.Require().NoError(err)

	options = suite.Quirks.DatabaseOptions()
	options["adbc.flight.sql.client_option.tls_skip_verify"] = "false"
	_, err = suite.Driver.NewDatabase(options)
	suite.Require().NoError(err)

	options = suite.Quirks.DatabaseOptions()
	options["adbc.flight.sql.client_option.tls_skip_verify"] = "invalid"
	_, err = suite.Driver.NewDatabase(options)
	suite.Require().ErrorContains(err, "Invalid value for database option 'adbc.flight.sql.client_option.tls_skip_verify': 'invalid'")
}

func (suite *OptionTests) TestUnknownOption() {
	options := suite.Quirks.DatabaseOptions()
	options["unknown option"] = "unknown value"
	_, err := suite.Driver.NewDatabase(options)
	suite.Require().ErrorContains(err, "Unknown database option 'unknown option'")
}

type PartitionTests struct {
	suite.Suite

	Driver adbc.Driver
	Quirks validation.DriverQuirks

	DB   adbc.Database
	Cnxn adbc.Connection
	ctx  context.Context
}

func (suite *PartitionTests) SetupTest() {
	suite.Driver = suite.Quirks.SetupDriver(suite.T())
	var err error
	suite.DB, err = suite.Driver.NewDatabase(suite.Quirks.DatabaseOptions())
	suite.Require().NoError(err)
	suite.ctx = context.Background()
	suite.Cnxn, err = suite.DB.Open(suite.ctx)
	suite.Require().NoError(err)
}

func (suite *PartitionTests) TearDownTest() {
	suite.Require().NoError(suite.Cnxn.Close())
	suite.Quirks.TearDownDriver(suite.T(), suite.Driver)
	suite.Cnxn = nil
	suite.DB = nil
	suite.Driver = nil
}

func (suite *PartitionTests) TestIntrospectPartitions() {
	stmt, err := suite.Cnxn.NewStatement()
	suite.Require().NoError(err)
	defer stmt.Close()

	suite.Require().NoError(stmt.SetSqlQuery("SELECT 42"))

	_, partitions, _, err := stmt.ExecutePartitions(context.Background())
	suite.Require().NoError(err)
	suite.Require().Equal(uint64(1), partitions.NumPartitions)

	info := &flight.FlightInfo{}
	suite.Require().NoError(proto.Unmarshal(partitions.PartitionIDs[0], info))
	suite.Require().Equal(int64(-1), info.TotalBytes)
	suite.Require().Equal(int64(-1), info.TotalRecords)
	suite.Require().Equal(1, len(info.Endpoint))
	suite.Require().Equal(0, len(info.Endpoint[0].Location))
}

type StatementTests struct {
	suite.Suite

	Driver adbc.Driver
	Quirks validation.DriverQuirks

	DB   adbc.Database
	Cnxn adbc.Connection
	Stmt adbc.Statement
	ctx  context.Context
}

func (suite *StatementTests) SetupTest() {
	suite.Driver = suite.Quirks.SetupDriver(suite.T())
	var err error
	suite.DB, err = suite.Driver.NewDatabase(suite.Quirks.DatabaseOptions())

	suite.Require().NoError(err)
	suite.ctx = context.Background()
	suite.Cnxn, err = suite.DB.Open(suite.ctx)
	suite.Require().NoError(err)
	suite.Stmt, err = suite.Cnxn.NewStatement()
	suite.Require().NoError(err)
}

func (suite *StatementTests) TearDownTest() {
	suite.Require().NoError(suite.Stmt.Close())
	suite.Require().NoError(suite.Cnxn.Close())
	suite.Quirks.TearDownDriver(suite.T(), suite.Driver)
	suite.Cnxn = nil
	suite.DB = nil
	suite.Driver = nil
}

func (suite *StatementTests) TestQueueSizeOption() {
	var err error
	option := "adbc.flight.sql.rpc.queue_size"

	err = suite.Stmt.SetOption(option, "")
	suite.Require().ErrorContains(err, "Invalid value for statement option 'adbc.flight.sql.rpc.queue_size': '' is not a positive integer")

	err = suite.Stmt.SetOption(option, "foo")
	suite.Require().ErrorContains(err, "Invalid value for statement option 'adbc.flight.sql.rpc.queue_size': 'foo' is not a positive integer")

	err = suite.Stmt.SetOption(option, "-1")
	suite.Require().ErrorContains(err, "Invalid value for statement option 'adbc.flight.sql.rpc.queue_size': '-1' is not a positive integer")

	err = suite.Stmt.SetOption(option, "1")
	suite.Require().NoError(err)
}

func (suite *StatementTests) TestSubstrait() {
	err := suite.Stmt.SetSubstraitPlan([]byte("foo"))
	suite.Require().NoError(err)

	_, _, err = suite.Stmt.ExecuteQuery(context.Background())
	suite.Require().ErrorContains(err, "not implemented")
	var adbcError adbc.Error
	suite.ErrorAs(err, &adbcError)
	suite.Equal(adbc.StatusNotImplemented, adbcError.Code)
}

func (suite *StatementTests) TestUnknownOption() {
	err := suite.Stmt.SetOption("unknown option", "")
	suite.Require().ErrorContains(err, "Unknown statement option 'unknown option'")
}

type HeaderTests struct {
	suite.Suite

	Driver adbc.Driver
	Quirks *FlightSQLQuirks

	DB   adbc.Database
	Cnxn adbc.Connection
	ctx  context.Context
}

func (suite *HeaderTests) SetupTest() {
	suite.Driver = suite.Quirks.SetupDriver(suite.T())
	var err error
	opts := suite.Quirks.DatabaseOptions()
	opts[driver.OptionAuthorizationHeader] = "auth-header-token"
	opts["adbc.flight.sql.rpc.call_header.x-header-one"] = "value 1"
	suite.DB, err = suite.Driver.NewDatabase(opts)

	suite.Require().NoError(err)
	suite.ctx = context.Background()
	suite.Cnxn, err = suite.DB.Open(suite.ctx)
	suite.Require().NoError(err)
}

func (suite *HeaderTests) TearDownTest() {
	suite.Require().NoError(suite.Cnxn.Close())
	suite.Quirks.TearDownDriver(suite.T(), suite.Driver)
	suite.Cnxn = nil
	suite.DB = nil
	suite.Driver = nil
}

func (suite *HeaderTests) TestDatabaseOptAuthorization() {
	stmt, err := suite.Cnxn.NewStatement()
	suite.Require().NoError(err)
	defer stmt.Close()

	suite.Require().NoError(stmt.SetSqlQuery("timeout"))
	_, _, err = stmt.ExecuteQuery(suite.ctx)
	suite.Error(err)

	suite.Contains(suite.Quirks.middle.recordedHeaders.Get("authorization"), "auth-header-token")
}

func (suite *HeaderTests) TestUserAgent() {
	stmt, err := suite.Cnxn.NewStatement()
	suite.Require().NoError(err)
	defer func() {
		suite.Require().NoError(stmt.Close())
	}()

	suite.Require().NoError(stmt.SetSqlQuery("timeout"))
	_, _, err = stmt.ExecuteQuery(suite.ctx)
	suite.Error(err)

	userAgents := suite.Quirks.middle.recordedHeaders.Get("user-agent")
	suite.NotEmpty(userAgents)
	for _, agent := range userAgents {
		suite.Contains(agent, "ADBC Flight SQL Driver")
		suite.Contains(agent, "grpc-go")
	}
}

func (suite *HeaderTests) TestConnection() {
	// can't change authorization header on connection, you have to set it
	// as an option on the database object when creating the connection.
	suite.Require().Error(suite.Cnxn.(adbc.PostInitOptions).
		SetOption(driver.OptionAuthorizationHeader, "auth-cnxn-token"))
	suite.Require().NoError(suite.Cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.call_header.x-span-id", "my span id"))

	stmt, err := suite.Cnxn.NewStatement()
	suite.Require().NoError(err)
	defer stmt.Close()

	suite.Require().NoError(suite.Cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.call_header.x-user-agent", "Flight SQL ADBC"))

	suite.Require().NoError(stmt.SetSqlQuery("timeout"))
	_, _, err = stmt.ExecuteQuery(suite.ctx)
	suite.Error(err)

	suite.Contains(suite.Quirks.middle.recordedHeaders.Get("authorization"), "auth-header-token")
	suite.Contains(suite.Quirks.middle.recordedHeaders.Get("x-span-id"), "my span id")
	// adding a header to the connection *after* constructing a statement does not
	// propagate to the statement. This is important, for example, if you wanted
	// different open telemetry headers on different statements of a connection.
	suite.NotContains(suite.Quirks.middle.recordedHeaders, "x-user-agent")
}

func (suite *HeaderTests) TestStatement() {
	// inherit connection headers
	suite.Require().NoError(suite.Cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.call_header.x-span-id", "my span id"))

	stmt, err := suite.Cnxn.NewStatement()
	suite.Require().NoError(err)
	defer stmt.Close()

	// changes do not propagate to previously created statements
	suite.Require().NoError(suite.Cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.call_header.x-span-id", "super span id"))

	suite.Require().NoError(stmt.SetSqlQuery("timeout"))
	_, _, err = stmt.ExecuteQuery(suite.ctx)
	suite.Error(err)
	suite.Contains(suite.Quirks.middle.recordedHeaders.Get("x-span-id"), "my span id")
	suite.Quirks.middle.recordedHeaders = metadata.MD{}

	// set to empty string to remove it
	suite.Require().NoError(stmt.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.call_header.x-span-id", ""))
	_, _, err = stmt.ExecuteQuery(suite.ctx)
	suite.Error(err)
	suite.NotContains(suite.Quirks.middle.recordedHeaders, "x-span-id")
	suite.Quirks.middle.recordedHeaders = metadata.MD{}

	suite.Require().NoError(stmt.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.call_header.x-span-id", "my span id"))

	_, _, err = stmt.ExecuteQuery(suite.ctx)
	suite.Error(err)
	suite.Contains(suite.Quirks.middle.recordedHeaders.Get("x-span-id"), "my span id")
	suite.Quirks.middle.recordedHeaders = metadata.MD{}
}

func (suite *HeaderTests) TestCombined() {
	suite.Require().NoError(suite.Cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.call_header.x-header-two", "value 2"))

	stmt, err := suite.Cnxn.NewStatement()
	suite.Require().NoError(err)
	defer stmt.Close()

	suite.Require().NoError(stmt.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.call_header.x-header-three", "value 3"))
	suite.Require().NoError(stmt.SetSqlQuery("timeout"))
	_, _, err = stmt.ExecuteQuery(suite.ctx)
	suite.Error(err)

	// added in setuptest for database options
	suite.Contains(suite.Quirks.middle.recordedHeaders.Get("x-header-one"), "value 1")
	suite.Contains(suite.Quirks.middle.recordedHeaders.Get("x-header-two"), "value 2")
	suite.Contains(suite.Quirks.middle.recordedHeaders.Get("x-header-three"), "value 3")
}

func (suite *HeaderTests) TestPrepared() {
	stmt, err := suite.Cnxn.NewStatement()
	suite.Require().NoError(err)

	suite.Require().NoError(stmt.SetSqlQuery("timeout"))

	suite.Require().NoError(suite.Cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.call_header.x-header-one", "value 1"))
	suite.Require().NoError(stmt.Prepare(suite.ctx))

	suite.Require().NoError(stmt.SetOption("adbc.flight.sql.rpc.call_header.x-header-two", "value 2"))
	suite.Require().NoError(stmt.Close())

	suite.Contains(suite.Quirks.middle.recordedHeaders.Get("x-header-one"), "value 1")
	suite.Contains(suite.Quirks.middle.recordedHeaders.Get("x-header-two"), "value 2")
}

type AuthnTests struct {
	suite.Suite

	s    flight.Server
	db   adbc.Database
	cnxn adbc.Connection
}

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

func (suite *AuthnTests) SetupSuite() {
	suite.s = flight.NewServerWithMiddleware([]flight.ServerMiddleware{
		{Stream: authnTestStream, Unary: authnTestUnary},
	})
	suite.s.RegisterFlightService(flightsql.NewFlightServer(&AuthnTestServer{}))
	suite.Require().NoError(suite.s.Init("localhost:0"))
	suite.s.SetShutdownOnSignals(os.Interrupt, os.Kill)
	go func() {
		_ = suite.s.Serve()
	}()

	uri := "grpc+tcp://" + suite.s.Addr().String()
	var err error
	suite.db, err = (driver.Driver{}).NewDatabase(map[string]string{
		"uri":                            uri,
		driver.OptionAuthorizationHeader: "Bearer initial",
	})
	suite.Require().NoError(err)
}

func (suite *AuthnTests) SetupTest() {
	var err error
	suite.cnxn, err = suite.db.Open(context.Background())
	suite.Require().NoError(err)
}

func (suite *AuthnTests) TearDownTest() {
	suite.Require().NoError(suite.cnxn.Close())
}

func (suite *AuthnTests) TearDownSuite() {
	suite.db = nil
	suite.s.Shutdown()
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

type TimeoutTestSuite struct {
	suite.Suite

	s    flight.Server
	db   adbc.Database
	cnxn adbc.Connection
}

func (ts *TimeoutTestSuite) SetupSuite() {
	ts.s = flight.NewServerWithMiddleware(nil)
	ts.s.RegisterFlightService(flightsql.NewFlightServer(&TimeoutTestServer{}))
	ts.Require().NoError(ts.s.Init("localhost:0"))
	ts.s.SetShutdownOnSignals(os.Interrupt, os.Kill)
	go func() {
		_ = ts.s.Serve()
	}()

	uri := "grpc+tcp://" + ts.s.Addr().String()
	var err error
	ts.db, err = (driver.Driver{}).NewDatabase(map[string]string{
		"uri": uri,
	})
	ts.Require().NoError(err)
}

func (ts *TimeoutTestSuite) SetupTest() {
	var err error
	ts.cnxn, err = ts.db.Open(context.Background())
	ts.Require().NoError(err)
}

func (ts *TimeoutTestSuite) TearDownTest() {
	ts.Require().NoError(ts.cnxn.Close())
}

func (ts *TimeoutTestSuite) TearDownSuite() {
	ts.db = nil
	ts.s.Shutdown()
}

func (ts *TimeoutTestSuite) TestInvalidValues() {
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

func (ts *TimeoutTestSuite) TestRemoveTimeout() {
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

func (ts *TimeoutTestSuite) TestDoActionTimeout() {
	ts.NoError(ts.cnxn.(adbc.PostInitOptions).
		SetOption("adbc.flight.sql.rpc.timeout_seconds.update", "0.1"))

	stmt, err := ts.cnxn.NewStatement()
	ts.Require().NoError(err)
	defer stmt.Close()

	ts.Require().NoError(stmt.SetSqlQuery("fetch"))
	var adbcErr adbc.Error
	ts.ErrorAs(stmt.Prepare(context.Background()), &adbcErr)
	ts.Equal(adbc.StatusTimeout, adbcErr.Code, adbcErr.Error())
}

func (ts *TimeoutTestSuite) TestDoGetTimeout() {
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

func (ts *TimeoutTestSuite) TestDoPutTimeout() {
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

func (ts *TimeoutTestSuite) TestGetFlightInfoTimeout() {
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

func (ts *TimeoutTestSuite) TestDontTimeout() {
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

type TLSTests struct {
	suite.Suite

	Driver adbc.Driver
	Quirks *FlightSQLQuirks

	DB   adbc.Database
	Cnxn adbc.Connection
	Stmt adbc.Statement
	ctx  context.Context
}

func (suite *TLSTests) SetupTest() {
	var err error

	// Generate a self-signed certificate in-process for testing
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	suite.Require().NoError(err)
	certTemplate := x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			Organization: []string{"Unit Tests Incorporated"},
		},
		IPAddresses:           []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
		NotBefore:             time.Now(),
		NotAfter:              time.Now().Add(time.Hour),
		KeyUsage:              x509.KeyUsageKeyEncipherment,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
	}
	certDer, err := x509.CreateCertificate(rand.Reader, &certTemplate, &certTemplate, &privKey.PublicKey, privKey)
	suite.Require().NoError(err)
	buffer := &bytes.Buffer{}
	suite.Require().NoError(pem.Encode(buffer, &pem.Block{Type: "CERTIFICATE", Bytes: certDer}))
	certBytes := make([]byte, buffer.Len())
	copy(certBytes, buffer.Bytes())
	buffer.Reset()
	suite.Require().NoError(pem.Encode(buffer, &pem.Block{Type: "RSA PRIVATE KEY", Bytes: x509.MarshalPKCS1PrivateKey(privKey)}))
	keyBytes := make([]byte, buffer.Len())
	copy(keyBytes, buffer.Bytes())

	cert, err := tls.X509KeyPair(certBytes, keyBytes)
	suite.Require().NoError(err)

	suite.Require().NoError(err)
	tlsConfig := &tls.Config{Certificates: []tls.Certificate{cert}}
	tlsCreds := credentials.NewTLS(tlsConfig)
	suite.Quirks.opts = []grpc.ServerOption{grpc.Creds(tlsCreds)}

	suite.Driver = suite.Quirks.SetupDriver(suite.T())
	suite.DB, err = suite.Driver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: "grpc+tls://" + suite.Quirks.s.Addr().String(),
		"adbc.flight.sql.client_option.tls_skip_verify": "true",
	})

	suite.Require().NoError(err)
	suite.ctx = context.Background()
	suite.Cnxn, err = suite.DB.Open(suite.ctx)
	suite.Require().NoError(err)
	suite.Stmt, err = suite.Cnxn.NewStatement()
	suite.Require().NoError(err)
}

func (suite *TLSTests) TearDownTest() {
	suite.Require().NoError(suite.Stmt.Close())
	suite.Require().NoError(suite.Cnxn.Close())
	suite.Quirks.TearDownDriver(suite.T(), suite.Driver)
	suite.Cnxn = nil
	suite.DB = nil
	suite.Driver = nil
}

func (suite *TLSTests) TestSimpleQuery() {
	suite.NoError(suite.Stmt.SetSqlQuery("SELECT 1"))
	reader, _, err := suite.Stmt.ExecuteQuery(suite.ctx)
	suite.NoError(err)
	defer reader.Release()

	for reader.Next() {
	}
	suite.NoError(reader.Err())
}

func (suite *TLSTests) TestInvalidOptions() {
	db, err := suite.Driver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: "grpc+tls://" + suite.Quirks.s.Addr().String(),
		"adbc.flight.sql.client_option.tls_skip_verify": "false",
	})
	suite.Require().NoError(err)

	cnxn, err := db.Open(suite.ctx)
	suite.Require().NoError(err)
	stmt, err := cnxn.NewStatement()
	suite.Require().NoError(err)

	suite.NoError(stmt.SetSqlQuery("SELECT 1"))
	_, _, err = stmt.ExecuteQuery(suite.ctx)
	suite.Contains(err.Error(), "Unavailable")
}

type ConnectionTests struct {
	suite.Suite

	alloc   *memory.CheckedAllocator
	server  flight.Server
	service *flight.BaseFlightServer

	Driver adbc.Driver
	DB     adbc.Database
	Cnxn   adbc.Connection
	ctx    context.Context
}

func (suite *ConnectionTests) SetupSuite() {
	suite.alloc = memory.NewCheckedAllocator(memory.DefaultAllocator)

	suite.server = flight.NewServerWithMiddleware(nil)
	suite.NoError(suite.server.Init("localhost:0"))
	suite.service = &flight.BaseFlightServer{}
	suite.server.RegisterFlightService(suite.service)

	go func() {
		// Explicitly ignore error
		_ = suite.server.Serve()
	}()

	var err error
	suite.ctx = context.Background()
	suite.Driver = driver.Driver{Alloc: suite.alloc}
	suite.DB, err = suite.Driver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: "grpc+tcp://" + suite.server.Addr().String(),
	})
	suite.Require().NoError(err)
	suite.Cnxn, err = suite.DB.Open(suite.ctx)
	suite.Require().NoError(err)
}

func (suite *ConnectionTests) TearDownSuite() {
	suite.server.Shutdown()
	suite.alloc.AssertSize(suite.T(), 0)
}

func (suite *ConnectionTests) TestGetInfo() {
	reader, err := suite.Cnxn.GetInfo(suite.ctx, []adbc.InfoCode{})
	// Should not error, even though server does not implement GetInfo
	suite.Require().NoError(err)
	defer reader.Release()

	driverName := false
	driverVersion := false
	driverArrowVersion := false
	for reader.Next() {
		code := reader.Record().Column(0).(*array.Uint32)
		values := reader.Record().Column(1).(*array.DenseUnion)
		stringValues := values.Field(0).(*array.String)
		for i := 0; i < int(reader.Record().NumRows()); i++ {
			switch adbc.InfoCode(code.Value(i)) {
			case adbc.InfoDriverName:
				{
					driverName = true
					suite.Require().Equal("ADBC Flight SQL Driver - Go", stringValues.Value(int(values.ValueOffset(i))))
				}
			case adbc.InfoDriverVersion:
				{
					driverVersion = true
					// Can't assert on value here since test won't have debug.ReadBuildInfo
				}
			case adbc.InfoDriverArrowVersion:
				{
					driverArrowVersion = true
					// Can't assert on value here since test won't have debug.ReadBuildInfo
				}
			}
		}
	}
	suite.Require().NoError(reader.Err())

	suite.Require().True(driverName)
	suite.Require().True(driverVersion)
	suite.Require().True(driverArrowVersion)
}

type DomainSocketTests struct {
	suite.Suite

	alloc   *memory.CheckedAllocator
	server  flight.Server
	service *example.SQLiteFlightSQLServer
	db      *sql.DB

	Driver adbc.Driver
	DB     adbc.Database
	Cnxn   adbc.Connection
	Stmt   adbc.Statement
	ctx    context.Context
}

func (suite *DomainSocketTests) SetupSuite() {
	suite.alloc = memory.NewCheckedAllocator(memory.DefaultAllocator)

	tempDir, err := os.MkdirTemp("", "adbc-flight-sql-tests-*")
	suite.NoError(err)
	defer os.RemoveAll(tempDir)

	listenSocket := filepath.Join(tempDir, "adbc.sock")

	listener, err := net.Listen("unix", listenSocket)
	suite.NoError(err)

	suite.server = flight.NewServerWithMiddleware(nil)
	suite.service, err = example.NewSQLiteFlightSQLServer(suite.db)
	suite.NoError(err)
	suite.server.RegisterFlightService(flightsql.NewFlightServer(suite.service))
	suite.server.InitListener(listener)

	go func() {
		// Explicitly ignore error
		_ = suite.server.Serve()
	}()

	suite.ctx = context.Background()
	suite.Driver = driver.Driver{Alloc: suite.alloc}
	suite.DB, err = suite.Driver.NewDatabase(map[string]string{
		adbc.OptionKeyURI: "grpc+unix://" + listenSocket,
	})
	suite.Require().NoError(err)
	suite.Cnxn, err = suite.DB.Open(suite.ctx)
	suite.Require().NoError(err)
	suite.Stmt, err = suite.Cnxn.NewStatement()
	suite.Require().NoError(err)
}

func (suite *DomainSocketTests) TearDownSuite() {
	suite.Require().NoError(suite.Stmt.Close())
	suite.Require().NoError(suite.Cnxn.Close())
	suite.server.Shutdown()
	suite.alloc.AssertSize(suite.T(), 0)
}

func (suite *DomainSocketTests) TestSimpleQueryDomainSocket() {
	suite.NoError(suite.Stmt.SetSqlQuery("SELECT 1"))
	reader, _, err := suite.Stmt.ExecuteQuery(suite.ctx)
	suite.NoError(err)
	defer reader.Release()

	for reader.Next() {
	}
	suite.NoError(reader.Err())
}
