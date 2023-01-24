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
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	driver "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/flight"
	"github.com/apache/arrow/go/v11/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v11/arrow/flight/flightsql/example"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/protobuf/proto"
)

type FlightSQLQuirks struct {
	srv *example.SQLiteFlightSQLServer
	s   flight.Server

	done chan bool
	mem  *memory.CheckedAllocator
}

func (s *FlightSQLQuirks) SetupDriver(t *testing.T) adbc.Driver {
	s.done = make(chan bool)
	var err error
	s.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	s.s = flight.NewServerWithMiddleware(nil)
	s.srv, err = example.NewSQLiteFlightSQLServer()
	require.NoError(t, err)
	s.srv.Alloc = s.mem

	s.s.RegisterFlightService(flightsql.NewFlightServer(s.srv))
	require.NoError(t, s.s.Init("localhost:0"))
	s.s.SetShutdownOnSignals(os.Interrupt, os.Kill)
	go func() {
		defer close(s.done)
		_ = s.s.Serve()
	}()

	return driver.Driver{Alloc: s.mem}
}

func (s *FlightSQLQuirks) TearDownDriver(t *testing.T, _ adbc.Driver) {
	s.s.Shutdown()
	<-s.done
	s.srv = nil
	s.mem.AssertSize(t, 0)
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
func (s *FlightSQLQuirks) SupportsTransactions() bool            { return false }
func (s *FlightSQLQuirks) SupportsGetParameterSchema() bool      { return false }
func (s *FlightSQLQuirks) SupportsDynamicParameterBinding() bool { return true }
func (s *FlightSQLQuirks) GetMetadata(code adbc.InfoCode) interface{} {
	switch code {
	case adbc.InfoDriverName:
		return "ADBC Flight SQL Driver - Go"
	// runtime/debug.ReadBuildInfo doesn't currently work for tests
	// github.com/golang/go/issues/33976
	case adbc.InfoDriverVersion:
		return ""
	case adbc.InfoDriverArrowVersion:
		return ""
	case adbc.InfoVendorName:
		return "db_name"
	case adbc.InfoVendorVersion:
		return "sqlite 3"
	case adbc.InfoVendorArrowVersion:
		return "11.0.0-SNAPSHOT"
	}

	return nil
}

func TestADBCFlightSQL(t *testing.T) {
	q := &FlightSQLQuirks{}
	suite.Run(t, &validation.DatabaseTests{Quirks: q})
	suite.Run(t, &validation.ConnectionTests{Quirks: q})
	suite.Run(t, &validation.StatementTests{Quirks: q})

	suite.Run(t, &PartitionTests{Quirks: q})
	suite.Run(t, &SSLTests{Quirks: q})
}

// Driver-specific tests

type SSLTests struct {
	suite.Suite

	Driver adbc.Driver
	Quirks validation.DriverQuirks
}

func (suite *SSLTests) SetupTest() {
	suite.Driver = suite.Quirks.SetupDriver(suite.T())
}

func (suite *SSLTests) TearDownTest() {
	suite.Quirks.TearDownDriver(suite.T(), suite.Driver)
	suite.Driver = nil
}

func (suite *SSLTests) TestMutualTLS() {
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

func (suite *SSLTests) TestOverrideHostname() {
	// Just checks that the option is accepted - doesn't actually configure TLS
	options := suite.Quirks.DatabaseOptions()
	options["adbc.flight.sql.client_option.tls_override_hostname"] = "hostname"
	_, err := suite.Driver.NewDatabase(options)
	suite.Require().NoError(err)
}

func (suite *SSLTests) TestRootCerts() {
	// Just checks that the option is accepted - doesn't actually configure TLS
	options := suite.Quirks.DatabaseOptions()
	options["adbc.flight.sql.client_option.tls_root_certs"] = "these are not valid certs"
	_, err := suite.Driver.NewDatabase(options)
	suite.Require().ErrorContains(err, "Invalid value for database option 'adbc.flight.sql.client_option.tls_root_certs': failed to append certificates")
}

func (suite *SSLTests) TestSkipVerify() {
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

func (suite *SSLTests) TestUnknownOption() {
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
