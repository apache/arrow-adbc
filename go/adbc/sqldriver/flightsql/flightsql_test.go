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
	"database/sql"
	_ "github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql"
	"github.com/apache/arrow/go/v12/arrow/flight"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v12/arrow/flight/flightsql/example"
	"github.com/apache/arrow/go/v12/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
	"os"
	"testing"
)

func Example() {
	// Be sure to import the driver first:
	// import _ "github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql"

	db, err := sql.Open("flightsql", "uri=grpc://localhost:12345")
	if err != nil {
		panic(err)
	}

	if err = db.Ping(); err != nil {
		panic(err)
	}
}

type SQLDriverFlightSQLSuite struct {
	suite.Suite

	srv      *example.SQLiteFlightSQLServer
	s        flight.Server
	opts     []grpc.ServerOption
	sqliteDB *sql.DB

	done chan bool
	mem  *memory.CheckedAllocator
}

func (suite *SQLDriverFlightSQLSuite) SetupTest() {
	var err error

	suite.sqliteDB, err = example.CreateDB()
	require.NoError(suite.T(), err)

	suite.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	suite.s = flight.NewServerWithMiddleware(nil, suite.opts...)
	require.NoError(suite.T(), err)
	suite.srv, err = example.NewSQLiteFlightSQLServer(suite.sqliteDB)
	require.NoError(suite.T(), err)
	suite.srv.Alloc = suite.mem

	suite.s.RegisterFlightService(flightsql.NewFlightServer(suite.srv))
	require.NoError(suite.T(), suite.s.Init("localhost:0"))
	suite.s.SetShutdownOnSignals(os.Interrupt, os.Kill)
	suite.done = make(chan bool)
	go func() {
		defer close(suite.done)
		_ = suite.s.Serve()
	}()
}

func (suite *SQLDriverFlightSQLSuite) TearDownTest() {
	if suite.done == nil {
		return
	}

	suite.s.Shutdown()
	<-suite.done
	suite.srv = nil
	suite.mem.AssertSize(suite.T(), 0)
	_ = suite.sqliteDB.Close()
	suite.done = nil
}

func (suite *SQLDriverFlightSQLSuite) dsn() string {
	return "uri=grpc+tcp://" + suite.s.Addr().String()
}

type srvQuery string

func (s srvQuery) GetQuery() string { return string(s) }

func (s srvQuery) GetTransactionId() []byte { return nil }

func (suite *SQLDriverFlightSQLSuite) TestQuery() {
	db, err := sql.Open("flightsql", suite.dsn())
	require.NoError(suite.T(), err)
	defer db.Close()

	_, err = suite.srv.DoPutCommandStatementUpdate(
		context.Background(), srvQuery("CREATE TABLE t (k, v)"))
	require.NoError(suite.T(), err)
	_, err = suite.srv.DoPutCommandStatementUpdate(
		context.Background(), srvQuery("INSERT INTO t (k, v) VALUES ('one', 'alpha'), ('two', 'bravo')"))
	require.NoError(suite.T(), err)

	// TODO db.Exec() fails:
	// Received unexpected error:
	// Invalid State: SqlState: , msg: [Flight SQL Statement] must call Prepare before GetParameterSchema
	//
	//_, err = db.Exec("CREATE TABLE t (k, v)")
	//require.NoError(suite.T(), err)
	//defer db.Exec("DROP TABLE IF EXISTS t")
	//_, err = db.Exec("INSERT INTO t (k, v) VALUES ('one', 'alpha'), ('two', 'bravo')")
	//require.NoError(suite.T(), err)

	var expected int = 2
	var actual int
	row := db.QueryRow("SELECT count(*) FROM t")
	err = row.Scan(&actual)
	if assert.NoError(suite.T(), err) {
		assert.Equal(suite.T(), expected, actual)
	}
}

func TestSQLDriverFlightSQL(t *testing.T) {
	suite.Run(t, new(SQLDriverFlightSQLSuite))
}
