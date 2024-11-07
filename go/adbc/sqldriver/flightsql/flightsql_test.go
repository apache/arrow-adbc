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
	"database/sql"
	"os"
	"testing"

	_ "github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/example"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/suite"
	"google.golang.org/grpc"
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
	suite.Require().NoError(err)

	suite.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	suite.s = flight.NewServerWithMiddleware(nil, suite.opts...)
	suite.Require().NoError(err)
	suite.srv, err = example.NewSQLiteFlightSQLServer(suite.sqliteDB)
	suite.Require().NoError(err)
	suite.srv.Alloc = suite.mem

	suite.s.RegisterFlightService(flightsql.NewFlightServer(suite.srv))
	suite.Require().NoError(suite.s.Init("localhost:0"))
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

func (suite *SQLDriverFlightSQLSuite) TestQuery() {
	db, err := sql.Open("flightsql", suite.dsn())
	suite.Require().NoError(err)
	defer db.Close()

	_, err = db.Exec("CREATE TABLE t (k, v)")
	suite.Require().NoError(err)
	result, err := db.Exec("INSERT INTO t (k, v) VALUES ('one', 'alpha'), ('two', 'bravo')")
	suite.Require().NoError(err)

	n, err := result.RowsAffected()
	suite.Require().NoError(err)
	suite.EqualValues(2, n)

	var expected int = 2
	var actual int
	row := db.QueryRow("SELECT count(*) FROM t")
	err = row.Scan(&actual)
	if suite.NoError(err) {
		suite.Equal(expected, actual)
	}
}

func TestSQLDriverFlightSQL(t *testing.T) {
	suite.Run(t, new(SQLDriverFlightSQLSuite))
}
