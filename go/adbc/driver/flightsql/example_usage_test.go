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

// RECIPE STARTS HERE

// Tests that use the SQLite server example.

package flightsql_test

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"log"

	"github.com/apache/arrow-adbc/go/adbc"
	drv "github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	sqlite "github.com/apache/arrow-go/v18/arrow/flight/flightsql/example"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "modernc.org/sqlite"
)

var headers = map[string]string{"foo": "bar"}

func FlightSQLExample(uri string) (err error) {
	ctx := context.Background()
	options := map[string]string{
		adbc.OptionKeyURI: uri,
	}

	for k, v := range headers {
		options[drv.OptionRPCCallHeaderPrefix+k] = v
	}

	var alloc memory.Allocator
	drv := drv.NewDriver(alloc)
	db, err := drv.NewDatabase(options)
	if err != nil {
		return fmt.Errorf("failed to open database: %s\n", err.Error())
	}
	defer func() {
		err = errors.Join(err, db.Close())
	}()

	cnxn, err := db.Open(ctx)
	if err != nil {
		return fmt.Errorf("failed to open connection: %s", err.Error())
	}
	defer func() {
		err = errors.Join(err, cnxn.Close())
	}()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		return fmt.Errorf("failed to create statement: %s", err.Error())
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	if err = stmt.SetSqlQuery("SELECT 1 AS theresult"); err != nil {
		return fmt.Errorf("failed to set query: %s", err.Error())
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return fmt.Errorf("failed to execute query: %s", err.Error())
	}
	defer reader.Release()

	for reader.Next() {
		arr, ok := reader.RecordBatch().Column(0).(*array.Int64)
		if !ok {
			return fmt.Errorf("result data was not int64")
		}
		for i := 0; i < arr.Len(); i++ {
			if arr.IsNull(i) {
				fmt.Println("theresult: NULL")
			} else {
				fmt.Printf("theresult: %d\n", arr.Value(i))
			}
		}
	}

	return nil
}

func Example() {
	// For this example we will spawn the Flight SQL server ourselves.

	// Create a new database that isn't tied to any other databases that
	// may be in process.
	db, err := sql.Open("sqlite", "file:example_in_memory?mode=memory")
	if err != nil {
		log.Fatal(err)
	}
	defer func() {
		err := db.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	srv, err := sqlite.NewSQLiteFlightSQLServer(db)
	if err != nil {
		log.Fatal(err)
	}

	server := flight.NewServerWithMiddleware(nil)
	server.RegisterFlightService(flightsql.NewFlightServer(srv))
	err = server.Init("localhost:8080")
	if err != nil {
		log.Fatal(err)
	}

	go func() {
		if err := server.Serve(); err != nil {
			log.Fatal(err)
		}
	}()

	uri := fmt.Sprintf("grpc://%s", server.Addr().String())
	if err := FlightSQLExample(uri); err != nil {
		log.Printf("Error: %s\n", err.Error())
	}

	server.Shutdown()

	// Output:
	// theresult: 1
}
