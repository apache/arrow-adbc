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

//go:build cgo

package sqldriver_test

import (
	"database/sql"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow-adbc/go/adbc/sqldriver"
)

func Example() {
	sql.Register("adbc", sqldriver.Driver{&drivermgr.Driver{}})

	// AdbcDriverInit is the assumed entrypoint by default, but i'll keep
	// it specified explicitly here for demonstration purposes.
	// this also assumes that libadbc_driver_sqlite.so is on your LD_LIBRARY_PATH
	db, err := sql.Open("adbc", "driver=adbc_driver_sqlite;entrypoint=AdbcDriverInit")
	if err != nil {
		panic(err)
	}

	rows, err := db.Query("SELECT ?", 1)
	if err != nil {
		panic(err)
	}
	defer func() {
		if err := rows.Close(); err != nil {
			panic(err)
		}
	}()

	colNames, err := rows.Columns()
	if err != nil {
		panic(err)
	}

	fmt.Println(colNames)
	cols, err := rows.ColumnTypes()
	if err != nil {
		panic(err)
	}
	fmt.Println(cols[0].Name())
	fmt.Println(cols[0].Nullable())

	for rows.Next() {
		var v int64
		if err := rows.Scan(&v); err != nil {
			panic(err)
		}
		fmt.Println(v)
	}

	// Output:
	// [?]
	// ?
	// true true
	// 1
}
