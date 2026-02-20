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

package adbc_test

import (
	"context"
	"fmt"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
)

// ExampleAsDatabaseContext demonstrates wrapping a non-context Database
// to use with context-aware code.
func ExampleAsDatabaseContext() {
	var db adbc.Database // obtained from driver

	// Wrap the database to add context support
	dbCtx := adbc.AsDatabaseContext(db)

	// Now you can use context with all operations
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	// Open connection with context
	conn, err := dbCtx.Open(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer conn.Close(ctx)

	fmt.Println("Connected successfully")
}

// ExampleConnectionContext demonstrates using context for cancellation.
func ExampleConnectionContext() {
	var connCtx adbc.ConnectionContext // obtained from DatabaseContext.Open

	// Create a context with cancellation
	ctx, cancel := context.WithCancel(context.Background())

	// Cancel after some condition
	go func() {
		// Simulate cancellation after some event
		time.Sleep(100 * time.Millisecond)
		cancel()
	}()

	// This operation will be cancelled
	stmt, err := connCtx.NewStatement(ctx)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
	defer stmt.Close(ctx)

	fmt.Println("Statement created")
}
