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

package databricks

import (
	"context"
	"sync"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
)

func newTestDatabase(t *testing.T) *databaseImpl {
	t.Helper()
	info := driverbase.DefaultDriverInfo("Databricks")
	drvBase := driverbase.NewDriverImplBase(info, memory.DefaultAllocator)
	dbBase, err := driverbase.NewDatabaseImplBase(context.Background(), &drvBase)
	if err != nil {
		t.Fatal(err)
	}
	return &databaseImpl{
		DatabaseImplBase: dbBase,
		serverHostname:   "test.cloud.databricks.com",
		httpPath:         "/sql/1.0/warehouses/test",
		accessToken:      "test-token",
		authType:         OptionValueAuthTypePAT,
		port:             443,
	}
}

// TestConcurrentOpen verifies that concurrent Open() calls on the same
// databaseImpl do not race on connection pool initialization. This
// reproduces the scenario from dbt-core#13387 where multiple goroutines
// calling Open() simultaneously could both attempt to start the OAuth
// listener on the same port.
//
// Run with: go test -race -run TestConcurrentOpen
func TestConcurrentOpen(t *testing.T) {
	db := newTestDatabase(t)

	// We expect Open() to fail (no real Databricks server), but the
	// important thing is that concurrent calls don't race on d.db/d.needsRefresh.
	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines)

	errs := make([]error, goroutines)
	for i := 0; i < goroutines; i++ {
		go func(idx int) {
			defer wg.Done()
			_, errs[idx] = db.Open(context.Background())
		}(i)
	}
	wg.Wait()

	// All goroutines should get the same error (connection failure to test host).
	// The key assertion is that we reached here without a data race or panic.
	for i, err := range errs {
		assert.Error(t, err, "goroutine %d should fail (no real server)", i)
	}
}

// TestConcurrentSetOptionAndOpen verifies that SetOption and Open can be
// called concurrently without data races.
func TestConcurrentSetOptionAndOpen(t *testing.T) {
	db := newTestDatabase(t)

	const goroutines = 10
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = db.SetOption(OptionQueryTimeout, "30s")
		}()
		go func() {
			defer wg.Done()
			_, _ = db.Open(context.Background())
		}()
	}
	wg.Wait()
}

// TestConcurrentCloseAndOpen verifies that Close and Open can be called
// concurrently without data races or panics.
func TestConcurrentCloseAndOpen(t *testing.T) {
	db := newTestDatabase(t)

	const goroutines = 5
	var wg sync.WaitGroup
	wg.Add(goroutines * 2)

	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			_ = db.Close()
		}()
		go func() {
			defer wg.Done()
			_, _ = db.Open(context.Background())
		}()
	}
	wg.Wait()
}
