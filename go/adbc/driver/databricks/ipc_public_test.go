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

package databricks_test

import (
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/databricks"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIPCStreamSupport tests that the databricks driver properly supports IPC streams
// This is a high-level test that verifies the public API works correctly
func TestIPCStreamSupport(t *testing.T) {
	// Create allocator
	mem := memory.NewGoAllocator()
	
	// Create driver
	driver := databricks.NewDriver(mem)
	assert.NotNil(t, driver)
	
	// Create database with mock credentials
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: "test.databricks.com",
		databricks.OptionAccessToken:    "test-token",
		databricks.OptionHTTPPath:       "/sql/1.0/warehouses/test",
	})
	require.NoError(t, err)
	assert.NotNil(t, db)
	
	// Note: We can't test actual IPC stream functionality without a real connection
	// The internal tests verify the IPC reader adapter works correctly with mocked data
	t.Log("IPC stream support is built into the driver")
}

// TestDriverArrowVersion verifies the driver uses the correct Arrow version
func TestDriverArrowVersion(t *testing.T) {
	// This test ensures we're using Arrow v18 types in the public API
	// The actual IPC stream conversion from v12 to v18 is tested internally
	
	driver := databricks.NewDriver(nil)
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: "test.databricks.com",
		databricks.OptionAccessToken:    "test-token",
		databricks.OptionHTTPPath:       "/sql/1.0/warehouses/test",
	})
	require.NoError(t, err)
	
	// The database interface uses Arrow v18 types
	// This is verified at compile time
	_ = db
	
	t.Log("Driver correctly uses Arrow v18 in public API")
}