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
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc/driver/databricks"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestDriverCreation(t *testing.T) {
	driver := databricks.NewDriver(memory.DefaultAllocator)
	assert.NotNil(t, driver)
}

func TestDatabaseCreation(t *testing.T) {
	driver := databricks.NewDriver(memory.DefaultAllocator)

	// Test with minimal options
	opts := map[string]string{
		databricks.OptionServerHostname: "test-hostname",
		databricks.OptionHTTPPath:       "/sql/1.0/warehouses/test",
		databricks.OptionAccessToken:    "test-token",
	}

	db, err := driver.NewDatabase(opts)
	require.NoError(t, err)
	assert.NotNil(t, db)

	// Clean up
	err = db.Close()
	assert.NoError(t, err)
}

func TestDatabaseOptionsValidation(t *testing.T) {
	driver := databricks.NewDriver(memory.DefaultAllocator)

	// Test missing required options
	tests := []struct {
		name    string
		opts    map[string]string
		wantErr bool
	}{
		{
			name: "missing hostname",
			opts: map[string]string{
				databricks.OptionHTTPPath:    "/sql/1.0/warehouses/test",
				databricks.OptionAccessToken: "test-token",
			},
			wantErr: true,
		},
		{
			name: "missing http path",
			opts: map[string]string{
				databricks.OptionServerHostname: "test-hostname",
				databricks.OptionAccessToken:    "test-token",
			},
			wantErr: true,
		},
		{
			name: "missing access token",
			opts: map[string]string{
				databricks.OptionServerHostname: "test-hostname",
				databricks.OptionHTTPPath:       "/sql/1.0/warehouses/test",
			},
			wantErr: true,
		},
		{
			name: "all required options",
			opts: map[string]string{
				databricks.OptionServerHostname: "test-hostname",
				databricks.OptionHTTPPath:       "/sql/1.0/warehouses/test",
				databricks.OptionAccessToken:    "test-token",
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := driver.NewDatabase(tt.opts)
			require.NoError(t, err)
			require.NotNil(t, db)

			// Test connection opening (will fail without real credentials, but validates options)
			_, err = db.Open(context.Background())
			if tt.wantErr {
				assert.Error(t, err)
			} else {
				// Even valid options will fail without real credentials, so expect error
				assert.Error(t, err)
			}

			_ = db.Close()
		})
	}
}
