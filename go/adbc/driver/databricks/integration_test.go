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

//go:build integration
// +build integration

package databricks_test

import (
	"context"
	"database/sql"
	"fmt"
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/databricks"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	_ "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestIPCStreamIntegration tests the full end-to-end flow with IPC streams
// To run this test, set the following environment variables:
// - DATABRICKS_HOST: Your Databricks workspace URL
// - DATABRICKS_TOKEN: Your Databricks access token
// - DATABRICKS_HTTP_PATH: SQL warehouse HTTP path
func TestIPCStreamIntegration(t *testing.T) {
	host := os.Getenv("DATABRICKS_HOST")
	token := os.Getenv("DATABRICKS_TOKEN")
	httpPath := os.Getenv("DATABRICKS_HTTP_PATH")

	if host == "" || token == "" || httpPath == "" {
		t.Skip("Skipping integration test: DATABRICKS_HOST, DATABRICKS_TOKEN, and DATABRICKS_HTTP_PATH must be set")
	}

	ctx := context.Background()

	// Create ADBC driver and database
	driver := databricks.NewDriver(nil)
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: host,
		databricks.OptionAccessToken:    token,
		databricks.OptionHTTPPath:       httpPath,
	})
	require.NoError(t, err)

	// Create connection
	conn, err := db.Open(ctx)
	require.NoError(t, err)
	defer conn.Close()

	// Create statement
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer stmt.Close()

	// Test 1: Simple query with various data types
	t.Run("SimpleQuery", func(t *testing.T) {
		err := stmt.SetSqlQuery(`
			SELECT 
				1 as int_col,
				1.5 as double_col,
				'hello' as string_col,
				true as bool_col,
				CAST('2023-01-01' AS DATE) as date_col
		`)
		require.NoError(t, err)

		reader, rowsAffected, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		assert.Equal(t, int64(-1), rowsAffected) // Unknown rows affected
		defer reader.Release()

		// Verify schema
		schema := reader.Schema()
		assert.Equal(t, 5, len(schema.Fields()))
		assert.Equal(t, "int_col", schema.Field(0).Name)
		assert.Equal(t, "double_col", schema.Field(1).Name)
		assert.Equal(t, "string_col", schema.Field(2).Name)
		assert.Equal(t, "bool_col", schema.Field(3).Name)
		assert.Equal(t, "date_col", schema.Field(4).Name)

		// Read data
		hasData := reader.Next()
		assert.True(t, hasData)

		record := reader.Record()
		assert.Equal(t, int64(1), record.NumRows())
		assert.Equal(t, int64(5), record.NumCols())

		// Verify values
		assert.Equal(t, int32(1), record.Column(0).(*array.Int32).Value(0))
		assert.Equal(t, float64(1.5), record.Column(1).(*array.Float64).Value(0))
		assert.Equal(t, "hello", record.Column(2).(*array.String).Value(0))
		assert.Equal(t, true, record.Column(3).(*array.Boolean).Value(0))
		// Date verification depends on the exact Arrow type used by Databricks

		// No more data
		hasData = reader.Next()
		assert.False(t, hasData)
	})

	// Test 2: Query with multiple batches
	t.Run("MultipleBatches", func(t *testing.T) {
		// Generate a query that returns multiple rows
		err := stmt.SetSqlQuery(`
			SELECT id, id * 2 as doubled
			FROM range(0, 10000)
		`)
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		totalRows := int64(0)
		batchCount := 0

		for reader.Next() {
			record := reader.Record()
			totalRows += record.NumRows()
			batchCount++

			// Verify data integrity for first few rows of each batch
			idCol := record.Column(0).(*array.Int64)
			doubledCol := record.Column(1).(*array.Int64)

			for i := 0; i < int(minInt64(5, record.NumRows())); i++ {
				id := idCol.Value(i)
				doubled := doubledCol.Value(i)
				assert.Equal(t, id*2, doubled, "Row %d: expected %d*2=%d, got %d", i, id, id*2, doubled)
			}
		}

		assert.Equal(t, int64(10000), totalRows)
		assert.Greater(t, batchCount, 1, "Expected multiple batches for 10000 rows")
		t.Logf("Received %d batches for 10000 rows", batchCount)
	})

	// Test 3: Empty result set
	t.Run("EmptyResultSet", func(t *testing.T) {
		err := stmt.SetSqlQuery(`
			SELECT * FROM range(0, 0)
		`)
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		// Should have schema but no data
		schema := reader.Schema()
		assert.NotNil(t, schema)

		hasData := reader.Next()
		assert.False(t, hasData)
	})

	// Test 4: Large strings and binary data
	t.Run("LargeData", func(t *testing.T) {
		err := stmt.SetSqlQuery(`
			SELECT 
				repeat('x', 10000) as large_string,
				unhex(repeat('FF', 5000)) as binary_data
		`)
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		hasData := reader.Next()
		assert.True(t, hasData)

		record := reader.Record()
		stringCol := record.Column(0).(*array.String)
		binaryCol := record.Column(1).(*array.Binary)

		assert.Equal(t, 10000, len(stringCol.Value(0)))
		assert.Equal(t, 5000, len(binaryCol.Value(0)))
	})
}

func minInt64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// TestIPCStreamPerformance is a basic performance test
func TestIPCStreamPerformance(t *testing.T) {
	host := os.Getenv("DATABRICKS_HOST")
	token := os.Getenv("DATABRICKS_TOKEN")
	httpPath := os.Getenv("DATABRICKS_HTTP_PATH")

	if host == "" || token == "" || httpPath == "" {
		t.Skip("Skipping performance test: environment variables not set")
	}

	// Connect using standard database/sql for comparison
	dsn := fmt.Sprintf("token:%s@%s%s", token, host, httpPath)
	sqlDB, err := sql.Open("databricks", dsn)
	require.NoError(t, err)
	defer sqlDB.Close()

	ctx := context.Background()

	// Measure standard SQL row scanning
	t.Run("StandardSQL", func(t *testing.T) {
		rows, err := sqlDB.QueryContext(ctx, "SELECT id, id * 2 as doubled FROM range(0, 100000)")
		require.NoError(t, err)
		defer rows.Close()

		count := 0
		for rows.Next() {
			var id, doubled int64
			err := rows.Scan(&id, &doubled)
			require.NoError(t, err)
			count++
		}
		assert.Equal(t, 100000, count)
	})

	// Measure ADBC with IPC streams
	t.Run("ADBCIPCStream", func(t *testing.T) {
		driver := databricks.NewDriver(nil)
		db, err := driver.NewDatabase(map[string]string{
			databricks.OptionServerHostname: host,
			databricks.OptionAccessToken:    token,
			databricks.OptionHTTPPath:       httpPath,
		})
		require.NoError(t, err)

		conn, err := db.Open(ctx)
		require.NoError(t, err)
		defer conn.Close()

		stmt, err := conn.NewStatement()
		require.NoError(t, err)
		defer stmt.Close()

		err = stmt.SetSqlQuery("SELECT id, id * 2 as doubled FROM range(0, 100000)")
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		totalRows := int64(0)
		for reader.Next() {
			record := reader.Record()
			totalRows += record.NumRows()
		}
		assert.Equal(t, int64(100000), totalRows)
	})
}

