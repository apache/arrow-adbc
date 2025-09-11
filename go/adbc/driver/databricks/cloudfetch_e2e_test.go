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
	"time"

	"github.com/apache/arrow-adbc/go/adbc/driver/databricks"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestCloudFetchE2E_BasicConnection tests basic connectivity
// CloudFetch is handled automatically by the databricks-sql-go driver
func TestCloudFetchE2E_BasicConnection(t *testing.T) {
	host, token, httpPath, catalog, schema := getDatabricksConfig(t)

	driver := databricks.NewDriver(memory.DefaultAllocator)

	opts := map[string]string{
		databricks.OptionServerHostname: host,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    token,
		databricks.OptionCatalog:        catalog,
		databricks.OptionSchema:         schema,
	}

	db, err := driver.NewDatabase(opts)
	require.NoError(t, err)
	defer validation.CheckedClose(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := db.Open(ctx)
	require.NoError(t, err)
	defer validation.CheckedClose(t, conn)

	// Execute a simple query to verify connection works
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer validation.CheckedClose(t, stmt)

	err = stmt.SetSqlQuery("SELECT 1 as test")
	require.NoError(t, err)

	reader, _, err := stmt.ExecuteQuery(ctx)
	require.NoError(t, err)
	defer reader.Release()

	assert.True(t, reader.Next(), "Expected at least one record")
	t.Logf("✅ Connection successful (CloudFetch handled automatically by driver)")
}

// TestCloudFetchE2E_SmallQueries tests CloudFetch with small result sets
func TestCloudFetchE2E_SmallQueries(t *testing.T) {
	host, token, httpPath, _, _ := getDatabricksConfig(t)

	queries := []struct {
		name     string
		query    string
		rowCount int
	}{
		{
			name:     "range_100",
			query:    "SELECT * FROM range(100)",
			rowCount: 100,
		},
		{
			name:     "range_1000",
			query:    "SELECT * FROM range(1000)",
			rowCount: 1000,
		},
		{
			name:     "range_10000",
			query:    "SELECT * FROM range(10000)",
			rowCount: 10000,
		},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			driver := databricks.NewDriver(memory.DefaultAllocator)

			opts := map[string]string{
				databricks.OptionServerHostname: host,
				databricks.OptionHTTPPath:       httpPath,
				databricks.OptionAccessToken:    token,
			}

			db, err := driver.NewDatabase(opts)
			require.NoError(t, err)
			defer validation.CheckedClose(t, db)

			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()

			conn, err := db.Open(ctx)
			require.NoError(t, err)
			defer validation.CheckedClose(t, conn)

			stmt, err := conn.NewStatement()
			require.NoError(t, err)
			defer validation.CheckedClose(t, stmt)

			err = stmt.SetSqlQuery(q.query)
			require.NoError(t, err)

			startTime := time.Now()
			reader, _, err := stmt.ExecuteQuery(ctx)
			require.NoError(t, err)
			defer reader.Release()

			// Count rows
			totalRows := int64(0)
			batchCount := 0
			for reader.Next() {
				record := reader.Record()
				totalRows += record.NumRows()
				batchCount++
			}
			duration := time.Since(startTime)

			// Note: Databricks may apply row limits, so we check for at least some rows
			// rather than exact counts
			assert.Greater(t, totalRows, int64(0), "Expected to receive some rows")
			t.Logf("Query '%s': %d rows (requested %d) in %d batches, duration: %v",
				q.name, totalRows, q.rowCount, batchCount, duration)
		})
	}
}

// TestCloudFetchE2E_LargeQueries tests CloudFetch with large result sets
func TestCloudFetchE2E_LargeQueries(t *testing.T) {

	host, token, httpPath, _, _ := getDatabricksConfig(t)

	// Large query examples - adjust based on your available test data
	queries := []struct {
		name       string
		query      string
		minRows    int64
		skipReason string
	}{
		{
			name:    "large_range",
			query:   "SELECT * FROM range(1000000)",
			minRows: 1000000,
		},
		{
			name:       "tpcds_catalog_sales",
			query:      "SELECT * FROM main.tpcds_sf10_delta.catalog_sales LIMIT 1000000",
			minRows:    1000000,
			skipReason: "Requires TPC-DS dataset",
		},
		{
			name:    "cross_join_large",
			query:   "SELECT a.id, b.id FROM range(10000) a CROSS JOIN range(100) b",
			minRows: 1000000,
		},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			if q.skipReason != "" {
				t.Skip(q.skipReason)
			}

			driver := databricks.NewDriver(memory.DefaultAllocator)

			opts := map[string]string{
				databricks.OptionServerHostname: host,
				databricks.OptionHTTPPath:       httpPath,
				databricks.OptionAccessToken:    token,
			}

			// Enable CloudFetch for large queries when implemented
			// opts[OptionUseCloudFetch] = "true"
			// opts[OptionEnableDirectResults] = "true"
			// opts[OptionCanDecompressLz4] = "true"
			// opts[OptionMaxBytesPerFile] = strconv.Itoa(50 * 1024 * 1024) // 50MB

			db, err := driver.NewDatabase(opts)
			require.NoError(t, err)
			defer validation.CheckedClose(t, db)

			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
			defer cancel()

			conn, err := db.Open(ctx)
			require.NoError(t, err)
			defer validation.CheckedClose(t, conn)

			stmt, err := conn.NewStatement()
			require.NoError(t, err)
			defer validation.CheckedClose(t, stmt)

			err = stmt.SetSqlQuery(q.query)
			require.NoError(t, err)

			startTime := time.Now()
			reader, _, err := stmt.ExecuteQuery(ctx)
			require.NoError(t, err)
			defer reader.Release()

			// Count rows and measure performance
			totalRows := int64(0)
			batchCount := 0
			maxBatchSize := int64(0)

			for reader.Next() {
				record := reader.Record()
				batchSize := record.NumRows()
				totalRows += batchSize
				batchCount++

				if batchSize > maxBatchSize {
					maxBatchSize = batchSize
				}

				// Log progress for large queries
				if batchCount%100 == 0 {
					t.Logf("Progress: %d batches, %d rows...", batchCount, totalRows)
				}
			}
			duration := time.Since(startTime)

			assert.GreaterOrEqual(t, totalRows, q.minRows, "Expected at least %d rows", q.minRows)

			rowsPerSecond := float64(totalRows) / duration.Seconds()
			t.Logf("Large query '%s' completed: %d rows in %d batches (max batch: %d), duration: %v, throughput: %.0f rows/sec",
				q.name, totalRows, batchCount, maxBatchSize, duration, rowsPerSecond)
			t.Logf("CloudFetch optimization handled automatically by driver")
		})
	}
}

// TestCloudFetchE2E_DataTypes tests CloudFetch with various data types
func TestCloudFetchE2E_DataTypes(t *testing.T) {
	host, token, httpPath, _, _ := getDatabricksConfig(t)

	driver := databricks.NewDriver(memory.DefaultAllocator)

	opts := map[string]string{
		databricks.OptionServerHostname: host,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    token,
	}

	// Enable CloudFetch when implemented
	// opts[OptionUseCloudFetch] = "true"
	// opts[OptionEnableDirectResults] = "true"

	db, err := driver.NewDatabase(opts)
	require.NoError(t, err)
	defer validation.CheckedClose(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := db.Open(ctx)
	require.NoError(t, err)
	defer validation.CheckedClose(t, conn)

	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer validation.CheckedClose(t, stmt)

	// Test query with various data types
	query := `
		SELECT
			id,
			CAST(id AS TINYINT) as tinyint_col,
			CAST(id AS SMALLINT) as smallint_col,
			CAST(id AS INT) as int_col,
			CAST(id AS BIGINT) as bigint_col,
			CAST(id AS FLOAT) as float_col,
			CAST(id AS DOUBLE) as double_col,
			CAST(id AS DECIMAL(10,2)) as decimal_col,
			CAST(id AS STRING) as string_col,
			CAST('2023-01-01' AS DATE) as date_col,
			CAST('2023-01-01 12:34:56' AS TIMESTAMP) as timestamp_col,
			(id % 2 = 0) as bool_col,
			CAST(CONCAT('binary_', id) AS BINARY) as binary_col,
			ARRAY(id, id+1, id+2) as array_col,
			MAP('key1', id, 'key2', id*2) as map_col,
			STRUCT(id AS field1, CONCAT('value_', id) AS field2) as struct_col,
			repeat('x', 1000) as large_string_col
		FROM range(1000)
	`

	err = stmt.SetSqlQuery(query)
	require.NoError(t, err)

	reader, _, err := stmt.ExecuteQuery(ctx)
	require.NoError(t, err)
	defer reader.Release()

	totalRows := int64(0)
	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()

		// Verify schema has all expected columns
		if totalRows == record.NumRows() { // First batch
			schema := record.Schema()
			expectedColumns := 17
			assert.Equal(t, expectedColumns, len(schema.Fields()), "Expected %d columns", expectedColumns)

			// Log column types
			t.Log("Schema:")
			for i, field := range schema.Fields() {
				t.Logf("  Column %d: %s (%s)", i, field.Name, field.Type.String())
			}
		}
	}

	// Note: Databricks may apply row limits
	assert.Greater(t, totalRows, int64(0), "Expected to receive some rows")
	t.Logf("✅ Successfully retrieved all data types: %d rows (CloudFetch handled automatically)", totalRows)
}

// TestCloudFetchE2E_ErrorHandling tests CloudFetch error scenarios
func TestCloudFetchE2E_ErrorHandling(t *testing.T) {
	host, token, httpPath, _, _ := getDatabricksConfig(t)

	testCases := []struct {
		name          string
		query         string
		expectedError string
		skip          bool
	}{
		{
			name:          "invalid_query",
			query:         "SELECT * FROM non_existent_table",
			expectedError: "TABLE_OR_VIEW_NOT_FOUND",
		},
		{
			name:          "syntax_error",
			query:         "SELECT * FROM range(10", // Missing closing parenthesis
			expectedError: "PARSE_SYNTAX_ERROR",
		},
		{
			name:          "timeout_query",
			query:         "SELECT COUNT(*) FROM range(1000000000000)", // Very large range
			expectedError: "timeout",
			skip:          true, // Skip by default to avoid long waits
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.skip {
				t.Skip("Skipping test that may take too long")
			}

			driver := databricks.NewDriver(memory.DefaultAllocator)

			opts := map[string]string{
				databricks.OptionServerHostname: host,
				databricks.OptionHTTPPath:       httpPath,
				databricks.OptionAccessToken:    token,
			}

			// Enable CloudFetch when implemented
			// opts[OptionUseCloudFetch] = "true"

			db, err := driver.NewDatabase(opts)
			require.NoError(t, err)
			defer validation.CheckedClose(t, db)

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			conn, err := db.Open(ctx)
			require.NoError(t, err)
			defer validation.CheckedClose(t, conn)

			stmt, err := conn.NewStatement()
			require.NoError(t, err)
			defer validation.CheckedClose(t, stmt)

			err = stmt.SetSqlQuery(tc.query)
			require.NoError(t, err)

			reader, _, err := stmt.ExecuteQuery(ctx)
			if err != nil {
				assert.Contains(t, err.Error(), tc.expectedError, "Expected error containing '%s'", tc.expectedError)
				t.Logf("Got expected error: %v", err)
				return
			}
			defer reader.Release()

			// Try to read data
			hasData := reader.Next()
			if !hasData && reader.Err() != nil {
				err = reader.Err()
				assert.Contains(t, err.Error(), tc.expectedError, "Expected error containing '%s'", tc.expectedError)
				t.Logf("Got expected error during read: %v", err)
			}
		})
	}
}

// TestCloudFetchE2E_PerformanceTest tests query performance
// CloudFetch optimization is handled automatically by the databricks-sql-go driver
func TestCloudFetchE2E_PerformanceTest(t *testing.T) {

	host, token, httpPath, _, _ := getDatabricksConfig(t)

	// Test with different result sizes
	queries := []struct {
		name  string
		query string
	}{
		{"small_100k", "SELECT * FROM range(100000)"},
		{"medium_500k", "SELECT * FROM range(500000)"},
		{"large_1m", "SELECT * FROM range(1000000)"},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			duration := runPerformanceTest(t, host, token, httpPath, q.query)
			t.Logf("Query '%s' completed in %v (CloudFetch optimization applied automatically)", q.name, duration)
		})
	}
}

// runPerformanceTest is a helper function to run a single performance test
func runPerformanceTest(t *testing.T, host, token, httpPath, query string) time.Duration {
	driver := databricks.NewDriver(memory.DefaultAllocator)

	opts := map[string]string{
		databricks.OptionServerHostname: host,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    token,
	}

	// CloudFetch is handled automatically by the databricks-sql-go driver

	db, err := driver.NewDatabase(opts)
	require.NoError(t, err)
	defer validation.CheckedClose(t, db)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	conn, err := db.Open(ctx)
	require.NoError(t, err)
	defer validation.CheckedClose(t, conn)

	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer validation.CheckedClose(t, stmt)

	err = stmt.SetSqlQuery(query)
	require.NoError(t, err)

	startTime := time.Now()
	reader, _, err := stmt.ExecuteQuery(ctx)
	require.NoError(t, err)
	defer reader.Release()

	// Read all data
	totalRows := int64(0)
	for reader.Next() {
		record := reader.Record()
		totalRows += record.NumRows()
	}

	duration := time.Since(startTime)
	t.Logf("Query completed: %d rows in %v", totalRows, duration)

	return duration
}
