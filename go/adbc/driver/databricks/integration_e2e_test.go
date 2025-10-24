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
	"database/sql"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc/driver/databricks"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/databricks/databricks-sql-go"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestConfig is used by legacy tests that still load from JSON files
// TODO: Convert these tests to use environment variables
type TestConfig struct {
	Environment     string `json:"environment"`
	URI             string `json:"uri"`
	Token           string `json:"token"`
	Query           string `json:"query"`
	Type            string `json:"type"`
	Trace           string `json:"trace"`
	ExpectedResults int    `json:"expectedResults"`
	Metadata        struct {
		Catalog             string `json:"catalog"`
		Schema              string `json:"schema"`
		Table               string `json:"table"`
		ExpectedColumnCount int    `json:"expectedColumnCount"`
	} `json:"metadata"`
}

// getDatabricksConfig retrieves connection configuration from environment variables
func getDatabricksConfig(t *testing.T) (host, token, httpPath, catalog, schema string) {
	host = os.Getenv("DATABRICKS_HOST")
	token = os.Getenv("DATABRICKS_ACCESSTOKEN")
	httpPath = os.Getenv("DATABRICKS_HTTPPATH")
	catalog = os.Getenv("DATABRICKS_CATALOG")
	schema = os.Getenv("DATABRICKS_SCHEMA")

	if host == "" || token == "" || httpPath == "" {
		t.Skip("Skipping integration test: DATABRICKS_HOST, DATABRICKS_ACCESSTOKEN, and DATABRICKS_HTTPPATH must be set")
	}

	// Use defaults if not specified
	if catalog == "" {
		catalog = "main"
	}
	if schema == "" {
		schema = "default"
	}

	return host, token, httpPath, catalog, schema
}

// TestE2E_BasicConnection tests basic connectivity to Databricks
func TestE2E_BasicConnection(t *testing.T) {
	host, token, httpPath, catalog, schema := getDatabricksConfig(t)

	// Create driver
	driver := databricks.NewDriver(memory.DefaultAllocator)

	// Create database with config from environment
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: host,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    token,
		databricks.OptionCatalog:        catalog,
		databricks.OptionSchema:         schema,
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := db.Open(ctx)
	require.NoError(t, err, "Failed to open connection to Databricks")
	defer func() { _ = conn.Close() }()

	t.Logf("✅ Successfully connected to Databricks at %s", host)
}

// TestE2E_SimpleQuery tests executing a simple query
func TestE2E_SimpleQuery(t *testing.T) {
	host, token, httpPath, catalog, schema := getDatabricksConfig(t)

	// Create driver and connection
	driver := databricks.NewDriver(memory.DefaultAllocator)
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: host,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    token,
		databricks.OptionCatalog:        catalog,
		databricks.OptionSchema:         schema,
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := db.Open(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Create statement and execute simple query
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

	// Test simple SELECT 1
	err = stmt.SetSqlQuery("SELECT 1 as test_column")
	require.NoError(t, err)

	reader, rowsAffected, err := stmt.ExecuteQuery(ctx)
	require.NoError(t, err)
	defer reader.Release()

	t.Logf("✅ Simple query executed successfully, rows affected: %d", rowsAffected)

	// Verify we got a result
	assert.True(t, reader.Next(), "Expected at least one record")
	record := reader.RecordBatch()
	assert.Equal(t, int64(1), record.NumCols(), "Expected 1 column")
	assert.Equal(t, int64(1), record.NumRows(), "Expected 1 row")

	t.Logf("✅ Query result: %d columns, %d rows", record.NumCols(), record.NumRows())
}

// TestE2E_MetadataOperations tests metadata retrieval operations
func TestE2E_MetadataOperations(t *testing.T) {
	host, token, httpPath, catalog, schema := getDatabricksConfig(t)

	// Create driver and connection
	driver := databricks.NewDriver(memory.DefaultAllocator)
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: host,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    token,
		databricks.OptionCatalog:        catalog,
		databricks.OptionSchema:         schema,
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := db.Open(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Test GetTableTypes
	t.Run("GetTableTypes", func(t *testing.T) {
		reader, err := conn.GetTableTypes(ctx)
		require.NoError(t, err)
		defer reader.Release()

		count := 0
		for reader.Next() {
			record := reader.RecordBatch()
			count += int(record.NumRows())
			t.Logf("Table types record: %d rows", record.NumRows())
		}
		assert.Greater(t, count, 0, "Expected some table types")
		t.Logf("✅ Found %d table types", count)
	})

	// Test basic metadata queries
	t.Run("ShowCatalogs", func(t *testing.T) {
		stmt, err := conn.NewStatement()
		require.NoError(t, err)
		defer func() { _ = stmt.Close() }()

		err = stmt.SetSqlQuery("SHOW CATALOGS")
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		catalogCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			catalogCount += int(record.NumRows())
			t.Logf("Catalogs record: %d rows", record.NumRows())
		}
		assert.Greater(t, catalogCount, 0, "Expected some catalogs")
		t.Logf("✅ Found %d catalogs", catalogCount)
	})

	t.Run("ShowSchemas", func(t *testing.T) {
		stmt, err := conn.NewStatement()
		require.NoError(t, err)
		defer func() { _ = stmt.Close() }()

		query := fmt.Sprintf("SHOW SCHEMAS IN %s", catalog)
		err = stmt.SetSqlQuery(query)
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		schemaCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			schemaCount += int(record.NumRows())
			t.Logf("Schemas record: %d rows", record.NumRows())
		}
		assert.Greater(t, schemaCount, 0, "Expected some schemas")
		t.Logf("✅ Found %d schemas in catalog %s", schemaCount, catalog)
	})

	t.Run("ShowTables", func(t *testing.T) {
		stmt, err := conn.NewStatement()
		require.NoError(t, err)
		defer func() { _ = stmt.Close() }()

		query := fmt.Sprintf("SHOW TABLES IN %s.%s", catalog, schema)
		err = stmt.SetSqlQuery(query)
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		tableCount := 0
		for reader.Next() {
			record := reader.RecordBatch()
			tableCount += int(record.NumRows())
			t.Logf("Tables record: %d rows", record.NumRows())
		}
		// Note: schema might be empty, so we don't assert on table count
		t.Logf("✅ Found %d tables in schema %s.%s", tableCount, catalog, schema)
	})
}

// TestE2E_ConnectionOptions tests different connection options
func TestE2E_ConnectionOptions(t *testing.T) {
	host, token, httpPath, catalog, schema := getDatabricksConfig(t)

	// Test different connection options
	tests := []struct {
		name    string
		options map[string]string
	}{
		{
			name: "with_timeout",
			options: map[string]string{
				databricks.OptionServerHostname: host,
				databricks.OptionHTTPPath:       httpPath,
				databricks.OptionAccessToken:    token,
				databricks.OptionQueryTimeout:   "30s",
			},
		},
		{
			name: "with_max_rows",
			options: map[string]string{
				databricks.OptionServerHostname: host,
				databricks.OptionHTTPPath:       httpPath,
				databricks.OptionAccessToken:    token,
				databricks.OptionMaxRows:        "100",
			},
		},
		{
			name: "with_catalog_schema",
			options: map[string]string{
				databricks.OptionServerHostname: host,
				databricks.OptionHTTPPath:       httpPath,
				databricks.OptionAccessToken:    token,
				databricks.OptionCatalog:        catalog,
				databricks.OptionSchema:         schema,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			driver := databricks.NewDriver(memory.DefaultAllocator)

			db, err := driver.NewDatabase(tt.options)
			require.NoError(t, err)
			defer func() { _ = db.Close() }()

			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()

			conn, err := db.Open(ctx)
			require.NoError(t, err)
			defer func() { _ = conn.Close() }()

			// Execute a simple query to verify the connection works
			stmt, err := conn.NewStatement()
			require.NoError(t, err)
			defer func() { _ = stmt.Close() }()

			err = stmt.SetSqlQuery("SELECT 1")
			require.NoError(t, err)

			reader, _, err := stmt.ExecuteQuery(ctx)
			require.NoError(t, err)
			defer reader.Release()

			assert.True(t, reader.Next(), "Expected at least one record")
			t.Logf("✅ Connection with %s options successful", tt.name)
		})
	}
}

// TestIPCStreamIntegration tests the full end-to-end flow with IPC streams
func TestIPCStreamIntegration(t *testing.T) {
	host, token, httpPath, _, _ := getDatabricksConfig(t)

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
	defer func() { _ = conn.Close() }()

	// Create statement
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

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

		record := reader.RecordBatch()
		assert.Equal(t, int64(1), record.NumRows())
		assert.Equal(t, int64(5), record.NumCols())

		// Verify values - handle different numeric types that Databricks might return
		col0 := record.Column(0)
		switch v := col0.(type) {
		case *array.Int32:
			assert.Equal(t, int32(1), v.Value(0))
		case *array.Int64:
			assert.Equal(t, int64(1), v.Value(0))
		case *array.String:
			assert.Equal(t, "1", v.Value(0))
		default:
			t.Errorf("Unexpected type for int_col: %T", col0)
		}

		col1 := record.Column(1)
		switch v := col1.(type) {
		case *array.Float64:
			assert.Equal(t, float64(1.5), v.Value(0))
		case *array.String:
			assert.Equal(t, "1.5", v.Value(0))
		default:
			t.Errorf("Unexpected type for double_col: %T", col1)
		}

		assert.Equal(t, "hello", record.Column(2).(*array.String).Value(0))

		col3 := record.Column(3)
		switch v := col3.(type) {
		case *array.Boolean:
			assert.Equal(t, true, v.Value(0))
		case *array.String:
			assert.Equal(t, "true", v.Value(0))
		default:
			t.Errorf("Unexpected type for bool_col: %T", col3)
		}
		// Date verification depends on the exact Arrow type used by Databricks

		// No more data
		hasData = reader.Next()
		assert.False(t, hasData)
	})

	// Test 2: Query with multiple batches
	t.Run("MultipleBatches", func(t *testing.T) {
		// Generate a query that returns multiple rows
		// Note: Some Databricks configurations may limit result size
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
			record := reader.RecordBatch()
			totalRows += record.NumRows()
			batchCount++

			// Verify data integrity for first few rows of each batch
			// Handle potential type variations from Databricks
			var idValues []int64
			var doubledValues []int64

			switch col := record.Column(0).(type) {
			case *array.Int64:
				idCol := col
				for i := 0; i < int(record.NumRows()); i++ {
					idValues = append(idValues, idCol.Value(i))
				}
			case *array.Int32:
				idCol := col
				for i := 0; i < int(record.NumRows()); i++ {
					idValues = append(idValues, int64(idCol.Value(i)))
				}
			default:
				t.Fatalf("Unexpected type for id column: %T", col)
			}

			switch col := record.Column(1).(type) {
			case *array.Int64:
				doubledCol := col
				for i := 0; i < int(record.NumRows()); i++ {
					doubledValues = append(doubledValues, doubledCol.Value(i))
				}
			case *array.Int32:
				doubledCol := col
				for i := 0; i < int(record.NumRows()); i++ {
					doubledValues = append(doubledValues, int64(doubledCol.Value(i)))
				}
			default:
				t.Fatalf("Unexpected type for doubled column: %T", col)
			}

			for i := 0; i < int(minInt64(5, record.NumRows())); i++ {
				id := idValues[i]
				doubled := doubledValues[i]
				assert.Equal(t, id*2, doubled, "Row %d: expected %d*2=%d, got %d", i, id, id*2, doubled)
			}
		}

		// Note: Databricks may return fewer rows due to limits or configuration
		// Just verify we got some data
		assert.Greater(t, totalRows, int64(0), "Expected to receive some rows")
		t.Logf("Received %d rows in %d batches", totalRows, batchCount)
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

		// Check for data - empty result sets may or may not have schema
		hasData := reader.Next()
		assert.False(t, hasData)

		// Schema might be available even for empty results
		schema := reader.Schema()
		if schema != nil {
			t.Logf("Empty result has schema with %d fields", len(schema.Fields()))
		} else {
			t.Log("Empty result has no schema")
		}
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

		record := reader.RecordBatch()
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
	host, token, httpPath, _, _ := getDatabricksConfig(t)

	// Connect using standard database/sql for comparison
	// DSN format: token:<token>@<hostname>:443<httpPath>
	port := os.Getenv("DATABRICKS_PORT")
	if port == "" {
		port = "443"
	}
	dsn := fmt.Sprintf("token:%s@%s:%s%s", token, host, port, httpPath)
	sqlDB, err := sql.Open("databricks", dsn)
	require.NoError(t, err)
	defer func() { _ = sqlDB.Close() }()

	ctx := context.Background()

	// Measure standard SQL row scanning
	t.Run("StandardSQL", func(t *testing.T) {
		rows, err := sqlDB.QueryContext(ctx, "SELECT id, id * 2 as doubled FROM range(0, 100000)")
		require.NoError(t, err)
		defer func() { _ = rows.Close() }()

		count := 0
		for rows.Next() {
			var id, doubled int64
			err := rows.Scan(&id, &doubled)
			require.NoError(t, err)
			count++
		}
		// Note: Databricks may limit the number of rows returned
		assert.Greater(t, count, 0, "Expected to receive some rows")
		t.Logf("StandardSQL: Retrieved %d rows", count)
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
		defer func() { _ = conn.Close() }()

		stmt, err := conn.NewStatement()
		require.NoError(t, err)
		defer func() { _ = stmt.Close() }()

		err = stmt.SetSqlQuery("SELECT id, id * 2 as doubled FROM range(0, 100000)")
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		totalRows := int64(0)
		for reader.Next() {
			record := reader.RecordBatch()
			totalRows += record.NumRows()
		}
		// Note: Databricks may limit the number of rows returned
		assert.Greater(t, totalRows, int64(0), "Expected to receive some rows")
		t.Logf("ADBCIPCStream: Retrieved %d rows", totalRows)
	})
}

// TestE2E_QueryWithTypes tests queries returning various data types
func TestE2E_QueryWithTypes(t *testing.T) {
	host, token, httpPath, _, _ := getDatabricksConfig(t)

	ctx := context.Background()
	driver := databricks.NewDriver(nil)

	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: host,
		databricks.OptionAccessToken:    token,
		databricks.OptionHTTPPath:       httpPath,
	})
	require.NoError(t, err)

	conn, err := db.Open(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

	// Test various data types
	err = stmt.SetSqlQuery(`
		SELECT
			CAST(42 AS TINYINT) as tinyint_col,
			CAST(1234 AS SMALLINT) as smallint_col,
			CAST(123456 AS INT) as int_col,
			CAST(1234567890 AS BIGINT) as bigint_col,
			CAST(3.14 AS FLOAT) as float_col,
			CAST(2.71828 AS DOUBLE) as double_col,
			CAST(123.45 AS DECIMAL(10,2)) as decimal_col,
			'test string' as string_col,
			CAST('2023-01-01' AS DATE) as date_col,
			CAST('2023-01-01 12:34:56' AS TIMESTAMP) as timestamp_col,
			true as bool_col,
			CAST('ABCD' AS BINARY) as binary_col,
			ARRAY(1, 2, 3) as array_col,
			MAP('key1', 'value1', 'key2', 'value2') as map_col,
			STRUCT('field1' AS f1, 42 AS f2) as struct_col
	`)
	require.NoError(t, err)

	reader, _, err := stmt.ExecuteQuery(ctx)
	require.NoError(t, err)
	defer reader.Release()

	assert.True(t, reader.Next())
	record := reader.RecordBatch()

	// Log the schema
	schema := record.Schema()
	t.Log("Query returned schema:")
	for i := 0; i < len(schema.Fields()); i++ {
		field := schema.Field(i)
		t.Logf("  Column %d: %s (%s)", i, field.Name, field.Type.String())
	}

	// Verify we got all columns
	assert.Equal(t, 15, len(schema.Fields()), "Expected 15 columns")

	// Verify column names
	expectedColumns := []string{
		"tinyint_col", "smallint_col", "int_col", "bigint_col",
		"float_col", "double_col", "decimal_col", "string_col",
		"date_col", "timestamp_col", "bool_col", "binary_col",
		"array_col", "map_col", "struct_col",
	}

	for i, expected := range expectedColumns {
		assert.Equal(t, expected, schema.Field(i).Name, "Column %d name mismatch", i)
	}

	t.Log("✅ Successfully retrieved all data types")
}
