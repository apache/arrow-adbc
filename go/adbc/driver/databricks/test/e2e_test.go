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
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc/driver/databricks"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func loadTestConfigE2E(configPath string) (*TestConfig, error) {
	file, err := os.Open(configPath)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	bytes, err := io.ReadAll(file)
	if err != nil {
		return nil, err
	}

	var config TestConfig
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return nil, err
	}

	return &config, nil
}

func parseConnectionInfoE2E(uri string) (hostname, httpPath string, err error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}

	hostname = parsed.Host
	httpPath = parsed.Path
	return hostname, httpPath, nil
}

func TestE2E_BasicConnection(t *testing.T) {
	config, err := loadTestConfigE2E("/Users/jade.wang/tmp/adbc_test_config.json")
	if err != nil {
		t.Skipf("Could not load test config: %v", err)
	}

	hostname, httpPath, err := parseConnectionInfoE2E(config.URI)
	require.NoError(t, err)

	// Create driver
	driver := databricks.NewDriver(memory.DefaultAllocator)

	// Create database with config from file
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: hostname,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    config.Token,
		databricks.OptionCatalog:        config.Metadata.Catalog,
		databricks.OptionSchema:         config.Metadata.Schema,
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	// Test connection
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	conn, err := db.Open(ctx)
	require.NoError(t, err, "Failed to open connection to Databricks")
	defer func() { _ = conn.Close() }()

	t.Logf("✅ Successfully connected to Databricks at %s", hostname)
}

func TestE2E_SimpleQuery(t *testing.T) {
	config, err := loadTestConfigE2E("/Users/jade.wang/tmp/adbc_test_config.json")
	if err != nil {
		t.Skipf("Could not load test config: %v", err)
	}

	hostname, httpPath, err := parseConnectionInfoE2E(config.URI)
	require.NoError(t, err)

	// Create driver and connection
	driver := databricks.NewDriver(memory.DefaultAllocator)
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: hostname,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    config.Token,
		databricks.OptionCatalog:        config.Metadata.Catalog,
		databricks.OptionSchema:         config.Metadata.Schema,
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
	record := reader.Record()
	assert.Equal(t, int64(1), record.NumCols(), "Expected 1 column")
	assert.Equal(t, int64(1), record.NumRows(), "Expected 1 row")

	t.Logf("✅ Query result: %d columns, %d rows", record.NumCols(), record.NumRows())
}

func TestE2E_ConfiguredQuery(t *testing.T) {
	config, err := loadTestConfigE2E("/Users/jade.wang/tmp/adbc_test_config.json")
	if err != nil {
		t.Skipf("Could not load test config: %v", err)
	}

	hostname, httpPath, err := parseConnectionInfoE2E(config.URI)
	require.NoError(t, err)

	// Create driver and connection
	driver := databricks.NewDriver(memory.DefaultAllocator)
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: hostname,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    config.Token,
		databricks.OptionCatalog:        config.Metadata.Catalog,
		databricks.OptionSchema:         config.Metadata.Schema,
	})
	require.NoError(t, err)
	defer func() { _ = db.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	conn, err := db.Open(ctx)
	require.NoError(t, err)
	defer func() { _ = conn.Close() }()

	// Create statement and execute the query from config
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

	t.Logf("Executing query: %s", config.Query)
	err = stmt.SetSqlQuery(config.Query)
	require.NoError(t, err)

	reader, rowsAffected, err := stmt.ExecuteQuery(ctx)
	require.NoError(t, err)
	defer reader.Release()

	t.Logf("✅ Configured query executed successfully, rows affected: %d", rowsAffected)

	// Read and verify results
	recordCount := 0
	for reader.Next() {
		record := reader.Record()
		recordCount++
		t.Logf("Record %d: %d columns, %d rows", recordCount, record.NumCols(), record.NumRows())

		// Print first few column names for verification
		schema := record.Schema()
		for i := 0; i < int(record.NumCols()) && i < 5; i++ {
			field := schema.Field(i)
			t.Logf("  Column %d: %s (%s)", i, field.Name, field.Type.String())
		}
		if record.NumCols() > 5 {
			t.Logf("  ... and %d more columns", record.NumCols()-5)
		}
	}

	if config.ExpectedResults > 0 {
		assert.GreaterOrEqual(t, recordCount, config.ExpectedResults, "Expected at least %d result records", config.ExpectedResults)
	}

	t.Logf("✅ Query returned %d record batches", recordCount)
}

func TestE2E_MetadataOperations(t *testing.T) {
	config, err := loadTestConfigE2E("/Users/jade.wang/tmp/adbc_test_config.json")
	if err != nil {
		t.Skipf("Could not load test config: %v", err)
	}

	hostname, httpPath, err := parseConnectionInfoE2E(config.URI)
	require.NoError(t, err)

	// Create driver and connection
	driver := databricks.NewDriver(memory.DefaultAllocator)
	db, err := driver.NewDatabase(map[string]string{
		databricks.OptionServerHostname: hostname,
		databricks.OptionHTTPPath:       httpPath,
		databricks.OptionAccessToken:    config.Token,
		databricks.OptionCatalog:        config.Metadata.Catalog,
		databricks.OptionSchema:         config.Metadata.Schema,
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
			record := reader.Record()
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
			record := reader.Record()
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

		query := fmt.Sprintf("SHOW SCHEMAS IN %s", config.Metadata.Catalog)
		err = stmt.SetSqlQuery(query)
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		schemaCount := 0
		for reader.Next() {
			record := reader.Record()
			schemaCount += int(record.NumRows())
			t.Logf("Schemas record: %d rows", record.NumRows())
		}
		assert.Greater(t, schemaCount, 0, "Expected some schemas")
		t.Logf("✅ Found %d schemas in catalog %s", schemaCount, config.Metadata.Catalog)
	})

	t.Run("ShowTables", func(t *testing.T) {
		stmt, err := conn.NewStatement()
		require.NoError(t, err)
		defer func() { _ = stmt.Close() }()

		query := fmt.Sprintf("SHOW TABLES IN %s.%s", config.Metadata.Catalog, config.Metadata.Schema)
		err = stmt.SetSqlQuery(query)
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		tableCount := 0
		for reader.Next() {
			record := reader.Record()
			tableCount += int(record.NumRows())
			t.Logf("Tables record: %d rows", record.NumRows())
		}
		assert.Greater(t, tableCount, 0, "Expected some tables")
		t.Logf("✅ Found %d tables in schema %s.%s", tableCount, config.Metadata.Catalog, config.Metadata.Schema)
	})
}

func TestE2E_ConnectionOptions(t *testing.T) {
	config, err := loadTestConfigE2E("/Users/jade.wang/tmp/adbc_test_config.json")
	if err != nil {
		t.Skipf("Could not load test config: %v", err)
	}

	hostname, httpPath, err := parseConnectionInfoE2E(config.URI)
	require.NoError(t, err)

	// Test different connection options
	tests := []struct {
		name    string
		options map[string]string
	}{
		{
			name: "with_timeout",
			options: map[string]string{
				databricks.OptionServerHostname: hostname,
				databricks.OptionHTTPPath:       httpPath,
				databricks.OptionAccessToken:    config.Token,
				databricks.OptionQueryTimeout:   "30s",
			},
		},
		{
			name: "with_max_rows",
			options: map[string]string{
				databricks.OptionServerHostname: hostname,
				databricks.OptionHTTPPath:       httpPath,
				databricks.OptionAccessToken:    config.Token,
				databricks.OptionMaxRows:        "100",
			},
		},
		{
			name: "with_catalog_schema",
			options: map[string]string{
				databricks.OptionServerHostname: hostname,
				databricks.OptionHTTPPath:       httpPath,
				databricks.OptionAccessToken:    config.Token,
				databricks.OptionCatalog:        config.Metadata.Catalog,
				databricks.OptionSchema:         config.Metadata.Schema,
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
