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
	"io"
	"net/url"
	"os"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc/driver/databricks"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/require"
)

func loadTestConfigPractical(configPath string) (*TestConfig, error) {
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

func parseConnectionInfoPractical(uri string) (hostname, httpPath string, err error) {
	parsed, err := url.Parse(uri)
	if err != nil {
		return "", "", err
	}

	hostname = parsed.Host
	httpPath = parsed.Path
	return hostname, httpPath, nil
}

func TestPractical_RealTableQuery(t *testing.T) {
	config, err := loadTestConfigPractical("/Users/jade.wang/tmp/adbc_test_config.json")
	if err != nil {
		t.Skipf("Could not load test config: %v", err)
	}

	hostname, httpPath, err := parseConnectionInfoPractical(config.URI)
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

	// First, let's find a table that exists
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

	// Get list of tables first
	err = stmt.SetSqlQuery("SHOW TABLES LIMIT 1")
	require.NoError(t, err)

	reader, _, err := stmt.ExecuteQuery(ctx)
	require.NoError(t, err)
	defer reader.Release()

	if reader.Next() {
		record := reader.Record()
		t.Logf("✅ Found at least one table")
		t.Logf("Table listing columns: %d, rows: %d", record.NumCols(), record.NumRows())

		// Print column information
		schema := record.Schema()
		for i := 0; i < int(record.NumCols()); i++ {
			field := schema.Field(i)
			t.Logf("  Column %d: %s (%s)", i, field.Name, field.Type.String())
		}

		// Try to get the first table name if it's in the expected format
		if record.NumRows() > 0 && record.NumCols() >= 2 {
			// Typically SHOW TABLES returns database, tableName, isTemporary
			// Let's try querying the first table
			stmt2, err := conn.NewStatement()
			require.NoError(t, err)
			defer func() { _ = stmt2.Close() }()

			// Use a simple SELECT with LIMIT to test querying a real table
			err = stmt2.SetSqlQuery("SELECT * FROM (SHOW TABLES LIMIT 1)")
			require.NoError(t, err)

			reader2, rowsAffected, err := stmt2.ExecuteQuery(ctx)
			require.NoError(t, err)
			defer reader2.Release()

			t.Logf("✅ Successfully queried table metadata, rows affected: %d", rowsAffected)

			recordCount := 0
			for reader2.Next() {
				record := reader2.Record()
				recordCount++
				t.Logf("Result record %d: %d columns, %d rows", recordCount, record.NumCols(), record.NumRows())
			}
		}
	}
}

func TestPractical_CommonQueries(t *testing.T) {
	config, err := loadTestConfigPractical("/Users/jade.wang/tmp/adbc_test_config.json")
	if err != nil {
		t.Skipf("Could not load test config: %v", err)
	}

	hostname, httpPath, err := parseConnectionInfoPractical(config.URI)
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

	// Test various common queries
	queries := []struct {
		name  string
		query string
	}{
		{"current_catalog", "SELECT current_catalog()"},
		{"current_schema", "SELECT current_schema()"},
		{"current_user", "SELECT current_user()"},
		{"version", "SELECT version()"},
		{"simple_math", "SELECT 1 + 1 AS result, 'hello' AS greeting"},
		{"date_functions", "SELECT current_date(), current_timestamp()"},
	}

	for _, q := range queries {
		t.Run(q.name, func(t *testing.T) {
			stmt, err := conn.NewStatement()
			require.NoError(t, err)
			defer func() { _ = stmt.Close() }()

			err = stmt.SetSqlQuery(q.query)
			require.NoError(t, err)

			reader, rowsAffected, err := stmt.ExecuteQuery(ctx)
			require.NoError(t, err)
			defer reader.Release()

			t.Logf("✅ Query '%s' executed successfully, rows affected: %d", q.name, rowsAffected)

			recordCount := 0
			for reader.Next() {
				record := reader.Record()
				recordCount++
				t.Logf("  Record %d: %d columns, %d rows", recordCount, record.NumCols(), record.NumRows())
			}
		})
	}
}
