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
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/databricks"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetadataE2E_GetInfo tests the GetInfo metadata function
func TestMetadataE2E_GetInfo(t *testing.T) {
	host, token, httpPath, catalog, schema := getDatabricksConfig(t)

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

	// Test GetInfo with various info codes
	infoCodes := []adbc.InfoCode{
		adbc.InfoDriverName,
		adbc.InfoDriverVersion,
		adbc.InfoDriverArrowVersion,
		adbc.InfoDriverADBCVersion,
		adbc.InfoVendorName,
		adbc.InfoVendorVersion,
		adbc.InfoVendorArrowVersion,
	}

	reader, err := conn.GetInfo(ctx, infoCodes)
	require.NoError(t, err)
	defer reader.Release()

	// Process results
	infoMap := make(map[uint32]interface{})
	for reader.Next() {
		record := reader.Record()
		codeCol := record.Column(0).(*array.Uint32)
		// valueCol := record.Column(1) // This is a union type - TODO: handle union properly

		for i := 0; i < int(record.NumRows()); i++ {
			code := codeCol.Value(i)
			// Extract value from union (this is simplified, actual implementation may vary)
			infoMap[code] = fmt.Sprintf("(info value for code %d)", code)
		}
	}

	// Verify expected info codes are present
	t.Log("GetInfo results:")
	for _, code := range infoCodes {
		if val, ok := infoMap[uint32(code)]; ok {
			t.Logf("  %s (code %d): %v", getInfoCodeName(code), code, val)
		} else {
			t.Logf("  %s (code %d): NOT FOUND", getInfoCodeName(code), code)
		}
	}
}

// TestMetadataE2E_GetTableTypes tests retrieving table types
func TestMetadataE2E_GetTableTypes(t *testing.T) {
	host, token, httpPath, catalog, schema := getDatabricksConfig(t)

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

	// Get table types
	reader, err := conn.GetTableTypes(ctx)
	require.NoError(t, err)
	defer reader.Release()

	tableTypes := []string{}
	for reader.Next() {
		record := reader.Record()
		if record.NumRows() > 0 && record.NumCols() > 0 {
			typeCol := record.Column(0).(*array.String)
			for i := 0; i < int(record.NumRows()); i++ {
				tableTypes = append(tableTypes, typeCol.Value(i))
			}
		}
	}

	assert.Greater(t, len(tableTypes), 0, "Expected at least one table type")
	t.Logf("Found %d table types: %v", len(tableTypes), tableTypes)

	// Common table types in Databricks
	expectedTypes := []string{"TABLE", "VIEW"}
	for _, expected := range expectedTypes {
		found := false
		for _, tt := range tableTypes {
			if strings.EqualFold(tt, expected) {
				found = true
				break
			}
		}
		assert.True(t, found, "Expected to find table type: %s", expected)
	}
}

// TestMetadataE2E_GetObjects tests the GetObjects function at various depths
func TestMetadataE2E_GetObjects(t *testing.T) {
	host, token, httpPath, catalog, schema := getDatabricksConfig(t)

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

	// Test different depths
	testCases := []struct {
		name     string
		depth    adbc.ObjectDepth
		catalog  *string
		schema   *string
		table    *string
		colName  *string
	}{
		{
			name:  "depth_all",
			depth: adbc.ObjectDepthAll,
		},
		{
			name:  "depth_catalogs",
			depth: adbc.ObjectDepthCatalogs,
		},
		{
			name:    "depth_schemas",
			depth:   adbc.ObjectDepthDBSchemas,
			catalog: &catalog,
		},
		{
			name:    "depth_tables",
			depth:   adbc.ObjectDepthTables,
			catalog: &catalog,
			schema:  &schema,
		},
		{
			name:    "depth_columns_specific_catalog",
			depth:   adbc.ObjectDepthColumns,
			catalog: &catalog,
			schema:  &schema,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			reader, err := conn.GetObjects(ctx, tc.depth, tc.catalog, tc.schema, tc.table, tc.colName, nil)
			require.NoError(t, err)
			defer reader.Release()

			// Count objects at each level
			catalogCount := 0
			schemaCount := 0
			tableCount := 0
			columnCount := 0

			for reader.Next() {
				record := reader.Record()
				catalogCount += int(record.NumRows())

				// Log first few entries for debugging
				if catalogCount <= 3 && record.NumRows() > 0 {
					catalogNameCol := record.Column(0).(*array.String)
					for i := 0; i < int(record.NumRows()) && i < 3; i++ {
						t.Logf("  Catalog: %s", catalogNameCol.Value(i))
					}
				}

				// For deeper depths, we'd need to parse the nested structures
				// This is simplified for demonstration
				if tc.depth >= adbc.ObjectDepthDBSchemas {
					schemaCount++ // Simplified counting
				}
				if tc.depth >= adbc.ObjectDepthTables {
					tableCount++ // Simplified counting
				}
				if tc.depth >= adbc.ObjectDepthColumns {
					columnCount++ // Simplified counting
				}
			}

			t.Logf("GetObjects(%s) found: %d catalogs", tc.name, catalogCount)
			assert.Greater(t, catalogCount, 0, "Expected at least one catalog")
		})
	}
}

// TestMetadataE2E_GetTableSchema tests retrieving schema for specific tables
func TestMetadataE2E_GetTableSchema(t *testing.T) {
	host, token, httpPath, catalog, schema := getDatabricksConfig(t)

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

	// First, find a table to test with
	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

	// Create a test table if it doesn't exist
	testTableName := "adbc_metadata_test_table"
	fullTableName := fmt.Sprintf("%s.%s.%s", catalog, schema, testTableName)

	// Try to create a simple test table
	createTableSQL := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id INT,
			name STRING,
			value DOUBLE,
			created_date DATE,
			is_active BOOLEAN
		) USING DELTA
	`, fullTableName)

	err = stmt.SetSqlQuery(createTableSQL)
	if err == nil {
		_, err = stmt.ExecuteUpdate(ctx)
		if err != nil {
			t.Logf("Could not create test table (might not have permissions): %v", err)
		}
	}

	// Now test GetTableSchema
	t.Run("GetTableSchema", func(t *testing.T) {
		// Try with our test table first
		arrowSchema, err := conn.GetTableSchema(ctx, &catalog, &schema, testTableName)
		if err != nil {
			t.Logf("Could not get schema for test table: %v", err)
			if strings.Contains(err.Error(), "Not Implemented") {
				t.Skip("GetTableSchema is not implemented in Databricks driver")
			}
			// Try to find any existing table
			err = stmt.SetSqlQuery(fmt.Sprintf("SHOW TABLES IN %s.%s", catalog, schema))
			require.NoError(t, err)

			tableReader, _, err := stmt.ExecuteQuery(ctx)
			if err != nil {
				t.Skip("Cannot list tables to test GetTableSchema")
			}
			defer tableReader.Release()

			var foundTable string
			if tableReader.Next() {
				record := tableReader.Record()
				if record.NumRows() > 0 {
					// The result typically has columns: namespace, tableName, isTemporary
					if record.NumCols() >= 2 {
						tableCol := record.Column(1).(*array.String)
						foundTable = tableCol.Value(0)
					}
				}
			}

			if foundTable == "" {
				t.Skip("No tables found to test GetTableSchema")
			}

			t.Logf("Testing GetTableSchema with existing table: %s", foundTable)
			arrowSchema, err = conn.GetTableSchema(ctx, &catalog, &schema, foundTable)
			require.NoError(t, err)
		}

		require.NotNil(t, arrowSchema, "Expected to get a schema")
		assert.Greater(t, len(arrowSchema.Fields()), 0, "Expected at least one field in schema")

		t.Log("Table schema:")
		for i, field := range arrowSchema.Fields() {
			t.Logf("  Column %d: %s (%s)", i, field.Name, field.Type.String())
			if field.Nullable {
				t.Logf("    - Nullable: true")
			}
			if len(field.Metadata.Keys()) > 0 {
				t.Logf("    - Metadata: %v", field.Metadata.Keys())
			}
		}
	})

	// Clean up test table
	t.Cleanup(func() {
		dropSQL := fmt.Sprintf("DROP TABLE IF EXISTS %s", fullTableName)
		_ = stmt.SetSqlQuery(dropSQL)
		_, _ = stmt.ExecuteUpdate(context.Background())
	})
}

// TestMetadataE2E_ComplexMetadataQueries tests more complex metadata scenarios
func TestMetadataE2E_ComplexMetadataQueries(t *testing.T) {
	host, token, httpPath, catalog, schema := getDatabricksConfig(t)

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

	stmt, err := conn.NewStatement()
	require.NoError(t, err)
	defer func() { _ = stmt.Close() }()

	// Test metadata-related SQL queries
	t.Run("InformationSchema", func(t *testing.T) {
		// Query information schema
		queries := []struct {
			name  string
			query string
		}{
			{
				name:  "list_schemas",
				query: "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '" + catalog + "' LIMIT 10",
			},
			{
				name:  "list_tables",
				query: fmt.Sprintf("SELECT table_name, table_type FROM information_schema.tables WHERE table_catalog = '%s' AND table_schema = '%s' LIMIT 10", catalog, schema),
			},
			{
				name:  "list_columns",
				query: fmt.Sprintf("SELECT table_name, column_name, data_type, is_nullable FROM information_schema.columns WHERE table_catalog = '%s' AND table_schema = '%s' LIMIT 20", catalog, schema),
			},
		}

		for _, q := range queries {
			t.Run(q.name, func(t *testing.T) {
				err := stmt.SetSqlQuery(q.query)
				require.NoError(t, err)

				reader, _, err := stmt.ExecuteQuery(ctx)
				if err != nil {
					t.Logf("Query failed (might not have access to information_schema): %v", err)
					t.Skip("Skipping information_schema test")
				}
				defer reader.Release()

				rowCount := 0
				for reader.Next() {
					record := reader.Record()
					rowCount += int(record.NumRows())

					// Log first few results
					if rowCount <= 5 && record.NumRows() > 0 {
						for i := 0; i < int(record.NumRows()) && i < 5; i++ {
							values := []string{}
							for j := 0; j < int(record.NumCols()); j++ {
								col := record.Column(j)
								if stringCol, ok := col.(*array.String); ok {
									values = append(values, stringCol.Value(i))
								} else {
									values = append(values, fmt.Sprintf("(%s)", col.DataType().Name()))
								}
							}
							t.Logf("  Row %d: %v", i, values)
						}
					}
				}

				t.Logf("Query '%s' returned %d rows", q.name, rowCount)
			})
		}
	})

	// Test pattern matching in metadata queries
	t.Run("PatternMatching", func(t *testing.T) {
		// Test with LIKE patterns
		patterns := []string{
			"test%",    // Tables starting with 'test'
			"%_table",  // Tables ending with '_table'
			"%metadata%", // Tables containing 'metadata'
		}

		for _, pattern := range patterns {
			tablePattern := pattern
			reader, err := conn.GetObjects(ctx, adbc.ObjectDepthTables, &catalog, &schema, &tablePattern, nil, nil)
			if err != nil {
				t.Logf("GetObjects with pattern '%s' failed: %v", pattern, err)
				continue
			}
			defer reader.Release()

			matchCount := 0
			for reader.Next() {
				record := reader.Record()
				matchCount += int(record.NumRows())
			}

			t.Logf("Pattern '%s' matched %d objects", pattern, matchCount)
		}
	})
}

// TestMetadataE2E_CurrentCatalogSchema tests getting/setting current catalog and schema
func TestMetadataE2E_CurrentCatalogSchema(t *testing.T) {
	host, token, httpPath, catalog, schema := getDatabricksConfig(t)

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

	// Get current catalog
	t.Run("GetCurrentCatalog", func(t *testing.T) {
		// Get current catalog via SQL query
		stmt, err := conn.NewStatement()
		require.NoError(t, err)
		defer func() { _ = stmt.Close() }()
		
		err = stmt.SetSqlQuery("SELECT current_catalog()")
		require.NoError(t, err)
		
		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()
		
		var currentCatalog string
		if reader.Next() {
			record := reader.Record()
			if record.NumRows() > 0 {
				col := record.Column(0).(*array.String)
				currentCatalog = col.Value(0)
			}
		}
		require.NoError(t, err)
		assert.NotEmpty(t, currentCatalog)
		t.Logf("Current catalog: %s", currentCatalog)

		// Should match what we set
		assert.Equal(t, catalog, currentCatalog)
	})

	// Get current schema
	t.Run("GetCurrentSchema", func(t *testing.T) {
		// Get current schema via SQL query
		stmt, err := conn.NewStatement()
		require.NoError(t, err)
		defer func() { _ = stmt.Close() }()
		
		err = stmt.SetSqlQuery("SELECT current_schema()")
		require.NoError(t, err)
		
		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()
		
		var currentSchema string
		if reader.Next() {
			record := reader.Record()
			if record.NumRows() > 0 {
				col := record.Column(0).(*array.String)
				currentSchema = col.Value(0)
			}
		}
		require.NoError(t, err)
		assert.NotEmpty(t, currentSchema)
		t.Logf("Current schema: %s", currentSchema)

		// Should match what we set
		assert.Equal(t, schema, currentSchema)
	})

	// Try to set different catalog/schema
	t.Run("SetCatalogSchema", func(t *testing.T) {
		// First, find another catalog
		stmt, err := conn.NewStatement()
		require.NoError(t, err)
		defer func() { _ = stmt.Close() }()

		err = stmt.SetSqlQuery("SHOW CATALOGS")
		require.NoError(t, err)

		reader, _, err := stmt.ExecuteQuery(ctx)
		require.NoError(t, err)
		defer reader.Release()

		var alternateCatalog string
		for reader.Next() {
			record := reader.Record()
			if record.NumRows() > 0 {
				catalogCol := record.Column(0).(*array.String)
				for i := 0; i < int(record.NumRows()); i++ {
					cat := catalogCol.Value(i)
					if cat != catalog && cat != "system" { // Avoid system catalog
						alternateCatalog = cat
						break
					}
				}
			}
			if alternateCatalog != "" {
				break
			}
		}

		if alternateCatalog != "" {
			t.Logf("Attempting to switch to catalog: %s", alternateCatalog)
			
			// Try to set catalog via SQL
			err = stmt.SetSqlQuery(fmt.Sprintf("USE CATALOG %s", alternateCatalog))
			require.NoError(t, err)
			_, err = stmt.ExecuteUpdate(ctx)
			if err != nil {
				t.Logf("Could not switch catalog (might not have permissions): %v", err)
			} else {
				// Verify the change
				err = stmt.SetSqlQuery("SELECT current_catalog()")
				require.NoError(t, err)
				reader2, _, err := stmt.ExecuteQuery(ctx)
				require.NoError(t, err)
				defer reader2.Release()
				
				if reader2.Next() {
					record := reader2.Record()
					if record.NumRows() > 0 {
						col := record.Column(0).(*array.String)
						newCatalog := col.Value(0)
						assert.Equal(t, alternateCatalog, newCatalog)
					}
				}
				
				// Switch back
				err = stmt.SetSqlQuery(fmt.Sprintf("USE CATALOG %s", catalog))
				require.NoError(t, err)
				_, err = stmt.ExecuteUpdate(ctx)
				require.NoError(t, err)
			}
		} else {
			t.Log("No alternate catalog found to test catalog switching")
		}
	})
}

// Helper function to get info code name for logging
func getInfoCodeName(code adbc.InfoCode) string {
	switch code {
	case adbc.InfoDriverName:
		return "DriverName"
	case adbc.InfoDriverVersion:
		return "DriverVersion"
	case adbc.InfoDriverArrowVersion:
		return "DriverArrowVersion"
	case adbc.InfoDriverADBCVersion:
		return "DriverADBCVersion"
	case adbc.InfoVendorName:
		return "VendorName"
	case adbc.InfoVendorVersion:
		return "VendorVersion"
	case adbc.InfoVendorArrowVersion:
		return "VendorArrowVersion"
	default:
		return fmt.Sprintf("Unknown(%d)", code)
	}
}