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
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	_ "github.com/databricks/databricks-sql-go"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	// Connection parameters
	serverHostname string
	httpPath       string
	accessToken    string
	port           string
	catalog        string
	dbSchema       string

	// Query options
	queryTimeout        time.Duration
	maxRows             int64
	queryRetryCount     int
	downloadThreadCount int

	// TLS/SSL options
	sslMode     string
	sslRootCert string

	// OAuth options
	oauthClientID     string
	oauthClientSecret string
	oauthRefreshToken string

	// Database connection
	db *sql.DB

	// Current autocommit state
	autocommit bool
}

func (c *connectionImpl) connect(ctx context.Context) error {
	// Build connection string
	dsn := c.buildConnectionString()

	// Open connection using databricks-sql-go
	db, err := sql.Open("databricks", dsn)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to open connection: %v", err),
		}
	}

	// Test the connection
	if err := db.PingContext(ctx); err != nil {
		_ = db.Close() // Ignore error on cleanup
		return adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to ping database: %v", err),
		}
	}

	c.db = db
	c.autocommit = true // Databricks defaults to autocommit

	return nil
}

func (c *connectionImpl) buildConnectionString() string {
	// DSN format: token:access_token@hostname:port/httpPath?params

	// Get port or use default
	port := c.port
	if port == "" {
		port = "443" // Default HTTPS port for Databricks
	}

	// Build base DSN
	dsn := fmt.Sprintf("token:%s@%s:%s%s", c.accessToken, c.serverHostname, port, c.httpPath)

	// Build query parameters
	var params []string

	if c.catalog != "" {
		params = append(params, fmt.Sprintf("catalog=%s", c.catalog))
	}
	if c.dbSchema != "" {
		params = append(params, fmt.Sprintf("schema=%s", c.dbSchema))
	}
	if c.queryTimeout > 0 {
		// Convert duration to seconds for databricks-sql-go
		timeoutSeconds := int(c.queryTimeout.Seconds())
		params = append(params, fmt.Sprintf("timeout=%d", timeoutSeconds))
	}
	if c.maxRows > 0 {
		params = append(params, fmt.Sprintf("maxRows=%d", c.maxRows))
	}
	if c.downloadThreadCount > 0 {
		params = append(params, fmt.Sprintf("maxDownloadThreads=%d", c.downloadThreadCount))
	}

	// Add parameters to DSN if any
	if len(params) > 0 {
		dsn += "?" + strings.Join(params, "&")
	}

	return dsn
}

func (c *connectionImpl) Close() error {
	if c.db != nil {
		return c.db.Close()
	}
	return nil
}

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statementImpl{
		conn: c,
	}, nil
}

// Autocommit interface implementation
func (c *connectionImpl) GetAutocommit() bool {
	return c.autocommit
}

func (c *connectionImpl) SetAutocommit(autocommit bool) error {
	// Databricks SQL doesn't support explicit transaction control in the same way
	// as traditional databases. Most operations are implicitly committed.
	// We'll track the autocommit state but won't change the underlying connection behavior.
	c.autocommit = autocommit
	return nil
}

// CurrentNamespacer interface implementation
func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return c.catalog, nil
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return c.dbSchema, nil
}

func (c *connectionImpl) SetCurrentCatalog(catalog string) error {
	// Use the database to execute USE CATALOG
	if c.db != nil && catalog != "" {
		_, err := c.db.Exec(fmt.Sprintf("USE CATALOG %s", catalog))
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to set catalog: %v", err),
			}
		}
	}
	c.catalog = catalog
	return nil
}

func (c *connectionImpl) SetCurrentDbSchema(schema string) error {
	// Use the database to execute USE SCHEMA
	if c.db != nil && schema != "" {
		_, err := c.db.Exec(fmt.Sprintf("USE SCHEMA %s", schema))
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to set schema: %v", err),
			}
		}
	}
	c.dbSchema = schema
	return nil
}

// TableTypeLister interface implementation
func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// Databricks supports these table types
	return []string{"TABLE", "VIEW", "EXTERNAL_TABLE", "MANAGED_TABLE"}, nil
}

func (c *connectionImpl) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	// Databricks supports these table types
	tableTypes := []string{"TABLE", "VIEW", "EXTERNAL_TABLE", "MANAGED_TABLE"}

	// Create Arrow schema for table types
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "table_type", Type: arrow.BinaryTypes.String},
	}, nil)

	// Create record batch
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	tableTypeBuilder := bldr.Field(0).(*array.StringBuilder)
	for _, tableType := range tableTypes {
		tableTypeBuilder.Append(tableType)
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	reader, err := array.NewRecordReader(schema, []arrow.Record{rec})
	if err != nil {
		return nil, err
	}
	return reader, nil
}

// Transaction methods (Databricks has limited transaction support)
func (c *connectionImpl) Commit(ctx context.Context) error {
	// Databricks SQL doesn't support explicit transactions in the traditional sense.
	// Most operations are auto-committed. We'll track state but not perform any operation.
	return nil
}

func (c *connectionImpl) Rollback(ctx context.Context) error {
	// Databricks SQL doesn't support explicit transactions in the traditional sense.
	// Most operations are auto-committed. We'll track state but not perform any operation.
	return nil
}

// DbObjectsEnumerator interface implementation
func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	query := "SHOW CATALOGS"
	if catalogFilter != nil {
		query += fmt.Sprintf(" LIKE '%s'", *catalogFilter)
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to query catalogs: %v", err),
		}
	}
	defer func() { _ = rows.Close() }()

	var catalogs []string
	for rows.Next() {
		var catalog string
		if err := rows.Scan(&catalog); err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to scan catalog: %v", err),
			}
		}
		catalogs = append(catalogs, catalog)
	}

	return catalogs, rows.Err()
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	query := fmt.Sprintf("SHOW SCHEMAS IN %s", catalog)
	if schemaFilter != nil {
		query += fmt.Sprintf(" LIKE '%s'", *schemaFilter)
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to query schemas: %v", err),
		}
	}
	defer func() { _ = rows.Close() }()

	var schemas []string
	for rows.Next() {
		var schema string
		if err := rows.Scan(&schema); err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to scan schema: %v", err),
			}
		}
		schemas = append(schemas, schema)
	}

	return schemas, rows.Err()
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	query := fmt.Sprintf("SHOW TABLES IN %s.%s", catalog, schema)
	if tableFilter != nil {
		query += fmt.Sprintf(" LIKE '%s'", *tableFilter)
	}

	rows, err := c.db.QueryContext(ctx, query)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to query tables: %v", err),
		}
	}
	defer func() { _ = rows.Close() }()

	var tables []driverbase.TableInfo
	for rows.Next() {
		var database, tableName, isTemporary string
		if err := rows.Scan(&database, &tableName, &isTemporary); err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to scan table: %v", err),
			}
		}

		tableInfo := driverbase.TableInfo{
			TableName:        tableName,
			TableType:        "TABLE", // Default to TABLE, could be improved with more detailed queries
			TableColumns:     nil,     // Schema would need separate query
			TableConstraints: nil,     // Constraints would need separate query
		}

		tables = append(tables, tableInfo)
	}

	return tables, rows.Err()
}

func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	// This is a simplified implementation. A full implementation would need to:
	// 1. Query INFORMATION_SCHEMA or system tables
	// 2. Build proper Arrow record structure
	// 3. Handle all the filtering parameters

	// For now, return empty result
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String},
		{Name: "catalog_db_schemas", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	}, nil)

	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	rec := bldr.NewRecord()
	defer rec.Release()

	reader, err := array.NewRecordReader(schema, []arrow.Record{rec})
	if err != nil {
		return nil, err
	}
	return reader, nil
}
