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
	"errors"
	"fmt"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	_ "github.com/databricks/databricks-sql-go"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	// Connection settings
	catalog  string
	dbSchema string

	// Database connection
	conn *sql.Conn
}

func (c *connectionImpl) Close() error {
	if c.conn == nil {
		return adbc.Error{Code: adbc.StatusInvalidState}
	}
	defer func() {
		c.conn = nil
	}()
	return c.conn.Close()
}

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statementImpl{
		conn: c,
	}, nil
}

func (c *connectionImpl) SetAutocommit(autocommit bool) error {
	// Databricks SQL doesn't support explicit transaction control in the same way
	// as traditional databases. Most operations are implicitly committed.
	if !autocommit {
		return adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("disabling autocommit is not supported"),
		}
	}
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
	if catalog == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "catalog cannot be empty",
		}
	}
	if c.conn == nil {
		return nil
	}
	escapedCatalog := strings.ReplaceAll(catalog, "`", "``")
	_, err := c.conn.ExecContext(context.Background(), fmt.Sprintf("USE CATALOG `%s`", escapedCatalog))
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to set catalog: %v", err),
		}
	}
	c.catalog = catalog
	return nil
}

func (c *connectionImpl) SetCurrentDbSchema(schema string) error {
	if schema == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  "schema cannot be empty",
		}
	}
	if c.conn == nil {
		return nil
	}
	escapedSchema := strings.ReplaceAll(schema, "`", "``")
	_, err := c.conn.ExecContext(context.Background(), fmt.Sprintf("USE SCHEMA `%s`", escapedSchema))
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to set schema: %v", err),
		}
	}
	c.dbSchema = schema
	return nil
}

// TableTypeLister interface implementation
func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// Databricks supports these table types
	return []string{"TABLE", "VIEW", "EXTERNAL_TABLE", "MANAGED_TABLE", "STREAMING_TABLE", "MATERIALIZED_VIEW"}, nil
}

// Transaction methods (Databricks has limited transaction support)
func (c *connectionImpl) Commit(ctx context.Context) error {
	// Most operations are auto-committed.
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("Commit is not supported"),
	}
}

func (c *connectionImpl) Rollback(ctx context.Context) error {
	// Databricks SQL doesn't support explicit transactions in the traditional sense.
	// Most operations are auto-committed.
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("rollback is not supported"),
	}
}

// DbObjectsEnumerator interface implementation
func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	query := "SHOW CATALOGS"
	if catalogFilter != nil {
		escapedFilter := strings.ReplaceAll(*catalogFilter, "'", "''")
		query += fmt.Sprintf(" LIKE '%s'", escapedFilter)
	}

	rows, err := c.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to query catalogs: %v", err),
		}
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

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

	return catalogs, errors.Join(err, rows.Err())
}

func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	escapedCatalog := strings.ReplaceAll(catalog, "`", "``")
	query := fmt.Sprintf("SHOW SCHEMAS IN `%s`", escapedCatalog)
	if schemaFilter != nil {
		escapedFilter := strings.ReplaceAll(*schemaFilter, "'", "''")
		query += fmt.Sprintf(" LIKE '%s'", escapedFilter)
	}

	rows, err := c.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to query schemas: %v", err),
		}
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

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

	err = errors.Join(err, rows.Err())
	return schemas, err
}

func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	escapedCatalog := strings.ReplaceAll(catalog, "`", "``")
	escapedSchema := strings.ReplaceAll(schema, "`", "``")
	query := fmt.Sprintf("SHOW TABLES IN `%s`.`%s`", escapedCatalog, escapedSchema)
	if tableFilter != nil {
		escapedFilter := strings.ReplaceAll(*tableFilter, "'", "''")
		query += fmt.Sprintf(" LIKE '%s'", escapedFilter)
	}

	rows, err := c.conn.QueryContext(ctx, query)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to query tables: %v", err),
		}
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

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

	return tables, errors.Join(err, rows.Err())
}
