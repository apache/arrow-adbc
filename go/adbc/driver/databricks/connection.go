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

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/databricks/databricks-sdk-go"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	client *databricks.WorkspaceClient
	// Default Catalog name (optional)
	catalog string
	// Default Schema name (optional)
	dbSchema string
}

func sanitizeSchema(schema string) (string, error) {
	// TODO(felipecrv): sanitize databricks schemas
	return schema, nil
}

func (conn *connectionImpl) StatementExecution() sql.StatementExecutionInterface {
	if conn.client == nil {
		return nil
	}
	return conn.client.StatementExecution
}

// driverbase.CurrentNamespacer {{{

func (conn *connectionImpl) GetCurrentCatalog() (string, error) {
	return conn.catalog, nil
}

func (conn *connectionImpl) GetCurrentDbSchema() (string, error) {
	return conn.dbSchema, nil
}

func (conn *connectionImpl) SetCurrentCatalog(value string) error {
	conn.catalog = value
	return nil
}

func (conn *connectionImpl) SetCurrentDbSchema(value string) error {
	sanitized, err := sanitizeSchema(value)
	if err != nil {
		return err
	}
	conn.dbSchema = sanitized
	return nil
}

// }}}

// driverbase.TableTypeLister {{{

func (c *connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	// TODO(felipecrv): implement ListTableTypes
	return []string{
		"TABLE",
		"VIEW",
	}, nil
}

// }}}

// driverbase.AutocommitSetter {{{

func (conn *connectionImpl) SetAutocommit(enabled bool) error {
	if enabled {
		return nil
	}
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "SetAutocommit to `false` is not yet implemented",
	}
}

// }}}

// driverbase.DbObjectsEnumerator {{{

func (conn *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	catalogPattern, err := internal.PatternToRegexp(catalogFilter)
	if err != nil {
		return nil, err
	}
	if catalogPattern == nil {
		catalogPattern = internal.AcceptAll
	}

	res := make([]string, 0)
	// TODO: implement GetCatalogs
	// for _, catalog := range conn.client.Catalogs() {
	//   if catalogPattern.MatchString(catalog) {
	//     res = append(res, catalog)
	//   }
	// }
	return res, nil
}

func (conn *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	schemaPattern, err := internal.PatternToRegexp(schemaFilter)
	if err != nil {
		return nil, err
	}
	if schemaPattern == nil {
		schemaPattern = internal.AcceptAll
	}

	res := make([]string, 0)
	// TODO: implement GetDBSchemasForCatalog
	return res, nil
}

func (conn *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	tablePattern, err := internal.PatternToRegexp(tableFilter)
	if err != nil {
		return nil, err
	}
	if tablePattern == nil {
		tablePattern = internal.AcceptAll
	}

	res := make([]driverbase.TableInfo, 0)
	// TODO: implement GetTablesForDBSchema
	return res, nil
}

// }}}

// NewStatement initializes a new statement object tied to this connection
func (conn *connectionImpl) NewStatement() (adbc.Statement, error) {
	return NewStatement(conn)
}

// Close closes this connection and releases any associated resources.
func (conn *connectionImpl) Close() error {
	// TODO: think about the consequences of this to statements and readers
	conn.client = nil
	conn.Closed = true
	return nil
}
