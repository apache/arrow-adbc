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
	"fmt"
	"os"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/databricks"
	"github.com/apache/arrow-adbc/go/adbc/validation"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/suite"
)

type DatabricksQuirks struct {
	ctx      context.Context
	mem      *memory.CheckedAllocator
	hostname string
	httpPath string
	token    string
	catalog  string
	schema   string
}

func (q *DatabricksQuirks) SetupDriver(t *testing.T) adbc.Driver {
	q.mem = memory.NewCheckedAllocator(memory.DefaultAllocator)
	return databricks.NewDriver(q.mem)
}

func (q *DatabricksQuirks) TearDownDriver(t *testing.T, _ adbc.Driver) {
	q.mem.AssertSize(t, 0)
}

func (q *DatabricksQuirks) Alloc() memory.Allocator {
	return q.mem
}

func (q *DatabricksQuirks) DatabaseOptions() map[string]string {
	return map[string]string{
		databricks.OptionServerHostname: q.hostname,
		databricks.OptionHTTPPath:       q.httpPath,
		databricks.OptionAccessToken:    q.token,
		databricks.OptionCatalog:        q.catalog,
		databricks.OptionSchema:         q.schema,
	}
}

func (q *DatabricksQuirks) BindParameter(index int) string {
	return "?"
}

func (q *DatabricksQuirks) SupportsBulkIngest(mode string) bool {
	return false // Databricks SQL doesn't support bulk ingest via ADBC yet
}

func (q *DatabricksQuirks) SupportsConcurrentStatements() bool {
	return true
}

func (q *DatabricksQuirks) SupportsCurrentCatalogSchema() bool {
	return true
}

func (q *DatabricksQuirks) SupportsGetSetOptions() bool {
	return true
}

func (q *DatabricksQuirks) SupportsExecuteSchema() bool {
	return false
}

func (q *DatabricksQuirks) SupportsPartitionedData() bool {
	return false // Databricks SQL doesn't support partitioned result sets
}

func (q *DatabricksQuirks) SupportsStatistics() bool {
	return false
}

func (q *DatabricksQuirks) SupportsTransactions() bool {
	return false // Databricks SQL has limited transaction support
}

func (q *DatabricksQuirks) SupportsGetParameterSchema() bool {
	return false // Not implemented yet
}

func (q *DatabricksQuirks) SupportsDynamicParameterBinding() bool {
	return true
}

func (q *DatabricksQuirks) SupportsErrorIngestIncompatibleSchema() bool {
	return false
}

func (q *DatabricksQuirks) GetMetadata(code adbc.InfoCode) interface{} {
	// Return default values for info codes
	switch code {
	case adbc.InfoDriverName:
		return "Databricks"
	case adbc.InfoDriverVersion:
		return "1.0.0"
	default:
		return nil
	}
}

func (q *DatabricksQuirks) Catalog() string {
	return q.catalog
}

func (q *DatabricksQuirks) DBSchema() string {
	return q.schema
}

func (q *DatabricksQuirks) SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata {
	return arrow.Metadata{}
}

func (q *DatabricksQuirks) CreateSampleTable(tableName string, r arrow.Record) error {
	// This would need to create a table in Databricks
	// For now, we'll skip this functionality in tests
	return nil
}

func (q *DatabricksQuirks) DropTable(cn adbc.Connection, tableName string) error {
	// This would need to drop a table in Databricks
	stmt, err := cn.NewStatement()
	if err != nil {
		return err
	}
	defer func() { _ = stmt.Close() }()

	if err := stmt.SetSqlQuery(fmt.Sprintf("DROP TABLE IF EXISTS %s", tableName)); err != nil {
		return err
	}

	_, err = stmt.ExecuteUpdate(q.ctx)
	return err
}

func TestDatabricksValidation(t *testing.T) {
	// Skip if credentials not provided
	hostname := os.Getenv("DATABRICKS_SERVER_HOSTNAME")
	httpPath := os.Getenv("DATABRICKS_HTTP_PATH")
	token := os.Getenv("DATABRICKS_ACCESS_TOKEN")
	catalog := os.Getenv("DATABRICKS_CATALOG")
	schema := os.Getenv("DATABRICKS_SCHEMA")

	if hostname == "" || httpPath == "" || token == "" {
		t.Skip("Databricks credentials not provided (DATABRICKS_SERVER_HOSTNAME, DATABRICKS_HTTP_PATH, DATABRICKS_ACCESS_TOKEN)")
	}

	// Use defaults if not specified
	if catalog == "" {
		catalog = "main"
	}
	if schema == "" {
		schema = "default"
	}

	quirks := &DatabricksQuirks{
		ctx:      context.Background(),
		hostname: hostname,
		httpPath: httpPath,
		token:    token,
		catalog:  catalog,
		schema:   schema,
	}

	// Run basic validation tests
	suite.Run(t, &validation.DatabaseTests{
		Quirks: quirks,
	})

	suite.Run(t, &validation.ConnectionTests{
		Quirks: quirks,
	})

	suite.Run(t, &validation.StatementTests{
		Quirks: quirks,
	})
}
