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

// Package validation is a driver-agnostic test suite intended to aid in
// driver development for ADBC drivers. It provides a series of utilities
// and defined tests that can be used to validate a driver follows the
// correct and expected behavior.
package validation

import (
	"context"
	"io"
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/utils"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"
)

type DriverQuirks interface {
	// Called in SetupTest to initialize anything needed for testing
	SetupDriver(*testing.T) adbc.Driver
	// Called in TearDownTest to clean up anything necessary in between tests
	TearDownDriver(*testing.T, adbc.Driver)
	// Return the list of key/value pairs of options to pass when
	// calling NewDatabase
	DatabaseOptions() map[string]string
	// Return the SQL to reference the bind parameter for a given index
	BindParameter(index int) string
	// Whether the driver supports bulk ingest
	SupportsBulkIngest(mode string) bool
	// Whether two statements can be used at the same time on a single connection
	SupportsConcurrentStatements() bool
	// Whether current catalog/schema are supported
	SupportsCurrentCatalogSchema() bool
	// Whether GetSetOptions is supported
	SupportsGetSetOptions() bool
	// Whether AdbcStatementExecuteSchema should work
	SupportsExecuteSchema() bool
	// Whether AdbcStatementExecutePartitions should work
	SupportsPartitionedData() bool
	// Whether statistics are supported
	SupportsStatistics() bool
	// Whether transactions are supported (Commit/Rollback on connection)
	SupportsTransactions() bool
	// Whether retrieving the schema of prepared statement params is supported
	SupportsGetParameterSchema() bool
	// Whether it supports dynamic parameter binding in queries
	SupportsDynamicParameterBinding() bool
	// Whether it returns an error when attempting to ingest with an incompatible schema
	SupportsErrorIngestIncompatibleSchema() bool
	// Expected Metadata responses
	GetMetadata(adbc.InfoCode) interface{}
	// Create a sample table from an arrow record
	CreateSampleTable(tableName string, r arrow.Record) error
	// Field Metadata for Sample Table for comparison
	SampleTableSchemaMetadata(tblName string, dt arrow.DataType) arrow.Metadata
	// have the driver drop a table with the correct SQL syntax
	DropTable(adbc.Connection, string) error

	Catalog() string
	DBSchema() string

	Alloc() memory.Allocator
}

type DatabaseTests struct {
	suite.Suite

	Driver adbc.Driver
	Quirks DriverQuirks
}

func (d *DatabaseTests) SetupTest() {
	d.Driver = d.Quirks.SetupDriver(d.T())
}

func (d *DatabaseTests) TearDownTest() {
	d.Quirks.TearDownDriver(d.T(), d.Driver)
	d.Driver = nil
}

func (d *DatabaseTests) TestNewDatabase() {
	db, err := d.Driver.NewDatabase(d.Quirks.DatabaseOptions())
	d.NoError(err)
	d.NotNil(db)
	d.Implements((*adbc.Database)(nil), db)
	d.NoError(db.Close())
}

type ConnectionTests struct {
	suite.Suite

	Driver adbc.Driver
	Quirks DriverQuirks

	DB adbc.Database
}

func (c *ConnectionTests) SetupTest() {
	c.Driver = c.Quirks.SetupDriver(c.T())
	var err error
	c.DB, err = c.Driver.NewDatabase(c.Quirks.DatabaseOptions())
	c.Require().NoError(err)
}

func (c *ConnectionTests) TearDownTest() {
	c.Quirks.TearDownDriver(c.T(), c.Driver)
	c.Driver = nil
	c.NoError(c.DB.Close())
	c.DB = nil
}

func (c *ConnectionTests) TestGetSetOptions() {
	cnxn, err := c.DB.Open(context.Background())
	c.NoError(err)
	c.NotNil(cnxn)

	stmt, err := cnxn.NewStatement()
	c.NoError(err)
	c.NotNil(stmt)

	expected := c.Quirks.SupportsGetSetOptions()

	_, ok := c.DB.(adbc.GetSetOptions)
	c.Equal(expected, ok)

	_, ok = cnxn.(adbc.GetSetOptions)
	c.Equal(expected, ok)

	_, ok = stmt.(adbc.GetSetOptions)
	c.Equal(expected, ok)

	c.NoError(stmt.Close())
	c.NoError(cnxn.Close())
}

func (c *ConnectionTests) TestNewConn() {
	cnxn, err := c.DB.Open(context.Background())
	c.NoError(err)
	c.NotNil(cnxn)

	c.NoError(cnxn.Close())
}

func (c *ConnectionTests) TestCloseConnTwice() {
	cnxn, err := c.DB.Open(context.Background())
	c.NoError(err)
	c.NotNil(cnxn)

	c.NoError(cnxn.Close())
	err = cnxn.Close()
	var adbcError adbc.Error
	c.ErrorAs(err, &adbcError)
	c.Equal(adbc.StatusInvalidState, adbcError.Code)
}

func (c *ConnectionTests) TestConcurrent() {
	cnxn, err := c.DB.Open(context.Background())
	c.Require().NoError(err)
	cnxn2, err := c.DB.Open(context.Background())
	c.Require().NoError(err)

	c.NoError(cnxn.Close())
	c.NoError(cnxn2.Close())
}

func (c *ConnectionTests) TestAutocommitDefault() {
	ctx := context.Background()
	// even if not supported, drivers should act as if autocommit
	// is enabled, and return INVALID_STATE if the client tries to
	// commit or rollback
	cnxn, err := c.DB.Open(ctx)
	c.NoError(err)
	defer CheckedClose(c.T(), cnxn)

	if getset, ok := cnxn.(adbc.GetSetOptions); ok {
		value, err := getset.GetOption(adbc.OptionKeyAutoCommit)
		c.NoError(err)
		c.Equal(adbc.OptionValueEnabled, value)
	}

	expectedCode := adbc.StatusInvalidState
	var adbcError adbc.Error
	err = cnxn.Commit(ctx)
	c.ErrorAs(err, &adbcError)
	c.Equal(expectedCode, adbcError.Code)
	err = cnxn.Rollback(ctx)
	c.ErrorAs(err, &adbcError)
	c.Equal(expectedCode, adbcError.Code)

	// if the driver supports setting options after init, it should error
	// on an invalid option value for autocommit
	if cnxnopts, ok := cnxn.(adbc.PostInitOptions); ok {
		c.Error(cnxnopts.SetOption(adbc.OptionKeyAutoCommit, "invalid"))
	}
}

func (c *ConnectionTests) TestAutocommitToggle() {
	ctx := context.Background()
	cnxn, err := c.DB.Open(ctx)
	c.NoError(err)
	defer CheckedClose(c.T(), cnxn)

	if !c.Quirks.SupportsTransactions() {
		return
	}

	// if the connection doesn't support setting options after init
	// then there's nothing to test here
	cnxnopt, ok := cnxn.(adbc.PostInitOptions)
	if !ok {
		return
	}

	// it is ok to enable autocommit when it is already enabled
	c.NoError(cnxnopt.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueEnabled))
	c.NoError(cnxnopt.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))

	if getset, ok := cnxn.(adbc.GetSetOptions); ok {
		value, err := getset.GetOption(adbc.OptionKeyAutoCommit)
		c.NoError(err)
		c.Equal(adbc.OptionValueDisabled, value)
	}

	// it is ok to disable autocommit when it isn't enabled
	c.NoError(cnxnopt.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))

	if getset, ok := cnxn.(adbc.GetSetOptions); ok {
		value, err := getset.GetOption(adbc.OptionKeyAutoCommit)
		c.NoError(err)
		c.Equal(adbc.OptionValueDisabled, value)
	}
}

func (c *ConnectionTests) TestMetadataCurrentCatalog() {
	ctx := context.Background()
	cnxn, err := c.DB.Open(ctx)
	c.NoError(err)
	defer CheckedClose(c.T(), cnxn)
	getset, ok := cnxn.(adbc.GetSetOptions)

	if !c.Quirks.SupportsGetSetOptions() {
		c.False(ok)
		return
	}
	c.True(ok)
	value, err := getset.GetOption(adbc.OptionKeyCurrentCatalog)
	if c.Quirks.SupportsCurrentCatalogSchema() {
		c.NoError(err)
		c.Equal(c.Quirks.Catalog(), value)
	} else {
		c.Error(err)
	}
}

func (c *ConnectionTests) TestMetadataCurrentDbSchema() {
	ctx := context.Background()
	cnxn, err := c.DB.Open(ctx)
	c.NoError(err)
	defer CheckedClose(c.T(), cnxn)
	getset, ok := cnxn.(adbc.GetSetOptions)

	if !c.Quirks.SupportsGetSetOptions() {
		c.False(ok)
		return
	}
	c.True(ok)
	value, err := getset.GetOption(adbc.OptionKeyCurrentDbSchema)
	if c.Quirks.SupportsCurrentCatalogSchema() {
		c.NoError(err)
		c.Equal(c.Quirks.DBSchema(), value)
	} else {
		c.Error(err)
	}
}

func (c *ConnectionTests) TestMetadataGetInfo() {
	ctx := context.Background()
	cnxn, err := c.DB.Open(ctx)
	c.NoError(err)
	defer CheckedClose(c.T(), cnxn)

	info := []adbc.InfoCode{
		adbc.InfoDriverName,
		adbc.InfoDriverVersion,
		adbc.InfoDriverArrowVersion,
		adbc.InfoDriverADBCVersion,
		adbc.InfoVendorName,
		adbc.InfoVendorVersion,
		adbc.InfoVendorArrowVersion,
	}

	rdr, err := cnxn.GetInfo(ctx, info)
	c.Require().NoError(err)
	defer rdr.Release()

	c.Truef(adbc.GetInfoSchema.Equal(rdr.Schema()), "expected: %s\ngot: %s",
		adbc.GetInfoSchema, rdr.Schema())

	for rdr.Next() {
		rec := rdr.Record()
		codeCol := rec.Column(0).(*array.Uint32)
		valUnion := rec.Column(1).(*array.DenseUnion)
		for i := 0; i < int(rec.NumRows()); i++ {
			code := codeCol.Value(i)
			child := valUnion.Field(valUnion.ChildID(i))
			offset := int(valUnion.ValueOffset(i))
			valUnion.GetOneForMarshal(i)
			if child.IsNull(offset) {
				exp := c.Quirks.GetMetadata(adbc.InfoCode(code))
				c.Nilf(exp, "got nil for info %s, expected: %s", adbc.InfoCode(code), exp)
			} else {
				expected := c.Quirks.GetMetadata(adbc.InfoCode(code))
				var actual interface{}

				switch valUnion.ChildID(i) {
				case 0:
					// String
					actual = child.(*array.String).Value(offset)
				case 1:
					// bool
					actual = child.(*array.Boolean).Value(offset)
				case 2:
					// int64
					actual = child.(*array.Int64).Value(offset)
				default:
					c.FailNow("Unknown union type code", valUnion.ChildID(i))
				}

				c.Equal(expected, actual, adbc.InfoCode(code).String())
			}
		}
	}
}

func (c *ConnectionTests) TestMetadataGetStatistics() {
	ctx := context.Background()
	cnxn, err := c.DB.Open(ctx)
	c.NoError(err)
	defer CheckedClose(c.T(), cnxn)

	if c.Quirks.SupportsStatistics() {
		stats, ok := cnxn.(adbc.ConnectionGetStatistics)
		c.True(ok)
		reader, err := stats.GetStatistics(ctx, nil, nil, nil, true)
		c.NoError(err)
		defer reader.Release()
	} else {
		stats, ok := cnxn.(adbc.ConnectionGetStatistics)
		if ok {
			_, err := stats.GetStatistics(ctx, nil, nil, nil, true)
			var adbcErr adbc.Error
			c.ErrorAs(err, &adbcErr)
			c.Equal(adbc.StatusNotImplemented, adbcErr.Code)
		}
	}
}

func (c *ConnectionTests) TestMetadataGetTableSchema() {
	rec, _, err := array.RecordFromJSON(c.Quirks.Alloc(), arrow.NewSchema(
		[]arrow.Field{
			{Name: "ints", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "strings", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil), strings.NewReader(`[
			{"ints": 42, "strings": "foo"},
			{"ints": -42, "strings": null},
			{"ints": null, "strings": ""}
		]`))
	c.Require().NoError(err)
	defer rec.Release()

	ctx := context.Background()
	cnxn, err := c.DB.Open(ctx)
	c.Require().NoError(err)
	defer CheckedClose(c.T(), cnxn)

	c.Require().NoError(c.Quirks.CreateSampleTable("sample_test", rec))

	sc, err := cnxn.GetTableSchema(ctx, nil, nil, "sample_test")
	c.Require().NoError(err)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int64, Nullable: true,
			Metadata: c.Quirks.SampleTableSchemaMetadata("sample_test", arrow.PrimitiveTypes.Int64)},
		{Name: "strings", Type: arrow.BinaryTypes.String, Nullable: true,
			Metadata: c.Quirks.SampleTableSchemaMetadata("sample_test", arrow.BinaryTypes.String)},
	}, nil)

	c.Truef(expectedSchema.Equal(sc), "expected: %s\ngot: %s", expectedSchema, sc)
}

func (c *ConnectionTests) TestMetadataGetTableTypes() {
	ctx := context.Background()
	cnxn, err := c.DB.Open(ctx)
	c.NoError(err)
	defer CheckedClose(c.T(), cnxn)

	rdr, err := cnxn.GetTableTypes(ctx)
	c.Require().NoError(err)
	defer rdr.Release()

	c.Truef(adbc.TableTypesSchema.Equal(rdr.Schema()), "expected: %s\ngot: %s", adbc.TableTypesSchema, rdr.Schema())
	c.True(rdr.Next())
}

func (c *ConnectionTests) TestMetadataGetObjectsColumns() {
	ctx := context.Background()
	cnxn, err := c.DB.Open(ctx)
	c.NoError(err)
	defer CheckedClose(c.T(), cnxn)

	ingestCatalogName := c.Quirks.Catalog()
	ingestSchemaName := c.Quirks.DBSchema()
	ingestTableName := "bulk_ingest"

	c.Require().NoError(c.Quirks.DropTable(cnxn, ingestTableName))
	rec, _, err := array.RecordFromJSON(c.Quirks.Alloc(), arrow.NewSchema(
		[]arrow.Field{
			{Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
			{Name: "strings", Type: arrow.BinaryTypes.String, Nullable: true},
		}, nil), strings.NewReader(`[
			{"int64s": 42, "strings": "foo"},
			{"int64s": -42, "strings": null},
			{"int64s": null, "strings": ""}
		]`))
	c.Require().NoError(err)
	defer rec.Release()

	c.Require().NoError(c.Quirks.CreateSampleTable(ingestTableName, rec))

	catalogFilterInvalid := ingestCatalogName + "_invalid"
	dbSchemaFilterInvalid := ingestSchemaName + "_invalid"
	tableFilterInvalid := ingestTableName + "_invalid"
	columnFilter := "in%"
	tests := []struct {
		name           string
		depth          adbc.ObjectDepth
		catalogFilter  *string
		dbSchemaFilter *string
		tableFilter    *string
		columnFilter   *string
		tableTypes     []string

		expectFindCatalog  bool
		expectFindDbSchema bool
		expectFindTable    bool
		expectedColnames   []string
		expectedPositions  []int32
	}{
		{
			name:              "depth catalog no filter",
			depth:             adbc.ObjectDepthCatalogs,
			expectFindCatalog: true,
		},
		{
			name:               "depth dbSchema no filter",
			depth:              adbc.ObjectDepthDBSchemas,
			expectFindCatalog:  true,
			expectFindDbSchema: true,
		},
		{
			name:               "depth table no filter",
			depth:              adbc.ObjectDepthTables,
			expectFindCatalog:  true,
			expectFindDbSchema: true,
			expectFindTable:    true,
		},
		{
			name:               "depth column no filter",
			depth:              adbc.ObjectDepthColumns,
			expectFindCatalog:  true,
			expectFindDbSchema: true,
			expectFindTable:    true,
			expectedColnames:   []string{"int64s", "strings"},
			expectedPositions:  []int32{1, 2},
		},
		{
			name:               "filter catalog valid",
			depth:              adbc.ObjectDepthColumns,
			catalogFilter:      &ingestCatalogName,
			expectFindCatalog:  true,
			expectFindDbSchema: true,
			expectFindTable:    true,
			expectedColnames:   []string{"int64s", "strings"},
			expectedPositions:  []int32{1, 2},
		},
		{
			name:          "filter catalog invalid",
			depth:         adbc.ObjectDepthColumns,
			catalogFilter: &catalogFilterInvalid,
		},
		{
			name:               "filter dbSchema valid",
			depth:              adbc.ObjectDepthColumns,
			dbSchemaFilter:     &ingestSchemaName,
			expectFindCatalog:  true,
			expectFindDbSchema: true,
			expectFindTable:    true,
			expectedColnames:   []string{"int64s", "strings"},
			expectedPositions:  []int32{1, 2},
		},
		{
			name:              "filter dbSchema invalid",
			depth:             adbc.ObjectDepthColumns,
			dbSchemaFilter:    &dbSchemaFilterInvalid,
			expectFindCatalog: true,
		},
		{
			name:               "filter table valid",
			depth:              adbc.ObjectDepthColumns,
			tableFilter:        &ingestTableName,
			expectFindCatalog:  true,
			expectFindDbSchema: true,
			expectFindTable:    true,
			expectedColnames:   []string{"int64s", "strings"},
			expectedPositions:  []int32{1, 2},
		},
		{
			name:               "filter table invalid",
			depth:              adbc.ObjectDepthColumns,
			tableFilter:        &tableFilterInvalid,
			expectFindCatalog:  true,
			expectFindDbSchema: true,
		},
		{
			name:               "filter column: in%",
			depth:              adbc.ObjectDepthColumns,
			columnFilter:       &columnFilter,
			expectFindCatalog:  true,
			expectFindDbSchema: true,
			expectFindTable:    true,
			expectedColnames:   []string{"int64s"},
			expectedPositions:  []int32{1},
		},
	}

	for _, tt := range tests {
		c.Run(tt.name, func() {
			rdr, err := cnxn.GetObjects(ctx, tt.depth, tt.catalogFilter, tt.dbSchemaFilter, tt.tableFilter, tt.columnFilter, tt.tableTypes)
			c.Require().NoError(err)
			defer rdr.Release()

			c.Truef(adbc.GetObjectsSchema.Equal(rdr.Schema()), "expected: %s\ngot: %s", adbc.GetObjectsSchema, rdr.Schema())
			c.True(rdr.Next())
			rec := rdr.Record()
			var (
				foundCatalog         = false
				foundDbSchema        = false
				foundTable           = false
				catalogs             = rec.Column(0).(*array.String)
				catalogDbSchemasList = rec.Column(1).(*array.List)
				catalogDbSchemas     = catalogDbSchemasList.ListValues().(*array.Struct)
				dbSchemaNames        = catalogDbSchemas.Field(0).(*array.String)
				dbSchemaTablesList   = catalogDbSchemas.Field(1).(*array.List)
				dbSchemaTables       = dbSchemaTablesList.ListValues().(*array.Struct)
				tableColumnsList     = dbSchemaTables.Field(2).(*array.List)
				tableColumns         = tableColumnsList.ListValues().(*array.Struct)

				colnames  = make([]string, 0)
				positions = make([]int32, 0)
			)
			for row := 0; row < int(rec.NumRows()); row++ {
				catalogName := catalogs.Value(row)

				if strings.EqualFold(catalogName, ingestCatalogName) {
					foundCatalog = true

					dbSchemaIdxStart, dbSchemaIdxEnd := catalogDbSchemasList.ValueOffsets(row)
					for dbSchemaIdx := dbSchemaIdxStart; dbSchemaIdx < dbSchemaIdxEnd; dbSchemaIdx++ {
						schemaName := dbSchemaNames.Value(int(dbSchemaIdx))

						if strings.EqualFold(schemaName, ingestSchemaName) {
							foundDbSchema = true

							tblIdxStart, tblIdxEnd := dbSchemaTablesList.ValueOffsets(int(dbSchemaIdx))
							for tblIdx := tblIdxStart; tblIdx < tblIdxEnd; tblIdx++ {
								tableName := dbSchemaTables.Field(0).(*array.String).Value(int(tblIdx))

								if strings.EqualFold(tableName, ingestTableName) {
									foundTable = true

									colIdxStart, colIdxEnd := tableColumnsList.ValueOffsets(int(tblIdx))
									for colIdx := colIdxStart; colIdx < colIdxEnd; colIdx++ {
										name := tableColumns.Field(0).(*array.String).Value(int(colIdx))
										colnames = append(colnames, strings.ToLower(name))
										positions = append(positions, tableColumns.Field(1).(*array.Int32).Value(int(colIdx)))
									}
								}
							}
						}
					}
				}
			}

			c.False(rdr.Next())
			c.Equal(tt.expectFindCatalog, foundCatalog)
			c.Equal(tt.expectFindDbSchema, foundDbSchema)
			c.Equal(tt.expectFindTable, foundTable)
			c.ElementsMatch(tt.expectedColnames, colnames)
			c.ElementsMatch(tt.expectedPositions, positions)
		})
	}
}

type StatementTests struct {
	suite.Suite

	Driver adbc.Driver
	Quirks DriverQuirks

	DB   adbc.Database
	Cnxn adbc.Connection
	ctx  context.Context
}

func (s *StatementTests) SetupTest() {
	s.Driver = s.Quirks.SetupDriver(s.T())
	var err error
	s.DB, err = s.Driver.NewDatabase(s.Quirks.DatabaseOptions())
	s.Require().NoError(err)
	s.ctx = context.Background()
	s.Cnxn, err = s.DB.Open(s.ctx)
	s.Require().NoError(err)
}

func (s *StatementTests) TearDownTest() {
	s.Require().NoError(s.Cnxn.Close())
	s.Quirks.TearDownDriver(s.T(), s.Driver)
	s.Cnxn = nil
	s.NoError(s.DB.Close())
	s.DB = nil
	s.Driver = nil
}

func (s *StatementTests) TestNewStatement() {
	stmt, err := s.Cnxn.NewStatement()
	s.NoError(err)
	s.NotNil(stmt)
	s.NoError(stmt.Close())

	var adbcError adbc.Error
	// statement already closed
	s.ErrorAs(stmt.Close(), &adbcError)
	s.Equal(adbc.StatusInvalidState, adbcError.Code)

	stmt, err = s.Cnxn.NewStatement()
	s.NoError(err)

	// cannot execute without a query
	_, _, err = stmt.ExecuteQuery(s.ctx)
	s.ErrorAs(err, &adbcError)
	s.Equal(adbc.StatusInvalidState, adbcError.Code)
}

func (s *StatementTests) TestSqlExecuteSchema() {
	if !s.Quirks.SupportsExecuteSchema() {
		s.T().SkipNow()
	}

	stmt, err := s.Cnxn.NewStatement()
	s.Require().NoError(err)
	defer CheckedClose(s.T(), stmt)

	es, ok := stmt.(adbc.StatementExecuteSchema)
	s.Require().True(ok, "%#v does not support ExecuteSchema", es)

	s.Run("no query", func() {
		var adbcErr adbc.Error

		schema, err := es.ExecuteSchema(s.ctx)
		s.ErrorAs(err, &adbcErr)
		s.Equal(adbc.StatusInvalidState, adbcErr.Code)
		s.Nil(schema)
	})

	s.Run("query", func() {
		s.NoError(stmt.SetSqlQuery("SELECT 1, 'string'"))

		schema, err := es.ExecuteSchema(s.ctx)
		s.NoError(err)
		s.Equal(2, len(schema.Fields()))
		s.True(schema.Field(0).Type.ID() == arrow.INT32 || schema.Field(0).Type.ID() == arrow.INT64)
		s.Equal(arrow.STRING, schema.Field(1).Type.ID())
	})

	s.Run("prepared", func() {
		s.NoError(stmt.SetSqlQuery("SELECT 1, 'string'"))
		s.NoError(stmt.Prepare(s.ctx))

		schema, err := es.ExecuteSchema(s.ctx)
		s.NoError(err)
		s.Equal(2, len(schema.Fields()))
		s.True(schema.Field(0).Type.ID() == arrow.INT32 || schema.Field(0).Type.ID() == arrow.INT64)
		s.Equal(arrow.STRING, schema.Field(1).Type.ID())
	})
}

func (s *StatementTests) TestSqlPartitionedInts() {
	stmt, err := s.Cnxn.NewStatement()
	s.Require().NoError(err)
	defer CheckedClose(s.T(), stmt)

	s.NoError(stmt.SetSqlQuery("SELECT 42"))

	var adbcError adbc.Error
	if !s.Quirks.SupportsPartitionedData() {
		_, _, _, err := stmt.ExecutePartitions(s.ctx)
		s.ErrorAs(err, &adbcError)
		s.Equal(adbc.StatusNotImplemented, adbcError.Code)
		return
	}

	sc, part, rows, err := stmt.ExecutePartitions(s.ctx)
	s.Require().NoError(err)

	s.EqualValues(1, part.NumPartitions)
	s.Len(part.PartitionIDs, 1)
	s.True(rows == 1 || rows == -1, rows)

	if sc != nil {
		s.Len(sc.Fields(), 1)
	}

	cxn, err := s.DB.Open(s.ctx)
	s.Require().NoError(err)
	defer CheckedClose(s.T(), cxn)

	rdr, err := cxn.ReadPartition(s.ctx, part.PartitionIDs[0])
	s.Require().NoError(err)
	defer rdr.Release()

	sc = rdr.Schema()
	s.Require().NotNil(sc)
	s.Len(sc.Fields(), 1)

	s.True(rdr.Next())
	rec := rdr.Record()
	s.EqualValues(1, rec.NumCols())
	s.EqualValues(1, rec.NumRows())

	switch arr := rec.Column(0).(type) {
	case *array.Int32:
		s.EqualValues(42, arr.Value(0))
	case *array.Int64:
		s.EqualValues(42, arr.Value(0))
	}

	s.False(rdr.Next())
}

func (s *StatementTests) TestSQLPrepareGetParameterSchema() {
	stmt, err := s.Cnxn.NewStatement()
	s.NoError(err)
	defer CheckedClose(s.T(), stmt)

	query := "SELECT " + s.Quirks.BindParameter(0) + ", " + s.Quirks.BindParameter(1)
	s.NoError(stmt.SetSqlQuery(query))
	s.NoError(stmt.Prepare(s.ctx))

	sc, err := stmt.GetParameterSchema()
	if !s.Quirks.SupportsGetParameterSchema() {
		var adbcError adbc.Error
		s.ErrorAs(err, &adbcError)
		s.Equal(adbc.StatusNotImplemented, adbcError.Code)
		return
	}
	s.NoError(err)

	// it's allowed to be nil as some systems don't provide param schemas
	if sc != nil {
		s.Len(sc.Fields(), 2)
	}
}

func (s *StatementTests) TestSQLPrepareSelectParams() {
	if !s.Quirks.SupportsDynamicParameterBinding() {
		s.T().SkipNow()
	}

	stmt, err := s.Cnxn.NewStatement()
	s.NoError(err)
	defer CheckedClose(s.T(), stmt)

	query := "SELECT " + s.Quirks.BindParameter(0) + ", " + s.Quirks.BindParameter(1)
	s.Require().NoError(stmt.SetSqlQuery(query))
	s.Require().NoError(stmt.Prepare(s.ctx))

	schema := arrow.NewSchema([]arrow.Field{
		{Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "strings", Type: arrow.BinaryTypes.String, Nullable: true},
	}, nil)

	bldr := array.NewRecordBuilder(s.Quirks.Alloc(), schema)
	defer bldr.Release()
	bldr.Field(0).(*array.Int64Builder).AppendValues([]int64{42, -42, 0}, []bool{true, true, false})
	bldr.Field(1).(*array.StringBuilder).AppendValues([]string{"", "", "bar"}, []bool{true, false, true})
	batch := bldr.NewRecord()
	defer batch.Release()

	s.Require().NoError(stmt.Bind(s.ctx, batch))
	rdr, affected, err := stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	defer rdr.Release()
	s.True(affected == 1 || affected == -1, affected)

	var nrows int64
	for rdr.Next() {
		rec := rdr.Record()
		s.Require().NotNil(rec)
		s.EqualValues(2, rec.NumCols())

		start, end := nrows, nrows+rec.NumRows()
		switch arr := rec.Column(0).(type) {
		case *array.Int32:

		case *array.Int64:
			s.True(array.SliceEqual(arr, 0, int64(arr.Len()), batch.Column(0), start, end))
		}

		s.True(array.SliceEqual(rec.Column(1), 0, rec.NumRows(), batch.Column(1), start, end))
		nrows += rec.NumRows()
	}
	s.EqualValues(3, nrows)
	s.False(rdr.Next())
	s.NoError(rdr.Err())
}

func (s *StatementTests) TestSQLPrepareSelectNoParams() {
	stmt, err := s.Cnxn.NewStatement()
	s.NoError(err)
	defer CheckedClose(s.T(), stmt)

	s.NoError(stmt.SetSqlQuery("SELECT 1"))
	s.NoError(stmt.Prepare(s.ctx))

	rdr, n, err := stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	s.True(n == 1 || n == -1)
	defer rdr.Release()

	sc := rdr.Schema()
	s.Require().NotNil(sc)
	s.Len(sc.Fields(), 1)

	s.True(rdr.Next())
	rec := rdr.Record()
	s.EqualValues(1, rec.NumCols())
	s.EqualValues(1, rec.NumRows())

	switch arr := rec.Column(0).(type) {
	case *array.Int32:
		s.EqualValues(1, arr.Value(0))
	case *array.Int64:
		s.EqualValues(1, arr.Value(0))
	}

	s.False(rdr.Next())
}

func (s *StatementTests) TestSqlPrepareErrorParamCountMismatch() {
	if !s.Quirks.SupportsDynamicParameterBinding() {
		s.T().SkipNow()
	}

	query := "SELECT " + s.Quirks.BindParameter(0) + ", " + s.Quirks.BindParameter(1)
	stmt, err := s.Cnxn.NewStatement()
	s.NoError(err)
	defer CheckedClose(s.T(), stmt)

	s.NoError(stmt.SetSqlQuery(query))
	s.NoError(stmt.Prepare(s.ctx))

	batchbldr := array.NewRecordBuilder(s.Quirks.Alloc(), arrow.NewSchema(
		[]arrow.Field{{Name: "int64s", Type: arrow.PrimitiveTypes.Int64}}, nil))
	defer batchbldr.Release()
	bldr := batchbldr.Field(0).(*array.Int64Builder)
	bldr.AppendValues([]int64{42, -42, 0}, []bool{true, true, false})
	batch := batchbldr.NewRecord()
	defer batch.Release()

	s.NoError(stmt.Bind(s.ctx, batch))
	_, _, err = stmt.ExecuteQuery(s.ctx)
	s.Error(err)
}

func (s *StatementTests) TestSqlIngestInts() {
	if !s.Quirks.SupportsBulkIngest(adbc.OptionValueIngestModeCreate) {
		s.T().SkipNow()
	}

	s.Require().NoError(s.Quirks.DropTable(s.Cnxn, "bulk_ingest"))

	schema := arrow.NewSchema([]arrow.Field{{
		Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)

	batchbldr := array.NewRecordBuilder(s.Quirks.Alloc(), schema)
	defer batchbldr.Release()
	bldr := batchbldr.Field(0).(*array.Int64Builder)
	bldr.AppendValues([]int64{42, -42, 0}, []bool{true, true, false})
	batch := batchbldr.NewRecord()
	defer batch.Release()

	stmt, err := s.Cnxn.NewStatement()
	s.Require().NoError(err)
	defer CheckedClose(s.T(), stmt)

	s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))
	s.Require().NoError(stmt.Bind(s.ctx, batch))

	affected, err := stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	if affected != -1 && affected != 3 {
		s.FailNowf("invalid number of affected rows", "should be -1 or 3, got: %d", affected)
	}

	// use order by clause to ensure we get the same order as the input batch
	s.Require().NoError(stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest" ORDER BY "int64s" DESC NULLS LAST`))
	rdr, rows, err := stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	if rows != -1 && rows != 3 {
		s.FailNowf("invalid number of returned rows", "should be -1 or 3, got: %d", rows)
	}
	defer rdr.Release()

	s.Truef(schema.Equal(utils.RemoveSchemaMetadata(rdr.Schema())), "expected: %s\n got: %s", schema, rdr.Schema())
	s.Require().True(rdr.Next())
	rec := rdr.Record()
	s.EqualValues(3, rec.NumRows())
	s.EqualValues(1, rec.NumCols())

	s.Truef(array.Equal(rec.Column(0), batch.Column(0)), "expected: %s\ngot: %s", batch.Column(0), rec.Column(0))

	s.Require().False(rdr.Next())
	s.Require().NoError(rdr.Err())
}

func (s *StatementTests) TestSqlIngestAppend() {
	if !s.Quirks.SupportsBulkIngest(adbc.OptionValueIngestModeAppend) {
		s.T().SkipNow()
	}

	s.Require().NoError(s.Quirks.DropTable(s.Cnxn, "bulk_ingest"))

	schema := arrow.NewSchema([]arrow.Field{{
		Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)

	batchbldr := array.NewRecordBuilder(s.Quirks.Alloc(), schema)
	defer batchbldr.Release()
	bldr := batchbldr.Field(0).(*array.Int64Builder)
	bldr.AppendValues([]int64{42}, []bool{true})
	batch := batchbldr.NewRecord()
	defer batch.Release()

	// ingest and create table
	stmt, err := s.Cnxn.NewStatement()
	s.Require().NoError(err)
	defer CheckedClose(s.T(), stmt)

	s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))
	s.Require().NoError(stmt.Bind(s.ctx, batch))

	affected, err := stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	if affected != -1 && affected != 1 {
		s.FailNowf("invalid number of affected rows", "should be -1 or 1, got: %d", affected)
	}

	// now append
	bldr.AppendValues([]int64{-42, 0}, []bool{true, false})
	batch2 := batchbldr.NewRecord()
	defer batch2.Release()

	s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))

	if !s.Quirks.SupportsBulkIngest(adbc.OptionValueIngestModeAppend) {
		s.T().SkipNow()
	}
	s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend))
	s.Require().NoError(stmt.Bind(s.ctx, batch2))

	affected, err = stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	if affected != -1 && affected != 2 {
		s.FailNowf("invalid number of affected rows", "should be -1 or 2, got: %d", affected)
	}

	// use order by clause to ensure we get the same order as the input batch
	s.Require().NoError(stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest" ORDER BY "int64s" DESC NULLS LAST`))
	rdr, rows, err := stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	if rows != -1 && rows != 3 {
		s.FailNowf("invalid number of returned rows", "should be -1 or 3, got: %d", rows)
	}
	defer rdr.Release()

	s.Truef(schema.Equal(utils.RemoveSchemaMetadata(rdr.Schema())), "expected: %s\n got: %s", schema, rdr.Schema())
	s.Require().True(rdr.Next())
	rec := rdr.Record()
	s.EqualValues(3, rec.NumRows())
	s.EqualValues(1, rec.NumCols())

	exp, err := array.Concatenate([]arrow.Array{batch.Column(0), batch2.Column(0)}, s.Quirks.Alloc())
	s.Require().NoError(err)
	defer exp.Release()
	s.Truef(array.Equal(rec.Column(0), exp), "expected: %s\ngot: %s", exp, rec.Column(0))

	s.Require().False(rdr.Next())
	s.Require().NoError(rdr.Err())
}

func (s *StatementTests) TestSqlIngestReplace() {
	if !s.Quirks.SupportsBulkIngest(adbc.OptionValueIngestModeReplace) {
		s.T().SkipNow()
	}

	s.Require().NoError(s.Quirks.DropTable(s.Cnxn, "bulk_ingest"))

	schema := arrow.NewSchema([]arrow.Field{{
		Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)

	batchbldr := array.NewRecordBuilder(s.Quirks.Alloc(), schema)
	defer batchbldr.Release()
	bldr := batchbldr.Field(0).(*array.Int64Builder)
	bldr.AppendValues([]int64{42}, []bool{true})
	batch := batchbldr.NewRecord()
	defer batch.Release()

	// ingest and create table
	stmt, err := s.Cnxn.NewStatement()
	s.Require().NoError(err)
	defer CheckedClose(s.T(), stmt)

	s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))
	s.Require().NoError(stmt.Bind(s.ctx, batch))

	affected, err := stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	if affected != -1 && affected != 1 {
		s.FailNowf("invalid number of affected rows", "should be -1 or 1, got: %d", affected)
	}

	// now replace
	schema = arrow.NewSchema([]arrow.Field{{
		Name: "newintcol", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	batchbldr2 := array.NewRecordBuilder(s.Quirks.Alloc(), schema)
	defer batchbldr2.Release()
	bldr2 := batchbldr2.Field(0).(*array.Int64Builder)
	bldr2.AppendValues([]int64{42}, []bool{true})
	batch2 := batchbldr2.NewRecord()
	defer batch2.Release()

	s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))
	s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeReplace))
	s.Require().NoError(stmt.Bind(s.ctx, batch2))

	affected, err = stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	if affected != -1 && affected != 1 {
		s.FailNowf("invalid number of affected rows", "should be -1 or 1, got: %d", affected)
	}

	s.Require().NoError(stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest"`))
	rdr, rows, err := stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	if rows != -1 && rows != 1 {
		s.FailNowf("invalid number of returned rows", "should be -1 or 1, got: %d", rows)
	}
	defer rdr.Release()

	s.Truef(schema.Equal(utils.RemoveSchemaMetadata(rdr.Schema())), "expected: %s\n got: %s", schema, rdr.Schema())
	s.Require().True(rdr.Next())
	rec := rdr.Record()
	s.EqualValues(1, rec.NumRows())
	s.EqualValues(1, rec.NumCols())
	col, ok := rec.Column(0).(*array.Int64)
	s.True(ok)
	s.Equal(int64(42), col.Value(0))

	s.Require().False(rdr.Next())
	s.Require().NoError(rdr.Err())
}

func (s *StatementTests) TestSqlIngestCreateAppend() {
	if !s.Quirks.SupportsBulkIngest(adbc.OptionValueIngestModeCreateAppend) {
		s.T().SkipNow()
	}

	s.Require().NoError(s.Quirks.DropTable(s.Cnxn, "bulk_ingest"))

	schema := arrow.NewSchema([]arrow.Field{{
		Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)

	batchbldr := array.NewRecordBuilder(s.Quirks.Alloc(), schema)
	defer batchbldr.Release()
	bldr := batchbldr.Field(0).(*array.Int64Builder)
	bldr.AppendValues([]int64{42}, []bool{true})
	batch := batchbldr.NewRecord()
	defer batch.Release()

	// ingest and create table
	stmt, err := s.Cnxn.NewStatement()
	s.Require().NoError(err)
	defer CheckedClose(s.T(), stmt)

	s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))
	s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreateAppend))
	s.Require().NoError(stmt.Bind(s.ctx, batch))

	affected, err := stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	if affected != -1 && affected != 1 {
		s.FailNowf("invalid number of affected rows", "should be -1 or 1, got: %d", affected)
	}

	// append
	s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))
	s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreateAppend))
	s.Require().NoError(stmt.Bind(s.ctx, batch))

	affected, err = stmt.ExecuteUpdate(s.ctx)
	s.Require().NoError(err)
	if affected != -1 && affected != 1 {
		s.FailNowf("invalid number of affected rows", "should be -1 or 1, got: %d", affected)
	}

	// validate
	s.Require().NoError(stmt.SetSqlQuery(`SELECT * FROM "bulk_ingest"`))
	rdr, rows, err := stmt.ExecuteQuery(s.ctx)
	s.Require().NoError(err)
	if rows != -1 && rows != 2 {
		s.FailNowf("invalid number of returned rows", "should be -1 or 2, got: %d", rows)
	}
	defer rdr.Release()

	s.Truef(schema.Equal(utils.RemoveSchemaMetadata(rdr.Schema())), "expected: %s\n got: %s", schema, rdr.Schema())
	s.Require().True(rdr.Next())
	rec := rdr.Record()
	s.EqualValues(2, rec.NumRows())
	s.EqualValues(1, rec.NumCols())
	col, ok := rec.Column(0).(*array.Int64)
	s.True(ok)
	s.Equal(int64(42), col.Value(0))
	s.Equal(int64(42), col.Value(1))

	s.Require().False(rdr.Next())
	s.Require().NoError(rdr.Err())
}

func (s *StatementTests) TestSqlIngestErrors() {
	if !s.Quirks.SupportsBulkIngest(adbc.OptionValueIngestModeCreate) {
		s.T().SkipNow()
	}

	s.Require().NoError(s.Quirks.DropTable(s.Cnxn, "bulk_ingest"))

	stmt, err := s.Cnxn.NewStatement()
	s.Require().NoError(err)
	defer CheckedClose(s.T(), stmt)

	s.Run("ingest without bind", func() {
		var e adbc.Error
		s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))

		_, _, err := stmt.ExecuteQuery(s.ctx)
		s.ErrorAs(err, &e)
		s.Equal(adbc.StatusInvalidState, e.Code)
	})

	s.Run("append to nonexistent table", func() {
		if !s.Quirks.SupportsBulkIngest(adbc.OptionValueIngestModeAppend) {
			s.T().SkipNow()
		}

		s.Require().NoError(s.Quirks.DropTable(s.Cnxn, "bulk_ingest"))
		schema := arrow.NewSchema([]arrow.Field{{
			Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)

		batchbldr := array.NewRecordBuilder(s.Quirks.Alloc(), schema)
		defer batchbldr.Release()
		bldr := batchbldr.Field(0).(*array.Int64Builder)
		bldr.AppendValues([]int64{42, -42, 0}, []bool{true, true, false})
		batch := batchbldr.NewRecord()
		defer batch.Release()

		s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))
		s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend))
		s.Require().NoError(stmt.Bind(s.ctx, batch))

		var e adbc.Error
		_, _, err := stmt.ExecuteQuery(s.ctx)
		s.ErrorAs(err, &e)
		s.NotEqual(adbc.StatusOK, e.Code)
		// SQLSTATE 42S02 == table or view not found
		s.Equal([5]byte{'4', '2', 'S', '0', '2'}, e.SqlState)
	})

	s.Run("overwrite and incompatible schema", func() {
		if !s.Quirks.SupportsErrorIngestIncompatibleSchema() {
			s.T().SkipNow()
		}

		s.Require().NoError(s.Quirks.DropTable(s.Cnxn, "bulk_ingest"))
		schema := arrow.NewSchema([]arrow.Field{{
			Name: "int64s", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)

		batchbldr := array.NewRecordBuilder(s.Quirks.Alloc(), schema)
		defer batchbldr.Release()
		bldr := batchbldr.Field(0).(*array.Int64Builder)
		bldr.AppendValues([]int64{42, -42, 0}, []bool{true, true, false})
		batch := batchbldr.NewRecord()
		defer batch.Release()

		s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))
		s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeCreate))
		s.Require().NoError(stmt.Bind(s.ctx, batch))

		// create it
		_, err := stmt.ExecuteUpdate(s.ctx)
		s.Require().NoError(err)

		// error if we try to create again
		s.Require().NoError(stmt.Bind(s.ctx, batch))

		var e adbc.Error
		_, err = stmt.ExecuteUpdate(s.ctx)
		s.ErrorAs(err, &e)
		s.Equal(adbc.StatusInternal, e.Code)

		// try to append an incompatible schema
		schema, _ = schema.AddField(1, arrow.Field{Name: "coltwo", Type: arrow.PrimitiveTypes.Int64, Nullable: true})
		batchbldr = array.NewRecordBuilder(s.Quirks.Alloc(), schema)
		defer batchbldr.Release()
		batchbldr.Field(0).AppendNull()
		batchbldr.Field(1).AppendNull()
		batch = batchbldr.NewRecord()
		defer batch.Release()

		if !s.Quirks.SupportsBulkIngest(adbc.OptionValueIngestModeCreate) {
			s.T().SkipNow()
		}

		s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestTargetTable, "bulk_ingest"))
		s.Require().NoError(stmt.SetOption(adbc.OptionKeyIngestMode, adbc.OptionValueIngestModeAppend))
		s.Require().NoError(stmt.Bind(s.ctx, batch))

		_, err = stmt.ExecuteUpdate(s.ctx)
		s.ErrorAs(err, &e)
		s.NotEqual(adbc.StatusOK, e.Code)
	})
}

// CheckedClose is a helper for deferring Close() with a potential error in a test.
// For example: `defer CheckedClose(suite.T(), stmt)`
func CheckedClose(t *testing.T, c io.Closer) {
	assert.NoError(t, c.Close())
}

// CheckedClose is a helper for deferring a function with a potential error in a test.
// For example: `defer CheckedCleanup(suite.T(), func() error { return os.Remove(path) })`
func CheckedCleanup(t *testing.T, c func() error) {
	assert.NoError(t, c())
}
