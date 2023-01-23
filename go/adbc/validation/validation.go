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
	"strings"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v11/arrow/memory"
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
	// Whether two statements can be used at the same time on a single connection
	SupportsConcurrentStatements() bool
	// Whether AdbcStatementExecutePartitions should work
	SupportsPartitionedData() bool
	// Whether transactions are supported (Commit/Rollback on connection)
	SupportsTransactions() bool
	// Whether retrieving the schema of prepared statement params is supported
	SupportsGetParameterSchema() bool
	// Expected Metadata responses
	GetMetadata(adbc.InfoCode) interface{}
	// Create a sample table from an arrow record
	CreateSampleTable(tableName string, r arrow.Record) error
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
	c.DB = nil
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
	cnxn, _ := c.DB.Open(context.Background())
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
	cnxn, _ := c.DB.Open(ctx)
	defer cnxn.Close()

	expectedCode := adbc.StatusInvalidState
	var adbcError adbc.Error
	err := cnxn.Commit(ctx)
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
	cnxn, _ := c.DB.Open(ctx)
	defer cnxn.Close()

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

	// it is ok to disable autocommit when it isn't enabled
	c.NoError(cnxnopt.SetOption(adbc.OptionKeyAutoCommit, adbc.OptionValueDisabled))
}

func (c *ConnectionTests) TestMetadataGetInfo() {
	ctx := context.Background()
	cnxn, _ := c.DB.Open(ctx)
	defer cnxn.Close()

	info := []adbc.InfoCode{
		adbc.InfoDriverName,
		adbc.InfoDriverVersion,
		adbc.InfoDriverArrowVersion,
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
			// currently we only define utf8 values for metadata
			c.Equal(c.Quirks.GetMetadata(adbc.InfoCode(code)), child.(*array.String).Value(i), adbc.InfoCode(code).String())
		}
	}
}

func (c *ConnectionTests) TestMetadataGetTableSchema() {
	rec, _, err := array.RecordFromJSON(memory.DefaultAllocator, arrow.NewSchema(
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
	cnxn, _ := c.DB.Open(ctx)
	defer cnxn.Close()

	c.Require().NoError(c.Quirks.CreateSampleTable("sample_test", rec))

	sc, err := cnxn.GetTableSchema(ctx, nil, nil, "sample_test")
	c.Require().NoError(err)

	expectedSchema := arrow.NewSchema([]arrow.Field{
		{Name: "ints", Type: arrow.PrimitiveTypes.Int64,
			Metadata: arrow.MetadataFrom(map[string]string{
				flightsql.ScaleKey: "15", flightsql.IsReadOnlyKey: "0", flightsql.IsAutoIncrementKey: "0",
				flightsql.TableNameKey: "sample_test", flightsql.PrecisionKey: "10"})},
		{Name: "strings", Type: arrow.BinaryTypes.String,
			Metadata: arrow.MetadataFrom(map[string]string{
				flightsql.ScaleKey: "15", flightsql.IsReadOnlyKey: "0", flightsql.IsAutoIncrementKey: "0",
				flightsql.TableNameKey: "sample_test"})},
	}, nil)

	c.Truef(expectedSchema.Equal(sc), "expected: %s\ngot: %s", expectedSchema, sc)
}

func (c *ConnectionTests) TestMetadataGetTableTypes() {
	ctx := context.Background()
	cnxn, _ := c.DB.Open(ctx)
	defer cnxn.Close()

	rdr, err := cnxn.GetTableTypes(ctx)
	c.Require().NoError(err)
	defer rdr.Release()

	c.Truef(adbc.TableTypesSchema.Equal(rdr.Schema()), "expected: %s\ngot: %s", adbc.TableTypesSchema, rdr.Schema())
	c.True(rdr.Next())
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
	s.DB = nil
	s.Driver = nil
}

func (s *StatementTests) TestNewStatement() {
	stmt, err := s.Cnxn.NewStatement()
	s.NoError(err)
	s.NotNil(stmt)
	s.NoError(stmt.Close())

	var adbcError adbc.Error
	s.ErrorAs(stmt.Close(), &adbcError)
	s.Equal(adbc.StatusInvalidState, adbcError.Code)

	stmt, err = s.Cnxn.NewStatement()
	s.NoError(err)
	_, _, err = stmt.ExecuteQuery(s.ctx)
	s.ErrorAs(err, &adbcError)
	s.Equal(adbc.StatusInvalidState, adbcError.Code)
}

func (s *StatementTests) TestSqlPartitionedInts() {
	stmt, err := s.Cnxn.NewStatement()
	s.Require().NoError(err)
	defer stmt.Close()

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
	defer cxn.Close()

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
	defer stmt.Close()

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

func (s *StatementTests) TestSQLPrepareSelectNoParams() {
	stmt, err := s.Cnxn.NewStatement()
	s.NoError(err)
	defer stmt.Close()

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
