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

//go:build cgo

package drivermgr_test

import (
	"context"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/drivermgr"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/stretchr/testify/suite"
	"github.com/substrait-io/substrait-go/extensions"
	"github.com/substrait-io/substrait-go/plan"
	"github.com/substrait-io/substrait-go/types"
	"google.golang.org/protobuf/proto"
)

type DriverMgrDuckDBSuite struct {
	suite.Suite

	ctx  context.Context
	drv  adbc.Driver
	db   adbc.Database
	conn adbc.Connection
}

func (dm *DriverMgrDuckDBSuite) SetupSuite() {
	dm.ctx = context.TODO()
	dm.drv = drivermgr.Driver{}
	var err error
	dm.db, err = dm.drv.NewDatabase(map[string]string{
		"driver":     "duckdb",
		"entrypoint": "duckdb_adbc_init",
	})
	dm.NoError(err)

	cnxn, err := dm.db.Open(dm.ctx)
	dm.NoError(err)
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	dm.NoError(err)
	defer stmt.Close()

	// Setup substrait extension
	dm.NoError(stmt.SetSqlQuery("INSTALL substrait"))
	_, err = stmt.ExecuteUpdate(dm.ctx)
	dm.NoError(err)

	dm.NoError(stmt.SetSqlQuery("LOAD substrait"))
	_, err = stmt.ExecuteUpdate(dm.ctx)
	dm.NoError(err)

	dm.NoError(stmt.SetSqlQuery("CREATE TABLE test_table (id INTEGER PRIMARY KEY, name TEXT)"))
	// Don't check nrows, duckdb currently does not support it
	_, err = stmt.ExecuteUpdate(dm.ctx)
	dm.NoError(err)

	dm.NoError(stmt.SetSqlQuery("INSERT INTO test_table (id, name) VALUES (1, 'test')"))
	// Don't check nrows, duckdb currently does not support it
	_, err = stmt.ExecuteUpdate(dm.ctx)
	dm.NoError(err)

	dm.NoError(stmt.SetSqlQuery("INSERT INTO test_table (id, name) VALUES (2, 'test')"))
	// Don't check nrows, duckdb currently does not support it
	_, err = stmt.ExecuteUpdate(dm.ctx)
	dm.NoError(err)

	dm.NoError(stmt.SetSqlQuery("INSERT INTO test_table (id, name) VALUES (3, 'tested')"))
	// Don't check nrows, duckdb currently does not support it
	_, err = stmt.ExecuteUpdate(dm.ctx)
	dm.NoError(err)
}

func (dm *DriverMgrDuckDBSuite) TearDownSuite() {
	dm.NoError(dm.db.Close())
}

func (dm *DriverMgrDuckDBSuite) SetupTest() {
	cnxn, err := dm.db.Open(dm.ctx)
	dm.Require().NoError(err)
	dm.conn = cnxn
}

func (dm *DriverMgrDuckDBSuite) TearDownTest() {
	dm.NoError(dm.conn.Close())
}

func (dm *DriverMgrDuckDBSuite) TestSubstraitExecute() {
	// Building Substrait plan equivalent to 'SELECT COUNT(*) AS count FROM test_table;'
	bldr := plan.NewBuilderDefault()
	scan := bldr.NamedScan(
		[]string{"test_table"},
		types.NamedStruct{
			Names: []string{"id", "names"},
			Struct: types.StructType{
				Nullability: types.NullabilityRequired,
				Types: []types.Type{
					&types.Int32Type{Nullability: types.NullabilityRequired},
					&types.StringType{Nullability: types.NullabilityNullable},
				},
			},
		},
	)

	countFn, err := bldr.AggregateFn(extensions.SubstraitDefaultURIPrefix+"functions_aggregate_generic.yaml", "count", nil)
	dm.Require().NoError(err)

	root, err := bldr.AggregateColumns(scan, []plan.AggRelMeasure{bldr.Measure(countFn, nil)})
	dm.Require().NoError(err)

	plan, err := bldr.Plan(root, []string{"count"})
	dm.Require().NoError(err)

	planProto, err := plan.ToProto()
	dm.Require().NoError(err)

	serialized, err := proto.Marshal(planProto)
	dm.Require().NoError(err)

	st, err := dm.conn.NewStatement()
	dm.Require().NoError(err)
	defer st.Close()

	dm.Require().NoError(st.SetSubstraitPlan(serialized))
	rdr, _, err := st.ExecuteQuery(context.TODO())
	dm.Require().NoError(err)
	defer rdr.Release()

	expectedSchema := arrow.NewSchema([]arrow.Field{{Name: "count", Type: arrow.PrimitiveTypes.Int64, Nullable: true}}, nil)
	dm.Require().True(rdr.Next())
	rec := rdr.Record()
	dm.Require().Equal(expectedSchema, rec.Schema())
	dm.Require().Equal(int64(1), rec.NumCols())
	dm.Require().Equal(int64(1), rec.NumRows())
	dm.Require().Equal(int64(3), rec.Column(0).(*array.Int64).Value(0))

	// Only expected one record
	dm.Require().False(rdr.Next())
}

func TestDriverMgrDuckDB(t *testing.T) {
	suite.Run(t, new(DriverMgrDuckDBSuite))
}
