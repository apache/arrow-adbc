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

// Package panicdummy implements a simple ADBC driver that just
// panics, which is intended only for use in automated testing.
package panicdummy

import (
	"context"
	"fmt"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func maybePanic(fname string) {
	if fname == os.Getenv("PANICDUMMY_FUNC") {
		message := os.Getenv("PANICDUMMY_MESSAGE")
		if len(message) == 0 {
			message = fmt.Sprintf("We panicked in %s!", fname)
		}
		panic(message)
	}
}

type Driver struct {
	Alloc memory.Allocator
}

// NewDriver creates a new PanicDummy driver using the given Arrow allocator.
func NewDriver(alloc memory.Allocator) adbc.Driver {
	return Driver{Alloc: alloc}
}

func (d Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	return d.NewDatabaseWithContext(context.Background(), opts)
}

func (d Driver) NewDatabaseWithContext(ctx context.Context, opts map[string]string) (adbc.Database, error) {
	maybePanic("NewDatabaseWithContext")
	return &database{}, nil
}

type database struct{}

func (d *database) SetOptions(cnOptions map[string]string) error {
	maybePanic("DatabaseSetOptions")
	return nil
}

func (d *database) Open(ctx context.Context) (adbc.Connection, error) {
	maybePanic("DatabaseOpen")
	return &cnxn{}, nil
}

func (d *database) Close() error {
	maybePanic("DatabaseClose")
	return nil
}

type cnxn struct{}

func (c *cnxn) SetOption(key, value string) error {
	maybePanic("ConnectionSetOption")
	return nil
}

func (c *cnxn) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	maybePanic("ConnectionGetInfo")
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	maybePanic("ConnectionGetObjects")
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	maybePanic("ConnectionGetTableSchema")
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	maybePanic("ConnectionGetTableTypes")
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) Commit(ctx context.Context) error {
	maybePanic("ConnectionCommit")
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) Rollback(ctx context.Context) error {
	maybePanic("ConnectionRollback")
	return adbc.Error{Code: adbc.StatusNotImplemented}
}

func (c *cnxn) NewStatement() (adbc.Statement, error) {
	maybePanic("ConnectionNewStatement")
	return &statement{}, nil
}

// Close closes this connection and releases any associated resources.
func (c *cnxn) Close() error {
	maybePanic("ConnectionClose")
	return nil
}

func (c *cnxn) ReadPartition(ctx context.Context, serializedPartition []byte) (rdr array.RecordReader, err error) {
	maybePanic("ConnectionReadPartition")
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

type statement struct{}

func (s *statement) Close() error {
	maybePanic("StatementClose")
	return nil
}

func (s *statement) SetOption(key string, val string) error {
	maybePanic("StatementSetOption")
	return nil
}

func (s *statement) SetSqlQuery(query string) error {
	maybePanic("StatementSetSqlQuery")
	return nil
}

func (s *statement) ExecuteQuery(ctx context.Context) (rdr array.RecordReader, nrec int64, err error) {
	maybePanic("StatementExecuteQuery")
	return nil, -1, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *statement) ExecuteUpdate(ctx context.Context) (n int64, err error) {
	maybePanic("StatementExecuteUpdate")
	return -1, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *statement) Prepare(ctx context.Context) error {
	maybePanic("StatementPrepare")
	return nil
}

func (s *statement) SetSubstraitPlan(plan []byte) error {
	maybePanic("StatementSetSubstraitPlan")
	return nil
}

func (s *statement) Bind(_ context.Context, values arrow.RecordBatch) error {
	maybePanic("StatementBind")
	values.Release()
	return nil
}

func (s *statement) BindStream(_ context.Context, stream array.RecordReader) error {
	maybePanic("StatementBindStream")
	stream.Release()
	return nil
}

func (s *statement) GetParameterSchema() (*arrow.Schema, error) {
	maybePanic("StatementGetParameterSchema")
	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (s *statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	maybePanic("StatementExecutePartitions")
	return nil, adbc.Partitions{}, -1, adbc.Error{Code: adbc.StatusNotImplemented}
}
