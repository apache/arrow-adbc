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

package adbc

import (
	"context"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// databaseContextAdapter wraps a Database to implement DatabaseContext.
type databaseContextAdapter struct {
	db Database
}

// AsDatabaseContext wraps a Database to implement DatabaseContext.
// This adapter allows using a non-context Database implementation with
// context-aware code. The context parameter is used for methods that
// already accept context (like Open), but is effectively ignored for
// methods that don't (like Close, SetOptions) since the underlying
// implementation cannot respond to cancellation or deadlines.
func AsDatabaseContext(db Database) DatabaseContext {
	if db == nil {
		return nil
	}
	return &databaseContextAdapter{db: db}
}

func (d *databaseContextAdapter) SetOptions(ctx context.Context, opts map[string]string) error {
	// Context cannot be propagated to SetOptions since it doesn't accept context
	return d.db.SetOptions(opts)
}

func (d *databaseContextAdapter) Open(ctx context.Context) (ConnectionContext, error) {
	// Pass context through since Open already accepts it
	conn, err := d.db.Open(ctx)
	if err != nil {
		return nil, err
	}
	// Wrap the returned Connection as ConnectionContext
	return AsConnectionContext(conn), nil
}

func (d *databaseContextAdapter) Close(ctx context.Context) error {
	// Context cannot be propagated to Close since it doesn't accept context
	return d.db.Close()
}

// connectionContextAdapter wraps a Connection to implement ConnectionContext.
type connectionContextAdapter struct {
	conn Connection
}

// AsConnectionContext wraps a Connection to implement ConnectionContext.
// This adapter allows using a non-context Connection implementation with
// context-aware code. The context parameter is passed through for methods
// that already accept context, but is ignored for methods that don't.
func AsConnectionContext(conn Connection) ConnectionContext {
	if conn == nil {
		return nil
	}
	// Note: We cannot check if conn already implements ConnectionContext
	// because Connection and ConnectionContext have conflicting Close method signatures.
	// Connection.Close() vs ConnectionContext.Close(ctx)
	return &connectionContextAdapter{conn: conn}
}

func (c *connectionContextAdapter) GetInfo(ctx context.Context, infoCodes []InfoCode) (array.RecordReader, error) {
	return c.conn.GetInfo(ctx, infoCodes)
}

func (c *connectionContextAdapter) GetObjects(ctx context.Context, depth ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	return c.conn.GetObjects(ctx, depth, catalog, dbSchema, tableName, columnName, tableType)
}

func (c *connectionContextAdapter) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return c.conn.GetTableSchema(ctx, catalog, dbSchema, tableName)
}

func (c *connectionContextAdapter) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	return c.conn.GetTableTypes(ctx)
}

func (c *connectionContextAdapter) Commit(ctx context.Context) error {
	return c.conn.Commit(ctx)
}

func (c *connectionContextAdapter) Rollback(ctx context.Context) error {
	return c.conn.Rollback(ctx)
}

func (c *connectionContextAdapter) NewStatement(ctx context.Context) (StatementContext, error) {
	// Context cannot be propagated to NewStatement since it doesn't accept context
	stmt, err := c.conn.NewStatement()
	if err != nil {
		return nil, err
	}
	// Wrap the returned Statement as StatementContext
	return AsStatementContext(stmt), nil
}

func (c *connectionContextAdapter) Close(ctx context.Context) error {
	// Context cannot be propagated to Close since it doesn't accept context
	return c.conn.Close()
}

func (c *connectionContextAdapter) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return c.conn.ReadPartition(ctx, serializedPartition)
}

// statementContextAdapter wraps a Statement to implement StatementContext.
type statementContextAdapter struct {
	stmt Statement
}

// AsStatementContext wraps a Statement to implement StatementContext.
// This adapter allows using a non-context Statement implementation with
// context-aware code. The context parameter is passed through for methods
// that already accept context, but is ignored for methods that don't.
func AsStatementContext(stmt Statement) StatementContext {
	if stmt == nil {
		return nil
	}
	// Note: We cannot check if stmt already implements StatementContext
	// because Statement and StatementContext have conflicting Close method signatures.
	// Statement.Close() vs StatementContext.Close(ctx)
	return &statementContextAdapter{stmt: stmt}
}

func (s *statementContextAdapter) Close(ctx context.Context) error {
	// Context cannot be propagated to Close since it doesn't accept context
	return s.stmt.Close()
}

func (s *statementContextAdapter) SetOption(ctx context.Context, key, val string) error {
	// Context cannot be propagated to SetOption since it doesn't accept context
	return s.stmt.SetOption(key, val)
}

func (s *statementContextAdapter) SetSqlQuery(ctx context.Context, query string) error {
	// Context cannot be propagated to SetSqlQuery since it doesn't accept context
	return s.stmt.SetSqlQuery(query)
}

func (s *statementContextAdapter) SetSubstraitPlan(ctx context.Context, plan []byte) error {
	// Context cannot be propagated to SetSubstraitPlan since it doesn't accept context
	return s.stmt.SetSubstraitPlan(plan)
}

func (s *statementContextAdapter) GetParameterSchema(ctx context.Context) (*arrow.Schema, error) {
	// Context cannot be propagated to GetParameterSchema since it doesn't accept context
	return s.stmt.GetParameterSchema()
}

func (s *statementContextAdapter) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	return s.stmt.ExecuteQuery(ctx)
}

func (s *statementContextAdapter) ExecuteUpdate(ctx context.Context) (int64, error) {
	return s.stmt.ExecuteUpdate(ctx)
}

func (s *statementContextAdapter) Prepare(ctx context.Context) error {
	return s.stmt.Prepare(ctx)
}

func (s *statementContextAdapter) ExecutePartitions(ctx context.Context) (*arrow.Schema, Partitions, int64, error) {
	return s.stmt.ExecutePartitions(ctx)
}

func (s *statementContextAdapter) Bind(ctx context.Context, values arrow.RecordBatch) error {
	return s.stmt.Bind(ctx, values)
}

func (s *statementContextAdapter) BindStream(ctx context.Context, stream array.RecordReader) error {
	return s.stmt.BindStream(ctx, stream)
}
