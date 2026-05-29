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

package adbc_test

import (
	"context"
	"errors"
	"testing"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

// mockConnection is a mock implementation of adbc.Connection for testing
type mockConnection struct {
	mock.Mock
}

func (m *mockConnection) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	args := m.Called(ctx, infoCodes)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(array.RecordReader), args.Error(1)
}

func (m *mockConnection) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	args := m.Called(ctx, depth, catalog, dbSchema, tableName, columnName, tableType)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(array.RecordReader), args.Error(1)
}

func (m *mockConnection) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	args := m.Called(ctx, catalog, dbSchema, tableName)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*arrow.Schema), args.Error(1)
}

func (m *mockConnection) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(array.RecordReader), args.Error(1)
}

func (m *mockConnection) Commit(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockConnection) Rollback(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockConnection) NewStatement() (adbc.Statement, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(adbc.Statement), args.Error(1)
}

func (m *mockConnection) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockConnection) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	args := m.Called(ctx, serializedPartition)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(array.RecordReader), args.Error(1)
}

// mockStatement is a mock implementation of adbc.Statement for testing
type mockStatement struct {
	mock.Mock
}

func (m *mockStatement) Close() error {
	args := m.Called()
	return args.Error(0)
}

func (m *mockStatement) SetOption(key, val string) error {
	args := m.Called(key, val)
	return args.Error(0)
}

func (m *mockStatement) SetSqlQuery(query string) error {
	args := m.Called(query)
	return args.Error(0)
}

func (m *mockStatement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Get(1).(int64), args.Error(2)
	}
	return args.Get(0).(array.RecordReader), args.Get(1).(int64), args.Error(2)
}

func (m *mockStatement) ExecuteUpdate(ctx context.Context) (int64, error) {
	args := m.Called(ctx)
	return args.Get(0).(int64), args.Error(1)
}

func (m *mockStatement) Prepare(ctx context.Context) error {
	args := m.Called(ctx)
	return args.Error(0)
}

func (m *mockStatement) SetSubstraitPlan(plan []byte) error {
	args := m.Called(plan)
	return args.Error(0)
}

func (m *mockStatement) Bind(ctx context.Context, values arrow.RecordBatch) error {
	args := m.Called(ctx, values)
	return args.Error(0)
}

func (m *mockStatement) BindStream(ctx context.Context, stream array.RecordReader) error {
	args := m.Called(ctx, stream)
	return args.Error(0)
}

func (m *mockStatement) GetParameterSchema() (*arrow.Schema, error) {
	args := m.Called()
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(*arrow.Schema), args.Error(1)
}

func (m *mockStatement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Get(1).(adbc.Partitions), args.Get(2).(int64), args.Error(3)
	}
	return args.Get(0).(*arrow.Schema), args.Get(1).(adbc.Partitions), args.Get(2).(int64), args.Error(3)
}

// mockDatabase is a mock implementation of adbc.Database for testing
type mockDatabase struct {
	mock.Mock
}

func (m *mockDatabase) SetOptions(opts map[string]string) error {
	args := m.Called(opts)
	return args.Error(0)
}

func (m *mockDatabase) Open(ctx context.Context) (adbc.Connection, error) {
	args := m.Called(ctx)
	if args.Get(0) == nil {
		return nil, args.Error(1)
	}
	return args.Get(0).(adbc.Connection), args.Error(1)
}

func (m *mockDatabase) Close() error {
	args := m.Called()
	return args.Error(0)
}

func TestAsDatabaseContext(t *testing.T) {
	db := &mockDatabase{}
	dbCtx := adbc.AsDatabaseContext(db)

	assert.NotNil(t, dbCtx)

	// Test SetOptions with context
	opts := map[string]string{"key": "value"}
	db.On("SetOptions", opts).Return(nil)

	err := dbCtx.SetOptions(context.Background(), opts)
	assert.NoError(t, err)
	db.AssertExpectations(t)

	// Test Close with context
	db.On("Close").Return(nil)
	err = dbCtx.Close(context.Background())
	assert.NoError(t, err)
	db.AssertExpectations(t)

	// Test Open with context
	mockConn := &mockConnection{}
	db.On("Open", mock.Anything).Return(mockConn, nil)

	conn, err := dbCtx.Open(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, conn)
	db.AssertExpectations(t)
}

func TestAsDatabaseContext_ErrorHandling(t *testing.T) {
	db := &mockDatabase{}
	dbCtx := adbc.AsDatabaseContext(db)

	expectedErr := errors.New("test error")
	db.On("Close").Return(expectedErr)

	err := dbCtx.Close(context.Background())
	assert.Equal(t, expectedErr, err)
	db.AssertExpectations(t)
}

func TestAsConnectionContext(t *testing.T) {
	conn := &mockConnection{}
	connCtx := adbc.AsConnectionContext(conn)

	assert.NotNil(t, connCtx)

	// Test NewStatement with context
	mockStmt := &mockStatement{}
	conn.On("NewStatement").Return(mockStmt, nil)

	stmt, err := connCtx.NewStatement(context.Background())
	assert.NoError(t, err)
	assert.NotNil(t, stmt)
	conn.AssertExpectations(t)

	// Test Close with context
	conn.On("Close").Return(nil)
	err = connCtx.Close(context.Background())
	assert.NoError(t, err)
	conn.AssertExpectations(t)
}

func TestAsConnectionContext_PassThrough(t *testing.T) {
	conn := &mockConnection{}
	connCtx := adbc.AsConnectionContext(conn)

	// Test that context is passed through for methods that already accept it
	ctx := context.Background()
	conn.On("Commit", ctx).Return(nil)

	err := connCtx.Commit(ctx)
	assert.NoError(t, err)
	conn.AssertExpectations(t)
}

func TestAsStatementContext(t *testing.T) {
	stmt := &mockStatement{}
	stmtCtx := adbc.AsStatementContext(stmt)

	assert.NotNil(t, stmtCtx)

	// Test SetOption with context
	stmt.On("SetOption", "key", "value").Return(nil)
	err := stmtCtx.SetOption(context.Background(), "key", "value")
	assert.NoError(t, err)
	stmt.AssertExpectations(t)

	// Test SetSqlQuery with context
	stmt.On("SetSqlQuery", "SELECT 1").Return(nil)
	err = stmtCtx.SetSqlQuery(context.Background(), "SELECT 1")
	assert.NoError(t, err)
	stmt.AssertExpectations(t)

	// Test Close with context
	stmt.On("Close").Return(nil)
	err = stmtCtx.Close(context.Background())
	assert.NoError(t, err)
	stmt.AssertExpectations(t)
}

func TestAsStatementContext_ContextPassThrough(t *testing.T) {
	stmt := &mockStatement{}
	stmtCtx := adbc.AsStatementContext(stmt)

	// Test that context is passed through for methods that already accept it
	ctx := context.Background()
	stmt.On("Prepare", ctx).Return(nil)

	err := stmtCtx.Prepare(ctx)
	assert.NoError(t, err)
	stmt.AssertExpectations(t)
}

func TestAsDatabaseContext_Nil(t *testing.T) {
	dbCtx := adbc.AsDatabaseContext(nil)
	assert.Nil(t, dbCtx)
}

func TestAsConnectionContext_Nil(t *testing.T) {
	connCtx := adbc.AsConnectionContext(nil)
	assert.Nil(t, connCtx)
}

func TestAsStatementContext_Nil(t *testing.T) {
	stmtCtx := adbc.AsStatementContext(nil)
	assert.Nil(t, stmtCtx)
}
