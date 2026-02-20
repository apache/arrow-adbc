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
	"github.com/apache/arrow/go/v15/arrow"
	"github.com/apache/arrow/go/v15/arrow/array"
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
	db.On("Open", mock.AnythingOfType("*context.emptyCtx")).Return(mockConn, nil)

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
