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

package driverbase

import (
	"context"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"golang.org/x/exp/slog"
)

const (
	StatementMessageOptionUnknown = "Unknown statement option"
)

// StatementImpl is an interface that drivers implement to provide
// vendor-specific functionality.
type StatementImpl interface {
	adbc.Statement
	adbc.GetSetOptions
	adbc.StatementExecuteSchema
	adbc.DatabaseLogging
	Base() *StatementImplBase
}

// StatementImplBase is a struct that provides default implementations of some of the
// methods defined in the StatementImpl interface. It is meant to be used as a composite
// struct for a driver's StatementImpl implementation.
//
// It is up to the driver implementor to understand the semantics of the default
// behavior provided. For example, in some cases the default implementation may provide
// a fallback value while in other cases it may provide a partial-result which must be
// merged with the driver-specific-result, if any.
type StatementImplBase struct {
	Alloc       memory.Allocator
	ErrorHelper ErrorHelper
	Logger      *slog.Logger
}

// NewStatementImplBase instantiates StatementImplBase.
//
//   - connection is a ConnectionImplBase containing the common resources from the parent
//     connection, allowing the Arrow allocator, error handler, and logger to be reused.
func NewStatementImplBase(connection *ConnectionImplBase) StatementImplBase {
	return StatementImplBase{Alloc: connection.Alloc, ErrorHelper: connection.ErrorHelper, Logger: connection.Logger}
}

func (base *StatementImplBase) Base() *StatementImplBase {
	return base
}

func (base *StatementImplBase) SetLogger(logger *slog.Logger) {
	if logger != nil {
		base.Logger = logger
	} else {
		base.Logger = nilLogger()
	}
}

func (base *StatementImplBase) Bind(ctx context.Context, values arrow.Record) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Bind is not implemented")
}

func (base *StatementImplBase) BindStream(ctx context.Context, stream array.RecordReader) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "BindStream is not implemented")
}

func (base *StatementImplBase) Close() error {
	return nil
}

func (base *StatementImplBase) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, 0, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecutePartitions is not implemented")
}

func (base *StatementImplBase) ExecuteQuery(context.Context) (array.RecordReader, int64, error) {
	return nil, 0, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteQuery is not implemented")
}

func (base *StatementImplBase) ExecuteSchema(context.Context) (*arrow.Schema, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteSchema is not implemented")
}

func (base *StatementImplBase) ExecuteUpdate(context.Context) (int64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ExecuteUpdate is not implemented")
}

func (base *StatementImplBase) GetParameterSchema() (*arrow.Schema, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "GetParameterSchema is not implemented")
}

func (base *StatementImplBase) Prepare(context.Context) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Prepare is not implemented")
}

func (base *StatementImplBase) SetSqlQuery(query string) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "SetSqlQuery is not implemented")
}

func (base *StatementImplBase) SetSubstraitPlan(plan []byte) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "SetSubstraitPlan is not implemented")
}

func (base *StatementImplBase) GetOption(key string) (string, error) {
	return "", base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (base *StatementImplBase) GetOptionBytes(key string) ([]byte, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (base *StatementImplBase) GetOptionDouble(key string) (float64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (base *StatementImplBase) GetOptionInt(key string) (int64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (base *StatementImplBase) SetOption(key string, val string) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (base *StatementImplBase) SetOptionBytes(key string, val []byte) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (base *StatementImplBase) SetOptionDouble(key string, val float64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (base *StatementImplBase) SetOptionInt(key string, val int64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

var _ StatementImpl = (*StatementImplBase)(nil)
