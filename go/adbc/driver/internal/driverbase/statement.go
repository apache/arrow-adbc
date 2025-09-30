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
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

const (
	StatementMessageOptionUnknown              = "Unknown statement option"
	StatementMessageOptionUnsupported          = "Unsupported statement option"
	StatementMessageTraceParentIncorrectFormat = "Incorrect or unsupported trace parent format"
)

type StatementImpl interface {
	adbc.Statement
	adbc.StatementExecuteSchema
	adbc.GetSetOptions
	adbc.OTelTracing
	Base() *StatementImplBase
}

type StatementImplBase struct {
	ErrorHelper ErrorHelper
	Tracer      trace.Tracer

	cnxn        *ConnectionImplBase
	traceParent string
}

type Statement interface {
	adbc.Statement
	adbc.GetSetOptions
}

type statement struct {
	StatementImpl
}

func NewStatementImplBase(cnxn *ConnectionImplBase, errorHelper ErrorHelper) StatementImplBase {
	return StatementImplBase{
		ErrorHelper: errorHelper,
		Tracer:      cnxn.Tracer,
		cnxn:        cnxn,
	}
}

func NewStatement(impl StatementImpl) Statement {
	return &statement{
		StatementImpl: impl,
	}
}

func (st *StatementImplBase) SetOption(key, value string) error {
	switch strings.ToLower(key) {
	case adbc.OptionKeyTelemetryTraceParent:
		st.SetTraceParent(strings.TrimSpace(value))
		return nil
	}
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) SetOptionBytes(key string, value []byte) error {
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) SetOptionInt(key string, value int64) error {
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) SetOptionDouble(key string, value float64) error {
	return st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) GetOption(key string) (string, error) {
	switch strings.ToLower(key) {
	case adbc.OptionKeyTelemetryTraceParent:
		return st.GetTraceParent(), nil
	}
	return "", st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) GetOptionBytes(key string) ([]byte, error) {
	return nil, st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) GetOptionInt(key string) (int64, error) {
	return 0, st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) GetOptionDouble(key string) (float64, error) {
	return 0, st.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", StatementMessageOptionUnknown, key)
}

func (st *StatementImplBase) GetTraceParent() string {
	return st.traceParent
}

func (st *StatementImplBase) SetTraceParent(traceParent string) {
	st.traceParent = traceParent
}

func (st *StatementImplBase) StartSpan(
	ctx context.Context,
	spanName string,
	opts ...trace.SpanStartOption,
) (context.Context, trace.Span) {
	ctx, _ = maybeAddTraceParent(ctx, st.cnxn, st)
	return st.Tracer.Start(ctx, spanName, opts...)
}

func (st *StatementImplBase) GetInitialSpanAttributes() []attribute.KeyValue {
	return st.cnxn.GetInitialSpanAttributes()
}
