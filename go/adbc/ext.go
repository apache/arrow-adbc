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
	"errors"
	"fmt"
	"log/slog"

	"github.com/apache/arrow-go/v18/arrow/array"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
)

// DatabaseLogging is a Database that also supports logging information to an
// application-supplied log sink.
//
// EXPERIMENTAL. Not formally part of the ADBC APIs.
type DatabaseLogging interface {
	SetLogger(*slog.Logger)
}

// OTelTracingInit is a Database that also supports OpenTelemetry tracing.
//
// EXPERIMENTAL. Not formally part of the ADBC APIs.
type OTelTracingInit interface {
	InitTracing(ctx context.Context, driverName string, driverVersion string) error
}

// DriverWithContext is an extension interface to allow the creation of a database
// by providing an existing [context.Context] to initialize OpenTelemetry tracing.
// It is similar to [database/sql.Driver] taking a map of keys and values as options
// to initialize a [Connection] to the database. Any common connection
// state can live in the Driver itself, for example an in-memory database
// can place ownership of the actual database in this driver.
//
// Any connection specific options should be set using SetOptions before
// calling Open.
//
// EXPERIMENTAL. Not formally part of the ADBC APIs.
type DriverWithContext interface {
	NewDatabaseWithContext(ctx context.Context, opts map[string]string) (Database, error)
}

// OTelTracing is an interface that supports instrumentation of [OpenTelementry tracing].
//
// EXPERIMENTAL. Not formally part of the ADBC APIs.
//
// [OpenTelementry tracing]: https://opentelemetry.io/docs/concepts/signals/traces/
type OTelTracing interface {
	// Sets the trace parent from an external trace span. A blank value, removes the parent relationship.
	SetTraceParent(string)
	// Gets the trace parent from an external trace span. A blank value, indicates no parent relationship.
	GetTraceParent() string
	// Starts a new [span] and returns a [trace.Span] which can be used to
	// [set the status], [add attributes], [add events], etc. Implementers should enhance
	// the [context.Context] with the provided trace parent value, if it exists
	//
	// [span]: https://opentelemetry.io/docs/concepts/signals/traces/#span-context
	// [add events]: https://opentelemetry.io/docs/languages/go/instrumentation/#events
	// [set the status]: https://opentelemetry.io/docs/languages/go/instrumentation/#set-span-status
	// [add attributes]: https://opentelemetry.io/docs/languages/go/instrumentation/#span-attributes
	StartSpan(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span)

	// Gets the initial span attributes for any newly started span.
	GetInitialSpanAttributes() []attribute.KeyValue
}

// IngestStreamOption bundles the most-common IngestStream settings.
// Any other ADBC options can go into Extra.
type IngestStreamOption struct {
	TargetTable string            // required
	IngestMode  string            // required, e.g. adbc.OptionValueIngestModeCreateAppend, or OptionValueIngestModeReplace
	Extra       map[string]string // any other stmt.SetOption(...) args
}

// IngestStream is a helper for executing a bulk ingestion. This is a wrapper around
// the five-step boilerplate of NewStatement, SetOption, Bind,
// Execute, and Close.
//
// This is not part of the ADBC API specification.
func IngestStream(ctx context.Context, cnxn Connection, reader array.RecordReader, opt IngestStreamOption) (int64, error) {
	// 1) Create a new statement
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return -1, fmt.Errorf("IngestStream: NewStatement: %w", err)
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	// 2) Bind the record batch stream
	if err = stmt.BindStream(ctx, reader); err != nil {
		return -1, fmt.Errorf("IngestStream: BindStream: %w", err)
	}

	// Set required options
	if err := stmt.SetOption(OptionKeyIngestTargetTable, opt.TargetTable); err != nil {
		return 0, fmt.Errorf("IngestStream: SetOption(target_table=%s): %w", opt.TargetTable, err)
	}
	if err := stmt.SetOption(OptionKeyIngestMode, opt.IngestMode); err != nil {
		return 0, fmt.Errorf("IngestStream: SetOption(mode=%s): %w", opt.IngestMode, err)
	}

	// Set any extras
	for k, v := range opt.Extra {
		if err := stmt.SetOption(k, v); err != nil {
			return 0, fmt.Errorf("IngestStream: SetOption(%s=%s): %w", k, v, err)
		}
	}

	// 4) Execute the update
	count, err := stmt.ExecuteUpdate(ctx)
	if err != nil {
		return 0, fmt.Errorf("IngestStream: ExecuteUpdate: %w", err)
	}

	return count, nil
}
