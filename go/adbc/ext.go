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
	"log/slog"

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
	// [set the status], [add attributes], [add events], etc. Implementors should enhance
	// the [context.Context] with the provided trace parent value, if it exists
	//
	// [span]: https://opentelemetry.io/docs/concepts/signals/traces/#span-context
	// [add events]: https://opentelemetry.io/docs/languages/go/instrumentation/#events
	// [set the status]: https://opentelemetry.io/docs/languages/go/instrumentation/#set-span-status
	// [add attributes]: https://opentelemetry.io/docs/languages/go/instrumentation/#span-attributes
	StartSpan(ctx context.Context, spanName string, opts ...trace.SpanStartOption) (context.Context, trace.Span)

	GetInitialSpanAttributes() *[]attribute.KeyValue
}
