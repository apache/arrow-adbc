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
	"strings"

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

// IngestStreamOption bundles the IngestStream options.
// Driver specific options can go into Extra.
type IngestStreamOptions struct {
	// Optional catalog/catalogue name
	Catalog string

	// Optional database schema (namespace)
	DBSchema string

	// If true, ingest into a temporary table
	Temporary bool

	// Driver-specific options
	Extra map[string]string
}

// IngestStream is a helper for executing a bulk ingestion. This is a wrapper around
// the five-step boilerplate of NewStatement, SetOption, Bind,
// Execute, and Close.
//
// This is not part of the ADBC API specification.
func IngestStream(ctx context.Context, cnxn Connection, reader array.RecordReader, targetTable, ingestMode string, opt IngestStreamOptions) (int64, error) {
	// Create a new statement
	stmt, err := cnxn.NewStatement()
	if err != nil {
		return -1, fmt.Errorf("error during ingestion: NewStatement: %w", err)
	}
	defer func() {
		err = errors.Join(err, stmt.Close())
	}()

	// Bind the record batch stream
	if err = stmt.BindStream(ctx, reader); err != nil {
		return -1, fmt.Errorf("error during ingestion: BindStream: %w", err)
	}

	// Set required options
	if err = stmt.SetOption(OptionKeyIngestTargetTable, targetTable); err != nil {
		return -1, fmt.Errorf("error during ingestion: SetOption(target_table=%s): %w", targetTable, err)
	}
	if err = stmt.SetOption(OptionKeyIngestMode, ingestMode); err != nil {
		return -1, fmt.Errorf("error during ingestion: SetOption(mode=%s): %w", ingestMode, err)
	}

	// Set other options if provided
	if opt.Catalog != "" {
		if err = stmt.SetOption(OptionValueIngestTargetCatalog, opt.Catalog); err != nil {
			return -1, fmt.Errorf("error during ingestion: target_catalog=%s: %w", opt.Catalog, err)
		}
	}
	if opt.DBSchema != "" {
		if err = stmt.SetOption(OptionValueIngestTargetDBSchema, opt.DBSchema); err != nil {
			return -1, fmt.Errorf("error during ingestion: target_db_schema=%s: %w", opt.DBSchema, err)
		}
	}
	if opt.Temporary {
		if err = stmt.SetOption(OptionValueIngestTemporary, OptionValueEnabled); err != nil {
			return -1, fmt.Errorf("error during ingestion: temporary=true: %w", err)
		}
	}

	// Set driver specific options
	for k, v := range opt.Extra {
		if err = stmt.SetOption(k, v); err != nil {
			return -1, fmt.Errorf("error during ingestion: SetOption(%s=%s): %w", k, v, err)
		}
	}

	// Execute the update
	var count int64
	count, err = stmt.ExecuteUpdate(ctx)
	if err != nil {
		return -1, fmt.Errorf("error during ingestion: ExecuteUpdate: %w", err)
	}

	return count, nil
}

// VersionInfo contains comprehensive driver and library version information.
type VersionInfo struct {
	DriverName    string            `json:"driver_name"`    // e.g., "ADBC PostgreSQL Driver"
	DriverVersion string            `json:"driver_version"` // e.g., "15.13.0000"
	LibraryInfo   map[string]string `json:"library_info"`   // Additional library versions, protocol info, etc.
}

// GetVersionInfo retrieves comprehensive driver version information from a connection.
// This helper function encapsulates the complex process of querying driver info codes,
// handling streaming record batches and union arrays, extracting and safely cloning
// string values, and managing errors with fallback defaults.
//
// Returns detailed version information including driver name, version, and additional
// library information such as Arrow version, vendor details, and ADBC version.
//
// This is not part of the ADBC API specification.
func GetVersionInfo(ctx context.Context, cnxn Connection) (*VersionInfo, error) {
	stream, err := cnxn.GetInfo(ctx, []InfoCode{
		InfoDriverName,
		InfoDriverVersion,
		InfoDriverArrowVersion,
		InfoDriverADBCVersion,
		InfoVendorName,
		InfoVendorVersion,
		InfoVendorArrowVersion,
		InfoVendorSql,
		InfoVendorSubstrait,
		InfoVendorSubstraitMinVersion,
		InfoVendorSubstraitMaxVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("error during GetInfo: %w", err)
	}
	defer stream.Release()

	var name, version string
	libInfo := make(map[string]string)

	for stream.Next() {
		batch := stream.Record()
		defer batch.Release()
		codeArr := batch.Column(0).(*array.Uint32)
		unionArr := batch.Column(1).(*array.DenseUnion)

		for i := 0; i < int(batch.NumRows()); i++ {
			code := InfoCode(codeArr.Value(i))
			child := unionArr.Field(unionArr.ChildID(i))
			offset := int(unionArr.ValueOffset(i))
			unionArr.GetOneForMarshal(i)

			if child.IsNull(offset) {
				continue
			}

			// Handle different union types based on child ID (similar to validation tests)
			switch unionArr.ChildID(i) {
			case 0: // String values
				if strArray, ok := child.(*array.String); ok {
					// Create a copy of the string to avoid memory issues
					val := strings.Clone(strArray.Value(offset))

					switch code {
					case InfoDriverName:
						name = val
					case InfoDriverVersion:
						version = val
					case InfoDriverArrowVersion:
						libInfo["driver_arrow_version"] = val
					case InfoVendorName:
						libInfo["vendor_name"] = val
					case InfoVendorVersion:
						libInfo["vendor_version"] = val
					case InfoVendorArrowVersion:
						libInfo["vendor_arrow_version"] = val
					case InfoVendorSubstraitMinVersion:
						libInfo["vendor_substrait_min_version"] = val
					case InfoVendorSubstraitMaxVersion:
						libInfo["vendor_substrait_max_version"] = val
					}
				}

			case 1: // Boolean values
				if boolArray, ok := child.(*array.Boolean); ok {
					val := boolArray.Value(offset)

					switch code {
					case InfoVendorSql:
						if val {
							libInfo["vendor_sql_support"] = "true"
						} else {
							libInfo["vendor_sql_support"] = "false"
						}
					case InfoVendorSubstrait:
						if val {
							libInfo["vendor_substrait_support"] = "true"
						} else {
							libInfo["vendor_substrait_support"] = "false"
						}
					}
				}

			case 2: // Int64 values
				if int64Array, ok := child.(*array.Int64); ok {
					val := int64Array.Value(offset)

					switch code {
					case InfoDriverADBCVersion:
						libInfo["driver_adbc_version"] = fmt.Sprintf("%d", val)
					}
				}
			}
		}
	}

	if err := stream.Err(); err != nil {
		return nil, fmt.Errorf("error reading info stream: %w", err)
	}

	// Set defaults if not provided
	if name == "" {
		name = "ADBC Driver"
	}
	if version == "" {
		version = "unknown"
	}

	return &VersionInfo{
		DriverName:    name,
		DriverVersion: version,
		LibraryInfo:   libInfo,
	}, nil
}
