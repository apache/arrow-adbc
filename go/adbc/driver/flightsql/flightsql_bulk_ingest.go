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

package flightsql

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// ingestOptions holds the configuration for bulk ingestion operations.
type ingestOptions struct {
	targetTable string
	mode        string
	catalog     *string
	dbSchema    *string
	temporary   bool
}

// buildTableDefinitionOptions maps ADBC ingest modes to FlightSQL TableDefinitionOptions.
func buildTableDefinitionOptions(mode string) *flightsql.TableDefinitionOptions {
	opts := &flightsql.TableDefinitionOptions{}

	switch mode {
	case adbc.OptionValueIngestModeCreate:
		// Create new table, fail if exists
		opts.IfNotExist = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_NOT_EXIST_OPTION_CREATE
		opts.IfExists = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_FAIL
	case adbc.OptionValueIngestModeAppend:
		// Append to existing table, fail if not exists
		opts.IfNotExist = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_NOT_EXIST_OPTION_FAIL
		opts.IfExists = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_APPEND
	case adbc.OptionValueIngestModeReplace:
		// Replace table if exists, create if not
		opts.IfNotExist = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_NOT_EXIST_OPTION_CREATE
		opts.IfExists = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_REPLACE
	case adbc.OptionValueIngestModeCreateAppend:
		// Create table if not exists, append if exists
		opts.IfNotExist = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_NOT_EXIST_OPTION_CREATE
		opts.IfExists = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_APPEND
	default:
		// Default to create mode
		opts.IfNotExist = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_NOT_EXIST_OPTION_CREATE
		opts.IfExists = pb.CommandStatementIngest_TableDefinitionOptions_TABLE_EXISTS_OPTION_FAIL
	}

	return opts
}

// buildExecuteIngestOpts creates ExecuteIngestOpts from ingestOptions.
func buildExecuteIngestOpts(opts ingestOptions) *flightsql.ExecuteIngestOpts {
	tableDefOpts := buildTableDefinitionOptions(opts.mode)

	ingestOpts := &flightsql.ExecuteIngestOpts{
		TableDefinitionOptions: tableDefOpts,
		Table:                  opts.targetTable,
		Temporary:              opts.temporary,
	}

	if opts.catalog != nil {
		ingestOpts.Catalog = opts.catalog
	}
	if opts.dbSchema != nil {
		ingestOpts.Schema = opts.dbSchema
	}

	return ingestOpts
}

// createRecordReaderFromBatch converts a single record batch to a RecordReader.
func createRecordReaderFromBatch(batch arrow.RecordBatch) (array.RecordReader, error) {
	rdr, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
	if err != nil {
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("[Flight SQL Statement] failed to create record reader: %s", err),
			Code: adbc.StatusInternal,
		}
	}
	return rdr, nil
}

// executeIngest performs bulk ingestion using the FlightSQL client's ExecuteIngest method.
// This is called from the statement when a target table has been set for bulk ingest.
func (s *statement) executeIngest(ctx context.Context) (nRows int64, err error) {
	var span trace.Span
	ctx, span = internal.StartSpan(ctx, "statement.executeIngest", s)
	defer func() {
		span.SetAttributes(attribute.Int64("ingested.row_count", nRows))
		internal.EndSpan(span, err)
	}()

	if s.streamBind == nil && s.bound == nil {
		return -1, adbc.Error{
			Msg:  "[Flight SQL Statement] must call Bind before bulk ingestion",
			Code: adbc.StatusInvalidState,
		}
	}

	catalogStr := ""
	if s.catalog != nil {
		catalogStr = *s.catalog
	}
	dbSchemaStr := ""
	if s.dbSchema != nil {
		dbSchemaStr = *s.dbSchema
	}
	startAttrs := []attribute.KeyValue{
		attribute.String("target_table", s.targetTable),
		attribute.String("mode", s.ingestMode),
		attribute.String("catalog", catalogStr),
		attribute.String("db_schema", dbSchemaStr),
		attribute.Bool("temporary", s.temporary),
		attribute.Bool("streamBind", s.streamBind != nil),
		attribute.Bool("recordBound", s.bound != nil),
	}
	span.SetAttributes(startAttrs...)

	opts := ingestOptions{
		targetTable: s.targetTable,
		mode:        s.ingestMode,
		catalog:     s.catalog,
		dbSchema:    s.dbSchema,
		temporary:   s.temporary,
	}

	ingestOpts := buildExecuteIngestOpts(opts)

	// Get the record reader to ingest
	var rdr array.RecordReader
	if s.streamBind != nil {
		rdr = s.streamBind
	} else {
		rdr, err = createRecordReaderFromBatch(s.bound)
		if err != nil {
			return -1, err
		}
	}

	ctx = metadata.NewOutgoingContext(ctx, s.hdrs)
	var header, trailer metadata.MD
	callOpts := append([]grpc.CallOption{}, grpc.Header(&header), grpc.Trailer(&trailer), s.timeouts)

	nRows, err = s.cnxn.cl.ExecuteIngest(ctx, rdr, ingestOpts, callOpts...)
	finishAttrs := []attribute.KeyValue{
		attribute.Int64("rowsIngested", nRows),
	}
	finishAttrs = append(finishAttrs, correlationHeaderAttrs(header)...)
	finishAttrs = append(finishAttrs, correlationHeaderAttrs(trailer)...)
	if err != nil {
		err = adbcFromFlightStatusWithDetails(err, header, trailer, "ExecuteIngest")
		return -1, err
	}
	span.SetAttributes(finishAttrs...)

	return nRows, nil
}
