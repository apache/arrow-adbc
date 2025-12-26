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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	pb "github.com/apache/arrow-go/v18/arrow/flight/gen/flight"
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

// executeIngestWithReader performs the bulk ingest operation with the given record reader.
func executeIngestWithReader(
	ctx context.Context,
	client *flightsql.Client,
	rdr array.RecordReader,
	opts *flightsql.ExecuteIngestOpts,
	callOpts ...grpc.CallOption,
) (int64, error) {
	return client.ExecuteIngest(ctx, rdr, opts, callOpts...)
}

// createRecordReaderFromBatch converts a single record batch to a RecordReader.
func createRecordReaderFromBatch(batch arrow.RecordBatch) (array.RecordReader, error) {
	rdr, err := array.NewRecordReader(batch.Schema(), []arrow.RecordBatch{batch})
	if err != nil {
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("[Flight SQL Statement] failed to create record reader: %s", err.Error()),
			Code: adbc.StatusInternal,
		}
	}
	return rdr, nil
}

// executeIngest performs bulk ingestion using the FlightSQL client's ExecuteIngest method.
// This is called from the statement when a target table has been set for bulk ingest.
func (s *statement) executeIngest(ctx context.Context) (int64, error) {
	if s.streamBind == nil && s.bound == nil {
		return -1, adbc.Error{
			Msg:  "[Flight SQL Statement] must call Bind before bulk ingestion",
			Code: adbc.StatusInvalidState,
		}
	}

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
	var err error
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

	nRows, err := executeIngestWithReader(ctx, s.cnxn.cl, rdr, ingestOpts, callOpts...)
	if err != nil {
		return -1, adbcFromFlightStatusWithDetails(err, header, trailer, "ExecuteIngest")
	}

	return nRows, nil
}
