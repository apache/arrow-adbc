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

package salesforce

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/pkg"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type statement struct {
	alloc memory.Allocator
	cnxn  *connectionImpl

	query string

	// Parameter binding
	paramBinding  *arrow.Record
	streamBinding array.RecordReader
}

// Close cleans up the statement
func (s *statement) Close() error {
	s.paramBinding = nil
	if s.streamBinding != nil {
		s.streamBinding.Release()
		s.streamBinding = nil
	}
	return nil
}

// SetSqlQuery sets the SQL query to be executed
func (s *statement) SetSqlQuery(query string) error {
	s.query = query
	return nil
}

// ExecuteQuery executes the current query and returns results
func (s *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.query == "" {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}
	return s.executeSQLQuery(ctx)
}

// executeSQLQuery executes a SQL query using the Salesforce Data Cloud APIs
func (s *statement) executeSQLQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if s.cnxn.client == nil || s.cnxn.client.GetDataCloudToken() == nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "connection not properly initialized",
		}
	}

	rowLimit := s.cnxn.getQueryRowLimit()

	queryRequest := &api.SqlQueryRequest{
		SQL:      s.query,
		RowLimit: rowLimit,
	}

	response, err := api.ExecuteSqlQuery(ctx, s.cnxn.client, queryRequest)
	if err != nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("SQL query execution failed: %v", err),
		}
	}

	// Convert the response to Arrow format
	reader, rowCount, err := s.convertSqlQueryResponseToArrow(response)
	if err != nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to convert query response to Arrow: %v", err),
		}
	}

	return reader, rowCount, nil
}

// convertSqlQueryResponseToArrow converts SQL Query API response to Arrow format
func (s *statement) convertSqlQueryResponseToArrow(response *api.SqlQueryResponse) (array.RecordReader, int64, error) {
	if len(response.Data) == 0 {
		// Return empty reader with schema if available
		schema := s.buildArrowSchema(response.Metadata)
		reader, err := array.NewRecordReader(schema, []arrow.Record{})
		return reader, 0, err
	}

	schema := s.buildArrowSchema(response.Metadata)
	records, err := s.buildArrowRecords(schema, response.Data)
	if err != nil {
		return nil, 0, err
	}

	reader, err := array.NewRecordReader(schema, records)
	if err != nil {
		return nil, 0, err
	}

	return reader, int64(response.ReturnedRows), nil
}

// Bind operations
func (s *statement) Bind(ctx context.Context, values arrow.Record) error {
	s.paramBinding = &values
	return nil
}

func (s *statement) BindStream(ctx context.Context, stream array.RecordReader) error {
	s.streamBinding = stream
	return nil
}

// ExecuteUpdate executes a statement that doesn't return results (INSERT, UPDATE, DELETE)
func (s *statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	return 0, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteUpdate not yet implemented for Salesforce",
	}
}

// Prepare is typically used for prepared statements
func (s *statement) Prepare(ctx context.Context) error {
	// Salesforce Data Cloud doesn't support traditional prepared statements
	// We can validate the query syntax here if needed
	return nil
}

// Additional required interface methods
func (s *statement) GetOption(key string) (string, error) {
	return "", adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) GetOptionInt(key string) (int64, error) {
	return 0, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) SetOption(key, value string) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) SetOptionDouble(key string, value float64) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) SetOptionInt(key string, value int64) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) SetSubstraitPlan(plan []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Substrait plans not supported for Salesforce",
	}
}

func (s *statement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "parameter schema not yet implemented",
	}
}

func (s *statement) Cancel(ctx context.Context) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "query cancellation not yet implemented for Salesforce",
	}
}

func (s *statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, 0, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "partitioned execution not supported for Salesforce",
	}
}
