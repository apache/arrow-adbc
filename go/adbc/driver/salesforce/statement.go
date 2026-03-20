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
	"strconv"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	sftypes "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/api/types"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type statement struct {
	// TODO: this should embed driverbase.StatementImplBase eventually
	alloc memory.Allocator
	cnxn  *connectionImpl

	query string

	// Create DLO options
	dloCategory   string
	dloPrimaryKey string
	dloWriteMode  string

	// Data Transform options
	targetDLO            string
	dataTransformTimeout time.Duration
	backoffConfig        sftypes.BackoffConfig
}

// Close cleans up the statement
func (s *statement) Close() error {
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
	if s.cnxn.client == nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "connection not properly initialized",
		}
	}

	if s.targetDLO != "" {
		return s.executeCreateTable(ctx)
	}

	rowLimit := s.cnxn.getQueryRowLimit()

	queryRequest := &sftypes.SqlQueryRequest{
		SQL:      s.query,
		RowLimit: rowLimit,
	}

	response, err := s.cnxn.client.CreateSqlQuery(ctx, queryRequest, nil)
	if err != nil {
		err = fmt.Errorf("SQL query execution failed: %w", err)
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  err.Error(),
		}
	}

	// Convert the response to Arrow format
	reader, rowCount, err := s.convertSqlQueryResponseToArrow(response)
	if err != nil {
		err = fmt.Errorf("failed to convert query response to Arrow: %w", err)
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  err.Error(),
		}
	}

	return reader, rowCount, nil
}

// convertSqlQueryResponseToArrow converts SQL Query API response to Arrow format
func (s *statement) convertSqlQueryResponseToArrow(response *sftypes.SqlQueryResponse) (array.RecordReader, int64, error) {
	if len(response.Data) == 0 {
		// Return empty reader with schema if available
		schema := s.buildArrowSchema(response.Metadata)
		reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{})
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

// executeCreateTable implements the CREATE TABLE flow using Data Transforms.
// It validates the transform, creates it, waits for it to become active, runs it,
// and returns an empty RecordReader with the inferred schema.
func (s *statement) executeCreateTable(ctx context.Context) (array.RecordReader, int64, error) {
	client := s.cnxn.client
	transformName := s.targetDLO

	writeMode := sftypes.WriteModeOverwrite
	if s.dloWriteMode != "" {
		writeMode = sftypes.WriteMode(s.dloWriteMode)
	}

	req := &sftypes.DataTransformRequest{
		Name:          transformName,
		Label:         transformName,
		Type:          sftypes.DataTransformTypeBatch,
		DataSpaceName: s.cnxn.dataSpace,
		Definition: sftypes.DataTransformDefinition{
			Type:    sftypes.DataTransformDefinitionTypeDCSQL,
			Version: "1.0",
			Manifest: sftypes.DataTransformManifest{
				Nodes: sftypes.DataTransformNodes{
					sftypes.DataTransformNodeID("node_" + s.targetDLO): {
						Name:         s.targetDLO,
						RelationName: s.targetDLO,
						Config: sftypes.DataTransformNodeConfig{
							Materialized: sftypes.MaterializationTable,
							WriteMode:    writeMode,
						},
						CompiledCode: s.query,
					},
				},
			},
		},
	}

	primaryKey := s.dloPrimaryKey
	if primaryKey == "" {
		primaryKey = "Id"
	}

	// Validate and create the transform
	dt, validation, err := client.ValidateAndCreateTransform(ctx, req, primaryKey)
	if err != nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("validate and create transform failed: %v", err),
		}
	}

	// Build Arrow schema from validation output data objects
	schema := s.buildSchemaFromValidation(validation, transformName)

	// Wait for the transform to become Active
	dt, err = client.WaitForTransformStatus(ctx, dt.Name, &s.backoffConfig, sftypes.StatusActive, sftypes.StatusError)
	if err != nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("waiting for transform active: %v", err),
		}
	}
	if dt.Status.IsError() {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("transform entered error status after creation"),
		}
	}

	// Associate the target DLO with the dataspace so it can be queried via SQL after the transform runs
	if resp, err := client.UpsertDataSpaceMembers(ctx, s.cnxn.dataSpace, []sftypes.DataSpaceMember{
		{Name: s.targetDLO},
	}); err != nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to associate DLO with dataspace: %v - %v", err, resp),
		}
	}

	if true { // wait
		// Run the transform and wait for completion
		dt, err = client.RunAndWaitForTransform(ctx, dt.Name, &s.backoffConfig)
		if err != nil {
			return nil, 0, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("run and wait for transform failed: %v", err),
			}
		}
		if !dt.LastRunStatus.IsSuccess() {
			return nil, 0, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("transform run ended with status %s (error=%v)", dt.LastRunStatus, dt.LastRunErrorCode),
			}
		}
	} else {
		ar, err := client.RunDataTransform(ctx, dt.Name)
		if err != nil {
			return nil, 0, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("run (no wait) for transform failed: %v", err),
			}
		}
		_ = ar
	}
	// Return empty RecordReader with the inferred schema
	reader, err := array.NewRecordReader(schema, []arrow.RecordBatch{})
	if err != nil {
		return nil, 0, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("creating record reader: %v", err),
		}
	}
	return reader, 0, nil
}

// buildSchemaFromValidation builds an Arrow schema from the validation output data objects.
func (s *statement) buildSchemaFromValidation(validation *sftypes.DataTransformValidation, transformName string) *arrow.Schema {
	if validation == nil {
		return arrow.NewSchema(nil, nil)
	}

	odos, ok := validation.OutputDataObjects[transformName]
	if !ok || len(odos) == 0 {
		return arrow.NewSchema(nil, nil)
	}

	var fields []arrow.Field
	for _, odo := range odos {
		for _, f := range odo.Fields {
			arrowType := SalesforceDLOTypeToArrowType(f.Type)
			fields = append(fields, arrow.Field{
				Name:     f.Name,
				Type:     arrowType,
				Nullable: !f.IsPrimaryKey,
			})
		}
	}

	return arrow.NewSchema(fields, nil)
}

// Bind operations
func (s *statement) Bind(_ context.Context, _ arrow.RecordBatch) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "parameter binding not supported for Salesforce",
	}
}

func (s *statement) BindStream(_ context.Context, _ array.RecordReader) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "stream binding not supported for Salesforce",
	}
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
	switch key {
	case OptionStringDLOCategory:
		return s.dloCategory, nil
	case OptionStringDLOPrimaryKey:
		return s.dloPrimaryKey, nil
	case OptionStringDLOWriteMode:
		return s.dloWriteMode, nil
	case OptionStringDLOMaterialized: // TODO
		return "table", nil
	case OptionStringTargetDLO:
		return s.targetDLO, nil
	}
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
	switch key {
	case OptionDoubleBackoffMultiplier:
		return s.backoffConfig.Multiplier, nil
	case OptionDoubleBackoffJitter:
		return s.backoffConfig.RandomFactor, nil
	}
	return 0, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown double type statement option: %s", key),
	}
}

func (s *statement) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionIntDataTransformRunTimeout:
		return s.dataTransformTimeout.Milliseconds(), nil
	case OptionIntBackoffInitialIntervalMs:
		return s.backoffConfig.InitialInterval.Milliseconds(), nil
	case OptionIntBackoffMaxIntervalMs:
		return s.backoffConfig.MaxInterval.Milliseconds(), nil
	case OptionIntBackoffMaxElapsedTimeMs:
		return s.backoffConfig.MaxElapsedTime.Milliseconds(), nil
	}
	return 0, adbc.Error{
		Code: adbc.StatusNotFound,
		Msg:  fmt.Sprintf("unknown int type statement option: %s", key),
	}
}

func (s *statement) SetOption(key, value string) error {
	switch key {
	case OptionStringDLOCategory:
		s.dloCategory = value
	case OptionStringDLOPrimaryKey:
		s.dloPrimaryKey = value
	case OptionStringDLOWriteMode:
		s.dloWriteMode = value // TODO validate
	case OptionStringDLOMaterialized: // TODO
		// TODO: noop for now
	case OptionStringTargetDLO:
		s.targetDLO = value
	default:
		// Try parsing as int option
		if intVal, err := strconv.ParseInt(value, 10, 64); err == nil {
			return s.SetOptionInt(key, intVal)
		}
		// Try parsing as double option
		if floatVal, err := strconv.ParseFloat(value, 64); err == nil {
			return s.SetOptionDouble(key, floatVal)
		}
		return adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("unknown statement option: %s", key),
		}
	}
	return nil
}

func (s *statement) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unknown statement option: %s", key),
	}
}

func (s *statement) SetOptionDouble(key string, value float64) error {
	switch key {
	case OptionDoubleBackoffMultiplier:
		s.backoffConfig.Multiplier = value
	case OptionDoubleBackoffJitter:
		s.backoffConfig.RandomFactor = value
	default:
		return adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("unknown double type statement option: %s", key),
		}
	}
	return nil
}

func (s *statement) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionIntDataTransformRunTimeout:
		s.dataTransformTimeout = time.Duration(value) * time.Millisecond
	case OptionIntBackoffInitialIntervalMs:
		s.backoffConfig.InitialInterval = time.Duration(value) * time.Millisecond
	case OptionIntBackoffMaxIntervalMs:
		s.backoffConfig.MaxInterval = time.Duration(value) * time.Millisecond
	case OptionIntBackoffMaxElapsedTimeMs:
		s.backoffConfig.MaxElapsedTime = time.Duration(value) * time.Millisecond
	default:
		return adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("unknown int type statement option: %s", key),
		}
	}
	return nil
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
