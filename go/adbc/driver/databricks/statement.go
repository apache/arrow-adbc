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

package databricks

import (
	"context"
	"fmt"
	"log"
	"strconv"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sdk-go/retries"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

const (
	OptionIntByteLimit = "adbc.databricks.byte_limit"
	OptionIntRowLimit  = "adbc.databricks.row_limit"

	OptionBoolTruncated = "adbc.databricks.truncated" // read-only
)

type statement struct {
	alloc memory.Allocator
	conn  *connectionImpl

	// req is the request to be sent to the Databricks SQL API
	req *sql.ExecuteStatementRequest

	// statementId is the ID of the current statement in the Databricks SQL API
	statementId string
	// The manifest from the first StatementResponse
	manifest *sql.ResultManifest
}

func NewStatement(conn *connectionImpl) (adbc.Statement, error) {
	return &statement{
		alloc: conn.Alloc,
		conn:  conn,

		req: &sql.ExecuteStatementRequest{
			// Configurable via OptionIntByteLimit
			ByteLimit: 0,
			Catalog:   conn.catalog,
			// Arrow responses can't be INLINE, so we always use EXTERNAL_LINKS
			Disposition: sql.DispositionExternalLinks,
			Format:      sql.FormatArrowStream,
			// Continue execution asynchronously after the short wait timeout
			OnWaitTimeout: sql.ExecuteStatementRequestOnWaitTimeoutContinue,
			// TODO: set this when binding is implemented
			Parameters: []sql.StatementParameterListItem{},
			// Configurable via OptionIntRowLimit
			RowLimit: 0,
			Schema:   conn.dbSchema,
			// Populated by SetSqlQuery()
			Statement: "",
			// TODO: make WaitTimeout configurable
			// Short timeout to get to the first record batch or just
			// the statement ID for polling later. Must be in [5s-50s].
			WaitTimeout: "5s",
			WarehouseId: conn.client.Config.WarehouseID,
		},

		statementId: "",
		manifest:    nil,
	}, nil
}

func (stmt *statement) resetStatement(sql string) error {
	// TODO: think about cancelation and stmt re-use
	stmt.req.Parameters = nil
	stmt.req.Statement = sql
	stmt.statementId = ""
	stmt.manifest = nil
	return nil
}

func (stmt *statement) SetOption(key, val string) error {
	switch key {
	case OptionBoolTruncated:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[Databricks] `%s` property is read-only", key),
		}
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[Databricks] Unknown statement string type option `%s`", key),
		}
	}
}

func (stmt *statement) GetOption(key string) (string, error) {
	switch key {
	case OptionBoolTruncated:
		truncated := false
		if stmt.manifest != nil {
			truncated = stmt.manifest.Truncated
		}
		return strconv.FormatBool(truncated), nil
	default:
		val, err := stmt.conn.GetOption(key)
		if err == nil {
			return val, nil
		}
		return "", err
	}
}

func (stmt *statement) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionIntRowLimit:
		stmt.req.RowLimit = value
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("[Databricks] Unknown statement string type option `%s`", key),
		}
	}
	return nil
}

func (stmt *statement) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionIntRowLimit:
		if stmt.req != nil {
			return stmt.req.RowLimit, nil
		}
		return 0, nil
	default:
		val, err := stmt.conn.GetOptionInt(key)
		if err == nil {
			return val, nil
		}
		return 0, err
	}
}

func (stmt *statement) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[Databricks] Unknown statement bytes type option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (stmt *statement) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Msg:  fmt.Sprintf("[Databricks] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

func (stmt *statement) SetOptionDouble(key string, value float64) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[Databricks] Unknown statement double type option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (stmt *statement) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[Databricks] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

// SetSqlQuery sets the query string to be executed.
//
// The query can then be executed with any of the Execute methods.
// For queries expected to be executed repeatedly, Prepare should be
// called before execution.
func (stmt *statement) SetSqlQuery(query string) error {
	return stmt.resetStatement(query)
}

// ExecuteQuery executes the current query or prepared statement
// and returnes a RecordReader for the results along with the number
// of rows affected if known, otherwise it will be -1.
//
// This invalidates any prior result sets on this statement.
//
// Since ADBC 1.1.0: releasing the returned RecordReader without
// consuming it fully is equivalent to calling AdbcStatementCancel.
func (stmt *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if stmt.req.Statement == "" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "ExecuteQuery called before SetSqlQuery",
		}
	}
	// TODO: perform more validations
	reader, err := stmt.executeQueryInternal(ctx)
	if err != nil {
		return nil, -1, err
	}
	return reader, reader.TotalRowCount, nil
}

func (stmt *statement) executeQueryInternal(ctx context.Context) (*reader, error) {
	se := stmt.conn.StatementExecution()
	res, err := se.ExecuteStatement(ctx, *stmt.req)
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusUnknown,
			Msg:  fmt.Sprintf("[Databricks] failed to execute statement: %s", err),
		}
	}

	// Statement execution state:
	//
	// - `FAILED`: execution failed; reason for failure described in accomanying
	//   error message.
	// - `CANCELED`: user canceled; can come from explicit cancel call, or timeout
	//   with `on_wait_timeout=CANCEL`.
	// - `CLOSED`: execution successful, and statement closed; result no longer
	//   available for fetch.
	// - `RUNNING`: running.
	// - `PENDING`: waiting for warehouse.
	// - `SUCCEEDED`: execution was successful, result data available for fetch.

	state := res.Status.State
	switch state {
	case sql.StatementStateSucceeded:
		if res.Result.ChunkIndex != 0 {
			log.Fatal("first ChunkIndex is not 0")
		}
		return NewRecordReader(se, res.StatementId, res.Result, res.Manifest)
	case sql.StatementStatePending, sql.StatementStateRunning:
		// Keep polling until the statement reaches a terminal state
		// TODO: make this configurable
		timeout := 20 * time.Minute
		return retries.Poll(ctx, timeout,
			func() (*reader, *retries.Err) {
				res, err := se.GetStatement(ctx, sql.GetStatementRequest{
					StatementId: res.StatementId,
				})
				if err != nil {
					adbcErr := adbc.Error{
						Code: adbc.StatusUnknown,
						Msg:  fmt.Sprintf("[Databricks] Failed to execute statement: %s", err),
					}
					return nil, retries.Halt(adbcErr)
				}
				state := res.Status.State
				switch state {
				case sql.StatementStateSucceeded:
					r, err := NewRecordReader(se, res.StatementId, res.Result, res.Manifest)
					if err != nil {
						return nil, retries.Halt(err)
					}
					return r, nil
				case sql.StatementStateRunning, sql.StatementStatePending:
					return nil, retries.Continues(state.String())
				case sql.StatementStateFailed, sql.StatementStateCanceled, sql.StatementStateClosed:
					adbcErr := errorFromStmtStatus(res.Status)
					return nil, retries.Halt(adbcErr)
				default:
					return nil, retries.Halt(unexpectedExecutionState(state))
				}
			})
	case sql.StatementStateFailed, sql.StatementStateCanceled, sql.StatementStateClosed:
		return nil, errorFromStmtStatus(res.Status)
	default:
		return nil, unexpectedExecutionState(state)
	}
}

func unexpectedExecutionState(state sql.StatementState) error {
	return adbc.Error{
		Code: adbc.StatusInternal,
		Msg:  fmt.Sprintf("[Databricks] Unexpected execution state: %s", state.String()),
	}
}

func errorFromStmtStatus(s *sql.StatementStatus) adbc.Error {
	msg := s.State.String()
	if s.Error != nil {
		msg = fmt.Sprintf("%s: %s %s", msg, s.Error.ErrorCode, s.Error.Message)
	}
	adbcCode := adbc.StatusUnknown
	if s.State == sql.StatementStateCanceled || s.State == sql.StatementStateClosed {
		adbcCode = adbc.StatusCancelled
	}
	return adbc.Error{
		Code: adbcCode,
		Msg:  msg,
	}
}

// ExecuteUpdate executes a statement that does not generate a result
// set. It returns the number of rows affected if known, otherwise -1.
func (stmt *statement) ExecuteUpdate(context.Context) (int64, error) {
	// TODO
	return -1, nil
}

// Prepare turns this statement into a prepared statement to be executed
// multiple times. This invalidates any prior result sets.
func (stmt *statement) Prepare(context.Context) error {
	return nil
}

func (stmt *statement) SetSubstraitPlan(plan []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Substrait not yet implemented for Databricks driver",
	}
}

// Bind uses an arrow record batch to bind parameters to the query.
//
// This can be used for bulk inserts or for prepared statements.
// The driver will call release on the passed in Record when it is done,
// but it may not do this until the statement is closed or another
// record is bound.
func (stmt *statement) Bind(ctx context.Context, values arrow.Record) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Bind not yet implemented for Databricks driver",
	}
}

// BindStream uses a record batch stream to bind parameters for this
// query. This can be used for bulk inserts or prepared statements.
//
// The driver will call Release on the record reader, but may not do this
// until Close is called.
func (stmt *statement) BindStream(ctx context.Context, stream array.RecordReader) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "BindStream not yet implemented for Databricks driver",
	}
}

// GetParameterSchema returns an Arrow schema representation of
// the expected parameters to be bound.
//
// This retrieves an Arrow Schema describing the number, names, and
// types of the parameters in a parameterized statement. The fields
// of the schema should be in order of the ordinal position of the
// parameters; named parameters should appear only once.
//
// If the parameter does not have a name, or a name cannot be determined,
// the name of the corresponding field in the schema will be an empty
// string. If the type cannot be determined, the type of the corresponding
// field will be NA (NullType).
//
// This should be called only after calling Prepare.
//
// This should return an error with StatusNotImplemented if the schema
// cannot be determined.
func (stmt *statement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetParameterSchema not yet implemented for Databricks driver",
	}
}

// ExecutePartitions executes the current statement and gets the results
// as a partitioned result set.
//
// It returns the Schema of the result set, the collection of partition
// descriptors and the number of rows affected, if known. If unknown,
// the number of rows affected will be -1.
//
// If the driver does not support partitioned results, this will return
// an error with a StatusNotImplemented code.
//
// When OptionKeyIncremental is set, this should be called
// repeatedly until receiving an empty Partitions.
func (stmt *statement) ExecutePartitions(context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecutePartitions not yet implemented for Databricks driver",
	}
}

// Close releases any relevant resources associated with this statement
// and closes it (particularly if it is a prepared statement).
//
// A statement instance should not be used after Close is called.
func (stmt *statement) Close() error {
	if stmt.statementId == "" {
		return nil
	}
	se := stmt.conn.StatementExecution()
	if se == nil {
		return nil
	}
	ctx := context.TODO()
	req := sql.CancelExecutionRequest{
		StatementId: stmt.statementId,
	}
	err := se.CancelExecution(ctx, req)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusUnknown,
			Msg:  fmt.Sprintf("Failed to cancel statement %s: %s", stmt.statementId, err),
		}
	}
	stmt.statementId = ""
	return nil
}

var _ adbc.GetSetOptions = (*statement)(nil)
