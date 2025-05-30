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

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/databricks/databricks-sdk-go/service/compute"
)

const (
	CmdOptionIntByteLimit  = "adbc.databricks.byte_limit"
	CmdOptionIntRowLimit   = "adbc.databricks.row_limit"
	CmdOptionBoolTruncated = "adbc.databricks.truncated" // read-only
)

type command struct {
	alloc memory.Allocator
	conn  *connectionImpl

	req *compute.Command

	commandId string
}

func NewCommand(conn *connectionImpl) (adbc.Statement, error) {
	return &command{
		alloc: conn.Alloc,
		conn:  conn,
		req: &compute.Command{
			Command:   "",
			Language:  compute.LanguageSql,
			ClusterId: conn.client.Config.ClusterID,
			ContextId: conn.contextId,
		},
	}, nil
}

func (cmd *command) resetCommand(sql string) error {
	// TODO: think about cancelation and cmd re-use
	cmd.req.Command = sql
	cmd.commandId = ""
	return nil
}

// SetOption sets a string option on this statement.
func (cmd *command) SetOption(key, val string) error {
	switch key {
	case OptionBoolTruncated:
		return NewAdbcError(
			fmt.Sprintf("[Databricks] `%s` property is read-only", key),
			adbc.StatusInvalidArgument,
		)
	default:
		return NewAdbcError(
			fmt.Sprintf("[Databricks] Unknown command string type option `%s`", key),
			adbc.StatusInvalidArgument,
		)
	}
}

// GetOption gets a string option from this statement.
func (cmd *command) GetOption(key string) (string, error) {
	switch key {
	case OptionBoolTruncated:
		// CommandExecution API doesn't have a truncated flag like StatementExecution
		return "false", nil
	default:
		val, err := cmd.conn.GetOption(key)
		if err == nil {
			return val, nil
		}
		return "", err
	}
}

// SetOptionInt sets an integer option on this statement.
func (cmd *command) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionIntRowLimit:
		// CommandExecution API doesn't have a row limit option
		return NewAdbcError(
			fmt.Sprintf("[Databricks] Row limit not supported for CommandExecution API"),
			adbc.StatusNotImplemented,
		)
	default:
		return NewAdbcError(
			fmt.Sprintf("[Databricks] Unknown command integer type option `%s`", key),
			adbc.StatusInvalidArgument,
		)
	}
}

// GetOptionInt gets an integer option from this statement.
func (cmd *command) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionIntRowLimit:
		// CommandExecution API doesn't have a row limit option
		return 0, nil
	default:
		val, err := cmd.conn.GetOptionInt(key)
		if err == nil {
			return val, nil
		}
		return 0, err
	}
}

// SetOptionBytes sets a bytes option on this statement.
func (cmd *command) SetOptionBytes(key string, value []byte) error {
	return NewAdbcError(
		fmt.Sprintf("[Databricks] Unknown command bytes type option '%s'", key),
		adbc.StatusNotImplemented,
	)
}

// GetOptionBytes gets a bytes option from this statement.
func (cmd *command) GetOptionBytes(key string) ([]byte, error) {
	return nil, NewAdbcError(
		fmt.Sprintf("[Databricks] Unknown command option '%s'", key),
		adbc.StatusNotFound,
	)
}

// SetOptionDouble sets a double option on this statement.
func (cmd *command) SetOptionDouble(key string, value float64) error {
	return NewAdbcError(
		fmt.Sprintf("[Databricks] Unknown command double type option '%s'", key),
		adbc.StatusNotImplemented,
	)
}

// GetOptionDouble gets a double option from this statement.
func (cmd *command) GetOptionDouble(key string) (float64, error) {
	return 0, NewAdbcError(
		fmt.Sprintf("[Databricks] Unknown command option '%s'", key),
		adbc.StatusNotFound,
	)
}

// SetSqlQuery sets the query string to be executed.
func (cmd *command) SetSqlQuery(query string) error {
	return cmd.resetCommand(query)
}

// ExecuteQuery executes the current query or prepared statement
// and returns a RecordReader for the results along with the number
// of rows affected if known, otherwise it will be -1.
func (cmd *command) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if cmd.req.Command == "" {
		return nil, -1, NewAdbcError(
			"ExecuteQuery called before SetSqlQuery",
			adbc.StatusInvalidState,
		)
	}
	reader, err := cmd.executeQueryInternal(ctx)
	if err != nil {
		return nil, -1, err
	}
	return reader, reader.TotalRowCount(), nil
}

func (cmd *command) executeQueryInternal(ctx context.Context) (*commandReader, error) {
	ce := cmd.conn.CommandExecution()
	executor, err := ce.Start(ctx, cmd.conn.client.Config.ClusterID, compute.LanguageSql)
	if err != nil {
		return nil, NewAdbcError(
			fmt.Sprintf("[Databricks] failed to start command execution: %s", err),
			adbc.StatusUnknown,
		)
	}

	res, err := executor.Execute(ctx, cmd.req.Command)
	if err != nil {
		return nil, NewAdbcError(
			fmt.Sprintf("[Databricks] failed to execute command: %s", err),
			adbc.StatusUnknown,
		)
	}

	for {
		switch res.ResultType {
		case compute.ResultTypeTable:
			// CommandExecution API doesn't have a ChunkIndex like StatementExecution
			return NewCommandRecordReader(ce, cmd.commandId, res)
		case compute.ResultTypeText:
			return NewCommandRecordReader(ce, cmd.commandId, res)
		case compute.ResultTypeError:
			return nil, NewAdbcError(
				fmt.Sprintf("[Databricks] command execution failed: %s", res.Error()),
				adbc.StatusUnknown,
			)
		default:
			return nil, NewAdbcError(
				fmt.Sprintf("[Databricks] Unexpected command result type: %s", res.ResultType),
				adbc.StatusInternal,
			)
		}
	}
}

// ExecuteUpdate executes a statement that does not generate a result
// set. It returns the number of rows affected if known, otherwise -1.
func (cmd *command) ExecuteUpdate(ctx context.Context) (int64, error) {
	if cmd.req.Command == "" {
		return -1, NewAdbcError(
			"ExecuteUpdate called before SetSqlQuery",
			adbc.StatusInvalidState,
		)
	}

	// For CommandExecution API, we can use ExecuteCommand and check if it returns a result
	ce := cmd.conn.CommandExecution()
	executor, err := ce.Start(ctx, cmd.conn.client.Config.ClusterID, compute.LanguageSql)
	if err != nil {
		return -1, NewAdbcError(
			fmt.Sprintf("[Databricks] failed to start command execution: %s", err),
			adbc.StatusUnknown,
		)
	}
	res, err := executor.Execute(ctx, cmd.req.Command)
	if err != nil {
		return -1, NewAdbcError(
			fmt.Sprintf("[Databricks] failed to execute command: %s", err),
			adbc.StatusUnknown,
		)
	}

	if res.ResultType == compute.ResultTypeError {
		return -1, NewAdbcError(
			fmt.Sprintf("[Databricks] command execution failed: %s", res.Error()),
			adbc.StatusUnknown,
		)
	}

	// For updates, we don't have a row count in the CommandExecution API
	return -1, nil
}

// Prepare turns this statement into a prepared statement to be executed
// multiple times. This invalidates any prior result sets.
func (cmd *command) Prepare(ctx context.Context) error {
	// CommandExecution API doesn't support prepared statements
	return nil
}

// SetSubstraitPlan sets a Substrait plan to be executed.
func (cmd *command) SetSubstraitPlan(plan []byte) error {
	return NewAdbcError(
		"Substrait not yet implemented for Databricks driver",
		adbc.StatusNotImplemented,
	)
}

// Bind uses an arrow record batch to bind parameters to the query.
func (cmd *command) Bind(ctx context.Context, values arrow.Record) error {
	return NewAdbcError(
		"Bind not yet implemented for Databricks driver",
		adbc.StatusNotImplemented,
	)
}

// BindStream uses a record batch stream to bind parameters for this
// query. This can be used for bulk inserts or prepared statements.
func (cmd *command) BindStream(ctx context.Context, stream array.RecordReader) error {
	return NewAdbcError(
		"BindStream not yet implemented for Databricks driver",
		adbc.StatusNotImplemented,
	)
}

// GetParameterSchema returns an Arrow schema representation of
// the expected parameters to be bound.
func (cmd *command) GetParameterSchema() (*arrow.Schema, error) {
	return nil, NewAdbcError(
		"GetParameterSchema not yet implemented for Databricks driver",
		adbc.StatusNotImplemented,
	)
}

// ExecutePartitions executes the current statement and gets the results
// as a partitioned result set.
func (cmd *command) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, NewAdbcError(
		"ExecutePartitions not yet implemented for Databricks driver",
		adbc.StatusNotImplemented,
	)
}

// Close releases any relevant resources associated with this statement
// and closes it (particularly if it is a prepared statement).
func (cmd *command) Close() error {
	if cmd.commandId == "" {
		return nil
	}
	ce := cmd.conn.CommandExecution()
	if ce == nil {
		return nil
	}
	ctx := context.TODO()
	req := compute.CancelCommand{
		ClusterId: cmd.conn.client.Config.ClusterID,

		CommandId: cmd.commandId,
		ContextId: cmd.req.ContextId,
	}
	cancelResult, err := ce.Cancel(ctx, req)
	if err != nil {
		return NewAdbcError(
			fmt.Sprintf("Failed to cancel command %s: %s", cmd.commandId, err),
			adbc.StatusUnknown,
		)
	}
	res, err := cancelResult.Get()
	if err != nil {
		return NewAdbcError(
			fmt.Sprintf("Failed to cancel command %s: %s", cmd.commandId, err),
			adbc.StatusUnknown,
		)
	}
	if res.Status != compute.CommandStatusCancelled {
		return NewAdbcError(
			fmt.Sprintf("Failed to cancel command %s: %s", cmd.commandId, res.Status),
			adbc.StatusUnknown,
		)
	}
	cmd.commandId = ""
	return nil
}

var _ adbc.Statement = (*command)(nil)
var _ adbc.GetSetOptions = (*command)(nil)
