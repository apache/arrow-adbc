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
	"database/sql"
	"database/sql/driver"
	"errors"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"

	dbsqlrows "github.com/databricks/databricks-sql-go/rows"
)

type statementImpl struct {
	conn       *connectionImpl
	query      string
	parameters []interface{}
	prepared   *sql.Stmt
}

func (s *statementImpl) Close() error {
	if s.conn == nil {
		return adbc.Error{
			Msg:  "statement already closed",
			Code: adbc.StatusInvalidState,
		}
	}
	if s.prepared != nil {
		return s.prepared.Close()
	}
	s.conn = nil
	return nil
}

func (s *statementImpl) SetOption(key, val string) error {
	// No statement-specific options are supported yet
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  fmt.Sprintf("unsupported statement option: %s", key),
	}
}

func (s *statementImpl) SetSqlQuery(query string) error {
	s.query = query
	// Reset prepared statement if query changes
	if s.prepared != nil {
		if err := s.prepared.Close(); err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidState,
				Msg:  fmt.Sprintf("failed to close previous prepared statement: %v", err),
			}
		}
		s.prepared = nil
	}
	return nil
}

func (s *statementImpl) Prepare(ctx context.Context) error {
	if s.query == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}

	stmt, err := s.conn.conn.PrepareContext(ctx, s.query)
	if err != nil {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  fmt.Sprintf("failed to prepare statement: %v", err),
		}
	}

	s.prepared = stmt
	return nil
}

func (s *statementImpl) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	// TODO: Prepared statement support with raw connections
	if s.prepared != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  "Prepared statements are not yet supported via `execute query`",
		}
	}

	if s.query == "" {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}

	// Execute query using raw driver interface to get Arrow batches
	var driverRows driver.Rows
	var err error
	err = s.conn.conn.Raw(func(driverConn interface{}) error {
		// Use raw driver interface for direct Arrow access
		// Convert parameters to driver.NamedValue slice
		queryerCtx := driverConn.(driver.QueryerContext)
		var driverArgs []driver.NamedValue
		for i, param := range s.parameters {
			driverArgs = append(driverArgs, driver.NamedValue{
				Ordinal: i + 1,
				Value:   param,
			})
		}
		driverRows, err = queryerCtx.QueryContext(ctx, s.query, driverArgs)
		return err
	})

	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to execute query: %v", err),
		}
	}

	defer func() {
		if closeErr := driverRows.Close(); closeErr != nil {
			err = errors.Join(err, closeErr)
		}
	}()

	// Convert to databricks rows interface to get Arrow batches
	databricksRows, ok := driverRows.(dbsqlrows.Rows)
	if !ok {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  "driver rows do not support Arrow batches",
		}
	}

	// Use the IPC stream interface (zero-copy)
	reader, err := newIPCReaderAdapter(ctx, databricksRows)
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to create IPC reader adapter: %v", err),
		}
	}

	// Return -1 for rowsAffected (unknown) since we can't count without consuming
	// The ADBC spec allows -1 to indicate "unknown number of rows affected"
	return reader, -1, nil
}

func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	var result sql.Result
	var err error

	if s.prepared != nil {
		result, err = s.prepared.ExecContext(ctx, s.parameters...)
	} else if s.query != "" {
		result, err = s.conn.conn.ExecContext(ctx, s.query, s.parameters...)
	} else {
		return -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}

	if err != nil {
		return -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to execute update: %v", err),
		}
	}

	rowsAffected, err := result.RowsAffected()
	if err != nil {
		return -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to get rows affected: %v", err),
		}
	}

	return rowsAffected, nil
}

func (s *statementImpl) Bind(ctx context.Context, values arrow.Record) error {
	return adbc.Error{
		Msg:  "Bind not yet implemented for Databricks driver",
		Code: adbc.StatusNotImplemented,
	}
}

func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	return adbc.Error{
		Msg:  "Bind not yet implemented for Databricks driver",
		Code: adbc.StatusNotImplemented,
	}
}

func (s *statementImpl) GetParameterSchema() (*arrow.Schema, error) {
	// This would require parsing the SQL query to determine parameter types
	// For now, return nil to indicate unknown schema
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "parameter schema detection not implemented",
	}
}

func (s *statementImpl) SetSubstraitPlan(plan []byte) error {
	// Databricks SQL doesn't support Substrait plans
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Substrait plans not supported",
	}
}

func (s *statementImpl) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	// Databricks SQL doesn't support partitioned result sets
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "partitioned result sets not supported",
	}
}
