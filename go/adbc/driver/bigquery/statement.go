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

package bigquery

import (
	"cloud.google.com/go/bigquery"
	"context"
	"fmt"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
	"time"
)

// todos for bigquery.QueryConfig
// - TableDefinitions
// - Parameters
// - TimePartitioning
// - RangePartitioning
// - Clustering
// - Labels
// - DestinationEncryptionConfig
// - SchemaUpdateOptions
// - ConnectionProperties

type statement struct {
	ConnectionImpl *ConnectionImpl
	query          string
	queryConfig    bigquery.QueryConfig
	projectID      *string

	alloc memory.Allocator
}

// Close releases any relevant resources associated with this statement
// and closes it (particularly if it is a prepared statement).
//
// A statement instance should not be used after Close is called.
func (st *statement) Close() error {
	return nil
}

func (st *statement) GetOption(key string) (string, error) {
	switch key {
	case OptionStringProjectID:
		if st.projectID == nil {
			val, err := st.ConnectionImpl.GetOption(OptionStringProjectID)
			if err != nil {
				return "", err
			} else {
				return val, nil
			}
		} else {
			return *st.projectID, nil
		}
	case OptionStringQueryDestinationTable:
		return tableToString(st.queryConfig.Dst), nil
	case OptionStringQueryDefaultProjectID:
		return st.queryConfig.DefaultProjectID, nil
	case OptionStringQueryDefaultDatasetID:
		return st.queryConfig.DefaultDatasetID, nil
	case OptionStringQueryCreateDisposition:
		return tableCreateDispositionToString(st.queryConfig.CreateDisposition), nil
	case OptionStringQueryWriteDisposition:
		return tableWriteDispositionToString(st.queryConfig.WriteDisposition), nil
	case OptionBoolQueryDisableQueryCache:
		return boolToString(st.queryConfig.DisableQueryCache), nil
	case OptionBoolDisableFlattenedResults:
		return boolToString(st.queryConfig.DisableFlattenedResults), nil
	case OptionBoolQueryAllowLargeResults:
		return boolToString(st.queryConfig.AllowLargeResults), nil
	case OptionStringQueryPriority:
		return queryPriorityToString(st.queryConfig.Priority), nil
	case OptionBoolQueryUseLegacySQL:
		return boolToString(st.queryConfig.UseLegacySQL), nil
	case OptionBoolQueryDryRun:
		return boolToString(st.queryConfig.DryRun), nil
	case OptionBoolQueryCreateSession:
		return boolToString(st.queryConfig.CreateSession), nil
	default:
		val, err := st.ConnectionImpl.GetOption(key)
		if err == nil {
			return val, nil
		}
		return "", adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement string type option `%s`", key),
		}
	}
}

func (st *statement) GetOptionBytes(key string) ([]byte, error) {
	switch key {
	default:
		val, err := st.ConnectionImpl.GetOptionBytes(key)
		if err == nil {
			return val, nil
		}
		return nil, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement bytes type option `%s`", key),
		}
	}
}

func (st *statement) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionIntQueryMaxBillingTier:
		return int64(st.queryConfig.MaxBillingTier), nil
	case OptionIntQueryMaxBytesBilled:
		return st.queryConfig.MaxBytesBilled, nil
	case OptionIntQueryJobTimeout:
		return st.queryConfig.JobTimeout.Milliseconds(), nil
	default:
		val, err := st.ConnectionImpl.GetOptionInt(key)
		if err == nil {
			return val, nil
		}
		return 0, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement int type option `%s`", key),
		}
	}
}

func (st *statement) GetOptionDouble(key string) (float64, error) {
	switch key {
	default:
		val, err := st.ConnectionImpl.GetOptionDouble(key)
		if err == nil {
			return val, nil
		}
		return 0, adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement double type option `%s`", key),
		}
	}
}

func (st *statement) setQueryConfig(key, v string) (bool, error) {
	switch key {
	case OptionStringQueryDestinationTable:
		val, err := stringToTable(v)
		if err == nil {
			st.queryConfig.Dst = val
		} else {
			return true, err
		}
	case OptionStringQueryDefaultProjectID:
		st.queryConfig.DefaultProjectID = v
	case OptionStringQueryDefaultDatasetID:
		st.queryConfig.DefaultDatasetID = v
	case OptionStringQueryCreateDisposition:
		val, err := stringToTableCreateDisposition(v)
		if err == nil {
			st.queryConfig.CreateDisposition = val
		} else {
			return true, err
		}
	case OptionStringQueryWriteDisposition:
		val, err := stringToTableWriteDisposition(v)
		if err == nil {
			st.queryConfig.WriteDisposition = val
		} else {
			return true, err
		}
	case OptionBoolQueryDisableQueryCache:
		val, err := stringToBool(v)
		if err == nil {
			st.queryConfig.DisableQueryCache = val
		} else {
			return true, err
		}
	case OptionBoolDisableFlattenedResults:
		val, err := stringToBool(v)
		if err == nil {
			st.queryConfig.DisableFlattenedResults = val
		} else {
			return true, err
		}
	case OptionBoolQueryAllowLargeResults:
		val, err := stringToBool(v)
		if err == nil {
			st.queryConfig.AllowLargeResults = val
		} else {
			return true, err
		}
	case OptionStringQueryPriority:
		val, err := stringToQueryPriority(v)
		if err == nil {
			st.queryConfig.Priority = val
		} else {
			return true, err
		}
	case OptionBoolQueryUseLegacySQL:
		val, err := stringToBool(v)
		if err == nil {
			st.queryConfig.UseLegacySQL = val
		} else {
			return true, err
		}
	case OptionBoolQueryDryRun:
		val, err := stringToBool(v)
		if err == nil {
			st.queryConfig.DryRun = val
		} else {
			return true, err
		}
	case OptionBoolQueryCreateSession:
		val, err := stringToBool(v)
		if err == nil {
			st.queryConfig.CreateSession = val
		} else {
			return true, err
		}

	default:
		return false, nil
	}
	return true, nil
}

func (st *statement) setQueryConfigInt(key string, value int64) (bool, error) {
	switch key {
	case OptionIntQueryMaxBillingTier:
		st.queryConfig.MaxBillingTier = int(value)
	case OptionIntQueryMaxBytesBilled:
		st.queryConfig.MaxBytesBilled = value
	case OptionIntQueryJobTimeout:
		st.queryConfig.JobTimeout = time.Duration(value) * time.Millisecond
	default:
		return false, nil
	}
	return true, nil
}

func (st *statement) SetOption(key string, value string) error {
	handled, err := st.setQueryConfig(key, value)
	if handled {
		if err != nil {
			return err
		} else {
			return nil
		}
	}

	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("unknown statement string type option `%s`", key),
	}
}

func (st *statement) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("unknown statement bytes type option `%s`", key),
	}
}

func (st *statement) SetOptionInt(key string, value int64) error {
	handled, err := st.setQueryConfigInt(key, value)
	if handled {
		if err != nil {
			return err
		} else {
			return nil
		}
	}

	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("unknown statement int type option `%s`", key),
	}
}

func (st *statement) SetOptionDouble(key string, value float64) error {
	return adbc.Error{
		Code: adbc.StatusInvalidArgument,
		Msg:  fmt.Sprintf("unknown statement double type option `%s`", key),
	}
}

// SetSqlQuery sets the query string to be executed.
//
// The query can then be executed with any of the Execute methods.
// For queries expected to be executed repeatedly, Prepare should be
// called before execution.
func (st *statement) SetSqlQuery(query string) error {
	st.query = query
	return nil
}

// ExecuteQuery executes the current query or prepared statement
// and returns a RecordReader for the results along with the number
// of rows affected if known, otherwise it will be -1.
//
// This invalidates any prior result sets on this statement.
func (st *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	projectID, err := st.GetOption(OptionStringProjectID)
	if err != nil {
		return nil, -1, err
	}
	authType, err := st.GetOption(OptionStringAuthType)
	if err != nil {
		return nil, -1, err
	}
	credentials, err := st.GetOption(OptionStringCredentials)
	if err != nil {
		return nil, -1, err
	}
	client, err := newClient(ctx, projectID, authType, credentials)
	if err != nil {
		return nil, -1, err
	}

	reader, err := newRecordReader(ctx, client, projectID, st.query, st.queryConfig, st.alloc)
	if err != nil {
		return nil, -1, err
	}
	return reader, -1, nil
}

// ExecuteUpdate executes a statement that does not generate a result
// set. It returns the number of rows affected if known, otherwise -1.
func (st *statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	return -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteUpdate not yet implemented for BigQuery driver",
	}
}

// ExecuteSchema gets the schema of the result set of a query without executing it.
func (st *statement) ExecuteSchema(ctx context.Context) (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecuteSchema not yet implemented for BigQuery driver",
	}
}

// Prepare turns this statement into a prepared statement to be executed
// multiple times. This invalidates any prior result sets.
func (st *statement) Prepare(_ context.Context) error {
	return nil
}

// SetSubstraitPlan allows setting a serialized Substrait execution
// plan into the query or for querying Substrait-related metadata.
//
// Drivers are not required to support both SQL and Substrait semantics.
// If they do, it may be via converting between representations internally.
//
// Like SetSqlQuery, after this is called the query can be executed
// using any of the Execute methods. If the query is expected to be
// executed repeatedly, Prepare should be called first on the statement.
func (st *statement) SetSubstraitPlan(plan []byte) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Substrait not yet implemented for BigQuery driver",
	}
}

// Bind uses an arrow record batch to bind parameters to the query.
//
// This can be used for bulk inserts or for prepared statements.
// The driver will call release on the passed in Record when it is done,
// but it may not do this until the statement is closed or another
// record is bound.
func (st *statement) Bind(_ context.Context, values arrow.Record) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "Bind not yet implemented for BigQuery driver",
	}
}

// BindStream uses a record batch stream to bind parameters for this
// query. This can be used for bulk inserts or prepared statements.
//
// The driver will call Release on the record reader, but may not do this
// until Close is called.
func (st *statement) BindStream(_ context.Context, stream array.RecordReader) error {
	return adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "BindStream not yet implemented for BigQuery driver",
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
func (st *statement) GetParameterSchema() (*arrow.Schema, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "GetParameterSchema not yet implemented for BigQuery driver",
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
func (st *statement) ExecutePartitions(ctx context.Context) (*arrow.Schema, adbc.Partitions, int64, error) {
	return nil, adbc.Partitions{}, -1, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ExecutePartitions not yet implemented for BigQuery driver",
	}
}
