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
	"context"
	"fmt"
	"strconv"
	"time"

	"cloud.google.com/go/bigquery"
	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
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
	connectionImpl *connectionImpl
	query          *bigquery.Query
	parameterMode  string
	paramBinding   arrow.Record
	streamBinding  array.RecordReader
}

// Close releases any relevant resources associated with this statement
// and closes it (particularly if it is a prepared statement).
//
// A statement instance should not be used after Close is called.
func (st *statement) Close() error {
	st.clearParameters()
	return nil
}

func (st *statement) GetOption(key string) (string, error) {
	switch key {
	case OptionStringProjectID:
		val, err := st.connectionImpl.GetOption(OptionStringProjectID)
		if err != nil {
			return "", err
		} else {
			return val, nil
		}
	case OptionStringQueryParameterMode:
		return st.parameterMode, nil
	case OptionStringQueryDestinationTable:
		return tableToString(st.query.QueryConfig.Dst), nil
	case OptionStringQueryDefaultProjectID:
		return st.query.QueryConfig.DefaultProjectID, nil
	case OptionStringQueryDefaultDatasetID:
		return st.query.QueryConfig.DefaultDatasetID, nil
	case OptionStringQueryCreateDisposition:
		return string(st.query.QueryConfig.CreateDisposition), nil
	case OptionStringQueryWriteDisposition:
		return string(st.query.QueryConfig.WriteDisposition), nil
	case OptionBoolQueryDisableQueryCache:
		return strconv.FormatBool(st.query.QueryConfig.DisableQueryCache), nil
	case OptionBoolDisableFlattenedResults:
		return strconv.FormatBool(st.query.QueryConfig.DisableFlattenedResults), nil
	case OptionBoolQueryAllowLargeResults:
		return strconv.FormatBool(st.query.QueryConfig.AllowLargeResults), nil
	case OptionStringQueryPriority:
		return string(st.query.QueryConfig.Priority), nil
	case OptionBoolQueryUseLegacySQL:
		return strconv.FormatBool(st.query.QueryConfig.UseLegacySQL), nil
	case OptionBoolQueryDryRun:
		return strconv.FormatBool(st.query.QueryConfig.DryRun), nil
	case OptionBoolQueryCreateSession:
		return strconv.FormatBool(st.query.QueryConfig.CreateSession), nil
	default:
		val, err := st.connectionImpl.GetOption(key)
		if err == nil {
			return val, nil
		}
		return "", err
	}
}

func (st *statement) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionIntQueryMaxBillingTier:
		return int64(st.query.QueryConfig.MaxBillingTier), nil
	case OptionIntQueryMaxBytesBilled:
		return st.query.QueryConfig.MaxBytesBilled, nil
	case OptionIntQueryJobTimeout:
		return st.query.QueryConfig.JobTimeout.Milliseconds(), nil
	default:
		val, err := st.connectionImpl.GetOptionInt(key)
		if err == nil {
			return val, nil
		}
		return 0, err
	}
}

func (st *statement) SetOption(key string, v string) error {
	switch key {
	case OptionStringQueryParameterMode:
		switch v {
		case OptionValueQueryParameterModeNamed, OptionValueQueryParameterModePositional:
			st.parameterMode = v
		default:
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("Parameter mode for the statement can only be either %s or %s", OptionValueQueryParameterModeNamed, OptionValueQueryParameterModePositional),
			}
		}
	case OptionStringQueryDestinationTable:
		val, err := stringToTable(v)
		if err == nil {
			st.query.QueryConfig.Dst = val
		} else {
			return err
		}
	case OptionStringQueryDefaultProjectID:
		st.query.QueryConfig.DefaultProjectID = v
	case OptionStringQueryDefaultDatasetID:
		st.query.QueryConfig.DefaultDatasetID = v
	case OptionStringQueryCreateDisposition:
		val, err := stringToTableCreateDisposition(v)
		if err == nil {
			st.query.QueryConfig.CreateDisposition = val
		} else {
			return err
		}
	case OptionStringQueryWriteDisposition:
		val, err := stringToTableWriteDisposition(v)
		if err == nil {
			st.query.QueryConfig.WriteDisposition = val
		} else {
			return err
		}
	case OptionBoolQueryDisableQueryCache:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.query.QueryConfig.DisableQueryCache = val
		} else {
			return err
		}
	case OptionBoolDisableFlattenedResults:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.query.QueryConfig.DisableFlattenedResults = val
		} else {
			return err
		}
	case OptionBoolQueryAllowLargeResults:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.query.QueryConfig.AllowLargeResults = val
		} else {
			return err
		}
	case OptionStringQueryPriority:
		val, err := stringToQueryPriority(v)
		if err == nil {
			st.query.QueryConfig.Priority = val
		} else {
			return err
		}
	case OptionBoolQueryUseLegacySQL:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.query.QueryConfig.UseLegacySQL = val
		} else {
			return err
		}
	case OptionBoolQueryDryRun:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.query.QueryConfig.DryRun = val
		} else {
			return err
		}
	case OptionBoolQueryCreateSession:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.query.QueryConfig.CreateSession = val
		} else {
			return err
		}

	default:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement string type option `%s`", key),
		}
	}
	return nil
}

func (st *statement) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionIntQueryMaxBillingTier:
		st.query.QueryConfig.MaxBillingTier = int(value)
	case OptionIntQueryMaxBytesBilled:
		st.query.QueryConfig.MaxBytesBilled = value
	case OptionIntQueryJobTimeout:
		st.query.QueryConfig.JobTimeout = time.Duration(value) * time.Millisecond
	default:
		return adbc.Error{
			Code: adbc.StatusInvalidArgument,
			Msg:  fmt.Sprintf("unknown statement string type option `%s`", key),
		}
	}
	return nil
}

// SetSqlQuery sets the query string to be executed.
//
// The query can then be executed with any of the Execute methods.
// For queries expected to be executed repeatedly, Prepare should be
// called before execution.
func (st *statement) SetSqlQuery(query string) error {
	st.query.QueryConfig.Q = query
	return nil
}

// ExecuteQuery executes the current query or prepared statement
// and returns a RecordReader for the results along with the number
// of rows affected if known, otherwise it will be -1.
//
// This invalidates any prior result sets on this statement.
func (st *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	parameters, err := st.getQueryParameter()
	if err != nil {
		return nil, 0, err
	}
	if parameters != nil {
		st.query.QueryConfig.Parameters = parameters
	}
	reader, affectedRows, err := newRecordReader(ctx, st.query, st.connectionImpl.Alloc)
	if err != nil {
		return nil, -1, err
	}
	return reader, affectedRows, nil
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

func arrowValueToQueryParameterValue(value arrow.Array) bigquery.QueryParameter {
	parameter := bigquery.QueryParameter{}
	if value.IsNull(0) {
		parameter.Value = &bigquery.QueryParameterValue{
			Type: bigquery.StandardSQLDataType{
				TypeKind: "NULL",
			},
			Value: "NULL",
		}
		return parameter
	}

	// https://cloud.google.com/bigquery/docs/reference/storage#arrow_schema_details
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/StandardSqlDataType#typekind
	switch value.DataType().ID() {
	case arrow.BOOL:
		// GoogleSQL type: BOOLEAN
		parameter.Value = &bigquery.QueryParameterValue{
			Type: bigquery.StandardSQLDataType{
				TypeKind: "BOOL",
			},
			Value: value.ValueStr(0),
		}
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		parameter.Value = &bigquery.QueryParameterValue{
			Type: bigquery.StandardSQLDataType{
				TypeKind: "INT64",
			},
			Value: value.ValueStr(0),
		}
	case arrow.FLOAT32, arrow.FLOAT64:
		parameter.Value = &bigquery.QueryParameterValue{
			Type: bigquery.StandardSQLDataType{
				TypeKind: "FLOAT64",
			},
			Value: value.ValueStr(0),
		}
	case arrow.BINARY, arrow.BINARY_VIEW, arrow.LARGE_BINARY:
		// todo: Encoded as a base64 string per RFC 4648, section 4.
		parameter.Value = &bigquery.QueryParameterValue{
			Type: bigquery.StandardSQLDataType{
				TypeKind: "BYTES",
			},
			Value: value.ValueStr(0),
		}
	case arrow.STRING, arrow.STRING_VIEW, arrow.LARGE_STRING:
		parameter.Value = &bigquery.QueryParameterValue{
			Type: bigquery.StandardSQLDataType{
				TypeKind: "STRING",
			},
			Value: value.ValueStr(0),
		}
	case arrow.DATE32, arrow.DATE64:
		// todo: Encoded as RFC 3339 full-date format string: 1985-04-12
		parameter.Value = &bigquery.QueryParameterValue{
			Type: bigquery.StandardSQLDataType{
				TypeKind: "DATE",
			},
			Value: value.ValueStr(0),
		}
	case arrow.TIMESTAMP:
		// todo: Encoded as an RFC 3339 timestamp with mandatory "Z" time zone string: 1985-04-12T23:20:50.52Z
		parameter.Value = &bigquery.QueryParameterValue{
			Type: bigquery.StandardSQLDataType{
				TypeKind: "TIMESTAMP",
			},
			Value: value.ValueStr(0),
		}
	case arrow.TIME32, arrow.TIME64:
		// todo: Encoded as RFC 3339 partial-time format string: 23:20:50.52
		parameter.Value = &bigquery.QueryParameterValue{
			Type: bigquery.StandardSQLDataType{
				TypeKind: "TIME",
			},
			Value: value.ValueStr(0),
		}
	case arrow.DECIMAL128:
		parameter.Value = &bigquery.QueryParameterValue{
			Type: bigquery.StandardSQLDataType{
				TypeKind: "NUMERIC",
			},
			Value: value.ValueStr(0),
		}
	case arrow.DECIMAL256:
		parameter.Value = &bigquery.QueryParameterValue{
			Type: bigquery.StandardSQLDataType{
				TypeKind: "BIGNUMERIC",
			},
			Value: value.ValueStr(0),
		}
	}

	return parameter
}

func (st *statement) getQueryParameter() ([]bigquery.QueryParameter, error) {
	var values arrow.Record
	if st.paramBinding != nil {
		values = st.paramBinding
	} else if st.streamBinding != nil {
		if st.streamBinding.Next() {
			values = st.streamBinding.Record()
		} else {
			return nil, adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  "Cannot bind query parameters: stream has no next record",
			}
		}
	} else {
		return nil, nil
	}

	parameters := make([]bigquery.QueryParameter, values.NumCols())
	includeName := st.parameterMode == OptionValueQueryParameterModeNamed
	for i, v := range values.Columns() {
		parameters[i] = arrowValueToQueryParameterValue(v)
		if includeName {
			parameters[i].Name = values.ColumnName(i)
		}
	}
	return parameters, nil
}

func (st *statement) clearParameters() {
	if st.paramBinding != nil {
		st.paramBinding.Release()
		st.paramBinding = nil
	}
	if st.streamBinding != nil {
		st.streamBinding.Release()
		st.streamBinding = nil
	}
}

// SetParameters takes a record batch to send as the parameter bindings when
// executing. It should match the schema from ParameterSchema.
//
// This will call Retain on the record to ensure it doesn't get released out
// from under the statement. Release will be called on a previous binding
// record or reader if it existed, and will be called upon calling Close on the
// PreparedStatement.
func (st *statement) SetParameters(binding arrow.Record) {
	st.clearParameters()
	st.paramBinding = binding
	if st.paramBinding != nil {
		st.paramBinding.Retain()
	}
}

// SetRecordReader takes a RecordReader to send as the parameter bindings when
// executing. It should match the schema from ParameterSchema.
//
// This will call Retain on the reader to ensure it doesn't get released out
// from under the statement. Release will be called on a previous binding
// record or reader if it existed, and will be called upon calling Close on the
// PreparedStatement.
func (st *statement) SetRecordReader(binding array.RecordReader) {
	st.clearParameters()
	binding.Retain()
	st.streamBinding = binding
	st.streamBinding.Retain()
}

// Bind uses an arrow record batch to bind parameters to the query.
//
// This can be used for bulk inserts or for prepared statements.
// The driver will call release on the passed in Record when it is done,
// but it may not do this until the statement is closed or another
// record is bound.
func (st *statement) Bind(_ context.Context, values arrow.Record) error {
	st.SetParameters(values)
	return nil
}

// BindStream uses a record batch stream to bind parameters for this
// query. This can be used for bulk inserts or prepared statements.
//
// The driver will call Release on the record reader, but may not do this
// until Close is called.
func (st *statement) BindStream(_ context.Context, stream array.RecordReader) error {
	st.SetRecordReader(stream)
	return nil
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
