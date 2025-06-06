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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// todos for bigqueryConfig
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
	alloc memory.Allocator
	cnxn  *connectionImpl

	queryConfig            bigquery.QueryConfig
	parameterMode          string
	paramBinding           arrow.Record
	streamBinding          array.RecordReader
	resultRecordBufferSize int
	prefetchConcurrency    int
}

func (st *statement) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Msg:  fmt.Sprintf("[BigQuery] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

func (st *statement) GetOptionDouble(key string) (float64, error) {
	return 0, adbc.Error{
		Msg:  fmt.Sprintf("[BigQuery] Unknown statement option '%s'", key),
		Code: adbc.StatusNotFound,
	}
}

func (st *statement) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[BigQuery] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

func (st *statement) SetOptionDouble(key string, value float64) error {
	return adbc.Error{
		Msg:  fmt.Sprintf("[BigQuery] Unknown statement option '%s'", key),
		Code: adbc.StatusNotImplemented,
	}
}

// Close releases any relevant resources associated with this statement
// and closes it (particularly if it is a prepared statement).
//
// A statement instance should not be used after Close is called.
func (st *statement) Close() error {
	if st.cnxn == nil {
		return adbc.Error{
			Msg:  "statement already closed",
			Code: adbc.StatusInvalidState}
	}

	st.clearParameters()
	st.cnxn = nil
	return nil
}

func (st *statement) GetOption(key string) (string, error) {
	switch key {
	case OptionStringProjectID:
		val, err := st.cnxn.GetOption(OptionStringProjectID)
		if err != nil {
			return "", err
		} else {
			return val, nil
		}
	case OptionStringQueryParameterMode:
		return st.parameterMode, nil
	case OptionStringQueryDestinationTable:
		return tableToString(st.queryConfig.Dst), nil
	case OptionStringQueryDefaultProjectID:
		return st.queryConfig.DefaultProjectID, nil
	case OptionStringQueryDefaultDatasetID:
		return st.queryConfig.DefaultDatasetID, nil
	case OptionStringQueryCreateDisposition:
		return string(st.queryConfig.CreateDisposition), nil
	case OptionStringQueryWriteDisposition:
		return string(st.queryConfig.WriteDisposition), nil
	case OptionBoolQueryDisableQueryCache:
		return strconv.FormatBool(st.queryConfig.DisableQueryCache), nil
	case OptionBoolDisableFlattenedResults:
		return strconv.FormatBool(st.queryConfig.DisableFlattenedResults), nil
	case OptionBoolQueryAllowLargeResults:
		return strconv.FormatBool(st.queryConfig.AllowLargeResults), nil
	case OptionStringQueryPriority:
		return string(st.queryConfig.Priority), nil
	case OptionBoolQueryUseLegacySQL:
		return strconv.FormatBool(st.queryConfig.UseLegacySQL), nil
	case OptionBoolQueryDryRun:
		return strconv.FormatBool(st.queryConfig.DryRun), nil
	case OptionBoolQueryCreateSession:
		return strconv.FormatBool(st.queryConfig.CreateSession), nil
	default:
		val, err := st.cnxn.GetOption(key)
		if err == nil {
			return val, nil
		}
		return "", err
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
	case OptionIntQueryResultBufferSize:
		return int64(st.resultRecordBufferSize), nil
	case OptionIntQueryPrefetchConcurrency:
		return int64(st.prefetchConcurrency), nil
	default:
		val, err := st.cnxn.GetOptionInt(key)
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
		val, err := stringToTable(st.cnxn.catalog, st.cnxn.dbSchema, v)
		if err == nil {
			st.queryConfig.Dst = val
		} else {
			return err
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
			return err
		}
	case OptionStringQueryWriteDisposition:
		val, err := stringToTableWriteDisposition(v)
		if err == nil {
			st.queryConfig.WriteDisposition = val
		} else {
			return err
		}
	case OptionBoolQueryDisableQueryCache:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.queryConfig.DisableQueryCache = val
		} else {
			return err
		}
	case OptionBoolDisableFlattenedResults:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.queryConfig.DisableFlattenedResults = val
		} else {
			return err
		}
	case OptionBoolQueryAllowLargeResults:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.queryConfig.AllowLargeResults = val
		} else {
			return err
		}
	case OptionStringQueryPriority:
		val, err := stringToQueryPriority(v)
		if err == nil {
			st.queryConfig.Priority = val
		} else {
			return err
		}
	case OptionBoolQueryUseLegacySQL:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.queryConfig.UseLegacySQL = val
		} else {
			return err
		}
	case OptionBoolQueryDryRun:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.queryConfig.DryRun = val
		} else {
			return err
		}
	case OptionBoolQueryCreateSession:
		val, err := strconv.ParseBool(v)
		if err == nil {
			st.queryConfig.CreateSession = val
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
		st.queryConfig.MaxBillingTier = int(value)
	case OptionIntQueryMaxBytesBilled:
		st.queryConfig.MaxBytesBilled = value
	case OptionIntQueryJobTimeout:
		st.queryConfig.JobTimeout = time.Duration(value) * time.Millisecond
	case OptionIntQueryResultBufferSize:
		st.resultRecordBufferSize = int(value)
		return nil
	case OptionIntQueryPrefetchConcurrency:
		st.prefetchConcurrency = int(value)
		return nil
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
	st.queryConfig.Q = query
	return nil
}

// ExecuteQuery executes the current query or prepared statement
// and returns a RecordReader for the results along with the number
// of rows affected if known, otherwise it will be -1.
//
// This invalidates any prior result sets on this statement.
func (st *statement) ExecuteQuery(ctx context.Context) (array.RecordReader, int64, error) {
	if st.queryConfig.Q == "" {
		return nil, -1, adbc.Error{
			Msg:  "cannot execute without a query",
			Code: adbc.StatusInvalidState,
		}
	}

	rdr, err := st.getBoundParameterReader()
	if err != nil {
		return nil, -1, err
	}

	return newRecordReader(ctx, st.query(), rdr, st.parameterMode, st.cnxn.Alloc, st.resultRecordBufferSize, st.prefetchConcurrency)
}

// ExecuteUpdate executes a statement that does not generate a result
// set. It returns the number of rows affected if known, otherwise -1.
func (st *statement) ExecuteUpdate(ctx context.Context) (int64, error) {
	boundParameters, err := st.getBoundParameterReader()
	if err != nil {
		return -1, err
	}

	if boundParameters == nil {
		_, totalRows, err := runQuery(ctx, st.query(), true)
		if err != nil {
			return -1, err
		}
		return totalRows, nil
	} else {
		totalRows := int64(0)
		for boundParameters.Next() {
			values := boundParameters.Record()
			for i := 0; i < int(values.NumRows()); i++ {
				parameters, err := getQueryParameter(values, i, st.parameterMode)
				if err != nil {
					return -1, err
				}
				if parameters != nil {
					st.queryConfig.Parameters = parameters
				}

				_, currentRows, err := runQuery(ctx, st.query(), true)
				if err != nil {
					return -1, err
				}
				totalRows += currentRows
			}
		}
		return totalRows, nil
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
	if st.queryConfig.Q == "" {
		return adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "cannot prepare statement with no query",
		}
	}
	// bigquery doesn't provide a "Prepare" api, this is a no-op
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

func (st *statement) query() *bigquery.Query {
	query := st.cnxn.client.Query("")
	query.QueryConfig = st.queryConfig
	return query
}

func arrowDataTypeToTypeKind(field arrow.Field, value arrow.Array) (bigquery.StandardSQLDataType, error) {
	// https://cloud.google.com/bigquery/docs/reference/storage#arrow_schema_details
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/StandardSqlDataType#typekind
	switch value.DataType().ID() {
	case arrow.BOOL:
		return bigquery.StandardSQLDataType{
			TypeKind: "BOOL",
		}, nil
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		return bigquery.StandardSQLDataType{
			TypeKind: "INT64",
		}, nil
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		return bigquery.StandardSQLDataType{
			TypeKind: "FLOAT64",
		}, nil
	case arrow.BINARY, arrow.BINARY_VIEW, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
		return bigquery.StandardSQLDataType{
			TypeKind: "BYTES",
		}, nil
	case arrow.STRING, arrow.STRING_VIEW, arrow.LARGE_STRING:
		return bigquery.StandardSQLDataType{
			TypeKind: "STRING",
		}, nil
	case arrow.DATE32, arrow.DATE64:
		return bigquery.StandardSQLDataType{
			TypeKind: "DATE",
		}, nil
	case arrow.TIMESTAMP:
		return bigquery.StandardSQLDataType{
			TypeKind: "TIMESTAMP",
		}, nil
	case arrow.TIME32, arrow.TIME64:
		return bigquery.StandardSQLDataType{
			TypeKind: "TIME",
		}, nil
	case arrow.DECIMAL128:
		return bigquery.StandardSQLDataType{
			TypeKind: "NUMERIC",
		}, nil
	case arrow.DECIMAL256:
		return bigquery.StandardSQLDataType{
			TypeKind: "BIGNUMERIC",
		}, nil
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST, arrow.LIST_VIEW, arrow.LARGE_LIST_VIEW:
		elemField := field.Type.(*arrow.ListType).ElemField()
		elemType, err := arrowDataTypeToTypeKind(elemField, value.(*array.List).ListValues())
		if err != nil {
			return bigquery.StandardSQLDataType{}, err
		}
		return bigquery.StandardSQLDataType{
			TypeKind:         "ARRAY",
			ArrayElementType: &elemType,
		}, nil
	case arrow.STRUCT:
		numFields := value.(*array.Struct).NumField()
		structType := bigquery.StandardSQLStructType{
			Fields: make([]*bigquery.StandardSQLField, 0),
		}
		for i := 0; i < numFields; i++ {
			currentField := field.Type.(*arrow.StructType).Field(i)
			currentFieldArray := value.(*array.Struct).Field(i)
			childType, err := arrowDataTypeToTypeKind(currentField, currentFieldArray)
			if err != nil {
				return bigquery.StandardSQLDataType{}, err
			}
			sqlField := bigquery.StandardSQLField{
				Name: currentField.Name,
				Type: &childType,
			}
			structType.Fields = append(structType.Fields, &sqlField)
		}
		return bigquery.StandardSQLDataType{
			TypeKind:   "STRUCT",
			StructType: &structType,
		}, nil
	default:
		// todo: implement all other types
		//
		// - arrow.DURATION
		//   For arrow.DURATION, I'm not sure which SQL DataType would be a good
		//   representation for it. `DATETIME` could be a potential one for it,
		//   if we count from `0000-01-01T00:00:00.000000Z`
		//
		// - arrow.INTERVAL_MONTHS
		// - arrow.INTERVAL_DAY_TIME
		// - arrow.INTERVAL_MONTH_DAY_NANO
		//   `DATETIME` could be a potential fit for all interval types, but
		//   the issue is there's no rules about how many days are in a month.
		//
		// - arrow.RUN_END_ENCODED
		// - arrow.SPARSE_UNION
		// - arrow.DENSE_UNION
		// - arrow.DICTIONARY
		// - arrow.MAP
		return bigquery.StandardSQLDataType{}, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("Parameter type %v is not yet implemented for BigQuery driver", value.DataType().ID()),
		}
	}
}

func arrowValueToQueryParameterValue(field arrow.Field, value arrow.Array, i int) (bigquery.QueryParameter, error) {
	// https://cloud.google.com/bigquery/docs/reference/storage#arrow_schema_details
	// https://cloud.google.com/bigquery/docs/reference/rest/v2/StandardSqlDataType#typekind
	parameter := bigquery.QueryParameter{}
	sqlDataType, err := arrowDataTypeToTypeKind(field, value)
	if err != nil {
		return bigquery.QueryParameter{}, err
	}
	if value.IsNull(i) {
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: "NULL",
		}
		return parameter, nil
	}
	switch value.DataType().ID() {
	case arrow.BOOL:
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: value.ValueStr(i),
		}
	case arrow.INT8, arrow.INT16, arrow.INT32, arrow.INT64, arrow.UINT8, arrow.UINT16, arrow.UINT32, arrow.UINT64:
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: value.ValueStr(i),
		}
	case arrow.FLOAT16, arrow.FLOAT32, arrow.FLOAT64:
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: value.ValueStr(i),
		}
	case arrow.BINARY, arrow.BINARY_VIEW, arrow.LARGE_BINARY, arrow.FIXED_SIZE_BINARY:
		// Encoded as a base64 string per RFC 4648, section 4.
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: value.ValueStr(i),
		}
	case arrow.STRING, arrow.STRING_VIEW, arrow.LARGE_STRING:
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: value.ValueStr(i),
		}
	case arrow.DATE32:
		// Encoded as RFC 3339 full-date format string: 1985-04-12
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: value.ValueStr(i),
		}
	case arrow.DATE64:
		// Encoded as RFC 3339 full-date format string: 1985-04-12
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: value.ValueStr(i),
		}
	case arrow.TIMESTAMP:
		// Encoded as an RFC 3339 timestamp with mandatory "Z" time zone string: 1985-04-12T23:20:50.52Z
		// BigQuery can only do microsecond resolution
		toTime, _ := value.DataType().(*arrow.TimestampType).GetToTimeFunc()
		encoded := toTime(value.(*array.Timestamp).Value(i)).Format("2006-01-02T15:04:05.999999Z07:00")
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: encoded,
		}
	case arrow.TIME32:
		// Encoded as RFC 3339 partial-time format string: 23:20:50.52
		encoded := value.(*array.Time32).Value(i).FormattedString(value.DataType().(*arrow.Time32Type).Unit)
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: encoded,
		}
	case arrow.TIME64:
		// Encoded as RFC 3339 partial-time format string: 23:20:50.52
		//
		// cannot use the default format, which will cause errors like
		//   googleapi: Error 400: Unparsable query parameter `` in type `TYPE_TIME`,
		//   Invalid time string "00:00:00.000000001" value: '00:00:00.000000001', invalid
		encoded := value.(*array.Time64).Value(i).FormattedString(arrow.Microsecond)
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: encoded,
		}
	case arrow.DECIMAL128:
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: value.ValueStr(i),
		}
	case arrow.DECIMAL256:
		parameter.Value = &bigquery.QueryParameterValue{
			Type:  sqlDataType,
			Value: value.ValueStr(i),
		}
	case arrow.LIST, arrow.FIXED_SIZE_LIST, arrow.LIST_VIEW:
		start, end := value.(*array.List).ValueOffsets(i)
		elemField := field.Type.(*arrow.ListType).ElemField()
		arrayValues := make([]bigquery.QueryParameterValue, end-start)
		for row := start; row < end; row++ {
			pv, err := arrowValueToQueryParameterValue(elemField, value.(*array.List).ListValues(), int(row))
			if err != nil {
				return bigquery.QueryParameter{}, err
			}
			arrayValues[row-start].Value = pv.Value
		}

		parameter.Value = &bigquery.QueryParameterValue{
			Type:       sqlDataType,
			ArrayValue: arrayValues,
		}
	case arrow.LARGE_LIST_VIEW:
		start, end := value.(*array.LargeListView).ValueOffsets(i)
		elemField := field.Type.(*arrow.LargeListType).ElemField()
		arrayValues := make([]bigquery.QueryParameterValue, end-start)
		for row := start; row < end; row++ {
			pv, err := arrowValueToQueryParameterValue(elemField, value.(*array.LargeListView).ListValues(), int(row))
			if err != nil {
				return bigquery.QueryParameter{}, err
			}
			arrayValues[row-start].Value = pv.Value
		}

		parameter.Value = &bigquery.QueryParameterValue{
			Type:       sqlDataType,
			ArrayValue: arrayValues,
		}
	case arrow.STRUCT:
		numFields := value.(*array.Struct).NumField()
		childFields := field.Type.(*arrow.StructType).Fields()
		structValues := make(map[string]bigquery.QueryParameterValue)
		for j := 0; j < numFields; j++ {
			currentField := childFields[j]
			fieldName := currentField.Name
			if len(fieldName) == 0 {
				return bigquery.QueryParameter{}, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  "child field name cannot be empty for structs",
				}
			}
			currentFieldArray := value.(*array.Struct).Field(j)
			pv, err := arrowValueToQueryParameterValue(currentField, currentFieldArray, i)
			if err != nil {
				return bigquery.QueryParameter{}, err
			}
			_, found := structValues[fieldName]
			if found {
				return bigquery.QueryParameter{}, adbc.Error{
					Code: adbc.StatusInvalidArgument,
					Msg:  fmt.Sprintf("duplicated child field `%s` found in structs", fieldName),
				}
			}
			structValues[fieldName] = *pv.Value.(*bigquery.QueryParameterValue)
		}

		parameter.Value = &bigquery.QueryParameterValue{
			Type:        sqlDataType,
			StructValue: structValues,
		}
	default:
		// todo: implement all other types
		return parameter, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("Parameter type %v is not yet implemented for BigQuery driver", value.DataType().ID()),
		}
	}

	return parameter, nil
}

func (st *statement) getBoundParameterReader() (array.RecordReader, error) {
	if st.paramBinding != nil {
		rdr, err := array.NewRecordReader(st.paramBinding.Schema(), []arrow.Record{st.paramBinding})
		if err != nil {
			return nil, err
		}
		st.streamBinding = rdr
		return st.streamBinding, nil
	} else if st.streamBinding != nil {
		return st.streamBinding, nil
	} else {
		return nil, nil
	}
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

var _ adbc.GetSetOptions = (*statement)(nil)
