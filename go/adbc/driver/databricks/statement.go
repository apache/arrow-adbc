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
	"fmt"
	"reflect"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

type statementImpl struct {
	conn       *connectionImpl
	query      string
	parameters []interface{}
	prepared   *sql.Stmt
}

func (s *statementImpl) Close() error {
	if s.prepared != nil {
		return s.prepared.Close()
	}
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
		_ = s.prepared.Close() // Ignore error on cleanup
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

	stmt, err := s.conn.db.PrepareContext(ctx, s.query)
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
	var rows *sql.Rows
	var err error

	if s.prepared != nil {
		rows, err = s.prepared.QueryContext(ctx, s.parameters...)
	} else if s.query != "" {
		rows, err = s.conn.db.QueryContext(ctx, s.query, s.parameters...)
	} else {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInvalidState,
			Msg:  "no query set",
		}
	}

	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to execute query: %v", err),
		}
	}

	reader, rowsAffected, err := s.rowsToRecordReader(ctx, rows)
	if err != nil {
		_ = rows.Close() // Ignore error on cleanup
		return nil, -1, err
	}

	return reader, rowsAffected, nil
}

func (s *statementImpl) ExecuteUpdate(ctx context.Context) (int64, error) {
	var result sql.Result
	var err error

	if s.prepared != nil {
		result, err = s.prepared.ExecContext(ctx, s.parameters...)
	} else if s.query != "" {
		result, err = s.conn.db.ExecContext(ctx, s.query, s.parameters...)
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
	// Convert Arrow record to parameters
	s.parameters = make([]interface{}, values.NumCols())

	for i := 0; i < int(values.NumCols()); i++ {
		col := values.Column(i)
		if col.Len() == 0 {
			s.parameters[i] = nil
			continue
		}

		// Take the first value from each column
		value, err := s.arrowToGoValue(col, 0)
		if err != nil {
			return adbc.Error{
				Code: adbc.StatusInvalidArgument,
				Msg:  fmt.Sprintf("failed to convert parameter %d: %v", i, err),
			}
		}
		s.parameters[i] = value
	}

	return nil
}

func (s *statementImpl) BindStream(ctx context.Context, stream array.RecordReader) error {
	// For simplicity, we'll just bind the first record
	if stream.Next() {
		return s.Bind(ctx, stream.Record())
	}
	return nil
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

func (s *statementImpl) rowsToRecordReader(ctx context.Context, rows *sql.Rows) (array.RecordReader, int64, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to get column types: %v", err),
		}
	}

	// Build Arrow schema from SQL column types
	fields := make([]arrow.Field, len(columnTypes))
	for i, colType := range columnTypes {
		arrowType, err := s.sqlTypeToArrowType(colType)
		if err != nil {
			return nil, -1, err
		}
		fields[i] = arrow.Field{
			Name: colType.Name(),
			Type: arrowType,
		}
	}

	schema := arrow.NewSchema(fields, nil)

	// Collect all rows first
	var allRows [][]interface{}
	var totalRows int64

	for rows.Next() {
		// Create value slice for scanning
		values := make([]interface{}, len(columnTypes))
		scanArgs := make([]interface{}, len(columnTypes))
		for i := range values {
			scanArgs[i] = &values[i]
		}

		if err := rows.Scan(scanArgs...); err != nil {
			return nil, -1, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to scan row: %v", err),
			}
		}

		// Copy values for storage
		rowValues := make([]interface{}, len(values))
		copy(rowValues, values)
		allRows = append(allRows, rowValues)
		totalRows++
	}

	if err := rows.Err(); err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("row iteration error: %v", err),
		}
	}

	// Build Arrow record from collected rows
	if len(allRows) == 0 {
		// Return empty record reader
		reader, err := array.NewRecordReader(schema, []arrow.Record{})
		if err != nil {
			return nil, -1, adbc.Error{
				Code: adbc.StatusInternal,
				Msg:  fmt.Sprintf("failed to create empty record reader: %v", err),
			}
		}
		return reader, 0, nil
	}

	// Create record builder
	bldr := array.NewRecordBuilder(memory.DefaultAllocator, schema)
	defer bldr.Release()

	// Reserve space
	bldr.Reserve(int(totalRows))

	// Fill builders with data
	for rowIdx, row := range allRows {
		for colIdx, value := range row {
			builder := bldr.Field(colIdx)
			if value == nil {
				builder.AppendNull()
			} else {
				err := s.appendValueToBuilder(builder, value, fields[colIdx].Type)
				if err != nil {
					return nil, -1, adbc.Error{
						Code: adbc.StatusInternal,
						Msg:  fmt.Sprintf("failed to append value at row %d, col %d: %v", rowIdx, colIdx, err),
					}
				}
			}
		}
	}

	// Create record
	record := bldr.NewRecord()
	defer record.Release()

	// Create record reader
	reader, err := array.NewRecordReader(schema, []arrow.Record{record})
	if err != nil {
		return nil, -1, adbc.Error{
			Code: adbc.StatusInternal,
			Msg:  fmt.Sprintf("failed to create record reader: %v", err),
		}
	}

	return reader, totalRows, nil
}

func (s *statementImpl) sqlTypeToArrowType(colType *sql.ColumnType) (arrow.DataType, error) {
	switch colType.DatabaseTypeName() {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean, nil
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8, nil
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16, nil
	case "INT", "INTEGER":
		return arrow.PrimitiveTypes.Int32, nil
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, nil
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32, nil
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, nil
	case "STRING", "VARCHAR", "CHAR":
		return arrow.BinaryTypes.String, nil
	case "BINARY":
		return arrow.BinaryTypes.Binary, nil
	case "TIMESTAMP":
		return arrow.FixedWidthTypes.Timestamp_us, nil
	case "DATE":
		return arrow.FixedWidthTypes.Date32, nil
	case "DECIMAL":
		precision, scale, ok := colType.DecimalSize()
		if !ok {
			precision, scale = 38, 0
		}
		return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
	default:
		// Default to string for unknown types
		return arrow.BinaryTypes.String, nil
	}
}

func (s *statementImpl) appendValueToBuilder(builder array.Builder, value interface{}, arrowType arrow.DataType) error {
	switch arrowType.ID() {
	case arrow.BOOL:
		if b, ok := value.(bool); ok {
			builder.(*array.BooleanBuilder).Append(b)
		} else {
			return fmt.Errorf("expected bool, got %T", value)
		}
	case arrow.INT8:
		if i, ok := value.(int8); ok {
			builder.(*array.Int8Builder).Append(i)
		} else {
			return fmt.Errorf("expected int8, got %T", value)
		}
	case arrow.INT16:
		if i, ok := value.(int16); ok {
			builder.(*array.Int16Builder).Append(i)
		} else {
			return fmt.Errorf("expected int16, got %T", value)
		}
	case arrow.INT32:
		if i, ok := value.(int32); ok {
			builder.(*array.Int32Builder).Append(i)
		} else if i, ok := value.(int); ok {
			builder.(*array.Int32Builder).Append(int32(i))
		} else {
			return fmt.Errorf("expected int32, got %T", value)
		}
	case arrow.INT64:
		if i, ok := value.(int64); ok {
			builder.(*array.Int64Builder).Append(i)
		} else if i, ok := value.(int); ok {
			builder.(*array.Int64Builder).Append(int64(i))
		} else {
			return fmt.Errorf("expected int64, got %T", value)
		}
	case arrow.FLOAT32:
		if f, ok := value.(float32); ok {
			builder.(*array.Float32Builder).Append(f)
		} else {
			return fmt.Errorf("expected float32, got %T", value)
		}
	case arrow.FLOAT64:
		if f, ok := value.(float64); ok {
			builder.(*array.Float64Builder).Append(f)
		} else {
			return fmt.Errorf("expected float64, got %T", value)
		}
	case arrow.STRING:
		if s, ok := value.(string); ok {
			builder.(*array.StringBuilder).Append(s)
		} else {
			// Convert other types to string
			builder.(*array.StringBuilder).Append(fmt.Sprintf("%v", value))
		}
	case arrow.BINARY:
		if b, ok := value.([]byte); ok {
			builder.(*array.BinaryBuilder).Append(b)
		} else {
			return fmt.Errorf("expected []byte, got %T", value)
		}
	default:
		// For unsupported types, try to convert based on builder type
		switch b := builder.(type) {
		case *array.StringBuilder:
			b.Append(fmt.Sprintf("%v", value))
		case *array.Date32Builder:
			// Handle date types - convert string dates to Arrow date
			if _, ok := value.(string); ok {
				// For now, append as epoch days (simplified)
				// TODO: Implement proper date parsing
				b.Append(0)
			} else {
				b.AppendNull()
			}
		case *array.TimestampBuilder:
			// Handle timestamp types
			if ts, ok := value.(string); ok {
				// For now, append as zero timestamp (simplified)
				_ = ts
				b.Append(0) // This would need proper timestamp parsing
			} else {
				b.AppendNull()
			}
		default:
			// If we can't handle the type, append null
			builder.AppendNull()
		}
	}
	return nil
}

func (s *statementImpl) arrowToGoValue(arr arrow.Array, idx int) (interface{}, error) {
	if arr.IsNull(idx) {
		return nil, nil
	}

	switch a := arr.(type) {
	case *array.Boolean:
		return a.Value(idx), nil
	case *array.Int8:
		return a.Value(idx), nil
	case *array.Int16:
		return a.Value(idx), nil
	case *array.Int32:
		return a.Value(idx), nil
	case *array.Int64:
		return a.Value(idx), nil
	case *array.Float32:
		return a.Value(idx), nil
	case *array.Float64:
		return a.Value(idx), nil
	case *array.String:
		return a.Value(idx), nil
	case *array.Binary:
		return a.Value(idx), nil
	default:
		return nil, adbc.Error{
			Code: adbc.StatusNotImplemented,
			Msg:  fmt.Sprintf("conversion from Arrow type %v not implemented", reflect.TypeOf(arr)),
		}
	}
}
