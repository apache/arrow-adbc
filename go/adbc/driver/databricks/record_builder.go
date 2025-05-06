package databricks

import (
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/decimal256"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// ColumnBuilder is an interface for building Arrow arrays
type ColumnBuilder interface {
	Append(value interface{}) bool
	Builder() array.Builder
}

// StringColumnBuilder handles string values
type StringColumnBuilder struct {
	builder *array.StringBuilder
}

func (b *StringColumnBuilder) Append(value interface{}) bool {
	if str, ok := value.(string); ok {
		b.builder.Append(str)
		return true
	}
	return false
}

func (b *StringColumnBuilder) Builder() array.Builder {
	return b.builder
}

// Int32ColumnBuilder handles int32 values
type Int32ColumnBuilder struct {
	builder *array.Int32Builder
}

// the go value type can be float even if the DBX schema type is integer
func (b *Int32ColumnBuilder) Append(value interface{}) bool {
	switch v := value.(type) {
	case int:
		b.builder.Append(int32(v))
	case int32:
		b.builder.Append(v)
	case int64:
		b.builder.Append(int32(v))
	case float64:
		b.builder.Append(int32(v))
	case float32:
		b.builder.Append(int32(v))
	default:
		return false
	}
	return true
}

func (b *Int32ColumnBuilder) Builder() array.Builder {
	return b.builder
}

// Int64ColumnBuilder handles generating int64 arrow arrays from DBX int64 values
type Int64ColumnBuilder struct {
	builder *array.Int64Builder
}

// the go value type can be float even if the DBX schema type is integer
func (b *Int64ColumnBuilder) Append(value interface{}) bool {
	switch v := value.(type) {
	case int64:
		b.builder.Append(v)
	case int:
		b.builder.Append(int64(v))
	case int16:
		b.builder.Append(int64(v))
	case int32:
		b.builder.Append(int64(v))
	case float64:
		b.builder.Append(int64(v))
	case float32:
		b.builder.Append(int64(v))
	default:
		return false
	}
	return true
}

func (b *Int64ColumnBuilder) Builder() array.Builder {
	return b.builder
}

// Float32ColumnBuilder handles float32 values
type Float32ColumnBuilder struct {
	builder *array.Float32Builder
}

func (b *Float32ColumnBuilder) Append(value interface{}) bool {
	switch v := value.(type) {
	case float32:
		b.builder.Append(v)
	case float64:
		b.builder.Append(float32(v))
	default:
		return false
	}
	return true
}

func (b *Float32ColumnBuilder) Builder() array.Builder {
	return b.builder
}

// Float64ColumnBuilder handles float64 values
type Float64ColumnBuilder struct {
	builder *array.Float64Builder
}

func (b *Float64ColumnBuilder) Append(value interface{}) bool {
	switch v := value.(type) {
	case float64:
		b.builder.Append(v)
	default:
		return false
	}
	return true
}

func (b *Float64ColumnBuilder) Builder() array.Builder {
	return b.builder
}

// BooleanColumnBuilder handles boolean values
type BooleanColumnBuilder struct {
	builder *array.BooleanBuilder
}

func (b *BooleanColumnBuilder) Append(value interface{}) bool {
	if v, ok := value.(bool); ok {
		b.builder.Append(v)
		return true
	}
	return false
}

func (b *BooleanColumnBuilder) Builder() array.Builder {
	return b.builder
}

// TimestampColumnBuilder handles timestamp values
type TimestampColumnBuilder struct {
	builder *array.TimestampBuilder
}

// DBX "timestamps" are represented as strings in the format "YYYY-MM-DDTHH:MM:SS"
func (b *TimestampColumnBuilder) Append(value interface{}) bool {
	if t, ok := value.(time.Time); ok {
		ts, err := arrow.TimestampFromTime(t, arrow.Second)
		if err != nil {
			return false
		}
		b.builder.Append(ts)
	} else if t, ok := value.(string); ok {
		t, err := time.Parse(time.RFC3339, t)
		if err != nil {
			return false
		}
		ts, err := arrow.TimestampFromTime(t, arrow.Second)
		if err != nil {
			return false
		}
		b.builder.Append(ts)
	} else {
		return false
	}
		return true
}

func (b *TimestampColumnBuilder) Builder() array.Builder {
	return b.builder
}

// Date32ColumnBuilder handles date32 values
type Date32ColumnBuilder struct {
	builder *array.Date32Builder
}

// DBX "dates" are represented as strings in the format "YYYY-MM-DD"
func (b *Date32ColumnBuilder) Append(value interface{}) bool {
	if t, ok := value.(time.Time); ok {
		b.builder.Append(arrow.Date32FromTime(t))
	} else if t, ok := value.(string); ok {
		t, err := time.Parse(time.DateOnly, t)
		if err != nil {
			return false
		}
		b.builder.Append(arrow.Date32FromTime(t))
	} else {
		return false
	}
	return true
}

func (b *Date32ColumnBuilder) Builder() array.Builder {
	return b.builder
}

// DecimalColumnBuilder handles decimal values
type DecimalColumnBuilder struct {
	builder *array.Decimal256Builder
}

func (b *DecimalColumnBuilder) Append(value interface{}) bool {
	switch v := value.(type) {
	case float64:
		val, err := decimal256.FromFloat64(v, 38, 2)
		if err != nil {
			return false
		}
		b.builder.Append(val)
	case float32:
		val, err := decimal256.FromFloat64(float64(v), 38, 2)
		if err != nil {
			return false
		}
		b.builder.Append(val)
	case string:
		val, err := decimal256.FromString(v, 38, 2)
		if err != nil {
			return false
		}
		b.builder.Append(val)
	default:
		return false
	}
	return true
}

func (b *DecimalColumnBuilder) Builder() array.Builder {
	return b.builder
}


// RecordBuilder handles building Arrow record batches from arrays of rows
type RecordBuilder struct {
	schema   *arrow.Schema
	mem      memory.Allocator
	builders []ColumnBuilder
}

// NewRecordBuilder creates a new RecordBuilder with the given schema
func NewRecordBuilder(schema *arrow.Schema) *RecordBuilder {
	mem := memory.NewGoAllocator()
	builders := make([]ColumnBuilder, len(schema.Fields()))

	for i, field := range schema.Fields() {
		switch field.Type.ID() {
		case arrow.STRING:
			builders[i] = &StringColumnBuilder{
				builder: array.NewStringBuilder(mem),
			}
		case arrow.INT32:
			builders[i] = &Int32ColumnBuilder{
				builder: array.NewInt32Builder(mem),
			}
		case arrow.INT64:
			builders[i] = &Int64ColumnBuilder{
				builder: array.NewInt64Builder(mem),
			}
		case arrow.FLOAT32:
			builders[i] = &Float32ColumnBuilder{
				builder: array.NewFloat32Builder(mem),
			}
		case arrow.FLOAT64:
			builders[i] = &Float64ColumnBuilder{
				builder: array.NewFloat64Builder(mem),
			}
		case arrow.BOOL:
			builders[i] = &BooleanColumnBuilder{
				builder: array.NewBooleanBuilder(mem),
			}
		case arrow.TIMESTAMP:
			builders[i] = &TimestampColumnBuilder{
				builder: array.NewTimestampBuilder(mem, field.Type.(*arrow.TimestampType)),
			}
		case arrow.DATE32:
			builders[i] = &Date32ColumnBuilder{
				builder: array.NewDate32Builder(mem),
			}
		case arrow.DECIMAL256:
			builders[i] = &DecimalColumnBuilder{
				builder: array.NewDecimal256Builder(mem, field.Type.(*arrow.Decimal256Type)),
			}	
		}
	}

	return &RecordBuilder{
		schema:   schema,
		mem:      mem,
		builders: builders,
	}
}

// Append appends a single row of data to the builders
func (rb *RecordBuilder) Append(row []interface{}) error {
	for i, val := range row {
		if val == nil {
			rb.builders[i].Builder().AppendNull()
		} else {
			append_result := rb.builders[i].Append(val)
			if !append_result {
				return fmt.Errorf("failed to append value %v for column %d with type %T, schema type %T", val, i, val, rb.schema.Fields()[i].Type)
			}
		}
	}
	return nil
}

// NewRecord creates a new Arrow record from the accumulated data
func (rb *RecordBuilder) NewRecord() arrow.Record {
	fields := make([]arrow.Array, len(rb.builders))
	for i, builder := range rb.builders {
		fields[i] = builder.Builder().NewArray()
	}
	return array.NewRecord(rb.schema, fields, int64(fields[0].Len()))
}

// Release releases the memory allocated by the builders
func (rb *RecordBuilder) Release() {
	for _, builder := range rb.builders {
		if builder != nil {
			builder.Builder().Release()
		}
	}
}

// BuildFromRows creates a new Arrow record from an array of rows
func BuildFromRows(schema *arrow.Schema, rows []interface{}) (arrow.Record, error) {
	rb := NewRecordBuilder(schema)
	defer rb.Release()
	for _, row := range rows {
		err := rb.Append(row.([]interface{}))
		if err != nil {
			return nil, err
		}
	}

	return rb.NewRecord(), nil
}
