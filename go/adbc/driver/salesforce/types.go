package salesforce

import (
	"fmt"
	"log"

	api "github.com/apache/arrow-adbc/go/adbc/driver/salesforce/gosalesforce/pkg"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
)

// buildArrowSchema builds Arrow schema from SQL Query API metadata
func (s *statement) buildArrowSchema(metadata []api.SqlQueryMetadata) *arrow.Schema {
	fields := make([]arrow.Field, len(metadata))

	for i, col := range metadata {
		arrowType := SalesforceTypeToArrowType(col.Type)
		field := arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: col.Nullable,
		}
		fields[i] = field
	}

	return arrow.NewSchema(fields, nil)
}

// buildArrowRecords converts the raw data to Arrow records
func (s *statement) buildArrowRecords(schema *arrow.Schema, data [][]interface{}) ([]arrow.Record, error) {
	if len(data) == 0 {
		return []arrow.Record{}, nil
	}

	// For now, create a simple single record
	// In a full implementation, you might want to batch this
	builders := make([]array.Builder, len(schema.Fields()))
	for i, field := range schema.Fields() {
		builders[i] = array.NewBuilder(s.alloc, field.Type)
	}
	defer func() {
		for _, builder := range builders {
			builder.Release()
		}
	}()

	// Add data to builders
	for _, row := range data {
		for i, value := range row {
			if i >= len(builders) {
				break // Skip extra columns
			}

			if value == nil {
				builders[i].AppendNull()
			} else {
				appendValueToBuilder(builders[i], value, schema.Field(i).Type)
			}
		}
	}

	// Build arrays
	arrays := make([]arrow.Array, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
	}

	// Create record
	record := array.NewRecord(schema, arrays, int64(len(data)))

	// Release arrays
	for _, arr := range arrays {
		arr.Release()
	}

	return []arrow.Record{record}, nil
}

// appendValueToBuilder appends a value to the appropriate builder type using the dataType
func appendValueToBuilder(builder array.Builder, value interface{}, dataType arrow.DataType) {
	// Convert value based on the target Arrow data type
	switch dataType.ID() {
	case arrow.STRING:
		b := builder.(*array.StringBuilder)
		if str, ok := value.(string); ok {
			b.Append(str)
		} else {
			b.Append(fmt.Sprintf("%v", value))
		}

	case arrow.INT64:
		b := builder.(*array.Int64Builder)
		if convertedValue, ok := convertToInt64(value); ok {
			b.Append(convertedValue)
		} else {
			log.Printf("DEBUG: Failed to convert %T(%v) to int64\n", value, value)
			b.AppendNull()
		}

	case arrow.INT32:
		b := builder.(*array.Int32Builder)
		if convertedValue, ok := convertToInt32(value); ok {
			b.Append(convertedValue)
		} else {
			log.Printf("DEBUG: Failed to convert %T(%v) to int32\n", value, value)
			b.AppendNull()
		}

	case arrow.INT16:
		b := builder.(*array.Int16Builder)
		if convertedValue, ok := convertToInt16(value); ok {
			b.Append(convertedValue)
		} else {
			log.Printf("DEBUG: Failed to convert %T(%v) to int16\n", value, value)
			b.AppendNull()
		}

	case arrow.FLOAT64:
		b := builder.(*array.Float64Builder)
		if convertedValue, ok := convertToFloat64(value); ok {
			b.Append(convertedValue)
		} else {
			log.Printf("DEBUG: Failed to convert %T(%v) to float64\n", value, value)
			b.AppendNull()
		}

	case arrow.FLOAT32:
		b := builder.(*array.Float32Builder)
		if convertedValue, ok := convertToFloat32(value); ok {
			b.Append(convertedValue)
		} else {
			log.Printf("DEBUG: Failed to convert %T(%v) to float32\n", value, value)
			b.AppendNull()
		}

	case arrow.BOOL:
		b := builder.(*array.BooleanBuilder)
		if convertedValue, ok := value.(bool); ok {
			b.Append(convertedValue)
		} else {
			log.Printf("DEBUG: Failed to convert %T(%v) to bool\n", value, value)
			b.AppendNull()
		}
	case arrow.TIMESTAMP:
		b := builder.(*array.TimestampBuilder)
		if convertedValue, ok := convertToTimestamp(value); ok {
			b.Append(convertedValue)
		} else {
			log.Printf("DEBUG: Failed to convert %T(%v) to timestamp\n", value, value)
			b.AppendNull()
		}

	default:
		log.Printf("DEBUG: Unsupported data type %v for value type %T with value %v\n", dataType, value, value)
		builder.AppendNull()
	}
}

func convertToTimestamp(value interface{}) (arrow.Timestamp, bool) {
	switch v := value.(type) {
	case string:
		timestamp, err := arrow.TimestampFromString(v, arrow.Microsecond)
		if err == nil {
			return timestamp, true
		}
	}
	return 0, false
}

// Helper functions for type conversion
func convertToInt64(value interface{}) (int64, bool) {
	switch v := value.(type) {
	case int64:
		return v, true
	case int:
		return int64(v), true
	case int32:
		return int64(v), true
	// the deserialized integer value's runtime type is float64
	// value is from [SqlQueryResponse.Data]
	case float64:
		return int64(v), true
	}
	return 0, false
}

func convertToInt32(value interface{}) (int32, bool) {
	switch v := value.(type) {
	case int32:
		return v, true
	case int:
		return int32(v), true
	case int64:
		return int32(v), true
	case float64:
		return int32(v), true
	}
	return 0, false
}

func convertToInt16(value interface{}) (int16, bool) {
	switch v := value.(type) {
	case int16:
		return v, true
	case int:
		return int16(v), true
	case float64:
		return int16(v), true
	}
	return 0, false
}

func convertToFloat64(value interface{}) (float64, bool) {
	switch v := value.(type) {
	case float64:
		return v, true
	case float32:
		return float64(v), true
	}
	return 0, false
}

func convertToFloat32(value interface{}) (float32, bool) {
	switch v := value.(type) {
	case float32:
		return v, true
	case float64:
		return float32(v), true
	}
	return 0, false
}

// SalesforceTypeToArrowType converts a Salesforce type to an Arrow type
// reference: https://developer.salesforce.com/docs/data/connectapi/references/spec?meta=createSqlQuery
func SalesforceTypeToArrowType(sfType string) arrow.DataType {
	switch sfType {
	case "Varchar", "Char":
		return arrow.BinaryTypes.String
	case "BigInt":
		return arrow.PrimitiveTypes.Int64
	case "Integer":
		return arrow.PrimitiveTypes.Int32
	case "SmallInt":
		return arrow.PrimitiveTypes.Int16
	case "Double":
		return arrow.PrimitiveTypes.Float64
	case "Numeric", "Float":
		return arrow.PrimitiveTypes.Float32
	case "Bool":
		return arrow.FixedWidthTypes.Boolean
	case "Date":
		return arrow.FixedWidthTypes.Date32
	case "Time":
		return arrow.FixedWidthTypes.Time32ms
	case "Timestamp", "TimestampTZ":
		return arrow.FixedWidthTypes.Timestamp_ms
	case "Oid":
		return arrow.PrimitiveTypes.Uint32
	case "Unspecified":
		return arrow.Null
	default:
		// Handle ArrayOfX types
		if len(sfType) > 7 && sfType[:7] == "ArrayOf" {
			elementType := sfType[7:] // Extract the element type after "ArrayOf"
			elementArrowType := SalesforceTypeToArrowType(elementType)
			return arrow.ListOf(elementArrowType)
		}
		// Default to string for unknown types
		return arrow.BinaryTypes.String
	}
}
