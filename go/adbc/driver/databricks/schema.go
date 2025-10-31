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
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/databricks/databricks-sdk-go/service/sql"
)

const DbxSchemaTypeText = "type_text"
const DbxSchemaTypeTextNewConvention = "DBX:type"

const (
	decimalTypeRegexRaw  = `(?i)^\s*(?:DECIMAL|DEC|NUMERIC)\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)\s*$`
	intervalTypeRegexRaw = `(?i)^\s*INTERVAL\s+(YEAR(?:\s+TO\s+MONTH)?|MONTH|DAY(?:\s+TO\s+(HOUR|MINUTE|SECOND))?|HOUR(?:\s+TO\s+(MINUTE|SECOND))?|MINUTE(?:\s+TO\s+SECOND)?|SECOND)`
)

var (
	decimalTypeRegex  = regexp.MustCompile(decimalTypeRegexRaw)
	intervalTypeRegex = regexp.MustCompile(intervalTypeRegexRaw)
)

// Basic DBX Types to Arrow Types (no extra processing needed)
// https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-datatypes
var basicTypeToArrowTypeMap = map[sql.ColumnInfoTypeName]arrow.DataType{
	// Integral numeric types (whole numbers)
	sql.ColumnInfoTypeNameByte:  arrow.PrimitiveTypes.Int8,  // 1-byte signed integer
	sql.ColumnInfoTypeNameShort: arrow.PrimitiveTypes.Int16, // 2-byte signed integer
	sql.ColumnInfoTypeNameInt:   arrow.PrimitiveTypes.Int32, // 4-byte signed integer
	sql.ColumnInfoTypeNameLong:  arrow.PrimitiveTypes.Int64, // 8-byte signed integer

	// Binary floating point types
	sql.ColumnInfoTypeNameFloat:  arrow.PrimitiveTypes.Float32, // 4-byte single-precision
	sql.ColumnInfoTypeNameDouble: arrow.PrimitiveTypes.Float64, // 8-byte double-precision

	// Date-time types
	sql.ColumnInfoTypeNameDate: arrow.FixedWidthTypes.Date32, // year, month, day without timezone

	// Simple types
	sql.ColumnInfoTypeNameString:  arrow.BinaryTypes.String,      // character string values
	sql.ColumnInfoTypeNameBinary:  arrow.BinaryTypes.Binary,      // byte sequence values
	sql.ColumnInfoTypeNameBoolean: arrow.FixedWidthTypes.Boolean, // Boolean values
	sql.ColumnInfoTypeNameNull:    arrow.Null,                    // untyped NULL - not supported by Delta Lake
}

var stringTypeToArrowTypeMap = map[string]arrow.DataType{
	// Integral numeric types (whole numbers)
	"TINYINT":  arrow.PrimitiveTypes.Int8,  // 1-byte signed integer
	"BYTE":     arrow.PrimitiveTypes.Int8,  // 1-byte signed integer
	"SMALLINT": arrow.PrimitiveTypes.Int16, // 2-byte signed integer
	"SHORT":    arrow.PrimitiveTypes.Int16, // 2-byte signed integer
	"INT":      arrow.PrimitiveTypes.Int32, // 4-byte signed integer
	"INTEGER":  arrow.PrimitiveTypes.Int32, // 4-byte signed integer
	"LONG":     arrow.PrimitiveTypes.Int64, // 8-byte signed integer
	"BIGINT":   arrow.PrimitiveTypes.Int64, // 8-byte signed integer

	// Binary floating point types
	"FLOAT":  arrow.PrimitiveTypes.Float32, // 4-byte single-precision
	"REAL":   arrow.PrimitiveTypes.Float32, // 4-byte single-precision
	"DOUBLE": arrow.PrimitiveTypes.Float64, // 8-byte double-precision

	// Date-time types
	"DATE": arrow.FixedWidthTypes.Date32, // year, month, day without timezone

	// Simple types
	"STRING":  arrow.BinaryTypes.String,      // character string values
	"BINARY":  arrow.BinaryTypes.Binary,      // byte sequence values
	"BOOLEAN": arrow.FixedWidthTypes.Boolean, // Boolean values
	"VOID":    arrow.Null,                    // untyped NULL - not supported by Delta Lake
	"NULL":    arrow.Null,                    // untyped NULL - not supported by Delta Lake
}

func strToInt(str string) (int, error) {
	// expect base 10, parse to 8 bits
	out, err := strconv.ParseInt(str, 10, 8)
	if err != nil {
		return 0, err
	}
	return int(out), nil
}

// tracks bracket and parenthesis depth for parsing nested types
type depthTracker struct {
	stack []byte
}

var delimiterMap = map[byte]byte{
	')': '(',
	'>': '<',
}

// updates the depth counters based on the given character
func (dt *depthTracker) updateDepth(char byte) error {
	switch char {
	case '<', '(':
		dt.stack = append(dt.stack, char)
	case '>', ')':
		if len(dt.stack) == 0 {
			return fmt.Errorf("unmatched closing delimiter '%c' (empty stack)", char)
		}
		expected := delimiterMap[char]
		actual := dt.stack[len(dt.stack)-1]
		if actual != expected {
			return fmt.Errorf("unmatched closing delimiter '%c' (expected '%c', got '%c')", char, expected, actual)
		}
		dt.stack = dt.stack[:len(dt.stack)-1]
	}
	return nil
}

// returns true if the stack is empty (at top level)
func (dt *depthTracker) isAtTopLevel() bool {
	return len(dt.stack) == 0
}

func ClusterModeSchemaToArrowSchema(dbxSchema []map[string]interface{}) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(dbxSchema))
	for i, col := range dbxSchema {
		var arrowType arrow.DataType
		colType := strings.Trim(col["type"].(string), "\"")
		switch {
		case colType == "int":
			arrowType = arrow.PrimitiveTypes.Int32
		case colType == "integer":
			arrowType = arrow.PrimitiveTypes.Int32
		case colType == "string":
			arrowType = arrow.BinaryTypes.String
		case colType == "short":
			arrowType = arrow.PrimitiveTypes.Int16
		case colType == "long":
			arrowType = arrow.PrimitiveTypes.Int64
		case colType == "float":
			arrowType = arrow.PrimitiveTypes.Float32
		case colType == "double":
			arrowType = arrow.PrimitiveTypes.Float64
		case colType == "boolean":
			arrowType = arrow.FixedWidthTypes.Boolean
		case colType == "timestamp":
			arrowType = &arrow.TimestampType{Unit: arrow.Second}
		case colType == "date":
			arrowType = arrow.FixedWidthTypes.Date32
		case strings.HasPrefix(colType, "decimal"):
			// Parse decimal precision and scale from format "decimal(precision,scale)"
			if matches := regexp.MustCompile(`decimal\((\d+),(\d+)\)`).FindStringSubmatch(colType); matches != nil {
				precision, _ := strToInt(matches[1])
				scale, _ := strToInt(matches[2])
				arrowType = &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}
			} else {
				arrowType = &arrow.Decimal128Type{}
			}
		default:
			err := fmt.Errorf("unsupported type: %v", colType)
			return nil, err
		}
		fields[i] = arrow.Field{
			Name:     col["name"].(string),
			Type:     arrowType,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				DbxSchemaTypeText:              colType,
				DbxSchemaTypeTextNewConvention: colType,
			}),
		}
	}
	// TODO: include relevant metadata from dbrx into the Arrow schema
	return arrow.NewSchema(fields, nil), nil
}

func ResultSchemaToArrowSchema(dbxSchema *sql.ResultSchema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, dbxSchema.ColumnCount)
	for i, col := range dbxSchema.Columns {
		arrowType, err := getArrowTypeFromColumnInfo(col)
		if err != nil {
			return nil, err
		}
		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				"type_name":                    string(col.TypeName),
				"position":                     col.Name,
				DbxSchemaTypeText:              col.TypeText,
				DbxSchemaTypeTextNewConvention: col.TypeText,
			}),
		}
	}
	metadata := arrow.MetadataFrom(map[string]string{
		"column_count": strconv.Itoa(dbxSchema.ColumnCount),
	})
	return arrow.NewSchema(fields, &metadata), nil
}

// extracts the content between the outermost <...> after the given prefix.
func extractOutermostBracketContent(s, prefix string) (string, bool) {
	if !strings.HasPrefix(strings.ToUpper(strings.TrimSpace(s)), strings.ToUpper(prefix)+"<") || !strings.HasSuffix(strings.TrimSpace(s), ">") {
		return "", false
	}
	start := len(prefix) + 1
	depth := 0
	for i := start; i < len(s); i++ {
		if s[i] == '<' {
			depth++
		} else if s[i] == '>' {
			if depth == 0 {
				// Found the matching closing bracket
				return s[start:i], true
			}
			depth--
		}
	}
	// If we get here, brackets were unbalanced
	return "", false
}

func getArrowTypeFromStringType(col string) (arrow.DataType, error) {
	// simple types - base case
	if arrowType, ok := stringTypeToArrowTypeMap[col]; ok {
		return arrowType, nil
	}
	// match the IPC return type from Databricks
	if strings.EqualFold(col, "TIMESTAMP") || strings.EqualFold(col, "TIMESTAMP_NTZ") {
		return &arrow.TimestampType{
			Unit:     arrow.Microsecond,
			TimeZone: "Etc/UTC",
		}, nil
	}
	// { DECIMAL | DEC | NUMERIC } [ (  p [ , s ] ) ]
	if matches := decimalTypeRegex.FindStringSubmatch(col); matches != nil {
		var err error
		precision, err := strToInt(matches[1])
		if err != nil {
			return nil, fmt.Errorf("invalid decimal precision: %v", err)
		}

		var scale = 0
		// Match scale if present
		if len(matches) > 2 && matches[2] != "" {
			parsed_scale, err := strToInt(matches[2])
			if err != nil {
				return nil, fmt.Errorf("invalid decimal scale: %v", err)
			}
			scale = parsed_scale
		}

		return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
	}
	// ARRAY < elementType >
	if content, ok := extractOutermostBracketContent(col, "ARRAY"); ok {
		elementArrowType, err := getArrowTypeFromStringType(strings.TrimSpace(content))
		if err != nil {
			return nil, fmt.Errorf("failed to parse array element type: %w", err)
		}
		return arrow.ListOf(elementArrowType), nil
	}
	// MAP <keyType, valueType>
	if content, ok := extractOutermostBracketContent(col, "MAP"); ok {
		keyType, valueType, err := parseMapKeyValue(content)
		if err != nil {
			return nil, fmt.Errorf("invalid map type format: %w", err)
		}
		keyArrowType, err := getArrowTypeFromStringType(keyType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse map key type: %w", err)
		}
		valueArrowType, err := getArrowTypeFromStringType(valueType)
		if err != nil {
			return nil, fmt.Errorf("failed to parse map value type: %w", err)
		}
		return arrow.MapOf(keyArrowType, valueArrowType), nil
	}
	// INTERVAL types - comprehensive pattern for all interval types
	if matches := intervalTypeRegex.FindStringSubmatch(col); matches != nil {
		// Year-month intervals
		if strings.Contains(matches[1], "YEAR") || matches[1] == "MONTH" {
			return arrow.FixedWidthTypes.MonthInterval, nil
		}
		// All other intervals are day-time intervals
		return arrow.FixedWidthTypes.Duration_s, nil
	}
	// STRUCT < [fieldName [:] fieldType [NOT NULL] [COLLATE collationName] [COMMENT str] [, …] ] >
	if content, ok := extractOutermostBracketContent(col, "STRUCT"); ok {
		fields, err := parseStructFields(content)
		if err != nil {
			return nil, fmt.Errorf("failed to parse struct: %w", err)
		}
		arrowFields := make([]arrow.Field, 0, len(fields))
		for _, field := range fields {
			fieldName, fieldType, nullable, err := parseStructField(field)
			if err != nil {
				return nil, fmt.Errorf("failed to parse struct field '%s': %w", field, err)
			}
			fieldArrowType, err := getArrowTypeFromStringType(fieldType)
			if err != nil {
				return nil, fmt.Errorf("failed to parse struct field type %s: %w", fieldName, err)
			}
			arrowFields = append(arrowFields, arrow.Field{
				Name:     fieldName,
				Type:     fieldArrowType,
				Nullable: nullable,
			})
		}
		return arrow.StructOf(arrowFields...), nil
	}
	// Use Null to occupy schema column position, so metadata remains correctly aligned even when the type is unrecognized.
	return arrow.Null, fmt.Errorf("unrecognized type: %s", col)
}

// parses comma separated struct fields and handles nesting
func parseStructFields(fieldsStr string) ([]string, error) {
	var fields []string
	var currentField strings.Builder
	dt := &depthTracker{}
	inQuotes := false

	for i := 0; i < len(fieldsStr); i++ {
		char := fieldsStr[i]

		switch char {
		case '"':
			// Handle quoted strings, but check for escaped quotes
			if i > 0 && fieldsStr[i-1] == '\\' {
				// This is an escaped quote, treat as regular character
				currentField.WriteByte(char)
			} else {
				// Toggle quote state
				inQuotes = !inQuotes
				currentField.WriteByte(char)
			}
		case '<', '>', '(', ')':
			if !inQuotes {
				err := dt.updateDepth(char)
				if err != nil {
					return nil, err
				}
			}
			currentField.WriteByte(char)
		case ',':
			if !inQuotes && dt.isAtTopLevel() {
				// This comma separates fields, not nested type parameters or quoted content
				field := strings.TrimSpace(currentField.String())
				if field != "" {
					fields = append(fields, field)
				}
				currentField.Reset()
				continue
			} else {
				// This comma is inside brackets, parentheses, or quotes, so it's part of the type
				currentField.WriteByte(char)
			}
		default:
			currentField.WriteByte(char)
		}
	}
	// last field
	field := strings.TrimSpace(currentField.String())
	if field != "" {
		fields = append(fields, field)
	}

	return fields, nil
}

// parses a single struct field
func parseStructField(field string) (fieldName, fieldType string, nullable bool, err error) {
	// Split on first colon to get field name and the rest
	parts := strings.SplitN(field, ":", 2)
	if len(parts) != 2 {
		return "", "", true, fmt.Errorf("invalid struct field format: missing colon")
	}

	fieldName = strings.TrimSpace(parts[0])
	rest := strings.TrimSpace(parts[1])

	// Extract field type and modifiers
	fieldType, nullable = extractFieldTypeAndModifiers(rest)

	return fieldName, fieldType, nullable, nil
}

// extracts the field type and determines nullability
// todo(jason): handle comments and collations (if they show up from the API)
func extractFieldTypeAndModifiers(rest string) (fieldType string, nullable bool) {
	nullable = true

	// Split by whitespace to find modifiers
	parts := strings.Fields(rest)
	if len(parts) == 0 {
		return "", true
	}

	// For simple types (one word), use the first word as the type
	// For complex types with brackets, use findTypeEnd
	if strings.ContainsAny(parts[0], "<>()") || strings.HasPrefix(strings.ToUpper(rest), "INTERVAL") {
		// Complex type - use findTypeEnd to handle nested brackets
		typeEnd := findTypeEnd(rest)
		fieldType = strings.TrimSpace(rest[:typeEnd])
		remaining := strings.TrimSpace(rest[typeEnd:])

		// Check for NOT NULL modifier
		if strings.Contains(strings.ToUpper(remaining), "NOT NULL") {
			nullable = false
		}
	} else {
		// Simple type - first word is the type, rest are modifiers
		fieldType = parts[0]

		// Join remaining parts and check for NOT NULL
		if len(parts) > 1 {
			remaining := strings.Join(parts[1:], " ")
			if strings.Contains(strings.ToUpper(remaining), "NOT NULL") {
				nullable = false
			}
		}
	}

	return fieldType, nullable
}

// finds the end of the type definition, handling nested brackets and multi-word types
func findTypeEnd(s string) int {
	dt := &depthTracker{}
	inQuotes := false

	if strings.HasPrefix(s, "INTERVAL") {
		// Match INTERVAL types at the start
		if m := intervalTypeRegex.FindStringIndex(s); m != nil {
			return m[1]
		}
	}
	for i := 0; i < len(s); i++ {
		char := s[i]
		switch char {
		case '"':
			// Handle quoted strings, but check for escaped quotes
			if i > 0 && s[i-1] == '\\' {
				// This is an escaped quote, continue
			} else {
				// Toggle quote state
				inQuotes = !inQuotes
			}
		case '<', '>':
			if !inQuotes {
				dt.updateDepth(char)
			}
		case ' ':
			if !inQuotes && dt.isAtTopLevel() {
				return i
			}
		}
	}
	return len(s)
}

// parseMapKeyValue parses the key and value types from a MAP type string
func parseMapKeyValue(mapContent string) (keyType, valueType string, err error) {
	var currentType strings.Builder
	dt := &depthTracker{}

	for i := 0; i < len(mapContent); i++ {
		char := mapContent[i]

		switch char {
		case '<', '>', '(', ')':
			dt.updateDepth(char)
			currentType.WriteByte(char)
		case ',':
			if dt.isAtTopLevel() {
				// This comma separates key and value types
				keyType = strings.TrimSpace(currentType.String())
				valueType = strings.TrimSpace(mapContent[i+1:])
				return keyType, valueType, nil
			} else {
				// This comma is inside nested brackets or parentheses, so it's part of the type
				currentType.WriteByte(char)
			}
		default:
			currentType.WriteByte(char)
		}
	}

	return "", "", fmt.Errorf("no comma found to separate key and value types")
}

func getArrowTypeFromColumnInfo(col sql.ColumnInfo) (arrow.DataType, error) {
	if arrowType, ok := basicTypeToArrowTypeMap[col.TypeName]; ok {
		return arrowType, nil
	}
	switch col.TypeName {
	case sql.ColumnInfoTypeNameDecimal:
		precision, scale := col.TypePrecision, col.TypeScale
		if precision == 0 {
			return getArrowTypeFromStringType(strings.TrimSpace(col.TypeText))
		}
		if precision <= 38 {
			return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
		}
		return &arrow.Decimal256Type{Precision: int32(precision), Scale: int32(scale)}, nil
	// todo(jasonlin45): Add support for TIMESTAMP_NTZ
	case sql.ColumnInfoTypeNameTimestamp:
		return &arrow.TimestampType{
			Unit:     arrow.Microsecond,
			TimeZone: "Etc/UTC", // todo(jasonlin45): Support session timezone
		}, nil
	case sql.ColumnInfoTypeNameArray,
		sql.ColumnInfoTypeNameMap,
		sql.ColumnInfoTypeNameStruct,
		sql.ColumnInfoTypeNameInterval:
		return getArrowTypeFromStringType(strings.TrimSpace(col.TypeText))
	default:
		return getArrowTypeFromStringType(strings.TrimSpace(col.TypeText))
	}
}
