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

const DbxSchemaTypeKey = "type_name"

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

func ClusterModeSchemaToArrowSchema(dbx_schema []map[string]interface{}) (*arrow.Schema, error) {
	fields := make([]arrow.Field, len(dbx_schema))
	for i, col := range dbx_schema {
		var arrowType arrow.DataType
		colType := strings.Trim(col[DbxSchemaTypeKey].(string), "\"")
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
				precision, _ := strconv.ParseInt(matches[1], 10, 32)
				scale, _ := strconv.ParseInt(matches[2], 10, 32)
				arrowType = &arrow.Decimal256Type{Precision: int32(precision), Scale: int32(scale)}
			} else {
				arrowType = &arrow.Decimal256Type{}
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
				MetadataKeyDatabricksType: colType,
			}),
		}
	}
	// TODO: include relevant metadata from dbrx into the Arrow schema
	return arrow.NewSchema(fields, nil), nil
}

func ResultSchemaToArrowSchema(dbx_schema *sql.ResultSchema) (*arrow.Schema, error) {
	fields := make([]arrow.Field, dbx_schema.ColumnCount)
	for i, col := range dbx_schema.Columns {
		arrowType, err := getArrowTypeFromColumnInfo(col)
		if err != nil {
			return nil, err
		}
		fields[i] = arrow.Field{
			Name:     col.Name,
			Type:     arrowType,
			Nullable: true,
			Metadata: arrow.MetadataFrom(map[string]string{
				MetadataKeyDatabricksType: string(col.TypeName),
				"position":                col.Name,
				"type_text":               col.TypeText,
			}),
		}
	}
	metadata := arrow.MetadataFrom(map[string]string{
		"column_count": strconv.Itoa(dbx_schema.ColumnCount),
	})
	return arrow.NewSchema(fields, &metadata), nil
}

func getArrowTypeFromColumnInfo(col sql.ColumnInfo) (arrow.DataType, error) {
	if arrowType, ok := basicTypeToArrowTypeMap[col.TypeName]; ok {
		return arrowType, nil
	}
	switch col.TypeName {
	case sql.ColumnInfoTypeNameDecimal:
		precision, scale := col.TypePrecision, col.TypeScale
		if precision == 0 {
			// Mostly used by the recursive case (array, map, etc)
			// "DECIMAL(p,s)"
			if matches := regexp.MustCompile(`DECIMAL\((\d+),(\d+)\)`).FindStringSubmatch(col.TypeText); matches != nil {
				var err error
				precision, err = strconv.Atoi(matches[1])
				if err != nil {
					return nil, fmt.Errorf("invalid decimal precision: %v", err)
				}
				scale, err = strconv.Atoi(matches[2])
				if err != nil {
					return nil, fmt.Errorf("invalid decimal scale: %v", err)
				}
			}
		}
		if precision <= 38 {
			return &arrow.Decimal128Type{Precision: int32(precision), Scale: int32(scale)}, nil
		}
		return &arrow.Decimal256Type{Precision: int32(precision), Scale: int32(scale)}, nil
	// todo(jasonlin45): Add support for TIMESTAMP_NTZ
	case sql.ColumnInfoTypeNameTimestamp:
		return &arrow.TimestampType{
			Unit:     arrow.Microsecond,
			TimeZone: "UTC", // todo(jasonlin45): Support session timezone
		}, nil
	case sql.ColumnInfoTypeNameArray:
		// Only available from the full SQL type spec
		// parse from "ARRAY<elementType>"
		if matches := regexp.MustCompile(`ARRAY<(.+)>`).FindStringSubmatch(col.TypeText); matches != nil {
			elementType := matches[1]
			// Create a temporary ColumnInfo to recursively parse the element type
			elementCol := sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeName(elementType),
			}

			elementArrowType, err := getArrowTypeFromColumnInfo(elementCol)
			if err != nil {
				return nil, fmt.Errorf("failed to parse array element type: %w", err)
			}
			return arrow.ListOf(elementArrowType), nil
		}
		return nil, fmt.Errorf("invalid array type format: %v", col.TypeName)
	case sql.ColumnInfoTypeNameMap:
		// "MAP<keyType,valueType>"
		if matches := regexp.MustCompile(`MAP<(.+),(.+)>`).FindStringSubmatch(col.TypeText); matches != nil {
			keyType := matches[1]
			valueType := matches[2]

			// temporary ColumnInfo structs to recursively parse the types
			keyCol := sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeName(keyType),
			}
			valueCol := sql.ColumnInfo{
				TypeName: sql.ColumnInfoTypeName(valueType),
			}

			keyArrowType, err := getArrowTypeFromColumnInfo(keyCol)
			if err != nil {
				return nil, fmt.Errorf("failed to parse map key type: %w", err)
			}
			valueArrowType, err := getArrowTypeFromColumnInfo(valueCol)
			if err != nil {
				return nil, fmt.Errorf("failed to parse map value type: %w", err)
			}

			return arrow.MapOf(keyArrowType, valueArrowType), nil
		}
		return nil, fmt.Errorf("invalid map type format: %v", col.TypeName)
	case sql.ColumnInfoTypeNameStruct:
		// todo(jasonlin45): Full support for struct field. No support for NOT NULL, COLLATE or COMMENT available
		// STRUCT < [fieldName [:] fieldType [NOT NULL] [COLLATE collationName] [COMMENT str] [, …] ] >
		if matches := regexp.MustCompile(`STRUCT<(.+)>`).FindStringSubmatch(col.TypeText); matches != nil {
			fieldsStr := matches[1]

			fields := []string{}

			// fields are comma separated, nested types are in parentheses
			fieldRegex := regexp.MustCompile(`([^,()]+(?:\([^()]*\)[^,()]*)*)`)
			matches := fieldRegex.FindAllString(fieldsStr, -1)
			for _, field := range matches {
				fields = append(fields, strings.TrimSpace(field))
			}

			// Parse each field
			arrowFields := make([]arrow.Field, 0, len(fields))
			for _, field := range fields {
				// Split on first colon to get field name and type
				parts := strings.SplitN(field, ":", 2)
				if len(parts) != 2 {
					return nil, fmt.Errorf("invalid struct field format: %s", field)
				}
				fieldName := strings.TrimSpace(parts[0])
				fieldType := strings.TrimSpace(parts[1])

				// Create temporary ColumnInfo to parse the field type
				fieldCol := sql.ColumnInfo{
					TypeName: sql.ColumnInfoTypeName(fieldType),
				}

				fieldArrowType, err := getArrowTypeFromColumnInfo(fieldCol)
				if err != nil {
					return nil, fmt.Errorf("failed to parse struct field type %s: %w", fieldName, err)
				}

				arrowFields = append(arrowFields, arrow.Field{
					Name:     fieldName,
					Type:     fieldArrowType,
					Nullable: true,
				})
			}

			return arrow.StructOf(arrowFields...), nil
		}
		return nil, fmt.Errorf("invalid struct type format: %v", col.TypeName)
	case sql.ColumnInfoTypeNameInterval:
		/* Syntax
		INTERVAL { yearMonthIntervalQualifier | dayTimeIntervalQualifier }

		yearMonthIntervalQualifier
		 { YEAR [TO MONTH] |
		   MONTH }

		dayTimeIntervalQualifier
		 { DAY [TO { HOUR | MINUTE | SECOND } ] |
		   HOUR [TO { MINUTE | SECOND } ] |
		   MINUTE [TO SECOND] |
		   SECOND }
		*/
		// Helper function to map interval units to Arrow type
		getIntervalType := func(startUnit, endUnit string) (arrow.DataType, error) {
			switch {
			case startUnit == "YEAR" && (endUnit == "" || endUnit == "MONTH"):
				return arrow.FixedWidthTypes.MonthInterval, nil
			case startUnit == "DAY" && (endUnit == "" || endUnit == "HOUR" || endUnit == "MINUTE" || endUnit == "SECOND"),
				startUnit == "HOUR" && (endUnit == "" || endUnit == "MINUTE" || endUnit == "SECOND"),
				startUnit == "MINUTE" && (endUnit == "" || endUnit == "SECOND"),
				startUnit == "SECOND":
				return arrow.FixedWidthTypes.DayTimeInterval, nil
			default:
				return nil, fmt.Errorf("unsupported interval qualifier: %s TO %s", startUnit, endUnit)
			}
		}

		// First try to use TypeIntervalType if available
		if col.TypeIntervalType != "" {
			// Split into start and end units
			parts := strings.Split(col.TypeIntervalType, " TO ")
			startUnit := parts[0]
			endUnit := ""
			if len(parts) > 1 {
				endUnit = parts[1]
			}
			return getIntervalType(startUnit, endUnit)
		}

		// Fall back to parsing TypeText for recursive calls
		if matches := regexp.MustCompile(`INTERVAL\s+(\w+)(?:\s+TO\s+(\w+))?`).FindStringSubmatch(col.TypeText); matches != nil {
			startUnit := strings.ToUpper(matches[1])
			endUnit := ""
			if len(matches) > 2 {
				endUnit = strings.ToUpper(matches[2])
			}
			return getIntervalType(startUnit, endUnit)
		}
		return nil, fmt.Errorf("invalid interval type format: %v", col.TypeText)
	default:
		return nil, fmt.Errorf("unsupported type: %v", col.TypeName)
	}
}
