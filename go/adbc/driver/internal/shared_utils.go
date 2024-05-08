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

package internal

import (
	"context"
	"database/sql"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
)

const (
	Unique     = "UNIQUE"
	PrimaryKey = "PRIMARY KEY"
	ForeignKey = "FOREIGN KEY"
)

type CatalogAndSchema struct {
	Catalog, Schema string
}

type CatalogSchemaTable struct {
	Catalog, Schema, Table string
}

type CatalogSchemaTableColumn struct {
	Catalog, Schema, Table, Column string
}

type TableInfo struct {
	Name, TableType string
	Schema          *arrow.Schema
}

type Metadata struct {
	Created                                                                                                                      time.Time
	ColName, DataType, Dbname, Kind, Schema, TblName, TblType, IdentGen, IdentIncrement, Comment, ConstraintName, ConstraintType sql.NullString
	OrdinalPos                                                                                                                   int
	NumericPrec, NumericPrecRadix, NumericScale, DatetimePrec                                                                    sql.NullInt16
	IsNullable, IsIdent                                                                                                          bool
	CharMaxLength, CharOctetLength                                                                                               sql.NullInt32
}

type UsageSchema struct {
	ForeignKeyCatalog, ForeignKeyDbSchema, ForeignKeyTable, ForeignKeyColName string
}

type ConstraintSchema struct {
	ConstraintName, ConstraintType string
	ConstraintColumnNames          []string
	ConstraintColumnUsages         []UsageSchema
}

type GetObjDBSchemasFn func(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, metadataRecords []Metadata) (map[string][]string, error)
type GetObjTablesFn func(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, tableName *string, columnName *string, tableType []string, metadataRecords []Metadata) (map[CatalogAndSchema][]TableInfo, error)
type SchemaToTableInfo = map[CatalogAndSchema][]TableInfo

// Helper function that compiles a SQL-style pattern (%, _) to a regex
func PatternToRegexp(pattern *string) (*regexp.Regexp, error) {
	if pattern == nil {
		return nil, nil
	}

	var builder strings.Builder
	if _, err := builder.WriteString("(?i)^"); err != nil {
		return nil, err
	}
	for _, c := range *pattern {
		switch {
		case c == rune('_'):
			if _, err := builder.WriteString("."); err != nil {
				return nil, err
			}
		case c == rune('%'):
			if _, err := builder.WriteString(".*"); err != nil {
				return nil, err
			}
		default:
			if _, err := builder.WriteString(regexp.QuoteMeta(string([]rune{c}))); err != nil {
				return nil, err
			}
		}
	}
	if _, err := builder.WriteString("$"); err != nil {
		return nil, err
	}
	return regexp.Compile(builder.String())
}

// Helper to store state needed for GetObjects
type GetObjects struct {
	Ctx        context.Context
	Depth      adbc.ObjectDepth
	Catalog    *string
	DbSchema   *string
	TableName  *string
	ColumnName *string
	TableType  []string

	builder           *array.RecordBuilder
	schemaLookup      map[string][]string
	tableLookup       map[CatalogAndSchema][]TableInfo
	ConstraintLookup  map[CatalogSchemaTable][]ConstraintSchema
	MetadataRecords   []Metadata
	catalogPattern    *regexp.Regexp
	columnNamePattern *regexp.Regexp

	catalogNameBuilder           *array.StringBuilder
	catalogDbSchemasBuilder      *array.ListBuilder
	catalogDbSchemasItems        *array.StructBuilder
	dbSchemaNameBuilder          *array.StringBuilder
	dbSchemaTablesBuilder        *array.ListBuilder
	dbSchemaTablesItems          *array.StructBuilder
	tableNameBuilder             *array.StringBuilder
	tableTypeBuilder             *array.StringBuilder
	tableColumnsBuilder          *array.ListBuilder
	tableColumnsItems            *array.StructBuilder
	columnNameBuilder            *array.StringBuilder
	ordinalPositionBuilder       *array.Int32Builder
	remarksBuilder               *array.StringBuilder
	xdbcDataTypeBuilder          *array.Int16Builder
	xdbcTypeNameBuilder          *array.StringBuilder
	xdbcColumnSizeBuilder        *array.Int32Builder
	xdbcDecimalDigitsBuilder     *array.Int16Builder
	xdbcNumPrecRadixBuilder      *array.Int16Builder
	xdbcNullableBuilder          *array.Int16Builder
	xdbcColumnDefBuilder         *array.StringBuilder
	xdbcSqlDataTypeBuilder       *array.Int16Builder
	xdbcDatetimeSubBuilder       *array.Int16Builder
	xdbcCharOctetLengthBuilder   *array.Int32Builder
	xdbcIsNullableBuilder        *array.StringBuilder
	xdbcScopeCatalogBuilder      *array.StringBuilder
	xdbcScopeSchemaBuilder       *array.StringBuilder
	xdbcScopeTableBuilder        *array.StringBuilder
	xdbcIsAutoincrementBuilder   *array.BooleanBuilder
	xdbcIsGeneratedcolumnBuilder *array.BooleanBuilder
	tableConstraintsBuilder      *array.ListBuilder
	tableConstraintsItems        *array.StructBuilder
	constraintNameBuilder        *array.StringBuilder
	constraintTypeBuilder        *array.StringBuilder
	constraintColumnNameBuilder  *array.ListBuilder
	constraintColumnUsageBuilder *array.ListBuilder
	constraintColumnNameItems    *array.StringBuilder
	constraintColumnUsageItems   *array.StructBuilder
	columnUsageCatalogBuilder    *array.StringBuilder
	columnUsageSchemaBuilder     *array.StringBuilder
	columnUsageTableBuilder      *array.StringBuilder
	columnUsageColumnBuilder     *array.StringBuilder
}

func (g *GetObjects) Init(mem memory.Allocator, getObj GetObjDBSchemasFn, getTbls GetObjTablesFn) error {
	if catalogToDbSchemas, err := getObj(g.Ctx, g.Depth, g.Catalog, g.DbSchema, g.MetadataRecords); err != nil {
		return err
	} else {
		g.schemaLookup = catalogToDbSchemas
	}

	if tableLookup, err := getTbls(g.Ctx, g.Depth, g.Catalog, g.DbSchema, g.TableName, g.ColumnName, g.TableType, g.MetadataRecords); err != nil {
		return err
	} else {
		g.tableLookup = tableLookup
	}

	if catalogPattern, err := PatternToRegexp(g.Catalog); err != nil {
		return adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInvalidArgument,
		}
	} else {
		g.catalogPattern = catalogPattern
	}
	if columnNamePattern, err := PatternToRegexp(g.ColumnName); err != nil {
		return adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInvalidArgument,
		}
	} else {
		g.columnNamePattern = columnNamePattern
	}

	g.builder = array.NewRecordBuilder(mem, adbc.GetObjectsSchema)
	g.catalogNameBuilder = g.builder.Field(0).(*array.StringBuilder)
	g.catalogDbSchemasBuilder = g.builder.Field(1).(*array.ListBuilder)
	g.catalogDbSchemasItems = g.catalogDbSchemasBuilder.ValueBuilder().(*array.StructBuilder)
	g.dbSchemaNameBuilder = g.catalogDbSchemasItems.FieldBuilder(0).(*array.StringBuilder)
	g.dbSchemaTablesBuilder = g.catalogDbSchemasItems.FieldBuilder(1).(*array.ListBuilder)
	g.dbSchemaTablesItems = g.dbSchemaTablesBuilder.ValueBuilder().(*array.StructBuilder)
	g.tableNameBuilder = g.dbSchemaTablesItems.FieldBuilder(0).(*array.StringBuilder)
	g.tableTypeBuilder = g.dbSchemaTablesItems.FieldBuilder(1).(*array.StringBuilder)
	g.tableColumnsBuilder = g.dbSchemaTablesItems.FieldBuilder(2).(*array.ListBuilder)
	g.tableColumnsItems = g.tableColumnsBuilder.ValueBuilder().(*array.StructBuilder)
	g.columnNameBuilder = g.tableColumnsItems.FieldBuilder(0).(*array.StringBuilder)
	g.ordinalPositionBuilder = g.tableColumnsItems.FieldBuilder(1).(*array.Int32Builder)
	g.remarksBuilder = g.tableColumnsItems.FieldBuilder(2).(*array.StringBuilder)
	g.xdbcDataTypeBuilder = g.tableColumnsItems.FieldBuilder(3).(*array.Int16Builder)
	g.xdbcTypeNameBuilder = g.tableColumnsItems.FieldBuilder(4).(*array.StringBuilder)
	g.xdbcColumnSizeBuilder = g.tableColumnsItems.FieldBuilder(5).(*array.Int32Builder)
	g.xdbcDecimalDigitsBuilder = g.tableColumnsItems.FieldBuilder(6).(*array.Int16Builder)
	g.xdbcNumPrecRadixBuilder = g.tableColumnsItems.FieldBuilder(7).(*array.Int16Builder)
	g.xdbcNullableBuilder = g.tableColumnsItems.FieldBuilder(8).(*array.Int16Builder)
	g.xdbcColumnDefBuilder = g.tableColumnsItems.FieldBuilder(9).(*array.StringBuilder)
	g.xdbcSqlDataTypeBuilder = g.tableColumnsItems.FieldBuilder(10).(*array.Int16Builder)
	g.xdbcDatetimeSubBuilder = g.tableColumnsItems.FieldBuilder(11).(*array.Int16Builder)
	g.xdbcCharOctetLengthBuilder = g.tableColumnsItems.FieldBuilder(12).(*array.Int32Builder)
	g.xdbcIsNullableBuilder = g.tableColumnsItems.FieldBuilder(13).(*array.StringBuilder)
	g.xdbcScopeCatalogBuilder = g.tableColumnsItems.FieldBuilder(14).(*array.StringBuilder)
	g.xdbcScopeSchemaBuilder = g.tableColumnsItems.FieldBuilder(15).(*array.StringBuilder)
	g.xdbcScopeTableBuilder = g.tableColumnsItems.FieldBuilder(16).(*array.StringBuilder)
	g.xdbcIsAutoincrementBuilder = g.tableColumnsItems.FieldBuilder(17).(*array.BooleanBuilder)
	g.xdbcIsGeneratedcolumnBuilder = g.tableColumnsItems.FieldBuilder(18).(*array.BooleanBuilder)
	g.tableConstraintsBuilder = g.dbSchemaTablesItems.FieldBuilder(3).(*array.ListBuilder)
	g.tableConstraintsItems = g.tableConstraintsBuilder.ValueBuilder().(*array.StructBuilder)
	g.constraintNameBuilder = g.tableConstraintsItems.FieldBuilder(0).(*array.StringBuilder)
	g.constraintTypeBuilder = g.tableConstraintsItems.FieldBuilder(1).(*array.StringBuilder)
	g.constraintColumnNameBuilder = g.tableConstraintsItems.FieldBuilder(2).(*array.ListBuilder)
	g.constraintColumnNameItems = g.constraintColumnNameBuilder.ValueBuilder().(*array.StringBuilder)
	g.constraintColumnUsageBuilder = g.tableConstraintsItems.FieldBuilder(3).(*array.ListBuilder)
	g.constraintColumnUsageItems = g.constraintColumnUsageBuilder.ValueBuilder().(*array.StructBuilder)
	g.columnUsageCatalogBuilder = g.constraintColumnUsageItems.FieldBuilder(0).(*array.StringBuilder)
	g.columnUsageSchemaBuilder = g.constraintColumnUsageItems.FieldBuilder(1).(*array.StringBuilder)
	g.columnUsageTableBuilder = g.constraintColumnUsageItems.FieldBuilder(2).(*array.StringBuilder)
	g.columnUsageColumnBuilder = g.constraintColumnUsageItems.FieldBuilder(3).(*array.StringBuilder)

	return nil
}

func (g *GetObjects) Release() {
	g.builder.Release()
}

func (g *GetObjects) Finish() (array.RecordReader, error) {
	record := g.builder.NewRecord()
	defer record.Release()

	result, err := array.NewRecordReader(g.builder.Schema(), []arrow.Record{record})
	if err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInternal,
		}
	}
	return result, nil
}

func (g *GetObjects) AppendCatalog(catalogName string) {
	if g.catalogPattern != nil && !g.catalogPattern.MatchString(catalogName) {
		return
	}
	g.catalogNameBuilder.Append(catalogName)

	if g.Depth == adbc.ObjectDepthCatalogs {
		g.catalogDbSchemasBuilder.AppendNull()
		return
	}

	g.catalogDbSchemasBuilder.Append(true)

	for _, dbSchemaName := range g.schemaLookup[catalogName] {
		g.appendDbSchema(catalogName, dbSchemaName)
	}
}

func (g *GetObjects) appendDbSchema(catalogName, dbSchemaName string) {
	g.dbSchemaNameBuilder.Append(dbSchemaName)
	g.catalogDbSchemasItems.Append(true)

	if g.Depth == adbc.ObjectDepthDBSchemas {
		g.dbSchemaTablesBuilder.AppendNull()
		return
	}
	g.dbSchemaTablesBuilder.Append(true)

	catalogAndSchema := CatalogAndSchema{Catalog: catalogName, Schema: dbSchemaName}
	for _, tableInfo := range g.tableLookup[catalogAndSchema] {
		g.appendTableInfo(tableInfo, catalogAndSchema)
	}
}

func (g *GetObjects) appendTableInfo(tableInfo TableInfo, catalogAndSchema CatalogAndSchema) {
	g.tableNameBuilder.Append(tableInfo.Name)
	g.tableTypeBuilder.Append(tableInfo.TableType)
	g.dbSchemaTablesItems.Append(true)

	g.appendTableConstraints(tableInfo, catalogAndSchema)
	g.appendColumnsInfo(tableInfo)
}

func (g *GetObjects) appendTableConstraints(tableInfo TableInfo, catalogAndSchema CatalogAndSchema) {
	if g.Depth == adbc.ObjectDepthTables {
		g.tableConstraintsBuilder.AppendNull()
		return
	}

	g.tableConstraintsBuilder.Append(true)
	if len(g.ConstraintLookup) == 0 {
		// Empty list
		return
	}

	catalogSchemaTable := CatalogSchemaTable{Catalog: catalogAndSchema.Catalog, Schema: catalogAndSchema.Schema, Table: tableInfo.Name}
	constraintSchemaData, exists := g.ConstraintLookup[catalogSchemaTable]

	if exists {
		for _, data := range constraintSchemaData {
			g.constraintNameBuilder.Append(data.ConstraintName)
			g.constraintTypeBuilder.Append(data.ConstraintType)
			g.appendConstraintColumns(data)
			g.appendConstraintColumnUsages(data)
			g.tableConstraintsItems.Append(true)
		}
	}
}

func (g *GetObjects) appendConstraintColumns(constraintSchema ConstraintSchema) {
	if len(constraintSchema.ConstraintColumnNames) == 0 {
		g.constraintColumnNameBuilder.AppendNull()
	} else {
		g.constraintColumnNameBuilder.Append(true)
		for _, columnName := range constraintSchema.ConstraintColumnNames {
			g.constraintColumnNameItems.Append(columnName)
		}
	}
}

func (g *GetObjects) appendConstraintColumnUsages(constraintSchema ConstraintSchema) {
	if len(constraintSchema.ConstraintColumnUsages) == 0 {
		g.constraintColumnUsageBuilder.AppendNull()
	} else {
		g.constraintColumnUsageBuilder.Append(true)
		for _, columnUsages := range constraintSchema.ConstraintColumnUsages {
			g.columnUsageCatalogBuilder.Append(columnUsages.ForeignKeyCatalog)
			g.columnUsageSchemaBuilder.Append(columnUsages.ForeignKeyDbSchema)
			g.columnUsageTableBuilder.Append(columnUsages.ForeignKeyTable)
			g.columnUsageColumnBuilder.Append(columnUsages.ForeignKeyColName)
			g.constraintColumnUsageItems.Append(true)
		}
	}
}

func (g *GetObjects) appendColumnsInfo(tableInfo TableInfo) {
	if g.Depth == adbc.ObjectDepthTables {
		g.tableColumnsBuilder.AppendNull()
		return
	}

	g.tableColumnsBuilder.Append(true)

	if tableInfo.Schema == nil {
		return
	}

	for colIndex, column := range tableInfo.Schema.Fields() {
		if g.columnNamePattern != nil && !g.columnNamePattern.MatchString(column.Name) {
			continue
		}
		g.columnNameBuilder.Append(column.Name)
		if !column.HasMetadata() {
			g.ordinalPositionBuilder.Append(int32(colIndex + 1))
			g.remarksBuilder.AppendNull()

			g.xdbcDataTypeBuilder.AppendNull()
			g.xdbcTypeNameBuilder.AppendNull()
			g.xdbcNullableBuilder.AppendNull()
			g.xdbcIsNullableBuilder.AppendNull()
			g.xdbcColumnSizeBuilder.AppendNull()
			g.xdbcDecimalDigitsBuilder.AppendNull()
			g.xdbcNumPrecRadixBuilder.AppendNull()
			g.xdbcCharOctetLengthBuilder.AppendNull()
			g.xdbcDatetimeSubBuilder.AppendNull()
			g.xdbcSqlDataTypeBuilder.AppendNull()
		} else {
			if remark, ok := column.Metadata.GetValue("COMMENT"); ok {
				g.remarksBuilder.Append(remark)
			} else {
				g.remarksBuilder.AppendNull()
			}

			if typeName, ok := column.Metadata.GetValue("XDBC_TYPE_NAME"); ok {
				g.xdbcTypeNameBuilder.Append(typeName)
			} else {
				g.xdbcTypeNameBuilder.AppendNull()
			}

			if strNullable, ok := column.Metadata.GetValue("XDBC_NULLABLE"); ok {
				nullable, _ := strconv.ParseBool(strNullable)
				g.xdbcNullableBuilder.Append(boolToInt16(nullable))
			} else {
				g.xdbcNullableBuilder.AppendNull()
			}

			if strIsNullable, ok := column.Metadata.GetValue("XDBC_IS_NULLABLE"); ok {
				g.xdbcIsNullableBuilder.Append(strIsNullable)
			} else {
				g.xdbcIsNullableBuilder.AppendNull()
			}

			if strDataTypeId, ok := column.Metadata.GetValue("XDBC_DATA_TYPE"); ok {
				dataTypeId64, _ := strconv.ParseInt(strDataTypeId, 10, 16)
				g.xdbcDataTypeBuilder.Append(int16(dataTypeId64))

			} else {
				g.xdbcDataTypeBuilder.AppendNull()
			}

			if strSqlDataTypeId, ok := column.Metadata.GetValue("XDBC_SQL_DATA_TYPE"); ok {
				sqlDataTypeId64, _ := strconv.ParseInt(strSqlDataTypeId, 10, 16)
				g.xdbcSqlDataTypeBuilder.Append(int16(sqlDataTypeId64))
			} else {
				g.xdbcSqlDataTypeBuilder.AppendNull()
			}

			if strPrecision, ok := column.Metadata.GetValue("XDBC_PRECISION"); ok { // for numeric values
				precision64, _ := strconv.ParseInt(strPrecision, 10, 32)
				g.xdbcColumnSizeBuilder.Append(int32(precision64))
			} else if strCharLimit, ok := column.Metadata.GetValue("CHARACTER_MAXIMUM_LENGTH"); ok { // for text values
				charLimit64, _ := strconv.ParseInt(strCharLimit, 10, 32)
				g.xdbcColumnSizeBuilder.Append(int32(charLimit64))
			} else {
				g.xdbcColumnSizeBuilder.AppendNull()
			}

			if strScale, ok := column.Metadata.GetValue("XDBC_SCALE"); ok {
				scale64, _ := strconv.ParseInt(strScale, 10, 16)
				g.xdbcDecimalDigitsBuilder.Append(int16(scale64))
			} else {
				g.xdbcDecimalDigitsBuilder.AppendNull()
			}

			if strPrecRadix, ok := column.Metadata.GetValue("XDBC_NUM_PREC_RADIX"); ok {
				precRadix64, _ := strconv.ParseInt(strPrecRadix, 10, 16)
				g.xdbcNumPrecRadixBuilder.Append(int16(precRadix64))
			} else {
				g.xdbcNumPrecRadixBuilder.AppendNull()
			}

			if strCharOctetLen, ok := column.Metadata.GetValue("XDBC_CHAR_OCTET_LENGTH"); ok {
				charOctLen64, _ := strconv.ParseInt(strCharOctetLen, 10, 32)
				g.xdbcCharOctetLengthBuilder.Append(int32(charOctLen64))
			} else {
				g.xdbcCharOctetLengthBuilder.AppendNull()
			}

			if strDateTimeSub, ok := column.Metadata.GetValue("XDBC_DATETIME_SUB"); ok {
				dateTimeSub64, _ := strconv.ParseInt(strDateTimeSub, 10, 16)
				g.xdbcDatetimeSubBuilder.Append(int16(dateTimeSub64))
			} else {
				g.xdbcDatetimeSubBuilder.AppendNull()
			}

			pos := int32(colIndex + 1)

			if ordinal, ok := column.Metadata.GetValue("ORDINAL_POSITION"); ok {
				v, err := strconv.ParseInt(ordinal, 10, 32)
				if err == nil {
					pos = int32(v)
				}
			}
			g.ordinalPositionBuilder.Append(pos)
		}

		g.xdbcColumnDefBuilder.AppendNull()
		g.xdbcScopeCatalogBuilder.AppendNull()
		g.xdbcScopeSchemaBuilder.AppendNull()
		g.xdbcScopeTableBuilder.AppendNull()
		g.xdbcIsAutoincrementBuilder.AppendNull()
		g.xdbcIsGeneratedcolumnBuilder.AppendNull()

		g.tableColumnsItems.Append(true)
	}
}

func boolToInt16(b bool) int16 {
	if b {
		return 1
	}
	return 0
}

// The JDBC/ODBC-defined type of any object.
// All the values here are the sames as in the JDBC and ODBC specs.
type XdbcDataType int32

const (
	XdbcDataType_XDBC_UNKNOWN_TYPE  XdbcDataType = 0
	XdbcDataType_XDBC_CHAR          XdbcDataType = 1
	XdbcDataType_XDBC_NUMERIC       XdbcDataType = 2
	XdbcDataType_XDBC_DECIMAL       XdbcDataType = 3
	XdbcDataType_XDBC_INTEGER       XdbcDataType = 4
	XdbcDataType_XDBC_SMALLINT      XdbcDataType = 5
	XdbcDataType_XDBC_FLOAT         XdbcDataType = 6
	XdbcDataType_XDBC_REAL          XdbcDataType = 7
	XdbcDataType_XDBC_DOUBLE        XdbcDataType = 8
	XdbcDataType_XDBC_DATETIME      XdbcDataType = 9
	XdbcDataType_XDBC_INTERVAL      XdbcDataType = 10
	XdbcDataType_XDBC_VARCHAR       XdbcDataType = 12
	XdbcDataType_XDBC_DATE          XdbcDataType = 91
	XdbcDataType_XDBC_TIME          XdbcDataType = 92
	XdbcDataType_XDBC_TIMESTAMP     XdbcDataType = 93
	XdbcDataType_XDBC_LONGVARCHAR   XdbcDataType = -1
	XdbcDataType_XDBC_BINARY        XdbcDataType = -2
	XdbcDataType_XDBC_VARBINARY     XdbcDataType = -3
	XdbcDataType_XDBC_LONGVARBINARY XdbcDataType = -4
	XdbcDataType_XDBC_BIGINT        XdbcDataType = -5
	XdbcDataType_XDBC_TINYINT       XdbcDataType = -6
	XdbcDataType_XDBC_BIT           XdbcDataType = -7
	XdbcDataType_XDBC_WCHAR         XdbcDataType = -8
	XdbcDataType_XDBC_WVARCHAR      XdbcDataType = -9
)
