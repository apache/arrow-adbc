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
	"regexp"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.30.0"
	"go.opentelemetry.io/otel/trace"
)

const (
	Unique     = "UNIQUE"
	PrimaryKey = "PRIMARY KEY"
	ForeignKey = "FOREIGN KEY"
)

var (
	AcceptAll = regexp.MustCompile(".*")
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

type UsageSchema struct {
	ForeignKeyCatalog, ForeignKeyDbSchema, ForeignKeyTable, ForeignKeyColName string
}

type ConstraintSchema struct {
	ConstraintName, ConstraintType string
	ConstraintColumnNames          []string
	ConstraintColumnUsages         []UsageSchema
}

type GetObjDBSchemasFn func(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string) (map[string][]string, error)
type GetObjTablesFn func(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, tableName *string, columnName *string, tableType []string) (map[CatalogAndSchema][]TableInfo, error)
type SchemaToTableInfo = map[CatalogAndSchema][]TableInfo
type MetadataToBuilders = map[string]array.Builder
type MetadataHandlers = func(md arrow.Field, builder array.Builder)
type MetadataToHandlers = map[string]MetadataHandlers

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

	metadataHandler XdbcMetadataBuilder
}

func (g *GetObjects) Init(mem memory.Allocator, getObj GetObjDBSchemasFn, getTbls GetObjTablesFn, mdHandler XdbcMetadataBuilder) error {
	if catalogToDbSchemas, err := getObj(g.Ctx, g.Depth, g.Catalog, g.DbSchema); err != nil {
		return err
	} else {
		g.schemaLookup = catalogToDbSchemas
	}

	if tableLookup, err := getTbls(g.Ctx, g.Depth, g.Catalog, g.DbSchema, g.TableName, g.ColumnName, g.TableType); err != nil {
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

	if mdHandler == nil {
		mdHandler = &DefaultXdbcMetadataBuilder{}
	}
	g.metadataHandler = mdHandler

	return nil
}

func (g *GetObjects) Release() {
	g.builder.Release()
}

func (g *GetObjects) Finish() (array.RecordReader, error) {
	record := g.builder.NewRecordBatch()
	defer record.Release()

	result, err := array.NewRecordReader(g.builder.Schema(), []arrow.RecordBatch{record})
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

		g.metadataHandler.SetMetadata(column.Metadata)

		pos := int32(colIndex + 1)

		g.columnNameBuilder.Append(column.Name)
		g.metadataHandler.SetOrdinalPosition(pos, g.ordinalPositionBuilder)
		g.metadataHandler.SetRemarks(g.remarksBuilder)
		g.metadataHandler.SetXdbcDataType(column.Type, g.xdbcDataTypeBuilder)
		g.metadataHandler.SetXdbcTypeName(g.xdbcTypeNameBuilder)
		g.metadataHandler.SetXdbcColumnSize(g.xdbcColumnSizeBuilder)
		g.metadataHandler.SetXdbcDecimalDigits(g.xdbcDecimalDigitsBuilder)
		g.metadataHandler.SetXdbcNumPrecRadix(g.xdbcNumPrecRadixBuilder)
		g.metadataHandler.SetXdbcNullable(g.xdbcNullableBuilder)
		g.metadataHandler.SetXdbcColumnDef(g.xdbcColumnDefBuilder)
		g.metadataHandler.SetXdbcSqlDataType(column.Type, g.xdbcSqlDataTypeBuilder)
		g.metadataHandler.SetXdbcDatetimeSub(g.xdbcDatetimeSubBuilder)
		g.metadataHandler.SetXdbcCharOctetLength(g.xdbcCharOctetLengthBuilder)
		g.metadataHandler.SetXdbcIsNullable(g.xdbcIsNullableBuilder)
		g.metadataHandler.SetXdbcScopeCatalog(g.xdbcScopeCatalogBuilder)
		g.metadataHandler.SetXdbcScopeSchema(g.xdbcScopeSchemaBuilder)
		g.metadataHandler.SetXdbcScopeTable(g.xdbcScopeTableBuilder)
		g.metadataHandler.SetXdbcIsAutoincrement(g.xdbcIsAutoincrementBuilder)
		g.metadataHandler.SetXdbcIsAutogeneratedColumn(g.xdbcIsGeneratedcolumnBuilder)

		g.tableColumnsItems.Append(true)
	}
}

type XdbcMetadataBuilder interface {
	Metadata() *arrow.Metadata
	SetMetadata(md arrow.Metadata)
	SetOrdinalPosition(defaultPos int32, b *array.Int32Builder)
	SetRemarks(b *array.StringBuilder)
	SetXdbcDataType(defaultType arrow.DataType, b *array.Int16Builder)
	SetXdbcTypeName(b *array.StringBuilder)
	SetXdbcColumnSize(b *array.Int32Builder)
	SetXdbcDecimalDigits(b *array.Int16Builder)
	SetXdbcNumPrecRadix(b *array.Int16Builder)
	SetXdbcNullable(b *array.Int16Builder)
	SetXdbcColumnDef(b *array.StringBuilder)
	SetXdbcSqlDataType(defaultType arrow.DataType, b *array.Int16Builder)
	SetXdbcDatetimeSub(b *array.Int16Builder)
	SetXdbcCharOctetLength(b *array.Int32Builder)
	SetXdbcIsNullable(b *array.StringBuilder)
	SetXdbcScopeCatalog(b *array.StringBuilder)
	SetXdbcScopeSchema(b *array.StringBuilder)
	SetXdbcScopeTable(b *array.StringBuilder)
	SetXdbcIsAutoincrement(b *array.BooleanBuilder)
	SetXdbcIsAutogeneratedColumn(b *array.BooleanBuilder)
}

type DefaultXdbcMetadataBuilder struct {
	Data *arrow.Metadata
}

func (c *DefaultXdbcMetadataBuilder) Metadata() *arrow.Metadata {
	return c.Data
}

func (c *DefaultXdbcMetadataBuilder) SetMetadata(md arrow.Metadata) {
	c.Data = &md
}

func (c *DefaultXdbcMetadataBuilder) findStrVal(key string) (string, bool) {
	if c.Data == nil {
		return "", false
	}
	idx := c.Data.FindKey(key)
	if idx == -1 {
		return "", false
	}
	return c.Data.Values()[idx], true
}

func (c *DefaultXdbcMetadataBuilder) findBoolVal(key string) (bool, bool) {
	if c.Data == nil {
		return false, false
	}
	idx := c.Data.FindKey(key)
	if idx == -1 {
		return false, false
	}
	v, err := strconv.ParseBool(c.Data.Values()[idx])
	if err != nil {
		return false, false
	}
	return v, true
}

func (c *DefaultXdbcMetadataBuilder) findInt32Val(key string) (int32, bool) {
	if c.Data == nil {
		return 0, false
	}
	idx := c.Data.FindKey(key)
	if idx == -1 {
		return 0, false
	}
	v, err := strconv.ParseInt(c.Data.Values()[idx], 10, 32)
	if err != nil {
		return 0, false
	}
	return int32(v), true
}

func (c *DefaultXdbcMetadataBuilder) findInt16Val(key string) (int16, bool) {
	if c.Data == nil {
		return 0, false
	}
	idx := c.Data.FindKey(key)
	if idx == -1 {
		return 0, false
	}
	v, err := strconv.ParseInt(c.Data.Values()[idx], 10, 32)
	if err != nil {
		return 0, false
	}
	return int16(v), true
}

func (c *DefaultXdbcMetadataBuilder) SetOrdinalPosition(defaultPos int32, b *array.Int32Builder) {
	if v, ok := c.findInt32Val(ORDINAL_POSITION); ok {
		b.Append(v)
	} else {
		b.Append(defaultPos)
	}
}
func (b *DefaultXdbcMetadataBuilder) SetRemarks(builder *array.StringBuilder) {
	if v, ok := b.findStrVal(REMARKS); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcDataType(columnType arrow.DataType, builder *array.Int16Builder) {
	if v, ok := b.findInt16Val(XDBC_DATA_TYPE); ok {
		builder.Append(v)
	} else {
		builder.Append(int16(ToXdbcDataType(columnType)))
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcTypeName(builder *array.StringBuilder) {
	if v, ok := b.findStrVal(XDBC_TYPE_NAME); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcColumnSize(builder *array.Int32Builder) {
	if v, ok := b.findInt32Val(XDBC_COLUMN_SIZE); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcDecimalDigits(builder *array.Int16Builder) {
	if v, ok := b.findInt16Val(XDBC_DECIMAL_DIGITS); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcNumPrecRadix(builder *array.Int16Builder) {
	if v, ok := b.findInt16Val(XDBC_NUM_PREC_RADIX); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcNullable(builder *array.Int16Builder) {
	if v, ok := b.findInt16Val(XDBC_NULLABLE); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcColumnDef(builder *array.StringBuilder) {
	if v, ok := b.findStrVal(XDBC_COLUMN_DEF); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcSqlDataType(columnType arrow.DataType, builder *array.Int16Builder) {
	if v, ok := b.findInt16Val(XDBC_SQL_DATA_TYPE); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcDatetimeSub(builder *array.Int16Builder) {
	if v, ok := b.findInt16Val(XDBC_DATETIME_SUB); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcCharOctetLength(builder *array.Int32Builder) {
	if v, ok := b.findInt32Val(XDBC_CHAR_OCTET_LENGTH); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcIsNullable(builder *array.StringBuilder) {
	if v, ok := b.findStrVal(XDBC_IS_NULLABLE); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcScopeCatalog(builder *array.StringBuilder) {
	if v, ok := b.findStrVal(XDBC_SCOPE_CATALOG); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcScopeSchema(builder *array.StringBuilder) {
	if v, ok := b.findStrVal(XDBC_SCOPE_SCHEMA); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcScopeTable(builder *array.StringBuilder) {
	if v, ok := b.findStrVal(XDBC_SCOPE_TABLE); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcIsAutoincrement(builder *array.BooleanBuilder) {
	if v, ok := b.findBoolVal(XDBC_IS_AUTOINCREMENT); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

func (b *DefaultXdbcMetadataBuilder) SetXdbcIsAutogeneratedColumn(builder *array.BooleanBuilder) {
	if v, ok := b.findBoolVal(XDBC_IS_AUTOGENERATEDCOLUMN); ok {
		builder.Append(v)
	} else {
		builder.AppendNull()
	}
}

const (
	COLUMN_NAME                 = "COLUMN_NAME"
	ORDINAL_POSITION            = "ORDINAL_POSITION"
	REMARKS                     = "REMARKS"
	XDBC_DATA_TYPE              = "XDBC_DATA_TYPE"
	XDBC_TYPE_NAME              = "XDBC_TYPE_NAME"
	XDBC_COLUMN_SIZE            = "XDBC_COLUMN_SIZE"
	XDBC_DECIMAL_DIGITS         = "XDBC_DECIMAL_DIGITS"
	XDBC_NUM_PREC_RADIX         = "XDBC_NUM_PREC_RADIX"
	XDBC_NULLABLE               = "XDBC_NULLABLE"
	XDBC_COLUMN_DEF             = "XDBC_COLUMN_DEF"
	XDBC_SQL_DATA_TYPE          = "XDBC_SQL_DATA_TYPE"
	XDBC_DATETIME_SUB           = "XDBC_DATETIME_SUB"
	XDBC_CHAR_OCTET_LENGTH      = "XDBC_CHAR_OCTET_LENGTH"
	XDBC_IS_NULLABLE            = "XDBC_IS_NULLABLE"
	XDBC_SCOPE_CATALOG          = "XDBC_SCOPE_CATALOG"
	XDBC_SCOPE_SCHEMA           = "XDBC_SCOPE_SCHEMA"
	XDBC_SCOPE_TABLE            = "XDBC_SCOPE_TABLE"
	XDBC_IS_AUTOINCREMENT       = "XDBC_IS_AUTOINCREMENT"
	XDBC_IS_AUTOGENERATEDCOLUMN = "XDBC_IS_AUTOGENERATEDCOLUMN"
)

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
	XdbcDataType_XDBC_GUID          XdbcDataType = -11
)

func ToXdbcDataType(dt arrow.DataType) (xdbcType XdbcDataType) {
	if dt == nil {
		return XdbcDataType_XDBC_UNKNOWN_TYPE
	}

	switch dt.ID() {
	case arrow.EXTENSION:
		switch dt.(arrow.ExtensionType).ExtensionName() {
		case "arrow.uuid":
			return XdbcDataType_XDBC_GUID
		default:
			return ToXdbcDataType(dt.(arrow.ExtensionType).StorageType())
		}
	case arrow.DICTIONARY:
		return ToXdbcDataType(dt.(*arrow.DictionaryType).ValueType)
	case arrow.RUN_END_ENCODED:
		return ToXdbcDataType(dt.(*arrow.RunEndEncodedType).Encoded())
	case arrow.INT8, arrow.UINT8:
		return XdbcDataType_XDBC_TINYINT
	case arrow.INT16, arrow.UINT16:
		return XdbcDataType_XDBC_SMALLINT
	case arrow.INT32, arrow.UINT32:
		return XdbcDataType_XDBC_INTEGER
	case arrow.INT64, arrow.UINT64:
		return XdbcDataType_XDBC_BIGINT
	case arrow.FLOAT32, arrow.FLOAT16, arrow.FLOAT64:
		return XdbcDataType_XDBC_FLOAT
	case arrow.DECIMAL, arrow.DECIMAL256:
		return XdbcDataType_XDBC_DECIMAL
	case arrow.STRING, arrow.LARGE_STRING:
		return XdbcDataType_XDBC_VARCHAR
	case arrow.BINARY, arrow.LARGE_BINARY:
		return XdbcDataType_XDBC_BINARY
	case arrow.FIXED_SIZE_BINARY:
		return XdbcDataType_XDBC_BINARY
	case arrow.BOOL:
		return XdbcDataType_XDBC_BIT
	case arrow.TIME32, arrow.TIME64:
		return XdbcDataType_XDBC_TIME
	case arrow.DATE32, arrow.DATE64:
		return XdbcDataType_XDBC_DATE
	case arrow.TIMESTAMP:
		return XdbcDataType_XDBC_TIMESTAMP
	case arrow.DENSE_UNION, arrow.SPARSE_UNION:
		return XdbcDataType_XDBC_VARBINARY
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return XdbcDataType_XDBC_VARBINARY
	case arrow.STRUCT, arrow.MAP:
		return XdbcDataType_XDBC_VARBINARY
	default:
		return XdbcDataType_XDBC_UNKNOWN_TYPE
	}
}

// Starts a trace.Span with the given spanName for the tracing object with
// the given ctx context.
func StartSpan(ctx context.Context, spanName string, tracing adbc.OTelTracing, opts ...trace.SpanStartOption) (context.Context, trace.Span) {
	if tracing == nil {
		return ctx, trace.SpanFromContext(ctx)
	}

	attrs := tracing.GetInitialSpanAttributes()
	attrs = append(attrs, semconv.DBOperationName(spanName))
	opts = append(opts, trace.WithAttributes(attrs...))

	return tracing.StartSpan(ctx, spanName, opts...)
}

// Ends the given span. If err is not nil, then the
// error is recorded and the status is set appropriately.
// Otherwise, the status is set to Ok.
func EndSpan(span trace.Span, err error, options ...trace.SpanEndOption) {
	if err != nil {
		span.RecordError(err)
		if adbcError, ok := err.(adbc.Error); ok {
			span.SetAttributes(semconv.ErrorTypeKey.String(adbcError.Code.String()))
		}
		span.SetStatus(codes.Error, err.Error())
	} else {
		span.SetStatus(codes.Ok, "")
	}
	span.End(options...)
}
