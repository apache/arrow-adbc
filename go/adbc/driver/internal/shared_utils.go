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
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
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

	metadataHandlers MetadataToHandlers
	metadataBuilders MetadataToBuilders
}

func (g *GetObjects) Init(mem memory.Allocator, getObj GetObjDBSchemasFn, getTbls GetObjTablesFn, overrideMetadataHandlers MetadataToHandlers) error {
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

	g.metadataBuilders = MetadataToBuilders{}
	g.metadataBuilders[REMARKS] = g.tableColumnsItems.FieldBuilder(2)
	g.metadataBuilders[XDBC_DATA_TYPE] = g.tableColumnsItems.FieldBuilder(3)
	g.metadataBuilders[XDBC_TYPE_NAME] = g.tableColumnsItems.FieldBuilder(4)
	g.metadataBuilders[XDBC_COLUMN_SIZE] = g.tableColumnsItems.FieldBuilder(5)
	g.metadataBuilders[XDBC_DECIMAL_DIGITS] = g.tableColumnsItems.FieldBuilder(6)
	g.metadataBuilders[XDBC_NUM_PREC_RADIX] = g.tableColumnsItems.FieldBuilder(7)
	g.metadataBuilders[XDBC_NULLABLE] = g.tableColumnsItems.FieldBuilder(8)
	g.metadataBuilders[XDBC_COLUMN_DEF] = g.tableColumnsItems.FieldBuilder(9)
	g.metadataBuilders[XDBC_SQL_DATA_TYPE] = g.tableColumnsItems.FieldBuilder(10)
	g.metadataBuilders[XDBC_DATETIME_SUB] = g.tableColumnsItems.FieldBuilder(11)
	g.metadataBuilders[XDBC_CHAR_OCTET_LENGTH] = g.tableColumnsItems.FieldBuilder(12)
	g.metadataBuilders[XDBC_IS_NULLABLE] = g.tableColumnsItems.FieldBuilder(13)
	g.metadataBuilders[XDBC_SCOPE_CATALOG] = g.tableColumnsItems.FieldBuilder(14)
	g.metadataBuilders[XDBC_SCOPE_SCHEMA] = g.tableColumnsItems.FieldBuilder(15)
	g.metadataBuilders[XDBC_SCOPE_TABLE] = g.tableColumnsItems.FieldBuilder(16)
	g.metadataBuilders[XDBC_IS_AUTOINCREMENT] = g.tableColumnsItems.FieldBuilder(17)
	g.metadataBuilders[XDBC_IS_AUTOGENERATEDCOLUMN] = g.tableColumnsItems.FieldBuilder(18)

	g.metadataHandlers = g.initMetadataHandlers(overrideMetadataHandlers)

	return nil
}

func parseColumnMetadata(key string, column arrow.Field, builder array.Builder) {
	if column.HasMetadata() {
		if value, ok := column.Metadata.GetValue(key); ok {
			var parsedInt int64
			var parsedBool bool
			var err error
			switch b := builder.(type) {
			case *array.Int16Builder:
				parsedInt, err = strconv.ParseInt(value, 10, 16)
				if err != nil {
					b.AppendNull()
				} else {
					b.Append(int16(parsedInt))
				}
			case *array.Int32Builder:
				parsedInt, err = strconv.ParseInt(value, 10, 32)
				if err != nil {
					b.AppendNull()
				} else {
					b.Append(int32(parsedInt))
				}
			case *array.Int64Builder:
				parsedInt, err = strconv.ParseInt(value, 10, 64)
				if err != nil {
					b.AppendNull()
				} else {
					b.Append(parsedInt)
				}
			case *array.BooleanBuilder:
				parsedBool, err = strconv.ParseBool(value)
				if err != nil {
					b.AppendNull()
				} else {
					b.Append(parsedBool)
				}
			case *array.StringBuilder:
				b.Append(value)
			default:
				b.AppendNull()
			}
		} else {
			builder.AppendNull()
		}
	} else {
		builder.AppendNull()
	}
}

func (g *GetObjects) initMetadataHandlers(overrides MetadataToHandlers) MetadataToHandlers {

	createHandler := func(key string) MetadataHandlers {
		return func(column arrow.Field, builder array.Builder) {
			parseColumnMetadata(key, column, builder)
		}
	}

	metadataHandlers := make(MetadataToHandlers)

	checkOverrideOrDefault := func(key string) {
		if overrides[key] != nil {
			metadataHandlers[key] = overrides[key]
		} else {
			metadataHandlers[key] = createHandler(key)
		}
	}

	checkOverrideOrDefault(REMARKS)
	checkOverrideOrDefault(XDBC_DATA_TYPE)
	checkOverrideOrDefault(XDBC_TYPE_NAME)
	checkOverrideOrDefault(XDBC_COLUMN_SIZE)
	checkOverrideOrDefault(XDBC_DECIMAL_DIGITS)
	checkOverrideOrDefault(XDBC_NUM_PREC_RADIX)
	checkOverrideOrDefault(XDBC_NULLABLE)
	checkOverrideOrDefault(XDBC_COLUMN_DEF)
	checkOverrideOrDefault(XDBC_SQL_DATA_TYPE)
	checkOverrideOrDefault(XDBC_DATETIME_SUB)
	checkOverrideOrDefault(XDBC_CHAR_OCTET_LENGTH)
	checkOverrideOrDefault(XDBC_IS_NULLABLE)
	checkOverrideOrDefault(XDBC_SCOPE_CATALOG)
	checkOverrideOrDefault(XDBC_SCOPE_SCHEMA)
	checkOverrideOrDefault(XDBC_SCOPE_TABLE)
	checkOverrideOrDefault(XDBC_IS_AUTOINCREMENT)
	checkOverrideOrDefault(XDBC_IS_AUTOGENERATEDCOLUMN)

	return metadataHandlers
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

		pos := int32(colIndex + 1)

		g.columnNameBuilder.Append(column.Name)
		g.ordinalPositionBuilder.Append(pos)

		for key, mdHandler := range g.metadataHandlers {
			mdHandler(column, g.metadataBuilders[key])
		}

		g.tableColumnsItems.Append(true)
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
)

func ToXdbcDataType(dt arrow.DataType) (xdbcType XdbcDataType) {
	switch dt.ID() {
	case arrow.EXTENSION:
		return ToXdbcDataType(dt.(arrow.ExtensionType).StorageType())
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
