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
	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
	"github.com/apache/arrow/go/v12/arrow/memory"
)

type CatalogAndSchema struct {
	Catalog, Schema string
}

type TableInfo struct {
	Name, TableType string
	Schema          *arrow.Schema
}

type GetObjDBSchemasFn func(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string) (map[string][]string, error)
type GetObjTablesFn func(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, tableName *string, columnName *string, tableType []string) (map[CatalogAndSchema][]TableInfo, error)
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
}

func (g *GetObjects) Init(mem memory.Allocator, getObj GetObjDBSchemasFn, getTbls GetObjTablesFn) error {
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

	for _, tableInfo := range g.tableLookup[CatalogAndSchema{
		Catalog: catalogName,
		Schema:  dbSchemaName,
	}] {
		g.appendTableInfo(tableInfo)
	}
}

func (g *GetObjects) appendTableInfo(tableInfo TableInfo) {
	g.tableNameBuilder.Append(tableInfo.Name)
	g.tableTypeBuilder.Append(tableInfo.TableType)
	g.dbSchemaTablesItems.Append(true)

	if g.Depth == adbc.ObjectDepthTables {
		g.tableColumnsBuilder.AppendNull()
		g.tableConstraintsBuilder.AppendNull()
		return
	}
	g.tableColumnsBuilder.Append(true)
	// TODO: unimplemented for now
	g.tableConstraintsBuilder.Append(true)

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
		} else {
			if remark, ok := column.Metadata.GetValue("COMMENT"); ok {
				g.remarksBuilder.Append(remark)
			} else {
				g.remarksBuilder.AppendNull()
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

		g.xdbcDataTypeBuilder.AppendNull()
		g.xdbcTypeNameBuilder.AppendNull()
		g.xdbcColumnSizeBuilder.AppendNull()
		g.xdbcDecimalDigitsBuilder.AppendNull()
		g.xdbcNumPrecRadixBuilder.AppendNull()
		g.xdbcNullableBuilder.AppendNull()
		g.xdbcColumnDefBuilder.AppendNull()
		g.xdbcSqlDataTypeBuilder.AppendNull()
		g.xdbcDatetimeSubBuilder.AppendNull()
		g.xdbcCharOctetLengthBuilder.AppendNull()
		g.xdbcIsNullableBuilder.AppendNull()
		g.xdbcScopeCatalogBuilder.AppendNull()
		g.xdbcScopeSchemaBuilder.AppendNull()
		g.xdbcScopeTableBuilder.AppendNull()
		g.xdbcIsAutoincrementBuilder.AppendNull()
		g.xdbcIsGeneratedcolumnBuilder.AppendNull()

		g.tableColumnsItems.Append(true)
	}
}
