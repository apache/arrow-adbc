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

package snowflake

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"embed"
	"encoding/json"
	"fmt"
	"io"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/snowflakedb/gosnowflake"
)

const (
	defaultStatementQueueSize  = 200
	defaultPrefetchConcurrency = 10

	queryTemplateGetObjectsAll       = "get_objects_all.sql"
	queryTemplateGetObjectsCatalogs  = "get_objects_catalogs.sql"
	queryTemplateGetObjectsDbSchemas = "get_objects_dbschemas.sql"
	queryTemplateGetObjectsTables    = "get_objects_tables.sql"
)

//go:embed queries/*
var queryTemplates embed.FS

type snowflakeConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
	driver.Pinger
	QueryArrowStream(context.Context, string, ...driver.NamedValue) (gosnowflake.ArrowStreamLoader, error)
}

type connectionImpl struct {
	driverbase.ConnectionImplBase

	cn    snowflakeConn
	db    *databaseImpl
	ctor  gosnowflake.Connector
	sqldb *sql.DB

	activeTransaction bool
	useHighPrecision  bool
}

func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	queryFile := queryTemplateGetObjectsAll
	switch depth {
	case adbc.ObjectDepthCatalogs:
		queryFile = queryTemplateGetObjectsCatalogs
	case adbc.ObjectDepthDBSchemas:
		queryFile = queryTemplateGetObjectsDbSchemas
	case adbc.ObjectDepthTables:
		queryFile = queryTemplateGetObjectsTables
	}

	f, err := queryTemplates.Open(path.Join("queries", queryFile))
	if err != nil {
		return nil, err
	}
	defer f.Close()

	var bldr strings.Builder
	if _, err := io.Copy(&bldr, f); err != nil {
		return nil, err
	}

	args := []any{
		driverbase.PatternToNamedArg("CATALOG", catalog),
		driverbase.PatternToNamedArg("DB_SCHEMA", dbSchema),
		driverbase.PatternToNamedArg("TABLE", tableName),
		driverbase.PatternToNamedArg("COLUMN", columnName),
	}

	query := bldr.String()
	rows, err := c.sqldb.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	catalogCh := make(chan driverbase.GetObjectsInfo, 1)
	readerCh := make(chan array.RecordReader)
	errCh := make(chan error)

	go func() {
		rdr, err := driverbase.BuildGetObjectsRecordReader(c.Alloc, catalogCh)
		if err != nil {
			errCh <- err
		}

		readerCh <- rdr
		close(readerCh)
	}()

	for rows.Next() {
		var getObjectsCatalog driverbase.GetObjectsInfo
		if err := rows.Scan(&getObjectsCatalog); err != nil {
			return nil, errToAdbcErr(adbc.StatusInvalidData, err)
		}

		// A few columns need additional processing outside of Snowflake
		for i, sch := range getObjectsCatalog.CatalogDbSchemas {
			for j, tab := range sch.DbSchemaTables {
				for k, col := range tab.TableColumns {
					field := c.toArrowField(col)
					xdbcDataType := toXdbcDataType(field.Type)

					getObjectsCatalog.CatalogDbSchemas[i].DbSchemaTables[j].TableColumns[k].XdbcDataType = int16(field.Type.ID())
					getObjectsCatalog.CatalogDbSchemas[i].DbSchemaTables[j].TableColumns[k].XdbcSqlDataType = int16(xdbcDataType)
				}
			}
		}

		catalogCh <- getObjectsCatalog
	}
	close(catalogCh)

	select {
	case rdr := <-readerCh:
		return rdr, nil
	case err := <-errCh:
		return nil, err
	}
}

// GetCatalogs implements driverbase.DbObjectsEnumeratorV2.
func (c *connectionImpl) GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error) {
	var queryBldr strings.Builder

	if _, err := queryBldr.WriteString("SELECT database_name FROM information_schema.databases"); err != nil {
		return nil, err
	}

	if catalogFilter != nil {
		if _, err := fmt.Fprintf(&queryBldr, " WHERE database_name ILIKE '%s'", *catalogFilter); err != nil {
			return nil, err
		}
	}

	query := queryBldr.String()

	rows, err := c.sqldb.QueryContext(ctx, query, nil)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	catalogs := make([]string, 0)
	for rows.Next() {
		var catalog sql.NullString
		if err := rows.Scan(&catalog); err != nil {
			return nil, errToAdbcErr(adbc.StatusInvalidData, err)
		}

		if catalog.Valid {
			catalogs = append(catalogs, catalog.String)
		}
	}

	return catalogs, nil
}

// GetDBSchemasForCatalog implements driverbase.DbObjectsEnumeratorV2.
func (c *connectionImpl) GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	var queryBldr strings.Builder

	if _, err := fmt.Fprintf(&queryBldr, "SELECT schema_name FROM information_schema.schemata WHERE catalog_name = '%s'", catalog); err != nil {
		return nil, err
	}

	if schemaFilter != nil {
		if _, err := fmt.Fprintf(&queryBldr, " AND schema_name ILIKE '%s'", *schemaFilter); err != nil {
			return nil, err
		}
	}

	query := queryBldr.String()

	rows, err := c.sqldb.QueryContext(ctx, query, nil)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	dbSchemas := make([]string, 0)
	for rows.Next() {
		var dbSchema sql.NullString
		if err := rows.Scan(&dbSchema); err != nil {
			return nil, errToAdbcErr(adbc.StatusInvalidData, err)
		}

		if dbSchema.Valid {
			dbSchemas = append(dbSchemas, dbSchema.String)
		}
	}

	return dbSchemas, nil
}

// GetTablesForDBSchema implements driverbase.DbObjectsEnumeratorV2.
func (c *connectionImpl) GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]driverbase.TableInfo, error) {
	var (
		withBldr      strings.Builder
		selectBldr    strings.Builder
		fromBldr      strings.Builder
		conditionBldr strings.Builder
	)

	if _, err := fmt.Fprint(&withBldr, `
		WITH constraints AS (
			SELECT
				table_catalog,
				table_schema,
				table_name,
				ARRAY_AGG({'constraint_name': constraint_name, 'constraint_type': constraint_type}) table_constraints,
			FROM information_schema.table_constraints
			GROUP BY table_catalog, table_schema, table_name
		)`,
	); err != nil {
		return nil, err
	}

	if _, err := fmt.Fprint(&selectBldr, `
		SELECT
			table_name,
			table_type,
			table_constraints,`,
	); err != nil {
		return nil, err
	}

	if _, err := fmt.Fprint(&fromBldr, `
		FROM information_schema.tables
		LEFT JOIN constraints
		USING (table_catalog, table_schema, table_name)`,
	); err != nil {
		return nil, err
	}

	if _, err := fmt.Fprintf(&conditionBldr, `
		WHERE table_catalog = '%s' AND table_schema = '%s'`,
		catalog, schema); err != nil {
		return nil, err
	}

	if includeColumns {
		if _, err := fmt.Fprint(&withBldr, `,
			columns AS (
				SELECT
					table_catalog,
					table_schema,
					table_name,
					ARRAY_AGG({
						'column_name': column_name,
						'ordinal_position': ordinal_position,
						'remarks': comment,
						'xdbc_type_name': data_type,
						'xdbc_is_nullable': is_nullable,
						'xdbc_nullable': is_nullable::boolean::int,
						'xdbc_column_size': coalesce(character_maximum_length, numeric_precision),
						'xdbc_char_octet_length': character_octet_length,
						'xdbc_decimal_digits': numeric_scale,
						'xdbc_num_prec_radix': numeric_precision_radix,
						'xdbc_datetime_sub': datetime_precision
					}) table_columns,
				FROM information_schema.columns`,
		); err != nil {
			return nil, err
		}

		if columnFilter != nil {
			if _, err := fmt.Fprintf(&withBldr, `
				WHERE column_name ILIKE '%s'`,
				*columnFilter); err != nil {
				return nil, err
			}
		}

		if _, err := fmt.Fprint(&withBldr, `
				GROUP BY table_catalog, table_schema, table_name
			)`,
		); err != nil {
			return nil, err
		}

		if _, err := fmt.Fprint(&selectBldr, `
			table_columns,`,
		); err != nil {
			return nil, err
		}

		if _, err := fmt.Fprint(&fromBldr, `
			LEFT JOIN columns
			USING (table_catalog, table_schema, table_name)`,
		); err != nil {
			return nil, err
		}
	}

	if tableFilter != nil {
		if _, err := fmt.Fprintf(&conditionBldr, `
				AND table_name ILIKE '%s'`,
			*tableFilter); err != nil {
			return nil, err
		}
	}

	query := withBldr.String() + selectBldr.String() + fromBldr.String() + conditionBldr.String()

	rows, err := c.sqldb.QueryContext(ctx, query, nil)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	tableInfos := make([]driverbase.TableInfo, 0)
	for rows.Next() {
		var (
			tableName   sql.NullString
			tableType   sql.NullString
			columns     columnInfoArray
			constraints constraintInfoArray
		)

		dest := []any{&tableName, &tableType, &constraints}
		if includeColumns {
			dest = append(dest, &columns)
		}

		if err := rows.Scan(dest...); err != nil {
			return nil, errToAdbcErr(adbc.StatusInvalidData, err)
		}

		if !(tableName.Valid && tableType.Valid) {
			return nil, fmt.Errorf("table_name and table_type fields of GetTables schema cannot be null, found: table_name='%s', table_type='%s'", tableName.String, tableType.String)
		}

		info := driverbase.TableInfo{
			TableName:        tableName.String,
			TableType:        tableType.String,
			TableColumns:     columns.Value,
			TableConstraints: constraints.Value,
		}

		// A few columns need additional processing outside of Snowflake
		for i, col := range info.TableColumns {
			field := c.toArrowField(col)
			xdbcDataType := toXdbcDataType(field.Type)

			info.TableColumns[i].XdbcDataType = int16(field.Type.ID())
			info.TableColumns[i].XdbcSqlDataType = int16(xdbcDataType)
		}

		tableInfos = append(tableInfos, info)
	}

	return tableInfos, nil
}

// columnInfoArray is a container for a slice of ColumnInfo's which implements the sql.Scanner interface.
// The Scan() method handles mapping the snowflake response to the fields defined in GetObjectsSchema.
type columnInfoArray struct {
	Value []driverbase.ColumnInfo
}

// Scan implements sql.Scanner.
func (c *columnInfoArray) Scan(src any) error {
	if src == nil {
		return nil
	}

	var b []byte
	switch s := src.(type) {
	case []byte:
		b = s
	case string:
		b = []byte(s)
	default:
		return fmt.Errorf("unexpected driver value for ColumnInfoArray: %s", s)
	}

	return json.Unmarshal(b, &c.Value)
}

// constraintInfoArray is a container for a slice of ConstraintInfo's which implements the sql.Scanner interface.
// The Scan() method handles mapping the snowflake response to the fields defined in GetObjectsSchema.
type constraintInfoArray struct {
	Value []driverbase.ConstraintInfo
}

// Scan implements sql.Scanner.
func (c *constraintInfoArray) Scan(src any) error {
	if src == nil {
		return nil
	}

	var b []byte
	switch s := src.(type) {
	case []byte:
		b = s
	case string:
		b = []byte(s)
	default:
		return fmt.Errorf("unexpected driver value for ConstraintInfoArray: %s", s)
	}

	return json.Unmarshal(b, &c.Value)
}

// PrepareDriverInfo implements driverbase.DriverInfoPreparer.
func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if err := c.ConnectionImplBase.DriverInfo.RegisterInfoCode(adbc.InfoVendorSql, true); err != nil {
		return err
	}
	return c.ConnectionImplBase.DriverInfo.RegisterInfoCode(adbc.InfoVendorSubstrait, false)
}

// ListTableTypes implements driverbase.TableTypeLister.
func (*connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"BASE TABLE", "TEMPORARY TABLE", "VIEW"}, nil
}

// GetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return c.getStringQuery("SELECT CURRENT_DATABASE()")
}

// GetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return c.getStringQuery("SELECT CURRENT_SCHEMA()")
}

// SetCurrentCatalog implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentCatalog(value string) error {
	_, err := c.cn.ExecContext(context.Background(), "USE DATABASE ?", []driver.NamedValue{{Value: value}})
	return err
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentDbSchema(value string) error {
	_, err := c.cn.ExecContext(context.Background(), "USE SCHEMA ?", []driver.NamedValue{{Value: value}})
	return err
}

// SetAutocommit implements driverbase.AutocommitSetter.
func (c *connectionImpl) SetAutocommit(enabled bool) error {
	if enabled {
		if c.activeTransaction {
			_, err := c.cn.ExecContext(context.Background(), "COMMIT", nil)
			if err != nil {
				return errToAdbcErr(adbc.StatusInternal, err)
			}
			c.activeTransaction = false
		}
		_, err := c.cn.ExecContext(context.Background(), "ALTER SESSION SET AUTOCOMMIT = true", nil)
		return err
	}

	if !c.activeTransaction {
		_, err := c.cn.ExecContext(context.Background(), "BEGIN", nil)
		if err != nil {
			return errToAdbcErr(adbc.StatusInternal, err)
		}
		c.activeTransaction = true
	}
	_, err := c.cn.ExecContext(context.Background(), "ALTER SESSION SET AUTOCOMMIT = false", nil)
	return err
}

var loc = time.Now().Location()

func (c *connectionImpl) toArrowField(columnInfo driverbase.ColumnInfo) arrow.Field {
	field := arrow.Field{Name: columnInfo.ColumnName, Nullable: columnInfo.XdbcNullable != 0}

	switch columnInfo.XdbcTypeName {
	case "NUMBER":
		if c.useHighPrecision {
			field.Type = &arrow.Decimal128Type{
				Precision: int32(columnInfo.XdbcColumnSize),
				Scale:     int32(columnInfo.XdbcDecimalDigits),
			}
		} else {
			if columnInfo.XdbcDecimalDigits == 0 {
				field.Type = arrow.PrimitiveTypes.Int64
			} else {
				field.Type = arrow.PrimitiveTypes.Float64
			}
		}
	case "FLOAT":
		fallthrough
	case "DOUBLE":
		field.Type = arrow.PrimitiveTypes.Float64
	case "TEXT":
		field.Type = arrow.BinaryTypes.String
	case "BINARY":
		field.Type = arrow.BinaryTypes.Binary
	case "BOOLEAN":
		field.Type = arrow.FixedWidthTypes.Boolean
	case "ARRAY":
		fallthrough
	case "VARIANT":
		fallthrough
	case "OBJECT":
		// snowflake will return each value as a string
		field.Type = arrow.BinaryTypes.String
	case "DATE":
		field.Type = arrow.FixedWidthTypes.Date32
	case "TIME":
		field.Type = arrow.FixedWidthTypes.Time64ns
	case "DATETIME":
		fallthrough
	case "TIMESTAMP", "TIMESTAMP_NTZ":
		field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond}
	case "TIMESTAMP_LTZ":
		field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: loc.String()}
	case "TIMESTAMP_TZ":
		field.Type = arrow.FixedWidthTypes.Timestamp_ns
	case "GEOGRAPHY":
		fallthrough
	case "GEOMETRY":
		field.Type = arrow.BinaryTypes.String
	}

	return field
}

func toXdbcDataType(dt arrow.DataType) (xdbcType internal.XdbcDataType) {
	switch dt.ID() {
	case arrow.EXTENSION:
		return toXdbcDataType(dt.(arrow.ExtensionType).StorageType())
	case arrow.DICTIONARY:
		return toXdbcDataType(dt.(*arrow.DictionaryType).ValueType)
	case arrow.RUN_END_ENCODED:
		return toXdbcDataType(dt.(*arrow.RunEndEncodedType).Encoded())
	case arrow.INT8, arrow.UINT8:
		return internal.XdbcDataType_XDBC_TINYINT
	case arrow.INT16, arrow.UINT16:
		return internal.XdbcDataType_XDBC_SMALLINT
	case arrow.INT32, arrow.UINT32:
		return internal.XdbcDataType_XDBC_SMALLINT
	case arrow.INT64, arrow.UINT64:
		return internal.XdbcDataType_XDBC_BIGINT
	case arrow.FLOAT32, arrow.FLOAT16, arrow.FLOAT64:
		return internal.XdbcDataType_XDBC_FLOAT
	case arrow.DECIMAL, arrow.DECIMAL256:
		return internal.XdbcDataType_XDBC_DECIMAL
	case arrow.STRING, arrow.LARGE_STRING:
		return internal.XdbcDataType_XDBC_VARCHAR
	case arrow.BINARY, arrow.LARGE_BINARY:
		return internal.XdbcDataType_XDBC_BINARY
	case arrow.FIXED_SIZE_BINARY:
		return internal.XdbcDataType_XDBC_BINARY
	case arrow.BOOL:
		return internal.XdbcDataType_XDBC_BIT
	case arrow.TIME32, arrow.TIME64:
		return internal.XdbcDataType_XDBC_TIME
	case arrow.DATE32, arrow.DATE64:
		return internal.XdbcDataType_XDBC_DATE
	case arrow.TIMESTAMP:
		return internal.XdbcDataType_XDBC_TIMESTAMP
	case arrow.DENSE_UNION, arrow.SPARSE_UNION:
		return internal.XdbcDataType_XDBC_VARBINARY
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return internal.XdbcDataType_XDBC_VARBINARY
	case arrow.STRUCT, arrow.MAP:
		return internal.XdbcDataType_XDBC_VARBINARY
	default:
		return internal.XdbcDataType_XDBC_UNKNOWN_TYPE
	}
}

func descToField(name, typ, isnull, primary string, comment sql.NullString) (field arrow.Field, err error) {
	field.Name = strings.ToLower(name)
	if isnull == "Y" {
		field.Nullable = true
	}
	md := make(map[string]string)
	md["DATA_TYPE"] = typ
	md["PRIMARY_KEY"] = primary
	if comment.Valid {
		md["COMMENT"] = comment.String
	}
	field.Metadata = arrow.MetadataFrom(md)

	paren := strings.Index(typ, "(")
	if paren == -1 {
		// types without params
		switch typ {
		case "FLOAT":
			fallthrough
		case "DOUBLE":
			field.Type = arrow.PrimitiveTypes.Float64
		case "DATE":
			field.Type = arrow.FixedWidthTypes.Date32
		// array, object and variant are all represented as strings by
		// snowflake's return
		case "ARRAY":
			fallthrough
		case "OBJECT":
			fallthrough
		case "VARIANT":
			field.Type = arrow.BinaryTypes.String
		case "GEOGRAPHY":
			fallthrough
		case "GEOMETRY":
			field.Type = arrow.BinaryTypes.String
		case "BOOLEAN":
			field.Type = arrow.FixedWidthTypes.Boolean
		default:
			err = adbc.Error{
				Msg:  fmt.Sprintf("Snowflake Data Type %s not implemented", typ),
				Code: adbc.StatusNotImplemented,
			}
		}
		return
	}

	prefix := typ[:paren]
	switch prefix {
	case "VARCHAR", "TEXT":
		field.Type = arrow.BinaryTypes.String
	case "BINARY", "VARBINARY":
		field.Type = arrow.BinaryTypes.Binary
	case "NUMBER":
		comma := strings.Index(typ, ",")
		scale, err := strconv.ParseInt(typ[comma+1:len(typ)-1], 10, 32)
		if err != nil {
			return field, adbc.Error{
				Msg:  "could not parse Scale from type '" + typ + "'",
				Code: adbc.StatusInvalidData,
			}
		}
		if scale == 0 {
			field.Type = arrow.PrimitiveTypes.Int64
		} else {
			field.Type = arrow.PrimitiveTypes.Float64
		}
	case "TIME":
		field.Type = arrow.FixedWidthTypes.Time64ns
	case "DATETIME":
		fallthrough
	case "TIMESTAMP", "TIMESTAMP_NTZ":
		field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond}
	case "TIMESTAMP_LTZ":
		field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: loc.String()}
	case "TIMESTAMP_TZ":
		field.Type = arrow.FixedWidthTypes.Timestamp_ns
	default:
		err = adbc.Error{
			Msg:  fmt.Sprintf("Snowflake Data Type %s not implemented", typ),
			Code: adbc.StatusNotImplemented,
		}
	}
	return
}

func (c *connectionImpl) getStringQuery(query string) (string, error) {
	result, err := c.cn.QueryContext(context.Background(), query, nil)
	if err != nil {
		return "", errToAdbcErr(adbc.StatusInternal, err)
	}
	defer result.Close()

	if len(result.Columns()) != 1 {
		return "", adbc.Error{
			Msg:  fmt.Sprintf("[Snowflake] Internal query returned wrong number of columns: %s", result.Columns()),
			Code: adbc.StatusInternal,
		}
	}

	dest := make([]driver.Value, 1)
	err = result.Next(dest)
	if err == io.EOF {
		return "", adbc.Error{
			Msg:  "[Snowflake] Internal query returned no rows",
			Code: adbc.StatusInternal,
		}
	} else if err != nil {
		return "", errToAdbcErr(adbc.StatusInternal, err)
	}

	value, ok := dest[0].(string)
	if !ok {
		return "", adbc.Error{
			Msg:  fmt.Sprintf("[Snowflake] Internal query returned wrong type of value: %s", dest[0]),
			Code: adbc.StatusInternal,
		}
	}

	return value, nil
}

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	tblParts := make([]string, 0, 3)
	if catalog != nil {
		tblParts = append(tblParts, quoteTblName(*catalog))
	}
	if dbSchema != nil {
		tblParts = append(tblParts, quoteTblName(*dbSchema))
	}
	tblParts = append(tblParts, quoteTblName(tableName))
	fullyQualifiedTable := strings.Join(tblParts, ".")

	rows, err := c.sqldb.QueryContext(ctx, `DESC TABLE `+fullyQualifiedTable)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	var (
		name, typ, kind, isnull, primary, unique          string
		def, check, expr, comment, policyName, privDomain sql.NullString
		fields                                            = []arrow.Field{}
	)

	for rows.Next() {
		err := rows.Scan(&name, &typ, &kind, &isnull, &def, &primary, &unique,
			&check, &expr, &comment, &policyName, &privDomain)
		if err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}

		f, err := descToField(name, typ, isnull, primary, comment)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	sc := arrow.NewSchema(fields, nil)
	return sc, nil
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Commit(_ context.Context) error {
	_, err := c.cn.ExecContext(context.Background(), "COMMIT", nil)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err)
	}

	_, err = c.cn.ExecContext(context.Background(), "BEGIN", nil)
	return errToAdbcErr(adbc.StatusInternal, err)
}

// Rollback rolls back any pending transactions. Only used if autocommit
// is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *connectionImpl) Rollback(_ context.Context) error {
	_, err := c.cn.ExecContext(context.Background(), "ROLLBACK", nil)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err)
	}

	_, err = c.cn.ExecContext(context.Background(), "BEGIN", nil)
	return errToAdbcErr(adbc.StatusInternal, err)
}

// NewStatement initializes a new statement object tied to this connection
func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	defaultIngestOptions := DefaultIngestOptions()
	return &statement{
		alloc:               c.db.Alloc,
		cnxn:                c,
		queueSize:           defaultStatementQueueSize,
		prefetchConcurrency: defaultPrefetchConcurrency,
		useHighPrecision:    c.useHighPrecision,
		ingestOptions:       defaultIngestOptions,
	}, nil
}

// Close closes this connection and releases any associated resources.
func (c *connectionImpl) Close() error {
	if c.sqldb == nil || c.cn == nil {
		return adbc.Error{Code: adbc.StatusInvalidState}
	}

	if err := c.sqldb.Close(); err != nil {
		return err
	}
	c.sqldb = nil

	defer func() {
		c.cn = nil
	}()
	return c.cn.Close()
}

// ReadPartition constructs a statement for a partition of a query. The
// results can then be read independently using the returned RecordReader.
//
// A partition can be retrieved by using ExecutePartitions on a statement.
func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ReadPartition not yet implemented for snowflake driver",
	}
}

func (c *connectionImpl) SetOption(key, value string) error {
	switch key {
	case OptionUseHighPrecision:
		// statements will inherit the value of the OptionUseHighPrecision
		// from the connection, but the option can be overridden at the
		// statement level if SetOption is called on the statement.
		switch value {
		case adbc.OptionValueEnabled:
			c.useHighPrecision = true
		case adbc.OptionValueDisabled:
			c.useHighPrecision = false
		default:
			return adbc.Error{
				Msg:  "[Snowflake] invalid value for option " + key + ": " + value,
				Code: adbc.StatusInvalidArgument,
			}
		}
		return nil
	default:
		return adbc.Error{
			Msg:  "[Snowflake] unknown connection option " + key + ": " + value,
			Code: adbc.StatusInvalidArgument,
		}
	}
}

var _ sql.Scanner = (*columnInfoArray)(nil)
var _ sql.Scanner = (*constraintInfoArray)(nil)
