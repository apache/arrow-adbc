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
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"runtime"
	"strconv"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/errgroup"
)

const (
	defaultStatementQueueSize  = 100
	defaultPrefetchConcurrency = 5

	queryTemplateGetObjectsAll           = "get_objects_all.sql"
	queryTemplateGetObjectsDbSchemas     = "get_objects_dbschemas.sql"
	queryTemplateGetObjectsTables        = "get_objects_tables.sql"
	queryTemplateGetObjectsTerseCatalogs = "get_objects_terse_catalogs.sql"
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

	cn   snowflakeConn
	db   *databaseImpl
	ctor driver.Connector

	activeTransaction bool
	useHighPrecision  bool
}

func escapeSingleQuoteForLike(arg string) string {
	if len(arg) == 0 {
		return arg
	}

	idx := strings.IndexByte(arg, '\'')
	if idx == -1 {
		return arg
	}

	var b strings.Builder
	b.Grow(len(arg))

	for {
		before, after, found := strings.Cut(arg, `'`)
		b.WriteString(before)
		if !found {
			return b.String()
		}

		if before[len(before)-1] != '\\' {
			b.WriteByte('\\')
		}
		b.WriteByte('\'')
		arg = after
	}
}

func getQueryID(ctx context.Context, query string, driverConn driver.QueryerContext) (string, error) {
	rows, err := driverConn.QueryContext(ctx, query, nil)
	if err != nil {
		return "", err
	}

	return rows.(gosnowflake.SnowflakeRows).GetQueryID(), rows.Close()
}

const (
	objSchemas   = "SCHEMAS"
	objDatabases = "DATABASES"
	objViews     = "VIEWS"
	objTables    = "TABLES"
	objObjects   = "OBJECTS"
)

func addLike(query string, pattern *string) string {
	if pattern != nil && len(*pattern) > 0 && *pattern != "%" && *pattern != ".*" {
		query += " LIKE '" + escapeSingleQuoteForLike(*pattern) + "'"
	}
	return query
}

func goGetQueryID(ctx context.Context, conn driver.QueryerContext, grp *errgroup.Group, objType string, catalog, dbSchema, tableName *string, outQueryID *string) {
	grp.Go(func() error {
		query := "SHOW TERSE /* ADBC:getObjects */ " + objType
		switch objType {
		case objDatabases:
			query = addLike(query, catalog)
			query += " IN ACCOUNT"
		case objSchemas:
			query = addLike(query, dbSchema)

			if catalog == nil || isWildcardStr(*catalog) {
				query += " IN ACCOUNT"
			} else {
				query += " IN DATABASE " + quoteTblName(*catalog)
			}
		case objViews, objTables, objObjects:
			query = addLike(query, tableName)

			if catalog == nil || isWildcardStr(*catalog) {
				query += " IN ACCOUNT"
			} else {
				escapedCatalog := quoteTblName(*catalog)
				if dbSchema == nil || isWildcardStr(*dbSchema) {
					query += " IN DATABASE " + escapedCatalog
				} else {
					query += " IN SCHEMA " + escapedCatalog + "." + quoteTblName(*dbSchema)
				}
			}
		default:
			return fmt.Errorf("unimplemented object type")
		}

		var err error
		*outQueryID, err = getQueryID(ctx, query, conn)
		return err
	})
}

func isWildcardStr(ident string) bool {
	return strings.ContainsAny(ident, "_%")
}

func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (rdr array.RecordReader, err error) {
	var (
		pkQueryID, fkQueryID, uniqueQueryID, terseDbQueryID string
		showSchemaQueryID, tableQueryID                     string
	)

	conn := c.cn
	var hasViews, hasTables bool
	for _, t := range tableType {
		if strings.EqualFold("VIEW", t) {
			hasViews = true
		} else if strings.EqualFold("TABLE", t) {
			hasTables = true
		}
	}

	// force empty result from SHOW TABLES if tableType list is not empty
	// and does not contain TABLE or VIEW in the list.
	// we need this because we should have non-null db_schema_tables when
	// depth is Tables, Columns or All.
	var badTableType = "tabletypedoesnotexist"
	if len(tableType) > 0 && depth >= adbc.ObjectDepthTables && !hasViews && !hasTables {
		tableName = &badTableType
		tableType = []string{"TABLE"}
	}

	gQueryIDs, gQueryIDsCtx := errgroup.WithContext(ctx)
	queryFile := queryTemplateGetObjectsAll
	switch depth {
	case adbc.ObjectDepthCatalogs:
		queryFile = queryTemplateGetObjectsTerseCatalogs
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
			catalog, dbSchema, tableName, &terseDbQueryID)
	case adbc.ObjectDepthDBSchemas:
		queryFile = queryTemplateGetObjectsDbSchemas
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objSchemas,
			catalog, dbSchema, tableName, &showSchemaQueryID)
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
			catalog, dbSchema, tableName, &terseDbQueryID)
	case adbc.ObjectDepthTables:
		queryFile = queryTemplateGetObjectsTables
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objSchemas,
			catalog, dbSchema, tableName, &showSchemaQueryID)
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
			catalog, dbSchema, tableName, &terseDbQueryID)

		objType := objObjects
		if len(tableType) == 1 {
			if strings.EqualFold("VIEW", tableType[0]) {
				objType = objViews
			} else if strings.EqualFold("TABLE", tableType[0]) {
				objType = objTables
			}
		}

		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objType,
			catalog, dbSchema, tableName, &tableQueryID)
	default:
		var suffix string
		if catalog == nil || isWildcardStr(*catalog) {
			suffix = " IN ACCOUNT"
		} else {
			escapedCatalog := quoteTblName(*catalog)
			if dbSchema == nil || isWildcardStr(*dbSchema) {
				suffix = " IN DATABASE " + escapedCatalog
			} else {
				escapedSchema := quoteTblName(*dbSchema)
				if tableName == nil || isWildcardStr(*tableName) {
					suffix = " IN SCHEMA " + escapedCatalog + "." + escapedSchema
				} else {
					escapedTable := quoteTblName(*tableName)
					suffix = " IN TABLE " + escapedCatalog + "." + escapedSchema + "." + escapedTable
				}
			}
		}

		// Detailed constraint info not available in information_schema
		// Need to dispatch SHOW queries and use conn.Raw to extract the queryID for reuse in GetObjects query
		gQueryIDs.Go(func() (err error) {
			pkQueryID, err = getQueryID(gQueryIDsCtx, "SHOW PRIMARY KEYS /* ADBC:getObjectsTables */"+suffix, conn)
			return err
		})

		gQueryIDs.Go(func() (err error) {
			fkQueryID, err = getQueryID(gQueryIDsCtx, "SHOW IMPORTED KEYS /* ADBC:getObjectsTables */"+suffix, conn)
			return err
		})

		gQueryIDs.Go(func() (err error) {
			uniqueQueryID, err = getQueryID(gQueryIDsCtx, "SHOW UNIQUE KEYS /* ADBC:getObjectsTables */"+suffix, conn)
			return err
		})

		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objDatabases,
			catalog, dbSchema, tableName, &terseDbQueryID)
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objSchemas,
			catalog, dbSchema, tableName, &showSchemaQueryID)

		objType := objObjects
		if len(tableType) == 1 {
			if strings.EqualFold("VIEW", tableType[0]) {
				objType = objViews
			} else if strings.EqualFold("TABLE", tableType[0]) {
				objType = objTables
			}
		}
		goGetQueryID(gQueryIDsCtx, conn, gQueryIDs, objType,
			catalog, dbSchema, tableName, &tableQueryID)
	}

	queryBytes, err := fs.ReadFile(queryTemplates, path.Join("queries", queryFile))
	if err != nil {
		return nil, err
	}

	// Need constraint subqueries to complete before we can query GetObjects
	if err := gQueryIDs.Wait(); err != nil {
		return nil, err
	}

	args := []sql.NamedArg{
		// Optional filter patterns
		driverbase.PatternToNamedArg("CATALOG", catalog),
		driverbase.PatternToNamedArg("DB_SCHEMA", dbSchema),
		driverbase.PatternToNamedArg("TABLE", tableName),
		driverbase.PatternToNamedArg("COLUMN", columnName),

		// QueryIDs for constraint data if depth is tables or deeper
		// or if the depth is catalog and catalog is null
		sql.Named("PK_QUERY_ID", pkQueryID),
		sql.Named("FK_QUERY_ID", fkQueryID),
		sql.Named("UNIQUE_QUERY_ID", uniqueQueryID),
		sql.Named("SHOW_DB_QUERY_ID", terseDbQueryID),
		sql.Named("SHOW_SCHEMA_QUERY_ID", showSchemaQueryID),
		sql.Named("SHOW_TABLE_QUERY_ID", tableQueryID),
	}

	nvargs := make([]driver.NamedValue, len(args))
	for i, arg := range args {
		nvargs[i] = driver.NamedValue{
			Name:    arg.Name,
			Ordinal: i + 1,
			Value:   arg.Value,
		}
	}

	query := string(queryBytes)
	rows, err := conn.QueryContext(ctx, query, nvargs)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	catalogCh := make(chan driverbase.GetObjectsInfo, runtime.NumCPU())
	errCh := make(chan error)

	go func() {
		defer close(catalogCh)
		dest := make([]driver.Value, len(rows.Columns()))
		for {
			if err := rows.Next(dest); err != nil {
				if errors.Is(err, io.EOF) {
					return
				}
				errCh <- errToAdbcErr(adbc.StatusInvalidData, err)
				return
			}

			var getObjectsCatalog driverbase.GetObjectsInfo
			if err := getObjectsCatalog.Scan(dest[0]); err != nil {
				errCh <- errToAdbcErr(adbc.StatusInvalidData, err)
				return
			}

			// A few columns need additional processing outside of Snowflake
			for i, sch := range getObjectsCatalog.CatalogDbSchemas {
				for j, tab := range sch.DbSchemaTables {
					for k, col := range tab.TableColumns {
						field := c.toArrowField(col)
						xdbcDataType := internal.ToXdbcDataType(field.Type)

						if field.Type != nil {
							getObjectsCatalog.CatalogDbSchemas[i].DbSchemaTables[j].TableColumns[k].XdbcDataType = driverbase.Nullable(int16(field.Type.ID()))
						}
						getObjectsCatalog.CatalogDbSchemas[i].DbSchemaTables[j].TableColumns[k].XdbcSqlDataType = driverbase.Nullable(int16(xdbcDataType))
					}
				}
			}

			catalogCh <- getObjectsCatalog
		}
	}()

	return driverbase.BuildGetObjectsRecordReader(c.Alloc, catalogCh, errCh)
}

// PrepareDriverInfo implements driverbase.DriverInfoPreparer.
func (c *connectionImpl) PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error {
	if err := c.DriverInfo.RegisterInfoCode(adbc.InfoVendorSql, true); err != nil {
		return err
	}
	return c.DriverInfo.RegisterInfoCode(adbc.InfoVendorSubstrait, false)
}

// ListTableTypes implements driverbase.TableTypeLister.
func (*connectionImpl) ListTableTypes(ctx context.Context) ([]string, error) {
	return []string{"TABLE", "VIEW"}, nil
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
	_, err := c.cn.ExecContext(context.Background(), fmt.Sprintf("USE DATABASE %s;", quoteTblName(value)), nil)
	return err
}

// SetCurrentDbSchema implements driverbase.CurrentNamespacer.
func (c *connectionImpl) SetCurrentDbSchema(value string) error {
	_, err := c.cn.ExecContext(context.Background(), fmt.Sprintf("USE SCHEMA %s;", quoteTblName(value)), nil)
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

func (c *connectionImpl) toArrowField(columnInfo driverbase.ColumnInfo) arrow.Field {
	field := arrow.Field{Name: columnInfo.ColumnName, Nullable: driverbase.ValueOrZero(columnInfo.XdbcNullable) != 0}

	switch driverbase.ValueOrZero(columnInfo.XdbcTypeName) {
	case "NUMBER":
		if c.useHighPrecision {
			field.Type = &arrow.Decimal128Type{
				Precision: int32(driverbase.ValueOrZero(columnInfo.XdbcColumnSize)),
				Scale:     int32(driverbase.ValueOrZero(columnInfo.XdbcDecimalDigits)),
			}
		} else {
			if driverbase.ValueOrZero(columnInfo.XdbcDecimalDigits) == 0 {
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
		field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
	case "TIMESTAMP_TZ":
		field.Type = arrow.FixedWidthTypes.Timestamp_ns
	case "GEOGRAPHY":
		fallthrough
	case "GEOMETRY":
		field.Type = arrow.BinaryTypes.String
	case "VECTOR":
		// despite the fact that Snowflake *does* support returning data
		// for VECTOR typed columns as Arrow FixedSizeLists, there's no way
		// currently to retrieve enough metadata to construct the proper type
		// for it
	}

	return field
}

func descToField(name, typ, isnull, primary string, comment sql.NullString) (field arrow.Field, err error) {
	field.Name = name
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
		field.Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: "UTC"}
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

func (c *connectionImpl) getStringQuery(query string) (value string, err error) {
	result, err := c.cn.QueryContext(context.Background(), query, nil)
	if err != nil {
		return "", errToAdbcErr(adbc.StatusInternal, err)
	}
	defer func() {
		err = errors.Join(err, result.Close())
	}()

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

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (sc *arrow.Schema, err error) {
	tblParts := make([]string, 0, 3)
	if catalog != nil {
		tblParts = append(tblParts, quoteTblName(*catalog))
	}
	if dbSchema != nil {
		tblParts = append(tblParts, quoteTblName(*dbSchema))
	}
	tblParts = append(tblParts, quoteTblName(tableName))
	fullyQualifiedTable := strings.Join(tblParts, ".")

	rows, err := c.cn.QueryContext(ctx, `DESC TABLE `+fullyQualifiedTable, nil)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer func() {
		err = errors.Join(err, rows.Close())
	}()

	var (
		name, typ, isnull, primary string
		comment                    sql.NullString
		fields                     = []arrow.Field{}
	)

	// columns are:
	// name, type, kind, isnull, primary, unique, def, check, expr, comment, policyName, privDomain
	dest := make([]driver.Value, len(rows.Columns()))
	for {
		if err := rows.Next(dest); err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}

		name = dest[0].(string)
		typ = dest[1].(string)
		isnull = dest[3].(string)
		primary = dest[5].(string)
		if err := comment.Scan(dest[9]); err != nil {
			return nil, errToAdbcErr(adbc.StatusIO, err)
		}

		f, err := descToField(name, typ, isnull, primary, comment)
		if err != nil {
			return nil, err
		}
		fields = append(fields, f)
	}

	sc = arrow.NewSchema(fields, nil)
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
	if c.cn == nil {
		return adbc.Error{Code: adbc.StatusInvalidState}
	}

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
