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
	"fmt"
	"io"
	"io/fs"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow/go/v18/arrow"
	"github.com/apache/arrow/go/v18/arrow/array"
	"github.com/snowflakedb/gosnowflake"
	"golang.org/x/sync/errgroup"
)

const (
	defaultStatementQueueSize  = 200
	defaultPrefetchConcurrency = 10

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

	cn    snowflakeConn
	db    *databaseImpl
	ctor  gosnowflake.Connector
	sqldb *sql.DB

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

func getQueryID(ctx context.Context, query string, driverConn any) (string, error) {
	rows, err := driverConn.(driver.QueryerContext).QueryContext(ctx, query, nil)
	if err != nil {
		return "", err
	}

	return rows.(gosnowflake.SnowflakeRows).GetQueryID(), rows.Close()
}

func isWildcardStr(ident string) bool {
	return strings.ContainsAny(ident, "_%")
}

func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	var (
		pkQueryID, fkQueryID, uniqueQueryID, terseDbQueryID string
		showSchemaQueryID, tableQueryID                     string
	)

	conn, err := c.sqldb.Conn(ctx)
	if err != nil {
		return nil, err
	}
	defer conn.Close()

	gQueryIDs, gQueryIDsCtx := errgroup.WithContext(ctx)

	queryFile := queryTemplateGetObjectsAll
	switch depth {
	case adbc.ObjectDepthCatalogs:
		queryFile = queryTemplateGetObjectsTerseCatalogs
		gQueryIDs.Go(func() error {
			return conn.Raw(func(driverConn any) (err error) {
				query := "SHOW TERSE /* ADBC:getObjectsCatalogs */ DATABASES"
				if catalog != nil && len(*catalog) > 0 && *catalog != "%" && *catalog != ".*" {
					query += " LIKE '" + escapeSingleQuoteForLike(*catalog) + "'"
				}
				query += " IN ACCOUNT"

				terseDbQueryID, err = getQueryID(gQueryIDsCtx, query, driverConn)
				return
			})
		})
	case adbc.ObjectDepthDBSchemas:
		queryFile = queryTemplateGetObjectsDbSchemas
		gQueryIDs.Go(func() error {
			return conn.Raw(func(driverConn any) (err error) {
				query := "SHOW TERSE /* ADBC:getObjectsDBSchemas */ SCHEMAS"
				if dbSchema != nil && len(*dbSchema) > 0 && *dbSchema != "%" && *dbSchema != ".*" {
					query += " LIKE '" + escapeSingleQuoteForLike(*dbSchema) + "'"
				}
				if catalog == nil || isWildcardStr(*catalog) {
					query += " IN ACCOUNT"
				} else {
					query += " IN DATABASE \"" + quoteTblName(*catalog) + "\""
				}

				showSchemaQueryID, err = getQueryID(gQueryIDsCtx, query, driverConn)
				return
			})
		})

		gQueryIDs.Go(func() error {
			return conn.Raw(func(driverConn any) (err error) {
				query := "SHOW TERSE /* ADBC:getObjectsDBSchemas */ DATABASES"
				if catalog != nil && len(*catalog) > 0 && *catalog != "%" && *catalog != ".*" {
					query += " LIKE '" + escapeSingleQuoteForLike(*catalog) + "'"
				}
				query += " IN ACCOUNT"

				terseDbQueryID, err = getQueryID(gQueryIDsCtx, query, driverConn)
				return
			})
		})
	case adbc.ObjectDepthTables:
		queryFile = queryTemplateGetObjectsTables
		fallthrough
	default:
		var suffix string
		if catalog == nil {
			suffix = " IN ACCOUNT"
		} else {
			escapedCatalog := quoteTblName(*catalog)
			if dbSchema == nil || isWildcardStr(*dbSchema) {
				suffix = " IN DATABASE \"" + escapedCatalog + "\""
			} else {
				escapedSchema := quoteTblName(*dbSchema)
				if tableName == nil || isWildcardStr(*tableName) {
					suffix = " IN SCHEMA \"" + escapedCatalog + "\".\"" + escapedSchema + "\""
				} else {
					escapedTable := quoteTblName(*tableName)
					suffix = " IN TABLE \"" + escapedCatalog + "\".\"" + escapedSchema + "\".\"" + escapedTable + "\""
				}
			}
		}

		// Detailed constraint info not available in information_schema
		// Need to dispatch SHOW queries and use conn.Raw to extract the queryID for reuse in GetObjects query
		gQueryIDs.Go(func() error {
			return conn.Raw(func(driverConn any) (err error) {
				pkQueryID, err = getQueryID(gQueryIDsCtx, "SHOW PRIMARY KEYS /* ADBC:getObjectsTables */"+suffix, driverConn)
				return err
			})
		})

		gQueryIDs.Go(func() error {
			return conn.Raw(func(driverConn any) (err error) {
				fkQueryID, err = getQueryID(gQueryIDsCtx, "SHOW IMPORTED KEYS /* ADBC:getObjectsTables */"+suffix, driverConn)
				return err
			})
		})

		gQueryIDs.Go(func() error {
			return conn.Raw(func(driverConn any) (err error) {
				uniqueQueryID, err = getQueryID(gQueryIDsCtx, "SHOW UNIQUE KEYS /* ADBC:getObjectsTables */"+suffix, driverConn)
				return err
			})
		})

		gQueryIDs.Go(func() error {
			return conn.Raw(func(driverConn any) (err error) {
				query := "SHOW TERSE /* ADBC:getObjectsDBSchemas */ SCHEMAS"
				if dbSchema != nil && len(*dbSchema) > 0 && *dbSchema != "%" && *dbSchema != ".*" {
					query += " LIKE '" + escapeSingleQuoteForLike(*dbSchema) + "'"
				}
				if catalog == nil || isWildcardStr(*catalog) {
					query += " IN ACCOUNT"
				} else {
					query += " IN DATABASE \"" + quoteTblName(*catalog) + "\""
				}

				showSchemaQueryID, err = getQueryID(gQueryIDsCtx, query, driverConn)
				return
			})
		})

		gQueryIDs.Go(func() error {
			return conn.Raw(func(driverConn any) (err error) {
				query := "SHOW TERSE /* ADBC:getObjectsDBSchemas */ DATABASES"
				if catalog != nil && len(*catalog) > 0 && *catalog != "%" && *catalog != ".*" {
					query += " LIKE '" + escapeSingleQuoteForLike(*catalog) + "'"
				}
				query += " IN ACCOUNT"

				terseDbQueryID, err = getQueryID(gQueryIDsCtx, query, driverConn)
				return
			})
		})

		gQueryIDs.Go(func() error {
			return conn.Raw(func(driverConn any) (err error) {
				objType := "objects"
				if len(tableType) == 1 {
					if strings.EqualFold("VIEW", tableType[0]) {
						objType = "views"
					} else if strings.EqualFold("TABLE", tableType[0]) {
						objType = "tables"
					}
				}

				query := "SHOW TERSE /* ADBC:getObjectsTables */ " + objType
				if tableName != nil && len(*tableName) > 0 && *tableName != "%" && *tableName != ".*" {
					query += " LIKE '" + escapeSingleQuoteForLike(*tableName) + "'"
				}
				if catalog == nil || isWildcardStr(*catalog) {
					query += " IN ACCOUNT"
				} else {
					escapedCatalog := quoteTblName(*catalog)
					if dbSchema == nil || isWildcardStr(*dbSchema) {
						query += " IN DATABASE \"" + escapedCatalog + "\""
					} else {
						query += " IN SCHEMA \"" + escapedCatalog + "\".\"" + quoteTblName(*dbSchema) + "\""
					}
				}

				tableQueryID, err = getQueryID(gQueryIDsCtx, query, driverConn)
				return
			})
		})
	}

	queryBytes, err := fs.ReadFile(queryTemplates, path.Join("queries", queryFile))
	if err != nil {
		return nil, err
	}

	// Need constraint subqueries to complete before we can query GetObjects
	if err := gQueryIDs.Wait(); err != nil {
		return nil, err
	}

	args := []any{
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

	// currently only the Columns / all case still requires a current database/schema
	// to be propagated. The rest of the cases all solely use SHOW queries for the metadata
	// just as done by the snowflake JDBC driver. In those cases we don't need to propagate
	// the current session database/schema.
	if depth == adbc.ObjectDepthColumns || depth == adbc.ObjectDepthAll {
		// the connection that is used is not the same connection context where the database may have been set
		// if the caller called SetCurrentCatalog() so need to ensure the database context is appropriate
		if !isNilOrEmpty(catalog) {
			_, e := conn.ExecContext(context.Background(), fmt.Sprintf("USE DATABASE %s;", quoteTblName(*catalog)), nil)
			if e != nil {
				return nil, errToAdbcErr(adbc.StatusIO, e)
			}
		}

		// the connection that is used is not the same connection context where the schema may have been set
		// if the caller called SetCurrentDbSchema() so need to ensure the schema context is appropriate
		if !isNilOrEmpty(dbSchema) {
			_, e2 := conn.ExecContext(context.Background(), fmt.Sprintf("USE SCHEMA %s;", quoteTblName(*dbSchema)), nil)
			if e2 != nil {
				return nil, errToAdbcErr(adbc.StatusIO, e2)
			}
		}
	}

	query := string(queryBytes)
	rows, err := conn.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	catalogCh := make(chan driverbase.GetObjectsInfo, 5)
	errCh := make(chan error)

	go func() {
		defer close(catalogCh)
		for rows.Next() {
			var getObjectsCatalog driverbase.GetObjectsInfo
			if err := rows.Scan(&getObjectsCatalog); err != nil {
				errCh <- errToAdbcErr(adbc.StatusInvalidData, err)
				return
			}

			// A few columns need additional processing outside of Snowflake
			for i, sch := range getObjectsCatalog.CatalogDbSchemas {
				for j, tab := range sch.DbSchemaTables {
					for k, col := range tab.TableColumns {
						field := c.toArrowField(col)
						xdbcDataType := driverbase.ToXdbcDataType(field.Type)

						getObjectsCatalog.CatalogDbSchemas[i].DbSchemaTables[j].TableColumns[k].XdbcDataType = driverbase.Nullable(int16(field.Type.ID()))
						getObjectsCatalog.CatalogDbSchemas[i].DbSchemaTables[j].TableColumns[k].XdbcSqlDataType = driverbase.Nullable(int16(xdbcDataType))
					}
				}
			}

			catalogCh <- getObjectsCatalog
		}
	}()

	return driverbase.BuildGetObjectsRecordReader(c.Alloc, catalogCh, errCh)
}

func isNilOrEmpty(str *string) bool {
	return str == nil || *str == ""
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

var loc = time.Now().Location()

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
