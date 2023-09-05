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
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow/go/v13/arrow"
	"github.com/apache/arrow/go/v13/arrow/array"
	"github.com/snowflakedb/gosnowflake"
)

const (
	defaultStatementQueueSize  = 200
	defaultPrefetchConcurrency = 10
)

type snowflakeConn interface {
	driver.Conn
	driver.ConnBeginTx
	driver.ConnPrepareContext
	driver.ExecerContext
	driver.QueryerContext
	driver.Pinger
	QueryArrowStream(context.Context, string, ...driver.NamedValue) (gosnowflake.ArrowStreamLoader, error)
}

type cnxn struct {
	cn    snowflakeConn
	db    *database
	ctor  gosnowflake.Connector
	sqldb *sql.DB

	activeTransaction bool
}

// Metadata methods
// Generally these methods return an array.RecordReader that
// can be consumed to retrieve metadata about the database as Arrow
// data. The returned metadata has an expected schema given in the
// doc strings of the specific methods. Schema fields are nullable
// unless otherwise marked. While no Statement is used in these
// methods, the result set may count as an active statement to the
// driver for the purposes of concurrency management (e.g. if the
// driver has a limit on concurrent active statements and it must
// execute a SQL query internally in order to implement the metadata
// method).
//
// Some methods accept "search pattern" arguments, which are strings
// that can contain the special character "%" to match zero or more
// characters, or "_" to match exactly one character. (See the
// documentation of DatabaseMetaData in JDBC or "Pattern Value Arguments"
// in the ODBC documentation.) Escaping is not currently supported.
// GetInfo returns metadata about the database/driver.
//
// The result is an Arrow dataset with the following schema:
//
//	Field Name									| Field Type
//	----------------------------|-----------------------------
//	info_name					   				| uint32 not null
//	info_value									| INFO_SCHEMA
//
// INFO_SCHEMA is a dense union with members:
//
//	Field Name (Type Code)			| Field Type
//	----------------------------|-----------------------------
//	string_value (0)						| utf8
//	bool_value (1)							| bool
//	int64_value (2)							| int64
//	int32_bitmask (3)						| int32
//	string_list (4)							| list<utf8>
//	int32_to_int32_list_map (5)	| map<int32, list<int32>>
//
// Each metadatum is identified by an integer code. The recognized
// codes are defined as constants. Codes [0, 10_000) are reserved
// for ADBC usage. Drivers/vendors will ignore requests for unrecognized
// codes (the row will be omitted from the result).
func (c *cnxn) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	const strValTypeID arrow.UnionTypeCode = 0
	const intValTypeID arrow.UnionTypeCode = 2

	if len(infoCodes) == 0 {
		infoCodes = infoSupportedCodes
	}

	bldr := array.NewRecordBuilder(c.db.alloc, adbc.GetInfoSchema)
	defer bldr.Release()
	bldr.Reserve(len(infoCodes))

	infoNameBldr := bldr.Field(0).(*array.Uint32Builder)
	infoValueBldr := bldr.Field(1).(*array.DenseUnionBuilder)
	strInfoBldr := infoValueBldr.Child(int(strValTypeID)).(*array.StringBuilder)
	intInfoBldr := infoValueBldr.Child(int(intValTypeID)).(*array.Int64Builder)

	for _, code := range infoCodes {
		switch code {
		case adbc.InfoDriverName:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(infoDriverName)
		case adbc.InfoDriverVersion:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(infoDriverVersion)
		case adbc.InfoDriverArrowVersion:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(infoDriverArrowVersion)
		case adbc.InfoDriverADBCVersion:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(intValTypeID)
			intInfoBldr.Append(adbc.AdbcVersion1_1_0)
		case adbc.InfoVendorName:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(infoVendorName)
		default:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.AppendNull()
		}
	}

	final := bldr.NewRecord()
	defer final.Release()
	return array.NewRecordReader(adbc.GetInfoSchema, []arrow.Record{final})
}

// GetObjects gets a hierarchical view of all catalogs, database schemas,
// tables, and columns.
//
// The result is an Arrow Dataset with the following schema:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	catalog_name								| utf8
//	catalog_db_schemas					| list<DB_SCHEMA_SCHEMA>
//
// DB_SCHEMA_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	db_schema_name							| utf8
//	db_schema_tables						|	list<TABLE_SCHEMA>
//
// TABLE_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type
//	----------------------------|----------------------------
//	table_name									| utf8 not null
//	table_type									|	utf8 not null
//	table_columns								| list<COLUMN_SCHEMA>
//	table_constraints						| list<CONSTRAINT_SCHEMA>
//
// COLUMN_SCHEMA is a Struct with the fields:
//
//		Field Name 									| Field Type					| Comments
//		----------------------------|---------------------|---------
//		column_name									| utf8 not null				|
//		ordinal_position						| int32								| (1)
//		remarks											| utf8								| (2)
//		xdbc_data_type							| int16								| (3)
//		xdbc_type_name							| utf8								| (3)
//		xdbc_column_size						| int32								| (3)
//		xdbc_decimal_digits					| int16								| (3)
//		xdbc_num_prec_radix					| int16								| (3)
//		xdbc_nullable								| int16								| (3)
//		xdbc_column_def							| utf8								| (3)
//		xdbc_sql_data_type					| int16								| (3)
//		xdbc_datetime_sub						| int16								| (3)
//		xdbc_char_octet_length			| int32								| (3)
//		xdbc_is_nullable						| utf8								| (3)
//		xdbc_scope_catalog					| utf8								| (3)
//		xdbc_scope_schema						| utf8								| (3)
//		xdbc_scope_table						| utf8								| (3)
//		xdbc_is_autoincrement				| bool								| (3)
//		xdbc_is_generatedcolumn			| bool								| (3)
//
//	 1. The column's ordinal position in the table (starting from 1).
//	 2. Database-specific description of the column.
//	 3. Optional Value. Should be null if not supported by the driver.
//	    xdbc_values are meant to provide JDBC/ODBC-compatible metadata
//	    in an agnostic manner.
//
// CONSTRAINT_SCHEMA is a Struct with the fields:
//
//	Field Name									| Field Type					| Comments
//	----------------------------|---------------------|---------
//	constraint_name							| utf8								|
//	constraint_type							| utf8 not null				| (1)
//	constraint_column_names			| list<utf8> not null | (2)
//	constraint_column_usage			| list<USAGE_SCHEMA>	| (3)
//
// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
// 2. The columns on the current table that are constrained, in order.
// 3. For FOREIGN KEY only, the referenced table and columns.
//
// USAGE_SCHEMA is a Struct with fields:
//
//	Field Name									|	Field Type
//	----------------------------|----------------------------
//	fk_catalog									| utf8
//	fk_db_schema								| utf8
//	fk_table										| utf8 not null
//	fk_column_name							| utf8 not null
//
// For the parameters: If nil is passed, then that parameter will not
// be filtered by at all. If an empty string, then only objects without
// that property (ie: catalog or db schema) will be returned.
//
// tableName and columnName must be either nil (do not filter by
// table name or column name) or non-empty.
//
// All non-empty, non-nil strings should be a search pattern (as described
// earlier).
func (c *cnxn) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	g := internal.GetObjects{Ctx: ctx, Depth: depth, Catalog: catalog, DbSchema: dbSchema, TableName: tableName, ColumnName: columnName, TableType: tableType}
	if err := g.Init(c.db.alloc, c.getObjectsDbSchemas, c.getObjectsTables); err != nil {
		return nil, err
	}
	defer g.Release()

	rows, err := c.sqldb.QueryContext(ctx, "SHOW TERSE DATABASES", nil)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var (
		created              time.Time
		name                 string
		kind, dbname, schema sql.NullString
	)
	for rows.Next() {
		if err := rows.Scan(&created, &name, &kind, &dbname, &schema); err != nil {
			return nil, errToAdbcErr(adbc.StatusInvalidData, err)
		}

		// SNOWFLAKE catalog contains functions and no tables
		if name == "SNOWFLAKE" {
			continue
		}

		// schema for SHOW TERSE DATABASES is:
		// created_on:timestamp, name:text, kind:null, database_name:null, schema_name:null
		// the last three columns are always null because they are not applicable for databases
		// so we want values[1].(string) for the name
		g.AppendCatalog(name)
	}

	return g.Finish()
}

func (c *cnxn) getObjectsDbSchemas(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string) (result map[string][]string, err error) {
	if depth == adbc.ObjectDepthCatalogs {
		return
	}

	conditions := make([]string, 0)
	if catalog != nil && *catalog != "" {
		conditions = append(conditions, ` CATALOG_NAME LIKE \'`+*catalog+`\'`)
	}
	if dbSchema != nil && *dbSchema != "" {
		conditions = append(conditions, ` SCHEMA_NAME LIKE \'`+*dbSchema+`\'`)
	}

	cond := strings.Join(conditions, " AND ")
	if cond != "" {
		cond = `statement := 'SELECT * FROM (' || statement || ') WHERE ` + cond + `';`
	}

	result = make(map[string][]string)
	const queryPrefix = `DECLARE
	    c1 CURSOR FOR SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES;
			res RESULTSET;
			counter INTEGER DEFAULT 0;
			statement VARCHAR DEFAULT '';
		BEGIN
		  FOR rec IN c1 DO
				LET sharelist RESULTSET := (EXECUTE IMMEDIATE 'SHOW SHARES LIKE \'%' || rec.database_name || '%\'');
				LET cnt RESULTSET := (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
				LET cnt_cur CURSOR for cnt;
				LET share_cnt INTEGER DEFAULT 0;
				OPEN cnt_cur;
				FETCH cnt_cur INTO share_cnt;
				CLOSE cnt_cur;

				IF (share_cnt > 0) THEN
					LET c2 CURSOR for sharelist;
					LET created_on TIMESTAMP;
					LET kind VARCHAR DEFAULT '';
					LET share_name VARCHAR DEFAULT '';
					LET dbname VARCHAR DEFAULT '';
					OPEN c2;
					FETCH c2 INTO created_on, kind, share_name, dbname;
					CLOSE c2;
					IF (dbname = '') THEN
						CONTINUE;
					END IF;
				END IF;

				IF (counter > 0) THEN
				  statement := statement || ' UNION ALL ';
				END IF;
				statement := statement || ' SELECT CATALOG_NAME, SCHEMA_NAME FROM ' || rec.database_name || '.INFORMATION_SCHEMA.SCHEMATA';
				counter := counter + 1;
			END FOR;
		  `
	const querySuffix = `
	    res := (EXECUTE IMMEDIATE :statement);
			RETURN TABLE (res);
		END;`

	query := queryPrefix + cond + querySuffix
	var rows *sql.Rows
	rows, err = c.sqldb.QueryContext(ctx, query)
	if err != nil {
		err = errToAdbcErr(adbc.StatusIO, err)
		return
	}
	defer rows.Close()

	var catalogName, schemaName string
	for rows.Next() {
		if err = rows.Scan(&catalogName, &schemaName); err != nil {
			err = errToAdbcErr(adbc.StatusIO, err)
			return
		}

		cat, ok := result[catalogName]
		if !ok {
			cat = make([]string, 0, 1)
		}
		result[catalogName] = append(cat, schemaName)
	}

	return
}

var loc = time.Now().Location()

func toField(name string, isnullable bool, dataType string, numPrec, numPrecRadix, numScale sql.NullInt16, isIdent bool, identGen, identInc, comment sql.NullString, ordinalPos int) (ret arrow.Field) {
	ret.Name, ret.Nullable = name, isnullable
	switch dataType {
	case "NUMBER":
		if !numScale.Valid || numScale.Int16 == 0 {
			ret.Type = arrow.PrimitiveTypes.Int64
		} else {
			ret.Type = arrow.PrimitiveTypes.Float64
		}
	case "FLOAT":
		fallthrough
	case "DOUBLE":
		ret.Type = arrow.PrimitiveTypes.Float64
	case "TEXT":
		ret.Type = arrow.BinaryTypes.String
	case "BINARY":
		ret.Type = arrow.BinaryTypes.Binary
	case "BOOLEAN":
		ret.Type = arrow.FixedWidthTypes.Boolean
	case "ARRAY":
		fallthrough
	case "VARIANT":
		fallthrough
	case "OBJECT":
		// snowflake will return each value as a string
		ret.Type = arrow.BinaryTypes.String
	case "DATE":
		ret.Type = arrow.FixedWidthTypes.Date32
	case "TIME":
		ret.Type = arrow.FixedWidthTypes.Time64ns
	case "DATETIME":
		fallthrough
	case "TIMESTAMP", "TIMESTAMP_NTZ":
		ret.Type = &arrow.TimestampType{Unit: arrow.Nanosecond}
	case "TIMESTAMP_LTZ":
		ret.Type = &arrow.TimestampType{Unit: arrow.Nanosecond, TimeZone: loc.String()}
	case "TIMESTAMP_TZ":
		ret.Type = arrow.FixedWidthTypes.Timestamp_ns
	case "GEOGRAPHY":
		fallthrough
	case "GEOMETRY":
		ret.Type = arrow.BinaryTypes.String
	}

	md := make(map[string]string)
	md["TYPE_NAME"] = dataType
	if isIdent {
		md["IS_IDENTITY"] = "YES"
		md["IDENTITY_GENERATION"] = identGen.String
		md["IDENTITY_INCREMENT"] = identInc.String
	}
	if comment.Valid {
		md["COMMENT"] = comment.String
	}
	md["ORDINAL_POSITION"] = strconv.Itoa(ordinalPos)

	ret.Metadata = arrow.MetadataFrom(md)
	return
}

func (c *cnxn) getObjectsTables(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (result internal.SchemaToTableInfo, err error) {
	if depth == adbc.ObjectDepthCatalogs || depth == adbc.ObjectDepthDBSchemas {
		return
	}

	result = make(internal.SchemaToTableInfo)
	includeSchema := depth == adbc.ObjectDepthAll || depth == adbc.ObjectDepthColumns

	conditions := make([]string, 0)
	if catalog != nil && *catalog != "" {
		conditions = append(conditions, ` TABLE_CATALOG ILIKE \'`+*catalog+`\'`)
	}
	if dbSchema != nil && *dbSchema != "" {
		conditions = append(conditions, ` TABLE_SCHEMA ILIKE \'`+*dbSchema+`\'`)
	}
	if tableName != nil && *tableName != "" {
		conditions = append(conditions, ` TABLE_NAME ILIKE \'`+*tableName+`\'`)
	}

	const queryPrefix = `DECLARE
		c1 CURSOR FOR SELECT DATABASE_NAME FROM INFORMATION_SCHEMA.DATABASES;
		res RESULTSET;
		counter INTEGER DEFAULT 0;
		statement VARCHAR DEFAULT '';
	BEGIN
		FOR rec IN c1 DO
			LET sharelist RESULTSET := (EXECUTE IMMEDIATE 'SHOW SHARES LIKE \'%' || rec.database_name || '%\'');
			LET cnt RESULTSET := (SELECT COUNT(*) FROM TABLE(RESULT_SCAN(LAST_QUERY_ID())));
			LET cnt_cur CURSOR for cnt;
			LET share_cnt INTEGER DEFAULT 0;
			OPEN cnt_cur;
			FETCH cnt_cur INTO share_cnt;
			CLOSE cnt_cur;

			IF (share_cnt > 0) THEN
				LET c2 CURSOR for sharelist;
				LET created_on TIMESTAMP;
				LET kind VARCHAR DEFAULT '';
				LET share_name VARCHAR DEFAULT '';
				LET dbname VARCHAR DEFAULT '';
				OPEN c2;
				FETCH c2 INTO created_on, kind, share_name, dbname;
				CLOSE c2;
				IF (dbname = '') THEN
					CONTINUE;
				END IF;
			END IF;
			IF (counter > 0) THEN
				statement := statement || ' UNION ALL ';
			END IF;
			`

	const noSchema = `statement := statement || ' SELECT table_catalog, table_schema, table_name, table_type FROM ' || rec.database_name || '.INFORMATION_SCHEMA.TABLES';
			counter := counter + 1;
		END FOR;
		`

	const getSchema = `statement := statement ||
		' SELECT
				table_catalog, table_schema, table_name, column_name,
				ordinal_position, is_nullable::boolean, data_type, numeric_precision,
				numeric_precision_radix, numeric_scale, is_identity::boolean,
				identity_generation, identity_increment, comment
		FROM ' || rec.database_name || '.INFORMATION_SCHEMA.COLUMNS';

		  counter := counter + 1;
		END FOR;
	  `

	const querySuffix = `
		res := (EXECUTE IMMEDIATE :statement);
		RETURN TABLE (res);
	END;`

	// first populate the tables and table types
	var rows *sql.Rows
	var tblConditions []string
	if len(tableType) > 0 {
		tblConditions = append(conditions, ` TABLE_TYPE IN (\'`+strings.Join(tableType, `\',\'`)+`\')`)
	} else {
		tblConditions = conditions
	}

	cond := strings.Join(tblConditions, " AND ")
	if cond != "" {
		cond = `statement := 'SELECT * FROM (' || statement || ') WHERE ` + cond + `';`
	}
	query := queryPrefix + noSchema + cond + querySuffix
	rows, err = c.sqldb.QueryContext(ctx, query)
	if err != nil {
		err = errToAdbcErr(adbc.StatusIO, err)
		return
	}
	defer rows.Close()

	var tblCat, tblSchema, tblName string
	var tblType sql.NullString
	for rows.Next() {
		if err = rows.Scan(&tblCat, &tblSchema, &tblName, &tblType); err != nil {
			err = errToAdbcErr(adbc.StatusIO, err)
			return
		}

		key := internal.CatalogAndSchema{
			Catalog: tblCat, Schema: tblSchema}

		result[key] = append(result[key], internal.TableInfo{
			Name: tblName, TableType: tblType.String})
	}

	if includeSchema {
		// if we need to include the schemas of the tables, make another fetch
		// to fetch the columns and column info
		if columnName != nil && *columnName != "" {
			conditions = append(conditions, ` column_name ILIKE \'`+*columnName+`\'`)
		}
		cond = strings.Join(conditions, " AND ")
		if cond != "" {
			cond = " WHERE " + cond
		}
		cond = `statement := 'SELECT * FROM (' || statement || ')` + cond +
			` ORDER BY table_catalog, table_schema, table_name, ordinal_position';`
		query = queryPrefix + getSchema + cond + querySuffix
		rows, err = c.sqldb.QueryContext(ctx, query)
		if err != nil {
			return
		}
		defer rows.Close()

		var (
			colName, dataType                           string
			identGen, identIncrement, comment           sql.NullString
			ordinalPos                                  int
			numericPrec, numericPrecRadix, numericScale sql.NullInt16
			isNullable, isIdent                         bool

			prevKey      internal.CatalogAndSchema
			curTableInfo *internal.TableInfo
			fieldList    = make([]arrow.Field, 0)
		)

		for rows.Next() {
			// order here matches the order of the columns requested in the query
			err = rows.Scan(&tblCat, &tblSchema, &tblName, &colName,
				&ordinalPos, &isNullable, &dataType, &numericPrec,
				&numericPrecRadix, &numericScale, &isIdent, &identGen,
				&identIncrement, &comment)
			if err != nil {
				err = errToAdbcErr(adbc.StatusIO, err)
				return
			}

			key := internal.CatalogAndSchema{Catalog: tblCat, Schema: tblSchema}
			if prevKey != key || (curTableInfo != nil && curTableInfo.Name != tblName) {
				if len(fieldList) > 0 && curTableInfo != nil {
					curTableInfo.Schema = arrow.NewSchema(fieldList, nil)
					fieldList = fieldList[:0]
				}

				info := result[key]
				for i := range info {
					if info[i].Name == tblName {
						curTableInfo = &info[i]
						break
					}
				}
			}

			prevKey = key
			fieldList = append(fieldList, toField(colName, isNullable, dataType, numericPrec, numericPrecRadix, numericScale, isIdent, identGen, identIncrement, comment, ordinalPos))
		}

		if len(fieldList) > 0 && curTableInfo != nil {
			curTableInfo.Schema = arrow.NewSchema(fieldList, nil)
		}
	}
	return
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

func (c *cnxn) GetOption(key string) (string, error) {
	switch key {
	case adbc.OptionKeyAutoCommit:
		if c.activeTransaction {
			// No autocommit
			return adbc.OptionValueDisabled, nil
		} else {
			// Autocommit
			return adbc.OptionValueEnabled, nil
		}
	case adbc.OptionKeyCurrentCatalog:
		return c.getStringQuery("SELECT CURRENT_DATABASE()")
	case adbc.OptionKeyCurrentDbSchema:
		return c.getStringQuery("SELECT CURRENT_SCHEMA()")
	}

	return "", adbc.Error{
		Msg:  "[Snowflake] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *cnxn) getStringQuery(query string) (string, error) {
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

func (c *cnxn) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Msg:  "[Snowflake] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *cnxn) GetOptionInt(key string) (int64, error) {
	return 0, adbc.Error{
		Msg:  "[Snowflake] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *cnxn) GetOptionDouble(key string) (float64, error) {
	return 0.0, adbc.Error{
		Msg:  "[Snowflake] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *cnxn) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	tblParts := make([]string, 0, 3)
	if catalog != nil {
		tblParts = append(tblParts, strconv.Quote(strings.ToUpper(*catalog)))
	}
	if dbSchema != nil {
		tblParts = append(tblParts, strconv.Quote(strings.ToUpper(*dbSchema)))
	}
	tblParts = append(tblParts, strconv.Quote(strings.ToUpper(tableName)))
	fullyQualifiedTable := strings.Join(tblParts, ".")

	rows, err := c.sqldb.QueryContext(ctx, `DESC TABLE `+fullyQualifiedTable)
	if err != nil {
		return nil, errToAdbcErr(adbc.StatusIO, err)
	}
	defer rows.Close()

	var (
		name, typ, kind, isnull, primary, unique string
		def, check, expr, comment, policyName    sql.NullString
		fields                                   = []arrow.Field{}
	)

	for rows.Next() {
		err := rows.Scan(&name, &typ, &kind, &isnull, &def, &primary, &unique,
			&check, &expr, &comment, &policyName)
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

// GetTableTypes returns a list of the table types in the database.
//
// The result is an arrow dataset with the following schema:
//
//	Field Name			| Field Type
//	----------------|--------------
//	table_type			| utf8 not null
func (c *cnxn) GetTableTypes(_ context.Context) (array.RecordReader, error) {
	bldr := array.NewRecordBuilder(c.db.alloc, adbc.TableTypesSchema)
	defer bldr.Release()

	bldr.Field(0).(*array.StringBuilder).AppendValues([]string{"BASE TABLE", "TEMPORARY TABLE", "VIEW"}, nil)
	final := bldr.NewRecord()
	defer final.Release()
	return array.NewRecordReader(adbc.TableTypesSchema, []arrow.Record{final})
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
func (c *cnxn) Commit(_ context.Context) error {
	if !c.activeTransaction {
		return adbc.Error{
			Msg:  "no active transaction, cannot commit",
			Code: adbc.StatusInvalidState,
		}
	}

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
func (c *cnxn) Rollback(_ context.Context) error {
	if !c.activeTransaction {
		return adbc.Error{
			Msg:  "no active transaction, cannot rollback",
			Code: adbc.StatusInvalidState,
		}
	}

	_, err := c.cn.ExecContext(context.Background(), "ROLLBACK", nil)
	if err != nil {
		return errToAdbcErr(adbc.StatusInternal, err)
	}

	_, err = c.cn.ExecContext(context.Background(), "BEGIN", nil)
	return errToAdbcErr(adbc.StatusInternal, err)
}

// NewStatement initializes a new statement object tied to this connection
func (c *cnxn) NewStatement() (adbc.Statement, error) {
	return &statement{
		alloc:               c.db.alloc,
		cnxn:                c,
		queueSize:           defaultStatementQueueSize,
		prefetchConcurrency: defaultPrefetchConcurrency,
	}, nil
}

// Close closes this connection and releases any associated resources.
func (c *cnxn) Close() error {
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
func (c *cnxn) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, adbc.Error{
		Code: adbc.StatusNotImplemented,
		Msg:  "ReadPartition not yet implemented for snowflake driver",
	}
}

func (c *cnxn) SetOption(key, value string) error {
	switch key {
	case adbc.OptionKeyAutoCommit:
		switch value {
		case adbc.OptionValueEnabled:
			if c.activeTransaction {
				_, err := c.cn.ExecContext(context.Background(), "COMMIT", nil)
				if err != nil {
					return errToAdbcErr(adbc.StatusInternal, err)
				}
				c.activeTransaction = false
			}
			_, err := c.cn.ExecContext(context.Background(), "ALTER SESSION SET AUTOCOMMIT = true", nil)
			return err
		case adbc.OptionValueDisabled:
			if !c.activeTransaction {
				_, err := c.cn.ExecContext(context.Background(), "BEGIN", nil)
				if err != nil {
					return errToAdbcErr(adbc.StatusInternal, err)
				}
				c.activeTransaction = true
			}
			_, err := c.cn.ExecContext(context.Background(), "ALTER SESSION SET AUTOCOMMIT = false", nil)
			return err
		default:
			return adbc.Error{
				Msg:  "[Snowflake] invalid value for option " + key + ": " + value,
				Code: adbc.StatusInvalidArgument,
			}
		}
	case adbc.OptionKeyCurrentCatalog:
		_, err := c.cn.ExecContext(context.Background(), "USE DATABASE ?", []driver.NamedValue{{Value: value}})
		return err
	case adbc.OptionKeyCurrentDbSchema:
		_, err := c.cn.ExecContext(context.Background(), "USE SCHEMA ?", []driver.NamedValue{{Value: value}})
		return err
	default:
		return adbc.Error{
			Msg:  "[Snowflake] unknown connection option " + key + ": " + value,
			Code: adbc.StatusInvalidArgument,
		}
	}
}

func (c *cnxn) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Msg:  "[Snowflake] unknown connection option",
		Code: adbc.StatusNotImplemented,
	}
}

func (c *cnxn) SetOptionInt(key string, value int64) error {
	return adbc.Error{
		Msg:  "[Snowflake] unknown connection option",
		Code: adbc.StatusNotImplemented,
	}
}

func (c *cnxn) SetOptionDouble(key string, value float64) error {
	return adbc.Error{
		Msg:  "[Snowflake] unknown connection option",
		Code: adbc.StatusNotImplemented,
	}
}
