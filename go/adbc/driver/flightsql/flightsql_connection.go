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

package flightsql

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow/go/v14/arrow"
	"github.com/apache/arrow/go/v14/arrow/array"
	"github.com/apache/arrow/go/v14/arrow/flight"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v14/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow/go/v14/arrow/ipc"
	"github.com/bluele/gcache"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type cnxn struct {
	cl *flightsql.Client

	db          *databaseImpl
	clientCache gcache.Cache
	hdrs        metadata.MD
	timeouts    timeoutOption
	txn         *flightsql.Txn
	supportInfo support
}

var adbcToFlightSQLInfo = map[adbc.InfoCode]flightsql.SqlInfo{
	adbc.InfoVendorName:         flightsql.SqlInfoFlightSqlServerName,
	adbc.InfoVendorVersion:      flightsql.SqlInfoFlightSqlServerVersion,
	adbc.InfoVendorArrowVersion: flightsql.SqlInfoFlightSqlServerArrowVersion,
}

func doGet(ctx context.Context, cl *flightsql.Client, endpoint *flight.FlightEndpoint, clientCache gcache.Cache, opts ...grpc.CallOption) (rdr *flight.Reader, err error) {
	if len(endpoint.Location) == 0 {
		return cl.DoGet(ctx, endpoint.Ticket, opts...)
	}

	var (
		cc interface{}
	)

	for _, loc := range endpoint.Location {
		cc, err = clientCache.Get(loc.Uri)
		if err != nil {
			continue
		}

		conn := cc.(*flightsql.Client)
		rdr, err = conn.DoGet(ctx, endpoint.Ticket, opts...)
		if err != nil {
			continue
		}

		return
	}

	return nil, err
}

func (c *cnxn) GetOption(key string) (string, error) {
	if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
		name := strings.TrimPrefix(key, OptionRPCCallHeaderPrefix)
		headers := c.hdrs.Get(name)
		if len(headers) > 0 {
			return headers[0], nil
		}
		return "", adbc.Error{
			Msg:  "[Flight SQL] unknown header",
			Code: adbc.StatusNotFound,
		}
	}

	switch key {
	case OptionTimeoutFetch:
		return c.timeouts.fetchTimeout.String(), nil
	case OptionTimeoutQuery:
		return c.timeouts.queryTimeout.String(), nil
	case OptionTimeoutUpdate:
		return c.timeouts.updateTimeout.String(), nil
	case adbc.OptionKeyAutoCommit:
		if c.txn != nil {
			// No autocommit
			return adbc.OptionValueDisabled, nil
		} else {
			// Autocommit
			return adbc.OptionValueEnabled, nil
		}
	case adbc.OptionKeyCurrentCatalog:
		return "", adbc.Error{
			Msg:  "[Flight SQL] current catalog not supported",
			Code: adbc.StatusNotFound,
		}

	case adbc.OptionKeyCurrentDbSchema:
		return "", adbc.Error{
			Msg:  "[Flight SQL] current schema not supported",
			Code: adbc.StatusNotFound,
		}
	}

	return "", adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *cnxn) GetOptionBytes(key string) ([]byte, error) {
	return nil, adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *cnxn) GetOptionInt(key string) (int64, error) {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		val, err := c.GetOptionDouble(key)
		if err != nil {
			return 0, err
		}
		return int64(val), nil
	}

	return 0, adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *cnxn) GetOptionDouble(key string) (float64, error) {
	switch key {
	case OptionTimeoutFetch:
		return c.timeouts.fetchTimeout.Seconds(), nil
	case OptionTimeoutQuery:
		return c.timeouts.queryTimeout.Seconds(), nil
	case OptionTimeoutUpdate:
		return c.timeouts.updateTimeout.Seconds(), nil
	}

	return 0.0, adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *cnxn) SetOption(key, value string) error {
	if strings.HasPrefix(key, OptionRPCCallHeaderPrefix) {
		name := strings.TrimPrefix(key, OptionRPCCallHeaderPrefix)
		if value == "" {
			c.hdrs.Delete(name)
		} else {
			c.hdrs.Append(name, value)
		}
		return nil
	}

	switch key {
	case OptionTimeoutFetch, OptionTimeoutQuery, OptionTimeoutUpdate:
		return c.timeouts.setTimeoutString(key, value)
	case adbc.OptionKeyAutoCommit:
		autocommit := true
		switch value {
		case adbc.OptionValueEnabled:
			// Do nothing
		case adbc.OptionValueDisabled:
			autocommit = false
		default:
			return adbc.Error{
				Msg:  "[Flight SQL] invalid value for option " + key + ": " + value,
				Code: adbc.StatusInvalidArgument,
			}
		}

		if autocommit && c.txn == nil {
			// no-op don't even error if the server didn't support transactions
			return nil
		}

		if !c.supportInfo.transactions {
			return errNoTransactionSupport
		}

		ctx := metadata.NewOutgoingContext(context.Background(), c.hdrs)
		var err error
		if c.txn != nil {
			if err = c.txn.Commit(ctx, c.timeouts); err != nil {
				return adbc.Error{
					Msg:  "[Flight SQL] failed to update autocommit: " + err.Error(),
					Code: adbc.StatusIO,
				}
			}
		}

		if autocommit {
			c.txn = nil
			return nil
		}

		if c.txn, err = c.cl.BeginTransaction(ctx, c.timeouts); err != nil {
			return adbc.Error{
				Msg:  "[Flight SQL] failed to update autocommit: " + err.Error(),
				Code: adbc.StatusIO,
			}
		}
		return nil

	default:
		return adbc.Error{
			Msg:  "[Flight SQL] unknown connection option",
			Code: adbc.StatusNotImplemented,
		}
	}
}

func (c *cnxn) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotImplemented,
	}
}

func (c *cnxn) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionTimeoutFetch, OptionTimeoutQuery, OptionTimeoutUpdate:
		return c.timeouts.setTimeout(key, float64(value))
	}

	return adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotImplemented,
	}
}

func (c *cnxn) SetOptionDouble(key string, value float64) error {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		return c.timeouts.setTimeout(key, value)
	}

	return adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotImplemented,
	}
}

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

	bldr := array.NewRecordBuilder(c.cl.Alloc, adbc.GetInfoSchema)
	defer bldr.Release()
	bldr.Reserve(len(infoCodes))

	infoNameBldr := bldr.Field(0).(*array.Uint32Builder)
	infoValueBldr := bldr.Field(1).(*array.DenseUnionBuilder)
	strInfoBldr := infoValueBldr.Child(int(strValTypeID)).(*array.StringBuilder)
	intInfoBldr := infoValueBldr.Child(int(intValTypeID)).(*array.Int64Builder)

	translated := make([]flightsql.SqlInfo, 0, len(infoCodes))
	for _, code := range infoCodes {
		if t, ok := adbcToFlightSQLInfo[code]; ok {
			translated = append(translated, t)
			continue
		}

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
		}
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	info, err := c.cl.GetSqlInfo(ctx, translated, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err == nil {
		for i, endpoint := range info.Endpoint {
			var header, trailer metadata.MD
			rdr, err := doGet(ctx, c.cl, endpoint, c.clientCache, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
			if err != nil {
				return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetInfo(DoGet): endpoint %d: %s", i, endpoint.Location)
			}

			for rdr.Next() {
				rec := rdr.Record()
				field := rec.Column(0).(*array.Uint32)
				info := rec.Column(1).(*array.DenseUnion)

				for i := 0; i < int(rec.NumRows()); i++ {
					switch flightsql.SqlInfo(field.Value(i)) {
					case flightsql.SqlInfoFlightSqlServerName:
						infoNameBldr.Append(uint32(adbc.InfoVendorName))
					case flightsql.SqlInfoFlightSqlServerVersion:
						infoNameBldr.Append(uint32(adbc.InfoVendorVersion))
					case flightsql.SqlInfoFlightSqlServerArrowVersion:
						infoNameBldr.Append(uint32(adbc.InfoVendorArrowVersion))
					default:
						continue
					}

					infoValueBldr.Append(info.TypeCode(i))
					// we know we're only doing string fields here right now
					v := info.Field(info.ChildID(i)).(*array.String).
						Value(int(info.ValueOffset(i)))
					strInfoBldr.Append(v)
				}
			}

			if err := checkContext(rdr.Err(), ctx); err != nil {
				return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetInfo(DoGet): endpoint %d: %s", i, endpoint.Location)
			}
		}
	} else if grpcstatus.Code(err) != grpccodes.Unimplemented {
		return nil, adbcFromFlightStatus(err, "GetInfo(GetSqlInfo)")
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
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	g := internal.GetObjects{Ctx: ctx, Depth: depth, Catalog: catalog, DbSchema: dbSchema, TableName: tableName, ColumnName: columnName, TableType: tableType}
	if err := g.Init(c.db.Alloc, c.getObjectsDbSchemas, c.getObjectsTables); err != nil {
		return nil, err
	}
	defer g.Release()

	var header, trailer metadata.MD
	// To avoid an N+1 query problem, we assume result sets here will fit in memory and build up a single response.
	info, err := c.cl.GetCatalogs(ctx, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetCatalogs)")
	}

	header = metadata.MD{}
	trailer = metadata.MD{}
	rdr, err := c.readInfo(ctx, schema_ref.Catalogs, info, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetCatalogs)")
	}
	defer rdr.Release()

	foundCatalog := false
	for rdr.Next() {
		arr := rdr.Record().Column(0).(*array.String)
		for i := 0; i < arr.Len(); i++ {
			// XXX: force copy since accessor is unsafe
			catalogName := string([]byte(arr.Value(i)))
			g.AppendCatalog(catalogName)
			foundCatalog = true
		}
	}

	// Implementations like Dremio report no catalogs, but still have schemas
	if !foundCatalog && depth != adbc.ObjectDepthCatalogs {
		g.AppendCatalog("")
	}

	if err := checkContext(rdr.Err(), ctx); err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetCatalogs)")
	}
	return g.Finish()
}

// Helper function to read and validate a metadata stream
func (c *cnxn) readInfo(ctx context.Context, expectedSchema *arrow.Schema, info *flight.FlightInfo, opts ...grpc.CallOption) (array.RecordReader, error) {
	// use a default queueSize for the reader
	rdr, err := newRecordReader(ctx, c.db.Alloc, c.cl, info, c.clientCache, 5, opts...)
	if err != nil {
		return nil, adbcFromFlightStatus(err, "DoGet")
	}

	if !rdr.Schema().Equal(expectedSchema) {
		rdr.Release()
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("Invalid schema returned for: expected %s, got %s", expectedSchema.String(), rdr.Schema().String()),
			Code: adbc.StatusInternal,
		}
	}
	return rdr, nil
}

// Helper function to build up a map of catalogs to DB schemas
func (c *cnxn) getObjectsDbSchemas(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, metadataRecords []internal.Metadata) (result map[string][]string, err error) {
	if depth == adbc.ObjectDepthCatalogs {
		return
	}
	result = make(map[string][]string)
	var header, trailer metadata.MD
	// Pre-populate the map of which schemas are in which catalogs
	info, err := c.cl.GetDBSchemas(ctx, &flightsql.GetDBSchemasOpts{DbSchemaFilterPattern: dbSchema}, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetDBSchemas)")
	}

	header = metadata.MD{}
	trailer = metadata.MD{}
	rdr, err := c.readInfo(ctx, schema_ref.DBSchemas, info, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetDBSchemas)")
	}
	defer rdr.Release()

	for rdr.Next() {
		// Nullable
		catalog := rdr.Record().Column(0).(*array.String)
		// Non-nullable
		dbSchema := rdr.Record().Column(1).(*array.String)

		for i := 0; i < catalog.Len(); i++ {
			catalogName := ""
			if !catalog.IsNull(i) {
				catalogName = string([]byte(catalog.Value(i)))
			}
			result[catalogName] = append(result[catalogName], string([]byte(dbSchema.Value(i))))
		}
	}

	if err := checkContext(rdr.Err(), ctx); err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetCatalogs)")
	}
	return
}

func (c *cnxn) getObjectsTables(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string, metadataRecords []internal.Metadata) (result internal.SchemaToTableInfo, err error) {
	if depth == adbc.ObjectDepthCatalogs || depth == adbc.ObjectDepthDBSchemas {
		return
	}
	result = make(map[internal.CatalogAndSchema][]internal.TableInfo)

	// Pre-populate the map of which schemas are in which catalogs
	includeSchema := depth == adbc.ObjectDepthAll || depth == adbc.ObjectDepthColumns
	var header, trailer metadata.MD
	info, err := c.cl.GetTables(ctx, &flightsql.GetTablesOpts{
		DbSchemaFilterPattern:  dbSchema,
		TableNameFilterPattern: tableName,
		TableTypes:             tableType,
		IncludeSchema:          includeSchema,
	}, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetTables)")
	}

	expectedSchema := schema_ref.Tables
	if includeSchema {
		expectedSchema = schema_ref.TablesWithIncludedSchema
	}
	header = metadata.MD{}
	trailer = metadata.MD{}
	rdr, err := c.readInfo(ctx, expectedSchema, info, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatus(err, "GetObjects(GetTables)")
	}
	defer rdr.Release()

	for rdr.Next() {
		// Nullable
		catalog := rdr.Record().Column(0).(*array.String)
		dbSchema := rdr.Record().Column(1).(*array.String)
		// Non-nullable
		tableName := rdr.Record().Column(2).(*array.String)
		tableType := rdr.Record().Column(3).(*array.String)

		for i := 0; i < catalog.Len(); i++ {
			catalogName := ""
			dbSchemaName := ""
			if !catalog.IsNull(i) {
				catalogName = string([]byte(catalog.Value(i)))
			}
			if !dbSchema.IsNull(i) {
				dbSchemaName = string([]byte(dbSchema.Value(i)))
			}
			key := internal.CatalogAndSchema{
				Catalog: catalogName,
				Schema:  dbSchemaName,
			}

			var schema *arrow.Schema
			if includeSchema {
				reader, err := ipc.NewReader(bytes.NewReader(rdr.Record().Column(4).(*array.Binary).Value(i)))
				if err != nil {
					return nil, adbc.Error{
						Msg:  err.Error(),
						Code: adbc.StatusInternal,
					}
				}
				schema = reader.Schema()
				reader.Release()
			}

			result[key] = append(result[key], internal.TableInfo{
				Name:      string([]byte(tableName.Value(i))),
				TableType: string([]byte(tableType.Value(i))),
				Schema:    schema,
			})
		}
	}

	if err := checkContext(rdr.Err(), ctx); err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetTables)")
	}
	return
}

func (c *cnxn) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	opts := &flightsql.GetTablesOpts{
		Catalog:                catalog,
		DbSchemaFilterPattern:  dbSchema,
		TableNameFilterPattern: &tableName,
		IncludeSchema:          true,
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	info, err := c.cl.GetTables(ctx, opts, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetTableSchema(GetTables)")
	}

	header = metadata.MD{}
	trailer = metadata.MD{}
	rdr, err := doGet(ctx, c.cl, info.Endpoint[0], c.clientCache, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetTableSchema(DoGet)")
	}
	defer rdr.Release()

	rec, err := rdr.Read()
	if err != nil {
		if err == io.EOF {
			return nil, adbc.Error{
				Msg:  "No table found",
				Code: adbc.StatusNotFound,
			}
		}
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetTableSchema(DoGet)")
	}

	numRows := rec.NumRows()
	switch {
	case numRows == 0:
		return nil, adbc.Error{
			Code: adbc.StatusNotFound,
		}
	case numRows > math.MaxInt32:
		return nil, adbc.Error{
			Msg:  "[Flight SQL] GetTableSchema cannot handle tables with number of rows > 2^31 - 1",
			Code: adbc.StatusNotImplemented,
		}
	}

	var s *arrow.Schema
	for i := 0; i < int(numRows); i++ {
		currentTableName := rec.Column(2).(*array.String).Value(i)
		if currentTableName == tableName {
			// returned schema should be
			//    0: catalog_name: utf8
			//    1: db_schema_name: utf8
			//    2: table_name: utf8 not null
			//    3: table_type: utf8 not null
			//    4: table_schema: bytes not null
			schemaBytes := rec.Column(4).(*array.Binary).Value(i)
			s, err = flight.DeserializeSchema(schemaBytes, c.db.Alloc)
			if err != nil {
				return nil, adbcFromFlightStatus(err, "GetTableSchema")
			}
			return s, nil
		}
	}

	return s, adbc.Error{
		Msg:  "[Flight SQL] GetTableSchema could not find a table with a matching schema",
		Code: adbc.StatusNotFound,
	}
}

// GetTableTypes returns a list of the table types in the database.
//
// The result is an arrow dataset with the following schema:
//
//	Field Name			| Field Type
//	----------------|--------------
//	table_type			| utf8 not null
func (c *cnxn) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	info, err := c.cl.GetTableTypes(ctx, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetTableTypes")
	}

	return newRecordReader(ctx, c.db.Alloc, c.cl, info, c.clientCache, 5)
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
// When not supported, the convention is that it should act as if autocommit
// is enabled and return INVALID_STATE errors.
func (c *cnxn) Commit(ctx context.Context) error {
	if c.txn == nil {
		return adbc.Error{
			Msg:  "[Flight SQL] Cannot commit when autocommit is enabled",
			Code: adbc.StatusInvalidState,
		}
	}

	if !c.supportInfo.transactions {
		return errNoTransactionSupport
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	err := c.txn.Commit(ctx, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return adbcFromFlightStatusWithDetails(err, header, trailer, "Commit")
	}

	header = metadata.MD{}
	trailer = metadata.MD{}
	c.txn, err = c.cl.BeginTransaction(ctx, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return adbcFromFlightStatusWithDetails(err, header, trailer, "BeginTransaction")
	}
	return nil
}

// Rollback rolls back any pending transactions. Only used if autocommit
// is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
// When not supported, the convention is that it should act as if autocommit
// is enabled and return INVALID_STATE errors.
func (c *cnxn) Rollback(ctx context.Context) error {
	if c.txn == nil {
		return adbc.Error{
			Msg:  "[Flight SQL] Cannot rollback when autocommit is enabled",
			Code: adbc.StatusInvalidState,
		}
	}

	if !c.supportInfo.transactions {
		return errNoTransactionSupport
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	err := c.txn.Rollback(ctx, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return adbcFromFlightStatusWithDetails(err, header, trailer, "Rollback")
	}

	header = metadata.MD{}
	trailer = metadata.MD{}
	c.txn, err = c.cl.BeginTransaction(ctx, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return adbcFromFlightStatusWithDetails(err, header, trailer, "BeginTransaction")
	}
	return nil
}

// NewStatement initializes a new statement object tied to this connection
func (c *cnxn) NewStatement() (adbc.Statement, error) {
	return &statement{
		alloc:       c.db.Alloc,
		clientCache: c.clientCache,
		hdrs:        c.hdrs.Copy(),
		queueSize:   5,
		timeouts:    c.timeouts,
		cnxn:        c,
	}, nil
}

func (c *cnxn) execute(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	if c.txn != nil {
		return c.txn.Execute(ctx, query, opts...)
	}

	return c.cl.Execute(ctx, query, opts...)
}

func (c *cnxn) executeSchema(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.SchemaResult, error) {
	if c.txn != nil {
		return c.txn.GetExecuteSchema(ctx, query, opts...)
	}

	return c.cl.GetExecuteSchema(ctx, query, opts...)
}

func (c *cnxn) executeSubstrait(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	if c.txn != nil {
		return c.txn.ExecuteSubstrait(ctx, plan, opts...)
	}

	return c.cl.ExecuteSubstrait(ctx, plan, opts...)
}

func (c *cnxn) executeSubstraitSchema(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (*flight.SchemaResult, error) {
	if c.txn != nil {
		return c.txn.GetExecuteSubstraitSchema(ctx, plan, opts...)
	}

	return c.cl.GetExecuteSubstraitSchema(ctx, plan, opts...)
}

func (c *cnxn) executeUpdate(ctx context.Context, query string, opts ...grpc.CallOption) (n int64, err error) {
	if c.txn != nil {
		return c.txn.ExecuteUpdate(ctx, query, opts...)
	}

	return c.cl.ExecuteUpdate(ctx, query, opts...)
}

func (c *cnxn) executeSubstraitUpdate(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (n int64, err error) {
	if c.txn != nil {
		return c.txn.ExecuteSubstraitUpdate(ctx, plan, opts...)
	}

	return c.cl.ExecuteSubstraitUpdate(ctx, plan, opts...)
}

func (c *cnxn) prepare(ctx context.Context, query string, opts ...grpc.CallOption) (*flightsql.PreparedStatement, error) {
	if c.txn != nil {
		return c.txn.Prepare(ctx, query, opts...)
	}

	return c.cl.Prepare(ctx, query, opts...)
}

func (c *cnxn) prepareSubstrait(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (*flightsql.PreparedStatement, error) {
	if c.txn != nil {
		return c.txn.PrepareSubstrait(ctx, plan, opts...)
	}

	return c.cl.PrepareSubstrait(ctx, plan, opts...)
}

// Close closes this connection and releases any associated resources.
func (c *cnxn) Close() error {
	if c.cl == nil {
		return adbc.Error{
			Msg:  "[Flight SQL Connection] trying to close already closed connection",
			Code: adbc.StatusInvalidState,
		}
	}

	err := c.cl.Close()
	c.cl = nil
	return adbcFromFlightStatus(err, "Close")
}

// ReadPartition constructs a statement for a partition of a query. The
// results can then be read independently using the returned RecordReader.
//
// A partition can be retrieved by using ExecutePartitions on a statement.
func (c *cnxn) ReadPartition(ctx context.Context, serializedPartition []byte) (rdr array.RecordReader, err error) {
	var info flight.FlightInfo
	if err := proto.Unmarshal(serializedPartition, &info); err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusInvalidArgument,
		}
	}

	// The driver only ever returns one endpoint.
	if len(info.Endpoint) != 1 {
		return nil, adbc.Error{
			Msg:  fmt.Sprintf("Invalid partition: expected 1 endpoint, got %d", len(info.Endpoint)),
			Code: adbc.StatusInvalidArgument,
		}
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	rdr, err = doGet(ctx, c.cl, info.Endpoint[0], c.clientCache, c.timeouts)
	if err != nil {
		return nil, adbcFromFlightStatus(err, "ReadPartition(DoGet)")
	}
	return rdr, nil
}

var (
	_ adbc.PostInitOptions = (*cnxn)(nil)
)
