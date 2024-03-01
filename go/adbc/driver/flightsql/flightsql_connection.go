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
	"encoding/json"
	"fmt"
	"io"
	"math"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/driverbase"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql/schema_ref"
	flightproto "github.com/apache/arrow/go/v16/arrow/flight/gen/flight"
	"github.com/apache/arrow/go/v16/arrow/ipc"
	"github.com/bluele/gcache"
	"google.golang.org/grpc"
	grpccodes "google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

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
		cc          interface{}
		hasFallback bool
	)

	for _, loc := range endpoint.Location {
		if loc.Uri == flight.LocationReuseConnection {
			hasFallback = true
			continue
		}

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

	if hasFallback {
		return cl.DoGet(ctx, endpoint.Ticket, opts...)
	}

	return nil, err
}

func (c *connectionImpl) getSessionOptions(ctx context.Context) (map[string]interface{}, error) {
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	rawOptions, err := c.cl.GetSessionOptions(ctx, &flight.GetSessionOptionsRequest{}, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		// We're going to make a bit of a concession to backwards compatibility
		// here and ignore UNIMPLEMENTED or INVALID_ARGUMENT
		grpcStatus := grpcstatus.Convert(err)
		if grpcStatus.Code() == grpccodes.InvalidArgument || grpcStatus.Code() == grpccodes.Unimplemented {
			return map[string]interface{}{}, nil
		}
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetSessionOptions")
	}

	options := make(map[string]interface{}, len(rawOptions.SessionOptions))
	for k, rawValue := range rawOptions.SessionOptions {
		switch v := rawValue.OptionValue.(type) {
		case *flightproto.SessionOptionValue_BoolValue:
			options[k] = v.BoolValue
		case *flightproto.SessionOptionValue_DoubleValue:
			options[k] = v.DoubleValue
		case *flightproto.SessionOptionValue_Int64Value:
			options[k] = v.Int64Value
		case *flightproto.SessionOptionValue_StringValue:
			options[k] = v.StringValue
		case *flightproto.SessionOptionValue_StringListValue_:
			if v.StringListValue.Values == nil {
				options[k] = make([]string, 0)
			} else {
				options[k] = v.StringListValue.Values
			}
		case nil:
			options[k] = nil
		default:
			return nil, adbc.Error{
				Code: adbc.StatusNotImplemented,
				Msg:  fmt.Sprintf("[FlightSQL] Unknown session option type %#v", rawValue),
			}
		}
	}
	return options, nil
}

func (c *connectionImpl) setSessionOptions(ctx context.Context, key string, val interface{}) error {
	req := flight.SetSessionOptionsRequest{}

	var err error
	req.SessionOptions, err = flight.NewSessionOptionValues(map[string]any{key: val})
	if err != nil {
		return adbc.Error{
			Msg:  fmt.Sprintf("[Flight SQL] Invalid session option %s=%#v: %s", key, val, err.Error()),
			Code: adbc.StatusInvalidArgument,
		}
	}

	var header, trailer metadata.MD
	errors, err := c.cl.SetSessionOptions(ctx, &req, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		return adbcFromFlightStatusWithDetails(err, header, trailer, "GetSessionOptions")
	}
	if len(errors.Errors) > 0 {
		msg := strings.Builder{}
		fmt.Fprint(&msg, "[Flight SQL] Could not set option(s) ")

		first := true
		for k, v := range errors.Errors {
			if !first {
				fmt.Fprint(&msg, ", ")
			}
			first = false

			errmsg := "unknown error"
			switch v.Value {
			case flightproto.SetSessionOptionsResult_INVALID_NAME:
				errmsg = "invalid name"
			case flightproto.SetSessionOptionsResult_INVALID_VALUE:
				errmsg = "invalid value"
			case flightproto.SetSessionOptionsResult_ERROR:
				errmsg = "error setting option"
			}
			fmt.Fprintf(&msg, "'%s' (%s)", k, errmsg)
		}

		return adbc.Error{
			Msg:  msg.String(),
			Code: adbc.StatusInvalidArgument,
		}
	}
	return nil
}

func getSessionOption[T any](options map[string]interface{}, key string, defaultVal T, valueType string) (T, error) {
	rawValue, ok := options[key]
	if !ok {
		return defaultVal, adbc.Error{
			Msg:  fmt.Sprintf("[Flight SQL] unknown session option '%s'", key),
			Code: adbc.StatusNotFound,
		}
	}
	value, ok := rawValue.(T)
	if !ok {
		return defaultVal, adbc.Error{
			Msg:  fmt.Sprintf("[Flight SQL] session option %s=%#v is not %s value", key, rawValue, valueType),
			Code: adbc.StatusNotFound,
		}
	}
	return value, nil
}

func (c *connectionImpl) GetOption(key string) (string, error) {
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
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return "", err
		}
		if catalog, ok := options["catalog"]; ok {
			if val, ok := catalog.(string); ok {
				return val, nil
			}
			return "", adbc.Error{
				Msg:  fmt.Sprintf("[FlightSQL] Server returned non-string catalog %#v", catalog),
				Code: adbc.StatusInternal,
			}
		}
		return "", adbc.Error{
			Msg:  "[FlightSQL] current catalog not supported",
			Code: adbc.StatusNotFound,
		}

	case adbc.OptionKeyCurrentDbSchema:
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return "", err
		}
		if schema, ok := options["schema"]; ok {
			if val, ok := schema.(string); ok {
				return val, nil
			}
			return "", adbc.Error{
				Msg:  fmt.Sprintf("[FlightSQL] Server returned non-string schema %#v", schema),
				Code: adbc.StatusInternal,
			}
		}
		return "", adbc.Error{
			Msg:  "[FlightSQL] current schema not supported",
			Code: adbc.StatusNotFound,
		}
	case OptionSessionOptions:
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return "", err
		}
		encoded, err := json.Marshal(options)
		if err != nil {
			return "", adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] Could not encode option values: %s", err.Error()),
				Code: adbc.StatusInternal,
			}
		}
		return string(encoded), nil
	}
	switch {
	case strings.HasPrefix(key, OptionSessionOptionPrefix):
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return "", err
		}
		name := key[len(OptionSessionOptionPrefix):]
		return getSessionOption(options, name, "", "a string")
	case strings.HasPrefix(key, OptionBoolSessionOptionPrefix):
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return "", err
		}
		name := key[len(OptionBoolSessionOptionPrefix):]
		v, err := getSessionOption(options, name, false, "a boolean")
		if err != nil {
			return "", err
		}
		if v {
			return adbc.OptionValueEnabled, nil
		}
		return adbc.OptionValueDisabled, nil
	case strings.HasPrefix(key, OptionStringListSessionOptionPrefix):
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return "", err
		}
		name := key[len(OptionStringListSessionOptionPrefix):]
		v, err := getSessionOption[[]string](options, name, nil, "a string list")
		if err != nil {
			return "", err
		}
		encoded, err := json.Marshal(v)
		if err != nil {
			return "", adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] Could not encode option value: %s", err.Error()),
				Code: adbc.StatusInternal,
			}
		}
		return string(encoded), nil
	}

	return "", adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *connectionImpl) GetOptionBytes(key string) ([]byte, error) {
	switch key {
	case OptionSessionOptions:
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return nil, err
		}
		encoded, err := json.Marshal(options)
		if err != nil {
			return nil, adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] Could not encode option values: %s", err.Error()),
				Code: adbc.StatusInternal,
			}
		}
		return encoded, nil
	}

	return nil, adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotFound,
	}
}

func (c *connectionImpl) GetOptionInt(key string) (int64, error) {
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
	if strings.HasPrefix(key, OptionSessionOptionPrefix) {
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return 0, err
		}
		name := key[len(OptionSessionOptionPrefix):]
		return getSessionOption(options, name, int64(0), "an integer")
	}

	return c.ConnectionImplBase.GetOptionInt(key)
}

func (c *connectionImpl) GetOptionDouble(key string) (float64, error) {
	switch key {
	case OptionTimeoutFetch:
		return c.timeouts.fetchTimeout.Seconds(), nil
	case OptionTimeoutQuery:
		return c.timeouts.queryTimeout.Seconds(), nil
	case OptionTimeoutUpdate:
		return c.timeouts.updateTimeout.Seconds(), nil
	}
	if strings.HasPrefix(key, OptionSessionOptionPrefix) {
		options, err := c.getSessionOptions(context.Background())
		if err != nil {
			return 0, err
		}
		name := key[len(OptionSessionOptionPrefix):]
		return getSessionOption(options, name, float64(0.0), "a floating-point")
	}

	return c.ConnectionImplBase.GetOptionDouble(key)
}

func (c *connectionImpl) SetOption(key, value string) error {
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
	case adbc.OptionKeyCurrentCatalog:
		return c.setSessionOptions(context.Background(), "catalog", value)
	case adbc.OptionKeyCurrentDbSchema:
		return c.setSessionOptions(context.Background(), "schema", value)
	}

	switch {
	case strings.HasPrefix(key, OptionSessionOptionPrefix):
		name := key[len(OptionSessionOptionPrefix):]
		return c.setSessionOptions(context.Background(), name, value)
	case strings.HasPrefix(key, OptionBoolSessionOptionPrefix):
		name := key[len(OptionBoolSessionOptionPrefix):]
		switch value {
		case adbc.OptionValueEnabled:
			return c.setSessionOptions(context.Background(), name, true)
		case adbc.OptionValueDisabled:
			return c.setSessionOptions(context.Background(), name, false)
		default:
			return adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] invalid boolean session option value %s=%s", name, value),
				Code: adbc.StatusNotImplemented,
			}
		}
	case strings.HasPrefix(key, OptionStringListSessionOptionPrefix):
		name := key[len(OptionStringListSessionOptionPrefix):]
		stringlist := make([]string, 0)
		if err := json.Unmarshal([]byte(value), &stringlist); err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("[Flight SQL] invalid string list session option value %s=%s: %s", name, value, err.Error()),
				Code: adbc.StatusNotImplemented,
			}
		}
		return c.setSessionOptions(context.Background(), name, stringlist)
	case strings.HasPrefix(key, OptionEraseSessionOptionPrefix):
		name := key[len(OptionEraseSessionOptionPrefix):]
		return c.setSessionOptions(context.Background(), name, nil)
	}

	return c.ConnectionImplBase.SetOption(key, value)
}

func (c *connectionImpl) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionTimeoutFetch, OptionTimeoutQuery, OptionTimeoutUpdate:
		return c.timeouts.setTimeout(key, float64(value))
	}
	if strings.HasPrefix(key, OptionSessionOptionPrefix) {
		name := key[len(OptionSessionOptionPrefix):]
		return c.setSessionOptions(context.Background(), name, value)
	}

	return c.ConnectionImplBase.SetOptionInt(key, value)
}

func (c *connectionImpl) SetOptionDouble(key string, value float64) error {
	switch key {
	case OptionTimeoutFetch:
		fallthrough
	case OptionTimeoutQuery:
		fallthrough
	case OptionTimeoutUpdate:
		return c.timeouts.setTimeout(key, value)
	}
	if strings.HasPrefix(key, OptionSessionOptionPrefix) {
		name := key[len(OptionSessionOptionPrefix):]
		return c.setSessionOptions(context.Background(), name, value)
	}

	return c.ConnectionImplBase.SetOptionDouble(key, value)
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
func (c *connectionImpl) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	driverInfo := c.ConnectionImplBase.DriverInfo

	if len(infoCodes) == 0 {
		infoCodes = driverInfo.InfoSupportedCodes()
	}

	translated := make([]flightsql.SqlInfo, 0, len(infoCodes))
	for _, code := range infoCodes {
		if t, ok := adbcToFlightSQLInfo[code]; ok {
			translated = append(translated, t)
		}
	}

	// None of the requested info codes are available on the server, so just return the local info
	if len(translated) == 0 {
		return c.ConnectionImplBase.GetInfo(ctx, infoCodes)
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	info, err := c.cl.GetSqlInfo(ctx, translated, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)

	// Just return local driver info if GetSqlInfo hasn't been implemented on the server
	if grpcstatus.Code(err) == grpccodes.Unimplemented {
		return c.ConnectionImplBase.GetInfo(ctx, infoCodes)
	}

	if err != nil {
		return nil, adbcFromFlightStatus(err, "GetInfo(GetSqlInfo)")
	}

	// No error, go get the SqlInfo from the server
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

			var adbcInfoCode adbc.InfoCode
			for i := 0; i < int(rec.NumRows()); i++ {
				switch flightsql.SqlInfo(field.Value(i)) {
				case flightsql.SqlInfoFlightSqlServerName:
					adbcInfoCode = adbc.InfoVendorName
				case flightsql.SqlInfoFlightSqlServerVersion:
					adbcInfoCode = adbc.InfoVendorVersion
				case flightsql.SqlInfoFlightSqlServerArrowVersion:
					adbcInfoCode = adbc.InfoVendorArrowVersion
				default:
					continue
				}

				// we know we're only doing string fields here right now
				v := info.Field(info.ChildID(i)).(*array.String).
					Value(int(info.ValueOffset(i)))
				driverInfo.RegisterInfoCode(adbcInfoCode, v)
			}
		}

		if err := checkContext(rdr.Err(), ctx); err != nil {
			return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetInfo(DoGet): endpoint %d: %s", i, endpoint.Location)
		}
	}

	return c.ConnectionImplBase.GetInfo(ctx, infoCodes)
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
func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
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
func (c *connectionImpl) readInfo(ctx context.Context, expectedSchema *arrow.Schema, info *flight.FlightInfo, opts ...grpc.CallOption) (array.RecordReader, error) {
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
func (c *connectionImpl) getObjectsDbSchemas(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, metadataRecords []internal.Metadata) (result map[string][]string, err error) {
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

func (c *connectionImpl) getObjectsTables(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string, metadataRecords []internal.Metadata) (result internal.SchemaToTableInfo, err error) {
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

func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
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
func (c *connectionImpl) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
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
func (c *connectionImpl) Commit(ctx context.Context) error {
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
func (c *connectionImpl) Rollback(ctx context.Context) error {
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
func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statementImpl{
		StatementImplBase: driverbase.NewStatementImplBase(&c.ConnectionImplBase),
		alloc:             c.db.Alloc,
		clientCache:       c.clientCache,
		hdrs:              c.hdrs.Copy(),
		queueSize:         5,
		timeouts:          c.timeouts,
		cnxn:              c,
	}, nil
}

func (c *connectionImpl) execute(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	if c.txn != nil {
		return c.txn.Execute(ctx, query, opts...)
	}

	return c.cl.Execute(ctx, query, opts...)
}

func (c *connectionImpl) executeSchema(ctx context.Context, query string, opts ...grpc.CallOption) (*flight.SchemaResult, error) {
	if c.txn != nil {
		return c.txn.GetExecuteSchema(ctx, query, opts...)
	}

	return c.cl.GetExecuteSchema(ctx, query, opts...)
}

func (c *connectionImpl) executeSubstrait(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (*flight.FlightInfo, error) {
	if c.txn != nil {
		return c.txn.ExecuteSubstrait(ctx, plan, opts...)
	}

	return c.cl.ExecuteSubstrait(ctx, plan, opts...)
}

func (c *connectionImpl) executeSubstraitSchema(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (*flight.SchemaResult, error) {
	if c.txn != nil {
		return c.txn.GetExecuteSubstraitSchema(ctx, plan, opts...)
	}

	return c.cl.GetExecuteSubstraitSchema(ctx, plan, opts...)
}

func (c *connectionImpl) executeUpdate(ctx context.Context, query string, opts ...grpc.CallOption) (n int64, err error) {
	if c.txn != nil {
		return c.txn.ExecuteUpdate(ctx, query, opts...)
	}

	return c.cl.ExecuteUpdate(ctx, query, opts...)
}

func (c *connectionImpl) executeSubstraitUpdate(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (n int64, err error) {
	if c.txn != nil {
		return c.txn.ExecuteSubstraitUpdate(ctx, plan, opts...)
	}

	return c.cl.ExecuteSubstraitUpdate(ctx, plan, opts...)
}

func (c *connectionImpl) poll(ctx context.Context, query string, retryDescriptor *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error) {
	if c.txn != nil {
		return c.txn.ExecutePoll(ctx, query, retryDescriptor, opts...)
	}

	return c.cl.ExecutePoll(ctx, query, retryDescriptor, opts...)
}

func (c *connectionImpl) pollSubstrait(ctx context.Context, plan flightsql.SubstraitPlan, retryDescriptor *flight.FlightDescriptor, opts ...grpc.CallOption) (*flight.PollInfo, error) {
	if c.txn != nil {
		return c.txn.ExecuteSubstraitPoll(ctx, plan, retryDescriptor, opts...)
	}

	return c.cl.ExecuteSubstraitPoll(ctx, plan, retryDescriptor, opts...)
}

func (c *connectionImpl) prepare(ctx context.Context, query string, opts ...grpc.CallOption) (*flightsql.PreparedStatement, error) {
	if c.txn != nil {
		return c.txn.Prepare(ctx, query, opts...)
	}

	return c.cl.Prepare(ctx, query, opts...)
}

func (c *connectionImpl) prepareSubstrait(ctx context.Context, plan flightsql.SubstraitPlan, opts ...grpc.CallOption) (*flightsql.PreparedStatement, error) {
	if c.txn != nil {
		return c.txn.PrepareSubstrait(ctx, plan, opts...)
	}

	return c.cl.PrepareSubstrait(ctx, plan, opts...)
}

// Close closes this connection and releases any associated resources.
func (c *connectionImpl) Close() error {
	if c.cl == nil {
		return adbc.Error{
			Msg:  "[Flight SQL Connection] trying to close already closed connection",
			Code: adbc.StatusInvalidState,
		}
	}

	ctx := metadata.NewOutgoingContext(context.Background(), c.hdrs)
	var header, trailer metadata.MD
	_, err := c.cl.CloseSession(ctx, &flight.CloseSessionRequest{}, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)
	if err != nil {
		grpcStatus := grpcstatus.Convert(err)
		// Ignore unimplemented
		if grpcStatus.Code() != grpccodes.Unimplemented {
			// Ignore the error since server may not support it and may not properly return UNIMPLEMENTED
			// TODO(https://github.com/apache/arrow-adbc/issues/1243): log a proper warning
			c.db.Logger.Debug("failed to close session", "error", err.Error())
		}
	}

	err = c.cl.Close()
	c.cl = nil
	return adbcFromFlightStatus(err, "Close")
}

// ReadPartition constructs a statement for a partition of a query. The
// results can then be read independently using the returned RecordReader.
//
// A partition can be retrieved by using ExecutePartitions on a statement.
func (c *connectionImpl) ReadPartition(ctx context.Context, serializedPartition []byte) (rdr array.RecordReader, err error) {
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
	_ adbc.PostInitOptions = (*connectionImpl)(nil)
)
