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
	"github.com/apache/arrow-adbc/go/adbc/driver/driverbase"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/flight"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v16/arrow/flight/flightsql/schema_ref"
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

	cl          *flightsql.Client
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

func (c *connectionImpl) SetAutocommit(enabled bool) error {
	if enabled && c.txn == nil {
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

	if enabled {
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
	}

	return c.ConnectionImplBase.GetOption(key)
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
	}
	return c.ConnectionImplBase.SetOption(key, value)
}

func (c *connectionImpl) SetOptionBytes(key string, value []byte) error {
	return adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotImplemented,
	}
}

func (c *connectionImpl) SetOptionInt(key string, value int64) error {
	switch key {
	case OptionTimeoutFetch, OptionTimeoutQuery, OptionTimeoutUpdate:
		return c.timeouts.setTimeout(key, float64(value))
	}

	return adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotImplemented,
	}
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

	return adbc.Error{
		Msg:  "[Flight SQL] unknown connection option",
		Code: adbc.StatusNotImplemented,
	}
}

func (c *connectionImpl) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (map[adbc.InfoCode]interface{}, error) {
	infoValues := make(map[adbc.InfoCode]interface{})

	translated := make([]flightsql.SqlInfo, 0, len(infoCodes))
	for _, code := range infoCodes {
		if t, ok := adbcToFlightSQLInfo[code]; ok {
			translated = append(translated, t)
			continue
		}
		switch code {
		case adbc.InfoDriverName:
			infoValues[adbc.InfoDriverName] = infoDriverName
		case adbc.InfoDriverVersion:
			infoValues[adbc.InfoDriverVersion] = infoDriverVersion
		}
	}

	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	info, err := c.cl.GetSqlInfo(ctx, translated, grpc.Header(&header), grpc.Trailer(&trailer), c.timeouts)

	if err != nil {
		if grpcstatus.Code(err) == grpccodes.Unimplemented {
			// Server doesn't support this
			return infoValues, nil
		}
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetInfo(GetSqlInfo)")
	}

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
					infoValues[adbc.InfoVendorName] = info.Field(info.ChildID(i)).(*array.String).
						Value(int(info.ValueOffset(i)))
				case flightsql.SqlInfoFlightSqlServerVersion:
					infoValues[adbc.InfoVendorVersion] = info.Field(info.ChildID(i)).(*array.String).
						Value(int(info.ValueOffset(i)))
				case flightsql.SqlInfoFlightSqlServerArrowVersion:
					infoValues[adbc.InfoVendorArrowVersion] = info.Field(info.ChildID(i)).(*array.String).
						Value(int(info.ValueOffset(i)))
				}
			}
		}

		if err := checkContext(rdr.Err(), ctx); err != nil {
			return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetInfo(DoGet): endpoint %d: %s", i, endpoint.Location)
		}
	}

	return infoValues, nil
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

func (c *connectionImpl) GetObjectsCatalogs(ctx context.Context, catalog *string) ([]string, error) {
	var header, trailer metadata.MD
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

	catalogs := make([]string, 0)

	for rdr.Next() {
		arr := rdr.Record().Column(0).(*array.String)
		for i := 0; i < arr.Len(); i++ {
			// XXX: force copy since accessor is unsafe
			catalogName := string([]byte(arr.Value(i)))
			catalogs = append(catalogs, catalogName)
		}
	}

	if err := checkContext(rdr.Err(), ctx); err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetObjects(GetCatalogs)")
	}
	return catalogs, nil
}

// Helper function to build up a map of catalogs to DB schemas
func (c *connectionImpl) GetObjectsDbSchemas(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, metadataRecords []internal.Metadata) (result map[string][]string, err error) {
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

func (c *connectionImpl) GetObjectsTables(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string, metadataRecords []internal.Metadata) (result internal.SchemaToTableInfo, err error) {
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

func (c *connectionImpl) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	ctx = metadata.NewOutgoingContext(ctx, c.hdrs)
	var header, trailer metadata.MD
	info, err := c.cl.GetTableTypes(ctx, c.timeouts, grpc.Header(&header), grpc.Trailer(&trailer))
	if err != nil {
		return nil, adbcFromFlightStatusWithDetails(err, header, trailer, "GetTableTypes")
	}

	return newRecordReader(ctx, c.db.Alloc, c.cl, info, c.clientCache, 5)
}

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

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statement{
		alloc:       c.db.Alloc,
		clientCache: c.clientCache,
		hdrs:        c.hdrs.Copy(),
		queueSize:   5,
		timeouts:    c.timeouts,
		cnxn:        c,
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

	err := c.cl.Close()
	c.cl = nil
	return adbcFromFlightStatus(err, "Close")
}

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
