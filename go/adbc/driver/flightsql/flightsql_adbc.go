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

// Package flightsql is an ADBC Driver Implementation for Flight SQL
// natively in go.
//
// It can be used to register a driver for database/sql by importing
// github.com/apache/arrow-adbc/go/adbc/sqldriver and running:
//
//     sql.Register("flightsql", sqldriver.Driver{flightsql.Driver{}})
//
// You can then open a flightsql connection with the database/sql
// standard package by using:
//
//     db, err := sql.Open("flightsql", "uri=<flight sql db url>")
//
// The URI passed *must* contain a scheme, most likely "grpc+tcp://"
package flightsql

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"net/url"
	"runtime/debug"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow/go/v11/arrow"
	"github.com/apache/arrow/go/v11/arrow/array"
	"github.com/apache/arrow/go/v11/arrow/flight"
	"github.com/apache/arrow/go/v11/arrow/flight/flightsql"
	"github.com/apache/arrow/go/v11/arrow/memory"
	"github.com/bluele/gcache"
	"golang.org/x/exp/maps"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"
)

const (
	OptionMTLSCertChain       = "adbc.flight.sql.client_option.mtls_cert_chain"
	OptionMTLSPrivateKey      = "adbc.flight.sql.client_option.mtls_private_key"
	OptionSSLOverrideHostname = "adbc.flight.sql.client_option.tls_override_hostname"
	OptionSSLSkipVerify       = "adbc.flight.sql.client_option.tls_skip_verify"
	OptionSSLRootCerts        = "adbc.flight.sql.client_option.tls_root_certs"

	infoDriverName = "ADBC Flight SQL Driver - Go"
)

var (
	infoDriverVersion      string
	infoDriverArrowVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case dep.Path == "github.com/apache/arrow-adbc/go/adbc/driver/flightsql":
				infoDriverVersion = dep.Version
			case strings.HasPrefix(dep.Path, "github.com/apache/arrow/go/"):
				infoDriverArrowVersion = dep.Version
			}
		}
	}
}

type Driver struct {
	Alloc memory.Allocator
}

func (d Driver) NewDatabase(opts map[string]string) (adbc.Database, error) {
	opts = maps.Clone(opts)
	uri, ok := opts[adbc.OptionKeyURI]
	if !ok {
		return nil, adbc.Error{
			Msg:  "URI required for a FlightSQL DB",
			Code: adbc.StatusInvalidArgument,
		}
	}
	delete(opts, adbc.OptionKeyURI)

	db := &database{alloc: d.Alloc}
	if db.alloc == nil {
		db.alloc = memory.DefaultAllocator
	}

	var err error
	if db.uri, err = url.Parse(uri); err != nil {
		return nil, adbc.Error{Msg: err.Error(), Code: adbc.StatusInvalidArgument}
	}

	return db, db.SetOptions(opts)
}

type database struct {
	uri        *url.URL
	creds      credentials.TransportCredentials
	user, pass string

	alloc memory.Allocator
}

func (d *database) SetOptions(cnOptions map[string]string) error {
	var tlsConfig tls.Config

	mtlsCert := cnOptions[OptionMTLSCertChain]
	mtlsKey := cnOptions[OptionMTLSPrivateKey]
	if mtlsCert != "" && mtlsKey != "" {
		cert, err := tls.X509KeyPair([]byte(mtlsCert), []byte(mtlsKey))
		if err != nil {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid mTLS certificate: %#v", err),
				Code: adbc.StatusInvalidArgument,
			}
		}
		tlsConfig.Certificates = []tls.Certificate{cert}
		delete(cnOptions, OptionMTLSCertChain)
		delete(cnOptions, OptionMTLSPrivateKey)
	} else if mtlsCert != "" {
		return adbc.Error{
			Msg:  fmt.Sprintf("Must provide both '%s' and '%s', only provided '%s'", OptionMTLSCertChain, OptionMTLSPrivateKey, OptionMTLSCertChain),
			Code: adbc.StatusInvalidArgument,
		}
	} else if mtlsKey != "" {
		return adbc.Error{
			Msg:  fmt.Sprintf("Must provide both '%s' and '%s', only provided '%s'", OptionMTLSCertChain, OptionMTLSPrivateKey, OptionMTLSPrivateKey),
			Code: adbc.StatusInvalidArgument,
		}
	}

	if hostname, ok := cnOptions[OptionSSLOverrideHostname]; ok {
		tlsConfig.ServerName = hostname
		delete(cnOptions, OptionSSLOverrideHostname)
	}

	if val, ok := cnOptions[OptionSSLSkipVerify]; ok {
		if val == adbc.OptionValueEnabled {
			tlsConfig.InsecureSkipVerify = true
		} else if val == adbc.OptionValueDisabled {
			tlsConfig.InsecureSkipVerify = false
		} else {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': '%s'", OptionSSLSkipVerify, val),
				Code: adbc.StatusInvalidArgument,
			}
		}
		delete(cnOptions, OptionSSLSkipVerify)
	}

	if cert, ok := cnOptions[OptionSSLRootCerts]; ok {
		cp := x509.NewCertPool()
		if !cp.AppendCertsFromPEM([]byte(cert)) {
			return adbc.Error{
				Msg:  fmt.Sprintf("Invalid value for database option '%s': failed to append certificates", OptionSSLRootCerts),
				Code: adbc.StatusInvalidArgument,
			}
		}
		tlsConfig.RootCAs = cp
		delete(cnOptions, OptionSSLRootCerts)
	}

	d.creds = credentials.NewTLS(&tlsConfig)

	if u, ok := cnOptions[adbc.OptionKeyUsername]; ok {
		d.user = u
		delete(cnOptions, adbc.OptionKeyUsername)
	}

	if p, ok := cnOptions[adbc.OptionKeyPassword]; ok {
		d.pass = p
		delete(cnOptions, adbc.OptionKeyPassword)
	}

	for key := range cnOptions {
		return adbc.Error{
			Msg:  fmt.Sprintf("Unknown database option '%s'", key),
			Code: adbc.StatusInvalidArgument,
		}
	}

	return nil
}

type bearerAuthMiddleware struct {
	token string
}

func (b *bearerAuthMiddleware) StartCall(ctx context.Context) context.Context {
	if b.token != "" {
		return metadata.AppendToOutgoingContext(ctx, "authorization", b.token)
	}

	return ctx
}

func getFlightClient(ctx context.Context, loc string, d *database) (*flightsql.Client, error) {
	authMiddle := &bearerAuthMiddleware{}

	uri, err := url.Parse(loc)
	if err != nil {
		return nil, adbc.Error{Msg: fmt.Sprintf("Invalid URI '%s': %s", loc, err), Code: adbc.StatusInvalidArgument}
	}
	creds := d.creds
	if uri.Scheme == "grpc" || uri.Scheme == "grpc+tcp" {
		creds = insecure.NewCredentials()
	}

	cl, err := flightsql.NewClient(uri.Host, nil, []flight.ClientMiddleware{
		flight.CreateClientMiddleware(authMiddle)}, grpc.WithTransportCredentials(creds))
	if err != nil {
		return nil, adbc.Error{
			Msg:  err.Error(),
			Code: adbc.StatusIO,
		}
	}

	cl.Alloc = d.alloc
	if d.user != "" {
		ctx, err = cl.Client.AuthenticateBasicToken(ctx, d.user, d.pass)
		if err != nil {
			return nil, adbc.Error{
				Msg:  err.Error(),
				Code: adbc.StatusUnauthenticated,
			}
		}

		if md, ok := metadata.FromOutgoingContext(ctx); ok {
			authMiddle.token = md.Get("Authorization")[0]
		}
	}

	return cl, nil
}

func (d *database) Open(ctx context.Context) (adbc.Connection, error) {
	cl, err := getFlightClient(ctx, d.uri.String(), d)
	if err != nil {
		return nil, err
	}

	cache := gcache.New(20).LRU().
		Expiration(5 * time.Minute).
		LoaderFunc(func(loc interface{}) (interface{}, error) {
			uri, ok := loc.(string)
			if !ok {
				return nil, adbc.Error{Code: adbc.StatusInternal}
			}

			cl, err := getFlightClient(context.Background(), uri, d)
			if err != nil {
				return nil, err
			}

			cl.Alloc = d.alloc
			return cl, nil
		}).
		EvictedFunc(func(_, client interface{}) {
			conn := client.(*flightsql.Client)
			conn.Close()
		}).Build()
	return &cnxn{cl: cl, db: d, clientCache: cache}, nil
}

type cnxn struct {
	cl *flightsql.Client

	db          *database
	clientCache gcache.Cache
}

var adbcToFlightSQLInfo = map[adbc.InfoCode]flightsql.SqlInfo{
	adbc.InfoVendorName:         flightsql.SqlInfoFlightSqlServerName,
	adbc.InfoVendorVersion:      flightsql.SqlInfoFlightSqlServerVersion,
	adbc.InfoVendorArrowVersion: flightsql.SqlInfoFlightSqlServerArrowVersion,
}

func doGet(ctx context.Context, cl *flightsql.Client, endpoint *flight.FlightEndpoint, clientCache gcache.Cache) (rdr *flight.Reader, err error) {
	if len(endpoint.Location) == 0 {
		return cl.DoGet(ctx, endpoint.Ticket)
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
		rdr, err = conn.DoGet(ctx, endpoint.Ticket)
		if err != nil {
			continue
		}

		return
	}

	return nil, err
}

func (c *cnxn) SetOption(key, value string) error {
	switch key {
	case adbc.OptionKeyAutoCommit:
		return adbc.Error{
			Msg:  "[Flight SQL] transactions not yet supported",
			Code: adbc.StatusNotImplemented,
		}
	default:
		return adbc.Error{
			Msg:  "[Flight SQL] unknown connection option",
			Code: adbc.StatusNotImplemented,
		}
	}
}

// GetInfo returns metadata about the database/driver.
//
// The result is an Arrow dataset with the following schema:
//
//    Field Name									| Field Type
//    ----------------------------|-----------------------------
//    info_name					   				| uint32 not null
//    info_value									| INFO_SCHEMA
//
// INFO_SCHEMA is a dense union with members:
//
// 		Field Name (Type Code)			| Field Type
//		----------------------------|-----------------------------
//		string_value (0)						| utf8
//		bool_value (1)							| bool
//		int64_value (2)							| int64
//		int32_bitmask (3)						| int32
//		string_list (4)							| list<utf8>
//		int32_to_int32_list_map (5)	| map<int32, list<int32>>
//
// Each metadatum is identified by an integer code. The recognized
// codes are defined as constants. Codes [0, 10_000) are reserved
// for ADBC usage. Drivers/vendors will ignore requests for unrecognized
// codes (the row will be omitted from the result).
func (c *cnxn) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	const strValTypeID arrow.UnionTypeCode = 0

	if len(infoCodes) == 0 {
		infoCodes = maps.Keys(adbcToFlightSQLInfo)
	}

	bldr := array.NewRecordBuilder(c.cl.Alloc, adbc.GetInfoSchema)
	defer bldr.Release()
	bldr.Reserve(len(infoCodes))

	infoNameBldr := bldr.Field(0).(*array.Uint32Builder)
	infoValueBldr := bldr.Field(1).(*array.DenseUnionBuilder)
	strInfoBldr := infoValueBldr.Child(0).(*array.StringBuilder)

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
		}
	}

	info, err := c.cl.GetSqlInfo(ctx, translated)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}

	for _, endpoint := range info.Endpoint {
		rdr, err := doGet(ctx, c.cl, endpoint, c.clientCache)
		if err != nil {
			return nil, adbcFromFlightStatus(err)
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
				}

				infoValueBldr.Append(info.TypeCode(i))
				// we know we're only doing string fields here right now
				v := info.Field(info.ChildID(i)).(*array.String).
					Value(int(info.ValueOffset(i)))
				strInfoBldr.Append(v)
			}
		}

		if rdr.Err() != nil {
			return nil, adbcFromFlightStatus(rdr.Err())
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
//		Field Name									| Field Type
//		----------------------------|----------------------------
//		catalog_name								| utf8
//		catalog_db_schemas					| list<DB_SCHEMA_SCHEMA>
//
// DB_SCHEMA_SCHEMA is a Struct with the fields:
//
//		Field Name									| Field Type
//		----------------------------|----------------------------
//		db_schema_name							| utf8
//		db_schema_tables						|	list<TABLE_SCHEMA>
//
// TABLE_SCHEMA is a Struct with the fields:
//
//		Field Name									| Field Type
//		----------------------------|----------------------------
//		table_name									| utf8 not null
//		table_type									|	utf8 not null
//		table_columns								| list<COLUMN_SCHEMA>
//		table_constraints						| list<CONSTRAINT_SCHEMA>
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
// 1. The column's ordinal position in the table (starting from 1).
// 2. Database-specific description of the column.
// 3. Optional Value. Should be null if not supported by the driver.
//	  xdbc_values are meant to provide JDBC/ODBC-compatible metadata
//		in an agnostic manner.
//
// CONSTRAINT_SCHEMA is a Struct with the fields:
//
//		Field Name									| Field Type					| Comments
//		----------------------------|---------------------|---------
//		constraint_name							| utf8								|
//		constraint_type							| utf8 not null				| (1)
//		constraint_column_names			| list<utf8> not null | (2)
//		constraint_column_usage			| list<USAGE_SCHEMA>	| (3)
//
// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
// 2. The columns on the current table that are constrained, in order.
// 3. For FOREIGN KEY only, the referenced table and columns.
//
// USAGE_SCHEMA is a Struct with fields:
//
//		Field Name									|	Field Type
//		----------------------------|----------------------------
//		fk_catalog									| utf8
//		fk_db_schema								| utf8
//		fk_table										| utf8 not null
//		fk_column_name							| utf8 not null
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
	return nil, adbc.Error{
		Msg:  "Not implemented: GetObjects",
		Code: adbc.StatusNotImplemented,
	}
}

func (c *cnxn) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	opts := &flightsql.GetTablesOpts{
		Catalog:                catalog,
		DbSchemaFilterPattern:  dbSchema,
		TableNameFilterPattern: &tableName,
		IncludeSchema:          true,
	}

	info, err := c.cl.GetTables(ctx, opts)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}

	rdr, err := doGet(ctx, c.cl, info.Endpoint[0], c.clientCache)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
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
		return nil, adbcFromFlightStatus(err)
	}

	if rec.NumRows() == 0 {
		return nil, adbc.Error{
			Code: adbc.StatusNotFound,
		}
	}

	// returned schema should be
	//    0: catalog_name: utf8
	//    1: db_schema_name: utf8
	//    2: table_name: utf8 not null
	//    3: table_type: utf8 not null
	//    4: table_schema: bytes not null
	schemaBytes := rec.Column(4).(*array.Binary).Value(0)
	s, err := flight.DeserializeSchema(schemaBytes, c.db.alloc)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}
	return s, nil
}

// GetTableTypes returns a list of the table types in the database.
//
// The result is an arrow dataset with the following schema:
//
//		Field Name			| Field Type
//		----------------|--------------
//		table_type			| utf8 not null
//
func (c *cnxn) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	info, err := c.cl.GetTableTypes(ctx)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}

	return newRecordReader(ctx, c.db.alloc, c.cl, info, c.clientCache)
}

// Commit commits any pending transactions on this connection, it should
// only be used if autocommit is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
// When not supported, the convention is that it should act as if autocommit
// is enabled and return INVALID_STATE errors.
func (c *cnxn) Commit(_ context.Context) error {
	return adbc.Error{
		Msg:  "[Flight SQL] Transaction methods are not implemented yet",
		Code: adbc.StatusInvalidState}
}

// Rollback rolls back any pending transactions. Only used if autocommit
// is disabled.
//
// Behavior is undefined if this is mixed with SQL transaction statements.
// When not supported, the convention is that it should act as if autocommit
// is enabled and return INVALID_STATE errors.
func (c *cnxn) Rollback(_ context.Context) error {
	return adbc.Error{
		Msg:  "[Flight SQL] Transaction methods are not implemented yet",
		Code: adbc.StatusInvalidState}
}

// NewStatement initializes a new statement object tied to this connection
func (c *cnxn) NewStatement() (adbc.Statement, error) {
	return &statement{
		alloc:       c.db.alloc,
		cl:          c.cl,
		clientCache: c.clientCache,
	}, nil
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
	return err
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

	rdr, err = doGet(ctx, c.cl, info.Endpoint[0], c.clientCache)
	if err != nil {
		return nil, adbcFromFlightStatus(err)
	}
	return rdr, nil
}

var (
	_ adbc.PostInitOptions = (*cnxn)(nil)
)
