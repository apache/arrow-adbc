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

package driverbase

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

const (
	ConnectionMessageOptionUnknown     = "Unknown connection option"
	ConnectionMessageOptionUnsupported = "Unsupported connection option"
	ConnectionMessageCannotCommit      = "Cannot commit when autocommit is enabled"
	ConnectionMessageCannotRollback    = "Cannot rollback when autocommit is enabled"
)

// ConnectionImpl is an interface that drivers implement to provide
// vendor-specific functionality.
type ConnectionImpl interface {
	adbc.Connection
	adbc.GetSetOptions
	Base() *ConnectionImplBase

	// // Will be called at most once
	// Close() error
	// // Will not be called unless autocommit is disabled
	// Commit(context.Context) error
	// GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error)
	// NewStatement() (adbc.Statement, error)
	// ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error)
	// // Will not be called unless autocommit is disabled
	// Rollback(context.Context) error
	// SetAutocommit(enabled bool) error
}

type CurrentNamespacer interface {
	CurrentCatalog() (string, bool)
	CurrentDbSchema() (string, bool)
	SetCurrentCatalog(string) error
	SetCurrentDbSchema(string) error
}

type DriverInfoPreparer interface {
	PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error
}

type TableTypeLister interface {
	ListTableTypes(ctx context.Context) ([]string, error)
}

type AutocommitSetter interface {
	SetAutocommit(enabled bool) error
}

// Connection is the interface satisfied by the result of the NewConnection constructor,
// given an input is provided satisfying the ConnectionImpl interface.
type Connection interface {
	adbc.Connection
	adbc.GetSetOptions
}

// ConnectionImplBase is a struct that provides default implementations of some of the
// methods defined in the ConnectionImpl interface. It is meant to be used as a composite
// struct for a driver's ConnectionImpl implementation.
//
// It is up to the driver implementor to understand the semantics of the default
// behavior provided. For example, in some cases the default implementation may provide
// a fallback value while in other cases it may provide a partial-result which must be
// merged with the driver-specific-result, if any.
type ConnectionImplBase struct {
	Alloc       memory.Allocator
	ErrorHelper ErrorHelper
	DriverInfo  *DriverInfo

	Autocommit bool
	Closed     bool
}

// NewConnectionImplBase instantiates ConnectionImplBase.
//
//   - database is a DatabaseImplBase containing the common resources from the parent
//     database, allowing the Arrow allocator, error handler, and logger to be reused.
func NewConnectionImplBase(database *DatabaseImplBase) ConnectionImplBase {
	return ConnectionImplBase{
		Alloc:       database.Alloc,
		ErrorHelper: database.ErrorHelper,
		DriverInfo:  database.DriverInfo,
		Autocommit:  true,
		Closed:      false,
	}
}

func (base *ConnectionImplBase) Base() *ConnectionImplBase {
	return base
}

func (base *ConnectionImplBase) Commit(ctx context.Context) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Commit")
}

func (base *ConnectionImplBase) Rollback(context.Context) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Rollback")
}

func (base *ConnectionImplBase) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {

	if len(infoCodes) == 0 {
		infoCodes = base.DriverInfo.InfoSupportedCodes()
	}

	bldr := array.NewRecordBuilder(base.Alloc, adbc.GetInfoSchema)
	defer bldr.Release()
	bldr.Reserve(len(infoCodes))

	infoNameBldr := bldr.Field(0).(*array.Uint32Builder)
	infoValueBldr := bldr.Field(1).(*array.DenseUnionBuilder)
	strInfoBldr := infoValueBldr.Child(int(adbc.InfoValueStringType)).(*array.StringBuilder)
	intInfoBldr := infoValueBldr.Child(int(adbc.InfoValueInt64Type)).(*array.Int64Builder)

	for _, code := range infoCodes {
		switch code {
		case adbc.InfoDriverName:
			name, ok := base.DriverInfo.GetInfoDriverName()
			if !ok {
				continue
			}

			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(adbc.InfoValueStringType)
			strInfoBldr.Append(name)
		case adbc.InfoDriverVersion:
			version, ok := base.DriverInfo.GetInfoDriverVersion()
			if !ok {
				continue
			}

			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(adbc.InfoValueStringType)
			strInfoBldr.Append(version)
		case adbc.InfoDriverArrowVersion:
			arrowVersion, ok := base.DriverInfo.GetInfoDriverArrowVersion()
			if !ok {
				continue
			}

			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(adbc.InfoValueStringType)
			strInfoBldr.Append(arrowVersion)
		case adbc.InfoDriverADBCVersion:
			adbcVersion, ok := base.DriverInfo.GetInfoDriverADBCVersion()
			if !ok {
				continue
			}

			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(adbc.InfoValueInt64Type)
			intInfoBldr.Append(adbcVersion)
		case adbc.InfoVendorName:
			name, ok := base.DriverInfo.GetInfoVendorName()
			if !ok {
				continue
			}

			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(adbc.InfoValueStringType)
			strInfoBldr.Append(name)
		default:
			infoNameBldr.Append(uint32(code))
			value, ok := base.DriverInfo.GetInfoForInfoCode(code)
			if !ok {
				infoValueBldr.AppendNull()
				continue
			}

			// TODO: Handle other custom info types
			infoValueBldr.Append(adbc.InfoValueStringType)
			strInfoBldr.Append(fmt.Sprint(value))
		}
	}

	final := bldr.NewRecord()
	defer final.Release()
	return array.NewRecordReader(adbc.GetInfoSchema, []arrow.Record{final})
}

func (base *ConnectionImplBase) Close() error {
	return nil
}

func (base *ConnectionImplBase) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "GetObjects")
}

func (base *ConnectionImplBase) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "GetTableSchema")
}

func (base *ConnectionImplBase) GetTableTypes(context.Context) (array.RecordReader, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "GetTableTypes")
}

func (base *ConnectionImplBase) NewStatement() (adbc.Statement, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "NewStatement")
}

func (base *ConnectionImplBase) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ReadPartition")
}

func (base *ConnectionImplBase) GetOption(key string) (string, error) {
	return "", base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) GetOptionBytes(key string) ([]byte, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) GetOptionDouble(key string) (float64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) GetOptionInt(key string) (int64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) SetOption(key string, val string) error {
	switch key {
	case adbc.OptionKeyAutoCommit:
		return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", ConnectionMessageOptionUnsupported, key)
	}
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) SetOptionBytes(key string, val []byte) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) SetOptionDouble(key string, val float64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

func (base *ConnectionImplBase) SetOptionInt(key string, val int64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "%s '%s'", ConnectionMessageOptionUnknown, key)
}

type connection struct {
	ConnectionImpl

	getObjectsHelper   GetObjectsHelper
	currentNamespacer  CurrentNamespacer
	driverInfoPreparer DriverInfoPreparer
	tableTypeLister    TableTypeLister
	autocommitSetter   AutocommitSetter
}

type GetObjectsHelper interface {
	GetObjectsCatalogs(ctx context.Context, catalog *string) ([]string, error)
	GetObjectsDbSchemas(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, metadataRecords []internal.Metadata) (map[string][]string, error)
	GetObjectsTables(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, tableName *string, columnName *string, tableType []string, metadataRecords []internal.Metadata) (map[internal.CatalogAndSchema][]internal.TableInfo, error)
}

type connectionImplOption = func(*connection)

func WithGetObjectsHelper(helper GetObjectsHelper) connectionImplOption {
	return func(conn *connection) {
		conn.getObjectsHelper = helper
	}
}

func WithCurrentNamespacer(helper CurrentNamespacer) connectionImplOption {
	return func(conn *connection) {
		conn.currentNamespacer = helper
	}
}

func WithDriverInfoPreparer(helper DriverInfoPreparer) connectionImplOption {
	return func(conn *connection) {
		conn.driverInfoPreparer = helper
	}
}

func WithTableTypeLister(helper TableTypeLister) connectionImplOption {
	return func(conn *connection) {
		conn.tableTypeLister = helper
	}
}

func WithAutocommitSetter(helper AutocommitSetter) connectionImplOption {
	return func(conn *connection) {
		conn.autocommitSetter = helper
	}
}

// NewConnection wraps a ConnectionImpl to create an adbc.Connection.
func NewConnection(impl ConnectionImpl, opts ...connectionImplOption) Connection {
	conn := &connection{ConnectionImpl: impl}
	for _, opt := range opts {
		opt(conn)
	}

	return conn
}

// GetObjects implements Connection.
func (cnxn *connection) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	helper := cnxn.getObjectsHelper

	// If the getObjectsHelper has not been set, then the driver implementor has elected to provide their own GetObjects implementation
	if helper == nil {
		return cnxn.ConnectionImpl.GetObjects(ctx, depth, catalog, dbSchema, tableName, columnName, tableType)
	}

	// To avoid an N+1 query problem, we assume result sets here will fit in memory and build up a single response.
	g := internal.GetObjects{Ctx: ctx, Depth: depth, Catalog: catalog, DbSchema: dbSchema, TableName: tableName, ColumnName: columnName, TableType: tableType}
	if err := g.Init(cnxn.Base().Alloc, helper.GetObjectsDbSchemas, helper.GetObjectsTables); err != nil {
		return nil, err
	}
	defer g.Release()

	catalogs, err := helper.GetObjectsCatalogs(ctx, catalog)
	if err != nil {
		return nil, err
	}

	foundCatalog := false
	for _, catalog := range catalogs {
		g.AppendCatalog(catalog)
		foundCatalog = true
	}

	// Implementations like Dremio report no catalogs, but still have schemas
	if !foundCatalog && depth != adbc.ObjectDepthCatalogs {
		g.AppendCatalog("")
	}
	return g.Finish()
}

func (cnxn *connection) GetOption(key string) (string, error) {
	switch key {
	case adbc.OptionKeyAutoCommit:
		if cnxn.Base().Autocommit {
			return adbc.OptionValueEnabled, nil
		} else {
			return adbc.OptionValueDisabled, nil
		}
	case adbc.OptionKeyCurrentCatalog:
		if cnxn.currentNamespacer != nil {
			val, ok := cnxn.currentNamespacer.CurrentCatalog()
			if !ok {
				return "", cnxn.Base().ErrorHelper.Errorf(adbc.StatusNotFound, "The current catalog has not been set")
			}
			return val, nil
		}
	case adbc.OptionKeyCurrentDbSchema:
		if cnxn.currentNamespacer != nil {
			val, ok := cnxn.currentNamespacer.CurrentDbSchema()
			if !ok {
				return "", cnxn.Base().ErrorHelper.Errorf(adbc.StatusNotFound, "The current db schema has not been set")
			}
			return val, nil
		}
	}
	return cnxn.ConnectionImpl.GetOption(key)
}

func (cnxn *connection) SetOption(key string, val string) error {
	switch key {
	case adbc.OptionKeyAutoCommit:
		if cnxn.autocommitSetter != nil {
			if val == adbc.OptionValueEnabled {
				return cnxn.autocommitSetter.SetAutocommit(true)
			}
			if val == adbc.OptionValueDisabled {
				return cnxn.autocommitSetter.SetAutocommit(false)
			}
		}
	case adbc.OptionKeyCurrentCatalog:
		if cnxn.currentNamespacer != nil {
			return cnxn.currentNamespacer.SetCurrentCatalog(val)
		}
	case adbc.OptionKeyCurrentDbSchema:
		if cnxn.currentNamespacer != nil {
			return cnxn.currentNamespacer.SetCurrentDbSchema(val)
		}
	}
	return cnxn.ConnectionImpl.SetOption(key, val)
}

func (cnxn *connection) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	if cnxn.driverInfoPreparer != nil {
		if err := cnxn.driverInfoPreparer.PrepareDriverInfo(ctx, infoCodes); err != nil {
			return nil, err
		}
	}

	return cnxn.Base().GetInfo(ctx, infoCodes)
}

func (cnxn *connection) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	if cnxn.tableTypeLister != nil {
		tableTypes, err := cnxn.tableTypeLister.ListTableTypes(ctx)
		if err != nil {
			return nil, err
		}

		bldr := array.NewRecordBuilder(cnxn.Base().Alloc, adbc.TableTypesSchema)
		defer bldr.Release()

		bldr.Field(0).(*array.StringBuilder).AppendValues(tableTypes, nil)
		final := bldr.NewRecord()
		defer final.Release()
		return array.NewRecordReader(adbc.TableTypesSchema, []arrow.Record{final})
	}

	return cnxn.ConnectionImpl.GetTableTypes(ctx)
}

func (cnxn *connection) Commit(ctx context.Context) error {
	if cnxn.Base().Autocommit {
		return cnxn.Base().ErrorHelper.Errorf(adbc.StatusInvalidState, ConnectionMessageCannotCommit)
	}
	return cnxn.ConnectionImpl.Commit(ctx)
}

func (cnxn *connection) Rollback(ctx context.Context) error {
	if cnxn.Base().Autocommit {
		return cnxn.Base().ErrorHelper.Errorf(adbc.StatusInvalidState, ConnectionMessageCannotRollback)
	}
	return cnxn.ConnectionImpl.Rollback(ctx)
}

var _ ConnectionImpl = (*ConnectionImplBase)(nil)
