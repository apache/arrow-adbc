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
	"log/slog"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
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
}

// CurrentNamespacer is an interface that drivers may implement to delegate
// stateful namespacing with DB catalogs and schemas. The appropriate (Get/Set)Options
// implementations will be provided using the results of these methods.
type CurrentNamespacer interface {
	GetCurrentCatalog() (string, error)
	GetCurrentDbSchema() (string, error)
	SetCurrentCatalog(string) error
	SetCurrentDbSchema(string) error
}

// DriverInfoPreparer is an interface that drivers may implement to add/update
// DriverInfo values whenever adbc.Connection.GetInfo() is called.
type DriverInfoPreparer interface {
	PrepareDriverInfo(ctx context.Context, infoCodes []adbc.InfoCode) error
}

// TableTypeLister is an interface that drivers may implement to simplify the
// implementation of adbc.Connection.GetTableTypes() for backends that do not natively
// send these values as arrow records. The conversion of the result to a RecordReader
// is handled automatically.
type TableTypeLister interface {
	ListTableTypes(ctx context.Context) ([]string, error)
}

// AutocommitSetter is an interface that drivers may implement to simplify the
// implementation of autocommit state management. There is no need to implement
// this for backends that do not support autocommit, as this is already the default
// behavior. SetAutocommit should only attempt to update the autocommit state in the
// backend. Local driver state is automatically updated if the result of this call
// does not produce an error. (Get/Set)Options implementations are provided automatically
// as well/
type AutocommitSetter interface {
	SetAutocommit(enabled bool) error
}

// DbObjectsEnumerator is an interface that drivers may implement to simplify the
// implementation of adbc.Connection.GetObjects(). By independently implementing lookup
// for catalogs, dbSchemas and tables, the driverbase is able to provide the full
// GetObjects functionality for arbitrary search patterns and lookup depth.
type DbObjectsEnumerator interface {
	GetObjectsCatalogs(ctx context.Context, catalog *string) ([]string, error)
	GetObjectsDbSchemas(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, metadataRecords []internal.Metadata) (map[string][]string, error)
	GetObjectsTables(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, tableName *string, columnName *string, tableType []string, metadataRecords []internal.Metadata) (map[internal.CatalogAndSchema][]internal.TableInfo, error)
}

// Connection is the interface satisfied by the result of the NewConnection constructor,
// given that an input is provided satisfying the ConnectionImpl interface.
type Connection interface {
	adbc.Connection
	adbc.GetSetOptions
}

// ConnectionImplBase is a struct that provides default implementations of the
// ConnectionImpl interface. It is meant to be used as a composite struct for a
// driver's ConnectionImpl implementation.
type ConnectionImplBase struct {
	Alloc       memory.Allocator
	ErrorHelper ErrorHelper
	DriverInfo  *DriverInfo
	Logger      *slog.Logger

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
		Logger:      database.Logger,
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
	boolInfoBldr := infoValueBldr.Child(int(adbc.InfoValueBooleanType)).(*array.BooleanBuilder)

	for _, code := range infoCodes {
		infoNameBldr.Append(uint32(code))
		value, ok := base.DriverInfo.GetInfoForInfoCode(code)

		// We want to return a null value if the info_code requested is set to nil.
		// The null value needs a type so we arbitrarily choose string (type_code: 0)
		if value == nil {
			value = ""
			ok = false
		}

		switch v := value.(type) {
		case string:
			infoValueBldr.Append(adbc.InfoValueStringType)
			if ok {
				strInfoBldr.Append(v)
			} else {
				strInfoBldr.AppendNull()
			}
		case int64:
			infoValueBldr.Append(adbc.InfoValueInt64Type)
			if ok {
				intInfoBldr.Append(v)
			} else {
				intInfoBldr.AppendNull()
			}
		case bool:
			infoValueBldr.Append(adbc.InfoValueBooleanType)
			if ok {
				boolInfoBldr.Append(v)
			} else {
				boolInfoBldr.AppendNull()
			}
		default:
			return nil, fmt.Errorf("no defined type code for info_value of type %T", v)
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

	dbObjectsEnumerator DbObjectsEnumerator
	currentNamespacer   CurrentNamespacer
	driverInfoPreparer  DriverInfoPreparer
	tableTypeLister     TableTypeLister
	autocommitSetter    AutocommitSetter
}

type ConnectionBuilder struct {
	connection *connection
}

func NewConnectionBuilder(impl ConnectionImpl) *ConnectionBuilder {
	return &ConnectionBuilder{connection: &connection{ConnectionImpl: impl}}
}

func (b *ConnectionBuilder) WithDbObjectsEnumerator(helper DbObjectsEnumerator) *ConnectionBuilder {
	if b == nil {
		panic("nil ConnectionBuilder: cannot reuse after calling Connection()")
	}
	b.connection.dbObjectsEnumerator = helper
	return b
}

func (b *ConnectionBuilder) WithCurrentNamespacer(helper CurrentNamespacer) *ConnectionBuilder {
	if b == nil {
		panic("nil ConnectionBuilder: cannot reuse after calling Connection()")
	}
	b.connection.currentNamespacer = helper
	return b
}

func (b *ConnectionBuilder) WithDriverInfoPreparer(helper DriverInfoPreparer) *ConnectionBuilder {
	if b == nil {
		panic("nil ConnectionBuilder: cannot reuse after calling Connection()")
	}
	b.connection.driverInfoPreparer = helper
	return b
}

func (b *ConnectionBuilder) WithAutocommitSetter(helper AutocommitSetter) *ConnectionBuilder {
	if b == nil {
		panic("nil ConnectionBuilder: cannot reuse after calling Connection()")
	}
	b.connection.autocommitSetter = helper
	return b
}

func (b *ConnectionBuilder) WithTableTypeLister(helper TableTypeLister) *ConnectionBuilder {
	if b == nil {
		panic("nil ConnectionBuilder: cannot reuse after calling Connection()")
	}
	b.connection.tableTypeLister = helper
	return b
}

func (b *ConnectionBuilder) Connection() Connection {
	conn := b.connection
	b.connection = nil
	return conn
}

// GetObjects implements Connection.
func (cnxn *connection) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	helper := cnxn.dbObjectsEnumerator

	// If the dbObjectsEnumerator has not been set, then the driver implementor has elected to provide their own GetObjects implementation
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
			val, err := cnxn.currentNamespacer.GetCurrentCatalog()
			if err != nil {
				return "", cnxn.Base().ErrorHelper.Errorf(adbc.StatusNotFound, "failed to get current catalog: %s", err)
			}
			return val, nil
		}
	case adbc.OptionKeyCurrentDbSchema:
		if cnxn.currentNamespacer != nil {
			val, err := cnxn.currentNamespacer.GetCurrentDbSchema()
			if err != nil {
				return "", cnxn.Base().ErrorHelper.Errorf(adbc.StatusNotFound, "failed to get current db schema: %s", err)
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

			var autocommit bool
			switch val {
			case adbc.OptionValueEnabled:
				autocommit = true
			case adbc.OptionValueDisabled:
				autocommit = false
			default:
				return cnxn.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "cannot set value %s for key %s", val, key)
			}

			err := cnxn.autocommitSetter.SetAutocommit(autocommit)
			if err == nil {
				// Only update the driver state if the action was successful
				cnxn.Base().Autocommit = autocommit
			}

			return err
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
	if cnxn.tableTypeLister == nil {
		return cnxn.ConnectionImpl.GetTableTypes(ctx)
	}

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

func (cnxn *connection) Close() error {
	if cnxn.Base().Closed {
		return cnxn.Base().ErrorHelper.Errorf(adbc.StatusInvalidState, "Trying to close already closed connection")
	}

	err := cnxn.ConnectionImpl.Close()
	if err == nil {
		cnxn.Base().Closed = true
	}

	return err
}

var _ ConnectionImpl = (*ConnectionImplBase)(nil)
