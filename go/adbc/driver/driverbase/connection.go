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
	"runtime/debug"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow/go/v16/arrow"
	"github.com/apache/arrow/go/v16/arrow/array"
	"github.com/apache/arrow/go/v16/arrow/memory"
)

var (
	infoDriverArrowVersion string
)

func init() {
	if info, ok := debug.ReadBuildInfo(); ok {
		for _, dep := range info.Deps {
			switch {
			case strings.HasPrefix(dep.Path, "github.com/apache/arrow/go/"):
				infoDriverArrowVersion = dep.Version
			}
		}
	}
	// XXX: Deps not populated in tests
	// https://github.com/golang/go/issues/33976
	if infoDriverArrowVersion == "" {
		infoDriverArrowVersion = "(unknown or development build)"
	}
}

// ConnectionImpl is an interface that drivers implement to provide
// vendor-specific functionality.
type ConnectionImpl interface {
	// adbc.ConnectionGetStatistics
	adbc.GetSetOptions
	Base() *ConnectionImplBase

	// Will be called at most once
	Close() error
	// Will not be called unless autocommit is disabled
	Commit(context.Context) error
	CurrentCatalog() (string, bool)
	CurrentDbSchema() (string, bool)
	// Get boxed values for info codes
	GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (map[adbc.InfoCode]interface{}, error)
	// Get all info codes the driver supports (minus the 6 standard codes
	// which are assumed to always be supported)
	GetInfoCodes() []adbc.InfoCode
	// Get all catalogs
	GetObjectsCatalogs(ctx context.Context, catalog *string) ([]string, error)
	GetObjectsDbSchemas(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, metadataRecords []internal.Metadata) (map[string][]string, error)
	GetObjectsTables(ctx context.Context, depth adbc.ObjectDepth, catalog *string, schema *string, tableName *string, columnName *string, tableType []string, metadataRecords []internal.Metadata) (map[internal.CatalogAndSchema][]internal.TableInfo, error)
	GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error)
	GetTableTypes(ctx context.Context) (array.RecordReader, error)
	NewStatement() (adbc.Statement, error)
	ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error)
	// Will not be called unless autocommit is disabled
	Rollback(context.Context) error
	SetAutocommit(enabled bool) error
}

// ConnectionImplBase is a struct that provides default implementations of the
// ConnectionImpl interface. It is meant to be used as a composite struct for a
// driver's ConnectionImpl implementation.
type ConnectionImplBase struct {
	Alloc       memory.Allocator
	ErrorHelper ErrorHelper

	Autocommit bool
	Closed     bool
}

// NewConnectionImplBase instantiates ConnectionImplBase.
func NewConnectionImplBase(database *DatabaseImplBase) ConnectionImplBase {
	return ConnectionImplBase{
		Alloc:       database.Alloc,
		ErrorHelper: database.ErrorHelper,
		Autocommit:  true,
		Closed:      false,
	}
}

func (base *ConnectionImplBase) Base() *ConnectionImplBase {
	return base
}

func (base *ConnectionImplBase) Close() error {
	return nil
}

func (base *ConnectionImplBase) Commit() error {
	return nil
}

func (base *ConnectionImplBase) CurrentCatalog() (string, bool) {
	return "", false
}

func (base *ConnectionImplBase) CurrentDbSchema() (string, bool) {
	return "", false
}

func (base *ConnectionImplBase) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (map[adbc.InfoCode]interface{}, error) {
	return nil, nil
}

func (base *ConnectionImplBase) GetInfoCodes() []adbc.InfoCode {
	return []adbc.InfoCode{}
}

func (base *ConnectionImplBase) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "GetTableSchema")
}

func (base *ConnectionImplBase) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "GetTableTypes")
}

func (base *ConnectionImplBase) ReadPartition(ctx context.Context, serializedPartition []byte) (rdr array.RecordReader, err error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "ReadPartition")
}

func (base *ConnectionImplBase) SetAutocommit(enabled bool) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Autocommit")
}

func (base *ConnectionImplBase) GetOption(key string) (string, error) {
	return "", base.ErrorHelper.Errorf(adbc.StatusNotFound, "Unknown connection option '%s'", key)
}

func (base *ConnectionImplBase) GetOptionBytes(key string) ([]byte, error) {
	return nil, base.ErrorHelper.Errorf(adbc.StatusNotFound, "Unknown connection option '%s'", key)
}

func (base *ConnectionImplBase) GetOptionDouble(key string) (float64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "Unknown connection option '%s'", key)
}

func (base *ConnectionImplBase) GetOptionInt(key string) (int64, error) {
	return 0, base.ErrorHelper.Errorf(adbc.StatusNotFound, "Unknown connection option '%s'", key)
}

func (base *ConnectionImplBase) SetOption(key string, val string) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Unknown connection option '%s'", key)
}

func (base *ConnectionImplBase) SetOptionBytes(key string, val []byte) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Unknown connection option '%s'", key)
}

func (base *ConnectionImplBase) SetOptionDouble(key string, val float64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Unknown connection option '%s'", key)
}

func (base *ConnectionImplBase) SetOptionInt(key string, val int64) error {
	return base.ErrorHelper.Errorf(adbc.StatusNotImplemented, "Unknown connection option '%s'", key)
}

// connection is the implementation of adbc.Connection.
type connection struct {
	impl ConnectionImpl
}

// NewConnection wraps a ConnectionImpl to create an adbc.Connection.
func NewConnection(impl ConnectionImpl) adbc.Connection {
	return &connection{
		impl: impl,
	}
}

func (cnxn *connection) Close() error {
	if cnxn.impl.Base().Closed {
		return cnxn.impl.Base().ErrorHelper.Errorf(adbc.StatusInvalidState, "Trying to close already closed connection")
	}

	return cnxn.impl.Close()
}

func (cnxn *connection) Commit(ctx context.Context) error {
	if cnxn.impl.Base().Autocommit {
		return cnxn.impl.Base().ErrorHelper.Errorf(adbc.StatusInvalidState, "Cannot commit when autocommit is enabled")
	}
	return cnxn.impl.Commit(ctx)
}

func (cnxn *connection) GetInfo(ctx context.Context, infoCodes []adbc.InfoCode) (array.RecordReader, error) {
	const strValTypeID arrow.UnionTypeCode = 0
	const intValTypeID arrow.UnionTypeCode = 2

	if len(infoCodes) == 0 {
		infoCodes = append([]adbc.InfoCode{
			adbc.InfoVendorName,
			adbc.InfoVendorVersion,
			adbc.InfoVendorArrowVersion,
			adbc.InfoDriverName,
			adbc.InfoDriverVersion,
			adbc.InfoDriverArrowVersion,
			adbc.InfoDriverADBCVersion,
		}, cnxn.impl.GetInfoCodes()...)
	}

	bldr := array.NewRecordBuilder(cnxn.impl.Base().Alloc, adbc.GetInfoSchema)
	defer bldr.Release()
	bldr.Reserve(len(infoCodes))

	infoNameBldr := bldr.Field(0).(*array.Uint32Builder)
	infoValueBldr := bldr.Field(1).(*array.DenseUnionBuilder)
	strInfoBldr := infoValueBldr.Child(int(strValTypeID)).(*array.StringBuilder)
	intInfoBldr := infoValueBldr.Child(int(intValTypeID)).(*array.Int64Builder)

	// Handle some codes up front
	driverCodes := []adbc.InfoCode{}
	for _, code := range infoCodes {
		switch code {
		case adbc.InfoDriverArrowVersion:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(infoDriverArrowVersion)
		case adbc.InfoDriverADBCVersion:
			infoNameBldr.Append(uint32(code))
			infoValueBldr.Append(intValTypeID)
			intInfoBldr.Append(adbc.AdbcVersion1_1_0)
		default:
			driverCodes = append(driverCodes, code)
		}
	}

	values, err := cnxn.impl.GetInfo(ctx, driverCodes)
	if err != nil {
		return nil, err
	}

	for code, rawValue := range values {
		infoNameBldr.Append(uint32(code))
		if v, ok := rawValue.(string); ok {
			infoValueBldr.Append(strValTypeID)
			strInfoBldr.Append(v)
		} else if v, ok := rawValue.(int64); ok {
			infoValueBldr.Append(intValTypeID)
			intInfoBldr.Append(v)
		} else {
			panic("driverbase/connection: other info types are not currently implemented")
		}
	}
	final := bldr.NewRecord()
	defer final.Release()
	return array.NewRecordReader(adbc.GetInfoSchema, []arrow.Record{final})
}

func (cnxn *connection) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error) {
	// To avoid an N+1 query problem, we assume result sets here will fit in memory and build up a single response.
	g := internal.GetObjects{Ctx: ctx, Depth: depth, Catalog: catalog, DbSchema: dbSchema, TableName: tableName, ColumnName: columnName, TableType: tableType}
	if err := g.Init(cnxn.impl.Base().Alloc, cnxn.impl.GetObjectsDbSchemas, cnxn.impl.GetObjectsTables); err != nil {
		return nil, err
	}
	defer g.Release()

	catalogs, err := cnxn.impl.GetObjectsCatalogs(ctx, catalog)
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
		if cnxn.impl.Base().Autocommit {
			return adbc.OptionValueEnabled, nil
		} else {
			return adbc.OptionValueDisabled, nil
		}
	case adbc.OptionKeyCurrentCatalog:
		val, ok := cnxn.impl.CurrentCatalog()
		if !ok {
			return "", cnxn.impl.Base().ErrorHelper.Errorf(adbc.StatusNotFound, "%s not supported", key)
		}
		return val, nil
	case adbc.OptionKeyCurrentDbSchema:
		val, ok := cnxn.impl.CurrentDbSchema()
		if !ok {
			return "", cnxn.impl.Base().ErrorHelper.Errorf(adbc.StatusNotFound, "%s not supported", key)
		}
		return val, nil
	}
	return cnxn.impl.GetOption(key)
}

func (cnxn *connection) GetOptionBytes(key string) ([]byte, error) {
	return cnxn.impl.GetOptionBytes(key)
}

func (cnxn *connection) GetOptionDouble(key string) (float64, error) {
	return cnxn.impl.GetOptionDouble(key)
}

func (cnxn *connection) GetOptionInt(key string) (int64, error) {
	return cnxn.impl.GetOptionInt(key)
}

func (cnxn *connection) GetStatistics(ctx context.Context, catalog, dbSchema, tableName *string, approximate bool) (array.RecordReader, error) {

	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (cnxn *connection) GetStatisticNames(ctx context.Context) (array.RecordReader, error) {

	return nil, adbc.Error{Code: adbc.StatusNotImplemented}
}

func (cnxn *connection) GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error) {
	return cnxn.impl.GetTableSchema(ctx, catalog, dbSchema, tableName)
}

func (cnxn *connection) GetTableTypes(ctx context.Context) (array.RecordReader, error) {
	return cnxn.impl.GetTableTypes(ctx)
}

func (cnxn *connection) NewStatement() (adbc.Statement, error) {
	return cnxn.impl.NewStatement()
}

func (cnxn *connection) ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error) {
	return cnxn.impl.ReadPartition(ctx, serializedPartition)

}

func (cnxn *connection) Rollback(ctx context.Context) error {
	if cnxn.impl.Base().Autocommit {
		return cnxn.impl.Base().ErrorHelper.Errorf(adbc.StatusInvalidState, "Cannot rollback when autocommit is enabled")
	}
	return cnxn.impl.Rollback(ctx)
}

func (cnxn *connection) SetOption(key string, val string) error {
	switch key {
	case adbc.OptionKeyAutoCommit:
		autocommit := true
		switch val {
		case adbc.OptionValueEnabled:
			// Do nothing
		case adbc.OptionValueDisabled:
			autocommit = false
		default:
			return cnxn.impl.Base().ErrorHelper.Errorf(adbc.StatusInvalidArgument, "Invalid value for %s: %s", key, val)
		}

		if autocommit == cnxn.impl.Base().Autocommit {
			// No-op
			return nil
		}
		if err := cnxn.impl.SetAutocommit(autocommit); err != nil {
			return err
		}
		cnxn.impl.Base().Autocommit = autocommit
		return nil
	}
	return cnxn.impl.SetOption(key, val)
}

func (cnxn *connection) SetOptionBytes(key string, val []byte) error {
	return cnxn.impl.SetOptionBytes(key, val)
}

func (cnxn *connection) SetOptionDouble(key string, val float64) error {
	return cnxn.impl.SetOptionDouble(key, val)
}

func (cnxn *connection) SetOptionInt(key string, val int64) error {
	return cnxn.impl.SetOptionInt(key, val)
}
