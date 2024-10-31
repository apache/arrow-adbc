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
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"golang.org/x/sync/errgroup"
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
	GetCatalogs(ctx context.Context, catalogFilter *string) ([]string, error)
	GetDBSchemasForCatalog(ctx context.Context, catalog string, schemaFilter *string) ([]string, error)
	GetTablesForDBSchema(ctx context.Context, catalog string, schema string, tableFilter *string, columnFilter *string, includeColumns bool) ([]TableInfo, error)
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

	concurrency int
}

type ConnectionBuilder struct {
	connection *connection
}

func NewConnectionBuilder(impl ConnectionImpl) *ConnectionBuilder {
	return &ConnectionBuilder{connection: &connection{ConnectionImpl: impl, concurrency: -1}}
}

func (b *ConnectionBuilder) WithDbObjectsEnumerator(helper DbObjectsEnumerator) *ConnectionBuilder {
	if b == nil {
		panic("nil ConnectionBuilder: cannot reuse after calling Connection()")
	}
	b.connection.dbObjectsEnumerator = helper
	return b
}

func (b *ConnectionBuilder) WithConcurrency(concurrency int) *ConnectionBuilder {
	if b == nil {
		panic("nil ConnectionBuilder: cannot reuse after calling Connection()")
	}
	b.connection.concurrency = concurrency
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

	catalogs, err := helper.GetCatalogs(ctx, catalog)
	if err != nil {
		return nil, err
	}

	bufferSize := len(catalogs)
	addCatalogCh := make(chan GetObjectsInfo, bufferSize)
	errCh := make(chan error, 1)
	go func() {
		defer close(addCatalogCh)
		for _, cat := range catalogs {
			addCatalogCh <- GetObjectsInfo{CatalogName: Nullable(cat)}
		}
	}()

	if depth == adbc.ObjectDepthCatalogs {
		close(errCh)
		return BuildGetObjectsRecordReader(cnxn.Base().Alloc, addCatalogCh, errCh)
	}

	g, ctxG := errgroup.WithContext(ctx)

	gSchemas, ctxSchemas := errgroup.WithContext(ctxG)
	gSchemas.SetLimit(cnxn.concurrency)
	addDbSchemasCh := make(chan GetObjectsInfo, bufferSize)
	for info := range addCatalogCh {
		info := info
		gSchemas.Go(func() error {
			dbSchemas, err := helper.GetDBSchemasForCatalog(ctxSchemas, ValueOrZero(info.CatalogName), dbSchema)
			if err != nil {
				return err
			}

			info.CatalogDbSchemas = make([]DBSchemaInfo, len(dbSchemas))
			for i, sch := range dbSchemas {
				info.CatalogDbSchemas[i] = DBSchemaInfo{DbSchemaName: Nullable(sch)}
			}

			addDbSchemasCh <- info

			return nil
		})
	}

	g.Go(func() error { defer close(addDbSchemasCh); return gSchemas.Wait() })

	if depth == adbc.ObjectDepthDBSchemas {
		rdr, err := BuildGetObjectsRecordReader(cnxn.Base().Alloc, addDbSchemasCh, errCh)
		return rdr, errors.Join(err, g.Wait())
	}

	gTables, ctxTables := errgroup.WithContext(ctxG)
	gTables.SetLimit(cnxn.concurrency)
	addTablesCh := make(chan GetObjectsInfo, bufferSize)
	for info := range addDbSchemasCh {
		info := info

		gTables.Go(func() error {
			gTablesInner, ctxTablesInner := errgroup.WithContext(ctxTables)
			gTablesInner.SetLimit(cnxn.concurrency)
			dbSchemaInfoCh := make(chan DBSchemaInfo, len(info.CatalogDbSchemas))
			for _, catalogDbSchema := range info.CatalogDbSchemas {
				catalogDbSchema := catalogDbSchema
				gTablesInner.Go(func() error {
					includeColumns := depth == adbc.ObjectDepthColumns
					tables, err := helper.GetTablesForDBSchema(ctxTablesInner, ValueOrZero(info.CatalogName), ValueOrZero(catalogDbSchema.DbSchemaName), tableName, columnName, includeColumns)
					if err != nil {
						return err
					}

					catalogDbSchema.DbSchemaTables = tables
					dbSchemaInfoCh <- catalogDbSchema

					return nil
				})
			}

			g.Go(func() error { defer close(dbSchemaInfoCh); return gTablesInner.Wait() })

			var i int
			for dbSchema := range dbSchemaInfoCh {
				info.CatalogDbSchemas[i] = dbSchema
				i++
			}

			addTablesCh <- info

			return nil
		})
	}

	g.Go(func() error { defer close(addTablesCh); return gTables.Wait() })

	rdr, err := BuildGetObjectsRecordReader(cnxn.Base().Alloc, addTablesCh, errCh)
	return rdr, errors.Join(err, g.Wait())
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

// ConstraintColumnUsage is a structured representation of adbc.UsageSchema
type ConstraintColumnUsage struct {
	ForeignKeyCatalog  *string `json:"fk_catalog,omitempty"`
	ForeignKeyDbSchema *string `json:"fk_db_schema,omitempty"`
	ForeignKeyTable    string  `json:"fk_table"`
	ForeignKeyColumn   string  `json:"fk_column_name"`
}

// ConstraintInfo is a structured representation of adbc.ConstraintSchema
type ConstraintInfo struct {
	ConstraintName        *string                 `json:"constraint_name,omitempty"`
	ConstraintType        string                  `json:"constraint_type"`
	ConstraintColumnNames requiredList[string]    `json:"constraint_column_names"`
	ConstraintColumnUsage []ConstraintColumnUsage `json:"constraint_column_usage,omitempty"`
}

// RequiredList is a wrapper for a slice of values that is not considered
// "nullable" for serialization purposes.
// When marshaling JSON, the empty value is serialized as "[]" instead of "null".
func RequiredList[T any](vals []T) requiredList[T] {
	return requiredList[T](vals)
}

type requiredList[T any] []T

func (n *requiredList[T]) UnmarshalJSON(data []byte) error {
	v := (*[]T)(n)
	return json.Unmarshal(data, v)
}

func (n requiredList[T]) MarshalJSON() ([]byte, error) {
	if n == nil {
		return []byte("[]"), nil
	}

	v := []T(n)
	return json.Marshal(v)
}

// ColumnInfo is a structured representation of adbc.ColumnSchema
type ColumnInfo struct {
	ColumnName            string  `json:"column_name"`
	OrdinalPosition       *int32  `json:"ordinal_position,omitempty"`
	Remarks               *string `json:"remarks,omitempty"`
	XdbcDataType          *int16  `json:"xdbc_data_type,omitempty"`
	XdbcTypeName          *string `json:"xdbc_type_name,omitempty"`
	XdbcColumnSize        *int32  `json:"xdbc_column_size,omitempty"`
	XdbcDecimalDigits     *int16  `json:"xdbc_decimal_digits,omitempty"`
	XdbcNumPrecRadix      *int16  `json:"xdbc_num_prec_radix,omitempty"`
	XdbcNullable          *int16  `json:"xdbc_nullable,omitempty"`
	XdbcColumnDef         *string `json:"xdbc_column_def,omitempty"`
	XdbcSqlDataType       *int16  `json:"xdbc_sql_data_type,omitempty"`
	XdbcDatetimeSub       *int16  `json:"xdbc_datetime_sub,omitempty"`
	XdbcCharOctetLength   *int32  `json:"xdbc_char_octet_length,omitempty"`
	XdbcIsNullable        *string `json:"xdbc_is_nullable,omitempty"`
	XdbcScopeCatalog      *string `json:"xdbc_scope_catalog,omitempty"`
	XdbcScopeSchema       *string `json:"xdbc_scope_schema,omitempty"`
	XdbcScopeTable        *string `json:"xdbc_scope_table,omitempty"`
	XdbcIsAutoincrement   *bool   `json:"xdbc_is_autoincrement,omitempty"`
	XdbcIsGeneratedcolumn *bool   `json:"xdbc_is_generatedcolumn,omitempty"`
}

// TableInfo is a structured representation of adbc.TableSchema
type TableInfo struct {
	TableName        string           `json:"table_name"`
	TableType        string           `json:"table_type"`
	TableColumns     []ColumnInfo     `json:"table_columns"`
	TableConstraints []ConstraintInfo `json:"table_constraints"`
}

// DBSchemaInfo is a structured representation of adbc.DBSchemaSchema
type DBSchemaInfo struct {
	DbSchemaName   *string     `json:"db_schema_name,omitempty"`
	DbSchemaTables []TableInfo `json:"db_schema_tables"`
}

// GetObjectsInfo is a structured representation of adbc.GetObjectsSchema
type GetObjectsInfo struct {
	CatalogName      *string        `json:"catalog_name,omitempty"`
	CatalogDbSchemas []DBSchemaInfo `json:"catalog_db_schemas"`
}

// Scan implements sql.Scanner.
func (g *GetObjectsInfo) Scan(src any) error {
	if src == nil {
		return nil
	}

	var b []byte
	switch s := src.(type) {
	case []byte:
		b = s
	case string:
		b = []byte(s)
	default:
		return fmt.Errorf("unexpected driver value for GetObjectsInfo: %s", s)
	}

	return json.Unmarshal(b, g)
}

// BuildGetObjectsRecordReader constructs a RecordReader for the GetObjects ADBC method.
// It accepts a channel of GetObjectsInfo to allow concurrent retrieval of metadata and
// serialization to Arrow record.
func BuildGetObjectsRecordReader(mem memory.Allocator, in <-chan GetObjectsInfo, errCh <-chan error) (array.RecordReader, error) {
	bldr := array.NewRecordBuilder(mem, adbc.GetObjectsSchema)
	defer bldr.Release()

CATALOGLOOP:
	for {
		select {
		case catalog, ok := <-in:
			if !ok {
				break CATALOGLOOP
			}
			b, err := json.Marshal(catalog)
			if err != nil {
				return nil, err
			}

			if err := json.Unmarshal(b, bldr); err != nil {
				return nil, err
			}
		case err := <-errCh:
			return nil, err
		}
	}

	rec := bldr.NewRecord()
	defer rec.Release()

	return array.NewRecordReader(adbc.GetObjectsSchema, []arrow.Record{rec})
}

func PatternToNamedArg(name string, pattern *string) sql.NamedArg {
	if pattern == nil {
		return sql.Named(name, "%")
	}
	return sql.Named(name, *pattern)
}

func ToXdbcDataType(dt arrow.DataType) (xdbcType internal.XdbcDataType) {
	switch dt.ID() {
	case arrow.EXTENSION:
		return ToXdbcDataType(dt.(arrow.ExtensionType).StorageType())
	case arrow.DICTIONARY:
		return ToXdbcDataType(dt.(*arrow.DictionaryType).ValueType)
	case arrow.RUN_END_ENCODED:
		return ToXdbcDataType(dt.(*arrow.RunEndEncodedType).Encoded())
	case arrow.INT8, arrow.UINT8:
		return internal.XdbcDataType_XDBC_TINYINT
	case arrow.INT16, arrow.UINT16:
		return internal.XdbcDataType_XDBC_SMALLINT
	case arrow.INT32, arrow.UINT32:
		return internal.XdbcDataType_XDBC_SMALLINT
	case arrow.INT64, arrow.UINT64:
		return internal.XdbcDataType_XDBC_BIGINT
	case arrow.FLOAT32, arrow.FLOAT16, arrow.FLOAT64:
		return internal.XdbcDataType_XDBC_FLOAT
	case arrow.DECIMAL, arrow.DECIMAL256:
		return internal.XdbcDataType_XDBC_DECIMAL
	case arrow.STRING, arrow.LARGE_STRING:
		return internal.XdbcDataType_XDBC_VARCHAR
	case arrow.BINARY, arrow.LARGE_BINARY:
		return internal.XdbcDataType_XDBC_BINARY
	case arrow.FIXED_SIZE_BINARY:
		return internal.XdbcDataType_XDBC_BINARY
	case arrow.BOOL:
		return internal.XdbcDataType_XDBC_BIT
	case arrow.TIME32, arrow.TIME64:
		return internal.XdbcDataType_XDBC_TIME
	case arrow.DATE32, arrow.DATE64:
		return internal.XdbcDataType_XDBC_DATE
	case arrow.TIMESTAMP:
		return internal.XdbcDataType_XDBC_TIMESTAMP
	case arrow.DENSE_UNION, arrow.SPARSE_UNION:
		return internal.XdbcDataType_XDBC_VARBINARY
	case arrow.LIST, arrow.LARGE_LIST, arrow.FIXED_SIZE_LIST:
		return internal.XdbcDataType_XDBC_VARBINARY
	case arrow.STRUCT, arrow.MAP:
		return internal.XdbcDataType_XDBC_VARBINARY
	default:
		return internal.XdbcDataType_XDBC_UNKNOWN_TYPE
	}
}

// Nullable wraps a value and returns a pointer to the value, which is
// how nullable values are represented for purposes of JSON serialization.
func Nullable[T any](val T) *T {
	return &val
}

// ValueOrZero safely dereferences a pointer, returning the zero-value
// of the underlying type in the case of a nil pointer.
func ValueOrZero[T any](val *T) T {
	var res T
	if val == nil {
		return res
	}
	return *val
}

var _ ConnectionImpl = (*ConnectionImplBase)(nil)
