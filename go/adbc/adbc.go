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

// Package adbc defines the interfaces for Arrow Database
// Connectivity.
//
// An Arrow-based interface between applications and database
// drivers.  ADBC aims to provide a vendor-independent API for SQL
// and Substrait-based database access that is targeted at
// analytics/OLAP use cases.
//
// This API is intended to be implemented directly by drivers and
// used directly by client applications.  To assist portability
// between different vendors, a "driver manager" library is also
// provided, which implements this same API, but dynamically loads
// drivers internally and forwards calls appropriately.
//
// In general, it's expected for objects to allow serialized access
// safely from multiple goroutines, but not necessarily concurrent
// access. Specific implementations may allow concurrent access.
//
// EXPERIMENTAL. Interface subject to change.
package adbc

import (
	"context"
	"fmt"

	"github.com/apache/arrow/go/v12/arrow"
	"github.com/apache/arrow/go/v12/arrow/array"
)

//go:generate go run golang.org/x/tools/cmd/stringer -type Status -linecomment
//go:generate go run golang.org/x/tools/cmd/stringer -type InfoCode -linecomment

// Error is the detailed error for an operation
type Error struct {
	// Msg is a string representing a human readable error message
	Msg string
	// Code is the ADBC status representing this error
	Code Status
	// VendorCode is a vendor-specific error codee, if applicable
	VendorCode int32
	// SqlState is a SQLSTATE error code, if provided, as defined
	// by the SQL:2003 standard. If not set, it will be "\0\0\0\0\0"
	SqlState [5]byte
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: SqlState: %s, msg: %s", e.Code, string(e.SqlState[:]), e.Msg)
}

// Status represents an error code for operations that may fail
type Status uint8

const (
	// No Error
	StatusOK Status = iota // OK
	// An unknown error occurred.
	//
	// May indicate a driver-side or database-side error
	StatusUnknown // Unknown
	// The operation is not implemented or supported.
	//
	// May indicate a driver-side or database-side error
	StatusNotImplemented // Not Implemented
	// A requested resource was not found.
	//
	// May indicate a driver-side or database-side error
	StatusNotFound // Not Found
	// A requested resource already exists
	//
	// May indicate a driver-side or database-side error
	StatusAlreadyExists // Already Exists
	// The arguments are invalid, likely a programming error.
	//
	// For instance, they may be of the wrong format, or out of range.
	//
	// May indicate a driver-side or database-side error.
	StatusInvalidArgument // Invalid Argument
	// The preconditions for the operation are not met, likely a
	// programming error.
	//
	// For instance, the object may be uninitialized, or may not
	// have been fully configured.
	//
	// May indicate a driver-side or database-side error
	StatusInvalidState // Invalid State
	// Invalid data was processed (not a programming error)
	//
	// For instance, a division by zero may have occurred during query
	// execution.
	//
	// May indicate a database-side error only.
	StatusInvalidData // Invalid Data
	// The database's integrity was affected.
	//
	// For instance, a foreign key check may have failed, or a uniqueness
	// constraint may have been violated.
	//
	// May indicate a database-side error only.
	StatusIntegrity // Integrity Issue
	// An error internal to the driver or database occurred.
	//
	// May indicate a driver-side or database-side error.
	StatusInternal // Internal
	// An I/O error occurred.
	//
	// For instance a remote service may be unavailable.
	//
	// May indicate a driver-side or database-side error.
	StatusIO // I/O
	// The operation was cancelled, not due to a timeout.
	//
	// May indicate a driver-side or database-side error.
	StatusCancelled // Cancelled
	// The operation was cancelled due to a timeout.
	//
	// May indicate a driver-side or database-side error.
	StatusTimeout // Timeout
	// Authentication failed.
	//
	// May indicate a database-side error only.
	StatusUnauthenticated // Unauthenticated
	// The client is not authorized to perform the given operation.
	//
	// May indicate a database-side error only.
	StatusUnauthorized // Unauthorized
)

const (
	AdbcVersion1_0_0 int64 = 1_000_000
	AdbcVersion1_1_0 int64 = 1_001_000
)

// Canonical option values
const (
	OptionValueEnabled  = "true"
	OptionValueDisabled = "false"
	OptionKeyAutoCommit = "adbc.connection.autocommit"
	// The current catalog.
	OptionKeyCurrentCatalog = "adbc.connection.catalog"
	// The current schema.
	OptionKeyCurrentDbSchema = "adbc.connection.db_schema"
	// Make ExecutePartitions nonblocking.
	OptionKeyIncremental = "adbc.statement.exec.incremental"
	// Get the progress
	OptionKeyProgress                 = "adbc.statement.exec.progress"
	OptionKeyIngestTargetTable        = "adbc.ingest.target_table"
	OptionKeyIngestMode               = "adbc.ingest.mode"
	OptionKeyIsolationLevel           = "adbc.connection.transaction.isolation_level"
	OptionKeyReadOnly                 = "adbc.connection.readonly"
	OptionValueIngestModeCreate       = "adbc.ingest.mode.create"
	OptionValueIngestModeAppend       = "adbc.ingest.mode.append"
	OptionValueIngestModeReplace      = "adbc.ingest.mode.replace"
	OptionValueIngestModeCreateAppend = "adbc.ingest.mode.create_append"
	OptionKeyURI                      = "uri"
	OptionKeyUsername                 = "username"
	OptionKeyPassword                 = "password"
)

type OptionIsolationLevel string

const (
	LevelDefault         OptionIsolationLevel = "adbc.connection.transaction.isolation.default"
	LevelReadUncommitted OptionIsolationLevel = "adbc.connection.transaction.isolation.read_uncommitted"
	LevelReadCommitted   OptionIsolationLevel = "adbc.connection.transaction.isolation.read_committed"
	LevelRepeatableRead  OptionIsolationLevel = "adbc.connection.transaction.isolation.repeatable_read"
	LevelSnapshot        OptionIsolationLevel = "adbc.connection.transaction.isolation.snapshot"
	LevelSerializable    OptionIsolationLevel = "adbc.connection.transaction.isolation.serializable"
	LevelLinearizable    OptionIsolationLevel = "adbc.connection.transaction.isolation.linearizable"
)

// Canonical property values
const (
	PropertyProgress = "adbc.statement.exec.progress"
)

// Driver is the entry point for the interface. It is similar to
// database/sql.Driver taking a map of keys and values as options
// to initialize a Connection to the database. Any common connection
// state can live in the Driver itself, for example an in-memory database
// can place ownership of the actual database in this driver.
//
// Any connection specific options should be set using SetOptions before
// calling Open.
//
// The provided context.Context is for dialing purposes only
// (see net.DialContext) and should not be stored or used for other purposes.
// A default timeout should still be used when dialing as a connection
// pool may call Connect asynchronously to any query.
//
// A driver can also optionally implement io.Closer if there is a need
// or desire for it.
type Driver interface {
	NewDatabase(opts map[string]string) (Database, error)
}

type Database interface {
	SetOptions(map[string]string) error
	Open(ctx context.Context) (Connection, error)
}

type InfoCode uint32

const (
	// The database vendor/product name (e.g. the server name)
	// (type: utf8)
	InfoVendorName InfoCode = 0 // VendorName
	// The database vendor/product version (type: utf8)
	InfoVendorVersion InfoCode = 1 // VendorVersion
	// The database vendor/product Arrow library version (type: utf8)
	InfoVendorArrowVersion InfoCode = 2 // VendorArrowVersion

	// The driver name (type: utf8)
	InfoDriverName InfoCode = 100 // DriverName
	// The driver version (type: utf8)
	InfoDriverVersion InfoCode = 101 // DriverVersion
	// The driver Arrow library version (type: utf8)
	InfoDriverArrowVersion InfoCode = 102 // DriverArrowVersion
	// The driver ADBC API version (type: int64)
	InfoDriverADBCVersion InfoCode = 103 // DriverADBCVersion
)

type ObjectDepth int

const (
	ObjectDepthAll ObjectDepth = iota
	ObjectDepthCatalogs
	ObjectDepthDBSchemas
	ObjectDepthTables
	ObjectDepthColumns = ObjectDepthAll
)

// Connection is an active Database connection.
//
// It provides methods for creating statements, using transactions
// and so on.
//
// Connections are not required to be safely accessible by concurrent
// goroutines.
type Connection interface {
	// Metadata methods
	//
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
	//
	// Since ADBC 1.1.0: the range [500, 1_000) is reserved for "XDBC"
	// information, which is the same metadata provided by the same info
	// code range in the Arrow Flight SQL GetSqlInfo RPC.
	GetInfo(ctx context.Context, infoCodes []InfoCode) (array.RecordReader, error)

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
	GetObjects(ctx context.Context, depth ObjectDepth, catalog, dbSchema, tableName, columnName *string, tableType []string) (array.RecordReader, error)

	GetTableSchema(ctx context.Context, catalog, dbSchema *string, tableName string) (*arrow.Schema, error)

	// GetTableTypes returns a list of the table types in the database.
	//
	// The result is an arrow dataset with the following schema:
	//
	//		Field Name			| Field Type
	//		----------------|--------------
	//		table_type			| utf8 not null
	//
	GetTableTypes(context.Context) (array.RecordReader, error)

	// Commit commits any pending transactions on this connection, it should
	// only be used if autocommit is disabled.
	//
	// Behavior is undefined if this is mixed with SQL transaction statements.
	Commit(context.Context) error

	// Rollback rolls back any pending transactions. Only used if autocommit
	// is disabled.
	//
	// Behavior is undefined if this is mixed with SQL transaction statements.
	Rollback(context.Context) error

	// NewStatement initializes a new statement object tied to this connection
	NewStatement() (Statement, error)

	// Close closes this connection and releases any associated resources.
	Close() error

	// ReadPartition constructs a statement for a partition of a query. The
	// results can then be read independently using the returned RecordReader.
	//
	// A partition can be retrieved by using ExecutePartitions on a statement.
	ReadPartition(ctx context.Context, serializedPartition []byte) (array.RecordReader, error)
}

// PostInitOptions is an optional interface which can be implemented by
// drivers which allow modifying and setting options after initializing
// a connection or statement.
type PostInitOptions interface {
	SetOption(key, value string) error
}

// Partitions represent a partitioned result set.
//
// Some backends may internally partition the results. These partitions
// are exposed to clients who may wish to integrate them with a threaded
// or distributed execution model, where partitions can be divided among
// threads or machines and fetched in parallel.
//
// To use partitioning, execute the statement with ExecutePartitions to
// get the partition descriptors. Then call ReadPartition on a connection
// to turn individual descriptors into RecordReader instances. This may
// be done on a different connection than the one the partition was
// created with, or even in a different process on a different machine.
//
// Drivers are not required to support partitioning.
type Partitions struct {
	NumPartitions uint64
	PartitionIDs  [][]byte
}

// Statement is a container for all state needed to execute a database
// query, such as the query itself, parameters for prepared statements,
// driver parameters, etc.
//
// Statements may represent a single query or a prepared statement.
//
// Statements may be used multiple times and can be reconfigured
// (e.g. they can be reused to execute multiple different queries).
// However, executing a statement (and changing certain other state)
// will invalidate result sets obtained prior to that execution.
//
// Multiple statements may be created from a single connection.
// However, the driver may block or error if they are used concurrently
// (whether from a single goroutine or from multiple simultaneous
// goroutines).
//
// Statements are not required to be goroutine-safe, but they can be
// used from multiple goroutines as long as clients serialize accesses
// to a statement.
type Statement interface {
	// Close releases any relevant resources associated with this statement
	// and closes it (particularly if it is a prepared statement).
	//
	// A statement instance should not be used after Close is called.
	Close() error

	// SetOption sets a string option on this statement
	SetOption(key, val string) error

	// SetSqlQuery sets the query string to be executed.
	//
	// The query can then be executed with any of the Execute methods.
	// For queries expected to be executed repeatedly, Prepare should be
	// called before execution.
	SetSqlQuery(query string) error

	// ExecuteQuery executes the current query or prepared statement
	// and returnes a RecordReader for the results along with the number
	// of rows affected if known, otherwise it will be -1.
	//
	// This invalidates any prior result sets on this statement.
	//
	// Since ADBC 1.1.0: releasing the returned RecordReader without
	// consuming it fully is equivalent to calling AdbcStatementCancel.
	ExecuteQuery(context.Context) (array.RecordReader, int64, error)

	// ExecuteUpdate executes a statement that does not generate a result
	// set. It returns the number of rows affected if known, otherwise -1.
	ExecuteUpdate(context.Context) (int64, error)

	// Prepare turns this statement into a prepared statement to be executed
	// multiple times. This invalidates any prior result sets.
	Prepare(context.Context) error

	// SetSubstraitPlan allows setting a serialized Substrait execution
	// plan into the query or for querying Substrait-related metadata.
	//
	// Drivers are not required to support both SQL and Substrait semantics.
	// If they do, it may be via converting between representations internally.
	//
	// Like SetSqlQuery, after this is called the query can be executed
	// using any of the Execute methods. If the query is expected to be
	// executed repeatedly, Prepare should be called first on the statement.
	SetSubstraitPlan(plan []byte) error

	// Bind uses an arrow record batch to bind parameters to the query.
	//
	// This can be used for bulk inserts or for prepared statements.
	// The driver will call release on the passed in Record when it is done,
	// but it may not do this until the statement is closed or another
	// record is bound.
	Bind(ctx context.Context, values arrow.Record) error

	// BindStream uses a record batch stream to bind parameters for this
	// query. This can be used for bulk inserts or prepared statements.
	//
	// The driver will call Release on the record reader, but may not do this
	// until Close is called.
	BindStream(ctx context.Context, stream array.RecordReader) error

	// GetParameterSchema returns an Arrow schema representation of
	// the expected parameters to be bound.
	//
	// This retrieves an Arrow Schema describing the number, names, and
	// types of the parameters in a parameterized statement. The fields
	// of the schema should be in order of the ordinal position of the
	// parameters; named parameters should appear only once.
	//
	// If the parameter does not have a name, or a name cannot be determined,
	// the name of the corresponding field in the schema will be an empty
	// string. If the type cannot be determined, the type of the corresponding
	// field will be NA (NullType).
	//
	// This should be called only after calling Prepare.
	//
	// This should return an error with StatusNotImplemented if the schema
	// cannot be determined.
	GetParameterSchema() (*arrow.Schema, error)

	// ExecutePartitions executes the current statement and gets the results
	// as a partitioned result set.
	//
	// It returns the Schema of the result set, the collection of partition
	// descriptors and the number of rows affected, if known. If unknown,
	// the number of rows affected will be -1.
	//
	// If the driver does not support partitioned results, this will return
	// an error with a StatusNotImplemented code.
	//
	// When OptionKeyIncremental is set, this should be called
	// repeatedly until receiving an empty Partitions.
	ExecutePartitions(context.Context) (*arrow.Schema, Partitions, int64, error)
}

// StatementCancel is a Statement that also supports Cancel.
//
// Since ADBC API revision 1.1.0.
type StatementCancel interface {
	// Cancel stops execution of an in-progress query.
	//
	// This can be called during ExecuteQuery (or similar), or while
	// consuming a RecordReader returned from such.  Calling this
	// function should make the other functions return an error with a
	// StatusCancelled code.
	//
	// This must always be thread-safe (other operations are not
	// necessarily thread-safe).
	Cancel() error
}

// StatementExecuteSchema is a Statement that also supports ExecuteSchema.
//
// Since ADBC API revision 1.1.0.
type StatementExecuteSchema interface {
	// ExecuteSchema gets the schema of the result set of a query without executing it.
	ExecuteSchema(context.Context) (*arrow.Schema, error)
}

// GetSetOptions is a PostInitOptions that also supports getting and setting property values of different types.
//
// Since ADBC API revision 1.1.0.
type GetSetOptions interface {
	PostInitOptions

	SetOption(key, value string) error
	SetOptionInt(key, value int64) error
	SetOptionDouble(key, value float64) error
	GetOptionInt(key string) (int64, error)
	GetOptionDouble(key string) (float64, error)
}
