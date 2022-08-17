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

/// \file ADBC: Arrow Database connectivity
///
/// An Arrow-based interface between applications and database
/// drivers.  ADBC aims to provide a vendor-independent API for SQL
/// and Substrait-based database access that is targeted at
/// analytics/OLAP use cases.
///
/// This API is intended to be implemented directly by drivers and
/// used directly by client applications.  To assist portability
/// between different vendors, a "driver manager" library is also
/// provided, which implements this same API, but dynamically loads
/// drivers internally and forwards calls appropriately.
///
/// EXPERIMENTAL. Interface subject to change.

#pragma once

#include <stddef.h>
#include <stdint.h>

/// \defgroup Arrow C Data Interface
/// Definitions for the C Data Interface/C Stream Interface.
///
/// See https://arrow.apache.org/docs/format/CDataInterface.html
///
/// @{

//! @cond Doxygen_Suppress

#ifdef __cplusplus
extern "C" {
#endif

// Extra guard for versions of Arrow without the canonical guard
#ifndef ARROW_FLAG_DICTIONARY_ORDERED

#ifndef ARROW_C_DATA_INTERFACE
#define ARROW_C_DATA_INTERFACE

#define ARROW_FLAG_DICTIONARY_ORDERED 1
#define ARROW_FLAG_NULLABLE 2
#define ARROW_FLAG_MAP_KEYS_SORTED 4

struct ArrowSchema {
  // Array type description
  const char* format;
  const char* name;
  const char* metadata;
  int64_t flags;
  int64_t n_children;
  struct ArrowSchema** children;
  struct ArrowSchema* dictionary;

  // Release callback
  void (*release)(struct ArrowSchema*);
  // Opaque producer-specific data
  void* private_data;
};

struct ArrowArray {
  // Array data description
  int64_t length;
  int64_t null_count;
  int64_t offset;
  int64_t n_buffers;
  int64_t n_children;
  const void** buffers;
  struct ArrowArray** children;
  struct ArrowArray* dictionary;

  // Release callback
  void (*release)(struct ArrowArray*);
  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_DATA_INTERFACE

#ifndef ARROW_C_STREAM_INTERFACE
#define ARROW_C_STREAM_INTERFACE

struct ArrowArrayStream {
  // Callback to get the stream type
  // (will be the same for all arrays in the stream).
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowSchema must be released independently from the stream.
  int (*get_schema)(struct ArrowArrayStream*, struct ArrowSchema* out);

  // Callback to get the next array
  // (if no error and the array is released, the stream has ended)
  //
  // Return value: 0 if successful, an `errno`-compatible error code otherwise.
  //
  // If successful, the ArrowArray must be released independently from the stream.
  int (*get_next)(struct ArrowArrayStream*, struct ArrowArray* out);

  // Callback to get optional detailed error information.
  // This must only be called if the last stream operation failed
  // with a non-0 return code.
  //
  // Return value: pointer to a null-terminated character array describing
  // the last error, or NULL if no description is available.
  //
  // The returned pointer is only valid until the next operation on this stream
  // (including release).
  const char* (*get_last_error)(struct ArrowArrayStream*);

  // Release callback: release the stream's own resources.
  // Note that arrays returned by `get_next` must be individually released.
  void (*release)(struct ArrowArrayStream*);

  // Opaque producer-specific data
  void* private_data;
};

#endif  // ARROW_C_STREAM_INTERFACE
#endif  // ARROW_FLAG_DICTIONARY_ORDERED

//! @endcond

/// @}

#ifndef ADBC
#define ADBC

// Storage class macros for Windows
// Allow overriding/aliasing with application-defined macros
#if !defined(ADBC_EXPORT)
#if defined(_WIN32)
#if defined(ADBC_EXPORTING)
#define ADBC_EXPORT __declspec(dllexport)
#else
#define ADBC_EXPORT __declspec(dllimport)
#endif  // defined(ADBC_EXPORTING)
#else
#define ADBC_EXPORT
#endif  // defined(_WIN32)
#endif  // !defined(ADBC_EXPORT)

/// \page object-model Object Model
///
/// ADBC uses structs with free functions that operate on those
/// structs to model objects.
///
/// In general, objects allow serialized access from multiple threads,
/// but not concurrent access.  Specific implementations may permit
/// multiple threads.

// Forward declarations
struct AdbcDriver;
struct AdbcStatement;

/// \defgroup adbc-error-handling Error Handling
/// ADBC uses integer error codes to signal errors. To provide more
/// detail about errors, functions may also return an AdbcError via an
/// optional out parameter, which can be inspected. If provided, it is
/// the responsibility of the caller to zero-initialize the AdbcError
/// value.
///
/// @{

/// \brief Error codes for operations that may fail.
typedef uint8_t AdbcStatusCode;

/// \brief No error.
#define ADBC_STATUS_OK 0
/// \brief An unknown error occurred.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_UNKNOWN 1
/// \brief The operation is not implemented or supported.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_NOT_IMPLEMENTED 2
/// \brief A requested resource was not found.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_NOT_FOUND 3
/// \brief A requested resource already exists.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_ALREADY_EXISTS 4
/// \brief The arguments are invalid, likely a programming error.
///
/// For instance, they may be of the wrong format, or out of range.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_INVALID_ARGUMENT 5
/// \brief The preconditions for the operation are not met, likely a
///   programming error.
///
/// For instance, the object may be uninitialized, or may have not
/// been fully configured.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_INVALID_STATE 6
/// \brief Invalid data was processed (not a programming error).
///
/// For instance, a division by zero may have occurred during query
/// execution.
///
/// May indicate a database-side error only.
#define ADBC_STATUS_INVALID_DATA 7
/// \brief The database's integrity was affected.
///
/// For instance, a foreign key check may have failed, or a uniqueness
/// constraint may have been violated.
///
/// May indicate a database-side error only.
#define ADBC_STATUS_INTEGRITY 8
/// \brief An error internal to the driver or database occurred.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_INTERNAL 9
/// \brief An I/O error occurred.
///
/// For instance, a remote service may be unavailable.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_IO 10
/// \brief The operation was cancelled, not due to a timeout.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_CANCELLED 11
/// \brief The operation was cancelled due to a timeout.
///
/// May indicate a driver-side or database-side error.
#define ADBC_STATUS_TIMEOUT 12
/// \brief Authentication failed.
///
/// May indicate a database-side error only.
#define ADBC_STATUS_UNAUTHENTICATED 13
/// \brief The client is not authorized to perform the given operation.
///
/// May indicate a database-side error only.
#define ADBC_STATUS_UNAUTHORIZED 14

/// \brief A detailed error message for an operation.
struct ADBC_EXPORT AdbcError {
  /// \brief The error message.
  char* message;

  /// \brief A vendor-specific error code, if applicable.
  int32_t vendor_code;

  /// \brief A SQLSTATE error code, if provided, as defined by the
  ///   SQL:2003 standard.  If not set, it should be set to
  ///   "\0\0\0\0\0".
  char sqlstate[5];

  /// \brief Release the contained error.
  ///
  /// Unlike other structures, this is an embedded callback to make it
  /// easier for the driver manager and driver to cooperate.
  void (*release)(struct AdbcError* error);
};

/// @}

/// \brief Canonical option value for enabling an option.
#define ADBC_OPTION_VALUE_ENABLED "true"
/// \brief Canonical option value for disabling an option.
#define ADBC_OPTION_VALUE_DISABLED "false"

/// \defgroup adbc-database Database Initialization
/// Clients first initialize a database, then create a connection
/// (below).  This gives the implementation a place to initialize and
/// own any common connection state.  For example, in-memory databases
/// can place ownership of the actual database in this object.
/// @{

/// \brief An instance of a database.
///
/// Must be kept alive as long as any connections exist.
struct ADBC_EXPORT AdbcDatabase {
  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;
  /// \brief The associated driver (used by the driver manager to help
  ///   track state).
  struct AdbcDriver* private_driver;
};

/// \brief Allocate a new (but uninitialized) database.
ADBC_EXPORT
AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error);

/// \brief Set a char* option.
///
/// Options may be set before AdbcDatabaseInit.  Some drivers may
/// support setting options after initialization as well.
///
/// \return ADBC_STATUS_NOT_IMPLEMENTED if the option is not recognized
ADBC_EXPORT
AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error);

/// \brief Finish setting options and initialize the database.
///
/// Some drivers may support setting options after initialization
/// as well.
ADBC_EXPORT
AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error);

/// \brief Destroy this database. No connections may exist.
/// \param[in] database The database to release.
/// \param[out] error An optional location to return an error
///   message if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error);

/// @}

/// \defgroup adbc-connection Connection Establishment
/// Functions for creating, using, and releasing database connections.
/// @{

/// \brief An active database connection.
///
/// Provides methods for query execution, managing prepared
/// statements, using transactions, and so on.
///
/// Connections are not required to be thread-safe, but they can be
/// used from multiple threads so long as clients take care to
/// serialize accesses to a connection.
struct ADBC_EXPORT AdbcConnection {
  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;
  /// \brief The associated driver (used by the driver manager to help
  ///   track state).
  struct AdbcDriver* private_driver;
};

/// \brief Allocate a new (but uninitialized) connection.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error);

/// \brief Set a char* option.
///
/// Options may be set before AdbcConnectionInit.  Some drivers may
/// support setting options after initialization as well.
///
/// \return ADBC_STATUS_NOT_IMPLEMENTED if the option is not recognized
ADBC_EXPORT
AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error);

/// \brief Finish setting options and initialize the connection.
///
/// Some drivers may support setting options after initialization
/// as well.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database, struct AdbcError* error);

/// \brief Destroy this connection.
///
/// \param[in] connection The connection to release.
/// \param[out] error An optional location to return an error
///   message if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error);

/// \defgroup adbc-connection-metadata Metadata
/// Functions for retrieving metadata about the database.
///
/// Generally, these functions return an ArrowArrayStream that can be
/// consumed to get the metadata as Arrow data.  The returned metadata
/// has an expected schema given in the function docstring. Schema
/// fields are nullable unless otherwise marked.  While no
/// AdbcStatement is used in these functions, the result set may count
/// as an active statement to the driver for the purposes of
/// concurrency management (e.g. if the driver has a limit on
/// concurrent active statements and it must execute a SQL query
/// internally in order to implement the metadata function).
///
/// Some functions accept "search pattern" arguments, which are
/// strings that can contain the special character "%" to match zero
/// or more characters, or "_" to match exactly one character.  (See
/// the documentation of DatabaseMetaData in JDBC or "Pattern Value
/// Arguments" in the ODBC documentation.)  Escaping is not currently
/// supported.
///
/// @{

/// \brief Get metadata about the database/driver.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name                  | Field Type
/// ----------------------------|------------------------
/// info_name                   | uint32 not null
/// info_value                  | INFO_SCHEMA
///
/// INFO_SCHEMA is a dense union with members:
///
/// Field Name (Type Code)      | Field Type
/// ----------------------------|------------------------
/// string_value (0)            | utf8
/// bool_value (1)              | bool
/// int64_value (2)             | int64
/// int32_bitmask (3)           | int32
/// string_list (4)             | list<utf8>
/// int32_to_int32_list_map (5) | map<int32, list<int32>>
///
/// Each metadatum is identified by an integer code.  The recognized
/// codes are defined as constants.  Codes [0, 10_000) are reserved
/// for ADBC usage.  Drivers/vendors will ignore requests for
/// unrecognized codes (the row will be omitted from the result).
///
/// \param[in] connection The connection to query.
/// \param[in] info_codes A list of metadata codes to fetch, or NULL
///   to fetch all.
/// \param[in] info_codes_length The length of the info_codes
///   parameter.  Ignored if info_codes is NULL.
/// \param[out] out The result set.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection* connection,
                                     uint32_t* info_codes, size_t info_codes_length,
                                     struct ArrowArrayStream* out,
                                     struct AdbcError* error);

/// \brief The database vendor/product name (e.g. the server name).
///   (type: utf8).
#define ADBC_INFO_VENDOR_NAME 0
/// \brief The database vendor/product version (type: utf8).
#define ADBC_INFO_VENDOR_VERSION 1
/// \brief The database vendor/product Arrow library version (type:
///   utf8).
#define ADBC_INFO_VENDOR_ARROW_VERSION 2

/// \brief The driver name (type: utf8).
#define ADBC_INFO_DRIVER_NAME 100
/// \brief The driver version (type: utf8).
#define ADBC_INFO_DRIVER_VERSION 101
/// \brief The driver Arrow library version (type: utf8).
#define ADBC_INFO_DRIVER_ARROW_VERSION 102

/// \brief Get a hierarchical view of all catalogs, database schemas,
///   tables, and columns.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name               | Field Type
/// -------------------------|-----------------------
/// catalog_name             | utf8
/// catalog_db_schemas       | list<DB_SCHEMA_SCHEMA>
///
/// DB_SCHEMA_SCHEMA is a Struct with fields:
///
/// Field Name               | Field Type
/// -------------------------|-----------------------
/// db_schema_name           | utf8
/// db_schema_tables         | list<TABLE_SCHEMA>
///
/// TABLE_SCHEMA is a Struct with fields:
///
/// Field Name               | Field Type
/// -------------------------|-----------------------
/// table_name               | utf8 not null
/// table_type               | utf8 not null
/// table_columns            | list<COLUMN_SCHEMA>
/// table_constraints        | list<CONSTRAINT_SCHEMA>
///
/// COLUMN_SCHEMA is a Struct with fields:
///
/// Field Name               | Field Type            | Comments
/// -------------------------|-----------------------|---------
/// column_name              | utf8 not null         |
/// ordinal_position         | int32                 | (1)
/// remarks                  | utf8                  | (2)
/// xdbc_data_type           | int16                 | (3)
/// xdbc_type_name           | utf8                  | (3)
/// xdbc_column_size         | int32                 | (3)
/// xdbc_decimal_digits      | int16                 | (3)
/// xdbc_num_prec_radix      | int16                 | (3)
/// xdbc_nullable            | int16                 | (3)
/// xdbc_column_def          | utf8                  | (3)
/// xdbc_sql_data_type       | int16                 | (3)
/// xdbc_datetime_sub        | int16                 | (3)
/// xdbc_char_octet_length   | int32                 | (3)
/// xdbc_is_nullable         | utf8                  | (3)
/// xdbc_scope_catalog       | utf8                  | (3)
/// xdbc_scope_schema        | utf8                  | (3)
/// xdbc_scope_table         | utf8                  | (3)
/// xdbc_is_autoincrement    | bool                  | (3)
/// xdbc_is_generatedcolumn  | bool                  | (3)
///
/// 1. The column's ordinal position in the table (starting from 1).
/// 2. Database-specific description of the column.
/// 3. Optional value.  Should be null if not supported by the driver.
///    xdbc_ values are meant to provide JDBC/ODBC-compatible metadata
///    in an agnostic manner.
///
/// CONSTRAINT_SCHEMA is a Struct with fields:
///
/// Field Name               | Field Type            | Comments
/// -------------------------|-----------------------|---------
/// constraint_name          | utf8                  |
/// constraint_type          | utf8 not null         | (1)
/// constraint_column_names  | list<utf8> not null   | (2)
/// constraint_column_usage  | list<USAGE_SCHEMA>    | (3)
///
/// 1. One of 'CHECK', 'FOREIGN KEY', 'PRIMARY KEY', or 'UNIQUE'.
/// 2. The columns on the current table that are constrained, in
///    order.
/// 3. For FOREIGN KEY only, the referenced table and columns.
///
/// USAGE_SCHEMA is a Struct with fields:
///
/// Field Name               | Field Type            | Comments
/// -------------------------|-----------------------|---------
/// fk_catalog               | utf8                  |
/// fk_db_schema             | utf8                  |
/// fk_table                 | utf8 not null         |
/// fk_column_name           | utf8 not null         |
///
/// \param[in] connection The database connection.
/// \param[in] depth The level of nesting to display. If 0, display
///   all levels. If 1, display only catalogs (i.e.  catalog_schemas
///   will be null). If 2, display only catalogs and schemas
///   (i.e. db_schema_tables will be null), and so on.
/// \param[in] catalog Only show tables in the given catalog. If NULL,
///   do not filter by catalog. If an empty string, only show tables
///   without a catalog.  May be a search pattern (see section
///   documentation).
/// \param[in] db_schema Only show tables in the given database schema. If
///   NULL, do not filter by database schema. If an empty string, only show
///   tables without a database schema. May be a search pattern (see section
///   documentation).
/// \param[in] table_name Only show tables with the given name. If NULL, do not
///   filter by name. May be a search pattern (see section documentation).
/// \param[in] table_type Only show tables matching one of the given table
///   types. If NULL, show tables of any type. Valid table types can be fetched
///   from GetTableTypes.  Terminate the list with a NULL entry.
/// \param[in] column_name Only show columns with the given name. If
///   NULL, do not filter by name.  May be a search pattern (see
///   section documentation).
/// \param[out] out The result set.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_type,
                                        const char* column_name,
                                        struct ArrowArrayStream* out,
                                        struct AdbcError* error);

#define ADBC_OBJECT_DEPTH_ALL 0
#define ADBC_OBJECT_DEPTH_CATALOGS 1
#define ADBC_OBJECT_DEPTH_DB_SCHEMAS 2
#define ADBC_OBJECT_DEPTH_TABLES 3
#define ADBC_OBJECT_DEPTH_COLUMNS ADBC_OBJECT_DEPTH_ALL

/// \brief Get the Arrow schema of a table.
///
/// \param[in] connection The database connection.
/// \param[in] catalog The catalog (or nullptr if not applicable).
/// \param[in] db_schema The database schema (or nullptr if not applicable).
/// \param[in] table_name The table name.
/// \param[out] schema The table schema.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name,
                                            struct ArrowSchema* schema,
                                            struct AdbcError* error);

/// \brief Get a list of table types in the database.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name     | Field Type
/// ---------------|--------------
/// table_type     | utf8 not null
///
/// \param[in] connection The database connection.
/// \param[out] out The result set.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error);

/// @}

/// \defgroup adbc-connection-partition Partitioned Results
/// Some databases may internally partition the results. These
/// partitions are exposed to clients who may wish to integrate them
/// with a threaded or distributed execution model, where partitions
/// can be divided among threads or machines for processing.
///
/// Drivers are not required to support partitioning.
///
/// Partitions are not ordered. If the result set is sorted,
/// implementations should return a single partition.
///
/// @{

/// \brief Construct a statement for a partition of a query. The
///   statement can then be read independently.
///
/// A partition can be retrieved from AdbcStatementGetPartitionDesc.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionDeserializePartitionDesc(struct AdbcConnection* connection,
                                                      const uint8_t* serialized_partition,
                                                      size_t serialized_length,
                                                      struct AdbcStatement* statement,
                                                      struct AdbcError* error);

/// @}

/// \defgroup adbc-connection-transaction Transaction Semantics
///
/// Connections start out in auto-commit mode by default (if
/// applicable for the given vendor). Use AdbcConnectionSetOption and
/// ADBC_CONNECTION_OPTION_AUTO_COMMIT to change this.
///
/// @{

/// \brief The name of the canonical option for whether autocommit is
///   enabled.
#define ADBC_CONNECTION_OPTION_AUTOCOMMIT "adbc.connection.autocommit"

/// \brief Commit any pending transactions. Only used if autocommit is
///   disabled.
///
/// Behavior is undefined if this is mixed with SQL transaction
/// statements.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error);

/// \brief Roll back any pending transactions. Only used if autocommit
///   is disabled.
///
/// Behavior is undefined if this is mixed with SQL transaction
/// statements.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection* connection,
                                      struct AdbcError* error);

/// @}

/// @}

/// \defgroup adbc-statement Managing Statements
/// Applications should first initialize a statement with
/// AdbcStatementNew. Then, the statement should be configured with
/// functions like AdbcStatementSetSqlQuery and
/// AdbcStatementSetOption. Finally, the statement can be executed
/// with AdbcStatementExecute (or call AdbcStatementPrepare first to
/// turn it into a prepared statement instead).
/// @{

/// \brief A container for all state needed to execute a database
/// query, such as the query itself, parameters for prepared
/// statements, driver parameters, etc.
///
/// Statements may represent queries or prepared statements.
///
/// Statements may be used multiple times and can be reconfigured
/// (e.g. they can be reused to execute multiple different queries).
/// However, executing a statement (and changing certain other state)
/// will invalidate result sets obtained prior to that execution.
///
/// Multiple statements may be created from a single connection.
/// However, the driver may block or error if they are used
/// concurrently (whether from a single thread or multiple threads).
///
/// Statements are not required to be thread-safe, but they can be
/// used from multiple threads so long as clients take care to
/// serialize accesses to a statement.
struct ADBC_EXPORT AdbcStatement {
  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;

  /// \brief The associated driver (used by the driver manager to help
  ///   track state).
  struct AdbcDriver* private_driver;
};

/// \brief Create a new statement for a given connection.
///
/// Set options on the statement, then call AdbcStatementExecute or
/// AdbcStatementPrepare.
ADBC_EXPORT
AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement, struct AdbcError* error);

/// \brief Destroy a statement.
/// \param[in] statement The statement to release.
/// \param[out] error An optional location to return an error
///   message if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error);

/// \brief Execute a statement and get the results.
///
/// This invalidates any prior result sets.
///
/// \param[in] statement The statement to execute.
/// \param[in] out_type The expected result type:
///   - ADBC_OUTPUT_TYPE_ARROW for an ArrowArrayStream;
///   - ADBC_OUTPUT_TYPE_PARTITIONS for a count of partitions (see \ref
///     adbc-statement-partition below).
///   - ADBC_OUTPUT_TYPE_UPDATE if the query should not generate a
///     result set;
///   The result set will be in out.
/// \param[out] out The results. Must be NULL for output type UPDATE, a
///   pointer to an ArrowArrayStream for ARROW_ARRAY_STREAM, or a
///   pointer to a size_t for PARTITIONS.
/// \param[out] rows_affected The number of rows affected if known,
///   else -1. Pass NULL if the client does not want this information.
/// \param[out] error An optional location to return an error
///   message if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementExecute(struct AdbcStatement* statement, int output_type,
                                    void* out, int64_t* rows_affected,
                                    struct AdbcError* error);

/// \brief Arrow data is expected from AdbcStatementExecute.  Pass
///   ArrowArrayStream* to out.
#define ADBC_OUTPUT_TYPE_ARROW 0
/// \brief Partitions are expected from AdbcStatementExecute.  Pass
///   size_t* to out to get the number of partitions, and use
///   AdbcStatementGetPartitionDesc to get a partition.
///
/// Drivers are not required to support partitioning.  In that case,
/// AdbcStatementExecute will return ADBC_STATUS_NOT_IMPLEMENTED.
#define ADBC_OUTPUT_TYPE_PARTITIONS 1
/// \brief No results are expected from AdbcStatementExecute.  Pass
///   NULL to out.
#define ADBC_OUTPUT_TYPE_UPDATE 2

/// \brief Turn this statement into a prepared statement to be
///   executed multiple times.
///
/// This invalidates any prior result sets.
ADBC_EXPORT
AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error);

/// \defgroup adbc-statement-sql SQL Semantics
/// Functions for executing SQL queries, or querying SQL-related
/// metadata. Drivers are not required to support both SQL and
/// Substrait semantics. If they do, it may be via converting
/// between representations internally.
/// @{

/// \brief Set the SQL query to execute.
///
/// The query can then be executed with AdbcStatementExecute.  For
/// queries expected to be executed repeatedly, AdbcStatementPrepare
/// the statement first.
///
/// \param[in] statement The statement.
/// \param[in] query The query to execute.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error);

/// @}

/// \defgroup adbc-statement-substrait Substrait Semantics
/// Functions for executing Substrait plans, or querying
/// Substrait-related metadata.  Drivers are not required to support
/// both SQL and Substrait semantics.  If they do, it may be via
/// converting between representations internally.
/// @{

/// \brief Set the Substrait plan to execute.
///
/// The query can then be executed with AdbcStatementExecute.  For
/// queries expected to be executed repeatedly, AdbcStatementPrepare
/// the statement first.
///
/// \param[in] statement The statement.
/// \param[in] plan The serialized substrait.Plan to execute.
/// \param[in] length The length of the serialized plan.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcStatementSetSubstraitPlan(struct AdbcStatement* statement,
                                             const uint8_t* plan, size_t length,
                                             struct AdbcError* error);

/// @}

/// \brief Bind Arrow data. This can be used for bulk inserts or
///   prepared statements.
///
/// \param[in] statement The statement to bind to.
/// \param[in] values The values to bind. The driver will call the
///   release callback itself, although it may not do this until the
///   statement is released.
/// \param[in] schema The schema of the values to bind.
/// \param[out] error An optional location to return an error message
///   if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error);

/// \brief Bind Arrow data. This can be used for bulk inserts or
///   prepared statements.
/// \param[in] statement The statement to bind to.
/// \param[in] stream The values to bind. The driver will call the
///   release callback itself, although it may not do this until the
///   statement is released.
/// \param[out] error An optional location to return an error message
///   if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement* statement,
                                       struct ArrowArrayStream* stream,
                                       struct AdbcError* error);

/// \brief Get the schema for bound parameters.
///
/// This retrieves an Arrow schema describing the number, names, and
/// types of the parameters in a parameterized statement.  The fields
/// of the schema should be in order of the ordinal position of the
/// parameters; named parameters should appear only once.
///
/// If the parameter does not have a name, or the name cannot be
/// determined, the name of the corresponding field in the schema will
/// be an empty string.  If the type cannot be determined, the type of
/// the corresponding field will be NA (NullType).
///
/// This should be called after AdbcStatementPrepare.
///
/// \return ADBC_STATUS_NOT_IMPLEMENTED if the schema cannot be determined.
ADBC_EXPORT
AdbcStatusCode AdbcStatementGetParameterSchema(struct AdbcStatement* statement,
                                               struct ArrowSchema* schema,
                                               struct AdbcError* error);

/// \brief Set a string option on a statement.
ADBC_EXPORT
AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error);

/// \defgroup adbc-statement-ingestion Bulk Data Ingestion
/// While it is possible to insert data via prepared statements, it can
/// be more efficient to explicitly perform a bulk insert.  For
/// compatible drivers, this can be accomplished by setting up and
/// executing a statement.  Instead of setting a SQL query or Substrait
/// plan, bind the source data via AdbcStatementBind, and set the name
/// of the table to be created via AdbcStatementSetOption and the
/// options below.  Then, call AdbcStatementExecute with
/// ADBC_OUTPUT_TYPE_UPDATE.
///
/// @{

/// \brief The name of the target table for a bulk insert.
///
/// The driver should attempt to create the table if it does not
/// exist.  If the table exists but has a different schema,
/// ADBC_STATUS_ALREADY_EXISTS should be raised.  Else, data should be
/// appended to the target table.
#define ADBC_INGEST_OPTION_TARGET_TABLE "adbc.ingest.target_table"
/// \brief Whether to create (the default) or append.
#define ADBC_INGEST_OPTION_MODE "adbc.ingest.mode"
/// \brief Create the table and insert data; error if the table exists.
#define ADBC_INGEST_OPTION_MODE_CREATE "adbc.ingest.mode.create"
/// \brief Do not create the table, and insert data; error if the
///   table does not exist (ADBC_STATUS_NOT_FOUND) or does not match
///   the schema of the data to append (ADBC_STATUS_ALREADY_EXISTS).
#define ADBC_INGEST_OPTION_MODE_APPEND "adbc.ingest.mode.append"

/// @}

/// \defgroup adbc-statement-partition Partitioned Results
/// Some backends may internally partition the results. These
/// partitions are exposed to clients who may wish to integrate them
/// with a threaded or distributed execution model, where partitions
/// can be divided among threads or machines and fetched in parallel.
///
/// To use partitioning, pass ADBC_OUTPUT_TYPE_PARTITIONS to
/// AdbcStatementExecute.  Then, use these functions to get the actual
/// partition descriptors.  Call AdbcConnectionDeserializePartitionDesc
/// to turn the individual descriptors into AdbcStatement instances.
///
/// Drivers are not required to support partitioning.
///
/// @{

/// \brief Get the length of a serialized descriptor for a partition.
///
/// \param[in] statement The statement.
/// \param[in] index Which partition to get.
/// \param[out] length The length of the serialized partition.
/// \param[out] error An optional location to return an error message if
///   necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                 size_t index, size_t* length,
                                                 struct AdbcError* error);

/// \brief Get the serialized descriptor for a partition.
///
/// A partition can be turned back into a statement via
/// AdbcConnectionDeserializePartitionDesc. Effectively, this means
/// AdbcStatement is similar to arrow::flight::FlightInfo in
/// Flight/Flight SQL and AdbcStatementGetPartitionDesc is similar to
/// getting the arrow::flight::Ticket.
///
/// \param[in] statement The statement.
/// \param[in] index Which partition to get.
/// \param[out] partition_desc A caller-allocated buffer, to which the
///   serialized partition will be written. The length to allocate can be
///   queried with AdbcStatementGetPartitionDescSize.
/// \param[out] error An optional location to return an error message if
///   necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementGetPartitionDesc(struct AdbcStatement* statement,
                                             size_t index, uint8_t* partition_desc,
                                             struct AdbcError* error);

/// @}

/// @}

/// \defgroup adbc-driver Driver Initialization
///
/// These functions are intended to help support integration between a
/// driver and the driver manager.
/// @{

/// \brief An instance of an initialized database driver.
///
/// This provides a common interface for vendor-specific driver
/// initialization routines. Drivers should populate this struct, and
/// applications can call ADBC functions through this struct, without
/// worrying about multiple definitions of the same symbol.
struct ADBC_EXPORT AdbcDriver {
  /// \brief Opaque driver-defined state.
  /// This field is NULLPTR if the driver is unintialized/freed (but
  /// it need not have a value even if the driver is initialized).
  void* private_data;
  /// \brief Opaque driver manager-defined state.
  /// This field is NULLPTR if the driver is unintialized/freed (but
  /// it need not have a value even if the driver is initialized).
  void* private_manager;

  /// \brief Release the driver and perform any cleanup.
  ///
  /// Unlike other structures, this is an embedded callback to make it
  /// easier for the driver manager and driver to cooperate.
  AdbcStatusCode (*release)(struct AdbcDriver* driver, struct AdbcError* error);

  AdbcStatusCode (*DatabaseNew)(struct AdbcDatabase*, struct AdbcError*);
  AdbcStatusCode (*DatabaseSetOption)(struct AdbcDatabase*, const char*, const char*,
                                      struct AdbcError*);
  AdbcStatusCode (*DatabaseInit)(struct AdbcDatabase*, struct AdbcError*);
  AdbcStatusCode (*DatabaseRelease)(struct AdbcDatabase*, struct AdbcError*);

  AdbcStatusCode (*ConnectionNew)(struct AdbcConnection*, struct AdbcError*);
  AdbcStatusCode (*ConnectionSetOption)(struct AdbcConnection*, const char*, const char*,
                                        struct AdbcError*);
  AdbcStatusCode (*ConnectionInit)(struct AdbcConnection*, struct AdbcDatabase*,
                                   struct AdbcError*);
  AdbcStatusCode (*ConnectionRelease)(struct AdbcConnection*, struct AdbcError*);

  AdbcStatusCode (*ConnectionCommit)(struct AdbcConnection*, struct AdbcError*);
  AdbcStatusCode (*ConnectionDeserializePartitionDesc)(struct AdbcConnection*,
                                                       const uint8_t*, size_t,
                                                       struct AdbcStatement*,
                                                       struct AdbcError*);
  AdbcStatusCode (*ConnectionRollback)(struct AdbcConnection*, struct AdbcError*);
  AdbcStatusCode (*ConnectionGetInfo)(struct AdbcConnection*, uint32_t*, size_t,
                                      struct ArrowArrayStream*, struct AdbcError*);
  AdbcStatusCode (*ConnectionGetObjects)(struct AdbcConnection*, int, const char*,
                                         const char*, const char*, const char**,
                                         const char*, struct ArrowArrayStream*,
                                         struct AdbcError*);
  AdbcStatusCode (*ConnectionGetTableSchema)(struct AdbcConnection*, const char*,
                                             const char*, const char*,
                                             struct ArrowSchema*, struct AdbcError*);
  AdbcStatusCode (*ConnectionGetTableTypes)(struct AdbcConnection*,
                                            struct ArrowArrayStream*, struct AdbcError*);

  AdbcStatusCode (*StatementNew)(struct AdbcConnection*, struct AdbcStatement*,
                                 struct AdbcError*);
  AdbcStatusCode (*StatementRelease)(struct AdbcStatement*, struct AdbcError*);
  AdbcStatusCode (*StatementBind)(struct AdbcStatement*, struct ArrowArray*,
                                  struct ArrowSchema*, struct AdbcError*);
  AdbcStatusCode (*StatementBindStream)(struct AdbcStatement*, struct ArrowArrayStream*,
                                        struct AdbcError*);
  AdbcStatusCode (*StatementExecute)(struct AdbcStatement*, int, void*, int64_t*,
                                     struct AdbcError*);
  AdbcStatusCode (*StatementPrepare)(struct AdbcStatement*, struct AdbcError*);
  AdbcStatusCode (*StatementGetParameterSchema)(struct AdbcStatement*,
                                                struct ArrowSchema*, struct AdbcError*);
  AdbcStatusCode (*StatementGetPartitionDescSize)(struct AdbcStatement*, size_t, size_t*,
                                                  struct AdbcError*);
  AdbcStatusCode (*StatementGetPartitionDesc)(struct AdbcStatement*, size_t, uint8_t*,
                                              struct AdbcError*);
  AdbcStatusCode (*StatementSetOption)(struct AdbcStatement*, const char*, const char*,
                                       struct AdbcError*);
  AdbcStatusCode (*StatementSetSqlQuery)(struct AdbcStatement*, const char*,
                                         struct AdbcError*);
  AdbcStatusCode (*StatementSetSubstraitPlan)(struct AdbcStatement*, const uint8_t*,
                                              size_t, struct AdbcError*);
  // Do not edit fields. New fields can only be appended to the end.
};

/// \brief Common entry point for drivers via the driver manager
///   (which uses dlopen(3)/LoadLibrary). The driver manager is told
///   to load a library and call a function of this type to load the
///   driver.
///
/// \param[in] count The number of entries to initialize. Provides
///   backwards compatibility if the struct definition is changed.
/// \param[out] driver The table of function pointers to initialize.
/// \param[out] initialized How much of the table was actually
///   initialized (can be less than count).
/// \param[out] error An optional location to return an error message
///   if necessary.
typedef AdbcStatusCode (*AdbcDriverInitFunc)(size_t count, struct AdbcDriver* driver,
                                             size_t* initialized,
                                             struct AdbcError* error);
// TODO: use sizeof() instead of count, or version the
// struct/entrypoint instead?

// For use with count
#define ADBC_VERSION_0_0_1 27

/// @}

#endif  // ADBC

#ifdef __cplusplus
}
#endif

/// \page typical-usage Typical Usage Patterns
/// (TODO: describe request sequences)

/// \page decoder-ring Comparison with Other APIs
///
/// <table>
///   <caption>Equivalent concepts between ADBC and other APIs</caption>
///   <tr>
///     <th>Concept/API              </th>
///     <th>ADBC                     </th>
///     <th>database/sql (Golang)    </th>
///     <th>DBAPI 2.0 (PEP 249)      </th>
///     <th>Flight SQL               </th>
///     <th>JDBC                     </th>
///     <th>ODBC                     </th>
///   </tr>
///   <tr>
///     <td>Shared connection state  </td>
///     <td>AdbcDatabase             </td>
///     <td>DB                       </td>
///     <td>-                        </td>
///     <td>-                        </td>
///     <td>-                        </td>
///     <td>-                        </td>
///   </tr>
///   <tr>
///     <td>Database connection      </td>
///     <td>AdbcConnection           </td>
///     <td>Conn                     </td>
///     <td>Connection               </td>
///     <td>FlightSqlClient          </td>
///     <td>Connection               </td>
///     <td>SQLHANDLE (connection)   </td>
///   </tr>
///   <tr>
///     <td>Query state              </td>
///     <td>AdbcStatement            </td>
///     <td>-                        </td>
///     <td>Cursor                   </td>
///     <td>-                        </td>
///     <td>Statement                </td>
///     <td>SQLHANDLE (statement)    </td>
///   </tr>
///   <tr>
///     <td>Prepared statement handle</td>
///     <td>AdbcStatement            </td>
///     <td>Stmt                     </td>
///     <td>Cursor                   </td>
///     <td>PreparedStatement        </td>
///     <td>PreparedStatement        </td>
///     <td>SQLHANDLE (statement)    </td>
///   </tr>
///   <tr>
///     <td>Result set               </td>
///     <td>ArrowArrayStream         </td>
///     <td>*Rows                    </td>
///     <td>Cursor                   </td>
///     <td>FlightInfo               </td>
///     <td>ResultSet                </td>
///     <td>SQLHANDLE (statement)    </td>
///   </tr>
/// </table>

/// \page compatibility Backwards and Forwards Compatibility

/// \page concurrency Concurrency and Thread Safety
///
/// In general, objects allow serialized access from multiple threads:
/// one thread may make a call, and once finished, another thread may
/// make a call.  They do not allow concurrent access from multiple
/// threads.
///
/// Somewhat related is the question of overlapping/concurrent
/// execution of multi-step operations, from a single thread or
/// multiple threads.  For example, two AdbcStatement objects can be
/// created from the same AdbcConnection:
///
/// ```c
/// struct AdbcStatement stmt1;
/// struct AdbcStatement stmt2;
///
/// // Ignoring error handling for brevity
/// AdbcStatementNew(&conn, &stmt1, NULL);
/// AdbcStatementNew(&conn, &stmt2, NULL);
/// AdbcStatementSetSqlQuery(&stmt1, "SELECT * FROM a", NULL);
/// AdbcStatementSetSqlQuery(&stmt2, "SELECT * FROM b", NULL);
///
/// AdbcStatementExecute(&stmt1, NULL);
/// AdbcStatementExecute(&stmt2, NULL);
/// // What happens to the result set of stmt1?
/// ```
///
/// What happens if the client application calls
/// `AdbcStatementExecute` on `stmt1`, then on `stmt2`, without
/// reading the result set of `stmt1`?  Some existing client
/// libraries/protocols, like libpq, don't support concurrent
/// execution of queries from a single connection.  So the driver
/// would have to either 1) buffer all results into memory during the
/// first `Execute` 2) issue an error on the second `Execute`, or 3)
/// invalidate the first statement's result set on the second
/// `Execute`.
///
/// In this case, ADBC allows drivers to choose 1) or 2).  If possible
/// and reasonable, the driver should allow concurrent execution,
/// whether because the underlying protocol is designed for it or by
/// buffering result sets.  But the driver is allowed to error if it
/// is not possible to support it.
///
/// Another use case is having a single statement, but executing it
/// multiple times and reading the result sets concurrently.  A client
/// might desire to do this with a prepared statement, for instance:
///
/// ```c
/// // Ignoring error handling for brevity
/// struct AdbcStatement stmt;
/// AdbcStatementNew(&conn, &stmt, NULL);
/// AdbcStatementSetSqlQuery(&stmt, "SELECT * FROM a WHERE foo > ?", NULL);
/// AdbcStatementPrepare(&stmt, NULL);
///
/// AdbcStatementBind(&stmt, &array1, &schema, NULL);
/// AdbcStatementExecute(&stmt, NULL);
/// AdbcStatementGetStream(&stmt, &stream, NULL);
/// // Spawn a thread to process `stream`
///
/// AdbcStatementBind(&stmt, &array2, &schema, NULL);
/// AdbcStatementExecute(&stmt, NULL);
/// // What happens to `stream` here?
/// ```
///
/// ADBC chooses to disallow this (specifically: the second call to
/// `Execute` must invalidate the result set of the first call),
/// because generally, existing APIs do not support 'overlapping'
/// usage of a single prepared statement in this way.
