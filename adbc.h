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

#pragma once

#include <stddef.h>
#include <stdint.h>

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

// EXPERIMENTAL: C stream interface

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

#ifndef ADBC
#define ADBC

// Storage class macros for Windows
#if defined(_WIN32)
#if defined(ADBC_EXPORTING)
#define ADBC_EXPORT __declspec(dllexport)
#else
#define ADBC_EXPORT __declspec(dllimport)
#endif  // defined(ADBC_EXPORTING)
#else
#define ADBC_EXPORT
#endif  // defined(_WIN32)

/// \file ADBC: Arrow DataBase connectivity (client API)
///
/// Implemented by libadbc.so (provided by Arrow/C++), which in turn
/// dynamically loads the appropriate database driver.
///
/// EXPERIMENTAL. Interface subject to change.

/// \page object-model Object Model
///
/// Except where noted, objects are not thread-safe and clients should
/// take care to serialize accesses to methods.

// Forward declarations
struct AdbcDriver;
struct AdbcStatement;

/// \defgroup adbc-error-handling Error handling primitives.
/// ADBC uses integer error codes to signal errors. To provide more
/// detail about errors, functions may also return an AdbcError via an
/// optional out parameter, which can be inspected. If provided, it is
/// the responsibility of the caller to zero-initialize the AdbcError
/// value.
///
/// @{

/// Error codes for operations that may fail.
typedef uint8_t AdbcStatusCode;

/// No error.
#define ADBC_STATUS_OK 0
/// An unknown error occurred.
#define ADBC_STATUS_UNKNOWN 1
/// The operation is not implemented.
#define ADBC_STATUS_NOT_IMPLEMENTED 2
/// An operation was attempted on an uninitialized object.
#define ADBC_STATUS_UNINITIALIZED 3
/// The arguments are invalid.
#define ADBC_STATUS_INVALID_ARGUMENT 4
/// The object is in an invalid state for the given operation.
#define ADBC_STATUS_INTERNAL 5
/// An I/O error occurred.
#define ADBC_STATUS_IO 6

/// \brief A detailed error message for an operation.
struct ADBC_EXPORT AdbcError {
  /// \brief The error message.
  char* message;

  /// \brief Release the contained error.
  ///
  /// Unlike other structures, this is an embedded callback to make it
  /// easier for the driver manager and driver to cooperate.
  void (*release)(struct AdbcError* error);
};

/// }@

/// \defgroup adbc-database Database initialization.
/// Clients first initialize a database, then connect to the database
/// (below). For client-server databases, one of these steps may be a
/// no-op; for in-memory or otherwise non-client-server databases,
/// this gives the implementation a place to initialize and own any
/// common connection state.
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
  AdbcDriver* private_driver;
};

/// \brief Allocate a new (but uninitialized) database.
ADBC_EXPORT
AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error);

/// \brief Set a char* option.
ADBC_EXPORT
AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error);

/// \brief Finish setting options and initialize the database.
///
/// Some backends may support setting options after initialization
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

/// }@

/// \defgroup adbc-connection Connection establishment.
/// @{

/// \brief A set of connection options.
struct ADBC_EXPORT AdbcConnectionOptions {
  /// \brief The database to connect to.
  struct AdbcDatabase* database;

  /// \brief A driver-specific connection string.
  ///
  /// Should be in ODBC-style format ("Key1=Value1;Key2=Value2").
  const char* target;
};

/// \brief An active database connection.
///
/// Provides methods for query execution, managing prepared
/// statements, using transactions, and so on.
///
/// Connections are not thread-safe and clients should take care to
/// serialize accesses to a connection.
struct ADBC_EXPORT AdbcConnection {
  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;
  /// \brief The associated driver (used by the driver manager to help
  ///   track state).
  AdbcDriver* private_driver;
};

/// \brief Allocate a new (but uninitialized) connection.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionNew(struct AdbcDatabase* database,
                                 struct AdbcConnection* connection,
                                 struct AdbcError* error);

ADBC_EXPORT
AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error);

/// \brief Finish setting options and initialize the connection.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcError* error);

/// \brief Destroy this connection.
/// \param[in] connection The connection to release.
/// \param[out] error An optional location to return an error
///   message if necessary.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error);

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

/// }@

/// \defgroup adbc-connection-metadata Metadata
/// Functions for retrieving metadata about the database.
///
/// Generally, these functions return an AdbcStatement that can be evaluated to
/// get the metadata as Arrow data. The returned metadata has an expected
/// schema given in the function docstring. Schema fields are nullable unless
/// otherwise marked.
///
/// Some functions accept a "search pattern" argument, which is a string that
/// can contain the special character "%" to match zero or more characters, or
/// "_" to match exactly one character. (See the documentation of
/// DatabaseMetaData in JDBC or "Pattern Value Arguments" in the ODBC
/// documentation.)
///
/// TODO: escaping in search patterns?
///
/// @{

/// \brief Get a list of catalogs in the database.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name     | Field Type
/// ---------------|--------------
/// catalog_name   | utf8 not null
///
/// \param[in] connection The database connection.
/// \param[out] statement The result set.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetCatalogs(struct AdbcConnection* connection,
                                         struct AdbcStatement* statement,
                                         struct AdbcError* error);

/// \brief Get a list of schemas in the database.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name     | Field Type
/// ---------------|--------------
/// catalog_name   | utf8
/// db_schema_name | utf8 not null
///
/// \param[in] connection The database connection.
/// \param[out] statement The result set.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetDbSchemas(struct AdbcConnection* connection,
                                          struct AdbcStatement* statement,
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
/// \param[out] statement The result set.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct AdbcStatement* statement,
                                           struct AdbcError* error);

/// \brief Get a list of tables matching the given criteria.
///
/// The result is an Arrow dataset with the following schema:
///
/// Field Name     | Field Type
/// ---------------|--------------
/// catalog_name   | utf8
/// db_schema_name | utf8
/// table_name     | utf8 not null
/// table_type     | utf8 not null
///
/// \param[in] connection The database connection.
/// \param[in] catalog Only show tables in the given catalog. If NULL, do not
///   filter by catalog. If an empty string, only show tables without a
///   catalog.
/// \param[in] db_schema Only show tables in the given database schema. If
///   NULL, do not filter by database schema. If an empty string, only show
///   tables without a database schema. May be a search pattern (see section
///   documentation).
/// \param[in] table_name Only show tables with the given name. If NULL, do not
///   filter by name. May be a search pattern (see section documentation).
/// \param[in] table_types Only show tables matching one of the given table
///   types. If NULL, show tables of any type. Valid table types can be fetched
///   from get_table_types.
/// \param[out] statement The result set.
/// \param[out] error Error details, if an error occurs.
ADBC_EXPORT
AdbcStatusCode AdbcConnectionGetTables(struct AdbcConnection* connection,
                                       const char* catalog, const char* db_schema,
                                       const char* table_name, const char** table_types,
                                       struct AdbcStatement* statement,
                                       struct AdbcError* error);
/// }@

/// }@

/// \defgroup adbc-statement Managing statements.
/// Applications should first initialize a statement with
/// AdbcStatementNew. Then, the statement should be configured with
/// functions like AdbcStatementSetSqlQuery and
/// AdbcStatementSetOption. Finally, the statement can be executed
/// with AdbcStatementExecute (or call AdbcStatementPrepare first to
/// turn it into a prepared statement instead).
/// @{

/// \brief An instance of a database query, from parameters set before
///   execution to the result of execution.
///
/// Statements are not thread-safe and clients should take care to
/// serialize access.
struct ADBC_EXPORT AdbcStatement {
  /// \brief Opaque implementation-defined state.
  /// This field is NULLPTR iff the connection is unintialized/freed.
  void* private_data;

  /// \brief The associated driver (used by the driver manager to help
  ///   track state).
  AdbcDriver* private_driver;
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

/// \brief Execute a statement.
ADBC_EXPORT
AdbcStatusCode AdbcStatementExecute(struct AdbcStatement* statement,
                                    struct AdbcError* error);

/// \brief Create a prepared statement to be executed multiple times.
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

/// }@

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

/// }@

/// \brief Bind Arrow data. This can be used for bulk inserts or
///   prepared statements.
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
                                       struct ArrowArrayStream* values,
                                       struct AdbcError* error);

/// \brief Read the result of a statement.
///
/// This method can be called only once per execution of the
/// statement. It may not be called if any of the partitioning methods
/// have been called (see below).
///
/// \return out A stream of Arrow data. The stream itself must be
///   released before the statement is released.
ADBC_EXPORT
AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement* statement,
                                      struct ArrowArrayStream* out,
                                      struct AdbcError* error);

/// \brief Set a string option on a statement.
ADBC_EXPORT
AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error);

/// \defgroup adbc-statement-ingestion Bulk Data Ingestion
/// While it is possible to insert data via prepared statements, it
/// can be more efficient to explicitly perform a bulk insert.  For
/// compatible drivers, this can be accomplished by setting up and
/// executing a statement.  Instead of setting a SQL query or
/// Substrait plan, bind the source data via AdbcStatementBind, and
/// set the name of the table to be created via AdbcStatementSetOption
/// and the options below.
///
/// @{

/// \brief The name of the target table for a bulk insert.
#define ADBC_INGEST_OPTION_TARGET_TABLE "adbc.ingest.target_table"

/// }@

// TODO: methods to get a particular result set from the statement,
// etc. especially for prepared statements with parameter batches

/// \defgroup adbc-statement-partition Partitioned Results
/// Some backends may internally partition the results. These
/// partitions are exposed to clients who may wish to integrate them
/// with a threaded or distributed execution model, where partitions
/// can be divided among threads or machines. Partitions are exposed
/// as an iterator.
///
/// Drivers are not required to support partitioning. In this case,
/// num_partitions will return 0. They are required to support
/// AdbcStatementGetStream.
///
/// If any of the partitioning methods are called,
/// AdbcStatementGetStream may not be called, and vice versa.
///
/// @{

/// \brief Get the length of the serialized descriptor for the current
///   partition.
///
/// This method must be called first, before calling other partitioning
/// methods. This method may block and perform I/O.
/// \param[in] statement The statement.
/// \param[out] length The length of the serialized partition, or 0 if there
///   are no more partitions.
/// \param[out] error An optional location to return an error message if
///   necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                 size_t* length, struct AdbcError* error);

/// \brief Get the serialized descriptor for the current partition, and advance
///   the iterator.
///
/// This method may block and perform I/O.
///
/// A partition can be turned back into a statement via
/// AdbcConnectionDeserializePartitionDesc. Effectively, this means AdbcStatement
/// is similar to arrow::flight::FlightInfo in Flight/Flight SQL and
/// get_partitions is similar to getting the arrow::flight::Ticket.
///
/// \param[in] statement The statement.
/// \param[out] partition_desc A caller-allocated buffer, to which the
///   serialized partition will be written. The length to allocate can be
///   queried with AdbcStatementGetPartitionDescSize.
/// \param[out] error An optional location to return an error message if
///   necessary.
ADBC_EXPORT
AdbcStatusCode AdbcStatementGetPartitionDesc(struct AdbcStatement* statement,
                                             uint8_t* partition_desc,
                                             struct AdbcError* error);

/// }@

/// }@

/// \defgroup adbc-driver Driver initialization.
/// @{

/// \brief An instance of an initialized database driver.
///
/// This provides a common interface for implementation-specific
/// driver initialization routines. Drivers should populate this
/// struct, and applications can call ADBC functions through this
/// struct, without worrying about multiple definitions of the same
/// symbol.
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

  AdbcStatusCode (*ConnectionNew)(struct AdbcDatabase*, struct AdbcConnection*,
                                  struct AdbcError*);
  AdbcStatusCode (*ConnectionSetOption)(struct AdbcConnection*, const char*, const char*,
                                        struct AdbcError*);
  AdbcStatusCode (*ConnectionInit)(struct AdbcConnection*, struct AdbcError*);
  AdbcStatusCode (*ConnectionRelease)(struct AdbcConnection*, struct AdbcError*);
  AdbcStatusCode (*ConnectionDeserializePartitionDesc)(struct AdbcConnection*,
                                                       const uint8_t*, size_t,
                                                       struct AdbcStatement*,
                                                       struct AdbcError*);

  AdbcStatusCode (*ConnectionGetCatalogs)(struct AdbcConnection*, struct AdbcStatement*,
                                          struct AdbcError*);
  AdbcStatusCode (*ConnectionGetDbSchemas)(struct AdbcConnection*, struct AdbcStatement*,
                                           struct AdbcError*);
  AdbcStatusCode (*ConnectionGetTableTypes)(struct AdbcConnection*, struct AdbcStatement*,
                                            struct AdbcError*);
  AdbcStatusCode (*ConnectionGetTables)(struct AdbcConnection*, const char*, const char*,
                                        const char*, const char**, struct AdbcStatement*,
                                        struct AdbcError*);

  AdbcStatusCode (*StatementNew)(struct AdbcConnection*, struct AdbcStatement*,
                                 struct AdbcError*);
  AdbcStatusCode (*StatementRelease)(struct AdbcStatement*, struct AdbcError*);
  AdbcStatusCode (*StatementBind)(struct AdbcStatement*, struct ArrowArray*,
                                  struct ArrowSchema*, struct AdbcError*);
  AdbcStatusCode (*StatementBindStream)(struct AdbcStatement*, struct ArrowArrayStream*,
                                        struct AdbcError*);
  AdbcStatusCode (*StatementExecute)(struct AdbcStatement*, struct AdbcError*);
  AdbcStatusCode (*StatementPrepare)(struct AdbcStatement*, struct AdbcError*);
  AdbcStatusCode (*StatementGetStream)(struct AdbcStatement*, struct ArrowArrayStream*,
                                       struct AdbcError*);
  AdbcStatusCode (*StatementGetPartitionDescSize)(struct AdbcStatement*, size_t*,
                                                  struct AdbcError*);
  AdbcStatusCode (*StatementGetPartitionDesc)(struct AdbcStatement*, uint8_t*,
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
#define ADBC_VERSION_0_0_1 25

/// }@

/// \page typical-usage Typical Usage Patterns
/// (TODO: describe request sequences)

/// \page decoder-ring Decoder Ring
///
/// ADBC - Flight SQL - JDBC - ODBC
///
/// AdbcConnection - FlightClient - Connection - Connection handle
///
/// AdbcStatement - FlightInfo - Statement - Statement handle
///
/// ArrowArrayStream - FlightStream (Java)/RecordBatchReader (C++) -
/// ResultSet - Statement handle

/// \page compatibility Backwards and Forwards Compatibility

#endif  // ADBC

#ifdef __cplusplus
}
#endif
