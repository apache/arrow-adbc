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

#include "adbc.h"

#include <errno.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <sqlite3.h>

#include "nanoarrow.h"
#include "statement_reader.h"
#include "types.h"
#include "utils.h"

static char kDefaultUri[] = "file:adbc_driver_sqlite?mode=memory&cache=shared";
static uint32_t kSupportedInfoCodes[] = {
    ADBC_INFO_VENDOR_NAME,    ADBC_INFO_VENDOR_VERSION,       ADBC_INFO_DRIVER_NAME,
    ADBC_INFO_DRIVER_VERSION, ADBC_INFO_DRIVER_ARROW_VERSION,
};

// Private names (to avoid conflicts when using the driver manager)

#define CHECK_DB_INIT(NAME, ERROR)                             \
  if (!NAME->private_data) {                                   \
    SetError(ERROR, "%s: database not initialized", __func__); \
    return ADBC_STATUS_INVALID_STATE;                          \
  }
#define CHECK_CONN_INIT(NAME, ERROR)                             \
  if (!NAME->private_data) {                                     \
    SetError(ERROR, "%s: connection not initialized", __func__); \
    return ADBC_STATUS_INVALID_STATE;                            \
  }
#define CHECK_STMT_INIT(NAME, ERROR)                            \
  if (!NAME->private_data) {                                    \
    SetError(ERROR, "%s: statement not initialized", __func__); \
    return ADBC_STATUS_INVALID_STATE;                           \
  }

AdbcStatusCode SqliteDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  if (database->private_data) {
    SetError(error, "AdbcDatabaseNew: database already allocated");
    return ADBC_STATUS_INVALID_STATE;
  }

  database->private_data = malloc(sizeof(struct SqliteDatabase));
  memset(database->private_data, 0, sizeof(struct SqliteDatabase));
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                       const char* value, struct AdbcError* error) {
  CHECK_DB_INIT(database, error);
  struct SqliteDatabase* db = (struct SqliteDatabase*)database->private_data;

  if (strcmp(key, "uri") == 0) {
    if (db->uri) free(db->uri);
    size_t len = strlen(value) + 1;
    db->uri = malloc(len);
    strncpy(db->uri, value, len);
    return ADBC_STATUS_OK;
  }
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

int OpenDatabase(const char* maybe_uri, sqlite3** db, struct AdbcError* error) {
  const char* uri = maybe_uri ? maybe_uri : kDefaultUri;
  int rc = sqlite3_open_v2(uri, db,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                           /*zVfs=*/NULL);
  if (rc != SQLITE_OK) {
    if (*db) {
      SetError(error, "Failed to open %s: %s", uri, sqlite3_errmsg(*db));
    } else {
      SetError(error, "Failed to open %s: failed to allocate memory", uri);
    }
    (void)sqlite3_close(*db);
    *db = NULL;
    return ADBC_STATUS_IO;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode ExecuteQuery(struct SqliteConnection* conn, const char* query,
                            struct AdbcError* error) {
  sqlite3_stmt* stmt = NULL;
  int rc = sqlite3_prepare_v2(conn->conn, query, strlen(query), &stmt, /*pzTail=*/NULL);
  while (rc != SQLITE_DONE && rc != SQLITE_ERROR) {
    rc = sqlite3_step(stmt);
  }
  rc = sqlite3_finalize(stmt);
  if (rc != SQLITE_OK && rc != SQLITE_DONE) {
    SetError(error, "Failed to execute query \"%s\": %s", query,
             sqlite3_errmsg(conn->conn));
    return ADBC_STATUS_INTERNAL;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteDatabaseInit(struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  CHECK_DB_INIT(database, error);
  struct SqliteDatabase* db = (struct SqliteDatabase*)database->private_data;

  if (db->db) {
    SetError(error, "AdbcDatabaseInit: database already initialized");
    return ADBC_STATUS_INVALID_STATE;
  }

  return OpenDatabase(db->uri, &db->db, error);
}

AdbcStatusCode SqliteDatabaseRelease(struct AdbcDatabase* database,
                                     struct AdbcError* error) {
  CHECK_DB_INIT(database, error);
  struct SqliteDatabase* db = (struct SqliteDatabase*)database->private_data;

  size_t connection_count = db->connection_count;
  if (db->uri) free(db->uri);
  if (db->db) {
    if (sqlite3_close(db->db) == SQLITE_BUSY) {
      SetError(error, "AdbcDatabaseRelease: connection is busy");
      return ADBC_STATUS_IO;
    }
  }
  free(database->private_data);
  database->private_data = NULL;

  if (connection_count > 0) {
    SetError(error, "AdbcDatabaseRelease: %ld open connections when released",
             connection_count);
    return ADBC_STATUS_INVALID_STATE;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionNew(struct AdbcConnection* connection,
                                   struct AdbcError* error) {
  if (connection->private_data) {
    SetError(error, "AdbcConnectionNew: connection already allocated");
    return ADBC_STATUS_INVALID_STATE;
  }

  connection->private_data = malloc(sizeof(struct SqliteConnection));
  memset(connection->private_data, 0, sizeof(struct SqliteConnection));
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionSetOption(struct AdbcConnection* connection,
                                         const char* key, const char* value,
                                         struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;
  if (strcmp(key, ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0) {
    if (strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0) {
      if (conn->active_transaction) {
        AdbcStatusCode status = ExecuteQuery(conn, "COMMIT", error);
        if (status != ADBC_STATUS_OK) return status;
        conn->active_transaction = 0;
      } else {
        // no-op
      }
    } else if (strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
      if (conn->active_transaction) {
        // no-op
      } else {
        // begin
        AdbcStatusCode status = ExecuteQuery(conn, "BEGIN", error);
        if (status != ADBC_STATUS_OK) return status;
        conn->active_transaction = 1;
      }
    } else {
      SetError(error, "Invalid connection option value %s=%s", key, value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return ADBC_STATUS_OK;
  }
  SetError(error, "Unknown connection option %s=%s", key, value);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteConnectionInit(struct AdbcConnection* connection,
                                    struct AdbcDatabase* database,
                                    struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  CHECK_DB_INIT(database, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;
  struct SqliteDatabase* db = (struct SqliteDatabase*)database->private_data;

  if (conn->conn) {
    SetError(error, "AdbcConnectionInit: connection already initialized");
    return ADBC_STATUS_INVALID_STATE;
  }
  return OpenDatabase(db->uri, &conn->conn, error);
}

AdbcStatusCode SqliteConnectionRelease(struct AdbcConnection* connection,
                                       struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;

  if (conn->conn) {
    int rc = sqlite3_close(conn->conn);
    if (rc == SQLITE_BUSY) {
      SetError(error, "AdbcConnectionRelease: connection is busy");
      return ADBC_STATUS_IO;
    }
  }
  free(connection->private_data);
  connection->private_data = NULL;

  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionGetInfoAppendStringImpl(struct ArrowArray* array,
                                                       uint32_t info_code,
                                                       const char* info_value,
                                                       struct AdbcError* error) {
  CHECK_NA(INTERNAL, ArrowArrayAppendUInt(array->children[0], info_code), error);
  // Append to type variant
  struct ArrowStringView value;
  value.data = info_value;
  value.n_bytes = strlen(info_value);
  CHECK_NA(INTERNAL, ArrowArrayAppendString(array->children[1]->children[0], value),
           error);
  // Append type code (by hand)
  struct ArrowBuffer* codes = ArrowArrayBuffer(array->children[1], 0);
  CHECK_NA(INTERNAL, ArrowBufferAppendInt8(codes, 0), error);
  // Append type offset (by hand)
  struct ArrowBuffer* offsets = ArrowArrayBuffer(array->children[1], 1);
  CHECK_NA(INTERNAL,
           ArrowBufferAppendInt32(offsets, array->children[1]->children[0]->length - 1),
           error);
  array->children[1]->length++;
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionGetInfoImpl(uint32_t* info_codes, size_t info_codes_length,
                                           struct ArrowSchema* schema,
                                           struct ArrowArray* array,
                                           struct AdbcError* error) {
  CHECK_NA(INTERNAL, ArrowSchemaInit(schema, NANOARROW_TYPE_STRUCT), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(schema, /*num_columns=*/2), error);

  CHECK_NA(INTERNAL, ArrowSchemaInit(schema->children[0], NANOARROW_TYPE_UINT32), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(schema->children[0], "info_name"), error);
  schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;

  struct ArrowSchema* info_value = schema->children[1];
  // TODO(apache/arrow-nanoarrow#73): formal union support
  // initialize with dummy then override
  CHECK_NA(INTERNAL, ArrowSchemaInit(info_value, NANOARROW_TYPE_UINT32), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetFormat(info_value, "+ud:0,1,2,3,4,5"), error);

  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value, "info_value"), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(info_value, /*num_columns=*/6), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(info_value->children[0], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[0], "string_value"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(info_value->children[1], NANOARROW_TYPE_BOOL),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[1], "bool_value"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(info_value->children[2], NANOARROW_TYPE_INT64),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[2], "int64_value"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(info_value->children[3], NANOARROW_TYPE_INT32),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[3], "int32_bitmask"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(info_value->children[4], NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[4], "string_list"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(info_value->children[5], NANOARROW_TYPE_MAP), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(info_value->children[5], "int32_to_int32_list_map"), error);

  CHECK_NA(INTERNAL,
           ArrowSchemaAllocateChildren(info_value->children[4], /*num_columns=*/1),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaInit(info_value->children[4]->children[0], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[4]->children[0], "item"),
           error);

  // XXX: nanoarrow could possibly use helpers for nested types like this
  CHECK_NA(INTERNAL,
           ArrowSchemaAllocateChildren(info_value->children[5], /*num_columns=*/1),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaInit(info_value->children[5]->children[0], NANOARROW_TYPE_STRUCT),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaAllocateChildren(info_value->children[5]->children[0],
                                       /*num_columns=*/2),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaInit(info_value->children[5]->children[0]->children[0],
                           NANOARROW_TYPE_INT32),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(info_value->children[5]->children[0]->children[0], "key"),
           error);
  info_value->children[5]->children[0]->children[0]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL,
           ArrowSchemaInit(info_value->children[5]->children[0]->children[1],
                           NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(info_value->children[5]->children[0]->children[1], "item"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaAllocateChildren(info_value->children[5]->children[0]->children[1],
                                       /*num_columns=*/1),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaInit(info_value->children[5]->children[0]->children[1]->children[0],
                           NANOARROW_TYPE_INT32),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(
               info_value->children[5]->children[0]->children[1]->children[0], "item"),
           error);

  struct ArrowError na_error = {0};
  CHECK_NA_DETAIL(INTERNAL, ArrowArrayInitFromSchema(array, schema, &na_error), &na_error,
                  error);
  CHECK_NA(INTERNAL, ArrowArrayStartAppending(array), error);

  for (size_t i = 0; i < info_codes_length; i++) {
    switch (info_codes[i]) {
      case ADBC_INFO_VENDOR_NAME:
        RAISE(SqliteConnectionGetInfoAppendStringImpl(array, info_codes[i], "SQLite",
                                                      error));
        break;
      case ADBC_INFO_VENDOR_VERSION:
        RAISE(SqliteConnectionGetInfoAppendStringImpl(array, info_codes[i],
                                                      sqlite3_libversion(), error));
        break;
      case ADBC_INFO_DRIVER_NAME:
        RAISE(SqliteConnectionGetInfoAppendStringImpl(array, info_codes[i],
                                                      "ADBC SQLite Driver", error));
        break;
      case ADBC_INFO_DRIVER_VERSION:
        // TODO(lidavidm): fill in driver version
        RAISE(SqliteConnectionGetInfoAppendStringImpl(array, info_codes[i], "(unknown)",
                                                      error));
        break;
      case ADBC_INFO_DRIVER_ARROW_VERSION:
        RAISE(SqliteConnectionGetInfoAppendStringImpl(array, info_codes[i],
                                                      NANOARROW_BUILD_ID, error));
        break;
      default:
        // Ignore
        continue;
    }
    CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);
  }

  CHECK_NA_DETAIL(INTERNAL, ArrowArrayFinishBuilding(array, &na_error), &na_error, error);

  return ADBC_STATUS_OK;
}  // NOLINT(whitespace/indent)

AdbcStatusCode SqliteConnectionGetInfo(struct AdbcConnection* connection,
                                       uint32_t* info_codes, size_t info_codes_length,
                                       struct ArrowArrayStream* out,
                                       struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);

  if (!info_codes) {
    info_codes = kSupportedInfoCodes;
    info_codes_length = sizeof(kSupportedInfoCodes);
  }

  struct ArrowSchema schema = {0};
  struct ArrowArray array = {0};

  AdbcStatusCode status =
      SqliteConnectionGetInfoImpl(info_codes, info_codes_length, &schema, &array, error);
  if (status != ADBC_STATUS_OK) {
    if (schema.release) schema.release(&schema);
    if (array.release) array.release(&array);
    return status;
  }

  return BatchToArrayStream(&array, &schema, out, error);
}

AdbcStatusCode SqliteConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                          const char* catalog, const char* db_schema,
                                          const char* table_name, const char** table_type,
                                          const char* column_name,
                                          struct ArrowArrayStream* out,
                                          struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteConnectionGetTableSchema(struct AdbcConnection* connection,
                                              const char* catalog, const char* db_schema,
                                              const char* table_name,
                                              struct ArrowSchema* schema,
                                              struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;
  if (catalog != NULL && strlen(catalog) > 0) {
    // TODO: map 'catalog' to SQLite attached database
    memset(schema, 0, sizeof(*schema));
    return ADBC_STATUS_OK;
  } else if (db_schema != NULL && strlen(db_schema) > 0) {
    // SQLite does not support schemas
    memset(schema, 0, sizeof(*schema));
    return ADBC_STATUS_OK;
  } else if (table_name == NULL) {
    SetError(error, "AdbcConnectionGetTableSchema: must provide table_name");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  struct StringBuilder query = {0};
  StringBuilderInit(&query, /*initial_size=*/64);
  StringBuilderAppend(&query, "SELECT * FROM ");
  StringBuilderAppend(&query, table_name);

  sqlite3_stmt* stmt = NULL;
  int rc =
      sqlite3_prepare_v2(conn->conn, query.buffer, query.size, &stmt, /*pzTail=*/NULL);
  StringBuilderReset(&query);
  if (rc != SQLITE_OK) {
    SetError(error, "Failed to prepare query: %s", sqlite3_errmsg(conn->conn));
    return ADBC_STATUS_INTERNAL;
  }

  struct ArrowArrayStream stream = {0};
  AdbcStatusCode status = SqliteExportReader(conn->conn, stmt, /*binder=*/NULL,
                                             /*batch_size=*/64, &stream, error);
  if (status == ADBC_STATUS_OK) {
    int code = stream.get_schema(&stream, schema);
    if (code != 0) {
      SetError(error, "Failed to get schema: (%d) %s", code, strerror(code));
      status = ADBC_STATUS_IO;
    }
  }
  if (stream.release) {
    stream.release(&stream);
  }
  (void)sqlite3_finalize(stmt);
  return status;
}

AdbcStatusCode SqliteConnectionGetTableTypesImpl(struct ArrowSchema* schema,
                                                 struct ArrowArray* array,
                                                 struct AdbcError* error) {
  CHECK_NA(INTERNAL, ArrowSchemaInit(schema, NANOARROW_TYPE_STRUCT), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(schema, /*num_columns=*/1), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(schema->children[0], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(schema->children[0], "table_type"), error);
  schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;

  CHECK_NA(INTERNAL, ArrowArrayInitFromSchema(array, schema, NULL), error);
  CHECK_NA(INTERNAL, ArrowArrayStartAppending(array), error);

  struct ArrowStringView value = {0};
  value.data = "table";
  value.n_bytes = strlen(value.data);
  CHECK_NA(INTERNAL, ArrowArrayAppendString(array->children[0], value), error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);
  value.data = "view";
  value.n_bytes = strlen(value.data);
  CHECK_NA(INTERNAL, ArrowArrayAppendString(array->children[0], value), error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);

  CHECK_NA(INTERNAL, ArrowArrayFinishBuilding(array, NULL), error);
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionGetTableTypes(struct AdbcConnection* connection,
                                             struct ArrowArrayStream* out,
                                             struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;

  struct ArrowSchema schema = {0};
  struct ArrowArray array = {0};

  AdbcStatusCode status = SqliteConnectionGetTableTypesImpl(&schema, &array, error);
  if (status != ADBC_STATUS_OK) {
    if (schema.release) schema.release(&schema);
    if (array.release) array.release(&array);
    return status;
  }
  return BatchToArrayStream(&array, &schema, out, error);
}

AdbcStatusCode SqliteConnectionReadPartition(struct AdbcConnection* connection,
                                             const uint8_t* serialized_partition,
                                             size_t serialized_length,
                                             struct ArrowArrayStream* out,
                                             struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteConnectionCommit(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;
  if (!conn->active_transaction) {
    SetError(error, "No active transaction, cannot commit");
    return ADBC_STATUS_INVALID_STATE;
  }

  AdbcStatusCode status = ExecuteQuery(conn, "COMMIT", error);
  if (status != ADBC_STATUS_OK) return status;
  return ExecuteQuery(conn, "BEGIN", error);
}

AdbcStatusCode SqliteConnectionRollback(struct AdbcConnection* connection,
                                        struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;
  if (!conn->active_transaction) {
    SetError(error, "No active transaction, cannot rollback");
    return ADBC_STATUS_INVALID_STATE;
  }

  AdbcStatusCode status = ExecuteQuery(conn, "ROLLBACK", error);
  if (status != ADBC_STATUS_OK) return status;
  return ExecuteQuery(conn, "BEGIN", error);
}

AdbcStatusCode SqliteStatementNew(struct AdbcConnection* connection,
                                  struct AdbcStatement* statement,
                                  struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;
  if (statement->private_data) {
    SetError(error, "AdbcStatementNew: statement already allocated");
    return ADBC_STATUS_INVALID_STATE;
  } else if (!conn->conn) {
    SetError(error, "AdbcStatementNew: connection is not initialized");
    return ADBC_STATUS_INVALID_STATE;
  }

  statement->private_data = malloc(sizeof(struct SqliteStatement));
  memset(statement->private_data, 0, sizeof(struct SqliteStatement));
  struct SqliteStatement* stmt = (struct SqliteStatement*)statement->private_data;
  stmt->conn = conn->conn;

  // Default options
  stmt->batch_size = 1024;

  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteStatementRelease(struct AdbcStatement* statement,
                                      struct AdbcError* error) {
  CHECK_STMT_INIT(statement, error);
  struct SqliteStatement* stmt = (struct SqliteStatement*)statement->private_data;

  int rc = SQLITE_OK;
  if (stmt->stmt) {
    rc = sqlite3_finalize(stmt->stmt);
  }
  if (stmt->query) free(stmt->query);
  SqliteBinderRelease(&stmt->binder);
  if (stmt->target_table) free(stmt->target_table);
  if (rc != SQLITE_OK) {
    SetError(error, "AdbcStatementRelease: statement failed to finalize: (%d) %s", rc,
             sqlite3_errmsg(stmt->conn));
  }
  free(statement->private_data);
  statement->private_data = NULL;

  return rc == SQLITE_OK ? ADBC_STATUS_OK : ADBC_STATUS_IO;
}

AdbcStatusCode SqliteStatementPrepare(struct AdbcStatement* statement,
                                      struct AdbcError* error) {
  CHECK_STMT_INIT(statement, error);
  struct SqliteStatement* stmt = (struct SqliteStatement*)statement->private_data;

  if (!stmt->query) {
    SetError(error, "Must SetSqlQuery before ExecuteQuery or Prepare");
    return ADBC_STATUS_INVALID_STATE;
  }
  if (stmt->prepared == 0) {
    if (stmt->stmt) {
      int rc = sqlite3_finalize(stmt->stmt);
      stmt->stmt = NULL;
      if (rc != SQLITE_OK) {
        SetError(error, "Failed to finalize previous statement: (%d) %s", rc,
                 sqlite3_errmsg(stmt->conn));
        return ADBC_STATUS_IO;
      }
    }

    int rc =
        sqlite3_prepare_v2(stmt->conn, stmt->query, (int)stmt->query_len, &stmt->stmt,
                           /*pzTail=*/NULL);
    if (rc != SQLITE_OK) {
      SetError(error, "Failed to prepare query: %s\nQuery:%s", sqlite3_errmsg(stmt->conn),
               stmt->query);
      (void)sqlite3_finalize(stmt->stmt);
      stmt->stmt = NULL;
      return ADBC_STATUS_IO;
    }
    stmt->prepared = 1;
  }

  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteStatementInitIngest(struct SqliteStatement* stmt,
                                         sqlite3_stmt** insert_statement,
                                         struct AdbcError* error) {
  AdbcStatusCode code = ADBC_STATUS_OK;

  // Create statements for CREATE TABLE / INSERT
  struct StringBuilder create_query = {0};
  struct StringBuilder insert_query = {0};
  StringBuilderInit(&create_query, 256);
  StringBuilderInit(&insert_query, 256);

  StringBuilderAppend(&create_query, "CREATE TABLE ");
  StringBuilderAppend(&create_query, stmt->target_table);
  StringBuilderAppend(&create_query, " (");

  StringBuilderAppend(&insert_query, "INSERT INTO ");
  StringBuilderAppend(&insert_query, stmt->target_table);
  StringBuilderAppend(&insert_query, " VALUES (");

  for (int i = 0; i < stmt->binder.schema.n_children; i++) {
    if (i > 0) StringBuilderAppend(&create_query, ", ");
    // XXX: should escape the column name too
    StringBuilderAppend(&create_query, stmt->binder.schema.children[i]->name);

    if (i > 0) StringBuilderAppend(&insert_query, ", ");
    StringBuilderAppend(&insert_query, "?");
  }
  StringBuilderAppend(&create_query, ")");
  StringBuilderAppend(&insert_query, ")");

  sqlite3_stmt* create = NULL;
  if (!stmt->append) {
    // Create table
    int rc = sqlite3_prepare_v2(stmt->conn, create_query.buffer, (int)create_query.size,
                                &create, /*pzTail=*/NULL);
    if (rc == SQLITE_OK) {
      rc = sqlite3_step(create);
    }

    if (rc != SQLITE_OK && rc != SQLITE_DONE) {
      SetError(error, "Failed to create table: %s (executed '%s')",
               sqlite3_errmsg(stmt->conn), create_query.buffer);
      code = ADBC_STATUS_INTERNAL;
    }
  }

  if (code == ADBC_STATUS_OK) {
    int rc = sqlite3_prepare_v2(stmt->conn, insert_query.buffer, (int)insert_query.size,
                                insert_statement, /*pzTail=*/NULL);
    if (rc != SQLITE_OK) {
      SetError(error, "Failed to prepare statement: %s (executed '%s')",
               sqlite3_errmsg(stmt->conn), insert_query.buffer);
      code = ADBC_STATUS_INTERNAL;
    }
  }

  sqlite3_finalize(create);
  StringBuilderReset(&create_query);
  StringBuilderReset(&insert_query);
  return code;
}

AdbcStatusCode SqliteStatementExecuteIngest(struct SqliteStatement* stmt,
                                            int64_t* rows_affected,
                                            struct AdbcError* error) {
  if (!stmt->binder.schema.release) {
    SetError(error, "Must Bind() before bulk ingestion");
    return ADBC_STATUS_INVALID_STATE;
  }

  sqlite3_stmt* insert = NULL;
  AdbcStatusCode status = SqliteStatementInitIngest(stmt, &insert, error);

  int64_t row_count = 0;
  if (status == ADBC_STATUS_OK) {
    while (1) {
      char finished = 0;
      status = SqliteBinderBindNext(&stmt->binder, stmt->conn, insert, &finished, error);
      if (status != ADBC_STATUS_OK || finished) break;

      int rc = 0;
      do {
        rc = sqlite3_step(insert);
      } while (rc == SQLITE_ROW);
      if (rc != SQLITE_DONE) {
        SetError(error, "Failed to execute statement: %s", sqlite3_errmsg(stmt->conn));
        status = ADBC_STATUS_INTERNAL;
        break;
      }
      row_count++;
    }
  }

  if (rows_affected) *rows_affected = row_count;
  if (insert) sqlite3_finalize(insert);
  SqliteBinderRelease(&stmt->binder);
  return status;
}

AdbcStatusCode SqliteStatementExecuteQuery(struct AdbcStatement* statement,
                                           struct ArrowArrayStream* out,
                                           int64_t* rows_affected,
                                           struct AdbcError* error) {
  CHECK_STMT_INIT(statement, error);
  struct SqliteStatement* stmt = (struct SqliteStatement*)statement->private_data;

  if (stmt->target_table) {
    return SqliteStatementExecuteIngest(stmt, rows_affected, error);
  }

  AdbcStatusCode status = SqliteStatementPrepare(statement, error);
  if (status != ADBC_STATUS_OK) return status;

  if (stmt->binder.schema.release) {
    int64_t expected = sqlite3_bind_parameter_count(stmt->stmt);
    int64_t actual = stmt->binder.schema.n_children;
    if (actual != expected) {
      SetError(error, "Parameter count mismatch: expected %lld but found %lld", expected,
               actual);
      return ADBC_STATUS_INVALID_STATE;
    }
  }

  if (!out) {
    // Update
    sqlite3_mutex_enter(sqlite3_db_mutex(stmt->conn));

    AdbcStatusCode status = ADBC_STATUS_OK;
    int64_t rows = 0;

    while (1) {
      if (stmt->binder.schema.release) {
        char finished = 0;
        status =
            SqliteBinderBindNext(&stmt->binder, stmt->conn, stmt->stmt, &finished, error);
        if (status != ADBC_STATUS_OK || finished) {
          break;
        }
      }

      while (sqlite3_step(stmt->stmt) == SQLITE_ROW) {
        rows++;
      }
      if (!stmt->binder.schema.release) break;
    }

    if (sqlite3_reset(stmt->stmt) != SQLITE_OK) {
      status = ADBC_STATUS_IO;
      const char* msg = sqlite3_errmsg(stmt->conn);
      SetError(error, "Failed to execute query: %s",
               (msg == NULL) ? "(unknown error)" : msg);
    }

    sqlite3_mutex_leave(sqlite3_db_mutex(stmt->conn));

    SqliteBinderRelease(&stmt->binder);
    if (rows_affected) *rows_affected = rows;
    return status;
  }

  // Query
  if (rows_affected) *rows_affected = -1;
  struct SqliteBinder* binder = stmt->binder.schema.release ? &stmt->binder : NULL;
  return SqliteExportReader(stmt->conn, stmt->stmt, binder, stmt->batch_size, out, error);
}

AdbcStatusCode SqliteStatementSetSqlQuery(struct AdbcStatement* statement,
                                          const char* query, struct AdbcError* error) {
  CHECK_STMT_INIT(statement, error);
  struct SqliteStatement* stmt = (struct SqliteStatement*)statement->private_data;

  if (stmt->query) {
    free(stmt->query);
    stmt->query = NULL;
  }
  if (stmt->target_table) {
    free(stmt->target_table);
    stmt->target_table = NULL;
  }
  size_t len = strlen(query) + 1;
  stmt->query = malloc(len);
  stmt->query_len = len;
  stmt->prepared = 0;
  strncpy(stmt->query, query, len);
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteStatementSetSubstraitPlan(struct AdbcStatement* statement,
                                               const uint8_t* plan, size_t length,
                                               struct AdbcError* error) {
  CHECK_STMT_INIT(statement, error);
  SetError(error, "Substrait is not supported");
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteStatementBind(struct AdbcStatement* statement,
                                   struct ArrowArray* values, struct ArrowSchema* schema,
                                   struct AdbcError* error) {
  CHECK_STMT_INIT(statement, error);
  struct SqliteStatement* stmt = (struct SqliteStatement*)statement->private_data;
  return SqliteBinderSetArray(&stmt->binder, values, schema, error);
}

AdbcStatusCode SqliteStatementBindStream(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* stream,
                                         struct AdbcError* error) {
  CHECK_STMT_INIT(statement, error);
  struct SqliteStatement* stmt = (struct SqliteStatement*)statement->private_data;
  return SqliteBinderSetArrayStream(&stmt->binder, stream, error);
}

AdbcStatusCode SqliteStatementGetParameterSchema(struct AdbcStatement* statement,
                                                 struct ArrowSchema* schema,
                                                 struct AdbcError* error) {
  AdbcStatusCode status = SqliteStatementPrepare(statement, error);
  if (status != ADBC_STATUS_OK) return status;

  struct SqliteStatement* stmt = (struct SqliteStatement*)statement->private_data;
  int num_params = sqlite3_bind_parameter_count(stmt->stmt);
  if (num_params < 0) {
    // Should not happen
    SetError(error, "SQLite returned negative parameter count");
    return ADBC_STATUS_INTERNAL;
  }

  CHECK_NA(INTERNAL, ArrowSchemaInit(schema, NANOARROW_TYPE_STRUCT), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(schema, num_params), error);
  char buffer[11];
  for (int i = 0; i < num_params; i++) {
    const char* name = sqlite3_bind_parameter_name(stmt->stmt, i + 1);
    if (name == NULL) {
      snprintf(buffer, sizeof(buffer), "%d", i);
      name = buffer;
    }
    CHECK_NA(INTERNAL, ArrowSchemaInit(schema->children[i], NANOARROW_TYPE_NA), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(schema->children[i], name), error);
  }

  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteStatementSetOption(struct AdbcStatement* statement, const char* key,
                                        const char* value, struct AdbcError* error) {
  CHECK_STMT_INIT(statement, error);
  struct SqliteStatement* stmt = (struct SqliteStatement*)statement->private_data;

  if (strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0) {
    if (stmt->query) {
      free(stmt->query);
      stmt->query = NULL;
    }
    if (stmt->target_table) {
      free(stmt->target_table);
      stmt->target_table = NULL;
    }

    size_t len = strlen(value) + 1;
    stmt->target_table = (char*)malloc(len);
    strncpy(stmt->target_table, value, len);
    return ADBC_STATUS_OK;
  } else if (strcmp(key, ADBC_INGEST_OPTION_MODE) == 0) {
    if (strcmp(value, ADBC_INGEST_OPTION_MODE_APPEND) == 0) {
      stmt->append = 1;
    } else if (strcmp(value, ADBC_INGEST_OPTION_MODE_CREATE) == 0) {
      stmt->append = 0;
    } else {
      SetError(error, "Invalid statement option value %s=%s", key, value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return ADBC_STATUS_OK;
  }
  SetError(error, "Unknown statement option %s=%s", key, value);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteStatementExecutePartitions(struct AdbcStatement* statement,
                                                struct ArrowSchema* schema,
                                                struct AdbcPartitions* partitions,
                                                int64_t* rows_affected,
                                                struct AdbcError* error) {
  CHECK_STMT_INIT(statement, error);
  SetError(error, "Partitioned result sets are not supported");
  return ADBC_STATUS_NOT_IMPLEMENTED;
}  // NOLINT(whitespace/indent)

AdbcStatusCode SqliteInitFunc(int version, void* driver, struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

// Public names

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  return SqliteDatabaseNew(database, error);
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return SqliteDatabaseSetOption(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  return SqliteDatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return SqliteDatabaseRelease(database, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return SqliteConnectionNew(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return SqliteConnectionSetOption(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  return SqliteConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  return SqliteConnectionRelease(connection, error);
}

AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection* connection,
                                     uint32_t* info_codes, size_t info_codes_length,
                                     struct ArrowArrayStream* out,
                                     struct AdbcError* error) {
  return SqliteConnectionGetInfo(connection, info_codes, info_codes_length, out, error);
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_type,
                                        const char* column_name,
                                        struct ArrowArrayStream* out,
                                        struct AdbcError* error) {
  return SqliteConnectionGetObjects(connection, depth, catalog, db_schema, table_name,
                                    table_type, column_name, out, error);
}

AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name,
                                            struct ArrowSchema* schema,
                                            struct AdbcError* error) {
  return SqliteConnectionGetTableSchema(connection, catalog, db_schema, table_name,
                                        schema, error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return SqliteConnectionGetTableTypes(connection, out, error);
}

AdbcStatusCode AdbcConnectionReadPartition(struct AdbcConnection* connection,
                                           const uint8_t* serialized_partition,
                                           size_t serialized_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return SqliteConnectionReadPartition(connection, serialized_partition,
                                       serialized_length, out, error);
}

AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return SqliteConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  return SqliteConnectionRollback(connection, error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  return SqliteStatementNew(connection, statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return SqliteStatementRelease(statement, error);
}

AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* out,
                                         int64_t* rows_affected,
                                         struct AdbcError* error) {
  return SqliteStatementExecuteQuery(statement, out, rows_affected, error);
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return SqliteStatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return SqliteStatementSetSqlQuery(statement, query, error);
}

AdbcStatusCode AdbcStatementSetSubstraitPlan(struct AdbcStatement* statement,
                                             const uint8_t* plan, size_t length,
                                             struct AdbcError* error) {
  return SqliteStatementSetSubstraitPlan(statement, plan, length, error);
}

AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error) {
  return SqliteStatementBind(statement, values, schema, error);
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement* statement,
                                       struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return SqliteStatementBindStream(statement, stream, error);
}

AdbcStatusCode AdbcStatementGetParameterSchema(struct AdbcStatement* statement,
                                               struct ArrowSchema* schema,
                                               struct AdbcError* error) {
  return SqliteStatementGetParameterSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error) {
  return SqliteStatementSetOption(statement, key, value, error);
}

AdbcStatusCode AdbcStatementExecutePartitions(struct AdbcStatement* statement,
                                              struct ArrowSchema* schema,
                                              struct AdbcPartitions* partitions,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  return SqliteStatementExecutePartitions(statement, schema, partitions, rows_affected,
                                          error);
}  // NOLINT(whitespace/indent)
// due to https://github.com/cpplint/cpplint/pull/189

AdbcStatusCode AdbcInitFunc(int version, void* driver, struct AdbcError* error) {
  return SqliteInitFunc(version, driver, error);
}
