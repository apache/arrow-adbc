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
#include <inttypes.h>
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

static const char kDefaultUri[] = "file:adbc_driver_sqlite?mode=memory&cache=shared";
// The batch size for query results (and for initial type inference)
static const char kStatementOptionBatchRows[] = "adbc.sqlite.query.batch_rows";
static const uint32_t kSupportedInfoCodes[] = {
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
  SetError(error, "Unknown database option %s=%s", key, value);
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
  struct ArrowStringView value = ArrowCharView(info_value);
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

AdbcStatusCode SqliteConnectionGetInfoImpl(const uint32_t* info_codes,
                                           size_t info_codes_length,
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
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[5]->children[0], "entries"),
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
        RAISE_ADBC(SqliteConnectionGetInfoAppendStringImpl(array, info_codes[i], "SQLite",
                                                           error));
        break;
      case ADBC_INFO_VENDOR_VERSION:
        RAISE_ADBC(SqliteConnectionGetInfoAppendStringImpl(array, info_codes[i],
                                                           sqlite3_libversion(), error));
        break;
      case ADBC_INFO_DRIVER_NAME:
        RAISE_ADBC(SqliteConnectionGetInfoAppendStringImpl(array, info_codes[i],
                                                           "ADBC SQLite Driver", error));
        break;
      case ADBC_INFO_DRIVER_VERSION:
        // TODO(lidavidm): fill in driver version
        RAISE_ADBC(SqliteConnectionGetInfoAppendStringImpl(array, info_codes[i],
                                                           "(unknown)", error));
        break;
      case ADBC_INFO_DRIVER_ARROW_VERSION:
        RAISE_ADBC(SqliteConnectionGetInfoAppendStringImpl(array, info_codes[i],
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

  // XXX: mistake in adbc.h (should have been const pointer)
  const uint32_t* codes = info_codes;
  if (!info_codes) {
    codes = kSupportedInfoCodes;
    info_codes_length = sizeof(kSupportedInfoCodes) / sizeof(kSupportedInfoCodes[0]);
  }

  struct ArrowSchema schema = {0};
  struct ArrowArray array = {0};

  AdbcStatusCode status =
      SqliteConnectionGetInfoImpl(codes, info_codes_length, &schema, &array, error);
  if (status != ADBC_STATUS_OK) {
    if (schema.release) schema.release(&schema);
    if (array.release) array.release(&array);
    return status;
  }

  return BatchToArrayStream(&array, &schema, out, error);
}

AdbcStatusCode SqliteConnectionGetObjectsSchema(struct ArrowSchema* schema,
                                                struct AdbcError* error) {
  CHECK_NA(INTERNAL, ArrowSchemaInit(schema, NANOARROW_TYPE_STRUCT), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(schema, /*num_columns=*/2), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(schema->children[0], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(schema->children[0], "catalog_name"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(schema->children[1], NANOARROW_TYPE_LIST), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(schema->children[1], "catalog_db_schemas"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(schema->children[1], 1), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaInit(schema->children[1]->children[0], NANOARROW_TYPE_STRUCT),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(schema->children[1]->children[0], "item"), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(schema->children[1]->children[0], 2),
           error);

  struct ArrowSchema* db_schema_schema = schema->children[1]->children[0];
  CHECK_NA(INTERNAL,
           ArrowSchemaInit(db_schema_schema->children[0], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(db_schema_schema->children[0], "db_schema_name"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(db_schema_schema->children[1], NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(db_schema_schema->children[1], "db_schema_tables"), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(db_schema_schema->children[1], 1),
           error);
  CHECK_NA(
      INTERNAL,
      ArrowSchemaInit(db_schema_schema->children[1]->children[0], NANOARROW_TYPE_STRUCT),
      error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(db_schema_schema->children[1]->children[0], "item"), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaAllocateChildren(db_schema_schema->children[1]->children[0], 4),
           error);

  struct ArrowSchema* table_schema = db_schema_schema->children[1]->children[0];
  CHECK_NA(INTERNAL, ArrowSchemaInit(table_schema->children[0], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(table_schema->children[0], "table_name"), error);
  table_schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL, ArrowSchemaInit(table_schema->children[1], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(table_schema->children[1], "table_type"), error);
  table_schema->children[1]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL, ArrowSchemaInit(table_schema->children[2], NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(table_schema->children[2], "table_columns"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(table_schema->children[2], 1), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaInit(table_schema->children[2]->children[0], NANOARROW_TYPE_STRUCT),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(table_schema->children[2]->children[0], "item"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaAllocateChildren(table_schema->children[2]->children[0], 19),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(table_schema->children[3], NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(table_schema->children[3], "table_constraints"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(table_schema->children[3], 1), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaInit(table_schema->children[3]->children[0], NANOARROW_TYPE_STRUCT),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(table_schema->children[3]->children[0], "item"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaAllocateChildren(table_schema->children[3]->children[0], 4), error);

  struct ArrowSchema* column_schema = table_schema->children[2]->children[0];
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[0], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[0], "column_name"),
           error);
  column_schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[1], NANOARROW_TYPE_INT32),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[1], "ordinal_position"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[2], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[2], "remarks"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[3], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[3], "xdbc_data_type"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[4], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[4], "xdbc_type_name"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[5], NANOARROW_TYPE_INT32),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[5], "xdbc_column_size"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[6], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[6], "xdbc_decimal_digits"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[7], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[7], "xdbc_num_prec_radix"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[8], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[8], "xdbc_nullable"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[9], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[9], "xdbc_column_def"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[10], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[10], "xdbc_sql_data_type"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[11], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[11], "xdbc_datetime_sub"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[12], NANOARROW_TYPE_INT32),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[12], "xdbc_char_octet_length"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[13], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[13], "xdbc_is_nullable"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[14], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[14], "xdbc_scope_catalog"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[15], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[15], "xdbc_scope_schema"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[16], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[16], "xdbc_scope_table"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[17], NANOARROW_TYPE_BOOL),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[17], "xdbc_is_autoincrement"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(column_schema->children[18], NANOARROW_TYPE_BOOL),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[18], "xdbc_is_generatedcolumn"),
           error);

  struct ArrowSchema* constraint_schema = table_schema->children[3]->children[0];
  CHECK_NA(INTERNAL,
           ArrowSchemaInit(constraint_schema->children[0], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(constraint_schema->children[0], "constraint_name"), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaInit(constraint_schema->children[1], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(constraint_schema->children[1], "constraint_type"), error);
  constraint_schema->children[1]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL, ArrowSchemaInit(constraint_schema->children[2], NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(constraint_schema->children[2], "constraint_column_names"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(constraint_schema->children[2], 1),
           error);
  CHECK_NA(
      INTERNAL,
      ArrowSchemaInit(constraint_schema->children[2]->children[0], NANOARROW_TYPE_STRING),
      error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(constraint_schema->children[2]->children[0], "item"),
           error);
  constraint_schema->children[2]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL, ArrowSchemaInit(constraint_schema->children[3], NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(constraint_schema->children[3], "constraint_column_usage"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(constraint_schema->children[3], 1),
           error);
  CHECK_NA(
      INTERNAL,
      ArrowSchemaInit(constraint_schema->children[3]->children[0], NANOARROW_TYPE_STRUCT),
      error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(constraint_schema->children[3]->children[0], "item"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaAllocateChildren(constraint_schema->children[3]->children[0], 4),
           error);

  struct ArrowSchema* usage_schema = constraint_schema->children[3]->children[0];
  CHECK_NA(INTERNAL, ArrowSchemaInit(usage_schema->children[0], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(usage_schema->children[0], "fk_catalog"), error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(usage_schema->children[1], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(usage_schema->children[1], "fk_db_schema"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaInit(usage_schema->children[2], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(usage_schema->children[2], "fk_table"), error);
  usage_schema->children[2]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL, ArrowSchemaInit(usage_schema->children[3], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(usage_schema->children[3], "fk_column_name"),
           error);
  usage_schema->children[3]->flags &= ~ARROW_FLAG_NULLABLE;

  return ADBC_STATUS_OK;
}

static const char kTableQuery[] =
    "SELECT name, type "
    "FROM sqlite_master "
    "WHERE name LIKE ? AND type <> 'index'"
    "ORDER BY name ASC";
static const char kColumnQuery[] =
    "SELECT cid, name, type, \"notnull\", dflt_value "
    "FROM pragma_table_info(?) "
    "WHERE name LIKE ? "
    "ORDER BY cid ASC";
static const char kPrimaryKeyQuery[] =
    "SELECT name "
    "FROM pragma_table_info(?) "
    "WHERE pk > 0 "
    "ORDER BY pk ASC";
static const char kForeignKeyQuery[] =
    "SELECT id, seq, \"table\", \"from\", \"to\" "
    "FROM pragma_foreign_key_list(?) "
    "ORDER BY id, seq ASC";

AdbcStatusCode SqliteConnectionGetColumnsImpl(
    struct SqliteConnection* conn, const char* table_name, const char* column_name,
    struct ArrowArray* table_columns_col, sqlite3_stmt* stmt, struct AdbcError* error) {
  struct ArrowArray* table_columns_items = table_columns_col->children[0];
  struct ArrowArray* column_name_col = table_columns_items->children[0];
  struct ArrowArray* ordinal_position_col = table_columns_items->children[1];
  struct ArrowArray* remarks_col = table_columns_items->children[2];
  struct ArrowArray* xdbc_data_type_col = table_columns_items->children[3];
  struct ArrowArray* xdbc_type_name_col = table_columns_items->children[4];
  struct ArrowArray* xdbc_column_size_col = table_columns_items->children[5];
  struct ArrowArray* xdbc_decimal_digits_col = table_columns_items->children[6];
  struct ArrowArray* xdbc_num_prec_radix_col = table_columns_items->children[7];
  struct ArrowArray* xdbc_nullable_col = table_columns_items->children[8];
  struct ArrowArray* xdbc_column_def_col = table_columns_items->children[9];
  struct ArrowArray* xdbc_sql_data_type_col = table_columns_items->children[10];
  struct ArrowArray* xdbc_datetime_sub_col = table_columns_items->children[11];
  struct ArrowArray* xdbc_char_octet_length_col = table_columns_items->children[12];
  struct ArrowArray* xdbc_is_nullable_col = table_columns_items->children[13];
  struct ArrowArray* xdbc_scope_catalog_col = table_columns_items->children[14];
  struct ArrowArray* xdbc_scope_schema_col = table_columns_items->children[15];
  struct ArrowArray* xdbc_scope_table_col = table_columns_items->children[16];
  struct ArrowArray* xdbc_is_autoincrement_col = table_columns_items->children[17];
  struct ArrowArray* xdbc_is_generatedcolumn_col = table_columns_items->children[18];

  int rc = sqlite3_reset(stmt);
  RAISE(INTERNAL, rc == SQLITE_OK, sqlite3_errmsg(conn->conn), error);

  rc = sqlite3_bind_text64(stmt, 1, table_name, strlen(table_name), SQLITE_STATIC,
                           SQLITE_UTF8);
  RAISE(INTERNAL, rc == SQLITE_OK, sqlite3_errmsg(conn->conn), error);

  if (column_name) {
    rc = sqlite3_bind_text64(stmt, 2, column_name, strlen(column_name), SQLITE_STATIC,
                             SQLITE_UTF8);
  } else {
    rc = sqlite3_bind_text64(stmt, 2, "%", 1, SQLITE_STATIC, SQLITE_UTF8);
  }
  RAISE(INTERNAL, rc == SQLITE_OK, sqlite3_errmsg(conn->conn), error);

  while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    const char* col_name = (const char*)sqlite3_column_text(stmt, 1);
    struct ArrowStringView str = {.data = col_name,
                                  .n_bytes = sqlite3_column_bytes(stmt, 1)};
    CHECK_NA(INTERNAL, ArrowArrayAppendString(column_name_col, str), error);

    const int32_t col_cid = sqlite3_column_int(stmt, 0);
    CHECK_NA(INTERNAL, ArrowArrayAppendInt(ordinal_position_col, col_cid + 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(remarks_col, 1), error);

    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_data_type_col, 1), error);

    const char* col_type = (const char*)sqlite3_column_text(stmt, 2);
    if (col_type) {
      str.data = col_type;
      str.n_bytes = sqlite3_column_bytes(stmt, 2);
      CHECK_NA(INTERNAL, ArrowArrayAppendString(xdbc_type_name_col, str), error);
    } else {
      CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_type_name_col, 1), error);
    }

    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_column_size_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_decimal_digits_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_num_prec_radix_col, 1), error);

    const int32_t col_notnull = sqlite3_column_int(stmt, 3);
    if (col_notnull == 0) {
      // JDBC columnNullable == 1
      CHECK_NA(INTERNAL, ArrowArrayAppendInt(xdbc_nullable_col, 1), error);
    } else {
      // JDBC columnNoNulls == 0
      CHECK_NA(INTERNAL, ArrowArrayAppendInt(xdbc_nullable_col, 0), error);
    }

    const char* col_def = (const char*)sqlite3_column_text(stmt, 4);
    if (col_def) {
      str.data = col_def;
      str.n_bytes = sqlite3_column_bytes(stmt, 4);
      CHECK_NA(INTERNAL, ArrowArrayAppendString(xdbc_column_def_col, str), error);
    } else {
      CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_column_def_col, 1), error);
    }

    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_sql_data_type_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_datetime_sub_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_char_octet_length_col, 1), error);

    if (col_notnull == 0) {
      str.data = "YES";
      str.n_bytes = 3;
    } else {
      str.data = "NO";
      str.n_bytes = 2;
    }
    CHECK_NA(INTERNAL, ArrowArrayAppendString(xdbc_is_nullable_col, str), error);

    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_scope_catalog_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_scope_schema_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_scope_table_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_is_autoincrement_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_is_generatedcolumn_col, 1), error);

    CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_columns_items), error);
  }

  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionGetConstraintsImpl(
    struct SqliteConnection* conn, const char* table_name, const char* column_name,
    struct ArrowArray* table_constraints_col, sqlite3_stmt* pk_stmt,
    sqlite3_stmt* fk_stmt, struct AdbcError* error) {
  struct ArrowArray* table_constraints_items = table_constraints_col->children[0];
  struct ArrowArray* constraint_name_col = table_constraints_items->children[0];
  struct ArrowArray* constraint_type_col = table_constraints_items->children[1];
  struct ArrowArray* constraint_column_names_col = table_constraints_items->children[2];
  struct ArrowArray* constraint_column_names_items =
      constraint_column_names_col->children[0];
  struct ArrowArray* constraint_column_usage_col = table_constraints_items->children[3];
  struct ArrowArray* constraint_column_usage_items =
      constraint_column_usage_col->children[0];
  struct ArrowArray* fk_catalog_col = constraint_column_usage_items->children[0];
  struct ArrowArray* fk_db_schema_col = constraint_column_usage_items->children[1];
  struct ArrowArray* fk_table_col = constraint_column_usage_items->children[2];
  struct ArrowArray* fk_column_name_col = constraint_column_usage_items->children[3];

  // We can get primary keys and foreign keys, but not unique
  // constraints (unless we parse the SQL table definition)

  int rc = sqlite3_reset(pk_stmt);
  RAISE(INTERNAL, rc == SQLITE_OK, sqlite3_errmsg(conn->conn), error);

  rc = sqlite3_bind_text64(pk_stmt, 1, table_name, strlen(table_name), SQLITE_STATIC,
                           SQLITE_UTF8);
  RAISE(INTERNAL, rc == SQLITE_OK, sqlite3_errmsg(conn->conn), error);

  char has_primary_key = 0;
  while ((rc = sqlite3_step(pk_stmt)) == SQLITE_ROW) {
    if (!has_primary_key) {
      has_primary_key = 1;
      CHECK_NA(INTERNAL, ArrowArrayAppendNull(constraint_name_col, 1), error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(constraint_name_col, ArrowCharView("PRIMARY KEY")),
               error);
    }
    CHECK_NA(
        INTERNAL,
        ArrowArrayAppendString(
            constraint_column_names_items,
            (struct ArrowStringView){.data = (const char*)sqlite3_column_text(pk_stmt, 0),
                                     .n_bytes = sqlite3_column_bytes(pk_stmt, 0)}),
        error);
  }
  if (has_primary_key) {
    CHECK_NA(INTERNAL, ArrowArrayFinishElement(constraint_column_names_col), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(constraint_column_usage_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_constraints_items), error);
  }

  rc = sqlite3_reset(fk_stmt);
  RAISE(INTERNAL, rc == SQLITE_OK, sqlite3_errmsg(conn->conn), error);

  rc = sqlite3_bind_text64(fk_stmt, 1, table_name, strlen(table_name), SQLITE_STATIC,
                           SQLITE_UTF8);
  RAISE(INTERNAL, rc == SQLITE_OK, sqlite3_errmsg(conn->conn), error);

  int prev_fk_id = -1;
  while ((rc = sqlite3_step(fk_stmt)) == SQLITE_ROW) {
    const int fk_id = sqlite3_column_int(fk_stmt, 0);
    const int fk_seq = sqlite3_column_int(fk_stmt, 1);
    const char* to_table = (const char*)sqlite3_column_text(fk_stmt, 2);
    const char* from_col = (const char*)sqlite3_column_text(fk_stmt, 3);
    const char* to_col = (const char*)sqlite3_column_text(fk_stmt, 4);

    if (fk_id != prev_fk_id) {
      CHECK_NA(INTERNAL, ArrowArrayAppendNull(constraint_name_col, 1), error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(constraint_name_col, ArrowCharView("FOREIGN KEY")),
               error);

      if (prev_fk_id != -1) {
        CHECK_NA(INTERNAL, ArrowArrayFinishElement(constraint_column_names_col), error);
        CHECK_NA(INTERNAL, ArrowArrayFinishElement(constraint_column_usage_col), error);
        CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_constraints_items), error);
      }
      prev_fk_id = fk_id;

      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(
                   constraint_column_names_items,
                   (struct ArrowStringView){.data = from_col,
                                            .n_bytes = sqlite3_column_bytes(pk_stmt, 3)}),
               error);
      CHECK_NA(INTERNAL, ArrowArrayAppendString(fk_catalog_col, ArrowCharView("main")),
               error);
      CHECK_NA(INTERNAL, ArrowArrayAppendNull(fk_db_schema_col, 1), error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(
                   fk_table_col,
                   (struct ArrowStringView){.data = to_table,
                                            .n_bytes = sqlite3_column_bytes(pk_stmt, 2)}),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(
                   fk_column_name_col,
                   (struct ArrowStringView){.data = to_col,
                                            .n_bytes = sqlite3_column_bytes(pk_stmt, 4)}),
               error);
    }
  }
  if (prev_fk_id != -1) {
    CHECK_NA(INTERNAL, ArrowArrayFinishElement(constraint_column_names_col), error);
    CHECK_NA(INTERNAL, ArrowArrayFinishElement(constraint_column_usage_col), error);
    CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_constraints_items), error);
  }

  return ADBC_STATUS_OK;
}  // NOLINT(whitespace/indent)

AdbcStatusCode SqliteConnectionGetTablesInner(
    struct SqliteConnection* conn, sqlite3_stmt* tables_stmt, sqlite3_stmt* columns_stmt,
    sqlite3_stmt* pk_stmt, sqlite3_stmt* fk_stmt, const char** table_type,
    const char* column_name, struct ArrowArray* db_schema_tables_col,
    struct AdbcError* error) {
  struct ArrowArray* db_schema_tables_items = db_schema_tables_col->children[0];
  struct ArrowArray* table_name_col = db_schema_tables_items->children[0];
  struct ArrowArray* table_type_col = db_schema_tables_items->children[1];
  struct ArrowArray* table_columns_col = db_schema_tables_items->children[2];
  struct ArrowArray* table_constraints_col = db_schema_tables_items->children[3];

  int rc = SQLITE_OK;
  while ((rc = sqlite3_step(tables_stmt)) == SQLITE_ROW) {
    const char* cur_table_type = (const char*)sqlite3_column_text(tables_stmt, 1);

    if (table_type) {
      const char** current = table_type;
      char found = 0;
      while (*current) {
        if (strcmp(*current, cur_table_type) == 0) {
          found = 1;
          break;
        }
        current++;
      }
      if (!found) continue;
    }

    struct ArrowStringView str = {.data = cur_table_type,
                                  .n_bytes = sqlite3_column_bytes(tables_stmt, 1)};
    CHECK_NA(INTERNAL, ArrowArrayAppendString(table_type_col, str), error);

    const char* cur_table = (const char*)sqlite3_column_text(tables_stmt, 0);
    str.data = cur_table;
    str.n_bytes = sqlite3_column_bytes(tables_stmt, 0);
    CHECK_NA(INTERNAL, ArrowArrayAppendString(table_name_col, str), error);

    if (columns_stmt == NULL) {
      CHECK_NA(INTERNAL, ArrowArrayAppendNull(table_columns_col, 1), error);
      CHECK_NA(INTERNAL, ArrowArrayAppendNull(table_constraints_col, 1), error);
    } else {
      // XXX: n + 1 query pattern. You can join on a pragma so we
      // could avoid this in principle but it complicates the
      // unpacking code here quite a bit, so ignore for now.
      RAISE_ADBC(SqliteConnectionGetColumnsImpl(conn, cur_table, column_name,
                                                table_columns_col, columns_stmt, error));
      // Not strictly necessary, but we passed SQLITE_STATIC when
      // binding so don't let the reference leak
      (void)sqlite3_clear_bindings(columns_stmt);
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_columns_col), error);

      RAISE_ADBC(SqliteConnectionGetConstraintsImpl(
          conn, cur_table, column_name, table_constraints_col, pk_stmt, fk_stmt, error));
      (void)sqlite3_clear_bindings(pk_stmt);
      (void)sqlite3_clear_bindings(fk_stmt);
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_constraints_col), error);
    }
    CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_tables_items), error);
  }

  if (rc != SQLITE_DONE) {
    SetError(error, "Failed to query for tables: %s", sqlite3_errmsg(conn->conn));
    return ADBC_STATUS_INTERNAL;
  }
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_tables_col), error);
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionGetTablesImpl(struct SqliteConnection* conn, int depth,
                                             const char* table_name,
                                             const char** table_type,
                                             const char* column_name,
                                             struct ArrowArray* db_schema_tables_col,
                                             struct AdbcError* error) {
  sqlite3_stmt* tables_stmt = NULL;
  sqlite3_stmt* columns_stmt = NULL;
  sqlite3_stmt* pk_stmt = NULL;
  sqlite3_stmt* fk_stmt = NULL;
  int rc = SQLITE_OK;

  if (rc == SQLITE_OK) {
    rc = sqlite3_prepare_v2(conn->conn, kTableQuery, sizeof(kTableQuery), &tables_stmt,
                            /*pzTail=*/NULL);
  }
  if (rc == SQLITE_OK && depth == ADBC_OBJECT_DEPTH_COLUMNS) {
    rc = sqlite3_prepare_v2(conn->conn, kColumnQuery, sizeof(kColumnQuery), &columns_stmt,
                            /*pzTail=*/NULL);
  }
  if (rc == SQLITE_OK && depth == ADBC_OBJECT_DEPTH_COLUMNS) {
    rc = sqlite3_prepare_v2(conn->conn, kPrimaryKeyQuery, sizeof(kPrimaryKeyQuery),
                            &pk_stmt, /*pzTail=*/NULL);
  }
  if (rc == SQLITE_OK && depth == ADBC_OBJECT_DEPTH_COLUMNS) {
    rc = sqlite3_prepare_v2(conn->conn, kForeignKeyQuery, sizeof(kForeignKeyQuery),
                            &fk_stmt, /*pzTail=*/NULL);
  }
  if (rc == SQLITE_OK) {
    if (table_name) {
      rc = sqlite3_bind_text64(tables_stmt, 1, table_name, strlen(table_name),
                               SQLITE_STATIC, SQLITE_UTF8);
    } else {
      rc = sqlite3_bind_text64(tables_stmt, 1, "%", 1, SQLITE_STATIC, SQLITE_UTF8);
    }
  }

  AdbcStatusCode status = ADBC_STATUS_OK;
  if (rc == SQLITE_OK) {
    status = SqliteConnectionGetTablesInner(conn, tables_stmt, columns_stmt, pk_stmt,
                                            fk_stmt, table_type, column_name,
                                            db_schema_tables_col, error);
  } else {
    SetError(error, "Failed to query for tables: %s", sqlite3_errmsg(conn->conn));
    status = ADBC_STATUS_INTERNAL;
  }

  sqlite3_finalize(tables_stmt);
  sqlite3_finalize(columns_stmt);
  sqlite3_finalize(pk_stmt);
  sqlite3_finalize(fk_stmt);
  return status;
}

AdbcStatusCode SqliteConnectionGetObjectsImpl(
    struct SqliteConnection* conn, int depth, const char* catalog, const char* db_schema,
    const char* table_name, const char** table_type, const char* column_name,
    struct ArrowSchema* schema, struct ArrowArray* array, struct AdbcError* error) {
  RAISE_ADBC(SqliteConnectionGetObjectsSchema(schema, error));

  struct ArrowError na_error = {0};
  CHECK_NA_DETAIL(INTERNAL, ArrowArrayInitFromSchema(array, schema, &na_error), &na_error,
                  error);
  CHECK_NA(INTERNAL, ArrowArrayStartAppending(array), error);

  struct ArrowArray* catalog_name_col = array->children[0];
  struct ArrowArray* catalog_db_schemas_col = array->children[1];

  struct ArrowArray* catalog_db_schemas_items = catalog_db_schemas_col->children[0];
  struct ArrowArray* db_schema_name_col = catalog_db_schemas_items->children[0];
  struct ArrowArray* db_schema_tables_col = catalog_db_schemas_items->children[1];

  // TODO: support proper filters
  if (!catalog || strcmp(catalog, "main") == 0) {
    // Default the primary catalog to "main"
    // https://www.sqlite.org/cli.html
    // > The ".databases" command shows a list of all databases open
    // > in the current connection. There will always be at least
    // > 2. The first one is "main", the original database opened.

    CHECK_NA(INTERNAL, ArrowArrayAppendString(catalog_name_col, ArrowCharView("main")),
             error);

    if (depth == ADBC_OBJECT_DEPTH_CATALOGS) {
      CHECK_NA(INTERNAL, ArrowArrayAppendNull(catalog_db_schemas_col, 1), error);
    } else if (!db_schema || db_schema == NULL) {
      // For our purposes, we'll consider SQLite to always have a
      // single, unnamed schema within each catalog.
      CHECK_NA(INTERNAL, ArrowArrayAppendNull(db_schema_name_col, 1), error);
      if (depth == ADBC_OBJECT_DEPTH_DB_SCHEMAS) {
        CHECK_NA(INTERNAL, ArrowArrayAppendNull(db_schema_tables_col, 1), error);
      } else {
        RAISE_ADBC(SqliteConnectionGetTablesImpl(conn, depth, table_name, table_type,
                                                 column_name, db_schema_tables_col,
                                                 error));
      }
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(catalog_db_schemas_items), error);
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(catalog_db_schemas_col), error);
    } else {
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(catalog_db_schemas_col), error);
    }
    CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);

    // TODO: implement "temp", other attached databases as catalogs
  }

  CHECK_NA_DETAIL(INTERNAL, ArrowArrayFinishBuilding(array, &na_error), &na_error, error);
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                          const char* catalog, const char* db_schema,
                                          const char* table_name, const char** table_type,
                                          const char* column_name,
                                          struct ArrowArrayStream* out,
                                          struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;

  struct ArrowSchema schema = {0};
  struct ArrowArray array = {0};

  AdbcStatusCode status =
      SqliteConnectionGetObjectsImpl(conn, depth, catalog, db_schema, table_name,
                                     table_type, column_name, &schema, &array, error);
  if (status != ADBC_STATUS_OK) {
    if (schema.release) schema.release(&schema);
    if (array.release) array.release(&array);
    return status;
  }

  return BatchToArrayStream(&array, &schema, out, error);
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
  AdbcStatusCode status = AdbcSqliteExportReader(conn->conn, stmt, /*binder=*/NULL,
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

  CHECK_NA(INTERNAL, ArrowArrayAppendString(array->children[0], ArrowCharView("table")),
           error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);
  CHECK_NA(INTERNAL, ArrowArrayAppendString(array->children[0], ArrowCharView("view")),
           error);
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
  AdbcSqliteBinderRelease(&stmt->binder);
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
      return ADBC_STATUS_INVALID_ARGUMENT;
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
      status =
          AdbcSqliteBinderBindNext(&stmt->binder, stmt->conn, insert, &finished, error);
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
  AdbcSqliteBinderRelease(&stmt->binder);
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
      SetError(error, "Parameter count mismatch: expected %" PRId64 " but found %" PRId64,
               expected, actual);
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
        status = AdbcSqliteBinderBindNext(&stmt->binder, stmt->conn, stmt->stmt,
                                          &finished, error);
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

    AdbcSqliteBinderRelease(&stmt->binder);
    if (rows_affected) *rows_affected = rows;
    return status;
  }

  // Query
  if (rows_affected) *rows_affected = -1;
  struct AdbcSqliteBinder* binder = stmt->binder.schema.release ? &stmt->binder : NULL;
  return AdbcSqliteExportReader(stmt->conn, stmt->stmt, binder, stmt->batch_size, out,
                                error);
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
  return AdbcSqliteBinderSetArray(&stmt->binder, values, schema, error);
}

AdbcStatusCode SqliteStatementBindStream(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* stream,
                                         struct AdbcError* error) {
  CHECK_STMT_INIT(statement, error);
  struct SqliteStatement* stmt = (struct SqliteStatement*)statement->private_data;
  return AdbcSqliteBinderSetArrayStream(&stmt->binder, stream, error);
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
  } else if (strcmp(key, kStatementOptionBatchRows) == 0) {
    char* end = NULL;
    long batch_size = strtol(value, &end, /*base=*/10);  // NOLINT(runtime/int)
    if (errno != 0) {
      SetError(error, "Invalid statement option value %s=%s (out of range)", key, value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    } else if (batch_size <= 0) {
      SetError(error,
               "Invalid statement option value %s=%s (value is non-positive or invalid)",
               key, value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    } else if (batch_size > (long)INT_MAX) {  // NOLINT(runtime/int)
      SetError(error,
               "Invalid statement option value %s=%s (value is out of range of int)", key,
               value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    stmt->batch_size = (int)batch_size;
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

AdbcStatusCode SqliteDriverInit(int version, void* raw_driver, struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0) {
    SetError(error, "Only version %d supported, got %d", ADBC_VERSION_1_0_0, version);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  struct AdbcDriver* driver = (struct AdbcDriver*)raw_driver;
  memset(driver, 0, sizeof(*driver));
  driver->DatabaseInit = SqliteDatabaseInit;
  driver->DatabaseNew = SqliteDatabaseNew;
  driver->DatabaseRelease = SqliteDatabaseRelease;
  driver->DatabaseSetOption = SqliteDatabaseSetOption;

  driver->ConnectionCommit = SqliteConnectionCommit;
  driver->ConnectionGetInfo = SqliteConnectionGetInfo;
  driver->ConnectionGetObjects = SqliteConnectionGetObjects;
  driver->ConnectionGetTableSchema = SqliteConnectionGetTableSchema;
  driver->ConnectionGetTableTypes = SqliteConnectionGetTableTypes;
  driver->ConnectionInit = SqliteConnectionInit;
  driver->ConnectionNew = SqliteConnectionNew;
  driver->ConnectionReadPartition = SqliteConnectionReadPartition;
  driver->ConnectionRelease = SqliteConnectionRelease;
  driver->ConnectionRollback = SqliteConnectionRollback;
  driver->ConnectionSetOption = SqliteConnectionSetOption;

  driver->StatementBind = SqliteStatementBind;
  driver->StatementBindStream = SqliteStatementBindStream;
  driver->StatementExecuteQuery = SqliteStatementExecuteQuery;
  driver->StatementGetParameterSchema = SqliteStatementGetParameterSchema;
  driver->StatementNew = SqliteStatementNew;
  driver->StatementPrepare = SqliteStatementPrepare;
  driver->StatementRelease = SqliteStatementRelease;
  driver->StatementSetOption = SqliteStatementSetOption;
  driver->StatementSetSqlQuery = SqliteStatementSetSqlQuery;
  return ADBC_STATUS_OK;
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

AdbcStatusCode AdbcDriverInit(int version, void* driver, struct AdbcError* error) {
  return SqliteDriverInit(version, driver, error);
}
