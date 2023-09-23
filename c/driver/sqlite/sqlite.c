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

#include <nanoarrow/nanoarrow.h>
#include <sqlite3.h>

#include "common/options.h"
#include "common/utils.h"
#include "statement_reader.h"
#include "types.h"

static const char kDefaultUri[] = "file:adbc_driver_sqlite?mode=memory&cache=shared";
// The batch size for query results (and for initial type inference)
static const char kStatementOptionBatchRows[] = "adbc.sqlite.query.batch_rows";
static const uint32_t kSupportedInfoCodes[] = {
    ADBC_INFO_VENDOR_NAME,    ADBC_INFO_VENDOR_VERSION,       ADBC_INFO_DRIVER_NAME,
    ADBC_INFO_DRIVER_VERSION, ADBC_INFO_DRIVER_ARROW_VERSION,
};

// Private names (to avoid conflicts when using the driver manager)

#define CHECK_DB_INIT(NAME, ERROR)                                      \
  if (!NAME->private_data) {                                            \
    SetError(ERROR, "[SQLite] %s: database not initialized", __func__); \
    return ADBC_STATUS_INVALID_STATE;                                   \
  }
#define CHECK_CONN_INIT(NAME, ERROR)                                      \
  if (!NAME->private_data) {                                              \
    SetError(ERROR, "[SQLite] %s: connection not initialized", __func__); \
    return ADBC_STATUS_INVALID_STATE;                                     \
  }
#define CHECK_STMT_INIT(NAME, ERROR)                                     \
  if (!NAME->private_data) {                                             \
    SetError(ERROR, "[SQLite] %s: statement not initialized", __func__); \
    return ADBC_STATUS_INVALID_STATE;                                    \
  }

AdbcStatusCode SqliteDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  if (database->private_data) {
    SetError(error, "[SQLite] AdbcDatabaseNew: database already allocated");
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
  SetError(error, "[SQLite] Unknown database option %s=%s", key,
           value ? value : "(NULL)");
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteDatabaseSetOptionBytes(struct AdbcDatabase* database,
                                            const char* key, const uint8_t* value,
                                            size_t length, struct AdbcError* error) {
  CHECK_DB_INIT(database, error);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteDatabaseSetOptionDouble(struct AdbcDatabase* database,
                                             const char* key, double value,
                                             struct AdbcError* error) {
  CHECK_DB_INIT(database, error);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteDatabaseSetOptionInt(struct AdbcDatabase* database, const char* key,
                                          int64_t value, struct AdbcError* error) {
  CHECK_DB_INIT(database, error);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

int OpenDatabase(const char* maybe_uri, sqlite3** db, struct AdbcError* error) {
  const char* uri = maybe_uri ? maybe_uri : kDefaultUri;
  int rc = sqlite3_open_v2(uri, db,
                           SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                           /*zVfs=*/NULL);
  if (rc != SQLITE_OK) {
    if (*db) {
      SetError(error, "[SQLite] Failed to open %s: %s", uri, sqlite3_errmsg(*db));
    } else {
      SetError(error, "[SQLite] Failed to open %s: failed to allocate memory", uri);
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
    SetError(error, "[SQLite] Failed to execute query \"%s\": %s", query,
             sqlite3_errmsg(conn->conn));
    return ADBC_STATUS_INTERNAL;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteDatabaseGetOption(struct AdbcDatabase* database, const char* key,
                                       char* value, size_t* length,
                                       struct AdbcError* error) {
  CHECK_DB_INIT(database, error);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode SqliteDatabaseGetOptionBytes(struct AdbcDatabase* database,
                                            const char* key, uint8_t* value,
                                            size_t* length, struct AdbcError* error) {
  CHECK_DB_INIT(database, error);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode SqliteDatabaseGetOptionDouble(struct AdbcDatabase* database,
                                             const char* key, double* value,
                                             struct AdbcError* error) {
  CHECK_DB_INIT(database, error);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode SqliteDatabaseGetOptionInt(struct AdbcDatabase* database, const char* key,
                                          int64_t* value, struct AdbcError* error) {
  CHECK_DB_INIT(database, error);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode SqliteDatabaseInit(struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  CHECK_DB_INIT(database, error);
  struct SqliteDatabase* db = (struct SqliteDatabase*)database->private_data;

  if (db->db) {
    SetError(error, "[SQLite] AdbcDatabaseInit: database already initialized");
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
      SetError(error, "[SQLite] AdbcDatabaseRelease: connection is busy");
      return ADBC_STATUS_IO;
    }
  }
  free(database->private_data);
  database->private_data = NULL;

  if (connection_count > 0) {
    // -Wpedantic gives a warning if we use size_t in a printf() context
    SetError(error, "[SQLite] AdbcDatabaseRelease: %ld open connections when released",
             (long)connection_count);  // NOLINT(runtime/int)
    return ADBC_STATUS_INVALID_STATE;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionNew(struct AdbcConnection* connection,
                                   struct AdbcError* error) {
  if (connection->private_data) {
    SetError(error, "[SQLite] AdbcConnectionNew: connection already allocated");
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
      SetError(error, "[SQLite] Invalid connection option value %s=%s", key, value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return ADBC_STATUS_OK;
  }
  SetError(error, "[SQLite] Unknown connection option %s=%s", key,
           value ? value : "(NULL)");
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteConnectionSetOptionBytes(struct AdbcConnection* connection,
                                              const char* key, const uint8_t* value,
                                              size_t length, struct AdbcError* error) {
  CHECK_DB_INIT(connection, error);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteConnectionSetOptionDouble(struct AdbcConnection* connection,
                                               const char* key, double value,
                                               struct AdbcError* error) {
  CHECK_DB_INIT(connection, error);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteConnectionSetOptionInt(struct AdbcConnection* connection,
                                            const char* key, int64_t value,
                                            struct AdbcError* error) {
  CHECK_DB_INIT(connection, error);
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
    SetError(error, "[SQLite] AdbcConnectionInit: connection already initialized");
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
      SetError(error, "[SQLite] AdbcConnectionRelease: connection is busy");
      return ADBC_STATUS_IO;
    }
  }
  free(connection->private_data);
  connection->private_data = NULL;

  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionGetInfoImpl(const uint32_t* info_codes,
                                           size_t info_codes_length,
                                           struct ArrowSchema* schema,
                                           struct ArrowArray* array,
                                           struct AdbcError* error) {
  RAISE_ADBC(AdbcInitConnectionGetInfoSchema(info_codes, info_codes_length, schema, array,
                                             error));
  for (size_t i = 0; i < info_codes_length; i++) {
    switch (info_codes[i]) {
      case ADBC_INFO_VENDOR_NAME:
        RAISE_ADBC(
            AdbcConnectionGetInfoAppendString(array, info_codes[i], "SQLite", error));
        break;
      case ADBC_INFO_VENDOR_VERSION:
        RAISE_ADBC(AdbcConnectionGetInfoAppendString(array, info_codes[i],
                                                     sqlite3_libversion(), error));
        break;
      case ADBC_INFO_DRIVER_NAME:
        RAISE_ADBC(AdbcConnectionGetInfoAppendString(array, info_codes[i],
                                                     "ADBC SQLite Driver", error));
        break;
      case ADBC_INFO_DRIVER_VERSION:
        // TODO(lidavidm): fill in driver version
        RAISE_ADBC(
            AdbcConnectionGetInfoAppendString(array, info_codes[i], "(unknown)", error));
        break;
      case ADBC_INFO_DRIVER_ARROW_VERSION:
        RAISE_ADBC(AdbcConnectionGetInfoAppendString(array, info_codes[i],
                                                     NANOARROW_VERSION, error));
        break;
      default:
        // Ignore
        continue;
    }
    CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);
  }

  struct ArrowError na_error = {0};
  CHECK_NA_DETAIL(INTERNAL, ArrowArrayFinishBuildingDefault(array, &na_error), &na_error,
                  error);

  return ADBC_STATUS_OK;
}  // NOLINT(whitespace/indent)

AdbcStatusCode SqliteConnectionGetInfo(struct AdbcConnection* connection,
                                       const uint32_t* info_codes,
                                       size_t info_codes_length,
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
                                  .size_bytes = sqlite3_column_bytes(stmt, 1)};
    CHECK_NA(INTERNAL, ArrowArrayAppendString(column_name_col, str), error);

    const int32_t col_cid = sqlite3_column_int(stmt, 0);
    CHECK_NA(INTERNAL, ArrowArrayAppendInt(ordinal_position_col, col_cid + 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(remarks_col, 1), error);

    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_data_type_col, 1), error);

    const char* col_type = (const char*)sqlite3_column_text(stmt, 2);
    if (col_type) {
      str.data = col_type;
      str.size_bytes = sqlite3_column_bytes(stmt, 2);
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
      str.size_bytes = sqlite3_column_bytes(stmt, 4);
      CHECK_NA(INTERNAL, ArrowArrayAppendString(xdbc_column_def_col, str), error);
    } else {
      CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_column_def_col, 1), error);
    }

    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_sql_data_type_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_datetime_sub_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_char_octet_length_col, 1), error);

    if (col_notnull == 0) {
      str.data = "YES";
      str.size_bytes = 3;
    } else {
      str.data = "NO";
      str.size_bytes = 2;
    }
    CHECK_NA(INTERNAL, ArrowArrayAppendString(xdbc_is_nullable_col, str), error);

    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_scope_catalog_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_scope_schema_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_scope_table_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_is_autoincrement_col, 1), error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(xdbc_is_generatedcolumn_col, 1), error);

    CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_columns_items), error);
  }
  RAISE(INTERNAL, rc == SQLITE_DONE, sqlite3_errmsg(conn->conn), error);

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
               ArrowArrayAppendString(constraint_type_col, ArrowCharView("PRIMARY KEY")),
               error);
    }
    CHECK_NA(
        INTERNAL,
        ArrowArrayAppendString(
            constraint_column_names_items,
            (struct ArrowStringView){.data = (const char*)sqlite3_column_text(pk_stmt, 0),
                                     .size_bytes = sqlite3_column_bytes(pk_stmt, 0)}),
        error);
  }
  RAISE(INTERNAL, rc == SQLITE_DONE, sqlite3_errmsg(conn->conn), error);
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
    // Foreign key seq is sqlite3_column_int(fk_stmt, 1);
    const char* to_table = (const char*)sqlite3_column_text(fk_stmt, 2);
    const char* from_col = (const char*)sqlite3_column_text(fk_stmt, 3);
    const char* to_col = (const char*)sqlite3_column_text(fk_stmt, 4);

    // New foreign key constraint or -constraint sets
    if (fk_id != prev_fk_id) {
      // Not first constraint of the table
      if (prev_fk_id != -1) {
        CHECK_NA(INTERNAL, ArrowArrayFinishElement(constraint_column_names_col), error);
        CHECK_NA(INTERNAL, ArrowArrayFinishElement(constraint_column_usage_col), error);
        CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_constraints_items), error);
      }
      prev_fk_id = fk_id;

      CHECK_NA(INTERNAL, ArrowArrayAppendNull(constraint_name_col, 1), error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(constraint_type_col, ArrowCharView("FOREIGN KEY")),
               error);
    }
    CHECK_NA(INTERNAL,
             ArrowArrayAppendString(
                 constraint_column_names_items,
                 (struct ArrowStringView){
                     .data = from_col, .size_bytes = sqlite3_column_bytes(fk_stmt, 3)}),
             error);
    CHECK_NA(INTERNAL, ArrowArrayAppendString(fk_catalog_col, ArrowCharView("main")),
             error);
    CHECK_NA(INTERNAL, ArrowArrayAppendNull(fk_db_schema_col, 1), error);
    CHECK_NA(INTERNAL,
             ArrowArrayAppendString(
                 fk_table_col,
                 (struct ArrowStringView){
                     .data = to_table, .size_bytes = sqlite3_column_bytes(fk_stmt, 2)}),
             error);
    CHECK_NA(INTERNAL,
             ArrowArrayAppendString(
                 fk_column_name_col,
                 (struct ArrowStringView){
                     .data = to_col, .size_bytes = sqlite3_column_bytes(fk_stmt, 4)}),
             error);

    CHECK_NA(INTERNAL, ArrowArrayFinishElement(constraint_column_usage_items), error);
  }
  RAISE(INTERNAL, rc == SQLITE_DONE, sqlite3_errmsg(conn->conn), error);
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
                                  .size_bytes = sqlite3_column_bytes(tables_stmt, 1)};
    CHECK_NA(INTERNAL, ArrowArrayAppendString(table_type_col, str), error);

    const char* cur_table = (const char*)sqlite3_column_text(tables_stmt, 0);
    str.data = cur_table;
    str.size_bytes = sqlite3_column_bytes(tables_stmt, 0);
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
    SetError(error, "[SQLite] Failed to query for tables: %s",
             sqlite3_errmsg(conn->conn));
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
    SetError(error, "[SQLite] Failed to query for tables: %s",
             sqlite3_errmsg(conn->conn));
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
  RAISE_ADBC(AdbcInitConnectionObjectsSchema(schema, error));

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

  CHECK_NA_DETAIL(INTERNAL, ArrowArrayFinishBuildingDefault(array, &na_error), &na_error,
                  error);
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

AdbcStatusCode SqliteConnectionGetOption(struct AdbcConnection* connection,
                                         const char* key, char* value, size_t* length,
                                         struct AdbcError* error) {
  CHECK_DB_INIT(connection, error);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode SqliteConnectionGetOptionBytes(struct AdbcConnection* connection,
                                              const char* key, uint8_t* value,
                                              size_t* length, struct AdbcError* error) {
  CHECK_DB_INIT(connection, error);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode SqliteConnectionGetOptionDouble(struct AdbcConnection* connection,
                                               const char* key, double* value,
                                               struct AdbcError* error) {
  CHECK_DB_INIT(connection, error);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode SqliteConnectionGetOptionInt(struct AdbcConnection* connection,
                                            const char* key, int64_t* value,
                                            struct AdbcError* error) {
  CHECK_DB_INIT(connection, error);
  return ADBC_STATUS_NOT_FOUND;
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
    SetError(error, "[SQLite] AdbcConnectionGetTableSchema: must provide table_name");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  sqlite3_str* query = sqlite3_str_new(NULL);
  if (sqlite3_str_errcode(query)) {
    SetError(error, "[SQLite] %s", sqlite3_errmsg(conn->conn));
    return ADBC_STATUS_INTERNAL;
  }

  sqlite3_str_appendf(query, "%s%Q", "SELECT * FROM ", table_name);
  if (sqlite3_str_errcode(query)) {
    SetError(error, "[SQLite] %s", sqlite3_errmsg(conn->conn));
    sqlite3_free(sqlite3_str_finish(query));
    return ADBC_STATUS_INTERNAL;
  }

  sqlite3_stmt* stmt = NULL;
  int rc = sqlite3_prepare_v2(conn->conn, sqlite3_str_value(query),
                              sqlite3_str_length(query), &stmt, /*pzTail=*/NULL);
  sqlite3_free(sqlite3_str_finish(query));
  if (rc != SQLITE_OK) {
    SetError(error, "[SQLite] GetTableSchema: %s", sqlite3_errmsg(conn->conn));
    return ADBC_STATUS_NOT_FOUND;
  }

  struct ArrowArrayStream stream = {0};
  AdbcStatusCode status = AdbcSqliteExportReader(conn->conn, stmt, /*binder=*/NULL,
                                                 /*batch_size=*/64, &stream, error);
  if (status == ADBC_STATUS_OK) {
    int code = stream.get_schema(&stream, schema);
    if (code != 0) {
      SetError(error, "[SQLite] Failed to get schema: (%d) %s", code, strerror(code));
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
  ArrowSchemaInit(schema);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(schema, NANOARROW_TYPE_STRUCT), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(schema, /*num_columns=*/1), error);
  ArrowSchemaInit(schema->children[0]);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_STRING),
           error);
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

  CHECK_NA(INTERNAL, ArrowArrayFinishBuildingDefault(array, NULL), error);
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionGetTableTypes(struct AdbcConnection* connection,
                                             struct ArrowArrayStream* out,
                                             struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);

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
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteConnectionCommit(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  CHECK_CONN_INIT(connection, error);
  struct SqliteConnection* conn = (struct SqliteConnection*)connection->private_data;
  if (!conn->active_transaction) {
    SetError(error, "[SQLite] No active transaction, cannot commit");
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
    SetError(error, "[SQLite] No active transaction, cannot rollback");
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
    SetError(error, "[SQLite] AdbcStatementNew: statement already allocated");
    return ADBC_STATUS_INVALID_STATE;
  } else if (!conn->conn) {
    SetError(error, "[SQLite] AdbcStatementNew: connection is not initialized");
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
  if (stmt->target_catalog) free(stmt->target_catalog);
  if (stmt->target_table) free(stmt->target_table);
  if (rc != SQLITE_OK) {
    SetError(error,
             "[SQLite] AdbcStatementRelease: statement failed to finalize: (%d) %s", rc,
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
    SetError(error, "[SQLite] Must SetSqlQuery before ExecuteQuery or Prepare");
    return ADBC_STATUS_INVALID_STATE;
  }
  if (stmt->prepared == 0) {
    if (stmt->stmt) {
      int rc = sqlite3_finalize(stmt->stmt);
      stmt->stmt = NULL;
      if (rc != SQLITE_OK) {
        SetError(error, "[SQLite] Failed to finalize previous statement: (%d) %s", rc,
                 sqlite3_errmsg(stmt->conn));
        return ADBC_STATUS_IO;
      }
    }

    int rc =
        sqlite3_prepare_v2(stmt->conn, stmt->query, (int)stmt->query_len, &stmt->stmt,
                           /*pzTail=*/NULL);
    if (rc != SQLITE_OK) {
      SetError(error, "[SQLite] Failed to prepare query: %s\nQuery:%s",
               sqlite3_errmsg(stmt->conn), stmt->query);
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
  sqlite3_str* create_query = NULL;
  sqlite3_str* insert_query = NULL;
  char* table = NULL;

  create_query = sqlite3_str_new(NULL);
  if (sqlite3_str_errcode(create_query)) {
    SetError(error, "[SQLite] %s", sqlite3_errmsg(stmt->conn));
    code = ADBC_STATUS_INTERNAL;
    goto cleanup;
  }

  insert_query = sqlite3_str_new(NULL);
  if (sqlite3_str_errcode(insert_query)) {
    SetError(error, "[SQLite] %s", sqlite3_errmsg(stmt->conn));
    code = ADBC_STATUS_INTERNAL;
    goto cleanup;
  }

  if (stmt->target_catalog != NULL && stmt->temporary != 0) {
    SetError(error, "[SQLite] Cannot specify both %s and %s",
             ADBC_INGEST_OPTION_TARGET_CATALOG, ADBC_INGEST_OPTION_TEMPORARY);
    code = ADBC_STATUS_INVALID_STATE;
    goto cleanup;
  }

  if (stmt->target_catalog != NULL) {
    table = sqlite3_mprintf("\"%w\" . \"%w\"", stmt->target_catalog, stmt->target_table);
  } else if (stmt->temporary == 0) {
    // If not temporary, explicitly target the main database
    table = sqlite3_mprintf("main . \"%w\"", stmt->target_table);
  } else {
    // OK to be redundant (CREATE TEMP TABLE temp.foo)
    table = sqlite3_mprintf("temp . \"%w\"", stmt->target_table);
  }

  if (table == NULL) {
    // Allocation failure
    code = ADBC_STATUS_INTERNAL;
    goto cleanup;
  }

  if (stmt->temporary != 0) {
    sqlite3_str_appendf(create_query, "CREATE TEMPORARY TABLE %s (", table);
  } else {
    sqlite3_str_appendf(create_query, "CREATE TABLE %s (", table);
  }
  if (sqlite3_str_errcode(create_query)) {
    SetError(error, "[SQLite] Failed to build CREATE: %s", sqlite3_errmsg(stmt->conn));
    code = ADBC_STATUS_INTERNAL;
    goto cleanup;
  }

  sqlite3_str_appendf(insert_query, "INSERT INTO %s VALUES (", table);
  if (sqlite3_str_errcode(insert_query)) {
    SetError(error, "[SQLite] Failed to build INSERT: %s", sqlite3_errmsg(stmt->conn));
    code = ADBC_STATUS_INTERNAL;
    goto cleanup;
  }

  struct ArrowError arrow_error = {0};
  struct ArrowSchemaView view = {0};
  for (int i = 0; i < stmt->binder.schema.n_children; i++) {
    if (i > 0) {
      sqlite3_str_appendf(create_query, "%s", ", ");
      if (sqlite3_str_errcode(create_query)) {
        SetError(error, "[SQLite] Failed to build CREATE: %s",
                 sqlite3_errmsg(stmt->conn));
        code = ADBC_STATUS_INTERNAL;
        goto cleanup;
      }
    }

    sqlite3_str_appendf(create_query, "\"%w\"", stmt->binder.schema.children[i]->name);
    if (sqlite3_str_errcode(create_query)) {
      SetError(error, "[SQLite] Failed to build CREATE: %s", sqlite3_errmsg(stmt->conn));
      code = ADBC_STATUS_INTERNAL;
      goto cleanup;
    }

    int status =
        ArrowSchemaViewInit(&view, stmt->binder.schema.children[i], &arrow_error);
    if (status != 0) {
      SetError(error, "[SQLite] Failed to parse schema for column %d: %s (%d): %s", i,
               strerror(status), status, arrow_error.message);
      code = ADBC_STATUS_INTERNAL;
      goto cleanup;
    }

    switch (view.type) {
      case NANOARROW_TYPE_BOOL:
      case NANOARROW_TYPE_UINT8:
      case NANOARROW_TYPE_UINT16:
      case NANOARROW_TYPE_UINT32:
      case NANOARROW_TYPE_UINT64:
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_INT16:
      case NANOARROW_TYPE_INT32:
      case NANOARROW_TYPE_INT64:
        sqlite3_str_appendf(create_query, " INTEGER");
        break;
      case NANOARROW_TYPE_FLOAT:
      case NANOARROW_TYPE_DOUBLE:
        sqlite3_str_appendf(create_query, " REAL");
        break;
      case NANOARROW_TYPE_STRING:
      case NANOARROW_TYPE_LARGE_STRING:
      case NANOARROW_TYPE_DATE32:
        sqlite3_str_appendf(create_query, " TEXT");
        break;
      case NANOARROW_TYPE_BINARY:
        sqlite3_str_appendf(create_query, " BLOB");
        break;
      default:
        break;
    }

    sqlite3_str_appendf(insert_query, "%s?", (i > 0 ? ", " : ""));
    if (sqlite3_str_errcode(insert_query)) {
      SetError(error, "[SQLite] Failed to build INSERT: %s", sqlite3_errmsg(stmt->conn));
      code = ADBC_STATUS_INTERNAL;
      goto cleanup;
    }
  }

  sqlite3_str_appendchar(create_query, 1, ')');
  if (sqlite3_str_errcode(create_query)) {
    SetError(error, "[SQLite] Failed to build CREATE: %s", sqlite3_errmsg(stmt->conn));
    code = ADBC_STATUS_INTERNAL;
    goto cleanup;
  }

  sqlite3_str_appendchar(insert_query, 1, ')');
  if (sqlite3_str_errcode(insert_query)) {
    SetError(error, "[SQLite] Failed to build INSERT: %s", sqlite3_errmsg(stmt->conn));
    code = ADBC_STATUS_INTERNAL;
    goto cleanup;
  }

  sqlite3_stmt* create = NULL;
  if (!stmt->append) {
    // Create table
    int rc =
        sqlite3_prepare_v2(stmt->conn, sqlite3_str_value(create_query),
                           sqlite3_str_length(create_query), &create, /*pzTail=*/NULL);
    if (rc == SQLITE_OK) {
      rc = sqlite3_step(create);
    }

    if (rc != SQLITE_OK && rc != SQLITE_DONE) {
      SetError(error, "[SQLite] Failed to create table: %s (executed '%.*s')",
               sqlite3_errmsg(stmt->conn), sqlite3_str_length(create_query),
               sqlite3_str_value(create_query));
      code = ADBC_STATUS_INTERNAL;
    }
  }

  if (code == ADBC_STATUS_OK) {
    int rc = sqlite3_prepare_v2(stmt->conn, sqlite3_str_value(insert_query),
                                sqlite3_str_length(insert_query), insert_statement,
                                /*pzTail=*/NULL);
    if (rc != SQLITE_OK) {
      SetError(error, "[SQLite] Failed to prepare statement: %s (executed '%.*s')",
               sqlite3_errmsg(stmt->conn), sqlite3_str_length(insert_query),
               sqlite3_str_value(insert_query));
      code = ADBC_STATUS_INTERNAL;
    }
  }

  sqlite3_finalize(create);

cleanup:
  sqlite3_free(sqlite3_str_finish(create_query));
  sqlite3_free(sqlite3_str_finish(insert_query));
  if (table != NULL) sqlite3_free(table);
  return code;
}

AdbcStatusCode SqliteStatementExecuteIngest(struct SqliteStatement* stmt,
                                            int64_t* rows_affected,
                                            struct AdbcError* error) {
  if (!stmt->binder.schema.release) {
    SetError(error, "[SQLite] Must Bind() before bulk ingestion");
    return ADBC_STATUS_INVALID_STATE;
  }

  sqlite3_stmt* insert = NULL;
  AdbcStatusCode status = SqliteStatementInitIngest(stmt, &insert, error);

  int64_t row_count = 0;
  int is_autocommit = sqlite3_get_autocommit(stmt->conn);
  if (status == ADBC_STATUS_OK) {
    if (is_autocommit) sqlite3_exec(stmt->conn, "BEGIN TRANSACTION", 0, 0, 0);

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
        SetError(error, "[SQLite] Failed to execute statement: %s",
                 sqlite3_errmsg(stmt->conn));
        status = ADBC_STATUS_INTERNAL;
        break;
      }
      row_count++;
    }

    if (is_autocommit) sqlite3_exec(stmt->conn, "COMMIT", 0, 0, 0);
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
      SetError(error,
               "[SQLite] Parameter count mismatch: expected %" PRId64
               " but found %" PRId64,
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
      SetError(error, "[SQLite] Failed to execute query: %s",
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
  if (stmt->target_catalog) {
    free(stmt->target_catalog);
    stmt->target_catalog = NULL;
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
  SetError(error, "[SQLite] Substrait is not supported");
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

AdbcStatusCode SqliteStatementGetOption(struct AdbcStatement* statement, const char* key,
                                        char* value, size_t* length,
                                        struct AdbcError* error) {
  CHECK_DB_INIT(statement, error);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode SqliteStatementGetOptionBytes(struct AdbcStatement* statement,
                                             const char* key, uint8_t* value,
                                             size_t* length, struct AdbcError* error) {
  CHECK_DB_INIT(statement, error);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode SqliteStatementGetOptionDouble(struct AdbcStatement* statement,
                                              const char* key, double* value,
                                              struct AdbcError* error) {
  CHECK_DB_INIT(statement, error);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode SqliteStatementGetOptionInt(struct AdbcStatement* statement,
                                           const char* key, int64_t* value,
                                           struct AdbcError* error) {
  CHECK_DB_INIT(statement, error);
  return ADBC_STATUS_NOT_FOUND;
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
    SetError(error, "[SQLite] SQLite returned negative parameter count");
    return ADBC_STATUS_INTERNAL;
  }

  ArrowSchemaInit(schema);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(schema, NANOARROW_TYPE_STRUCT), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(schema, num_params), error);
  char buffer[11];
  for (int i = 0; i < num_params; i++) {
    const char* name = sqlite3_bind_parameter_name(stmt->stmt, i + 1);
    if (name == NULL) {
      snprintf(buffer, sizeof(buffer), "%d", i);
      name = buffer;
    }
    ArrowSchemaInit(schema->children[i]);
    CHECK_NA(INTERNAL, ArrowSchemaSetType(schema->children[i], NANOARROW_TYPE_NA), error);
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
  } else if (strcmp(key, ADBC_INGEST_OPTION_TARGET_CATALOG) == 0) {
    if (stmt->query) {
      free(stmt->query);
      stmt->query = NULL;
    }
    if (stmt->target_catalog) {
      free(stmt->target_catalog);
      stmt->target_catalog = NULL;
    }

    size_t len = strlen(value) + 1;
    stmt->target_catalog = (char*)malloc(len);
    strncpy(stmt->target_catalog, value, len);
    return ADBC_STATUS_OK;
  } else if (strcmp(key, ADBC_INGEST_OPTION_MODE) == 0) {
    if (strcmp(value, ADBC_INGEST_OPTION_MODE_APPEND) == 0) {
      stmt->append = 1;
    } else if (strcmp(value, ADBC_INGEST_OPTION_MODE_CREATE) == 0) {
      stmt->append = 0;
    } else {
      SetError(error, "[SQLite] Invalid statement option value %s=%s", key, value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return ADBC_STATUS_OK;
  } else if (strcmp(key, ADBC_INGEST_OPTION_TEMPORARY) == 0) {
    if (strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0) {
      stmt->temporary = 1;
    } else if (strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
      stmt->temporary = 0;
    } else {
      SetError(error, "[SQLite] Invalid statement option value %s=%s", key, value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return ADBC_STATUS_OK;
  } else if (strcmp(key, kStatementOptionBatchRows) == 0) {
    char* end = NULL;
    long batch_size = strtol(value, &end, /*base=*/10);  // NOLINT(runtime/int)
    if (errno != 0) {
      SetError(error, "[SQLite] Invalid statement option value %s=%s (out of range)", key,
               value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    } else if (batch_size <= 0) {
      SetError(error,
               "[SQLite] Invalid statement option value %s=%s (value is non-positive or "
               "invalid)",
               key, value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    } else if (batch_size > (long)INT_MAX) {  // NOLINT(runtime/int)
      SetError(
          error,
          "[SQLite] Invalid statement option value %s=%s (value is out of range of int)",
          key, value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    stmt->batch_size = (int)batch_size;
    return ADBC_STATUS_OK;
  }
  SetError(error, "[SQLite] Unknown statement option %s=%s", key,
           value ? value : "(NULL)");
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteStatementSetOptionBytes(struct AdbcStatement* statement,
                                             const char* key, const uint8_t* value,
                                             size_t length, struct AdbcError* error) {
  CHECK_DB_INIT(statement, error);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteStatementSetOptionDouble(struct AdbcStatement* statement,
                                              const char* key, double value,
                                              struct AdbcError* error) {
  CHECK_DB_INIT(statement, error);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteStatementSetOptionInt(struct AdbcStatement* statement,
                                           const char* key, int64_t value,
                                           struct AdbcError* error) {
  CHECK_DB_INIT(statement, error);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteStatementExecutePartitions(struct AdbcStatement* statement,
                                                struct ArrowSchema* schema,
                                                struct AdbcPartitions* partitions,
                                                int64_t* rows_affected,
                                                struct AdbcError* error) {
  CHECK_STMT_INIT(statement, error);
  SetError(error, "[SQLite] Partitioned result sets are not supported");
  return ADBC_STATUS_NOT_IMPLEMENTED;
}  // NOLINT(whitespace/indent)

AdbcStatusCode SqliteDriverInit(int version, void* raw_driver, struct AdbcError* error) {
  if (version != ADBC_VERSION_1_0_0) {
    SetError(error, "[SQLite] Only version %d supported, got %d", ADBC_VERSION_1_0_0,
             version);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  struct AdbcDriver* driver = (struct AdbcDriver*)raw_driver;
  memset(driver, 0, ADBC_DRIVER_1_0_0_SIZE);
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

AdbcStatusCode AdbcDatabaseGetOption(struct AdbcDatabase* database, const char* key,
                                     char* value, size_t* length,
                                     struct AdbcError* error) {
  return SqliteDatabaseGetOption(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseGetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          uint8_t* value, size_t* length,
                                          struct AdbcError* error) {
  return SqliteDatabaseGetOptionBytes(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseGetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t* value, struct AdbcError* error) {
  return SqliteDatabaseGetOptionInt(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseGetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double* value, struct AdbcError* error) {
  return SqliteDatabaseGetOptionDouble(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  return SqliteDatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  return SqliteDatabaseNew(database, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return SqliteDatabaseRelease(database, error);
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return SqliteDatabaseSetOption(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          const uint8_t* value, size_t length,
                                          struct AdbcError* error) {
  return SqliteDatabaseSetOptionBytes(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseSetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t value, struct AdbcError* error) {
  return SqliteDatabaseSetOptionInt(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double value, struct AdbcError* error) {
  return SqliteDatabaseSetOptionDouble(database, key, value, error);
}

AdbcStatusCode AdbcConnectionCancel(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode AdbcConnectionGetOption(struct AdbcConnection* connection, const char* key,
                                       char* value, size_t* length,
                                       struct AdbcError* error) {
  return SqliteConnectionGetOption(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionGetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, uint8_t* value,
                                            size_t* length, struct AdbcError* error) {
  return SqliteConnectionGetOptionBytes(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionGetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t* value,
                                          struct AdbcError* error) {
  return SqliteConnectionGetOptionInt(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double* value,
                                             struct AdbcError* error) {
  return SqliteConnectionGetOptionDouble(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return SqliteConnectionNew(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return SqliteConnectionSetOption(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, const uint8_t* value,
                                            size_t length, struct AdbcError* error) {
  return SqliteConnectionSetOptionBytes(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionSetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t value,
                                          struct AdbcError* error) {
  return SqliteConnectionSetOptionInt(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double value,
                                             struct AdbcError* error) {
  return SqliteConnectionSetOptionDouble(connection, key, value, error);
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
                                     const uint32_t* info_codes, size_t info_codes_length,
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

AdbcStatusCode AdbcConnectionGetStatistics(struct AdbcConnection* connection,
                                           const char* catalog, const char* db_schema,
                                           const char* table_name, char approximate,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode AdbcConnectionGetStatisticNames(struct AdbcConnection* connection,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
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

AdbcStatusCode AdbcStatementCancel(struct AdbcStatement* statement,
                                   struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
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

AdbcStatusCode AdbcStatementExecuteSchema(struct AdbcStatement* statement,
                                          struct ArrowSchema* schema,
                                          struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
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

AdbcStatusCode AdbcStatementGetOption(struct AdbcStatement* statement, const char* key,
                                      char* value, size_t* length,
                                      struct AdbcError* error) {
  return SqliteStatementGetOption(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, uint8_t* value,
                                           size_t* length, struct AdbcError* error) {
  return SqliteStatementGetOptionBytes(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t* value, struct AdbcError* error) {
  return SqliteStatementGetOptionInt(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double* value,
                                            struct AdbcError* error) {
  return SqliteStatementGetOptionDouble(statement, key, value, error);
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

AdbcStatusCode AdbcStatementSetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, const uint8_t* value,
                                           size_t length, struct AdbcError* error) {
  return SqliteStatementSetOptionBytes(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementSetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t value, struct AdbcError* error) {
  return SqliteStatementSetOptionInt(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double value,
                                            struct AdbcError* error) {
  return SqliteStatementSetOptionDouble(statement, key, value, error);
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

ADBC_EXPORT
AdbcStatusCode AdbcDriverInit(int version, void* driver, struct AdbcError* error) {
  return SqliteDriverInit(version, driver, error);
}
