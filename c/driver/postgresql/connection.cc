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

#include "connection.h"

#include <cinttypes>
#include <cstddef>
#include <cstring>
#include <iterator>
#include <memory>
#include <string>

#include <adbc.h>
#include <libpq-fe.h>

#include "database.h"
#include "utils.h"

namespace {

static const uint32_t kSupportedInfoCodes[] = {
    ADBC_INFO_VENDOR_NAME,    ADBC_INFO_VENDOR_VERSION,       ADBC_INFO_DRIVER_NAME,
    ADBC_INFO_DRIVER_VERSION, ADBC_INFO_DRIVER_ARROW_VERSION,
};

struct PqRecord {
  const char* db_name;
  const int len;
  const bool is_null;
};

class PqResultRow {
 public:
  PqResultRow(pg_result* result, int row_num) : result_(result), row_num_(row_num) {
    ncols_ = PQnfields(result);
  }
  class iterator {
    pg_result* iter_result_;
    int iter_row_num_;
    int iter_col_num_ = 0;
    int iter_ncols_;

   public:
    explicit iterator(pg_result* result, int ncols, int row_num, int col_num = 0)
        : iter_result_(result),
          iter_ncols_(ncols),
          iter_row_num_(row_num),
          iter_col_num_(col_num) {}
    iterator& operator++() {
      iter_col_num_++;
      return *this;
    }

    iterator operator++(int) {
      iterator retval = *this;
      ++(*this);
      return retval;
    }
    bool operator==(iterator other) const {
      return iter_result_ == other.iter_result_ && iter_row_num_ == other.iter_row_num_ &&
             iter_col_num_ == other.iter_col_num_;
    }
    bool operator!=(iterator other) const { return !(*this == other); }
    PqRecord operator*() {
      const char* db_name = PQgetvalue(iter_result_, iter_row_num_, iter_col_num_);
      const int len = PQgetlength(iter_result_, iter_row_num_, iter_col_num_);
      const bool is_null = PQgetisnull(iter_result_, iter_row_num_, iter_col_num_);

      return PqRecord{db_name, len, is_null};
    }
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = PqRecord;
    using pointer = PqRecord*;
    using reference = PqRecord&;
  };

  iterator begin() { return iterator(result_, ncols_, row_num_); }
  iterator end() { return iterator(result_, ncols_, row_num_, ncols_); }

 private:
  pg_result* result_ = nullptr;
  int row_num_;
  int ncols_;
};

class PqResultHelper {
 public:
  PqResultHelper(PGconn* conn, const char* query) : conn_(conn) {
    query_ = std::string(query);
  }
  pg_result* Execute() {
    result_ = PQexec(conn_, query_.c_str());
    ExecStatusType pq_status = PQresultStatus(result_);
    if (pq_status == PGRES_TUPLES_OK) {
      num_rows_ = PQntuples(result_);
    }
    return result_;
  }

  ~PqResultHelper() {
    if (result_ != nullptr) PQclear(result_);
  }

  class iterator {
    pg_result* iter_result_;
    int iter_num_rows_;
    int iter_curr_row_ = 0;

   public:
    explicit iterator(pg_result* result, int num_rows, int curr_row = 0)
        : iter_result_(result), iter_num_rows_(num_rows), iter_curr_row_(curr_row) {}
    iterator& operator++() {
      iter_curr_row_++;
      return *this;
    }
    iterator operator++(int) {
      iterator retval = *this;
      ++(*this);
      return retval;
    }
    bool operator==(iterator other) const {
      return iter_result_ == other.iter_result_ &&
             iter_num_rows_ == other.iter_num_rows_ &&
             iter_curr_row_ == other.iter_curr_row_;
    }
    bool operator!=(iterator other) const { return !(*this == other); }
    PqResultRow operator*() { return PqResultRow(iter_result_, iter_curr_row_); }
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = PqResultRow::iterator;
    using pointer = const PqResultRow::iterator*;
    using reference = const PqResultRow::iterator&;
  };

  iterator begin() { return iterator(result_, num_rows_, 0); }
  iterator end() { return iterator(result_, num_rows_, num_rows_); }

 private:
  pg_result* result_ = nullptr;
  PGconn* conn_;
  std::string query_;
  int num_rows_ = 0;
};

}  // namespace

namespace adbcpq {

AdbcStatusCode PostgresConnection::Commit(struct AdbcError* error) {
  if (autocommit_) {
    SetError(error, "%s", "[libpq] Cannot commit when autocommit is enabled");
    return ADBC_STATUS_INVALID_STATE;
  }

  PGresult* result = PQexec(conn_, "COMMIT");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    SetError(error, "%s%s", "[libpq] Failed to commit: ", PQerrorMessage(conn_));
    PQclear(result);
    return ADBC_STATUS_IO;
  }
  PQclear(result);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnectionGetInfoImpl(const uint32_t* info_codes,
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
            AdbcConnectionGetInfoAppendString(array, info_codes[i], "PostgreSQL", error));
        break;
      case ADBC_INFO_VENDOR_VERSION:
        RAISE_ADBC(AdbcConnectionGetInfoAppendString(
            array, info_codes[i], std::to_string(PQlibVersion()).c_str(), error));
        break;
      case ADBC_INFO_DRIVER_NAME:
        RAISE_ADBC(AdbcConnectionGetInfoAppendString(array, info_codes[i],
                                                     "ADBC PostgreSQL Driver", error));
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
}

AdbcStatusCode PostgresConnection::GetInfo(struct AdbcConnection* connection,
                                           uint32_t* info_codes, size_t info_codes_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  // XXX: mistake in adbc.h (should have been const pointer)
  const uint32_t* codes = info_codes;
  if (!info_codes) {
    codes = kSupportedInfoCodes;
    info_codes_length = sizeof(kSupportedInfoCodes) / sizeof(kSupportedInfoCodes[0]);
  }

  struct ArrowSchema schema = {0};
  struct ArrowArray array = {0};

  AdbcStatusCode status =
      PostgresConnectionGetInfoImpl(codes, info_codes_length, &schema, &array, error);
  if (status != ADBC_STATUS_OK) {
    if (schema.release) schema.release(&schema);
    if (array.release) array.release(&array);
    return status;
  }

  return BatchToArrayStream(&array, &schema, out, error);
}

AdbcStatusCode PostgresConnectionGetObjectsImpl(
    PGconn* conn, int depth, const char* catalog, const char* db_schema,
    const char* table_name, const char** table_types, const char* column_name,
    struct ArrowSchema* schema, struct ArrowArray* array, struct AdbcError* error) {
  RAISE_ADBC(AdbcInitConnectionObjectsSchema(schema, error));

  struct ArrowError na_error = {0};
  CHECK_NA_DETAIL(INTERNAL, ArrowArrayInitFromSchema(array, schema, &na_error), &na_error,
                  error);
  CHECK_NA(INTERNAL, ArrowArrayStartAppending(array), error);

  struct ArrowArray* catalog_name_col = array->children[0];
  struct ArrowArray* catalog_db_schemas_col = array->children[1];

  // TODO: support proper filters
  if (!catalog) {
    struct StringBuilder query = {0};
    if (StringBuilderInit(&query, /*initial_size=*/256) != 0) return ADBC_STATUS_INTERNAL;

    if (StringBuilderAppend(&query, "%s", "SELECT datname FROM pg_catalog.pg_database")) {
      return ADBC_STATUS_INTERNAL;
    }

    PqResultHelper result_helper = PqResultHelper{conn, query.buffer};
    StringBuilderReset(&query);

    auto result = result_helper.Execute();
    for (auto row : result_helper) {
      for (auto pq_record : row) {
        auto db_name = pq_record.db_name;
        CHECK_NA(INTERNAL,
                 ArrowArrayAppendString(catalog_name_col, ArrowCharView(db_name)), error);
        if (depth == ADBC_OBJECT_DEPTH_CATALOGS) {
          CHECK_NA(INTERNAL, ArrowArrayAppendNull(catalog_db_schemas_col, 1), error);
        } else {
          return ADBC_STATUS_NOT_IMPLEMENTED;
        }
        CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);
      }
    }
  }

  CHECK_NA_DETAIL(INTERNAL, ArrowArrayFinishBuildingDefault(array, &na_error), &na_error,
                  error);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::GetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_types,
    const char* column_name, struct ArrowArrayStream* out, struct AdbcError* error) {
  struct ArrowSchema schema = {0};
  struct ArrowArray array = {0};

  AdbcStatusCode status =
      PostgresConnectionGetObjectsImpl(conn_, depth, catalog, db_schema, table_name,
                                       table_types, column_name, &schema, &array, error);

  if (status != ADBC_STATUS_OK) {
    if (schema.release) schema.release(&schema);
    if (array.release) array.release(&array);
    return status;
  }

  return BatchToArrayStream(&array, &schema, out, error);
}

AdbcStatusCode PostgresConnection::GetTableSchema(const char* catalog,
                                                  const char* db_schema,
                                                  const char* table_name,
                                                  struct ArrowSchema* schema,
                                                  struct AdbcError* error) {
  AdbcStatusCode final_status = ADBC_STATUS_OK;
  struct StringBuilder query = {0};
  if (StringBuilderInit(&query, /*initial_size=*/256) != 0) return ADBC_STATUS_INTERNAL;

  if (StringBuilderAppend(
          &query, "%s",
          "SELECT attname, atttypid "
          "FROM pg_catalog.pg_class AS cls "
          "INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid "
          "INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid "
          "WHERE attr.attnum >= 0 AND cls.oid = '") != 0)
    return ADBC_STATUS_INTERNAL;

  if (db_schema != nullptr) {
    char* schema = PQescapeIdentifier(conn_, db_schema, strlen(db_schema));
    if (schema == NULL) {
      SetError(error, "%s%s", "Faled to escape schema: ", PQerrorMessage(conn_));
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    int ret = StringBuilderAppend(&query, "%s%s", schema, ".");
    PQfreemem(schema);

    if (ret != 0) return ADBC_STATUS_INTERNAL;
  }

  char* table = PQescapeIdentifier(conn_, table_name, strlen(table_name));
  if (table == NULL) {
    SetError(error, "%s%s", "Failed to escape table: ", PQerrorMessage(conn_));
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  int ret = StringBuilderAppend(&query, "%s%s", table, "'::regclass::oid");
  PQfreemem(table);

  if (ret != 0) return ADBC_STATUS_INTERNAL;

  PqResultHelper result_helper = PqResultHelper{conn_, query.buffer};
  StringBuilderReset(&query);
  pg_result* result = result_helper.Execute();

  ExecStatusType pq_status = PQresultStatus(result);
  auto uschema = nanoarrow::UniqueSchema();

  if (pq_status == PGRES_TUPLES_OK) {
    int num_rows = PQntuples(result);
    ArrowSchemaInit(uschema.get());
    CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(uschema.get(), num_rows), error);

    ArrowError na_error;
    for (int row = 0; row < num_rows; row++) {
      const char* colname = PQgetvalue(result, row, 0);
      const Oid pg_oid = static_cast<uint32_t>(
          std::strtol(PQgetvalue(result, row, 1), /*str_end=*/nullptr, /*base=*/10));

      PostgresType pg_type;
      if (type_resolver_->Find(pg_oid, &pg_type, &na_error) != NANOARROW_OK) {
        SetError(error, "%s%d%s%s%s%" PRIu32, "Column #", row + 1, " (\"", colname,
                 "\") has unknown type code ", pg_oid);
        final_status = ADBC_STATUS_NOT_IMPLEMENTED;
        break;
      }

      CHECK_NA(INTERNAL, pg_type.WithFieldName(colname).SetSchema(uschema->children[row]),
               error);
    }
  } else {
    SetError(error, "%s%s", "Failed to get table schema: ", PQerrorMessage(conn_));
    final_status = ADBC_STATUS_IO;
  }

  uschema.move(schema);
  return final_status;
}

AdbcStatusCode PostgresConnectionGetTableTypesImpl(struct ArrowSchema* schema,
                                                   struct ArrowArray* array,
                                                   struct AdbcError* error) {
  // See 'relkind' in https://www.postgresql.org/docs/current/catalog-pg-class.html
  auto uschema = nanoarrow::UniqueSchema();
  ArrowSchemaInit(uschema.get());

  CHECK_NA(INTERNAL, ArrowSchemaSetType(uschema.get(), NANOARROW_TYPE_STRUCT), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(uschema.get(), /*num_columns=*/1),
           error);
  ArrowSchemaInit(uschema.get()->children[0]);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(uschema.get()->children[0], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(uschema.get()->children[0], "table_type"), error);
  uschema.get()->children[0]->flags &= ~ARROW_FLAG_NULLABLE;

  CHECK_NA(INTERNAL, ArrowArrayInitFromSchema(array, uschema.get(), NULL), error);
  CHECK_NA(INTERNAL, ArrowArrayStartAppending(array), error);

  CHECK_NA(INTERNAL, ArrowArrayAppendString(array->children[0], ArrowCharView("table")),
           error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);
  CHECK_NA(INTERNAL,
           ArrowArrayAppendString(array->children[0], ArrowCharView("toast_table")),
           error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);
  CHECK_NA(INTERNAL, ArrowArrayAppendString(array->children[0], ArrowCharView("view")),
           error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);
  CHECK_NA(INTERNAL,
           ArrowArrayAppendString(array->children[0], ArrowCharView("materialized_view")),
           error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);
  CHECK_NA(INTERNAL,
           ArrowArrayAppendString(array->children[0], ArrowCharView("foreign_table")),
           error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);
  CHECK_NA(INTERNAL,
           ArrowArrayAppendString(array->children[0], ArrowCharView("partitioned_table")),
           error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);

  CHECK_NA(INTERNAL, ArrowArrayFinishBuildingDefault(array, NULL), error);

  uschema.move(schema);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::GetTableTypes(struct AdbcConnection* connection,
                                                 struct ArrowArrayStream* out,
                                                 struct AdbcError* error) {
  struct ArrowSchema schema = {0};
  struct ArrowArray array = {0};

  AdbcStatusCode status = PostgresConnectionGetTableTypesImpl(&schema, &array, error);
  if (status != ADBC_STATUS_OK) {
    if (schema.release) schema.release(&schema);
    if (array.release) array.release(&array);
    return status;
  }
  return BatchToArrayStream(&array, &schema, out, error);
}

AdbcStatusCode PostgresConnection::Init(struct AdbcDatabase* database,
                                        struct AdbcError* error) {
  if (!database || !database->private_data) {
    SetError(error, "%s", "[libpq] Must provide an initialized AdbcDatabase");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  database_ =
      *reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  type_resolver_ = database_->type_resolver();
  return database_->Connect(&conn_, error);
}

AdbcStatusCode PostgresConnection::Release(struct AdbcError* error) {
  if (conn_) {
    return database_->Disconnect(&conn_, error);
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::Rollback(struct AdbcError* error) {
  if (autocommit_) {
    SetError(error, "%s", "[libpq] Cannot rollback when autocommit is enabled");
    return ADBC_STATUS_INVALID_STATE;
  }

  PGresult* result = PQexec(conn_, "ROLLBACK");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    SetError(error, "%s%s", "[libpq] Failed to rollback: ", PQerrorMessage(conn_));
    PQclear(result);
    return ADBC_STATUS_IO;
  }
  PQclear(result);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::SetOption(const char* key, const char* value,
                                             struct AdbcError* error) {
  if (std::strcmp(key, ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0) {
    bool autocommit = true;
    if (std::strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0) {
      autocommit = true;
    } else if (std::strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
      autocommit = false;
    } else {
      SetError(error, "%s%s%s%s", "[libpq] Invalid value for option ", key, ": ", value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    if (autocommit != autocommit_) {
      const char* query = autocommit ? "COMMIT" : "BEGIN TRANSACTION";

      PGresult* result = PQexec(conn_, query);
      if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        SetError(error, "%s%s",
                 "[libpq] Failed to update autocommit: ", PQerrorMessage(conn_));
        PQclear(result);
        return ADBC_STATUS_IO;
      }
      PQclear(result);
      autocommit_ = autocommit;
    }
    return ADBC_STATUS_OK;
  }
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}
}  // namespace adbcpq
