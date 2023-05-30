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

#include <cassert>
#include <cinttypes>
#include <cstring>
#include <memory>
#include <string>
#include <utility>
#include <vector>

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
  const char* data;
  const int len;
  const bool is_null;
};

// Used by PqResultHelper to provide index-based access to the records within each
// row of a pg_result
class PqResultRow {
 public:
  PqResultRow(pg_result* result, int row_num) : result_(result), row_num_(row_num) {
    ncols_ = PQnfields(result);
  }

  PqRecord operator[](const int& col_num) {
    assert(col_num < ncols_);
    const char* data = PQgetvalue(result_, row_num_, col_num);
    const int len = PQgetlength(result_, row_num_, col_num);
    const bool is_null = PQgetisnull(result_, row_num_, col_num);

    return PqRecord{data, len, is_null};
  }

 private:
  pg_result* result_ = nullptr;
  int row_num_;
  int ncols_;
};

// Helper to manager the lifecycle of a PQResult. The query argument
// will be evaluated as part of the constructor, with the desctructor handling cleanup
// Caller must call Prepare then Execute, checking both for an OK AdbcStatusCode
// prior to iterating
class PqResultHelper {
 public:
  explicit PqResultHelper(PGconn* conn, std::string query, struct AdbcError* error)
      : conn_(conn), query_(std::move(query)), error_(error) {}

  explicit PqResultHelper(PGconn* conn, std::string query,
                          std::vector<std::string> param_values, struct AdbcError* error)
      : conn_(conn),
        query_(std::move(query)),
        param_values_(param_values),
        error_(error) {}

  AdbcStatusCode Prepare() {
    // TODO: make stmtName a unique identifier?
    PGresult* result =
        PQprepare(conn_, /*stmtName=*/"", query_.c_str(), param_values_.size(), NULL);
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      SetError(error_, "[libpq] Failed to prepare query: %s\nQuery was:%s",
               PQerrorMessage(conn_), query_.c_str());
      PQclear(result);
      return ADBC_STATUS_IO;
    }

    PQclear(result);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Execute() {
    std::vector<const char*> param_c_strs;

    for (auto index = 0; index < param_values_.size(); index++) {
      param_c_strs.push_back(param_values_[index].c_str());
    }

    result_ = PQexecPrepared(conn_, "", param_values_.size(), param_c_strs.data(), NULL,
                             NULL, 0);

    if (PQresultStatus(result_) != PGRES_TUPLES_OK) {
      SetError(error_, "[libpq] Failed to execute query: %s", PQerrorMessage(conn_));
      return ADBC_STATUS_IO;
    }

    return ADBC_STATUS_OK;
  }

  ~PqResultHelper() {
    if (result_ != nullptr) {
      PQclear(result_);
    }
  }

  int NumRows() { return PQntuples(result_); }

  int NumColumns() { return PQnfields(result_); }

  class iterator {
    const PqResultHelper& outer_;
    int curr_row_ = 0;

   public:
    explicit iterator(const PqResultHelper& outer, int curr_row = 0)
        : outer_(outer), curr_row_(curr_row) {}
    iterator& operator++() {
      curr_row_++;
      return *this;
    }
    iterator operator++(int) {
      iterator retval = *this;
      ++(*this);
      return retval;
    }
    bool operator==(iterator other) const {
      return outer_.result_ == other.outer_.result_ && curr_row_ == other.curr_row_;
    }
    bool operator!=(iterator other) const { return !(*this == other); }
    PqResultRow operator*() { return PqResultRow(outer_.result_, curr_row_); }
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::vector<PqResultRow>;
    using pointer = const std::vector<PqResultRow>*;
    using reference = const std::vector<PqResultRow>&;
  };

  iterator begin() { return iterator(*this); }
  iterator end() { return iterator(*this, NumRows()); }

 private:
  pg_result* result_ = nullptr;
  PGconn* conn_;
  std::string query_;
  std::vector<std::string> param_values_;
  struct AdbcError* error_;
};

class PqGetObjectsHelper {
 public:
  PqGetObjectsHelper(PGconn* conn, int depth, const char* catalog, const char* db_schema,
                     const char* table_name, const char** table_types,
                     const char* column_name, struct ArrowSchema* schema,
                     struct ArrowArray* array, struct AdbcError* error)
      : conn_(conn),
        depth_(depth),
        catalog_(catalog),
        db_schema_(db_schema),
        table_name_(table_name),
        table_types_(table_types),
        column_name_(column_name),
        schema_(schema),
        array_(array),
        error_(error) {
    na_error_ = {0};
  }

  AdbcStatusCode GetObjects() {
    PqResultHelper curr_db_helper =
        PqResultHelper{conn_, std::string("SELECT current_database()"), error_};

    RAISE_ADBC(curr_db_helper.Prepare());
    RAISE_ADBC(curr_db_helper.Execute());

    assert(curr_db_helper.NumRows() == 1);
    auto curr_iter = curr_db_helper.begin();
    PqResultRow db_row = *curr_iter;
    current_db_ = std::string(db_row[0].data);

    RAISE_ADBC(InitArrowArray());

    catalog_name_col_ = array_->children[0];
    catalog_db_schemas_col_ = array_->children[1];
    catalog_db_schemas_items_ = catalog_db_schemas_col_->children[0];
    db_schema_name_col_ = catalog_db_schemas_items_->children[0];
    db_schema_tables_col_ = catalog_db_schemas_items_->children[1];

    RAISE_ADBC(AppendCatalogs());
    RAISE_ADBC(FinishArrowArray());
    return ADBC_STATUS_OK;
  }

 private:
  AdbcStatusCode InitArrowArray() {
    RAISE_ADBC(AdbcInitConnectionObjectsSchema(schema_, error_));

    CHECK_NA_DETAIL(INTERNAL, ArrowArrayInitFromSchema(array_, schema_, &na_error_),
                    &na_error_, error_);

    CHECK_NA(INTERNAL, ArrowArrayStartAppending(array_), error_);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode AppendSchemas(std::string db_name) {
    // postgres only allows you to list schemas for the currently connected db
    if (db_name == current_db_) {
      struct StringBuilder query = {0};
      if (StringBuilderInit(&query, /*initial_size*/ 256)) {
        return ADBC_STATUS_INTERNAL;
      }

      const char* stmt =
          "SELECT nspname FROM pg_catalog.pg_namespace WHERE "
          "nspname !~ '^pg_' AND nspname <> 'information_schema'";

      if (StringBuilderAppend(&query, "%s", stmt)) {
        StringBuilderReset(&query);
        return ADBC_STATUS_INTERNAL;
      }

      std::vector<std::string> params;
      if (db_schema_ != NULL) {
        if (StringBuilderAppend(&query, "%s", " AND nspname = $1")) {
          StringBuilderReset(&query);
          return ADBC_STATUS_INTERNAL;
        }
        params.push_back(db_schema_);
      }

      auto result_helper =
          PqResultHelper{conn_, std::string(query.buffer), params, error_};
      StringBuilderReset(&query);

      RAISE_ADBC(result_helper.Prepare());
      RAISE_ADBC(result_helper.Execute());

      for (PqResultRow row : result_helper) {
        const char* schema_name = row[0].data;
        CHECK_NA(INTERNAL,
                 ArrowArrayAppendString(db_schema_name_col_, ArrowCharView(schema_name)),
                 error_);
        if (depth_ >= ADBC_OBJECT_DEPTH_TABLES) {
          return ADBC_STATUS_NOT_IMPLEMENTED;
        } else {
          CHECK_NA(INTERNAL, ArrowArrayAppendNull(db_schema_tables_col_, 1), error_);
        }
        CHECK_NA(INTERNAL, ArrowArrayFinishElement(catalog_db_schemas_items_), error_);
      }
    }

    CHECK_NA(INTERNAL, ArrowArrayFinishElement(catalog_db_schemas_col_), error_);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode AppendCatalogs() {
    struct StringBuilder query = {0};
    if (StringBuilderInit(&query, /*initial_size=*/256) != 0) return ADBC_STATUS_INTERNAL;

    if (StringBuilderAppend(&query, "%s", "SELECT datname FROM pg_catalog.pg_database")) {
      return ADBC_STATUS_INTERNAL;
    }

    std::vector<std::string> params;
    if (catalog_ != NULL) {
      if (StringBuilderAppend(&query, "%s", " WHERE datname = $1")) {
        StringBuilderReset(&query);
        return ADBC_STATUS_INTERNAL;
      }
      params.push_back(catalog_);
    }

    PqResultHelper result_helper =
        PqResultHelper{conn_, std::string(query.buffer), params, error_};
    StringBuilderReset(&query);

    RAISE_ADBC(result_helper.Prepare());
    RAISE_ADBC(result_helper.Execute());

    for (PqResultRow row : result_helper) {
      const char* db_name = row[0].data;
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(catalog_name_col_, ArrowCharView(db_name)), error_);
      if (depth_ == ADBC_OBJECT_DEPTH_CATALOGS) {
        CHECK_NA(INTERNAL, ArrowArrayAppendNull(catalog_db_schemas_col_, 1), error_);
      } else {
        RAISE_ADBC(AppendSchemas(std::string(db_name)));
      }
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(array_), error_);
    }

    return ADBC_STATUS_OK;
  }

  AdbcStatusCode FinishArrowArray() {
    CHECK_NA_DETAIL(INTERNAL, ArrowArrayFinishBuildingDefault(array_, &na_error_),
                    &na_error_, error_);

    return ADBC_STATUS_OK;
  }

  PGconn* conn_;
  int depth_;
  const char* catalog_;
  const char* db_schema_;
  const char* table_name_;
  const char** table_types_;
  const char* column_name_;
  struct ArrowSchema* schema_;
  struct ArrowArray* array_;
  struct AdbcError* error_;
  struct ArrowError na_error_;
  std::string current_db_;
  struct ArrowArray* catalog_name_col_;
  struct ArrowArray* catalog_db_schemas_col_;
  struct ArrowArray* catalog_db_schemas_items_;
  struct ArrowArray* db_schema_name_col_;
  struct ArrowArray* db_schema_tables_col_;
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

AdbcStatusCode PostgresConnection::GetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_types,
    const char* column_name, struct ArrowArrayStream* out, struct AdbcError* error) {
  struct ArrowSchema schema = {0};
  struct ArrowArray array = {0};

  PqGetObjectsHelper helper =
      PqGetObjectsHelper(conn_, depth, catalog, db_schema, table_name, table_types,
                         column_name, &schema, &array, error);
  AdbcStatusCode status = helper.GetObjects();

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
  std::vector<std::string> params;
  if (StringBuilderInit(&query, /*initial_size=*/256) != 0) return ADBC_STATUS_INTERNAL;

  if (StringBuilderAppend(
          &query, "%s",
          "SELECT attname, atttypid "
          "FROM pg_catalog.pg_class AS cls "
          "INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid "
          "INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid "
          "WHERE attr.attnum >= 0 AND cls.oid = ") != 0)
    return ADBC_STATUS_INTERNAL;

  if (db_schema != nullptr) {
    if (StringBuilderAppend(&query, "%s", "$1.")) {
      StringBuilderReset(&query);
      return ADBC_STATUS_INTERNAL;
    }
    params.push_back(db_schema);
  }

  if (StringBuilderAppend(&query, "%s%" PRIu64 "%s", "$",
                          static_cast<uint64_t>(params.size() + 1), "::regclass::oid")) {
    StringBuilderReset(&query);
    return ADBC_STATUS_INTERNAL;
  }
  params.push_back(table_name);

  PqResultHelper result_helper =
      PqResultHelper{conn_, std::string(query.buffer), params, error};
  StringBuilderReset(&query);

  RAISE_ADBC(result_helper.Prepare());
  RAISE_ADBC(result_helper.Execute());

  auto uschema = nanoarrow::UniqueSchema();
  ArrowSchemaInit(uschema.get());
  CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(uschema.get(), result_helper.NumRows()),
           error);

  ArrowError na_error;
  int row_counter = 0;
  for (auto row : result_helper) {
    const char* colname = row[0].data;
    const Oid pg_oid =
        static_cast<uint32_t>(std::strtol(row[1].data, /*str_end=*/nullptr, /*base=*/10));

    PostgresType pg_type;
    if (type_resolver_->Find(pg_oid, &pg_type, &na_error) != NANOARROW_OK) {
      SetError(error, "%s%d%s%s%s%" PRIu32, "Column #", row_counter + 1, " (\"", colname,
               "\") has unknown type code ", pg_oid);
      final_status = ADBC_STATUS_NOT_IMPLEMENTED;
      break;
    }
    CHECK_NA(INTERNAL,
             pg_type.WithFieldName(colname).SetSchema(uschema->children[row_counter]),
             error);
    row_counter++;
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
