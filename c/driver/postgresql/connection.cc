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
#include <cmath>
#include <cstring>
#include <memory>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>

#include <adbc.h>
#include <libpq-fe.h>

#include "database.h"
#include "driver/common/utils.h"
#include "driver/framework/catalog.h"
#include "error.h"
#include "result_helper.h"

namespace adbcpq {
namespace {

static const uint32_t kSupportedInfoCodes[] = {
    ADBC_INFO_VENDOR_NAME,          ADBC_INFO_VENDOR_VERSION,
    ADBC_INFO_DRIVER_NAME,          ADBC_INFO_DRIVER_VERSION,
    ADBC_INFO_DRIVER_ARROW_VERSION, ADBC_INFO_DRIVER_ADBC_VERSION,
};

static const std::unordered_map<std::string, std::string> kPgTableTypes = {
    {"table", "r"},       {"view", "v"},          {"materialized_view", "m"},
    {"toast_table", "t"}, {"foreign_table", "f"}, {"partitioned_table", "p"}};

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
    RAISE_ADBC(InitArrowArray());

    catalog_name_col_ = array_->children[0];
    catalog_db_schemas_col_ = array_->children[1];
    catalog_db_schemas_items_ = catalog_db_schemas_col_->children[0];
    db_schema_name_col_ = catalog_db_schemas_items_->children[0];
    db_schema_tables_col_ = catalog_db_schemas_items_->children[1];
    schema_table_items_ = db_schema_tables_col_->children[0];
    table_name_col_ = schema_table_items_->children[0];
    table_type_col_ = schema_table_items_->children[1];

    table_columns_col_ = schema_table_items_->children[2];
    table_columns_items_ = table_columns_col_->children[0];
    column_name_col_ = table_columns_items_->children[0];
    column_position_col_ = table_columns_items_->children[1];
    column_remarks_col_ = table_columns_items_->children[2];

    table_constraints_col_ = schema_table_items_->children[3];
    table_constraints_items_ = table_constraints_col_->children[0];
    constraint_name_col_ = table_constraints_items_->children[0];
    constraint_type_col_ = table_constraints_items_->children[1];

    constraint_column_names_col_ = table_constraints_items_->children[2];
    constraint_column_name_col_ = constraint_column_names_col_->children[0];

    constraint_column_usages_col_ = table_constraints_items_->children[3];
    constraint_column_usage_items_ = constraint_column_usages_col_->children[0];
    fk_catalog_col_ = constraint_column_usage_items_->children[0];
    fk_db_schema_col_ = constraint_column_usage_items_->children[1];
    fk_table_col_ = constraint_column_usage_items_->children[2];
    fk_column_name_col_ = constraint_column_usage_items_->children[3];

    RAISE_ADBC(AppendCatalogs());
    RAISE_ADBC(FinishArrowArray());
    return ADBC_STATUS_OK;
  }

 private:
  AdbcStatusCode InitArrowArray() {
    RAISE_ADBC(adbc::driver::AdbcInitConnectionObjectsSchema(schema_).ToAdbc(error_));

    CHECK_NA_DETAIL(INTERNAL, ArrowArrayInitFromSchema(array_, schema_, &na_error_),
                    &na_error_, error_);

    CHECK_NA(INTERNAL, ArrowArrayStartAppending(array_), error_);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode AppendSchemas(std::string db_name) {
    // postgres only allows you to list schemas for the currently connected db
    if (!strcmp(db_name.c_str(), PQdb(conn_))) {
      struct StringBuilder query;
      std::memset(&query, 0, sizeof(query));
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
        if (depth_ == ADBC_OBJECT_DEPTH_DB_SCHEMAS) {
          CHECK_NA(INTERNAL, ArrowArrayAppendNull(db_schema_tables_col_, 1), error_);
        } else {
          RAISE_ADBC(AppendTables(std::string(schema_name)));
        }
        CHECK_NA(INTERNAL, ArrowArrayFinishElement(catalog_db_schemas_items_), error_);
      }
    }

    CHECK_NA(INTERNAL, ArrowArrayFinishElement(catalog_db_schemas_col_), error_);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode AppendCatalogs() {
    struct StringBuilder query;
    std::memset(&query, 0, sizeof(query));
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

  AdbcStatusCode AppendTables(std::string schema_name) {
    struct StringBuilder query;
    std::memset(&query, 0, sizeof(query));
    if (StringBuilderInit(&query, /*initial_size*/ 512)) {
      return ADBC_STATUS_INTERNAL;
    }

    std::vector<std::string> params = {schema_name};
    const char* stmt =
        "SELECT c.relname, CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' "
        "WHEN 'm' THEN 'materialized view' WHEN 't' THEN 'TOAST table' "
        "WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' END "
        "AS reltype FROM pg_catalog.pg_class c "
        "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
        "WHERE c.relkind IN ('r','v','m','t','f','p') "
        "AND pg_catalog.pg_table_is_visible(c.oid) AND n.nspname = $1";

    if (StringBuilderAppend(&query, "%s", stmt)) {
      StringBuilderReset(&query);
      return ADBC_STATUS_INTERNAL;
    }

    if (table_name_ != nullptr) {
      if (StringBuilderAppend(&query, "%s", " AND c.relname LIKE $2")) {
        StringBuilderReset(&query);
        return ADBC_STATUS_INTERNAL;
      }

      params.push_back(std::string(table_name_));
    }

    if (table_types_ != nullptr) {
      std::vector<std::string> table_type_filter;
      const char** table_types = table_types_;
      while (*table_types != NULL) {
        auto table_type_str = std::string(*table_types);
        auto search = kPgTableTypes.find(table_type_str);
        if (search != kPgTableTypes.end()) {
          table_type_filter.push_back(search->second);
        }
        table_types++;
      }

      if (!table_type_filter.empty()) {
        std::ostringstream oss;
        bool first = true;
        oss << "(";
        for (const auto& str : table_type_filter) {
          if (!first) {
            oss << ", ";
          }
          oss << "'" << str << "'";
          first = false;
        }
        oss << ")";

        if (StringBuilderAppend(&query, "%s%s", " AND c.relkind IN ",
                                oss.str().c_str())) {
          StringBuilderReset(&query);
          return ADBC_STATUS_INTERNAL;
        }
      } else {
        // no matching table type means no records should come back
        if (StringBuilderAppend(&query, "%s", " AND false")) {
          StringBuilderReset(&query);
          return ADBC_STATUS_INTERNAL;
        }
      }
    }

    auto result_helper = PqResultHelper{conn_, query.buffer, params, error_};
    StringBuilderReset(&query);

    RAISE_ADBC(result_helper.Prepare());
    RAISE_ADBC(result_helper.Execute());
    for (PqResultRow row : result_helper) {
      const char* table_name = row[0].data;
      const char* table_type = row[1].data;

      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(table_name_col_, ArrowCharView(table_name)),
               error_);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(table_type_col_, ArrowCharView(table_type)),
               error_);
      if (depth_ == ADBC_OBJECT_DEPTH_TABLES) {
        CHECK_NA(INTERNAL, ArrowArrayAppendNull(table_columns_col_, 1), error_);
        CHECK_NA(INTERNAL, ArrowArrayAppendNull(table_constraints_col_, 1), error_);
      } else {
        auto table_name_s = std::string(table_name);
        RAISE_ADBC(AppendColumns(schema_name, table_name_s));
        RAISE_ADBC(AppendConstraints(schema_name, table_name_s));
      }
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(schema_table_items_), error_);
    }

    CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_tables_col_), error_);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode AppendColumns(std::string schema_name, std::string table_name) {
    struct StringBuilder query;
    std::memset(&query, 0, sizeof(query));
    if (StringBuilderInit(&query, /*initial_size*/ 512)) {
      return ADBC_STATUS_INTERNAL;
    }

    std::vector<std::string> params = {schema_name, table_name};
    const char* stmt =
        "SELECT attr.attname, attr.attnum, "
        "pg_catalog.col_description(cls.oid, attr.attnum) "
        "FROM pg_catalog.pg_attribute AS attr "
        "INNER JOIN pg_catalog.pg_class AS cls ON attr.attrelid = cls.oid "
        "INNER JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace "
        "WHERE attr.attnum > 0 AND NOT attr.attisdropped "
        "AND nsp.nspname LIKE $1 AND cls.relname LIKE $2";

    if (StringBuilderAppend(&query, "%s", stmt)) {
      StringBuilderReset(&query);
      return ADBC_STATUS_INTERNAL;
    }

    if (column_name_ != NULL) {
      if (StringBuilderAppend(&query, "%s", " AND attr.attname LIKE $3")) {
        StringBuilderReset(&query);
        return ADBC_STATUS_INTERNAL;
      }

      params.push_back(std::string(column_name_));
    }

    auto result_helper = PqResultHelper{conn_, query.buffer, params, error_};
    StringBuilderReset(&query);

    RAISE_ADBC(result_helper.Prepare());
    RAISE_ADBC(result_helper.Execute());

    for (PqResultRow row : result_helper) {
      const char* column_name = row[0].data;
      const char* position = row[1].data;

      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(column_name_col_, ArrowCharView(column_name)),
               error_);
      int ival = atol(position);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendInt(column_position_col_, static_cast<int64_t>(ival)),
               error_);
      if (row[2].is_null) {
        CHECK_NA(INTERNAL, ArrowArrayAppendNull(column_remarks_col_, 1), error_);
      } else {
        const char* remarks = row[2].data;
        CHECK_NA(INTERNAL,
                 ArrowArrayAppendString(column_remarks_col_, ArrowCharView(remarks)),
                 error_);
      }

      // no xdbc_ values for now
      for (auto i = 3; i < 19; i++) {
        CHECK_NA(INTERNAL, ArrowArrayAppendNull(table_columns_items_->children[i], 1),
                 error_);
      }

      CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_columns_items_), error_);
    }

    CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_columns_col_), error_);
    return ADBC_STATUS_OK;
  }

  // libpq PQexecParams can use either text or binary transfers
  // For now we are using text transfer internally, so arrays are sent
  // back like {element1, element2} within a const char*
  std::vector<std::string> PqTextArrayToVector(std::string text_array) {
    text_array.erase(0, 1);
    text_array.erase(text_array.size() - 1);

    std::vector<std::string> elements;
    std::stringstream ss(std::move(text_array));
    std::string tmp;

    while (getline(ss, tmp, ',')) {
      elements.push_back(std::move(tmp));
    }

    return elements;
  }

  AdbcStatusCode AppendConstraints(std::string schema_name, std::string table_name) {
    struct StringBuilder query;
    std::memset(&query, 0, sizeof(query));
    if (StringBuilderInit(&query, /*initial_size*/ 4096)) {
      return ADBC_STATUS_INTERNAL;
    }

    std::vector<std::string> params = {schema_name, table_name};
    const char* stmt =
        "WITH fk_unnest AS ( "
        "    SELECT "
        "        con.conname, "
        "        'FOREIGN KEY' AS contype, "
        "        conrelid, "
        "        UNNEST(con.conkey) AS conkey, "
        "        confrelid, "
        "        UNNEST(con.confkey) AS confkey "
        "    FROM pg_catalog.pg_constraint AS con "
        "    INNER JOIN pg_catalog.pg_class AS cls ON cls.oid = conrelid "
        "    INNER JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace "
        "    WHERE con.contype = 'f' AND nsp.nspname LIKE $1 "
        "    AND cls.relname LIKE $2 "
        "), "
        "fk_names AS ( "
        "    SELECT "
        "        fk_unnest.conname, "
        "        fk_unnest.contype, "
        "        fk_unnest.conkey, "
        "        fk_unnest.confkey, "
        "        attr.attname, "
        "        fnsp.nspname AS fschema, "
        "        fcls.relname AS ftable, "
        "        fattr.attname AS fattname "
        "    FROM fk_unnest "
        "    INNER JOIN pg_catalog.pg_class AS cls ON cls.oid = fk_unnest.conrelid "
        "    INNER JOIN pg_catalog.pg_class AS fcls ON fcls.oid = fk_unnest.confrelid "
        "    INNER JOIN pg_catalog.pg_namespace AS fnsp ON fnsp.oid = fcls.relnamespace"
        "    INNER JOIN pg_catalog.pg_attribute AS attr ON attr.attnum = "
        "fk_unnest.conkey "
        "        AND attr.attrelid = fk_unnest.conrelid "
        "    LEFT JOIN pg_catalog.pg_attribute AS fattr ON fattr.attnum =  "
        "fk_unnest.confkey "
        "        AND fattr.attrelid = fk_unnest.confrelid "
        "), "
        "fkeys AS ( "
        "    SELECT "
        "        conname, "
        "        contype, "
        "        ARRAY_AGG(attname ORDER BY conkey) AS colnames, "
        "        fschema, "
        "        ftable, "
        "        ARRAY_AGG(fattname ORDER BY confkey) AS fcolnames "
        "    FROM fk_names "
        "    GROUP BY "
        "        conname, "
        "        contype, "
        "        fschema, "
        "        ftable "
        "), "
        "other_constraints AS ( "
        "    SELECT con.conname, CASE con.contype WHEN 'c' THEN 'CHECK' WHEN 'u' THEN  "
        "    'UNIQUE' WHEN 'p' THEN 'PRIMARY KEY' END AS contype, "
        "    ARRAY_AGG(attr.attname) AS colnames "
        "    FROM pg_catalog.pg_constraint AS con  "
        "    CROSS JOIN UNNEST(conkey) AS conkeys  "
        "    INNER JOIN pg_catalog.pg_class AS cls ON cls.oid = con.conrelid  "
        "    INNER JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace  "
        "    INNER JOIN pg_catalog.pg_attribute AS attr ON attr.attnum = conkeys  "
        "    AND cls.oid = attr.attrelid  "
        "    WHERE con.contype IN ('c', 'u', 'p') AND nsp.nspname LIKE $1 "
        "    AND cls.relname LIKE $2 "
        "    GROUP BY conname, contype "
        ") "
        "SELECT "
        "    conname, contype, colnames, fschema, ftable, fcolnames "
        "FROM fkeys "
        "UNION ALL "
        "SELECT "
        "    conname, contype, colnames, NULL, NULL, NULL "
        "FROM other_constraints";

    if (StringBuilderAppend(&query, "%s", stmt)) {
      StringBuilderReset(&query);
      return ADBC_STATUS_INTERNAL;
    }

    if (column_name_ != NULL) {
      if (StringBuilderAppend(&query, "%s", " WHERE conname LIKE $3")) {
        StringBuilderReset(&query);
        return ADBC_STATUS_INTERNAL;
      }

      params.push_back(std::string(column_name_));
    }

    auto result_helper = PqResultHelper{conn_, query.buffer, params, error_};
    StringBuilderReset(&query);

    RAISE_ADBC(result_helper.Prepare());
    RAISE_ADBC(result_helper.Execute());

    for (PqResultRow row : result_helper) {
      const char* constraint_name = row[0].data;
      const char* constraint_type = row[1].data;

      CHECK_NA(
          INTERNAL,
          ArrowArrayAppendString(constraint_name_col_, ArrowCharView(constraint_name)),
          error_);

      CHECK_NA(
          INTERNAL,
          ArrowArrayAppendString(constraint_type_col_, ArrowCharView(constraint_type)),
          error_);

      auto constraint_column_names = PqTextArrayToVector(std::string(row[2].data));
      for (const auto& constraint_column_name : constraint_column_names) {
        CHECK_NA(INTERNAL,
                 ArrowArrayAppendString(constraint_column_name_col_,
                                        ArrowCharView(constraint_column_name.c_str())),
                 error_);
      }
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(constraint_column_names_col_), error_);

      if (!strcmp(constraint_type, "FOREIGN KEY")) {
        assert(!row[3].is_null);
        assert(!row[4].is_null);
        assert(!row[5].is_null);

        const char* constraint_ftable_schema = row[3].data;
        const char* constraint_ftable_name = row[4].data;
        auto constraint_fcolumn_names = PqTextArrayToVector(std::string(row[5].data));
        for (const auto& constraint_fcolumn_name : constraint_fcolumn_names) {
          CHECK_NA(INTERNAL,
                   ArrowArrayAppendString(fk_catalog_col_, ArrowCharView(PQdb(conn_))),
                   error_);
          CHECK_NA(INTERNAL,
                   ArrowArrayAppendString(fk_db_schema_col_,
                                          ArrowCharView(constraint_ftable_schema)),
                   error_);
          CHECK_NA(INTERNAL,
                   ArrowArrayAppendString(fk_table_col_,
                                          ArrowCharView(constraint_ftable_name)),
                   error_);
          CHECK_NA(INTERNAL,
                   ArrowArrayAppendString(fk_column_name_col_,
                                          ArrowCharView(constraint_fcolumn_name.c_str())),
                   error_);

          CHECK_NA(INTERNAL, ArrowArrayFinishElement(constraint_column_usage_items_),
                   error_);
        }
      }
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(constraint_column_usages_col_), error_);
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_constraints_items_), error_);
    }

    CHECK_NA(INTERNAL, ArrowArrayFinishElement(table_constraints_col_), error_);
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
  struct ArrowArray* catalog_name_col_;
  struct ArrowArray* catalog_db_schemas_col_;
  struct ArrowArray* catalog_db_schemas_items_;
  struct ArrowArray* db_schema_name_col_;
  struct ArrowArray* db_schema_tables_col_;
  struct ArrowArray* schema_table_items_;
  struct ArrowArray* table_name_col_;
  struct ArrowArray* table_type_col_;
  struct ArrowArray* table_columns_col_;
  struct ArrowArray* table_columns_items_;
  struct ArrowArray* column_name_col_;
  struct ArrowArray* column_position_col_;
  struct ArrowArray* column_remarks_col_;
  struct ArrowArray* table_constraints_col_;
  struct ArrowArray* table_constraints_items_;
  struct ArrowArray* constraint_name_col_;
  struct ArrowArray* constraint_type_col_;
  struct ArrowArray* constraint_column_names_col_;
  struct ArrowArray* constraint_column_name_col_;
  struct ArrowArray* constraint_column_usages_col_;
  struct ArrowArray* constraint_column_usage_items_;
  struct ArrowArray* fk_catalog_col_;
  struct ArrowArray* fk_db_schema_col_;
  struct ArrowArray* fk_table_col_;
  struct ArrowArray* fk_column_name_col_;
};

// A notice processor that does nothing with notices. In the future we can log
// these, but this suppresses the default of printing to stderr.
void SilentNoticeProcessor(void* /*arg*/, const char* /*message*/) {}

}  // namespace

AdbcStatusCode PostgresConnection::Cancel(struct AdbcError* error) {
  // > errbuf must be a char array of size errbufsize (the recommended size is
  // > 256 bytes).
  // https://www.postgresql.org/docs/current/libpq-cancel.html
  char errbuf[256];
  // > The return value is 1 if the cancel request was successfully dispatched
  // > and 0 if not.
  if (PQcancel(cancel_, errbuf, sizeof(errbuf)) != 1) {
    SetError(error, "[libpq] Failed to cancel operation: %s", errbuf);
    return ADBC_STATUS_UNKNOWN;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::Commit(struct AdbcError* error) {
  if (autocommit_) {
    SetError(error, "%s", "[libpq] Cannot commit when autocommit is enabled");
    return ADBC_STATUS_INVALID_STATE;
  }

  PGresult* result = PQexec(conn_, "COMMIT; BEGIN TRANSACTION");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    AdbcStatusCode code = SetError(error, result, "%s%s",
                                   "[libpq] Failed to commit: ", PQerrorMessage(conn_));
    PQclear(result);
    return code;
  }
  PQclear(result);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::PostgresConnectionGetInfoImpl(
    const uint32_t* info_codes, size_t info_codes_length, struct ArrowSchema* schema,
    struct ArrowArray* array, struct AdbcError* error) {
  RAISE_ADBC(adbc::driver::AdbcInitConnectionGetInfoSchema(schema, array).ToAdbc(error));

  for (size_t i = 0; i < info_codes_length; i++) {
    switch (info_codes[i]) {
      case ADBC_INFO_VENDOR_NAME:
        RAISE_ADBC(adbc::driver::AdbcConnectionGetInfoAppendString(array, info_codes[i],
                                                                   "PostgreSQL")
                       .ToAdbc(error));
        break;
      case ADBC_INFO_VENDOR_VERSION: {
        const char* stmt = "SHOW server_version_num";
        auto result_helper = PqResultHelper{conn_, std::string(stmt), error};
        RAISE_ADBC(result_helper.Prepare());
        RAISE_ADBC(result_helper.Execute());
        auto it = result_helper.begin();
        if (it == result_helper.end()) {
          SetError(error, "[libpq] PostgreSQL returned no rows for '%s'", stmt);
          return ADBC_STATUS_INTERNAL;
        }
        const char* server_version_num = (*it)[0].data;

        RAISE_ADBC(adbc::driver::AdbcConnectionGetInfoAppendString(array, info_codes[i],
                                                                   server_version_num)
                       .ToAdbc(error));
        break;
      }
      case ADBC_INFO_DRIVER_NAME:
        RAISE_ADBC(adbc::driver::AdbcConnectionGetInfoAppendString(
                       array, info_codes[i], "ADBC PostgreSQL Driver")
                       .ToAdbc(error));
        break;
      case ADBC_INFO_DRIVER_VERSION:
        // TODO(lidavidm): fill in driver version
        RAISE_ADBC(adbc::driver::AdbcConnectionGetInfoAppendString(array, info_codes[i],
                                                                   "(unknown)")
                       .ToAdbc(error));
        break;
      case ADBC_INFO_DRIVER_ARROW_VERSION:
        RAISE_ADBC(adbc::driver::AdbcConnectionGetInfoAppendString(array, info_codes[i],
                                                                   NANOARROW_VERSION)
                       .ToAdbc(error));
        break;
      case ADBC_INFO_DRIVER_ADBC_VERSION:
        RAISE_ADBC(adbc::driver::AdbcConnectionGetInfoAppendInt(array, info_codes[i],
                                                                ADBC_VERSION_1_1_0)
                       .ToAdbc(error));
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
                                           const uint32_t* info_codes,
                                           size_t info_codes_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  if (!info_codes) {
    info_codes = kSupportedInfoCodes;
    info_codes_length = sizeof(kSupportedInfoCodes) / sizeof(kSupportedInfoCodes[0]);
  }

  struct ArrowSchema schema;
  std::memset(&schema, 0, sizeof(schema));
  struct ArrowArray array;
  std::memset(&array, 0, sizeof(array));

  AdbcStatusCode status = PostgresConnectionGetInfoImpl(info_codes, info_codes_length,
                                                        &schema, &array, error);
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
  struct ArrowSchema schema;
  std::memset(&schema, 0, sizeof(schema));
  struct ArrowArray array;
  std::memset(&array, 0, sizeof(array));

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

AdbcStatusCode PostgresConnection::GetOption(const char* option, char* value,
                                             size_t* length, struct AdbcError* error) {
  std::string output;
  if (std::strcmp(option, ADBC_CONNECTION_OPTION_CURRENT_CATALOG) == 0) {
    output = PQdb(conn_);
  } else if (std::strcmp(option, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA) == 0) {
    PqResultHelper result_helper{conn_, "SELECT CURRENT_SCHEMA", {}, error};
    RAISE_ADBC(result_helper.Prepare());
    RAISE_ADBC(result_helper.Execute());
    auto it = result_helper.begin();
    if (it == result_helper.end()) {
      SetError(error, "[libpq] PostgreSQL returned no rows for 'SELECT CURRENT_SCHEMA'");
      return ADBC_STATUS_INTERNAL;
    }
    output = (*it)[0].data;
  } else if (std::strcmp(option, ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0) {
    output = autocommit_ ? ADBC_OPTION_VALUE_ENABLED : ADBC_OPTION_VALUE_DISABLED;
  } else {
    return ADBC_STATUS_NOT_FOUND;
  }

  if (output.size() + 1 <= *length) {
    std::memcpy(value, output.c_str(), output.size() + 1);
  }
  *length = output.size() + 1;
  return ADBC_STATUS_OK;
}
AdbcStatusCode PostgresConnection::GetOptionBytes(const char* option, uint8_t* value,
                                                  size_t* length,
                                                  struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}
AdbcStatusCode PostgresConnection::GetOptionInt(const char* option, int64_t* value,
                                                struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}
AdbcStatusCode PostgresConnection::GetOptionDouble(const char* option, double* value,
                                                   struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode PostgresConnectionGetStatisticsImpl(PGconn* conn, const char* db_schema,
                                                   const char* table_name,
                                                   struct ArrowSchema* schema,
                                                   struct ArrowArray* array,
                                                   struct AdbcError* error) {
  // Set up schema
  auto uschema = nanoarrow::UniqueSchema();
  {
    ArrowSchemaInit(uschema.get());
    CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(uschema.get(), /*num_columns=*/2), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetType(uschema->children[0], NANOARROW_TYPE_STRING),
             error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(uschema->children[0], "catalog_name"), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetType(uschema->children[1], NANOARROW_TYPE_LIST),
             error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(uschema->children[1], "catalog_db_schemas"),
             error);
    CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(uschema->children[1]->children[0], 2),
             error);
    uschema->children[1]->flags &= ~ARROW_FLAG_NULLABLE;

    struct ArrowSchema* db_schema_schema = uschema->children[1]->children[0];
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(db_schema_schema->children[0], NANOARROW_TYPE_STRING),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetName(db_schema_schema->children[0], "db_schema_name"), error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(db_schema_schema->children[1], NANOARROW_TYPE_LIST),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetName(db_schema_schema->children[1], "db_schema_statistics"),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetTypeStruct(db_schema_schema->children[1]->children[0], 5),
             error);
    db_schema_schema->children[1]->flags &= ~ARROW_FLAG_NULLABLE;

    struct ArrowSchema* statistics_schema = db_schema_schema->children[1]->children[0];
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(statistics_schema->children[0], NANOARROW_TYPE_STRING),
             error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(statistics_schema->children[0], "table_name"),
             error);
    statistics_schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(statistics_schema->children[1], NANOARROW_TYPE_STRING),
             error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(statistics_schema->children[1], "column_name"),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(statistics_schema->children[2], NANOARROW_TYPE_INT16),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetName(statistics_schema->children[2], "statistic_key"), error);
    statistics_schema->children[2]->flags &= ~ARROW_FLAG_NULLABLE;
    CHECK_NA(INTERNAL,
             ArrowSchemaSetTypeUnion(statistics_schema->children[3],
                                     NANOARROW_TYPE_DENSE_UNION, 4),
             error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetName(statistics_schema->children[3], "statistic_value"),
             error);
    statistics_schema->children[3]->flags &= ~ARROW_FLAG_NULLABLE;
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(statistics_schema->children[4], NANOARROW_TYPE_BOOL),
             error);
    CHECK_NA(
        INTERNAL,
        ArrowSchemaSetName(statistics_schema->children[4], "statistic_is_approximate"),
        error);
    statistics_schema->children[4]->flags &= ~ARROW_FLAG_NULLABLE;

    struct ArrowSchema* value_schema = statistics_schema->children[3];
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(value_schema->children[0], NANOARROW_TYPE_INT64), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(value_schema->children[0], "int64"), error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(value_schema->children[1], NANOARROW_TYPE_UINT64), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(value_schema->children[1], "uint64"), error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(value_schema->children[2], NANOARROW_TYPE_DOUBLE), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(value_schema->children[2], "float64"), error);
    CHECK_NA(INTERNAL,
             ArrowSchemaSetType(value_schema->children[3], NANOARROW_TYPE_BINARY), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(value_schema->children[3], "binary"), error);
  }

  // Set up builders
  struct ArrowError na_error = {0};
  CHECK_NA_DETAIL(INTERNAL, ArrowArrayInitFromSchema(array, uschema.get(), &na_error),
                  &na_error, error);
  CHECK_NA(INTERNAL, ArrowArrayStartAppending(array), error);

  struct ArrowArray* catalog_name_col = array->children[0];
  struct ArrowArray* catalog_db_schemas_col = array->children[1];
  struct ArrowArray* catalog_db_schemas_items = catalog_db_schemas_col->children[0];
  struct ArrowArray* db_schema_name_col = catalog_db_schemas_items->children[0];
  struct ArrowArray* db_schema_statistics_col = catalog_db_schemas_items->children[1];
  struct ArrowArray* db_schema_statistics_items = db_schema_statistics_col->children[0];
  struct ArrowArray* statistics_table_name_col = db_schema_statistics_items->children[0];
  struct ArrowArray* statistics_column_name_col = db_schema_statistics_items->children[1];
  struct ArrowArray* statistics_key_col = db_schema_statistics_items->children[2];
  struct ArrowArray* statistics_value_col = db_schema_statistics_items->children[3];
  struct ArrowArray* statistics_is_approximate_col =
      db_schema_statistics_items->children[4];
  // struct ArrowArray* value_int64_col = statistics_value_col->children[0];
  // struct ArrowArray* value_uint64_col = statistics_value_col->children[1];
  struct ArrowArray* value_float64_col = statistics_value_col->children[2];
  // struct ArrowArray* value_binary_col = statistics_value_col->children[3];

  // Query (could probably be massively improved)
  std::string query = R"(
    WITH
      class AS (
        SELECT nspname, relname, reltuples
        FROM pg_namespace
        INNER JOIN pg_class ON pg_class.relnamespace = pg_namespace.oid
      )
    SELECT tablename, attname, null_frac, avg_width, n_distinct, reltuples
    FROM pg_stats
    INNER JOIN class ON pg_stats.schemaname = class.nspname AND pg_stats.tablename = class.relname
    WHERE pg_stats.schemaname = $1 AND tablename LIKE $2
    ORDER BY tablename
)";

  CHECK_NA(INTERNAL, ArrowArrayAppendString(catalog_name_col, ArrowCharView(PQdb(conn))),
           error);
  CHECK_NA(INTERNAL, ArrowArrayAppendString(db_schema_name_col, ArrowCharView(db_schema)),
           error);

  constexpr int8_t kStatsVariantFloat64 = 2;

  std::string prev_table;

  {
    PqResultHelper result_helper{
        conn, query, {db_schema, table_name ? table_name : "%"}, error};
    RAISE_ADBC(result_helper.Prepare());
    RAISE_ADBC(result_helper.Execute());

    for (PqResultRow row : result_helper) {
      auto reltuples = row[5].ParseDouble();
      if (!reltuples) {
        SetError(error, "[libpq] Invalid double value in reltuples: '%s'", row[5].data);
        return ADBC_STATUS_INTERNAL;
      }

      if (std::strcmp(prev_table.c_str(), row[0].data) != 0) {
        CHECK_NA(INTERNAL,
                 ArrowArrayAppendString(statistics_table_name_col,
                                        ArrowStringView{row[0].data, row[0].len}),
                 error);
        CHECK_NA(INTERNAL, ArrowArrayAppendNull(statistics_column_name_col, 1), error);
        CHECK_NA(INTERNAL,
                 ArrowArrayAppendInt(statistics_key_col, ADBC_STATISTIC_ROW_COUNT_KEY),
                 error);
        CHECK_NA(INTERNAL, ArrowArrayAppendDouble(value_float64_col, *reltuples), error);
        CHECK_NA(INTERNAL,
                 ArrowArrayFinishUnionElement(statistics_value_col, kStatsVariantFloat64),
                 error);
        CHECK_NA(INTERNAL, ArrowArrayAppendInt(statistics_is_approximate_col, 1), error);
        CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_statistics_items), error);
        prev_table = std::string(row[0].data, row[0].len);
      }

      auto null_frac = row[2].ParseDouble();
      if (!null_frac) {
        SetError(error, "[libpq] Invalid double value in null_frac: '%s'", row[2].data);
        return ADBC_STATUS_INTERNAL;
      }

      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_table_name_col,
                                      ArrowStringView{row[0].data, row[0].len}),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_column_name_col,
                                      ArrowStringView{row[1].data, row[1].len}),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendInt(statistics_key_col, ADBC_STATISTIC_NULL_COUNT_KEY),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendDouble(value_float64_col, *null_frac * *reltuples), error);
      CHECK_NA(INTERNAL,
               ArrowArrayFinishUnionElement(statistics_value_col, kStatsVariantFloat64),
               error);
      CHECK_NA(INTERNAL, ArrowArrayAppendInt(statistics_is_approximate_col, 1), error);
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_statistics_items), error);

      auto average_byte_width = row[3].ParseDouble();
      if (!average_byte_width) {
        SetError(error, "[libpq] Invalid double value in avg_width: '%s'", row[3].data);
        return ADBC_STATUS_INTERNAL;
      }

      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_table_name_col,
                                      ArrowStringView{row[0].data, row[0].len}),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_column_name_col,
                                      ArrowStringView{row[1].data, row[1].len}),
               error);
      CHECK_NA(
          INTERNAL,
          ArrowArrayAppendInt(statistics_key_col, ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_KEY),
          error);
      CHECK_NA(INTERNAL, ArrowArrayAppendDouble(value_float64_col, *average_byte_width),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayFinishUnionElement(statistics_value_col, kStatsVariantFloat64),
               error);
      CHECK_NA(INTERNAL, ArrowArrayAppendInt(statistics_is_approximate_col, 1), error);
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_statistics_items), error);

      auto n_distinct = row[4].ParseDouble();
      if (!n_distinct) {
        SetError(error, "[libpq] Invalid double value in avg_width: '%s'", row[4].data);
        return ADBC_STATUS_INTERNAL;
      }

      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_table_name_col,
                                      ArrowStringView{row[0].data, row[0].len}),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendString(statistics_column_name_col,
                                      ArrowStringView{row[1].data, row[1].len}),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayAppendInt(statistics_key_col, ADBC_STATISTIC_DISTINCT_COUNT_KEY),
               error);
      // > If greater than zero, the estimated number of distinct values in
      // > the column. If less than zero, the negative of the number of
      // > distinct values divided by the number of rows.
      // https://www.postgresql.org/docs/current/view-pg-stats.html
      CHECK_NA(INTERNAL,
               ArrowArrayAppendDouble(
                   value_float64_col,
                   *n_distinct > 0 ? *n_distinct : (std::fabs(*n_distinct) * *reltuples)),
               error);
      CHECK_NA(INTERNAL,
               ArrowArrayFinishUnionElement(statistics_value_col, kStatsVariantFloat64),
               error);
      CHECK_NA(INTERNAL, ArrowArrayAppendInt(statistics_is_approximate_col, 1), error);
      CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_statistics_items), error);
    }
  }

  CHECK_NA(INTERNAL, ArrowArrayFinishElement(db_schema_statistics_col), error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(catalog_db_schemas_items), error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(catalog_db_schemas_col), error);
  CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);

  CHECK_NA_DETAIL(INTERNAL, ArrowArrayFinishBuildingDefault(array, &na_error), &na_error,
                  error);
  uschema.move(schema);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::GetStatistics(const char* catalog,
                                                 const char* db_schema,
                                                 const char* table_name, bool approximate,
                                                 struct ArrowArrayStream* out,
                                                 struct AdbcError* error) {
  // Simplify our jobs here
  if (!approximate) {
    SetError(error, "[libpq] Exact statistics are not implemented");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  } else if (!db_schema) {
    SetError(error, "[libpq] Must request statistics for a single schema");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  } else if (catalog && std::strcmp(catalog, PQdb(conn_)) != 0) {
    SetError(error, "[libpq] Can only request statistics for current catalog");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  struct ArrowSchema schema;
  std::memset(&schema, 0, sizeof(schema));
  struct ArrowArray array;
  std::memset(&array, 0, sizeof(array));

  AdbcStatusCode status = PostgresConnectionGetStatisticsImpl(
      conn_, db_schema, table_name, &schema, &array, error);
  if (status != ADBC_STATUS_OK) {
    if (schema.release) schema.release(&schema);
    if (array.release) array.release(&array);
    return status;
  }

  return BatchToArrayStream(&array, &schema, out, error);
}

AdbcStatusCode PostgresConnectionGetStatisticNamesImpl(struct ArrowSchema* schema,
                                                       struct ArrowArray* array,
                                                       struct AdbcError* error) {
  auto uschema = nanoarrow::UniqueSchema();
  ArrowSchemaInit(uschema.get());

  CHECK_NA(INTERNAL, ArrowSchemaSetType(uschema.get(), NANOARROW_TYPE_STRUCT), error);
  CHECK_NA(INTERNAL, ArrowSchemaAllocateChildren(uschema.get(), /*num_columns=*/2),
           error);

  ArrowSchemaInit(uschema.get()->children[0]);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(uschema.get()->children[0], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(uschema.get()->children[0], "statistic_name"),
           error);
  uschema.get()->children[0]->flags &= ~ARROW_FLAG_NULLABLE;

  ArrowSchemaInit(uschema.get()->children[1]);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(uschema.get()->children[1], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(uschema.get()->children[1], "statistic_key"),
           error);
  uschema.get()->children[1]->flags &= ~ARROW_FLAG_NULLABLE;

  CHECK_NA(INTERNAL, ArrowArrayInitFromSchema(array, uschema.get(), NULL), error);
  CHECK_NA(INTERNAL, ArrowArrayStartAppending(array), error);
  CHECK_NA(INTERNAL, ArrowArrayFinishBuildingDefault(array, NULL), error);

  uschema.move(schema);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::GetStatisticNames(struct ArrowArrayStream* out,
                                                     struct AdbcError* error) {
  // We don't support any extended statistics, just return an empty stream
  struct ArrowSchema schema;
  std::memset(&schema, 0, sizeof(schema));
  struct ArrowArray array;
  std::memset(&array, 0, sizeof(array));

  AdbcStatusCode status = PostgresConnectionGetStatisticNamesImpl(&schema, &array, error);
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

  std::string query =
      "SELECT attname, atttypid "
      "FROM pg_catalog.pg_class AS cls "
      "INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid "
      "INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid "
      "WHERE attr.attnum >= 0 AND cls.oid = $1::regclass::oid";

  std::vector<std::string> params;
  if (db_schema != nullptr) {
    params.push_back(std::string(db_schema) + "." + table_name);
  } else {
    params.push_back(table_name);
  }

  PqResultHelper result_helper =
      PqResultHelper{conn_, std::string(query.c_str()), params, error};

  RAISE_ADBC(result_helper.Prepare());
  auto result = result_helper.Execute();
  if (result != ADBC_STATUS_OK) {
    auto error_code = std::string(error->sqlstate, 5);
    if ((error_code == "42P01") || (error_code == "42602")) {
      return ADBC_STATUS_NOT_FOUND;
    }
    return result;
  }

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

  for (auto const& table_type : kPgTableTypes) {
    CHECK_NA(INTERNAL,
             ArrowArrayAppendString(array->children[0],
                                    ArrowCharView(table_type.first.c_str())),
             error);
    CHECK_NA(INTERNAL, ArrowArrayFinishElement(array), error);
  }

  CHECK_NA(INTERNAL, ArrowArrayFinishBuildingDefault(array, NULL), error);

  uschema.move(schema);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::GetTableTypes(struct AdbcConnection* connection,
                                                 struct ArrowArrayStream* out,
                                                 struct AdbcError* error) {
  struct ArrowSchema schema;
  std::memset(&schema, 0, sizeof(schema));
  struct ArrowArray array;
  std::memset(&array, 0, sizeof(array));

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
    SetError(error, "[libpq] Must provide an initialized AdbcDatabase");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  database_ =
      *reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  type_resolver_ = database_->type_resolver();

  RAISE_ADBC(database_->Connect(&conn_, error));

  cancel_ = PQgetCancel(conn_);
  if (!cancel_) {
    SetError(error, "[libpq] Could not initialize PGcancel");
    return ADBC_STATUS_UNKNOWN;
  }

  std::ignore = PQsetNoticeProcessor(conn_, SilentNoticeProcessor, nullptr);

  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::Release(struct AdbcError* error) {
  if (cancel_) {
    PQfreeCancel(cancel_);
    cancel_ = nullptr;
  }
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
  } else if (std::strcmp(key, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA) == 0) {
    // PostgreSQL doesn't accept a parameter here
    PqResultHelper result_helper{
        conn_, std::string("SET search_path TO ") + value, {}, error};
    RAISE_ADBC(result_helper.Prepare());
    RAISE_ADBC(result_helper.Execute());
    return ADBC_STATUS_OK;
  }
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresConnection::SetOptionBytes(const char* key, const uint8_t* value,
                                                  size_t length,
                                                  struct AdbcError* error) {
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresConnection::SetOptionDouble(const char* key, double value,
                                                   struct AdbcError* error) {
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresConnection::SetOptionInt(const char* key, int64_t value,
                                                struct AdbcError* error) {
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

}  // namespace adbcpq
