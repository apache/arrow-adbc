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

#include <array>
#include <cassert>
#include <cinttypes>
#include <cmath>
#include <cstring>
#include <memory>
#include <optional>
#include <sstream>
#include <string>
#include <string_view>
#include <unordered_map>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <fmt/format.h>
#include <libpq-fe.h>

#include "database.h"
#include "driver/common/utils.h"
#include "driver/framework/objects.h"
#include "driver/framework/utility.h"
#include "error.h"
#include "result_helper.h"

using adbc::driver::Result;
using adbc::driver::Status;

namespace adbcpq {
namespace {

constexpr std::string_view kConnectionOptionTransactionStatus =
    "adbc.postgresql.transaction_status";

static const uint32_t kSupportedInfoCodes[] = {
    ADBC_INFO_VENDOR_NAME,          ADBC_INFO_VENDOR_VERSION,
    ADBC_INFO_DRIVER_NAME,          ADBC_INFO_DRIVER_VERSION,
    ADBC_INFO_DRIVER_ARROW_VERSION, ADBC_INFO_DRIVER_ADBC_VERSION,
};

static const std::unordered_map<std::string, std::string> kPgTableTypes = {
    {"table", "r"},       {"view", "v"},          {"materialized_view", "m"},
    {"toast_table", "t"}, {"foreign_table", "f"}, {"partitioned_table", "p"}};

static const char* kCatalogQueryAll = "SELECT datname FROM pg_catalog.pg_database";

// catalog_name is not a parameter here or on any other queries
// because it will always be the currently connected database.
static const char* kSchemaQueryAll =
    "SELECT nspname FROM pg_catalog.pg_namespace WHERE "
    "nspname !~ '^pg_' AND nspname <> 'information_schema'";

// Parameterized on schema_name, relkind
// Note that when binding relkind as a string it must look like {"r", "v", ...}
// (i.e., double quotes). Binding a binary list<string> element also works.
static const char* kTablesQueryAll =
    "SELECT c.relname, CASE c.relkind WHEN 'r' THEN 'table' WHEN 'v' THEN 'view' "
    "WHEN 'm' THEN 'materialized view' WHEN 't' THEN 'TOAST table' "
    "WHEN 'f' THEN 'foreign table' WHEN 'p' THEN 'partitioned table' END "
    "AS reltype FROM pg_catalog.pg_class c "
    "LEFT JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace "
    "WHERE pg_catalog.pg_table_is_visible(c.oid) AND n.nspname = $1 AND c.relkind = "
    "ANY($2)";

// Parameterized on schema_name, table_name
static const char* kColumnsQueryAll =
    "SELECT attr.attname, attr.attnum, "
    "pg_catalog.col_description(cls.oid, attr.attnum) "
    "FROM pg_catalog.pg_attribute AS attr "
    "INNER JOIN pg_catalog.pg_class AS cls ON attr.attrelid = cls.oid "
    "INNER JOIN pg_catalog.pg_namespace AS nsp ON nsp.oid = cls.relnamespace "
    "WHERE attr.attnum > 0 AND NOT attr.attisdropped "
    "AND nsp.nspname LIKE $1 AND cls.relname LIKE $2";

// Parameterized on schema_name, table_name
static const char* kConstraintsQueryAll =
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
    "    WHERE con.contype = 'f' AND nsp.nspname = $1 "
    "    AND cls.relname = $2 "
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
    "    WHERE con.contype IN ('c', 'u', 'p') AND nsp.nspname = $1 "
    "    AND cls.relname = $2 "
    "    GROUP BY conname, contype "
    ") "
    "SELECT "
    "    conname, contype, colnames, fschema, ftable, fcolnames "
    "FROM fkeys "
    "UNION ALL "
    "SELECT "
    "    conname, contype, colnames, NULL, NULL, NULL "
    "FROM other_constraints";

class PostgresGetObjectsHelper : public adbc::driver::GetObjectsHelper {
 public:
  explicit PostgresGetObjectsHelper(PGconn* conn)
      : current_database_(PQdb(conn)),
        all_catalogs_(conn, kCatalogQueryAll),
        some_catalogs_(conn, CatalogQuery()),
        all_schemas_(conn, kSchemaQueryAll),
        some_schemas_(conn, SchemaQuery()),
        all_tables_(conn, kTablesQueryAll),
        some_tables_(conn, TablesQuery()),
        all_columns_(conn, kColumnsQueryAll),
        some_columns_(conn, ColumnsQuery()),
        all_constraints_(conn, kConstraintsQueryAll),
        some_constraints_(conn, ConstraintsQuery()) {}

  // Allow Redshift to execute this query without constraints
  // TODO(paleolimbot): Investigate to see if we can simplify the constraints query so
  // that it works on both!
  void SetEnableConstraints(bool enable_constraints) {
    enable_constraints_ = enable_constraints;
  }

  Status Load(adbc::driver::GetObjectsDepth depth,
              std::optional<std::string_view> catalog_filter,
              std::optional<std::string_view> schema_filter,
              std::optional<std::string_view> table_filter,
              std::optional<std::string_view> column_filter,
              const std::vector<std::string_view>& table_types) override {
    return Status::Ok();
  }

  Status LoadCatalogs(std::optional<std::string_view> catalog_filter) override {
    if (catalog_filter.has_value()) {
      UNWRAP_STATUS(some_catalogs_.Execute({std::string(*catalog_filter)}));
      next_catalog_ = some_catalogs_.Row(-1);
    } else {
      UNWRAP_STATUS(all_catalogs_.Execute());
      next_catalog_ = all_catalogs_.Row(-1);
    }

    return Status::Ok();
  };

  Result<std::optional<std::string_view>> NextCatalog() override {
    next_catalog_ = next_catalog_.Next();
    if (!next_catalog_.IsValid()) {
      return std::nullopt;
    }

    return next_catalog_[0].value();
  }

  Status LoadSchemas(std::string_view catalog,
                     std::optional<std::string_view> schema_filter) override {
    // PostgreSQL can only list for the current database
    if (catalog != current_database_) {
      return Status::Ok();
    }

    if (schema_filter.has_value()) {
      UNWRAP_STATUS(some_schemas_.Execute({std::string(*schema_filter)}));
      next_schema_ = some_schemas_.Row(-1);
    } else {
      UNWRAP_STATUS(all_schemas_.Execute());
      next_schema_ = all_schemas_.Row(-1);
    }
    return Status::Ok();
  };

  Result<std::optional<std::string_view>> NextSchema() override {
    next_schema_ = next_schema_.Next();
    if (!next_schema_.IsValid()) {
      return std::nullopt;
    }

    return next_schema_[0].value();
  }

  Status LoadTables(std::string_view catalog, std::string_view schema,
                    std::optional<std::string_view> table_filter,
                    const std::vector<std::string_view>& table_types) override {
    std::string table_types_bind = TableTypesArrayLiteral(table_types);

    if (table_filter.has_value()) {
      UNWRAP_STATUS(some_tables_.Execute(
          {std::string(schema), table_types_bind, std::string(*table_filter)}));
      next_table_ = some_tables_.Row(-1);
    } else {
      UNWRAP_STATUS(all_tables_.Execute({std::string(schema), table_types_bind}));
      next_table_ = all_tables_.Row(-1);
    }

    return Status::Ok();
  };

  Result<std::optional<Table>> NextTable() override {
    next_table_ = next_table_.Next();
    if (!next_table_.IsValid()) {
      return std::nullopt;
    }

    return Table{next_table_[0].value(), next_table_[1].value()};
  }

  Status LoadColumns(std::string_view catalog, std::string_view schema,
                     std::string_view table,
                     std::optional<std::string_view> column_filter) override {
    if (column_filter.has_value()) {
      UNWRAP_STATUS(some_columns_.Execute(
          {std::string(schema), std::string(table), std::string(*column_filter)}));
      next_column_ = some_columns_.Row(-1);
    } else {
      UNWRAP_STATUS(all_columns_.Execute({std::string(schema), std::string(table)}));
      next_column_ = all_columns_.Row(-1);
    }

    if (enable_constraints_) {
      if (column_filter.has_value()) {
        UNWRAP_STATUS(some_constraints_.Execute(
            {std::string(schema), std::string(table), std::string(*column_filter)}))
        next_constraint_ = some_constraints_.Row(-1);
      } else {
        UNWRAP_STATUS(
            all_constraints_.Execute({std::string(schema), std::string(table)}));
        next_constraint_ = all_constraints_.Row(-1);
      }
    }

    return Status::Ok();
  };

  Result<std::optional<Column>> NextColumn() override {
    next_column_ = next_column_.Next();
    if (!next_column_.IsValid()) {
      return std::nullopt;
    }

    Column col;
    col.column_name = next_column_[0].value();
    UNWRAP_RESULT(int64_t ordinal_position, next_column_[1].ParseInteger());
    col.ordinal_position = static_cast<int32_t>(ordinal_position);
    if (!next_column_[2].is_null) {
      col.remarks = next_column_[2].value();
    }

    return col;
  }

  Result<std::optional<Constraint>> NextConstraint() override {
    next_constraint_ = next_constraint_.Next();
    if (!next_constraint_.IsValid()) {
      return std::nullopt;
    }

    Constraint out;
    out.name = next_constraint_[0].data;
    out.type = next_constraint_[1].data;

    UNWRAP_RESULT(constraint_fcolumn_names_, next_constraint_[2].ParseTextArray());
    std::vector<std::string_view> fcolumn_names_view;
    for (const std::string& item : constraint_fcolumn_names_) {
      fcolumn_names_view.push_back(item);
    }
    out.column_names = std::move(fcolumn_names_view);

    if (out.type == "FOREIGN KEY") {
      assert(!next_constraint_[3].is_null);
      assert(!next_constraint_[3].is_null);
      assert(!next_constraint_[4].is_null);
      assert(!next_constraint_[5].is_null);

      out.usage = std::vector<ConstraintUsage>();
      UNWRAP_RESULT(constraint_fkey_names_, next_constraint_[5].ParseTextArray());

      for (const auto& item : constraint_fkey_names_) {
        ConstraintUsage usage;
        usage.catalog = current_database_;
        usage.schema = next_constraint_[3].data;
        usage.table = next_constraint_[4].data;
        usage.column = item;

        out.usage->push_back(usage);
      }
    }

    return out;
  }

 private:
  std::string current_database_;

  // Ready-to-Execute() queries
  PqResultHelper all_catalogs_;
  PqResultHelper some_catalogs_;
  PqResultHelper all_schemas_;
  PqResultHelper some_schemas_;
  PqResultHelper all_tables_;
  PqResultHelper some_tables_;
  PqResultHelper all_columns_;
  PqResultHelper some_columns_;
  PqResultHelper all_constraints_;
  PqResultHelper some_constraints_;

  // On Redshift, the constraints query fails
  bool enable_constraints_{true};

  // Iterator state for the catalogs/schema/table/column queries
  PqResultRow next_catalog_;
  PqResultRow next_schema_;
  PqResultRow next_table_;
  PqResultRow next_column_;
  PqResultRow next_constraint_;

  // Owning variants required because the framework versions of these
  // are all based on string_view and the result helper can only parse arrays
  // into std::vector<std::string>.
  std::vector<std::string> constraint_fcolumn_names_;
  std::vector<std::string> constraint_fkey_names_;

  // Queries that are slightly modified versions of the generic queries that allow
  // the filter for that level to be passed through as a parameter. Defined here
  // because global strings should be const char* according to cpplint and using
  // the + operator to concatenate them is the most concise way to construct them.

  // Parameterized on catalog_name
  static std::string CatalogQuery() {
    return std::string(kCatalogQueryAll) + " WHERE datname = $1";
  }

  // Parameterized on schema_name
  static std::string SchemaQuery() {
    return std::string(kSchemaQueryAll) + " AND nspname = $1";
  }

  // Parameterized on schema_name, relkind, table_name
  static std::string TablesQuery() {
    return std::string(kTablesQueryAll) + " AND c.relname LIKE $3";
  }

  // Parameterized on schema_name, table_name, column_name
  static std::string ColumnsQuery() {
    return std::string(kColumnsQueryAll) + " AND attr.attname LIKE $3";
  }

  // Parameterized on schema_name, table_name, column_name
  static std::string ConstraintsQuery() {
    return std::string(kConstraintsQueryAll) + " WHERE conname LIKE $3";
  }

  std::string TableTypesArrayLiteral(const std::vector<std::string_view>& table_types) {
    std::stringstream table_types_bind;
    table_types_bind << "{";
    int table_types_bind_len = 0;

    if (table_types.empty()) {
      for (const auto& item : kPgTableTypes) {
        if (table_types_bind_len > 0) {
          table_types_bind << ", ";
        }

        table_types_bind << "\"" << item.second << "\"";
        table_types_bind_len++;
      }
    } else {
      for (auto type : table_types) {
        const auto maybe_item = kPgTableTypes.find(std::string(type));
        if (maybe_item == kPgTableTypes.end()) {
          continue;
        }

        if (table_types_bind_len > 0) {
          table_types_bind << ", ";
        }

        table_types_bind << "\"" << maybe_item->second << "\"";
        table_types_bind_len++;
      }
    }

    table_types_bind << "}";
    return table_types_bind.str();
  }
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
    InternalAdbcSetError(error, "[libpq] Failed to cancel operation: %s", errbuf);
    return ADBC_STATUS_UNKNOWN;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::Commit(struct AdbcError* error) {
  if (autocommit_) {
    InternalAdbcSetError(error, "%s", "[libpq] Cannot commit when autocommit is enabled");
    return ADBC_STATUS_INVALID_STATE;
  }

  PGTransactionStatusType txn_status = PQtransactionStatus(conn_);
  if (txn_status == PQTRANS_IDLE) {
    // https://github.com/apache/arrow-adbc/issues/2673: don't rollback if the
    // transaction is idle, since it won't have any effect and PostgreSQL will
    // issue a warning on the server side
    return ADBC_STATUS_OK;
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

AdbcStatusCode PostgresConnection::GetInfo(struct AdbcConnection* connection,
                                           const uint32_t* info_codes,
                                           size_t info_codes_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  if (!info_codes) {
    info_codes = kSupportedInfoCodes;
    info_codes_length = sizeof(kSupportedInfoCodes) / sizeof(kSupportedInfoCodes[0]);
  }

  std::vector<adbc::driver::InfoValue> infos;

  for (size_t i = 0; i < info_codes_length; i++) {
    switch (info_codes[i]) {
      case ADBC_INFO_VENDOR_NAME:
        infos.push_back({info_codes[i], std::string(VendorName())});
        break;
      case ADBC_INFO_VENDOR_VERSION: {
        if (VendorName() == "Redshift") {
          const std::array<int, 3>& version = VendorVersion();
          std::string version_string = std::to_string(version[0]) + "." +
                                       std::to_string(version[1]) + "." +
                                       std::to_string(version[2]);
          infos.push_back({info_codes[i], std::move(version_string)});

        } else {
          // Gives a version in the form 140000 instead of 14.0.0
          const char* stmt = "SHOW server_version_num";
          auto result_helper = PqResultHelper{conn_, std::string(stmt)};
          RAISE_STATUS(error, result_helper.Execute());
          auto it = result_helper.begin();
          if (it == result_helper.end()) {
            InternalAdbcSetError(error, "[libpq] PostgreSQL returned no rows for '%s'",
                                 stmt);
            return ADBC_STATUS_INTERNAL;
          }
          const char* server_version_num = (*it)[0].data;
          infos.push_back({info_codes[i], server_version_num});
        }

        break;
      }
      case ADBC_INFO_DRIVER_NAME:
        infos.push_back({info_codes[i], "ADBC PostgreSQL Driver"});
        break;
      case ADBC_INFO_DRIVER_VERSION:
        // TODO(lidavidm): fill in driver version
        infos.push_back({info_codes[i], "(unknown)"});
        break;
      case ADBC_INFO_DRIVER_ARROW_VERSION:
        infos.push_back({info_codes[i], NANOARROW_VERSION});
        break;
      case ADBC_INFO_DRIVER_ADBC_VERSION:
        infos.push_back({info_codes[i], ADBC_VERSION_1_1_0});
        break;
      default:
        // Ignore
        continue;
    }
  }

  RAISE_ADBC(adbc::driver::MakeGetInfoStream(infos, out).ToAdbc(error));
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::GetObjects(
    struct AdbcConnection* connection, int c_depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_type,
    const char* column_name, struct ArrowArrayStream* out, struct AdbcError* error) {
  PostgresGetObjectsHelper helper(conn_);
  helper.SetEnableConstraints(VendorName() != "Redshift");

  const auto catalog_filter =
      catalog ? std::make_optional(std::string_view(catalog)) : std::nullopt;
  const auto schema_filter =
      db_schema ? std::make_optional(std::string_view(db_schema)) : std::nullopt;
  const auto table_filter =
      table_name ? std::make_optional(std::string_view(table_name)) : std::nullopt;
  const auto column_filter =
      column_name ? std::make_optional(std::string_view(column_name)) : std::nullopt;
  std::vector<std::string_view> table_type_filter;
  while (table_type && *table_type) {
    if (*table_type) {
      table_type_filter.push_back(std::string_view(*table_type));
    }
    table_type++;
  }

  using adbc::driver::GetObjectsDepth;

  GetObjectsDepth depth = GetObjectsDepth::kColumns;
  switch (c_depth) {
    case ADBC_OBJECT_DEPTH_CATALOGS:
      depth = GetObjectsDepth::kCatalogs;
      break;
    case ADBC_OBJECT_DEPTH_COLUMNS:
      depth = GetObjectsDepth::kColumns;
      break;
    case ADBC_OBJECT_DEPTH_DB_SCHEMAS:
      depth = GetObjectsDepth::kSchemas;
      break;
    case ADBC_OBJECT_DEPTH_TABLES:
      depth = GetObjectsDepth::kTables;
      break;
    default:
      return Status::InvalidArgument("[libpq] GetObjects: invalid depth ", c_depth)
          .ToAdbc(error);
  }

  auto status = BuildGetObjects(&helper, depth, catalog_filter, schema_filter,
                                table_filter, column_filter, table_type_filter, out);
  RAISE_STATUS(error, helper.Close());
  RAISE_STATUS(error, status);

  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::GetOption(const char* option, char* value,
                                             size_t* length, struct AdbcError* error) {
  std::string output;
  if (std::strcmp(option, ADBC_CONNECTION_OPTION_CURRENT_CATALOG) == 0) {
    output = PQdb(conn_);
  } else if (std::strcmp(option, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA) == 0) {
    PqResultHelper result_helper{conn_, "SELECT CURRENT_SCHEMA()"};
    RAISE_STATUS(error, result_helper.Execute());
    auto it = result_helper.begin();
    if (it == result_helper.end()) {
      InternalAdbcSetError(
          error, "[libpq] PostgreSQL returned no rows for 'SELECT CURRENT_SCHEMA()'");
      return ADBC_STATUS_INTERNAL;
    }
    output = (*it)[0].data;
  } else if (std::strcmp(option, ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0) {
    output = autocommit_ ? ADBC_OPTION_VALUE_ENABLED : ADBC_OPTION_VALUE_DISABLED;
  } else if (std::strcmp(option, kConnectionOptionTransactionStatus.data()) == 0) {
    switch (PQtransactionStatus(conn_)) {
      case PQTRANS_IDLE:
        output = "idle";
        break;
      case PQTRANS_ACTIVE:
        output = "active";
        break;
      case PQTRANS_INTRANS:
        output = "intrans";
        break;
      case PQTRANS_INERROR:
        output = "inerror";
        break;
      case PQTRANS_UNKNOWN:
      default:
        output = "unknown";
        break;
    }
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
    PqResultHelper result_helper{conn, query};
    RAISE_STATUS(error,
                 result_helper.Execute({db_schema, table_name ? table_name : "%"}));

    for (PqResultRow row : result_helper) {
      auto reltuples = row[5].ParseDouble();
      if (!reltuples) {
        InternalAdbcSetError(error, "[libpq] Invalid double value in reltuples: '%s'",
                             row[5].data);
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
        InternalAdbcSetError(error, "[libpq] Invalid double value in null_frac: '%s'",
                             row[2].data);
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
        InternalAdbcSetError(error, "[libpq] Invalid double value in avg_width: '%s'",
                             row[3].data);
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
        InternalAdbcSetError(error, "[libpq] Invalid double value in avg_width: '%s'",
                             row[4].data);
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
    InternalAdbcSetError(error, "[libpq] Exact statistics are not implemented");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  } else if (!db_schema) {
    InternalAdbcSetError(error, "[libpq] Must request statistics for a single schema");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  } else if (catalog && std::strcmp(catalog, PQdb(conn_)) != 0) {
    InternalAdbcSetError(error,
                         "[libpq] Can only request statistics for current catalog");
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

  adbc::driver::MakeArrayStream(&schema, &array, out);
  return ADBC_STATUS_OK;
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

  adbc::driver::MakeArrayStream(&schema, &array, out);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::GetTableSchema(const char* catalog,
                                                  const char* db_schema,
                                                  const char* table_name,
                                                  struct ArrowSchema* schema,
                                                  struct AdbcError* error) {
  AdbcStatusCode final_status = ADBC_STATUS_OK;

  char* quoted = PQescapeIdentifier(conn_, table_name, strlen(table_name));
  std::string table_name_str(quoted);
  PQfreemem(quoted);

  if (db_schema != nullptr) {
    quoted = PQescapeIdentifier(conn_, db_schema, strlen(db_schema));
    table_name_str = std::string(quoted) + "." + table_name_str;
    PQfreemem(quoted);
  }

  std::string query =
      "SELECT attname, atttypid "
      "FROM pg_catalog.pg_class AS cls "
      "INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid "
      "INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid "
      "WHERE attr.attnum >= 0 AND cls.oid = $1::regclass::oid "
      "ORDER BY attr.attnum";

  std::vector<std::string> params = {table_name_str};

  PqResultHelper result_helper = PqResultHelper{conn_, std::string(query.c_str())};

  RAISE_STATUS(error, result_helper.Execute(params));

  auto uschema = nanoarrow::UniqueSchema();
  ArrowSchemaInit(uschema.get());
  CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(uschema.get(), result_helper.NumRows()),
           error);

  int row_counter = 0;
  for (auto row : result_helper) {
    const char* colname = row[0].data;
    const Oid pg_oid =
        static_cast<uint32_t>(std::strtol(row[1].data, /*str_end=*/nullptr, /*base=*/10));

    PostgresType pg_type;
    if (type_resolver_->FindWithDefault(pg_oid, &pg_type) != NANOARROW_OK) {
      InternalAdbcSetError(error, "%s%d%s%s%s%" PRIu32,
                           "Error resolving type code for column #", row_counter + 1,
                           " (\"", colname, "\")  with oid ", pg_oid);
      final_status = ADBC_STATUS_NOT_IMPLEMENTED;
      break;
    }
    CHECK_NA(INTERNAL,
             pg_type.WithFieldName(colname).SetSchema(uschema->children[row_counter],
                                                      std::string(VendorName())),
             error);
    row_counter++;
  }
  uschema.move(schema);

  return final_status;
}

AdbcStatusCode PostgresConnection::GetTableTypes(struct AdbcConnection* connection,
                                                 struct ArrowArrayStream* out,
                                                 struct AdbcError* error) {
  std::vector<std::string> table_types;
  table_types.reserve(kPgTableTypes.size());
  for (auto const& table_type : kPgTableTypes) {
    table_types.push_back(table_type.first);
  }

  RAISE_STATUS(error, adbc::driver::MakeTableTypesStream(table_types, out));
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::Init(struct AdbcDatabase* database,
                                        struct AdbcError* error) {
  if (!database || !database->private_data) {
    InternalAdbcSetError(error, "[libpq] Must provide an initialized AdbcDatabase");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  database_ =
      *reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  type_resolver_ = database_->type_resolver();

  RAISE_ADBC(database_->Connect(&conn_, error));

  cancel_ = PQgetCancel(conn_);
  if (!cancel_) {
    InternalAdbcSetError(error, "[libpq] Could not initialize PGcancel");
    return ADBC_STATUS_UNKNOWN;
  }

  std::ignore = PQsetNoticeProcessor(conn_, SilentNoticeProcessor, nullptr);

  for (const auto& [key, value] : post_init_options_) {
    RAISE_ADBC(SetOption(key.data(), value.data(), error));
  }
  post_init_options_.clear();

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
    InternalAdbcSetError(error, "%s",
                         "[libpq] Cannot rollback when autocommit is enabled");
    return ADBC_STATUS_INVALID_STATE;
  }

  PGTransactionStatusType txn_status = PQtransactionStatus(conn_);
  if (txn_status == PQTRANS_IDLE) {
    // https://github.com/apache/arrow-adbc/issues/2673: don't rollback if the
    // transaction is idle, since it won't have any effect and PostgreSQL will
    // issue a warning on the server side
    return ADBC_STATUS_OK;
  }

  PGresult* result = PQexec(conn_, "ROLLBACK AND CHAIN");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    InternalAdbcSetError(error, "%s%s",
                         "[libpq] Failed to rollback: ", PQerrorMessage(conn_));
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
      InternalAdbcSetError(error, "%s%s%s%s", "[libpq] Invalid value for option ", key,
                           ": ", value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    if (!conn_) {
      post_init_options_.emplace_back(key, value);
      return ADBC_STATUS_OK;
    }

    if (autocommit != autocommit_) {
      const char* query = autocommit ? "COMMIT" : "BEGIN TRANSACTION";

      PGresult* result = PQexec(conn_, query);
      if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        InternalAdbcSetError(error, "%s%s", "[libpq] Failed to update autocommit: ",
                             PQerrorMessage(conn_));
        PQclear(result);
        return ADBC_STATUS_IO;
      }
      PQclear(result);
      autocommit_ = autocommit;
    }
    return ADBC_STATUS_OK;
  } else if (std::strcmp(key, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA) == 0) {
    if (!conn_) {
      post_init_options_.emplace_back(key, value);
      return ADBC_STATUS_OK;
    }

    // PostgreSQL doesn't accept a parameter here
    char* value_esc = PQescapeIdentifier(conn_, value, strlen(value));
    if (!value_esc) {
      InternalAdbcSetError(error, "[libpq] Could not escape identifier: %s",
                           PQerrorMessage(conn_));
      return ADBC_STATUS_INTERNAL;
    }
    std::string query = fmt::format("SET search_path TO {}", value_esc);
    PQfreemem(value_esc);

    PqResultHelper result_helper{conn_, query};
    RAISE_STATUS(error, result_helper.Execute());
    return ADBC_STATUS_OK;
  }
  InternalAdbcSetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresConnection::SetOptionBytes(const char* key, const uint8_t* value,
                                                  size_t length,
                                                  struct AdbcError* error) {
  InternalAdbcSetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresConnection::SetOptionDouble(const char* key, double value,
                                                   struct AdbcError* error) {
  InternalAdbcSetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresConnection::SetOptionInt(const char* key, int64_t value,
                                                struct AdbcError* error) {
  InternalAdbcSetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

std::string_view PostgresConnection::VendorName() { return database_->VendorName(); }

const std::array<int, 3>& PostgresConnection::VendorVersion() {
  return database_->VendorVersion();
}

}  // namespace adbcpq
