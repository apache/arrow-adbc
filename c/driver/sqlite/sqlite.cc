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

#include <cstdio>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <sqlite3.h>
#include <nanoarrow/nanoarrow.hpp>

#define ADBC_FRAMEWORK_USE_FMT
#include "driver/framework/base_driver.h"
#include "driver/framework/connection.h"
#include "driver/framework/database.h"
#include "driver/framework/statement.h"
#include "driver/framework/status.h"
#include "driver/sqlite/statement_reader.h"

namespace adbc::sqlite {

using driver::Result;
using driver::Status;
namespace status = adbc::driver::status;

namespace {
constexpr std::string_view kDefaultUri =
    "file:adbc_driver_sqlite?mode=memory&cache=shared";
constexpr std::string_view kConnectionOptionEnableLoadExtension =
    "adbc.sqlite.load_extension.enabled";
constexpr std::string_view kConnectionOptionLoadExtensionPath =
    "adbc.sqlite.load_extension.path";
constexpr std::string_view kConnectionOptionLoadExtensionEntrypoint =
    "adbc.sqlite.load_extension.entrypoint";
/// The batch size for query results (and for initial type inference)
constexpr std::string_view kStatementOptionBatchRows = "adbc.sqlite.query.batch_rows";

std::string_view GetColumnText(sqlite3_stmt* stmt, int index) {
  return {
      reinterpret_cast<const char*>(sqlite3_column_text(stmt, index)),
      static_cast<size_t>(sqlite3_column_bytes(stmt, index)),
  };
}

class SqliteMutexGuard {
 public:
  explicit SqliteMutexGuard(sqlite3* conn) : conn_(conn) {
    sqlite3_mutex_enter(sqlite3_db_mutex(conn_));
  }
  ~SqliteMutexGuard() {
    if (conn_) {
      sqlite3_mutex_leave(sqlite3_db_mutex(conn_));
    }
    conn_ = nullptr;
  }

 private:
  sqlite3* conn_;
};

class SqliteStringBuilder {
 public:
  SqliteStringBuilder() : str_(sqlite3_str_new(nullptr)) {}

  ~SqliteStringBuilder() {
    // sqlite3_free is no-op on nullptr
    sqlite3_free(result_);
    result_ = nullptr;
    if (str_) {
      sqlite3_free(sqlite3_str_finish(str_));
      str_ = nullptr;
    }
  }

  void Reset() {
    std::ignore = GetString();
    sqlite3_free(result_);
    result_ = nullptr;
    str_ = sqlite3_str_new(nullptr);
  }

  void Append(std::string_view fmt, ...) {
    if (str_) {
      va_list args;
      va_start(args, fmt);
      sqlite3_str_vappendf(str_, fmt.data(), args);
      va_end(args);
    }
  }

  Result<std::string_view> GetString() {
    int len = 0;
    if (!result_) {
      if (int rc = sqlite3_str_errcode(str_); rc == SQLITE_NOMEM) {
        return status::Internal("out of memory building query");
      } else if (rc == SQLITE_TOOBIG) {
        return status::Internal("query too long");
      } else if (rc != SQLITE_OK) {
        return status::fmt::Internal("unknown SQLite error ({})", rc);
      }
      len = sqlite3_str_length(str_);
      result_ = sqlite3_str_finish(str_);
      str_ = nullptr;
    }
    return std::string_view(result_, len);
  }

 private:
  sqlite3_str* str_ = nullptr;
  char* result_ = nullptr;
};

class SqliteQuery {
 public:
  explicit SqliteQuery(sqlite3* conn, std::string_view query)
      : conn_(conn), query_(query) {}

  Status Init() {
    int rc = sqlite3_prepare_v2(conn_, query_.data(), static_cast<int>(query_.size()),
                                &stmt_, /*pzTail=*/nullptr);
    if (rc != SQLITE_OK) {
      return Close(rc);
    }
    return status::Ok();
  }

  Result<bool> Next() {
    if (!stmt_) {
      return status::fmt::Internal(
          "query already finished or never initialized\nquery was: {}", query_);
    }
    int rc = sqlite3_step(stmt_);
    if (rc == SQLITE_ROW) {
      return true;
    } else if (rc == SQLITE_DONE) {
      return false;
    }
    return Close(rc);
  }

  Status Close(int last_rc) {
    if (stmt_) {
      int rc = sqlite3_finalize(stmt_);
      stmt_ = nullptr;
      if (rc != SQLITE_OK && rc != SQLITE_DONE) {
        return status::fmt::Internal("failed to execute: {}\nquery was: {}",
                                     sqlite3_errmsg(conn_), query_);
      }
    } else if (last_rc != SQLITE_OK) {
      return status::fmt::Internal("failed to execute: {}\nquery was: {}",
                                   sqlite3_errmsg(conn_), query_);
    }
    return status::Ok();
  }

  Status Close() { return Close(SQLITE_OK); }

  sqlite3_stmt* stmt() const { return stmt_; }

  static Status Execute(sqlite3* conn, std::string_view query) {
    SqliteQuery q(conn, query);
    UNWRAP_STATUS(q.Init());
    while (true) {
      UNWRAP_RESULT(bool has_row, q.Next());
      if (!has_row) break;
    }
    return q.Close();
  }

  template <typename BindFunc, typename RowFunc>
  static Status Scan(sqlite3* conn, std::string_view query, BindFunc&& bind_func,
                     RowFunc&& row_func) {
    SqliteQuery q(conn, query);
    UNWRAP_STATUS(q.Init());

    int rc = std::forward<BindFunc>(bind_func)(q.stmt_);
    if (rc != SQLITE_OK) return q.Close();

    while (true) {
      UNWRAP_RESULT(bool has_row, q.Next());
      if (!has_row) break;

      rc = std::forward<RowFunc>(row_func)(q.stmt_);
      if (rc != SQLITE_OK) break;
    }
    return q.Close();
  }

 private:
  sqlite3* conn_ = nullptr;
  std::string_view query_;
  sqlite3_stmt* stmt_ = nullptr;
};

constexpr std::string_view kNoFilter = "%";

struct SqliteGetObjectsHelper : public driver::GetObjectsHelper {
  explicit SqliteGetObjectsHelper(sqlite3* conn) : conn(conn) {}

  Status Load(driver::GetObjectsDepth depth,
              std::optional<std::string_view> catalog_filter,
              std::optional<std::string_view> schema_filter,
              std::optional<std::string_view> table_filter,
              std::optional<std::string_view> column_filter,
              const std::vector<std::string_view>& table_types) override {
    std::string query =
        "SELECT DISTINCT name FROM pragma_database_list() WHERE name LIKE ?";

    UNWRAP_STATUS(SqliteQuery::Scan(
        conn, query,
        [&](sqlite3_stmt* stmt) {
          auto filter = catalog_filter.value_or(kNoFilter);
          return sqlite3_bind_text(stmt, 1, filter.data(),
                                   static_cast<int>(filter.size()), SQLITE_STATIC);
        },
        [&](sqlite3_stmt* stmt) {
          catalogs.emplace_back(GetColumnText(stmt, 0));
          return SQLITE_OK;
        }));

    // SQLite doesn't have schemas, so we assume each catalog has a single
    // unnamed schema.
    if (!schema_filter.has_value() || schema_filter->empty()) {
      schemas = {""};
    } else {
      schemas = {};
    }

    return status::Ok();
  }

  Status LoadCatalogs(std::optional<std::string_view> catalog_filter) override {
    return status::Ok();
  };

  Result<std::optional<std::string_view>> NextCatalog() override {
    if (next_catalog >= catalogs.size()) return std::nullopt;
    return catalogs[next_catalog++];
  }

  Status LoadSchemas(std::string_view catalog,
                     std::optional<std::string_view> schema_filter) override {
    next_schema = 0;
    return status::Ok();
  };

  Result<std::optional<std::string_view>> NextSchema() override {
    if (next_schema >= schemas.size()) return std::nullopt;
    return schemas[next_schema++];
  }

  Status LoadTables(std::string_view catalog, std::string_view schema,
                    std::optional<std::string_view> table_filter,
                    const std::vector<std::string_view>& table_types) override {
    next_table = 0;
    tables.clear();
    if (!schema.empty()) return status::Ok();

    SqliteStringBuilder builder;
    builder.Append(R"(SELECT name, type FROM "%w" . sqlite_master WHERE name LIKE ?)",
                   catalog.data());
    if (!table_types.empty()) {
      builder.Append(" AND (");
      bool first = true;
      for (const auto& table_type : table_types) {
        if (first) {
          builder.Append(" type = %Q", table_type.data());
          first = false;
        } else {
          builder.Append(" OR type = %Q", table_type.data());
        }
      }
      builder.Append(" )");
    }
    UNWRAP_RESULT(auto query, builder.GetString());

    return SqliteQuery::Scan(
        conn, query,
        [&](sqlite3_stmt* stmt) {
          auto filter = table_filter.value_or(kNoFilter);
          return sqlite3_bind_text(stmt, 1, filter.data(),
                                   static_cast<int>(filter.size()), SQLITE_STATIC);
        },
        [&](sqlite3_stmt* stmt) {
          tables.emplace_back(GetColumnText(stmt, 0), GetColumnText(stmt, 1));
          return SQLITE_OK;
        });
  };

  Result<std::optional<Table>> NextTable() override {
    if (next_table >= tables.size()) return std::nullopt;
    const auto& table = tables[next_table++];
    return Table{table.first, table.second};
  }

  Status LoadColumns(std::string_view catalog, std::string_view schema,
                     std::string_view table,
                     std::optional<std::string_view> column_filter) override {
    // XXX: pragma_table_info doesn't appear to work with bind parameters
    // XXX: because we're saving the SqliteQuery, we also need to save the string builder
    columns_query.Reset();
    columns_query.Append(
        R"(SELECT cid, name, type, 'notnull', dflt_value FROM pragma_table_info(%Q, %Q) WHERE NAME LIKE ?)",
        table.data(), catalog.data());
    UNWRAP_RESULT(auto query, columns_query.GetString());
    assert(!query.empty());

    columns.emplace(conn, query);
    UNWRAP_STATUS(columns->Init());

    auto filter = column_filter.value_or(kNoFilter);
    int rc = sqlite3_bind_text(columns->stmt(), 1, filter.data(),
                               static_cast<int>(filter.size()), SQLITE_STATIC);
    if (rc != SQLITE_OK) {
      return columns->Close(rc);
    }

    // As with columns, we could return constraints iteratively instead of
    // reading them all up front, but that complicates the state management

    // We can get primary keys and foreign keys, but not unique constraints
    // (unless we parse the SQL table definition)

    // XXX: n + 1 query pattern. You can join on a pragma so we could avoid
    // this in principle but it complicates the unpacking code here quite a
    // bit, so ignore for now. Also, we already have to issue a query per table.
    constraints.clear();
    next_constraint = 0;

    // Get the primary key
    {
      SqliteStringBuilder builder;
      builder.Append(
          R"(SELECT name FROM pragma_table_info(%Q, %Q) WHERE pk > 0 ORDER BY pk ASC)",
          table.data(), catalog.data());
      UNWRAP_RESULT(auto pk_query, builder.GetString());
      std::vector<std::string> pk;
      UNWRAP_STATUS(SqliteQuery::Scan(
          conn, pk_query, [](sqlite3_stmt*) { return SQLITE_OK; },
          [&](sqlite3_stmt* stmt) {
            pk.emplace_back(std::string(GetColumnText(stmt, 0)));
            return SQLITE_OK;
          }));
      if (!pk.empty()) {
        // it would be nice to have C++20 designated initializers...
        constraints.emplace_back(OwnedConstraint{
            std::nullopt,
            "PRIMARY KEY",
            std::move(pk),
            std::nullopt,
        });
      }
    }

    // Get any foreign keys
    if (catalog == "main") {
      // XXX: it appears experimentally that pragma_foreign_key_list won't let
      // you specify the database, making the result ambiguous. We'll only
      // query for the main catalog, but it appears if there's a table with
      // the same name in a different database, SQLite will still happily
      // return it here.
      constexpr std::string_view kForeignKeyQuery =
          R"(SELECT id, seq, "table", "from", "to"
             FROM pragma_foreign_key_list(?)
             ORDER BY id, seq ASC)";
      int prev_id = -1;
      UNWRAP_STATUS(SqliteQuery::Scan(
          conn, kForeignKeyQuery,
          [&](sqlite3_stmt* stmt) {
            return sqlite3_bind_text(stmt, 1, table.data(),
                                     static_cast<int>(table.size()), SQLITE_STATIC);
          },
          [&](sqlite3_stmt* stmt) {
            int fk_id = sqlite3_column_int(stmt, 0);
            auto to_table = GetColumnText(stmt, 2);
            auto from_col = GetColumnText(stmt, 3);
            auto to_col = GetColumnText(stmt, 4);

            if (fk_id != prev_id) {
              prev_id = fk_id;
              constraints.emplace_back(OwnedConstraint{
                  std::nullopt,
                  "FOREIGN KEY",
                  {},
                  std::make_optional<std::vector<OwnedConstraintUsage>>(),
              });
            }
            constraints.back().column_names.emplace_back(from_col);
            constraints.back().usage->emplace_back(OwnedConstraintUsage{
                "main",
                "",
                std::string(to_table),
                std::string(to_col),
            });

            return SQLITE_OK;
          }));
    }

    return status::Ok();
  };

  Result<std::optional<Column>> NextColumn() override {
    if (!columns) return std::nullopt;
    UNWRAP_RESULT(auto has_next, columns->Next());
    if (!has_next) {
      auto query = std::move(*columns);
      columns.reset();
      UNWRAP_STATUS(query.Close());
      return std::nullopt;
    }

    ColumnXdbc xdbc;
    bool notnull = sqlite3_column_int(columns->stmt(), 3) != 0;
    xdbc.xdbc_type_name = GetColumnText(columns->stmt(), 2);
    xdbc.xdbc_nullable =
        notnull ? std::make_optional<int16_t>(0) : std::make_optional<int16_t>(1);
    if (sqlite3_column_type(columns->stmt(), 4) != SQLITE_NULL) {
      xdbc.xdbc_column_def = GetColumnText(columns->stmt(), 4);
    }
    xdbc.xdbc_is_nullable = notnull ? "NO" : "YES";
    return Column{
        reinterpret_cast<const char*>(sqlite3_column_text(columns->stmt(), 1)),
        sqlite3_column_int(columns->stmt(), 0) + 1,
        std::nullopt,
        xdbc,
    };
  }

  Result<std::optional<Constraint>> NextConstraint() override {
    if (next_constraint >= constraints.size()) return std::nullopt;
    return constraints[next_constraint++].ToDriver();
  }

  struct OwnedConstraintUsage {
    std::optional<std::string> catalog;
    std::optional<std::string> schema;
    std::string table;
    std::string column;

    ConstraintUsage ToDriver() const {
      auto catalog = this->catalog ? std::make_optional(std::string_view(*this->catalog))
                                   : std::nullopt;
      auto schema = this->schema ? std::make_optional(std::string_view(*this->schema))
                                 : std::nullopt;
      return {catalog, schema, table, column};
    }
  };

  struct OwnedConstraint {
    std::optional<std::string> name;
    std::string type;
    std::vector<std::string> column_names;
    std::optional<std::vector<OwnedConstraintUsage>> usage;

    Constraint ToDriver() const {
      auto name =
          this->name ? std::make_optional(std::string_view(*this->name)) : std::nullopt;
      std::vector<std::string_view> column_names;
      std::vector<ConstraintUsage> usages;
      for (const auto& column_name : this->column_names) {
        column_names.emplace_back(column_name);
      }
      if (this->usage) {
        for (const auto& usage : *this->usage) {
          usages.emplace_back(usage.ToDriver());
        }
        return {name, type, std::move(column_names), std::move(usages)};
      }
      return {name, type, std::move(column_names), std::nullopt};
    }
  };

  sqlite3* conn = nullptr;
  std::vector<std::string> catalogs;
  std::vector<std::string> schemas;
  std::vector<std::pair<std::string, std::string>> tables;
  std::vector<OwnedConstraint> constraints;
  SqliteStringBuilder columns_query;
  std::optional<SqliteQuery> columns;
  size_t next_catalog = 0;
  size_t next_schema = 0;
  size_t next_table = 0;
  size_t next_constraint = 0;
};

class SqliteDatabase : public driver::Database<SqliteDatabase> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[SQLite]";

  Result<sqlite3*> OpenConnection() {
    sqlite3* conn;
    int rc = sqlite3_open_v2(uri_.c_str(), &conn,
                             SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                             /*zVfs=*/nullptr);
    if (rc != SQLITE_OK) {
      Status status;
      if (conn_) {
        status = status::fmt::IO("failed to open '{}': {}", uri_, sqlite3_errmsg(conn));
      } else {
        status = status::fmt::IO("failed to open '{}': failed to allocate memory", uri_);
      }
      (void)sqlite3_close(conn);
      return status;
    }
    return conn;
  }

  Status InitImpl() override {
    UNWRAP_RESULT(conn_, OpenConnection());
    return status::Ok();
  }

  Status ReleaseImpl() override {
    if (conn_) {
      int rc = sqlite3_close_v2(conn_);
      if (rc != SQLITE_OK) {
        return status::fmt::IO("failed to close connection: ({}) {}", rc,
                               sqlite3_errmsg(conn_));
      }
      conn_ = nullptr;
    }
    return Base::ReleaseImpl();
  }

  Status SetOptionImpl(std::string_view key, driver::Option value) override {
    if (key == "uri") {
      if (lifecycle_state_ != driver::LifecycleState::kUninitialized) {
        return status::InvalidState("cannot set uri after AdbcDatabaseInit");
      }
      UNWRAP_RESULT(auto uri, value.AsString());
      uri_ = std::move(uri);
      return status::Ok();
    }
    return Base::SetOptionImpl(key, value);
  }

 private:
  std::string uri_{kDefaultUri};
  sqlite3* conn_ = nullptr;
};

class SqliteConnection : public driver::Connection<SqliteConnection> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[SQLite]";

  sqlite3* conn() const { return conn_; }

  Status CommitImpl() {
    UNWRAP_STATUS(CheckOpen());
    UNWRAP_STATUS(SqliteQuery::Execute(conn_, "COMMIT"));
    // begin another transaction, since we're not in autocommit
    return SqliteQuery::Execute(conn_, "BEGIN");
  }

  Result<std::optional<std::string>> GetCurrentCatalogImpl() { return "main"; }

  Result<std::unique_ptr<driver::GetObjectsHelper>> GetObjectsImpl() {
    return std::make_unique<SqliteGetObjectsHelper>(conn_);
  }

  Status GetTableSchemaImpl(std::optional<std::string_view> catalog,
                            std::optional<std::string_view> db_schema,
                            std::string_view table_name, ArrowSchema* schema) {
    if (db_schema.has_value() && !db_schema->empty()) {
      return status::NotImplemented("SQLite does not support schemas");
    }

    SqliteStringBuilder builder;
    builder.Append(R"(SELECT * FROM "%w" . "%w")", catalog.value_or("main").data(),
                   table_name.data());
    UNWRAP_RESULT(std::string_view query, builder.GetString());

    sqlite3_stmt* stmt = nullptr;
    int rc =
        sqlite3_prepare_v2(conn_, query.data(), static_cast<int>(query.size()), &stmt,
                           /*pzTail=*/nullptr);
    if (rc != SQLITE_OK) {
      (void)sqlite3_finalize(stmt);
      return status::fmt::NotFound("GetTableSchema: {}", sqlite3_errmsg(conn_));
    }

    nanoarrow::UniqueArrayStream stream;
    struct AdbcError error = ADBC_ERROR_INIT;
    AdbcStatusCode status =
        InternalAdbcSqliteExportReader(conn_, stmt, /*binder=*/NULL,
                                       /*batch_size=*/64, stream.get(), &error);
    if (status == ADBC_STATUS_OK) {
      int code = stream->get_schema(stream.get(), schema);
      if (code != 0) {
        (void)sqlite3_finalize(stmt);
        return status::fmt::IO("failed to get schema: ({}) {}", code,
                               std::strerror(code));
      }
    }
    (void)sqlite3_finalize(stmt);
    return Status::FromAdbc(status, error);
  }

  Result<std::vector<std::string>> GetTableTypesImpl() {
    return std::vector<std::string>{"table", "view"};
  }

  Result<std::vector<driver::InfoValue>> InfoImpl(const std::vector<uint32_t>& codes) {
    static std::vector<uint32_t> kDefaultCodes{
        ADBC_INFO_VENDOR_NAME,    ADBC_INFO_VENDOR_VERSION,       ADBC_INFO_DRIVER_NAME,
        ADBC_INFO_DRIVER_VERSION, ADBC_INFO_DRIVER_ARROW_VERSION,
    };
    std::reference_wrapper<const std::vector<uint32_t>> codes_ref(codes);
    if (codes.empty()) {
      codes_ref = kDefaultCodes;
    }

    std::vector<driver::InfoValue> result;
    for (const auto code : codes_ref.get()) {
      switch (code) {
        case ADBC_INFO_VENDOR_NAME:
          result.emplace_back(code, "SQLite");
          break;
        case ADBC_INFO_VENDOR_VERSION:
          result.emplace_back(code, sqlite3_libversion());
          break;
        case ADBC_INFO_DRIVER_NAME:
          result.emplace_back(code, "ADBC SQLite Driver");
          break;
        case ADBC_INFO_DRIVER_VERSION:
          // TODO(lidavidm): fill in driver version
          result.emplace_back(code, "(unknown)");
          break;
        case ADBC_INFO_DRIVER_ARROW_VERSION:
          result.emplace_back(code, NANOARROW_VERSION);
          break;
        default:
          // Ignore
          continue;
      }
    }

    return result;
  }

  Status InitImpl(void* parent) {
    auto& db = *reinterpret_cast<SqliteDatabase*>(parent);
    UNWRAP_RESULT(conn_, db.OpenConnection());
    return status::Ok();
  }

  Status ReleaseImpl() {
    if (conn_) {
      int rc = sqlite3_close_v2(conn_);
      if (rc != SQLITE_OK) {
        return status::fmt::IO("failed to close connection: ({}) {}", rc,
                               sqlite3_errmsg(conn_));
      }
      conn_ = nullptr;
    }
    return Connection::ReleaseImpl();
  }

  Status RollbackImpl() {
    UNWRAP_STATUS(CheckOpen());
    UNWRAP_STATUS(SqliteQuery::Execute(conn_, "ROLLBACK"));
    return SqliteQuery::Execute(conn_, "BEGIN");
  }

  Status SetOptionImpl(std::string_view key, driver::Option value) {
    if (key == kConnectionOptionEnableLoadExtension) {
      if (!conn_ || lifecycle_state_ != driver::LifecycleState::kInitialized) {
        return status::InvalidState(
            "cannot enable extension loading before AdbcConnectionInit");
      }
      UNWRAP_RESULT(const bool enabled, value.AsBool());
      int rc = sqlite3_db_config(conn_, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION,
                                 enabled ? 1 : 0, nullptr);
      if (rc != SQLITE_OK) {
        return status::fmt::IO("cannot enable extension loading: {}",
                               sqlite3_errmsg(conn_));
      }
      return status::Ok();
    } else if (key == kConnectionOptionLoadExtensionPath) {
      if (!conn_ || lifecycle_state_ != driver::LifecycleState::kInitialized) {
        return status::InvalidState("cannot load extension before AdbcConnectionInit");
      }

      UNWRAP_RESULT(extension_path_, value.AsString());
      return status::Ok();
    } else if (key == kConnectionOptionLoadExtensionEntrypoint) {
#if !defined(ADBC_SQLITE_WITH_NO_LOAD_EXTENSION)
      if (extension_path_.empty()) {
        return status::fmt::InvalidState("{} can only be set after {}",
                                         kConnectionOptionLoadExtensionEntrypoint,
                                         kConnectionOptionLoadExtensionPath);
      }
      const char* extension_entrypoint = nullptr;
      if (value.has_value()) {
        UNWRAP_RESULT(auto entrypoint, value.AsString());
        extension_entrypoint = entrypoint.data();
      }

      char* message = NULL;
      int rc = sqlite3_load_extension(conn_, extension_path_.c_str(),
                                      extension_entrypoint, &message);
      if (rc != SQLITE_OK) {
        auto status = status::fmt::Unknown(
            "failed to load extension {} (entrypoint {}): {}", extension_path_,
            extension_entrypoint ? extension_entrypoint : "(NULL)",
            message ? message : "(unknown error)");
        if (message) sqlite3_free(message);
        return status;
      }
      extension_path_.clear();
      return status::Ok();
#else
      return status::NotImplemented(
          "this driver build does not support extension loading");
#endif
    }
    return Base::SetOptionImpl(key, value);
  }

  Status ToggleAutocommitImpl(bool enable_autocommit) {
    UNWRAP_STATUS(CheckOpen());
    if (enable_autocommit) {
      // that means we have an open transaction, just commit
      return SqliteQuery::Execute(conn_, "COMMIT");
    }
    // that means we have no open transaction, just begin
    return SqliteQuery::Execute(conn_, "BEGIN");
  }

 private:
  Status CheckOpen() const {
    if (!conn_) {
      return status::InvalidState("connection is not open");
    }
    return status::Ok();
  }

  sqlite3* conn_ = nullptr;
  // Temporarily hold the extension path (since the path and entrypoint need
  // to be set separately)
  std::string extension_path_;
};

class SqliteStatement : public driver::Statement<SqliteStatement> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[SQLite]";

  Status BindImpl() {
    if (bind_parameters_.release) {
      struct AdbcError error = ADBC_ERROR_INIT;
      if (AdbcStatusCode code =
              InternalAdbcSqliteBinderSetArrayStream(&binder_, &bind_parameters_, &error);
          code != ADBC_STATUS_OK) {
        return Status::FromAdbc(code, error);
      }
    }
    return status::Ok();
  }

  Result<int64_t> ExecuteIngestImpl(IngestState& state) {
    UNWRAP_STATUS(BindImpl());
    if (!binder_.schema.release) {
      return status::InvalidState("must Bind() before bulk ingestion");
    }

    // Parameter validation

    if (state.target_catalog && state.temporary) {
      return status::fmt::InvalidState("{} Cannot set both {} and {}", kErrorPrefix,
                                       ADBC_INGEST_OPTION_TARGET_CATALOG,
                                       ADBC_INGEST_OPTION_TEMPORARY);
    } else if (state.target_schema) {
      return status::fmt::NotImplemented("{} {} not supported", kErrorPrefix,
                                         ADBC_INGEST_OPTION_TARGET_DB_SCHEMA);
    } else if (!state.target_table) {
      return status::fmt::InvalidState("{} Must set {}", kErrorPrefix,
                                       ADBC_INGEST_OPTION_TARGET_TABLE);
    }

    // Create statements for creating the table, inserting a row, and the table name

    SqliteStringBuilder create_query, drop_query, insert_query, table_builder;
    if (state.target_catalog) {
      table_builder.Append(R"("%w" . "%w")", state.target_catalog->c_str(),
                           state.target_table->c_str());
    } else if (state.temporary) {
      // OK to be redundant (CREATE TEMP TABLE temp.foo)
      table_builder.Append(R"(temp . "%w")", state.target_table->c_str());
    } else {
      // If not temporary, explicitly target the main database
      table_builder.Append(R"(main . "%w")", state.target_table->c_str());
    }

    UNWRAP_RESULT(std::string_view table, table_builder.GetString());

    switch (state.table_exists_) {
      case Base::TableExists::kAppend:
        if (state.temporary) {
          create_query.Append("CREATE TEMPORARY TABLE IF NOT EXISTS %s (", table.data());
        } else {
          create_query.Append("CREATE TABLE IF NOT EXISTS %s (", table.data());
        }
        break;
      case Base::TableExists::kFail:
      case Base::TableExists::kReplace:
        if (state.temporary) {
          create_query.Append("CREATE TEMPORARY TABLE %s (", table.data());
        } else {
          create_query.Append("CREATE TABLE %s (", table.data());
        }
        drop_query.Append("DROP TABLE IF EXISTS %s", table.data());
        break;
    }

    insert_query.Append("INSERT INTO %s (", table.data());

    struct ArrowError arrow_error = {0};
    struct ArrowSchemaView view;
    std::memset(&view, 0, sizeof(view));
    for (int i = 0; i < binder_.schema.n_children; i++) {
      if (i > 0) {
        create_query.Append(", ");
        insert_query.Append(", ");
      }

      create_query.Append(R"("%w")", binder_.schema.children[i]->name);
      insert_query.Append(R"("%w")", binder_.schema.children[i]->name);

      int status = ArrowSchemaViewInit(&view, binder_.schema.children[i], &arrow_error);
      if (status != 0) {
        return status::fmt::Internal("failed to parse schema for column {}: {} ({}): {}",
                                     i, std::strerror(status), status,
                                     arrow_error.message);
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
          create_query.Append(" INTEGER");
          break;
        case NANOARROW_TYPE_FLOAT:
        case NANOARROW_TYPE_DOUBLE:
          create_query.Append(" REAL");
          break;
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING:
        case NANOARROW_TYPE_DATE32:
          create_query.Append(" TEXT");
          break;
        case NANOARROW_TYPE_BINARY:
          create_query.Append(" BLOB");
          break;
        default:
          break;
      }
    }

    create_query.Append(")");
    insert_query.Append(") VALUES (");
    for (int i = 0; i < binder_.schema.n_children; i++) {
      insert_query.Append("%s?", (i > 0 ? ", " : ""));
    }
    insert_query.Append(")");

    UNWRAP_RESULT(std::string_view create, create_query.GetString());
    UNWRAP_RESULT(std::string_view drop, drop_query.GetString());
    UNWRAP_RESULT(std::string_view insert, insert_query.GetString());

    // Drop/create tables as needed

    switch (state.table_exists_) {
      case Base::TableExists::kAppend:
      case Base::TableExists::kFail:
        // Do nothing
        break;
      case Base::TableExists::kReplace: {
        UNWRAP_STATUS(::adbc::sqlite::SqliteQuery::Execute(conn_, drop));
        break;
      }
    }
    switch (state.table_does_not_exist_) {
      case Base::TableDoesNotExist::kCreate: {
        UNWRAP_STATUS(::adbc::sqlite::SqliteQuery::Execute(conn_, create));
        break;
      }
      case Base::TableDoesNotExist::kFail:
        // Do nothing
        break;
    }

    // Insert
    int64_t row_count = 0;
    const int is_autocommit = sqlite3_get_autocommit(conn_);
    if (is_autocommit) {
      UNWRAP_STATUS(::adbc::sqlite::SqliteQuery::Execute(conn_, "BEGIN"));
    }

    assert(!insert.empty());
    sqlite3_stmt* stmt = nullptr;
    {
      int rc = sqlite3_prepare_v2(conn_, insert.data(), static_cast<int>(insert.size()),
                                  &stmt, /*pzTail=*/nullptr);
      if (rc != SQLITE_OK) {
        std::ignore = sqlite3_finalize(stmt);
        return status::fmt::Internal("failed to prepare: {}\nquery was: {}",
                                     sqlite3_errmsg(conn_), insert);
      }
    }
    assert(stmt != nullptr);

    AdbcStatusCode status_code = ADBC_STATUS_OK;
    Status status = status::Ok();
    struct AdbcError error = ADBC_ERROR_INIT;
    while (true) {
      char finished = 0;
      status_code =
          InternalAdbcSqliteBinderBindNext(&binder_, conn_, stmt, &finished, &error);
      if (status_code != ADBC_STATUS_OK || finished) {
        status = Status::FromAdbc(status_code, error);
        break;
      }

      int rc = 0;
      do {
        rc = sqlite3_step(stmt);
      } while (rc == SQLITE_ROW);
      if (rc != SQLITE_DONE) {
        status = status::fmt::Internal("failed to execute: {}\nquery was: {}",
                                       sqlite3_errmsg(conn_), insert.data());
        status_code = ADBC_STATUS_INTERNAL;
        break;
      }
      row_count++;
    }
    std::ignore = sqlite3_finalize(stmt);

    if (is_autocommit) {
      if (status_code == ADBC_STATUS_OK) {
        UNWRAP_STATUS(::adbc::sqlite::SqliteQuery::Execute(conn_, "COMMIT"));
      } else {
        UNWRAP_STATUS(::adbc::sqlite::SqliteQuery::Execute(conn_, "ROLLBACK"));
      }
    }

    if (status_code != ADBC_STATUS_OK) {
      return status;
    }
    return row_count;
  }

  Result<int64_t> ExecuteQueryImpl(ArrowArrayStream* stream) {
    struct AdbcError error = ADBC_ERROR_INIT;
    UNWRAP_STATUS(BindImpl());

    const int64_t expected = sqlite3_bind_parameter_count(stmt_);
    const int64_t actual = binder_.schema.n_children;
    if (actual != expected) {
      return status::fmt::InvalidState(
          "parameter count mismatch: expected {} but found {}", expected, actual);
    }

    auto status = InternalAdbcSqliteExportReader(
        conn_, stmt_, binder_.schema.release ? &binder_ : nullptr, batch_size_, stream,
        &error);
    if (status != ADBC_STATUS_OK) {
      return Status::FromAdbc(status, error);
    }
    return -1;
  }

  Result<int64_t> ExecuteQueryImpl(PreparedState& state, ArrowArrayStream* stream) {
    return ExecuteQueryImpl(stream);
  }

  Result<int64_t> ExecuteQueryImpl(QueryState& state, ArrowArrayStream* stream) {
    UNWRAP_STATUS(PrepareImpl(state));
    return ExecuteQueryImpl(stream);
  }

  Result<int64_t> ExecuteUpdateImpl() {
    UNWRAP_STATUS(BindImpl());

    const int64_t expected = sqlite3_bind_parameter_count(stmt_);
    const int64_t actual = binder_.schema.n_children;
    if (actual != expected) {
      return status::fmt::InvalidState(
          "parameter count mismatch: expected {} but found {}", expected, actual);
    }

    int64_t output_rows = 0;
    int64_t changed_rows = 0;

    SqliteMutexGuard guard(conn_);

    while (true) {
      if (binder_.schema.release) {
        char finished = 0;
        struct AdbcError error = ADBC_ERROR_INIT;
        if (AdbcStatusCode code = InternalAdbcSqliteBinderBindNext(&binder_, conn_, stmt_,
                                                                   &finished, &error);
            code != ADBC_STATUS_OK) {
          InternalAdbcSqliteBinderRelease(&binder_);
          return Status::FromAdbc(code, error);
        } else if (finished != 0) {
          break;
        }
      }

      while (sqlite3_step(stmt_) == SQLITE_ROW) {
        output_rows++;
      }

      if (sqlite3_column_count(stmt_) == 0) {
        changed_rows += sqlite3_changes(conn_);
      }

      if (!binder_.schema.release) break;
    }
    InternalAdbcSqliteBinderRelease(&binder_);

    if (sqlite3_reset(stmt_) != SQLITE_OK) {
      const char* msg = sqlite3_errmsg(conn_);
      return status::fmt::IO("failed to execute query: {}",
                             msg ? msg : "(unknown error)");
    }

    if (sqlite3_column_count(stmt_) == 0) {
      return changed_rows;
    } else {
      return output_rows;
    }
  }

  Result<int64_t> ExecuteUpdateImpl(PreparedState& state) { return ExecuteUpdateImpl(); }

  Result<int64_t> ExecuteUpdateImpl(QueryState& state) {
    UNWRAP_STATUS(PrepareImpl(state));
    return ExecuteUpdateImpl();
  }

  Status GetParameterSchemaImpl(PreparedState& state, ArrowSchema* schema) {
    int num_params = sqlite3_bind_parameter_count(stmt_);
    if (num_params < 0) {
      // Should not happen
      return status::fmt::Internal("{} SQLite returned negative parameter count",
                                   kErrorPrefix);
    }

    nanoarrow::UniqueSchema uschema;
    ArrowSchemaInit(uschema.get());
    UNWRAP_ERRNO(Internal, ArrowSchemaSetType(uschema.get(), NANOARROW_TYPE_STRUCT));
    UNWRAP_ERRNO(Internal, ArrowSchemaAllocateChildren(uschema.get(), num_params));
    char buffer[12];
    for (int i = 0; i < num_params; i++) {
      const char* name = sqlite3_bind_parameter_name(stmt_, i + 1);
      if (name == NULL) {
        snprintf(buffer, sizeof(buffer), "%d", i);
        name = buffer;
      }
      ArrowSchemaInit(uschema->children[i]);
      UNWRAP_ERRNO(Internal, ArrowSchemaSetType(uschema->children[i], NANOARROW_TYPE_NA));
      UNWRAP_ERRNO(Internal, ArrowSchemaSetName(uschema->children[i], name));
    }

    uschema.move(schema);
    return status::Ok();
  }

  Status InitImpl(void* parent) {
    conn_ = reinterpret_cast<SqliteConnection*>(parent)->conn();
    return Statement::InitImpl(parent);
  }

  Status PrepareImpl(QueryState& state) {
    if (stmt_) {
      int rc = sqlite3_finalize(stmt_);
      stmt_ = nullptr;
      if (rc != SQLITE_OK) {
        return status::fmt::IO("{} Failed to finalize previous statement: ({}) {}",
                               kErrorPrefix, rc, sqlite3_errmsg(conn_));
      }
    }

    int rc = sqlite3_prepare_v2(conn_, state.query.c_str(),
                                static_cast<int>(state.query.size()), &stmt_,
                                /*pzTail=*/nullptr);
    if (rc != SQLITE_OK) {
      std::string msg = sqlite3_errmsg(conn_);
      std::ignore = sqlite3_finalize(stmt_);
      stmt_ = NULL;
      return status::fmt::InvalidArgument("{} Failed to prepare query: {}\nquery: {}",
                                          kErrorPrefix, msg, state.query);
    }
    return status::Ok();
  }

  Status ReleaseImpl() {
    if (stmt_) {
      int rc = sqlite3_finalize(stmt_);
      stmt_ = nullptr;
      if (rc != SQLITE_OK) {
        return status::fmt::IO("{} Failed to finalize statement: ({}) {}", kErrorPrefix,
                               rc, sqlite3_errmsg(conn_));
      }
    }
    InternalAdbcSqliteBinderRelease(&binder_);
    return Statement::ReleaseImpl();
  }

  Status SetOptionImpl(std::string_view key, driver::Option value) {
    if (key == kStatementOptionBatchRows) {
      UNWRAP_RESULT(int64_t batch_size, value.AsInt());
      if (batch_size >= std::numeric_limits<int>::max() || batch_size <= 0) {
        return status::fmt::InvalidArgument(
            "{} Invalid statement option value {}={} (value is non-positive or out of "
            "range of int)",
            kErrorPrefix, key, value.Format());
      }
      batch_size_ = static_cast<int>(batch_size);
      return status::Ok();
    }
    return Base::SetOptionImpl(key, std::move(value));
  }

  int batch_size_ = 1024;
  AdbcSqliteBinder binder_;
  sqlite3* conn_ = nullptr;
  sqlite3_stmt* stmt_ = nullptr;
};

using SqliteDriver =
    adbc::driver::Driver<SqliteDatabase, SqliteConnection, SqliteStatement>;
}  // namespace
}  // namespace adbc::sqlite

// Public names

extern "C" {
#if !defined(ADBC_NO_COMMON_ENTRYPOINTS)
AdbcStatusCode AdbcDatabaseGetOption(struct AdbcDatabase* database, const char* key,
                                     char* value, size_t* length,
                                     struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOption<>(database, key, value, length, error);
}

AdbcStatusCode AdbcDatabaseGetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          uint8_t* value, size_t* length,
                                          struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOptionBytes<>(database, key, value, length,
                                                       error);
}

AdbcStatusCode AdbcDatabaseGetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t* value, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOptionInt<>(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseGetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double* value, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOptionDouble<>(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CDatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CNew<>(database, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CRelease<>(database, error);
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOption<>(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionBytes(struct AdbcDatabase* database, const char* key,
                                          const uint8_t* value, size_t length,
                                          struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOptionBytes<>(database, key, value, length,
                                                       error);
}

AdbcStatusCode AdbcDatabaseSetOptionInt(struct AdbcDatabase* database, const char* key,
                                        int64_t value, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOptionInt<>(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseSetOptionDouble(struct AdbcDatabase* database, const char* key,
                                           double value, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOptionDouble<>(database, key, value, error);
}

AdbcStatusCode AdbcConnectionCancel(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CConnectionCancel(connection, error);
}

AdbcStatusCode AdbcConnectionGetOption(struct AdbcConnection* connection, const char* key,
                                       char* value, size_t* length,
                                       struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOption<>(connection, key, value, length, error);
}

AdbcStatusCode AdbcConnectionGetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, uint8_t* value,
                                            size_t* length, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOptionBytes<>(connection, key, value, length,
                                                       error);
}

AdbcStatusCode AdbcConnectionGetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t* value,
                                          struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOptionInt<>(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionGetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double* value,
                                             struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOptionDouble<>(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CNew<>(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOption<>(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionBytes(struct AdbcConnection* connection,
                                            const char* key, const uint8_t* value,
                                            size_t length, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOptionBytes<>(connection, key, value, length,
                                                       error);
}

AdbcStatusCode AdbcConnectionSetOptionInt(struct AdbcConnection* connection,
                                          const char* key, int64_t value,
                                          struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOptionInt<>(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionSetOptionDouble(struct AdbcConnection* connection,
                                             const char* key, double value,
                                             struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOptionDouble<>(connection, key, value, error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CRelease<>(connection, error);
}

AdbcStatusCode AdbcConnectionGetInfo(struct AdbcConnection* connection,
                                     const uint32_t* info_codes, size_t info_codes_length,
                                     struct ArrowArrayStream* out,
                                     struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CConnectionGetInfo(connection, info_codes,
                                                        info_codes_length, out, error);
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_type,
                                        const char* column_name,
                                        struct ArrowArrayStream* out,
                                        struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CConnectionGetObjects(
      connection, depth, catalog, db_schema, table_name, table_type, column_name, out,
      error);
}

AdbcStatusCode AdbcConnectionGetStatistics(struct AdbcConnection* connection,
                                           const char* catalog, const char* db_schema,
                                           const char* table_name, char approximate,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CConnectionGetStatistics(
      connection, catalog, db_schema, table_name, approximate, out, error);
}

AdbcStatusCode AdbcConnectionGetStatisticNames(struct AdbcConnection* connection,
                                               struct ArrowArrayStream* out,
                                               struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CConnectionGetStatisticNames(connection, out, error);
}

AdbcStatusCode AdbcConnectionGetTableSchema(struct AdbcConnection* connection,
                                            const char* catalog, const char* db_schema,
                                            const char* table_name,
                                            struct ArrowSchema* schema,
                                            struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CConnectionGetTableSchema(
      connection, catalog, db_schema, table_name, schema, error);
}

AdbcStatusCode AdbcConnectionGetTableTypes(struct AdbcConnection* connection,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CConnectionGetTableTypes(connection, out, error);
}

AdbcStatusCode AdbcConnectionReadPartition(struct AdbcConnection* connection,
                                           const uint8_t* serialized_partition,
                                           size_t serialized_length,
                                           struct ArrowArrayStream* out,
                                           struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CConnectionReadPartition(
      connection, serialized_partition, serialized_length, out, error);
}

AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CConnectionRollback(connection, error);
}

AdbcStatusCode AdbcStatementCancel(struct AdbcStatement* statement,
                                   struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CStatementCancel(statement, error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CStatementNew(connection, statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CRelease<>(statement, error);
}

AdbcStatusCode AdbcStatementExecuteQuery(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* out,
                                         int64_t* rows_affected,
                                         struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CStatementExecuteQuery(statement, out, rows_affected,
                                                            error);
}

AdbcStatusCode AdbcStatementExecuteSchema(struct AdbcStatement* statement,
                                          struct ArrowSchema* schema,
                                          struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CStatementExecuteSchema(statement, schema, error);
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CStatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CStatementSetSqlQuery(statement, query, error);
}

AdbcStatusCode AdbcStatementSetSubstraitPlan(struct AdbcStatement* statement,
                                             const uint8_t* plan, size_t length,
                                             struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CStatementSetSubstraitPlan(statement, plan, length,
                                                                error);
}

AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CStatementBind(statement, values, schema, error);
}

AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement* statement,
                                       struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CStatementBindStream(statement, stream, error);
}

AdbcStatusCode AdbcStatementGetOption(struct AdbcStatement* statement, const char* key,
                                      char* value, size_t* length,
                                      struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOption<>(statement, key, value, length, error);
}

AdbcStatusCode AdbcStatementGetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, uint8_t* value,
                                           size_t* length, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOptionBytes<>(statement, key, value, length,
                                                       error);
}

AdbcStatusCode AdbcStatementGetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t* value, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOptionInt<>(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double* value,
                                            struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CGetOptionDouble<>(statement, key, value, error);
}

AdbcStatusCode AdbcStatementGetParameterSchema(struct AdbcStatement* statement,
                                               struct ArrowSchema* schema,
                                               struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CStatementGetParameterSchema(statement, schema,
                                                                  error);
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOption<>(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionBytes(struct AdbcStatement* statement,
                                           const char* key, const uint8_t* value,
                                           size_t length, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOptionBytes<>(statement, key, value, length,
                                                       error);
}

AdbcStatusCode AdbcStatementSetOptionInt(struct AdbcStatement* statement, const char* key,
                                         int64_t value, struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOptionInt<>(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetOptionDouble(struct AdbcStatement* statement,
                                            const char* key, double value,
                                            struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CSetOptionDouble<>(statement, key, value, error);
}

AdbcStatusCode AdbcStatementExecutePartitions(struct AdbcStatement* statement,
                                              struct ArrowSchema* schema,
                                              struct AdbcPartitions* partitions,
                                              int64_t* rows_affected,
                                              struct AdbcError* error) {
  return adbc::sqlite::SqliteDriver::CStatementExecutePartitions(
      statement, schema, partitions, rows_affected, error);
}

[[maybe_unused]] ADBC_EXPORT AdbcStatusCode AdbcDriverInit(int version, void* raw_driver,
                                                           AdbcError* error) {
  return adbc::sqlite::SqliteDriver::Init(version, raw_driver, error);
}
#endif  // ADBC_NO_COMMON_ENTRYPOINTS

[[maybe_unused]] ADBC_EXPORT AdbcStatusCode AdbcDriverSqliteInit(int version,
                                                                 void* raw_driver,
                                                                 AdbcError* error) {
  return adbc::sqlite::SqliteDriver::Init(version, raw_driver, error);
}

[[maybe_unused]] ADBC_EXPORT AdbcStatusCode SqliteDriverInit(int version,
                                                             void* raw_driver,
                                                             AdbcError* error) {
  return adbc::sqlite::SqliteDriver::Init(version, raw_driver, error);
}
}
