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

#include <sqlite3.h>

#include <cstring>
#include <memory>
#include <mutex>
#include <string>
#include <unordered_map>
#include <unordered_set>

#include <arrow/builder.h>
#include <arrow/c/bridge.h>
#include <arrow/record_batch.h>
#include <arrow/status.h>
#include <arrow/table.h>
#include <arrow/util/logging.h>
#include <arrow/util/string_builder.h>

#include "adbc.h"
#include "drivers/util.h"

namespace {

using arrow::Status;

void ReleaseError(struct AdbcError* error) {
  delete[] error->message;
  error->message = nullptr;
}

template <typename... Args>
void SetError(struct AdbcError* error, Args&&... args) {
  if (!error) return;
  std::string message =
      arrow::util::StringBuilder("[SQLite3] ", std::forward<Args>(args)...);
  if (error->message) {
    message.reserve(message.size() + 1 + std::strlen(error->message));
    message.append(1, '\n');
    message.append(error->message);
    delete[] error->message;
  }
  error->message = new char[message.size() + 1];
  message.copy(error->message, message.size());
  error->message[message.size()] = '\0';
  error->release = ReleaseError;
}

void SetError(sqlite3* db, const std::string& source, struct AdbcError* error) {
  return SetError(error, source, ": ", sqlite3_errmsg(db));
}

AdbcStatusCode CheckRc(sqlite3* db, int rc, const char* context,
                       struct AdbcError* error) {
  if (rc != SQLITE_OK) {
    SetError(db, context, error);
    return ADBC_STATUS_IO;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode CheckRc(sqlite3* db, sqlite3_stmt* stmt, int rc, const char* context,
                       struct AdbcError* error) {
  if (rc != SQLITE_OK) {
    SetError(db, context, error);
    rc = sqlite3_finalize(stmt);
    if (rc != SQLITE_OK) {
      SetError(db, "sqlite3_finalize", error);
    }
    return ADBC_STATUS_IO;
  }
  return ADBC_STATUS_OK;
}

template <typename CallbackFn>
AdbcStatusCode DoQuery(sqlite3* db, sqlite3_stmt* stmt, struct AdbcError* error,
                       CallbackFn&& callback) {
  auto status = std::move(callback)();
  std::ignore = CheckRc(db, stmt, sqlite3_finalize(stmt), "sqlite3_finalize", error);
  return status;
}

template <typename CallbackFn>
AdbcStatusCode DoQuery(sqlite3* db, const char* query, struct AdbcError* error,
                       CallbackFn&& callback) {
  sqlite3_stmt* stmt;
  int rc = sqlite3_prepare_v2(db, query, std::strlen(query), &stmt, /*pzTail=*/nullptr);
  if (rc != SQLITE_OK) return CheckRc(db, stmt, rc, "sqlite3_prepare_v2", error);
  auto status = std::move(callback)(stmt);
  std::ignore = CheckRc(db, stmt, sqlite3_finalize(stmt), "sqlite3_finalize", error);
  return status;
}

arrow::Status ToArrowStatus(AdbcStatusCode code, struct AdbcError* error) {
  if (code == ADBC_STATUS_OK) return Status::OK();
  // TODO:
  return Status::UnknownError(code);
}

AdbcStatusCode FromArrowStatus(const Status& status, struct AdbcError* error) {
  if (status.ok()) return ADBC_STATUS_OK;
  SetError(error, status);
  // TODO: map Arrow codes to ADBC codes
  return ADBC_STATUS_INTERNAL;
}

std::shared_ptr<arrow::Schema> StatementToSchema(sqlite3_stmt* stmt) {
  // TODO: this is fundamentally the wrong way to go about
  // this. instead, we need to act like the CSV/JSON readers: sample
  // several rows and dynamically update the column type as we go.
  const int num_columns = sqlite3_column_count(stmt);
  arrow::FieldVector fields(num_columns);
  for (int i = 0; i < num_columns; i++) {
    const char* column_name = sqlite3_column_name(stmt, i);
    const int column_type = sqlite3_column_type(stmt, i);
    std::shared_ptr<arrow::DataType> arrow_type = nullptr;
    switch (column_type) {
      case SQLITE_INTEGER:
        arrow_type = arrow::int64();
        break;
      case SQLITE_FLOAT:
        arrow_type = arrow::float64();
        break;
      case SQLITE_BLOB:
        arrow_type = arrow::binary();
        break;
      case SQLITE_TEXT:
        arrow_type = arrow::utf8();
        break;
      case SQLITE_NULL:
      default:
        arrow_type = arrow::null();
        break;
    }
    fields[i] = arrow::field(column_name, std::move(arrow_type));
  }
  return arrow::schema(std::move(fields));
}

class SqliteDatabaseImpl {
 public:
  SqliteDatabaseImpl() : db_(nullptr), connection_count_(0) {}

  AdbcStatusCode Connect(sqlite3** db, struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (!db_) {
      SetError(error, "Database not yet initialized, call AdbcDatabaseInit");
      return ADBC_STATUS_INVALID_STATE;
    }
    // Create a new connection
    if (database_uri_ == ":memory:") {
      // unless the special ":memory:" filename is used
      *db = db_;
    } else {
      int rc =
          sqlite3_open_v2(database_uri_.c_str(), db,
                          SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                          /*zVfs=*/nullptr);
      ADBC_RETURN_NOT_OK(CheckRc(*db, nullptr, rc, "sqlite3_open_v2", error));
    }
    ++connection_count_;
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Init(struct AdbcError* error) {
    if (db_) {
      SetError(error, "Database already initialized");
      return ADBC_STATUS_INVALID_STATE;
    }
    database_uri_ = "file:adbc_sqlite_driver?mode=memory&cache=shared";
    auto it = options_.find("filename");
    if (it != options_.end()) {
      database_uri_ = it->second;
    }

    int rc = sqlite3_open_v2(database_uri_.c_str(), &db_,
                             SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                             /*zVfs=*/nullptr);
    ADBC_RETURN_NOT_OK(CheckRc(db_, nullptr, rc, "sqlite3_open_v2", error));
    options_.clear();
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error) {
    if (db_) {
      SetError(error, "Database already initialized");
      return ADBC_STATUS_INVALID_STATE;
    }
    options_[key] = value;
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Disconnect(sqlite3* db, struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (--connection_count_ < 0) {
      SetError(error, "Connection count underflow");
      return ADBC_STATUS_INVALID_STATE;
    }
    // Close the database unless :memory:
    if (database_uri_ != ":memory:") {
      if (sqlite3_close(db) != SQLITE_OK) {
        if (db) SetError(db, "sqlite3_close", error);
        return ADBC_STATUS_IO;
      }
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Release(struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (connection_count_ > 0) {
      SetError(error, "Cannot release database with ", connection_count_,
               " open connections");
      return ADBC_STATUS_INVALID_STATE;
    }

    auto status = sqlite3_close(db_);
    if (status != SQLITE_OK) {
      if (db_) SetError(db_, "sqlite3_close", error);
      return ADBC_STATUS_IO;
    }
    return ADBC_STATUS_OK;
  }

 private:
  sqlite3* db_;
  int connection_count_;
  std::string database_uri_;
  std::unordered_map<std::string, std::string> options_;
  std::mutex mutex_;
};

class SqliteConnectionImpl {
 public:
  SqliteConnectionImpl() : database_(nullptr), db_(nullptr), autocommit_(true) {}

  sqlite3* db() const { return db_; }

  AdbcStatusCode GetTableSchema(const char* catalog, const char* db_schema,
                                const char* table_name, struct ArrowSchema* schema,
                                struct AdbcError* error) {
    if ((catalog && std::strlen(catalog) > 0) ||
        (db_schema && std::strlen(db_schema) > 0)) {
      std::memset(schema, 0, sizeof(*schema));
      SetError(error, "Catalog/schema are not supported");
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    std::string query = "SELECT * FROM ";
    char* escaped = sqlite3_mprintf("%w", table_name);
    if (!escaped) {
      // Failed to allocate
      SetError(error, "Could not escape table name (failed to allocate memory)");
      return ADBC_STATUS_INTERNAL;
    }
    query += escaped;
    sqlite3_free(escaped);

    std::shared_ptr<arrow::Schema> arrow_schema;
    ADBC_RETURN_NOT_OK(
        DoQuery(db_, query.c_str(), error, [&](sqlite3_stmt* stmt) -> AdbcStatusCode {
          if (sqlite3_step(stmt) == SQLITE_ERROR) {
            SetError(db_, "sqlite3_step", error);
            return ADBC_STATUS_IO;
          }
          arrow_schema = StatementToSchema(stmt);
          return ADBC_STATUS_OK;
        }));
    return FromArrowStatus(arrow::ExportSchema(*arrow_schema, schema), error);
  }

  AdbcStatusCode Init(struct AdbcDatabase* database, struct AdbcError* error) {
    if (!database->private_data) {
      SetError(error, "database is not initialized");
      return ADBC_STATUS_INVALID_STATE;
    }
    database_ =
        *reinterpret_cast<std::shared_ptr<SqliteDatabaseImpl>*>(database->private_data);
    return database_->Connect(&db_, error);
  }

  AdbcStatusCode Release(struct AdbcError* error) {
    if (!database_) return ADBC_STATUS_OK;
    return database_->Disconnect(db_, error);
  }

  AdbcStatusCode SetAutocommit(bool autocommit, struct AdbcError* error) {
    if (autocommit == autocommit_) return ADBC_STATUS_OK;
    autocommit_ = autocommit;

    const char* query = autocommit_ ? "COMMIT" : "BEGIN TRANSACTION";
    return DoQuery(db_, query, error, [&](sqlite3_stmt* stmt) -> AdbcStatusCode {
      return StepStatement(stmt, error);
    });
  }

  AdbcStatusCode Commit(struct AdbcError* error) {
    if (autocommit_) {
      SetError(error, "Cannot commit when in autocommit mode");
      return ADBC_STATUS_INVALID_STATE;
    }
    ADBC_RETURN_NOT_OK(
        DoQuery(db_, "COMMIT", error, [&](sqlite3_stmt* stmt) -> AdbcStatusCode {
          return StepStatement(stmt, error);
        }));
    return DoQuery(db_, "BEGIN TRANSACTION", error,
                   [&](sqlite3_stmt* stmt) { return StepStatement(stmt, error); });
  }

  AdbcStatusCode Rollback(struct AdbcError* error) {
    if (autocommit_) {
      SetError(error, "Cannot rollback when in autocommit mode");
      return ADBC_STATUS_INVALID_STATE;
    }
    ADBC_RETURN_NOT_OK(
        DoQuery(db_, "ROLLBACK", error, [&](sqlite3_stmt* stmt) -> AdbcStatusCode {
          return StepStatement(stmt, error);
        }));
    return DoQuery(db_, "BEGIN TRANSACTION", error,
                   [&](sqlite3_stmt* stmt) { return StepStatement(stmt, error); });
  }

 private:
  AdbcStatusCode StepStatement(sqlite3_stmt* stmt, struct AdbcError* error) {
    int rc = SQLITE_OK;
    while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
    }
    if (rc != SQLITE_DONE) {
      return CheckRc(db_, rc, "sqlite3_step", error);
    }
    return ADBC_STATUS_OK;
  }

  std::shared_ptr<SqliteDatabaseImpl> database_;
  sqlite3* db_;
  bool autocommit_;
};

AdbcStatusCode BindParameters(sqlite3_stmt* stmt, const arrow::RecordBatch& data,
                              int64_t row, int* rc, struct AdbcError* error) {
  int col_index = 1;
  for (const auto& column : data.columns()) {
    if (column->IsNull(row)) {
      *rc = sqlite3_bind_null(stmt, col_index);
    } else {
      switch (column->type()->id()) {
        case arrow::Type::INT64: {
          *rc = sqlite3_bind_int64(
              stmt, col_index, static_cast<const arrow::Int64Array&>(*column).Value(row));
          break;
        }
        case arrow::Type::STRING: {
          const auto& strings = static_cast<const arrow::StringArray&>(*column);
          *rc =
              sqlite3_bind_text64(stmt, col_index, strings.Value(row).data(),
                                  strings.value_length(row), SQLITE_STATIC, SQLITE_UTF8);
          break;
        }
        default:
          SetError(error, "Binding parameter of type ", *column->type());
          return ADBC_STATUS_NOT_IMPLEMENTED;
      }
    }
    if (*rc != SQLITE_OK) return ADBC_STATUS_IO;
    col_index++;
  }
  return ADBC_STATUS_OK;
}

class SqliteStatementReader : public arrow::RecordBatchReader {
 public:
  explicit SqliteStatementReader(
      std::shared_ptr<SqliteConnectionImpl> connection, sqlite3_stmt* stmt,
      std::shared_ptr<arrow::RecordBatchReader> bind_parameters)
      : connection_(std::move(connection)),
        stmt_(stmt),
        bind_parameters_(std::move(bind_parameters)),
        schema_(nullptr),
        next_parameters_(nullptr),
        bind_index_(0),
        done_(false) {}

  AdbcStatusCode Init(struct AdbcError* error) {
    // TODO: this crashes if the statement is closed while the reader
    // is still open.
    // Step the statement and get the schema (SQLite doesn't
    // necessarily know the schema until it begins to execute it)

    sqlite3* db = connection_->db();
    Status status;
    int rc = SQLITE_OK;
    if (bind_parameters_) {
      status = bind_parameters_->ReadNext(&next_parameters_);
      ADBC_RETURN_NOT_OK(FromArrowStatus(status, error));
      ADBC_RETURN_NOT_OK(BindNext(&rc, error));
      ADBC_RETURN_NOT_OK(CheckRc(db, stmt_, rc, "sqlite3_bind", error));
    }
    // XXX: with parameters, inferring the schema from the first
    // argument is inaccurate (what if one is null?). Is there a way
    // to hint to SQLite the real type?

    rc = sqlite3_step(stmt_);
    if (rc == SQLITE_ERROR) {
      return CheckRc(db, stmt_, rc, "sqlite3_step", error);
    }
    schema_ = StatementToSchema(stmt_);
    done_ = rc != SQLITE_ROW;
    return ADBC_STATUS_OK;
  }

  void OverrideSchema(std::shared_ptr<arrow::Schema> schema) {
    // TODO(ARROW-14705): use UnifySchemas for some sanity checking
    schema_ = std::move(schema);
  }

  std::shared_ptr<arrow::Schema> schema() const override {
    DCHECK(schema_);
    return schema_;
  }

  Status ReadNext(std::shared_ptr<arrow::RecordBatch>* batch) override {
    constexpr int64_t kBatchSize = 1024;
    if (done_) {
      *batch = nullptr;
      return Status::OK();
    }

    std::vector<std::unique_ptr<arrow::ArrayBuilder>> builders(schema_->num_fields());
    for (int i = 0; static_cast<size_t>(i) < builders.size(); i++) {
      // TODO: allow overriding memory pool
      ARROW_RETURN_NOT_OK(arrow::MakeBuilder(arrow::default_memory_pool(),
                                             schema_->field(i)->type(), &builders[i]));
    }

    sqlite3* db = connection_->db();

    // The statement was stepped once at the start, so step at the end of the loop
    int64_t num_rows = 0;
    for (int64_t row = 0; row < kBatchSize; row++) {
      for (int col = 0; col < schema_->num_fields(); col++) {
        const auto& field = schema_->field(col);
        switch (field->type()->id()) {
          case arrow::Type::DOUBLE: {
            // TODO: handle null values
            const sqlite3_int64 value = sqlite3_column_double(stmt_, col);
            ARROW_RETURN_NOT_OK(
                dynamic_cast<arrow::DoubleBuilder*>(builders[col].get())->Append(value));
            break;
          }
          case arrow::Type::INT64: {
            // TODO: handle null values
            const sqlite3_int64 value = sqlite3_column_int64(stmt_, col);
            ARROW_RETURN_NOT_OK(
                dynamic_cast<arrow::Int64Builder*>(builders[col].get())->Append(value));
            break;
          }
          case arrow::Type::NA: {
            // TODO: handle null values
            ARROW_RETURN_NOT_OK(
                dynamic_cast<arrow::NullBuilder*>(builders[col].get())->AppendNull());
            break;
          }
          case arrow::Type::STRING: {
            const char* value =
                reinterpret_cast<const char*>(sqlite3_column_text(stmt_, col));
            if (!value) {
              // TODO: check field nullability
              ARROW_RETURN_NOT_OK(
                  dynamic_cast<arrow::StringBuilder*>(builders[col].get())->AppendNull());
            } else {
              const arrow::util::string_view view(value, std::strlen(value));
              ARROW_RETURN_NOT_OK(dynamic_cast<arrow::StringBuilder*>(builders[col].get())
                                      ->Append(value));
            }
            break;
          }
          default:
            return Status::NotImplemented("[SQLite3] Cannot read field '", field->name(),
                                          "' of type ", field->type()->ToString());
        }
      }
      num_rows++;

      int status = sqlite3_step(stmt_);
      if (status == SQLITE_ROW) {
        continue;
      } else if (status == SQLITE_DONE) {
        if (bind_parameters_ &&
            (!next_parameters_ || bind_index_ >= next_parameters_->num_rows())) {
          ARROW_RETURN_NOT_OK(bind_parameters_->ReadNext(&next_parameters_));
          bind_index_ = 0;
        }

        if (next_parameters_ && bind_index_ < next_parameters_->num_rows()) {
          status = sqlite3_reset(stmt_);
          if (status != SQLITE_OK) {
            return Status::IOError("[SQLite3] sqlite3_reset: ", sqlite3_errmsg(db));
          }
          struct AdbcError error;
          ARROW_RETURN_NOT_OK(ToArrowStatus(BindNext(&status, &error), &error));
          status = sqlite3_step(stmt_);
          if (status == SQLITE_ROW) continue;
        } else {
          done_ = true;
          next_parameters_.reset();
        }
        break;
      }
      return Status::IOError("[SQLite3] sqlite3_step: ", sqlite3_errmsg(db));
    }

    arrow::ArrayVector arrays(builders.size());
    for (size_t i = 0; i < builders.size(); i++) {
      ARROW_RETURN_NOT_OK(builders[i]->Finish(&arrays[i]));
    }
    *batch = arrow::RecordBatch::Make(schema_, num_rows, std::move(arrays));
    return Status::OK();
  }

 private:
  AdbcStatusCode BindNext(int* rc, struct AdbcError* error) {
    if (!next_parameters_ || bind_index_ >= next_parameters_->num_rows()) {
      return ADBC_STATUS_OK;
    }
    return BindParameters(stmt_, *next_parameters_, bind_index_++, rc, error);
  }

  std::shared_ptr<SqliteConnectionImpl> connection_;
  sqlite3_stmt* stmt_;
  std::shared_ptr<arrow::RecordBatchReader> bind_parameters_;

  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::RecordBatch> next_parameters_;
  int64_t bind_index_;
  bool done_;
};

class SqliteStatementImpl {
 public:
  explicit SqliteStatementImpl(std::shared_ptr<SqliteConnectionImpl> connection)
      : connection_(std::move(connection)), stmt_(nullptr) {}

  AdbcStatusCode Close(struct AdbcError* error) {
    if (stmt_) {
      const int rc = sqlite3_finalize(stmt_);
      stmt_ = nullptr;
      ADBC_RETURN_NOT_OK(
          CheckRc(connection_->db(), nullptr, rc, "sqlite3_finalize", error));
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Bind(const std::shared_ptr<SqliteStatementImpl>& self,
                      struct ArrowArray* values, struct ArrowSchema* schema,
                      struct AdbcError* error) {
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = arrow::ImportRecordBatch(values, schema).Value(&batch);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_INTERNAL;
    }

    std::shared_ptr<arrow::Table> table;
    status = arrow::Table::FromRecordBatches({std::move(batch)}).Value(&table);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_INTERNAL;
    }

    bind_parameters_.reset(new arrow::TableBatchReader(std::move(table)));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Bind(const std::shared_ptr<SqliteStatementImpl>& self,
                      struct ArrowArrayStream* stream, struct AdbcError* error) {
    auto status = arrow::ImportRecordBatchReader(stream).Value(&bind_parameters_);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_INTERNAL;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Execute(const std::shared_ptr<SqliteStatementImpl>& self,
                         struct AdbcError* error) {
    if (stmt_) {
      return ExecutePrepared(error);
    } else if (!bulk_table_.empty()) {
      return ExecuteBulk(error);
    }
    SetError(error, "Cannot execute a statement without a query");
    return ADBC_STATUS_INVALID_STATE;
  }

  AdbcStatusCode Prepare(const std::shared_ptr<SqliteStatementImpl>& self,
                         struct AdbcError* error) {
    if (stmt_) {
      // No-op
      return ADBC_STATUS_OK;
    } else if (!bulk_table_.empty()) {
      SetError(error, "Cannot prepare with bulk insert");
      return ADBC_STATUS_INVALID_STATE;
    }
    SetError(error, "Cannot prepare a statement without a query");
    return ADBC_STATUS_INVALID_STATE;
  }

  AdbcStatusCode GetObjects(const std::shared_ptr<SqliteStatementImpl>& self, int depth,
                            const char* catalog, const char* db_schema,
                            const char* table_name, const char** table_type,
                            const char* column_name, struct AdbcError* error) {
    static std::shared_ptr<arrow::DataType> kColumnSchema = arrow::struct_({
        arrow::field("column_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("ordinal_position", arrow::int32()),
        arrow::field("remarks", arrow::utf8()),
        arrow::field("xdbc_data_type", arrow::int16()),
        arrow::field("xdbc_type_name", arrow::utf8()),
        arrow::field("xdbc_column_size", arrow::int32()),
        arrow::field("xdbc_decimal_digits", arrow::int16()),
        arrow::field("xdbc_num_prec_radix", arrow::int16()),
        arrow::field("xdbc_nullable", arrow::int16()),
        arrow::field("xdbc_column_def", arrow::utf8()),
        arrow::field("xdbc_sql_data_type", arrow::int16()),
        arrow::field("xdbc_datetime_sub", arrow::int16()),
        arrow::field("xdbc_char_octet_length", arrow::int32()),
        arrow::field("xdbc_is_nullable", arrow::utf8()),
        arrow::field("xdbc_scope_catalog", arrow::utf8()),
        arrow::field("xdbc_scope_schema", arrow::utf8()),
        arrow::field("xdbc_scope_table", arrow::utf8()),
        arrow::field("xdbc_is_autoincrement", arrow::boolean()),
        arrow::field("xdbc_is_generatedcolumn", arrow::boolean()),
    });
    static std::shared_ptr<arrow::DataType> kUsageSchema = arrow::struct_({
        arrow::field("fk_catalog", arrow::utf8()),
        arrow::field("fk_db_schema", arrow::utf8()),
        arrow::field("fk_table", arrow::utf8()),
        arrow::field("fk_column_name", arrow::utf8()),
    });
    static std::shared_ptr<arrow::DataType> kConstraintSchema = arrow::struct_({
        arrow::field("constraint_name", arrow::utf8()),
        arrow::field("constraint_type", arrow::utf8(), /*nullable=*/false),
        arrow::field("column_names", arrow::list(arrow::utf8()), /*nullable=*/false),
        arrow::field("column_names", arrow::list(kUsageSchema)),
    });
    static std::shared_ptr<arrow::DataType> kTableSchema = arrow::struct_({
        arrow::field("table_name", arrow::utf8(), /*nullable=*/false),
        arrow::field("table_type", arrow::utf8(), /*nullable=*/false),
        arrow::field("table_columns", arrow::list(kColumnSchema)),
        arrow::field("table_constraints", arrow::list(kConstraintSchema)),
    });
    static std::shared_ptr<arrow::DataType> kDbSchemaSchema = arrow::struct_({
        arrow::field("db_schema_name", arrow::utf8()),
        arrow::field("db_schema_tables", arrow::list(kTableSchema)),
    });
    static std::shared_ptr<arrow::Schema> kCatalogSchema = arrow::schema({
        arrow::field("catalog_name", arrow::utf8()),
        arrow::field("catalog_db_schemas", arrow::list(kDbSchemaSchema)),
    });

    static const char kTableQuery[] =
        R"(SELECT name, type
           FROM sqlite_master
           WHERE name LIKE ? AND type <> "index"
           ORDER BY name ASC)";
    static const char kColumnQuery[] =
        R"(SELECT cid, name
           FROM pragma_table_info(?)
           WHERE name LIKE ?
           ORDER BY cid ASC)";
    static const char kPrimaryKeyQuery[] =
        R"(SELECT name
           FROM pragma_table_info(?)
           WHERE pk > 0
           ORDER BY pk ASC)";
    static const char kForeignKeyQuery[] =
        R"(SELECT id, seq, "table", "from", "to"
           FROM pragma_foreign_key_list(?)
           ORDER BY id, seq ASC)";

    arrow::StringBuilder catalog_name;
    std::unique_ptr<arrow::ArrayBuilder> catalog_schemas_builder;
    ADBC_RETURN_NOT_OK(FromArrowStatus(
        MakeBuilder(arrow::default_memory_pool(), kCatalogSchema->field(1)->type(),
                    &catalog_schemas_builder),
        error));
    auto* catalog_schemas =
        static_cast<arrow::ListBuilder*>(catalog_schemas_builder.get());
    auto* catalog_schemas_items =
        static_cast<arrow::StructBuilder*>(catalog_schemas->value_builder());
    auto* db_schema_name =
        static_cast<arrow::StringBuilder*>(catalog_schemas_items->child_builder(0).get());
    auto* db_schema_tables =
        static_cast<arrow::ListBuilder*>(catalog_schemas_items->child_builder(1).get());
    auto* db_schema_tables_items =
        static_cast<arrow::StructBuilder*>(db_schema_tables->value_builder());
    auto* table_names = static_cast<arrow::StringBuilder*>(
        db_schema_tables_items->child_builder(0).get());
    auto* table_types = static_cast<arrow::StringBuilder*>(
        db_schema_tables_items->child_builder(1).get());
    auto* table_columns =
        static_cast<arrow::ListBuilder*>(db_schema_tables_items->child_builder(2).get());
    auto* table_columns_items =
        static_cast<arrow::StructBuilder*>(table_columns->value_builder());
    auto* column_names =
        static_cast<arrow::StringBuilder*>(table_columns_items->child_builder(0).get());
    auto* ordinal_positions =
        static_cast<arrow::Int32Builder*>(table_columns_items->child_builder(1).get());
    auto* table_constraints =
        static_cast<arrow::ListBuilder*>(db_schema_tables_items->child_builder(3).get());
    auto* table_constraints_items =
        static_cast<arrow::StructBuilder*>(table_constraints->value_builder());
    auto* constraint_names = static_cast<arrow::StringBuilder*>(
        table_constraints_items->child_builder(0).get());
    auto* constraint_types = static_cast<arrow::StringBuilder*>(
        table_constraints_items->child_builder(1).get());
    auto* constraint_column_names =
        static_cast<arrow::ListBuilder*>(table_constraints_items->child_builder(2).get());
    auto* constraint_column_names_items =
        static_cast<arrow::StringBuilder*>(constraint_column_names->value_builder());
    auto* constraint_column_usage =
        static_cast<arrow::ListBuilder*>(table_constraints_items->child_builder(3).get());
    auto* constraint_column_usage_items =
        static_cast<arrow::StructBuilder*>(constraint_column_usage->value_builder());
    auto* constraint_column_usage_fk_catalog = static_cast<arrow::StringBuilder*>(
        constraint_column_usage_items->child_builder(0).get());
    auto* constraint_column_usage_fk_db_schema = static_cast<arrow::StringBuilder*>(
        constraint_column_usage_items->child_builder(1).get());
    auto* constraint_column_usage_fk_table = static_cast<arrow::StringBuilder*>(
        constraint_column_usage_items->child_builder(2).get());
    auto* constraint_column_usage_fk_column_name = static_cast<arrow::StringBuilder*>(
        constraint_column_usage_items->child_builder(3).get());

    // TODO: filter properly, also implement other attached databases
    if (!catalog || std::strlen(catalog) == 0) {
      // https://www.sqlite.org/cli.html
      // > The ".databases" command shows a list of all databases open
      // > in the current connection. There will always be at least
      // > 2. The first one is "main", the original database opened.
      ADBC_RETURN_NOT_OK(FromArrowStatus(catalog_name.Append("main"), error));

      if (depth == ADBC_OBJECT_DEPTH_CATALOGS) {
        ADBC_RETURN_NOT_OK(FromArrowStatus(catalog_schemas->AppendNull(), error));
      } else if (!db_schema || std::strlen(db_schema) == 0) {
        ADBC_RETURN_NOT_OK(FromArrowStatus(catalog_schemas->Append(), error));
        ADBC_RETURN_NOT_OK(FromArrowStatus(db_schema_name->AppendNull(), error));
        if (depth == ADBC_OBJECT_DEPTH_DB_SCHEMAS) {
          ADBC_RETURN_NOT_OK(FromArrowStatus(db_schema_tables->AppendNull(), error));
        } else {
          // Look up tables

          std::unordered_set<std::string> table_type_filter;
          if (table_type) {
            while (*table_type) {
              table_type_filter.insert(*table_type);
              table_type++;
            }
          }

          sqlite3* db = connection_->db();
          ADBC_RETURN_NOT_OK(
              DoQuery(db, kTableQuery, error, [&](sqlite3_stmt* stmt) -> AdbcStatusCode {
                if (table_name) {
                  ADBC_RETURN_NOT_OK(CheckRc(
                      db,
                      sqlite3_bind_text64(stmt, 1, table_name, std::strlen(table_name),
                                          SQLITE_STATIC, SQLITE_UTF8),
                      "sqlite3_bind_text64", error));
                } else {
                  ADBC_RETURN_NOT_OK(CheckRc(
                      db,
                      sqlite3_bind_text64(stmt, 1, "%", 1, SQLITE_STATIC, SQLITE_UTF8),
                      "sqlite3_bind_text64", error));
                }

                int rc = SQLITE_OK;
                ADBC_RETURN_NOT_OK(FromArrowStatus(db_schema_tables->Append(), error));
                while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
                  const char* cur_table =
                      reinterpret_cast<const char*>(sqlite3_column_text(stmt, 0));
                  const char* cur_table_type =
                      reinterpret_cast<const char*>(sqlite3_column_text(stmt, 1));

                  if (!table_type_filter.empty() &&
                      table_type_filter.find(cur_table_type) == table_type_filter.end()) {
                    continue;
                  }

                  ADBC_RETURN_NOT_OK(
                      FromArrowStatus(table_names->Append(cur_table), error));
                  ADBC_RETURN_NOT_OK(
                      FromArrowStatus(table_types->Append(cur_table_type), error));
                  if (depth == ADBC_OBJECT_DEPTH_TABLES) {
                    ADBC_RETURN_NOT_OK(
                        FromArrowStatus(table_columns->AppendNull(), error));
                    ADBC_RETURN_NOT_OK(
                        FromArrowStatus(table_constraints->AppendNull(), error));
                  } else {
                    ADBC_RETURN_NOT_OK(FromArrowStatus(table_columns->Append(), error));
                    ADBC_RETURN_NOT_OK(DoQuery(
                        db, kColumnQuery, error,
                        [&](sqlite3_stmt* stmt) -> AdbcStatusCode {
                          ADBC_RETURN_NOT_OK(
                              CheckRc(db,
                                      sqlite3_bind_text64(stmt, 1, cur_table,
                                                          std::strlen(cur_table),
                                                          SQLITE_STATIC, SQLITE_UTF8),
                                      "sqlite3_bind_text64", error));

                          if (column_name) {
                            ADBC_RETURN_NOT_OK(
                                CheckRc(db,
                                        sqlite3_bind_text64(stmt, 2, column_name,
                                                            std::strlen(column_name),
                                                            SQLITE_STATIC, SQLITE_UTF8),
                                        "sqlite3_bind_text64", error));
                          } else {
                            ADBC_RETURN_NOT_OK(
                                CheckRc(db,
                                        sqlite3_bind_text64(stmt, 2, "%", 1,
                                                            SQLITE_STATIC, SQLITE_UTF8),
                                        "sqlite3_bind_text64", error));
                          }

                          int rc = SQLITE_OK;
                          int64_t row_count = 0;
                          while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
                            row_count++;
                            const int32_t cur_ordinal_position =
                                1 + sqlite3_column_int(stmt, 0);
                            const char* cur_column_name = reinterpret_cast<const char*>(
                                sqlite3_column_text(stmt, 1));

                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                column_names->Append(cur_column_name), error));
                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                ordinal_positions->Append(cur_ordinal_position), error));

                            ADBC_RETURN_NOT_OK(
                                FromArrowStatus(table_columns_items->Append(), error));
                          }
                          for (int i = 2; i < table_columns_items->num_children(); i++) {
                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                table_columns_items->child_builder(i)->AppendNulls(
                                    row_count),
                                error));
                          }
                          if (rc != SQLITE_DONE) {
                            return CheckRc(db, rc, "sqlite3_step", error);
                          }
                          return ADBC_STATUS_OK;
                        }));

                    // We can get primary key and foreign keys, but not unique (without
                    // parsing the table definition, at least)
                    ADBC_RETURN_NOT_OK(
                        FromArrowStatus(table_constraints->Append(), error));
                    ADBC_RETURN_NOT_OK(DoQuery(
                        db, kPrimaryKeyQuery, error,
                        [&](sqlite3_stmt* stmt) -> AdbcStatusCode {
                          ADBC_RETURN_NOT_OK(
                              CheckRc(db,
                                      sqlite3_bind_text64(stmt, 1, cur_table,
                                                          std::strlen(cur_table),
                                                          SQLITE_STATIC, SQLITE_UTF8),
                                      "sqlite3_bind_text64", error));

                          int rc = SQLITE_OK;
                          bool has_primary_key = false;
                          while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
                            if (!has_primary_key) {
                              ADBC_RETURN_NOT_OK(
                                  FromArrowStatus(constraint_names->AppendNull(), error));
                              ADBC_RETURN_NOT_OK(FromArrowStatus(
                                  constraint_types->Append("PRIMARY KEY"), error));
                              ADBC_RETURN_NOT_OK(FromArrowStatus(
                                  constraint_column_names->Append(), error));
                              ADBC_RETURN_NOT_OK(FromArrowStatus(
                                  constraint_column_usage->Append(), error));
                            }
                            has_primary_key = true;
                            const char* cur_column_name = reinterpret_cast<const char*>(
                                sqlite3_column_text(stmt, 0));

                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                constraint_column_names_items->Append(cur_column_name),
                                error));
                          }
                          if (has_primary_key) {
                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                table_constraints_items->Append(), error));
                          }
                          if (rc != SQLITE_DONE) {
                            return CheckRc(db, rc, "sqlite3_step", error);
                          }
                          return ADBC_STATUS_OK;
                        }));
                    ADBC_RETURN_NOT_OK(DoQuery(
                        db, kForeignKeyQuery, error,
                        [&](sqlite3_stmt* stmt) -> AdbcStatusCode {
                          ADBC_RETURN_NOT_OK(
                              CheckRc(db,
                                      sqlite3_bind_text64(stmt, 1, cur_table,
                                                          std::strlen(cur_table),
                                                          SQLITE_STATIC, SQLITE_UTF8),
                                      "sqlite3_bind_text64", error));

                          int rc = SQLITE_OK;
                          int prev_key_id = -1;
                          while ((rc = sqlite3_step(stmt)) == SQLITE_ROW) {
                            const int key_id = sqlite3_column_int(stmt, 0);
                            const int key_seq = sqlite3_column_int(stmt, 1);
                            const char* to_table = reinterpret_cast<const char*>(
                                sqlite3_column_text(stmt, 2));
                            const char* from_col = reinterpret_cast<const char*>(
                                sqlite3_column_text(stmt, 3));
                            const char* to_col = reinterpret_cast<const char*>(
                                sqlite3_column_text(stmt, 4));
                            if (key_id != prev_key_id) {
                              ADBC_RETURN_NOT_OK(
                                  FromArrowStatus(constraint_names->AppendNull(), error));
                              ADBC_RETURN_NOT_OK(FromArrowStatus(
                                  constraint_types->Append("FOREIGN KEY"), error));
                              ADBC_RETURN_NOT_OK(FromArrowStatus(
                                  constraint_column_names->Append(), error));
                              ADBC_RETURN_NOT_OK(FromArrowStatus(
                                  constraint_column_usage->Append(), error));
                              if (prev_key_id != -1) {
                                ADBC_RETURN_NOT_OK(FromArrowStatus(
                                    table_constraints_items->Append(), error));
                              }
                            }
                            prev_key_id = key_id;

                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                constraint_column_names_items->Append(from_col), error));
                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                constraint_column_usage_fk_catalog->AppendNull(), error));
                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                constraint_column_usage_fk_db_schema->AppendNull(),
                                error));
                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                constraint_column_usage_fk_table->Append(to_table),
                                error));
                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                constraint_column_usage_fk_column_name->Append(to_col),
                                error));
                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                constraint_column_usage_items->Append(), error));
                          }
                          if (prev_key_id != -1) {
                            ADBC_RETURN_NOT_OK(FromArrowStatus(
                                table_constraints_items->Append(), error));
                          }
                          if (rc != SQLITE_DONE) {
                            return CheckRc(db, rc, "sqlite3_step", error);
                          }
                          return ADBC_STATUS_OK;
                        }));
                  }

                  ADBC_RETURN_NOT_OK(
                      FromArrowStatus(db_schema_tables_items->Append(), error));
                }
                if (rc != SQLITE_DONE) {
                  return CheckRc(db, rc, "sqlite3_step", error);
                }
                return ADBC_STATUS_OK;
              }));
        }
        ADBC_RETURN_NOT_OK(FromArrowStatus(catalog_schemas_items->Append(), error));
      } else {
        ADBC_RETURN_NOT_OK(FromArrowStatus(catalog_schemas->Append(), error));
      }
    }

    arrow::ArrayVector arrays(2);
    ADBC_RETURN_NOT_OK(FromArrowStatus(catalog_name.Finish(&arrays[0]), error));
    ADBC_RETURN_NOT_OK(FromArrowStatus(catalog_schemas->Finish(&arrays[1]), error));
    const int64_t rows = arrays[0]->length();
    auto status =
        arrow::RecordBatchReader::Make(
            {
                arrow::RecordBatch::Make(kCatalogSchema, rows, std::move(arrays)),
            },
            kCatalogSchema)
            .Value(&result_reader_);
    ADBC_RETURN_NOT_OK(FromArrowStatus(status, error));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode GetTableTypes(const std::shared_ptr<SqliteStatementImpl>& self,
                               struct AdbcError* error) {
    auto schema =
        arrow::schema({arrow::field("table_type", arrow::utf8(), /*nullable=*/false)});

    arrow::StringBuilder builder;
    std::shared_ptr<arrow::Array> array;
    ADBC_RETURN_NOT_OK(FromArrowStatus(builder.Append("table"), error));
    ADBC_RETURN_NOT_OK(FromArrowStatus(builder.Append("view"), error));
    ADBC_RETURN_NOT_OK(FromArrowStatus(builder.Finish(&array), error));

    auto status =
        arrow::RecordBatchReader::Make(
            {
                arrow::RecordBatch::Make(schema, /*num_rows=*/2, {std::move(array)}),
            },
            schema)
            .Value(&result_reader_);
    ADBC_RETURN_NOT_OK(FromArrowStatus(status, error));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode GetStream(const std::shared_ptr<SqliteStatementImpl>& self,
                           struct ArrowArrayStream* out, struct AdbcError* error) {
    if (!result_reader_) {
      SetError(error, "Statement has not yet been executed");
      return ADBC_STATUS_INVALID_STATE;
    }
    auto status = arrow::ExportRecordBatchReader(result_reader_, out);
    if (!status.ok()) {
      SetError(error, "Could not initialize result reader: ", status);
      return ADBC_STATUS_INTERNAL;
    }
    result_reader_.reset();
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetOption(const std::shared_ptr<SqliteStatementImpl>& self,
                           const char* key, const char* value, struct AdbcError* error) {
    if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0) {
      // Bulk ingest

      // Clear previous statement, if any
      ADBC_RETURN_NOT_OK(Close(error));

      if (std::strlen(value) == 0) return ADBC_STATUS_INVALID_ARGUMENT;
      bulk_table_ = value;
      return ADBC_STATUS_OK;
    }
    SetError(error, "Unknown option: ", key);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode SetSqlQuery(const std::shared_ptr<SqliteStatementImpl>& self,
                             const char* query, struct AdbcError* error) {
    bulk_table_.clear();
    // Clear previous statement, if any
    ADBC_RETURN_NOT_OK(Close(error));

    sqlite3* db = connection_->db();
    int rc = sqlite3_prepare_v2(db, query, static_cast<int>(std::strlen(query)), &stmt_,
                                /*pzTail=*/nullptr);
    return CheckRc(connection_->db(), stmt_, rc, "sqlite3_prepare_v2", error);
  }

 private:
  AdbcStatusCode ExecuteBulk(struct AdbcError* error) {
    if (!bind_parameters_) {
      SetError(error, "Must AdbcStatementBind for bulk insertion");
      return ADBC_STATUS_INVALID_STATE;
    }

    sqlite3* db = connection_->db();

    // Create the table
    {
      // XXX: not injection-safe
      std::string query = "CREATE TABLE IF NOT EXISTS ";
      query += bulk_table_;
      query += " (";
      const auto& fields = bind_parameters_->schema()->fields();
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) query += ',';
        query += fields[i]->name();
      }
      query += ')';

      ADBC_RETURN_NOT_OK(
          DoQuery(db, query.c_str(), error, [&](sqlite3_stmt* stmt) -> AdbcStatusCode {
            const int rc = sqlite3_step(stmt);
            if (rc == SQLITE_DONE) return ADBC_STATUS_OK;
            return CheckRc(db, stmt, rc, "sqlite3_step", error);
          }));
    }

    // Insert the rows
    {
      std::string query = "INSERT INTO ";
      query += bulk_table_;
      query += " VALUES (";
      const auto& fields = bind_parameters_->schema()->fields();
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) query += ',';
        query += '?';
      }
      query += ')';

      sqlite3_stmt* stmt;
      int rc = sqlite3_prepare_v2(db, query.c_str(), static_cast<int>(query.size()),
                                  &stmt, /*pzTail=*/nullptr);
      if (rc != SQLITE_OK) {
        std::ignore = CheckRc(db, stmt, rc, "sqlite3_prepare_v2", error);
        return ADBC_STATUS_ALREADY_EXISTS;
      }
      ADBC_RETURN_NOT_OK(DoQuery(db, stmt, error, [&]() -> AdbcStatusCode {
        int rc = SQLITE_OK;
        while (true) {
          std::shared_ptr<arrow::RecordBatch> batch;
          ADBC_RETURN_NOT_OK(
              FromArrowStatus(bind_parameters_->Next().Value(&batch), error));
          if (!batch) break;

          for (int64_t row = 0; row < batch->num_rows(); row++) {
            ADBC_RETURN_NOT_OK(BindParameters(stmt, *batch, row, &rc, error));
            ADBC_RETURN_NOT_OK(CheckRc(db, stmt, rc, "sqlite3_bind", error));

            rc = sqlite3_step(stmt);
            if (rc != SQLITE_DONE) {
              return CheckRc(db, stmt, rc, "sqlite3_step", error);
            }

            rc = sqlite3_reset(stmt);
            ADBC_RETURN_NOT_OK(CheckRc(db, stmt, rc, "sqlite3_reset", error));

            rc = sqlite3_clear_bindings(stmt);
            ADBC_RETURN_NOT_OK(CheckRc(db, stmt, rc, "sqlite3_clear_bindings", error));
          }
        }
        return ADBC_STATUS_OK;
      }));
    }

    return ADBC_STATUS_OK;
  }

  AdbcStatusCode ExecutePrepared(struct AdbcError* error) {
    sqlite3* db = connection_->db();
    int rc = sqlite3_clear_bindings(stmt_);
    ADBC_RETURN_NOT_OK(CheckRc(db, stmt_, rc, "sqlite3_clear_bindings", error));

    rc = sqlite3_reset(stmt_);
    ADBC_RETURN_NOT_OK(CheckRc(db, stmt_, rc, "sqlite3_reset", error));
    auto reader = std::make_shared<SqliteStatementReader>(connection_, stmt_,
                                                          std::move(bind_parameters_));
    ADBC_RETURN_NOT_OK(reader->Init(error));
    result_reader_ = std::move(reader);
    return ADBC_STATUS_OK;
  }

  std::shared_ptr<SqliteConnectionImpl> connection_;

  // Query state

  // Bulk ingestion
  // Target of bulk ingestion (rather janky to store state like this, though…)
  std::string bulk_table_;

  // Prepared statements
  sqlite3_stmt* stmt_;
  std::shared_ptr<arrow::RecordBatchReader> bind_parameters_;

  std::shared_ptr<arrow::RecordBatchReader> result_reader_;
};

// ADBC interface implementation - as private functions so that these
// don't get replaced by the dynamic linker. If we implemented these
// under the Adbc* names, then DriverInit, the linker may resolve
// functions to the address of the functions provided by the driver
// manager instead of our functions.
//
// We could also:
// - Play games with RTLD_DEEPBIND - but this doesn't work with ASan
// - Use __attribute__((visibility("protected"))) - but this is
//   apparently poorly supported by some linkers
// - Play with -Bsymbolic(-functions) - but this has other
//   consequences and complicates the build setup
//
// So in the end some manual effort here was chosen.

AdbcStatusCode SqliteDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  auto impl = std::make_shared<SqliteDatabaseImpl>();
  database->private_data = new std::shared_ptr<SqliteDatabaseImpl>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteDatabaseInit(struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteDatabaseImpl>*>(database->private_data);
  return (*ptr)->Init(error);
}

AdbcStatusCode SqliteDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                       const char* value, struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteDatabaseImpl>*>(database->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode SqliteDatabaseRelease(struct AdbcDatabase* database,
                                     struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteDatabaseImpl>*>(database->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  database->private_data = nullptr;
  return status;
}

AdbcStatusCode SqliteConnectionCommit(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);
  return (*ptr)->Commit(error);
}

AdbcStatusCode SqliteConnectionGetObjects(
    struct AdbcConnection* connection, int depth, const char* catalog,
    const char* db_schema, const char* table_name, const char** table_types,
    const char* column_name, struct AdbcStatement* statement, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->GetObjects(*ptr, depth, catalog, db_schema, table_name, table_types,
                            column_name, error);
}

AdbcStatusCode SqliteConnectionGetTableSchema(struct AdbcConnection* connection,
                                              const char* catalog, const char* db_schema,
                                              const char* table_name,
                                              struct ArrowSchema* schema,
                                              struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);
  return (*ptr)->GetTableSchema(catalog, db_schema, table_name, schema, error);
}

AdbcStatusCode SqliteConnectionGetTableTypes(struct AdbcConnection* connection,
                                             struct AdbcStatement* statement,
                                             struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->GetTableTypes(*ptr, error);
}

AdbcStatusCode SqliteConnectionInit(struct AdbcConnection* connection,
                                    struct AdbcDatabase* database,
                                    struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);
  return (*ptr)->Init(database, error);
}

AdbcStatusCode SqliteConnectionNew(struct AdbcConnection* connection,
                                   struct AdbcError* error) {
  auto impl = std::make_shared<SqliteConnectionImpl>();
  connection->private_data = new std::shared_ptr<SqliteConnectionImpl>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionRelease(struct AdbcConnection* connection,
                                       struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  connection->private_data = nullptr;
  return status;
}

AdbcStatusCode SqliteConnectionRollback(struct AdbcConnection* connection,
                                        struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);
  return (*ptr)->Rollback(error);
}

AdbcStatusCode SqliteConnectionSetOption(struct AdbcConnection* connection,
                                         const char* key, const char* value,
                                         struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_INVALID_STATE;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);

  if (std::strcmp(key, ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0) {
    bool autocommit = false;
    if (std::strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
      autocommit = false;
    } else if (std::strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0) {
      autocommit = true;
    } else {
      SetError(error, "Invalid option value for autocommit: ", value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return (*ptr)->SetAutocommit(autocommit, error);
  } else {
    SetError(error, "Unknown option");
  }
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteStatementBind(struct AdbcStatement* statement,
                                   struct ArrowArray* values, struct ArrowSchema* schema,
                                   struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->Bind(*ptr, values, schema, error);
}

AdbcStatusCode SqliteStatementBindStream(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* stream,
                                         struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->Bind(*ptr, stream, error);
}

AdbcStatusCode SqliteStatementExecute(struct AdbcStatement* statement,
                                      struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->Execute(*ptr, error);
}

AdbcStatusCode SqliteStatementGetPartitionDesc(struct AdbcStatement* statement,
                                               uint8_t* partition_desc,
                                               struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                   size_t* length,
                                                   struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode SqliteStatementGetStream(struct AdbcStatement* statement,
                                        struct ArrowArrayStream* out,
                                        struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->GetStream(*ptr, out, error);
}

AdbcStatusCode SqliteStatementNew(struct AdbcConnection* connection,
                                  struct AdbcStatement* statement,
                                  struct AdbcError* error) {
  auto conn_ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);
  auto impl = std::make_shared<SqliteStatementImpl>(*conn_ptr);
  statement->private_data = new std::shared_ptr<SqliteStatementImpl>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteStatementPrepare(struct AdbcStatement* statement,
                                      struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->Prepare(*ptr, error);
}

AdbcStatusCode SqliteStatementRelease(struct AdbcStatement* statement,
                                      struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  statement->private_data = nullptr;
  return status;
}

AdbcStatusCode SqliteStatementSetOption(struct AdbcStatement* statement, const char* key,
                                        const char* value, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->SetOption(*ptr, key, value, error);
}

AdbcStatusCode SqliteStatementSetSqlQuery(struct AdbcStatement* statement,
                                          const char* query, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_INVALID_STATE;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->SetSqlQuery(*ptr, query, error);
}

}  // namespace

AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  return SqliteDatabaseInit(database, error);
}

AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  return SqliteDatabaseNew(database, error);
}

AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return SqliteDatabaseSetOption(database, key, value, error);
}

AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return SqliteDatabaseRelease(database, error);
}

AdbcStatusCode AdbcConnectionCommit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  return SqliteConnectionCommit(connection, error);
}

AdbcStatusCode AdbcConnectionGetObjects(struct AdbcConnection* connection, int depth,
                                        const char* catalog, const char* db_schema,
                                        const char* table_name, const char** table_types,
                                        const char* column_name,
                                        struct AdbcStatement* statement,
                                        struct AdbcError* error) {
  return SqliteConnectionGetObjects(connection, depth, catalog, db_schema, table_name,
                                    table_types, column_name, statement, error);
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
                                           struct AdbcStatement* statement,
                                           struct AdbcError* error) {
  return SqliteConnectionGetTableTypes(connection, statement, error);
}

AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcDatabase* database,
                                  struct AdbcError* error) {
  return SqliteConnectionInit(connection, database, error);
}

AdbcStatusCode AdbcConnectionNew(struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return SqliteConnectionNew(connection, error);
}

AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  return SqliteConnectionRelease(connection, error);
}

AdbcStatusCode AdbcConnectionRollback(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  return SqliteConnectionRollback(connection, error);
}

AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return SqliteConnectionSetOption(connection, key, value, error);
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

AdbcStatusCode AdbcStatementExecute(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return SqliteStatementExecute(statement, error);
}

AdbcStatusCode AdbcStatementGetPartitionDesc(struct AdbcStatement* statement,
                                             uint8_t* partition_desc,
                                             struct AdbcError* error) {
  return SqliteStatementGetPartitionDesc(statement, partition_desc, error);
}

AdbcStatusCode AdbcStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                 size_t* length,
                                                 struct AdbcError* error) {
  return SqliteStatementGetPartitionDescSize(statement, length, error);
}

AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement* statement,
                                      struct ArrowArrayStream* out,
                                      struct AdbcError* error) {
  return SqliteStatementGetStream(statement, out, error);
}

AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  return SqliteStatementNew(connection, statement, error);
}

AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return SqliteStatementPrepare(statement, error);
}

AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return SqliteStatementRelease(statement, error);
}

AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error) {
  return SqliteStatementSetOption(statement, key, value, error);
}

AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return SqliteStatementSetSqlQuery(statement, query, error);
}

extern "C" {
ADBC_EXPORT
AdbcStatusCode AdbcSqliteDriverInit(size_t count, struct AdbcDriver* driver,
                                    size_t* initialized, struct AdbcError* error) {
  if (count < ADBC_VERSION_0_0_1) return ADBC_STATUS_NOT_IMPLEMENTED;

  std::memset(driver, 0, sizeof(*driver));
  driver->DatabaseInit = SqliteDatabaseInit;
  driver->DatabaseNew = SqliteDatabaseNew;
  driver->DatabaseRelease = SqliteDatabaseRelease;
  driver->DatabaseSetOption = SqliteDatabaseSetOption;

  driver->ConnectionCommit = SqliteConnectionCommit;
  driver->ConnectionGetObjects = SqliteConnectionGetObjects;
  driver->ConnectionGetTableSchema = SqliteConnectionGetTableSchema;
  driver->ConnectionGetTableTypes = SqliteConnectionGetTableTypes;
  driver->ConnectionInit = SqliteConnectionInit;
  driver->ConnectionNew = SqliteConnectionNew;
  driver->ConnectionRelease = SqliteConnectionRelease;
  driver->ConnectionRollback = SqliteConnectionRollback;
  driver->ConnectionSetOption = SqliteConnectionSetOption;

  driver->StatementBind = SqliteStatementBind;
  driver->StatementBindStream = SqliteStatementBindStream;
  driver->StatementExecute = SqliteStatementExecute;
  driver->StatementGetPartitionDesc = SqliteStatementGetPartitionDesc;
  driver->StatementGetPartitionDescSize = SqliteStatementGetPartitionDescSize;
  driver->StatementGetStream = SqliteStatementGetStream;
  driver->StatementNew = SqliteStatementNew;
  driver->StatementPrepare = SqliteStatementPrepare;
  driver->StatementRelease = SqliteStatementRelease;
  driver->StatementSetOption = SqliteStatementSetOption;
  driver->StatementSetSqlQuery = SqliteStatementSetSqlQuery;
  *initialized = ADBC_VERSION_0_0_1;
  return ADBC_STATUS_OK;
}
}
