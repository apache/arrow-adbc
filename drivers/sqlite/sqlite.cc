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

void SetError(sqlite3* db, const std::string& source, struct AdbcError* error) {
  if (!error) return;
  std::string message =
      arrow::util::StringBuilder("[SQLite3] ", source, ": ", sqlite3_errmsg(db));
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

std::shared_ptr<arrow::Schema> StatementToSchema(sqlite3_stmt* stmt) {
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
  explicit SqliteDatabaseImpl() : db_(nullptr), connection_count_(0) {}

  sqlite3* Connect() {
    std::lock_guard<std::mutex> guard(mutex_);
    if (db_) ++connection_count_;
    return db_;
  }

  AdbcStatusCode Init(struct AdbcError* error) {
    if (db_) {
      SetError(error, "Database already initialized");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    const char* filename = ":memory:";
    auto it = options_.find("filename");
    if (it != options_.end()) filename = it->second.c_str();

    int rc = sqlite3_open_v2(filename, &db_, SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE,
                             /*zVfs=*/nullptr);
    ADBC_RETURN_NOT_OK(CheckRc(db_, nullptr, rc, "sqlite3_open_v2", error));
    options_.clear();
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error) {
    if (db_) {
      SetError(error, "Database already initialized");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    options_[key] = value;
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Disconnect(struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);
    if (--connection_count_ < 0) {
      SetError(error, "Connection count underflow");
      return ADBC_STATUS_INTERNAL;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Release(struct AdbcError* error) {
    std::lock_guard<std::mutex> guard(mutex_);

    if (connection_count_ > 0) {
      SetError(error, "Cannot release database with ", connection_count_,
               " open connections");
      return ADBC_STATUS_INTERNAL;
    }

    auto status = sqlite3_close(db_);
    if (status != SQLITE_OK) {
      if (db_) SetError(db_, "sqlite3_close", error);
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

 private:
  sqlite3* db_;
  int connection_count_;
  std::unordered_map<std::string, std::string> options_;
  std::mutex mutex_;
};

class SqliteConnectionImpl {
 public:
  explicit SqliteConnectionImpl(std::shared_ptr<SqliteDatabaseImpl> database)
      : database_(std::move(database)), db_(nullptr) {}

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  sqlite3* db() const { return db_; }

  AdbcStatusCode Init(struct AdbcError* error) {
    db_ = database_->Connect();
    if (!db_) {
      SetError(error, "Database not yet initialized!");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Release(struct AdbcError* error) { return database_->Disconnect(error); }

 private:
  std::shared_ptr<SqliteDatabaseImpl> database_;
  sqlite3* db_;
};

class SqliteStatementImpl : public arrow::RecordBatchReader {
 public:
  SqliteStatementImpl(std::shared_ptr<SqliteConnectionImpl> connection)
      : connection_(std::move(connection)),
        stmt_(nullptr),
        schema_(nullptr),
        bind_index_(0),
        done_(false) {}

  //----------------------------------------------------------
  // arrow::RecordBatchReader
  //----------------------------------------------------------

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
          case arrow::Type::INT64: {
            const sqlite3_int64 value = sqlite3_column_int64(stmt_, col);
            ARROW_RETURN_NOT_OK(
                dynamic_cast<arrow::Int64Builder*>(builders[col].get())->Append(value));
            break;
          }
          case arrow::Type::STRING: {
            const char* value =
                reinterpret_cast<const char*>(sqlite3_column_text(stmt_, col));
            if (!value) {
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
          RETURN_NOT_OK(BindNext());
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

  //----------------------------------------------------------
  // Common Functions
  //----------------------------------------------------------

  AdbcStatusCode Close(struct AdbcError* error) {
    if (stmt_) {
      const int rc = sqlite3_finalize(stmt_);
      stmt_ = nullptr;
      next_parameters_.reset();
      bind_parameters_.reset();
      ADBC_RETURN_NOT_OK(
          CheckRc(connection_->db(), nullptr, rc, "sqlite3_finalize", error));
      connection_.reset();
    }
    return ADBC_STATUS_OK;
  }

  //----------------------------------------------------------
  // Statement Functions
  //----------------------------------------------------------

  AdbcStatusCode Bind(const std::shared_ptr<SqliteStatementImpl>& self,
                      struct ArrowArray* values, struct ArrowSchema* schema,
                      struct AdbcError* error) {
    std::shared_ptr<arrow::RecordBatch> batch;
    auto status = arrow::ImportRecordBatch(values, schema).Value(&batch);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    std::shared_ptr<arrow::Table> table;
    status = arrow::Table::FromRecordBatches({std::move(batch)}).Value(&table);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    bind_parameters_.reset(new arrow::TableBatchReader(std::move(table)));
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Bind(const std::shared_ptr<SqliteStatementImpl>& self,
                      struct ArrowArrayStream* stream, struct AdbcError* error) {
    auto status = arrow::ImportRecordBatchReader(stream).Value(&bind_parameters_);
    if (!status.ok()) {
      SetError(error, status);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Execute(const std::shared_ptr<SqliteStatementImpl>& self,
                         struct AdbcError* error) {
    if (bulk_table_.empty()) {
      return ExecutePrepared(error);
    }
    return ExecuteBulk(error);
  }

  AdbcStatusCode GetStream(const std::shared_ptr<SqliteStatementImpl>& self,
                           struct ArrowArrayStream* out, struct AdbcError* error) {
    if (!stmt_ || !schema_) {
      SetError(error, "Statement has not yet been executed");
      return ADBC_STATUS_UNINITIALIZED;
    }
    auto status = arrow::ExportRecordBatchReader(self, out);
    if (!status.ok()) {
      SetError(error, "Could not initialize result reader: ", status);
      return ADBC_STATUS_UNKNOWN;
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetOption(const std::shared_ptr<SqliteStatementImpl>& self,
                           const char* key, const char* value, struct AdbcError* error) {
    if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0) {
      // Bulk ingest
      if (std::strlen(value) == 0) return ADBC_STATUS_INVALID_ARGUMENT;
      bulk_table_ = value;
      if (stmt_) {
        int rc = sqlite3_finalize(stmt_);
        ADBC_RETURN_NOT_OK(
            CheckRc(connection_->db(), nullptr, rc, "sqlite3_finalize", error));
      }
      return ADBC_STATUS_OK;
    }
    SetError(error, "Unknown option: ", key);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode SetSqlQuery(const std::shared_ptr<SqliteStatementImpl>& self,
                             const char* query, struct AdbcError* error) {
    bulk_table_.clear();
    sqlite3* db = connection_->db();
    int rc = sqlite3_prepare_v2(db, query, static_cast<int>(std::strlen(query)), &stmt_,
                                /*pzTail=*/nullptr);
    return CheckRc(connection_->db(), stmt_, rc, "sqlite3_prepare_v2", error);
  }

 private:
  arrow::Result<int> BindNext() {
    if (!next_parameters_ || bind_index_ >= next_parameters_->num_rows()) {
      return SQLITE_OK;
    }

    return BindImpl(stmt_, *next_parameters_, bind_index_++);
  }

  arrow::Result<int> BindImpl(sqlite3_stmt* stmt, const arrow::RecordBatch& data,
                              int64_t row) {
    int col_index = 1;
    for (const auto& column : data.columns()) {
      if (column->IsNull(row)) {
        const int rc = sqlite3_bind_null(stmt, col_index);
        if (rc != SQLITE_OK) return rc;
      } else {
        switch (column->type()->id()) {
          case arrow::Type::INT64: {
            const int rc = sqlite3_bind_int64(
                stmt, col_index,
                static_cast<const arrow::Int64Array&>(*column).Value(row));
            if (rc != SQLITE_OK) return rc;
            break;
          }
          case arrow::Type::STRING: {
            const auto& strings = static_cast<const arrow::StringArray&>(*column);
            const int rc = sqlite3_bind_text64(stmt, col_index, strings.Value(row).data(),
                                               strings.value_length(row), SQLITE_STATIC,
                                               SQLITE_UTF8);
            if (rc != SQLITE_OK) return rc;
            break;
          }
          default:
            return arrow::Status::NotImplemented("Binding parameter of type ",
                                                 *column->type());
        }
      }
      col_index++;
    }
    return SQLITE_OK;
  }

  AdbcStatusCode ExecuteBulk(struct AdbcError* error) {
    if (!bind_parameters_) {
      SetError(error, "Must AdbcStatementBind for bulk insertion");
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    sqlite3* db = connection_->db();
    sqlite3_stmt* stmt = nullptr;
    int rc = SQLITE_OK;

    auto check_status = [&](const arrow::Status& st) mutable {
      if (!st.ok()) {
        SetError(error, st);
        if (stmt) {
          rc = sqlite3_finalize(stmt);
          if (rc != SQLITE_OK) {
            SetError(db, "sqlite3_finalize", error);
          }
        }
        return ADBC_STATUS_IO;
      }
      return ADBC_STATUS_OK;
    };

    // Create the table
    // TODO: parameter to choose append/overwrite/error
    {
      // XXX: not injection-safe
      std::string query = "CREATE TABLE ";
      query += bulk_table_;
      query += " (";
      const auto& fields = bind_parameters_->schema()->fields();
      for (int i = 0; i < fields.size(); i++) {
        if (i > 0) query += ',';
        query += fields[i]->name();
      }
      query += ')';

      rc = sqlite3_prepare_v2(db, query.c_str(), static_cast<int>(query.size()), &stmt,
                              /*pzTail=*/nullptr);
      ADBC_RETURN_NOT_OK(CheckRc(db, stmt, rc, "sqlite3_prepare_v2", error));

      rc = sqlite3_step(stmt);
      if (rc != SQLITE_DONE) return CheckRc(db, stmt, rc, "sqlite3_step", error);

      rc = sqlite3_finalize(stmt);
      ADBC_RETURN_NOT_OK(CheckRc(db, stmt, rc, "sqlite3_finalize", error));
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
      rc = sqlite3_prepare_v2(db, query.c_str(), static_cast<int>(query.size()), &stmt,
                              /*pzTail=*/nullptr);
      ADBC_RETURN_NOT_OK(CheckRc(db, stmt, rc, query.c_str(), error));
    }

    while (true) {
      std::shared_ptr<arrow::RecordBatch> batch;
      auto status = bind_parameters_->Next().Value(&batch);
      ADBC_RETURN_NOT_OK(check_status(status));
      if (!batch) break;

      for (int64_t row = 0; row < batch->num_rows(); row++) {
        status = BindImpl(stmt, *batch, row).Value(&rc);
        ADBC_RETURN_NOT_OK(check_status(status));
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

    rc = sqlite3_finalize(stmt);
    return CheckRc(db, nullptr, rc, "sqlite3_finalize", error);
  }

  AdbcStatusCode ExecutePrepared(struct AdbcError* error) {
    sqlite3* db = connection_->db();
    int rc = SQLITE_OK;
    if (schema_) {
      rc = sqlite3_clear_bindings(stmt_);
      ADBC_RETURN_NOT_OK(CheckRc(db, stmt_, rc, "sqlite3_clear_bindings", error));

      rc = sqlite3_reset(stmt_);
      ADBC_RETURN_NOT_OK(CheckRc(db, stmt_, rc, "sqlite3_reset", error));
    }
    // Step the statement and get the schema (SQLite doesn't
    // necessarily know the schema until it begins to execute it)

    Status status;
    if (bind_parameters_) {
      status = bind_parameters_->ReadNext(&next_parameters_);
      if (status.ok()) status = BindNext().Value(&rc);
    }
    // XXX: with parameters, inferring the schema from the first
    // argument is inaccurate (what if one is null?). Is there a way
    // to hint to SQLite the real type?

    if (!status.ok()) {
      // TODO: map Arrow codes to ADBC codes
      SetError(error, status);
      return ADBC_STATUS_IO;
    }
    ADBC_RETURN_NOT_OK(CheckRc(db, stmt_, rc, "sqlite3_bind", error));

    rc = sqlite3_step(stmt_);
    if (rc == SQLITE_ERROR) {
      return CheckRc(db, stmt_, rc, "sqlite3_error", error);
    }
    schema_ = StatementToSchema(stmt_);
    done_ = rc != SQLITE_ROW;
    return ADBC_STATUS_OK;
  }

  std::shared_ptr<SqliteConnectionImpl> connection_;
  // Target of bulk ingestion (rather janky to store state like this, though…)
  std::string bulk_table_;
  sqlite3_stmt* stmt_;
  std::shared_ptr<arrow::Schema> schema_;
  std::shared_ptr<arrow::RecordBatchReader> bind_parameters_;
  std::shared_ptr<arrow::RecordBatch> next_parameters_;
  int64_t bind_index_;
  bool done_;
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
  if (!database->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteDatabaseImpl>*>(database->private_data);
  return (*ptr)->Init(error);
}

AdbcStatusCode SqliteDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                       const char* value, struct AdbcError* error) {
  if (!database || !database->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteDatabaseImpl>*>(database->private_data);
  return (*ptr)->SetOption(key, value, error);
}

AdbcStatusCode SqliteDatabaseRelease(struct AdbcDatabase* database,
                                     struct AdbcError* error) {
  if (!database->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteDatabaseImpl>*>(database->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  database->private_data = nullptr;
  return status;
}

AdbcStatusCode SqliteConnectionNew(struct AdbcDatabase* database,
                                   struct AdbcConnection* connection,
                                   struct AdbcError* error) {
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteDatabaseImpl>*>(database->private_data);
  auto impl = std::make_shared<SqliteConnectionImpl>(*ptr);
  connection->private_data = new std::shared_ptr<SqliteConnectionImpl>(impl);
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionSetOption(struct AdbcConnection* connection,
                                         const char* key, const char* value,
                                         struct AdbcError* error) {
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteConnectionInit(struct AdbcConnection* connection,
                                    struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);
  return (*ptr)->Init(error);
}

AdbcStatusCode SqliteConnectionRelease(struct AdbcConnection* connection,
                                       struct AdbcError* error) {
  if (!connection->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto ptr =
      reinterpret_cast<std::shared_ptr<SqliteConnectionImpl>*>(connection->private_data);
  AdbcStatusCode status = (*ptr)->Release(error);
  delete ptr;
  connection->private_data = nullptr;
  return status;
}

AdbcStatusCode SqliteStatementBind(struct AdbcStatement* statement,
                                   struct ArrowArray* values, struct ArrowSchema* schema,
                                   struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->Bind(*ptr, values, schema, error);
}

AdbcStatusCode SqliteStatementBindStream(struct AdbcStatement* statement,
                                         struct ArrowArrayStream* stream,
                                         struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->Bind(*ptr, stream, error);
}

AdbcStatusCode SqliteStatementExecute(struct AdbcStatement* statement,
                                      struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
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
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
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
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  // No-op
  return ADBC_STATUS_OK;
}

AdbcStatusCode SqliteStatementRelease(struct AdbcStatement* statement,
                                      struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  auto status = (*ptr)->Close(error);
  delete ptr;
  statement->private_data = nullptr;
  return status;
}

AdbcStatusCode SqliteStatementSetOption(struct AdbcStatement* statement, const char* key,
                                        const char* value, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->SetOption(*ptr, key, value, error);
}

AdbcStatusCode SqliteStatementSetSqlQuery(struct AdbcStatement* statement,
                                          const char* query, struct AdbcError* error) {
  if (!statement->private_data) return ADBC_STATUS_UNINITIALIZED;
  auto* ptr =
      reinterpret_cast<std::shared_ptr<SqliteStatementImpl>*>(statement->private_data);
  return (*ptr)->SetSqlQuery(*ptr, query, error);
}

}  // namespace

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcDatabaseInit(struct AdbcDatabase* database, struct AdbcError* error) {
  return SqliteDatabaseInit(database, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcDatabaseNew(struct AdbcDatabase* database, struct AdbcError* error) {
  return SqliteDatabaseNew(database, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcDatabaseSetOption(struct AdbcDatabase* database, const char* key,
                                     const char* value, struct AdbcError* error) {
  return SqliteDatabaseSetOption(database, key, value, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcDatabaseRelease(struct AdbcDatabase* database,
                                   struct AdbcError* error) {
  return SqliteDatabaseRelease(database, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionNew(struct AdbcDatabase* database,
                                 struct AdbcConnection* connection,
                                 struct AdbcError* error) {
  return SqliteConnectionNew(database, connection, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionSetOption(struct AdbcConnection* connection, const char* key,
                                       const char* value, struct AdbcError* error) {
  return SqliteConnectionSetOption(connection, key, value, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionInit(struct AdbcConnection* connection,
                                  struct AdbcError* error) {
  return SqliteConnectionInit(connection, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcConnectionRelease(struct AdbcConnection* connection,
                                     struct AdbcError* error) {
  return SqliteConnectionRelease(connection, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementBind(struct AdbcStatement* statement,
                                 struct ArrowArray* values, struct ArrowSchema* schema,
                                 struct AdbcError* error) {
  return SqliteStatementBind(statement, values, schema, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementBindStream(struct AdbcStatement* statement,
                                       struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  return SqliteStatementBindStream(statement, stream, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementExecute(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return SqliteStatementExecute(statement, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementGetPartitionDesc(struct AdbcStatement* statement,
                                             uint8_t* partition_desc,
                                             struct AdbcError* error) {
  return SqliteStatementGetPartitionDesc(statement, partition_desc, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementGetPartitionDescSize(struct AdbcStatement* statement,
                                                 size_t* length,
                                                 struct AdbcError* error) {
  return SqliteStatementGetPartitionDescSize(statement, length, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementGetStream(struct AdbcStatement* statement,
                                      struct ArrowArrayStream* out,
                                      struct AdbcError* error) {
  return SqliteStatementGetStream(statement, out, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementNew(struct AdbcConnection* connection,
                                struct AdbcStatement* statement,
                                struct AdbcError* error) {
  return SqliteStatementNew(connection, statement, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementPrepare(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return SqliteStatementPrepare(statement, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementRelease(struct AdbcStatement* statement,
                                    struct AdbcError* error) {
  return SqliteStatementRelease(statement, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementSetOption(struct AdbcStatement* statement, const char* key,
                                      const char* value, struct AdbcError* error) {
  return SqliteStatementSetOption(statement, key, value, error);
}

ADBC_DRIVER_EXPORT
AdbcStatusCode AdbcStatementSetSqlQuery(struct AdbcStatement* statement,
                                        const char* query, struct AdbcError* error) {
  return SqliteStatementSetSqlQuery(statement, query, error);
}

extern "C" {
ARROW_EXPORT
AdbcStatusCode AdbcSqliteDriverInit(size_t count, struct AdbcDriver* driver,
                                    size_t* initialized, struct AdbcError* error) {
  if (count < ADBC_VERSION_0_0_1) return ADBC_STATUS_NOT_IMPLEMENTED;

  std::memset(driver, 0, sizeof(*driver));
  driver->DatabaseInit = SqliteDatabaseInit;
  driver->DatabaseNew = SqliteDatabaseNew;
  driver->DatabaseRelease = SqliteDatabaseRelease;
  driver->DatabaseSetOption = SqliteDatabaseSetOption;

  driver->ConnectionInit = SqliteConnectionInit;
  driver->ConnectionNew = SqliteConnectionNew;
  driver->ConnectionRelease = SqliteConnectionRelease;
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
