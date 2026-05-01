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

#pragma once

#include <cerrno>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>

#include "driver/framework/status.h"

namespace adbc::client {

namespace internal {
class BaseDriver;
class BaseDatabase;
class BaseConnection;
class BaseStatement;
}  // namespace internal

using adbc::driver::Result;
using adbc::driver::Status;

class Context {
 public:
  enum class LogLevel { kInfo, kWarn };

  virtual ~Context() = default;

  virtual void OnUnreleasableStatement(
      std::shared_ptr<internal::BaseConnection> connection, AdbcStatement* statement,
      const AdbcError* error) {
    Log(LogLevel::kWarn, "leaking unreleasable statement");
  }

  virtual void OnUnreleasableConnection(std::shared_ptr<internal::BaseDatabase> database,
                                        AdbcConnection* connection,
                                        const AdbcError* error) {
    Log(LogLevel::kWarn, "leaking unreleasable connection");
  }

  virtual void OnUnreleaseableDatabase(std::shared_ptr<internal::BaseDriver> driver,
                                       AdbcDatabase* database, const AdbcError* error) {
    Log(LogLevel::kWarn, "leaking unreleasable database");
  }

  virtual void OnUnreleaseableDriver(AdbcDriver* driver, const AdbcError* error) {
    Log(LogLevel::kWarn, "leaking unreleasable driver");
  }

  virtual void OnDeleteHandleWithoutClose(
      const std::shared_ptr<internal::BaseStatement>& statement) {
    Log(LogLevel::kWarn,
        "Leaking Statement handle; AdbcStatement will be auto-released when all child "
        "readers are released. Use Statement::Release() to avoid this message.");
  }

  virtual void OnDeleteHandleWithoutClose(
      const std::shared_ptr<internal::BaseConnection>& connection) {
    Log(LogLevel::kWarn,
        "Leaking Connection handle; AdbcConnection will be auto-released when all child "
        "readers are released. Use Connection::Release() to avoid this message.");
  }

  virtual void OnDeleteHandleWithoutClose(
      const std::shared_ptr<internal::BaseDatabase>& database) {
    Log(LogLevel::kWarn,
        "Leaking Database handle; AdbcDatabase will be auto-released when all child "
        "readers are released. Use Database::Release() to avoid this message.");
  }

  virtual void Log(LogLevel level, std::string_view message) {}

  static std::shared_ptr<Context> Default() { return std::make_unique<Context>(); }
};

namespace internal {

class BaseDriver {
 public:
  explicit BaseDriver(std::shared_ptr<Context> context = std::make_unique<Context>())
      : context_(context) {}

  BaseDriver(const BaseDriver& rhs) = delete;

  Status Load(AdbcDriverInitFunc init_func) {
    return Status::FromAdbc(init_func(ADBC_VERSION_1_1_0, &driver_, &error_), error_);
  }

  Status Unload() {
    AdbcStatusCode code = driver_.release(&driver_, &error_);
    return Status::FromAdbc(code, error_);
  }

  AdbcDriver* driver() { return &driver_; }

  Context* context() { return context_.get(); }

  Status CheckValid() {
    if (!driver_.release) {
      return Status::InvalidState("Driver is released");
    } else {
      return Status::Ok();
    }
  }

 private:
  std::shared_ptr<Context> context_;
  AdbcDriver driver_{};
  AdbcError error_{ADBC_ERROR_INIT};
};

class BaseObject {
 public:
  BaseObject() = default;
  const std::shared_ptr<BaseDriver>& GetSharedDriver() { return driver_; }

 protected:
  std::shared_ptr<BaseDriver> driver_;
  AdbcError error_{ADBC_ERROR_INIT};

  void NewBase(std::shared_ptr<BaseDriver> driver) { driver_ = driver; }
  void ReleaseBase() { driver_.reset(); }
  AdbcDriver* driver() { return driver_->driver(); }
  Context* context() { return driver_->context(); }
};

#define WRAP_CALL(func, ...) (driver()->func(&database_, __VA_ARGS__, &error_))
#define WRAP_CALL0(func) (driver()->func(&database_, &error_))

class BaseDatabase : public BaseObject {
 public:
  BaseDatabase() = default;
  BaseDatabase(const BaseDatabase& rhs) = delete;
  ~BaseDatabase() {
    if (driver_ && database_.private_data) {
      AdbcStatusCode code = WRAP_CALL0(DatabaseRelease);
      if (code != ADBC_STATUS_OK) {
        context()->OnUnreleaseableDatabase(driver_, &database_, &error_);
      }
    }
  }

  AdbcDatabase* database() { return &database_; }

  Status New(std::shared_ptr<BaseDriver> parent) {
    NewBase(std::move(parent));
    AdbcStatusCode code = WRAP_CALL0(DatabaseNew);
    return Status::FromAdbc(code, error_);
  }

  Status Init() {
    UNWRAP_STATUS(CheckValid());
    AdbcStatusCode code = WRAP_CALL0(DatabaseInit);
    return Status::FromAdbc(code, error_);
  }

  Status Release() {
    UNWRAP_STATUS(CheckValid());
    AdbcStatusCode code = WRAP_CALL0(DatabaseRelease);
    if (code == ADBC_STATUS_OK) {
      ReleaseBase();
      std::memset(&database_, 0, sizeof(database_));
    }

    return Status::FromAdbc(code, error_);
  }

  Status CheckValid() {
    if (!driver_ || !database_.private_data) {
      return Status::InvalidState("BaseDatabase is released");
    }

    return driver_->CheckValid();
  }

 private:
  AdbcDatabase database_{};
};

#undef WRAP_CALL
#undef WRAP_CALL0

#define WRAP_CALL(func, ...) (driver()->func(&connection_, __VA_ARGS__, &error_))
#define WRAP_CALL0(func) (driver()->func(&connection_, &error_))

class BaseConnection : public BaseObject {
 public:
  BaseConnection() = default;
  BaseConnection(const BaseConnection& rhs) = delete;
  ~BaseConnection() {
    if (driver_ && connection_.private_data) {
      AdbcStatusCode code = WRAP_CALL0(ConnectionRelease);
      if (code != ADBC_STATUS_OK) {
        context()->OnUnreleasableConnection(database_, &connection_, &error_);
      }
    }
  }

  AdbcConnection* connection() { return &connection_; }

  Status New(std::shared_ptr<BaseDatabase> database) {
    UNWRAP_STATUS(database->CheckValid());
    NewBase(database->GetSharedDriver());
    AdbcStatusCode code = WRAP_CALL0(ConnectionNew);
    if (code == ADBC_STATUS_OK) {
      database_ = database;
    }

    return Status::FromAdbc(code, error_);
  }

  Status Init() {
    UNWRAP_STATUS(CheckValid());
    AdbcStatusCode code = WRAP_CALL(ConnectionInit, database_->database());
    return Status::FromAdbc(code, error_);
  }

  Status Release() {
    UNWRAP_STATUS(CheckValid());
    AdbcStatusCode code = WRAP_CALL0(ConnectionRelease);
    if (code == ADBC_STATUS_OK) {
      ReleaseBase();
      database_.reset();
      std::memset(&connection_, 0, sizeof(connection_));
    }

    return Status::FromAdbc(code, error_);
  }

  Status Cancel() {
    UNWRAP_STATUS(CheckValid());
    AdbcStatusCode code = WRAP_CALL0(ConnectionCancel);
    return Status::FromAdbc(code, error_);
  }

  Status GetInfo(const uint32_t* info_codes, size_t n_info_codes, ArrowArrayStream* out) {
    UNWRAP_STATUS(CheckValid());
    AdbcStatusCode code = WRAP_CALL(ConnectionGetInfo, info_codes, n_info_codes, out);
    return Status::FromAdbc(code, error_);
  }

  Status CheckValid() {
    if (!driver_ || !database_ || !connection_.private_data) {
      return Status::InvalidState("BaseConnection is released");
    }

    return database_->CheckValid();
  }

 private:
  std::shared_ptr<BaseDatabase> database_;
  AdbcConnection connection_{};
};

#undef WRAP_CALL
#undef WRAP_CALL0

#define WRAP_CALL(func, ...) (driver_->driver()->func(&statement_, __VA_ARGS__, &error_))
#define WRAP_CALL0(func) (driver_->driver()->func(&statement_, &error_))

class BaseStatement : public BaseObject {
 public:
  BaseStatement() = default;
  BaseStatement(const BaseStatement& rhs) = delete;
  ~BaseStatement() {
    if (driver_ && statement_.private_data) {
      AdbcStatusCode code = WRAP_CALL0(StatementRelease);
      if (code != ADBC_STATUS_OK) {
        context()->OnUnreleasableStatement(connection_, &statement_, &error_);
      }
    }
  }

  Status New(std::shared_ptr<BaseConnection> connection) {
    NewBase(connection->GetSharedDriver());
    AdbcStatusCode code =
        driver()->StatementNew(connection->connection(), &statement_, &error_);
    if (code == ADBC_STATUS_OK) {
      connection_ = connection;
    }

    return Status::FromAdbc(code, error_);
  }

  Status Release() {
    UNWRAP_STATUS(CheckValid());
    AdbcStatusCode code = WRAP_CALL0(StatementRelease);
    if (code == ADBC_STATUS_OK) {
      ReleaseBase();
      connection_.reset();
      std::memset(&statement_, 0, sizeof(statement_));
    }

    return Status::FromAdbc(code, error_);
  }

  Status SetSqlQuery(const char* query) {
    UNWRAP_STATUS(CheckValid());
    AdbcStatusCode code = WRAP_CALL(StatementSetSqlQuery, query);
    return Status::FromAdbc(code, error_);
  }

  Status ExecuteQuery(ArrowArrayStream* stream, int64_t* affected_rows) {
    UNWRAP_STATUS(CheckValid());
    AdbcStatusCode code = WRAP_CALL(StatementExecuteQuery, stream, affected_rows);
    return Status::FromAdbc(code, error_);
  }

  Status CheckValid() {
    if (!driver_ || !connection_ || !statement_.private_data) {
      return Status::InvalidState("BaseStatement is released");
    }

    return connection_->CheckValid();
  }

 private:
  std::shared_ptr<BaseConnection> connection_;
  AdbcStatement statement_{};
};

#undef WRAP_CALL
#undef WRAP_CALL0

}  // namespace internal

template <typename Parent>
class Stream {
 public:
  explicit Stream(Parent parent) : parent_(parent) {}

  Stream& operator=(const Stream& rhs) = delete;
  Stream(const Stream& rhs) = delete;

  Stream(Stream&& rhs) : Stream(std::move(rhs.parent_)) {
    std::memcpy(&stream_, &rhs.stream_, sizeof(ArrowArrayStream));
    std::memset(&rhs.stream_, 0, sizeof(ArrowArrayStream));
    rows_affected_ = rhs.rows_affected_;
  }

  Stream& operator=(Stream&& rhs) {
    parent_ = std::move(rhs.parent_);
    std::memcpy(&stream_, &rhs.stream_, sizeof(ArrowArrayStream));
    std::memset(&rhs.stream_, 0, sizeof(ArrowArrayStream));
    rows_affected_ = rhs.rows_affected_;
    return *this;
  }

  ArrowArrayStream* stream() { return &stream_; }

  int64_t rows_affected() { return rows_affected_; }

  int64_t* mutable_rows_affected() { return &rows_affected_; }

  ~Stream() {
    if (stream_.release) {
      stream_.release(&stream_);
    }
  }

  void Export(ArrowArrayStream* out) {
    Stream* instance = new Stream();
    instance->parent_ = std::move(parent_);
    std::memcpy(&instance->stream_, &stream_, sizeof(ArrowArrayStream));
    std::memset(&stream_, 0, sizeof(ArrowArrayStream));
    instance->rows_affected_ = rows_affected_;

    out->get_schema = &CGetSchema;
    out->get_next = &CGetNext;
    out->get_last_error = &CGetLastError;
    out->release = &CRelease;
    out->private_data = instance;
  }

 private:
  Parent parent_;
  ArrowArrayStream stream_{};
  int64_t rows_affected_{-1};
  // For the specific case of a stream whose parent is no longer valid,
  // this lets us save the error message and return a const char* from
  // get_last_error().
  Status last_status_;

  static int CGetSchema(ArrowArrayStream* stream, ArrowSchema* schema) {
    auto private_data = reinterpret_cast<Stream*>(stream->private_data);
    if (!private_data->parent_->CheckValid().ok()) {
      return EADDRNOTAVAIL;
    }

    return private_data->GetSchema(schema);
  }

  static int CGetNext(ArrowArrayStream* stream, ArrowArray* array) {
    auto private_data = reinterpret_cast<Stream*>(stream->private_data);
    if (!private_data->parent_->CheckValid().ok()) {
      return EADDRNOTAVAIL;
    }

    return private_data->GetNext(array);
  }

  static const char* CGetLastError(ArrowArrayStream* stream) {
    auto private_data = reinterpret_cast<Stream*>(stream->private_data);
    private_data->last_status_ = private_data->CheckValid();
    if (!private_data->last_status_.ok()) {
      return private_data->last_status_.message();
    }

    return private_data->GetLastError();
  }

  static void CRelease(ArrowArrayStream* stream) {
    delete reinterpret_cast<Stream*>(stream->private_data);
    stream->release = nullptr;
    stream->private_data = nullptr;
  }
};

class Driver;
class Database;
class Connection;
class Statement;
using ConnectionStream = Stream<std::shared_ptr<internal::BaseConnection>>;
using StatementStream = Stream<std::shared_ptr<internal::BaseStatement>>;

class Statement {
 public:
  Statement& operator=(const Statement&) = delete;
  Statement(const Statement& rhs) = delete;
  Statement(Statement&& rhs) : base_(std::move(rhs.base_)) {}
  Statement& operator=(Statement&& rhs) {
    base_ = std::move(rhs.base_);
    return *this;
  }

  ~Statement() {
    if (base_ && base_->GetSharedDriver()) {
      base_->GetSharedDriver()->context()->OnDeleteHandleWithoutClose(base_);
    }
  }

  Status Release() {
    UNWRAP_STATUS(CheckValid());
    UNWRAP_STATUS(base_->Release());
    base_.reset();
    return Status::Ok();
  }

  Status SetSqlQuery(const std::string& query) {
    UNWRAP_STATUS(CheckValid());
    return base_->SetSqlQuery(query.c_str());
  }

  Result<StatementStream> ExecuteQuery() {
    UNWRAP_STATUS(CheckValid());
    StatementStream out(base_);
    UNWRAP_STATUS(base_->ExecuteQuery(out.stream(), out.mutable_rows_affected()));
    return out;
  }

 private:
  std::shared_ptr<internal::BaseStatement> base_;

  friend class Connection;
  explicit Statement(std::shared_ptr<internal::BaseStatement> base)
      : base_(std::move(base)) {}

  Status CheckValid() {
    if (!base_) {
      return Status::InvalidState("Statement handle has been released");
    } else {
      return Status::Ok();
    }
  }
};

class Connection {
 public:
  Connection& operator=(const Connection&) = delete;
  Connection(const Connection& rhs) = delete;
  Connection(Connection&& rhs) : base_(std::move(rhs.base_)) {}
  Connection& operator=(Connection&& rhs) {
    base_ = std::move(rhs.base_);
    return *this;
  }

  ~Connection() {
    if (base_ && base_->GetSharedDriver()) {
      base_->GetSharedDriver()->context()->OnDeleteHandleWithoutClose(base_);
    }
  }

  Status Release() {
    UNWRAP_STATUS(CheckValid());
    UNWRAP_STATUS(base_->Release());
    base_.reset();
    return Status::Ok();
  }

  Result<Statement> NewStatement() {
    UNWRAP_STATUS(CheckValid());
    auto child = std::make_shared<internal::BaseStatement>();
    UNWRAP_STATUS(child->New(base_));
    return Statement(std::move(child));
  }

  Status Cancel() {
    UNWRAP_STATUS(CheckValid());
    return base_->Cancel();
  }

  Result<ConnectionStream> GetInfo(const std::vector<uint32_t>& info_codes = {}) {
    UNWRAP_STATUS(CheckValid());
    ConnectionStream out(base_);
    UNWRAP_STATUS(base_->GetInfo(info_codes.data(), info_codes.size(), out.stream()));
    return out;
  }

 private:
  std::shared_ptr<internal::BaseConnection> base_;

  friend class Database;
  explicit Connection(std::shared_ptr<internal::BaseConnection> base)
      : base_(std::move(base)) {}

  Status CheckValid() {
    if (!base_) {
      return Status::InvalidState("Connection handle has been released");
    } else {
      return Status::Ok();
    }
  }
};

class Database {
 public:
  Database& operator=(const Database&) = delete;
  Database(const Database& rhs) = delete;
  Database(Database&& rhs) : base_(std::move(rhs.base_)) {}
  Database& operator=(Database&& rhs) {
    base_ = std::move(rhs.base_);
    return *this;
  }

  ~Database() {
    if (base_ && base_->GetSharedDriver()) {
      base_->GetSharedDriver()->context()->OnDeleteHandleWithoutClose(base_);
    }
  }

  Status Release() {
    UNWRAP_STATUS(CheckValid());
    UNWRAP_STATUS(base_->Release());
    base_.reset();
    return Status::Ok();
  }

  Result<Connection> NewConnection() {
    UNWRAP_STATUS(CheckValid());

    auto child = std::make_shared<internal::BaseConnection>();
    UNWRAP_STATUS(child->New(base_));
    UNWRAP_STATUS(child->Init());
    return Connection(std::move(child));
  }

 private:
  std::shared_ptr<internal::BaseDatabase> base_;

  friend class Driver;
  explicit Database(std::shared_ptr<internal::BaseDatabase> base)
      : base_(std::move(base)) {}

  Status CheckValid() {
    if (!base_) {
      return Status::InvalidState("Database handle has been released");
    } else {
      return Status::Ok();
    }
  }
};

class Driver {
 public:
  explicit Driver(std::shared_ptr<Context> context = Context::Default())
      : base_(std::make_shared<internal::BaseDriver>(std::move(context))) {}

  Driver(const Driver& rhs) = delete;
  Driver(Driver&& rhs) : base_(std::move(rhs.base_)) {}
  Driver& operator=(Driver&& rhs) {
    base_ = std::move(rhs.base_);
    return *this;
  }

  Status Load(AdbcDriverInitFunc init_func) { return base_->Load(init_func); }

  Status Unload() { return base_->Unload(); }

  Result<Database> NewDatabase() {
    auto child = std::make_shared<internal::BaseDatabase>();
    UNWRAP_STATUS(child->New(base_));
    UNWRAP_STATUS(child->Init());
    return Database(std::move(child));
  }

 private:
  std::shared_ptr<internal::BaseDriver> base_;
};

}  // namespace adbc::client
