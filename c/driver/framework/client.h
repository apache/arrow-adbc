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

#include <memory>

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

  virtual void Log(LogLevel level, std::string_view message) {}

  static std::shared_ptr<Context> Default() { return std::make_unique<Context>(); }
};

namespace internal {

class BaseDriver {
 public:
  BaseDriver(std::shared_ptr<Context> context = std::make_unique<Context>())
      : context_(context) {}
  Status Init(AdbcDriverInitFunc init_func) {
    return Status::FromAdbc(init_func(ADBC_VERSION_1_1_0, &driver_, &error_), error_);
  }

  AdbcDriver* driver() { return &driver_; }

  Context* context() { return context_.get(); }

 private:
  std::shared_ptr<Context> context_;
  AdbcDriver driver_{};
  AdbcError error_{ADBC_ERROR_INIT};
};

class BaseObject {
 public:
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
    AdbcStatusCode code = WRAP_CALL0(DatabaseRelease);
    if (code == ADBC_STATUS_OK) {
      ReleaseBase();
      std::memset(&database_, 0, sizeof(database_));
    }

    return Status::FromAdbc(code, error_);
  }

 private:
  AdbcDatabase database_{};

  Status CheckValid() {
    if (driver_) {
      return Status::Ok();
    } else {
      return driver::status::InvalidState("BaseDatabase is not valid");
    }
  }
};

#undef WRAP_CALL
#undef WRAP_CALL0

#define WRAP_CALL(func, ...) (driver()->func(&connection_, __VA_ARGS__, &error_))
#define WRAP_CALL0(func) (driver()->func(&connection_, &error_))

class BaseConnection : public BaseObject {
 public:
  ~BaseConnection() {
    AdbcStatusCode code = WRAP_CALL0(ConnectionRelease);
    if (code != ADBC_STATUS_OK) {
      context()->OnUnreleasableConnection(database_, &connection_, &error_);
    }
  }

  AdbcConnection* connection() { return &connection_; }

  Status New(std::shared_ptr<BaseDatabase> database) {
    NewBase(database->GetSharedDriver());
    AdbcStatusCode code = WRAP_CALL0(ConnectionNew);
    return Status::FromAdbc(code, error_);
  }

  Status Init() {
    UNWRAP_STATUS(CheckValid());
    AdbcStatusCode code = WRAP_CALL(ConnectionInit, database_->database());
    return Status::FromAdbc(code, error_);
  }

  Status Release() {
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

 private:
  std::shared_ptr<BaseDatabase> database_;
  AdbcConnection connection_{};

  Status CheckValid() {
    if (driver_ && database_ && connection_.private_data) {
      return Status::Ok();
    } else {
      return driver::status::InvalidState("BaseConnection is not valid");
    }
  }
};

#undef WRAP_CALL
#undef WRAP_CALL0

#define WRAP_CALL(func, ...) (driver_->driver()->func(&statement_, __VA_ARGS__, &error_))
#define WRAP_CALL0(func) (driver_->driver()->func(&statement_, &error_))

class BaseStatement : BaseObject {
 public:
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
    return Status::FromAdbc(code, error_);
  }

  Status Release() {
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

 private:
  std::shared_ptr<BaseConnection> connection_;
  AdbcStatement statement_{};

  Status CheckValid() {
    if (driver_ && connection_ && statement_.private_data) {
      return Status::Ok();
    } else {
      return driver::status::InvalidState("BaseStatement is not valid");
    }
  }
};

#undef WRAP_CALL
#undef WRAP_CALL0

}  // namespace internal

template <typename Parent>
class Stream {
 public:
  explicit Stream(Parent parent) : parent_(parent) {}
  Stream(Stream&& rhs) : Stream(rhs.get()) {
    parent_ = std::move(rhs.parent_);
    std::memcpy(&stream_, &rhs.stream_, sizeof(ArrowArrayStream));
    std::memset(rhs.stream_, 0, sizeof(ArrowArrayStream));
    rows_affected_ = rhs.rows_affected_;
  }

  Stream& operator=(Stream&& rhs) {
    parent_ = std::move(rhs.parent_);
    std::memcpy(&stream_, &rhs.stream_, sizeof(ArrowArrayStream));
    std::memset(rhs.stream_, 0, sizeof(ArrowArrayStream));
    rows_affected_ = rhs.rows_affected_;
    return *this;
  }

  Stream(const Stream& rhs) = delete;

  ArrowArrayStream* stream() { return &stream_; }

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
    std::memset(stream_, 0, sizeof(ArrowArrayStream));
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

  static int CGetSchema(ArrowArrayStream* stream, ArrowSchema* schema) {
    return reinterpret_cast<Stream*>(stream->private_data)->GetSchema(schema);
  }

  static int CGetNext(ArrowArrayStream* stream, ArrowArray* array) {
    return reinterpret_cast<Stream*>(stream->private_data)->GetNext(array);
  }

  static const char* CGetLastError(ArrowArrayStream* stream) {
    return reinterpret_cast<Stream*>(stream->private_data)->GetLastError();
  }

  static void CRelease(ArrowArrayStream* stream) {
    delete reinterpret_cast<Stream*>(stream->private_data);
    stream->release = nullptr;
    stream->private_data = nullptr;
  }
};

using ConnectionStream = Stream<std::shared_ptr<internal::BaseConnection>>;
using StatementStream = Stream<std::shared_ptr<internal::BaseStatement>>;

class Statement {
 public:
  Statement(std::shared_ptr<internal::BaseStatement> base) : base_(base) {}

  Status SetSqlQuery(const std::string& query) {
    return base_->SetSqlQuery(query.c_str());
  }

  Result<StatementStream> ExecuteQuery() {
    StatementStream out(base_);
    UNWRAP_STATUS(base_->ExecuteQuery(out.stream(), out.mutable_rows_affected()));
    return out;
  }

 private:
  std::shared_ptr<internal::BaseStatement> base_;
};

class Connection {
 public:
  Connection(std::shared_ptr<internal::BaseConnection> base) : base_(base) {}

  Result<Statement> NewStatement() {
    auto child = std::make_shared<internal::BaseStatement>();
    UNWRAP_STATUS(child->New(base_));
    return Statement(child);
  }

  Status Cancel() { return base_->Cancel(); }

  Result<ConnectionStream> GetInfo(const std::vector<uint32_t>& info_codes = {}) {
    ConnectionStream out(base_);
    UNWRAP_STATUS(base_->GetInfo(info_codes.data(), info_codes.size(), out.stream()));
    return out;
  }

 private:
  std::shared_ptr<internal::BaseConnection> base_;
};

class Database {
 public:
  Database(std::shared_ptr<internal::BaseDatabase> base) : base_(base) {}

  Result<Connection> NewConnection() {
    auto child = std::make_shared<internal::BaseConnection>();
    UNWRAP_STATUS(child->New(base_));
    UNWRAP_STATUS(child->Init());
    return Connection(child);
  }

 private:
  std::shared_ptr<internal::BaseDatabase> base_;
};

class Driver {
  Driver() : base_(std::make_shared<internal::BaseDriver>()) {}

  Status Init(AdbcDriverInitFunc init_func) { return base_->Init(init_func); }

  Result<Database> NewDatabase() {
    auto child = std::make_shared<internal::BaseDatabase>();
    UNWRAP_STATUS(child->New(base_));
    UNWRAP_STATUS(child->Init());
    return Database(child);
  }

 private:
  std::shared_ptr<internal::BaseDriver> base_;
};

}  // namespace adbc::client