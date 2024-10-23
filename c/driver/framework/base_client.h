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

#include <iostream>
#include <memory>

#include <arrow-adbc/adbc.h>

#include "driver/framework/status.h"

namespace adbc::client {

class BaseDriver;
class BaseDatabase;
class BaseConnection;
class BaseStatement;

using adbc::driver::Result;
using adbc::driver::Status;

using SharedDriver = std::shared_ptr<BaseDriver>;
using SharedDatabase = std::shared_ptr<BaseDatabase>;
using SharedConnection = std::shared_ptr<BaseConnection>;
using SharedStatement = std::shared_ptr<BaseStatement>;

class Context {
 public:
  enum class LogLevel { kInfo, kWarn };

  virtual void OnUnreleasableStatement(SharedConnection connection,
                                       AdbcStatement* statement, const AdbcError* error) {
    Log(LogLevel::kWarn, "leaking unreleasable statement");
  }

  virtual void OnUnreleasableConnection(SharedDatabase database,
                                        AdbcConnection* connection,
                                        const AdbcError* error) {
    Log(LogLevel::kWarn, "leaking unreleasable connection");
  }

  virtual void OnUnreleaseableDatabase(SharedDriver driver, AdbcDatabase* database,
                                       const AdbcError* error) {
    Log(LogLevel::kWarn, "leaking unreleasable database");
  }

  virtual void OnUnreleaseableDriver(AdbcDriver* driver, const AdbcError* error) {
    Log(LogLevel::kWarn, "leaking unreleasable driver");
  }

  virtual void Log(LogLevel level, std::string_view message) {}

  static std::shared_ptr<Context> Default() { return std::make_unique<Context>(); }
};

using SharedContext = std::shared_ptr<Context>;

class BaseDriver {
 public:
  BaseDriver(SharedContext context = std::make_unique<Context>()) : context_(context) {}
  Status Init(AdbcDriverInitFunc init_func) {
    return Status::FromAdbc(init_func(ADBC_VERSION_1_1_0, &driver_, &error_), error_);
  }

  AdbcDriver* driver() { return &driver_; }

  Context* context() { return context_.get(); }

 private:
  SharedContext context_;
  AdbcDriver driver_{};
  AdbcError error_{ADBC_ERROR_INIT};
};

class BaseObject {
 public:
  const SharedDriver& GetSharedDriver() { return driver_; }

 protected:
  SharedDriver driver_;
  AdbcError error_{ADBC_ERROR_INIT};

  void NewBase(SharedDriver driver) { driver_ = driver; }
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

  Status New(SharedDriver parent) {
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

  Status New(SharedDatabase database) {
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

  Status Cancel(const std::string& query) {
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
  SharedDatabase database_;
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

  Status New(SharedConnection connection) {
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

  Status SetSqlQuery(const std::string& query) {
    UNWRAP_STATUS(CheckValid());
    AdbcStatusCode code = WRAP_CALL(StatementSetSqlQuery, query.c_str());
    return Status::FromAdbc(code, error_);
  }

  Result<int64_t> ExecuteQuery(ArrowArrayStream* stream) {
    UNWRAP_STATUS(CheckValid());
    int64_t affected_rows = -1;
    AdbcStatusCode code = WRAP_CALL(StatementExecuteQuery, stream, &affected_rows);
    return Status::FromAdbc(code, error_);
  }

 private:
  SharedConnection connection_;
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

}  // namespace adbc::client