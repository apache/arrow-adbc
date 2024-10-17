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

#include <arrow-adbc/adbc.h>

#include "driver/framework/status.h"

namespace adbc::client {

class BaseDriver;
class BaseDatabase;
class BaseConnection;
class BaseStatement;

using adbc::driver::Result;
using adbc::driver::Status;

#define WRAP_CALL(func, ...) (driver_->func(&statement_, __VA_ARGS__, &error_))
#define WRAP_CALL0(func) (driver_->func(&statement_, &error_))

class BaseObject {
 protected:
  BaseObject(AdbcDriver* driver) : driver_(driver) {}
  AdbcDriver* driver_{nullptr};
  AdbcError error_{ADBC_ERROR_INIT};
};

class BaseStatement : BaseObject {
  friend class BaseConnection;

 public:
  BaseStatement(AdbcDriver* driver) : BaseObject(driver) {}
  ~BaseStatement() {
    if (!statement_.private_data) {
      return;
    }

    AdbcStatusCode code = WRAP_CALL0(StatementRelease);
    if (code != ADBC_STATUS_OK) {
      // TODO: Register with connection or context
    }
  }

  Status SetSqlQuery(const std::string& query) {
    AdbcStatusCode code = WRAP_CALL(StatementSetSqlQuery, query.c_str());
    return Status::FromAdbc(code, error_);
  }

  Result<int64_t> ExecuteQuery(ArrowArrayStream* stream) {
    int64_t affected_rows = -1;
    AdbcStatusCode code = WRAP_CALL(StatementExecuteQuery, stream, &affected_rows);
    return Status::FromAdbc(code, error_);
  }

 private:
  AdbcStatement statement_;
};

#undef WRAP_CALL
#undef WRAP_CALL0
#define WRAP_CALL(func, ...) (driver_->func(&connection_, __VA_ARGS__, &error_))
#define WRAP_CALL0(func) (driver_->func(&connection_, &error_))

class BaseConnection : BaseObject {
 public:
  BaseConnection(AdbcDriver* driver) : BaseObject(driver) {}
  ~BaseConnection() {
    AdbcStatusCode code = driver_->ConnectionRelease(&connection_, &error_);
    if (code != ADBC_STATUS_OK) {
      // TODO: Register with database or context
    }
  }

  Result<BaseStatement> NewStatement() {
    BaseStatement out(driver_);
    AdbcStatusCode code = driver_->StatementNew(&connection_, &out.statement_, &error_);
    if (code != ADBC_STATUS_OK) {
      return Status::FromAdbc(code, error_);
    }

    return out;
  }

  Status Cancel(const std::string& query) {
    AdbcStatusCode code = WRAP_CALL0(ConnectionCancel);
    return Status::FromAdbc(code, error_);
  }

  Status GetInfo(const uint32_t* info_codes, size_t n_info_codes, ArrowArrayStream* out) {
    AdbcStatusCode code = WRAP_CALL(ConnectionGetInfo, info_codes, n_info_codes, out);
    return Status::FromAdbc(code, error_);
  }

 private:
  AdbcConnection connection_;
};

#undef WRAP_CALL
#undef WRAP_CALL0

class BaseDriver {
 public:
  Status Init(AdbcDriverInitFunc init_func) {
    return Status::FromAdbc(init_func(ADBC_VERSION_1_1_0, &driver_, &error_), error_);
  }

 private:
  AdbcDriver driver_{};
  AdbcError error_{ADBC_ERROR_INIT};
};

}  // namespace adbc::client