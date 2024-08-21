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

#define R_NO_REMAP
#include <R.h>
#include <Rinternals.h>

#include <cstring>
#include <unordered_map>

#include "driver/framework/base_driver.h"

#include "arrow-adbc/adbc.h"

using adbc::driver::Option;
using adbc::driver::Result;
using adbc::driver::Status;

class VoidDatabase : public adbc::driver::BaseDatabase<VoidDatabase> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[void]";

  Status SetOptionImpl(std::string_view key, Option value) override {
    options_[std::string(key)] = value;
    return adbc::driver::status::Ok();
  }

  Result<Option> GetOption(std::string_view key) override {
    auto result = options_.find(std::string(key));
    if (result == options_.end()) {
      Status out(ADBC_STATUS_NOT_FOUND, "option not found");
      out.AddDetail("r.driver_test.option_key", std::string(key));
      return out;
    } else {
      return result->second;
    }
  }

 private:
  std::unordered_map<std::string, Option> options_;
};

class VoidConnection : public adbc::driver::BaseConnection<VoidConnection> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[void]";

  Status SetOptionImpl(std::string_view key, Option value) override {
    options_[std::string(key)] = value;
    return adbc::driver::status::Ok();
  }

  Result<Option> GetOption(std::string_view key) override {
    auto result = options_.find(std::string(key));
    if (result == options_.end()) {
      Status out(ADBC_STATUS_NOT_FOUND, "option not found");
      out.AddDetail("r.driver_test.option_key", std::string(key));
      return out;
    } else {
      return result->second;
    }
  }

 private:
  std::unordered_map<std::string, Option> options_;
};

class VoidStatement : public adbc::driver::BaseStatement<VoidStatement> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[void]";

  Status SetOptionImpl(std::string_view key, Option value) override {
    options_[std::string(key)] = value;
    return adbc::driver::status::Ok();
  }

  Result<Option> GetOption(std::string_view key) override {
    auto result = options_.find(std::string(key));
    if (result == options_.end()) {
      Status out(ADBC_STATUS_NOT_FOUND, "option not found");
      out.AddDetail("r.driver_test.option_key", std::string(key));
      return out;
    } else {
      return result->second;
    }
  }

 private:
  std::unordered_map<std::string, Option> options_;
};

static AdbcStatusCode VoidDriverInitFunc(int version, void* raw_driver,
                                         AdbcError* error) {
  using VoidDriver = adbc::driver::Driver<VoidDatabase, VoidConnection, VoidStatement>;
  return VoidDriver::Init(version, raw_driver, error);
}

extern "C" SEXP RAdbcVoidDriverInitFunc(void) {
  SEXP xptr =
      PROTECT(R_MakeExternalPtrFn((DL_FUNC)VoidDriverInitFunc, R_NilValue, R_NilValue));
  Rf_setAttrib(xptr, R_ClassSymbol, Rf_mkString("adbc_driver_init_func"));
  UNPROTECT(1);
  return xptr;
}

class MonkeyStatement : public adbc::driver::BaseStatement<MonkeyStatement> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[monkey]";

  MonkeyStatement() { stream_.release = nullptr; }

  ~MonkeyStatement() {
    if (stream_.release != nullptr) {
      stream_.release(&stream_);
    }
  }

  Status BindStreamImpl(ArrowArrayStream* stream) {
    if (stream_.release != nullptr) {
      stream_.release(&stream_);
    }

    std::memcpy(&stream_, stream, sizeof(ArrowArrayStream));
    stream->release = nullptr;
    return adbc::driver::status::Ok();
  }

  Result<int64_t> ExecuteQueryImpl(ArrowArrayStream* stream) {
    if (stream != nullptr) {
      std::memcpy(stream, &stream_, sizeof(ArrowArrayStream));
      stream_.release = nullptr;
    }

    return -1;
  }

 private:
  ArrowArrayStream stream_;
};

using MonkeyDriver = adbc::driver::Driver<VoidDatabase, VoidConnection, MonkeyStatement>;

static AdbcStatusCode MonkeyDriverInitFunc(int version, void* raw_driver,
                                           AdbcError* error) {
  return MonkeyDriver::Init(version, raw_driver, error);
}

extern "C" SEXP RAdbcMonkeyDriverInitFunc(void) {
  SEXP xptr =
      PROTECT(R_MakeExternalPtrFn((DL_FUNC)MonkeyDriverInitFunc, R_NilValue, R_NilValue));
  Rf_setAttrib(xptr, R_ClassSymbol, Rf_mkString("adbc_driver_init_func"));
  UNPROTECT(1);
  return xptr;
}

class LogDatabase : public adbc::driver::BaseDatabase<LogDatabase> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[log]";

  LogDatabase() { Rprintf("LogDatabaseNew()\n"); }

  ~LogDatabase() { Rprintf("LogDatabaseRelease()\n"); }

  AdbcStatusCode Init(void* parent, AdbcError* error) override {
    Rprintf("LogDatabaseInit()\n");
    return Base::Init(parent, error);
  }

  Status SetOptionImpl(std::string_view key, Option value) override {
    Rprintf("LogDatabaseSetOption()\n");
    return adbc::driver::status::Ok();
  }

  Result<Option> GetOption(std::string_view key) override {
    Rprintf("LogDatabaseGetOption()\n");
    return Base::GetOption(key);
  }
};

class LogConnection : public adbc::driver::BaseConnection<LogConnection> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[log]";

  LogConnection() { Rprintf("LogConnectionNew()\n"); }

  ~LogConnection() { Rprintf("LogConnectionRelease()\n"); }

  AdbcStatusCode Init(void* parent, AdbcError* error) override {
    Rprintf("LogConnectionInit()\n");
    return Base::Init(parent, error);
  }

  Status SetOptionImpl(std::string_view key, Option value) override {
    Rprintf("LogConnectionSetOption()\n");
    return adbc::driver::status::Ok();
  }

  AdbcStatusCode Commit(AdbcError* error) {
    Rprintf("LogConnectionCommit()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetInfo(const uint32_t* info_codes, size_t info_codes_length,
                         ArrowArrayStream* out, AdbcError* error) {
    Rprintf("LogConnectionGetInfo()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetObjects(int depth, const char* catalog, const char* db_schema,
                            const char* table_name, const char** table_type,
                            const char* column_name, ArrowArrayStream* out,
                            AdbcError* error) {
    Rprintf("LogConnectionGetObjects()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetTableSchema(const char* catalog, const char* db_schema,
                                const char* table_name, ArrowSchema* schema,
                                AdbcError* error) {
    Rprintf("LogConnectionGetTableSchema()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetTableTypes(ArrowArrayStream* out, AdbcError* error) {
    Rprintf("LogConnectionGetTableTypes()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode ReadPartition(const uint8_t* serialized_partition,
                               size_t serialized_length, ArrowArrayStream* out,
                               AdbcError* error) {
    Rprintf("LogConnectionReadPartition()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode Rollback(AdbcError* error) {
    Rprintf("LogConnectionRollback()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode Cancel(AdbcError* error) {
    Rprintf("LogConnectionCancel()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetStatistics(const char* catalog, const char* db_schema,
                               const char* table_name, char approximate,
                               ArrowArrayStream* out, AdbcError* error) {
    Rprintf("LogConnectionGetStatistics()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode GetStatisticNames(ArrowArrayStream* out, AdbcError* error) {
    Rprintf("LogConnectionGetStatisticNames()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
};

class LogStatement : public adbc::driver::BaseStatement<LogStatement> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[log]";

  ~LogStatement() { Rprintf("LogStatementRelease()\n"); }

  AdbcStatusCode Init(void* parent, AdbcError* error) override {
    Rprintf("LogStatementNew()\n");
    return Base::Init(parent, error);
  }

  Status SetOptionImpl(std::string_view key, Option value) override {
    Rprintf("LogStatementSetOption()\n");
    return adbc::driver::status::Ok();
  }

  AdbcStatusCode ExecuteQuery(ArrowArrayStream* stream, int64_t* rows_affected,
                              AdbcError* error) {
    Rprintf("LogStatementExecuteQuery()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode ExecuteSchema(ArrowSchema* schema, AdbcError* error) {
    Rprintf("LogStatementExecuteSchema()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode Prepare(AdbcError* error) {
    Rprintf("LogStatementPrepare()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode SetSqlQuery(const char* query, AdbcError* error) {
    Rprintf("LogStatementSetSqlQuery()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode SetSubstraitPlan(const uint8_t* plan, size_t length, AdbcError* error) {
    Rprintf("LogStatementSetSubstraitPlan()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode Bind(ArrowArray* values, ArrowSchema* schema, AdbcError* error) {
    Rprintf("LogStatementBind()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode BindStream(ArrowArrayStream* stream, AdbcError* error) {
    Rprintf("LogStatementBindStream()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  AdbcStatusCode Cancel(AdbcError* error) {
    Rprintf("LogStatementCancel()\n");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
};

using LogDriver = adbc::driver::Driver<LogDatabase, LogConnection, LogStatement>;

static AdbcStatusCode LogDriverInitFunc(int version, void* raw_driver, AdbcError* error) {
  return LogDriver::Init(version, raw_driver, error);
}

extern "C" SEXP RAdbcLogDriverInitFunc(void) {
  SEXP xptr =
      PROTECT(R_MakeExternalPtrFn((DL_FUNC)LogDriverInitFunc, R_NilValue, R_NilValue));
  Rf_setAttrib(xptr, R_ClassSymbol, Rf_mkString("adbc_driver_init_func"));
  UNPROTECT(1);
  return xptr;
}
