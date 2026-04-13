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

#include <mutex>
#include <string>

#include "db2_odbc.h"

#include <arrow-adbc/adbc.h>

#define ADBC_FRAMEWORK_USE_FMT
#include "driver/framework/database.h"
#include "driver/framework/status.h"

namespace adbc::db2 {

using driver::Option;
using driver::Result;
using driver::Status;
namespace status = adbc::driver::status;

class Db2Database : public driver::Database<Db2Database> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[DB2]";

  Db2Database() = default;
  ~Db2Database() = default;

  Status InitImpl() override;
  Status ReleaseImpl() override;
  Status SetOptionImpl(std::string_view key, Option value) override;

  SQLHENV henv() const { return henv_; }
  const std::string& connection_string() const { return conn_str_; }

  /// Allocate a new SQLHDBC from the environment handle.  Thread-safe.
  Result<SQLHDBC> AllocConnection();

  /// Free a connection handle.  Thread-safe.
  void FreeConnection(SQLHDBC hdbc);

 private:
  SQLHENV henv_ = SQL_NULL_HENV;
  std::string conn_str_;
  std::string database_;
  std::string hostname_;
  std::string port_;
  std::string uid_;
  std::string pwd_;
  std::mutex mu_;

  void BuildConnectionString();
};

}  // namespace adbc::db2
