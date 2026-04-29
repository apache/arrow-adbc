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

#include <string_view>

#include <arrow-adbc/adbc.h>

#include "db2_odbc.h"

#define ADBC_FRAMEWORK_USE_FMT
#include "driver/framework/connection.h"
#include "driver/framework/status.h"

#include "database.h"

namespace adbc::db2 {

using driver::Option;
using driver::Result;
using driver::Status;
namespace status = adbc::driver::status;

/// \brief ADBC connection for IBM Db2.
///
/// Connection lifecycle wraps a single SQLHDBC obtained from the
/// owning Db2Database.  Statement execution, metadata retrieval, and
/// transaction management are intentionally not implemented in this
/// initial driver scope and will be added in subsequent pull requests
/// (the framework default returns ADBC_STATUS_NOT_IMPLEMENTED).
class Db2Connection : public driver::Connection<Db2Connection> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[DB2]";

  Db2Connection() = default;
  ~Db2Connection() = default;

  Status InitImpl(void* parent);
  Status ReleaseImpl();

 private:
  Db2Database* database_ = nullptr;
  SQLHDBC hdbc_ = SQL_NULL_HDBC;
};

}  // namespace adbc::db2
