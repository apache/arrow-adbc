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

#define ADBC_FRAMEWORK_USE_FMT
#include "driver/framework/base_driver.h"
#include "driver/framework/statement.h"
#include "driver/framework/status.h"

namespace adbc::db2 {

/// \brief ADBC statement for IBM Db2.
///
/// Statement execution is not implemented in this initial driver
/// scope.  All execute/prepare/bind paths return
/// ADBC_STATUS_NOT_IMPLEMENTED via the framework defaults; this stub
/// exists only so that the driver instantiates a complete
/// adbc::driver::Driver template.
class Db2Statement : public driver::Statement<Db2Statement> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[DB2]";

  Db2Statement() = default;
  ~Db2Statement() = default;
};

}  // namespace adbc::db2
