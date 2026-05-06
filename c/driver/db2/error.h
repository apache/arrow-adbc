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

#include <string>

#include "db2_odbc.h"

#include "driver/framework/status.h"

namespace adbc::db2 {

using driver::Status;

/// Extract all diagnostic records from a DB2 CLI handle and produce a
/// Status.  |context| is prepended to the message for caller
/// identification (e.g. "[DB2] SQLDriverConnect").
Status Db2Error(SQLSMALLINT handle_type, SQLHANDLE handle, const char* context);

/// Convenience: check a SQLRETURN and return Status::Ok() on success,
/// or call Db2Error on failure.
Status CheckRc(SQLSMALLINT handle_type, SQLHANDLE handle, SQLRETURN rc,
               const char* context);

/// Map a 5-character SQLSTATE to the most appropriate AdbcStatusCode.
AdbcStatusCode SqlStateToAdbcStatus(const char* sqlstate);

}  // namespace adbc::db2
