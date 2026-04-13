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

#include "database.h"

#include <sstream>

#include "error.h"

namespace adbc::db2 {

void Db2Database::BuildConnectionString() {
  if (!conn_str_.empty()) return;  // user supplied a full URI

  std::ostringstream oss;
  if (!database_.empty()) oss << "DATABASE=" << database_ << ";";
  if (!hostname_.empty()) oss << "HOSTNAME=" << hostname_ << ";";
  if (!port_.empty()) oss << "PORT=" << port_ << ";";
  if (!hostname_.empty() || !port_.empty()) oss << "PROTOCOL=TCPIP;";
  if (!uid_.empty()) oss << "UID=" << uid_ << ";";
  if (!pwd_.empty()) oss << "PWD=" << pwd_ << ";";
  conn_str_ = oss.str();
}

Status Db2Database::InitImpl() {
  BuildConnectionString();

  if (conn_str_.empty()) {
    return status::InvalidArgument(
        "[DB2] No connection string provided. "
        "Set 'uri' or the individual options (adbc.db2.database, etc.).");
  }

  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv_);
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    return status::Internal("[DB2] Failed to allocate environment handle");
  }

  rc = SQLSetEnvAttr(henv_, SQL_ATTR_ODBC_VERSION,
                     reinterpret_cast<SQLPOINTER>(SQL_OV_ODBC3), 0);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_ENV, henv_, rc, "SQLSetEnvAttr(ODBC_VERSION)"));

  return status::Ok();
}

Status Db2Database::ReleaseImpl() {
  if (henv_ != SQL_NULL_HENV) {
    SQLFreeHandle(SQL_HANDLE_ENV, henv_);
    henv_ = SQL_NULL_HENV;
  }
  return Base::ReleaseImpl();
}

Status Db2Database::SetOptionImpl(std::string_view key, Option value) {
  if (key == "uri") {
    std::string_view uri;
    UNWRAP_RESULT(uri, value.AsString());
    conn_str_ = std::string(uri);
    return status::Ok();
  }

  if (key == "adbc.db2.database") {
    std::string_view v;
    UNWRAP_RESULT(v, value.AsString());
    database_ = std::string(v);
    return status::Ok();
  }
  if (key == "adbc.db2.hostname") {
    std::string_view v;
    UNWRAP_RESULT(v, value.AsString());
    hostname_ = std::string(v);
    return status::Ok();
  }
  if (key == "adbc.db2.port") {
    std::string_view v;
    UNWRAP_RESULT(v, value.AsString());
    port_ = std::string(v);
    return status::Ok();
  }
  if (key == "adbc.db2.uid" || key == "username") {
    std::string_view v;
    UNWRAP_RESULT(v, value.AsString());
    uid_ = std::string(v);
    return status::Ok();
  }
  if (key == "adbc.db2.pwd" || key == "password") {
    std::string_view v;
    UNWRAP_RESULT(v, value.AsString());
    pwd_ = std::string(v);
    return status::Ok();
  }

  return Base::SetOptionImpl(key, value);
}

Result<SQLHDBC> Db2Database::AllocConnection() {
  std::lock_guard<std::mutex> lock(mu_);

  SQLHDBC hdbc = SQL_NULL_HDBC;
  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_DBC, henv_, &hdbc);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_ENV, henv_, rc, "SQLAllocHandle(DBC)"));
  return hdbc;
}

void Db2Database::FreeConnection(SQLHDBC hdbc) {
  std::lock_guard<std::mutex> lock(mu_);
  if (hdbc != SQL_NULL_HDBC) {
    SQLFreeHandle(SQL_HANDLE_DBC, hdbc);
  }
}

}  // namespace adbc::db2
