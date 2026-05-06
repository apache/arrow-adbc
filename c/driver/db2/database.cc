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

#include <cctype>
#include <sstream>
#include <string>
#include <string_view>

#include <arrow-adbc/driver/db2.h>

#include "error.h"

namespace adbc::db2 {

bool Db2Database::IsValidPort(std::string_view port) const {
  if (port.empty()) return false;
  int value = 0;
  for (char c : port) {
    if (!std::isdigit(static_cast<unsigned char>(c))) return false;
    value = value * 10 + (c - '0');
    if (value > 65535) return false;
  }
  return value > 0;
}

void Db2Database::BuildConnectionString() {
  // If the user supplied a complete CLI/ODBC connection string via "uri",
  // honor it as-is and ignore the per-field options.
  if (!conn_str_.empty()) return;

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
        kErrorPrefix,
        " No connection string provided. Set the standard 'uri' option, "
        "or the individual options (",
        ADBC_DB2_OPTION_DATABASE, ", ", ADBC_DB2_OPTION_HOSTNAME, ", ",
        ADBC_DB2_OPTION_PORT, ", ", ADBC_DB2_OPTION_UID, ", ",
        ADBC_DB2_OPTION_PWD, ").");
  }

  SQLRETURN rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &henv_);
  if (rc != SQL_SUCCESS && rc != SQL_SUCCESS_WITH_INFO) {
    return status::Internal(kErrorPrefix,
                            " Failed to allocate ODBC environment handle (rc=", rc,
                            ")");
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
    std::string_view v;
    UNWRAP_RESULT(v, value.AsString());
    conn_str_.assign(v);
    return status::Ok();
  }
  if (key == ADBC_DB2_OPTION_DATABASE) {
    std::string_view v;
    UNWRAP_RESULT(v, value.AsString());
    database_.assign(v);
    return status::Ok();
  }
  if (key == ADBC_DB2_OPTION_HOSTNAME) {
    std::string_view v;
    UNWRAP_RESULT(v, value.AsString());
    hostname_.assign(v);
    return status::Ok();
  }
  if (key == ADBC_DB2_OPTION_PORT) {
    std::string_view v;
    UNWRAP_RESULT(v, value.AsString());
    if (!IsValidPort(v)) {
      return status::InvalidArgument(
          kErrorPrefix, " Invalid value for ", ADBC_DB2_OPTION_PORT,
          ". Expected an integer in range 1-65535.");
    }
    port_.assign(v);
    return status::Ok();
  }
  if (key == ADBC_DB2_OPTION_UID || key == "username") {
    std::string_view v;
    UNWRAP_RESULT(v, value.AsString());
    if (v.empty()) {
      return status::InvalidArgument(kErrorPrefix, " Empty username/UID is not allowed.");
    }
    uid_.assign(v);
    return status::Ok();
  }
  if (key == ADBC_DB2_OPTION_PWD || key == "password") {
    std::string_view v;
    UNWRAP_RESULT(v, value.AsString());
    if (v.empty()) {
      return status::InvalidArgument(kErrorPrefix, " Empty password/PWD is not allowed.");
    }
    pwd_.assign(v);
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
