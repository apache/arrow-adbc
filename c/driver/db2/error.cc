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

#include "error.h"

#include <cstring>
#include <sstream>
#include <string>

namespace adbc::db2 {

AdbcStatusCode SqlStateToAdbcStatus(const char* sqlstate) {
  if (!sqlstate || sqlstate[0] == '\0') return ADBC_STATUS_IO;

  // Class 00 -- success
  if (std::strncmp(sqlstate, "00", 2) == 0) return ADBC_STATUS_OK;

  // Class 08 -- connection exception
  if (std::strncmp(sqlstate, "08", 2) == 0) return ADBC_STATUS_IO;

  // Class 22 -- data exception
  if (std::strncmp(sqlstate, "22", 2) == 0) return ADBC_STATUS_INVALID_ARGUMENT;

  // Class 23 -- constraint violation
  if (std::strncmp(sqlstate, "23", 2) == 0) return ADBC_STATUS_ALREADY_EXISTS;

  // Class 28 -- authorization
  if (std::strncmp(sqlstate, "28", 2) == 0) return ADBC_STATUS_UNAUTHENTICATED;

  // Class 42 -- syntax / access rule violation
  if (std::strncmp(sqlstate, "42", 2) == 0) {
    if (std::strcmp(sqlstate, "42S02") == 0) return ADBC_STATUS_NOT_FOUND;
    if (std::strcmp(sqlstate, "42704") == 0) return ADBC_STATUS_NOT_FOUND;
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  // Class HY -- CLI-specific errors
  if (std::strncmp(sqlstate, "HY", 2) == 0) {
    if (std::strcmp(sqlstate, "HY001") == 0) return ADBC_STATUS_INTERNAL;
    if (std::strcmp(sqlstate, "HY008") == 0) return ADBC_STATUS_CANCELLED;
    if (std::strcmp(sqlstate, "HY010") == 0) return ADBC_STATUS_INVALID_STATE;
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  // Class IM -- driver manager
  if (std::strncmp(sqlstate, "IM", 2) == 0) return ADBC_STATUS_INTERNAL;

  return ADBC_STATUS_IO;
}

Status Db2Error(SQLSMALLINT handle_type, SQLHANDLE handle, const char* context) {
  std::ostringstream oss;
  oss << "[DB2] " << context;

  SQLCHAR sqlstate[6];
  SQLINTEGER native_error = 0;
  SQLCHAR msg_buf[SQL_MAX_MESSAGE_LENGTH + 1];
  SQLSMALLINT msg_len = 0;
  SQLSMALLINT rec = 1;
  AdbcStatusCode adbc_code = ADBC_STATUS_IO;

  std::string first_sqlstate;
  SQLINTEGER first_native_error = 0;

  while (SQLGetDiagRec(handle_type, handle, rec, sqlstate, &native_error, msg_buf,
                       sizeof(msg_buf), &msg_len) == SQL_SUCCESS) {
    sqlstate[5] = '\0';
    oss << " [" << reinterpret_cast<const char*>(sqlstate) << "] "
        << reinterpret_cast<const char*>(msg_buf) << " (native error " << native_error
        << ")";

    if (rec == 1) {
      first_sqlstate.assign(reinterpret_cast<const char*>(sqlstate), 5);
      first_native_error = native_error;
      adbc_code = SqlStateToAdbcStatus(first_sqlstate.c_str());
    }

    rec++;
  }

  if (rec == 1) {
    oss << ": (no diagnostic records available)";
  }

  Status status(adbc_code, oss.str());
  if (!first_sqlstate.empty()) {
    status.SetSqlState(first_sqlstate);
    status.AddDetail("db2.sqlstate", first_sqlstate);
    status.AddDetail("db2.native_error", std::to_string(first_native_error));
  }
  return status;
}

Status CheckRc(SQLSMALLINT handle_type, SQLHANDLE handle, SQLRETURN rc,
               const char* context) {
  if (rc == SQL_SUCCESS) return Status::Ok();
  if (rc == SQL_SUCCESS_WITH_INFO) return Status::Ok();
  if (rc == SQL_NO_DATA) return Status::Ok();
  return Db2Error(handle_type, handle, context);
}

}  // namespace adbc::db2
