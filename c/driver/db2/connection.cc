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

#include "connection.h"

#include "error.h"

namespace adbc::db2 {

Status Db2Connection::InitImpl(void* parent) {
  database_ = reinterpret_cast<Db2Database*>(parent);

  UNWRAP_RESULT(hdbc_, database_->AllocConnection());

  // SQLDriverConnect mutates the input string; copy to a stable buffer.
  const std::string& conn_str = database_->connection_string();
  SQLCHAR out_str[1024];
  SQLSMALLINT out_len = 0;
  SQLRETURN rc = SQLDriverConnect(
      hdbc_, /*WindowHandle=*/nullptr,
      const_cast<SQLCHAR*>(reinterpret_cast<const SQLCHAR*>(conn_str.c_str())),
      static_cast<SQLSMALLINT>(conn_str.size()), out_str, sizeof(out_str), &out_len,
      SQL_DRIVER_NOPROMPT);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLDriverConnect"));

  // ADBC's contract is autocommit-on by default; set it explicitly so the
  // driver behavior is independent of the underlying CLI configuration.
  rc = SQLSetConnectAttr(hdbc_, SQL_ATTR_AUTOCOMMIT,
                         reinterpret_cast<SQLPOINTER>(SQL_AUTOCOMMIT_ON), 0);
  UNWRAP_STATUS(CheckRc(SQL_HANDLE_DBC, hdbc_, rc, "SQLSetConnectAttr(AUTOCOMMIT)"));

  return status::Ok();
}

Status Db2Connection::ReleaseImpl() {
  if (hdbc_ != SQL_NULL_HDBC) {
    SQLDisconnect(hdbc_);
    if (database_ != nullptr) {
      database_->FreeConnection(hdbc_);
    }
    hdbc_ = SQL_NULL_HDBC;
  }
  return status::Ok();
}

}  // namespace adbc::db2
