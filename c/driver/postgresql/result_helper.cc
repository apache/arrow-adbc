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

#include "result_helper.h"

#include "driver/common/utils.h"
#include "error.h"

namespace adbcpq {

PqResultHelper::~PqResultHelper() {
  if (result_ != nullptr) {
    PQclear(result_);
  }
}

AdbcStatusCode PqResultHelper::Prepare() {
  // TODO: make stmtName a unique identifier?
  PGresult* result =
      PQprepare(conn_, /*stmtName=*/"", query_.c_str(), param_values_.size(), NULL);
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    AdbcStatusCode code =
        SetError(error_, result, "[libpq] Failed to prepare query: %s\nQuery was:%s",
                 PQerrorMessage(conn_), query_.c_str());
    PQclear(result);
    return code;
  }

  PQclear(result);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PqResultHelper::Execute() {
  std::vector<const char*> param_c_strs;

  for (size_t index = 0; index < param_values_.size(); index++) {
    param_c_strs.push_back(param_values_[index].c_str());
  }

  result_ =
      PQexecPrepared(conn_, "", param_values_.size(), param_c_strs.data(), NULL, NULL, 0);

  ExecStatusType status = PQresultStatus(result_);
  if (status != PGRES_TUPLES_OK && status != PGRES_COMMAND_OK) {
    AdbcStatusCode error =
        SetError(error_, result_, "[libpq] Failed to execute query '%s': %s",
                 query_.c_str(), PQerrorMessage(conn_));
    return error;
  }

  return ADBC_STATUS_OK;
}

}  // namespace adbcpq
