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

// Error handling utilities.

#pragma once

#include <cstring>
#include <string>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <libpq-fe.h>

#include <fmt/core.h>

#include "driver/framework/status.h"

using adbc::driver::Status;

namespace adbcpq {

struct DetailField {
  int code;
  std::string key;
};

static const std::vector<DetailField> kDetailFields = {
    {PG_DIAG_COLUMN_NAME, "PG_DIAG_COLUMN_NAME"},
    {PG_DIAG_CONTEXT, "PG_DIAG_CONTEXT"},
    {PG_DIAG_CONSTRAINT_NAME, "PG_DIAG_CONSTRAINT_NAME"},
    {PG_DIAG_DATATYPE_NAME, "PG_DIAG_DATATYPE_NAME"},
    {PG_DIAG_INTERNAL_POSITION, "PG_DIAG_INTERNAL_POSITION"},
    {PG_DIAG_INTERNAL_QUERY, "PG_DIAG_INTERNAL_QUERY"},
    {PG_DIAG_MESSAGE_PRIMARY, "PG_DIAG_MESSAGE_PRIMARY"},
    {PG_DIAG_MESSAGE_DETAIL, "PG_DIAG_MESSAGE_DETAIL"},
    {PG_DIAG_MESSAGE_HINT, "PG_DIAG_MESSAGE_HINT"},
    {PG_DIAG_SEVERITY_NONLOCALIZED, "PG_DIAG_SEVERITY_NONLOCALIZED"},
    {PG_DIAG_SQLSTATE, "PG_DIAG_SQLSTATE"},
    {PG_DIAG_STATEMENT_POSITION, "PG_DIAG_STATEMENT_POSITION"},
    {PG_DIAG_SCHEMA_NAME, "PG_DIAG_SCHEMA_NAME"},
    {PG_DIAG_TABLE_NAME, "PG_DIAG_TABLE_NAME"},
};

AdbcStatusCode ClassifySqlState(const char* sqlstate);

template <typename... Args>
Status MakeStatus(PGresult* result, const char* format_string, Args&&... args) {
  auto message = ::fmt::vformat(format_string, ::fmt::make_format_args(args...));

  AdbcStatusCode code = ADBC_STATUS_IO;
  char sqlstate_out[5];
  std::memset(sqlstate_out, 0, sizeof(sqlstate_out));

  if (result == nullptr) {
    return Status(code, message);
  }

  const char* sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
  if (sqlstate) {
    code = ClassifySqlState(sqlstate);
  }

  Status status(code, message);
  if (sqlstate) {
    status.SetSqlState(sqlstate);
  }
  for (const auto& field : kDetailFields) {
    const char* value = PQresultErrorField(result, field.code);
    if (value) {
      status.AddDetail(field.key, value);
    }
  }

  return status;
}

}  // namespace adbcpq
