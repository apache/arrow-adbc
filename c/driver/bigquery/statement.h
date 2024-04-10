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

#include <cinttypes>
#include <cstring>
#include <memory>
#include <optional>
#include <string>
#include <unordered_map>

#include <adbc.h>
#include <absl/types/optional.h>
#include "connection.h"

#include "common/utils.h"

namespace adbc_bigquery {
class BigqueryConnection;

class BigqueryStatement {
 public:
  BigqueryStatement() {}

  // ---------------------------------------------------------------------
  // ADBC API implementation

  AdbcStatusCode Bind(struct ArrowArray* values, struct ArrowSchema* schema,
                      struct AdbcError* error);
  AdbcStatusCode Bind(struct ArrowArrayStream* stream, struct AdbcError* error);
  AdbcStatusCode Cancel(struct AdbcError* error);
  AdbcStatusCode ExecuteQuery(struct ArrowArrayStream* stream, int64_t* rows_affected,
                              struct AdbcError* error);
  AdbcStatusCode ExecuteSchema(struct ArrowSchema* schema, struct AdbcError* error);
  AdbcStatusCode GetOption(const char* key, char* value, size_t* length,
                           struct AdbcError* error);
  AdbcStatusCode GetOptionBytes(const char* key, uint8_t* value, size_t* length,
                                struct AdbcError* error);
  AdbcStatusCode GetOptionDouble(const char* key, double* value, struct AdbcError* error);
  AdbcStatusCode GetOptionInt(const char* key, int64_t* value, struct AdbcError* error);
  AdbcStatusCode GetParameterSchema(struct ArrowSchema* schema, struct AdbcError* error);
  AdbcStatusCode New(struct AdbcConnection* connection, struct AdbcError* error);
  AdbcStatusCode Prepare(struct AdbcError* error);
  AdbcStatusCode Release(struct AdbcError* error);
  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error);
  AdbcStatusCode SetOptionBytes(const char* key, const uint8_t* value, size_t length,
                                struct AdbcError* error);
  AdbcStatusCode SetOptionDouble(const char* key, double value, struct AdbcError* error);
  AdbcStatusCode SetOptionInt(const char* key, int64_t value, struct AdbcError* error);
  AdbcStatusCode SetSqlQuery(const char* query, struct AdbcError* error);

  template <typename T>
  void GetAndAssignQueryRequestOption(const char* key, absl::optional<T>& assign_to) {
    auto value = GetQueryRequestOption<T>(key);
    if (value) {
      assign_to = *value;
    }
  }

  template <typename T = std::string>
  auto GetQueryRequestOption(const char* key) -> std::optional<T> {
    auto iter = options_.find(key);
    if (iter == options_.end()) {
      return {};
    } else {
      return handleOptionValue<T>(iter->second);
    }
  }

  template <typename T>
  auto GetQueryRequestOption(const char* key, T default_value) -> T {
    auto iter = options_.find(key);
    if (iter == options_.end()) {
      return default_value;
    } else {
      return handleOptionValue<T>(iter->second);
    }
  }

  template <typename T>
  auto handleOptionValue(const std::string& value) -> T {
    return T{};
  }

  template <>
  auto handleOptionValue(const std::string& value) -> std::string {
    return value;
  }

  template <>
  auto handleOptionValue<bool>(const std::string& value) -> bool {
    if (value == "true") {
      return true;
    } else if (value == "false") {
      return false;
    } else {
      return false;
    }
  }

  template <>
  auto handleOptionValue<std::uint32_t>(const std::string& value) -> std::uint32_t {
    return strtoul(value.c_str(), nullptr, 10);
  }

  template <>
  auto handleOptionValue<std::int64_t>(const std::string& value) -> std::int64_t {
    return strtoll(value.c_str(), nullptr, 10);
  }

 private:
  std::shared_ptr<BigqueryConnection> connection_;
  std::string sql_;
  std::unordered_map<std::string, std::string> options_;
};
}  // namespace adbc_bigquery
