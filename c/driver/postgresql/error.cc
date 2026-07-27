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

#include <stdarg.h>
#include <cstdio>
#include <cstring>
#include <string>
#include <string_view>
#include <vector>

#include <libpq-fe.h>

#include "driver/common/utils.h"

namespace adbcpq {

AdbcStatusCode ClassifySqlState(const char* sqlstate) {
  if (!sqlstate) {
    return ADBC_STATUS_IO;
  }

  const std::string_view state(sqlstate);

  // https://www.postgresql.org/docs/current/errcodes-appendix.html
  // Exact matches must precede class matches since PostgreSQL's classes contain
  // errors with distinct ADBC meanings.
  if (state == "57014") {
    return ADBC_STATUS_CANCELLED;
  }
  if (state == "0A000") {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  if (state == "42501") {
    return ADBC_STATUS_UNAUTHORIZED;
  }

  if (state == "42P01" || state == "42P02" || state == "42703" || state == "42883" ||
      state == "42704" || state == "42602" || state == "34000" || state == "3D000" ||
      state == "3F000" || state == "26000" || state == "58P01") {
    return ADBC_STATUS_NOT_FOUND;
  }

  if (state == "42701" || state == "42P03" || state == "42P04" || state == "42P05" ||
      state == "42P06" || state == "42P07" || state == "42712" || state == "42723" ||
      state == "42710" || state == "58P02") {
    return ADBC_STATUS_ALREADY_EXISTS;
  }

  const auto class_code = state.substr(0, 2);
  if (class_code == "22") {
    return ADBC_STATUS_INVALID_DATA;
  }
  if (class_code == "23") {
    return ADBC_STATUS_INTEGRITY;
  }
  if (class_code == "28") {
    return ADBC_STATUS_UNAUTHENTICATED;
  }
  if (class_code == "25" || class_code == "2D" || class_code == "3B" ||
      class_code == "55") {
    return ADBC_STATUS_INVALID_STATE;
  }
  if (class_code == "XX") {
    return ADBC_STATUS_INTERNAL;
  }
  if (class_code == "42") {
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  return ADBC_STATUS_IO;
}

AdbcStatusCode SetError(struct AdbcError* error, PGresult* result, const char* format,
                        ...) {
  if (error && error->release) {
    // TODO: combine the errors if possible
    error->release(error);
  }

  va_list args;
  va_start(args, format);
  std::string message;
  message.resize(1024);
  int chars_needed = vsnprintf(message.data(), message.size(), format, args);
  va_end(args);

  if (chars_needed > 0) {
    message.resize(chars_needed);
  } else {
    message.resize(0);
  }

  return MakeStatus(result, "{}", message).ToAdbc(error);
}

}  // namespace adbcpq
