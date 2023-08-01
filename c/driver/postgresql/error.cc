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

#include <iostream>

#include <adbc.h>
#include <sys/errno.h>
#include <cstring>

#include <common/utils.h>

namespace adbcpq {

void SetErrorPgResult(struct AdbcError* error, PGresult* result) {
  if (!error) return;
  const char* sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
  if (sqlstate) {
    std::strncpy(error->sqlstate, sqlstate, 5);
  }
}

int AdbcStatusCodeToErrno(AdbcStatusCode code) {
  switch (code) {
    case ADBC_STATUS_OK:
      return 0;
    case ADBC_STATUS_UNKNOWN:
      return EIO;
    case ADBC_STATUS_NOT_IMPLEMENTED:
      return ENOTSUP;
    case ADBC_STATUS_NOT_FOUND:
      return ENOENT;
    case ADBC_STATUS_ALREADY_EXISTS:
      return EEXIST;
    case ADBC_STATUS_INVALID_ARGUMENT:
    case ADBC_STATUS_INVALID_STATE:
      return EINVAL;
    case ADBC_STATUS_INVALID_DATA:
    case ADBC_STATUS_INTEGRITY:
    case ADBC_STATUS_INTERNAL:
    case ADBC_STATUS_IO:
      return EIO;
    case ADBC_STATUS_CANCELLED:
      return ECANCELED;
    case ADBC_STATUS_TIMEOUT:
      return ETIMEDOUT;
    case ADBC_STATUS_UNAUTHENTICATED:
      return EAUTH;
    case ADBC_STATUS_UNAUTHORIZED:
      return EACCES;
    default:
      return EIO;
  }
}

void ErrorDetailsState::SetError(struct AdbcError* error, const char* format, ...) {
  va_list args;
  va_start(args, format);
  ::SetErrorVariadic(error, format, args);
  va_end(args);

  std::lock_guard<std::mutex> guard(mutex_);
  details_.clear();
}

AdbcStatusCode ErrorDetailsState::SetError(struct AdbcError* error, PGresult* result, const char* format, ...) {
  va_list args;
  va_start(args, format);
  ::SetErrorVariadic(error, format, args);
  va_end(args);

  return SetDetail(error, result);
}

void ErrorDetailsState::SetDetail(std::string key, std::string value) {
  std::lock_guard<std::mutex> guard(mutex_);
  details_.emplace_back(std::move(key), std::move(value));
}

AdbcStatusCode ErrorDetailsState::SetDetail(struct AdbcError* error, PGresult* result) {
  std::lock_guard<std::mutex> guard(mutex_);
  const char* sqlstate = PQresultErrorField(result, PG_DIAG_SQLSTATE);
  if (error) {
    if (sqlstate) {
      static_assert(sizeof(error->sqlstate) == 5, "");
      std::strncpy(error->sqlstate, sqlstate, sizeof(error->sqlstate));
    }
  }

  AdbcStatusCode code = ADBC_STATUS_IO;
  if (sqlstate) {
    // Duplicate SQLSTATE since in the context of an ArrowArrayStream,
    // we have no way to return it through an AdbcError
    details_.emplace_back("SQLSTATE", sqlstate);

    // https://www.postgresql.org/docs/current/errcodes-appendix.html
    // We can extend this in the future
    if (std::strcmp(sqlstate, "57014") == 0) {
      code = ADBC_STATUS_CANCELLED;
    }
  }
  const char* primary = PQresultErrorField(result, PG_DIAG_MESSAGE_PRIMARY);
  if (primary) {
    details_.emplace_back("PG_DIAG_MESSAGE_PRIMARY", primary);
  }
  const char* detail = PQresultErrorField(result, PG_DIAG_MESSAGE_DETAIL);
  if  (detail) {
    details_.emplace_back("PG_DIAG_MESSAGE_DETAIL", detail);
  }
  const char* hint = PQresultErrorField(result, PG_DIAG_MESSAGE_HINT);
  if  (hint) {
    details_.emplace_back("PG_DIAG_MESSAGE_HINT", hint);
  }
  const char* context = PQresultErrorField(result, PG_DIAG_CONTEXT);
  if  (context) {
    details_.emplace_back("PG_DIAG_CONTEXT", context);
  }
  const char* source_file = PQresultErrorField(result, PG_DIAG_SOURCE_FILE);
  if  (source_file) {
    details_.emplace_back("PG_DIAG_SOURCE_FILE", source_file);
  }
  const char* source_line = PQresultErrorField(result, PG_DIAG_SOURCE_LINE);
  if  (source_line) {
    details_.emplace_back("PG_DIAG_SOURCE_LINE", source_line);
  }
  // There are many other fields in PQresultErrorField that we could extract
  return code;
}

void ErrorDetailsState::Clear() {
  std::lock_guard<std::mutex> guard(mutex_);
  details_.clear();
}

static const int kErrorDetailsPrefixLen = std::strlen(ADBC_OPTION_ERROR_DETAILS_PREFIX);

bool ErrorDetailsState::GetOption(const char* key, std::string* value) {
  if (std::strncmp(key, ADBC_OPTION_ERROR_DETAILS_PREFIX,
                   kErrorDetailsPrefixLen) == 0) {
    int64_t index = std::strtol(key + kErrorDetailsPrefixLen, nullptr, 10);
    if (errno != 0 || index < 0) return false;
    std::lock_guard<std::mutex> guard(mutex_);
    if (static_cast<size_t>(index) >= details_.size()) return false;

    *value = details_[index].first;
    return true;
  }
  return false;
}

bool ErrorDetailsState::GetOptionBytes(const char* key, std::string* value) {
  if (std::strncmp(key, ADBC_OPTION_ERROR_DETAILS_PREFIX,
                   kErrorDetailsPrefixLen) == 0) {
    int64_t index = std::strtol(key + kErrorDetailsPrefixLen, nullptr, 10);
    if (errno != 0 || index < 0) return false;
    std::lock_guard<std::mutex> guard(mutex_);
    if (static_cast<size_t>(index) >= details_.size()) return false;

    *value = details_[index].second;
    return true;
  }
  return false;
}

bool ErrorDetailsState::GetOptionInt(const char* key, int64_t* value) {
  if (std::strcmp(key, ADBC_OPTION_ERROR_DETAILS) == 0) {
    std::lock_guard<std::mutex> guard(mutex_);
    *value = static_cast<int64_t>(details_.size());
    return true;
  }
  return false;
}

}
