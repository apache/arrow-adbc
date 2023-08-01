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

#include <mutex>
#include <string>
#include <utility>
#include <vector>

#include <adbc.h>
#include <libpq-fe.h>

namespace adbcpq {

/// \brief Extract ADBC error information from a PGresult.
void SetErrorPgResult(struct AdbcError* error, PGresult* result);

/// \brief Translate an ADBC status code to an errno (for use
/// implementing ArrowArrayStream).
int AdbcStatusCodeToErrno(AdbcStatusCode code);

// The printf checking attribute doesn't work properly on gcc 4.8
// and results in spurious compiler warnings
#if defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 5)
#define ADBC_CHECK_PRINTF_ATTRIBUTE(x, y) __attribute__((format(printf, x, y)))
#else
#define ADBC_CHECK_PRINTF_ATTRIBUTE(x, y)
#endif

/// \brief Track state needed to support the error_details ADBC feature.
class ErrorDetailsState {
 public:
  /// \brief Set an error on an AdbcError, and clear the current error state.
  void SetError(struct AdbcError* error, const char* format, ...)
      ADBC_CHECK_PRINTF_ATTRIBUTE(3, 4);

  /// \brief Set an error on an AdbcError, and reinitialize the
  ///   current error state based on the PostgreSQL error metadata.
  AdbcStatusCode SetError(struct AdbcError* error, PGresult* result, const char* format,
                          ...) ADBC_CHECK_PRINTF_ATTRIBUTE(4, 5);

  /// \brief Explicitly insert an error detail.
  void SetDetail(std::string key, std::string value);

  /// \brief Explicitly insert an error detail.
  AdbcStatusCode SetDetail(struct AdbcError* error, PGresult* result);

  /// \brief Clear the current error state.
  void Clear();

  /// \brief Implement the ADBC error_details feature.
  bool GetOption(const char* key, std::string* value);

  /// \brief Implement the ADBC error_details feature.
  bool GetOptionBytes(const char* key, std::string* value);

  /// \brief Implement the ADBC error_details feature.
  bool GetOptionInt(const char* key, int64_t* value);

 private:
  std::mutex mutex_;
  std::vector<std::pair<std::string, std::string>> details_;
};

#undef ADBC_CHECK_PRINTF_ATTRIBUTE

}
