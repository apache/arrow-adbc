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

#include <stdint.h>

#include <adbc.h>

#if defined(__GNUC__)
#define SET_ERROR_ATTRIBUTE __attribute__((format(printf, 2, 3)))
#else
#define SET_ERROR_ATTRIBUTE
#endif

/// Set error details using a format string.
void SetError(struct AdbcError* error, const char* format, ...) SET_ERROR_ATTRIBUTE;

#undef SET_ERROR_ATTRIBUTE

/// Wrap a single batch as a stream.
AdbcStatusCode BatchToArrayStream(struct ArrowArray* values, struct ArrowSchema* schema,
                                  struct ArrowArrayStream* stream,
                                  struct AdbcError* error);

struct StringBuilder {
  char* buffer;
  // Not including null terminator
  size_t size;
  size_t capacity;
};
void StringBuilderInit(struct StringBuilder* builder, size_t initial_size);
void StringBuilderAppend(struct StringBuilder* builder, const char* value);
void StringBuilderReset(struct StringBuilder* builder);

/// Check an NanoArrow status code.
#define CHECK_NA(CODE, EXPR, ERROR)                                                    \
  do {                                                                                 \
    ArrowErrorCode arrow_error_code = (EXPR);                                          \
    if (arrow_error_code != 0) {                                                       \
      SetError(ERROR, "%s failed: (%d) %s\nDetail: %s:%d %s", #EXPR, arrow_error_code, \
               strerror(arrow_error_code), __FILE__, __LINE__, __FUNCTION__);          \
      return ADBC_STATUS_##CODE;                                                       \
    }                                                                                  \
  } while (0)

/// Check an NanoArrow status code.
#define CHECK_NA_DETAIL(CODE, EXPR, NA_ERROR, ERROR)                              \
  do {                                                                            \
    ArrowErrorCode arrow_error_code = (EXPR);                                     \
    if (arrow_error_code != 0) {                                                  \
      SetError(ERROR, "%s failed: (%d) %s: %s\nDetail: %s:%d %s", #EXPR,          \
               arrow_error_code, strerror(arrow_error_code), (NA_ERROR)->message, \
               __FILE__, __LINE__, __FUNCTION__);                                 \
      return ADBC_STATUS_##CODE;                                                  \
    }                                                                             \
  } while (0)

/// Check a generic status.
#define RAISE(CODE, EXPR, ERRMSG, ERROR)                                          \
  do {                                                                            \
    if (!(EXPR)) {                                                                \
      SetError(ERROR, "%s failed: %s\nDetail: %s:%d %s", #EXPR, ERRMSG, __FILE__, \
               __LINE__, __FUNCTION__);                                           \
      return ADBC_STATUS_##CODE;                                                  \
    }                                                                             \
  } while (0)

/// Check an NanoArrow status code.
#define RAISE_NA(EXPR)                                  \
  do {                                                  \
    ArrowErrorCode arrow_error_code = (EXPR);           \
    if (arrow_error_code != 0) return arrow_error_code; \
  } while (0)

/// Check an ADBC status code.
#define RAISE_ADBC(EXPR)                                             \
  do {                                                               \
    AdbcStatusCode adbc_status_code = (EXPR);                        \
    if (adbc_status_code != ADBC_STATUS_OK) return adbc_status_code; \
  } while (0)
