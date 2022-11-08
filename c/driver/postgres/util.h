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

#include <netinet/in.h>
#include <cstring>
#include <sstream>
#include <string>
#include <utility>

#if defined(__linux__)
#include <endian.h>
#elif defined(__APPLE__)
#include <machine/endian.h>
#else
static_assert(false, "Not supported on this platform");
#endif

#include "adbc.h"

namespace adbcpq {

#define CONCAT(x, y) x##y
#define MAKE_NAME(x, y) CONCAT(x, y)

// see arrow/util/string_builder.h

template <typename Head>
static inline void StringBuilderRecursive(std::stringstream& stream, Head&& head) {
  stream << head;
}

template <typename Head, typename... Tail>
static inline void StringBuilderRecursive(std::stringstream& stream, Head&& head,
                                          Tail&&... tail) {
  StringBuilderRecursive(stream, std::forward<Head>(head));
  StringBuilderRecursive(stream, std::forward<Tail>(tail)...);
}

template <typename... Args>
static inline std::string StringBuilder(Args&&... args) {
  std::stringstream ss;
  StringBuilderRecursive(ss, std::forward<Args>(args)...);
  return ss.str();
}

static inline void ReleaseError(struct AdbcError* error) {
  delete[] error->message;
  error->message = nullptr;
  error->release = nullptr;
}

template <typename... Args>
static inline void SetError(struct AdbcError* error, Args&&... args) {
  if (!error) return;
  std::string message = StringBuilder("[libpq] ", std::forward<Args>(args)...);
  if (error->message) {
    message.reserve(message.size() + 1 + std::strlen(error->message));
    message.append(1, '\n');
    message.append(error->message);
    delete[] error->message;
  }
  error->message = new char[message.size() + 1];
  message.copy(error->message, message.size());
  error->message[message.size()] = '\0';
  error->release = ReleaseError;
}

#define CHECK_IMPL(NAME, EXPR)          \
  do {                                  \
    const AdbcStatusCode NAME = (EXPR); \
    if (NAME != ADBC_STATUS_OK) {       \
      return NAME;                      \
    }                                   \
  } while (false)
#define CHECK(EXPR) CHECK_IMPL(MAKE_NAME(adbc_status_, __COUNTER__), EXPR)

#define CHECK_NA_ADBC_IMPL(NAME, EXPR, ERROR)                    \
  do {                                                           \
    const int NAME = (EXPR);                                     \
    if (NAME) {                                                  \
      SetError((ERROR), #EXPR " failed: ", std::strerror(NAME)); \
      return ADBC_STATUS_INTERNAL;                               \
    }                                                            \
  } while (false)
/// Check an errno-style code and return an ADBC code if necessary.
#define CHECK_NA_ADBC(EXPR, ERROR) \
  CHECK_NA_ADBC_IMPL(MAKE_NAME(errno_status_, __COUNTER__), EXPR, ERROR)

#define CHECK_NA_IMPL(NAME, EXPR) \
  do {                            \
    const int NAME = (EXPR);      \
    if (NAME) return NAME;        \
  } while (false)

/// Check an errno-style code and return it if necessary.
#define CHECK_NA(EXPR) CHECK_NA_IMPL(MAKE_NAME(errno_status_, __COUNTER__), EXPR)

/// Endianness helpers

uint32_t LoadNetworkUInt32(const char* buf) {
  uint32_t v = 0;
  std::memcpy(&v, buf, sizeof(uint32_t));
  return ntohl(v);
}

int64_t LoadNetworkUInt64(const char* buf) {
  uint64_t v = 0;
  std::memcpy(&v, buf, sizeof(uint64_t));
#if defined(__linux__)
  return be64toh(v);
#elif defined(__APPLE__)
  return ntohll(v);
#else
  static_assert(false, "Not supported on this platform");
#endif
}

int32_t LoadNetworkInt32(const char* buf) {
  return static_cast<int32_t>(LoadNetworkUInt32(buf));
}

int64_t LoadNetworkInt64(const char* buf) {
  return static_cast<int64_t>(LoadNetworkUInt64(buf));
}

uint64_t ToNetworkInt64(int64_t v) {
#if defined(__linux__)
  return htobe64(static_cast<uint64_t>(v));
#elif defined(__APPLE__)
  return htonll(static_cast<uint64_t>(v));
#else
  static_assert(false, "Not supported on this platform");
#endif
}

}  // namespace adbcpq
