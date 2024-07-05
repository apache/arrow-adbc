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

#include <cstring>

#include <nanoarrow/nanoarrow.hpp>

#ifdef _WIN32
#include <winsock2.h>
#else
#include <netinet/in.h>
#endif

#if defined(__linux__)
#include <endian.h>
#elif defined(__APPLE__)
#include <libkern/OSByteOrder.h>
#endif

#include "arrow-adbc/adbc.h"

namespace adbcpq {

#if defined(_WIN32) && defined(_MSC_VER)
static inline uint16_t SwapNetworkToHost(uint16_t x) { return ntohs(x); }
static inline uint16_t SwapHostToNetwork(uint16_t x) { return htons(x); }
static inline uint32_t SwapNetworkToHost(uint32_t x) { return ntohl(x); }
static inline uint32_t SwapHostToNetwork(uint32_t x) { return htonl(x); }
static inline uint64_t SwapNetworkToHost(uint64_t x) { return ntohll(x); }
static inline uint64_t SwapHostToNetwork(uint64_t x) { return htonll(x); }
#elif defined(_WIN32)
// e.g., msys2, where ntohll is not necessarily defined
static inline uint16_t SwapNetworkToHost(uint16_t x) { return ntohs(x); }
static inline uint16_t SwapHostToNetwork(uint16_t x) { return htons(x); }
static inline uint32_t SwapNetworkToHost(uint32_t x) { return ntohl(x); }
static inline uint32_t SwapHostToNetwork(uint32_t x) { return htonl(x); }
static inline uint64_t SwapNetworkToHost(uint64_t x) {
  return (((x & 0xFFULL) << 56) | ((x & 0xFF00ULL) << 40) | ((x & 0xFF0000ULL) << 24) |
          ((x & 0xFF000000ULL) << 8) | ((x & 0xFF00000000ULL) >> 8) |
          ((x & 0xFF0000000000ULL) >> 24) | ((x & 0xFF000000000000ULL) >> 40) |
          ((x & 0xFF00000000000000ULL) >> 56));
}
static inline uint64_t SwapHostToNetwork(uint64_t x) { return SwapNetworkToHost(x); }
#elif defined(__APPLE__)
static inline uint16_t SwapNetworkToHost(uint16_t x) { return OSSwapBigToHostInt16(x); }
static inline uint16_t SwapHostToNetwork(uint16_t x) { return OSSwapHostToBigInt16(x); }
static inline uint32_t SwapNetworkToHost(uint32_t x) { return OSSwapBigToHostInt32(x); }
static inline uint32_t SwapHostToNetwork(uint32_t x) { return OSSwapHostToBigInt32(x); }
static inline uint64_t SwapNetworkToHost(uint64_t x) { return OSSwapBigToHostInt64(x); }
static inline uint64_t SwapHostToNetwork(uint64_t x) { return OSSwapHostToBigInt64(x); }
#else
static inline uint16_t SwapNetworkToHost(uint16_t x) { return be16toh(x); }
static inline uint16_t SwapHostToNetwork(uint16_t x) { return htobe16(x); }
static inline uint32_t SwapNetworkToHost(uint32_t x) { return be32toh(x); }
static inline uint32_t SwapHostToNetwork(uint32_t x) { return htobe32(x); }
static inline uint64_t SwapNetworkToHost(uint64_t x) { return be64toh(x); }
static inline uint64_t SwapHostToNetwork(uint64_t x) { return htobe64(x); }
#endif

/// Endianness helpers

static inline uint16_t LoadNetworkUInt16(const char* buf) {
  uint16_t v = 0;
  std::memcpy(&v, buf, sizeof(uint16_t));
  return ntohs(v);
}

static inline uint32_t LoadNetworkUInt32(const char* buf) {
  uint32_t v = 0;
  std::memcpy(&v, buf, sizeof(uint32_t));
  return ntohl(v);
}

static inline int64_t LoadNetworkUInt64(const char* buf) {
  uint64_t v = 0;
  std::memcpy(&v, buf, sizeof(uint64_t));
  return SwapNetworkToHost(v);
}

static inline int16_t LoadNetworkInt16(const char* buf) {
  return static_cast<int16_t>(LoadNetworkUInt16(buf));
}

static inline int32_t LoadNetworkInt32(const char* buf) {
  return static_cast<int32_t>(LoadNetworkUInt32(buf));
}

static inline int64_t LoadNetworkInt64(const char* buf) {
  return static_cast<int64_t>(LoadNetworkUInt64(buf));
}

static inline double LoadNetworkFloat8(const char* buf) {
  uint64_t vint;
  memcpy(&vint, buf, sizeof(uint64_t));
  vint = SwapHostToNetwork(vint);
  double out;
  memcpy(&out, &vint, sizeof(double));
  return out;
}

#define ADBC_REGISTER_TO_NETWORK_FUNC(size)                          \
  static inline uint##size##_t ToNetworkInt##size(int##size##_t v) { \
    return SwapHostToNetwork(static_cast<uint##size##_t>(v));        \
  }

ADBC_REGISTER_TO_NETWORK_FUNC(16)
ADBC_REGISTER_TO_NETWORK_FUNC(32)
ADBC_REGISTER_TO_NETWORK_FUNC(64)

static inline uint32_t ToNetworkFloat4(float v) {
  uint32_t vint;
  memcpy(&vint, &v, sizeof(uint32_t));
  return SwapHostToNetwork(vint);
}

static inline uint64_t ToNetworkFloat8(double v) {
  uint64_t vint;
  memcpy(&vint, &v, sizeof(uint64_t));
  return SwapHostToNetwork(vint);
}

/// Helper to manage resources with RAII

template <typename T>
struct Releaser {
  static void Release(T* value) {
    if (value->release) {
      value->release(value);
    }
  }
};

template <>
struct Releaser<struct ArrowBuffer> {
  static void Release(struct ArrowBuffer* buffer) { ArrowBufferReset(buffer); }
};

template <>
struct Releaser<struct ArrowArrayView> {
  static void Release(struct ArrowArrayView* value) {
    if (value->storage_type != NANOARROW_TYPE_UNINITIALIZED) {
      ArrowArrayViewReset(value);
    }
  }
};

template <typename Resource>
struct Handle {
  Resource value;

  Handle() { std::memset(&value, 0, sizeof(value)); }

  ~Handle() { reset(); }

  Resource* operator->() { return &value; }

  void reset() { Releaser<Resource>::Release(&value); }
};

}  // namespace adbcpq
