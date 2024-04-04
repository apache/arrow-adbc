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

// Windows
#if !defined(NOMINMAX)
#define NOMINMAX
#endif

#include <cstdint>

// R 3.6 / Windows builds on a very old toolchain that does not define ENODATA
#if defined(_WIN32) && !defined(MSVC) && !defined(ENODATA)
#define ENODATA 120
#endif

namespace adbcpq {

// "PGCOPY\n\377\r\n\0"
static int8_t kPgCopyBinarySignature[] = {0x50, 0x47, 0x43, 0x4F,
                                          0x50, 0x59, 0x0A, static_cast<int8_t>(0xFF),
                                          0x0D, 0x0A, 0x00};

// The maximum value in microseconds that can be converted into nanoseconds
// without overflow
constexpr int64_t kMaxSafeMicrosToNanos = 9223372036854775L;

// The minimum value in microseconds that can be converted into nanoseconds
// without overflow
constexpr int64_t kMinSafeMicrosToNanos = -9223372036854775L;

}  // namespace adbcpq
