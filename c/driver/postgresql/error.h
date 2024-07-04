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

#include <adbc.h>
#include <libpq-fe.h>

namespace adbcpq {

// The printf checking attribute doesn't work properly on gcc 4.8
// and results in spurious compiler warnings
#if defined(__clang__) || (defined(__GNUC__) && __GNUC__ >= 5)
#define ADBC_CHECK_PRINTF_ATTRIBUTE(x, y) __attribute__((format(printf, x, y)))
#else
#define ADBC_CHECK_PRINTF_ATTRIBUTE(x, y)
#endif

/// \brief Set an error based on a PGresult, inferring the proper ADBC status
///   code from the PGresult.
AdbcStatusCode SetError(struct AdbcError* error, PGresult* result, const char* format,
                        ...) ADBC_CHECK_PRINTF_ATTRIBUTE(3, 4);

#undef ADBC_CHECK_PRINTF_ATTRIBUTE

}  // namespace adbcpq
