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

#include <string>
#include <unordered_map>

#include "arrow/result.h"
#include "arrow/util/string_view.h"

// TODO: dllimport/dllexport for Windows. Should that go in adbc.h instead?
#ifdef __linux__
#define ADBC_DRIVER_EXPORT
#else
#define ADBC_DRIVER_EXPORT
#endif  // ifdef __linux__

#define ADBC_RETURN_NOT_OK(expr)         \
  do {                                   \
    auto _s = (expr);                    \
    if (_s != ADBC_STATUS_OK) return _s; \
  } while (false)
