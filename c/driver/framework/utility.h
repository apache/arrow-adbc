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

#include <string>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>

#include "driver/framework/status.h"

namespace adbc::driver {

/// \brief Create an ArrowArrayStream with zero batches from a given ArrowSchema.
/// \ingroup adbc-framework-catalog
///
/// This function takes ownership of schema; the caller is responsible for
/// releasing out.
void MakeEmptyStream(ArrowSchema* schema, ArrowArrayStream* out);

/// \brief Create an ArrowArrayStream from a given ArrowSchema and ArrowArray.
/// \ingroup adbc-framework-catalog
///
/// The resulting ArrowArrayStream will contain zero batches if the length of the
/// array is zero, or exactly one batch if the length of the array is non-zero.
/// This function takes ownership of schema and array; the caller is responsible for
/// releasing out.
void MakeArrayStream(ArrowSchema* schema, ArrowArray* array, ArrowArrayStream* out);

/// \brief Create an ArrowArrayStream representation of a vector of table types.
/// \ingroup adbc-framework-catalog
///
/// Create an ArrowArrayStream representation of an array of table types
/// that can be used to implement AdbcConnectionGetTableTypes(). The caller is responsible
/// for releasing out on success.
Status MakeTableTypesStream(const std::vector<std::string>& table_types,
                            ArrowArrayStream* out);

/// \brief Representation of a single item in an array to be returned
/// from AdbcConnectionGetInfo().
/// \ingroup adbc-framework-catalog
struct InfoValue {
  uint32_t code;
  std::variant<std::string, int64_t> value;

  InfoValue(uint32_t code, std::variant<std::string, int64_t> value)
      : code(code), value(std::move(value)) {}
  InfoValue(uint32_t code, const char* value) : InfoValue(code, std::string(value)) {}
};

/// \brief Create an ArrowArrayStream to be returned from AdbcConnectionGetInfo().
/// \ingroup adbc-framework-catalog
///
/// The caller is responsible for releasing out on success.
Status MakeGetInfoStream(const std::vector<InfoValue>& infos, ArrowArrayStream* out);

}  // namespace adbc::driver
