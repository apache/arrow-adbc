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

#include <cstdint>
#include <unordered_map>

#include <nanoarrow.h>

namespace adbcpq {

enum class PgType : uint8_t {
  // TODO: is there a good null type?
  kBit,
  kBool,
  kDate,
  kFloat4,
  kFloat8,
  kInt2,
  kInt4,
  kInt8,
  kText,
  kTime,
  kTimestamp,
  kTimestampTz,
  kTimeTz,
  kVarChar,
};

struct TypeMapping {
  // Maps Postgres type OIDs to a standardized type name
  // Example: int8 == 20
  std::unordered_map<uint32_t, PgType> type_mapping;
  // Maps standardized type names to the Postgres type OID to use
  // Example: kInt8 == 20
  std::unordered_map<PgType, uint32_t> canonical_types;

  void Insert(uint32_t oid, const char* typname, const char* typreceive);
  /// \return 0 if not found
  uint32_t GetOid(PgType type) const;
};

bool FromPgTypreceive(const char* typreceive, PgType* out);

// TODO: this should be upstream
// const char* ArrowTypeToString(ArrowType type);

}  // namespace adbcpq
