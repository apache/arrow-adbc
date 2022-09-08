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

#include "type.h"

#include <cstring>

namespace adbcpq {
void TypeMapping::Insert(uint32_t oid, const char* typname, const char* typreceive) {
  PgType type;
  if (FromPgTypreceive(typreceive, &type)) {
    type_mapping[oid] = type;
  }

  // Record 'canonical' types
  if (std::strcmp(typname, "int8") == 0) {
    // DCHECK_EQ(type, PgType::kInt8);
    canonical_types[PgType::kInt8] = oid;
  } else if (std::strcmp(typname, "text") == 0) {
    canonical_types[PgType::kText] = oid;
  }
  // TODO: fill in remainder
}

uint32_t TypeMapping::GetOid(PgType type) const {
  auto it = canonical_types.find(type);
  if (it == canonical_types.end()) {
    return 0;
  }
  return it->second;
}

bool FromPgTypreceive(const char* typreceive, PgType* out) {
  if (std::strcmp(typreceive, "bitrecv") == 0) {
    *out = PgType::kBit;
  } else if (std::strcmp(typreceive, "boolrecv") == 0) {
    *out = PgType::kBool;
  } else if (std::strcmp(typreceive, "date_recv") == 0) {
    *out = PgType::kDate;
  } else if (std::strcmp(typreceive, "float4recv") == 0) {
    *out = PgType::kFloat4;
  } else if (std::strcmp(typreceive, "float8recv") == 0) {
    *out = PgType::kFloat8;
  } else if (std::strcmp(typreceive, "int2recv") == 0) {
    *out = PgType::kInt2;
  } else if (std::strcmp(typreceive, "int4recv") == 0) {
    *out = PgType::kInt4;
  } else if (std::strcmp(typreceive, "int8recv") == 0) {
    *out = PgType::kInt8;
  } else if (std::strcmp(typreceive, "textrecv") == 0) {
    *out = PgType::kText;
  } else if (std::strcmp(typreceive, "time_recv") == 0) {
    *out = PgType::kTime;
  } else if (std::strcmp(typreceive, "timestamp_recv") == 0) {
    *out = PgType::kTimestamp;
  } else if (std::strcmp(typreceive, "timestamptz_recv") == 0) {
    *out = PgType::kTimestampTz;
  } else if (std::strcmp(typreceive, "timetz_recv") == 0) {
    *out = PgType::kTimeTz;
  } else if (std::strcmp(typreceive, "varcharrecv") == 0) {
    *out = PgType::kVarChar;
  } else {
    return false;
  }
  return true;
}

}  // namespace adbcpq
