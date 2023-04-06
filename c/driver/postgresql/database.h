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
#include <memory>
#include <string>

#include <adbc.h>
#include <libpq-fe.h>

#include "postgres_type.h"
#include "type.h"

namespace adbcpq {
class PostgresDatabase {
 public:
  PostgresDatabase();
  ~PostgresDatabase();

  // Public ADBC API

  AdbcStatusCode Init(struct AdbcError* error);
  AdbcStatusCode Release(struct AdbcError* error);
  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error);

  // Internal implementation

  AdbcStatusCode Connect(PGconn** conn, struct AdbcError* error);
  AdbcStatusCode Disconnect(PGconn** conn, struct AdbcError* error);

  const std::shared_ptr<TypeMapping>& type_mapping() const { return type_mapping_; }

  const std::shared_ptr<PostgresTypeResolver>& type_resolver() const {
    return type_resolver_;
  }

 private:
  int32_t open_connections_;
  std::string uri_;
  std::shared_ptr<TypeMapping> type_mapping_;
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
};
}  // namespace adbcpq
