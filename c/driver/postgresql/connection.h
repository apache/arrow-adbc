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

#include <adbc.h>
#include <libpq-fe.h>

#include "postgres_type.h"
#include "type.h"

namespace adbcpq {
class PostgresDatabase;
class PostgresConnection {
 public:
  PostgresConnection() : database_(nullptr), conn_(nullptr), autocommit_(true) {}

  AdbcStatusCode Commit(struct AdbcError* error);
  AdbcStatusCode GetTableSchema(const char* catalog, const char* db_schema,
                                const char* table_name, struct ArrowSchema* schema,
                                struct AdbcError* error);
  AdbcStatusCode Init(struct AdbcDatabase* database, struct AdbcError* error);
  AdbcStatusCode Release(struct AdbcError* error);
  AdbcStatusCode Rollback(struct AdbcError* error);
  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error);

  PGconn* conn() const { return conn_; }
  const std::shared_ptr<PostgresTypeResolver>& type_resolver() const {
    return type_resolver_;
  }

 private:
  std::shared_ptr<PostgresDatabase> database_;
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
  PGconn* conn_;
  bool autocommit_;
};
}  // namespace adbcpq
