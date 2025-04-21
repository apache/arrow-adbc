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

#include <array>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <libpq-fe.h>

#include "postgres_type.h"

namespace adbcpq {
class PostgresDatabase;
class PostgresConnection {
 public:
  PostgresConnection()
      : database_(nullptr), conn_(nullptr), cancel_(nullptr), autocommit_(true) {}

  AdbcStatusCode Cancel(struct AdbcError* error);
  AdbcStatusCode Commit(struct AdbcError* error);
  AdbcStatusCode GetInfo(struct AdbcConnection* connection, const uint32_t* info_codes,
                         size_t info_codes_length, struct ArrowArrayStream* out,
                         struct AdbcError* error);
  AdbcStatusCode GetObjects(struct AdbcConnection* connection, int depth,
                            const char* catalog, const char* db_schema,
                            const char* table_name, const char** table_types,
                            const char* column_name, struct ArrowArrayStream* out,
                            struct AdbcError* error);
  AdbcStatusCode GetOption(const char* option, char* value, size_t* length,
                           struct AdbcError* error);
  AdbcStatusCode GetOptionBytes(const char* option, uint8_t* value, size_t* length,
                                struct AdbcError* error);
  AdbcStatusCode GetOptionDouble(const char* option, double* value,
                                 struct AdbcError* error);
  AdbcStatusCode GetOptionInt(const char* option, int64_t* value,
                              struct AdbcError* error);
  AdbcStatusCode GetStatistics(const char* catalog, const char* db_schema,
                               const char* table_name, bool approximate,
                               struct ArrowArrayStream* out, struct AdbcError* error);
  AdbcStatusCode GetStatisticNames(struct ArrowArrayStream* out, struct AdbcError* error);
  AdbcStatusCode GetTableSchema(const char* catalog, const char* db_schema,
                                const char* table_name, struct ArrowSchema* schema,
                                struct AdbcError* error);
  AdbcStatusCode GetTableTypes(struct AdbcConnection* connection,
                               struct ArrowArrayStream* out, struct AdbcError* error);
  AdbcStatusCode Init(struct AdbcDatabase* database, struct AdbcError* error);
  AdbcStatusCode Release(struct AdbcError* error);
  AdbcStatusCode Rollback(struct AdbcError* error);
  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error);
  AdbcStatusCode SetOptionBytes(const char* key, const uint8_t* value, size_t length,
                                struct AdbcError* error);
  AdbcStatusCode SetOptionDouble(const char* key, double value, struct AdbcError* error);
  AdbcStatusCode SetOptionInt(const char* key, int64_t value, struct AdbcError* error);

  PGconn* conn() const { return conn_; }
  const std::shared_ptr<PostgresTypeResolver>& type_resolver() const {
    return type_resolver_;
  }
  bool autocommit() const { return autocommit_; }
  std::string_view VendorName();
  const std::array<int, 3>& VendorVersion();

 private:
  std::shared_ptr<PostgresDatabase> database_;
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
  PGconn* conn_;
  PGcancel* cancel_;
  bool autocommit_;
  std::vector<std::pair<std::string, std::string>> post_init_options_;
};
}  // namespace adbcpq
