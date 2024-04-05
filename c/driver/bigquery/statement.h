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

#include <cinttypes>
#include <cstring>
#include <memory>
#include <string>

#include <adbc.h>
#include <google/cloud/bigquery/storage/v1/bigquery_read_client.h>

#include "common/utils.h"

namespace adbc_bigquery {
class BigqueryConnection;
class BigqueryStatement;

class ReadRowsIterator {
 public:
  using ReadRowsResponse = ::google::cloud::StreamRange<
      ::google::cloud::bigquery::storage::v1::ReadRowsResponse>;
  using ReadSession =
      std::shared_ptr<::google::cloud::bigquery::storage::v1::ReadSession>;

  ReadRowsIterator(const std::string& project_name, const std::string& table_name)
      : project_name_(project_name), table_name_(table_name) {}

  AdbcStatusCode init(struct AdbcError* error);

  friend class BigqueryStatement;

  static int get_next(struct ArrowArrayStream* stream, struct ArrowArray* error);
  static int get_schema(struct ArrowArrayStream* stream, struct ArrowSchema* error);
  static void release(struct ArrowArrayStream* stream);

 protected:
  std::string project_name_;
  std::string table_name_;
  decltype(::google::cloud::bigquery_storage_v1::MakeBigQueryReadConnection())
      connection_;
  std::shared_ptr<::google::cloud::bigquery_storage_v1::BigQueryReadClient> client_;
  std::shared_ptr<::google::cloud::bigquery::storage::v1::ReadSession> session_;
  std::shared_ptr<ReadRowsResponse> response_;
  ReadRowsResponse::iterator current_;

  struct ArrowSchema* parsed_schema_ = nullptr;
};

class BigqueryStatement {
 public:
  BigqueryStatement() : connection_(nullptr) {}

  // ---------------------------------------------------------------------
  // ADBC API implementation

  AdbcStatusCode Bind(struct ArrowArray* values, struct ArrowSchema* schema,
                      struct AdbcError* error);
  AdbcStatusCode Bind(struct ArrowArrayStream* stream, struct AdbcError* error);
  AdbcStatusCode Cancel(struct AdbcError* error);
  AdbcStatusCode ExecuteQuery(struct ArrowArrayStream* stream, int64_t* rows_affected,
                              struct AdbcError* error);
  AdbcStatusCode ExecuteSchema(struct ArrowSchema* schema, struct AdbcError* error);
  AdbcStatusCode GetOption(const char* key, char* value, size_t* length,
                           struct AdbcError* error);
  AdbcStatusCode GetOptionBytes(const char* key, uint8_t* value, size_t* length,
                                struct AdbcError* error);
  AdbcStatusCode GetOptionDouble(const char* key, double* value, struct AdbcError* error);
  AdbcStatusCode GetOptionInt(const char* key, int64_t* value, struct AdbcError* error);
  AdbcStatusCode GetParameterSchema(struct ArrowSchema* schema, struct AdbcError* error);
  AdbcStatusCode New(struct AdbcConnection* connection, struct AdbcError* error);
  AdbcStatusCode Prepare(struct AdbcError* error);
  AdbcStatusCode Release(struct AdbcError* error);
  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error);
  AdbcStatusCode SetOptionBytes(const char* key, const uint8_t* value, size_t length,
                                struct AdbcError* error);
  AdbcStatusCode SetOptionDouble(const char* key, double value, struct AdbcError* error);
  AdbcStatusCode SetOptionInt(const char* key, int64_t value, struct AdbcError* error);
  AdbcStatusCode SetSqlQuery(const char* query, struct AdbcError* error);

 private:
  std::shared_ptr<BigqueryConnection> connection_;
};
}  // namespace adbc_bigquery
