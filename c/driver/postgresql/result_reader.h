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

#include <libpq-fe.h>

#include "copy/reader.h"
#include "result_helper.h"

namespace adbcpq {

class PqResultArrayReader {
 public:
  PqResultArrayReader(PGconn* conn, std::shared_ptr<PostgresTypeResolver> type_resolver,
                      std::string query)
      : helper_(conn, std::move(query)), type_resolver_(type_resolver) {
    ArrowErrorInit(&na_error_);
    error_ = ADBC_ERROR_INIT;
  }

  ~PqResultArrayReader() { ResetErrors(); }

  int GetSchema(struct ArrowSchema* out);
  int GetNext(struct ArrowArray* out);
  const char* GetLastError();

  AdbcStatusCode ToArrayStream(int64_t* affected_rows, struct ArrowArrayStream* out,
                               struct AdbcError* error);

  AdbcStatusCode Initialize(struct AdbcError* error);

 private:
  PqResultHelper helper_;
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
  std::vector<std::unique_ptr<PostgresCopyFieldReader>> field_readers_;
  nanoarrow::UniqueSchema schema_;
  struct AdbcError error_;
  struct ArrowError na_error_;

  explicit PqResultArrayReader(PqResultArrayReader* other)
      : helper_(std::move(other->helper_)),
        type_resolver_(std::move(other->type_resolver_)),
        field_readers_(std::move(other->field_readers_)),
        schema_(std::move(other->schema_)) {
    ArrowErrorInit(&na_error_);
    error_ = ADBC_ERROR_INIT;
  }

  void ResetErrors() {
    ArrowErrorInit(&na_error_);

    if (error_.private_data != nullptr) {
      error_.release(&error_);
    }
    error_ = ADBC_ERROR_INIT;
  }
};

}  // namespace adbcpq
