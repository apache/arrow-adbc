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

#if !defined(NOMINMAX)
#define NOMINMAX
#endif

#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <libpq-fe.h>

#include "bind_stream.h"
#include "copy/reader.h"
#include "result_helper.h"

namespace adbcpq {

class PqResultArrayReader {
 public:
  PqResultArrayReader(PGconn* conn, std::shared_ptr<PostgresTypeResolver> type_resolver,
                      std::string query)
      : conn_(conn),
        helper_(conn, std::move(query)),
        type_resolver_(type_resolver),
        autocommit_(false) {
    ArrowErrorInit(&na_error_);
    error_ = ADBC_ERROR_INIT;
  }

  ~PqResultArrayReader() { ResetErrors(); }

  // Ensure the reader knows what the autocommit status was on creation. This is used
  // so that the temporary timezone setting required for parameter binding can be wrapped
  // in a transaction (or not) accordingly.
  void SetAutocommit(bool autocommit) { autocommit_ = autocommit; }

  void SetBind(struct ArrowArrayStream* stream) {
    bind_stream_ = std::make_unique<BindStream>();
    bind_stream_->SetBind(stream);
  }

  void SetVendorName(std::string_view vendor_name) {
    vendor_name_ = std::string(vendor_name);
  }

  int GetSchema(struct ArrowSchema* out);
  int GetNext(struct ArrowArray* out);
  const char* GetLastError();

  Status ToArrayStream(int64_t* affected_rows, struct ArrowArrayStream* out);

  Status Initialize(int64_t* affected_rows);

 private:
  PGconn* conn_;
  PqResultHelper helper_;
  std::unique_ptr<BindStream> bind_stream_;
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
  std::vector<std::unique_ptr<PostgresCopyFieldReader>> field_readers_;
  nanoarrow::UniqueSchema schema_;
  bool autocommit_;
  std::string vendor_name_;
  struct AdbcError error_;
  struct ArrowError na_error_;

  explicit PqResultArrayReader(PqResultArrayReader* other)
      : conn_(other->conn_),
        helper_(std::move(other->helper_)),
        bind_stream_(std::move(other->bind_stream_)),
        type_resolver_(std::move(other->type_resolver_)),
        field_readers_(std::move(other->field_readers_)),
        schema_(std::move(other->schema_)) {
    ArrowErrorInit(&na_error_);
    error_ = ADBC_ERROR_INIT;
  }

  Status BindNextAndExecute(int64_t* affected_rows);
  Status ExecuteAll(int64_t* affected_rows);

  void ResetErrors() {
    ArrowErrorInit(&na_error_);

    if (error_.private_data != nullptr) {
      error_.release(&error_);
    }
    error_ = ADBC_ERROR_INIT;
  }
};

}  // namespace adbcpq
