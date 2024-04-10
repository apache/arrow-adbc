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
#include <string_view>

#include <adbc.h>
#include <arrow/ipc/reader.h>
#include <google/cloud/bigquery/storage/v1/bigquery_read_client.h>

#include "common/utils.h"
#include "inputstream.h"

namespace adbc_bigquery {

class ReadRowsIterator {
 public:
  using ReadRowsResponse = ::google::cloud::StreamRange<
      ::google::cloud::bigquery::storage::v1::ReadRowsResponse>;
  using ReadSession =
      std::shared_ptr<::google::cloud::bigquery::storage::v1::ReadSession>;

  ReadRowsIterator(const std::string& project_name, const std::string& table_name)
      : project_name_(project_name), table_name_(table_name) {}

  AdbcStatusCode init(struct AdbcError* error);
  AdbcStatusCode read_next();

  friend class BigqueryStatement;

  static int get_next(struct ArrowArrayStream* stream, struct ArrowArray* error);
  static int get_schema(struct ArrowArrayStream* stream, struct ArrowSchema* error);
  static void release(struct ArrowArrayStream* stream);

 protected:
  std::string project_name_;
  std::string table_name_ =
      "projects/bigquery-poc-418913/datasets/google_trends/tables/small_top_terms";
  decltype(::google::cloud::bigquery_storage_v1::MakeBigQueryReadConnection())
      connection_;
  std::shared_ptr<::google::cloud::bigquery_storage_v1::BigQueryReadClient> client_;
  std::shared_ptr<::google::cloud::bigquery::storage::v1::ReadSession> session_;
  std::shared_ptr<ReadRowsResponse> response_;
  ReadRowsResponse::iterator current_;

  std::string_view serialized_schema_;
  std::string_view serialized_record_batch_;
  std::shared_ptr<adbc_bigquery::InputStream> input_stream_;
  std::shared_ptr<arrow::ipc::RecordBatchStreamReader> reader_;
  std::shared_ptr<arrow::RecordBatch> record_batch;

  struct ArrowSchema* parsed_schema_ = nullptr;
  struct ArrowArray* parsed_array_ = nullptr;
};

}  // namespace adbc_bigquery
