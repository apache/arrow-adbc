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
#include <string>
#include <string_view>
#include <vector>

#include "db2_odbc.h"

#include <arrow-adbc/adbc.h>
#include <nanoarrow/nanoarrow.h>

#define ADBC_FRAMEWORK_USE_FMT
#include "driver/framework/statement.h"
#include "driver/framework/status.h"

#include "connection.h"

namespace adbc::db2 {

using driver::Option;
using driver::Result;
using driver::Status;
namespace status = adbc::driver::status;

class Db2Statement : public driver::Statement<Db2Statement> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[DB2]";

  using QueryState = typename Base::QueryState;
  using PreparedState = typename Base::PreparedState;
  using IngestState = typename Base::IngestState;

  Db2Statement() = default;
  ~Db2Statement() = default;

  Status InitImpl(void* parent);
  Status ReleaseImpl();

  Status PrepareImpl(QueryState& state);

  Result<int64_t> ExecuteQueryImpl(QueryState& state, ArrowArrayStream* stream);
  Result<int64_t> ExecuteQueryImpl(PreparedState& state, ArrowArrayStream* stream);
  Result<int64_t> ExecuteUpdateImpl(QueryState& state);
  Result<int64_t> ExecuteUpdateImpl(PreparedState& state);
  Result<int64_t> ExecuteIngestImpl(IngestState& state);

  AdbcStatusCode Cancel(AdbcError* error);
  AdbcStatusCode ExecuteSchema(ArrowSchema* schema, AdbcError* error);
  Status GetParameterSchemaImpl(PreparedState& state, ArrowSchema* schema);

  Status SetOptionImpl(std::string_view key, Option value);

  int64_t batch_size() const { return batch_size_; }

 private:
  Db2Connection* connection_ = nullptr;
  SQLHSTMT hstmt_ = SQL_NULL_HSTMT;
  bool prepared_ = false;
  int64_t batch_size_ = 65536;

  Result<int64_t> ExecuteCommon(const std::string& query, bool is_prepared,
                                ArrowArrayStream* stream);
  Status AllocateStatement();
  void FreeStatement();

  Status SetupArrayView(ArrowArrayView* view, ArrowSchema* schema, ArrowArray* array);
  Status BindParameters(SQLHSTMT hstmt, ArrowSchema* schema,
                        ArrowArrayView* array_view, int64_t row);
  void ClearBindData();

  // Temporary storage for SQLBindParameter data, kept alive until SQLExecute returns.
  std::vector<int8_t> bind_data_int8_;
  std::vector<int16_t> bind_data_int16_;
  std::vector<int32_t> bind_data_int32_;
  std::vector<int64_t> bind_data_int64_;
  std::vector<float> bind_data_float_;
  std::vector<double> bind_data_double_;
  std::vector<std::string> bind_data_strings_;
  std::vector<SQL_DATE_STRUCT> bind_data_date_;
  std::vector<SQL_TIME_STRUCT> bind_data_time_;
  std::vector<SQL_TIMESTAMP_STRUCT> bind_data_timestamp_;
  std::vector<SQLLEN> bind_indicators_;
};

}  // namespace adbc::db2
