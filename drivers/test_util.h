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

#include <memory>

#include <arrow/c/bridge.h>
#include <arrow/ipc/json_simple.h>
#include <arrow/record_batch.h>
#include <arrow/result.h>
#include <arrow/testing/gtest_util.h>
#include <arrow/util/macros.h>
#include "adbc.h"

namespace adbc {

#define ADBC_ASSERT_OK(expr)          \
  do {                                \
    auto code_ = (expr);              \
    ASSERT_EQ(code_, ADBC_STATUS_OK); \
  } while (false)

#define ADBC_ASSERT_OK_WITH_ERROR(ERROR, EXPR)                                 \
  do {                                                                         \
    auto code_ = (EXPR);                                                       \
    if (code_ != ADBC_STATUS_OK) {                                             \
      std::string errmsg_ = ERROR.message ? ERROR.message : "(unknown error)"; \
      if (ERROR.message) error.release(&error);                                \
      ASSERT_EQ(code_, ADBC_STATUS_OK) << errmsg_;                             \
    }                                                                          \
  } while (false)

#define ADBC_ASSERT_ERROR_THAT(ERROR, PATTERN)                               \
  do {                                                                       \
    ASSERT_NE(ERROR.message, nullptr);                                       \
    std::string errmsg_ = ERROR.message ? ERROR.message : "(unknown error)"; \
      if (ERROR.message) error.release(&error);                                \
    ASSERT_THAT(errmsg_, PATTERN) << errmsg_;                                \
  } while (false)

#define ASSERT_SCHEMA_EQ(schema1, schema2) \
  ASSERT_TRUE((schema1).Equals((schema2))) \
      << "LHS: " << (schema1).ToString() << "RHS: " << (schema2).ToString()

static inline void ReadStatement(AdbcStatement* statement,
                                 std::shared_ptr<arrow::Schema>* schema,
                                 arrow::RecordBatchVector* batches) {
  AdbcError error = {};
  ArrowArrayStream stream;
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementGetStream(statement, &stream, &error));
  ASSERT_OK_AND_ASSIGN(auto reader, arrow::ImportRecordBatchReader(&stream));

  *schema = reader->schema();

  while (true) {
    ASSERT_OK_AND_ASSIGN(auto batch, reader->Next());
    if (!batch) break;
    batches->push_back(std::move(batch));
  }
  ADBC_ASSERT_OK_WITH_ERROR(error, AdbcStatementRelease(statement, &error));
}

static inline std::shared_ptr<arrow::RecordBatch> RecordBatchFromJSON(
    const std::shared_ptr<arrow::Schema>& schema, const std::string& json) {
  auto struct_type = arrow::struct_(schema->fields());
  std::shared_ptr<arrow::Array> struct_array =
      *arrow::ipc::internal::json::ArrayFromJSON(struct_type, json);
  return *arrow::RecordBatch::FromStructArray(struct_array);
}

}  // namespace adbc
