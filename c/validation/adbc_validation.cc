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

#include "adbc_validation.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <variant>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <gmock/gmock.h>
#include <gtest/gtest-matchers.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow.hpp>

#include "adbc_validation_util.h"
#include "common/options.h"

namespace adbc_validation {

//------------------------------------------------------------
// DriverQuirks

AdbcStatusCode DoIngestSampleTable(struct AdbcConnection* connection,
                                   const std::string& name,
                                   std::optional<std::string> db_schema,
                                   struct AdbcError* error) {
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  CHECK_OK(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64},
                                      {"strings", NANOARROW_TYPE_STRING}}));
  CHECK_OK((MakeBatch<int64_t, std::string>(&schema.value, &array.value, &na_error,
                                            {42, -42, std::nullopt},
                                            {"foo", std::nullopt, ""})));

  Handle<struct AdbcStatement> statement;
  CHECK_OK(AdbcStatementNew(connection, &statement.value, error));
  CHECK_OK(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                  name.c_str(), error));
  if (db_schema.has_value()) {
    CHECK_OK(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
                                    db_schema->c_str(), error));
  }
  CHECK_OK(AdbcStatementBind(&statement.value, &array.value, &schema.value, error));
  CHECK_OK(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
  CHECK_OK(AdbcStatementRelease(&statement.value, error));
  return ADBC_STATUS_OK;
}

AdbcStatusCode DriverQuirks::EnsureSampleTable(struct AdbcConnection* connection,
                                               const std::string& name,
                                               struct AdbcError* error) const {
  CHECK_OK(DropTable(connection, name, error));
  return CreateSampleTable(connection, name, error);
}

AdbcStatusCode DriverQuirks::CreateSampleTable(struct AdbcConnection* connection,
                                               const std::string& name,
                                               struct AdbcError* error) const {
  if (!supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  return DoIngestSampleTable(connection, name, std::nullopt, error);
}

AdbcStatusCode DriverQuirks::CreateSampleTable(struct AdbcConnection* connection,
                                               const std::string& name,
                                               const std::string& schema,
                                               struct AdbcError* error) const {
  if (!supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  return DoIngestSampleTable(connection, name, schema, error);
}
}  // namespace adbc_validation
