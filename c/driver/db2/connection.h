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

#include <atomic>
#include <memory>
#include <optional>
#include <string>
#include <string_view>
#include <vector>

#include "db2_odbc.h"

#include <arrow-adbc/adbc.h>

#define ADBC_FRAMEWORK_USE_FMT
#include "driver/framework/connection.h"
#include "driver/framework/objects.h"
#include "driver/framework/status.h"
#include "driver/framework/utility.h"

#include "database.h"

namespace adbc::db2 {

using driver::GetObjectsHelper;
using driver::InfoValue;
using driver::Option;
using driver::Result;
using driver::Status;
namespace status = adbc::driver::status;

class Db2Connection : public driver::Connection<Db2Connection> {
 public:
  [[maybe_unused]] constexpr static std::string_view kErrorPrefix = "[DB2]";

  Db2Connection() = default;
  ~Db2Connection() = default;

  Status InitImpl(void* parent);
  Status ReleaseImpl();

  Status CommitImpl();
  Status RollbackImpl();
  Status ToggleAutocommitImpl(bool enable_autocommit);

  Result<std::vector<InfoValue>> InfoImpl(const std::vector<uint32_t>& codes);
  Result<std::unique_ptr<GetObjectsHelper>> GetObjectsImpl();
  Status GetTableSchemaImpl(std::optional<std::string_view> catalog,
                            std::optional<std::string_view> db_schema,
                            std::string_view table_name, ArrowSchema* schema);
  Result<std::vector<std::string>> GetTableTypesImpl();

  Result<std::optional<std::string>> GetCurrentCatalogImpl();
  Result<std::optional<std::string>> GetCurrentSchemaImpl();

  AdbcStatusCode Cancel(AdbcError* error);

  SQLHDBC hdbc() const { return hdbc_; }
  Db2Database* database() const { return database_; }

  void RegisterActiveStatement(SQLHSTMT h) { active_hstmt_.store(h); }
  void UnregisterActiveStatement() { active_hstmt_.store(SQL_NULL_HSTMT); }

 private:
  Db2Database* database_ = nullptr;
  SQLHDBC hdbc_ = SQL_NULL_HDBC;
  std::atomic<SQLHSTMT> active_hstmt_{SQL_NULL_HSTMT};
};

/// GetObjectsHelper that uses SQLTables / SQLColumns catalog functions.
class Db2GetObjectsHelper : public GetObjectsHelper {
 public:
  explicit Db2GetObjectsHelper(SQLHDBC hdbc);
  ~Db2GetObjectsHelper() override = default;

  Status Load(driver::GetObjectsDepth depth,
              std::optional<std::string_view> catalog_filter,
              std::optional<std::string_view> schema_filter,
              std::optional<std::string_view> table_filter,
              std::optional<std::string_view> column_filter,
              const std::vector<std::string_view>& table_types) override;

  Status LoadCatalogs(std::optional<std::string_view> catalog_filter) override;
  Result<std::optional<std::string_view>> NextCatalog() override;

  Status LoadSchemas(std::string_view catalog,
                     std::optional<std::string_view> schema_filter) override;
  Result<std::optional<std::string_view>> NextSchema() override;

  Status LoadTables(std::string_view catalog, std::string_view schema,
                    std::optional<std::string_view> table_filter,
                    const std::vector<std::string_view>& table_types) override;
  Result<std::optional<Table>> NextTable() override;

  Status LoadColumns(std::string_view catalog, std::string_view schema,
                     std::string_view table,
                     std::optional<std::string_view> column_filter) override;
  Result<std::optional<Column>> NextColumn() override;
  Result<std::optional<Constraint>> NextConstraint() override;
  Status Close();

 private:
  Status LoadConstraintsFor(std::string_view catalog, std::string_view schema,
                            std::string_view table);
  SQLHDBC hdbc_;

  std::vector<std::string> catalogs_;
  size_t next_catalog_ = 0;
  std::string current_catalog_;
  std::vector<std::string> schemas_;
  size_t next_schema_ = 0;
  std::string current_schema_;
  std::vector<std::pair<std::string, std::string>> tables_;
  size_t next_table_ = 0;
  std::vector<Column> columns_;
  std::vector<std::string> column_names_;
  size_t next_column_ = 0;
  std::vector<Constraint> constraints_;
  std::vector<std::string> constraint_names_;
  std::vector<std::string> constraint_col_names_;
  size_t next_constraint_ = 0;
};

}  // namespace adbc::db2
