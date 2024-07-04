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
#include <optional>
#include <string>
#include <string_view>
#include <utility>
#include <variant>
#include <vector>

#include <adbc.h>

#include "driver/framework/status.h"

namespace adbc::driver {

/// \defgroup adbc-framework-catalog Catalog Utilities
/// Utilities for implementing catalog/metadata-related functions.
///
/// @{

/// \brief The GetObjects level.
enum class GetObjectsDepth {
  kCatalogs,
  kSchemas,
  kTables,
  kColumns,
};

/// \brief Helper to implement GetObjects.
struct GetObjectsHelper {
  virtual ~GetObjectsHelper() = default;

  struct Table {
    std::string_view name;
    std::string_view type;
  };

  struct ColumnXdbc {
    std::optional<int16_t> xdbc_data_type;
    std::optional<std::string_view> xdbc_type_name;
    std::optional<int32_t> xdbc_column_size;
    std::optional<int16_t> xdbc_decimal_digits;
    std::optional<int16_t> xdbc_num_prec_radix;
    std::optional<int16_t> xdbc_nullable;
    std::optional<std::string_view> xdbc_column_def;
    std::optional<int16_t> xdbc_sql_data_type;
    std::optional<int16_t> xdbc_datetime_sub;
    std::optional<int32_t> xdbc_char_octet_length;
    std::optional<std::string_view> xdbc_is_nullable;
    std::optional<std::string_view> xdbc_scope_catalog;
    std::optional<std::string_view> xdbc_scope_schema;
    std::optional<std::string_view> xdbc_scope_table;
    std::optional<bool> xdbc_is_autoincrement;
    std::optional<bool> xdbc_is_generatedcolumn;
  };

  struct Column {
    std::string_view column_name;
    int32_t ordinal_position;
    std::optional<std::string_view> remarks;
    std::optional<ColumnXdbc> xdbc;
  };

  struct ConstraintUsage {
    std::optional<std::string_view> catalog;
    std::optional<std::string_view> schema;
    std::string_view table;
    std::string_view column;
  };

  struct Constraint {
    std::optional<std::string_view> name;
    std::string_view type;
    std::vector<std::string_view> column_names;
    std::optional<std::vector<ConstraintUsage>> usage;
  };

  Status Close() { return status::Ok(); }

  /// \brief Fetch all metadata needed.  The driver is free to delay loading
  /// but this gives it a chance to load data up front.
  virtual Status Load(GetObjectsDepth depth,
                      std::optional<std::string_view> catalog_filter,
                      std::optional<std::string_view> schema_filter,
                      std::optional<std::string_view> table_filter,
                      std::optional<std::string_view> column_filter,
                      const std::vector<std::string_view>& table_types) {
    return status::NotImplemented("GetObjects");
  }

  virtual Status LoadCatalogs() {
    return status::NotImplemented("GetObjects at depth = catalog");
  };

  virtual Result<std::optional<std::string_view>> NextCatalog() { return std::nullopt; }

  virtual Status LoadSchemas(std::string_view catalog) {
    return status::NotImplemented("GetObjects at depth = schema");
  };

  virtual Result<std::optional<std::string_view>> NextSchema() { return std::nullopt; }

  virtual Status LoadTables(std::string_view catalog, std::string_view schema) {
    return status::NotImplemented("GetObjects at depth = table");
  };

  virtual Result<std::optional<Table>> NextTable() { return std::nullopt; }

  virtual Status LoadColumns(std::string_view catalog, std::string_view schema,
                             std::string_view table) {
    return status::NotImplemented("GetObjects at depth = column");
  };

  virtual Result<std::optional<Column>> NextColumn() { return std::nullopt; }

  virtual Result<std::optional<Constraint>> NextConstraint() { return std::nullopt; }
};

struct InfoValue {
  uint32_t code;
  std::variant<std::string, int64_t> value;

  explicit InfoValue(uint32_t code, std::variant<std::string, int64_t> value)
      : code(code), value(std::move(value)) {}
};

Status AdbcInitConnectionGetInfoSchema(struct ArrowSchema* schema,
                                       struct ArrowArray* array);
Status AdbcConnectionGetInfoAppendString(struct ArrowArray* array, uint32_t info_code,
                                         std::string_view info_value);
Status AdbcConnectionGetInfoAppendInt(struct ArrowArray* array, uint32_t info_code,
                                      int64_t info_value);
Status AdbcInitConnectionObjectsSchema(struct ArrowSchema* schema);
/// @}

}  // namespace adbc::driver
