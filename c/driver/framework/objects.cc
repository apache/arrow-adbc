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

#include "driver/framework/objects.h"

#include <string_view>

#include "driver/framework/catalog.h"
#include "driver/framework/status.h"

namespace adbc::driver {

namespace {
/// \brief A helper to convert std::string_view to Nanoarrow's ArrowStringView.
ArrowStringView ToStringView(std::string_view s) {
  return {
      s.data(),
      static_cast<int64_t>(s.size()),
  };
}

/// \brief A helper to append an optional value to an ArrowArray.
template <typename T>
Status AppendOptional(ArrowArray* array, std::optional<T> value) {
  if (!value) {
    UNWRAP_ERRNO(Internal, ArrowArrayAppendNull(array, 1));
  } else if constexpr (std::is_same_v<T, bool>) {
    UNWRAP_ERRNO(Internal, ArrowArrayAppendInt(array, *value));
  } else if constexpr (std::is_same_v<T, int16_t>) {
    UNWRAP_ERRNO(Internal, ArrowArrayAppendInt(array, *value));
  } else if constexpr (std::is_same_v<T, int32_t>) {
    UNWRAP_ERRNO(Internal, ArrowArrayAppendInt(array, *value));
  } else if constexpr (std::is_same_v<T, std::string_view>) {
    UNWRAP_ERRNO(Internal, ArrowArrayAppendString(array, ToStringView(*value)));
  } else {
    static_assert(!sizeof(T), "unimplemented type");
  }
  return status::Ok();
}

struct GetObjectsBuilder {
  GetObjectsBuilder(GetObjectsHelper* helper, GetObjectsDepth depth,
                    std::optional<std::string_view> catalog_filter,
                    std::optional<std::string_view> schema_filter,
                    std::optional<std::string_view> table_filter,
                    std::optional<std::string_view> column_filter,
                    const std::vector<std::string_view>& table_types,
                    struct ArrowSchema* schema, struct ArrowArray* array)
      : helper(helper),
        depth(depth),
        catalog_filter(catalog_filter),
        schema_filter(schema_filter),
        table_filter(table_filter),
        column_filter(column_filter),
        table_types(table_types),
        schema(schema),
        array(array) {
    std::memset(&na_error, 0, sizeof(na_error));
  }

  Status Build() {
    UNWRAP_STATUS(InitArrowArray());
    UNWRAP_STATUS(helper->Load(depth, catalog_filter, schema_filter, table_filter,
                               column_filter, table_types));

    catalog_name_col = array->children[0];
    catalog_db_schemas_col = array->children[1];
    catalog_db_schemas_items = catalog_db_schemas_col->children[0];
    db_schema_name_col = catalog_db_schemas_items->children[0];
    db_schema_tables_col = catalog_db_schemas_items->children[1];
    schema_table_items = db_schema_tables_col->children[0];
    table_name_col = schema_table_items->children[0];
    table_type_col = schema_table_items->children[1];

    table_columns_col = schema_table_items->children[2];
    table_columns_items = table_columns_col->children[0];
    column_name_col = table_columns_items->children[0];
    column_position_col = table_columns_items->children[1];
    column_remarks_col = table_columns_items->children[2];

    table_constraints_col = schema_table_items->children[3];
    table_constraints_items = table_constraints_col->children[0];
    constraint_name_col = table_constraints_items->children[0];
    constraint_type_col = table_constraints_items->children[1];

    constraint_column_names_col = table_constraints_items->children[2];
    constraint_column_name_col = constraint_column_names_col->children[0];

    constraint_column_usages_col = table_constraints_items->children[3];
    constraint_column_usage_items = constraint_column_usages_col->children[0];
    fk_catalog_col = constraint_column_usage_items->children[0];
    fk_db_schema_col = constraint_column_usage_items->children[1];
    fk_table_col = constraint_column_usage_items->children[2];
    fk_column_name_col = constraint_column_usage_items->children[3];

    UNWRAP_STATUS(AppendCatalogs());
    return FinishArrowArray();
  }

 private:
  Status InitArrowArray() {
    UNWRAP_STATUS(AdbcInitConnectionObjectsSchema(schema));
    UNWRAP_NANOARROW(na_error, Internal,
                     ArrowArrayInitFromSchema(array, schema, &na_error));
    UNWRAP_ERRNO(Internal, ArrowArrayStartAppending(array));
    return status::Ok();
  }

  Status AppendCatalogs() {
    UNWRAP_STATUS(helper->LoadCatalogs());
    while (true) {
      UNWRAP_RESULT(auto maybe_catalog, helper->NextCatalog());
      if (!maybe_catalog.has_value()) break;

      UNWRAP_ERRNO(Internal, ArrowArrayAppendString(catalog_name_col,
                                                    ToStringView(*maybe_catalog)));
      if (depth == GetObjectsDepth::kCatalogs) {
        UNWRAP_ERRNO(Internal, ArrowArrayAppendNull(catalog_db_schemas_col, 1));
      } else {
        UNWRAP_STATUS(AppendSchemas(*maybe_catalog));
      }
      UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(array));
    }
    return status::Ok();
  }

  Status AppendSchemas(std::string_view catalog) {
    UNWRAP_STATUS(helper->LoadSchemas(catalog));
    while (true) {
      UNWRAP_RESULT(auto maybe_schema, helper->NextSchema());
      if (!maybe_schema.has_value()) break;

      UNWRAP_ERRNO(Internal, ArrowArrayAppendString(db_schema_name_col,
                                                    ToStringView(*maybe_schema)));

      if (depth == GetObjectsDepth::kSchemas) {
        UNWRAP_ERRNO(Internal, ArrowArrayAppendNull(db_schema_tables_col, 1));
      } else {
        UNWRAP_STATUS(AppendTables(catalog, *maybe_schema));
      }
      UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(catalog_db_schemas_items));
    }

    UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(catalog_db_schemas_col));
    return status::Ok();
  }

  Status AppendTables(std::string_view catalog, std::string_view schema) {
    UNWRAP_STATUS(helper->LoadTables(catalog, schema));
    while (true) {
      UNWRAP_RESULT(auto maybe_table, helper->NextTable());
      if (!maybe_table.has_value()) break;

      UNWRAP_ERRNO(Internal, ArrowArrayAppendString(table_name_col,
                                                    ToStringView(maybe_table->name)));
      UNWRAP_ERRNO(Internal, ArrowArrayAppendString(table_type_col,
                                                    ToStringView(maybe_table->type)));
      if (depth == GetObjectsDepth::kTables) {
        UNWRAP_ERRNO(Internal, ArrowArrayAppendNull(table_columns_col, 1));
        UNWRAP_ERRNO(Internal, ArrowArrayAppendNull(table_constraints_col, 1));
      } else {
        UNWRAP_STATUS(AppendColumns(catalog, schema, maybe_table->name));
        UNWRAP_STATUS(AppendConstraints(catalog, schema, maybe_table->name));
      }
      UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(schema_table_items));
    }

    UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(db_schema_tables_col));
    return status::Ok();
  }

  Status AppendColumns(std::string_view catalog, std::string_view schema,
                       std::string_view table) {
    UNWRAP_STATUS(helper->LoadColumns(catalog, schema, table));
    while (true) {
      UNWRAP_RESULT(auto maybe_column, helper->NextColumn());
      if (!maybe_column.has_value()) break;
      const auto& column = *maybe_column;

      UNWRAP_ERRNO(Internal, ArrowArrayAppendString(column_name_col,
                                                    ToStringView(column.column_name)));
      UNWRAP_ERRNO(Internal,
                   ArrowArrayAppendInt(column_position_col, column.ordinal_position));
      if (column.remarks) {
        UNWRAP_ERRNO(Internal, ArrowArrayAppendString(column_remarks_col,
                                                      ToStringView(*column.remarks)));
      } else {
        UNWRAP_ERRNO(Internal, ArrowArrayAppendNull(column_remarks_col, 1));
      }

      if (column.xdbc) {
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[3],
                                     column.xdbc->xdbc_data_type));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[4],
                                     column.xdbc->xdbc_type_name));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[5],
                                     column.xdbc->xdbc_column_size));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[6],
                                     column.xdbc->xdbc_decimal_digits));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[7],
                                     column.xdbc->xdbc_num_prec_radix));
        UNWRAP_STATUS(
            AppendOptional(table_columns_items->children[8], column.xdbc->xdbc_nullable));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[9],
                                     column.xdbc->xdbc_column_def));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[10],
                                     column.xdbc->xdbc_sql_data_type));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[11],
                                     column.xdbc->xdbc_datetime_sub));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[12],
                                     column.xdbc->xdbc_char_octet_length));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[13],
                                     column.xdbc->xdbc_is_nullable));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[14],
                                     column.xdbc->xdbc_scope_catalog));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[15],
                                     column.xdbc->xdbc_scope_schema));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[16],
                                     column.xdbc->xdbc_scope_table));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[17],
                                     column.xdbc->xdbc_is_autoincrement));
        UNWRAP_STATUS(AppendOptional(table_columns_items->children[18],
                                     column.xdbc->xdbc_is_generatedcolumn));
      } else {
        for (auto i = 3; i < 19; i++) {
          UNWRAP_ERRNO(Internal,
                       ArrowArrayAppendNull(table_columns_items->children[i], 1));
        }
      }
      UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(table_columns_items));
    }

    UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(table_columns_col));
    return status::Ok();
  }

  Status AppendConstraints(std::string_view catalog, std::string_view schema,
                           std::string_view table) {
    while (true) {
      UNWRAP_RESULT(auto maybe_constraint, helper->NextConstraint());
      if (!maybe_constraint.has_value()) break;
      // XXX: copy to make gcc 12.2's -Wmaybe-uninitialized happy (only
      // happens with optimizations enabled)
      const auto constraint = *maybe_constraint;

      if (constraint.name) {
        UNWRAP_ERRNO(Internal, ArrowArrayAppendString(constraint_name_col,
                                                      ToStringView(*constraint.name)));
      } else {
        UNWRAP_ERRNO(Internal, ArrowArrayAppendNull(constraint_name_col, 1));
      }

      UNWRAP_ERRNO(Internal, ArrowArrayAppendString(constraint_type_col,
                                                    ToStringView(constraint.type)));

      for (const auto& constraint_column_name : constraint.column_names) {
        UNWRAP_ERRNO(Internal,
                     ArrowArrayAppendString(constraint_column_name_col,
                                            ToStringView(constraint_column_name)));
      }
      UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(constraint_column_names_col));

      if (constraint.usage) {
        for (const auto& usage : constraint.usage.value()) {
          if (usage.catalog) {
            UNWRAP_ERRNO(Internal, ArrowArrayAppendString(fk_catalog_col,
                                                          ToStringView(*usage.catalog)));
          } else {
            UNWRAP_ERRNO(Internal, ArrowArrayAppendNull(fk_catalog_col, 1));
          }
          if (usage.schema) {
            UNWRAP_ERRNO(Internal, ArrowArrayAppendString(fk_db_schema_col,
                                                          ToStringView(*usage.schema)));
          } else {
            UNWRAP_ERRNO(Internal, ArrowArrayAppendNull(fk_db_schema_col, 1));
          }
          UNWRAP_ERRNO(Internal,
                       ArrowArrayAppendString(fk_table_col, ToStringView(usage.table)));
          UNWRAP_ERRNO(Internal, ArrowArrayAppendString(fk_column_name_col,
                                                        ToStringView(usage.column)));

          UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(constraint_column_usage_items));
        }
        UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(constraint_column_usages_col));
      } else {
        UNWRAP_ERRNO(Internal, ArrowArrayAppendNull(constraint_column_usages_col, 1));
      }
      UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(table_constraints_items));
    }

    UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(table_constraints_col));
    return status::Ok();
  }

  Status FinishArrowArray() {
    UNWRAP_NANOARROW(na_error, Internal,
                     ArrowArrayFinishBuildingDefault(array, &na_error));
    return status::Ok();
  }

  GetObjectsHelper* helper;
  GetObjectsDepth depth;
  std::optional<std::string_view> catalog_filter;
  std::optional<std::string_view> schema_filter;
  std::optional<std::string_view> table_filter;
  std::optional<std::string_view> column_filter;
  const std::vector<std::string_view>& table_types;
  struct ArrowSchema* schema;
  struct ArrowArray* array;
  struct ArrowError na_error;
  struct ArrowArray* catalog_name_col;
  struct ArrowArray* catalog_db_schemas_col;
  struct ArrowArray* catalog_db_schemas_items;
  struct ArrowArray* db_schema_name_col;
  struct ArrowArray* db_schema_tables_col;
  struct ArrowArray* schema_table_items;
  struct ArrowArray* table_name_col;
  struct ArrowArray* table_type_col;
  struct ArrowArray* table_columns_col;
  struct ArrowArray* table_columns_items;
  struct ArrowArray* column_name_col;
  struct ArrowArray* column_position_col;
  struct ArrowArray* column_remarks_col;
  struct ArrowArray* table_constraints_col;
  struct ArrowArray* table_constraints_items;
  struct ArrowArray* constraint_name_col;
  struct ArrowArray* constraint_type_col;
  struct ArrowArray* constraint_column_names_col;
  struct ArrowArray* constraint_column_name_col;
  struct ArrowArray* constraint_column_usages_col;
  struct ArrowArray* constraint_column_usage_items;
  struct ArrowArray* fk_catalog_col;
  struct ArrowArray* fk_db_schema_col;
  struct ArrowArray* fk_table_col;
  struct ArrowArray* fk_column_name_col;
};
}  // namespace

Status BuildGetObjects(GetObjectsHelper* helper, GetObjectsDepth depth,
                       std::optional<std::string_view> catalog_filter,
                       std::optional<std::string_view> schema_filter,
                       std::optional<std::string_view> table_filter,
                       std::optional<std::string_view> column_filter,
                       const std::vector<std::string_view>& table_types,
                       struct ArrowSchema* schema, struct ArrowArray* array) {
  return GetObjectsBuilder(helper, depth, catalog_filter, schema_filter, table_filter,
                           column_filter, table_types, schema, array)
      .Build();
}
}  // namespace adbc::driver
