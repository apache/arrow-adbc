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

#include "driver/framework/catalog.h"

#include <nanoarrow/nanoarrow.h>

namespace adbc::driver {
Status AdbcInitConnectionGetInfoSchema(struct ArrowSchema* schema,
                                       struct ArrowArray* array) {
  ArrowSchemaInit(schema);
  UNWRAP_ERRNO(Internal, ArrowSchemaSetTypeStruct(schema, /*num_columns=*/2));

  UNWRAP_ERRNO(Internal, ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_UINT32));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(schema->children[0], "info_name"));
  schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;

  struct ArrowSchema* info_value = schema->children[1];
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetTypeUnion(info_value, NANOARROW_TYPE_DENSE_UNION, 6));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(info_value, "info_value"));

  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(info_value->children[0], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(info_value->children[0], "string_value"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(info_value->children[1], NANOARROW_TYPE_BOOL));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(info_value->children[1], "bool_value"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(info_value->children[2], NANOARROW_TYPE_INT64));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(info_value->children[2], "int64_value"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(info_value->children[3], NANOARROW_TYPE_INT32));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(info_value->children[3], "int32_bitmask"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(info_value->children[4], NANOARROW_TYPE_LIST));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(info_value->children[4], "string_list"));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetType(info_value->children[5], NANOARROW_TYPE_MAP));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(info_value->children[5], "int32_to_int32_list_map"));

  UNWRAP_ERRNO(Internal, ArrowSchemaSetType(info_value->children[4]->children[0],
                                            NANOARROW_TYPE_STRING));

  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(info_value->children[5]->children[0]->children[0],
                                  NANOARROW_TYPE_INT32));
  info_value->children[5]->children[0]->children[0]->flags &= ~ARROW_FLAG_NULLABLE;
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(info_value->children[5]->children[0]->children[1],
                                  NANOARROW_TYPE_LIST));
  UNWRAP_ERRNO(
      Internal,
      ArrowSchemaSetType(info_value->children[5]->children[0]->children[1]->children[0],
                         NANOARROW_TYPE_INT32));

  struct ArrowError na_error = {0};
  UNWRAP_NANOARROW(na_error, Internal,
                   ArrowArrayInitFromSchema(array, schema, &na_error));
  UNWRAP_ERRNO(Internal, ArrowArrayStartAppending(array));

  return status::Ok();
}

Status AdbcConnectionGetInfoAppendString(struct ArrowArray* array, uint32_t info_code,
                                         std::string_view info_value) {
  UNWRAP_ERRNO(Internal, ArrowArrayAppendUInt(array->children[0], info_code));
  // Append to type variant
  struct ArrowStringView value;
  value.data = info_value.data();
  value.size_bytes = static_cast<int64_t>(info_value.size());
  UNWRAP_ERRNO(Internal, ArrowArrayAppendString(array->children[1]->children[0], value));
  // Append type code/offset
  UNWRAP_ERRNO(Internal, ArrowArrayFinishUnionElement(array->children[1], /*type_id=*/0));
  return status::Ok();
}

Status AdbcConnectionGetInfoAppendInt(struct ArrowArray* array, uint32_t info_code,
                                      int64_t info_value) {
  UNWRAP_ERRNO(Internal, ArrowArrayAppendUInt(array->children[0], info_code));
  // Append to type variant
  UNWRAP_ERRNO(Internal,
               ArrowArrayAppendInt(array->children[1]->children[2], info_value));
  // Append type code/offset
  UNWRAP_ERRNO(Internal, ArrowArrayFinishUnionElement(array->children[1], /*type_id=*/2));
  return status::Ok();
}

Status AdbcInitConnectionObjectsSchema(struct ArrowSchema* schema) {
  ArrowSchemaInit(schema);
  UNWRAP_ERRNO(Internal, ArrowSchemaSetTypeStruct(schema, /*num_columns=*/2));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(schema->children[0], "catalog_name"));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetType(schema->children[1], NANOARROW_TYPE_LIST));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(schema->children[1], "catalog_db_schemas"));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetTypeStruct(schema->children[1]->children[0], 2));

  struct ArrowSchema* db_schema_schema = schema->children[1]->children[0];
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(db_schema_schema->children[0], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(db_schema_schema->children[0], "db_schema_name"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(db_schema_schema->children[1], NANOARROW_TYPE_LIST));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(db_schema_schema->children[1], "db_schema_tables"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetTypeStruct(db_schema_schema->children[1]->children[0], 4));

  struct ArrowSchema* table_schema = db_schema_schema->children[1]->children[0];
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(table_schema->children[0], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(table_schema->children[0], "table_name"));
  table_schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(table_schema->children[1], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(table_schema->children[1], "table_type"));
  table_schema->children[1]->flags &= ~ARROW_FLAG_NULLABLE;
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(table_schema->children[2], NANOARROW_TYPE_LIST));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(table_schema->children[2], "table_columns"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetTypeStruct(table_schema->children[2]->children[0], 19));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(table_schema->children[3], NANOARROW_TYPE_LIST));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(table_schema->children[3], "table_constraints"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetTypeStruct(table_schema->children[3]->children[0], 4));

  struct ArrowSchema* column_schema = table_schema->children[2]->children[0];
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[0], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(column_schema->children[0], "column_name"));
  column_schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[1], NANOARROW_TYPE_INT32));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[1], "ordinal_position"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[2], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(column_schema->children[2], "remarks"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[3], NANOARROW_TYPE_INT16));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[3], "xdbc_data_type"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[4], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[4], "xdbc_type_name"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[5], NANOARROW_TYPE_INT32));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[5], "xdbc_column_size"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[6], NANOARROW_TYPE_INT16));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[6], "xdbc_decimal_digits"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[7], NANOARROW_TYPE_INT16));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[7], "xdbc_num_prec_radix"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[8], NANOARROW_TYPE_INT16));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(column_schema->children[8], "xdbc_nullable"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[9], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[9], "xdbc_column_def"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[10], NANOARROW_TYPE_INT16));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[10], "xdbc_sql_data_type"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[11], NANOARROW_TYPE_INT16));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[11], "xdbc_datetime_sub"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[12], NANOARROW_TYPE_INT32));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[12], "xdbc_char_octet_length"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[13], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[13], "xdbc_is_nullable"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[14], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[14], "xdbc_scope_catalog"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[15], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[15], "xdbc_scope_schema"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[16], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[16], "xdbc_scope_table"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[17], NANOARROW_TYPE_BOOL));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(column_schema->children[17], "xdbc_is_autoincrement"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(column_schema->children[18], NANOARROW_TYPE_BOOL));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(column_schema->children[18],
                                            "xdbc_is_generatedcolumn"));

  struct ArrowSchema* constraint_schema = table_schema->children[3]->children[0];
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(constraint_schema->children[0], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(constraint_schema->children[0], "constraint_name"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(constraint_schema->children[1], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetName(constraint_schema->children[1], "constraint_type"));
  constraint_schema->children[1]->flags &= ~ARROW_FLAG_NULLABLE;
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(constraint_schema->children[2], NANOARROW_TYPE_LIST));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(constraint_schema->children[2],
                                            "constraint_column_names"));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetType(constraint_schema->children[2]->children[0],
                                            NANOARROW_TYPE_STRING));
  constraint_schema->children[2]->flags &= ~ARROW_FLAG_NULLABLE;
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(constraint_schema->children[3], NANOARROW_TYPE_LIST));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(constraint_schema->children[3],
                                            "constraint_column_usage"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetTypeStruct(constraint_schema->children[3]->children[0], 4));

  struct ArrowSchema* usage_schema = constraint_schema->children[3]->children[0];
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(usage_schema->children[0], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(usage_schema->children[0], "fk_catalog"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(usage_schema->children[1], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(usage_schema->children[1], "fk_db_schema"));
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(usage_schema->children[2], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(usage_schema->children[2], "fk_table"));
  usage_schema->children[2]->flags &= ~ARROW_FLAG_NULLABLE;
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(usage_schema->children[3], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(usage_schema->children[3], "fk_column_name"));
  usage_schema->children[3]->flags &= ~ARROW_FLAG_NULLABLE;

  return status::Ok();
}
}  // namespace adbc::driver
