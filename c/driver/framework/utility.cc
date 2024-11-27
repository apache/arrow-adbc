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

#include "driver/framework/utility.h"

#include <string>
#include <vector>

#include "arrow-adbc/adbc.h"
#include "nanoarrow/nanoarrow.hpp"

namespace adbc::driver {

void MakeEmptyStream(ArrowSchema* schema, ArrowArrayStream* out) {
  nanoarrow::EmptyArrayStream(schema).ToArrayStream(out);
}

void MakeArrayStream(ArrowSchema* schema, ArrowArray* array, ArrowArrayStream* out) {
  if (array->length == 0) {
    ArrowArrayRelease(array);
    std::memset(array, 0, sizeof(ArrowArray));

    MakeEmptyStream(schema, out);
  } else {
    nanoarrow::VectorArrayStream(schema, array).ToArrayStream(out);
  }
}

Status MakeTableTypesStream(const std::vector<std::string>& table_types,
                            ArrowArrayStream* out) {
  nanoarrow::UniqueArray array;
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());

  UNWRAP_ERRNO(Internal, ArrowSchemaSetType(schema.get(), NANOARROW_TYPE_STRUCT));
  UNWRAP_ERRNO(Internal, ArrowSchemaAllocateChildren(schema.get(), /*num_columns=*/1));
  ArrowSchemaInit(schema.get()->children[0]);
  UNWRAP_ERRNO(Internal,
               ArrowSchemaSetType(schema.get()->children[0], NANOARROW_TYPE_STRING));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(schema.get()->children[0], "table_type"));
  schema.get()->children[0]->flags &= ~ARROW_FLAG_NULLABLE;

  UNWRAP_ERRNO(Internal, ArrowArrayInitFromSchema(array.get(), schema.get(), NULL));
  UNWRAP_ERRNO(Internal, ArrowArrayStartAppending(array.get()));

  for (std::string const& table_type : table_types) {
    UNWRAP_ERRNO(Internal, ArrowArrayAppendString(array->children[0],
                                                  ArrowCharView(table_type.c_str())));
    UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(array.get()));
  }

  UNWRAP_ERRNO(Internal, ArrowArrayFinishBuildingDefault(array.get(), nullptr));
  MakeArrayStream(schema.get(), array.get(), out);
  return status::Ok();
}

namespace {
Status MakeGetInfoInit(ArrowSchema* schema, ArrowArray* array) {
  ArrowSchemaInit(schema);
  UNWRAP_ERRNO(Internal, ArrowSchemaSetTypeStruct(schema, /*num_columns=*/2));

  UNWRAP_ERRNO(Internal, ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_UINT32));
  UNWRAP_ERRNO(Internal, ArrowSchemaSetName(schema->children[0], "info_name"));
  schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;

  ArrowSchema* info_value = schema->children[1];
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

  UNWRAP_ERRNO(Internal, ArrowArrayInitFromSchema(array, schema, nullptr));
  UNWRAP_ERRNO(Internal, ArrowArrayStartAppending(array));

  return status::Ok();
}

Status MakeGetInfoAppendString(ArrowArray* array, uint32_t info_code,
                               std::string_view info_value) {
  UNWRAP_ERRNO(Internal, ArrowArrayAppendUInt(array->children[0], info_code));
  // Append to type variant
  ArrowStringView value;
  value.data = info_value.data();
  value.size_bytes = static_cast<int64_t>(info_value.size());
  UNWRAP_ERRNO(Internal, ArrowArrayAppendString(array->children[1]->children[0], value));
  // Append type code/offset
  UNWRAP_ERRNO(Internal, ArrowArrayFinishUnionElement(array->children[1], /*type_id=*/0));
  return status::Ok();
}

Status MakeGetInfoAppendInt(ArrowArray* array, uint32_t info_code, int64_t info_value) {
  UNWRAP_ERRNO(Internal, ArrowArrayAppendUInt(array->children[0], info_code));
  // Append to type variant
  UNWRAP_ERRNO(Internal,
               ArrowArrayAppendInt(array->children[1]->children[2], info_value));
  // Append type code/offset
  UNWRAP_ERRNO(Internal, ArrowArrayFinishUnionElement(array->children[1], /*type_id=*/2));
  return status::Ok();
}
}  // namespace

Status MakeGetInfoStream(const std::vector<InfoValue>& infos, ArrowArrayStream* out) {
  nanoarrow::UniqueSchema schema;
  nanoarrow::UniqueArray array;

  UNWRAP_STATUS(MakeGetInfoInit(schema.get(), array.get()));

  for (const auto& info : infos) {
    UNWRAP_STATUS(std::visit(
        [&](auto&& value) -> Status {
          using T = std::decay_t<decltype(value)>;
          if constexpr (std::is_same_v<T, std::string>) {
            return MakeGetInfoAppendString(array.get(), info.code, value);
          } else if constexpr (std::is_same_v<T, int64_t>) {
            return MakeGetInfoAppendInt(array.get(), info.code, value);
          } else {
            static_assert(!sizeof(T), "info value type not implemented");
          }
        },
        info.value));
    UNWRAP_ERRNO(Internal, ArrowArrayFinishElement(array.get()));
  }

  ArrowError na_error = {0};
  UNWRAP_NANOARROW(na_error, Internal,
                   ArrowArrayFinishBuildingDefault(array.get(), &na_error));
  MakeArrayStream(schema.get(), array.get(), out);
  return status::Ok();
}

}  // namespace adbc::driver
