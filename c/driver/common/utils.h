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

#include <stdbool.h>
#include <stdint.h>

#include <adbc.h>
#include "nanoarrow/nanoarrow.h"

#if defined(__GNUC__)
#define SET_ERROR_ATTRIBUTE __attribute__((format(printf, 2, 3)))
#else
#define SET_ERROR_ATTRIBUTE
#endif

#ifdef __cplusplus
extern "C" {
#endif

/// Set error details using a format string.
void SetError(struct AdbcError* error, const char* format, ...) SET_ERROR_ATTRIBUTE;

#undef SET_ERROR_ATTRIBUTE

/// Wrap a single batch as a stream.
AdbcStatusCode BatchToArrayStream(struct ArrowArray* values, struct ArrowSchema* schema,
                                  struct ArrowArrayStream* stream,
                                  struct AdbcError* error);

struct StringBuilder {
  char* buffer;
  // Not including null terminator
  size_t size;
  size_t capacity;
};
int StringBuilderInit(struct StringBuilder* builder, size_t initial_size);

#if defined(__GNUC__)
#define ADBC_STRING_BUILDER_FORMAT_CHECK __attribute__((format(printf, 2, 3)))
#else
#define ADBC_STRING_BUILDER_FORMAT_CHECK
#endif
int ADBC_STRING_BUILDER_FORMAT_CHECK StringBuilderAppend(struct StringBuilder* builder,
                                                         const char* fmt, ...);
void StringBuilderReset(struct StringBuilder* builder);

/// Check an NanoArrow status code.
#define CHECK_NA(CODE, EXPR, ERROR)                                                 \
  do {                                                                              \
    ArrowErrorCode arrow_error_code = (EXPR);                                       \
    if (arrow_error_code != 0) {                                                    \
      SetError(ERROR, "%s failed: (%d) %s\nDetail: %s:%d", #EXPR, arrow_error_code, \
               strerror(arrow_error_code), __FILE__, __LINE__);                     \
      return ADBC_STATUS_##CODE;                                                    \
    }                                                                               \
  } while (0)

/// Check an NanoArrow status code.
#define CHECK_NA_DETAIL(CODE, EXPR, NA_ERROR, ERROR)                                    \
  do {                                                                                  \
    ArrowErrorCode arrow_error_code = (EXPR);                                           \
    if (arrow_error_code != 0) {                                                        \
      SetError(ERROR, "%s failed: (%d) %s: %s\nDetail: %s:%d", #EXPR, arrow_error_code, \
               strerror(arrow_error_code), (NA_ERROR)->message, __FILE__, __LINE__);    \
      return ADBC_STATUS_##CODE;                                                        \
    }                                                                                   \
  } while (0)

/// Check a generic status.
#define RAISE(CODE, EXPR, ERRMSG, ERROR)                                       \
  do {                                                                         \
    if (!(EXPR)) {                                                             \
      SetError(ERROR, "%s failed: %s\nDetail: %s:%d", #EXPR, ERRMSG, __FILE__, \
               __LINE__);                                                      \
      return ADBC_STATUS_##CODE;                                               \
    }                                                                          \
  } while (0)

/// Check an NanoArrow status code.
#define RAISE_NA(EXPR)                                  \
  do {                                                  \
    ArrowErrorCode arrow_error_code = (EXPR);           \
    if (arrow_error_code != 0) return arrow_error_code; \
  } while (0)

/// Check an ADBC status code.
#define RAISE_ADBC(EXPR)                                             \
  do {                                                               \
    AdbcStatusCode adbc_status_code = (EXPR);                        \
    if (adbc_status_code != ADBC_STATUS_OK) return adbc_status_code; \
  } while (0)

/// \defgroup adbc-connection-utils Connection Utilities
/// Utilities for implementing connection-related functions for drivers
///
/// @{
AdbcStatusCode AdbcInitConnectionGetInfoSchema(const uint32_t* info_codes,
                                               size_t info_codes_length,
                                               struct ArrowSchema* schema,
                                               struct ArrowArray* array,
                                               struct AdbcError* error);
AdbcStatusCode AdbcConnectionGetInfoAppendString(struct ArrowArray* array,
                                                 uint32_t info_code,
                                                 const char* info_value,
                                                 struct AdbcError* error);

AdbcStatusCode AdbcInitConnectionObjectsSchema(struct ArrowSchema* schema,
                                               struct AdbcError* error);
/// @}

struct AdbcGetInfoUsage {
  struct ArrowStringView fk_catalog;
  struct ArrowStringView fk_db_schema;
  struct ArrowStringView fk_table;
  struct ArrowStringView fk_column_name;
};

struct AdbcGetInfoConstraint {
  struct ArrowStringView constraint_name;
  struct ArrowStringView constraint_type;
  struct ArrowStringView* constraint_column_names;
  int n_column_names;
  struct AdbcGetInfoUsage* constraint_column_usages;
  int n_column_usages;
};

struct AdbcGetInfoColumn {
  struct ArrowStringView column_name;
  int32_t ordinal_position;
  struct ArrowStringView remarks;
  int16_t xdbc_data_type;
  struct ArrowStringView xdbc_type_name;
  int32_t xdbc_column_size;
  int16_t xdbc_decimal_digits;
  int16_t xdbc_num_prec_radix;
  int16_t xdbc_nullable;
  struct ArrowStringView xdbc_column_def;
  int16_t xdbc_sql_data_type;
  int16_t xdbc_datetime_sub;
  int32_t xdbc_char_octet_length;
  struct ArrowStringView xdbc_is_nullable;
  struct ArrowStringView xdbc_scope_catalog;
  struct ArrowStringView xdbc_scope_schema;
  struct ArrowStringView xdbc_scope_table;
  bool xdbc_is_autoincrement;
  bool xdbc_is_generatedcolumn;
};

struct AdbcGetInfoTable {
  struct ArrowStringView table_name;
  struct ArrowStringView table_type;
  struct AdbcGetInfoColumn* table_columns;
  int n_table_columns;
  struct AdbcGetInfoConstraint* table_constraints;
  int n_table_constraints;
};

struct AdbcGetInfoSchema {
  struct ArrowStringView db_schema_name;
  struct AdbcGetInfoTable* db_schema_tables;
  int n_db_schema_tables;
};

struct AdbcGetInfoCatalog {
  struct ArrowStringView catalog_name;
  struct AdbcGetInfoSchema* catalog_db_schemas;
  int n_db_schemas;
};

struct AdbcGetInfoData {
  struct AdbcGetInfoCatalog catalogs;
  int n_catalogs;
  struct ArrowArray* catalog_name_array;
  struct ArrowArray* catalog_schemas_array;
  struct ArrowArray* db_schema_name_array;
  struct ArrowArray* db_schema_tables_array;
  struct ArrowArray* table_name_array;
  struct ArrowArray* table_type_array;
  struct ArrowArray* table_columns_array;
  struct ArrowArray* table_constraints_array;
  struct ArrowArray* column_name_array;
  struct ArrowArray* column_position_array;
  struct ArrowArray* column_remarks_array;
  struct ArrowArray* constraint_name_array;
  struct ArrowArray* constraint_type_array;
  struct ArrowArray* constraint_column_names_array;
  struct ArrowArray* constraint_column_usages_array;
  struct ArrowArray* fk_catalog_array;
  struct ArrowArray* fk_db_schema_array;
  struct ArrowArray* fk_table_array;
  struct ArrowArray* fk_column_name_array;
};

int AdbcGetInfoDataInit(struct AdbcGetInfoData* get_info_data, struct ArrowArray* array);
void AdbcGetInfoDataDelete(struct AdbcGetInfoData* get_info_data);

#ifdef __cplusplus
}
#endif
