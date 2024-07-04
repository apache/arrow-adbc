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

#include <stdarg.h>
#include <stdbool.h>
#include <stdint.h>
#include <string.h>

#include <adbc.h>
#include "nanoarrow/nanoarrow.h"

#ifdef __cplusplus
extern "C" {
#endif

int AdbcStatusCodeToErrno(AdbcStatusCode code);

// If using mingw's c99-compliant printf, we need a different format-checking attribute
#if defined(__USE_MINGW_ANSI_STDIO) && defined(__MINGW_PRINTF_FORMAT)
#define ADBC_CHECK_PRINTF_ATTRIBUTE __attribute__((format(__MINGW_PRINTF_FORMAT, 2, 3)))
#elif defined(__GNUC__)
#define ADBC_CHECK_PRINTF_ATTRIBUTE __attribute__((format(printf, 2, 3)))
#else
#define ADBC_CHECK_PRINTF_ATTRIBUTE
#endif

/// Set error message using a format string.
void SetError(struct AdbcError* error, const char* format,
              ...) ADBC_CHECK_PRINTF_ATTRIBUTE;

/// Set error message using a format string.
void SetErrorVariadic(struct AdbcError* error, const char* format, va_list args);

/// Add an error detail.
void AppendErrorDetail(struct AdbcError* error, const char* key, const uint8_t* detail,
                       size_t detail_length);

int CommonErrorGetDetailCount(const struct AdbcError* error);
struct AdbcErrorDetail CommonErrorGetDetail(const struct AdbcError* error, int index);

struct StringBuilder {
  char* buffer;
  // Not including null terminator
  size_t size;
  size_t capacity;
};
int StringBuilderInit(struct StringBuilder* builder, size_t initial_size);

int ADBC_CHECK_PRINTF_ATTRIBUTE StringBuilderAppend(struct StringBuilder* builder,
                                                    const char* fmt, ...);
void StringBuilderReset(struct StringBuilder* builder);

#undef ADBC_CHECK_PRINTF_ATTRIBUTE

/// Wrap a single batch as a stream.
AdbcStatusCode BatchToArrayStream(struct ArrowArray* values, struct ArrowSchema* schema,
                                  struct ArrowArrayStream* stream,
                                  struct AdbcError* error);

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

struct AdbcGetObjectsUsage {
  struct ArrowStringView fk_catalog;
  struct ArrowStringView fk_db_schema;
  struct ArrowStringView fk_table;
  struct ArrowStringView fk_column_name;
};

struct AdbcGetObjectsConstraint {
  struct ArrowStringView constraint_name;
  struct ArrowStringView constraint_type;
  struct ArrowStringView* constraint_column_names;
  int n_column_names;
  struct AdbcGetObjectsUsage** constraint_column_usages;
  int n_column_usages;
};

struct AdbcGetObjectsColumn {
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

struct AdbcGetObjectsTable {
  struct ArrowStringView table_name;
  struct ArrowStringView table_type;
  struct AdbcGetObjectsColumn** table_columns;
  int n_table_columns;
  struct AdbcGetObjectsConstraint** table_constraints;
  int n_table_constraints;
};

struct AdbcGetObjectsSchema {
  struct ArrowStringView db_schema_name;
  struct AdbcGetObjectsTable** db_schema_tables;
  int n_db_schema_tables;
};

struct AdbcGetObjectsCatalog {
  struct ArrowStringView catalog_name;
  struct AdbcGetObjectsSchema** catalog_db_schemas;
  int n_db_schemas;
};

struct AdbcGetObjectsData {
  struct AdbcGetObjectsCatalog** catalogs;
  int n_catalogs;
  struct ArrowArrayView* catalog_name_array;
  struct ArrowArrayView* catalog_schemas_array;
  struct ArrowArrayView* db_schema_name_array;
  struct ArrowArrayView* db_schema_tables_array;
  struct ArrowArrayView* table_name_array;
  struct ArrowArrayView* table_type_array;
  struct ArrowArrayView* table_columns_array;
  struct ArrowArrayView* table_constraints_array;
  struct ArrowArrayView* column_name_array;
  struct ArrowArrayView* column_position_array;
  struct ArrowArrayView* column_remarks_array;
  struct ArrowArrayView* xdbc_data_type_array;
  struct ArrowArrayView* xdbc_type_name_array;
  struct ArrowArrayView* xdbc_column_size_array;
  struct ArrowArrayView* xdbc_decimal_digits_array;
  struct ArrowArrayView* xdbc_num_prec_radix_array;
  struct ArrowArrayView* xdbc_nullable_array;
  struct ArrowArrayView* xdbc_column_def_array;
  struct ArrowArrayView* xdbc_sql_data_type_array;
  struct ArrowArrayView* xdbc_datetime_sub_array;
  struct ArrowArrayView* xdbc_char_octet_length_array;
  struct ArrowArrayView* xdbc_is_nullable_array;
  struct ArrowArrayView* xdbc_scope_catalog_array;
  struct ArrowArrayView* xdbc_scope_schema_array;
  struct ArrowArrayView* xdbc_scope_table_array;
  struct ArrowArrayView* xdbc_is_autoincrement_array;
  struct ArrowArrayView* xdbc_is_generatedcolumn_array;
  struct ArrowArrayView* constraint_name_array;
  struct ArrowArrayView* constraint_type_array;
  struct ArrowArrayView* constraint_column_names_array;
  struct ArrowArrayView* constraint_column_name_array;
  struct ArrowArrayView* constraint_column_usages_array;
  struct ArrowArrayView* fk_catalog_array;
  struct ArrowArrayView* fk_db_schema_array;
  struct ArrowArrayView* fk_table_array;
  struct ArrowArrayView* fk_column_name_array;
};

// does not copy any data from array
// returns NULL on error
struct AdbcGetObjectsData* AdbcGetObjectsDataInit(struct ArrowArrayView* array_view);
void AdbcGetObjectsDataDelete(struct AdbcGetObjectsData* get_objects_data);

// returns NULL on error
// for now all arguments are required
struct AdbcGetObjectsCatalog* AdbcGetObjectsDataGetCatalogByName(
    struct AdbcGetObjectsData* get_objects_data, const char* const catalog_name);
struct AdbcGetObjectsSchema* AdbcGetObjectsDataGetSchemaByName(
    struct AdbcGetObjectsData* get_objects_data, const char* const catalog_name,
    const char* const schema_name);
struct AdbcGetObjectsTable* AdbcGetObjectsDataGetTableByName(
    struct AdbcGetObjectsData* get_objects_data, const char* const catalog_name,
    const char* const schema_name, const char* const table_name);
struct AdbcGetObjectsColumn* AdbcGetObjectsDataGetColumnByName(
    struct AdbcGetObjectsData* get_objects_data, const char* const catalog_name,
    const char* const schema_name, const char* const table_name,
    const char* const column_name);
struct AdbcGetObjectsConstraint* AdbcGetObjectsDataGetConstraintByName(
    struct AdbcGetObjectsData* get_objects_data, const char* const catalog_name,
    const char* const schema_name, const char* const table_name,
    const char* const constraint_name);

#ifdef __cplusplus
}
#endif
