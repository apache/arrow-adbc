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

#include "utils.h"

#include <assert.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <adbc.h>

static size_t kErrorBufferSize = 1024;

int AdbcStatusCodeToErrno(AdbcStatusCode code) {
  switch (code) {
    case ADBC_STATUS_OK:
      return 0;
    case ADBC_STATUS_UNKNOWN:
      return EIO;
    case ADBC_STATUS_NOT_IMPLEMENTED:
      return ENOTSUP;
    case ADBC_STATUS_NOT_FOUND:
      return ENOENT;
    case ADBC_STATUS_ALREADY_EXISTS:
      return EEXIST;
    case ADBC_STATUS_INVALID_ARGUMENT:
    case ADBC_STATUS_INVALID_STATE:
      return EINVAL;
    case ADBC_STATUS_INVALID_DATA:
    case ADBC_STATUS_INTEGRITY:
    case ADBC_STATUS_INTERNAL:
    case ADBC_STATUS_IO:
      return EIO;
    case ADBC_STATUS_CANCELLED:
      return ECANCELED;
    case ADBC_STATUS_TIMEOUT:
      return ETIMEDOUT;
    case ADBC_STATUS_UNAUTHENTICATED:
      // FreeBSD/macOS have EAUTH, but not other platforms
    case ADBC_STATUS_UNAUTHORIZED:
      return EACCES;
    default:
      return EIO;
  }
}

/// For ADBC 1.1.0, the structure held in private_data.
struct AdbcErrorDetails {
  char* message;

  // The metadata keys (may be NULL).
  char** keys;
  // The metadata values (may be NULL).
  uint8_t** values;
  // The metadata value lengths (may be NULL).
  size_t* lengths;
  // The number of initialized metadata.
  int count;
  // The length of the keys/values/lengths arrays above.
  int capacity;
};

bool StringViewEquals(struct ArrowStringView stringView, const char* str) {
  int64_t len = (int64_t)strlen(str);
  return len == stringView.size_bytes &&
         !strncmp(stringView.data, str, stringView.size_bytes);
}

static void ReleaseErrorWithDetails(struct AdbcError* error) {
  struct AdbcErrorDetails* details = (struct AdbcErrorDetails*)error->private_data;
  free(details->message);

  for (int i = 0; i < details->count; i++) {
    free(details->keys[i]);
    free(details->values[i]);
  }

  free(details->keys);
  free(details->values);
  free(details->lengths);
  free(error->private_data);
  *error = ADBC_ERROR_INIT;
}

static void ReleaseError(struct AdbcError* error) {
  free(error->message);
  error->message = NULL;
  error->release = NULL;
}

void SetError(struct AdbcError* error, const char* format, ...) {
  va_list args;
  va_start(args, format);
  SetErrorVariadic(error, format, args);
  va_end(args);
}

void SetErrorVariadic(struct AdbcError* error, const char* format, va_list args) {
  if (!error) return;
  if (error->release) {
    // TODO: combine the errors if possible
    error->release(error);
  }

  if (error->vendor_code == ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA) {
    error->private_data = malloc(sizeof(struct AdbcErrorDetails));
    if (!error->private_data) return;

    struct AdbcErrorDetails* details = (struct AdbcErrorDetails*)error->private_data;

    details->message = malloc(kErrorBufferSize);
    if (!details->message) {
      free(details);
      return;
    }
    details->keys = NULL;
    details->values = NULL;
    details->lengths = NULL;
    details->count = 0;
    details->capacity = 0;

    error->message = details->message;
    error->release = &ReleaseErrorWithDetails;
  } else {
    error->message = malloc(kErrorBufferSize);
    if (!error->message) return;

    error->release = &ReleaseError;
  }

  vsnprintf(error->message, kErrorBufferSize, format, args);
}

void AppendErrorDetail(struct AdbcError* error, const char* key, const uint8_t* detail,
                       size_t detail_length) {
  if (error->release != ReleaseErrorWithDetails) return;

  struct AdbcErrorDetails* details = (struct AdbcErrorDetails*)error->private_data;
  if (details->count >= details->capacity) {
    int new_capacity = (details->capacity == 0) ? 4 : (2 * details->capacity);
    char** new_keys = calloc(new_capacity, sizeof(char*));
    if (!new_keys) {
      return;
    }

    uint8_t** new_values = calloc(new_capacity, sizeof(uint8_t*));
    if (!new_values) {
      free(new_keys);
      return;
    }

    size_t* new_lengths = calloc(new_capacity, sizeof(size_t*));
    if (!new_lengths) {
      free(new_keys);
      free(new_values);
      return;
    }

    if (details->keys != NULL) {
      memcpy(new_keys, details->keys, sizeof(char*) * details->count);
      free(details->keys);
    }
    details->keys = new_keys;

    if (details->values != NULL) {
      memcpy(new_values, details->values, sizeof(uint8_t*) * details->count);
      free(details->values);
    }
    details->values = new_values;

    if (details->lengths != NULL) {
      memcpy(new_lengths, details->lengths, sizeof(size_t) * details->count);
      free(details->lengths);
    }
    details->lengths = new_lengths;

    details->capacity = new_capacity;
  }

  char* key_data = strdup(key);
  if (!key_data) return;
  uint8_t* value_data = malloc(detail_length);
  if (!value_data) {
    free(key_data);
    return;
  }
  memcpy(value_data, detail, detail_length);

  int index = details->count;
  details->keys[index] = key_data;
  details->values[index] = value_data;
  details->lengths[index] = detail_length;

  details->count++;
}

int CommonErrorGetDetailCount(const struct AdbcError* error) {
  if (error->release != ReleaseErrorWithDetails) {
    return 0;
  }
  struct AdbcErrorDetails* details = (struct AdbcErrorDetails*)error->private_data;
  return details->count;
}

struct AdbcErrorDetail CommonErrorGetDetail(const struct AdbcError* error, int index) {
  if (error->release != ReleaseErrorWithDetails) {
    return (struct AdbcErrorDetail){NULL, NULL, 0};
  }
  struct AdbcErrorDetails* details = (struct AdbcErrorDetails*)error->private_data;
  if (index < 0 || index >= details->count) {
    return (struct AdbcErrorDetail){NULL, NULL, 0};
  }
  return (struct AdbcErrorDetail){
      .key = details->keys[index],
      .value = details->values[index],
      .value_length = details->lengths[index],
  };
}

struct SingleBatchArrayStream {
  struct ArrowSchema schema;
  struct ArrowArray batch;
};
static const char* SingleBatchArrayStreamGetLastError(struct ArrowArrayStream* stream) {
  (void)stream;
  return NULL;
}
static int SingleBatchArrayStreamGetNext(struct ArrowArrayStream* stream,
                                         struct ArrowArray* batch) {
  if (!stream || !stream->private_data) return EINVAL;
  struct SingleBatchArrayStream* impl =
      (struct SingleBatchArrayStream*)stream->private_data;

  memcpy(batch, &impl->batch, sizeof(*batch));
  memset(&impl->batch, 0, sizeof(*batch));
  return 0;
}
static int SingleBatchArrayStreamGetSchema(struct ArrowArrayStream* stream,
                                           struct ArrowSchema* schema) {
  if (!stream || !stream->private_data) return EINVAL;
  struct SingleBatchArrayStream* impl =
      (struct SingleBatchArrayStream*)stream->private_data;

  return ArrowSchemaDeepCopy(&impl->schema, schema);
}
static void SingleBatchArrayStreamRelease(struct ArrowArrayStream* stream) {
  if (!stream || !stream->private_data) return;
  struct SingleBatchArrayStream* impl =
      (struct SingleBatchArrayStream*)stream->private_data;
  impl->schema.release(&impl->schema);
  if (impl->batch.release) impl->batch.release(&impl->batch);
  free(impl);

  memset(stream, 0, sizeof(*stream));
}

AdbcStatusCode BatchToArrayStream(struct ArrowArray* values, struct ArrowSchema* schema,
                                  struct ArrowArrayStream* stream,
                                  struct AdbcError* error) {
  if (!values->release) {
    SetError(error, "ArrowArray is not initialized");
    return ADBC_STATUS_INTERNAL;
  } else if (!schema->release) {
    SetError(error, "ArrowSchema is not initialized");
    return ADBC_STATUS_INTERNAL;
  } else if (stream->release) {
    SetError(error, "ArrowArrayStream is already initialized");
    return ADBC_STATUS_INTERNAL;
  }

  struct SingleBatchArrayStream* impl =
      (struct SingleBatchArrayStream*)malloc(sizeof(*impl));
  memcpy(&impl->schema, schema, sizeof(*schema));
  memcpy(&impl->batch, values, sizeof(*values));
  memset(schema, 0, sizeof(*schema));
  memset(values, 0, sizeof(*values));
  stream->private_data = impl;
  stream->get_last_error = SingleBatchArrayStreamGetLastError;
  stream->get_next = SingleBatchArrayStreamGetNext;
  stream->get_schema = SingleBatchArrayStreamGetSchema;
  stream->release = SingleBatchArrayStreamRelease;

  return ADBC_STATUS_OK;
}

int StringBuilderInit(struct StringBuilder* builder, size_t initial_size) {
  builder->buffer = (char*)malloc(initial_size);
  if (builder->buffer == NULL) return errno;

  builder->size = 0;
  builder->capacity = initial_size;

  return 0;
}
int StringBuilderAppend(struct StringBuilder* builder, const char* fmt, ...) {
  va_list argptr;
  int bytes_available = (int)builder->capacity - (int)builder->size;

  va_start(argptr, fmt);
  int n = vsnprintf(builder->buffer + builder->size, bytes_available, fmt, argptr);
  va_end(argptr);

  if (n < 0) {
    return errno;
  } else if (n >= bytes_available) {  // output was truncated
    int bytes_needed = n - bytes_available + 1;
    builder->buffer = (char*)realloc(builder->buffer, builder->capacity + bytes_needed);
    if (builder->buffer == NULL) return errno;

    builder->capacity += bytes_needed;

    va_start(argptr, fmt);
    int ret = vsnprintf(builder->buffer + builder->size, n + 1, fmt, argptr);
    if (ret < 0) {
      va_end(argptr);
      return errno;
    }

    va_end(argptr);
  }
  builder->size += n;

  return 0;
}
void StringBuilderReset(struct StringBuilder* builder) {
  if (builder->buffer) {
    free(builder->buffer);
  }
  memset(builder, 0, sizeof(*builder));
}

AdbcStatusCode AdbcInitConnectionGetInfoSchema(struct ArrowSchema* schema,
                                               struct ArrowArray* array,
                                               struct AdbcError* error) {
  // TODO: use C equivalent of UniqueSchema to avoid incomplete schema
  // on error
  ArrowSchemaInit(schema);
  CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(schema, /*num_columns=*/2), error);

  CHECK_NA(INTERNAL, ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_UINT32),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(schema->children[0], "info_name"), error);
  schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;

  struct ArrowSchema* info_value = schema->children[1];
  CHECK_NA(INTERNAL, ArrowSchemaSetTypeUnion(info_value, NANOARROW_TYPE_DENSE_UNION, 6),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value, "info_value"), error);

  CHECK_NA(INTERNAL, ArrowSchemaSetType(info_value->children[0], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[0], "string_value"), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(info_value->children[1], NANOARROW_TYPE_BOOL),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[1], "bool_value"), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(info_value->children[2], NANOARROW_TYPE_INT64),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[2], "int64_value"), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(info_value->children[3], NANOARROW_TYPE_INT32),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[3], "int32_bitmask"), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(info_value->children[4], NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(info_value->children[4], "string_list"), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(info_value->children[5], NANOARROW_TYPE_MAP),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(info_value->children[5], "int32_to_int32_list_map"), error);

  CHECK_NA(
      INTERNAL,
      ArrowSchemaSetType(info_value->children[4]->children[0], NANOARROW_TYPE_STRING),
      error);

  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(info_value->children[5]->children[0]->children[0],
                              NANOARROW_TYPE_INT32),
           error);
  info_value->children[5]->children[0]->children[0]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(info_value->children[5]->children[0]->children[1],
                              NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(
      INTERNAL,
      ArrowSchemaSetType(info_value->children[5]->children[0]->children[1]->children[0],
                         NANOARROW_TYPE_INT32),
      error);

  struct ArrowError na_error = {0};
  CHECK_NA_DETAIL(INTERNAL, ArrowArrayInitFromSchema(array, schema, &na_error), &na_error,
                  error);
  CHECK_NA(INTERNAL, ArrowArrayStartAppending(array), error);

  return ADBC_STATUS_OK;
}  // NOLINT(whitespace/indent)

AdbcStatusCode AdbcConnectionGetInfoAppendString(struct ArrowArray* array,
                                                 uint32_t info_code,
                                                 const char* info_value,
                                                 struct AdbcError* error) {
  CHECK_NA(INTERNAL, ArrowArrayAppendUInt(array->children[0], info_code), error);
  // Append to type variant
  struct ArrowStringView value = ArrowCharView(info_value);
  CHECK_NA(INTERNAL, ArrowArrayAppendString(array->children[1]->children[0], value),
           error);
  // Append type code/offset
  CHECK_NA(INTERNAL, ArrowArrayFinishUnionElement(array->children[1], /*type_id=*/0),
           error);
  return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcConnectionGetInfoAppendInt(struct ArrowArray* array,
                                              uint32_t info_code, int64_t info_value,
                                              struct AdbcError* error) {
  CHECK_NA(INTERNAL, ArrowArrayAppendUInt(array->children[0], info_code), error);
  // Append to type variant
  CHECK_NA(INTERNAL, ArrowArrayAppendInt(array->children[1]->children[2], info_value),
           error);
  // Append type code/offset
  CHECK_NA(INTERNAL, ArrowArrayFinishUnionElement(array->children[1], /*type_id=*/2),
           error);
  return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcInitConnectionObjectsSchema(struct ArrowSchema* schema,
                                               struct AdbcError* error) {
  ArrowSchemaInit(schema);
  CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(schema, /*num_columns=*/2), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(schema->children[0], "catalog_name"), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(schema->children[1], NANOARROW_TYPE_LIST), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(schema->children[1], "catalog_db_schemas"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(schema->children[1]->children[0], 2),
           error);

  struct ArrowSchema* db_schema_schema = schema->children[1]->children[0];
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(db_schema_schema->children[0], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(db_schema_schema->children[0], "db_schema_name"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(db_schema_schema->children[1], NANOARROW_TYPE_LIST), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(db_schema_schema->children[1], "db_schema_tables"), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetTypeStruct(db_schema_schema->children[1]->children[0], 4),
           error);

  struct ArrowSchema* table_schema = db_schema_schema->children[1]->children[0];
  CHECK_NA(INTERNAL, ArrowSchemaSetType(table_schema->children[0], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(table_schema->children[0], "table_name"), error);
  table_schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL, ArrowSchemaSetType(table_schema->children[1], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(table_schema->children[1], "table_type"), error);
  table_schema->children[1]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL, ArrowSchemaSetType(table_schema->children[2], NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(table_schema->children[2], "table_columns"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(table_schema->children[2]->children[0], 19),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(table_schema->children[3], NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(table_schema->children[3], "table_constraints"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(table_schema->children[3]->children[0], 4),
           error);

  struct ArrowSchema* column_schema = table_schema->children[2]->children[0];
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(column_schema->children[0], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[0], "column_name"),
           error);
  column_schema->children[0]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL, ArrowSchemaSetType(column_schema->children[1], NANOARROW_TYPE_INT32),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[1], "ordinal_position"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(column_schema->children[2], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[2], "remarks"), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(column_schema->children[3], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[3], "xdbc_data_type"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(column_schema->children[4], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[4], "xdbc_type_name"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(column_schema->children[5], NANOARROW_TYPE_INT32),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[5], "xdbc_column_size"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(column_schema->children[6], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[6], "xdbc_decimal_digits"), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(column_schema->children[7], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[7], "xdbc_num_prec_radix"), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(column_schema->children[8], NANOARROW_TYPE_INT16),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[8], "xdbc_nullable"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(column_schema->children[9], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[9], "xdbc_column_def"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(column_schema->children[10], NANOARROW_TYPE_INT16), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[10], "xdbc_sql_data_type"), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(column_schema->children[11], NANOARROW_TYPE_INT16), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[11], "xdbc_datetime_sub"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(column_schema->children[12], NANOARROW_TYPE_INT32), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[12], "xdbc_char_octet_length"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(column_schema->children[13], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[13], "xdbc_is_nullable"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(column_schema->children[14], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[14], "xdbc_scope_catalog"), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(column_schema->children[15], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[15], "xdbc_scope_schema"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(column_schema->children[16], NANOARROW_TYPE_STRING), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(column_schema->children[16], "xdbc_scope_table"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(column_schema->children[17], NANOARROW_TYPE_BOOL),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[17], "xdbc_is_autoincrement"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(column_schema->children[18], NANOARROW_TYPE_BOOL),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(column_schema->children[18], "xdbc_is_generatedcolumn"),
           error);

  struct ArrowSchema* constraint_schema = table_schema->children[3]->children[0];
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(constraint_schema->children[0], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(constraint_schema->children[0], "constraint_name"), error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(constraint_schema->children[1], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(constraint_schema->children[1], "constraint_type"), error);
  constraint_schema->children[1]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(constraint_schema->children[2], NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(constraint_schema->children[2], "constraint_column_names"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(constraint_schema->children[2]->children[0],
                              NANOARROW_TYPE_STRING),
           error);
  constraint_schema->children[2]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL,
           ArrowSchemaSetType(constraint_schema->children[3], NANOARROW_TYPE_LIST),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetName(constraint_schema->children[3], "constraint_column_usage"),
           error);
  CHECK_NA(INTERNAL,
           ArrowSchemaSetTypeStruct(constraint_schema->children[3]->children[0], 4),
           error);

  struct ArrowSchema* usage_schema = constraint_schema->children[3]->children[0];
  CHECK_NA(INTERNAL, ArrowSchemaSetType(usage_schema->children[0], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(usage_schema->children[0], "fk_catalog"), error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(usage_schema->children[1], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(usage_schema->children[1], "fk_db_schema"),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetType(usage_schema->children[2], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(usage_schema->children[2], "fk_table"), error);
  usage_schema->children[2]->flags &= ~ARROW_FLAG_NULLABLE;
  CHECK_NA(INTERNAL, ArrowSchemaSetType(usage_schema->children[3], NANOARROW_TYPE_STRING),
           error);
  CHECK_NA(INTERNAL, ArrowSchemaSetName(usage_schema->children[3], "fk_column_name"),
           error);
  usage_schema->children[3]->flags &= ~ARROW_FLAG_NULLABLE;

  return ADBC_STATUS_OK;
}

struct AdbcGetObjectsData* AdbcGetObjectsDataInit(struct ArrowArrayView* array_view) {
  struct AdbcGetObjectsData* get_objects_data =
      (struct AdbcGetObjectsData*)calloc(1, sizeof(struct AdbcGetObjectsData));
  if (get_objects_data == NULL) {
    return NULL;
  }

  get_objects_data->catalog_name_array = array_view->children[0];
  get_objects_data->catalog_schemas_array = array_view->children[1];
  get_objects_data->n_catalogs = 0;

  struct ArrowArrayView* catalog_db_schemas_items =
      get_objects_data->catalog_schemas_array->children[0];
  get_objects_data->db_schema_name_array = catalog_db_schemas_items->children[0];
  get_objects_data->db_schema_tables_array = catalog_db_schemas_items->children[1];

  struct ArrowArrayView* schema_table_items =
      get_objects_data->db_schema_tables_array->children[0];
  get_objects_data->table_name_array = schema_table_items->children[0];
  get_objects_data->table_type_array = schema_table_items->children[1];
  get_objects_data->table_columns_array = schema_table_items->children[2];
  get_objects_data->table_constraints_array = schema_table_items->children[3];

  struct ArrowArrayView* table_columns_items =
      get_objects_data->table_columns_array->children[0];
  get_objects_data->column_name_array = table_columns_items->children[0];
  get_objects_data->column_position_array = table_columns_items->children[1];
  get_objects_data->column_remarks_array = table_columns_items->children[2];
  get_objects_data->xdbc_data_type_array = table_columns_items->children[3];
  get_objects_data->xdbc_type_name_array = table_columns_items->children[4];
  get_objects_data->xdbc_column_size_array = table_columns_items->children[5];
  get_objects_data->xdbc_decimal_digits_array = table_columns_items->children[6];
  get_objects_data->xdbc_num_prec_radix_array = table_columns_items->children[7];
  get_objects_data->xdbc_nullable_array = table_columns_items->children[8];
  get_objects_data->xdbc_column_def_array = table_columns_items->children[9];
  get_objects_data->xdbc_sql_data_type_array = table_columns_items->children[10];
  get_objects_data->xdbc_datetime_sub_array = table_columns_items->children[11];
  get_objects_data->xdbc_char_octet_length_array = table_columns_items->children[12];
  get_objects_data->xdbc_is_nullable_array = table_columns_items->children[13];
  get_objects_data->xdbc_scope_catalog_array = table_columns_items->children[14];
  get_objects_data->xdbc_scope_schema_array = table_columns_items->children[15];
  get_objects_data->xdbc_scope_table_array = table_columns_items->children[16];
  get_objects_data->xdbc_is_autoincrement_array = table_columns_items->children[17];
  get_objects_data->xdbc_is_generatedcolumn_array = table_columns_items->children[18];

  struct ArrowArrayView* table_constraints_items =
      get_objects_data->table_constraints_array->children[0];
  get_objects_data->constraint_name_array = table_constraints_items->children[0];
  get_objects_data->constraint_type_array = table_constraints_items->children[1];
  get_objects_data->constraint_column_names_array = table_constraints_items->children[2];
  get_objects_data->constraint_column_name_array =
      get_objects_data->constraint_column_names_array->children[0];
  get_objects_data->constraint_column_usages_array = table_constraints_items->children[3];

  struct ArrowArrayView* constraint_column_usage_items =
      get_objects_data->constraint_column_usages_array->children[0];
  get_objects_data->fk_catalog_array = constraint_column_usage_items->children[0];
  get_objects_data->fk_db_schema_array = constraint_column_usage_items->children[1];
  get_objects_data->fk_table_array = constraint_column_usage_items->children[2];
  get_objects_data->fk_column_name_array = constraint_column_usage_items->children[3];

  get_objects_data->catalogs = (struct AdbcGetObjectsCatalog**)calloc(
      array_view->array->length, sizeof(struct AdbcGetObjectsCatalog*));

  if (get_objects_data->catalogs == NULL) {
    goto error_handler;
  }

  for (int64_t catalog_idx = 0; catalog_idx < array_view->array->length; catalog_idx++) {
    struct AdbcGetObjectsCatalog* catalog =
        (struct AdbcGetObjectsCatalog*)calloc(1, sizeof(struct AdbcGetObjectsCatalog));
    if (catalog == NULL) {
      goto error_handler;
    }
    get_objects_data->catalogs[catalog_idx] = catalog;
    get_objects_data->n_catalogs++;

    catalog->n_db_schemas = 0;

    catalog->catalog_name =
        ArrowArrayViewGetStringUnsafe(get_objects_data->catalog_name_array, catalog_idx);

    int64_t db_schema_list_start = ArrowArrayViewListChildOffset(
        get_objects_data->catalog_schemas_array, catalog_idx);
    int64_t db_schema_list_end = ArrowArrayViewListChildOffset(
        get_objects_data->catalog_schemas_array, catalog_idx + 1);

    int64_t db_schema_len = db_schema_list_end - db_schema_list_start;

    if (db_schema_len == 0) {
      catalog->catalog_db_schemas = NULL;
    } else {
      catalog->catalog_db_schemas = (struct AdbcGetObjectsSchema**)calloc(
          db_schema_len, sizeof(struct AdbcGetObjectsSchema*));
      if (catalog->catalog_db_schemas == NULL) {
        goto error_handler;
      }

      for (int64_t db_schema_index = db_schema_list_start;
           db_schema_index < db_schema_list_end; db_schema_index++) {
        struct AdbcGetObjectsSchema* schema =
            (struct AdbcGetObjectsSchema*)calloc(1, sizeof(struct AdbcGetObjectsSchema));
        if (schema == NULL) {
          goto error_handler;
        }
        catalog->catalog_db_schemas[db_schema_index - db_schema_list_start] = schema;
        catalog->n_db_schemas++;
        schema->n_db_schema_tables = 0;

        schema->db_schema_name = ArrowArrayViewGetStringUnsafe(
            get_objects_data->db_schema_name_array, db_schema_index);
        int64_t table_list_start = ArrowArrayViewListChildOffset(
            get_objects_data->db_schema_tables_array, db_schema_index);
        int64_t table_list_end = ArrowArrayViewListChildOffset(
            get_objects_data->db_schema_tables_array, db_schema_index + 1);
        int64_t table_len = table_list_end - table_list_start;

        if (table_len == 0) {
          schema->db_schema_tables = NULL;
        } else {
          schema->db_schema_tables = (struct AdbcGetObjectsTable**)calloc(
              table_len, sizeof(struct AdbcGetObjectsTable*));
          if (schema->db_schema_tables == NULL) {
            goto error_handler;
          }

          for (int64_t table_index = table_list_start; table_index < table_list_end;
               table_index++) {
            struct AdbcGetObjectsTable* table = (struct AdbcGetObjectsTable*)calloc(
                1, sizeof(struct AdbcGetObjectsTable));
            if (table == NULL) {
              goto error_handler;
            }
            schema->db_schema_tables[table_index - table_list_start] = table;
            schema->n_db_schema_tables++;
            table->n_table_columns = 0;
            table->n_table_constraints = 0;

            table->table_name = ArrowArrayViewGetStringUnsafe(
                get_objects_data->table_name_array, table_index);
            table->table_type = ArrowArrayViewGetStringUnsafe(
                get_objects_data->table_type_array, table_index);

            int64_t columns_list_start = ArrowArrayViewListChildOffset(
                get_objects_data->table_columns_array, table_index);
            int64_t columns_list_end = ArrowArrayViewListChildOffset(
                get_objects_data->table_columns_array, table_index + 1);

            int64_t columns_len = columns_list_end - columns_list_start;

            if (columns_len == 0) {
              table->table_columns = NULL;
            } else {
              table->table_columns = (struct AdbcGetObjectsColumn**)calloc(
                  columns_len, sizeof(struct AdbcGetObjectsColumn*));
              if (table->table_columns == NULL) {
                goto error_handler;
              }

              for (int64_t column_index = columns_list_start;
                   column_index < columns_list_end; column_index++) {
                struct AdbcGetObjectsColumn* column =
                    (struct AdbcGetObjectsColumn*)calloc(
                        1, sizeof(struct AdbcGetObjectsColumn));
                if (column == NULL) {
                  goto error_handler;
                }
                table->table_columns[column_index - columns_list_start] = column;
                table->n_table_columns++;

                column->column_name = ArrowArrayViewGetStringUnsafe(
                    get_objects_data->column_name_array, column_index);
                column->ordinal_position = (int32_t)ArrowArrayViewGetIntUnsafe(
                    get_objects_data->column_position_array, column_index);
                column->remarks = ArrowArrayViewGetStringUnsafe(
                    get_objects_data->column_remarks_array, column_index);
                column->xdbc_data_type = (int16_t)ArrowArrayViewGetIntUnsafe(
                    get_objects_data->xdbc_data_type_array, column_index);
                column->xdbc_type_name = ArrowArrayViewGetStringUnsafe(
                    get_objects_data->xdbc_type_name_array, column_index);
                column->xdbc_column_size = (int32_t)ArrowArrayViewGetIntUnsafe(
                    get_objects_data->xdbc_column_size_array, column_index);
                column->xdbc_decimal_digits = (int16_t)ArrowArrayViewGetIntUnsafe(
                    get_objects_data->xdbc_decimal_digits_array, column_index);
                column->xdbc_num_prec_radix = (int16_t)ArrowArrayViewGetIntUnsafe(
                    get_objects_data->xdbc_num_prec_radix_array, column_index);
                column->xdbc_nullable = (int16_t)ArrowArrayViewGetIntUnsafe(
                    get_objects_data->xdbc_nullable_array, column_index);
                column->xdbc_column_def = ArrowArrayViewGetStringUnsafe(
                    get_objects_data->xdbc_column_def_array, column_index);
                column->xdbc_sql_data_type = (int16_t)ArrowArrayViewGetIntUnsafe(
                    get_objects_data->xdbc_sql_data_type_array, column_index);
                column->xdbc_datetime_sub = (int16_t)ArrowArrayViewGetIntUnsafe(
                    get_objects_data->xdbc_datetime_sub_array, column_index);
                column->xdbc_char_octet_length = (int32_t)ArrowArrayViewGetIntUnsafe(
                    get_objects_data->xdbc_char_octet_length_array, column_index);
                column->xdbc_scope_catalog = ArrowArrayViewGetStringUnsafe(
                    get_objects_data->xdbc_scope_catalog_array, column_index);
                column->xdbc_scope_schema = ArrowArrayViewGetStringUnsafe(
                    get_objects_data->xdbc_scope_schema_array, column_index);
                column->xdbc_scope_table = ArrowArrayViewGetStringUnsafe(
                    get_objects_data->xdbc_scope_table_array, column_index);
                // TODO: implement a nanoarrow GetBool view here?
                column->xdbc_is_autoincrement = ArrowArrayViewGetIntUnsafe(
                    get_objects_data->xdbc_is_autoincrement_array, column_index);
                column->xdbc_is_generatedcolumn = ArrowArrayViewGetIntUnsafe(
                    get_objects_data->xdbc_is_generatedcolumn_array, column_index);
              }
            }

            int64_t constraints_list_start = ArrowArrayViewListChildOffset(
                get_objects_data->table_constraints_array, table_index);
            int64_t constraints_list_end = ArrowArrayViewListChildOffset(
                get_objects_data->table_constraints_array, table_index + 1);
            int64_t constraints_len = constraints_list_end - constraints_list_start;

            if (constraints_len == 0) {
              table->table_constraints = NULL;
            } else {
              table->table_constraints = (struct AdbcGetObjectsConstraint**)calloc(
                  constraints_len, sizeof(struct AdbcGetObjectsConstraint*));
              if (table->table_constraints == NULL) {
                goto error_handler;
              }

              for (int64_t constraint_index = constraints_list_start;
                   constraint_index < constraints_list_end; constraint_index++) {
                struct AdbcGetObjectsConstraint* constraint =
                    (struct AdbcGetObjectsConstraint*)calloc(
                        1, sizeof(struct AdbcGetObjectsConstraint));
                if (constraint == NULL) {
                  goto error_handler;
                }
                table->table_constraints[constraint_index - constraints_list_start] =
                    constraint;
                table->n_table_constraints++;
                constraint->n_column_names = 0;
                constraint->n_column_usages = 0;

                constraint->constraint_name = ArrowArrayViewGetStringUnsafe(
                    get_objects_data->constraint_name_array, constraint_index);
                constraint->constraint_type = ArrowArrayViewGetStringUnsafe(
                    get_objects_data->constraint_type_array, constraint_index);
                int64_t constraint_column_names_start = ArrowArrayViewListChildOffset(
                    get_objects_data->constraint_column_names_array, constraint_index);
                int64_t constraint_column_names_end = ArrowArrayViewListChildOffset(
                    get_objects_data->constraint_column_names_array,
                    constraint_index + 1);
                int64_t constraint_column_names_len =
                    constraint_column_names_end - constraint_column_names_start;

                if (constraint_column_names_len == 0) {
                  constraint->constraint_column_names = NULL;
                } else {
                  constraint->constraint_column_names = (struct ArrowStringView*)calloc(
                      constraint_column_names_len, sizeof(struct ArrowStringView));
                  if (constraint->constraint_column_names == NULL) {
                    goto error_handler;
                  }

                  for (int64_t constraint_column_name_index =
                           constraint_column_names_start;
                       constraint_column_name_index < constraint_column_names_end;
                       constraint_column_name_index++) {
                    constraint->constraint_column_names[constraint_column_name_index -
                                                        constraint_column_names_start] =
                        ArrowArrayViewGetStringUnsafe(
                            get_objects_data->constraint_column_name_array,
                            constraint_column_name_index);
                    constraint->n_column_names++;
                  }
                }

                int64_t constraint_column_usages_start = ArrowArrayViewListChildOffset(
                    get_objects_data->constraint_column_usages_array, constraint_index);
                int64_t constraint_column_usages_end = ArrowArrayViewListChildOffset(
                    get_objects_data->constraint_column_usages_array,
                    constraint_index + 1);
                int64_t constraint_column_usages_len =
                    constraint_column_usages_end - constraint_column_usages_start;

                if (constraint_column_usages_len == 0) {
                  constraint->constraint_column_usages = NULL;
                } else {
                  constraint->constraint_column_usages =
                      (struct AdbcGetObjectsUsage**)calloc(
                          constraint_column_usages_len,
                          sizeof(struct AdbcGetObjectsUsage*));
                  if (constraint->constraint_column_usages == NULL) {
                    goto error_handler;
                  }

                  for (int64_t constraint_column_usage_index =
                           constraint_column_usages_start;
                       constraint_column_usage_index < constraint_column_usages_end;
                       constraint_column_usage_index++) {
                    struct AdbcGetObjectsUsage* usage =
                        (struct AdbcGetObjectsUsage*)calloc(
                            1, sizeof(struct AdbcGetObjectsUsage));
                    if (usage == NULL) {
                      goto error_handler;
                    }

                    usage->fk_catalog =
                        ArrowArrayViewGetStringUnsafe(get_objects_data->fk_catalog_array,
                                                      constraint_column_usage_index);
                    usage->fk_db_schema = ArrowArrayViewGetStringUnsafe(
                        get_objects_data->fk_db_schema_array,
                        constraint_column_usage_index);
                    usage->fk_table = ArrowArrayViewGetStringUnsafe(
                        get_objects_data->fk_table_array, constraint_column_usage_index);
                    usage->fk_column_name = ArrowArrayViewGetStringUnsafe(
                        get_objects_data->fk_column_name_array,
                        constraint_column_usage_index);

                    constraint->constraint_column_usages[constraint_column_usage_index -
                                                         constraint_column_usages_start] =
                        usage;
                    constraint->n_column_usages++;
                  }
                }
              }
            }
          }
        }
      }
    }
  }

  return get_objects_data;

error_handler:
  AdbcGetObjectsDataDelete(get_objects_data);
  return NULL;
}

void AdbcGetObjectsDataDelete(struct AdbcGetObjectsData* get_objects_data) {
  for (int64_t catalog_index = 0; catalog_index < get_objects_data->n_catalogs;
       catalog_index++) {
    struct AdbcGetObjectsCatalog* catalog = get_objects_data->catalogs[catalog_index];
    for (int64_t db_schema_index = 0; db_schema_index < catalog->n_db_schemas;
         db_schema_index++) {
      struct AdbcGetObjectsSchema* schema = catalog->catalog_db_schemas[db_schema_index];
      for (int64_t table_index = 0; table_index < schema->n_db_schema_tables;
           table_index++) {
        struct AdbcGetObjectsTable* table = schema->db_schema_tables[table_index];
        for (int64_t column_index = 0; column_index < table->n_table_columns;
             column_index++) {
          free(table->table_columns[column_index]);
        }

        free(table->table_columns);

        for (int64_t constraint_index = 0; constraint_index < table->n_table_constraints;
             constraint_index++) {
          struct AdbcGetObjectsConstraint* constraint =
              table->table_constraints[constraint_index];
          free(constraint->constraint_column_names);
          for (int64_t usage_index = 0; usage_index < constraint->n_column_usages;
               usage_index++) {
            free(constraint->constraint_column_usages[usage_index]);
          }

          free(constraint->constraint_column_usages);
          free(table->table_constraints[constraint_index]);
        }

        free(table->table_constraints);
        free(table);
      }

      free(schema->db_schema_tables);
      free(schema);
    }

    free(catalog->catalog_db_schemas);
    free(catalog);
  }

  free(get_objects_data->catalogs);
  free(get_objects_data);
}

struct AdbcGetObjectsCatalog* AdbcGetObjectsDataGetCatalogByName(
    struct AdbcGetObjectsData* get_objects_data, const char* const catalog_name) {
  if (catalog_name != NULL) {
    for (int64_t i = 0; i < get_objects_data->n_catalogs; i++) {
      struct AdbcGetObjectsCatalog* catalog = get_objects_data->catalogs[i];
      if (StringViewEquals(catalog->catalog_name, catalog_name)) {
        return catalog;
      }
    }
  }

  return NULL;
}

struct AdbcGetObjectsSchema* AdbcGetObjectsDataGetSchemaByName(
    struct AdbcGetObjectsData* get_objects_data, const char* const catalog_name,
    const char* const schema_name) {
  if (schema_name != NULL) {
    struct AdbcGetObjectsCatalog* catalog =
        AdbcGetObjectsDataGetCatalogByName(get_objects_data, catalog_name);
    if (catalog != NULL) {
      for (int64_t i = 0; i < catalog->n_db_schemas; i++) {
        struct AdbcGetObjectsSchema* schema = catalog->catalog_db_schemas[i];
        if (StringViewEquals(schema->db_schema_name, schema_name)) {
          return schema;
        }
      }
    }
  }

  return NULL;
}

struct AdbcGetObjectsTable* AdbcGetObjectsDataGetTableByName(
    struct AdbcGetObjectsData* get_objects_data, const char* const catalog_name,
    const char* const schema_name, const char* const table_name) {
  if (table_name != NULL) {
    struct AdbcGetObjectsSchema* schema =
        AdbcGetObjectsDataGetSchemaByName(get_objects_data, catalog_name, schema_name);
    if (schema != NULL) {
      for (int64_t i = 0; i < schema->n_db_schema_tables; i++) {
        struct AdbcGetObjectsTable* table = schema->db_schema_tables[i];
        if (StringViewEquals(table->table_name, table_name)) {
          return table;
        }
      }
    }
  }

  return NULL;
}

struct AdbcGetObjectsColumn* AdbcGetObjectsDataGetColumnByName(
    struct AdbcGetObjectsData* get_objects_data, const char* const catalog_name,
    const char* const schema_name, const char* const table_name,
    const char* const column_name) {
  if (column_name != NULL) {
    struct AdbcGetObjectsTable* table = AdbcGetObjectsDataGetTableByName(
        get_objects_data, catalog_name, schema_name, table_name);
    if (table != NULL) {
      for (int64_t i = 0; i < table->n_table_columns; i++) {
        struct AdbcGetObjectsColumn* column = table->table_columns[i];
        if (StringViewEquals(column->column_name, column_name)) {
          return column;
        }
      }
    }
  }

  return NULL;
}

struct AdbcGetObjectsConstraint* AdbcGetObjectsDataGetConstraintByName(
    struct AdbcGetObjectsData* get_objects_data, const char* const catalog_name,
    const char* const schema_name, const char* const table_name,
    const char* const constraint_name) {
  if (constraint_name != NULL) {
    struct AdbcGetObjectsTable* table = AdbcGetObjectsDataGetTableByName(
        get_objects_data, catalog_name, schema_name, table_name);
    if (table != NULL) {
      for (int64_t i = 0; i < table->n_table_constraints; i++) {
        struct AdbcGetObjectsConstraint* constraint = table->table_constraints[i];
        if (StringViewEquals(constraint->constraint_name, constraint_name)) {
          return constraint;
        }
      }
    }
  }

  return NULL;
}
