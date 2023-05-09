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

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <assert.h>
#include <nanoarrow/nanoarrow.h>

static size_t kErrorBufferSize = 256;

static void ReleaseError(struct AdbcError* error) {
  free(error->message);
  error->message = NULL;
  error->release = NULL;
}

void SetError(struct AdbcError* error, const char* format, ...) {
  if (!error) return;
  if (error->release) {
    // TODO: combine the errors if possible
    error->release(error);
  }
  error->message = malloc(kErrorBufferSize);
  if (!error->message) return;

  error->release = &ReleaseError;

  va_list args;
  va_start(args, format);
  vsnprintf(error->message, kErrorBufferSize, format, args);
  va_end(args);
}

struct SingleBatchArrayStream {
  struct ArrowSchema schema;
  struct ArrowArray batch;
};
static const char* SingleBatchArrayStreamGetLastError(struct ArrowArrayStream* stream) {
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
  int bytes_available = builder->capacity - builder->size;

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

AdbcStatusCode AdbcInitConnectionGetInfoSchema(const uint32_t* info_codes,
                                               size_t info_codes_length,
                                               struct ArrowSchema* schema,
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
