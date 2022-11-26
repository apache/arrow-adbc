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

#include <nanoarrow.h>

static size_t kErrorBufferSize = 256;
static char kErrorPrefix[] = "[SQLite] ";

void ReleaseError(struct AdbcError* error) {
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

  memcpy(error->message, kErrorPrefix, sizeof(kErrorPrefix));

  va_list args;
  va_start(args, format);
  vsnprintf(error->message + sizeof(kErrorPrefix) - 1,
            kErrorBufferSize - sizeof(kErrorPrefix) + 1, format, args);
  va_end(args);
}

struct SingleBatchArrayStream {
  struct ArrowSchema schema;
  struct ArrowArray batch;
};
const char* SingleBatchArrayStreamGetLastError(struct ArrowArrayStream* stream) {
  return NULL;
}
int SingleBatchArrayStreamGetNext(struct ArrowArrayStream* stream,
                                  struct ArrowArray* batch) {
  if (!stream || !stream->private_data) return EINVAL;
  struct SingleBatchArrayStream* impl =
      (struct SingleBatchArrayStream*)stream->private_data;

  memcpy(batch, &impl->batch, sizeof(*batch));
  memset(&impl->batch, 0, sizeof(*batch));
  return 0;
}
int SingleBatchArrayStreamGetSchema(struct ArrowArrayStream* stream,
                                    struct ArrowSchema* schema) {
  if (!stream || !stream->private_data) return EINVAL;
  struct SingleBatchArrayStream* impl =
      (struct SingleBatchArrayStream*)stream->private_data;

  return ArrowSchemaDeepCopy(&impl->schema, schema);
}
void SingleBatchArrayStreamRelease(struct ArrowArrayStream* stream) {
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

void StringBuilderInit(struct StringBuilder* builder, size_t initial_size) {
  builder->buffer = (char*)malloc(initial_size);
  builder->size = 0;
  builder->capacity = initial_size;
}
void StringBuilderAppend(struct StringBuilder* builder, const char* value) {
  size_t length = strlen(value);
  size_t new_size = builder->size + length;
  if (new_size > builder->capacity) {
    size_t new_capacity = builder->size + length - builder->capacity;
    if (builder->size == 0) new_capacity++;

    builder->buffer = realloc(builder->buffer, new_capacity);
    builder->capacity = new_capacity;
  }
  strncpy(builder->buffer + builder->size, value, length);
  builder->buffer[new_size] = '\0';
  builder->size = new_size;
}
void StringBuilderReset(struct StringBuilder* builder) {
  if (builder->buffer) {
    free(builder->buffer);
  }
  memset(builder, 0, sizeof(*builder));
}
