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

#include "statement_reader.h"

#include <assert.h>
#include <inttypes.h>
#include <math.h>
#include <stdint.h>
#include <stdio.h>
#include <time.h>

#include <adbc.h>
#include <nanoarrow/nanoarrow.h>
#include <sqlite3.h>

#include "driver/common/utils.h"

AdbcStatusCode AdbcSqliteBinderSet(struct AdbcSqliteBinder* binder,
                                   struct AdbcError* error) {
  int status = binder->params.get_schema(&binder->params, &binder->schema);
  if (status != 0) {
    const char* message = binder->params.get_last_error(&binder->params);
    if (!message) message = "(unknown error)";
    SetError(error, "Failed to get parameter schema: (%d) %s: %s", status,
             strerror(status), message);
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  struct ArrowError arrow_error = {0};
  status = ArrowArrayViewInitFromSchema(&binder->batch, &binder->schema, &arrow_error);
  if (status != 0) {
    SetError(error, "Failed to initialize array view: (%d) %s: %s", status,
             strerror(status), arrow_error.message);
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  if (binder->batch.storage_type != NANOARROW_TYPE_STRUCT) {
    SetError(error, "Bind parameters do not have root type STRUCT");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  binder->types =
      (enum ArrowType*)malloc(binder->schema.n_children * sizeof(enum ArrowType));

  struct ArrowSchemaView view = {0};
  for (int i = 0; i < binder->schema.n_children; i++) {
    status = ArrowSchemaViewInit(&view, binder->schema.children[i], &arrow_error);
    if (status != NANOARROW_OK) {
      SetError(error, "Failed to parse schema for column %d: %s (%d): %s", i,
               strerror(status), status, arrow_error.message);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    if (view.type == NANOARROW_TYPE_UNINITIALIZED) {
      SetError(error, "Column %d has UNINITIALIZED type", i);
      return ADBC_STATUS_INTERNAL;
    }

    if (view.type == NANOARROW_TYPE_DICTIONARY) {
      struct ArrowSchemaView value_view = {0};
      status = ArrowSchemaViewInit(&value_view, binder->schema.children[i]->dictionary,
                                   &arrow_error);
      if (status != NANOARROW_OK) {
        SetError(error, "Failed to parse schema for column %d->dictionary: %s (%d): %s",
                 i, strerror(status), status, arrow_error.message);
        return ADBC_STATUS_INVALID_ARGUMENT;
      }

      // We only support string/binary dictionary-encoded values
      switch (value_view.type) {
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING:
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY:
          break;
        default:
          SetError(error, "Column %d dictionary has unsupported type %s", i,
                   ArrowTypeString(value_view.type));
          return ADBC_STATUS_NOT_IMPLEMENTED;
      }
    }

    binder->types[i] = view.type;
  }

  return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcSqliteBinderSetArray(struct AdbcSqliteBinder* binder,
                                        struct ArrowArray* values,
                                        struct ArrowSchema* schema,
                                        struct AdbcError* error) {
  AdbcSqliteBinderRelease(binder);
  RAISE_ADBC(BatchToArrayStream(values, schema, &binder->params, error));
  return AdbcSqliteBinderSet(binder, error);
}  // NOLINT(whitespace/indent)
AdbcStatusCode AdbcSqliteBinderSetArrayStream(struct AdbcSqliteBinder* binder,
                                              struct ArrowArrayStream* values,
                                              struct AdbcError* error) {
  AdbcSqliteBinderRelease(binder);
  binder->params = *values;
  memset(values, 0, sizeof(*values));
  return AdbcSqliteBinderSet(binder, error);
}

#define SECONDS_PER_DAY 86400

/*
  Allocates to buf on success. Caller is responsible for freeing.
  On failure sets error and contents of buf are undefined.
*/
static AdbcStatusCode ArrowDate32ToIsoString(int32_t value, char** buf,
                                             struct AdbcError* error) {
  int strlen = 10;

#if SIZEOF_TIME_T < 8
  if ((value > INT32_MAX / SECONDS_PER_DAY) || (value < INT32_MIN / SECONDS_PER_DAY)) {
    SetError(error, "Date %" PRId32 " exceeds platform time_t bounds", value);

    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  time_t time = (time_t)(value * SECONDS_PER_DAY);
#else
  time_t time = value * SECONDS_PER_DAY;
#endif

  struct tm broken_down_time;

#if defined(_WIN32)
  if (gmtime_s(&broken_down_time, &time) != 0) {
    SetError(error, "Could not convert date %" PRId32 " to broken down time", value);

    return ADBC_STATUS_INVALID_ARGUMENT;
  }
#else
  if (gmtime_r(&time, &broken_down_time) != &broken_down_time) {
    SetError(error, "Could not convert date %" PRId32 " to broken down time", value);

    return ADBC_STATUS_INVALID_ARGUMENT;
  }
#endif

  char* tsstr = malloc(strlen + 1);
  if (tsstr == NULL) {
    return ADBC_STATUS_IO;
  }

  if (strftime(tsstr, strlen + 1, "%Y-%m-%d", &broken_down_time) == 0) {
    SetError(error, "Call to strftime for date %" PRId32 " with failed", value);
    free(tsstr);
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  *buf = tsstr;
  return ADBC_STATUS_OK;
}

/*
  Allocates to buf on success. Caller is responsible for freeing.
  On failure sets error and contents of buf are undefined.
*/
static AdbcStatusCode ArrowTimestampToIsoString(int64_t value, enum ArrowTimeUnit unit,
                                                char** buf, struct AdbcError* error) {
  int scale = 1;
  int strlen = 20;
  int rem = 0;

  switch (unit) {
    case NANOARROW_TIME_UNIT_SECOND:
      break;
    case NANOARROW_TIME_UNIT_MILLI:
      scale = 1000;
      strlen = 24;
      break;
    case NANOARROW_TIME_UNIT_MICRO:
      scale = 1000000;
      strlen = 27;
      break;
    case NANOARROW_TIME_UNIT_NANO:
      scale = 1000000000;
      strlen = 30;
      break;
  }

  rem = value % scale;
  if (rem < 0) {
    value -= scale;
    rem = scale + rem;
  }

  const int64_t seconds = value / scale;

#if SIZEOF_TIME_T < 8
  if ((seconds > INT32_MAX) || (seconds < INT32_MIN)) {
    SetError(error, "Timestamp %" PRId64 " with unit %d exceeds platform time_t bounds",
             value, unit);

    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  const time_t time = (time_t)seconds;
#else
  const time_t time = seconds;
#endif

  struct tm broken_down_time;

#if defined(_WIN32)
  if (gmtime_s(&broken_down_time, &time) != 0) {
    SetError(error,
             "Could not convert timestamp %" PRId64 " with unit %d to broken down time",
             value, unit);

    return ADBC_STATUS_INVALID_ARGUMENT;
  }
#else
  if (gmtime_r(&time, &broken_down_time) != &broken_down_time) {
    SetError(error,
             "Could not convert timestamp %" PRId64 " with unit %d to broken down time",
             value, unit);

    return ADBC_STATUS_INVALID_ARGUMENT;
  }
#endif

  char* tsstr = malloc(strlen + 1);
  if (tsstr == NULL) {
    return ADBC_STATUS_IO;
  }

  if (strftime(tsstr, strlen, "%Y-%m-%dT%H:%M:%S", &broken_down_time) == 0) {
    SetError(error, "Call to strftime for timestamp %" PRId64 " with unit %d failed",
             value, unit);
    free(tsstr);
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  assert(rem >= 0);
  switch (unit) {
    case NANOARROW_TIME_UNIT_SECOND:
      break;
    case NANOARROW_TIME_UNIT_MILLI:
      tsstr[19] = '.';
      snprintf(tsstr + 20, strlen - 20, "%03d", rem % 1000u);
      break;
    case NANOARROW_TIME_UNIT_MICRO:
      tsstr[19] = '.';
      snprintf(tsstr + 20, strlen - 20, "%06d", rem % 1000000u);
      break;
    case NANOARROW_TIME_UNIT_NANO:
      tsstr[19] = '.';
      snprintf(tsstr + 20, strlen - 20, "%09d", rem % 1000000000u);
      break;
  }

  *buf = tsstr;
  return ADBC_STATUS_OK;
}

AdbcStatusCode AdbcSqliteBinderBindNext(struct AdbcSqliteBinder* binder, sqlite3* conn,
                                        sqlite3_stmt* stmt, char* finished,
                                        struct AdbcError* error) {
  struct ArrowError arrow_error = {0};
  int status = 0;
  while (!binder->array.release || binder->next_row >= binder->array.length) {
    if (binder->array.release) {
      ArrowArrayViewReset(&binder->batch);
      binder->array.release(&binder->array);

      status =
          ArrowArrayViewInitFromSchema(&binder->batch, &binder->schema, &arrow_error);
      if (status != 0) {
        SetError(error, "Failed to initialize array view: (%d) %s: %s", status,
                 strerror(status), arrow_error.message);
        return ADBC_STATUS_INTERNAL;
      }
    }

    status = binder->params.get_next(&binder->params, &binder->array);
    if (status != 0) {
      const char* message = binder->params.get_last_error(&binder->params);
      if (!message) message = "(unknown error)";
      SetError(error, "Failed to get next parameter batch: (%d) %s: %s", status,
               strerror(status), message);
      return ADBC_STATUS_IO;
    }

    if (!binder->array.release) {
      *finished = 1;
      AdbcSqliteBinderRelease(binder);
      return ADBC_STATUS_OK;
    }

    status = ArrowArrayViewSetArray(&binder->batch, &binder->array, &arrow_error);
    if (status != 0) {
      SetError(error, "Failed to initialize array view: (%d) %s: %s", status,
               strerror(status), arrow_error.message);
      return ADBC_STATUS_INTERNAL;
    }

    binder->next_row = 0;
  }

  if (sqlite3_reset(stmt) != SQLITE_OK) {
    SetError(error, "Failed to reset statement: %s", sqlite3_errmsg(conn));
    return ADBC_STATUS_INTERNAL;
  }
  if (sqlite3_clear_bindings(stmt) != SQLITE_OK) {
    SetError(error, "Failed to clear statement bindings: %s", sqlite3_errmsg(conn));
    return ADBC_STATUS_INTERNAL;
  }

  for (int col = 0; col < binder->schema.n_children; col++) {
    if (ArrowArrayViewIsNull(binder->batch.children[col], binder->next_row)) {
      status = sqlite3_bind_null(stmt, col + 1);
    } else {
      switch (binder->types[col]) {
        case NANOARROW_TYPE_BINARY:
        case NANOARROW_TYPE_LARGE_BINARY: {
          struct ArrowBufferView value =
              ArrowArrayViewGetBytesUnsafe(binder->batch.children[col], binder->next_row);
          status = sqlite3_bind_blob(stmt, col + 1, value.data.as_char, value.size_bytes,
                                     SQLITE_STATIC);
          break;
        }
        case NANOARROW_TYPE_BOOL:
        case NANOARROW_TYPE_UINT8:
        case NANOARROW_TYPE_UINT16:
        case NANOARROW_TYPE_UINT32:
        case NANOARROW_TYPE_UINT64: {
          uint64_t value =
              ArrowArrayViewGetUIntUnsafe(binder->batch.children[col], binder->next_row);
          if (value > INT64_MAX) {
            SetError(error,
                     "Column %d has unsigned integer value %" PRIu64
                     "out of range of int64_t",
                     col, value);
            return ADBC_STATUS_INVALID_ARGUMENT;
          }
          status = sqlite3_bind_int64(stmt, col + 1, (int64_t)value);
          break;
        }
        case NANOARROW_TYPE_INT8:
        case NANOARROW_TYPE_INT16:
        case NANOARROW_TYPE_INT32:
        case NANOARROW_TYPE_INT64: {
          int64_t value =
              ArrowArrayViewGetIntUnsafe(binder->batch.children[col], binder->next_row);
          status = sqlite3_bind_int64(stmt, col + 1, value);
          break;
        }
        case NANOARROW_TYPE_FLOAT:
        case NANOARROW_TYPE_DOUBLE: {
          double value = ArrowArrayViewGetDoubleUnsafe(binder->batch.children[col],
                                                       binder->next_row);
          status = sqlite3_bind_double(stmt, col + 1, value);
          break;
        }
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_LARGE_STRING: {
          struct ArrowBufferView value =
              ArrowArrayViewGetBytesUnsafe(binder->batch.children[col], binder->next_row);
          status = sqlite3_bind_text(stmt, col + 1, value.data.as_char, value.size_bytes,
                                     SQLITE_STATIC);
          break;
        }
        case NANOARROW_TYPE_DICTIONARY: {
          int64_t value_index =
              ArrowArrayViewGetIntUnsafe(binder->batch.children[col], binder->next_row);
          if (ArrowArrayViewIsNull(binder->batch.children[col]->dictionary,
                                   value_index)) {
            status = sqlite3_bind_null(stmt, col + 1);
          } else {
            struct ArrowBufferView value = ArrowArrayViewGetBytesUnsafe(
                binder->batch.children[col]->dictionary, value_index);
            status = sqlite3_bind_text(stmt, col + 1, value.data.as_char,
                                       value.size_bytes, SQLITE_STATIC);
          }
          break;
        }
        case NANOARROW_TYPE_DATE32: {
          int64_t value =
              ArrowArrayViewGetIntUnsafe(binder->batch.children[col], binder->next_row);
          char* tsstr;

          if ((value > INT32_MAX) || (value < INT32_MIN)) {
            SetError(error,
                     "Column %d has value %" PRId64
                     " which exceeds the expected range "
                     "for an Arrow DATE32 type",
                     col, value);
            return ADBC_STATUS_INVALID_DATA;
          }

          RAISE_ADBC(ArrowDate32ToIsoString((int32_t)value, &tsstr, error));
          // SQLITE_TRANSIENT ensures the value is copied during bind
          status =
              sqlite3_bind_text(stmt, col + 1, tsstr, strlen(tsstr), SQLITE_TRANSIENT);

          free(tsstr);
          break;
        }
        case NANOARROW_TYPE_TIMESTAMP: {
          struct ArrowSchemaView bind_schema_view;
          RAISE_ADBC(ArrowSchemaViewInit(&bind_schema_view, binder->schema.children[col],
                                         &arrow_error));
          enum ArrowTimeUnit unit = bind_schema_view.time_unit;
          int64_t value =
              ArrowArrayViewGetIntUnsafe(binder->batch.children[col], binder->next_row);

          char* tsstr;
          RAISE_ADBC(ArrowTimestampToIsoString(value, unit, &tsstr, error));

          // SQLITE_TRANSIENT ensures the value is copied during bind
          status =
              sqlite3_bind_text(stmt, col + 1, tsstr, strlen(tsstr), SQLITE_TRANSIENT);
          free((char*)tsstr);
          break;
        }
        default:
          SetError(error, "Column %d has unsupported type %s", col,
                   ArrowTypeString(binder->types[col]));
          return ADBC_STATUS_NOT_IMPLEMENTED;
      }
    }

    if (status != SQLITE_OK) {
      SetError(error, "Failed to clear statement bindings: %s", sqlite3_errmsg(conn));
      return ADBC_STATUS_INTERNAL;
    }
  }

  binder->next_row++;
  *finished = 0;
  return ADBC_STATUS_OK;
}

void AdbcSqliteBinderRelease(struct AdbcSqliteBinder* binder) {
  if (binder->schema.release) {
    binder->schema.release(&binder->schema);
  }
  if (binder->params.release) {
    binder->params.release(&binder->params);
  }
  if (binder->types) {
    free(binder->types);
  }
  if (binder->array.release) {
    binder->array.release(&binder->array);
  }
  ArrowArrayViewReset(&binder->batch);
  memset(binder, 0, sizeof(*binder));
}

struct StatementReader {
  sqlite3* db;
  sqlite3_stmt* stmt;
  enum ArrowType* types;
  struct ArrowSchema schema;
  struct ArrowArray initial_batch;
  struct AdbcSqliteBinder* binder;
  struct ArrowError error;
  char done;
  int batch_size;
};

const char* StatementReaderGetLastError(struct ArrowArrayStream* self) {
  if (!self->release || !self->private_data) {
    return NULL;
  }

  struct StatementReader* reader = (struct StatementReader*)self->private_data;
  return reader->error.message;
}

void StatementReaderSetError(struct StatementReader* reader) {
  const char* msg = sqlite3_errmsg(reader->db);
  // Reset here so that we don't get an error again in StatementRelease
  (void)sqlite3_reset(reader->stmt);
  strncpy(reader->error.message, msg, sizeof(reader->error.message) - 1);
  reader->error.message[sizeof(reader->error.message) - 1] = '\0';
}

int StatementReaderGetOneValue(struct StatementReader* reader, int col,
                               struct ArrowArray* out) {
  int sqlite_type = sqlite3_column_type(reader->stmt, col);

  if (sqlite_type == SQLITE_NULL) {
    return ArrowArrayAppendNull(out, 1);
  }

  switch (reader->types[col]) {
    case NANOARROW_TYPE_INT64: {
      switch (sqlite_type) {
        case SQLITE_INTEGER: {
          int64_t value = sqlite3_column_int64(reader->stmt, col);
          return ArrowArrayAppendInt(out, value);
        }
        case SQLITE_FLOAT: {
          // TODO: behavior needs to be configurable
          snprintf(reader->error.message, sizeof(reader->error.message),
                   "[SQLite] Type mismatch in column %d: expected INT64 but got DOUBLE",
                   col);
          return EIO;
        }
        case SQLITE_TEXT:
        case SQLITE_BLOB: {
          snprintf(
              reader->error.message, sizeof(reader->error.message),
              "[SQLite] Type mismatch in column %d: expected INT64 but got STRING/BINARY",
              col);
          return EIO;
        }
        default: {
          snprintf(reader->error.message, sizeof(reader->error.message),
                   "[SQLite] Type mismatch in column %d: expected INT64 but got unknown "
                   "type %d",
                   col, sqlite_type);
          return ENOTSUP;
        }
      }
      break;
    }

    case NANOARROW_TYPE_DOUBLE: {
      switch (sqlite_type) {
        case SQLITE_INTEGER:
        case SQLITE_FLOAT: {
          // Let SQLite convert
          double value = sqlite3_column_double(reader->stmt, col);
          return ArrowArrayAppendDouble(out, value);
        }
        case SQLITE_TEXT:
        case SQLITE_BLOB: {
          snprintf(reader->error.message, sizeof(reader->error.message),
                   "[SQLite] Type mismatch in column %d: expected DOUBLE but got "
                   "STRING/BINARY",
                   col);
          return EIO;
        }
        default: {
          snprintf(reader->error.message, sizeof(reader->error.message),
                   "[SQLite] Type mismatch in column %d: expected DOUBLE but got unknown "
                   "type %d",
                   col, sqlite_type);
          return ENOTSUP;
        }
      }
      break;
    }

    case NANOARROW_TYPE_STRING: {
      switch (sqlite_type) {
        case SQLITE_INTEGER:
        case SQLITE_FLOAT:
        case SQLITE_TEXT:
        case SQLITE_BLOB:
          break;
        default: {
          snprintf(reader->error.message, sizeof(reader->error.message),
                   "[SQLite] Type mismatch in column %d: expected STRING but got unknown "
                   "type %d",
                   col, sqlite_type);
          return ENOTSUP;
        }
      }

      // Let SQLite convert
      struct ArrowStringView value = {
          .data = (const char*)sqlite3_column_text(reader->stmt, col),
          .size_bytes = sqlite3_column_bytes(reader->stmt, col),
      };
      return ArrowArrayAppendString(out, value);
    }

    case NANOARROW_TYPE_BINARY: {
      switch (sqlite_type) {
        case SQLITE_TEXT:
        case SQLITE_BLOB:
          break;
        default: {
          snprintf(reader->error.message, sizeof(reader->error.message),
                   "[SQLite] Type mismatch in column %d: expected BLOB but got unknown "
                   "type %d",
                   col, sqlite_type);
          return ENOTSUP;
        }
      }

      // Let SQLite convert
      struct ArrowBufferView value;
      value.data.data = sqlite3_column_blob(reader->stmt, col);
      value.size_bytes = sqlite3_column_bytes(reader->stmt, col);
      return ArrowArrayAppendBytes(out, value);
    }

    default:
      break;
  }

  snprintf(reader->error.message, sizeof(reader->error.message),
           "[SQLite] Internal error: unknown inferred column type %d",
           reader->types[col]);
  return ENOTSUP;
}

int StatementReaderGetNext(struct ArrowArrayStream* self, struct ArrowArray* out) {
  if (!self->release || !self->private_data) {
    return EINVAL;
  }

  struct StatementReader* reader = (struct StatementReader*)self->private_data;
  if (reader->initial_batch.release != NULL) {
    memcpy(out, &reader->initial_batch, sizeof(*out));
    memset(&reader->initial_batch, 0, sizeof(reader->initial_batch));
    return 0;
  } else if (reader->done) {
    out->release = NULL;
    return 0;
  }

  RAISE_NA(ArrowArrayInitFromSchema(out, &reader->schema, &reader->error));
  RAISE_NA(ArrowArrayStartAppending(out));
  int64_t batch_size = 0;
  int status = 0;

  sqlite3_mutex_enter(sqlite3_db_mutex(reader->db));
  while (batch_size < reader->batch_size) {
    int rc = sqlite3_step(reader->stmt);
    if (rc == SQLITE_DONE) {
      if (!reader->binder) {
        reader->done = 1;
        break;
      } else {
        char finished = 0;
        struct AdbcError error = {0};
        status = AdbcSqliteBinderBindNext(reader->binder, reader->db, reader->stmt,
                                          &finished, &error);
        if (status != ADBC_STATUS_OK) {
          reader->done = 1;
          status = EIO;
          if (error.release) {
            strncpy(reader->error.message, error.message,
                    sizeof(reader->error.message) - 1);
            reader->error.message[sizeof(reader->error.message) - 1] = '\0';
            error.release(&error);
          }
          break;
        } else if (finished) {
          reader->done = 1;
          break;
        }
        continue;
      }
    } else if (rc == SQLITE_ERROR) {
      reader->done = 1;
      status = EIO;
      StatementReaderSetError(reader);
      break;
    } else if (rc != SQLITE_ROW) {
      reader->done = 1;
      status = ADBC_STATUS_INTERNAL;
      StatementReaderSetError(reader);
      break;
    }

    for (int col = 0; col < reader->schema.n_children; col++) {
      status = StatementReaderGetOneValue(reader, col, out->children[col]);
      if (status != 0) break;
    }

    if (status != 0) break;
    batch_size++;
  }
  if (status == 0) {
    out->length = batch_size;
    for (int i = 0; i < reader->schema.n_children; i++) {
      status = ArrowArrayFinishBuildingDefault(out->children[i], &reader->error);
      if (status != 0) break;
    }

    // If we didn't read any rows, the reader is exhausted - don't generate a spurious
    // batch
    if (batch_size == 0) out->release(out);
  }

  sqlite3_mutex_leave(sqlite3_db_mutex(reader->db));
  return status;
}

int StatementReaderGetSchema(struct ArrowArrayStream* self, struct ArrowSchema* out) {
  if (!self->release || !self->private_data) {
    return EINVAL;
  }

  struct StatementReader* reader = (struct StatementReader*)self->private_data;
  return ArrowSchemaDeepCopy(&reader->schema, out);
}

void StatementReaderRelease(struct ArrowArrayStream* self) {
  if (self->private_data) {
    struct StatementReader* reader = (struct StatementReader*)self->private_data;
    if (reader->schema.release) {
      reader->schema.release(&reader->schema);
    }
    if (reader->initial_batch.release) {
      reader->initial_batch.release(&reader->initial_batch);
    }
    if (reader->types) {
      free(reader->types);
    }
    if (reader->binder) {
      AdbcSqliteBinderRelease(reader->binder);
    }

    free(self->private_data);
  }
  self->private_data = NULL;
  self->release = NULL;
  self->get_last_error = NULL;
  self->get_next = NULL;
  self->get_schema = NULL;
}

// -- Type inferring reader ------------------------------------------
//
// Every column starts as INT64. A certain number of rows is read, and
// the column is upcast if a different type is read.  Afterwards,
// 'compatible' values are upcast (int64 <: double <: string) and
// 'incompatible' values will raise an error.
//
// Notes:
// - The upcasting may be different when done during the inference
//   stage vs the reading stage, because in the former, we have to do
//   the upcasting, and in the latter, we let SQLite handle it.
// - We don't use unions since those are a pain to work with.
//
// Improvements/to-dos:
// - SQLITE_BLOB type values are not handled
// - Columns where no non-NULL value is seen should be typed as STRING
//   for maximum flexibility for later values
// - Make this more flexible (e.g. choose whether to attempt to cast
//   incompatible values, or insert a null, instead of erroring)
// - Add the option to try to use SQLite's designated type for a
//   column, instead of inferring the type (where possible).

/// Initialize buffers for the first (type-inferred) batch of data.
/// Use raw buffers since the types may change.
AdbcStatusCode StatementReaderInitializeInfer(int num_columns, size_t infer_rows,
                                              struct ArrowBitmap* validity,
                                              struct ArrowBuffer* data,
                                              struct ArrowBuffer* binary,
                                              enum ArrowType* current_type,
                                              struct AdbcError* error) {
  for (int i = 0; i < num_columns; i++) {
    ArrowBitmapInit(&validity[i]);
    CHECK_NA(INTERNAL, ArrowBitmapReserve(&validity[i], infer_rows), error);
    ArrowBufferInit(&data[i]);
    CHECK_NA(INTERNAL, ArrowBufferReserve(&data[i], infer_rows * sizeof(int64_t)), error);
    memset(&binary[i], 0, sizeof(struct ArrowBuffer));
    current_type[i] = NANOARROW_TYPE_INT64;
  }
  return ADBC_STATUS_OK;
}  // NOLINT(whitespace/indent)

/// Finalize the first (type-inferred) batch of data.
AdbcStatusCode StatementReaderInferFinalize(
    sqlite3_stmt* stmt, int num_columns, int64_t num_rows, struct StatementReader* reader,
    struct ArrowBitmap* validity, struct ArrowBuffer* data, struct ArrowBuffer* binary,
    enum ArrowType* current_type, struct AdbcError* error) {
  ArrowSchemaInit(&reader->schema);
  CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(&reader->schema, num_columns), error);
  for (int col = 0; col < num_columns; col++) {
    struct ArrowSchema* field = reader->schema.children[col];
    const char* name = sqlite3_column_name(stmt, col);
    CHECK_NA(INTERNAL, ArrowSchemaSetType(field, current_type[col]), error);
    CHECK_NA(INTERNAL, ArrowSchemaSetName(field, name), error);
  }
  CHECK_NA(INTERNAL,
           ArrowArrayInitFromSchema(&reader->initial_batch, &reader->schema, NULL),
           error);

  // Do validation up front, so that we either move 0 buffers or all buffers
  for (int col = 0; col < num_columns; col++) {
    if (current_type[col] == NANOARROW_TYPE_STRING ||
        current_type[col] == NANOARROW_TYPE_BINARY) {
      if (binary[col].data == NULL) {
        SetError(error, "INTERNAL: column has binary-like type but no backing buffer");
        return ADBC_STATUS_INTERNAL;
      }
    }
    reader->initial_batch.children[col]->length = num_rows;
  }

  reader->initial_batch.length = num_rows;

  for (int col = 0; col < num_columns; col++) {
    struct ArrowArray* arr = reader->initial_batch.children[col];
    ArrowArraySetValidityBitmap(arr, &validity[col]);
    // XXX: ignore return code since we've hardcoded the buffer index
    (void)ArrowArraySetBuffer(arr, 1, &data[col]);
    if (current_type[col] == NANOARROW_TYPE_STRING ||
        current_type[col] == NANOARROW_TYPE_BINARY) {
      (void)ArrowArraySetBuffer(arr, 2, &binary[col]);
    }
    arr->length = num_rows;
  }
  return ADBC_STATUS_OK;
}

// Convert an int64 typed column to double.
AdbcStatusCode StatementReaderUpcastInt64ToDouble(struct ArrowBuffer* data,
                                                  struct AdbcError* error) {
  struct ArrowBuffer doubles;
  ArrowBufferInit(&doubles);
  CHECK_NA(INTERNAL, ArrowBufferReserve(&doubles, data->capacity_bytes), error);

  size_t num_elements = data->size_bytes / sizeof(int64_t);
  const int64_t* elements = (const int64_t*)data->data;
  for (size_t i = 0; i < num_elements; i++) {
    double value = elements[i];
    ArrowBufferAppendUnsafe(&doubles, &value, sizeof(double));
  }
  ArrowBufferReset(data);
  ArrowBufferMove(&doubles, data);
  return ADBC_STATUS_OK;
}

AdbcStatusCode StatementReaderAppendInt64ToBinary(struct ArrowBuffer* offsets,
                                                  struct ArrowBuffer* binary,
                                                  int64_t value, int32_t* offset,
                                                  struct AdbcError* error) {
  // Make sure we have at least 21 bytes available (19 digits + sign + null)
  // Presumably this is enough, but manpage for snprintf makes no guarantees
  // about whether locale may affect this, so check for truncation regardless
  static const size_t kReserve = 21;
  size_t buffer_size = kReserve;
  CHECK_NA(INTERNAL, ArrowBufferReserve(binary, buffer_size), error);
  char* output = (char*)(binary->data + binary->size_bytes);
  int written = 0;
  while (1) {
    written = snprintf(output, buffer_size, "%" PRId64, value);
    if (written < 0) {
      SetError(error, "Encoding error when upcasting double to string");
      return ADBC_STATUS_INTERNAL;
    } else if (((size_t)written) >= buffer_size) {
      // Truncated, resize and try again
      // Check for overflow - presumably this can never happen...?
      if (UINT_MAX - buffer_size < buffer_size) {
        SetError(error, "Overflow when upcasting double to string");
        return ADBC_STATUS_INTERNAL;
      }
      CHECK_NA(INTERNAL, ArrowBufferReserve(binary, buffer_size), error);
      buffer_size += buffer_size;
      continue;
    }
    break;
  }
  *offset += written;
  binary->size_bytes += written;
  ArrowBufferAppendUnsafe(offsets, offset, sizeof(int32_t));
  return ADBC_STATUS_OK;
}

AdbcStatusCode StatementReaderAppendDoubleToBinary(struct ArrowBuffer* offsets,
                                                   struct ArrowBuffer* binary,
                                                   double value, int32_t* offset,
                                                   struct AdbcError* error) {
  static const size_t kReserve = 64;
  size_t buffer_size = kReserve;
  CHECK_NA(INTERNAL, ArrowBufferReserve(binary, buffer_size), error);
  char* output = (char*)(binary->data + binary->size_bytes);
  int written = 0;
  while (1) {
    written = snprintf(output, buffer_size, "%e", value);
    if (written < 0) {
      SetError(error, "Encoding error when upcasting double to string");
      return ADBC_STATUS_INTERNAL;
    } else if (((size_t)written) >= buffer_size) {
      // Truncated, resize and try again
      // Check for overflow - presumably this can never happen...?
      if (UINT_MAX - buffer_size < buffer_size) {
        SetError(error, "Overflow when upcasting double to string");
        return ADBC_STATUS_INTERNAL;
      }
      CHECK_NA(INTERNAL, ArrowBufferReserve(binary, buffer_size), error);
      buffer_size += buffer_size;
      continue;
    }
    break;
  }
  *offset += written;
  binary->size_bytes += written;
  ArrowBufferAppendUnsafe(offsets, offset, sizeof(int32_t));
  return ADBC_STATUS_OK;
}

AdbcStatusCode StatementReaderUpcastInt64ToBinary(struct ArrowBuffer* data,
                                                  struct ArrowBuffer* binary,
                                                  struct AdbcError* error) {
  struct ArrowBuffer offsets;
  ArrowBufferInit(&offsets);
  ArrowBufferInit(binary);
  CHECK_NA(INTERNAL, ArrowBufferReserve(&offsets, data->capacity_bytes), error);
  CHECK_NA(INTERNAL, ArrowBufferReserve(binary, data->capacity_bytes), error);

  size_t num_elements = data->size_bytes / sizeof(int64_t);
  const int64_t* elements = (const int64_t*)data->data;

  int32_t offset = 0;
  ArrowBufferAppendUnsafe(&offsets, &offset, sizeof(int32_t));
  for (size_t i = 0; i < num_elements; i++) {
    AdbcStatusCode status =
        StatementReaderAppendInt64ToBinary(&offsets, binary, elements[i], &offset, error);
    if (status != ADBC_STATUS_OK) return status;
  }
  ArrowBufferReset(data);
  ArrowBufferMove(&offsets, data);
  return ADBC_STATUS_OK;
}

AdbcStatusCode StatementReaderUpcastDoubleToBinary(struct ArrowBuffer* data,
                                                   struct ArrowBuffer* binary,
                                                   struct AdbcError* error) {
  struct ArrowBuffer offsets;
  ArrowBufferInit(&offsets);
  ArrowBufferInit(binary);
  CHECK_NA(INTERNAL, ArrowBufferReserve(&offsets, data->capacity_bytes), error);
  CHECK_NA(INTERNAL, ArrowBufferReserve(binary, data->capacity_bytes), error);

  size_t num_elements = data->size_bytes / sizeof(double);
  const double* elements = (const double*)data->data;

  int32_t offset = 0;
  ArrowBufferAppendUnsafe(&offsets, &offset, sizeof(int32_t));
  for (size_t i = 0; i < num_elements; i++) {
    AdbcStatusCode status = StatementReaderAppendDoubleToBinary(
        &offsets, binary, elements[i], &offset, error);
    if (status != ADBC_STATUS_OK) return status;
  }
  ArrowBufferReset(data);
  ArrowBufferMove(&offsets, data);
  return ADBC_STATUS_OK;
}

/// Append a single value to a single column.
AdbcStatusCode StatementReaderInferOneValue(
    sqlite3_stmt* stmt, int col, struct ArrowBitmap* validity, struct ArrowBuffer* data,
    struct ArrowBuffer* binary, enum ArrowType* current_type, struct AdbcError* error) {
  // TODO: static_assert sizeof(int64) == sizeof(double)

  int sqlite_type = sqlite3_column_type(stmt, col);
  switch (sqlite_type) {
    case SQLITE_NULL: {
      ArrowBitmapAppendUnsafe(validity, /*set=*/0, /*length=*/1);
      switch (*current_type) {
        case NANOARROW_TYPE_INT64: {
          int64_t value = 0;
          ArrowBufferAppendUnsafe(data, &value, sizeof(int64_t));
          break;
        }
        case NANOARROW_TYPE_DOUBLE: {
          double value = 0.0;
          ArrowBufferAppendUnsafe(data, &value, sizeof(int64_t));
          break;
        }
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_BINARY: {
          const int32_t offset = ((int32_t*)data->data)[data->size_bytes / 4 - 1];
          CHECK_NA(INTERNAL, ArrowBufferAppend(data, &offset, sizeof(offset)), error);
          return ADBC_STATUS_OK;
        }
        default:
          return ADBC_STATUS_INTERNAL;
      }
      break;
    }
    case SQLITE_INTEGER: {
      ArrowBitmapAppendUnsafe(validity, /*set=*/1, /*length=*/1);

      switch (*current_type) {
        case NANOARROW_TYPE_INT64: {
          int64_t value = sqlite3_column_int64(stmt, col);
          ArrowBufferAppendUnsafe(data, &value, sizeof(int64_t));
          break;
        }
        case NANOARROW_TYPE_DOUBLE: {
          double value = sqlite3_column_double(stmt, col);
          ArrowBufferAppendUnsafe(data, &value, sizeof(int64_t));
          break;
        }
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_BINARY: {
          int32_t offset = ((int32_t*)data->data)[data->size_bytes / 4 - 1];
          return StatementReaderAppendInt64ToBinary(
              data, binary, sqlite3_column_int64(stmt, col), &offset, error);
        }
        default:
          return ADBC_STATUS_INTERNAL;
      }
      break;
    }
    case SQLITE_FLOAT: {
      ArrowBitmapAppendUnsafe(validity, /*set=*/1, /*length=*/1);

      switch (*current_type) {
        case NANOARROW_TYPE_INT64: {
          AdbcStatusCode status = StatementReaderUpcastInt64ToDouble(data, error);
          if (status != ADBC_STATUS_OK) return status;
          *current_type = NANOARROW_TYPE_DOUBLE;
          double value = sqlite3_column_double(stmt, col);
          ArrowBufferAppendUnsafe(data, &value, sizeof(double));
          break;
        }
        case NANOARROW_TYPE_DOUBLE: {
          double value = sqlite3_column_double(stmt, col);
          ArrowBufferAppendUnsafe(data, &value, sizeof(double));
          break;
        }
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_BINARY: {
          int32_t offset = ((int32_t*)data->data)[data->size_bytes / 4 - 1];
          return StatementReaderAppendDoubleToBinary(
              data, binary, sqlite3_column_double(stmt, col), &offset, error);
        }
        default:
          return ADBC_STATUS_INTERNAL;
      }
      break;
    }
    case SQLITE_TEXT: {
      ArrowBitmapAppendUnsafe(validity, /*set=*/1, /*length=*/1);

      switch (*current_type) {
        case NANOARROW_TYPE_INT64: {
          AdbcStatusCode status = StatementReaderUpcastInt64ToBinary(data, binary, error);
          if (status != ADBC_STATUS_OK) return status;
          *current_type = NANOARROW_TYPE_STRING;
          break;
        }
        case NANOARROW_TYPE_DOUBLE: {
          AdbcStatusCode status =
              StatementReaderUpcastDoubleToBinary(data, binary, error);
          if (status != ADBC_STATUS_OK) return status;
          *current_type = NANOARROW_TYPE_STRING;
          break;
        }
        case NANOARROW_TYPE_STRING:
        case NANOARROW_TYPE_BINARY:
          break;
        default:
          return ADBC_STATUS_INTERNAL;
      }

      const unsigned char* value = sqlite3_column_text(stmt, col);
      const int size = sqlite3_column_bytes(stmt, col);
      const int32_t offset = ((int32_t*)data->data)[data->size_bytes / 4 - 1] + size;
      CHECK_NA(INTERNAL, ArrowBufferAppend(binary, value, size), error);
      CHECK_NA(INTERNAL, ArrowBufferAppend(data, &offset, sizeof(offset)), error);
      break;
    }
    case SQLITE_BLOB: {
      ArrowBitmapAppendUnsafe(validity, /*set=*/1, /*length=*/1);

      switch (*current_type) {
        case NANOARROW_TYPE_INT64: {
          AdbcStatusCode status = StatementReaderUpcastInt64ToBinary(data, binary, error);
          if (status != ADBC_STATUS_OK) return status;
          *current_type = NANOARROW_TYPE_BINARY;
          break;
        }
        case NANOARROW_TYPE_DOUBLE: {
          AdbcStatusCode status =
              StatementReaderUpcastDoubleToBinary(data, binary, error);
          if (status != ADBC_STATUS_OK) return status;
          *current_type = NANOARROW_TYPE_BINARY;
          break;
        }
        case NANOARROW_TYPE_STRING:
          *current_type = NANOARROW_TYPE_BINARY;
          break;
        case NANOARROW_TYPE_BINARY:
          break;
        default:
          return ADBC_STATUS_INTERNAL;
      }

      const void* value = sqlite3_column_blob(stmt, col);
      const int size = sqlite3_column_bytes(stmt, col);
      const int32_t offset = ((int32_t*)data->data)[data->size_bytes / 4 - 1] + size;
      CHECK_NA(INTERNAL, ArrowBufferAppend(binary, value, size), error);
      CHECK_NA(INTERNAL, ArrowBufferAppend(data, &offset, sizeof(offset)), error);
      break;
    }
    default: {
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }
  }
  return ADBC_STATUS_OK;
}  // NOLINT(whitespace/indent)

AdbcStatusCode AdbcSqliteExportReader(sqlite3* db, sqlite3_stmt* stmt,
                                      struct AdbcSqliteBinder* binder, size_t batch_size,
                                      struct ArrowArrayStream* stream,
                                      struct AdbcError* error) {
  struct StatementReader* reader = malloc(sizeof(struct StatementReader));
  memset(reader, 0, sizeof(struct StatementReader));
  reader->db = db;
  reader->stmt = stmt;
  reader->batch_size = batch_size;

  stream->private_data = reader;
  stream->release = StatementReaderRelease;
  stream->get_last_error = StatementReaderGetLastError;
  stream->get_next = StatementReaderGetNext;
  stream->get_schema = StatementReaderGetSchema;

  sqlite3_mutex_enter(sqlite3_db_mutex(db));

  const int num_columns = sqlite3_column_count(stmt);
  struct ArrowBitmap* validity = malloc(num_columns * sizeof(struct ArrowBitmap));
  struct ArrowBuffer* data = malloc(num_columns * sizeof(struct ArrowBuffer));
  struct ArrowBuffer* binary = malloc(num_columns * sizeof(struct ArrowBuffer));
  enum ArrowType* current_type = malloc(num_columns * sizeof(enum ArrowType));

  AdbcStatusCode status = StatementReaderInitializeInfer(
      num_columns, batch_size, validity, data, binary, current_type, error);

  if (binder) {
    char finished = 0;
    status = AdbcSqliteBinderBindNext(binder, db, stmt, &finished, error);
    if (finished) {
      reader->done = 1;
    }
  }

  if (status == ADBC_STATUS_OK && !reader->done) {
    int64_t num_rows = 0;
    while (((size_t)num_rows) < batch_size) {
      int rc = sqlite3_step(stmt);
      if (rc == SQLITE_DONE) {
        if (!binder) {
          reader->done = 1;
          break;
        } else {
          char finished = 0;
          status = AdbcSqliteBinderBindNext(binder, db, stmt, &finished, error);
          if (status != ADBC_STATUS_OK) break;
          if (finished) {
            reader->done = 1;
            break;
          }
        }
        continue;
      } else if (rc == SQLITE_ERROR) {
        SetError(error, "Failed to step query: %s", sqlite3_errmsg(db));
        status = ADBC_STATUS_IO;
        // Reset here so that we don't get an error again in StatementRelease
        (void)sqlite3_reset(stmt);
        break;
      } else if (rc != SQLITE_ROW) {
        status = ADBC_STATUS_INTERNAL;
        break;
      }

      for (int col = 0; col < num_columns; col++) {
        status = StatementReaderInferOneValue(stmt, col, &validity[col], &data[col],
                                              &binary[col], &current_type[col], error);
        if (status != ADBC_STATUS_OK) break;
      }
      if (status != ADBC_STATUS_OK) break;
      num_rows++;
    }

    if (status == ADBC_STATUS_OK) {
      status = StatementReaderInferFinalize(stmt, num_columns, num_rows, reader, validity,
                                            data, binary, current_type, error);
    }
  }

  if (status != ADBC_STATUS_OK) {
    // Free the individual buffers
    // This is OK, since InferFinalize either moves all buffers or no buffers
    for (int i = 0; i < num_columns; i++) {
      ArrowBitmapReset(&validity[i]);
      ArrowBufferReset(&data[i]);
      ArrowBufferReset(&binary[i]);
    }
    free(current_type);
  } else {
    reader->types = current_type;
    reader->binder = binder;
  }

  free(data);
  free(validity);
  free(binary);

  sqlite3_mutex_leave(sqlite3_db_mutex(db));
  return status;
}  // NOLINT(whitespace/indent)
