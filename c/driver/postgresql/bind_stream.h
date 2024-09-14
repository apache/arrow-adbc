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

#if !defined(NOMINMAX)
#define NOMINMAX
#endif

#include <algorithm>
#include <climits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>

#include "copy/writer.h"
#include "driver/common/utils.h"
#include "error.h"
#include "postgres_type.h"
#include "postgres_util.h"

namespace adbcpq {

/// The flag indicating to PostgreSQL that we want binary-format values.
constexpr int kPgBinaryFormat = 1;

/// Helper to manage bind parameters with a prepared statement
struct BindStream {
  Handle<struct ArrowArrayStream> bind;
  Handle<struct ArrowArrayView> array_view;
  Handle<struct ArrowArray> current;
  Handle<struct ArrowSchema> bind_schema;
  int64_t current_row = -1;

  std::vector<struct ArrowSchemaView> bind_schema_fields;
  std::vector<std::unique_ptr<PostgresCopyFieldWriter>> bind_field_writers;

  // OIDs for parameter types
  std::vector<uint32_t> param_types;
  std::vector<char*> param_values;
  std::vector<int> param_formats;
  std::vector<int> param_lengths;
  Handle<struct ArrowBuffer> param_buffer;

  bool has_tz_field = false;
  std::string tz_setting;

  struct ArrowError na_error;

  BindStream() {
    this->bind->release = nullptr;
    std::memset(&na_error, 0, sizeof(na_error));
  }

  void SetBind(struct ArrowArrayStream* stream) {
    this->bind.reset();
    ArrowArrayStreamMove(stream, &bind.value);
  }

  template <typename Callback>
  AdbcStatusCode Begin(Callback&& callback, struct AdbcError* error) {
    CHECK_NA_DETAIL(INTERNAL,
                    ArrowArrayStreamGetSchema(&bind.value, &bind_schema.value, &na_error),
                    &na_error, error);

    struct ArrowSchemaView bind_schema_view;
    CHECK_NA_DETAIL(INTERNAL,
                    ArrowSchemaViewInit(&bind_schema_view, &bind_schema.value, &na_error),
                    &na_error, error);
    if (bind_schema_view.type != ArrowType::NANOARROW_TYPE_STRUCT) {
      SetError(error, "%s", "[libpq] Bind parameters must have type STRUCT");
      return ADBC_STATUS_INVALID_STATE;
    }

    bind_schema_fields.resize(bind_schema->n_children);
    for (size_t i = 0; i < bind_schema_fields.size(); i++) {
      CHECK_NA(INTERNAL,
               ArrowSchemaViewInit(&bind_schema_fields[i], bind_schema->children[i],
                                   /*error*/ nullptr),
               error);
    }

    CHECK_NA_DETAIL(
        INTERNAL,
        ArrowArrayViewInitFromSchema(&array_view.value, &bind_schema.value, &na_error),
        &na_error, error);

    ArrowBufferInit(&param_buffer.value);

    return std::move(callback)();
  }

  AdbcStatusCode SetParamTypes(PGconn* pg_conn, const PostgresTypeResolver& type_resolver,
                               const bool autocommit, struct AdbcError* error) {
    param_types.resize(bind_schema->n_children);
    param_values.resize(bind_schema->n_children);
    param_lengths.resize(bind_schema->n_children);
    param_formats.resize(bind_schema->n_children, kPgBinaryFormat);
    bind_field_writers.resize(bind_schema->n_children);

    for (size_t i = 0; i < bind_field_writers.size(); i++) {
      PostgresType type;
      CHECK_NA_DETAIL(INTERNAL,
                      PostgresType::FromSchema(type_resolver, bind_schema->children[i],
                                               &type, &na_error),
                      &na_error, error);

      // tz-aware timestamps require special handling to set the timezone to UTC
      // prior to sending over the binary protocol; must be reset after execute
      if (!has_tz_field && type.type_id() == PostgresTypeId::kTimestamptz) {
        RAISE_ADBC(SetDatabaseTimezoneUTC(pg_conn, autocommit, error));
        has_tz_field = true;
      }

      std::unique_ptr<PostgresCopyFieldWriter> writer;
      CHECK_NA_DETAIL(
          INTERNAL,
          MakeCopyFieldWriter(bind_schema->children[i], array_view->children[i],
                              type_resolver, &writer, &na_error),
          &na_error, error);

      param_types[i] = type.oid();
      param_formats[i] = kPgBinaryFormat;
      bind_field_writers[i] = std::move(writer);
    }

    return ADBC_STATUS_OK;
  }

  AdbcStatusCode SetDatabaseTimezoneUTC(PGconn* pg_conn, const bool autocommit,
                                        struct AdbcError* error) {
    if (autocommit) {
      PGresult* begin_result = PQexec(pg_conn, "BEGIN");
      if (PQresultStatus(begin_result) != PGRES_COMMAND_OK) {
        AdbcStatusCode code =
            SetError(error, begin_result,
                     "[libpq] Failed to begin transaction for timezone data: %s",
                     PQerrorMessage(pg_conn));
        PQclear(begin_result);
        return code;
      }
      PQclear(begin_result);
    }

    PGresult* get_tz_result = PQexec(pg_conn, "SELECT current_setting('TIMEZONE')");
    if (PQresultStatus(get_tz_result) != PGRES_TUPLES_OK) {
      AdbcStatusCode code =
          SetError(error, get_tz_result, "[libpq] Could not query current timezone: %s",
                   PQerrorMessage(pg_conn));
      PQclear(get_tz_result);
      return code;
    }

    tz_setting = std::string(PQgetvalue(get_tz_result, 0, 0));
    PQclear(get_tz_result);

    PGresult* set_utc_result = PQexec(pg_conn, "SET TIME ZONE 'UTC'");
    if (PQresultStatus(set_utc_result) != PGRES_COMMAND_OK) {
      AdbcStatusCode code =
          SetError(error, set_utc_result, "[libpq] Failed to set time zone to UTC: %s",
                   PQerrorMessage(pg_conn));
      PQclear(set_utc_result);
      return code;
    }
    PQclear(set_utc_result);

    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Prepare(PGconn* pg_conn, const std::string& query,
                         struct AdbcError* error) {
    PGresult* result = PQprepare(pg_conn, /*stmtName=*/"", query.c_str(),
                                 /*nParams=*/bind_schema->n_children, param_types.data());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      AdbcStatusCode code =
          SetError(error, result, "[libpq] Failed to prepare query: %s\nQuery was:%s",
                   PQerrorMessage(pg_conn), query.c_str());
      PQclear(result);
      return code;
    }
    PQclear(result);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode PullNextArray(AdbcError* error) {
    if (current->release != nullptr) ArrowArrayRelease(&current.value);

    CHECK_NA_DETAIL(IO, ArrowArrayStreamGetNext(&bind.value, &current.value, &na_error),
                    &na_error, error);

    if (current->release != nullptr) {
      CHECK_NA_DETAIL(
          INTERNAL, ArrowArrayViewSetArray(&array_view.value, &current.value, &na_error),
          &na_error, error);
    }

    return ADBC_STATUS_OK;
  }

  AdbcStatusCode EnsureNextRow(AdbcError* error) {
    if (current->release != nullptr) {
      current_row++;
      if (current_row < current->length) {
        return ADBC_STATUS_OK;
      }
    }

    // Pull until we have an array with at least one row or the stream is finished
    do {
      RAISE_ADBC(PullNextArray(error));
      if (current->release == nullptr) {
        current_row = -1;
        return ADBC_STATUS_OK;
      }
    } while (current->length == 0);

    current_row = 0;
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode BindAndExecuteCurrentRow(PGconn* pg_conn, PGresult** result_out,
                                          int result_format, AdbcError* error) {
    param_buffer->size_bytes = 0;
    int64_t last_offset = 0;

    for (int64_t col = 0; col < array_view->n_children; col++) {
      if (!ArrowArrayViewIsNull(array_view->children[col], current_row)) {
        // Note that this Write() call currently writes the (int32_t) byte size of the
        // field in addition to the serialized value.
        CHECK_NA_DETAIL(
            INTERNAL,
            bind_field_writers[col]->Write(&param_buffer.value, current_row, &na_error),
            &na_error, error);
      } else {
        CHECK_NA(INTERNAL, ArrowBufferAppendInt32(&param_buffer.value, 0), error);
      }

      int64_t param_length = param_buffer->size_bytes - last_offset - sizeof(int32_t);
      if (param_length > INT32_MAX) {
        SetError(error, "Parameter %" PRId64 " serialized to >2GB of binary", col);
        return ADBC_STATUS_INTERNAL;
      }

      param_lengths[col] = static_cast<int>(param_length);
      last_offset = param_buffer->size_bytes;
    }

    last_offset = 0;
    for (int64_t col = 0; col < array_view->n_children; col++) {
      last_offset += sizeof(int32_t);
      if (param_lengths[col] == 0) {
        param_values[col] = nullptr;
      } else {
        param_values[col] = reinterpret_cast<char*>(param_buffer->data) + last_offset;
      }
      last_offset += param_lengths[col];
    }

    PGresult* result =
        PQexecPrepared(pg_conn, /*stmtName=*/"",
                       /*nParams=*/bind_schema->n_children, param_values.data(),
                       param_lengths.data(), param_formats.data(), result_format);

    ExecStatusType pg_status = PQresultStatus(result);
    if (pg_status != PGRES_COMMAND_OK && pg_status != PGRES_TUPLES_OK) {
      AdbcStatusCode code =
          SetError(error, result, "[libpq] Failed to execute prepared statement: %s %s",
                   PQresStatus(pg_status), PQerrorMessage(pg_conn));
      PQclear(result);
      return code;
    }

    *result_out = result;
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Cleanup(PGconn* pg_conn, AdbcError* error) {
    if (has_tz_field) {
      std::string reset_query = "SET TIME ZONE '" + tz_setting + "'";
      PGresult* reset_tz_result = PQexec(pg_conn, reset_query.c_str());
      if (PQresultStatus(reset_tz_result) != PGRES_COMMAND_OK) {
        AdbcStatusCode code =
            SetError(error, reset_tz_result, "[libpq] Failed to reset time zone: %s",
                     PQerrorMessage(pg_conn));
        PQclear(reset_tz_result);
        return code;
      }
      PQclear(reset_tz_result);

      PGresult* commit_result = PQexec(pg_conn, "COMMIT");
      if (PQresultStatus(commit_result) != PGRES_COMMAND_OK) {
        AdbcStatusCode code =
            SetError(error, commit_result, "[libpq] Failed to commit transaction: %s",
                     PQerrorMessage(pg_conn));
        PQclear(commit_result);
        return code;
      }
      PQclear(commit_result);
    }

    return ADBC_STATUS_OK;
  }

  AdbcStatusCode ExecuteCopy(PGconn* pg_conn, const PostgresTypeResolver& type_resolver,
                             int64_t* rows_affected, struct AdbcError* error) {
    if (rows_affected) *rows_affected = 0;

    PostgresCopyStreamWriter writer;
    CHECK_NA(INTERNAL, writer.Init(&bind_schema.value), error);
    CHECK_NA_DETAIL(INTERNAL, writer.InitFieldWriters(type_resolver, &na_error),
                    &na_error, error);

    CHECK_NA_DETAIL(INTERNAL, writer.WriteHeader(&na_error), &na_error, error);

    while (true) {
      RAISE_ADBC(PullNextArray(error));
      if (!current->release) break;

      CHECK_NA(INTERNAL, writer.SetArray(&current.value), error);

      // build writer buffer
      int write_result;
      do {
        write_result = writer.WriteRecord(&na_error);
      } while (write_result == NANOARROW_OK);

      // check if not ENODATA at exit
      if (write_result != ENODATA) {
        SetError(error, "Error occurred writing COPY data: %s", PQerrorMessage(pg_conn));
        return ADBC_STATUS_IO;
      }

      RAISE_ADBC(FlushCopyWriterToConn(pg_conn, writer, error));

      if (rows_affected) *rows_affected += current->length;
      writer.Rewind();
    }

    // If there were no arrays in the stream, we haven't flushed yet
    RAISE_ADBC(FlushCopyWriterToConn(pg_conn, writer, error));

    if (PQputCopyEnd(pg_conn, NULL) <= 0) {
      SetError(error, "Error message returned by PQputCopyEnd: %s",
               PQerrorMessage(pg_conn));
      return ADBC_STATUS_IO;
    }

    PGresult* result = PQgetResult(pg_conn);
    ExecStatusType pg_status = PQresultStatus(result);
    if (pg_status != PGRES_COMMAND_OK) {
      AdbcStatusCode code =
          SetError(error, result, "[libpq] Failed to execute COPY statement: %s %s",
                   PQresStatus(pg_status), PQerrorMessage(pg_conn));
      PQclear(result);
      return code;
    }

    PQclear(result);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode FlushCopyWriterToConn(PGconn* pg_conn,
                                       const PostgresCopyStreamWriter& writer,
                                       struct AdbcError* error) {
    // https://github.com/apache/arrow-adbc/issues/1921: PostgreSQL has a max
    // size for a single message that we need to respect (1 GiB - 1).  Since
    // the buffer can be chunked up as much as we want, go for 16 MiB as our
    // limit.
    // https://github.com/postgres/postgres/blob/23c5a0e7d43bc925c6001538f04a458933a11fc1/src/common/stringinfo.c#L28
    constexpr int64_t kMaxCopyBufferSize = 0x1000000;
    ArrowBuffer buffer = writer.WriteBuffer();

    auto* data = reinterpret_cast<char*>(buffer.data);
    int64_t remaining = buffer.size_bytes;
    while (remaining > 0) {
      int64_t to_write = std::min<int64_t>(remaining, kMaxCopyBufferSize);
      if (PQputCopyData(pg_conn, data, to_write) <= 0) {
        SetError(error, "Error writing tuple field data: %s", PQerrorMessage(pg_conn));
        return ADBC_STATUS_IO;
      }
      remaining -= to_write;
      data += to_write;
    }

    return ADBC_STATUS_OK;
  }
};
}  // namespace adbcpq
