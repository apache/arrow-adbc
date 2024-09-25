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

#include <algorithm>
#include <limits>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>

#include "copy/writer.h"
#include "error.h"
#include "postgres_type.h"
#include "postgres_util.h"
#include "result_helper.h"

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
  Status Begin(Callback&& callback) {
    UNWRAP_NANOARROW(
        na_error, Internal,
        ArrowArrayStreamGetSchema(&bind.value, &bind_schema.value, &na_error));

    struct ArrowSchemaView bind_schema_view;
    UNWRAP_NANOARROW(
        na_error, Internal,
        ArrowSchemaViewInit(&bind_schema_view, &bind_schema.value, &na_error));
    if (bind_schema_view.type != ArrowType::NANOARROW_TYPE_STRUCT) {
      return Status::InvalidState("[libpq] Bind parameters must have type STRUCT");
    }

    bind_schema_fields.resize(bind_schema->n_children);
    for (size_t i = 0; i < bind_schema_fields.size(); i++) {
      UNWRAP_ERRNO(Internal,
                   ArrowSchemaViewInit(&bind_schema_fields[i], bind_schema->children[i],
                                       /*error*/ nullptr));
    }

    UNWRAP_NANOARROW(
        na_error, Internal,
        ArrowArrayViewInitFromSchema(&array_view.value, &bind_schema.value, &na_error));

    ArrowBufferInit(&param_buffer.value);

    return std::move(callback)();
  }

  Status SetParamTypes(PGconn* pg_conn, const PostgresTypeResolver& type_resolver,
                       const bool autocommit) {
    param_types.resize(bind_schema->n_children);
    param_values.resize(bind_schema->n_children);
    param_lengths.resize(bind_schema->n_children);
    param_formats.resize(bind_schema->n_children, kPgBinaryFormat);
    bind_field_writers.resize(bind_schema->n_children);

    for (size_t i = 0; i < bind_field_writers.size(); i++) {
      PostgresType type;
      UNWRAP_NANOARROW(na_error, Internal,
                       PostgresType::FromSchema(type_resolver, bind_schema->children[i],
                                                &type, &na_error));

      // tz-aware timestamps require special handling to set the timezone to UTC
      // prior to sending over the binary protocol; must be reset after execute
      if (!has_tz_field && type.type_id() == PostgresTypeId::kTimestamptz) {
        UNWRAP_STATUS(SetDatabaseTimezoneUTC(pg_conn, autocommit));
        has_tz_field = true;
      }

      std::unique_ptr<PostgresCopyFieldWriter> writer;
      UNWRAP_NANOARROW(
          na_error, Internal,
          MakeCopyFieldWriter(bind_schema->children[i], array_view->children[i],
                              type_resolver, &writer, &na_error));

      param_types[i] = type.oid();
      param_formats[i] = kPgBinaryFormat;
      bind_field_writers[i] = std::move(writer);
    }

    return Status::Ok();
  }

  Status SetDatabaseTimezoneUTC(PGconn* pg_conn, const bool autocommit) {
    if (autocommit) {
      PqResultHelper helper(pg_conn, "BEGIN");
      UNWRAP_STATUS(helper.Execute());
    }

    PqResultHelper get_tz(pg_conn, "SELECT current_setting('TIMEZONE')");
    UNWRAP_STATUS(get_tz.Execute());
    for (auto row : get_tz) {
      tz_setting = row[0].value();
    }

    PqResultHelper set_utc(pg_conn, "SET TIME ZONE 'UTC'");
    UNWRAP_STATUS(set_utc.Execute());

    return Status::Ok();
  }

  Status Prepare(PGconn* pg_conn, const std::string& query) {
    PqResultHelper helper(pg_conn, query);
    UNWRAP_STATUS(helper.Prepare(param_types));
    return Status::Ok();
  }

  Status PullNextArray() {
    if (current->release != nullptr) ArrowArrayRelease(&current.value);

    UNWRAP_NANOARROW(na_error, IO,
                     ArrowArrayStreamGetNext(&bind.value, &current.value, &na_error));

    if (current->release != nullptr) {
      UNWRAP_NANOARROW(
          na_error, Internal,
          ArrowArrayViewSetArray(&array_view.value, &current.value, &na_error));
    }

    return Status::Ok();
  }

  Status EnsureNextRow() {
    if (current->release != nullptr) {
      current_row++;
      if (current_row < current->length) {
        return Status::Ok();
      }
    }

    // Pull until we have an array with at least one row or the stream is finished
    do {
      UNWRAP_STATUS(PullNextArray());
      if (current->release == nullptr) {
        current_row = -1;
        return Status::Ok();
      }
    } while (current->length == 0);

    current_row = 0;
    return Status::Ok();
  }

  Status BindAndExecuteCurrentRow(PGconn* pg_conn, PGresult** result_out,
                                  int result_format) {
    param_buffer->size_bytes = 0;
    int64_t last_offset = 0;

    for (int64_t col = 0; col < array_view->n_children; col++) {
      if (!ArrowArrayViewIsNull(array_view->children[col], current_row)) {
        // Note that this Write() call currently writes the (int32_t) byte size of the
        // field in addition to the serialized value.
        UNWRAP_NANOARROW(
            na_error, Internal,
            bind_field_writers[col]->Write(&param_buffer.value, current_row, &na_error));
      } else {
        UNWRAP_ERRNO(Internal, ArrowBufferAppendInt32(&param_buffer.value, 0));
      }

      int64_t param_length = param_buffer->size_bytes - last_offset - sizeof(int32_t);
      if (param_length > (std::numeric_limits<int>::max)()) {
        return Status::Internal("Paramter ", col, "serialized to >2GB of binary");
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
      Status status =
          MakeStatus(result, "[libpq] Failed to execute prepared statement: {} {}",
                     PQresStatus(pg_status), PQerrorMessage(pg_conn));
      PQclear(result);
      return status;
    }

    *result_out = result;
    return Status::Ok();
  }

  Status Cleanup(PGconn* pg_conn) {
    if (has_tz_field) {
      PqResultHelper reset(pg_conn, "SET TIME ZONE '" + tz_setting + "'");
      UNWRAP_STATUS(reset.Execute());

      PqResultHelper commit(pg_conn, "COMMIT");
      UNWRAP_STATUS(reset.Execute());
    }

    return Status::Ok();
  }

  Status ExecuteCopy(PGconn* pg_conn, const PostgresTypeResolver& type_resolver,
                     int64_t* rows_affected) {
    if (rows_affected) *rows_affected = 0;

    PostgresCopyStreamWriter writer;
    UNWRAP_ERRNO(Internal, writer.Init(&bind_schema.value));
    UNWRAP_NANOARROW(na_error, Internal,
                     writer.InitFieldWriters(type_resolver, &na_error));

    UNWRAP_NANOARROW(na_error, Internal, writer.WriteHeader(&na_error));

    while (true) {
      UNWRAP_STATUS(PullNextArray());
      if (!current->release) break;

      UNWRAP_ERRNO(Internal, writer.SetArray(&current.value));

      // build writer buffer
      int write_result;
      do {
        write_result = writer.WriteRecord(&na_error);
      } while (write_result == NANOARROW_OK);

      // check if not ENODATA at exit
      if (write_result != ENODATA) {
        return Status::IO("Error occurred writing COPY data: ", PQerrorMessage(pg_conn));
      }

      UNWRAP_STATUS(FlushCopyWriterToConn(pg_conn, writer));

      if (rows_affected) *rows_affected += current->length;
      writer.Rewind();
    }

    // If there were no arrays in the stream, we haven't flushed yet
    UNWRAP_STATUS(FlushCopyWriterToConn(pg_conn, writer));

    if (PQputCopyEnd(pg_conn, NULL) <= 0) {
      return Status::IO("Error message returned by PQputCopyEnd: ",
                        PQerrorMessage(pg_conn));
    }

    PGresult* result = PQgetResult(pg_conn);
    ExecStatusType pg_status = PQresultStatus(result);
    if (pg_status != PGRES_COMMAND_OK) {
      Status status =
          MakeStatus(result, "[libpq] Failed to execute COPY statement: {} {}",
                     PQresStatus(pg_status), PQerrorMessage(pg_conn));
      PQclear(result);
      return status;
    }

    PQclear(result);
    return Status::Ok();
  }

  Status FlushCopyWriterToConn(PGconn* pg_conn, const PostgresCopyStreamWriter& writer) {
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
        return Status::IO("Error writing tuple field data: ", PQerrorMessage(pg_conn));
      }
      remaining -= to_write;
      data += to_write;
    }

    return Status::Ok();
  }
};
}  // namespace adbcpq
