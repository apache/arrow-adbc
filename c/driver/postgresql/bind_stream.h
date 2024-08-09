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

  struct ArrowSchemaView bind_schema_view;
  std::vector<struct ArrowSchemaView> bind_schema_fields;

  // OIDs for parameter types
  std::vector<uint32_t> param_types;
  std::vector<char*> param_values;
  std::vector<int> param_lengths;
  std::vector<int> param_formats;
  std::vector<size_t> param_values_offsets;
  std::vector<char> param_values_buffer;
  // XXX: this assumes fixed-length fields only - will need more
  // consideration to deal with variable-length fields

  bool has_tz_field = false;
  std::string tz_setting;

  struct ArrowError na_error;

  explicit BindStream(struct ArrowArrayStream&& bind) {
    this->bind.value = std::move(bind);
    std::memset(&na_error, 0, sizeof(na_error));
  }

  template <typename Callback>
  AdbcStatusCode Begin(Callback&& callback, struct AdbcError* error) {
    CHECK_NA_DETAIL(INTERNAL,
                    ArrowArrayStreamGetSchema(&bind.value, &bind_schema.value, &na_error),
                    &na_error, error);
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

    return std::move(callback)();
  }

  AdbcStatusCode SetParamTypes(const PostgresTypeResolver& type_resolver,
                               struct AdbcError* error) {
    param_types.resize(bind_schema->n_children);
    param_values.resize(bind_schema->n_children);
    param_lengths.resize(bind_schema->n_children);
    param_formats.resize(bind_schema->n_children, kPgBinaryFormat);
    param_values_offsets.reserve(bind_schema->n_children);

    for (size_t i = 0; i < bind_schema_fields.size(); i++) {
      PostgresTypeId type_id;
      switch (bind_schema_fields[i].type) {
        case ArrowType::NANOARROW_TYPE_BOOL:
          type_id = PostgresTypeId::kBool;
          param_lengths[i] = 1;
          break;
        case ArrowType::NANOARROW_TYPE_INT8:
        case ArrowType::NANOARROW_TYPE_INT16:
          type_id = PostgresTypeId::kInt2;
          param_lengths[i] = 2;
          break;
        case ArrowType::NANOARROW_TYPE_INT32:
          type_id = PostgresTypeId::kInt4;
          param_lengths[i] = 4;
          break;
        case ArrowType::NANOARROW_TYPE_INT64:
          type_id = PostgresTypeId::kInt8;
          param_lengths[i] = 8;
          break;
        case ArrowType::NANOARROW_TYPE_FLOAT:
          type_id = PostgresTypeId::kFloat4;
          param_lengths[i] = 4;
          break;
        case ArrowType::NANOARROW_TYPE_DOUBLE:
          type_id = PostgresTypeId::kFloat8;
          param_lengths[i] = 8;
          break;
        case ArrowType::NANOARROW_TYPE_STRING:
        case ArrowType::NANOARROW_TYPE_LARGE_STRING:
          type_id = PostgresTypeId::kText;
          param_lengths[i] = 0;
          break;
        case ArrowType::NANOARROW_TYPE_BINARY:
          type_id = PostgresTypeId::kBytea;
          param_lengths[i] = 0;
          break;
        case ArrowType::NANOARROW_TYPE_DATE32:
          type_id = PostgresTypeId::kDate;
          param_lengths[i] = 4;
          break;
        case ArrowType::NANOARROW_TYPE_TIMESTAMP:
          type_id = PostgresTypeId::kTimestamp;
          param_lengths[i] = 8;
          break;
        case ArrowType::NANOARROW_TYPE_DURATION:
        case ArrowType::NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
          type_id = PostgresTypeId::kInterval;
          param_lengths[i] = 16;
          break;
        case ArrowType::NANOARROW_TYPE_DECIMAL128:
        case ArrowType::NANOARROW_TYPE_DECIMAL256:
          type_id = PostgresTypeId::kNumeric;
          param_lengths[i] = 0;
          break;
        case ArrowType::NANOARROW_TYPE_DICTIONARY: {
          struct ArrowSchemaView value_view;
          CHECK_NA(INTERNAL,
                   ArrowSchemaViewInit(&value_view, bind_schema->children[i]->dictionary,
                                       nullptr),
                   error);
          switch (value_view.type) {
            case NANOARROW_TYPE_BINARY:
            case NANOARROW_TYPE_LARGE_BINARY:
              type_id = PostgresTypeId::kBytea;
              param_lengths[i] = 0;
              break;
            case NANOARROW_TYPE_STRING:
            case NANOARROW_TYPE_LARGE_STRING:
              type_id = PostgresTypeId::kText;
              param_lengths[i] = 0;
              break;
            default:
              SetError(error, "%s%" PRIu64 "%s%s%s%s", "[libpq] Field #",
                       static_cast<uint64_t>(i + 1), " ('",
                       bind_schema->children[i]->name,
                       "') has unsupported dictionary value parameter type ",
                       ArrowTypeString(value_view.type));
              return ADBC_STATUS_NOT_IMPLEMENTED;
          }
          break;
        }
        default:
          SetError(error, "%s%" PRIu64 "%s%s%s%s", "[libpq] Field #",
                   static_cast<uint64_t>(i + 1), " ('", bind_schema->children[i]->name,
                   "') has unsupported parameter type ",
                   ArrowTypeString(bind_schema_fields[i].type));
          return ADBC_STATUS_NOT_IMPLEMENTED;
      }

      param_types[i] = type_resolver.GetOID(type_id);
      if (param_types[i] == 0) {
        SetError(error, "%s%" PRIu64 "%s%s%s%s", "[libpq] Field #",
                 static_cast<uint64_t>(i + 1), " ('", bind_schema->children[i]->name,
                 "') has type with no corresponding PostgreSQL type ",
                 ArrowTypeString(bind_schema_fields[i].type));
        return ADBC_STATUS_NOT_IMPLEMENTED;
      }
    }

    size_t param_values_length = 0;
    for (int length : param_lengths) {
      param_values_offsets.push_back(param_values_length);
      param_values_length += length;
    }
    param_values_buffer.resize(param_values_length);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Prepare(PGconn* pg_conn, const std::string& query,
                         struct AdbcError* error, const bool autocommit) {
    // tz-aware timestamps require special handling to set the timezone to UTC
    // prior to sending over the binary protocol; must be reset after execute
    for (int64_t col = 0; col < bind_schema->n_children; col++) {
      if ((bind_schema_fields[col].type == ArrowType::NANOARROW_TYPE_TIMESTAMP) &&
          (strcmp("", bind_schema_fields[col].timezone))) {
        has_tz_field = true;

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
          AdbcStatusCode code = SetError(error, get_tz_result,
                                         "[libpq] Could not query current timezone: %s",
                                         PQerrorMessage(pg_conn));
          PQclear(get_tz_result);
          return code;
        }

        tz_setting = std::string(PQgetvalue(get_tz_result, 0, 0));
        PQclear(get_tz_result);

        PGresult* set_utc_result = PQexec(pg_conn, "SET TIME ZONE 'UTC'");
        if (PQresultStatus(set_utc_result) != PGRES_COMMAND_OK) {
          AdbcStatusCode code = SetError(error, set_utc_result,
                                         "[libpq] Failed to set time zone to UTC: %s",
                                         PQerrorMessage(pg_conn));
          PQclear(set_utc_result);
          return code;
        }
        PQclear(set_utc_result);
        break;
      }
    }

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

  AdbcStatusCode Execute(PGconn* pg_conn, int64_t* rows_affected,
                         struct AdbcError* error) {
    if (rows_affected) *rows_affected = 0;
    PGresult* result = nullptr;

    int64_t row = -1;
    while (true) {
      RAISE_ADBC(EnsureNextRow(error));
      if (!current->release) break;
      row = current_row;

      for (int64_t col = 0; col < array_view->n_children; col++) {
        if (ArrowArrayViewIsNull(array_view->children[col], row)) {
          param_values[col] = nullptr;
          continue;
        } else {
          param_values[col] = param_values_buffer.data() + param_values_offsets[col];
        }
        switch (bind_schema_fields[col].type) {
          case ArrowType::NANOARROW_TYPE_BOOL: {
            const int8_t val = ArrowBitGet(
                array_view->children[col]->buffer_views[1].data.as_uint8, row);
            std::memcpy(param_values[col], &val, sizeof(int8_t));
            break;
          }

          case ArrowType::NANOARROW_TYPE_INT8: {
            const int16_t val =
                array_view->children[col]->buffer_views[1].data.as_int8[row];
            const uint16_t value = ToNetworkInt16(val);
            std::memcpy(param_values[col], &value, sizeof(int16_t));
            break;
          }
          case ArrowType::NANOARROW_TYPE_INT16: {
            const uint16_t value = ToNetworkInt16(
                array_view->children[col]->buffer_views[1].data.as_int16[row]);
            std::memcpy(param_values[col], &value, sizeof(int16_t));
            break;
          }
          case ArrowType::NANOARROW_TYPE_INT32: {
            const uint32_t value = ToNetworkInt32(
                array_view->children[col]->buffer_views[1].data.as_int32[row]);
            std::memcpy(param_values[col], &value, sizeof(int32_t));
            break;
          }
          case ArrowType::NANOARROW_TYPE_INT64: {
            const int64_t value = ToNetworkInt64(
                array_view->children[col]->buffer_views[1].data.as_int64[row]);
            std::memcpy(param_values[col], &value, sizeof(int64_t));
            break;
          }
          case ArrowType::NANOARROW_TYPE_FLOAT: {
            const uint32_t value = ToNetworkFloat4(
                array_view->children[col]->buffer_views[1].data.as_float[row]);
            std::memcpy(param_values[col], &value, sizeof(uint32_t));
            break;
          }
          case ArrowType::NANOARROW_TYPE_DOUBLE: {
            const uint64_t value = ToNetworkFloat8(
                array_view->children[col]->buffer_views[1].data.as_double[row]);
            std::memcpy(param_values[col], &value, sizeof(uint64_t));
            break;
          }
          case ArrowType::NANOARROW_TYPE_STRING:
          case ArrowType::NANOARROW_TYPE_LARGE_STRING:
          case ArrowType::NANOARROW_TYPE_BINARY: {
            const ArrowBufferView view =
                ArrowArrayViewGetBytesUnsafe(array_view->children[col], row);
            // TODO: overflow check?
            param_lengths[col] = static_cast<int>(view.size_bytes);
            param_values[col] = const_cast<char*>(view.data.as_char);
            break;
          }
          case ArrowType::NANOARROW_TYPE_DATE32: {
            // 2000-01-01
            constexpr int32_t kPostgresDateEpoch = 10957;
            const int32_t raw_value =
                array_view->children[col]->buffer_views[1].data.as_int32[row];
            if (raw_value < INT32_MIN + kPostgresDateEpoch) {
              SetError(error, "[libpq] Field #%" PRId64 "%s%s%s%" PRId64 "%s", col + 1,
                       "('", bind_schema->children[col]->name, "') Row #", row + 1,
                       "has value which exceeds postgres date limits");
              return ADBC_STATUS_INVALID_ARGUMENT;
            }

            const uint32_t value = ToNetworkInt32(raw_value - kPostgresDateEpoch);
            std::memcpy(param_values[col], &value, sizeof(int32_t));
            break;
          }
          case ArrowType::NANOARROW_TYPE_DURATION:
          case ArrowType::NANOARROW_TYPE_TIMESTAMP: {
            int64_t val = array_view->children[col]->buffer_views[1].data.as_int64[row];

            bool overflow_safe = true;

            auto unit = bind_schema_fields[col].time_unit;

            switch (unit) {
              case NANOARROW_TIME_UNIT_SECOND:
                overflow_safe =
                    val <= kMaxSafeSecondsToMicros && val >= kMinSafeSecondsToMicros;
                if (overflow_safe) {
                  val *= 1000000;
                }

                break;
              case NANOARROW_TIME_UNIT_MILLI:
                overflow_safe =
                    val <= kMaxSafeMillisToMicros && val >= kMinSafeMillisToMicros;
                if (overflow_safe) {
                  val *= 1000;
                }
                break;
              case NANOARROW_TIME_UNIT_MICRO:
                break;
              case NANOARROW_TIME_UNIT_NANO:
                val /= 1000;
                break;
            }

            if (!overflow_safe) {
              SetError(error,
                       "[libpq] Field #%" PRId64 " ('%s') Row #%" PRId64
                       " has value '%" PRIi64
                       "' which exceeds PostgreSQL timestamp limits",
                       col + 1, bind_schema->children[col]->name, row + 1,
                       array_view->children[col]->buffer_views[1].data.as_int64[row]);
              return ADBC_STATUS_INVALID_ARGUMENT;
            }

            if (val < (std::numeric_limits<int64_t>::min)() + kPostgresTimestampEpoch) {
              SetError(error,
                       "[libpq] Field #%" PRId64 " ('%s') Row #%" PRId64
                       " has value '%" PRIi64 "' which would underflow",
                       col + 1, bind_schema->children[col]->name, row + 1,
                       array_view->children[col]->buffer_views[1].data.as_int64[row]);
              return ADBC_STATUS_INVALID_ARGUMENT;
            }

            if (bind_schema_fields[col].type == ArrowType::NANOARROW_TYPE_TIMESTAMP) {
              const uint64_t value = ToNetworkInt64(val - kPostgresTimestampEpoch);
              std::memcpy(param_values[col], &value, sizeof(int64_t));
            } else if (bind_schema_fields[col].type ==
                       ArrowType::NANOARROW_TYPE_DURATION) {
              // postgres stores an interval as a 64 bit offset in microsecond
              // resolution alongside a 32 bit day and 32 bit month
              // for now we just send 0 for the day / month values
              const uint64_t value = ToNetworkInt64(val);
              std::memcpy(param_values[col], &value, sizeof(int64_t));
              std::memset(param_values[col] + sizeof(int64_t), 0, sizeof(int64_t));
            }
            break;
          }
          case ArrowType::NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO: {
            struct ArrowInterval interval;
            ArrowIntervalInit(&interval, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
            ArrowArrayViewGetIntervalUnsafe(array_view->children[col], row, &interval);

            const uint32_t months = ToNetworkInt32(interval.months);
            const uint32_t days = ToNetworkInt32(interval.days);
            const uint64_t ms = ToNetworkInt64(interval.ns / 1000);

            std::memcpy(param_values[col], &ms, sizeof(uint64_t));
            std::memcpy(param_values[col] + sizeof(uint64_t), &days, sizeof(uint32_t));
            std::memcpy(param_values[col] + sizeof(uint64_t) + sizeof(uint32_t), &months,
                        sizeof(uint32_t));
            break;
          }
          default:
            SetError(error, "%s%" PRId64 "%s%s%s%s", "[libpq] Field #", col + 1, " ('",
                     bind_schema->children[col]->name,
                     "') has unsupported type for ingestion ",
                     ArrowTypeString(bind_schema_fields[col].type));
            return ADBC_STATUS_NOT_IMPLEMENTED;
        }
      }

      result = PQexecPrepared(pg_conn, /*stmtName=*/"",
                              /*nParams=*/bind_schema->n_children, param_values.data(),
                              param_lengths.data(), param_formats.data(),
                              /*resultFormat=*/0 /*text*/);

      ExecStatusType pg_status = PQresultStatus(result);
      if (pg_status != PGRES_COMMAND_OK) {
        AdbcStatusCode code =
            SetError(error, result, "[libpq] Failed to execute prepared statement: %s %s",
                     PQresStatus(pg_status), PQerrorMessage(pg_conn));
        PQclear(result);
        return code;
      }

      PQclear(result);
    }
    if (rows_affected) *rows_affected += current->length;

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
    // https://github.com/apache/arrow-adbc/issues/1921: PostgreSQL has a max
    // size for a single message that we need to respect (1 GiB - 1).  Since
    // the buffer can be chunked up as much as we want, go for 16 MiB as our
    // limit.
    // https://github.com/postgres/postgres/blob/23c5a0e7d43bc925c6001538f04a458933a11fc1/src/common/stringinfo.c#L28
    constexpr int64_t kMaxCopyBufferSize = 0x1000000;
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

      ArrowBuffer buffer = writer.WriteBuffer();
      {
        auto* data = reinterpret_cast<char*>(buffer.data);
        int64_t remaining = buffer.size_bytes;
        while (remaining > 0) {
          int64_t to_write = std::min<int64_t>(remaining, kMaxCopyBufferSize);
          if (PQputCopyData(pg_conn, data, to_write) <= 0) {
            SetError(error, "Error writing tuple field data: %s",
                     PQerrorMessage(pg_conn));
            return ADBC_STATUS_IO;
          }
          remaining -= to_write;
          data += to_write;
        }
      }

      if (rows_affected) *rows_affected += current->length;
      writer.Rewind();
    }

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
};
}  // namespace adbcpq
