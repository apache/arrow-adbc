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

// Windows
#define NOMINMAX

#include "statement.h"

#include <array>
#include <cassert>
#include <cerrno>
#include <cinttypes>
#include <cstring>
#include <iostream>
#include <limits>
#include <memory>
#include <utility>
#include <vector>

#include <adbc.h>
#include <libpq-fe.h>
#include <nanoarrow/nanoarrow.hpp>

#include "connection.h"
#include "copy/writer.h"
#include "driver/common/options.h"
#include "driver/common/utils.h"
#include "error.h"
#include "postgres_type.h"
#include "postgres_util.h"
#include "result_helper.h"

namespace adbcpq {

namespace {
/// The flag indicating to PostgreSQL that we want binary-format values.
constexpr int kPgBinaryFormat = 1;

/// One-value ArrowArrayStream used to unify the implementations of Bind
struct OneValueStream {
  struct ArrowSchema schema;
  struct ArrowArray array;

  static int GetSchema(struct ArrowArrayStream* self, struct ArrowSchema* out) {
    OneValueStream* stream = static_cast<OneValueStream*>(self->private_data);
    return ArrowSchemaDeepCopy(&stream->schema, out);
  }
  static int GetNext(struct ArrowArrayStream* self, struct ArrowArray* out) {
    OneValueStream* stream = static_cast<OneValueStream*>(self->private_data);
    *out = stream->array;
    stream->array.release = nullptr;
    return 0;
  }
  static const char* GetLastError(struct ArrowArrayStream* self) { return NULL; }
  static void Release(struct ArrowArrayStream* self) {
    OneValueStream* stream = static_cast<OneValueStream*>(self->private_data);
    if (stream->schema.release) {
      stream->schema.release(&stream->schema);
      stream->schema.release = nullptr;
    }
    if (stream->array.release) {
      stream->array.release(&stream->array);
      stream->array.release = nullptr;
    }
    delete stream;
    self->release = nullptr;
  }
};

/// Build an PostgresType object from a PGresult*
AdbcStatusCode ResolvePostgresType(const PostgresTypeResolver& type_resolver,
                                   PGresult* result, PostgresType* out,
                                   struct AdbcError* error) {
  ArrowError na_error;
  const int num_fields = PQnfields(result);
  PostgresType root_type(PostgresTypeId::kRecord);

  for (int i = 0; i < num_fields; i++) {
    const Oid pg_oid = PQftype(result, i);
    PostgresType pg_type;
    if (type_resolver.Find(pg_oid, &pg_type, &na_error) != NANOARROW_OK) {
      SetError(error, "%s%d%s%s%s%d", "[libpq] Column #", i + 1, " (\"",
               PQfname(result, i), "\") has unknown type code ", pg_oid);
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    root_type.AppendChild(PQfname(result, i), pg_type);
  }

  *out = root_type;
  return ADBC_STATUS_OK;
}

/// Helper to manage bind parameters with a prepared statement
struct BindStream {
  Handle<struct ArrowArrayStream> bind;
  Handle<struct ArrowSchema> bind_schema;
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
    CHECK_NA(INTERNAL, bind->get_schema(&bind.value, &bind_schema.value), error);
    CHECK_NA(
        INTERNAL,
        ArrowSchemaViewInit(&bind_schema_view, &bind_schema.value, /*error*/ nullptr),
        error);

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

  AdbcStatusCode Prepare(PGconn* conn, const std::string& query, struct AdbcError* error,
                         const bool autocommit) {
    // tz-aware timestamps require special handling to set the timezone to UTC
    // prior to sending over the binary protocol; must be reset after execute
    for (int64_t col = 0; col < bind_schema->n_children; col++) {
      if ((bind_schema_fields[col].type == ArrowType::NANOARROW_TYPE_TIMESTAMP) &&
          (strcmp("", bind_schema_fields[col].timezone))) {
        has_tz_field = true;

        if (autocommit) {
          PGresult* begin_result = PQexec(conn, "BEGIN");
          if (PQresultStatus(begin_result) != PGRES_COMMAND_OK) {
            AdbcStatusCode code =
                SetError(error, begin_result,
                         "[libpq] Failed to begin transaction for timezone data: %s",
                         PQerrorMessage(conn));
            PQclear(begin_result);
            return code;
          }
          PQclear(begin_result);
        }

        PGresult* get_tz_result = PQexec(conn, "SELECT current_setting('TIMEZONE')");
        if (PQresultStatus(get_tz_result) != PGRES_TUPLES_OK) {
          AdbcStatusCode code = SetError(error, get_tz_result,
                                         "[libpq] Could not query current timezone: %s",
                                         PQerrorMessage(conn));
          PQclear(get_tz_result);
          return code;
        }

        tz_setting = std::string(PQgetvalue(get_tz_result, 0, 0));
        PQclear(get_tz_result);

        PGresult* set_utc_result = PQexec(conn, "SET TIME ZONE 'UTC'");
        if (PQresultStatus(set_utc_result) != PGRES_COMMAND_OK) {
          AdbcStatusCode code = SetError(error, set_utc_result,
                                         "[libpq] Failed to set time zone to UTC: %s",
                                         PQerrorMessage(conn));
          PQclear(set_utc_result);
          return code;
        }
        PQclear(set_utc_result);
        break;
      }
    }

    PGresult* result = PQprepare(conn, /*stmtName=*/"", query.c_str(),
                                 /*nParams=*/bind_schema->n_children, param_types.data());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      AdbcStatusCode code =
          SetError(error, result, "[libpq] Failed to prepare query: %s\nQuery was:%s",
                   PQerrorMessage(conn), query.c_str());
      PQclear(result);
      return code;
    }
    PQclear(result);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode Execute(PGconn* conn, int64_t* rows_affected, struct AdbcError* error) {
    if (rows_affected) *rows_affected = 0;
    PGresult* result = nullptr;

    while (true) {
      Handle<struct ArrowArray> array;
      int res = bind->get_next(&bind.value, &array.value);
      if (res != 0) {
        SetError(error,
                 "[libpq] Failed to read next batch from stream of bind parameters: "
                 "(%d) %s %s",
                 res, std::strerror(res), bind->get_last_error(&bind.value));
        return ADBC_STATUS_IO;
      }
      if (!array->release) break;

      Handle<struct ArrowArrayView> array_view;
      // TODO: include error messages
      CHECK_NA(
          INTERNAL,
          ArrowArrayViewInitFromSchema(&array_view.value, &bind_schema.value, nullptr),
          error);
      CHECK_NA(INTERNAL, ArrowArrayViewSetArray(&array_view.value, &array.value, nullptr),
               error);

      for (int64_t row = 0; row < array->length; row++) {
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
              std::memcpy(param_values[col] + sizeof(uint64_t) + sizeof(uint32_t),
                          &months, sizeof(uint32_t));
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

        result = PQexecPrepared(conn, /*stmtName=*/"",
                                /*nParams=*/bind_schema->n_children, param_values.data(),
                                param_lengths.data(), param_formats.data(),
                                /*resultFormat=*/0 /*text*/);

        ExecStatusType pg_status = PQresultStatus(result);
        if (pg_status != PGRES_COMMAND_OK) {
          AdbcStatusCode code = SetError(
              error, result, "[libpq] Failed to execute prepared statement: %s %s",
              PQresStatus(pg_status), PQerrorMessage(conn));
          PQclear(result);
          return code;
        }

        PQclear(result);
      }
      if (rows_affected) *rows_affected += array->length;

      if (has_tz_field) {
        std::string reset_query = "SET TIME ZONE '" + tz_setting + "'";
        PGresult* reset_tz_result = PQexec(conn, reset_query.c_str());
        if (PQresultStatus(reset_tz_result) != PGRES_COMMAND_OK) {
          AdbcStatusCode code =
              SetError(error, reset_tz_result, "[libpq] Failed to reset time zone: %s",
                       PQerrorMessage(conn));
          PQclear(reset_tz_result);
          return code;
        }
        PQclear(reset_tz_result);

        PGresult* commit_result = PQexec(conn, "COMMIT");
        if (PQresultStatus(commit_result) != PGRES_COMMAND_OK) {
          AdbcStatusCode code =
              SetError(error, commit_result, "[libpq] Failed to commit transaction: %s",
                       PQerrorMessage(conn));
          PQclear(commit_result);
          return code;
        }
        PQclear(commit_result);
      }
    }
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode ExecuteCopy(PGconn* conn, int64_t* rows_affected,
                             struct AdbcError* error) {
    if (rows_affected) *rows_affected = 0;

    PostgresCopyStreamWriter writer;
    CHECK_NA(INTERNAL, writer.Init(&bind_schema.value), error);
    CHECK_NA(INTERNAL, writer.InitFieldWriters(nullptr), error);

    CHECK_NA(INTERNAL, writer.WriteHeader(nullptr), error);

    while (true) {
      Handle<struct ArrowArray> array;
      int res = bind->get_next(&bind.value, &array.value);
      if (res != 0) {
        SetError(error,
                 "[libpq] Failed to read next batch from stream of bind parameters: "
                 "(%d) %s %s",
                 res, std::strerror(res), bind->get_last_error(&bind.value));
        return ADBC_STATUS_IO;
      }
      if (!array->release) break;

      CHECK_NA(INTERNAL, writer.SetArray(&array.value), error);

      // build writer buffer
      int write_result;
      do {
        write_result = writer.WriteRecord(nullptr);
      } while (write_result == NANOARROW_OK);

      // check if not ENODATA at exit
      if (write_result != ENODATA) {
        SetError(error, "Error occurred writing COPY data: %s", PQerrorMessage(conn));
        return ADBC_STATUS_IO;
      }

      ArrowBuffer buffer = writer.WriteBuffer();
      if (PQputCopyData(conn, reinterpret_cast<char*>(buffer.data), buffer.size_bytes) <=
          0) {
        SetError(error, "Error writing tuple field data: %s", PQerrorMessage(conn));
        return ADBC_STATUS_IO;
      }

      if (rows_affected) *rows_affected += array->length;
      writer.Rewind();
    }

    if (PQputCopyEnd(conn, NULL) <= 0) {
      SetError(error, "Error message returned by PQputCopyEnd: %s", PQerrorMessage(conn));
      return ADBC_STATUS_IO;
    }

    PGresult* result = PQgetResult(conn);
    ExecStatusType pg_status = PQresultStatus(result);
    if (pg_status != PGRES_COMMAND_OK) {
      AdbcStatusCode code =
          SetError(error, result, "[libpq] Failed to execute COPY statement: %s %s",
                   PQresStatus(pg_status), PQerrorMessage(conn));
      PQclear(result);
      return code;
    }

    PQclear(result);
    return ADBC_STATUS_OK;
  }
};
}  // namespace

int TupleReader::GetSchema(struct ArrowSchema* out) {
  assert(copy_reader_ != nullptr);

  int na_res = copy_reader_->GetSchema(out);
  if (out->release == nullptr) {
    SetError(&error_, "[libpq] Result set was already consumed or freed");
    status_ = ADBC_STATUS_INVALID_STATE;
    return AdbcStatusCodeToErrno(status_);
  } else if (na_res != NANOARROW_OK) {
    // e.g., Can't allocate memory
    SetError(&error_, "[libpq] Error copying schema");
    status_ = ADBC_STATUS_INTERNAL;
  }

  return na_res;
}

int TupleReader::InitQueryAndFetchFirst(struct ArrowError* error) {
  // Fetch + parse the header
  int get_copy_res = PQgetCopyData(conn_, &pgbuf_, /*async=*/0);
  data_.size_bytes = get_copy_res;
  data_.data.as_char = pgbuf_;

  if (get_copy_res == -2) {
    SetError(&error_, "[libpq] Fetch header failed: %s", PQerrorMessage(conn_));
    status_ = ADBC_STATUS_IO;
    return AdbcStatusCodeToErrno(status_);
  }

  int na_res = copy_reader_->ReadHeader(&data_, error);
  if (na_res != NANOARROW_OK) {
    SetError(&error_, "[libpq] ReadHeader failed: %s", error->message);
    status_ = ADBC_STATUS_IO;
    return AdbcStatusCodeToErrno(status_);
  }

  return NANOARROW_OK;
}

int TupleReader::AppendRowAndFetchNext(struct ArrowError* error) {
  // Parse the result (the header AND the first row are included in the first
  // call to PQgetCopyData())
  int na_res = copy_reader_->ReadRecord(&data_, error);
  if (na_res != NANOARROW_OK && na_res != ENODATA) {
    SetError(&error_, "[libpq] ReadRecord failed at row %" PRId64 ": %s", row_id_,
             error->message);
    status_ = ADBC_STATUS_IO;
    return na_res;
  }

  row_id_++;

  // Fetch + check
  PQfreemem(pgbuf_);
  pgbuf_ = nullptr;
  int get_copy_res = PQgetCopyData(conn_, &pgbuf_, /*async=*/0);
  data_.size_bytes = get_copy_res;
  data_.data.as_char = pgbuf_;

  if (get_copy_res == -2) {
    SetError(&error_, "[libpq] PQgetCopyData failed at row %" PRId64 ": %s", row_id_,
             PQerrorMessage(conn_));
    status_ = ADBC_STATUS_IO;
    return AdbcStatusCodeToErrno(status_);
  } else if (get_copy_res == -1) {
    // Returned when COPY has finished successfully
    return ENODATA;
  } else if ((copy_reader_->array_size_approx_bytes() + get_copy_res) >=
             batch_size_hint_bytes_) {
    // Appending the next row will result in an array larger than requested.
    // Return EOVERFLOW to force GetNext() to build the current result and return.
    return EOVERFLOW;
  } else {
    return NANOARROW_OK;
  }
}

int TupleReader::BuildOutput(struct ArrowArray* out, struct ArrowError* error) {
  if (copy_reader_->array_size_approx_bytes() == 0) {
    out->release = nullptr;
    return NANOARROW_OK;
  }

  int na_res = copy_reader_->GetArray(out, error);
  if (na_res != NANOARROW_OK) {
    SetError(&error_, "[libpq] Failed to build result array: %s", error->message);
    status_ = ADBC_STATUS_INTERNAL;
    return na_res;
  }

  return NANOARROW_OK;
}

int TupleReader::GetNext(struct ArrowArray* out) {
  if (is_finished_) {
    out->release = nullptr;
    return 0;
  }

  struct ArrowError error;
  error.message[0] = '\0';

  if (row_id_ == -1) {
    NANOARROW_RETURN_NOT_OK(InitQueryAndFetchFirst(&error));
    row_id_++;
  }

  int na_res;
  do {
    na_res = AppendRowAndFetchNext(&error);
    if (na_res == EOVERFLOW) {
      // The result would be too big to return if we appended the row. When EOVERFLOW is
      // returned, the copy reader leaves the output in a valid state. The data is left in
      // pg_buf_/data_ and will attempt to be appended on the next call to GetNext()
      return BuildOutput(out, &error);
    }
  } while (na_res == NANOARROW_OK);

  if (na_res != ENODATA) {
    return na_res;
  }

  is_finished_ = true;

  // Finish the result properly and return the last result. Note that BuildOutput() may
  // set tmp.release = nullptr if there were zero rows in the copy reader (can
  // occur in an overflow scenario).
  struct ArrowArray tmp;
  NANOARROW_RETURN_NOT_OK(BuildOutput(&tmp, &error));

  PQclear(result_);
  // Check the server-side response
  result_ = PQgetResult(conn_);
  const ExecStatusType pq_status = PQresultStatus(result_);
  if (pq_status != PGRES_COMMAND_OK) {
    const char* sqlstate = PQresultErrorField(result_, PG_DIAG_SQLSTATE);
    SetError(&error_, result_, "[libpq] Query failed [%s]: %s", PQresStatus(pq_status),
             PQresultErrorMessage(result_));

    if (tmp.release != nullptr) {
      tmp.release(&tmp);
    }

    if (sqlstate != nullptr && std::strcmp(sqlstate, "57014") == 0) {
      status_ = ADBC_STATUS_CANCELLED;
    } else {
      status_ = ADBC_STATUS_IO;
    }
    return AdbcStatusCodeToErrno(status_);
  }

  ArrowArrayMove(&tmp, out);
  return NANOARROW_OK;
}

void TupleReader::Release() {
  if (error_.release) {
    error_.release(&error_);
  }
  error_ = ADBC_ERROR_INIT;
  status_ = ADBC_STATUS_OK;

  if (result_) {
    PQclear(result_);
    result_ = nullptr;
  }

  if (pgbuf_) {
    PQfreemem(pgbuf_);
    pgbuf_ = nullptr;
  }

  if (copy_reader_) {
    copy_reader_.reset();
  }

  is_finished_ = false;
  row_id_ = -1;
}

void TupleReader::ExportTo(struct ArrowArrayStream* stream) {
  stream->get_schema = &GetSchemaTrampoline;
  stream->get_next = &GetNextTrampoline;
  stream->get_last_error = &GetLastErrorTrampoline;
  stream->release = &ReleaseTrampoline;
  stream->private_data = this;
}

const struct AdbcError* TupleReader::ErrorFromArrayStream(struct ArrowArrayStream* stream,
                                                          AdbcStatusCode* status) {
  if (!stream->private_data || stream->release != &ReleaseTrampoline) {
    return nullptr;
  }

  TupleReader* reader = static_cast<TupleReader*>(stream->private_data);
  if (status) {
    *status = reader->status_;
  }
  return &reader->error_;
}

int TupleReader::GetSchemaTrampoline(struct ArrowArrayStream* self,
                                     struct ArrowSchema* out) {
  if (!self || !self->private_data) return EINVAL;

  TupleReader* reader = static_cast<TupleReader*>(self->private_data);
  return reader->GetSchema(out);
}

int TupleReader::GetNextTrampoline(struct ArrowArrayStream* self,
                                   struct ArrowArray* out) {
  if (!self || !self->private_data) return EINVAL;

  TupleReader* reader = static_cast<TupleReader*>(self->private_data);
  return reader->GetNext(out);
}

const char* TupleReader::GetLastErrorTrampoline(struct ArrowArrayStream* self) {
  if (!self || !self->private_data) return nullptr;

  TupleReader* reader = static_cast<TupleReader*>(self->private_data);
  return reader->last_error();
}

void TupleReader::ReleaseTrampoline(struct ArrowArrayStream* self) {
  if (!self || !self->private_data) return;

  TupleReader* reader = static_cast<TupleReader*>(self->private_data);
  reader->Release();
  self->private_data = nullptr;
  self->release = nullptr;
}

AdbcStatusCode PostgresStatement::New(struct AdbcConnection* connection,
                                      struct AdbcError* error) {
  if (!connection || !connection->private_data) {
    SetError(error, "%s", "[libpq] Must provide an initialized AdbcConnection");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  connection_ =
      *reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  type_resolver_ = connection_->type_resolver();
  reader_.conn_ = connection_->conn();
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::Bind(struct ArrowArray* values,
                                       struct ArrowSchema* schema,
                                       struct AdbcError* error) {
  if (!values || !values->release) {
    SetError(error, "%s", "[libpq] Must provide non-NULL array");
    return ADBC_STATUS_INVALID_ARGUMENT;
  } else if (!schema || !schema->release) {
    SetError(error, "%s", "[libpq] Must provide non-NULL schema");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  if (bind_.release) bind_.release(&bind_);
  // Make a one-value stream
  bind_.private_data = new OneValueStream{*schema, *values};
  bind_.get_schema = &OneValueStream::GetSchema;
  bind_.get_next = &OneValueStream::GetNext;
  bind_.get_last_error = &OneValueStream::GetLastError;
  bind_.release = &OneValueStream::Release;
  std::memset(values, 0, sizeof(*values));
  std::memset(schema, 0, sizeof(*schema));
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::Bind(struct ArrowArrayStream* stream,
                                       struct AdbcError* error) {
  if (!stream || !stream->release) {
    SetError(error, "%s", "[libpq] Must provide non-NULL stream");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  // Move stream
  if (bind_.release) bind_.release(&bind_);
  bind_ = *stream;
  std::memset(stream, 0, sizeof(*stream));
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::Cancel(struct AdbcError* error) {
  // Ultimately the same underlying PGconn
  return connection_->Cancel(error);
}

AdbcStatusCode PostgresStatement::CreateBulkTable(
    const std::string& current_schema, const struct ArrowSchema& source_schema,
    const std::vector<struct ArrowSchemaView>& source_schema_fields,
    std::string* escaped_table, std::string* escaped_field_list,
    struct AdbcError* error) {
  PGconn* conn = connection_->conn();

  if (!ingest_.db_schema.empty() && ingest_.temporary) {
    SetError(error, "[libpq] Cannot set both %s and %s",
             ADBC_INGEST_OPTION_TARGET_DB_SCHEMA, ADBC_INGEST_OPTION_TEMPORARY);
    return ADBC_STATUS_INVALID_STATE;
  }

  {
    if (!ingest_.db_schema.empty()) {
      char* escaped =
          PQescapeIdentifier(conn, ingest_.db_schema.c_str(), ingest_.db_schema.size());
      if (escaped == nullptr) {
        SetError(error, "[libpq] Failed to escape target schema %s for ingestion: %s",
                 ingest_.db_schema.c_str(), PQerrorMessage(conn));
        return ADBC_STATUS_INTERNAL;
      }
      *escaped_table += escaped;
      *escaped_table += " . ";
      PQfreemem(escaped);
    } else if (ingest_.temporary) {
      // OK to be redundant (CREATE TEMPORARY TABLE pg_temp.foo)
      *escaped_table += "pg_temp . ";
    } else {
      // Explicitly specify the current schema to avoid any temporary tables
      // shadowing this table
      char* escaped =
          PQescapeIdentifier(conn, current_schema.c_str(), current_schema.size());
      *escaped_table += escaped;
      *escaped_table += " . ";
      PQfreemem(escaped);
    }

    if (!ingest_.target.empty()) {
      char* escaped =
          PQescapeIdentifier(conn, ingest_.target.c_str(), ingest_.target.size());
      if (escaped == nullptr) {
        SetError(error, "[libpq] Failed to escape target table %s for ingestion: %s",
                 ingest_.target.c_str(), PQerrorMessage(conn));
        return ADBC_STATUS_INTERNAL;
      }
      *escaped_table += escaped;
      PQfreemem(escaped);
    }
  }

  std::string create;

  if (ingest_.temporary) {
    create = "CREATE TEMPORARY TABLE ";
  } else {
    create = "CREATE TABLE ";
  }

  switch (ingest_.mode) {
    case IngestMode::kCreate:
    case IngestMode::kAppend:
      // Nothing to do
      break;
    case IngestMode::kReplace: {
      std::string drop = "DROP TABLE IF EXISTS " + *escaped_table;
      PGresult* result = PQexecParams(conn, drop.c_str(), /*nParams=*/0,
                                      /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                                      /*paramLengths=*/nullptr, /*paramFormats=*/nullptr,
                                      /*resultFormat=*/1 /*(binary)*/);
      if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        AdbcStatusCode code =
            SetError(error, result, "[libpq] Failed to drop table: %s\nQuery was: %s",
                     PQerrorMessage(conn), drop.c_str());
        PQclear(result);
        return code;
      }
      PQclear(result);
      break;
    }
    case IngestMode::kCreateAppend:
      create += "IF NOT EXISTS ";
      break;
  }
  create += *escaped_table;
  create += " (";

  for (size_t i = 0; i < source_schema_fields.size(); i++) {
    if (i > 0) {
      create += ", ";
      *escaped_field_list += ", ";
    }

    const char* unescaped = source_schema.children[i]->name;
    char* escaped = PQescapeIdentifier(conn, unescaped, std::strlen(unescaped));
    if (escaped == nullptr) {
      SetError(error, "[libpq] Failed to escape column %s for ingestion: %s", unescaped,
               PQerrorMessage(conn));
      return ADBC_STATUS_INTERNAL;
    }
    create += escaped;
    *escaped_field_list += escaped;
    PQfreemem(escaped);

    switch (source_schema_fields[i].type) {
      case ArrowType::NANOARROW_TYPE_BOOL:
        create += " BOOLEAN";
        break;
      case ArrowType::NANOARROW_TYPE_INT8:
      case ArrowType::NANOARROW_TYPE_INT16:
        create += " SMALLINT";
        break;
      case ArrowType::NANOARROW_TYPE_INT32:
        create += " INTEGER";
        break;
      case ArrowType::NANOARROW_TYPE_INT64:
        create += " BIGINT";
        break;
      case ArrowType::NANOARROW_TYPE_FLOAT:
        create += " REAL";
        break;
      case ArrowType::NANOARROW_TYPE_DOUBLE:
        create += " DOUBLE PRECISION";
        break;
      case ArrowType::NANOARROW_TYPE_STRING:
      case ArrowType::NANOARROW_TYPE_LARGE_STRING:
        create += " TEXT";
        break;
      case ArrowType::NANOARROW_TYPE_BINARY:
        create += " BYTEA";
        break;
      case ArrowType::NANOARROW_TYPE_DATE32:
        create += " DATE";
        break;
      case ArrowType::NANOARROW_TYPE_TIMESTAMP:
        if (strcmp("", source_schema_fields[i].timezone)) {
          create += " TIMESTAMPTZ";
        } else {
          create += " TIMESTAMP";
        }
        break;
      case ArrowType::NANOARROW_TYPE_DURATION:
      case ArrowType::NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
        create += " INTERVAL";
        break;
      case ArrowType::NANOARROW_TYPE_DECIMAL128:
      case ArrowType::NANOARROW_TYPE_DECIMAL256:
        create += " DECIMAL";
        break;
      case ArrowType::NANOARROW_TYPE_DICTIONARY: {
        struct ArrowSchemaView value_view;
        CHECK_NA(INTERNAL,
                 ArrowSchemaViewInit(&value_view, source_schema.children[i]->dictionary,
                                     nullptr),
                 error);
        switch (value_view.type) {
          case NANOARROW_TYPE_BINARY:
          case NANOARROW_TYPE_LARGE_BINARY:
            create += " BYTEA";
            break;
          case NANOARROW_TYPE_STRING:
          case NANOARROW_TYPE_LARGE_STRING:
            create += " TEXT";
            break;
          default:
            SetError(error, "%s%" PRIu64 "%s%s%s%s", "[libpq] Field #",
                     static_cast<uint64_t>(i + 1), " ('", source_schema.children[i]->name,
                     "') has unsupported dictionary value type for ingestion ",
                     ArrowTypeString(value_view.type));
            return ADBC_STATUS_NOT_IMPLEMENTED;
        }
        break;
      }
      default:
        SetError(error, "%s%" PRIu64 "%s%s%s%s", "[libpq] Field #",
                 static_cast<uint64_t>(i + 1), " ('", source_schema.children[i]->name,
                 "') has unsupported type for ingestion ",
                 ArrowTypeString(source_schema_fields[i].type));
        return ADBC_STATUS_NOT_IMPLEMENTED;
    }
  }

  if (ingest_.mode == IngestMode::kAppend) {
    return ADBC_STATUS_OK;
  }

  create += ")";
  SetError(error, "%s%s", "[libpq] ", create.c_str());
  PGresult* result = PQexecParams(conn, create.c_str(), /*nParams=*/0,
                                  /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                                  /*paramLengths=*/nullptr, /*paramFormats=*/nullptr,
                                  /*resultFormat=*/1 /*(binary)*/);
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    AdbcStatusCode code =
        SetError(error, result, "[libpq] Failed to create table: %s\nQuery was: %s",
                 PQerrorMessage(conn), create.c_str());
    PQclear(result);
    return code;
  }
  PQclear(result);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::ExecutePreparedStatement(
    struct ArrowArrayStream* stream, int64_t* rows_affected, struct AdbcError* error) {
  if (!bind_.release) {
    // TODO: set an empty stream just to unify the code paths
    SetError(error, "%s",
             "[libpq] Prepared statements without parameters are not implemented");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  if (stream) {
    // TODO:
    SetError(error, "%s",
             "[libpq] Prepared statements returning result sets are not implemented");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  BindStream bind_stream(std::move(bind_));
  std::memset(&bind_, 0, sizeof(bind_));

  RAISE_ADBC(bind_stream.Begin([&]() { return ADBC_STATUS_OK; }, error));
  RAISE_ADBC(bind_stream.SetParamTypes(*type_resolver_, error));
  RAISE_ADBC(
      bind_stream.Prepare(connection_->conn(), query_, error, connection_->autocommit()));
  RAISE_ADBC(bind_stream.Execute(connection_->conn(), rows_affected, error));
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::ExecuteQuery(struct ArrowArrayStream* stream,
                                               int64_t* rows_affected,
                                               struct AdbcError* error) {
  ClearResult();
  if (prepared_) {
    if (bind_.release || !stream) {
      return ExecutePreparedStatement(stream, rows_affected, error);
    }
    // XXX: don't use a prepared statement to execute a no-parameter
    // result-set-returning query for now, since we can't easily get
    // access to COPY there. (This might have to become sequential
    // executions of COPY (EXECUTE ($n, ...)) TO STDOUT which won't
    // get all the benefits of a prepared statement.) At preparation
    // time we don't know whether the query will be used with a result
    // set or not without analyzing the query (we could prepare both?)
    // and https://stackoverflow.com/questions/69233792 suggests that
    // you can't PREPARE a query containing COPY.
  }
  if (!stream && !ingest_.target.empty()) {
    return ExecuteUpdateBulk(rows_affected, error);
  }

  // Remove trailing semicolon(s) from the query before feeding it into COPY
  while (!query_.empty() && query_.back() == ';') {
    query_.pop_back();
  }

  if (query_.empty()) {
    SetError(error, "%s", "[libpq] Must SetSqlQuery before ExecuteQuery");
    return ADBC_STATUS_INVALID_STATE;
  }

  // 1. Prepare the query to get the schema
  {
    RAISE_ADBC(SetupReader(error));

    // If the caller did not request a result set or if there are no
    // inferred output columns (e.g. a CREATE or UPDATE), then don't
    // use COPY (which would fail anyways)
    if (!stream || reader_.copy_reader_->pg_type().n_children() == 0) {
      RAISE_ADBC(ExecuteUpdateQuery(rows_affected, error));
      if (stream) {
        struct ArrowSchema schema;
        std::memset(&schema, 0, sizeof(schema));
        RAISE_NA(reader_.copy_reader_->GetSchema(&schema));
        nanoarrow::EmptyArrayStream::MakeUnique(&schema).move(stream);
      }
      return ADBC_STATUS_OK;
    }

    // This resolves the reader specific to each PostgresType -> ArrowSchema
    // conversion. It is unlikely that this will fail given that we have just
    // inferred these conversions ourselves.
    struct ArrowError na_error;
    int na_res = reader_.copy_reader_->InitFieldReaders(&na_error);
    if (na_res != NANOARROW_OK) {
      SetError(error, "[libpq] Failed to initialize field readers: %s", na_error.message);
      return na_res;
    }
  }

  // 2. Execute the query with COPY to get binary tuples
  {
    std::string copy_query = "COPY (" + query_ + ") TO STDOUT (FORMAT binary)";
    reader_.result_ =
        PQexecParams(connection_->conn(), copy_query.c_str(), /*nParams=*/0,
                     /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                     /*paramLengths=*/nullptr, /*paramFormats=*/nullptr, kPgBinaryFormat);
    if (PQresultStatus(reader_.result_) != PGRES_COPY_OUT) {
      AdbcStatusCode code = SetError(
          error, reader_.result_,
          "[libpq] Failed to execute query: could not begin COPY: %s\nQuery was: %s",
          PQerrorMessage(connection_->conn()), copy_query.c_str());
      ClearResult();
      return code;
    }
    // Result is read from the connection, not the result, but we won't clear it here
  }

  reader_.ExportTo(stream);
  if (rows_affected) *rows_affected = -1;
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::ExecuteSchema(struct ArrowSchema* schema,
                                                struct AdbcError* error) {
  ClearResult();
  if (query_.empty()) {
    SetError(error, "%s", "[libpq] Must SetSqlQuery before ExecuteQuery");
    return ADBC_STATUS_INVALID_STATE;
  } else if (bind_.release) {
    // TODO: if we have parameters, bind them (since they can affect the output schema)
    SetError(error, "[libpq] ExecuteSchema with parameters is not implemented");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  RAISE_ADBC(SetupReader(error));
  CHECK_NA(INTERNAL, reader_.copy_reader_->GetSchema(schema), error);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::ExecuteUpdateBulk(int64_t* rows_affected,
                                                    struct AdbcError* error) {
  if (!bind_.release) {
    SetError(error, "%s", "[libpq] Must Bind() before Execute() for bulk ingestion");
    return ADBC_STATUS_INVALID_STATE;
  }

  // Need the current schema to avoid being shadowed by temp tables
  // This is a little unfortunate; we need another DB roundtrip
  std::string current_schema;
  {
    PqResultHelper result_helper{connection_->conn(), "SELECT CURRENT_SCHEMA", {}, error};
    RAISE_ADBC(result_helper.Prepare());
    RAISE_ADBC(result_helper.Execute());
    auto it = result_helper.begin();
    if (it == result_helper.end()) {
      SetError(error, "[libpq] PostgreSQL returned no rows for 'SELECT CURRENT_SCHEMA'");
      return ADBC_STATUS_INTERNAL;
    }
    current_schema = (*it)[0].data;
  }

  BindStream bind_stream(std::move(bind_));
  std::memset(&bind_, 0, sizeof(bind_));
  std::string escaped_table;
  std::string escaped_field_list;
  RAISE_ADBC(bind_stream.Begin(
      [&]() -> AdbcStatusCode {
        return CreateBulkTable(current_schema, bind_stream.bind_schema.value,
                               bind_stream.bind_schema_fields, &escaped_table,
                               &escaped_field_list, error);
      },
      error));
  RAISE_ADBC(bind_stream.SetParamTypes(*type_resolver_, error));

  std::string query = "COPY ";
  query += escaped_table;
  query += " (";
  query += escaped_field_list;
  query += ") FROM STDIN WITH (FORMAT binary)";
  PGresult* result = PQexec(connection_->conn(), query.c_str());
  if (PQresultStatus(result) != PGRES_COPY_IN) {
    AdbcStatusCode code =
        SetError(error, result, "[libpq] COPY query failed: %s\nQuery was:%s",
                 PQerrorMessage(connection_->conn()), query.c_str());
    PQclear(result);
    return code;
  }

  PQclear(result);
  RAISE_ADBC(bind_stream.ExecuteCopy(connection_->conn(), rows_affected, error));
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::ExecuteUpdateQuery(int64_t* rows_affected,
                                                     struct AdbcError* error) {
  // NOTE: must prepare first (used in ExecuteQuery)
  PGresult* result =
      PQexecPrepared(connection_->conn(), /*stmtName=*/"", /*nParams=*/0,
                     /*paramValues=*/nullptr, /*paramLengths=*/nullptr,
                     /*paramFormats=*/nullptr, /*resultFormat=*/kPgBinaryFormat);
  ExecStatusType status = PQresultStatus(result);
  if (status != PGRES_COMMAND_OK && status != PGRES_TUPLES_OK) {
    AdbcStatusCode code =
        SetError(error, result, "[libpq] Failed to execute query: %s\nQuery was:%s",
                 PQerrorMessage(connection_->conn()), query_.c_str());
    PQclear(result);
    return code;
  }
  if (rows_affected) {
    if (status == PGRES_TUPLES_OK) {
      *rows_affected = PQntuples(reader_.result_);
    } else {
      // In theory, PQcmdTuples would work here, but experimentally it gives
      // an empty string even for a DELETE.  (Also, why does it return a
      // string...)  Possibly, it doesn't work because we use PQexecPrepared
      // but the docstring is careful to specify it works on an EXECUTE of a
      // prepared statement.
      *rows_affected = -1;
    }
  }
  PQclear(result);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::GetOption(const char* key, char* value, size_t* length,
                                            struct AdbcError* error) {
  std::string result;
  if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0) {
    result = ingest_.target;
  } else if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA) == 0) {
    result = ingest_.db_schema;
  } else if (std::strcmp(key, ADBC_INGEST_OPTION_MODE) == 0) {
    switch (ingest_.mode) {
      case IngestMode::kCreate:
        result = ADBC_INGEST_OPTION_MODE_CREATE;
        break;
      case IngestMode::kAppend:
        result = ADBC_INGEST_OPTION_MODE_APPEND;
        break;
      case IngestMode::kReplace:
        result = ADBC_INGEST_OPTION_MODE_REPLACE;
        break;
      case IngestMode::kCreateAppend:
        result = ADBC_INGEST_OPTION_MODE_CREATE_APPEND;
        break;
    }
  } else if (std::strcmp(key, ADBC_POSTGRESQL_OPTION_BATCH_SIZE_HINT_BYTES) == 0) {
    result = std::to_string(reader_.batch_size_hint_bytes_);
  } else {
    SetError(error, "[libpq] Unknown statement option '%s'", key);
    return ADBC_STATUS_NOT_FOUND;
  }

  if (result.size() + 1 <= *length) {
    std::memcpy(value, result.data(), result.size() + 1);
  }
  *length = static_cast<int64_t>(result.size() + 1);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::GetOptionBytes(const char* key, uint8_t* value,
                                                 size_t* length,
                                                 struct AdbcError* error) {
  SetError(error, "[libpq] Unknown statement option '%s'", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode PostgresStatement::GetOptionDouble(const char* key, double* value,
                                                  struct AdbcError* error) {
  SetError(error, "[libpq] Unknown statement option '%s'", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode PostgresStatement::GetOptionInt(const char* key, int64_t* value,
                                               struct AdbcError* error) {
  std::string result;
  if (std::strcmp(key, ADBC_POSTGRESQL_OPTION_BATCH_SIZE_HINT_BYTES) == 0) {
    *value = reader_.batch_size_hint_bytes_;
    return ADBC_STATUS_OK;
  }
  SetError(error, "[libpq] Unknown statement option '%s'", key);
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode PostgresStatement::GetParameterSchema(struct ArrowSchema* schema,
                                                     struct AdbcError* error) {
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresStatement::Prepare(struct AdbcError* error) {
  if (query_.empty()) {
    SetError(error, "%s", "[libpq] Must SetSqlQuery() before Prepare()");
    return ADBC_STATUS_INVALID_STATE;
  }

  // Don't actually prepare until execution time, so we know the
  // parameter types
  prepared_ = true;
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::Release(struct AdbcError* error) {
  ClearResult();
  if (bind_.release) {
    bind_.release(&bind_);
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::SetSqlQuery(const char* query,
                                              struct AdbcError* error) {
  ingest_.target.clear();
  ingest_.db_schema.clear();
  query_ = query;
  prepared_ = false;
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::SetOption(const char* key, const char* value,
                                            struct AdbcError* error) {
  if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0) {
    query_.clear();
    ingest_.target = value;
    prepared_ = false;
  } else if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA) == 0) {
    query_.clear();
    if (value == nullptr) {
      ingest_.db_schema.clear();
    } else {
      ingest_.db_schema = value;
    }
    prepared_ = false;
  } else if (std::strcmp(key, ADBC_INGEST_OPTION_MODE) == 0) {
    if (std::strcmp(value, ADBC_INGEST_OPTION_MODE_CREATE) == 0) {
      ingest_.mode = IngestMode::kCreate;
    } else if (std::strcmp(value, ADBC_INGEST_OPTION_MODE_APPEND) == 0) {
      ingest_.mode = IngestMode::kAppend;
    } else if (std::strcmp(value, ADBC_INGEST_OPTION_MODE_REPLACE) == 0) {
      ingest_.mode = IngestMode::kReplace;
    } else if (std::strcmp(value, ADBC_INGEST_OPTION_MODE_CREATE_APPEND) == 0) {
      ingest_.mode = IngestMode::kCreateAppend;
    } else {
      SetError(error, "[libpq] Invalid value '%s' for option '%s'", value, key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    prepared_ = false;
  } else if (std::strcmp(key, ADBC_INGEST_OPTION_TEMPORARY) == 0) {
    if (std::strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0) {
      // https://github.com/apache/arrow-adbc/issues/1109: only clear the
      // schema if enabling since Python always sets the flag explicitly
      ingest_.temporary = true;
      ingest_.db_schema.clear();
    } else if (std::strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
      ingest_.temporary = false;
    } else {
      SetError(error, "[libpq] Invalid value '%s' for option '%s'", value, key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    prepared_ = false;
  } else if (std::strcmp(key, ADBC_POSTGRESQL_OPTION_BATCH_SIZE_HINT_BYTES) == 0) {
    int64_t int_value = std::atol(value);
    if (int_value <= 0) {
      SetError(error, "[libpq] Invalid value '%s' for option '%s'", value, key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    this->reader_.batch_size_hint_bytes_ = int_value;
  } else {
    SetError(error, "[libpq] Unknown statement option '%s'", key);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::SetOptionBytes(const char* key, const uint8_t* value,
                                                 size_t length, struct AdbcError* error) {
  SetError(error, "%s%s", "[libpq] Unknown statement option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresStatement::SetOptionDouble(const char* key, double value,
                                                  struct AdbcError* error) {
  SetError(error, "%s%s", "[libpq] Unknown statement option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresStatement::SetOptionInt(const char* key, int64_t value,
                                               struct AdbcError* error) {
  if (std::strcmp(key, ADBC_POSTGRESQL_OPTION_BATCH_SIZE_HINT_BYTES) == 0) {
    if (value <= 0) {
      SetError(error, "[libpq] Invalid value '%" PRIi64 "' for option '%s'", value, key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    this->reader_.batch_size_hint_bytes_ = value;
    return ADBC_STATUS_OK;
  }
  SetError(error, "[libpq] Unknown statement option '%s'", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresStatement::SetupReader(struct AdbcError* error) {
  // TODO: we should pipeline here and assume this will succeed
  PGresult* result = PQprepare(connection_->conn(), /*stmtName=*/"", query_.c_str(),
                               /*nParams=*/0, nullptr);
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    AdbcStatusCode code =
        SetError(error, result,
                 "[libpq] Failed to execute query: could not infer schema: failed to "
                 "prepare query: %s\nQuery was:%s",
                 PQerrorMessage(connection_->conn()), query_.c_str());
    PQclear(result);
    return code;
  }
  PQclear(result);
  result = PQdescribePrepared(connection_->conn(), /*stmtName=*/"");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    AdbcStatusCode code =
        SetError(error, result,
                 "[libpq] Failed to execute query: could not infer schema: failed to "
                 "describe prepared statement: %s\nQuery was:%s",
                 PQerrorMessage(connection_->conn()), query_.c_str());
    PQclear(result);
    return code;
  }

  // Resolve the information from the PGresult into a PostgresType
  PostgresType root_type;
  AdbcStatusCode status = ResolvePostgresType(*type_resolver_, result, &root_type, error);
  PQclear(result);
  if (status != ADBC_STATUS_OK) return status;

  // Initialize the copy reader and infer the output schema (i.e., error for
  // unsupported types before issuing the COPY query)
  reader_.copy_reader_ = std::make_unique<PostgresCopyStreamReader>();
  reader_.copy_reader_->Init(root_type);
  struct ArrowError na_error;
  int na_res = reader_.copy_reader_->InferOutputSchema(&na_error);
  if (na_res != NANOARROW_OK) {
    SetError(error, "[libpq] Failed to infer output schema: (%d) %s: %s", na_res,
             std::strerror(na_res), na_error.message);
    return ADBC_STATUS_INTERNAL;
  }
  return ADBC_STATUS_OK;
}

void PostgresStatement::ClearResult() {
  // TODO: we may want to synchronize here for safety
  reader_.Release();
}
}  // namespace adbcpq
