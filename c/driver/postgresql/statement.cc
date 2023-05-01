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

#include "statement.h"

#include <array>
#include <cerrno>
#include <cinttypes>
#include <cstring>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include <adbc.h>
#include <libpq-fe.h>
#include <nanoarrow/nanoarrow.h>

#include "connection.h"
#include "postgres_type.h"
#include "postgres_util.h"
#include "utils.h"

namespace adbcpq {

namespace {
/// The header that comes at the start of a binary COPY stream
constexpr std::array<char, 11> kPgCopyBinarySignature = {
    'P', 'G', 'C', 'O', 'P', 'Y', '\n', '\377', '\r', '\n', '\0'};
/// The flag indicating to PostgreSQL that we want binary-format values.
constexpr int kPgBinaryFormat = 1;
// A negative field length indicates a null.
constexpr int32_t kNullFieldLength = -1;

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

/// Helper to manage resources with RAII

template <typename T>
struct Releaser {
  static void Release(T* value) {
    if (value->release) {
      value->release(value);
    }
  }
};

template <>
struct Releaser<struct ArrowArrayView> {
  static void Release(struct ArrowArrayView* value) {
    if (value->storage_type != NANOARROW_TYPE_UNINITIALIZED) {
      ArrowArrayViewReset(value);
    }
  }
};

template <typename Resource>
struct Handle {
  Resource value;

  Handle() { std::memset(&value, 0, sizeof(value)); }

  ~Handle() { Releaser<Resource>::Release(&value); }

  Resource* operator->() { return &value; }
};

/// Build an Arrow schema from a PostgreSQL result set
AdbcStatusCode InferSchema(const PostgresTypeResolver& type_resolver, PGresult* result,
                           struct ArrowSchema* out, struct AdbcError* error) {
  ArrowError na_error;
  const int num_fields = PQnfields(result);
  ArrowSchemaInit(out);
  CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(out, num_fields), error);
  for (int i = 0; i < num_fields; i++) {
    const Oid pg_oid = PQftype(result, i);
    PostgresType pg_type;
    if (type_resolver.Find(pg_oid, &pg_type, &na_error) != NANOARROW_OK) {
      SetError(error, "%s%d%s%s%s%d", "[libpq] Column #", i + 1, " (\"",
               PQfname(result, i), "\") has unknown type code ", pg_oid);
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    CHECK_NA(INTERNAL,
             pg_type.WithFieldName(PQfname(result, i)).SetSchema(out->children[i]),
             error);
  }
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
        case ArrowType::NANOARROW_TYPE_DOUBLE:
          type_id = PostgresTypeId::kFloat8;
          param_lengths[i] = 8;
          break;
        case ArrowType::NANOARROW_TYPE_STRING:
          type_id = PostgresTypeId::kText;
          param_lengths[i] = 0;
          break;
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

  AdbcStatusCode Prepare(PGconn* conn, const std::string& query,
                         struct AdbcError* error) {
    PGresult* result = PQprepare(conn, /*stmtName=*/"", query.c_str(),
                                 /*nParams=*/bind_schema->n_children, param_types.data());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      SetError(error, "%s%s", "[libpq] Failed to prepare query: ", PQerrorMessage(conn));
      SetError(error, "%s%s", "[libpq] Query: ", query.c_str());
      PQclear(result);
      return ADBC_STATUS_IO;
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
        // TODO: include errno
        SetError(error, "%s%s",
                 "[libpq] Failed to read next batch from stream of bind parameters: ",
                 bind->get_last_error(&bind.value));
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
            case ArrowType::NANOARROW_TYPE_INT32: {
              const int64_t value = ToNetworkInt32(
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
            case ArrowType::NANOARROW_TYPE_DOUBLE: {
              const uint64_t value = ToNetworkFloat8(
                  array_view->children[col]->buffer_views[1].data.as_double[row]);
              std::memcpy(param_values[col], &value, sizeof(uint64_t));
              break;
            }
            case ArrowType::NANOARROW_TYPE_STRING: {
              const ArrowBufferView view =
                  ArrowArrayViewGetBytesUnsafe(array_view->children[col], row);
              // TODO: overflow check?
              param_lengths[col] = static_cast<int>(view.size_bytes);
              param_values[col] = const_cast<char*>(view.data.as_char);
              break;
            }
            default:
              // TODO: data type to string
              SetError(error, "%s%" PRId64 "%s%s%s%ud", "[libpq] Field #", col + 1, " ('",
                       bind_schema->children[col]->name,
                       "') has unsupported type for ingestion ",
                       bind_schema_fields[col].type);
              return ADBC_STATUS_NOT_IMPLEMENTED;
          }
        }

        result = PQexecPrepared(conn, /*stmtName=*/"",
                                /*nParams=*/bind_schema->n_children, param_values.data(),
                                param_lengths.data(), param_formats.data(),
                                /*resultFormat=*/0 /*text*/);

        if (PQresultStatus(result) != PGRES_COMMAND_OK) {
          SetError(error, "%s%s", "[libpq] Failed to execute prepared statement: ",
                   PQerrorMessage(conn));
          PQclear(result);
          return ADBC_STATUS_IO;
        }

        PQclear(result);
      }
      if (rows_affected) *rows_affected += array->length;
    }
    return ADBC_STATUS_OK;
  }
};
}  // namespace

int TupleReader::GetSchema(struct ArrowSchema* out) {
  if (!schema_.release) {
    last_error_ = "[libpq] Result set was already consumed or freed";
    return EINVAL;
  }

  std::memset(out, 0, sizeof(*out));
  NANOARROW_RETURN_NOT_OK(ArrowSchemaDeepCopy(&schema_, out));
  return 0;
}

int TupleReader::GetNext(struct ArrowArray* out) {
  if (!result_) {
    out->release = nullptr;
    return 0;
  }

  // Clear the result, since the data is actually read from the connection
  PQclear(result_);
  result_ = nullptr;

  struct ArrowError error;
  // TODO: consistently release out on error (use another trampoline?)
  int na_res = ArrowArrayInitFromSchema(out, &schema_, &error);
  if (na_res != 0) {
    last_error_ = "[libpq] Failed to init output array: " + std::to_string(na_res) +
                  std::strerror(na_res) + ": " + error.message;
    if (out->release) out->release(out);
    return na_res;
  }

  std::vector<ArrowSchemaView> fields(schema_.n_children);
  NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(out));
  for (int col = 0; col < schema_.n_children; col++) {
    na_res = ArrowSchemaViewInit(&fields[col], schema_.children[col], &error);
    if (na_res != 0) {
      last_error_ = "[libpq] Failed to init schema view: " + std::to_string(na_res) +
                    std::strerror(na_res) + ": " + error.message;
      if (out->release) out->release(out);
      return na_res;
    }
  }

  // TODO: we need to always PQgetResult

  char* buf = nullptr;
  int buf_size = 0;

  // Get the header
  {
    constexpr size_t kPqHeaderLength =
        kPgCopyBinarySignature.size() + sizeof(uint32_t) + sizeof(uint32_t);
    // https://www.postgresql.org/docs/14/sql-copy.html#id-1.9.3.55.9.4.5
    const int size = PQgetCopyData(conn_, &pgbuf_, /*async=*/0);
    if (size < static_cast<int>(kPqHeaderLength)) {
      return EIO;
    } else if (std::strcmp(pgbuf_, kPgCopyBinarySignature.data()) != 0) {
      return EIO;
    }
    buf = pgbuf_ + kPgCopyBinarySignature.size();

    uint32_t flags = LoadNetworkUInt32(buf);
    buf += sizeof(uint32_t);
    if (flags != 0) {
      return EIO;
    }

    // XXX: is this signed or unsigned? not stated by the docs
    uint32_t extension_length = LoadNetworkUInt32(buf);
    buf += sizeof(uint32_t) + extension_length;

    buf_size = size - (kPqHeaderLength + extension_length);
  }

  // Append each row
  int result_code = 0;
  int64_t num_rows = 0;
  last_error_.clear();
  do {
    result_code = AppendNext(fields.data(), buf, buf_size, &num_rows, out);
    PQfreemem(pgbuf_);
    pgbuf_ = buf = nullptr;
    if (result_code != 0) break;

    buf_size = PQgetCopyData(conn_, &pgbuf_, /*async=*/0);
    if (buf_size < 0) {
      pgbuf_ = buf = nullptr;
      break;
    }
    buf = pgbuf_;
  } while (true);

  // Finish the result array
  for (int col = 0; col < schema_.n_children; col++) {
    out->children[col]->length = num_rows;
  }
  out->length = num_rows;
  na_res = ArrowArrayFinishBuildingDefault(out, &error);
  if (na_res != 0) {
    result_code = na_res;
    if (!last_error_.empty()) last_error_ += '\n';
    last_error_ += "[libpq] Failed to build result array" + std::string(error.message);
  }

  // Check the server-side response
  result_ = PQgetResult(conn_);
  const int pq_status = PQresultStatus(result_);
  if (pq_status != PGRES_COMMAND_OK) {
    if (!last_error_.empty()) last_error_ += '\n';
    last_error_ += "[libpq] Query failed: (" + std::to_string(pq_status) + ") " +
                   PQresultErrorMessage(result_);
    result_code = EIO;
  }
  PQclear(result_);
  result_ = nullptr;
  return result_code;
}

void TupleReader::Release() {
  if (result_) {
    PQclear(result_);
    result_ = nullptr;
  }
  if (schema_.release) {
    schema_.release(&schema_);
  }
  if (pgbuf_) {
    PQfreemem(pgbuf_);
    pgbuf_ = nullptr;
  }
}

void TupleReader::ExportTo(struct ArrowArrayStream* stream) {
  stream->get_schema = &GetSchemaTrampoline;
  stream->get_next = &GetNextTrampoline;
  stream->get_last_error = &GetLastErrorTrampoline;
  stream->release = &ReleaseTrampoline;
  stream->private_data = this;
}

int TupleReader::AppendNext(struct ArrowSchemaView* fields, const char* buf, int buf_size,
                            int64_t* row_count, struct ArrowArray* out) {
  // https://www.postgresql.org/docs/14/sql-copy.html#id-1.9.3.55.9.4.6
  // TODO: DCHECK_GE(buf_size, 2) << "Buffer too short to contain field count";

  int16_t field_count = 0;
  std::memcpy(&field_count, buf, sizeof(int16_t));
  buf += sizeof(int16_t);
  field_count = ntohs(field_count);

  if (field_count == -1) {
    // end-of-stream
    return 0;
  } else if (field_count != schema_.n_children) {
    last_error_ = "[libpq] Expected " + std::to_string(schema_.n_children) +
                  " fields but found " + std::to_string(field_count);
    return EIO;
  }

  for (int col = 0; col < schema_.n_children; col++) {
    int32_t field_length = LoadNetworkInt32(buf);
    buf += sizeof(int32_t);

    // TODO: set error message here
    if (field_length != kNullFieldLength) {
      NANOARROW_RETURN_NOT_OK(
          AppendValue(fields, buf, col, *row_count, field_length, out));
      buf += field_length;
    } else {
      NANOARROW_RETURN_NOT_OK(ArrowArrayAppendNull(out->children[col], 1));
    }
  }
  (*row_count)++;
  return 0;
}

int TupleReader::AppendValue(struct ArrowSchemaView* fields, const char* buf, int col,
                             int64_t row_count, int32_t field_length,
                             struct ArrowArray* out) {
  switch (fields[col].type) {
    case NANOARROW_TYPE_BOOL: {
      uint8_t raw_value = buf[0];
      buf += 1;

      if (raw_value != 0 && raw_value != 1) {
        last_error_ = "[libpq] Column #" + std::to_string(col + 1) + " (\"" +
                      schema_.children[col]->name + "\"): invalid BOOL value " +
                      std::to_string(raw_value);
        return EINVAL;
      }
      NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(out->children[col], raw_value));
      break;
    }
    case NANOARROW_TYPE_DOUBLE: {
      static_assert(sizeof(double) == sizeof(uint64_t),
                    "Float is not same size as uint64_t");
      uint64_t raw_value = LoadNetworkUInt64(buf);
      buf += sizeof(uint64_t);
      double value = 0.0;
      std::memcpy(&value, &raw_value, sizeof(double));
      NANOARROW_RETURN_NOT_OK(ArrowArrayAppendDouble(out->children[col], value));
      break;
    }
    case NANOARROW_TYPE_FLOAT: {
      static_assert(sizeof(float) == sizeof(uint32_t),
                    "Float is not same size as uint32_t");
      uint32_t raw_value = LoadNetworkUInt32(buf);
      buf += sizeof(uint32_t);
      float value = 0.0;
      std::memcpy(&value, &raw_value, sizeof(float));
      NANOARROW_RETURN_NOT_OK(ArrowArrayAppendDouble(out->children[col], value));
      break;
    }
    case NANOARROW_TYPE_INT16: {
      int32_t value = LoadNetworkInt16(buf);
      buf += sizeof(int32_t);
      NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(out->children[col], value));
      break;
    }
    case NANOARROW_TYPE_INT32: {
      int32_t value = LoadNetworkInt32(buf);
      buf += sizeof(int32_t);
      NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(out->children[col], value));
      break;
    }
    case NANOARROW_TYPE_INT64: {
      int64_t value = LoadNetworkInt64(buf);
      buf += sizeof(int64_t);
      NANOARROW_RETURN_NOT_OK(ArrowArrayAppendInt(out->children[col], value));
      break;
    }
    case NANOARROW_TYPE_BINARY: {
      NANOARROW_RETURN_NOT_OK(
          ArrowArrayAppendBytes(out->children[col], {buf, field_length}));
      break;
    }
    case NANOARROW_TYPE_STRING: {
      // textsend() in varlena.c
      NANOARROW_RETURN_NOT_OK(
          ArrowArrayAppendString(out->children[col], {buf, field_length}));
      break;
    }
    default:
      last_error_ = "[libpq] Column #" + std::to_string(col + 1) + " (\"" +
                    schema_.children[col]->name + "\") has unsupported type " +
                    std::to_string(fields[col].type);
      return ENOTSUP;
  }
  return 0;
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

AdbcStatusCode PostgresStatement::CreateBulkTable(
    const struct ArrowSchema& source_schema,
    const std::vector<struct ArrowSchemaView>& source_schema_fields,
    struct AdbcError* error) {
  std::string create = "CREATE TABLE ";
  create += ingest_.target;
  create += " (";

  for (size_t i = 0; i < source_schema_fields.size(); i++) {
    if (i > 0) create += ", ";
    create += source_schema.children[i]->name;
    switch (source_schema_fields[i].type) {
      case ArrowType::NANOARROW_TYPE_INT16:
        create += " SMALLINT";
        break;
      case ArrowType::NANOARROW_TYPE_INT32:
        create += " INTEGER";
        break;
      case ArrowType::NANOARROW_TYPE_INT64:
        create += " BIGINT";
        break;
      case ArrowType::NANOARROW_TYPE_DOUBLE:
        create += " DOUBLE PRECISION";
        break;
      case ArrowType::NANOARROW_TYPE_STRING:
        create += " TEXT";
        break;
      default:
        // TODO: data type to string
        SetError(error, "%s%" PRIu64 "%s%s%s%ud", "[libpq] Field #",
                 static_cast<uint64_t>(i + 1), " ('", source_schema.children[i]->name,
                 "') has unsupported type for ingestion ", source_schema_fields[i].type);
        return ADBC_STATUS_NOT_IMPLEMENTED;
    }
  }

  create += ")";
  SetError(error, "%s%s", "[libpq] ", create.c_str());
  PGresult* result = PQexecParams(connection_->conn(), create.c_str(), /*nParams=*/0,
                                  /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                                  /*paramLengths=*/nullptr, /*paramFormats=*/nullptr,
                                  /*resultFormat=*/1 /*(binary)*/);
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    SetError(error, "%s%s",
             "[libpq] Failed to create table: ", PQerrorMessage(connection_->conn()));
    SetError(error, "%s%s", "[libpq] Query: ", create.c_str());
    PQclear(result);
    return ADBC_STATUS_IO;
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
  RAISE_ADBC(bind_stream.Prepare(connection_->conn(), query_, error));
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
  if (!stream) {
    if (!ingest_.target.empty()) {
      return ExecuteUpdateBulk(rows_affected, error);
    }
    return ExecuteUpdateQuery(rows_affected, error);
  }

  if (query_.empty()) {
    SetError(error, "%s", "[libpq] Must SetSqlQuery before ExecuteQuery");
    return ADBC_STATUS_INVALID_STATE;
  }

  // 1. Execute the query with LIMIT 0 to get the schema
  {
    // TODO: we should pipeline here and assume this will succeed
    std::string schema_query = "SELECT * FROM (" + query_ + ") AS ignored LIMIT 0";
    PGresult* result =
        PQexecParams(connection_->conn(), query_.c_str(), /*nParams=*/0,
                     /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                     /*paramLengths=*/nullptr, /*paramFormats=*/nullptr, kPgBinaryFormat);
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
      SetError(error, "%s%s", "[libpq] Query was: ", schema_query.c_str());
      SetError(error, "%s%s", "[libpq] Failed to execute query: could not infer schema: ",
               PQerrorMessage(connection_->conn()));
      PQclear(result);
      return ADBC_STATUS_IO;
    }
    AdbcStatusCode status = InferSchema(*type_resolver_, result, &reader_.schema_, error);
    PQclear(result);
    if (status != ADBC_STATUS_OK) return status;
  }

  // 2. Execute the query with COPY to get binary tuples
  {
    std::string copy_query = "COPY (" + query_ + ") TO STDOUT (FORMAT binary)";
    reader_.result_ =
        PQexecParams(connection_->conn(), copy_query.c_str(), /*nParams=*/0,
                     /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                     /*paramLengths=*/nullptr, /*paramFormats=*/nullptr, kPgBinaryFormat);
    if (PQresultStatus(reader_.result_) != PGRES_COPY_OUT) {
      SetError(error, "%s%s", "[libpq] Query was: ", copy_query.c_str());
      SetError(error, "%s%s", "[libpq] Failed to execute query: could not begin COPY: ",
               PQerrorMessage(connection_->conn()));
      ClearResult();
      return ADBC_STATUS_IO;
    }
    // Result is read from the connection, not the result, but we won't clear it here
  }

  reader_.ExportTo(stream);
  if (rows_affected) *rows_affected = -1;
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::ExecuteUpdateBulk(int64_t* rows_affected,
                                                    struct AdbcError* error) {
  if (!bind_.release) {
    SetError(error, "%s", "[libpq] Must Bind() before Execute() for bulk ingestion");
    return ADBC_STATUS_INVALID_STATE;
  }

  BindStream bind_stream(std::move(bind_));
  std::memset(&bind_, 0, sizeof(bind_));
  RAISE_ADBC(bind_stream.Begin(
      [&]() -> AdbcStatusCode {
        if (!ingest_.append) {
          // CREATE TABLE
          return CreateBulkTable(bind_stream.bind_schema.value,
                                 bind_stream.bind_schema_fields, error);
        }
        return ADBC_STATUS_OK;
      },
      error));
  RAISE_ADBC(bind_stream.SetParamTypes(*type_resolver_, error));

  std::string insert = "INSERT INTO ";
  insert += ingest_.target;
  insert += " VALUES (";
  for (size_t i = 0; i < bind_stream.bind_schema_fields.size(); i++) {
    if (i > 0) insert += ", ";
    insert += "$";
    insert += std::to_string(i + 1);
  }
  insert += ")";

  RAISE_ADBC(bind_stream.Prepare(connection_->conn(), insert, error));
  RAISE_ADBC(bind_stream.Execute(connection_->conn(), rows_affected, error));
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::ExecuteUpdateQuery(int64_t* rows_affected,
                                                     struct AdbcError* error) {
  if (query_.empty()) {
    SetError(error, "%s", "[libpq] Must SetSqlQuery before ExecuteQuery");
    return ADBC_STATUS_INVALID_STATE;
  }

  PGresult* result = nullptr;

  if (prepared_) {
    result = PQexecPrepared(connection_->conn(), /*stmtName=*/"", /*nParams=*/0,
                            /*paramValues=*/nullptr, /*paramLengths=*/nullptr,
                            /*paramFormats=*/nullptr, /*resultFormat=*/kPgBinaryFormat);
  } else {
    result = PQexecParams(connection_->conn(), query_.c_str(), /*nParams=*/0,
                          /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                          /*paramLengths=*/nullptr, /*paramFormats=*/nullptr,
                          /*resultFormat=*/kPgBinaryFormat);
  }
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    SetError(error, "%s%s", "[libpq] Query was: ", query_.c_str());
    SetError(error, "%s%s",
             "[libpq] Failed to execute query: ", PQerrorMessage(connection_->conn()));
    PQclear(result);
    return ADBC_STATUS_IO;
  }
  if (rows_affected) *rows_affected = PQntuples(reader_.result_);
  PQclear(result);
  return ADBC_STATUS_OK;
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
  query_ = query;
  prepared_ = false;
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::SetOption(const char* key, const char* value,
                                            struct AdbcError* error) {
  if (std::strcmp(key, ADBC_INGEST_OPTION_TARGET_TABLE) == 0) {
    query_.clear();
    ingest_.target = value;
  } else if (std::strcmp(key, ADBC_INGEST_OPTION_MODE) == 0) {
    if (std::strcmp(value, ADBC_INGEST_OPTION_MODE_CREATE) == 0) {
      ingest_.append = false;
    } else if (std::strcmp(value, ADBC_INGEST_OPTION_MODE_APPEND) == 0) {
      ingest_.append = true;
    } else {
      SetError(error, "%s%s%s%s", "[libpq] Invalid value ", value, " for option ", key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
  } else {
    SetError(error, "%s%s", "[libq] Unknown statement option ", key);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  return ADBC_STATUS_OK;
}

void PostgresStatement::ClearResult() {
  // TODO: we may want to synchronize here for safety
  reader_.Release();
}
}  // namespace adbcpq
