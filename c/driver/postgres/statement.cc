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

#include <netinet/in.h>
#include <array>
#include <cerrno>
#include <cstring>
#include <iostream>
#include <memory>
#include <utility>
#include <vector>

#include <adbc.h>
#include <libpq-fe.h>
#include <nanoarrow.h>

#include "connection.h"
#include "type.h"
#include "util.h"

namespace adbcpq {

namespace {
/// The header that comes at the start of a binary COPY stream
constexpr std::array<char, 11> kPgCopyBinarySignature = {
    'P', 'G', 'C', 'O', 'P', 'Y', '\n', '\377', '\r', '\n', '\0'};
/// The flag indicating to Postgres that we want binary-format values.
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

/// Build an Arrow schema from a Postgres result set
AdbcStatusCode InferSchema(const TypeMapping& type_mapping, PGresult* result,
                           struct ArrowSchema* out, struct AdbcError* error) {
  const int num_fields = PQnfields(result);
  CHECK_NA_ADBC(ArrowSchemaInit(out, NANOARROW_TYPE_STRUCT), error);
  CHECK_NA_ADBC(ArrowSchemaAllocateChildren(out, num_fields), error);
  for (int i = 0; i < num_fields; i++) {
    ArrowType field_type = NANOARROW_TYPE_NA;
    const Oid pg_type = PQftype(result, i);

    auto it = type_mapping.type_mapping.find(pg_type);
    if (it == type_mapping.type_mapping.end()) {
      SetError(error, "Column #", i + 1, " (\"", PQfname(result, i),
               "\") has unknown type code ", pg_type);
      return ADBC_STATUS_NOT_IMPLEMENTED;
    }

    switch (it->second) {
      // TODO: this mapping will eventually have to become dynamic,
      // because of complex types like arrays/records
      case PgType::kBool:
        field_type = NANOARROW_TYPE_BOOL;
        break;
      case PgType::kFloat4:
        field_type = NANOARROW_TYPE_FLOAT;
        break;
      case PgType::kFloat8:
        field_type = NANOARROW_TYPE_DOUBLE;
        break;
      case PgType::kInt2:
        field_type = NANOARROW_TYPE_INT16;
        break;
      case PgType::kInt4:
        field_type = NANOARROW_TYPE_INT32;
        break;
      case PgType::kInt8:
        field_type = NANOARROW_TYPE_INT64;
        break;
      case PgType::kText:
        field_type = NANOARROW_TYPE_STRING;
        break;
      default:
        SetError(error, "Column #", i + 1, " (\"", PQfname(result, i),
                 "\") has unimplemented type code ", pg_type);
        return ADBC_STATUS_NOT_IMPLEMENTED;
    }
    CHECK_NA_ADBC(ArrowSchemaInit(out->children[i], field_type), error);
    CHECK_NA_ADBC(ArrowSchemaSetName(out->children[i], PQfname(result, i)), error);
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
    CHECK_NA_ADBC(bind->get_schema(&bind.value, &bind_schema.value), error);
    CHECK_NA_ADBC(
        ArrowSchemaViewInit(&bind_schema_view, &bind_schema.value, /*error*/ nullptr),
        error);

    if (bind_schema_view.data_type != ArrowType::NANOARROW_TYPE_STRUCT) {
      SetError(error, "Bind parameters must have type STRUCT");
      return ADBC_STATUS_INVALID_STATE;
    }

    bind_schema_fields.resize(bind_schema->n_children);
    for (size_t i = 0; i < bind_schema_fields.size(); i++) {
      CHECK_NA_ADBC(ArrowSchemaViewInit(&bind_schema_fields[i], bind_schema->children[i],
                                        /*error*/ nullptr),
                    error);
    }

    return std::move(callback)();
  }

  AdbcStatusCode SetParamTypes(const TypeMapping& type_mapping, struct AdbcError* error) {
    param_types.resize(bind_schema->n_children);
    param_values.resize(bind_schema->n_children);
    param_lengths.resize(bind_schema->n_children);
    param_formats.resize(bind_schema->n_children, kPgBinaryFormat);
    param_values_offsets.reserve(bind_schema->n_children);

    for (size_t i = 0; i < bind_schema_fields.size(); i++) {
      PgType pg_type;
      switch (bind_schema_fields[i].data_type) {
        case ArrowType::NANOARROW_TYPE_INT16:
          pg_type = PgType::kInt2;
          param_lengths[i] = 2;
          break;
        case ArrowType::NANOARROW_TYPE_INT32:
          pg_type = PgType::kInt4;
          param_lengths[i] = 4;
          break;
        case ArrowType::NANOARROW_TYPE_INT64:
          pg_type = PgType::kInt8;
          param_lengths[i] = 8;
          break;
        case ArrowType::NANOARROW_TYPE_STRING:
          pg_type = PgType::kText;
          param_lengths[i] = 0;
          break;
        default:
          // TODO: data type to string
          SetError(error, "Field #", i + 1, " ('", bind_schema->children[i]->name,
                   "') has unsupported parameter type ", bind_schema_fields[i].data_type);
          return ADBC_STATUS_NOT_IMPLEMENTED;
      }

      param_types[i] = type_mapping.GetOid(pg_type);
      if (param_types[i] == 0) {
        // TODO: data type to string
        SetError(error, "Field #", i + 1, " ('", bind_schema->children[i]->name,
                 "') has type with no corresponding Postgres type ",
                 bind_schema_fields[i].data_type);
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
      SetError(error, "Failed to prepare query: ", PQerrorMessage(conn));
      SetError(error, "Query: ", query);
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
        SetError(error, "Failed to read next batch from stream of bind parameters: ",
                 bind->get_last_error(&bind.value));
        return ADBC_STATUS_IO;
      }
      if (!array->release) break;

      Handle<struct ArrowArrayView> array_view;
      // TODO: include error messages
      CHECK_NA_ADBC(
          ArrowArrayViewInitFromSchema(&array_view.value, &bind_schema.value, nullptr),
          error);
      CHECK_NA_ADBC(ArrowArrayViewSetArray(&array_view.value, &array.value, nullptr),
                    error);

      for (int64_t row = 0; row < array->length; row++) {
        for (int64_t col = 0; col < array_view->n_children; col++) {
          if (ArrowArrayViewIsNull(array_view->children[col], row)) {
            param_values[col] = nullptr;
            continue;
          } else {
            param_values[col] = param_values_buffer.data() + param_values_offsets[col];
          }
          switch (bind_schema_fields[col].data_type) {
            case ArrowType::NANOARROW_TYPE_INT64: {
              const int64_t value = ToNetworkInt64(
                  array_view->children[col]->buffer_views[1].data.as_int64[row]);
              std::memcpy(param_values[col], &value, sizeof(int64_t));
              break;
            }
            case ArrowType::NANOARROW_TYPE_STRING: {
              const ArrowBufferView view =
                  ArrowArrayViewGetBytesUnsafe(array_view->children[col], row);
              // TODO: overflow check?
              param_lengths[col] = static_cast<int>(view.n_bytes);
              param_values[col] = const_cast<char*>(view.data.as_char);
              break;
            }
            default:
              // TODO: data type to string
              SetError(error, "Field #", col + 1, " ('", bind_schema->children[col]->name,
                       "') has unsupported type for ingestion ",
                       bind_schema_fields[col].data_type);
              return ADBC_STATUS_NOT_IMPLEMENTED;
          }
        }

        result = PQexecPrepared(conn, /*stmtName=*/"",
                                /*nParams=*/bind_schema->n_children, param_values.data(),
                                param_lengths.data(), param_formats.data(),
                                /*resultFormat=*/0 /*text*/);

        if (PQresultStatus(result) != PGRES_COMMAND_OK) {
          SetError(error, "Failed to execute prepared statement: ", PQerrorMessage(conn));
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
  CHECK_NA(ArrowSchemaDeepCopy(&schema_, out));
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
    last_error_ = StringBuilder("[libpq] Failed to init output array: ", na_res,
                                std::strerror(na_res), ": ", error.message);
    if (out->release) out->release(out);
    return na_res;
  }

  std::vector<ArrowSchemaView> fields(schema_.n_children);
  CHECK_NA(ArrowArrayStartAppending(out));
  for (int col = 0; col < schema_.n_children; col++) {
    na_res = ArrowSchemaViewInit(&fields[col], schema_.children[col], &error);
    if (na_res != 0) {
      last_error_ = StringBuilder("[libpq] Failed to init schema view: ", na_res,
                                  std::strerror(na_res), ": ", error.message);
      if (out->release) out->release(out);
      return na_res;
    }

    struct ArrowBitmap validity_bitmap;
    ArrowBitmapInit(&validity_bitmap);
    ArrowArraySetValidityBitmap(out->children[col], &validity_bitmap);
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
    if (size < kPqHeaderLength) {
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
  na_res = ArrowArrayFinishBuilding(out, 0);
  if (na_res != 0) {
    result_code = na_res;
    if (!last_error_.empty()) last_error_ += '\n';
    last_error_ += StringBuilder("[libpq] Failed to build result array");
  }

  // Check the server-side response
  result_ = PQgetResult(conn_);
  const int pq_status = PQresultStatus(result_);
  if (pq_status != PGRES_COMMAND_OK) {
    if (!last_error_.empty()) last_error_ += '\n';
    last_error_ += StringBuilder("[libpq] Query failed: (", pq_status, ") ",
                                 PQresultErrorMessage(result_));
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
    last_error_ = StringBuilder("[libpq] Expected ", schema_.n_children,
                                " fields but found ", field_count);
    return EIO;
  }

  for (int col = 0; col < schema_.n_children; col++) {
    int32_t field_length = LoadNetworkInt32(buf);
    buf += sizeof(int32_t);

    struct ArrowBitmap* bitmap = ArrowArrayValidityBitmap(out->children[col]);

    // TODO: set error message here
    CHECK_NA(ArrowBitmapAppend(bitmap, field_length >= 0, 1));

    switch (fields[col].data_type) {
      case NANOARROW_TYPE_DOUBLE: {
        // DCHECK_EQ(field_length, 8);
        static_assert(sizeof(double) == sizeof(uint64_t),
                      "Float is not same size as uint64_t");
        struct ArrowBuffer* buffer = ArrowArrayBuffer(out->children[col], 1);
        uint64_t raw_value = LoadNetworkUInt64(buf);
        buf += sizeof(uint64_t);
        double value = 0.0;
        std::memcpy(&value, &raw_value, sizeof(double));
        CHECK_NA(ArrowBufferAppendDouble(buffer, value));
        break;
      }
      case NANOARROW_TYPE_FLOAT: {
        // DCHECK_EQ(field_length, 4);
        static_assert(sizeof(float) == sizeof(uint32_t),
                      "Float is not same size as uint32_t");
        struct ArrowBuffer* buffer = ArrowArrayBuffer(out->children[col], 1);
        uint32_t raw_value = LoadNetworkUInt32(buf);
        buf += sizeof(uint32_t);
        float value = 0.0;
        std::memcpy(&value, &raw_value, sizeof(float));
        CHECK_NA(ArrowBufferAppendFloat(buffer, value));
        break;
      }
      case NANOARROW_TYPE_INT32: {
        // DCHECK_EQ(field_length, 4);
        struct ArrowBuffer* buffer = ArrowArrayBuffer(out->children[col], 1);
        int32_t value = LoadNetworkInt32(buf);
        buf += sizeof(int32_t);
        CHECK_NA(ArrowBufferAppendInt32(buffer, value));
        break;
      }
      case NANOARROW_TYPE_INT64: {
        // DCHECK_EQ(field_length, 8);
        struct ArrowBuffer* buffer = ArrowArrayBuffer(out->children[col], 1);
        int64_t value = field_length < 0 ? 0 : LoadNetworkInt64(buf);
        buf += sizeof(int64_t);
        CHECK_NA(ArrowBufferAppendInt64(buffer, value));
        break;
      }
      case NANOARROW_TYPE_STRING: {
        // textsend() in varlena.c
        struct ArrowBuffer* offset = ArrowArrayBuffer(out->children[col], 1);
        struct ArrowBuffer* data = ArrowArrayBuffer(out->children[col], 2);
        const int32_t last_offset =
            reinterpret_cast<const int32_t*>(offset->data)[*row_count];
        CHECK_NA(ArrowBufferAppendInt32(offset, last_offset + field_length));
        CHECK_NA(ArrowBufferAppend(data, buf, field_length));
        buf += field_length;
        break;
      }
      default:
        last_error_ = StringBuilder("[libpq] Column #", col + 1, " (\"",
                                    schema_.children[col]->name,
                                    "\") has unsupported type ", fields[col].data_type);
        return ENOTSUP;
    }
  }
  (*row_count)++;
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
    SetError(error, "Must provide an initialized AdbcConnection");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  connection_ =
      *reinterpret_cast<std::shared_ptr<PostgresConnection>*>(connection->private_data);
  type_mapping_ = connection_->type_mapping();
  reader_.conn_ = connection_->conn();
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::Bind(struct ArrowArray* values,
                                       struct ArrowSchema* schema,
                                       struct AdbcError* error) {
  if (!values || !values->release) {
    SetError(error, "Must provide non-NULL array");
    return ADBC_STATUS_INVALID_ARGUMENT;
  } else if (!schema || !schema->release) {
    SetError(error, "Must provide non-NULL schema");
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
    SetError(error, "Must provide non-NULL stream");
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
    switch (source_schema_fields[i].data_type) {
      case ArrowType::NANOARROW_TYPE_INT16:
        create += " SMALLINT";
        break;
      case ArrowType::NANOARROW_TYPE_INT32:
        create += " INTEGER";
        break;
      case ArrowType::NANOARROW_TYPE_INT64:
        create += " BIGINT";
        break;
      case ArrowType::NANOARROW_TYPE_STRING:
        create += " TEXT";
        break;
      default:
        // TODO: data type to string
        SetError(error, "Field #", i + 1, " ('", source_schema.children[i]->name,
                 "') has unsupported type for ingestion ",
                 source_schema_fields[i].data_type);
        return ADBC_STATUS_NOT_IMPLEMENTED;
    }
  }

  create += ")";
  SetError(error, create);
  PGresult* result = PQexecParams(connection_->conn(), create.c_str(), /*nParams=*/0,
                                  /*paramTypes=*/nullptr, /*paramValues=*/nullptr,
                                  /*paramLengths=*/nullptr, /*paramFormats=*/nullptr,
                                  /*resultFormat=*/1 /*(binary)*/);
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    SetError(error, "Failed to create table: ", PQerrorMessage(connection_->conn()));
    SetError(error, "Query: ", create);
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
    SetError(error, "Prepared statements without parameters are not implemented");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  if (stream) {
    // TODO:
    SetError(error, "Prepared statements returning result sets are not implemented");
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }

  BindStream bind_stream(std::move(bind_));
  std::memset(&bind_, 0, sizeof(bind_));

  CHECK(bind_stream.Begin([&]() { return ADBC_STATUS_OK; }, error));
  CHECK(bind_stream.SetParamTypes(*type_mapping_, error));
  CHECK(bind_stream.Prepare(connection_->conn(), query_, error));
  CHECK(bind_stream.Execute(connection_->conn(), rows_affected, error));
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
    SetError(error, "Must SetSqlQuery before ExecuteQuery");
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
      SetError(error, "Query was: ", schema_query);
      SetError(error, "Failed to execute query: could not infer schema: ",
               PQerrorMessage(connection_->conn()));
      PQclear(result);
      return ADBC_STATUS_IO;
    }
    AdbcStatusCode status = InferSchema(*type_mapping_, result, &reader_.schema_, error);
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
      SetError(error, "Query was: ", copy_query);
      SetError(error, "Failed to execute query: could not begin COPY: ",
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
    SetError(error, "Must Bind() before Execute() for bulk ingestion");
    return ADBC_STATUS_INVALID_STATE;
  }

  BindStream bind_stream(std::move(bind_));
  std::memset(&bind_, 0, sizeof(bind_));
  CHECK(bind_stream.Begin(
      [&]() -> AdbcStatusCode {
        if (!ingest_.append) {
          // CREATE TABLE
          return CreateBulkTable(bind_stream.bind_schema.value,
                                 bind_stream.bind_schema_fields, error);
        }
        return ADBC_STATUS_OK;
      },
      error));
  CHECK(bind_stream.SetParamTypes(*type_mapping_, error));

  std::string insert = "INSERT INTO ";
  insert += ingest_.target;
  insert += " VALUES (";
  for (size_t i = 0; i < bind_stream.bind_schema_fields.size(); i++) {
    if (i > 0) insert += ", ";
    insert += "$";
    insert += std::to_string(i + 1);
  }
  insert += ")";

  CHECK(bind_stream.Prepare(connection_->conn(), insert, error));
  CHECK(bind_stream.Execute(connection_->conn(), rows_affected, error));
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresStatement::ExecuteUpdateQuery(int64_t* rows_affected,
                                                     struct AdbcError* error) {
  if (query_.empty()) {
    SetError(error, "Must SetSqlQuery before ExecuteQuery");
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
    SetError(error, "Query was: ", query_);
    SetError(error, "Failed to execute query: ", PQerrorMessage(connection_->conn()));
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
    SetError(error, "Must SetSqlQuery() before Prepare()");
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
      SetError(error, "Invalid value ", value, " for option ", key);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
  } else {
    SetError(error, "Unknown statement option ", key);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  return ADBC_STATUS_OK;
}

void PostgresStatement::ClearResult() {
  // TODO: we may want to synchronize here for safety
  reader_.Release();
}
}  // namespace adbcpq
