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
#include <nanoarrow/nanoarrow.hpp>

#include "common/utils.h"
#include "connection.h"
#include "postgres_copy_reader.h"
#include "postgres_type.h"
#include "postgres_util.h"

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
        case ArrowType::NANOARROW_TYPE_FLOAT:
          type_id = PostgresTypeId::kFloat4;
          param_lengths[i] = 4;
          break;
        case ArrowType::NANOARROW_TYPE_DOUBLE:
          type_id = PostgresTypeId::kFloat8;
          param_lengths[i] = 8;
          break;
        case ArrowType::NANOARROW_TYPE_STRING:
          type_id = PostgresTypeId::kText;
          param_lengths[i] = 0;
          break;
        case ArrowType::NANOARROW_TYPE_BINARY:
          type_id = PostgresTypeId::kBytea;
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
      SetError(error, "[libpq] Failed to prepare query: %s\nQuery was:%s",
               PQerrorMessage(conn), query.c_str());
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
            case ArrowType::NANOARROW_TYPE_BINARY: {
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
  int na_res = copy_reader_->GetSchema(out);
  if (out->release == nullptr) {
    StringBuilderAppend(&error_builder_,
                        "[libpq] Result set was already consumed or freed");
    return EINVAL;
  } else if (na_res != NANOARROW_OK) {
    // e.g., Can't allocate memory
    StringBuilderAppend(&error_builder_, "[libpq] Error copying schema");
  }

  return na_res;
}

int TupleReader::GetNext(struct ArrowArray* out) {
  if (!result_) {
    out->release = nullptr;
    return 0;
  }

  // Clear the result, since the data is actually read from the connection
  PQclear(result_);
  result_ = nullptr;

  // Clear the error builder
  error_builder_.size = 0;

  struct ArrowError error;
  error.message[0] = '\0';
  struct ArrowBufferView data;
  data.data.data = nullptr;
  data.size_bytes = 0;

  // Fetch + parse the header
  int get_copy_res = PQgetCopyData(conn_, &pgbuf_, /*async=*/0);
  if (get_copy_res == -2) {
    StringBuilderAppend(&error_builder_, "[libpq] Fetch header failed: %s",
                        PQerrorMessage(conn_));
    return EIO;
  }

  data.size_bytes = get_copy_res;
  data.data.as_char = pgbuf_;
  int na_res = copy_reader_->ReadHeader(&data, &error);
  if (na_res != NANOARROW_OK) {
    StringBuilderAppend(&error_builder_, "[libpq] ReadHeader failed: %s", error.message);
    return na_res;
  }

  int64_t row_id = 0;
  do {
    // Parse the result (the header AND the first row are included in the first
    // call to PQgetCopyData())
    na_res = copy_reader_->ReadRecord(&data, &error);
    if (na_res != NANOARROW_OK && na_res != ENODATA) {
      StringBuilderAppend(&error_builder_, "[libpq] ReadRecord failed at row %ld: %s",
                          static_cast<long>(row_id),  // NOLINT(runtime/int)
                          error.message);
      return na_res;
    }

    row_id++;

    // Fetch + check
    PQfreemem(pgbuf_);
    pgbuf_ = nullptr;
    get_copy_res = PQgetCopyData(conn_, &pgbuf_, /*async=*/0);
    if (get_copy_res == -2) {
      StringBuilderAppend(&error_builder_, "[libpq] Fetch row %ld failed: %s",
                          static_cast<long>(row_id),  // NOLINT(runtime/int)
                          PQerrorMessage(conn_));
      return EIO;
    } else if (get_copy_res == -1) {
      // Returned when COPY has finished
      break;
    }

    data.size_bytes = get_copy_res;
    data.data.as_char = pgbuf_;
  } while (true);

  na_res = copy_reader_->GetArray(out, &error);
  if (na_res != NANOARROW_OK) {
    StringBuilderAppend(&error_builder_, "[libpq] Failed to build result array: %s",
                        error.message);
    return na_res;
  }

  // Check the server-side response
  result_ = PQgetResult(conn_);
  const int pq_status = PQresultStatus(result_);
  if (pq_status != PGRES_COMMAND_OK) {
    StringBuilderAppend(&error_builder_, "[libpq] Query failed [%d]: %s", pq_status,
                        PQresultErrorMessage(result_));
    return EIO;
  }

  PQclear(result_);
  result_ = nullptr;
  return NANOARROW_OK;
}

void TupleReader::Release() {
  StringBuilderReset(&error_builder_);

  if (result_) {
    PQclear(result_);
    result_ = nullptr;
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
      case ArrowType::NANOARROW_TYPE_FLOAT:
        create += " REAL";
        break;
      case ArrowType::NANOARROW_TYPE_DOUBLE:
        create += " DOUBLE PRECISION";
        break;
      case ArrowType::NANOARROW_TYPE_STRING:
        create += " TEXT";
        break;
      case ArrowType::NANOARROW_TYPE_BINARY:
        create += " BYTEA";
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
    SetError(error, "[libpq] Failed to create table: %s\nQuery was: %s",
             PQerrorMessage(connection_->conn()), create.c_str());
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
  if (!stream && !ingest_.target.empty()) {
    return ExecuteUpdateBulk(rows_affected, error);
  }

  if (query_.empty()) {
    SetError(error, "%s", "[libpq] Must SetSqlQuery before ExecuteQuery");
    return ADBC_STATUS_INVALID_STATE;
  }

  // 1. Prepare the query to get the schema
  {
    // TODO: we should pipeline here and assume this will succeed
    PGresult* result = PQprepare(connection_->conn(), /*stmtName=*/"", query_.c_str(),
                                 /*nParams=*/0, nullptr);
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      SetError(error,
               "[libpq] Failed to execute query: could not infer schema: failed to "
               "prepare query: %s\nQuery was:%s",
               PQerrorMessage(connection_->conn()), query_.c_str());
      PQclear(result);
      return ADBC_STATUS_IO;
    }
    PQclear(result);
    result = PQdescribePrepared(connection_->conn(), /*stmtName=*/"");
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      SetError(error,
               "[libpq] Failed to execute query: could not infer schema: failed to "
               "describe prepared statement: %s\nQuery was:%s",
               PQerrorMessage(connection_->conn()), query_.c_str());
      PQclear(result);
      return ADBC_STATUS_IO;
    }

    // Resolve the information from the PGresult into a PostgresType
    PostgresType root_type;
    AdbcStatusCode status =
        ResolvePostgresType(*type_resolver_, result, &root_type, error);
    PQclear(result);
    if (status != ADBC_STATUS_OK) return status;

    // Initialize the copy reader and infer the output schema (i.e., error for
    // unsupported types before issuing the COPY query)
    reader_.copy_reader_.reset(new PostgresCopyStreamReader());
    reader_.copy_reader_->Init(root_type);
    struct ArrowError na_error;
    int na_res = reader_.copy_reader_->InferOutputSchema(&na_error);
    if (na_res != NANOARROW_OK) {
      SetError(error, "[libpq] Failed to infer output schema: %s", na_error.message);
      return na_res;
    }

    // If the caller did not request a result set or if there are no
    // inferred output columns (e.g. a CREATE or UPDATE), then don't
    // use COPY (which would fail anyways)
    if (!stream || root_type.n_children() == 0) {
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
    na_res = reader_.copy_reader_->InitFieldReaders(&na_error);
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
      SetError(error,
               "[libpq] Failed to execute query: could not begin COPY: %s\nQuery was: %s",
               PQerrorMessage(connection_->conn()), copy_query.c_str());
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
  // NOTE: must prepare first (used in ExecuteQuery)
  PGresult* result =
      PQexecPrepared(connection_->conn(), /*stmtName=*/"", /*nParams=*/0,
                     /*paramValues=*/nullptr, /*paramLengths=*/nullptr,
                     /*paramFormats=*/nullptr, /*resultFormat=*/kPgBinaryFormat);
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    SetError(error, "[libpq] Failed to execute query: %s\nQuery was:%s",
             PQerrorMessage(connection_->conn()), query_.c_str());
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
