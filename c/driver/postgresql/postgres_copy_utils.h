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

#include <cerrno>
#include <cstdint>
#include <memory>
#include <string>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.hpp>

#include "postgres_type.h"
#include "util.h"

namespace adbcpq {

static int8_t kPgCopyBinarySignature[] = {'P',  'G',    'C',  'O',  'P', 'Y',
                                          '\n', '\377', '\r', '\n', '\0'};

// Read a value from the buffer without checking the buffer size. Advances
// the cursor of data and reduces its size by sizeof(T).
template <typename T>
inline T ReadUnsafe(ArrowBufferView* data) {
  T out;
  memcpy(&out, data->data.data, sizeof(T));
  out = SwapNetworkToHost(out);
  data->data.as_uint8 += sizeof(T);
  data->size_bytes -= sizeof(T);
  return out;
}

// Define some explicit specializations for types that don't have a SwapNetworkToHost
// overload.
template <>
inline int8_t ReadUnsafe(ArrowBufferView* data) {
  int8_t out = data->data.as_int8[0];
  data->data.as_uint8 += sizeof(int8_t);
  data->size_bytes -= sizeof(int8_t);
  return out;
}

template <>
inline int16_t ReadUnsafe(ArrowBufferView* data) {
  return static_cast<int16_t>(ReadUnsafe<uint16_t>(data));
}

template <>
inline int32_t ReadUnsafe(ArrowBufferView* data) {
  return static_cast<int32_t>(ReadUnsafe<uint32_t>(data));
}

template <>
inline int64_t ReadUnsafe(ArrowBufferView* data) {
  return static_cast<int64_t>(ReadUnsafe<uint64_t>(data));
}

template <typename T>
ArrowErrorCode ReadChecked(ArrowBufferView* data, T* out, ArrowError* error) {
  if (data->size_bytes < static_cast<int64_t>(sizeof(T))) {
    ArrowErrorSet(error, "Unexpected end of input (expected %d bytes but found %ld)",
                  static_cast<int>(sizeof(T)),
                  static_cast<long>(data->size_bytes));  // NOLINT(runtime/int)
    return EINVAL;
  }

  *out = ReadUnsafe<T>(data);
  return NANOARROW_OK;
}

class PostgresCopyFieldReader {
 public:
  PostgresCopyFieldReader() : offsets_(nullptr), data_(nullptr) {
    memset(&schema_view_, 0, sizeof(ArrowSchemaView));
  }

  virtual ~PostgresCopyFieldReader() {}

  void Init(const PostgresType& pg_type) { pg_type_ = pg_type; }

  const PostgresType& InputType() const { return pg_type_; }

  virtual ArrowErrorCode InitSchema(ArrowSchema* schema) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view_, schema, nullptr));
    return NANOARROW_OK;
  }

  virtual ArrowErrorCode InitArray(ArrowArray* array) {
    // Cache some buffer pointers
    for (int32_t i = 0; i < 3; i++) {
      switch (schema_view_.layout.buffer_type[i]) {
        case NANOARROW_BUFFER_TYPE_DATA_OFFSET:
          if (schema_view_.layout.element_size_bits[i] == 32) {
            offsets_ = ArrowArrayBuffer(array, i);
          }
          break;
        case NANOARROW_BUFFER_TYPE_DATA:
          data_ = ArrowArrayBuffer(array, i);
          break;
        default:
          break;
      }
    }

    return NANOARROW_OK;
  }

  virtual ArrowErrorCode Read(ArrowBufferView* data, int32_t field_size_bytes,
                              ArrowArray* array, ArrowError* error) {
    return ENOTSUP;
  }

  virtual ArrowErrorCode FinishArray(ArrowArray* array, ArrowError* error) {
    return NANOARROW_OK;
  }

 protected:
  PostgresType pg_type_;
  ArrowSchemaView schema_view_;
  ArrowBuffer* offsets_;
  ArrowBuffer* data_;
  std::vector<std::unique_ptr<PostgresCopyFieldReader>> children_;
};

// Reader for a Postgres boolean (one byte -> bitmap)
class PostgresCopyBooleanFieldReader : public PostgresCopyFieldReader {
 public:
  ArrowErrorCode Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error) override {
    if (field_size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    if (field_size_bytes != 1) {
      ArrowErrorSet(error, "Expected field with one byte but found field with %d bytes",
                    static_cast<int>(field_size_bytes));
      return EINVAL;
    }

    int64_t bytes_required = _ArrowBytesForBits(array->length + 1);
    if (bytes_required > data_->size_bytes) {
      NANOARROW_RETURN_NOT_OK(
          ArrowBufferAppendFill(data_, 0, bytes_required - data_->size_bytes));
    }

    if (ReadUnsafe<int8_t>(data)) {
      ArrowBitSet(data_->data, array->length);
    } else {
      ArrowBitClear(data_->data, array->length);
    }

    array->length++;
    return NANOARROW_OK;
  }
};

// Reader for Pg->Arrow conversions whose representations are identical minus
// the bswap from network endian. This includes all integral and float types.
template <typename T>
class PostgresCopyNetworkEndianFieldReader : public PostgresCopyFieldReader {
 public:
  ArrowErrorCode Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error) override {
    if (field_size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    if (field_size_bytes != static_cast<int32_t>(sizeof(T))) {
      ArrowErrorSet(error, "Expected field with %d bytes but found field with %d bytes",
                    static_cast<int>(sizeof(T)), static_cast<int>(field_size_bytes));
      return EINVAL;
    }

    T value_uint = ReadUnsafe<T>(data);
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(data_, &value_uint, sizeof(T)));
    array->length++;
    return NANOARROW_OK;
  }
};

// Reader for Pg->Arrow conversions whose Arrow representation is simply the
// bytes of the field representation. This can be used with binary and string
// Arrow types and any Postgres type.
class PostgresCopyBinaryFieldReader : public PostgresCopyFieldReader {
 public:
  ArrowErrorCode Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error) override {
    if (data->size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    if (field_size_bytes > data->size_bytes) {
      ArrowErrorSet(error, "Expected %d bytes of field data but got %d bytes of input",
                    static_cast<int>(field_size_bytes),
                    static_cast<int>(data->size_bytes));
      return EINVAL;
    }

    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(data_, data->data.data, field_size_bytes));
    int32_t* offsets = reinterpret_cast<int32_t*>(offsets_->data);
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppendInt32(
        offsets_, offsets[array->length] + static_cast<int32_t>(data_->size_bytes)));

    array->length++;
    return NANOARROW_OK;
  }
};

//
class PostgresCopyArrayFieldReader : public PostgresCopyFieldReader {
 public:
  void InitChild(std::unique_ptr<PostgresCopyFieldReader> child) {
    child_ = std::move(child);
    child_->Init(*pg_type_.child(0));
  }

  ArrowErrorCode InitSchema(ArrowSchema* schema) override {
    NANOARROW_RETURN_NOT_OK(PostgresCopyFieldReader::InitSchema(schema));
    NANOARROW_RETURN_NOT_OK(child_->InitSchema(schema->children[0]));
    return NANOARROW_OK;
  }

  ArrowErrorCode InitArray(ArrowArray* array) override {
    NANOARROW_RETURN_NOT_OK(PostgresCopyFieldReader::InitArray(array));
    NANOARROW_RETURN_NOT_OK(child_->InitArray(array->children[0]));
    return NANOARROW_OK;
  }

  ArrowErrorCode Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error) override {
    if (data->size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    // Keep the cursor where we start to parse the array so we can check
    // the number of bytes read against the field size when finished
    const uint8_t* data0 = data->data.as_uint8;

    int32_t n_dim;
    NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &n_dim, error));
    int32_t flags;
    NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &flags, error));
    uint32_t element_type_oid;
    NANOARROW_RETURN_NOT_OK(ReadChecked<uint32_t>(data, &element_type_oid, error));

    // We could validate the OID here, but this is a poor fit for all cases
    // (e.g. testing) since the OID can be specific to each database

    if (n_dim < 0) {
      ArrowErrorSet(error, "Expected array n_dim > 0 but got %d",
                    static_cast<int>(n_dim));
      return EINVAL;
    }

    // This is apparently allowed
    if (n_dim == 0) {
      NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(array));
      return NANOARROW_OK;
    }

    int64_t n_items = 1;
    for (int32_t i = 0; i < n_dim; i++) {
      int32_t dim_size;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &dim_size, error));
      n_items *= dim_size;

      int32_t lower_bound;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &lower_bound, error));
      if (lower_bound != 0) {
        ArrowErrorSet(error, "Array value with lower bound != 0 is not supported");
        return EINVAL;
      }
    }

    ArrowBufferView field_data;
    for (int64_t i = 0; i < n_items; i++) {
      int32_t child_field_size_bytes;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &child_field_size_bytes, error));
      NANOARROW_RETURN_NOT_OK(
          child_->Read(data, child_field_size_bytes, array->children[0], error));
    }

    int64_t bytes_read = data->data.as_uint8 - data0;
    if (bytes_read != field_size_bytes) {
      ArrowErrorSet(error, "Expected to read %d bytes from array field but read %d bytes",
                    static_cast<int>(field_size_bytes), static_cast<int>(bytes_read));
      return EINVAL;
    }

    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(array));
    return NANOARROW_OK;
  }

 private:
  std::unique_ptr<PostgresCopyFieldReader> child_;
};

class PostgresCopyRecordFieldReader : public PostgresCopyFieldReader {
 public:
  void AppendChild(std::unique_ptr<PostgresCopyFieldReader> child) {
    int64_t child_i = static_cast<int64_t>(children_.size());
    children_.push_back(std::move(child));
    children_[child_i]->Init(*pg_type_.child(child_i));
  }

  ArrowErrorCode InitSchema(ArrowSchema* schema) override {
    NANOARROW_RETURN_NOT_OK(PostgresCopyFieldReader::InitSchema(schema));
    for (int64_t i = 0; i < schema->n_children; i++) {
      NANOARROW_RETURN_NOT_OK(children_[i]->InitSchema(schema->children[i]));
    }

    return NANOARROW_OK;
  }

  ArrowErrorCode InitArray(ArrowArray* array) override {
    NANOARROW_RETURN_NOT_OK(PostgresCopyFieldReader::InitArray(array));
    for (int64_t i = 0; i < array->n_children; i++) {
      NANOARROW_RETURN_NOT_OK(children_[i]->InitArray(array->children[i]));
    }

    return NANOARROW_OK;
  }

  ArrowErrorCode Read(ArrowBufferView* data, int32_t field_size_bytes, ArrowArray* array,
                      ArrowError* error) override {
    if (data->size_bytes == 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    // Keep the cursor where we start to parse the field so we can check
    // the number of bytes read against the field size when finished
    const uint8_t* data0 = data->data.as_uint8;

    int16_t n_fields;
    NANOARROW_RETURN_NOT_OK(ReadChecked<int16_t>(data, &n_fields, error));
    if (n_fields == -1) {
      return ENODATA;
    } else if (n_fields != array->n_children) {
      ArrowErrorSet(error,
                    "Expected -1 for end-of-stream or number of fields in output array "
                    "(%ld) but got %d",
                    static_cast<long>(array->n_children), static_cast<int>(n_fields));
      return EINVAL;
    }

    struct ArrowBufferView field_data;
    for (uint16_t i = 0; i < n_fields; i++) {
      int32_t child_field_size_bytes;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(data, &child_field_size_bytes, error));
      int result =
          children_[i]->Read(data, child_field_size_bytes, array->children[i], error);

      // On overflow, pretend all previous children for this struct were never
      // appended to. This leaves array in a valid state in the specific case
      // where EOVERFLOW was returned so that a higher level caller can attempt
      // to try again after creating a new array.
      if (result == EOVERFLOW) {
        for (int16_t j = 0; j < i; j++) {
          array->children[j]->length--;
        }
      }

      if (result != NANOARROW_OK) {
        return result;
      }
    }

    // field size == -1 means don't check (e.g., for a top-level row tuple)
    int64_t bytes_read = data->data.as_uint8 - data0;
    if (field_size_bytes != -1 && bytes_read != field_size_bytes) {
      ArrowErrorSet(error,
                    "Expected to read %d bytes from record field but read %d bytes",
                    static_cast<int>(field_size_bytes), static_cast<int>(bytes_read));
      return EINVAL;
    }

    array->length++;
    return NANOARROW_OK;
  }

 private:
  std::vector<std::unique_ptr<PostgresCopyFieldReader>> children_;
};

// Factory for a PostgresCopyFieldReader that instantiates the proper subclass
// and gives a nice error for Postgres type -> Arrow type conversions that aren't
// supported.
ArrowErrorCode ErrorCantConvert(ArrowError* error, const PostgresType& pg_type,
                                const ArrowSchemaView& schema_view) {
  ArrowErrorSet(error, "Can't convert Postgres type '%s' to Arrow type '%s'",
                pg_type.typname().c_str(), ArrowTypeString(schema_view.type));
  return EINVAL;
}

ArrowErrorCode MakeCopyFieldReader(const PostgresType& pg_type, ArrowSchema* schema,
                                   PostgresCopyFieldReader** out, ArrowError* error) {
  ArrowSchemaView schema_view;
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view, schema, nullptr));

  switch (schema_view.type) {
    case NANOARROW_TYPE_BOOL:
      switch (pg_type.recv()) {
        case PostgresType::PG_RECV_BOOL:
          *out = new PostgresCopyBooleanFieldReader();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_INT16:
      switch (pg_type.recv()) {
        case PostgresType::PG_RECV_INT2:
          *out = new PostgresCopyNetworkEndianFieldReader<uint16_t>();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_INT32:
      switch (pg_type.recv()) {
        case PostgresType::PG_RECV_INT4:
          *out = new PostgresCopyNetworkEndianFieldReader<uint32_t>();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_FLOAT:
      switch (pg_type.recv()) {
        case PostgresType::PG_RECV_FLOAT4:
          *out = new PostgresCopyNetworkEndianFieldReader<uint32_t>();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_INT64:
      switch (pg_type.recv()) {
        case PostgresType::PG_RECV_INT8:
          *out = new PostgresCopyNetworkEndianFieldReader<uint64_t>();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_DOUBLE:
      switch (pg_type.recv()) {
        case PostgresType::PG_RECV_FLOAT8:
          *out = new PostgresCopyNetworkEndianFieldReader<uint64_t>();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_STRING:
      switch (pg_type.recv()) {
        case PostgresType::PG_RECV_CHAR:
        case PostgresType::PG_RECV_VARCHAR:
        case PostgresType::PG_RECV_TEXT:
          *out = new PostgresCopyBinaryFieldReader();
          return NANOARROW_OK;
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_BINARY:
      // No need to check pg_type here: we can return the bytes of any
      // Postgres type as binary.
      *out = new PostgresCopyBinaryFieldReader();
      return NANOARROW_OK;

    case NANOARROW_TYPE_LIST:
      switch (pg_type.recv()) {
        case PostgresType::PG_RECV_ARRAY: {
          if (pg_type.n_children() != 1) {
            ArrowErrorSet(
                error, "Expected Postgres array type to have one child but found %ld",
                static_cast<long>(pg_type.n_children()));  // NOLINT(runtime/int)
            return EINVAL;
          }

          auto array_reader = std::unique_ptr<PostgresCopyArrayFieldReader>(
              new PostgresCopyArrayFieldReader());

          PostgresCopyFieldReader* child_reader;
          NANOARROW_RETURN_NOT_OK(MakeCopyFieldReader(
              *pg_type.child(0), schema->children[0], &child_reader, error));
          array_reader->InitChild(std::unique_ptr<PostgresCopyFieldReader>(child_reader));

          *out = array_reader.release();
          return NANOARROW_OK;
        }
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }

    case NANOARROW_TYPE_STRUCT:
      switch (pg_type.recv()) {
        case PostgresType::PG_RECV_RECORD: {
          if (pg_type.n_children() != schema->n_children) {
            ArrowErrorSet(error,
                          "Can't convert Postgres record type with %ld chlidren to Arrow "
                          "struct type with %ld children",
                          static_cast<long>(pg_type.n_children()),
                          static_cast<long>(schema->n_children));  // NOLINT(runtime/int)
            return EINVAL;
          }

          auto record_reader = std::unique_ptr<PostgresCopyRecordFieldReader>(
              new PostgresCopyRecordFieldReader());

          for (int64_t i = 0; i < pg_type.n_children(); i++) {
            PostgresCopyFieldReader* child_reader;
            NANOARROW_RETURN_NOT_OK(MakeCopyFieldReader(
                *pg_type.child(i), schema->children[i], &child_reader, error));
            record_reader->AppendChild(
                std::unique_ptr<PostgresCopyFieldReader>(child_reader));
          }

          *out = record_reader.release();
          return NANOARROW_OK;
        }
        default:
          return ErrorCantConvert(error, pg_type, schema_view);
      }
    default:
      return ErrorCantConvert(error, pg_type, schema_view);
  }
}

class PostgresCopyStreamReader {
 public:
  ArrowErrorCode Init(const PostgresType& pg_type) {
    if (pg_type.recv() != PostgresType::PG_RECV_RECORD) {
      return EINVAL;
    }

    root_reader_.Init(pg_type);
    return NANOARROW_OK;
  }

  ArrowErrorCode SetOutputSchema(ArrowSchema* schema, ArrowError* error) {
    if (std::string(schema_->format) != "+s") {
      ArrowErrorSet(
          error,
          "Expected output schema of type struct but got output schema with format '%s'",
          schema_->format);
      return EINVAL;
    }

    if (schema_->n_children != root_reader_.InputType().n_children()) {
      ArrowErrorSet(error,
                    "Expected output schema with %ld columns to match Postgres input but "
                    "got schema with %ld columns",
                    static_cast<long>(root_reader_.InputType().n_children()),
                    static_cast<long>(schema->n_children));  // NOLINT(runtime/int)
      return EINVAL;
    }

    schema_.reset(schema);
    return NANOARROW_OK;
  }

  ArrowErrorCode InferOutputSchema(ArrowError* error) {
    schema_.reset();
    ArrowSchemaInit(schema_.get());
    NANOARROW_RETURN_NOT_OK(root_reader_.InputType().SetSchema(schema_.get()));
    return NANOARROW_OK;
  }

  ArrowErrorCode InitFieldReaders(ArrowError* error) {
    if (schema_->release == nullptr) {
      return EINVAL;
    }

    const PostgresType& root_type = root_reader_.InputType();

    for (int64_t i = 0; i < root_type.n_children(); i++) {
      const PostgresType& child_type = *root_type.child(i);
      PostgresCopyFieldReader* child_reader;
      NANOARROW_RETURN_NOT_OK(
          MakeCopyFieldReader(child_type, schema_->children[i], &child_reader, error));
    }

    NANOARROW_RETURN_NOT_OK(root_reader_.InitSchema(schema_.get()));
    return NANOARROW_OK;
  }

  ArrowErrorCode ReadHeader(ArrowBufferView* data, ArrowError* error) {
    if (data->size_bytes < static_cast<int64_t>(sizeof(kPgCopyBinarySignature))) {
      ArrowErrorSet(error,
                    "Expected PGCOPY signature of %ld bytes at beginning of stream but "
                    "found %ld bytes of input",
                    static_cast<long>(sizeof(kPgCopyBinarySignature)),
                    static_cast<long>(data->size_bytes));  // NOLINT(runtime/int)
      return EINVAL;
    }

    if (memcmp(data->data.data, kPgCopyBinarySignature, sizeof(kPgCopyBinarySignature)) !=
        0) {
      ArrowErrorSet(error, "Invalid PGCOPY signature at beginning of stream");
      return EINVAL;
    }

    uint32_t flags;
    NANOARROW_RETURN_NOT_OK(ReadChecked<uint32_t>(data, &flags, error));
    uint32_t extension_length;
    NANOARROW_RETURN_NOT_OK(ReadChecked<uint32_t>(data, &extension_length, error));

    if (data->size_bytes < static_cast<int64_t>(extension_length)) {
      ArrowErrorSet(error,
                    "Expected %ld bytes of extension metadata at start of stream but "
                    "found %ld bytes of input",
                    static_cast<long>(extension_length),
                    static_cast<long>(data->size_bytes));  // NOLINT(runtime/int)
      return EINVAL;
    }

    data->data.as_uint8 += extension_length;
    data->size_bytes -= extension_length;
    return NANOARROW_OK;
  }

  ArrowErrorCode ReadRecord(ArrowBufferView* data, ArrowError* error) {
    if (array_->release == nullptr) {
      NANOARROW_RETURN_NOT_OK(
          ArrowArrayInitFromSchema(array_.get(), schema_.get(), error));
      NANOARROW_RETURN_NOT_OK(root_reader_.InitArray(array_.get()));
    }

    NANOARROW_RETURN_NOT_OK(root_reader_.Read(data, -1, array_.get(), error));
    return NANOARROW_OK;
  }

  ArrowErrorCode GetSchema(ArrowSchema* out) {
    return ArrowSchemaDeepCopy(schema_.get(), out);
  }

  void GetArray(ArrowArray* out) { ArrowArrayMove(array_.get(), out); }

 private:
  PostgresCopyRecordFieldReader root_reader_;
  nanoarrow::UniqueSchema schema_;
  nanoarrow::UniqueArray array_;
};

}  // namespace adbcpq
