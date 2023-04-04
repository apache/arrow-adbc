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
#include <string>
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
  if (data->size_bytes < sizeof(T)) {
    ArrowErrorSet(error, "Unexpected end of input (expected %d bytes but found %ld)",
                  (int)sizeof(T), (long)data->size_bytes);
    return EINVAL;
  }

  *out = ReadUnsafe<T>(data);
  return NANOARROW_OK;
}

class PostgresCopyReader {
 public:
  explicit PostgresCopyReader(const PostgresType& pg_type)
      : pg_type_(std::move(pg_type)), offsets_(nullptr), data_(nullptr) {
    memset(&schema_view_, 0, sizeof(ArrowSchemaView));
  }

  void AppendChild(PostgresCopyReader& child) { children_.push_back(std::move(child)); }

  const PostgresType& InputType() const { return pg_type_; }

  ArrowErrorCode InitSchema(ArrowSchema* schema) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view_, schema, nullptr));
    return NANOARROW_OK;
  }

  ArrowErrorCode InitArray(ArrowArray* array, ArrowSchema* schema) {
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array, schema, nullptr));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array));

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

  virtual ArrowErrorCode Read(ArrowBufferView data, ArrowArray* array,
                              ArrowError* error) {
    return ENOTSUP;
  }

  ArrowErrorCode FinishArray(ArrowArray* array, ArrowError* error) {
    return NANOARROW_OK;
  }

 protected:
  PostgresType pg_type_;
  ArrowSchemaView schema_view_;
  ArrowBuffer* offsets_;
  ArrowBuffer* data_;
  std::vector<PostgresCopyReader> children_;
};

// Converter for a Postgres boolean (one byte -> bitmap)
class PostgresCopyReaderBool : public PostgresCopyReader {
 public:
  explicit PostgresCopyReaderBool(const PostgresType& pg_type)
      : PostgresCopyReader(pg_type) {}

  ArrowErrorCode Read(ArrowBufferView data, ArrowArray* array,
                      ArrowError* error) override {
    if (data.size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    int64_t bytes_required = _ArrowBytesForBits(array->length + 1);
    if (bytes_required > data_->size_bytes) {
      NANOARROW_RETURN_NOT_OK(
          ArrowBufferAppendFill(data_, 0, bytes_required - data_->size_bytes));
    }

    int8_t value;
    NANOARROW_RETURN_NOT_OK(ReadChecked<int8_t>(&data, &value, error));

    if (value) {
      ArrowBitSet(data_->data, array->length);
    } else {
      ArrowBitClear(data_->data, array->length);
    }

    array->length++;
    return NANOARROW_OK;
  }
};

// Converter for Pg->Arrow conversions whose representations are identical minus
// the bswap from network endian. This includes all integral and float types.
template <typename T>
class PostgresCopyReaderNetworkEndian : public PostgresCopyReader {
 public:
  explicit PostgresCopyReaderNetworkEndian(const PostgresType& pg_type)
      : PostgresCopyReader(pg_type) {}

  ArrowErrorCode Read(ArrowBufferView data, ArrowArray* array,
                      ArrowError* error) override {
    if (data.size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    T value_uint;
    NANOARROW_RETURN_NOT_OK(ReadChecked<T>(&data, &value_uint, error));
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(&value_uint, sizeof(T)));
    array->length++;
    return NANOARROW_OK;
  }
};

using PostgresCopyReaderNetworkEndian16 = PostgresCopyReaderNetworkEndian<uint16_t>;
using PostgresCopyReaderNetworkEndian32 = PostgresCopyReaderNetworkEndian<uint32_t>;
using PostgresCopyReaderNetworkEndian64 = PostgresCopyReaderNetworkEndian<uint64_t>;

// Converter for Pg->Arrow conversions whose Arrow representation is simply the
// bytes of the field representation. This can be used with binary and string
// Arrow types and any postgres type.
class PostgresCopyReaderBinary : public PostgresCopyReader {
 public:
  explicit PostgresCopyReaderBinary(const PostgresType& pg_type)
      : PostgresCopyReader(pg_type) {}

  ArrowErrorCode Read(ArrowBufferView data, ArrowArray* array,
                      ArrowError* error) override {
    if (data.size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    NANOARROW_RETURN_NOT_OK(ArrowBufferAppendBufferView(data_, data));
    int32_t* offsets = reinterpret_cast<int32_t*>(offsets_->data);
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppendInt32(
        offsets_, offsets[array->length] + static_cast<int32_t>(data_->size_bytes)));

    array->length++;
    return NANOARROW_OK;
  }
};

class PostgresCopyReaderList : public PostgresCopyReader {
 public:
  explicit PostgresCopyReaderList(const PostgresType& pg_type)
      : PostgresCopyReader(pg_type) {}

  ArrowErrorCode Read(ArrowBufferView data, ArrowArray* array,
                      ArrowError* error) override {
    if (data.size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    int32_t n_dim;
    NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(&data, &n_dim, error));
    int32_t flags;
    NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(&data, &flags, error));
    uint32_t element_type_oid;
    NANOARROW_RETURN_NOT_OK(ReadChecked<uint32_t>(&data, &element_type_oid, error));

    if (element_type_oid != children_[0].InputType().oid()) {
      ArrowErrorSet(error,
                    "Expected array child value with oid %ld but got array child value "
                    "with oid %ld",
                    static_cast<long>(children_[0].InputType().oid()),
                    static_cast<long>(element_type_oid));
      return EINVAL;
    }

    if (n_dim <= 0) {
      ArrowErrorSet(error, "Expected array n_dim > 0 but got %d",
                    static_cast<int>(n_dim));
      return EINVAL;
    }

    int64_t n_items = 1;
    for (int32_t i = 0; i < n_dim; i++) {
      int32_t dim_size;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(&data, &dim_size, error));
      n_items *= dim_size;

      int32_t lower_bound;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(&data, &lower_bound, error));
      if (lower_bound != 0) {
        ArrowErrorSet(error, "Array value with lower bound != 0 is not supported");
        return EINVAL;
      }
    }

    ArrowBufferView field_data;
    for (int64_t i = 0; i < n_items; i++) {
      int32_t field_length;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(&data, &field_length, error));
      field_data.data.as_uint8 = data.data.as_uint8;
      field_data.size_bytes = field_length;

      // Note: Read() here is a virtual method call
      int result = children_[0].Read(field_data, array->children[i], error);
      if (result == EOVERFLOW) {
        for (int16_t j = 0; j < i; j++) {
          array->children[j]->length--;
        }

        return result;
      }

      if (field_length > 0) {
        data.data.as_uint8 += field_length;
        data.size_bytes -= field_length;
      }
    }

    NANOARROW_RETURN_NOT_OK(ArrowArrayFinishElement(array));
    return NANOARROW_OK;
  }
};

class PostgresCopyReaderStruct : public PostgresCopyReader {
 public:
  explicit PostgresCopyReaderStruct(const PostgresType& pg_type)
      : PostgresCopyReader(pg_type) {}

  ArrowErrorCode Read(ArrowBufferView data, ArrowArray* array,
                      ArrowError* error) override {
    if (data.size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    int16_t n_fields;
    NANOARROW_RETURN_NOT_OK(ReadChecked<int16_t>(&data, &n_fields, error));
    if (n_fields < 0) {
      return ENODATA;
    }

    struct ArrowBufferView field_data;
    for (uint16_t i = 0; i < n_fields; i++) {
      int32_t field_length;
      NANOARROW_RETURN_NOT_OK(ReadChecked<int32_t>(&data, &field_length, error));
      field_data.data.as_uint8 = data.data.as_uint8;
      field_data.size_bytes = field_length;

      // Note: Read() here is a virtual method call
      int result = children_[i].Read(field_data, array->children[i], error);
      if (result == EOVERFLOW) {
        for (int16_t j = 0; j < i; i++) {
          array->children[j]->length--;
        }

        return result;
      }

      if (field_length > 0) {
        data.data.as_uint8 += field_length;
        data.size_bytes -= field_length;
      }
    }

    array->length++;
    return NANOARROW_OK;
  }
};

}  // namespace adbcpq