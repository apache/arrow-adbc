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
#include <unordered_map>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.hpp>

#include "util.h"

namespace adbcpq {

template <typename T>
T ReadUnsafe(ArrowBufferView* data) {}

class ArrowConverter {
 public:
  ArrowConverter() : offsets_(nullptr), data_(nullptr) {
    memset(&schema_view_, 0, sizeof(ArrowSchemaView));
  }

  void AppendChild(ArrowConverter& child) { children_.push_back(std::move(child)); }

  virtual ArrowErrorCode InitSchema(ArrowSchema* schema) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view_, schema, nullptr));
    return NANOARROW_OK;
  }

  virtual ArrowErrorCode InitArray(ArrowArray* array, ArrowSchema* schema) {
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

  virtual ArrowErrorCode FinishArray(ArrowArray* array, ArrowError* error) {
    return NANOARROW_OK;
  }

 protected:
  ArrowType type_;
  ArrowSchemaView schema_view_;
  ArrowBuffer* offsets_;
  ArrowBuffer* data_;
  std::vector<ArrowConverter> children_;
};

// Converter for a Postgres boolean (one byte -> bitmap)
class ArrowConverterBool : public ArrowConverter {
 public:
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

    if (data.data.as_uint8[0]) {
      ArrowBitSet(data_->data, array->length);
    } else {
      ArrowBitClear(data_->data, array->length);
    }

    array->length++;
    return NANOARROW_OK;
  }
};

// Converter for Pg->Arrow conversions whose representations are identical (minus
// the bswap from network endian). This includes all integral and float types.
template <typename uint_type>
class ArrowConverterNetworkEndian : public ArrowConverter {
 public:
  ArrowErrorCode Read(ArrowBufferView data, ArrowArray* array,
                      ArrowError* error) override {
    if (data.size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    uint_type value_uint;
    memcpy(&value_uint, data.data.data, sizeof(uint_type));
    value_uint = SwapNetworkToHost(value_uint);

    NANOARROW_RETURN_NOT_OK(ArrowBufferAppend(&value_uint, sizeof(value_uint)));
    array->length++;
    return NANOARROW_OK;
  }
};

// Converter for Pg->Arrow conversions whose Arrow representation is simply the
// bytes of the field representation. This can be used with binary and string
// Arrow types and any postgres type.
class ArrowConverterBinary : public ArrowConverter {
 public:
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

class ArrowConverterList : public ArrowConverter {
 public:
  ArrowConverterList(ArrowConverter& child) { AppendChild(child); }

  ArrowErrorCode Read(ArrowBufferView data, ArrowArray* array,
                      ArrowError* error) override {
    if (data.size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    // int32_t ndim
    // int32_t flags
    // uint32_t element_type_oid
    // (struct int32_t dim_size, int32_t dim_lower_bound)[ndim]
    // (struct int32_t item_size_bytes, uint8_t[item_size_bytes])[nitems]

    return ENOTSUP;
  }
};

class ArrowConverterStruct : public ArrowConverter {
 public:
  ArrowErrorCode Read(ArrowBufferView data, ArrowArray* array,
                      ArrowError* error) override {
    if (data.size_bytes <= 0) {
      return ArrowArrayAppendNull(array, 1);
    }

    uint16_t n_fields = LoadNetworkInt16(data.data.as_char);
    data.data.as_char += sizeof(uint16_t);
    data.size_bytes -= sizeof(uint16_t);

    struct ArrowBufferView field_data;
    for (uint16_t i = 0; i < n_fields; i++) {
      field_data.size_bytes = LoadNetworkInt32(data.data.as_char) - sizeof(int32_t);
      data.data.as_char += sizeof(int32_t);
      data.size_bytes -= sizeof(int32_t);
      field_data.data.as_char = data.data.as_char;

      int result = children_[i].Read(field_data, array->children[i], error);
      if (result == EOVERFLOW) {
        for (int16_t j = 0; j < i; i++) {
          array->children[j]->length--;
        }

        return result;
      }

      data.data.as_char += field_data.size_bytes;
    }

    array->length++;
    return NANOARROW_OK;
  }
};

}  // namespace adbcpq