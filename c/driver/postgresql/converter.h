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
#include <cstring>

#include <nanoarrow/nanoarrow.h>

#include "type.h"

namespace adbcpq {

void BSwapArray(uint8_t* data, int64_t size_bytes, int32_t bitwidth) {
  switch (bitwidth) {
    case 1:
    case 8:
      break;
    case 16:
      break;
    case 32:
      break;
    case 64:
      break;
    default:
      break;
  }
}

class ArrowConverter {
 public:
  ArrowConverter(ArrowType type, PgType pg_type)
      : type_(type), pg_type_(pg_type), offsets_(nullptr), data_(nullptr) {
    memset(&schema_view_, 0, sizeof(ArrowSchemaView));
  }

  virtual ArrowErrorCode InitSchema(ArrowSchema* schema) {
    NANOARROW_RETURN_NOT_OK(ArrowSchemaInitFromType(schema, type_));
    NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view_, schema, nullptr));
    return NANOARROW_OK;
  }

  virtual ArrowErrorCode InitArray(ArrowArray* array, ArrowSchema* schema) {
    NANOARROW_RETURN_NOT_OK(ArrowArrayInitFromSchema(array, schema, nullptr));
    NANOARROW_RETURN_NOT_OK(ArrowArrayStartAppending(array));

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
                              ArrowError* error) = 0;

  virtual ArrowErrorCode FinishArray(ArrowArray* array, ArrowError* error) {
    return NANOARROW_OK;
  }

 protected:
  PgType pg_type_;
  ArrowType type_;
  ArrowSchemaView schema_view_;
  ArrowBuffer* offsets_;
  ArrowBuffer* large_offsets_;
  ArrowBuffer* data_;
};

// Converter for Pg->Arrow conversions whose representations are identical (minus
// the bswap from network endian). This includes all integral and float types.
class NumericArrowConverter : public ArrowConverter {
 public:
  NumericArrowConverter(ArrowType type, PgType pg_type)
      : ArrowConverter(type, pg_type), data_(nullptr) {}

  ArrowErrorCode InitSchema(ArrowSchema* schema) override {
    NANOARROW_RETURN_NOT_OK(ArrowConverter::InitSchema(schema));
    bitwidth_ = schema_view_.layout.element_size_bits[1];
    return NANOARROW_OK;
  }

  ArrowErrorCode InitArray(ArrowArray* array, ArrowSchema* schema) override {
    NANOARROW_RETURN_NOT_OK(ArrowConverter::InitArray(array, schema));
    data_ = ArrowArrayBuffer(array, 1);
    return NANOARROW_OK;
  }

  ArrowErrorCode Read(ArrowBufferView data, ArrowArray* array,
                      ArrowError* error) override {
    return ArrowBufferAppendBufferView(data_, data);
  }

  ArrowErrorCode FinishArray(ArrowArray* array, ArrowError* error) override {
    BSwapArray(data_->data, data_->size_bytes, bitwidth_);
    return NANOARROW_OK;
  }

 private:
  ArrowBuffer* data_;
  int32_t bitwidth_;
};

// Converter for Pg->Arrow conversions whose Arrow representation is simply the
// bytes of the field representation. This can be used with binary and string
// Arrow types and any postgres type.
class BinaryArrowConverter : public ArrowConverter {
 public:
  BinaryArrowConverter(ArrowType type, PgType pg_type)
      : ArrowConverter(type, pg_type), data_(nullptr) {}

  ArrowErrorCode InitArray(ArrowArray* array, ArrowSchema* schema) override {
    NANOARROW_RETURN_NOT_OK(ArrowConverter::InitArray(array, schema));
    offsets_ = ArrowArrayBuffer(array, 1);
    data_ = ArrowArrayBuffer(array, 2);
    return NANOARROW_OK;
  }

  ArrowErrorCode Read(ArrowBufferView data, ArrowArray* array,
                      ArrowError* error) override {
    if ((data_->size_bytes + data.size_bytes) > std::numeric_limits<int32_t>::max()) {
      return EOVERFLOW;
    }

    NANOARROW_RETURN_NOT_OK(ArrowBufferAppendBufferView(data_, data));
    NANOARROW_RETURN_NOT_OK(ArrowBufferAppendInt32(offsets_, (int32_t)data_->size_bytes));
    return NANOARROW_OK;
  }

 private:
  ArrowBuffer* offsets_;
  ArrowBuffer* data_;
  int32_t bitwidth_;
};

}  // namespace adbcpq
