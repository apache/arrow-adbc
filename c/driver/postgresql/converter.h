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
#include <vector>

#include <nanoarrow/nanoarrow.h>

#include "type.h"
#include "util.h"

namespace adbcpq {

class PostgresType {
 public:
  // As listed on https://www.postgresql.org/docs/current/datatype.html
  enum PgTypeId {
    PG_TYPE_UNINITIALIZED,
    PG_TYPE_BIGINT,
    PG_TYPE_BIGSERIAL,
    PG_TYPE_BIT,
    PG_TYPE_BIT_VARYING,
    PG_TYPE_BOOLEAN,
    PG_TYPE_BOX,
    PG_TYPE_BYTEA,
    PG_TYPE_CHARACTER,
    PG_TYPE_CHARACTER_VARYING,
    PG_TYPE_CIDR,
    PG_TYPE_CIRCLE,
    PG_TYPE_DATE,
    PG_TYPE_DOUBLE_PRECISION,
    PG_TYPE_INET,
    PG_TYPE_INTEGER,
    PG_TYPE_INTERVAL,
    PG_TYPE_JSON,
    PG_TYPE_JSONB,
    PG_TYPE_LINE,
    PG_TYPE_LSEG,
    PG_TYPE_MACADDR,
    PG_TYPE_MACADDR8,
    PG_TYPE_MONEY,
    PG_TYPE_NUMERIC,
    PG_TYPE_PATH,
    PG_TYPE_PG_LSN,
    PG_TYPE_PG_SNAPSHOT,
    PG_TYPE_POINT,
    PG_TYPE_POLYGON,
    PG_TYPE_REAL,
    PG_TYPE_SMALLINT,
    PG_TYPE_SMALLSERIAL,
    PG_TYPE_SERIAL,
    PG_TYPE_TEXT,
    PG_TYPE_TIME,
    PG_TYPE_TIMESTAMP,
    PG_TYPE_TSQUERY,
    PG_TYPE_TSVECTOR,
    PG_TYPE_TXID_SNAPSHOT,
    PG_TYPE_UUID,
    PG_TYPE_XML,

    PG_TYPE_ARRAY,
    PG_TYPE_COMPOSITE,
    PG_TYPE_RANGE
  };

  PostgresType(PgTypeId id, PgTypeId storage_id)
      : id_(id), storage_id_(storage_id), n_(-1), precision_(-1), scale_(-1) {}

  explicit PostgresType(PgTypeId id) : PostgresType(id, id) {}

  PgTypeId id() const { return id_; }
  PgTypeId storage_id() const { return storage_id_; }
  const std::string& name() const { return name_; }
  int32_t n() const { return n_; }
  int32_t precision() const { return precision_; }
  int32_t scale() const { return scale_; }
  const std::string& timezone() const { return timezone_; }
  int64_t n_children() const { return static_cast<int64_t>(children_.size()); }
  const PostgresType* child(int64_t i) const { return children_[i].get(); }

 private:
  PgTypeId id_;
  PgTypeId storage_id_;
  std::string name_;
  int32_t n_;
  int32_t precision_;
  int32_t scale_;
  std::string timezone_;
  std::vector<std::unique_ptr<PostgresType>> children_;

 public:
  PostgresType BigInt() { return PostgresType(PG_TYPE_BIGINT); }
  PostgresType BigSerial() { return PostgresType(PG_TYPE_BIGSERIAL, PG_TYPE_BIGINT); }
  PostgresType Bit(int32_t n) {
    PostgresType out(PG_TYPE_BIT, PG_TYPE_TEXT);
    out.n_ = n;
    return out;
  }
  PostgresType BitVarying(int32_t n) {
    PostgresType out(PG_TYPE_BIT_VARYING, PG_TYPE_TEXT);
    out.n_ = n;
    return out;
  }
  PostgresType Boolean() { return PostgresType(PG_TYPE_BOOLEAN); }
  PostgresType Bytea() { return PostgresType(PG_TYPE_BYTEA); }
  PostgresType Character(int32_t n) {
    PostgresType out(PG_TYPE_CHARACTER, PG_TYPE_TEXT);
    out.n_ = n;
    return out;
  }
  PostgresType CharacterVarying(int32_t n) {
    PostgresType out(PG_TYPE_CHARACTER_VARYING, PG_TYPE_TEXT);
    out.n_ = n;
    return out;
  }
  PostgresType Date() { return PostgresType(PG_TYPE_DATE, PG_TYPE_INTEGER); }
  PostgresType DoublePrecision() { return PostgresType(PG_TYPE_DOUBLE_PRECISION); }
  PostgresType Integer() { return PostgresType(PG_TYPE_INTEGER); }
  PostgresType Numeric(int32_t precision, int32_t scale) {
    PostgresType out(PG_TYPE_NUMERIC);
    out.precision_ = precision;
    out.scale_ = scale;
    return out;
  }
  PostgresType Real() { return PostgresType(PG_TYPE_REAL); }
  PostgresType SmallInt() { return PostgresType(PG_TYPE_SMALLINT); }
  PostgresType SmallSerial() {
    return PostgresType(PG_TYPE_SMALLSERIAL, PG_TYPE_SMALLINT);
  }
  PostgresType Serial() { return PostgresType(PG_TYPE_SERIAL, PG_TYPE_INTEGER); }
  PostgresType Text() { return PostgresType(PG_TYPE_TEXT); }
  PostgresType Time(const std::string& timezone = "") {
    PostgresType out(PG_TYPE_TIME);
    out.timezone_ = timezone;
    if (timezone == "") {
      out.storage_id_ = PG_TYPE_BIGINT;
    }
    return out;
  }
  PostgresType Timestamp(const std::string& timezone = "") {
    PostgresType out(PG_TYPE_TIMESTAMP, PG_TYPE_BIGINT);
    out.timezone_ = timezone;
    return out;
  }
};

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
    BufferToHostEndian(data_->data, data_->size_bytes, bitwidth_);
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
