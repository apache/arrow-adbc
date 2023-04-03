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

class PostgresField {
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
    PG_TYPE_UUID,
    PG_TYPE_XML,
    PG_TYPE_DOMAIN,

    PG_TYPE_ARRAY,
    PG_TYPE_COMPOSITE,
    PG_TYPE_RANGE
  };

  PostgresField(PgTypeId id, PgTypeId storage_id)
      : id_(id), storage_id_(storage_id), n_(-1), precision_(-1), scale_(-1) {}

  explicit PostgresField(PgTypeId id) : PostgresField(id, id) {}

  const std::string& field_name() const { return field_name_; }
  PgTypeId id() const { return id_; }
  PgTypeId storage_id() const { return storage_id_; }
  int32_t n() const { return n_; }
  int32_t precision() const { return precision_; }
  int32_t scale() const { return scale_; }
  const std::string& timezone() const { return timezone_; }
  int64_t n_children() const { return static_cast<int64_t>(children_.size()); }
  const PostgresField* child(int64_t i) const { return children_[i].get(); }

  std::string type_name() const {
    // e.g., some user-created type
    if (type_name_ != "") {
      return type_name_;
    }

    switch (id_) {
      case PG_TYPE_BIGINT:
        return "bigint";
      case PG_TYPE_BIGSERIAL:
        return "bigserial";
      case PG_TYPE_BIT:
        return "bit";
      case PG_TYPE_BIT_VARYING:
        return "bit varying";
      case PG_TYPE_BOOLEAN:
        return "boolean";
      case PG_TYPE_BOX:
        return "box";
      case PG_TYPE_BYTEA:
        return "bytea";
      case PG_TYPE_CHARACTER:
        return "character";
      case PG_TYPE_CHARACTER_VARYING:
        return "character varying";
      case PG_TYPE_CIDR:
        return "cidr";
      case PG_TYPE_CIRCLE:
        return "circle";
      case PG_TYPE_DATE:
        return "date";
      case PG_TYPE_DOUBLE_PRECISION:
        return "double precision";
      case PG_TYPE_INET:
        return "inet";
      case PG_TYPE_INTEGER:
        return "integer";
      case PG_TYPE_INTERVAL:
        return "interval";
      case PG_TYPE_JSON:
        return "json";
      case PG_TYPE_JSONB:
        return "jsonb";
      case PG_TYPE_LINE:
        return "line";
      case PG_TYPE_LSEG:
        return "lseg";
      case PG_TYPE_MACADDR:
        return "macaddr";
      case PG_TYPE_MACADDR8:
        return "macaddr8";
      case PG_TYPE_MONEY:
        return "money";
      case PG_TYPE_NUMERIC:
        return "numeric";
      case PG_TYPE_PATH:
        return "path";
      case PG_TYPE_PG_LSN:
        return "pg_lsn";
      case PG_TYPE_PG_SNAPSHOT:
        return "pg_snapshot";
      case PG_TYPE_POINT:
        return "point";
      case PG_TYPE_POLYGON:
        return "polygon";
      case PG_TYPE_REAL:
        return "real";
      case PG_TYPE_SMALLINT:
        return "smallint";
      case PG_TYPE_SMALLSERIAL:
        return "smallserial";
      case PG_TYPE_SERIAL:
        return "serial";
      case PG_TYPE_TEXT:
        return "text";
      case PG_TYPE_TIME:
        return "time";
      case PG_TYPE_TIMESTAMP:
        return "timestamp";
      case PG_TYPE_TSQUERY:
        return "tsquery";
      case PG_TYPE_TSVECTOR:
        return "tsvetor";
      case PG_TYPE_UUID:
        return "uuid";
      case PG_TYPE_XML:
        return "xml";

      case PG_TYPE_ARRAY:
        return "array";
      case PG_TYPE_COMPOSITE:
        return "composite";
      case PG_TYPE_RANGE:
        return "range";
      default:
        return "";
    }
  }

 private:
  std::string field_name_;
  PgTypeId id_;
  PgTypeId storage_id_;
  std::string type_name_;
  int32_t n_;
  int32_t precision_;
  int32_t scale_;
  std::string timezone_;
  std::vector<std::unique_ptr<PostgresField>> children_;

 public:
  static PostgresField BigInt() { return PostgresField(PG_TYPE_BIGINT); }
  static PostgresField BigSerial() {
    return PostgresField(PG_TYPE_BIGSERIAL, PG_TYPE_BIGINT);
  }
  static PostgresField Bit(int32_t n) {
    PostgresField out(PG_TYPE_BIT, PG_TYPE_TEXT);
    out.n_ = n;
    return out;
  }
  static PostgresField BitVarying(int32_t n) {
    PostgresField out(PG_TYPE_BIT_VARYING, PG_TYPE_TEXT);
    out.n_ = n;
    return out;
  }
  static PostgresField Boolean() { return PostgresField(PG_TYPE_BOOLEAN); }
  static PostgresField Bytea() { return PostgresField(PG_TYPE_BYTEA); }
  static PostgresField Character(int32_t n) {
    PostgresField out(PG_TYPE_CHARACTER, PG_TYPE_TEXT);
    out.n_ = n;
    return out;
  }
  static PostgresField CharacterVarying(int32_t n) {
    PostgresField out(PG_TYPE_CHARACTER_VARYING, PG_TYPE_TEXT);
    out.n_ = n;
    return out;
  }
  static PostgresField Date() { return PostgresField(PG_TYPE_DATE, PG_TYPE_INTEGER); }
  static PostgresField DoublePrecision() {
    return PostgresField(PG_TYPE_DOUBLE_PRECISION);
  }
  static PostgresField Integer() { return PostgresField(PG_TYPE_INTEGER); }
  static PostgresField Numeric(int32_t precision, int32_t scale) {
    PostgresField out(PG_TYPE_NUMERIC);
    out.precision_ = precision;
    out.scale_ = scale;
    return out;
  }
  static PostgresField Real() { return PostgresField(PG_TYPE_REAL); }
  static PostgresField SmallInt() { return PostgresField(PG_TYPE_SMALLINT); }
  static PostgresField SmallSerial() {
    return PostgresField(PG_TYPE_SMALLSERIAL, PG_TYPE_SMALLINT);
  }
  static PostgresField Serial() { return PostgresField(PG_TYPE_SERIAL, PG_TYPE_INTEGER); }
  static PostgresField Text() { return PostgresField(PG_TYPE_TEXT); }
  static PostgresField Time(const std::string& timezone = "") {
    PostgresField out(PG_TYPE_TIME);
    out.timezone_ = timezone;
    if (timezone == "") {
      out.storage_id_ = PG_TYPE_BIGINT;
    }
    return out;
  }
  static PostgresField Timestamp(const std::string& timezone = "") {
    PostgresField out(PG_TYPE_TIMESTAMP, PG_TYPE_BIGINT);
    out.timezone_ = timezone;
    return out;
  }

  static PostgresField Array(PostgresField& child) {
    PostgresField out(PG_TYPE_ARRAY);
    std::unique_ptr<PostgresField> child_ptr(new PostgresField(std::move(child)));
    out.children_.push_back(std::move(child_ptr));
    return out;
  }

  static PostgresField Composite(std::vector<std::unique_ptr<PostgresField>> children) {
    PostgresField out(PG_TYPE_ARRAY);
    out.children_ = std::move(children);
    return out;
  }

  static PostgresField Range(PostgresField& child) {
    PostgresField out(PG_TYPE_RANGE);
    std::unique_ptr<PostgresField> child_ptr(new PostgresField(std::move(child)));
    out.children_.push_back(std::move(child_ptr));
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
