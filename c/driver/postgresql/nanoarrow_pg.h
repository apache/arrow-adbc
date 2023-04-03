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
#include <unordered_map>
#include <vector>

#include <nanoarrow/nanoarrow.hpp>

#include "type.h"
#include "util.h"

namespace adbcpq {

class PostgresType {
 public:
  // From SELECT DISTINCT typreceive FROM pg_type;
  enum PgRecv {
    PG_RECV_UNINITIALIZED,
    PG_RECV_ANYARRAY,
    PG_RECV_ANYCOMPATIBLEARRAY,
    PG_RECV_ARRAY,
    PG_RECV_BIT,
    PG_RECV_BOOL,
    PG_RECV_BOX,
    PG_RECV_BPCHAR,
    PG_RECV_BRIN_BLOOM_SUMMARY,
    PG_RECV_BRIN_MINMAX_MULTI_SUMMARY,
    PG_RECV_BYTEA,
    PG_RECV_CASH,
    PG_RECV_CHAR,
    PG_RECV_CIDR,
    PG_RECV_CID,
    PG_RECV_CIRCLE,
    PG_RECV_CSTRING,
    PG_RECV_DATE,
    PG_RECV_DOMAIN,
    PG_RECV_FLOAT4,
    PG_RECV_FLOAT8,
    PG_RECV_INET,
    PG_RECV_INT2,
    PG_RECV_INT2VECTOR,
    PG_RECV_INT4,
    PG_RECV_INT8,
    PG_RECV_INTERVAL,
    PG_RECV_JSON,
    PG_RECV_JSONB,
    PG_RECV_JSONPATH,
    PG_RECV_LINE,
    PG_RECV_LSEG,
    PG_RECV_MACADDR,
    PG_RECV_MACADDR8,
    PG_RECV_MULTIRANGE,
    PG_RECV_NAME,
    PG_RECV_NUMERIC,
    PG_RECV_OID,
    PG_RECV_OIDVECTOR,
    PG_RECV_PATH,
    PG_RECV_PG_DDL_COMMAND,
    PG_RECV_PG_DEPENDENCIES,
    PG_RECV_PG_LSN,
    PG_RECV_PG_MCV_LIST,
    PG_RECV_PG_NDISTINCT,
    PG_RECV_PG_NODE_TREE,
    PG_RECV_PG_SNAPSHOT,
    PG_RECV_POINT,
    PG_RECV_POLY,
    PG_RECV_RANGE,
    PG_RECV_RECORD,
    PG_RECV_REGCLASS,
    PG_RECV_REGCOLLATION,
    PG_RECV_REGCONFIG,
    PG_RECV_REGDICTIONARY,
    PG_RECV_REGNAMESPACE,
    PG_RECV_REGOPERATOR,
    PG_RECV_REGOPER,
    PG_RECV_REGPROCEDURE,
    PG_RECV_REGPROC,
    PG_RECV_REGROLE,
    PG_RECV_REGTYPE,
    PG_RECV_TEXT,
    PG_RECV_TID,
    PG_RECV_TIME,
    PG_RECV_TIMESTAMP,
    PG_RECV_TIMESTAMPTZ,
    PG_RECV_TIMETZ,
    PG_RECV_TSQUERY,
    PG_RECV_TSVECTOR,
    PG_RECV_TXID_SNAPSHOT,
    PG_RECV_UNKNOWN,
    PG_RECV_UUID,
    PG_RECV_VARBIT,
    PG_RECV_VARCHAR,
    PG_RECV_VOID,
    PG_RECV_XID8,
    PG_RECV_XID,
    PG_RECV_XML
  };

  static std::vector<PgRecv> PgRecvAllBase() {
    return {PG_RECV_BIT,         PG_RECV_BOOL,   PG_RECV_BYTEA,    PG_RECV_CASH,
            PG_RECV_CHAR,        PG_RECV_DATE,   PG_RECV_FLOAT4,   PG_RECV_FLOAT8,
            PG_RECV_INT4,        PG_RECV_INT8,   PG_RECV_INTERVAL, PG_RECV_NUMERIC,
            PG_RECV_OID,         PG_RECV_TEXT,   PG_RECV_TIME,     PG_RECV_TIMESTAMP,
            PG_RECV_TIMESTAMPTZ, PG_RECV_TIMETZ, PG_RECV_UUID,     PG_RECV_VARBIT,
            PG_RECV_VARCHAR,     PG_RECV_ARRAY,  PG_RECV_RECORD,   PG_RECV_RANGE,
            PG_RECV_DOMAIN};
  }

  static std::string PgRecvName(PgRecv recv) {
    switch (recv) {
      case PG_RECV_ANYARRAY:
        return "anyarray_recv";
      case PG_RECV_ANYCOMPATIBLEARRAY:
        return "anycompatiblearray_recv";
      case PG_RECV_ARRAY:
        return "array_recv";
      case PG_RECV_BIT:
        return "bit_recv";
      case PG_RECV_BOOL:
        return "boolrecv";
      case PG_RECV_BOX:
        return "box_recv";
      case PG_RECV_BPCHAR:
        return "bpcharrecv";
      case PG_RECV_BRIN_BLOOM_SUMMARY:
        return "brin_bloom_summary_recv";
      case PG_RECV_BRIN_MINMAX_MULTI_SUMMARY:
        return "brin_minmax_multi_summary_recv";
      case PG_RECV_BYTEA:
        return "bytearecv";
      case PG_RECV_CASH:
        return "cash_recv";
      case PG_RECV_CHAR:
        return "charrecv";
      case PG_RECV_CIDR:
        return "cidr_recv";
      case PG_RECV_CID:
        return "cidrecv";
      case PG_RECV_CIRCLE:
        return "circle_recv";
      case PG_RECV_CSTRING:
        return "cstring_recv";
      case PG_RECV_DATE:
        return "date_recv";
      case PG_RECV_DOMAIN:
        return "domain_recv";
      case PG_RECV_FLOAT4:
        return "float4recv";
      case PG_RECV_FLOAT8:
        return "float8recv";
      case PG_RECV_INET:
        return "inet_recv";
      case PG_RECV_INT2:
        return "int2recv";
      case PG_RECV_INT2VECTOR:
        return "int2vectorrecv";
      case PG_RECV_INT4:
        return "int4recv";
      case PG_RECV_INT8:
        return "int8recv";
      case PG_RECV_INTERVAL:
        return "interval_recv";
      case PG_RECV_JSON:
        return "json_recv";
      case PG_RECV_JSONB:
        return "jsonb_recv";
      case PG_RECV_JSONPATH:
        return "jsonpath_recv";
      case PG_RECV_LINE:
        return "line_recv";
      case PG_RECV_LSEG:
        return "lseg_recv";
      case PG_RECV_MACADDR:
        return "macaddr_recv";
      case PG_RECV_MACADDR8:
        return "macaddr8_recv";
      case PG_RECV_MULTIRANGE:
        return "multirange_recv";
      case PG_RECV_NAME:
        return "namerecv";
      case PG_RECV_NUMERIC:
        return "numeric_recv";
      case PG_RECV_OID:
        return "oidrecv";
      case PG_RECV_OIDVECTOR:
        return "oidvectorrecv";
      case PG_RECV_PATH:
        return "path_recv";
      case PG_RECV_PG_DDL_COMMAND:
        return "pg_ddl_command_recv";
      case PG_RECV_PG_DEPENDENCIES:
        return "pg_dependencies_recv";
      case PG_RECV_PG_LSN:
        return "pg_lsn_recv";
      case PG_RECV_PG_MCV_LIST:
        return "pg_mcv_list_recv";
      case PG_RECV_PG_NDISTINCT:
        return "pg_ndistinct_recv";
      case PG_RECV_PG_NODE_TREE:
        return "pg_node_tree_recv";
      case PG_RECV_PG_SNAPSHOT:
        return "pg_snapshot_recv";
      case PG_RECV_POINT:
        return "point_recv";
      case PG_RECV_POLY:
        return "poly_recv";
      case PG_RECV_RANGE:
        return "range_recv";
      case PG_RECV_RECORD:
        return "record_recv";
      case PG_RECV_REGCLASS:
        return "regclassrecv";
      case PG_RECV_REGCOLLATION:
        return "regcollationrecv";
      case PG_RECV_REGCONFIG:
        return "regconfigrecv";
      case PG_RECV_REGDICTIONARY:
        return "regdictionaryrecv";
      case PG_RECV_REGNAMESPACE:
        return "regnamespacerecv";
      case PG_RECV_REGOPERATOR:
        return "regoperatorrecv";
      case PG_RECV_REGOPER:
        return "regoperrecv";
      case PG_RECV_REGPROCEDURE:
        return "regprocedurerecv";
      case PG_RECV_REGPROC:
        return "regprocrecv";
      case PG_RECV_REGROLE:
        return "regrolerecv";
      case PG_RECV_REGTYPE:
        return "regtyperecv";
      case PG_RECV_TEXT:
        return "textrecv";
      case PG_RECV_TID:
        return "tidrecv";
      case PG_RECV_TIME:
        return "time_recv";
      case PG_RECV_TIMESTAMP:
        return "timestamp_recv";
      case PG_RECV_TIMESTAMPTZ:
        return "timestamptz_recv";
      case PG_RECV_TIMETZ:
        return "timetz_recv";
      case PG_RECV_TSQUERY:
        return "tsqueryrecv";
      case PG_RECV_TSVECTOR:
        return "tsvectorrecv";
      case PG_RECV_TXID_SNAPSHOT:
        return "txid_snapshot_recv";
      case PG_RECV_UNKNOWN:
        return "unknownrecv";
      case PG_RECV_UUID:
        return "uuid_recv";
      case PG_RECV_VARBIT:
        return "varbit_recv";
      case PG_RECV_VARCHAR:
        return "varcharrecv";
      case PG_RECV_VOID:
        return "void_recv";
      case PG_RECV_XID8:
        return "xid8recv";
      case PG_RECV_XID:
        return "xidrecv";
      case PG_RECV_XML:
        return "xml_recv";
      default:
        return "";
    }
  }

  static std::string PgRecvTypname(PgRecv recv) {
    switch (recv) {
      case PG_RECV_BIT:
        return "bit";
      case PG_RECV_BOOL:
        return "bool";
      case PG_RECV_BYTEA:
        return "bytea";
      case PG_RECV_CASH:
        return "cash";
      case PG_RECV_CHAR:
        return "char";
      case PG_RECV_DATE:
        return "date";
      case PG_RECV_FLOAT4:
        return "float4";
      case PG_RECV_FLOAT8:
        return "float8";
      case PG_RECV_INT2:
        return "int2";
      case PG_RECV_INT4:
        return "int4";
      case PG_RECV_INT8:
        return "int8";
      case PG_RECV_INTERVAL:
        return "interval";
      case PG_RECV_NUMERIC:
        return "numeric";
      case PG_RECV_OID:
        return "oid";
      case PG_RECV_TEXT:
        return "text";
      case PG_RECV_TIME:
        return "time";
      case PG_RECV_TIMESTAMP:
        return "timestamp";
      case PG_RECV_TIMESTAMPTZ:
        return "timestamptz";
      case PG_RECV_TIMETZ:
        return "timetz";
      case PG_RECV_UUID:
        return "uuid";
      case PG_RECV_VARBIT:
        return "varbit";
      case PG_RECV_VARCHAR:
        return "varchar";

      case PG_RECV_ARRAY:
        return "array";
      case PG_RECV_RECORD:
        return "record";
      case PG_RECV_RANGE:
        return "range";
      case PG_RECV_DOMAIN:
        return "domain";
      default:
        return "";
    }
  }

  PostgresType(PgRecv recv) : oid_(0), recv_(recv) {}

  PostgresType() : PostgresType(PG_RECV_UNINITIALIZED) {}

  void AddRecordChild(const std::string& field_name, const PostgresType& type) {
    PostgresType child(type);
    children_.push_back(child.WithFieldName(field_name));
  }

  PostgresType WithFieldName(const std::string& field_name) const {
    PostgresType out(*this);
    out.field_name_ = field_name;
    return out;
  }

  PostgresType WithPgTypeInfo(uint32_t oid, const std::string& typname) const {
    PostgresType out(*this);
    out.oid_ = oid;
    out.typname_ = typname;
    return out;
  }

  PostgresType Array(uint32_t oid = 0, const std::string& typname = "") const {
    PostgresType out(PG_RECV_ARRAY);
    out.children_.push_back(WithFieldName("item"));
    out.oid_ = oid;
    out.typname_ = typname;
    return out;
  }

  PostgresType Domain(uint32_t oid, const std::string& typname) {
    return WithPgTypeInfo(oid, typname);
  }

  PostgresType Range(uint32_t oid = 0, const std::string& typname = "") const {
    PostgresType out(PG_RECV_RANGE);
    out.children_.push_back(WithFieldName("item"));
    out.oid_ = oid;
    out.typname_ = typname;
    return out;
  }

  uint32_t oid() const { return oid_; }
  PgRecv recv() const { return recv_; }
  const std::string& typname() { return typname_; }
  const std::string& field_name() const { return field_name_; }
  const int64_t n_children() const { return static_cast<int64_t>(children_.size()); }
  const PostgresType* child(int64_t i) const { return &children_[i]; }

  ArrowErrorCode SetSchema(ArrowSchema* schema) const {
    switch (recv_) {
      case PG_RECV_BOOL:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BOOL));
        break;
      case PG_RECV_INT2:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT16));
        break;
      case PG_RECV_INT4:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT32));
        break;
      case PG_RECV_INT8:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT64));
        break;
      case PG_RECV_FLOAT4:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_FLOAT));
        break;
      case PG_RECV_FLOAT8:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_DOUBLE));
        break;
      case PG_RECV_CHAR:
      case PG_RECV_VARCHAR:
      case PG_RECV_TEXT:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING));
        break;
      case PG_RECV_BYTEA:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
        break;

      case PG_RECV_RECORD:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(schema, n_children()));
        for (int64_t i = 0; i < n_children(); i++) {
          NANOARROW_RETURN_NOT_OK(children_[i].SetSchema(schema->children[i]));
        }
        break;

      case PG_RECV_ARRAY:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_LIST));
        NANOARROW_RETURN_NOT_OK(children_[0].SetSchema(schema->children[0]));
        break;
      default: {
        // For any types we don't explicitly know how to deal with, we can still
        // return the bytes postgres gives us and attach the type name as metadata
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
        nanoarrow::UniqueBuffer buffer;
        ArrowMetadataBuilderInit(buffer.get(), nullptr);
        NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderAppend(
            buffer.get(), ArrowCharView("ADBC:posgresql:typname"),
            ArrowCharView(typname_.c_str())));
        NANOARROW_RETURN_NOT_OK(
            ArrowSchemaSetMetadata(schema, reinterpret_cast<char*>(buffer->data)));
        break;
      }
    }

    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema, field_name_.c_str()));
    return NANOARROW_OK;
  }

 private:
  uint32_t oid_;
  PgRecv recv_;
  std::string typname_;
  std::string field_name_;
  std::vector<PostgresType> children_;

 public:
  static std::unordered_map<std::string, PostgresType> AllBase() {
    std::unordered_map<std::string, PostgresType> out;
    for (PgRecv recv : PgRecvAllBase()) {
      PostgresType type(recv);
      type.typname_ = PgRecvTypname(recv);
      out.insert({PgRecvName(recv), recv});
    }

    return out;
  }
};

class PostgresTypeResolver {
  struct Item {
    uint32_t oid;
    const char* typname;
    const char* typreceive;
    uint32_t child_oid;
    uint32_t base_oid;
    uint32_t class_oid;
  };

 public:
  PostgresTypeResolver() : base_(PostgresType::AllBase()) {}

  ArrowErrorCode Find(uint32_t oid, PostgresType* type_out, ArrowError* error) {
    auto result = mapping_.find(oid);
    if (result == mapping_.end()) {
      ArrowErrorSet(error, "Postgres type with oid %ld not found",
                    static_cast<long>(oid));
      return EINVAL;
    }

    *type_out = (*result).second;
    return NANOARROW_OK;
  }

  ArrowErrorCode Insert(const Item& item, ArrowError* error) {
    auto result = base_.find(item.typreceive);
    if (result == base_.end()) {
      ArrowErrorSet(error, "Base type not found for type '%s' with receive function '%s'",
                    item.typname, item.typreceive);
      return ENOTSUP;
    }

    const PostgresType& base = (*result).second;
    PostgresType type = base.WithPgTypeInfo(item.oid, item.typname);

    switch (base.recv()) {
      case PostgresType::PG_RECV_ARRAY: {
        PostgresType child;
        NANOARROW_RETURN_NOT_OK(Find(item.child_oid, &child, error));
        mapping_.insert({item.oid, child.Array(item.oid, item.typname)});
        break;
      }

      case PostgresType::PG_RECV_RECORD: {
        std::vector<std::pair<uint32_t, std::string>> child_desc;
        NANOARROW_RETURN_NOT_OK(ResolveClass(item.class_oid, &child_desc, error));

        PostgresType out(PostgresType::PG_RECV_RECORD);
        for (const auto& child_item : child_desc) {
          PostgresType child;
          NANOARROW_RETURN_NOT_OK(Find(child_item.first, &child, error));
          out.AddRecordChild(child_item.second, child);
        }

        mapping_.insert({item.oid, out});
        break;
      }

      case PostgresType::PG_RECV_DOMAIN: {
        PostgresType base_type;
        NANOARROW_RETURN_NOT_OK(Find(item.base_oid, &base_type, error));
        mapping_.insert({item.oid, base_type.Domain(item.oid, item.typname)});
        break;
      }

      case PostgresType::PG_RECV_RANGE: {
        PostgresType base_type;
        NANOARROW_RETURN_NOT_OK(Find(item.base_oid, &base_type, error));
        mapping_.insert({item.oid, base_type.Range(item.oid, item.typname)});
        break;
      }

      default:
        mapping_.insert({item.oid, type});
        break;
    }

    return NANOARROW_OK;
  }

  virtual ArrowErrorCode ResolveClass(uint32_t oid,
                                      std::vector<std::pair<uint32_t, std::string>>* out,
                                      ArrowError* error) {
    ArrowErrorSet(error, "Class definition with oid %ld not found",
                  static_cast<long>(oid));
    return EINVAL;
  }

 private:
  std::unordered_map<uint32_t, PostgresType> mapping_;
  std::unordered_map<std::string, PostgresType> base_;
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
