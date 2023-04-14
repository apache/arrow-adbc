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

namespace adbcpq {

// An enum of the types available in most Postgres pg_type tables
enum PostgresTypeId {
  PG_TYPE_UNINITIALIZED,
  PG_TYPE_ACLITEM,
  PG_TYPE_ANYARRAY,
  PG_TYPE_ANYCOMPATIBLEARRAY,
  PG_TYPE_ARRAY,
  PG_TYPE_BIT,
  PG_TYPE_BOOL,
  PG_TYPE_BOX,
  PG_TYPE_BPCHAR,
  PG_TYPE_BRIN_BLOOM_SUMMARY,
  PG_TYPE_BRIN_MINMAX_MULTI_SUMMARY,
  PG_TYPE_BYTEA,
  PG_TYPE_CASH,
  PG_TYPE_CHAR,
  PG_TYPE_CIDR,
  PG_TYPE_CID,
  PG_TYPE_CIRCLE,
  PG_TYPE_CSTRING,
  PG_TYPE_DATE,
  PG_TYPE_DOMAIN,
  PG_TYPE_FLOAT4,
  PG_TYPE_FLOAT8,
  PG_TYPE_INET,
  PG_TYPE_INT2,
  PG_TYPE_INT2VECTOR,
  PG_TYPE_INT4,
  PG_TYPE_INT8,
  PG_TYPE_INTERVAL,
  PG_TYPE_JSON,
  PG_TYPE_JSONB,
  PG_TYPE_JSONPATH,
  PG_TYPE_LINE,
  PG_TYPE_LSEG,
  PG_TYPE_MACADDR,
  PG_TYPE_MACADDR8,
  PG_TYPE_MULTIRANGE,
  PG_TYPE_NAME,
  PG_TYPE_NUMERIC,
  PG_TYPE_OID,
  PG_TYPE_OIDVECTOR,
  PG_TYPE_PATH,
  PG_TYPE_PG_DDL_COMMAND,
  PG_TYPE_PG_DEPENDENCIES,
  PG_TYPE_PG_LSN,
  PG_TYPE_PG_MCV_LIST,
  PG_TYPE_PG_NDISTINCT,
  PG_TYPE_PG_NODE_TREE,
  PG_TYPE_PG_SNAPSHOT,
  PG_TYPE_POINT,
  PG_TYPE_POLY,
  PG_TYPE_RANGE,
  PG_TYPE_RECORD,
  PG_TYPE_REGCLASS,
  PG_TYPE_REGCOLLATION,
  PG_TYPE_REGCONFIG,
  PG_TYPE_REGDICTIONARY,
  PG_TYPE_REGNAMESPACE,
  PG_TYPE_REGOPERATOR,
  PG_TYPE_REGOPER,
  PG_TYPE_REGPROCEDURE,
  PG_TYPE_REGPROC,
  PG_TYPE_REGROLE,
  PG_TYPE_REGTYPE,
  PG_TYPE_TEXT,
  PG_TYPE_TID,
  PG_TYPE_TIME,
  PG_TYPE_TIMESTAMP,
  PG_TYPE_TIMESTAMPTZ,
  PG_TYPE_TIMETZ,
  PG_TYPE_TSQUERY,
  PG_TYPE_TSVECTOR,
  PG_TYPE_TXID_SNAPSHOT,
  PG_TYPE_UNKNOWN,
  PG_TYPE_UUID,
  PG_TYPE_VARBIT,
  PG_TYPE_VARCHAR,
  PG_TYPE_VOID,
  PG_TYPE_XID8,
  PG_TYPE_XID,
  PG_TYPE_XML
};

// Returns the receive function name as defined in the typrecieve column
// of the pg_type table. This name is the one that gets used to look up
// the PostgresTypeId.
static inline const char* PostgresTyprecv(PostgresTypeId type_id);

// Returns a likely typname value for a given PostgresTypeId. This is useful
// for testing and error messages but may not be the actual value present
// in the pg_type typname column.
static inline const char* PostgresTypname(PostgresTypeId type_id);

// A vector of all type IDs, optionally including the nested types PG_TYPE_ARRAY,
// PG_TYPE_DOMAIN, PG_TYPE_RECORD, and PG_TYPE_RANGE.
static inline std::vector<PostgresTypeId> PostgresTypeIdAll(bool nested = true);

// An abstraction of a (potentially nested and/or parameterized) Postgres
// data type. This class is where default type conversion to/from Arrow
// is defined. It is intentionally copyable.
class PostgresType {
 public:
  explicit PostgresType(PostgresTypeId type_id) : oid_(0), type_id_(type_id) {}

  PostgresType() : PostgresType(PG_TYPE_UNINITIALIZED) {}

  void AppendChild(const std::string& field_name, const PostgresType& type) {
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
    PostgresType out(PG_TYPE_ARRAY);
    out.AppendChild("item", *this);
    out.oid_ = oid;
    out.typname_ = typname;
    return out;
  }

  PostgresType Domain(uint32_t oid, const std::string& typname) {
    return WithPgTypeInfo(oid, typname);
  }

  PostgresType Range(uint32_t oid = 0, const std::string& typname = "") const {
    PostgresType out(PG_TYPE_RANGE);
    out.AppendChild("item", *this);
    out.oid_ = oid;
    out.typname_ = typname;
    return out;
  }

  uint32_t oid() const { return oid_; }
  PostgresTypeId type_id() const { return type_id_; }
  const std::string& typname() const { return typname_; }
  const std::string& field_name() const { return field_name_; }
  int64_t n_children() const { return static_cast<int64_t>(children_.size()); }
  const PostgresType& child(int64_t i) const { return children_[i]; }

  // Sets appropriate fields of an ArrowSchema that has been initialized using
  // ArrowSchemaInit. This is a recursive operation (i.e., nested types will
  // initialize and set the appropriate number of children). Returns NANOARROW_OK
  // on success and perhaps ENOMEM if memory cannot be allocated. Types that
  // do not have a corresponding Arrow type are returned as Binary with field
  // metadata ADBC:posgresql:typname. These types can be represented as their
  // binary COPY representation in the output.
  ArrowErrorCode SetSchema(ArrowSchema* schema) const {
    switch (type_id_) {
      case PG_TYPE_BOOL:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BOOL));
        break;
      case PG_TYPE_INT2:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT16));
        break;
      case PG_TYPE_INT4:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT32));
        break;
      case PG_TYPE_INT8:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT64));
        break;
      case PG_TYPE_FLOAT4:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_FLOAT));
        break;
      case PG_TYPE_FLOAT8:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_DOUBLE));
        break;
      case PG_TYPE_CHAR:
      case PG_TYPE_BPCHAR:
      case PG_TYPE_VARCHAR:
      case PG_TYPE_TEXT:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING));
        break;
      case PG_TYPE_BYTEA:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
        break;

      case PG_TYPE_RECORD:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(schema, n_children()));
        for (int64_t i = 0; i < n_children(); i++) {
          NANOARROW_RETURN_NOT_OK(children_[i].SetSchema(schema->children[i]));
        }
        break;

      case PG_TYPE_ARRAY:
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
  PostgresTypeId type_id_;
  std::string typname_;
  std::string field_name_;
  std::vector<PostgresType> children_;
};

// Because type information is stored in a database's pg_type table, it can't
// truly be resolved until runtime; however, querying the database's pg_type table
// for every result is unlikely to be reasonable. This class is a cache of information
// from the pg_type table with appropriate lookup tables to resolve a PostgresType
// instance based on a oid (which is the information that libpq provides when
// inspecting a result object). Types can be added/removed from the pg_type table
// via SQL, so this cache may need to be periodically refreshed.
class PostgresTypeResolver {
 public:
  struct Item {
    uint32_t oid;
    const char* typname;
    const char* typreceive;
    uint32_t child_oid;
    uint32_t base_oid;
    uint32_t class_oid;
  };

  PostgresTypeResolver() : base_(AllBase()) {}

  // Place a resolved copy of a PostgresType with the appropriate oid in type_out
  // if NANOARROW_OK is returned or place a null-terminated error message into error
  // otherwise.
  ArrowErrorCode Find(uint32_t oid, PostgresType* type_out, ArrowError* error) const {
    auto result = mapping_.find(oid);
    if (result == mapping_.end()) {
      ArrowErrorSet(error, "Postgres type with oid %ld not found",
                    static_cast<long>(oid));  // NOLINT(runtime/int)
      return EINVAL;
    }

    *type_out = (*result).second;
    return NANOARROW_OK;
  }

  ArrowErrorCode FindArray(uint32_t child_oid, PostgresType* type_out,
                           ArrowError* error) const {
    auto array_oid_lookup = array_mapping_.find(child_oid);
    if (array_oid_lookup == array_mapping_.end()) {
      ArrowErrorSet(error, "Postgres array type with child oid %ld not found",
                    static_cast<long>(child_oid));  // NOLINT(runtime/int)
      return EINVAL;
    }

    return Find(array_oid_lookup->second, type_out, error);
  }

  // Resolve the oid for a given type_id. Returns 0 if the oid cannot be
  // resolved.
  uint32_t GetOID(PostgresTypeId type_id) const {
    auto result = reverse_mapping_.find(static_cast<int32_t>(type_id));
    if (result == reverse_mapping_.end()) {
      return 0;
    } else {
      return result->second;
    }
  }

  // Insert a type into this resolver. Returns NANOARROW_OK on success
  // or places a null-terminated error message into error otherwise. The order
  // of Inserts matters: Non-array types must be inserted before the corresponding
  // array types and class definitions must be inserted before the corresponding
  // class type using InsertClass().
  ArrowErrorCode Insert(const Item& item, ArrowError* error) {
    auto result = base_.find(item.typreceive);
    if (result == base_.end()) {
      ArrowErrorSet(error, "Base type not found for type '%s' with receive function '%s'",
                    item.typname, item.typreceive);
      return ENOTSUP;
    }

    const PostgresType& base = result->second;
    PostgresType type = base.WithPgTypeInfo(item.oid, item.typname);

    switch (base.type_id()) {
      case PG_TYPE_ARRAY: {
        PostgresType child;
        NANOARROW_RETURN_NOT_OK(Find(item.child_oid, &child, error));
        mapping_.insert({item.oid, child.Array(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        array_mapping_.insert({child.oid(), item.oid});
        break;
      }

      case PG_TYPE_RECORD: {
        std::vector<std::pair<std::string, uint32_t>> child_desc;
        NANOARROW_RETURN_NOT_OK(ResolveClass(item.class_oid, &child_desc, error));

        PostgresType out(PG_TYPE_RECORD);
        for (const auto& child_item : child_desc) {
          PostgresType child;
          NANOARROW_RETURN_NOT_OK(Find(child_item.second, &child, error));
          out.AppendChild(child_item.first, child);
        }

        mapping_.insert({item.oid, out.WithPgTypeInfo(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        break;
      }

      case PG_TYPE_DOMAIN: {
        PostgresType base_type;
        NANOARROW_RETURN_NOT_OK(Find(item.base_oid, &base_type, error));
        mapping_.insert({item.oid, base_type.Domain(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        break;
      }

      case PG_TYPE_RANGE: {
        PostgresType base_type;
        NANOARROW_RETURN_NOT_OK(Find(item.base_oid, &base_type, error));
        mapping_.insert({item.oid, base_type.Range(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        break;
      }

      default:
        mapping_.insert({item.oid, type});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        break;
    }

    return NANOARROW_OK;
  }

  // Insert a class definition. For the purposes of resolving a PostgresType
  // instance, this is simply a vector of field_name: oid tuples. The specified
  // OIDs need not have already been inserted into the type resolver. This
  // information can be found in the pg_attribute table (attname and atttypoid,
  // respectively).
  void InsertClass(uint32_t oid,
                   const std::vector<std::pair<std::string, uint32_t>>& cls) {
    classes_.insert({oid, cls});
  }

 private:
  std::unordered_map<uint32_t, PostgresType> mapping_;
  // We can't use PostgresTypeId as an unordered map key because there is no
  // built-in hasher for an enum on gcc 4.8 (i.e., R 3.6 on Windows).
  std::unordered_map<int32_t, uint32_t> reverse_mapping_;
  std::unordered_map<uint32_t, uint32_t> array_mapping_;
  std::unordered_map<uint32_t, std::vector<std::pair<std::string, uint32_t>>> classes_;
  std::unordered_map<std::string, PostgresType> base_;

  ArrowErrorCode ResolveClass(uint32_t oid,
                              std::vector<std::pair<std::string, uint32_t>>* out,
                              ArrowError* error) {
    auto result = classes_.find(oid);
    if (result == classes_.end()) {
      ArrowErrorSet(error, "Class definition with oid %ld not found",
                    static_cast<long>(oid));  // NOLINT(runtime/int)
      return EINVAL;
    }

    *out = result->second;
    return NANOARROW_OK;
  }

  // Returns a sentinel PostgresType instance for each type and builds a lookup
  // table based on the receive function name.
  static std::unordered_map<std::string, PostgresType> AllBase() {
    std::unordered_map<std::string, PostgresType> out;
    for (PostgresTypeId type_id : PostgresTypeIdAll()) {
      PostgresType type(type_id);
      out.insert(
          {PostgresTyprecv(type_id), type.WithPgTypeInfo(0, PostgresTypname(type_id))});
    }

    return out;
  }
};

static inline ArrowErrorCode PostgresTypeFromSchema(const PostgresTypeResolver& resolver,
                                                    ArrowSchema* schema,
                                                    PostgresType* out,
                                                    ArrowError* error) {
  ArrowSchemaView schema_view;
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view, schema, error));

  switch (schema_view.type) {
    case NANOARROW_TYPE_BOOL:
      return resolver.Find(resolver.GetOID(PG_TYPE_BOOL), out, error);
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_INT16:
      return resolver.Find(resolver.GetOID(PG_TYPE_INT2), out, error);
    case NANOARROW_TYPE_UINT16:
    case NANOARROW_TYPE_INT32:
      return resolver.Find(resolver.GetOID(PG_TYPE_INT4), out, error);
    case NANOARROW_TYPE_UINT32:
    case NANOARROW_TYPE_INT64:
      return resolver.Find(resolver.GetOID(PG_TYPE_INT8), out, error);
    case NANOARROW_TYPE_FLOAT:
      return resolver.Find(resolver.GetOID(PG_TYPE_FLOAT4), out, error);
    case NANOARROW_TYPE_DOUBLE:
      return resolver.Find(resolver.GetOID(PG_TYPE_FLOAT8), out, error);
    case NANOARROW_TYPE_STRING:
      return resolver.Find(resolver.GetOID(PG_TYPE_TEXT), out, error);
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      return resolver.Find(resolver.GetOID(PG_TYPE_BYTEA), out, error);
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_FIXED_SIZE_LIST: {
      PostgresType child;
      NANOARROW_RETURN_NOT_OK(
          PostgresTypeFromSchema(resolver, schema->children[0], &child, error));
      return resolver.FindArray(child.oid(), out, error);
    }

    default:
      ArrowErrorSet(error, "Can't map Arrow type '%s' to Postgres type",
                    ArrowTypeString(schema_view.type));
      return ENOTSUP;
  }
}

static inline const char* PostgresTyprecv(PostgresTypeId type_id) {
  switch (type_id) {
    case PG_TYPE_ACLITEM:
      return "aclitem_recv";
    case PG_TYPE_ANYARRAY:
      return "anyarray_recv";
    case PG_TYPE_ANYCOMPATIBLEARRAY:
      return "anycompatiblearray_recv";
    case PG_TYPE_ARRAY:
      return "array_recv";
    case PG_TYPE_BIT:
      return "bit_recv";
    case PG_TYPE_BOOL:
      return "boolrecv";
    case PG_TYPE_BOX:
      return "box_recv";
    case PG_TYPE_BPCHAR:
      return "bpcharrecv";
    case PG_TYPE_BRIN_BLOOM_SUMMARY:
      return "brin_bloom_summary_recv";
    case PG_TYPE_BRIN_MINMAX_MULTI_SUMMARY:
      return "brin_minmax_multi_summary_recv";
    case PG_TYPE_BYTEA:
      return "bytearecv";
    case PG_TYPE_CASH:
      return "cash_recv";
    case PG_TYPE_CHAR:
      return "charrecv";
    case PG_TYPE_CIDR:
      return "cidr_recv";
    case PG_TYPE_CID:
      return "cidrecv";
    case PG_TYPE_CIRCLE:
      return "circle_recv";
    case PG_TYPE_CSTRING:
      return "cstring_recv";
    case PG_TYPE_DATE:
      return "date_recv";
    case PG_TYPE_DOMAIN:
      return "domain_recv";
    case PG_TYPE_FLOAT4:
      return "float4recv";
    case PG_TYPE_FLOAT8:
      return "float8recv";
    case PG_TYPE_INET:
      return "inet_recv";
    case PG_TYPE_INT2:
      return "int2recv";
    case PG_TYPE_INT2VECTOR:
      return "int2vectorrecv";
    case PG_TYPE_INT4:
      return "int4recv";
    case PG_TYPE_INT8:
      return "int8recv";
    case PG_TYPE_INTERVAL:
      return "interval_recv";
    case PG_TYPE_JSON:
      return "json_recv";
    case PG_TYPE_JSONB:
      return "jsonb_recv";
    case PG_TYPE_JSONPATH:
      return "jsonpath_recv";
    case PG_TYPE_LINE:
      return "line_recv";
    case PG_TYPE_LSEG:
      return "lseg_recv";
    case PG_TYPE_MACADDR:
      return "macaddr_recv";
    case PG_TYPE_MACADDR8:
      return "macaddr8_recv";
    case PG_TYPE_MULTIRANGE:
      return "multirange_recv";
    case PG_TYPE_NAME:
      return "namerecv";
    case PG_TYPE_NUMERIC:
      return "numeric_recv";
    case PG_TYPE_OID:
      return "oidrecv";
    case PG_TYPE_OIDVECTOR:
      return "oidvectorrecv";
    case PG_TYPE_PATH:
      return "path_recv";
    case PG_TYPE_PG_NODE_TREE:
      return "pg_node_tree_recv";
    case PG_TYPE_PG_NDISTINCT:
      return "pg_ndistinct_recv";
    case PG_TYPE_PG_DEPENDENCIES:
      return "pg_dependencies_recv";
    case PG_TYPE_PG_LSN:
      return "pg_lsn_recv";
    case PG_TYPE_PG_MCV_LIST:
      return "pg_mcv_list_recv";
    case PG_TYPE_PG_DDL_COMMAND:
      return "pg_ddl_command_recv";
    case PG_TYPE_PG_SNAPSHOT:
      return "pg_snapshot_recv";
    case PG_TYPE_POINT:
      return "point_recv";
    case PG_TYPE_POLY:
      return "poly_recv";
    case PG_TYPE_RANGE:
      return "range_recv";
    case PG_TYPE_RECORD:
      return "record_recv";
    case PG_TYPE_REGCLASS:
      return "regclassrecv";
    case PG_TYPE_REGCOLLATION:
      return "regcollationrecv";
    case PG_TYPE_REGCONFIG:
      return "regconfigrecv";
    case PG_TYPE_REGDICTIONARY:
      return "regdictionaryrecv";
    case PG_TYPE_REGNAMESPACE:
      return "regnamespacerecv";
    case PG_TYPE_REGOPERATOR:
      return "regoperatorrecv";
    case PG_TYPE_REGOPER:
      return "regoperrecv";
    case PG_TYPE_REGPROCEDURE:
      return "regprocedurerecv";
    case PG_TYPE_REGPROC:
      return "regprocrecv";
    case PG_TYPE_REGROLE:
      return "regrolerecv";
    case PG_TYPE_REGTYPE:
      return "regtyperecv";
    case PG_TYPE_TEXT:
      return "textrecv";
    case PG_TYPE_TID:
      return "tidrecv";
    case PG_TYPE_TIME:
      return "time_recv";
    case PG_TYPE_TIMESTAMP:
      return "timestamp_recv";
    case PG_TYPE_TIMESTAMPTZ:
      return "timestamptz_recv";
    case PG_TYPE_TIMETZ:
      return "timetz_recv";
    case PG_TYPE_TSQUERY:
      return "tsqueryrecv";
    case PG_TYPE_TSVECTOR:
      return "tsvectorrecv";
    case PG_TYPE_TXID_SNAPSHOT:
      return "txid_snapshot_recv";
    case PG_TYPE_UNKNOWN:
      return "unknownrecv";
    case PG_TYPE_UUID:
      return "uuid_recv";
    case PG_TYPE_VARBIT:
      return "varbit_recv";
    case PG_TYPE_VARCHAR:
      return "varcharrecv";
    case PG_TYPE_VOID:
      return "void_recv";
    case PG_TYPE_XID8:
      return "xid8recv";
    case PG_TYPE_XID:
      return "xidrecv";
    case PG_TYPE_XML:
      return "xml_recv";
    default:
      return "";
  }
}

static inline const char* PostgresTypname(PostgresTypeId type_id) {
  switch (type_id) {
    case PG_TYPE_ACLITEM:
      return "aclitem";
    case PG_TYPE_ANYARRAY:
      return "anyarray";
    case PG_TYPE_ANYCOMPATIBLEARRAY:
      return "anycompatiblearray";
    case PG_TYPE_ARRAY:
      return "array";
    case PG_TYPE_BIT:
      return "bit";
    case PG_TYPE_BOOL:
      return "bool";
    case PG_TYPE_BOX:
      return "box";
    case PG_TYPE_BPCHAR:
      return "bpchar";
    case PG_TYPE_BRIN_BLOOM_SUMMARY:
      return "brin_bloom_summary";
    case PG_TYPE_BRIN_MINMAX_MULTI_SUMMARY:
      return "brin_minmax_multi_summary";
    case PG_TYPE_BYTEA:
      return "bytea";
    case PG_TYPE_CASH:
      return "cash";
    case PG_TYPE_CHAR:
      return "char";
    case PG_TYPE_CIDR:
      return "cidr";
    case PG_TYPE_CID:
      return "cid";
    case PG_TYPE_CIRCLE:
      return "circle";
    case PG_TYPE_CSTRING:
      return "cstring";
    case PG_TYPE_DATE:
      return "date";
    case PG_TYPE_DOMAIN:
      return "domain";
    case PG_TYPE_FLOAT4:
      return "float4";
    case PG_TYPE_FLOAT8:
      return "float8";
    case PG_TYPE_INET:
      return "inet";
    case PG_TYPE_INT2:
      return "int2";
    case PG_TYPE_INT2VECTOR:
      return "int2vector";
    case PG_TYPE_INT4:
      return "int4";
    case PG_TYPE_INT8:
      return "int8";
    case PG_TYPE_INTERVAL:
      return "interval";
    case PG_TYPE_JSON:
      return "json";
    case PG_TYPE_JSONB:
      return "jsonb";
    case PG_TYPE_JSONPATH:
      return "jsonpath";
    case PG_TYPE_LINE:
      return "line";
    case PG_TYPE_LSEG:
      return "lseg";
    case PG_TYPE_MACADDR:
      return "macaddr";
    case PG_TYPE_MACADDR8:
      return "macaddr8";
    case PG_TYPE_MULTIRANGE:
      return "multirange";
    case PG_TYPE_NAME:
      return "name";
    case PG_TYPE_NUMERIC:
      return "numeric";
    case PG_TYPE_OID:
      return "oid";
    case PG_TYPE_OIDVECTOR:
      return "oidvector";
    case PG_TYPE_PATH:
      return "path";
    case PG_TYPE_PG_NODE_TREE:
      return "pg_node_tree";
    case PG_TYPE_PG_NDISTINCT:
      return "pg_ndistinct";
    case PG_TYPE_PG_DEPENDENCIES:
      return "pg_dependencies";
    case PG_TYPE_PG_LSN:
      return "pg_lsn";
    case PG_TYPE_PG_MCV_LIST:
      return "pg_mcv_list";
    case PG_TYPE_PG_DDL_COMMAND:
      return "pg_ddl_command";
    case PG_TYPE_PG_SNAPSHOT:
      return "pg_snapshot";
    case PG_TYPE_POINT:
      return "point";
    case PG_TYPE_POLY:
      return "poly";
    case PG_TYPE_RANGE:
      return "range";
    case PG_TYPE_RECORD:
      return "record";
    case PG_TYPE_REGCLASS:
      return "regclass";
    case PG_TYPE_REGCOLLATION:
      return "regcollation";
    case PG_TYPE_REGCONFIG:
      return "regconfig";
    case PG_TYPE_REGDICTIONARY:
      return "regdictionary";
    case PG_TYPE_REGNAMESPACE:
      return "regnamespace";
    case PG_TYPE_REGOPERATOR:
      return "regoperator";
    case PG_TYPE_REGOPER:
      return "regoper";
    case PG_TYPE_REGPROCEDURE:
      return "regprocedure";
    case PG_TYPE_REGPROC:
      return "regproc";
    case PG_TYPE_REGROLE:
      return "regrole";
    case PG_TYPE_REGTYPE:
      return "regtype";
    case PG_TYPE_TEXT:
      return "text";
    case PG_TYPE_TID:
      return "tid";
    case PG_TYPE_TIME:
      return "time";
    case PG_TYPE_TIMESTAMP:
      return "timestamp";
    case PG_TYPE_TIMESTAMPTZ:
      return "timestamptz";
    case PG_TYPE_TIMETZ:
      return "timetz";
    case PG_TYPE_TSQUERY:
      return "tsquery";
    case PG_TYPE_TSVECTOR:
      return "tsvector";
    case PG_TYPE_TXID_SNAPSHOT:
      return "txid_snapshot";
    case PG_TYPE_UNKNOWN:
      return "unknown";
    case PG_TYPE_UUID:
      return "uuid";
    case PG_TYPE_VARBIT:
      return "varbit";
    case PG_TYPE_VARCHAR:
      return "varchar";
    case PG_TYPE_VOID:
      return "void";
    case PG_TYPE_XID8:
      return "xid8";
    case PG_TYPE_XID:
      return "xid";
    case PG_TYPE_XML:
      return "xml";
    default:
      return "";
  }
}

static inline std::vector<PostgresTypeId> PostgresTypeIdAll(bool nested) {
  std::vector<PostgresTypeId> base = {PG_TYPE_ACLITEM,
                                      PG_TYPE_ANYARRAY,
                                      PG_TYPE_ANYCOMPATIBLEARRAY,
                                      PG_TYPE_BIT,
                                      PG_TYPE_BOOL,
                                      PG_TYPE_BOX,
                                      PG_TYPE_BPCHAR,
                                      PG_TYPE_BRIN_BLOOM_SUMMARY,
                                      PG_TYPE_BRIN_MINMAX_MULTI_SUMMARY,
                                      PG_TYPE_BYTEA,
                                      PG_TYPE_CASH,
                                      PG_TYPE_CHAR,
                                      PG_TYPE_CIDR,
                                      PG_TYPE_CID,
                                      PG_TYPE_CIRCLE,
                                      PG_TYPE_CSTRING,
                                      PG_TYPE_DATE,
                                      PG_TYPE_FLOAT4,
                                      PG_TYPE_FLOAT8,
                                      PG_TYPE_INET,
                                      PG_TYPE_INT2,
                                      PG_TYPE_INT2VECTOR,
                                      PG_TYPE_INT4,
                                      PG_TYPE_INT8,
                                      PG_TYPE_INTERVAL,
                                      PG_TYPE_JSON,
                                      PG_TYPE_JSONB,
                                      PG_TYPE_JSONPATH,
                                      PG_TYPE_LINE,
                                      PG_TYPE_LSEG,
                                      PG_TYPE_MACADDR,
                                      PG_TYPE_MACADDR8,
                                      PG_TYPE_MULTIRANGE,
                                      PG_TYPE_NAME,
                                      PG_TYPE_NUMERIC,
                                      PG_TYPE_OID,
                                      PG_TYPE_OIDVECTOR,
                                      PG_TYPE_PATH,
                                      PG_TYPE_PG_NODE_TREE,
                                      PG_TYPE_PG_NDISTINCT,
                                      PG_TYPE_PG_DEPENDENCIES,
                                      PG_TYPE_PG_LSN,
                                      PG_TYPE_PG_MCV_LIST,
                                      PG_TYPE_PG_DDL_COMMAND,
                                      PG_TYPE_PG_SNAPSHOT,
                                      PG_TYPE_POINT,
                                      PG_TYPE_POLY,
                                      PG_TYPE_REGCLASS,
                                      PG_TYPE_REGCOLLATION,
                                      PG_TYPE_REGCONFIG,
                                      PG_TYPE_REGDICTIONARY,
                                      PG_TYPE_REGNAMESPACE,
                                      PG_TYPE_REGOPERATOR,
                                      PG_TYPE_REGOPER,
                                      PG_TYPE_REGPROCEDURE,
                                      PG_TYPE_REGPROC,
                                      PG_TYPE_REGROLE,
                                      PG_TYPE_REGTYPE,
                                      PG_TYPE_TEXT,
                                      PG_TYPE_TID,
                                      PG_TYPE_TIME,
                                      PG_TYPE_TIMESTAMP,
                                      PG_TYPE_TIMESTAMPTZ,
                                      PG_TYPE_TIMETZ,
                                      PG_TYPE_TSQUERY,
                                      PG_TYPE_TSVECTOR,
                                      PG_TYPE_TXID_SNAPSHOT,
                                      PG_TYPE_UNKNOWN,
                                      PG_TYPE_UUID,
                                      PG_TYPE_VARBIT,
                                      PG_TYPE_VARCHAR,
                                      PG_TYPE_VOID,
                                      PG_TYPE_XID8,
                                      PG_TYPE_XID,
                                      PG_TYPE_XML};

  if (nested) {
    base.push_back(PG_TYPE_ARRAY);
    base.push_back(PG_TYPE_RECORD);
    base.push_back(PG_TYPE_RANGE);
    base.push_back(PG_TYPE_DOMAIN);
  }

  return base;
}

}  // namespace adbcpq
