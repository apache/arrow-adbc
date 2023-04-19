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
  TYPE_ID_UNINITIALIZED,
  TYPE_ID_ACLITEM,
  TYPE_ID_ANYARRAY,
  TYPE_ID_ANYCOMPATIBLEARRAY,
  TYPE_ID_ARRAY,
  TYPE_ID_BIT,
  TYPE_ID_BOOL,
  TYPE_ID_BOX,
  TYPE_ID_BPCHAR,
  TYPE_ID_BRIN_BLOOM_SUMMARY,
  TYPE_ID_BRIN_MINMAX_MULTI_SUMMARY,
  TYPE_ID_BYTEA,
  TYPE_ID_CASH,
  TYPE_ID_CHAR,
  TYPE_ID_CIDR,
  TYPE_ID_CID,
  TYPE_ID_CIRCLE,
  TYPE_ID_CSTRING,
  TYPE_ID_DATE,
  TYPE_ID_DOMAIN,
  TYPE_ID_FLOAT4,
  TYPE_ID_FLOAT8,
  TYPE_ID_INET,
  TYPE_ID_INT2,
  TYPE_ID_INT2VECTOR,
  TYPE_ID_INT4,
  TYPE_ID_INT8,
  TYPE_ID_INTERVAL,
  TYPE_ID_JSON,
  TYPE_ID_JSONB,
  TYPE_ID_JSONPATH,
  TYPE_ID_LINE,
  TYPE_ID_LSEG,
  TYPE_ID_MACADDR,
  TYPE_ID_MACADDR8,
  TYPE_ID_MULTIRANGE,
  TYPE_ID_NAME,
  TYPE_ID_NUMERIC,
  TYPE_ID_OID,
  TYPE_ID_OIDVECTOR,
  TYPE_ID_PATH,
  TYPE_ID_PG_DDL_COMMAND,
  TYPE_ID_PG_DEPENDENCIES,
  TYPE_ID_PG_LSN,
  TYPE_ID_PG_MCV_LIST,
  TYPE_ID_PG_NDISTINCT,
  TYPE_ID_PG_NODE_TREE,
  TYPE_ID_PG_SNAPSHOT,
  TYPE_ID_POINT,
  TYPE_ID_POLY,
  TYPE_ID_RANGE,
  TYPE_ID_RECORD,
  TYPE_ID_REGCLASS,
  TYPE_ID_REGCOLLATION,
  TYPE_ID_REGCONFIG,
  TYPE_ID_REGDICTIONARY,
  TYPE_ID_REGNAMESPACE,
  TYPE_ID_REGOPERATOR,
  TYPE_ID_REGOPER,
  TYPE_ID_REGPROCEDURE,
  TYPE_ID_REGPROC,
  TYPE_ID_REGROLE,
  TYPE_ID_REGTYPE,
  TYPE_ID_TEXT,
  TYPE_ID_TID,
  TYPE_ID_TIME,
  TYPE_ID_TIMESTAMP,
  TYPE_ID_TIMESTAMPTZ,
  TYPE_ID_TIMETZ,
  TYPE_ID_TSQUERY,
  TYPE_ID_TSVECTOR,
  TYPE_ID_TXID_SNAPSHOT,
  TYPE_ID_UNKNOWN,
  TYPE_ID_UUID,
  TYPE_ID_VARBIT,
  TYPE_ID_VARCHAR,
  TYPE_ID_VOID,
  TYPE_ID_XID8,
  TYPE_ID_XID,
  TYPE_ID_XML
};

// Returns the receive function name as defined in the typrecieve column
// of the pg_type table. This name is the one that gets used to look up
// the PostgresTypeId.
static inline const char* PostgresTyprecv(PostgresTypeId type_id);

// Returns a likely typname value for a given PostgresTypeId. This is useful
// for testing and error messages but may not be the actual value present
// in the pg_type typname column.
static inline const char* PostgresTypname(PostgresTypeId type_id);

// A vector of all type IDs, optionally including the nested types TYPE_ID_ARRAY,
// TYPE_ID_DOMAIN, TYPE_ID_RECORD, and TYPE_ID_RANGE.
static inline std::vector<PostgresTypeId> PostgresTypeIdAll(bool nested = true);

// An abstraction of a (potentially nested and/or parameterized) Postgres
// data type. This class is where default type conversion to/from Arrow
// is defined. It is intentionally copyable.
class PostgresType {
 public:
  explicit PostgresType(PostgresTypeId type_id) : oid_(0), type_id_(type_id) {}

  PostgresType() : PostgresType(TYPE_ID_UNINITIALIZED) {}

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
    PostgresType out(TYPE_ID_ARRAY);
    out.AppendChild("item", *this);
    out.oid_ = oid;
    out.typname_ = typname;
    return out;
  }

  PostgresType Domain(uint32_t oid, const std::string& typname) {
    return WithPgTypeInfo(oid, typname);
  }

  PostgresType Range(uint32_t oid = 0, const std::string& typname = "") const {
    PostgresType out(TYPE_ID_RANGE);
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
      case TYPE_ID_BOOL:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BOOL));
        break;
      case TYPE_ID_INT2:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT16));
        break;
      case TYPE_ID_INT4:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT32));
        break;
      case TYPE_ID_INT8:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT64));
        break;
      case TYPE_ID_FLOAT4:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_FLOAT));
        break;
      case TYPE_ID_FLOAT8:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_DOUBLE));
        break;
      case TYPE_ID_CHAR:
      case TYPE_ID_BPCHAR:
      case TYPE_ID_VARCHAR:
      case TYPE_ID_TEXT:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING));
        break;
      case TYPE_ID_BYTEA:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
        break;

      case TYPE_ID_RECORD:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(schema, n_children()));
        for (int64_t i = 0; i < n_children(); i++) {
          NANOARROW_RETURN_NOT_OK(children_[i].SetSchema(schema->children[i]));
        }
        break;

      case TYPE_ID_ARRAY:
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
      case TYPE_ID_ARRAY: {
        PostgresType child;
        NANOARROW_RETURN_NOT_OK(Find(item.child_oid, &child, error));
        mapping_.insert({item.oid, child.Array(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        array_mapping_.insert({child.oid(), item.oid});
        break;
      }

      case TYPE_ID_RECORD: {
        std::vector<std::pair<std::string, uint32_t>> child_desc;
        NANOARROW_RETURN_NOT_OK(ResolveClass(item.class_oid, &child_desc, error));

        PostgresType out(TYPE_ID_RECORD);
        for (const auto& child_item : child_desc) {
          PostgresType child;
          NANOARROW_RETURN_NOT_OK(Find(child_item.second, &child, error));
          out.AppendChild(child_item.first, child);
        }

        mapping_.insert({item.oid, out.WithPgTypeInfo(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        break;
      }

      case TYPE_ID_DOMAIN: {
        PostgresType base_type;
        NANOARROW_RETURN_NOT_OK(Find(item.base_oid, &base_type, error));
        mapping_.insert({item.oid, base_type.Domain(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        break;
      }

      case TYPE_ID_RANGE: {
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
      return resolver.Find(resolver.GetOID(TYPE_ID_BOOL), out, error);
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_INT16:
      return resolver.Find(resolver.GetOID(TYPE_ID_INT2), out, error);
    case NANOARROW_TYPE_UINT16:
    case NANOARROW_TYPE_INT32:
      return resolver.Find(resolver.GetOID(TYPE_ID_INT4), out, error);
    case NANOARROW_TYPE_UINT32:
    case NANOARROW_TYPE_INT64:
      return resolver.Find(resolver.GetOID(TYPE_ID_INT8), out, error);
    case NANOARROW_TYPE_FLOAT:
      return resolver.Find(resolver.GetOID(TYPE_ID_FLOAT4), out, error);
    case NANOARROW_TYPE_DOUBLE:
      return resolver.Find(resolver.GetOID(TYPE_ID_FLOAT8), out, error);
    case NANOARROW_TYPE_STRING:
      return resolver.Find(resolver.GetOID(TYPE_ID_TEXT), out, error);
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      return resolver.Find(resolver.GetOID(TYPE_ID_BYTEA), out, error);
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
    case TYPE_ID_ACLITEM:
      return "aclitem_recv";
    case TYPE_ID_ANYARRAY:
      return "anyarray_recv";
    case TYPE_ID_ANYCOMPATIBLEARRAY:
      return "anycompatiblearray_recv";
    case TYPE_ID_ARRAY:
      return "array_recv";
    case TYPE_ID_BIT:
      return "bit_recv";
    case TYPE_ID_BOOL:
      return "boolrecv";
    case TYPE_ID_BOX:
      return "box_recv";
    case TYPE_ID_BPCHAR:
      return "bpcharrecv";
    case TYPE_ID_BRIN_BLOOM_SUMMARY:
      return "brin_bloom_summary_recv";
    case TYPE_ID_BRIN_MINMAX_MULTI_SUMMARY:
      return "brin_minmax_multi_summary_recv";
    case TYPE_ID_BYTEA:
      return "bytearecv";
    case TYPE_ID_CASH:
      return "cash_recv";
    case TYPE_ID_CHAR:
      return "charrecv";
    case TYPE_ID_CIDR:
      return "cidr_recv";
    case TYPE_ID_CID:
      return "cidrecv";
    case TYPE_ID_CIRCLE:
      return "circle_recv";
    case TYPE_ID_CSTRING:
      return "cstring_recv";
    case TYPE_ID_DATE:
      return "date_recv";
    case TYPE_ID_DOMAIN:
      return "domain_recv";
    case TYPE_ID_FLOAT4:
      return "float4recv";
    case TYPE_ID_FLOAT8:
      return "float8recv";
    case TYPE_ID_INET:
      return "inet_recv";
    case TYPE_ID_INT2:
      return "int2recv";
    case TYPE_ID_INT2VECTOR:
      return "int2vectorrecv";
    case TYPE_ID_INT4:
      return "int4recv";
    case TYPE_ID_INT8:
      return "int8recv";
    case TYPE_ID_INTERVAL:
      return "interval_recv";
    case TYPE_ID_JSON:
      return "json_recv";
    case TYPE_ID_JSONB:
      return "jsonb_recv";
    case TYPE_ID_JSONPATH:
      return "jsonpath_recv";
    case TYPE_ID_LINE:
      return "line_recv";
    case TYPE_ID_LSEG:
      return "lseg_recv";
    case TYPE_ID_MACADDR:
      return "macaddr_recv";
    case TYPE_ID_MACADDR8:
      return "macaddr8_recv";
    case TYPE_ID_MULTIRANGE:
      return "multirange_recv";
    case TYPE_ID_NAME:
      return "namerecv";
    case TYPE_ID_NUMERIC:
      return "numeric_recv";
    case TYPE_ID_OID:
      return "oidrecv";
    case TYPE_ID_OIDVECTOR:
      return "oidvectorrecv";
    case TYPE_ID_PATH:
      return "path_recv";
    case TYPE_ID_PG_NODE_TREE:
      return "pg_node_tree_recv";
    case TYPE_ID_PG_NDISTINCT:
      return "pg_ndistinct_recv";
    case TYPE_ID_PG_DEPENDENCIES:
      return "pg_dependencies_recv";
    case TYPE_ID_PG_LSN:
      return "pg_lsn_recv";
    case TYPE_ID_PG_MCV_LIST:
      return "pg_mcv_list_recv";
    case TYPE_ID_PG_DDL_COMMAND:
      return "pg_ddl_command_recv";
    case TYPE_ID_PG_SNAPSHOT:
      return "pg_snapshot_recv";
    case TYPE_ID_POINT:
      return "point_recv";
    case TYPE_ID_POLY:
      return "poly_recv";
    case TYPE_ID_RANGE:
      return "range_recv";
    case TYPE_ID_RECORD:
      return "record_recv";
    case TYPE_ID_REGCLASS:
      return "regclassrecv";
    case TYPE_ID_REGCOLLATION:
      return "regcollationrecv";
    case TYPE_ID_REGCONFIG:
      return "regconfigrecv";
    case TYPE_ID_REGDICTIONARY:
      return "regdictionaryrecv";
    case TYPE_ID_REGNAMESPACE:
      return "regnamespacerecv";
    case TYPE_ID_REGOPERATOR:
      return "regoperatorrecv";
    case TYPE_ID_REGOPER:
      return "regoperrecv";
    case TYPE_ID_REGPROCEDURE:
      return "regprocedurerecv";
    case TYPE_ID_REGPROC:
      return "regprocrecv";
    case TYPE_ID_REGROLE:
      return "regrolerecv";
    case TYPE_ID_REGTYPE:
      return "regtyperecv";
    case TYPE_ID_TEXT:
      return "textrecv";
    case TYPE_ID_TID:
      return "tidrecv";
    case TYPE_ID_TIME:
      return "time_recv";
    case TYPE_ID_TIMESTAMP:
      return "timestamp_recv";
    case TYPE_ID_TIMESTAMPTZ:
      return "timestamptz_recv";
    case TYPE_ID_TIMETZ:
      return "timetz_recv";
    case TYPE_ID_TSQUERY:
      return "tsqueryrecv";
    case TYPE_ID_TSVECTOR:
      return "tsvectorrecv";
    case TYPE_ID_TXID_SNAPSHOT:
      return "txid_snapshot_recv";
    case TYPE_ID_UNKNOWN:
      return "unknownrecv";
    case TYPE_ID_UUID:
      return "uuid_recv";
    case TYPE_ID_VARBIT:
      return "varbit_recv";
    case TYPE_ID_VARCHAR:
      return "varcharrecv";
    case TYPE_ID_VOID:
      return "void_recv";
    case TYPE_ID_XID8:
      return "xid8recv";
    case TYPE_ID_XID:
      return "xidrecv";
    case TYPE_ID_XML:
      return "xml_recv";
    default:
      return "";
  }
}

static inline const char* PostgresTypname(PostgresTypeId type_id) {
  switch (type_id) {
    case TYPE_ID_ACLITEM:
      return "aclitem";
    case TYPE_ID_ANYARRAY:
      return "anyarray";
    case TYPE_ID_ANYCOMPATIBLEARRAY:
      return "anycompatiblearray";
    case TYPE_ID_ARRAY:
      return "array";
    case TYPE_ID_BIT:
      return "bit";
    case TYPE_ID_BOOL:
      return "bool";
    case TYPE_ID_BOX:
      return "box";
    case TYPE_ID_BPCHAR:
      return "bpchar";
    case TYPE_ID_BRIN_BLOOM_SUMMARY:
      return "brin_bloom_summary";
    case TYPE_ID_BRIN_MINMAX_MULTI_SUMMARY:
      return "brin_minmax_multi_summary";
    case TYPE_ID_BYTEA:
      return "bytea";
    case TYPE_ID_CASH:
      return "cash";
    case TYPE_ID_CHAR:
      return "char";
    case TYPE_ID_CIDR:
      return "cidr";
    case TYPE_ID_CID:
      return "cid";
    case TYPE_ID_CIRCLE:
      return "circle";
    case TYPE_ID_CSTRING:
      return "cstring";
    case TYPE_ID_DATE:
      return "date";
    case TYPE_ID_DOMAIN:
      return "domain";
    case TYPE_ID_FLOAT4:
      return "float4";
    case TYPE_ID_FLOAT8:
      return "float8";
    case TYPE_ID_INET:
      return "inet";
    case TYPE_ID_INT2:
      return "int2";
    case TYPE_ID_INT2VECTOR:
      return "int2vector";
    case TYPE_ID_INT4:
      return "int4";
    case TYPE_ID_INT8:
      return "int8";
    case TYPE_ID_INTERVAL:
      return "interval";
    case TYPE_ID_JSON:
      return "json";
    case TYPE_ID_JSONB:
      return "jsonb";
    case TYPE_ID_JSONPATH:
      return "jsonpath";
    case TYPE_ID_LINE:
      return "line";
    case TYPE_ID_LSEG:
      return "lseg";
    case TYPE_ID_MACADDR:
      return "macaddr";
    case TYPE_ID_MACADDR8:
      return "macaddr8";
    case TYPE_ID_MULTIRANGE:
      return "multirange";
    case TYPE_ID_NAME:
      return "name";
    case TYPE_ID_NUMERIC:
      return "numeric";
    case TYPE_ID_OID:
      return "oid";
    case TYPE_ID_OIDVECTOR:
      return "oidvector";
    case TYPE_ID_PATH:
      return "path";
    case TYPE_ID_PG_NODE_TREE:
      return "pg_node_tree";
    case TYPE_ID_PG_NDISTINCT:
      return "pg_ndistinct";
    case TYPE_ID_PG_DEPENDENCIES:
      return "pg_dependencies";
    case TYPE_ID_PG_LSN:
      return "pg_lsn";
    case TYPE_ID_PG_MCV_LIST:
      return "pg_mcv_list";
    case TYPE_ID_PG_DDL_COMMAND:
      return "pg_ddl_command";
    case TYPE_ID_PG_SNAPSHOT:
      return "pg_snapshot";
    case TYPE_ID_POINT:
      return "point";
    case TYPE_ID_POLY:
      return "poly";
    case TYPE_ID_RANGE:
      return "range";
    case TYPE_ID_RECORD:
      return "record";
    case TYPE_ID_REGCLASS:
      return "regclass";
    case TYPE_ID_REGCOLLATION:
      return "regcollation";
    case TYPE_ID_REGCONFIG:
      return "regconfig";
    case TYPE_ID_REGDICTIONARY:
      return "regdictionary";
    case TYPE_ID_REGNAMESPACE:
      return "regnamespace";
    case TYPE_ID_REGOPERATOR:
      return "regoperator";
    case TYPE_ID_REGOPER:
      return "regoper";
    case TYPE_ID_REGPROCEDURE:
      return "regprocedure";
    case TYPE_ID_REGPROC:
      return "regproc";
    case TYPE_ID_REGROLE:
      return "regrole";
    case TYPE_ID_REGTYPE:
      return "regtype";
    case TYPE_ID_TEXT:
      return "text";
    case TYPE_ID_TID:
      return "tid";
    case TYPE_ID_TIME:
      return "time";
    case TYPE_ID_TIMESTAMP:
      return "timestamp";
    case TYPE_ID_TIMESTAMPTZ:
      return "timestamptz";
    case TYPE_ID_TIMETZ:
      return "timetz";
    case TYPE_ID_TSQUERY:
      return "tsquery";
    case TYPE_ID_TSVECTOR:
      return "tsvector";
    case TYPE_ID_TXID_SNAPSHOT:
      return "txid_snapshot";
    case TYPE_ID_UNKNOWN:
      return "unknown";
    case TYPE_ID_UUID:
      return "uuid";
    case TYPE_ID_VARBIT:
      return "varbit";
    case TYPE_ID_VARCHAR:
      return "varchar";
    case TYPE_ID_VOID:
      return "void";
    case TYPE_ID_XID8:
      return "xid8";
    case TYPE_ID_XID:
      return "xid";
    case TYPE_ID_XML:
      return "xml";
    default:
      return "";
  }
}

static inline std::vector<PostgresTypeId> PostgresTypeIdAll(bool nested) {
  std::vector<PostgresTypeId> base = {TYPE_ID_ACLITEM,
                                      TYPE_ID_ANYARRAY,
                                      TYPE_ID_ANYCOMPATIBLEARRAY,
                                      TYPE_ID_BIT,
                                      TYPE_ID_BOOL,
                                      TYPE_ID_BOX,
                                      TYPE_ID_BPCHAR,
                                      TYPE_ID_BRIN_BLOOM_SUMMARY,
                                      TYPE_ID_BRIN_MINMAX_MULTI_SUMMARY,
                                      TYPE_ID_BYTEA,
                                      TYPE_ID_CASH,
                                      TYPE_ID_CHAR,
                                      TYPE_ID_CIDR,
                                      TYPE_ID_CID,
                                      TYPE_ID_CIRCLE,
                                      TYPE_ID_CSTRING,
                                      TYPE_ID_DATE,
                                      TYPE_ID_FLOAT4,
                                      TYPE_ID_FLOAT8,
                                      TYPE_ID_INET,
                                      TYPE_ID_INT2,
                                      TYPE_ID_INT2VECTOR,
                                      TYPE_ID_INT4,
                                      TYPE_ID_INT8,
                                      TYPE_ID_INTERVAL,
                                      TYPE_ID_JSON,
                                      TYPE_ID_JSONB,
                                      TYPE_ID_JSONPATH,
                                      TYPE_ID_LINE,
                                      TYPE_ID_LSEG,
                                      TYPE_ID_MACADDR,
                                      TYPE_ID_MACADDR8,
                                      TYPE_ID_MULTIRANGE,
                                      TYPE_ID_NAME,
                                      TYPE_ID_NUMERIC,
                                      TYPE_ID_OID,
                                      TYPE_ID_OIDVECTOR,
                                      TYPE_ID_PATH,
                                      TYPE_ID_PG_NODE_TREE,
                                      TYPE_ID_PG_NDISTINCT,
                                      TYPE_ID_PG_DEPENDENCIES,
                                      TYPE_ID_PG_LSN,
                                      TYPE_ID_PG_MCV_LIST,
                                      TYPE_ID_PG_DDL_COMMAND,
                                      TYPE_ID_PG_SNAPSHOT,
                                      TYPE_ID_POINT,
                                      TYPE_ID_POLY,
                                      TYPE_ID_REGCLASS,
                                      TYPE_ID_REGCOLLATION,
                                      TYPE_ID_REGCONFIG,
                                      TYPE_ID_REGDICTIONARY,
                                      TYPE_ID_REGNAMESPACE,
                                      TYPE_ID_REGOPERATOR,
                                      TYPE_ID_REGOPER,
                                      TYPE_ID_REGPROCEDURE,
                                      TYPE_ID_REGPROC,
                                      TYPE_ID_REGROLE,
                                      TYPE_ID_REGTYPE,
                                      TYPE_ID_TEXT,
                                      TYPE_ID_TID,
                                      TYPE_ID_TIME,
                                      TYPE_ID_TIMESTAMP,
                                      TYPE_ID_TIMESTAMPTZ,
                                      TYPE_ID_TIMETZ,
                                      TYPE_ID_TSQUERY,
                                      TYPE_ID_TSVECTOR,
                                      TYPE_ID_TXID_SNAPSHOT,
                                      TYPE_ID_UNKNOWN,
                                      TYPE_ID_UUID,
                                      TYPE_ID_VARBIT,
                                      TYPE_ID_VARCHAR,
                                      TYPE_ID_VOID,
                                      TYPE_ID_XID8,
                                      TYPE_ID_XID,
                                      TYPE_ID_XML};

  if (nested) {
    base.push_back(TYPE_ID_ARRAY);
    base.push_back(TYPE_ID_RECORD);
    base.push_back(TYPE_ID_RANGE);
    base.push_back(TYPE_ID_DOMAIN);
  }

  return base;
}

}  // namespace adbcpq
