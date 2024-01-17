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
enum class NetezzaTypeId {
  kUninitialized,
  kAclitem,
  kAnyarray,
  kAnycompatiblearray,
  kArray,
  kBit,
  kBool,
  kBox,
  kBpchar,
  kBrinBloomSummary,
  kBrinMinmaxMultiSummary,
  kBytea,
  kCash,
  kChar,
  kCidr,
  kCid,
  kCircle,
  kCstring,
  kDate,
  kDomain,
  kFloat4,
  kFloat8,
  kInet,
  kInt2,
  kInt2vector,
  kInt4,
  kInt8,
  kInterval,
  kJson,
  kJsonb,
  kJsonpath,
  kLine,
  kLseg,
  kMacaddr,
  kMacaddr8,
  kMultirange,
  kName,
  kNumeric,
  kOid,
  kOidvector,
  kPath,
  kPgDdlCommand,
  kPgDependencies,
  kPgLsn,
  kPgMcvList,
  kPgNdistinct,
  kPgNodeTree,
  kPgSnapshot,
  kPoint,
  kPoly,
  kRange,
  kRecord,
  kRegclass,
  kRegcollation,
  kRegconfig,
  kRegdictionary,
  kRegnamespace,
  kRegoperator,
  kRegoper,
  kRegprocedure,
  kRegproc,
  kRegrole,
  kRegtype,
  kText,
  kTid,
  kTime,
  kTimestamp,
  kTimestamptz,
  kTimetz,
  kTsquery,
  kTsvector,
  kTxidSnapshot,
  kUnknown,
  kUuid,
  kVarbit,
  kVarchar,
  kVoid,
  kXid8,
  kXid,
  kXml,
  kUserDefined
};

// Returns the receive function name as defined in the typrecieve column
// of the pg_type table. This name is the one that gets used to look up
// the PostgresTypeId.
static inline const char* NetezzaTyprecv(NetezzaTypeId type_id);

// Returns a likely typname value for a given PostgresTypeId. This is useful
// for testing and error messages but may not be the actual value present
// in the pg_type typname column.
static inline const char* NetezzaTypname(NetezzaTypeId type_id);

// A vector of all type IDs, optionally including the nested types PostgresTypeId::ARRAY,
// PostgresTypeId::DOMAIN_, PostgresTypeId::RECORD, and PostgresTypeId::RANGE.
static inline std::vector<NetezzaTypeId> NetezzaTypeIdAll(bool nested = true);

class NetezzaTypeResolver;

// An abstraction of a (potentially nested and/or parameterized) Postgres
// data type. This class is where default type conversion to/from Arrow
// is defined. It is intentionally copyable.
class NetezzaType {
 public:
  explicit NetezzaType(NetezzaTypeId type_id) : oid_(0), type_id_(type_id) {}

  NetezzaType() : NetezzaType(NetezzaTypeId::kUninitialized) {}

  void AppendChild(const std::string& field_name, const NetezzaType& type) {
    NetezzaType child(type);
    children_.push_back(child.WithFieldName(field_name));
  }

  NetezzaType WithFieldName(const std::string& field_name) const {
    NetezzaType out(*this);
    out.field_name_ = field_name;
    return out;
  }

  NetezzaType WithPgTypeInfo(uint32_t oid, const std::string& typname) const {
    NetezzaType out(*this);
    out.oid_ = oid;
    out.typname_ = typname;
    return out;
  }

  NetezzaType Array(uint32_t oid = 0, const std::string& typname = "") const {
    NetezzaType out(NetezzaTypeId::kArray);
    out.AppendChild("item", *this);
    out.oid_ = oid;
    out.typname_ = typname;
    return out;
  }

  NetezzaType Domain(uint32_t oid, const std::string& typname) {
    return WithPgTypeInfo(oid, typname);
  }

  NetezzaType Range(uint32_t oid = 0, const std::string& typname = "") const {
    NetezzaType out(NetezzaTypeId::kRange);
    out.AppendChild("item", *this);
    out.oid_ = oid;
    out.typname_ = typname;
    return out;
  }

  uint32_t oid() const { return oid_; }
  NetezzaTypeId type_id() const { return type_id_; }
  const std::string& typname() const { return typname_; }
  const std::string& field_name() const { return field_name_; }
  int64_t n_children() const { return static_cast<int64_t>(children_.size()); }
  const NetezzaType& child(int64_t i) const { return children_[i]; }

  // Sets appropriate fields of an ArrowSchema that has been initialized using
  // ArrowSchemaInit. This is a recursive operation (i.e., nested types will
  // initialize and set the appropriate number of children). Returns NANOARROW_OK
  // on success and perhaps ENOMEM if memory cannot be allocated. Types that
  // do not have a corresponding Arrow type are returned as Binary with field
  // metadata ADBC:posgresql:typname. These types can be represented as their
  // binary COPY representation in the output.
  ArrowErrorCode SetSchema(ArrowSchema* schema) const {
    switch (type_id_) {
      // ---- Primitive types --------------------
      case NetezzaTypeId::kBool:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BOOL));
        break;
      case NetezzaTypeId::kInt2:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT16));
        break;
      case NetezzaTypeId::kInt4:
      case NetezzaTypeId::kOid:
      case NetezzaTypeId::kRegproc:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT32));
        break;
      case NetezzaTypeId::kInt8:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT64));
        break;
      case NetezzaTypeId::kFloat4:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_FLOAT));
        break;
      case NetezzaTypeId::kFloat8:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_DOUBLE));
        break;

      // ---- Numeric/Decimal-------------------
      case NetezzaTypeId::kNumeric:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING));
        break;

      // ---- Binary/string --------------------
      case NetezzaTypeId::kChar:
      case NetezzaTypeId::kBpchar:
      case NetezzaTypeId::kVarchar:
      case NetezzaTypeId::kText:
      case NetezzaTypeId::kName:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING));
        break;
      case NetezzaTypeId::kBytea:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
        break;

      // ---- Temporal --------------------
      case NetezzaTypeId::kDate:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_DATE32));
        break;

      case NetezzaTypeId::kTime:
        // We always return microsecond precision even if the type
        // specifies differently
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIME64,
                                                           NANOARROW_TIME_UNIT_MICRO,
                                                           /*timezone=*/nullptr));
        break;

      case NetezzaTypeId::kTimestamp:
        // We always return microsecond precision even if the type
        // specifies differently
        NANOARROW_RETURN_NOT_OK(
            ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, /*timezone=*/nullptr));
        break;

      case NetezzaTypeId::kTimestamptz:
        NANOARROW_RETURN_NOT_OK(
            ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, /*timezone=*/"UTC"));
        break;

      case NetezzaTypeId::kInterval:
        NANOARROW_RETURN_NOT_OK(
            ArrowSchemaSetType(schema, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO));
        break;

      // ---- Nested --------------------
      case NetezzaTypeId::kRecord:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(schema, n_children()));
        for (int64_t i = 0; i < n_children(); i++) {
          NANOARROW_RETURN_NOT_OK(children_[i].SetSchema(schema->children[i]));
        }
        break;

      case NetezzaTypeId::kArray:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_LIST));
        NANOARROW_RETURN_NOT_OK(children_[0].SetSchema(schema->children[0]));
        break;

      case NetezzaTypeId::kUserDefined:
      default: {
        // For user-defined types or types we don't explicitly know how to deal with, we
        // can still return the bytes postgres gives us and attach the type name as
        // metadata
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
        nanoarrow::UniqueBuffer buffer;
        ArrowMetadataBuilderInit(buffer.get(), nullptr);
        NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderAppend(
            buffer.get(), ArrowCharView("ADBC:postgresql:typname"),
            ArrowCharView(typname_.c_str())));
        NANOARROW_RETURN_NOT_OK(
            ArrowSchemaSetMetadata(schema, reinterpret_cast<char*>(buffer->data)));
        break;
      }
    }

    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema, field_name_.c_str()));
    return NANOARROW_OK;
  }

  static ArrowErrorCode FromSchema(const NetezzaTypeResolver& resolver,
                                   ArrowSchema* schema, NetezzaType* out,
                                   ArrowError* error);

 private:
  uint32_t oid_;
  NetezzaTypeId type_id_;
  std::string typname_;
  std::string field_name_;
  std::vector<NetezzaType> children_;
};

// Because type information is stored in a database's pg_type table, it can't
// truly be resolved until runtime; however, querying the database's pg_type table
// for every result is unlikely to be reasonable. This class is a cache of information
// from the pg_type table with appropriate lookup tables to resolve a PostgresType
// instance based on a oid (which is the information that libpq provides when
// inspecting a result object). Types can be added/removed from the pg_type table
// via SQL, so this cache may need to be periodically refreshed.
class NetezzaTypeResolver {
 public:
  struct Item {
    uint32_t oid;
    const char* typname;
    const char* typreceive;
    uint32_t child_oid;
    uint32_t base_oid;
    uint32_t class_oid;
  };

  NetezzaTypeResolver() : base_(AllBase()) {}

  // Place a resolved copy of a PostgresType with the appropriate oid in type_out
  // if NANOARROW_OK is returned or place a null-terminated error message into error
  // otherwise.
  ArrowErrorCode Find(uint32_t oid, NetezzaType* type_out, ArrowError* error) const {
    auto result = mapping_.find(oid);
    if (result == mapping_.end()) {
      ArrowErrorSet(error, "Postgres type with oid %ld not found",
                    static_cast<long>(oid));  // NOLINT(runtime/int)
      return EINVAL;
    }

    *type_out = (*result).second;
    return NANOARROW_OK;
  }

  ArrowErrorCode FindArray(uint32_t child_oid, NetezzaType* type_out,
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
  uint32_t GetOID(NetezzaTypeId type_id) const {
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
    NetezzaType base;

    if (result == base_.end()) {
      // This occurs when a user-defined type has defined a custom receive function
      // (e.g., PostGIS/geometry). The only way these types can be supported is
      // by returning binary unless we hard-code support for some extensions.
      base = NetezzaType(NetezzaTypeId::kUserDefined);
    } else {
      base = result->second;
    }

    NetezzaType type = base.WithPgTypeInfo(item.oid, item.typname);

    switch (base.type_id()) {
      case NetezzaTypeId::kArray: {
        NetezzaType child;
        NANOARROW_RETURN_NOT_OK(Find(item.child_oid, &child, error));
        mapping_.insert({item.oid, child.Array(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        array_mapping_.insert({child.oid(), item.oid});
        break;
      }

      case NetezzaTypeId::kRecord: {
        std::vector<std::pair<std::string, uint32_t>> child_desc;
        NANOARROW_RETURN_NOT_OK(ResolveClass(item.class_oid, &child_desc, error));

        NetezzaType out(NetezzaTypeId::kRecord);
        for (const auto& child_item : child_desc) {
          NetezzaType child;
          NANOARROW_RETURN_NOT_OK(Find(child_item.second, &child, error));
          out.AppendChild(child_item.first, child);
        }

        mapping_.insert({item.oid, out.WithPgTypeInfo(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        break;
      }

      case NetezzaTypeId::kDomain: {
        NetezzaType base_type;
        NANOARROW_RETURN_NOT_OK(Find(item.base_oid, &base_type, error));
        mapping_.insert({item.oid, base_type.Domain(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        break;
      }

      case NetezzaTypeId::kRange: {
        NetezzaType base_type;
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
  std::unordered_map<uint32_t, NetezzaType> mapping_;
  // We can't use PostgresTypeId as an unordered map key because there is no
  // built-in hasher for an enum on gcc 4.8 (i.e., R 3.6 on Windows).
  std::unordered_map<int32_t, uint32_t> reverse_mapping_;
  std::unordered_map<uint32_t, uint32_t> array_mapping_;
  std::unordered_map<uint32_t, std::vector<std::pair<std::string, uint32_t>>> classes_;
  std::unordered_map<std::string, NetezzaType> base_;

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
  static std::unordered_map<std::string, NetezzaType> AllBase() {
    std::unordered_map<std::string, NetezzaType> out;
    for (NetezzaTypeId type_id : NetezzaTypeIdAll()) {
      NetezzaType type(type_id);
      out.insert(
          {NetezzaTyprecv(type_id), type.WithPgTypeInfo(0, NetezzaTypname(type_id))});
    }

    return out;
  }
};

inline ArrowErrorCode NetezzaType::FromSchema(const NetezzaTypeResolver& resolver,
                                               ArrowSchema* schema, NetezzaType* out,
                                               ArrowError* error) {
  ArrowSchemaView schema_view;
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view, schema, error));

  switch (schema_view.type) {
    case NANOARROW_TYPE_BOOL:
      return resolver.Find(resolver.GetOID(NetezzaTypeId::kBool), out, error);
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_INT16:
      return resolver.Find(resolver.GetOID(NetezzaTypeId::kInt2), out, error);
    case NANOARROW_TYPE_UINT16:
    case NANOARROW_TYPE_INT32:
      return resolver.Find(resolver.GetOID(NetezzaTypeId::kInt4), out, error);
    case NANOARROW_TYPE_UINT32:
    case NANOARROW_TYPE_INT64:
      return resolver.Find(resolver.GetOID(NetezzaTypeId::kInt8), out, error);
    case NANOARROW_TYPE_FLOAT:
      return resolver.Find(resolver.GetOID(NetezzaTypeId::kFloat4), out, error);
    case NANOARROW_TYPE_DOUBLE:
      return resolver.Find(resolver.GetOID(NetezzaTypeId::kFloat8), out, error);
    case NANOARROW_TYPE_STRING:
      return resolver.Find(resolver.GetOID(NetezzaTypeId::kText), out, error);
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      return resolver.Find(resolver.GetOID(NetezzaTypeId::kBytea), out, error);
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_FIXED_SIZE_LIST: {
      NetezzaType child;
      NANOARROW_RETURN_NOT_OK(
          NetezzaType::FromSchema(resolver, schema->children[0], &child, error));
      return resolver.FindArray(child.oid(), out, error);
    }
    case NANOARROW_TYPE_DICTIONARY:
      // Dictionary arrays always resolve to the dictionary type when binding or ingesting
      return NetezzaType::FromSchema(resolver, schema->dictionary, out, error);

    default:
      ArrowErrorSet(error, "Can't map Arrow type '%s' to Postgres type",
                    ArrowTypeString(schema_view.type));
      return ENOTSUP;
  }
}

static inline const char* NetezzaTyprecv(NetezzaTypeId type_id) {
  // TODO: Remove the (Postgres) datatypes which are not applicable.
  switch (type_id) {
    case NetezzaTypeId::kAclitem:
      return "aclitem_recv";
    case NetezzaTypeId::kAnyarray:
      return "anyarray_recv";
    case NetezzaTypeId::kAnycompatiblearray:
      return "anycompatiblearray_recv";
    case NetezzaTypeId::kArray:
      return "ARRAY_IN";
    case NetezzaTypeId::kBit:
      return "bit_recv";
    case NetezzaTypeId::kBool:
      return "BOOLIN";
    case NetezzaTypeId::kBox:
      return "box_recv";
    case NetezzaTypeId::kBpchar:
      return "BPCHARIN";
    case NetezzaTypeId::kBrinBloomSummary:
      return "brin_bloom_summary_recv";
    case NetezzaTypeId::kBrinMinmaxMultiSummary:
      return "brin_minmax_multi_summary_recv";
    case NetezzaTypeId::kBytea:
      return "BYTEAIN";
    case NetezzaTypeId::kCash:
      return "cash_recv";
    case NetezzaTypeId::kChar:
      return "CHARIN";
    case NetezzaTypeId::kCidr:
      return "cidr_recv";
    case NetezzaTypeId::kCid:
      return "CIDIN";
    case NetezzaTypeId::kCircle:
      return "circle_recv";
    case NetezzaTypeId::kCstring:
      return "cstring_recv";
    case NetezzaTypeId::kDate:
      return "DATE_IN";
    case NetezzaTypeId::kDomain:
      return "domain_recv";
    case NetezzaTypeId::kFloat4:
      return "FLOAT4IN";
    case NetezzaTypeId::kFloat8:
      return "FLOAT8IN";
    case NetezzaTypeId::kInet:
      return "inet_recv";
    case NetezzaTypeId::kInt2:
      return "INT2IN";
    case NetezzaTypeId::kInt2vector:
      return "INT2VECTORIN";
    case NetezzaTypeId::kInt4:
      return "INT4IN";
    case NetezzaTypeId::kInt8:
      return "INT8IN";
    case NetezzaTypeId::kInterval:
      return "INTERVAL_IN";
    case NetezzaTypeId::kJson:
      return "JSON_IN";
    case NetezzaTypeId::kJsonb:
      return "JSONB_IN";
    case NetezzaTypeId::kJsonpath:
      return "JSONPATH_IN";
    case NetezzaTypeId::kLine:
      return "line_recv";
    case NetezzaTypeId::kLseg:
      return "lseg_recv";
    case NetezzaTypeId::kMacaddr:
      return "macaddr_recv";
    case NetezzaTypeId::kMacaddr8:
      return "macaddr8_recv";
    case NetezzaTypeId::kMultirange:
      return "multirange_recv";
    case NetezzaTypeId::kName:
      return "NAMEIN";
    case NetezzaTypeId::kNumeric:
      return "NUMERIC_IN";
    case NetezzaTypeId::kOid:
      return "OIDIN";
    case NetezzaTypeId::kOidvector:
      return "OIDVECTORIN";
    case NetezzaTypeId::kPath:
      return "path_recv";
    case NetezzaTypeId::kPgNodeTree:
      return "pg_node_tree_recv";
    case NetezzaTypeId::kPgNdistinct:
      return "pg_ndistinct_recv";
    case NetezzaTypeId::kPgDependencies:
      return "pg_dependencies_recv";
    case NetezzaTypeId::kPgLsn:
      return "pg_lsn_recv";
    case NetezzaTypeId::kPgMcvList:
      return "pg_mcv_list_recv";
    case NetezzaTypeId::kPgDdlCommand:
      return "pg_ddl_command_recv";
    case NetezzaTypeId::kPgSnapshot:
      return "pg_snapshot_recv";
    case NetezzaTypeId::kPoint:
      return "point_recv";
    case NetezzaTypeId::kPoly:
      return "poly_recv";
    case NetezzaTypeId::kRange:
      return "range_recv";
    case NetezzaTypeId::kRecord:
      return "record_recv";
    case NetezzaTypeId::kRegclass:
      return "regclassrecv";
    case NetezzaTypeId::kRegcollation:
      return "regcollationrecv";
    case NetezzaTypeId::kRegconfig:
      return "regconfigrecv";
    case NetezzaTypeId::kRegdictionary:
      return "regdictionaryrecv";
    case NetezzaTypeId::kRegnamespace:
      return "regnamespacerecv";
    case NetezzaTypeId::kRegoperator:
      return "regoperatorrecv";
    case NetezzaTypeId::kRegoper:
      return "regoperrecv";
    case NetezzaTypeId::kRegprocedure:
      return "regprocedurerecv";
    case NetezzaTypeId::kRegproc:
      return "REGPROCIN";
    case NetezzaTypeId::kRegrole:
      return "regrolerecv";
    case NetezzaTypeId::kRegtype:
      return "regtyperecv";
    case NetezzaTypeId::kText:
      return "TEXTIN";
    case NetezzaTypeId::kTid:
      return "TIDIN";
    case NetezzaTypeId::kTime:
      return "TIME_IN";
    case NetezzaTypeId::kTimestamp:
      return "TIMESTAMP_IN";
    case NetezzaTypeId::kTimestamptz:
      return "timestamptz_recv";
    case NetezzaTypeId::kTimetz:
      return "TIMETZ_IN";
    case NetezzaTypeId::kTsquery:
      return "tsqueryrecv";
    case NetezzaTypeId::kTsvector:
      return "tsvectorrecv";
    case NetezzaTypeId::kTxidSnapshot:
      return "txid_snapshot_recv";
    case NetezzaTypeId::kUnknown:
      return "TEXTIN";
    case NetezzaTypeId::kUuid:
      return "uuid_recv";
    case NetezzaTypeId::kVarbit:
      return "varbit_recv";
    case NetezzaTypeId::kVarchar:
      return "VARCHARIN";
    case NetezzaTypeId::kVoid:
      return "void_recv";
    case NetezzaTypeId::kXid8:
      return "xid8recv";
    case NetezzaTypeId::kXid:
      return "XIDIN";
    case NetezzaTypeId::kXml:
      return "xml_recv";
    default:
      return "";
  }
}

static inline const char* NetezzaTypname(NetezzaTypeId type_id) {
  // TODO: Remove the (Postgres) datatypes which are not applicable.
  switch (type_id) {
    case NetezzaTypeId::kAclitem:
      return "aclitem";
    case NetezzaTypeId::kAnyarray:
      return "anyarray";
    case NetezzaTypeId::kAnycompatiblearray:
      return "anycompatiblearray";
    case NetezzaTypeId::kArray:
      return "array";
    case NetezzaTypeId::kBit:
      return "bit";
    case NetezzaTypeId::kBool:
      return "BOOL";
    case NetezzaTypeId::kBox:
      return "box";
    case NetezzaTypeId::kBpchar:
      return "BPCHAR";
    case NetezzaTypeId::kBrinBloomSummary:
      return "brin_bloom_summary";
    case NetezzaTypeId::kBrinMinmaxMultiSummary:
      return "brin_minmax_multi_summary";
    case NetezzaTypeId::kBytea:
      return "BYTEA";
    case NetezzaTypeId::kCash:
      return "cash";
    case NetezzaTypeId::kChar:
      return "CHAR";
    case NetezzaTypeId::kCidr:
      return "cidr";
    case NetezzaTypeId::kCid:
      return "CID";
    case NetezzaTypeId::kCircle:
      return "circle";
    case NetezzaTypeId::kCstring:
      return "cstring";
    case NetezzaTypeId::kDate:
      return "DATE";
    case NetezzaTypeId::kDomain:
      return "domain";
    case NetezzaTypeId::kFloat4:
      return "FLOAT4";
    case NetezzaTypeId::kFloat8:
      return "FLOAT8";
    case NetezzaTypeId::kInet:
      return "inet";
    case NetezzaTypeId::kInt2:
      return "INT2";
    case NetezzaTypeId::kInt2vector:
      return "INT2VECTOR";
    case NetezzaTypeId::kInt4:
      return "INT4";
    case NetezzaTypeId::kInt8:
      return "INT8";
    case NetezzaTypeId::kInterval:
      return "INTERVAL";
    case NetezzaTypeId::kJson:
      return "JSON";
    case NetezzaTypeId::kJsonb:
      return "JSONB";
    case NetezzaTypeId::kJsonpath:
      return "JSONBPATH";
    case NetezzaTypeId::kLine:
      return "line";
    case NetezzaTypeId::kLseg:
      return "lseg";
    case NetezzaTypeId::kMacaddr:
      return "macaddr";
    case NetezzaTypeId::kMacaddr8:
      return "macaddr8";
    case NetezzaTypeId::kMultirange:
      return "multirange";
    case NetezzaTypeId::kName:
      return "NAME";
    case NetezzaTypeId::kNumeric:
      return "NUMERIC";
    case NetezzaTypeId::kOid:
      return "OID";
    case NetezzaTypeId::kOidvector:
      return "OICVECTOR";
    case NetezzaTypeId::kPath:
      return "path";
    case NetezzaTypeId::kPgNodeTree:
      return "pg_node_tree";
    case NetezzaTypeId::kPgNdistinct:
      return "pg_ndistinct";
    case NetezzaTypeId::kPgDependencies:
      return "pg_dependencies";
    case NetezzaTypeId::kPgLsn:
      return "pg_lsn";
    case NetezzaTypeId::kPgMcvList:
      return "pg_mcv_list";
    case NetezzaTypeId::kPgDdlCommand:
      return "pg_ddl_command";
    case NetezzaTypeId::kPgSnapshot:
      return "pg_snapshot";
    case NetezzaTypeId::kPoint:
      return "point";
    case NetezzaTypeId::kPoly:
      return "poly";
    case NetezzaTypeId::kRange:
      return "range";
    case NetezzaTypeId::kRecord:
      return "record";
    case NetezzaTypeId::kRegclass:
      return "regclass";
    case NetezzaTypeId::kRegcollation:
      return "regcollation";
    case NetezzaTypeId::kRegconfig:
      return "regconfig";
    case NetezzaTypeId::kRegdictionary:
      return "regdictionary";
    case NetezzaTypeId::kRegnamespace:
      return "regnamespace";
    case NetezzaTypeId::kRegoperator:
      return "regoperator";
    case NetezzaTypeId::kRegoper:
      return "regoper";
    case NetezzaTypeId::kRegprocedure:
      return "regprocedure";
    case NetezzaTypeId::kRegproc:
      return "REGPROC";
    case NetezzaTypeId::kRegrole:
      return "regrole";
    case NetezzaTypeId::kRegtype:
      return "regtype";
    case NetezzaTypeId::kText:
      return "TEXT";
    case NetezzaTypeId::kTid:
      return "TID";
    case NetezzaTypeId::kTime:
      return "TIME";
    case NetezzaTypeId::kTimestamp:
      return "TIMESTAMP";
    case NetezzaTypeId::kTimestamptz:
      return "timestamptz";
    case NetezzaTypeId::kTimetz:
      return "TIMETZ";
    case NetezzaTypeId::kTsquery:
      return "tsquery";
    case NetezzaTypeId::kTsvector:
      return "tsvector";
    case NetezzaTypeId::kTxidSnapshot:
      return "txid_snapshot";
    case NetezzaTypeId::kUnknown:
      return "UNKNOWN";
    case NetezzaTypeId::kUuid:
      return "uuid";
    case NetezzaTypeId::kVarbit:
      return "varbit";
    case NetezzaTypeId::kVarchar:
      return "VARCHAR";
    case NetezzaTypeId::kVoid:
      return "void";
    case NetezzaTypeId::kXid8:
      return "xid8";
    case NetezzaTypeId::kXid:
      return "XID";
    case NetezzaTypeId::kXml:
      return "xml";
    default:
      return "";
  }
}

static inline std::vector<NetezzaTypeId> NetezzaTypeIdAll(bool nested) {
  std::vector<NetezzaTypeId> base = {NetezzaTypeId::kAclitem,
                                      NetezzaTypeId::kAnyarray,
                                      NetezzaTypeId::kAnycompatiblearray,
                                      NetezzaTypeId::kBit,
                                      NetezzaTypeId::kBool,
                                      NetezzaTypeId::kBox,
                                      NetezzaTypeId::kBpchar,
                                      NetezzaTypeId::kBrinBloomSummary,
                                      NetezzaTypeId::kBrinMinmaxMultiSummary,
                                      NetezzaTypeId::kBytea,
                                      NetezzaTypeId::kCash,
                                      NetezzaTypeId::kChar,
                                      NetezzaTypeId::kCidr,
                                      NetezzaTypeId::kCid,
                                      NetezzaTypeId::kCircle,
                                      NetezzaTypeId::kCstring,
                                      NetezzaTypeId::kDate,
                                      NetezzaTypeId::kFloat4,
                                      NetezzaTypeId::kFloat8,
                                      NetezzaTypeId::kInet,
                                      NetezzaTypeId::kInt2,
                                      NetezzaTypeId::kInt2vector,
                                      NetezzaTypeId::kInt4,
                                      NetezzaTypeId::kInt8,
                                      NetezzaTypeId::kInterval,
                                      NetezzaTypeId::kJson,
                                      NetezzaTypeId::kJsonb,
                                      NetezzaTypeId::kJsonpath,
                                      NetezzaTypeId::kLine,
                                      NetezzaTypeId::kLseg,
                                      NetezzaTypeId::kMacaddr,
                                      NetezzaTypeId::kMacaddr8,
                                      NetezzaTypeId::kMultirange,
                                      NetezzaTypeId::kName,
                                      NetezzaTypeId::kNumeric,
                                      NetezzaTypeId::kOid,
                                      NetezzaTypeId::kOidvector,
                                      NetezzaTypeId::kPath,
                                      NetezzaTypeId::kPgNodeTree,
                                      NetezzaTypeId::kPgNdistinct,
                                      NetezzaTypeId::kPgDependencies,
                                      NetezzaTypeId::kPgLsn,
                                      NetezzaTypeId::kPgMcvList,
                                      NetezzaTypeId::kPgDdlCommand,
                                      NetezzaTypeId::kPgSnapshot,
                                      NetezzaTypeId::kPoint,
                                      NetezzaTypeId::kPoly,
                                      NetezzaTypeId::kRegclass,
                                      NetezzaTypeId::kRegcollation,
                                      NetezzaTypeId::kRegconfig,
                                      NetezzaTypeId::kRegdictionary,
                                      NetezzaTypeId::kRegnamespace,
                                      NetezzaTypeId::kRegoperator,
                                      NetezzaTypeId::kRegoper,
                                      NetezzaTypeId::kRegprocedure,
                                      NetezzaTypeId::kRegproc,
                                      NetezzaTypeId::kRegrole,
                                      NetezzaTypeId::kRegtype,
                                      NetezzaTypeId::kText,
                                      NetezzaTypeId::kTid,
                                      NetezzaTypeId::kTime,
                                      NetezzaTypeId::kTimestamp,
                                      NetezzaTypeId::kTimestamptz,
                                      NetezzaTypeId::kTimetz,
                                      NetezzaTypeId::kTsquery,
                                      NetezzaTypeId::kTsvector,
                                      NetezzaTypeId::kTxidSnapshot,
                                      NetezzaTypeId::kUnknown,
                                      NetezzaTypeId::kUuid,
                                      NetezzaTypeId::kVarbit,
                                      NetezzaTypeId::kVarchar,
                                      NetezzaTypeId::kVoid,
                                      NetezzaTypeId::kXid8,
                                      NetezzaTypeId::kXid,
                                      NetezzaTypeId::kXml};

  if (nested) {
    base.push_back(NetezzaTypeId::kArray);
    base.push_back(NetezzaTypeId::kRecord);
    base.push_back(NetezzaTypeId::kRange);
    base.push_back(NetezzaTypeId::kDomain);
  }

  return base;
}

}  // namespace adbcpq
