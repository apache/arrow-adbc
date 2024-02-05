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
  kBool = 16,
  kBytea,
  kChar,
  kName,
  kInt8,
  kInt2,
  kInt2vector,
  kInt4,
  kRegproc,
  kText,
  kOid,
  kTid,
  kXid,
  kCid,
  kOidvector,
  kSmgr = 210,
  kFloat4 = 701,
  kFloat8,
  kAbstime,
  kUnknown = 705,
  kBpchar = 1042,
  kVarchar,
  kDate = 1082,
  kTime,
  kTimestamp,
  kInterval = 1186,
  kTimetz = 1266,
  kNumeric = 1700,
  kInt1 = 2500,
  kNchar = 2522,
  kNvarchar = 2530,
  kStgeometry = 2552,
  kVarbinary = 2568,
  kUnkbinary = 2569,
  kJson = 2652,
  kJsonb,
  kJsonpath
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

  NetezzaType() : NetezzaType(NetezzaTypeId::kUnknown) {}

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

  NetezzaType Domain(uint32_t oid, const std::string& typname) {
    return WithPgTypeInfo(oid, typname);
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
      case NetezzaTypeId::kNchar:
      case NetezzaTypeId::kNvarchar:
      case NetezzaTypeId::kText:
      case NetezzaTypeId::kName:
      case NetezzaTypeId::kJson:
      case NetezzaTypeId::kJsonb:
      case NetezzaTypeId::kJsonpath:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING));
        break;
      case NetezzaTypeId::kBytea:
      case NetezzaTypeId::kInt1:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
        break;

      // ---- Temporal --------------------
      case NetezzaTypeId::kDate:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_DATE32));
        break;

      case NetezzaTypeId::kTime:
      case NetezzaTypeId::kTimetz:
      case NetezzaTypeId::kAbstime:
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

      case NetezzaTypeId::kInterval:
        NANOARROW_RETURN_NOT_OK(
            ArrowSchemaSetType(schema, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO));
        break;

      // ---- Nested --------------------
      case NetezzaTypeId::kOidvector:
      case NetezzaTypeId::kStgeometry:
      case NetezzaTypeId::kVarbinary:
      case NetezzaTypeId::kUnkbinary:
      default: {
        // For user-defined types or types we don't explicitly know how to deal with, we
        // can still return the bytes postgres gives us and attach the type name as
        // metadata
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
        nanoarrow::UniqueBuffer buffer;
        ArrowMetadataBuilderInit(buffer.get(), nullptr);
        NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderAppend(
            buffer.get(), ArrowCharView("ADBC:netezza:typname"),
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
      base = NetezzaType(NetezzaTypeId::kUnknown);
    } else {
      base = result->second;
    }

    NetezzaType type = base.WithPgTypeInfo(item.oid, item.typname);

    switch (base.type_id()) {
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
    case NetezzaTypeId::kBool:
      return "BOOLIN";
    case NetezzaTypeId::kBytea:
      return "BYTEAIN";
    case NetezzaTypeId::kChar:
      return "CHARIN";
    case NetezzaTypeId::kName:
      return "NAMEIN";
    case NetezzaTypeId::kInt8:
      return "INT8IN";
    case NetezzaTypeId::kInt2:
      return "INT2IN";
    case NetezzaTypeId::kInt2vector:
      return "INT2VECTORIN";
    case NetezzaTypeId::kInt4:
      return "INT4IN";
    case NetezzaTypeId::kRegproc:
      return "REGPROCIN";
    case NetezzaTypeId::kText:
      return "TEXTIN";
    case NetezzaTypeId::kOid:
      return "OIDIN";
    case NetezzaTypeId::kTid:
      return "TIDIN";
    case NetezzaTypeId::kXid:
      return "XIDIN";
    case NetezzaTypeId::kCid:
      return "CIDIN";
    case NetezzaTypeId::kOidvector:
      return "OIDVECTORIN";
    case NetezzaTypeId::kSmgr:
      return "SMGRIN";
    case NetezzaTypeId::kFloat4:
      return "FLOAT4IN";
    case NetezzaTypeId::kFloat8:
      return "FLOAT8IN";
    case NetezzaTypeId::kAbstime:
      return "NABSTIMEIN";
    case NetezzaTypeId::kUnknown:
      return "TEXTIN";
    case NetezzaTypeId::kBpchar:
      return "BPCHARIN";
    case NetezzaTypeId::kVarchar:
      return "VARCHARIN";
    case NetezzaTypeId::kDate:
      return "DATE_IN";
    case NetezzaTypeId::kTime:
      return "TIME_IN";
    case NetezzaTypeId::kTimestamp:
      return "TIMESTAMP_IN";
    case NetezzaTypeId::kInterval:
      return "INTERVAL_IN";
    case NetezzaTypeId::kTimetz:
      return "TIMETZ_IN";
    case NetezzaTypeId::kNumeric:
      return "NUMERIC_IN";
    case NetezzaTypeId::kInt1:
      return "INT1IN";
    case NetezzaTypeId::kNchar:
      return "NCHARIN";
    case NetezzaTypeId::kNvarchar:
      return "NVARCHARIN";
    case NetezzaTypeId::kStgeometry:
      return "GEOMETRYIN";
    case NetezzaTypeId::kVarbinary:
      return "VARBINARYIN";
    case NetezzaTypeId::kUnkbinary:
      return "VARBINARYIN";
    case NetezzaTypeId::kJson:
      return "JSON_IN";
    case NetezzaTypeId::kJsonb:
      return "JSONB_IN";
    case NetezzaTypeId::kJsonpath:
      return "JSONPATH_IN";
    default:
      return "";
  }
}

static inline const char* NetezzaTypname(NetezzaTypeId type_id) {
  // TODO: Remove the (Postgres) datatypes which are not applicable.
  switch (type_id) {
    case NetezzaTypeId::kBool:
      return "BOOL";
    case NetezzaTypeId::kBytea:
      return "BYTEA";
    case NetezzaTypeId::kChar:
      return "CHAR";
    case NetezzaTypeId::kName:
      return "NAME";
    case NetezzaTypeId::kInt8:
      return "INT8";
    case NetezzaTypeId::kInt2:
      return "INT2";
    case NetezzaTypeId::kInt2vector:
      return "INT2VECTOR";
    case NetezzaTypeId::kInt4:
      return "INT4";
    case NetezzaTypeId::kRegproc:
      return "REGPROC";
    case NetezzaTypeId::kText:
      return "TEXT";
    case NetezzaTypeId::kOid:
      return "OID";
    case NetezzaTypeId::kTid:
      return "TID";
    case NetezzaTypeId::kXid:
      return "XID";
    case NetezzaTypeId::kCid:
      return "CID";
    case NetezzaTypeId::kOidvector:
      return "OICVECTOR";
    case NetezzaTypeId::kSmgr:
      return "SMGR";
    case NetezzaTypeId::kFloat4:
      return "FLOAT4";
    case NetezzaTypeId::kFloat8:
      return "FLOAT8";
    case NetezzaTypeId::kAbstime:
      return "ABSTIME";
    case NetezzaTypeId::kUnknown:
      return "UNKNOWN";
    case NetezzaTypeId::kBpchar:
      return "BPCHAR";
    case NetezzaTypeId::kVarchar:
      return "VARCHAR";
    case NetezzaTypeId::kDate:
      return "DATE";
    case NetezzaTypeId::kTime:
      return "TIME";
    case NetezzaTypeId::kTimestamp:
      return "TIMESTAMP";
    case NetezzaTypeId::kInterval:
      return "INTERVAL";
    case NetezzaTypeId::kTimetz:
      return "TIMETZ";
    case NetezzaTypeId::kNumeric:
      return "NUMERIC";
    case NetezzaTypeId::kInt1:
      return "INT1";
    case NetezzaTypeId::kNchar:
      return "NCHAR";
    case NetezzaTypeId::kNvarchar:
      return "NVARCHAR";
    case NetezzaTypeId::kStgeometry:
      return "ST_GEOMETRY";
    case NetezzaTypeId::kVarbinary:
      return "VARBINARY";
    case NetezzaTypeId::kUnkbinary:
      return "UNKBINARY";
    case NetezzaTypeId::kJson:
      return "JSON";
    case NetezzaTypeId::kJsonb:
      return "JSONB";
    case NetezzaTypeId::kJsonpath:
      return "JSONBPATH";
    default:
      return "";
  }
}

static inline std::vector<NetezzaTypeId> NetezzaTypeIdAll(bool nested) {
  std::vector<NetezzaTypeId> base = {
                                      NetezzaTypeId::kBool,
                                      NetezzaTypeId::kBytea,
                                      NetezzaTypeId::kChar,
                                      NetezzaTypeId::kName,
                                      NetezzaTypeId::kInt8,
                                      NetezzaTypeId::kInt2,
                                      NetezzaTypeId::kInt2vector,
                                      NetezzaTypeId::kInt4,
                                      NetezzaTypeId::kRegproc,
                                      NetezzaTypeId::kText,
                                      NetezzaTypeId::kOid,
                                      NetezzaTypeId::kTid,
                                      NetezzaTypeId::kXid,
                                      NetezzaTypeId::kCid,
                                      NetezzaTypeId::kOidvector,
                                      NetezzaTypeId::kSmgr,
                                      NetezzaTypeId::kFloat4,
                                      NetezzaTypeId::kFloat8,
                                      NetezzaTypeId::kAbstime,
                                      NetezzaTypeId::kUnknown,
                                      NetezzaTypeId::kBpchar,
                                      NetezzaTypeId::kVarchar,
                                      NetezzaTypeId::kDate,
                                      NetezzaTypeId::kTime,
                                      NetezzaTypeId::kTimestamp,
                                      NetezzaTypeId::kInterval,
                                      NetezzaTypeId::kTimetz,
                                      NetezzaTypeId::kNumeric,
                                      NetezzaTypeId::kInt1,
                                      NetezzaTypeId::kNchar,
                                      NetezzaTypeId::kNvarchar,
                                      NetezzaTypeId::kStgeometry,
                                      NetezzaTypeId::kVarbinary,
                                      NetezzaTypeId::kUnkbinary,
                                      NetezzaTypeId::kJson,
                                      NetezzaTypeId::kJsonb,
                                      NetezzaTypeId::kJsonpath};

  return base;
}

}  // namespace adbcpq
