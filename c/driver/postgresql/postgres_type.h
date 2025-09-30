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
enum class PostgresTypeId {
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
  kEnum,
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
  kUserDefined,
  // This is not an actual type, but there are cases where all we have is an Oid
  // that was not inserted into the type resolver. We can't use "unknown" or "opaque"
  // or "void" because those names show up in actual pg_type tables.
  kUnnamedArrowOpaque
};

// Returns the receive function name as defined in the typrecieve column
// of the pg_type table. This name is the one that gets used to look up
// the PostgresTypeId.
static inline const char* PostgresTyprecv(PostgresTypeId type_id);

// Returns a likely typname value for a given PostgresTypeId. This is useful
// for testing and error messages but may not be the actual value present
// in the pg_type typname column.
static inline const char* PostgresTypname(PostgresTypeId type_id);

// A vector of all type IDs, optionally including the nested types PostgresTypeId::ARRAY,
// PostgresTypeId::DOMAIN_, PostgresTypeId::RECORD, and PostgresTypeId::RANGE.
static inline std::vector<PostgresTypeId> PostgresTypeIdAll(bool nested = true);

class PostgresTypeResolver;

// An abstraction of a (potentially nested and/or parameterized) Postgres
// data type. This class is where default type conversion to/from Arrow
// is defined. It is intentionally copyable.
class PostgresType {
 public:
  explicit PostgresType(PostgresTypeId type_id) : oid_(0), type_id_(type_id) {}

  PostgresType() : PostgresType(PostgresTypeId::kUninitialized) {}

  static PostgresType Unnamed(uint32_t oid) {
    return PostgresType(PostgresTypeId::kUnnamedArrowOpaque)
        .WithPgTypeInfo(oid, "unnamed<oid:" + std::to_string(oid) + ">");
  }

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
    PostgresType out(PostgresTypeId::kArray);
    out.AppendChild("item", *this);
    out.oid_ = oid;
    out.typname_ = typname;
    return out;
  }

  PostgresType Domain(uint32_t oid, const std::string& typname) {
    return WithPgTypeInfo(oid, typname);
  }

  PostgresType Range(uint32_t oid = 0, const std::string& typname = "") const {
    PostgresType out(PostgresTypeId::kRange);
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

  // The name used to communicate this type in a CREATE TABLE statement.
  // These are not necessarily the most idiomatic names to use but PostgreSQL
  // will accept typname() according to the "aliases" column in
  // https://www.postgresql.org/docs/current/datatype.html
  const std::string sql_type_name() const {
    switch (type_id_) {
      case PostgresTypeId::kArray:
        return children_[0].sql_type_name() + " ARRAY";
      default:
        return typname_;
    }
  }

  // Sets appropriate fields of an ArrowSchema that has been initialized using
  // ArrowSchemaInit. This is a recursive operation (i.e., nested types will
  // initialize and set the appropriate number of children). Returns NANOARROW_OK
  // on success and perhaps ENOMEM if memory cannot be allocated. Types that
  // do not have a corresponding Arrow type are returned as Binary with field
  // metadata ADBC:postgresql:typname. These types can be represented as their
  // binary COPY representation in the output.
  ArrowErrorCode SetSchema(ArrowSchema* schema,
                           const std::string& vendor_name = "PostgreSQL") const {
    switch (type_id_) {
      // ---- Primitive types --------------------
      case PostgresTypeId::kBool:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BOOL));
        break;
      case PostgresTypeId::kInt2:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT16));
        break;
      case PostgresTypeId::kInt4:
      case PostgresTypeId::kOid:
      case PostgresTypeId::kRegproc:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT32));
        break;
      case PostgresTypeId::kInt8:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT64));
        break;
      case PostgresTypeId::kFloat4:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_FLOAT));
        break;
      case PostgresTypeId::kFloat8:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_DOUBLE));
        break;
      case PostgresTypeId::kCash:
        // PostgreSQL appears to send an int64, without decimal point information
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT64));
        break;

      // ---- Numeric/Decimal-------------------
      case PostgresTypeId::kNumeric:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING));
        NANOARROW_RETURN_NOT_OK(AddPostgresTypeMetadata(schema, vendor_name));

        break;

      // ---- Binary/string --------------------
      case PostgresTypeId::kChar:
      case PostgresTypeId::kBpchar:
      case PostgresTypeId::kVarchar:
      case PostgresTypeId::kText:
      case PostgresTypeId::kName:
      case PostgresTypeId::kEnum:
      case PostgresTypeId::kJson:
      case PostgresTypeId::kJsonb:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING));
        break;
      case PostgresTypeId::kBytea:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
        break;

      // ---- Temporal --------------------
      case PostgresTypeId::kDate:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_DATE32));
        break;

      case PostgresTypeId::kTime:
        // We always return microsecond precision even if the type
        // specifies differently
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIME64,
                                                           NANOARROW_TIME_UNIT_MICRO,
                                                           /*timezone=*/nullptr));
        break;

      case PostgresTypeId::kTimestamp:
        // We always return microsecond precision even if the type
        // specifies differently
        NANOARROW_RETURN_NOT_OK(
            ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, /*timezone=*/nullptr));
        break;

      case PostgresTypeId::kTimestamptz:
        NANOARROW_RETURN_NOT_OK(
            ArrowSchemaSetTypeDateTime(schema, NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, /*timezone=*/"UTC"));
        break;

      case PostgresTypeId::kInterval:
        NANOARROW_RETURN_NOT_OK(
            ArrowSchemaSetType(schema, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO));
        break;

      // ---- Nested --------------------
      case PostgresTypeId::kRecord:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(schema, n_children()));
        for (int64_t i = 0; i < n_children(); i++) {
          NANOARROW_RETURN_NOT_OK(
              children_[i].SetSchema(schema->children[i], vendor_name));
        }
        break;

      case PostgresTypeId::kArray:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_LIST));
        NANOARROW_RETURN_NOT_OK(children_[0].SetSchema(schema->children[0], vendor_name));
        break;

      case PostgresTypeId::kInt2vector:
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_LIST));
        // Postgres conceives of this as a single type, so no child is
        // given. We need to allocate it ourselves.
        NANOARROW_RETURN_NOT_OK(
            ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT16));
        break;

      case PostgresTypeId::kUserDefined:
      default:
        // For user-defined types or types we don't explicitly know how to deal with, we
        // can still return the bytes postgres gives us and attach the type name as
        // metadata
        NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
        NANOARROW_RETURN_NOT_OK(AddPostgresTypeMetadata(schema, vendor_name));
        break;
    }

    NANOARROW_RETURN_NOT_OK(ArrowSchemaSetName(schema, field_name_.c_str()));
    return NANOARROW_OK;
  }

  static ArrowErrorCode FromSchema(const PostgresTypeResolver& resolver,
                                   ArrowSchema* schema, PostgresType* out,
                                   ArrowError* error);

 private:
  uint32_t oid_;
  PostgresTypeId type_id_;
  std::string typname_;
  std::string field_name_;
  std::vector<PostgresType> children_;

  static constexpr const char* kPostgresTypeKey = "ADBC:postgresql:typname";
  static constexpr const char* kExtensionName = "ARROW:extension:name";
  static constexpr const char* kOpaqueExtensionName = "arrow.opaque";
  static constexpr const char* kExtensionMetadata = "ARROW:extension:metadata";

  ArrowErrorCode AddPostgresTypeMetadata(ArrowSchema* schema,
                                         const std::string& vendor_name) const {
    // the typname_ may not always be set: an instance of this class can be
    // created with just the type id. That's why there is this here fallback to
    // resolve the type name of built-in types.
    const char* typname =
        !typname_.empty() ? typname_.c_str() : PostgresTypname(type_id_);
    nanoarrow::UniqueBuffer buffer;

    NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderInit(buffer.get(), nullptr));
    // TODO(lidavidm): we have deprecated this in favor of arrow.opaque,
    // remove once we feel enough time has passed
    NANOARROW_RETURN_NOT_OK(ArrowMetadataBuilderAppend(
        buffer.get(), ArrowCharView(kPostgresTypeKey), ArrowCharView(typname)));

    // Add the Opaque extension type metadata
    std::string metadata = R"({"type_name": ")";
    metadata += typname;
    metadata += R"(", "vendor_name": ")" + vendor_name + R"("})";
    NANOARROW_RETURN_NOT_OK(
        ArrowMetadataBuilderAppend(buffer.get(), ArrowCharView(kExtensionName),
                                   ArrowCharView(kOpaqueExtensionName)));
    NANOARROW_RETURN_NOT_OK(
        ArrowMetadataBuilderAppend(buffer.get(), ArrowCharView(kExtensionMetadata),
                                   ArrowStringView{
                                       metadata.c_str(),
                                       static_cast<int64_t>(metadata.size()),
                                   }));

    NANOARROW_RETURN_NOT_OK(
        ArrowSchemaSetMetadata(schema, reinterpret_cast<char*>(buffer->data)));

    return NANOARROW_OK;
  }
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

    *type_out = result->second;
    return NANOARROW_OK;
  }

  ArrowErrorCode FindWithDefault(uint32_t oid, PostgresType* type_out) {
    auto result = mapping_.find(oid);
    if (result == mapping_.end()) {
      *type_out = PostgresType::Unnamed(oid);
    } else {
      *type_out = result->second;
    }

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
    PostgresType base;

    if (result == base_.end()) {
      // This occurs when a user-defined type has defined a custom receive function
      // (e.g., PostGIS/geometry). The only way these types can be supported is
      // by returning binary unless we hard-code support for some extensions.
      base = PostgresType(PostgresTypeId::kUserDefined);
    } else {
      base = result->second;
    }

    PostgresType type = base.WithPgTypeInfo(item.oid, item.typname);

    switch (base.type_id()) {
      case PostgresTypeId::kArray: {
        PostgresType child;
        NANOARROW_RETURN_NOT_OK(Find(item.child_oid, &child, error));
        mapping_.insert({item.oid, child.Array(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        array_mapping_.insert({child.oid(), item.oid});
        break;
      }

      case PostgresTypeId::kRecord: {
        std::vector<std::pair<std::string, uint32_t>> child_desc;
        NANOARROW_RETURN_NOT_OK(ResolveClass(item.class_oid, &child_desc, error));

        PostgresType out(PostgresTypeId::kRecord);
        for (const auto& child_item : child_desc) {
          PostgresType child;
          NANOARROW_RETURN_NOT_OK(Find(child_item.second, &child, error));
          out.AppendChild(child_item.first, child);
        }

        mapping_.insert({item.oid, out.WithPgTypeInfo(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        break;
      }

      case PostgresTypeId::kDomain: {
        PostgresType base_type;
        NANOARROW_RETURN_NOT_OK(Find(item.base_oid, &base_type, error));
        mapping_.insert({item.oid, base_type.Domain(item.oid, item.typname)});
        reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
        break;
      }

      case PostgresTypeId::kRange: {
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

inline ArrowErrorCode PostgresType::FromSchema(const PostgresTypeResolver& resolver,
                                               ArrowSchema* schema, PostgresType* out,
                                               ArrowError* error) {
  ArrowSchemaView schema_view;
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view, schema, error));

  if (schema_view.extension_name.data != nullptr &&
      std::string_view(schema_view.extension_name.data,
                       schema_view.extension_name.size_bytes)
              .compare("arrow.json") == 0) {
    switch (schema_view.type) {
      case NANOARROW_TYPE_STRING:
      case NANOARROW_TYPE_LARGE_STRING:
      case NANOARROW_TYPE_STRING_VIEW:
        return resolver.Find(resolver.GetOID(PostgresTypeId::kJson), out, error);
      default:
        break;
    }
    ArrowErrorSet(
        error, "Field '%s' is of type arrow.json but storage type is not a string type",
        schema_view.schema->name);
    return EINVAL;
  }

  switch (schema_view.type) {
    case NANOARROW_TYPE_BOOL:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kBool), out, error);
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_INT16:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kInt2), out, error);
    case NANOARROW_TYPE_UINT16:
    case NANOARROW_TYPE_INT32:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kInt4), out, error);
    case NANOARROW_TYPE_UINT32:
    case NANOARROW_TYPE_INT64:
    case NANOARROW_TYPE_UINT64:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kInt8), out, error);
    case NANOARROW_TYPE_HALF_FLOAT:
    case NANOARROW_TYPE_FLOAT:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kFloat4), out, error);
    case NANOARROW_TYPE_DOUBLE:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kFloat8), out, error);
    case NANOARROW_TYPE_STRING:
    case NANOARROW_TYPE_LARGE_STRING:
    case NANOARROW_TYPE_STRING_VIEW:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kText), out, error);
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_LARGE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
    case NANOARROW_TYPE_BINARY_VIEW:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kBytea), out, error);
    case NANOARROW_TYPE_DATE32:
    case NANOARROW_TYPE_DATE64:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kDate), out, error);
    case NANOARROW_TYPE_TIME32:
    case NANOARROW_TYPE_TIME64:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kTime), out, error);
    case NANOARROW_TYPE_DURATION:
    case NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kInterval), out, error);
    case NANOARROW_TYPE_TIMESTAMP:
      if (std::string_view(schema_view.timezone).empty()) {
        return resolver.Find(resolver.GetOID(PostgresTypeId::kTimestamp), out, error);
      } else {
        return resolver.Find(resolver.GetOID(PostgresTypeId::kTimestamptz), out, error);
      }
    case NANOARROW_TYPE_DECIMAL128:
    case NANOARROW_TYPE_DECIMAL256:
      return resolver.Find(resolver.GetOID(PostgresTypeId::kNumeric), out, error);
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_FIXED_SIZE_LIST: {
      PostgresType child;
      NANOARROW_RETURN_NOT_OK(
          PostgresType::FromSchema(resolver, schema->children[0], &child, error));
      return resolver.FindArray(child.oid(), out, error);
    }
    case NANOARROW_TYPE_DICTIONARY:
      // Dictionary arrays always resolve to the dictionary type when binding or ingesting
      return PostgresType::FromSchema(resolver, schema->dictionary, out, error);

    default:
      ArrowErrorSet(error, "Can't map Arrow type '%s' to Postgres type",
                    ArrowTypeString(schema_view.type));
      return ENOTSUP;
  }
}

static inline const char* PostgresTyprecv(PostgresTypeId type_id) {
  switch (type_id) {
    case PostgresTypeId::kAclitem:
      return "aclitem_recv";
    case PostgresTypeId::kAnyarray:
      return "anyarray_recv";
    case PostgresTypeId::kAnycompatiblearray:
      return "anycompatiblearray_recv";
    case PostgresTypeId::kArray:
      return "array_recv";
    case PostgresTypeId::kBit:
      return "bit_recv";
    case PostgresTypeId::kBool:
      return "boolrecv";
    case PostgresTypeId::kBox:
      return "box_recv";
    case PostgresTypeId::kBpchar:
      return "bpcharrecv";
    case PostgresTypeId::kBrinBloomSummary:
      return "brin_bloom_summary_recv";
    case PostgresTypeId::kBrinMinmaxMultiSummary:
      return "brin_minmax_multi_summary_recv";
    case PostgresTypeId::kBytea:
      return "bytearecv";
    case PostgresTypeId::kCash:
      return "cash_recv";
    case PostgresTypeId::kChar:
      return "charrecv";
    case PostgresTypeId::kCidr:
      return "cidr_recv";
    case PostgresTypeId::kCid:
      return "cidrecv";
    case PostgresTypeId::kCircle:
      return "circle_recv";
    case PostgresTypeId::kCstring:
      return "cstring_recv";
    case PostgresTypeId::kDate:
      return "date_recv";
    case PostgresTypeId::kDomain:
      return "domain_recv";
    case PostgresTypeId::kEnum:
      return "enum_recv";
    case PostgresTypeId::kFloat4:
      return "float4recv";
    case PostgresTypeId::kFloat8:
      return "float8recv";
    case PostgresTypeId::kInet:
      return "inet_recv";
    case PostgresTypeId::kInt2:
      return "int2recv";
    case PostgresTypeId::kInt2vector:
      return "int2vectorrecv";
    case PostgresTypeId::kInt4:
      return "int4recv";
    case PostgresTypeId::kInt8:
      return "int8recv";
    case PostgresTypeId::kInterval:
      return "interval_recv";
    case PostgresTypeId::kJson:
      return "json_recv";
    case PostgresTypeId::kJsonb:
      return "jsonb_recv";
    case PostgresTypeId::kJsonpath:
      return "jsonpath_recv";
    case PostgresTypeId::kLine:
      return "line_recv";
    case PostgresTypeId::kLseg:
      return "lseg_recv";
    case PostgresTypeId::kMacaddr:
      return "macaddr_recv";
    case PostgresTypeId::kMacaddr8:
      return "macaddr8_recv";
    case PostgresTypeId::kMultirange:
      return "multirange_recv";
    case PostgresTypeId::kName:
      return "namerecv";
    case PostgresTypeId::kNumeric:
      return "numeric_recv";
    case PostgresTypeId::kOid:
      return "oidrecv";
    case PostgresTypeId::kOidvector:
      return "oidvectorrecv";
    case PostgresTypeId::kPath:
      return "path_recv";
    case PostgresTypeId::kPgNodeTree:
      return "pg_node_tree_recv";
    case PostgresTypeId::kPgNdistinct:
      return "pg_ndistinct_recv";
    case PostgresTypeId::kPgDependencies:
      return "pg_dependencies_recv";
    case PostgresTypeId::kPgLsn:
      return "pg_lsn_recv";
    case PostgresTypeId::kPgMcvList:
      return "pg_mcv_list_recv";
    case PostgresTypeId::kPgDdlCommand:
      return "pg_ddl_command_recv";
    case PostgresTypeId::kPgSnapshot:
      return "pg_snapshot_recv";
    case PostgresTypeId::kPoint:
      return "point_recv";
    case PostgresTypeId::kPoly:
      return "poly_recv";
    case PostgresTypeId::kRange:
      return "range_recv";
    case PostgresTypeId::kRecord:
      return "record_recv";
    case PostgresTypeId::kRegclass:
      return "regclassrecv";
    case PostgresTypeId::kRegcollation:
      return "regcollationrecv";
    case PostgresTypeId::kRegconfig:
      return "regconfigrecv";
    case PostgresTypeId::kRegdictionary:
      return "regdictionaryrecv";
    case PostgresTypeId::kRegnamespace:
      return "regnamespacerecv";
    case PostgresTypeId::kRegoperator:
      return "regoperatorrecv";
    case PostgresTypeId::kRegoper:
      return "regoperrecv";
    case PostgresTypeId::kRegprocedure:
      return "regprocedurerecv";
    case PostgresTypeId::kRegproc:
      return "regprocrecv";
    case PostgresTypeId::kRegrole:
      return "regrolerecv";
    case PostgresTypeId::kRegtype:
      return "regtyperecv";
    case PostgresTypeId::kText:
      return "textrecv";
    case PostgresTypeId::kTid:
      return "tidrecv";
    case PostgresTypeId::kTime:
      return "time_recv";
    case PostgresTypeId::kTimestamp:
      return "timestamp_recv";
    case PostgresTypeId::kTimestamptz:
      return "timestamptz_recv";
    case PostgresTypeId::kTimetz:
      return "timetz_recv";
    case PostgresTypeId::kTsquery:
      return "tsqueryrecv";
    case PostgresTypeId::kTsvector:
      return "tsvectorrecv";
    case PostgresTypeId::kTxidSnapshot:
      return "txid_snapshot_recv";
    case PostgresTypeId::kUnknown:
      return "unknownrecv";
    case PostgresTypeId::kUuid:
      return "uuid_recv";
    case PostgresTypeId::kVarbit:
      return "varbit_recv";
    case PostgresTypeId::kVarchar:
      return "varcharrecv";
    case PostgresTypeId::kVoid:
      return "void_recv";
    case PostgresTypeId::kXid8:
      return "xid8recv";
    case PostgresTypeId::kXid:
      return "xidrecv";
    case PostgresTypeId::kXml:
      return "xml_recv";
    default:
      return "";
  }
}

static inline const char* PostgresTypname(PostgresTypeId type_id) {
  switch (type_id) {
    case PostgresTypeId::kAclitem:
      return "aclitem";
    case PostgresTypeId::kAnyarray:
      return "anyarray";
    case PostgresTypeId::kAnycompatiblearray:
      return "anycompatiblearray";
    case PostgresTypeId::kArray:
      return "array";
    case PostgresTypeId::kBit:
      return "bit";
    case PostgresTypeId::kBool:
      return "bool";
    case PostgresTypeId::kBox:
      return "box";
    case PostgresTypeId::kBpchar:
      return "bpchar";
    case PostgresTypeId::kBrinBloomSummary:
      return "brin_bloom_summary";
    case PostgresTypeId::kBrinMinmaxMultiSummary:
      return "brin_minmax_multi_summary";
    case PostgresTypeId::kBytea:
      return "bytea";
    case PostgresTypeId::kCash:
      return "cash";
    case PostgresTypeId::kChar:
      return "char";
    case PostgresTypeId::kCidr:
      return "cidr";
    case PostgresTypeId::kCid:
      return "cid";
    case PostgresTypeId::kCircle:
      return "circle";
    case PostgresTypeId::kCstring:
      return "cstring";
    case PostgresTypeId::kDate:
      return "date";
    case PostgresTypeId::kDomain:
      return "domain";
    case PostgresTypeId::kEnum:
      return "enum";
    case PostgresTypeId::kFloat4:
      return "float4";
    case PostgresTypeId::kFloat8:
      return "float8";
    case PostgresTypeId::kInet:
      return "inet";
    case PostgresTypeId::kInt2:
      return "int2";
    case PostgresTypeId::kInt2vector:
      return "int2vector";
    case PostgresTypeId::kInt4:
      return "int4";
    case PostgresTypeId::kInt8:
      return "int8";
    case PostgresTypeId::kInterval:
      return "interval";
    case PostgresTypeId::kJson:
      return "json";
    case PostgresTypeId::kJsonb:
      return "jsonb";
    case PostgresTypeId::kJsonpath:
      return "jsonpath";
    case PostgresTypeId::kLine:
      return "line";
    case PostgresTypeId::kLseg:
      return "lseg";
    case PostgresTypeId::kMacaddr:
      return "macaddr";
    case PostgresTypeId::kMacaddr8:
      return "macaddr8";
    case PostgresTypeId::kMultirange:
      return "multirange";
    case PostgresTypeId::kName:
      return "name";
    case PostgresTypeId::kNumeric:
      return "numeric";
    case PostgresTypeId::kOid:
      return "oid";
    case PostgresTypeId::kOidvector:
      return "oidvector";
    case PostgresTypeId::kPath:
      return "path";
    case PostgresTypeId::kPgNodeTree:
      return "pg_node_tree";
    case PostgresTypeId::kPgNdistinct:
      return "pg_ndistinct";
    case PostgresTypeId::kPgDependencies:
      return "pg_dependencies";
    case PostgresTypeId::kPgLsn:
      return "pg_lsn";
    case PostgresTypeId::kPgMcvList:
      return "pg_mcv_list";
    case PostgresTypeId::kPgDdlCommand:
      return "pg_ddl_command";
    case PostgresTypeId::kPgSnapshot:
      return "pg_snapshot";
    case PostgresTypeId::kPoint:
      return "point";
    case PostgresTypeId::kPoly:
      return "poly";
    case PostgresTypeId::kRange:
      return "range";
    case PostgresTypeId::kRecord:
      return "record";
    case PostgresTypeId::kRegclass:
      return "regclass";
    case PostgresTypeId::kRegcollation:
      return "regcollation";
    case PostgresTypeId::kRegconfig:
      return "regconfig";
    case PostgresTypeId::kRegdictionary:
      return "regdictionary";
    case PostgresTypeId::kRegnamespace:
      return "regnamespace";
    case PostgresTypeId::kRegoperator:
      return "regoperator";
    case PostgresTypeId::kRegoper:
      return "regoper";
    case PostgresTypeId::kRegprocedure:
      return "regprocedure";
    case PostgresTypeId::kRegproc:
      return "regproc";
    case PostgresTypeId::kRegrole:
      return "regrole";
    case PostgresTypeId::kRegtype:
      return "regtype";
    case PostgresTypeId::kText:
      return "text";
    case PostgresTypeId::kTid:
      return "tid";
    case PostgresTypeId::kTime:
      return "time";
    case PostgresTypeId::kTimestamp:
      return "timestamp";
    case PostgresTypeId::kTimestamptz:
      return "timestamptz";
    case PostgresTypeId::kTimetz:
      return "timetz";
    case PostgresTypeId::kTsquery:
      return "tsquery";
    case PostgresTypeId::kTsvector:
      return "tsvector";
    case PostgresTypeId::kTxidSnapshot:
      return "txid_snapshot";
    case PostgresTypeId::kUnknown:
      return "unknown";
    case PostgresTypeId::kUuid:
      return "uuid";
    case PostgresTypeId::kVarbit:
      return "varbit";
    case PostgresTypeId::kVarchar:
      return "varchar";
    case PostgresTypeId::kVoid:
      return "void";
    case PostgresTypeId::kXid8:
      return "xid8";
    case PostgresTypeId::kXid:
      return "xid";
    case PostgresTypeId::kXml:
      return "xml";
    default:
      return "";
  }
}

static inline std::vector<PostgresTypeId> PostgresTypeIdAll(bool nested) {
  std::vector<PostgresTypeId> base = {PostgresTypeId::kAclitem,
                                      PostgresTypeId::kAnyarray,
                                      PostgresTypeId::kAnycompatiblearray,
                                      PostgresTypeId::kBit,
                                      PostgresTypeId::kBool,
                                      PostgresTypeId::kBox,
                                      PostgresTypeId::kBpchar,
                                      PostgresTypeId::kBrinBloomSummary,
                                      PostgresTypeId::kBrinMinmaxMultiSummary,
                                      PostgresTypeId::kBytea,
                                      PostgresTypeId::kCash,
                                      PostgresTypeId::kChar,
                                      PostgresTypeId::kCidr,
                                      PostgresTypeId::kCid,
                                      PostgresTypeId::kCircle,
                                      PostgresTypeId::kCstring,
                                      PostgresTypeId::kDate,
                                      PostgresTypeId::kEnum,
                                      PostgresTypeId::kFloat4,
                                      PostgresTypeId::kFloat8,
                                      PostgresTypeId::kInet,
                                      PostgresTypeId::kInt2,
                                      PostgresTypeId::kInt2vector,
                                      PostgresTypeId::kInt4,
                                      PostgresTypeId::kInt8,
                                      PostgresTypeId::kInterval,
                                      PostgresTypeId::kJson,
                                      PostgresTypeId::kJsonb,
                                      PostgresTypeId::kJsonpath,
                                      PostgresTypeId::kLine,
                                      PostgresTypeId::kLseg,
                                      PostgresTypeId::kMacaddr,
                                      PostgresTypeId::kMacaddr8,
                                      PostgresTypeId::kMultirange,
                                      PostgresTypeId::kName,
                                      PostgresTypeId::kNumeric,
                                      PostgresTypeId::kOid,
                                      PostgresTypeId::kOidvector,
                                      PostgresTypeId::kPath,
                                      PostgresTypeId::kPgNodeTree,
                                      PostgresTypeId::kPgNdistinct,
                                      PostgresTypeId::kPgDependencies,
                                      PostgresTypeId::kPgLsn,
                                      PostgresTypeId::kPgMcvList,
                                      PostgresTypeId::kPgDdlCommand,
                                      PostgresTypeId::kPgSnapshot,
                                      PostgresTypeId::kPoint,
                                      PostgresTypeId::kPoly,
                                      PostgresTypeId::kRegclass,
                                      PostgresTypeId::kRegcollation,
                                      PostgresTypeId::kRegconfig,
                                      PostgresTypeId::kRegdictionary,
                                      PostgresTypeId::kRegnamespace,
                                      PostgresTypeId::kRegoperator,
                                      PostgresTypeId::kRegoper,
                                      PostgresTypeId::kRegprocedure,
                                      PostgresTypeId::kRegproc,
                                      PostgresTypeId::kRegrole,
                                      PostgresTypeId::kRegtype,
                                      PostgresTypeId::kText,
                                      PostgresTypeId::kTid,
                                      PostgresTypeId::kTime,
                                      PostgresTypeId::kTimestamp,
                                      PostgresTypeId::kTimestamptz,
                                      PostgresTypeId::kTimetz,
                                      PostgresTypeId::kTsquery,
                                      PostgresTypeId::kTsvector,
                                      PostgresTypeId::kTxidSnapshot,
                                      PostgresTypeId::kUnknown,
                                      PostgresTypeId::kUuid,
                                      PostgresTypeId::kVarbit,
                                      PostgresTypeId::kVarchar,
                                      PostgresTypeId::kVoid,
                                      PostgresTypeId::kXid8,
                                      PostgresTypeId::kXid,
                                      PostgresTypeId::kXml};

  if (nested) {
    base.push_back(PostgresTypeId::kArray);
    base.push_back(PostgresTypeId::kRecord);
    base.push_back(PostgresTypeId::kRange);
    base.push_back(PostgresTypeId::kDomain);
  }

  return base;
}

}  // namespace adbcpq
