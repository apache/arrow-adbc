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

#include <nanoarrow/nanoarrow.h>

#include <adbc.h>

#if defined(ADBC_BUILDING_TESTS)
#define ADBC_EXPORT_TEST ADBC_EXPORT
#else
#define ADBC_EXPORT_TEST
#endif

namespace adbcpq {

// An enum of the types available in most Postgres pg_type tables
enum class PostgresTypeId {
  UNINITIALIZED,
  ACLITEM,
  ANYARRAY,
  ANYCOMPATIBLEARRAY,
  ARRAY,
  BIT,
  BOOL,
  BOX,
  BPCHAR,
  BRIN_BLOOM_SUMMARY,
  BRIN_MINMAX_MULTI_SUMMARY,
  BYTEA,
  CASH,
  CHAR,
  CIDR,
  CID,
  CIRCLE,
  CSTRING,
  DATE,
  DOMAIN_,
  FLOAT4,
  FLOAT8,
  INET,
  INT2,
  INT2VECTOR,
  INT4,
  INT8,
  INTERVAL,
  JSON,
  JSONB,
  JSONPATH,
  LINE,
  LSEG,
  MACADDR,
  MACADDR8,
  MULTIRANGE,
  NAME,
  NUMERIC,
  OID,
  OIDVECTOR,
  PATH,
  PG_DDL_COMMAND,
  PG_DEPENDENCIES,
  PG_LSN,
  PG_MCV_LIST,
  PG_NDISTINCT,
  PG_NODE_TREE,
  PG_SNAPSHOT,
  POINT,
  POLY,
  RANGE,
  RECORD,
  REGCLASS,
  REGCOLLATION,
  REGCONFIG,
  REGDICTIONARY,
  REGNAMESPACE,
  REGOPERATOR,
  REGOPER,
  REGPROCEDURE,
  REGPROC,
  REGROLE,
  REGTYPE,
  TEXT,
  TID,
  TIME,
  TIMESTAMP,
  TIMESTAMPTZ,
  TIMETZ,
  TSQUERY,
  TSVECTOR,
  TXID_SNAPSHOT,
  UNKNOWN,
  UUID,
  VARBIT,
  VARCHAR,
  VOID,
  XID8,
  XID,
  XML
};

// Returns the receive function name as defined in the typrecieve column
// of the pg_type table. This name is the one that gets used to look up
// the PostgresTypeId.
const char* PostgresTyprecv(PostgresTypeId type_id);

// Returns a likely typname value for a given PostgresTypeId. This is useful
// for testing and error messages but may not be the actual value present
// in the pg_type typname column.
const char* PostgresTypname(PostgresTypeId type_id);

// A vector of all type IDs, optionally including the nested types PostgresTypeId::ARRAY,
// PostgresTypeId::DOMAIN_, PostgresTypeId::RECORD, and PostgresTypeId::RANGE.
std::vector<PostgresTypeId> PostgresTypeIdAll(bool nested = true);

// Forward-declare the type resolver for use in PostgresType::FromSchema
class PostgresTypeResolver;

// An abstraction of a (potentially nested and/or parameterized) Postgres
// data type. This class is where default type conversion to/from Arrow
// is defined. It is intentionally copyable.
class PostgresType {
 public:
  explicit PostgresType(PostgresTypeId type_id) : oid_(0), type_id_(type_id) {}

  PostgresType() : PostgresType(PostgresTypeId::UNINITIALIZED) {}

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
    PostgresType out(PostgresTypeId::ARRAY);
    out.AppendChild("item", *this);
    out.oid_ = oid;
    out.typname_ = typname;
    return out;
  }

  PostgresType Domain(uint32_t oid, const std::string& typname) {
    return WithPgTypeInfo(oid, typname);
  }

  PostgresType Range(uint32_t oid = 0, const std::string& typname = "") const {
    PostgresType out(PostgresTypeId::RANGE);
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
  ADBC_EXPORT_TEST ArrowErrorCode SetSchema(ArrowSchema* schema) const;

  ADBC_EXPORT_TEST static ArrowErrorCode FromSchema(const PostgresTypeResolver& resolver,
                                                    ArrowSchema* schema,
                                                    PostgresType* out, ArrowError* error);

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
  ADBC_EXPORT_TEST ArrowErrorCode Insert(const Item& item, ArrowError* error);

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

}  // namespace adbcpq
