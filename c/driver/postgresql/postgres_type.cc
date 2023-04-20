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

#include <string>
#include <utility>
#include <vector>

#include <nanoarrow/nanoarrow.hpp>

#include "postgres_type.h"

namespace adbcpq {

ADBC_EXPORT_TEST ArrowErrorCode PostgresType::SetSchema(ArrowSchema* schema) const {
  switch (type_id_) {
    case PostgresTypeId::BOOL:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BOOL));
      break;
    case PostgresTypeId::INT2:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT16));
      break;
    case PostgresTypeId::INT4:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT32));
      break;
    case PostgresTypeId::INT8:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_INT64));
      break;
    case PostgresTypeId::FLOAT4:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_FLOAT));
      break;
    case PostgresTypeId::FLOAT8:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_DOUBLE));
      break;
    case PostgresTypeId::CHAR:
    case PostgresTypeId::BPCHAR:
    case PostgresTypeId::VARCHAR:
    case PostgresTypeId::TEXT:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_STRING));
      break;
    case PostgresTypeId::BYTEA:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetType(schema, NANOARROW_TYPE_BINARY));
      break;

    case PostgresTypeId::RECORD:
      NANOARROW_RETURN_NOT_OK(ArrowSchemaSetTypeStruct(schema, n_children()));
      for (int64_t i = 0; i < n_children(); i++) {
        NANOARROW_RETURN_NOT_OK(children_[i].SetSchema(schema->children[i]));
      }
      break;

    case PostgresTypeId::ARRAY:
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

ADBC_EXPORT_TEST ArrowErrorCode
PostgresType::FromSchema(const PostgresTypeResolver& resolver, ArrowSchema* schema,
                         PostgresType* out, ArrowError* error) {
  ArrowSchemaView schema_view;
  NANOARROW_RETURN_NOT_OK(ArrowSchemaViewInit(&schema_view, schema, error));

  switch (schema_view.type) {
    case NANOARROW_TYPE_BOOL:
      return resolver.Find(resolver.GetOID(PostgresTypeId::BOOL), out, error);
    case NANOARROW_TYPE_INT8:
    case NANOARROW_TYPE_UINT8:
    case NANOARROW_TYPE_INT16:
      return resolver.Find(resolver.GetOID(PostgresTypeId::INT2), out, error);
    case NANOARROW_TYPE_UINT16:
    case NANOARROW_TYPE_INT32:
      return resolver.Find(resolver.GetOID(PostgresTypeId::INT4), out, error);
    case NANOARROW_TYPE_UINT32:
    case NANOARROW_TYPE_INT64:
      return resolver.Find(resolver.GetOID(PostgresTypeId::INT8), out, error);
    case NANOARROW_TYPE_FLOAT:
      return resolver.Find(resolver.GetOID(PostgresTypeId::FLOAT4), out, error);
    case NANOARROW_TYPE_DOUBLE:
      return resolver.Find(resolver.GetOID(PostgresTypeId::FLOAT8), out, error);
    case NANOARROW_TYPE_STRING:
      return resolver.Find(resolver.GetOID(PostgresTypeId::TEXT), out, error);
    case NANOARROW_TYPE_BINARY:
    case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      return resolver.Find(resolver.GetOID(PostgresTypeId::BYTEA), out, error);
    case NANOARROW_TYPE_LIST:
    case NANOARROW_TYPE_LARGE_LIST:
    case NANOARROW_TYPE_FIXED_SIZE_LIST: {
      PostgresType child;
      NANOARROW_RETURN_NOT_OK(
          PostgresType::FromSchema(resolver, schema->children[0], &child, error));
      return resolver.FindArray(child.oid(), out, error);
    }

    default:
      ArrowErrorSet(error, "Can't map Arrow type '%s' to Postgres type",
                    ArrowTypeString(schema_view.type));
      return ENOTSUP;
  }
}

ADBC_EXPORT_TEST ArrowErrorCode PostgresTypeResolver::Insert(const Item& item,
                                                             ArrowError* error) {
  auto result = base_.find(item.typreceive);
  if (result == base_.end()) {
    ArrowErrorSet(error, "Base type not found for type '%s' with receive function '%s'",
                  item.typname, item.typreceive);
    return ENOTSUP;
  }

  const PostgresType& base = result->second;
  PostgresType type = base.WithPgTypeInfo(item.oid, item.typname);

  switch (base.type_id()) {
    case PostgresTypeId::ARRAY: {
      PostgresType child;
      NANOARROW_RETURN_NOT_OK(Find(item.child_oid, &child, error));
      mapping_.insert({item.oid, child.Array(item.oid, item.typname)});
      reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
      array_mapping_.insert({child.oid(), item.oid});
      break;
    }

    case PostgresTypeId::RECORD: {
      std::vector<std::pair<std::string, uint32_t>> child_desc;
      NANOARROW_RETURN_NOT_OK(ResolveClass(item.class_oid, &child_desc, error));

      PostgresType out(PostgresTypeId::RECORD);
      for (const auto& child_item : child_desc) {
        PostgresType child;
        NANOARROW_RETURN_NOT_OK(Find(child_item.second, &child, error));
        out.AppendChild(child_item.first, child);
      }

      mapping_.insert({item.oid, out.WithPgTypeInfo(item.oid, item.typname)});
      reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
      break;
    }

    case PostgresTypeId::DOMAIN_: {
      PostgresType base_type;
      NANOARROW_RETURN_NOT_OK(Find(item.base_oid, &base_type, error));
      mapping_.insert({item.oid, base_type.Domain(item.oid, item.typname)});
      reverse_mapping_.insert({static_cast<int32_t>(base.type_id()), item.oid});
      break;
    }

    case PostgresTypeId::RANGE: {
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

ADBC_EXPORT_TEST const char* PostgresTyprecv(PostgresTypeId type_id) {
  switch (type_id) {
    case PostgresTypeId::ACLITEM:
      return "aclitem_recv";
    case PostgresTypeId::ANYARRAY:
      return "anyarray_recv";
    case PostgresTypeId::ANYCOMPATIBLEARRAY:
      return "anycompatiblearray_recv";
    case PostgresTypeId::ARRAY:
      return "array_recv";
    case PostgresTypeId::BIT:
      return "bit_recv";
    case PostgresTypeId::BOOL:
      return "boolrecv";
    case PostgresTypeId::BOX:
      return "box_recv";
    case PostgresTypeId::BPCHAR:
      return "bpcharrecv";
    case PostgresTypeId::BRIN_BLOOM_SUMMARY:
      return "brin_bloom_summary_recv";
    case PostgresTypeId::BRIN_MINMAX_MULTI_SUMMARY:
      return "brin_minmax_multi_summary_recv";
    case PostgresTypeId::BYTEA:
      return "bytearecv";
    case PostgresTypeId::CASH:
      return "cash_recv";
    case PostgresTypeId::CHAR:
      return "charrecv";
    case PostgresTypeId::CIDR:
      return "cidr_recv";
    case PostgresTypeId::CID:
      return "cidrecv";
    case PostgresTypeId::CIRCLE:
      return "circle_recv";
    case PostgresTypeId::CSTRING:
      return "cstring_recv";
    case PostgresTypeId::DATE:
      return "date_recv";
    case PostgresTypeId::DOMAIN_:
      return "domain_recv";
    case PostgresTypeId::FLOAT4:
      return "float4recv";
    case PostgresTypeId::FLOAT8:
      return "float8recv";
    case PostgresTypeId::INET:
      return "inet_recv";
    case PostgresTypeId::INT2:
      return "int2recv";
    case PostgresTypeId::INT2VECTOR:
      return "int2vectorrecv";
    case PostgresTypeId::INT4:
      return "int4recv";
    case PostgresTypeId::INT8:
      return "int8recv";
    case PostgresTypeId::INTERVAL:
      return "interval_recv";
    case PostgresTypeId::JSON:
      return "json_recv";
    case PostgresTypeId::JSONB:
      return "jsonb_recv";
    case PostgresTypeId::JSONPATH:
      return "jsonpath_recv";
    case PostgresTypeId::LINE:
      return "line_recv";
    case PostgresTypeId::LSEG:
      return "lseg_recv";
    case PostgresTypeId::MACADDR:
      return "macaddr_recv";
    case PostgresTypeId::MACADDR8:
      return "macaddr8_recv";
    case PostgresTypeId::MULTIRANGE:
      return "multirange_recv";
    case PostgresTypeId::NAME:
      return "namerecv";
    case PostgresTypeId::NUMERIC:
      return "numeric_recv";
    case PostgresTypeId::OID:
      return "oidrecv";
    case PostgresTypeId::OIDVECTOR:
      return "oidvectorrecv";
    case PostgresTypeId::PATH:
      return "path_recv";
    case PostgresTypeId::PG_NODE_TREE:
      return "pg_node_tree_recv";
    case PostgresTypeId::PG_NDISTINCT:
      return "pg_ndistinct_recv";
    case PostgresTypeId::PG_DEPENDENCIES:
      return "pg_dependencies_recv";
    case PostgresTypeId::PG_LSN:
      return "pg_lsn_recv";
    case PostgresTypeId::PG_MCV_LIST:
      return "pg_mcv_list_recv";
    case PostgresTypeId::PG_DDL_COMMAND:
      return "pg_ddl_command_recv";
    case PostgresTypeId::PG_SNAPSHOT:
      return "pg_snapshot_recv";
    case PostgresTypeId::POINT:
      return "point_recv";
    case PostgresTypeId::POLY:
      return "poly_recv";
    case PostgresTypeId::RANGE:
      return "range_recv";
    case PostgresTypeId::RECORD:
      return "record_recv";
    case PostgresTypeId::REGCLASS:
      return "regclassrecv";
    case PostgresTypeId::REGCOLLATION:
      return "regcollationrecv";
    case PostgresTypeId::REGCONFIG:
      return "regconfigrecv";
    case PostgresTypeId::REGDICTIONARY:
      return "regdictionaryrecv";
    case PostgresTypeId::REGNAMESPACE:
      return "regnamespacerecv";
    case PostgresTypeId::REGOPERATOR:
      return "regoperatorrecv";
    case PostgresTypeId::REGOPER:
      return "regoperrecv";
    case PostgresTypeId::REGPROCEDURE:
      return "regprocedurerecv";
    case PostgresTypeId::REGPROC:
      return "regprocrecv";
    case PostgresTypeId::REGROLE:
      return "regrolerecv";
    case PostgresTypeId::REGTYPE:
      return "regtyperecv";
    case PostgresTypeId::TEXT:
      return "textrecv";
    case PostgresTypeId::TID:
      return "tidrecv";
    case PostgresTypeId::TIME:
      return "time_recv";
    case PostgresTypeId::TIMESTAMP:
      return "timestamp_recv";
    case PostgresTypeId::TIMESTAMPTZ:
      return "timestamptz_recv";
    case PostgresTypeId::TIMETZ:
      return "timetz_recv";
    case PostgresTypeId::TSQUERY:
      return "tsqueryrecv";
    case PostgresTypeId::TSVECTOR:
      return "tsvectorrecv";
    case PostgresTypeId::TXID_SNAPSHOT:
      return "txid_snapshot_recv";
    case PostgresTypeId::UNKNOWN:
      return "unknownrecv";
    case PostgresTypeId::UUID:
      return "uuid_recv";
    case PostgresTypeId::VARBIT:
      return "varbit_recv";
    case PostgresTypeId::VARCHAR:
      return "varcharrecv";
    case PostgresTypeId::VOID:
      return "void_recv";
    case PostgresTypeId::XID8:
      return "xid8recv";
    case PostgresTypeId::XID:
      return "xidrecv";
    case PostgresTypeId::XML:
      return "xml_recv";
    default:
      return "";
  }
}

ADBC_EXPORT_TEST const char* PostgresTypname(PostgresTypeId type_id) {
  switch (type_id) {
    case PostgresTypeId::ACLITEM:
      return "aclitem";
    case PostgresTypeId::ANYARRAY:
      return "anyarray";
    case PostgresTypeId::ANYCOMPATIBLEARRAY:
      return "anycompatiblearray";
    case PostgresTypeId::ARRAY:
      return "array";
    case PostgresTypeId::BIT:
      return "bit";
    case PostgresTypeId::BOOL:
      return "bool";
    case PostgresTypeId::BOX:
      return "box";
    case PostgresTypeId::BPCHAR:
      return "bpchar";
    case PostgresTypeId::BRIN_BLOOM_SUMMARY:
      return "brin_bloom_summary";
    case PostgresTypeId::BRIN_MINMAX_MULTI_SUMMARY:
      return "brin_minmax_multi_summary";
    case PostgresTypeId::BYTEA:
      return "bytea";
    case PostgresTypeId::CASH:
      return "cash";
    case PostgresTypeId::CHAR:
      return "char";
    case PostgresTypeId::CIDR:
      return "cidr";
    case PostgresTypeId::CID:
      return "cid";
    case PostgresTypeId::CIRCLE:
      return "circle";
    case PostgresTypeId::CSTRING:
      return "cstring";
    case PostgresTypeId::DATE:
      return "date";
    case PostgresTypeId::DOMAIN_:
      return "domain";
    case PostgresTypeId::FLOAT4:
      return "float4";
    case PostgresTypeId::FLOAT8:
      return "float8";
    case PostgresTypeId::INET:
      return "inet";
    case PostgresTypeId::INT2:
      return "int2";
    case PostgresTypeId::INT2VECTOR:
      return "int2vector";
    case PostgresTypeId::INT4:
      return "int4";
    case PostgresTypeId::INT8:
      return "int8";
    case PostgresTypeId::INTERVAL:
      return "interval";
    case PostgresTypeId::JSON:
      return "json";
    case PostgresTypeId::JSONB:
      return "jsonb";
    case PostgresTypeId::JSONPATH:
      return "jsonpath";
    case PostgresTypeId::LINE:
      return "line";
    case PostgresTypeId::LSEG:
      return "lseg";
    case PostgresTypeId::MACADDR:
      return "macaddr";
    case PostgresTypeId::MACADDR8:
      return "macaddr8";
    case PostgresTypeId::MULTIRANGE:
      return "multirange";
    case PostgresTypeId::NAME:
      return "name";
    case PostgresTypeId::NUMERIC:
      return "numeric";
    case PostgresTypeId::OID:
      return "oid";
    case PostgresTypeId::OIDVECTOR:
      return "oidvector";
    case PostgresTypeId::PATH:
      return "path";
    case PostgresTypeId::PG_NODE_TREE:
      return "pg_node_tree";
    case PostgresTypeId::PG_NDISTINCT:
      return "pg_ndistinct";
    case PostgresTypeId::PG_DEPENDENCIES:
      return "pg_dependencies";
    case PostgresTypeId::PG_LSN:
      return "pg_lsn";
    case PostgresTypeId::PG_MCV_LIST:
      return "pg_mcv_list";
    case PostgresTypeId::PG_DDL_COMMAND:
      return "pg_ddl_command";
    case PostgresTypeId::PG_SNAPSHOT:
      return "pg_snapshot";
    case PostgresTypeId::POINT:
      return "point";
    case PostgresTypeId::POLY:
      return "poly";
    case PostgresTypeId::RANGE:
      return "range";
    case PostgresTypeId::RECORD:
      return "record";
    case PostgresTypeId::REGCLASS:
      return "regclass";
    case PostgresTypeId::REGCOLLATION:
      return "regcollation";
    case PostgresTypeId::REGCONFIG:
      return "regconfig";
    case PostgresTypeId::REGDICTIONARY:
      return "regdictionary";
    case PostgresTypeId::REGNAMESPACE:
      return "regnamespace";
    case PostgresTypeId::REGOPERATOR:
      return "regoperator";
    case PostgresTypeId::REGOPER:
      return "regoper";
    case PostgresTypeId::REGPROCEDURE:
      return "regprocedure";
    case PostgresTypeId::REGPROC:
      return "regproc";
    case PostgresTypeId::REGROLE:
      return "regrole";
    case PostgresTypeId::REGTYPE:
      return "regtype";
    case PostgresTypeId::TEXT:
      return "text";
    case PostgresTypeId::TID:
      return "tid";
    case PostgresTypeId::TIME:
      return "time";
    case PostgresTypeId::TIMESTAMP:
      return "timestamp";
    case PostgresTypeId::TIMESTAMPTZ:
      return "timestamptz";
    case PostgresTypeId::TIMETZ:
      return "timetz";
    case PostgresTypeId::TSQUERY:
      return "tsquery";
    case PostgresTypeId::TSVECTOR:
      return "tsvector";
    case PostgresTypeId::TXID_SNAPSHOT:
      return "txid_snapshot";
    case PostgresTypeId::UNKNOWN:
      return "unknown";
    case PostgresTypeId::UUID:
      return "uuid";
    case PostgresTypeId::VARBIT:
      return "varbit";
    case PostgresTypeId::VARCHAR:
      return "varchar";
    case PostgresTypeId::VOID:
      return "void";
    case PostgresTypeId::XID8:
      return "xid8";
    case PostgresTypeId::XID:
      return "xid";
    case PostgresTypeId::XML:
      return "xml";
    default:
      return "";
  }
}

ADBC_EXPORT_TEST std::vector<PostgresTypeId> PostgresTypeIdAll(bool nested) {
  std::vector<PostgresTypeId> base = {PostgresTypeId::ACLITEM,
                                      PostgresTypeId::ANYARRAY,
                                      PostgresTypeId::ANYCOMPATIBLEARRAY,
                                      PostgresTypeId::BIT,
                                      PostgresTypeId::BOOL,
                                      PostgresTypeId::BOX,
                                      PostgresTypeId::BPCHAR,
                                      PostgresTypeId::BRIN_BLOOM_SUMMARY,
                                      PostgresTypeId::BRIN_MINMAX_MULTI_SUMMARY,
                                      PostgresTypeId::BYTEA,
                                      PostgresTypeId::CASH,
                                      PostgresTypeId::CHAR,
                                      PostgresTypeId::CIDR,
                                      PostgresTypeId::CID,
                                      PostgresTypeId::CIRCLE,
                                      PostgresTypeId::CSTRING,
                                      PostgresTypeId::DATE,
                                      PostgresTypeId::FLOAT4,
                                      PostgresTypeId::FLOAT8,
                                      PostgresTypeId::INET,
                                      PostgresTypeId::INT2,
                                      PostgresTypeId::INT2VECTOR,
                                      PostgresTypeId::INT4,
                                      PostgresTypeId::INT8,
                                      PostgresTypeId::INTERVAL,
                                      PostgresTypeId::JSON,
                                      PostgresTypeId::JSONB,
                                      PostgresTypeId::JSONPATH,
                                      PostgresTypeId::LINE,
                                      PostgresTypeId::LSEG,
                                      PostgresTypeId::MACADDR,
                                      PostgresTypeId::MACADDR8,
                                      PostgresTypeId::MULTIRANGE,
                                      PostgresTypeId::NAME,
                                      PostgresTypeId::NUMERIC,
                                      PostgresTypeId::OID,
                                      PostgresTypeId::OIDVECTOR,
                                      PostgresTypeId::PATH,
                                      PostgresTypeId::PG_NODE_TREE,
                                      PostgresTypeId::PG_NDISTINCT,
                                      PostgresTypeId::PG_DEPENDENCIES,
                                      PostgresTypeId::PG_LSN,
                                      PostgresTypeId::PG_MCV_LIST,
                                      PostgresTypeId::PG_DDL_COMMAND,
                                      PostgresTypeId::PG_SNAPSHOT,
                                      PostgresTypeId::POINT,
                                      PostgresTypeId::POLY,
                                      PostgresTypeId::REGCLASS,
                                      PostgresTypeId::REGCOLLATION,
                                      PostgresTypeId::REGCONFIG,
                                      PostgresTypeId::REGDICTIONARY,
                                      PostgresTypeId::REGNAMESPACE,
                                      PostgresTypeId::REGOPERATOR,
                                      PostgresTypeId::REGOPER,
                                      PostgresTypeId::REGPROCEDURE,
                                      PostgresTypeId::REGPROC,
                                      PostgresTypeId::REGROLE,
                                      PostgresTypeId::REGTYPE,
                                      PostgresTypeId::TEXT,
                                      PostgresTypeId::TID,
                                      PostgresTypeId::TIME,
                                      PostgresTypeId::TIMESTAMP,
                                      PostgresTypeId::TIMESTAMPTZ,
                                      PostgresTypeId::TIMETZ,
                                      PostgresTypeId::TSQUERY,
                                      PostgresTypeId::TSVECTOR,
                                      PostgresTypeId::TXID_SNAPSHOT,
                                      PostgresTypeId::UNKNOWN,
                                      PostgresTypeId::UUID,
                                      PostgresTypeId::VARBIT,
                                      PostgresTypeId::VARCHAR,
                                      PostgresTypeId::VOID,
                                      PostgresTypeId::XID8,
                                      PostgresTypeId::XID,
                                      PostgresTypeId::XML};

  if (nested) {
    base.push_back(PostgresTypeId::ARRAY);
    base.push_back(PostgresTypeId::RECORD);
    base.push_back(PostgresTypeId::RANGE);
    base.push_back(PostgresTypeId::DOMAIN_);
  }

  return base;
}

}  // namespace adbcpq
