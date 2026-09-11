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

#include "postgres_type.h"

#include <cstdint>
#include <string>
#include <utility>
#include <vector>

#include "postgresql/result_helper.h"

namespace adbcpq {

static Status InsertPgAttributeResult(const PqResultHelper& result,
                                      PostgresTypeResolver& resolver) {
  int num_rows = result.NumRows();
  std::vector<std::pair<std::string, uint32_t>> columns;
  int64_t current_type_oid = 0;

  if (result.NumColumns() != 3) {
    return Status::Internal(
        "Expected 3 columns from type resolver pg_attribute query but got ",
        result.NumColumns());
  }

  for (int row = 0; row < num_rows; row++) {
    PqResultRow item = result.Row(row);
    int64_t type_oid;
    UNWRAP_RESULT(type_oid, item[0].ParseInteger());
    std::string_view col_name = item[1].value();
    int64_t col_oid;
    UNWRAP_RESULT(col_oid, item[2].ParseInteger());

    if (type_oid != current_type_oid && !columns.empty()) {
      resolver.InsertClass(static_cast<uint32_t>(current_type_oid), columns);
      columns.clear();
      current_type_oid = type_oid;
    }

    columns.push_back({std::string(col_name), static_cast<uint32_t>(col_oid)});
  }

  if (!columns.empty()) {
    resolver.InsertClass(static_cast<uint32_t>(current_type_oid), columns);
  }

  return Status::Ok();
}

static Status InsertPgTypeResult(const PqResultHelper& result,
                                 PostgresTypeResolver& resolver) {
  if (result.NumColumns() != 5 && result.NumColumns() != 6) {
    return Status::Internal(
        "Expected 5 or 6 columns from type resolver pg_type query but got ",
        result.NumColumns());
  }

  int num_rows = result.NumRows();
  int num_cols = result.NumColumns();
  PostgresTypeResolver::Item type_item;

  for (int row = 0; row < num_rows; row++) {
    PqResultRow item = result.Row(row);
    int64_t oid;
    UNWRAP_RESULT(oid, item[0].ParseInteger());
    const char* typname = item[1].data;
    const char* typreceive = item[2].data;
    int64_t typbasetype;
    UNWRAP_RESULT(typbasetype, item[3].ParseInteger());
    int64_t typrelid;
    UNWRAP_RESULT(typrelid, item[4].ParseInteger());

    int64_t typarray;
    if (num_cols == 6) {
      UNWRAP_RESULT(typarray, item[5].ParseInteger());
    } else {
      typarray = 0;
    }

    // Special case the aclitem because it shows up in a bunch of internal tables
    if (strcmp(typname, "aclitem") == 0) {
      typreceive = "aclitem_recv";
    }

    type_item.oid = static_cast<uint32_t>(oid);
    type_item.typname = typname;
    type_item.typreceive = typreceive;
    type_item.class_oid = static_cast<uint32_t>(typrelid);
    type_item.base_oid = static_cast<uint32_t>(typbasetype);

    // XXX: it seems to be intentional that we ignore errors here?
    int insert_result = resolver.Insert(type_item, nullptr);

    // If there's an array type and the insert succeeded, add that now too
    if (insert_result == NANOARROW_OK && typarray != 0) {
      std::string array_typname = "_" + std::string(typname);
      type_item.oid = static_cast<uint32_t>(typarray);
      type_item.typname = array_typname.c_str();
      type_item.typreceive = "array_recv";
      type_item.child_oid = static_cast<uint32_t>(oid);

      resolver.Insert(type_item, nullptr);
    }
  }

  return Status::Ok();
}

Status RebuildTypeResolver(PGconn* conn, PostgresTypeResolver& resolver) {
  // We need a few queries to build the resolver. The current strategy might
  // fail for some recursive definitions (e.g., arrays of records of arrays).
  // First, one on the pg_attribute table to resolve column names/oids for
  // record types.
  const std::string kColumnsQuery = R"(
SELECT
    attrelid,
    attname,
    atttypid
FROM
    pg_catalog.pg_attribute
ORDER BY
    attrelid, attnum
)";

  // Second, a query of the pg_type table. This query may need a few attempts to handle
  // recursive definitions (e.g., record types with array column). This currently won't
  // handle range types because those rows don't have child OID information. Arrays types
  // are inserted after a successful insert of the element type.
  std::string type_query =
      "SELECT oid, typname, typreceive, typbasetype, typrelid, typarray FROM "
      "pg_catalog.pg_type WHERE (typreceive != 0 OR typsend != 0) AND typtype != 'r' AND "
      "typreceive::TEXT != 'array_recv'";

  // Insert record type definitions (this includes table schemas)
  PqResultHelper columns(conn, kColumnsQuery.c_str());
  UNWRAP_STATUS(columns.Execute());
  UNWRAP_STATUS(InsertPgAttributeResult(columns, resolver));

  // Attempt filling the resolver a few times to handle recursive definitions.
  int32_t max_attempts = 3;
  PqResultHelper types(conn, type_query);
  for (int32_t i = 0; i < max_attempts; i++) {
    UNWRAP_STATUS(types.Execute());
    UNWRAP_STATUS(InsertPgTypeResult(types, resolver));
  }

  return Status::Ok();
}

}  // namespace adbcpq

Status InternalAdbcRebuildTypeResolver(PGconn* conn,
                                       adbcpq::PostgresTypeResolver& resolver) {
  return adbcpq::RebuildTypeResolver(conn, resolver);
}

Status InternalAdbcInitializeTypeResolver(adbcpq::PostgresTypeResolver& resolver) {
  return adbcpq::InitializeTypeResolver(resolver);
}
