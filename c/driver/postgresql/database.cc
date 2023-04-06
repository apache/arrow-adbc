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

#include "database.h"

#include <cstring>
#include <memory>

#include <adbc.h>
#include <libpq-fe.h>
#include <nanoarrow/nanoarrow.h>

#include "util.h"

namespace adbcpq {

PostgresDatabase::PostgresDatabase() : open_connections_(0) {
  type_resolver_ = std::make_shared<PostgresTypeResolver>();
}
PostgresDatabase::~PostgresDatabase() = default;

AdbcStatusCode PostgresDatabase::Init(struct AdbcError* error) {
  // Connect to validate the parameters.
  PGconn* conn = nullptr;
  AdbcStatusCode final_status = Connect(&conn, error);
  if (final_status != ADBC_STATUS_OK) {
    return final_status;
  }

  // Build the type mapping table.
  const std::string kTypeQuery = R"(
SELECT
    oid,
    typname,
    typreceive
FROM
    pg_catalog.pg_type
WHERE
    typelem = 0 AND typrelid = 0 AND typbasetype = 0
)";

  pg_result* result = PQexec(conn, kTypeQuery.c_str());
  ExecStatusType pq_status = PQresultStatus(result);
  if (pq_status == PGRES_TUPLES_OK) {
    int num_rows = PQntuples(result);
    PostgresTypeResolver::Item item;

    for (int row = 0; row < num_rows; row++) {
      const uint32_t oid = static_cast<uint32_t>(
          std::strtol(PQgetvalue(result, row, 0), /*str_end=*/nullptr, /*base=*/10));
      const char* typname = PQgetvalue(result, row, 1);
      const char* typreceive = PQgetvalue(result, row, 2);

      item.oid = oid;
      item.typname = typname;
      item.typreceive = typreceive;

      // Intentionally ignoring types we don't know how to deal with. These will error
      // later if there is a query that actually contains them.
      type_resolver_->Insert(item, nullptr);
    }
  } else {
    SetError(error, "Failed to build type mapping table: ", PQerrorMessage(conn));
    final_status = ADBC_STATUS_IO;
  }
  PQclear(result);

  // Disconnect since PostgreSQL connections can be heavy.
  {
    AdbcStatusCode status = Disconnect(&conn, error);
    if (status != ADBC_STATUS_OK) final_status = status;
  }
  return final_status;
}

AdbcStatusCode PostgresDatabase::Release(struct AdbcError* error) {
  if (open_connections_ != 0) {
    SetError(error, "Database released with ", open_connections_, " open connections");
    return ADBC_STATUS_INVALID_STATE;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresDatabase::SetOption(const char* key, const char* value,
                                           struct AdbcError* error) {
  if (strcmp(key, "uri") == 0) {
    uri_ = value;
  } else {
    SetError(error, "Unknown database option ", key);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresDatabase::Connect(PGconn** conn, struct AdbcError* error) {
  if (uri_.empty()) {
    SetError(error, "Must set database option 'uri' before creating a connection");
    return ADBC_STATUS_INVALID_STATE;
  }
  *conn = PQconnectdb(uri_.c_str());
  if (PQstatus(*conn) != CONNECTION_OK) {
    SetError(error, "Failed to connect: ", PQerrorMessage(*conn));
    PQfinish(*conn);
    *conn = nullptr;
    return ADBC_STATUS_IO;
  }
  open_connections_++;
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresDatabase::Disconnect(PGconn** conn, struct AdbcError* error) {
  PQfinish(*conn);
  *conn = nullptr;
  if (--open_connections_ < 0) {
    SetError(error, "Open connection count underflowed");
    return ADBC_STATUS_INTERNAL;
  }
  return ADBC_STATUS_OK;
}
}  // namespace adbcpq
