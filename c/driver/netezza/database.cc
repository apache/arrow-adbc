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

#include <cinttypes>
#include <cstring>
#include <sstream>
#include <memory>
#include <utility>
#include <vector>

#include <adbc.h>
#include <libpq-fe.h>
#include <nanoarrow/nanoarrow.h>

#include "common/utils.h"

namespace adbcpq {

PostgresDatabase::PostgresDatabase() : open_connections_(0) {
  type_resolver_ = std::make_shared<NetezzaTypeResolver>();
}
PostgresDatabase::~PostgresDatabase() = default;

AdbcStatusCode PostgresDatabase::GetOption(const char* option, char* value,
                                           size_t* length, struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}
AdbcStatusCode PostgresDatabase::GetOptionBytes(const char* option, uint8_t* value,
                                                size_t* length, struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}
AdbcStatusCode PostgresDatabase::GetOptionInt(const char* option, int64_t* value,
                                              struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}
AdbcStatusCode PostgresDatabase::GetOptionDouble(const char* option, double* value,
                                                 struct AdbcError* error) {
  return ADBC_STATUS_NOT_FOUND;
}

AdbcStatusCode PostgresDatabase::Init(struct AdbcError* error) {
  // Connect to validate the parameters.
  return RebuildTypeResolver(error);
}

AdbcStatusCode PostgresDatabase::Release(struct AdbcError* error) {
  if (open_connections_ != 0) {
    SetError(error, "%s%" PRId32 "%s", "[libpq] Database released with ",
             open_connections_, " open connections");
    return ADBC_STATUS_INVALID_STATE;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresDatabase::SetOption(const char* key, const char* value,
                                           struct AdbcError* error) {
  if (strcmp(key, "uri") == 0) {
    uri_ = value;
  } else {
    SetError(error, "%s%s", "[libpq] Unknown database option ", key);
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresDatabase::SetOptionBytes(const char* key, const uint8_t* value,
                                                size_t length, struct AdbcError* error) {
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresDatabase::SetOptionDouble(const char* key, double value,
                                                 struct AdbcError* error) {
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresDatabase::SetOptionInt(const char* key, int64_t value,
                                              struct AdbcError* error) {
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}

AdbcStatusCode PostgresDatabase::Connect(PGconn** conn, struct AdbcError* error) {
  if (uri_.empty()) {
    SetError(error, "%s",
             "[libpq] Must set database option 'uri' before creating a connection");
    return ADBC_STATUS_INVALID_STATE;
  }

  /* 
  * To convert ADBC url into Netezza connection string
  * input uri = netezza://myuser:mypassword@myhost:port/database/schemaname/
  * expected output = "host=myhost port=port user=myuser password=mypassword dbname=database schema=schemaname"
  */
  // TODO: Refactor below code to parse the URL in a cleaner way.
  std::istringstream input_stream(uri_);
  std::string protocol, credentials, host_name, port, database_name, schema_name, junk;
  std::getline(input_stream, protocol, ':');
  std::getline(input_stream, junk, '/');
  std::getline(input_stream, junk, '/');
  std::getline(input_stream, credentials, '@');
  std::getline(input_stream, host_name, ':');
  std::getline(input_stream, port, '/');
  std::getline(input_stream, database_name, '/');
  std::getline(input_stream, schema_name, '/');
  

  std::string converted_uri = "host=" + host_name + " port=" + port 
  + " dbname=" + database_name
  + " user=" + credentials.substr(0, credentials.find(':')) 
  + " password=" + credentials.substr(credentials.find(':') + 1)
  + " bnr_connect=adbc";

  *conn = PQconnectdb(converted_uri.c_str());

  /*
  * Enable debug trace to libpq. So that we know what's going to the backend and
  * what's coming back.
  */
  FILE* logfile;
  logfile = fopen("/tmp/adbc_libpq.log", "a+");
  PQtrace(*conn, logfile);
  
  if (PQstatus(*conn) != CONNECTION_OK) {
    SetError(error, "%s%s", "[libpq] Failed to connect: ", PQerrorMessage(*conn));
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
    SetError(error, "%s", "[libpq] Open connection count underflowed");
    return ADBC_STATUS_INTERNAL;
  }
  return ADBC_STATUS_OK;
}

// Helpers for building the type resolver from queries
static inline int32_t InsertPgAttributeResult(
    PGresult* result, const std::shared_ptr<NetezzaTypeResolver>& resolver);

static inline int32_t InsertPgTypeResult(
    PGresult* result, const std::shared_ptr<NetezzaTypeResolver>& resolver);

AdbcStatusCode PostgresDatabase::RebuildTypeResolver(struct AdbcError* error) {
  PGconn* conn = nullptr;
  AdbcStatusCode final_status = Connect(&conn, error);
  if (final_status != ADBC_STATUS_OK) {
    return final_status;
  }

  // We need a few queries to build the resolver. The current strategy might
  // fail for some recursive definitions (e.g., arrays of records of arrays).
  // First, one on the pg_attribute table to resolve column names/oids for
  // record types.
  const std::string kColumnsQuery = R"(
SELECT
    ATTRELID,
    ATTNAME,
    ATTTYPID
FROM
    ADMIN._T_ATTRIBUTE
ORDER BY
    ATTRELID, ATTNUM
)";

  // Second, a query of the pg_type table. This query may need a few attempts to handle
  // recursive definitions (e.g., record types with array column). This currently won't
  // handle range types because those rows don't have child OID information. Arrays types
  // are inserted after a successful insert of the element type.
  const std::string kTypeQuery = R"(
SELECT
    OID,
    TYPNAME,
    TYPRECEIVE,
    TYPRELID
FROM
    ADMIN._T_TYPE
WHERE
    (TYPRECEIVE != 0 OR TYPNAME = 'aclitem') AND TYPTYPE != 'r'
ORDER BY
    oid
)";

  // Create a new type resolver (this instance's type_resolver_ member
  // will be updated at the end if this succeeds).
  auto resolver = std::make_shared<NetezzaTypeResolver>();

  // Insert record type definitions (this includes table schemas)
  PGresult* result = PQexec(conn, kColumnsQuery.c_str());
  ExecStatusType pq_status = PQresultStatus(result);
  if (pq_status == PGRES_TUPLES_OK) {
    InsertPgAttributeResult(result, resolver);
  } else {
    SetError(error, "%s%s",
             "[libpq] Failed to build attribute mapping table: ", PQerrorMessage(conn));
    final_status = ADBC_STATUS_IO;
  }

  PQclear(result);

  // Attempt filling the resolver a few times to handle recursive definitions.
  int32_t max_attempts = 3;
  for (int32_t i = 0; i < max_attempts; i++) {
    result = PQexec(conn, kTypeQuery.c_str());
    ExecStatusType pq_status = PQresultStatus(result);
    if (pq_status == PGRES_TUPLES_OK) {
      InsertPgTypeResult(result, resolver);
    } else {
      SetError(error, "%s%s",
               "[libpq] Failed to build type mapping table: ", PQerrorMessage(conn));
      final_status = ADBC_STATUS_IO;
    }

    PQclear(result);
    if (final_status != ADBC_STATUS_OK) {
      break;
    }
  }

  /*
  * The below code is commented since Netezza needs connection for next set of queries.
  * Otherwise, you'll encounter 'ERROR: Query was cancelled'.
  */
  // Disconnect since PostgreSQL connections can be heavy.
  /* {
    AdbcStatusCode status = Disconnect(&conn, error);
    if (status != ADBC_STATUS_OK) final_status = status;
  } */

  if (final_status == ADBC_STATUS_OK) {
    type_resolver_ = std::move(resolver);
  }

  return final_status;
}

static inline int32_t InsertPgAttributeResult(
    PGresult* result, const std::shared_ptr<NetezzaTypeResolver>& resolver) {
  int num_rows = PQntuples(result);
  std::vector<std::pair<std::string, uint32_t>> columns;
  uint32_t current_type_oid = 0;
  int32_t n_added = 0;

  for (int row = 0; row < num_rows; row++) {
    const uint32_t type_oid = static_cast<uint32_t>(
        std::strtol(PQgetvalue(result, row, 0), /*str_end=*/nullptr, /*base=*/10));
    const char* col_name = PQgetvalue(result, row, 1);
    const uint32_t col_oid = static_cast<uint32_t>(
        std::strtol(PQgetvalue(result, row, 2), /*str_end=*/nullptr, /*base=*/10));

    if (type_oid != current_type_oid && !columns.empty()) {
      resolver->InsertClass(current_type_oid, columns);
      columns.clear();
      current_type_oid = type_oid;
      n_added++;
    }

    columns.push_back({col_name, col_oid});
  }

  if (!columns.empty()) {
    resolver->InsertClass(current_type_oid, columns);
    n_added++;
  }

  return n_added;
}

static inline int32_t InsertPgTypeResult(
    PGresult* result, const std::shared_ptr<NetezzaTypeResolver>& resolver) {
  int num_rows = PQntuples(result);
  NetezzaTypeResolver::Item item;
  int32_t n_added = 0;

  for (int row = 0; row < num_rows; row++) {
    const uint32_t oid = static_cast<uint32_t>(
        std::strtol(PQgetvalue(result, row, 0), /*str_end=*/nullptr, /*base=*/10));
    const char* typname = PQgetvalue(result, row, 1);
    const char* typreceive = PQgetvalue(result, row, 2);
    const uint32_t typrelid = static_cast<uint32_t>(
        std::strtol(PQgetvalue(result, row, 3), /*str_end=*/nullptr, /*base=*/10));

    // Special case the aclitem because it shows up in a bunch of internal tables
    if (strcmp(typname, "aclitem") == 0) {
      typreceive = "aclitem_recv";
    }

    item.oid = oid;
    item.typname = typname;
    item.typreceive = typreceive;
    item.class_oid = typrelid;

    int result = resolver->Insert(item, nullptr);
    if (result == NANOARROW_OK) {

    }

    // Commented as not applicable to Netezza as of now.
    // If there's an array type and the insert succeeded, add that now too
    // if (result == NANOARROW_OK /*&& typarray != 0*/) {
    //   std::string array_typname = "_" + std::string(typname);
    //   // item.oid = typarray;
    //   item.typname = array_typname.c_str();
    //   item.typreceive = "array_recv";
    //   item.child_oid = oid;

    //   resolver->Insert(item, nullptr);
    // }
  }

  return n_added;
}

}  // namespace adbcpq
