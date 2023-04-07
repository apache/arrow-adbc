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

#include "connection.h"

#include <cstring>
#include <memory>
#include <string>

#include <adbc.h>

#include "database.h"
#include "util.h"

namespace adbcpq {
AdbcStatusCode PostgresConnection::Commit(struct AdbcError* error) {
  if (autocommit_) {
    SetError(error, "Cannot commit when autocommit is enabled");
    return ADBC_STATUS_INVALID_STATE;
  }

  PGresult* result = PQexec(conn_, "COMMIT");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    SetError(error, "Failed to commit: ", PQerrorMessage(conn_));
    PQclear(result);
    return ADBC_STATUS_IO;
  }
  PQclear(result);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::GetTableSchema(const char* catalog,
                                                  const char* db_schema,
                                                  const char* table_name,
                                                  struct ArrowSchema* schema,
                                                  struct AdbcError* error) {
  // TODO: sqlite uses a StringBuilder class that seems roughly equivalent
  // to a similar tool in arrow/util/string_builder.h but wasn't clear on
  // structure and how to use, so relying on simplistic appends for now
  AdbcStatusCode final_status;

  std::string query =
      "SELECT attname, atttypid "
      "FROM pg_catalog.pg_class AS cls "
      "INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid "
      "INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid "
      "WHERE attr.attnum >= 0 AND cls.oid = '";
  if (db_schema != nullptr) {
    query.append(db_schema);
    query.append(".");
  }
  query.append(table_name);
  query.append("'::regclass::oid");

  // char* stmt = PQescapeLiteral(conn_, query.c_str(), query.length());
  // if (stmt == nullptr) {
  //   SetError(error, "Failed to get table schema: ", PQerrorMessage(conn_));
  //  return ADBC_STATUS_INVALID_ARGUMENT;
  // }

  pg_result* result = PQexec(conn_, query.c_str());
  // PQfreemem(stmt);

  ExecStatusType pq_status = PQresultStatus(result);
  if (pq_status == PGRES_TUPLES_OK) {
    int num_rows = PQntuples(result);
    ArrowSchemaInit(schema);
    CHECK_NA_ADBC(ArrowSchemaSetTypeStruct(schema, num_rows), error);

    // TODO: much of this code is copied from statement.cc InferSchema
    for (int row = 0; row < num_rows; row++) {
      ArrowType field_type = NANOARROW_TYPE_NA;
      const char* colname = PQgetvalue(result, row, 0);
      const uint32_t oid = static_cast<uint32_t>(
          std::strtol(PQgetvalue(result, row, 1), /*str_end=*/nullptr, /*base=*/10));

      auto it = type_mapping_->type_mapping.find(oid);
      if (it == type_mapping_->type_mapping.end()) {
        SetError(error, "Column #", row + 1, " (\"", colname,
                 "\") has unknown type code ", oid);
        return ADBC_STATUS_NOT_IMPLEMENTED;
      }

      switch (it->second) {
        // TODO: this mapping will eventually have to become dynamic,
        // because of complex types like arrays/records
        case PgType::kBool:
          field_type = NANOARROW_TYPE_BOOL;
          break;
        case PgType::kFloat4:
          field_type = NANOARROW_TYPE_FLOAT;
          break;
        case PgType::kFloat8:
          field_type = NANOARROW_TYPE_DOUBLE;
          break;
        case PgType::kInt2:
          field_type = NANOARROW_TYPE_INT16;
          break;
        case PgType::kInt4:
          field_type = NANOARROW_TYPE_INT32;
          break;
        case PgType::kInt8:
          field_type = NANOARROW_TYPE_INT64;
          break;
        case PgType::kVarBinary:
          field_type = NANOARROW_TYPE_BINARY;
          break;
        case PgType::kText:
        case PgType::kVarChar:
          field_type = NANOARROW_TYPE_STRING;
          break;
        default:
          SetError(error, "Column #", row + 1, " (\"", colname,
                   "\") has unimplemented type code ", oid);
          return ADBC_STATUS_NOT_IMPLEMENTED;
      }
      CHECK_NA_ADBC(ArrowSchemaSetType(schema->children[row], field_type), error);
      CHECK_NA_ADBC(ArrowSchemaSetName(schema->children[row], colname), error);
    }
  } else {
    SetError(error, "Failed to get table schema: ", PQerrorMessage(conn_));
    final_status = ADBC_STATUS_IO;
  }
  PQclear(result);

  // TODO: Should we disconnect here?
  return final_status;
}

AdbcStatusCode PostgresConnection::Init(struct AdbcDatabase* database,
                                        struct AdbcError* error) {
  if (!database || !database->private_data) {
    SetError(error, "Must provide an initialized AdbcDatabase");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  database_ =
      *reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  type_mapping_ = database_->type_mapping();
  return database_->Connect(&conn_, error);
}

AdbcStatusCode PostgresConnection::Release(struct AdbcError* error) {
  if (conn_) {
    return database_->Disconnect(&conn_, error);
  }
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::Rollback(struct AdbcError* error) {
  if (autocommit_) {
    SetError(error, "Cannot rollback when autocommit is enabled");
    return ADBC_STATUS_INVALID_STATE;
  }

  PGresult* result = PQexec(conn_, "ROLLBACK");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    SetError(error, "Failed to rollback: ", PQerrorMessage(conn_));
    PQclear(result);
    return ADBC_STATUS_IO;
  }
  PQclear(result);
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::SetOption(const char* key, const char* value,
                                             struct AdbcError* error) {
  if (std::strcmp(key, ADBC_CONNECTION_OPTION_AUTOCOMMIT) == 0) {
    bool autocommit = true;
    if (std::strcmp(value, ADBC_OPTION_VALUE_ENABLED) == 0) {
      autocommit = true;
    } else if (std::strcmp(value, ADBC_OPTION_VALUE_DISABLED) == 0) {
      autocommit = false;
    } else {
      SetError(error, "Invalid value for option ", key, ": ", value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    if (autocommit != autocommit_) {
      const char* query = autocommit ? "COMMIT" : "BEGIN TRANSACTION";

      PGresult* result = PQexec(conn_, query);
      if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        SetError(error, "Failed to update autocommit: ", PQerrorMessage(conn_));
        PQclear(result);
        return ADBC_STATUS_IO;
      }
      PQclear(result);
      autocommit_ = autocommit;
    }
    return ADBC_STATUS_OK;
  }
  SetError(error, "Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}
}  // namespace adbcpq
