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

#include <cinttypes>
#include <cstring>
#include <memory>
#include <string>

#include <adbc.h>
#include <libpq-fe.h>

#include "database.h"
#include "utils.h"

namespace {
class PqResultHelper {
 public:
  PqResultHelper(PGconn* conn, const char* query) : conn_(conn) {
    query_ = std::string(query);
  }
  pg_result* Execute() {
    result_ = PQexec(conn_, query_.c_str());
    return result_;
  }

  ~PqResultHelper() {
    if (result_ != nullptr) PQclear(result_);
  }

 private:
  pg_result* result_ = nullptr;
  PGconn* conn_;
  std::string query_;
};
}  // namespace

namespace adbcpq {

AdbcStatusCode PostgresConnection::Commit(struct AdbcError* error) {
  if (autocommit_) {
    SetError(error, "%s", "[libpq] Cannot commit when autocommit is enabled");
    return ADBC_STATUS_INVALID_STATE;
  }

  PGresult* result = PQexec(conn_, "COMMIT");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    SetError(error, "%s%s", "[libpq] Failed to commit: ", PQerrorMessage(conn_));
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
  AdbcStatusCode final_status = ADBC_STATUS_OK;
  struct StringBuilder query = {0};
  if (StringBuilderInit(&query, /*initial_size=*/256) != 0) return ADBC_STATUS_INTERNAL;

  if (StringBuilderAppend(
          &query, "%s",
          "SELECT attname, atttypid "
          "FROM pg_catalog.pg_class AS cls "
          "INNER JOIN pg_catalog.pg_attribute AS attr ON cls.oid = attr.attrelid "
          "INNER JOIN pg_catalog.pg_type AS typ ON attr.atttypid = typ.oid "
          "WHERE attr.attnum >= 0 AND cls.oid = '") != 0)
    return ADBC_STATUS_INTERNAL;

  if (db_schema != nullptr) {
    char* schema = PQescapeIdentifier(conn_, db_schema, strlen(db_schema));
    if (schema == NULL) {
      SetError(error, "%s%s", "Faled to escape schema: ", PQerrorMessage(conn_));
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    int ret = StringBuilderAppend(&query, "%s%s", schema, ".");
    PQfreemem(schema);

    if (ret != 0) return ADBC_STATUS_INTERNAL;
  }

  char* table = PQescapeIdentifier(conn_, table_name, strlen(table_name));
  if (table == NULL) {
    SetError(error, "%s%s", "Failed to escape table: ", PQerrorMessage(conn_));
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  int ret = StringBuilderAppend(&query, "%s%s", table_name, "'::regclass::oid");
  PQfreemem(table);

  if (ret != 0) return ADBC_STATUS_INTERNAL;

  PqResultHelper result_helper = PqResultHelper{conn_, query.buffer};
  StringBuilderReset(&query);
  pg_result* result = result_helper.Execute();

  ExecStatusType pq_status = PQresultStatus(result);
  auto uschema = nanoarrow::UniqueSchema();

  if (pq_status == PGRES_TUPLES_OK) {
    int num_rows = PQntuples(result);
    ArrowSchemaInit(uschema.get());
    CHECK_NA(INTERNAL, ArrowSchemaSetTypeStruct(uschema.get(), num_rows), error);

    ArrowError na_error;
    for (int row = 0; row < num_rows; row++) {
      const char* colname = PQgetvalue(result, row, 0);
      const Oid pg_oid = static_cast<uint32_t>(
          std::strtol(PQgetvalue(result, row, 1), /*str_end=*/nullptr, /*base=*/10));

      PostgresType pg_type;
      if (type_resolver_->Find(pg_oid, &pg_type, &na_error) != NANOARROW_OK) {
        SetError(error, "%s%d%s%s%s%" PRIu32, "Column #", row + 1, " (\"", colname,
                 "\") has unknown type code ", pg_oid);
        final_status = ADBC_STATUS_NOT_IMPLEMENTED;
        break;
      }

      CHECK_NA(INTERNAL, pg_type.WithFieldName(colname).SetSchema(uschema->children[row]),
               error);
    }
  } else {
    SetError(error, "%s%s", "Failed to get table schema: ", PQerrorMessage(conn_));
    final_status = ADBC_STATUS_IO;
  }

  uschema.move(schema);
  return final_status;
}

AdbcStatusCode PostgresConnection::Init(struct AdbcDatabase* database,
                                        struct AdbcError* error) {
  if (!database || !database->private_data) {
    SetError(error, "%s", "[libpq] Must provide an initialized AdbcDatabase");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  database_ =
      *reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database->private_data);
  type_resolver_ = database_->type_resolver();
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
    SetError(error, "%s", "[libpq] Cannot rollback when autocommit is enabled");
    return ADBC_STATUS_INVALID_STATE;
  }

  PGresult* result = PQexec(conn_, "ROLLBACK");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    SetError(error, "%s%s", "[libpq] Failed to rollback: ", PQerrorMessage(conn_));
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
      SetError(error, "%s%s%s%s", "[libpq] Invalid value for option ", key, ": ", value);
      return ADBC_STATUS_INVALID_ARGUMENT;
    }

    if (autocommit != autocommit_) {
      const char* query = autocommit ? "COMMIT" : "BEGIN TRANSACTION";

      PGresult* result = PQexec(conn_, query);
      if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        SetError(error, "%s%s",
                 "[libpq] Failed to update autocommit: ", PQerrorMessage(conn_));
        PQclear(result);
        return ADBC_STATUS_IO;
      }
      PQclear(result);
      autocommit_ = autocommit;
    }
    return ADBC_STATUS_OK;
  }
  SetError(error, "%s%s", "[libpq] Unknown option ", key);
  return ADBC_STATUS_NOT_IMPLEMENTED;
}
}  // namespace adbcpq
