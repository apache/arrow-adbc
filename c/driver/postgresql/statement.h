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

#include <cstring>
#include <memory>
#include <string>
#include <vector>

#include <adbc.h>
#include <libpq-fe.h>
#include <nanoarrow/nanoarrow.h>

#include "common/utils.h"
#include "postgres_copy_reader.h"
#include "postgres_type.h"

#define ADBC_POSTGRESQL_OPTION_BATCH_SIZE_HINT_BYTES \
  "adbc.postgresql.batch_size_hint_bytes"

namespace adbcpq {
class PostgresConnection;
class PostgresStatement;

/// \brief An ArrowArrayStream that reads tuples from a PGresult.
class TupleReader final {
 public:
  TupleReader(PGconn* conn)
      : conn_(conn),
        result_(nullptr),
        pgbuf_(nullptr),
        copy_reader_(nullptr),
        row_id_(-1),
        batch_size_hint_bytes_(16777216),
        is_finished_(false) {
    StringBuilderInit(&error_builder_, 0);
    data_.data.as_char = nullptr;
    data_.size_bytes = 0;
  }

  int GetSchema(struct ArrowSchema* out);
  int GetNext(struct ArrowArray* out);
  const char* last_error() const {
    if (error_builder_.size > 0) {
      return error_builder_.buffer;
    } else {
      return nullptr;
    }
  }
  void Release();
  void ExportTo(struct ArrowArrayStream* stream);

 private:
  friend class PostgresStatement;

  int InitQueryAndFetchFirst(struct ArrowError* error);
  int AppendRowAndFetchNext(struct ArrowError* error);
  int BuildOutput(struct ArrowArray* out, struct ArrowError* error);

  static int GetSchemaTrampoline(struct ArrowArrayStream* self, struct ArrowSchema* out);
  static int GetNextTrampoline(struct ArrowArrayStream* self, struct ArrowArray* out);
  static const char* GetLastErrorTrampoline(struct ArrowArrayStream* self);
  static void ReleaseTrampoline(struct ArrowArrayStream* self);

  PGconn* conn_;
  PGresult* result_;
  char* pgbuf_;
  struct ArrowBufferView data_;
  struct StringBuilder error_builder_;
  std::unique_ptr<PostgresCopyStreamReader> copy_reader_;
  int64_t row_id_;
  int64_t batch_size_hint_bytes_;
  bool is_finished_;
};

class PostgresStatement {
 public:
  PostgresStatement()
      : connection_(nullptr), query_(), prepared_(false), reader_(nullptr) {
    std::memset(&bind_, 0, sizeof(bind_));
  }

  // ---------------------------------------------------------------------
  // ADBC API implementation

  AdbcStatusCode Bind(struct ArrowArray* values, struct ArrowSchema* schema,
                      struct AdbcError* error);
  AdbcStatusCode Bind(struct ArrowArrayStream* stream, struct AdbcError* error);
  AdbcStatusCode ExecuteQuery(struct ArrowArrayStream* stream, int64_t* rows_affected,
                              struct AdbcError* error);
  AdbcStatusCode GetParameterSchema(struct ArrowSchema* schema, struct AdbcError* error);
  AdbcStatusCode New(struct AdbcConnection* connection, struct AdbcError* error);
  AdbcStatusCode Prepare(struct AdbcError* error);
  AdbcStatusCode Release(struct AdbcError* error);
  AdbcStatusCode SetOption(const char* key, const char* value, struct AdbcError* error);
  AdbcStatusCode SetSqlQuery(const char* query, struct AdbcError* error);

  // ---------------------------------------------------------------------
  // Helper methods

  void ClearResult();
  AdbcStatusCode CreateBulkTable(
      const struct ArrowSchema& source_schema,
      const std::vector<struct ArrowSchemaView>& source_schema_fields,
      struct AdbcError* error);
  AdbcStatusCode ExecuteUpdateBulk(int64_t* rows_affected, struct AdbcError* error);
  AdbcStatusCode ExecuteUpdateQuery(int64_t* rows_affected, struct AdbcError* error);
  AdbcStatusCode ExecutePreparedStatement(struct ArrowArrayStream* stream,
                                          int64_t* rows_affected,
                                          struct AdbcError* error);

 private:
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
  std::shared_ptr<PostgresConnection> connection_;

  // Query state
  std::string query_;
  bool prepared_;
  struct ArrowArrayStream bind_;

  // Bulk ingest state
  struct {
    std::string target;
    bool append = false;
  } ingest_;

  TupleReader reader_;
};
}  // namespace adbcpq
