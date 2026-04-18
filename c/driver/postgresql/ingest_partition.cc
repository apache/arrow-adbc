// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

#include "ingest_partition.h"

#include <cstring>
#include <random>
#include <vector>

#include <libpq-fe.h>

#include "bind_stream.h"
#include "connection.h"
#include "driver/common/utils.h"
#include "error.h"
#include "postgres_type.h"
#include "result_helper.h"

namespace adbcpq {

namespace {

void WriteU32(uint8_t** p, uint32_t v) {
  std::memcpy(*p, &v, sizeof(v));
  *p += sizeof(v);
}

void WriteI64(uint8_t** p, int64_t v) {
  std::memcpy(*p, &v, sizeof(v));
  *p += sizeof(v);
}

void WriteString(uint8_t** p, const std::string& s) {
  WriteU32(p, static_cast<uint32_t>(s.size()));
  std::memcpy(*p, s.data(), s.size());
  *p += s.size();
}

bool ReadU32(const uint8_t** p, const uint8_t* end, uint32_t* out) {
  if (end - *p < static_cast<ptrdiff_t>(sizeof(uint32_t))) return false;
  std::memcpy(out, *p, sizeof(uint32_t));
  *p += sizeof(uint32_t);
  return true;
}

bool ReadI64(const uint8_t** p, const uint8_t* end, int64_t* out) {
  if (end - *p < static_cast<ptrdiff_t>(sizeof(int64_t))) return false;
  std::memcpy(out, *p, sizeof(int64_t));
  *p += sizeof(int64_t);
  return true;
}

bool ReadString(const uint8_t** p, const uint8_t* end, std::string* out) {
  uint32_t n;
  if (!ReadU32(p, end, &n)) return false;
  if (end - *p < static_cast<ptrdiff_t>(n)) return false;
  out->assign(reinterpret_cast<const char*>(*p), n);
  *p += n;
  return true;
}

std::string HexId(const std::array<uint8_t, 16>& id) {
  static const char kHex[] = "0123456789abcdef";
  std::string s(32, '0');
  for (size_t i = 0; i < 16; i++) {
    s[2 * i] = kHex[id[i] >> 4];
    s[2 * i + 1] = kHex[id[i] & 0x0F];
  }
  return s;
}

}  // namespace

void IngestHandle::GenerateId(std::array<uint8_t, 16>* out) {
  std::random_device rd;
  std::mt19937_64 gen(rd());
  uint64_t a = gen();
  uint64_t b = gen();
  std::memcpy(out->data(), &a, 8);
  std::memcpy(out->data() + 8, &b, 8);
}

namespace {
// Staging table name is "adbc_stg_" (9) + 32-hex handle id + "_" (1) + 16-hex
// suffix. Postgres' default NAMEDATALEN is 64, giving a 63-char identifier
// limit before silent truncation — which would cause name collisions and miss
// staging tables during Abort.
constexpr size_t kStagingPrefixLen = 9 + 32 + 1;
constexpr size_t kStagingSuffixLen = 16;
constexpr size_t kStagingMaxIdentLen = 63;
static_assert(kStagingPrefixLen + kStagingSuffixLen <= kStagingMaxIdentLen,
              "staging table name would exceed PostgreSQL NAMEDATALEN-1 and be "
              "silently truncated");
}  // namespace

std::string IngestHandle::StagingPrefix() const {
  return "adbc_stg_" + HexId(ingest_id) + "_";
}

size_t IngestHandle::SerializedSize() const {
  return kMagic.size() + ingest_id.size() + sizeof(uint32_t) * 3 + catalog.size() +
         db_schema.size() + table.size();
}

void IngestHandle::Serialize(uint8_t* out) const {
  uint8_t* p = out;
  std::memcpy(p, kMagic.data(), kMagic.size());
  p += kMagic.size();
  std::memcpy(p, ingest_id.data(), ingest_id.size());
  p += ingest_id.size();
  WriteString(&p, catalog);
  WriteString(&p, db_schema);
  WriteString(&p, table);
}

AdbcStatusCode IngestHandle::Parse(const uint8_t* bytes, size_t len, IngestHandle* out,
                                   struct AdbcError* error) {
  const uint8_t* p = bytes;
  const uint8_t* end = bytes + len;
  if (end - p < static_cast<ptrdiff_t>(kMagic.size() + 16)) {
    InternalAdbcSetError(error, "[libpq] ingest handle truncated");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  if (std::memcmp(p, kMagic.data(), kMagic.size()) != 0) {
    InternalAdbcSetError(error, "[libpq] ingest handle bad magic");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  p += kMagic.size();
  std::memcpy(out->ingest_id.data(), p, 16);
  p += 16;
  if (!ReadString(&p, end, &out->catalog) ||
      !ReadString(&p, end, &out->db_schema) ||
      !ReadString(&p, end, &out->table)) {
    InternalAdbcSetError(error, "[libpq] ingest handle truncated");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  return ADBC_STATUS_OK;
}

size_t IngestReceipt::SerializedSize() const {
  return kMagic.size() + sizeof(uint32_t) * 3 + staging_schema.size() +
         staging_table.size() + escaped_columns.size() + sizeof(int64_t);
}

void IngestReceipt::Serialize(uint8_t* out) const {
  uint8_t* p = out;
  std::memcpy(p, kMagic.data(), kMagic.size());
  p += kMagic.size();
  WriteString(&p, staging_schema);
  WriteString(&p, staging_table);
  WriteString(&p, escaped_columns);
  WriteI64(&p, row_count);
}

AdbcStatusCode IngestReceipt::Parse(const uint8_t* bytes, size_t len, IngestReceipt* out,
                                    struct AdbcError* error) {
  const uint8_t* p = bytes;
  const uint8_t* end = bytes + len;
  if (end - p < static_cast<ptrdiff_t>(kMagic.size())) {
    InternalAdbcSetError(error, "[libpq] ingest receipt truncated");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  if (std::memcmp(p, kMagic.data(), kMagic.size()) != 0) {
    InternalAdbcSetError(error, "[libpq] ingest receipt bad magic");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  p += kMagic.size();
  if (!ReadString(&p, end, &out->staging_schema) ||
      !ReadString(&p, end, &out->staging_table) ||
      !ReadString(&p, end, &out->escaped_columns) ||
      !ReadI64(&p, end, &out->row_count)) {
    InternalAdbcSetError(error, "[libpq] ingest receipt truncated");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  return ADBC_STATUS_OK;
}

namespace {

enum class IngestMode { kCreate, kAppend, kReplace, kCreateAppend };

AdbcStatusCode ParseMode(const char* mode, IngestMode* out, struct AdbcError* error) {
  if (mode == nullptr) {
    InternalAdbcSetError(error, "[libpq] ingest mode is required");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  if (std::strcmp(mode, ADBC_INGEST_OPTION_MODE_CREATE) == 0) {
    *out = IngestMode::kCreate;
  } else if (std::strcmp(mode, ADBC_INGEST_OPTION_MODE_APPEND) == 0) {
    *out = IngestMode::kAppend;
  } else if (std::strcmp(mode, ADBC_INGEST_OPTION_MODE_REPLACE) == 0) {
    *out = IngestMode::kReplace;
  } else if (std::strcmp(mode, ADBC_INGEST_OPTION_MODE_CREATE_APPEND) == 0) {
    *out = IngestMode::kCreateAppend;
  } else {
    InternalAdbcSetError(error, "[libpq] unknown ingest mode: %s", mode);
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  return ADBC_STATUS_OK;
}

std::string EscapeIdent(PGconn* conn, const std::string& s, struct AdbcError* error,
                        AdbcStatusCode* status) {
  char* esc = PQescapeIdentifier(conn, s.data(), s.size());
  if (esc == nullptr) {
    InternalAdbcSetError(error, "[libpq] failed to escape identifier %s: %s", s.c_str(),
                         PQerrorMessage(conn));
    *status = ADBC_STATUS_INTERNAL;
    return {};
  }
  std::string out = esc;
  PQfreemem(esc);
  return out;
}

AdbcStatusCode ResolveCurrentSchema(PGconn* conn, std::string* out,
                                    struct AdbcError* error) {
  PqResultHelper r(conn, "SELECT CURRENT_SCHEMA()");
  Status st = r.Execute();
  if (!st.ok()) {
    return st.ToAdbc(error);
  }
  auto it = r.begin();
  if (it == r.end()) {
    InternalAdbcSetError(error, "[libpq] CURRENT_SCHEMA returned no rows");
    return ADBC_STATUS_INTERNAL;
  }
  *out = (*it)[0].data;
  return ADBC_STATUS_OK;
}

AdbcStatusCode ExecSimple(PGconn* conn, const std::string& sql, struct AdbcError* error) {
  PGresult* result = PQexec(conn, sql.c_str());
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    AdbcStatusCode code =
        SetError(error, result, "[libpq] %s\nQuery was: %s", PQerrorMessage(conn),
                 sql.c_str());
    PQclear(result);
    return code;
  }
  PQclear(result);
  return ADBC_STATUS_OK;
}

// Build CREATE TABLE statement from an Arrow schema. `escaped_qualified_table` is
// the already-escaped, schema-qualified target (e.g. `"public"."t"`).
AdbcStatusCode BuildCreateTable(PGconn* conn, const PostgresTypeResolver& resolver,
                                const std::string& escaped_qualified_table,
                                const struct ArrowSchema& schema, std::string* sql_out,
                                struct AdbcError* error) {
  std::string sql = "CREATE TABLE " + escaped_qualified_table + " (";
  for (int64_t i = 0; i < schema.n_children; i++) {
    if (i > 0) sql += ", ";
    AdbcStatusCode code = ADBC_STATUS_OK;
    std::string col = EscapeIdent(conn, schema.children[i]->name, error, &code);
    if (code != ADBC_STATUS_OK) return code;
    sql += col;
    sql += " ";

    PostgresType pg_type;
    struct ArrowError na_error;
    int rc =
        PostgresType::FromSchema(resolver, schema.children[i], &pg_type, &na_error);
    if (rc != NANOARROW_OK) {
      InternalAdbcSetError(error, "[libpq] cannot map column %s: %s",
                           schema.children[i]->name, na_error.message);
      return ADBC_STATUS_INTERNAL;
    }
    sql += pg_type.sql_type_name();
  }
  sql += ")";
  *sql_out = std::move(sql);
  return ADBC_STATUS_OK;
}

}  // namespace

AdbcStatusCode PostgresConnection::BeginIngestPartitions(
    const char* target_catalog, const char* target_db_schema, const char* target_table,
    const char* mode, struct ArrowSchema* schema, struct AdbcIngestHandle* out_handle,
    struct AdbcError* error) {
  if (target_catalog != nullptr && *target_catalog != '\0') {
    InternalAdbcSetError(error,
                         "[libpq] target_catalog is not supported for partitioned ingest");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  if (target_table == nullptr || *target_table == '\0') {
    InternalAdbcSetError(error, "[libpq] target_table is required");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  IngestMode parsed_mode;
  AdbcStatusCode code = ParseMode(mode, &parsed_mode, error);
  if (code != ADBC_STATUS_OK) return code;

  bool needs_create = parsed_mode != IngestMode::kAppend;
  if (needs_create && (schema == nullptr || schema->release == nullptr)) {
    InternalAdbcSetError(
        error, "[libpq] schema is required for create/replace/create_append modes");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  IngestHandle handle;
  IngestHandle::GenerateId(&handle.ingest_id);
  handle.table = target_table;
  if (target_db_schema != nullptr && *target_db_schema != '\0') {
    handle.db_schema = target_db_schema;
  } else {
    code = ResolveCurrentSchema(conn_, &handle.db_schema, error);
    if (code != ADBC_STATUS_OK) return code;
  }

  // Build the escaped, schema-qualified target name.
  std::string escaped_schema = EscapeIdent(conn_, handle.db_schema, error, &code);
  if (code != ADBC_STATUS_OK) return code;
  std::string escaped_table = EscapeIdent(conn_, handle.table, error, &code);
  if (code != ADBC_STATUS_OK) return code;
  std::string qualified = escaped_schema + "." + escaped_table;

  if (parsed_mode == IngestMode::kReplace) {
    code = ExecSimple(conn_, "DROP TABLE IF EXISTS " + qualified, error);
    if (code != ADBC_STATUS_OK) return code;
  }

  if (needs_create) {
    std::string create_sql;
    code = BuildCreateTable(conn_, *type_resolver_, qualified, *schema, &create_sql,
                            error);
    if (code != ADBC_STATUS_OK) return code;
    if (parsed_mode == IngestMode::kCreateAppend) {
      // Replace "CREATE TABLE " with "CREATE TABLE IF NOT EXISTS "
      create_sql.insert(std::strlen("CREATE TABLE "), "IF NOT EXISTS ");
    }
    code = ExecSimple(conn_, create_sql, error);
    if (code != ADBC_STATUS_OK) return code;
  }

  auto* buf = new std::vector<uint8_t>(handle.SerializedSize());
  handle.Serialize(buf->data());
  out_handle->length = buf->size();
  out_handle->bytes = buf->data();
  out_handle->private_data = buf;
  out_handle->release = [](struct AdbcIngestHandle* self) {
    delete reinterpret_cast<std::vector<uint8_t>*>(self->private_data);
    self->private_data = nullptr;
    self->bytes = nullptr;
    self->length = 0;
    self->release = nullptr;
  };
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::WriteIngestPartition(
    const uint8_t* handle_bytes, size_t handle_len, struct ArrowArrayStream* data,
    struct AdbcIngestReceipt* out_receipt, struct AdbcError* error) {
  IngestHandle handle;
  AdbcStatusCode code = IngestHandle::Parse(handle_bytes, handle_len, &handle, error);
  if (code != ADBC_STATUS_OK) return code;

  if (data == nullptr || data->release == nullptr) {
    InternalAdbcSetError(error, "[libpq] data stream is required");
    return ADBC_STATUS_INVALID_ARGUMENT;
  }

  // Generate a unique staging table name scoped to the handle.
  std::array<uint8_t, 8> suffix_bytes;
  {
    std::random_device rd;
    std::mt19937_64 gen(rd());
    uint64_t v = gen();
    std::memcpy(suffix_bytes.data(), &v, 8);
  }
  static const char kHex[] = "0123456789abcdef";
  std::string suffix(16, '0');
  for (size_t i = 0; i < 8; i++) {
    suffix[2 * i] = kHex[suffix_bytes[i] >> 4];
    suffix[2 * i + 1] = kHex[suffix_bytes[i] & 0x0F];
  }
  std::string staging_table = handle.StagingPrefix() + suffix;

  std::string escaped_schema = EscapeIdent(conn_, handle.db_schema, error, &code);
  if (code != ADBC_STATUS_OK) return code;
  std::string escaped_target = EscapeIdent(conn_, handle.table, error, &code);
  if (code != ADBC_STATUS_OK) return code;
  std::string escaped_staging = EscapeIdent(conn_, staging_table, error, &code);
  if (code != ADBC_STATUS_OK) return code;

  std::string qualified_target = escaped_schema + "." + escaped_target;
  std::string qualified_staging = escaped_schema + "." + escaped_staging;

  // Initialize the bind stream so we can pull a schema for the column list.
  BindStream bind_stream;
  bind_stream.SetBind(data);

  std::string escaped_columns;
  Status begin_st = bind_stream.Begin([&]() -> Status {
    AdbcStatusCode inner = ADBC_STATUS_OK;
    for (int64_t i = 0; i < bind_stream.bind_schema->n_children; i++) {
      if (i > 0) escaped_columns += ", ";
      std::string col = EscapeIdent(conn_, bind_stream.bind_schema->children[i]->name,
                                    error, &inner);
      if (inner != ADBC_STATUS_OK) {
        return Status::Internal("[libpq] failed to escape column name");
      }
      escaped_columns += col;
    }
    return Status::Ok();
  });
  if (!begin_st.ok()) return begin_st.ToAdbc(error);

  // CREATE UNLOGGED TABLE staging (LIKE target). Constraints/defaults are
  // intentionally not copied — staging holds raw rows; the commit INSERT applies
  // target's defaults via the explicit column list.
  code = ExecSimple(
      conn_, "CREATE UNLOGGED TABLE " + qualified_staging + " (LIKE " + qualified_target +
                 ")",
      error);
  if (code != ADBC_STATUS_OK) return code;

  // Issue COPY ... FROM STDIN, then stream data via ExecuteCopy.
  std::string copy_sql = "COPY " + qualified_staging + " (" + escaped_columns +
                         ") FROM STDIN WITH (FORMAT binary)";
  PGresult* result = PQexec(conn_, copy_sql.c_str());
  if (PQresultStatus(result) != PGRES_COPY_IN) {
    AdbcStatusCode err = SetError(error, result, "[libpq] COPY failed: %s\nQuery: %s",
                                  PQerrorMessage(conn_), copy_sql.c_str());
    PQclear(result);
    ExecSimple(conn_, "DROP TABLE IF EXISTS " + qualified_staging, nullptr);
    return err;
  }
  PQclear(result);

  int64_t rows_written = 0;
  Status copy_st = bind_stream.ExecuteCopy(conn_, *type_resolver_, &rows_written);
  if (!copy_st.ok()) {
    ExecSimple(conn_, "DROP TABLE IF EXISTS " + qualified_staging, nullptr);
    return copy_st.ToAdbc(error);
  }

  IngestReceipt receipt;
  receipt.staging_schema = handle.db_schema;
  receipt.staging_table = staging_table;
  receipt.escaped_columns = escaped_columns;
  receipt.row_count = rows_written;

  // Write does irrecoverable work; unlike Begin we do not support two-phase
  // sizing. Caller must provide a reasonable buffer (a few KB is plenty).
  auto* buf = new std::vector<uint8_t>(receipt.SerializedSize());
  receipt.Serialize(buf->data());
  out_receipt->length = buf->size();
  out_receipt->bytes = buf->data();
  out_receipt->private_data = buf;
  out_receipt->release = [](struct AdbcIngestReceipt* self) {
    delete reinterpret_cast<std::vector<uint8_t>*>(self->private_data);
    self->private_data = nullptr;
    self->bytes = nullptr;
    self->length = 0;
    self->release = nullptr;
  };
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::CommitIngestPartitions(
    const uint8_t* handle_bytes, size_t handle_len, size_t num_receipts,
    const uint8_t** receipts, const size_t* receipt_lens, int64_t* rows_affected,
    struct AdbcError* error) {
  IngestHandle handle;
  AdbcStatusCode code = IngestHandle::Parse(handle_bytes, handle_len, &handle, error);
  if (code != ADBC_STATUS_OK) return code;

  std::vector<IngestReceipt> parsed(num_receipts);
  for (size_t i = 0; i < num_receipts; i++) {
    code = IngestReceipt::Parse(receipts[i], receipt_lens[i], &parsed[i], error);
    if (code != ADBC_STATUS_OK) return code;
  }

  std::string escaped_target_schema = EscapeIdent(conn_, handle.db_schema, error, &code);
  if (code != ADBC_STATUS_OK) return code;
  std::string escaped_target_table = EscapeIdent(conn_, handle.table, error, &code);
  if (code != ADBC_STATUS_OK) return code;
  std::string qualified_target = escaped_target_schema + "." + escaped_target_table;

  // Decide how to scope the commit to avoid silently mutating caller transaction
  // state: when no outer transaction is open, use BEGIN/COMMIT; when one is
  // already active, use a SAVEPOINT so we only release ingest-local work and
  // leave the caller's outer transaction intact. Reject error/unknown states.
  PGTransactionStatusType txn_status = PQtransactionStatus(conn_);
  bool use_savepoint;
  switch (txn_status) {
    case PQTRANS_IDLE:
      use_savepoint = false;
      break;
    case PQTRANS_INTRANS:
      use_savepoint = true;
      break;
    default:
      InternalAdbcSetError(
          error,
          "[libpq] cannot commit partitioned ingest: connection transaction state "
          "is not idle or in-transaction (status=%d)",
          static_cast<int>(txn_status));
      return ADBC_STATUS_INVALID_STATE;
  }

  static const char kSavepointName[] = "adbc_ingest_commit";
  const std::string open_sql =
      use_savepoint ? std::string("SAVEPOINT ") + kSavepointName : "BEGIN";
  const std::string commit_sql = use_savepoint
                                     ? std::string("RELEASE SAVEPOINT ") + kSavepointName
                                     : "COMMIT";
  const std::string rollback_sql =
      use_savepoint ? std::string("ROLLBACK TO SAVEPOINT ") + kSavepointName : "ROLLBACK";

  code = ExecSimple(conn_, open_sql, error);
  if (code != ADBC_STATUS_OK) return code;

  int64_t total_rows = 0;
  for (const auto& r : parsed) {
    std::string esc_sch = EscapeIdent(conn_, r.staging_schema, error, &code);
    if (code != ADBC_STATUS_OK) {
      ExecSimple(conn_, rollback_sql, nullptr);
      return code;
    }
    std::string esc_tbl = EscapeIdent(conn_, r.staging_table, error, &code);
    if (code != ADBC_STATUS_OK) {
      ExecSimple(conn_, rollback_sql, nullptr);
      return code;
    }
    std::string qualified_staging = esc_sch + "." + esc_tbl;

    std::string insert = "INSERT INTO " + qualified_target + " (" + r.escaped_columns +
                         ") SELECT " + r.escaped_columns + " FROM " + qualified_staging;
    code = ExecSimple(conn_, insert, error);
    if (code != ADBC_STATUS_OK) {
      ExecSimple(conn_, rollback_sql, nullptr);
      return code;
    }
    code = ExecSimple(conn_, "DROP TABLE " + qualified_staging, error);
    if (code != ADBC_STATUS_OK) {
      ExecSimple(conn_, rollback_sql, nullptr);
      return code;
    }
    total_rows += r.row_count;
  }

  code = ExecSimple(conn_, commit_sql, error);
  if (code != ADBC_STATUS_OK) {
    ExecSimple(conn_, rollback_sql, nullptr);
    return code;
  }

  if (rows_affected != nullptr) *rows_affected = total_rows;
  return ADBC_STATUS_OK;
}

AdbcStatusCode PostgresConnection::AbortIngestPartitions(
    const uint8_t* handle_bytes, size_t handle_len, size_t num_receipts,
    const uint8_t** receipts, const size_t* receipt_lens, struct AdbcError* error) {
  (void)num_receipts;
  (void)receipts;
  (void)receipt_lens;
  // Receipts are a hint. The handle is the authority for cleanup scope:
  // enumerate every staging table matching the handle's prefix and drop it.
  IngestHandle handle;
  AdbcStatusCode code = IngestHandle::Parse(handle_bytes, handle_len, &handle, error);
  if (code != ADBC_STATUS_OK) return code;

  std::string prefix = handle.StagingPrefix();
  // pg LIKE pattern: escape '%' and '_' in the prefix (not present in our prefix
  // by construction, but defensive).
  std::string like_pattern;
  for (char c : prefix) {
    if (c == '%' || c == '_' || c == '\\') like_pattern += '\\';
    like_pattern += c;
  }
  like_pattern += '%';

  PqResultHelper q(
      conn_,
      "SELECT table_schema, table_name FROM information_schema.tables "
      "WHERE table_schema = $1 AND table_name LIKE $2");
  Status st = q.Execute({handle.db_schema, like_pattern});
  if (!st.ok()) return st.ToAdbc(error);

  for (auto row : q) {
    AdbcStatusCode inner = ADBC_STATUS_OK;
    std::string esc_sch = EscapeIdent(conn_, row[0].data, error, &inner);
    if (inner != ADBC_STATUS_OK) return inner;
    std::string esc_tbl = EscapeIdent(conn_, row[1].data, error, &inner);
    if (inner != ADBC_STATUS_OK) return inner;
    // Best-effort: ignore individual drop failures so one orphan doesn't block others.
    ExecSimple(conn_, "DROP TABLE IF EXISTS " + esc_sch + "." + esc_tbl, nullptr);
  }

  return ADBC_STATUS_OK;
}

}  // namespace adbcpq
