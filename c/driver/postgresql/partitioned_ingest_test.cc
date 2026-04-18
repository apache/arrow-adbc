// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0

#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <mutex>
#include <optional>
#include <string>
#include <thread>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow.hpp>

#include "validation/adbc_validation_util.h"

namespace {

const char* RequireUri() {
  const char* uri = std::getenv("ADBC_POSTGRESQL_TEST_URI");
  if (!uri) {
    ADD_FAILURE() << "ADBC_POSTGRESQL_TEST_URI must be set";
  }
  return uri;
}

struct ConnPair {
  AdbcDatabase db{};
  AdbcConnection conn{};
};

void OpenConn(ConnPair* p, AdbcError* error, const char* uri) {
  ASSERT_EQ(AdbcDatabaseNew(&p->db, error), ADBC_STATUS_OK) << error->message;
  ASSERT_EQ(AdbcDatabaseSetOption(&p->db, "uri", uri, error), ADBC_STATUS_OK)
      << error->message;
  ASSERT_EQ(AdbcDatabaseInit(&p->db, error), ADBC_STATUS_OK) << error->message;
  ASSERT_EQ(AdbcConnectionNew(&p->conn, error), ADBC_STATUS_OK) << error->message;
  ASSERT_EQ(AdbcConnectionInit(&p->conn, &p->db, error), ADBC_STATUS_OK)
      << error->message;
}

void CloseConn(ConnPair* p, AdbcError* error) {
  AdbcConnectionRelease(&p->conn, error);
  AdbcDatabaseRelease(&p->db, error);
}

// Convenience: drop the test target table and any leftover staging tables.
void Cleanup(ConnPair* c, const std::string& table, AdbcError* error) {
  AdbcStatement stmt{};
  ASSERT_EQ(AdbcStatementNew(&c->conn, &stmt, error), ADBC_STATUS_OK);
  std::string sql = "DROP TABLE IF EXISTS " + table;
  ASSERT_EQ(AdbcStatementSetSqlQuery(&stmt, sql.c_str(), error), ADBC_STATUS_OK);
  AdbcStatementExecuteQuery(&stmt, nullptr, nullptr, error);
  AdbcStatementRelease(&stmt, error);
}

// Read a count from the target table.
int64_t SelectCount(ConnPair* c, const std::string& table, AdbcError* error) {
  AdbcStatement stmt{};
  EXPECT_EQ(AdbcStatementNew(&c->conn, &stmt, error), ADBC_STATUS_OK);
  std::string sql = "SELECT COUNT(*) FROM " + table;
  EXPECT_EQ(AdbcStatementSetSqlQuery(&stmt, sql.c_str(), error), ADBC_STATUS_OK);

  ArrowArrayStream stream{};
  int64_t rows_affected = 0;
  EXPECT_EQ(AdbcStatementExecuteQuery(&stmt, &stream, &rows_affected, error),
            ADBC_STATUS_OK)
      << error->message;

  ArrowSchema schema{};
  ArrowArray batch{};
  stream.get_schema(&stream, &schema);
  stream.get_next(&stream, &batch);

  int64_t count = 0;
  if (batch.length > 0) {
    nanoarrow::UniqueArrayView view;
    ArrowArrayViewInitFromSchema(view.get(), &schema, nullptr);
    ArrowArrayViewSetArray(view.get(), &batch, nullptr);
    count = ArrowArrayViewGetIntUnsafe(view->children[0], 0);
  }

  if (batch.release) batch.release(&batch);
  if (schema.release) schema.release(&schema);
  stream.release(&stream);
  AdbcStatementRelease(&stmt, error);
  return count;
}

// Helper: build a struct schema { id int32, label utf8 }.
void MakeIngestSchema(ArrowSchema* out) {
  ASSERT_EQ(adbc_validation::MakeSchema(
                out, {{"id", NANOARROW_TYPE_INT32}, {"label", NANOARROW_TYPE_STRING}}),
            0);
}

// Helper: build a one-batch stream of N rows starting at `start_id`.
void MakeBatchStream(ArrowArrayStream* stream, int32_t start_id, int32_t n) {
  ArrowSchema schema{};
  MakeIngestSchema(&schema);

  ArrowArray batch{};
  std::vector<std::optional<int32_t>> ids;
  std::vector<std::optional<std::string>> labels;
  ids.reserve(n);
  labels.reserve(n);
  for (int32_t i = 0; i < n; i++) {
    ids.push_back(start_id + i);
    labels.push_back("row-" + std::to_string(start_id + i));
  }
  ArrowError na_error{};
  ASSERT_EQ(adbc_validation::MakeBatch(&schema, &batch, &na_error, ids, labels), 0);

  std::vector<ArrowArray> batches;
  batches.push_back(batch);
  adbc_validation::MakeStream(stream, &schema, std::move(batches));
}

}  // namespace

class PostgresPartitionedIngestTest : public ::testing::Test {};

TEST_F(PostgresPartitionedIngestTest, CreateThenWriteThenCommit) {
  const char* uri = RequireUri();
  if (!uri) return;
  const std::string table = "adbc_partitioned_ingest_test";

  AdbcError error = ADBC_ERROR_INIT;
  ConnPair coordinator;
  OpenConn(&coordinator, &error, uri);
  Cleanup(&coordinator, table, &error);

  ArrowSchema ingest_schema{};
  MakeIngestSchema(&ingest_schema);

  AdbcIngestHandle handle{};
  ASSERT_EQ(AdbcConnectionBeginIngestPartitions(
                &coordinator.conn, /*catalog=*/nullptr, /*db_schema=*/nullptr,
                table.c_str(), ADBC_INGEST_OPTION_MODE_CREATE, &ingest_schema,
                &handle, &error),
            ADBC_STATUS_OK)
      << error.message;
  ingest_schema.release(&ingest_schema);

  // Spawn N workers, each on its own connection, each writing a partition.
  constexpr int kNumWorkers = 4;
  constexpr int kRowsPerWorker = 1000;
  std::vector<std::vector<uint8_t>> receipts(kNumWorkers);
  std::vector<std::thread> workers;
  std::mutex err_mu;
  std::vector<std::string> worker_errors;

  for (int w = 0; w < kNumWorkers; w++) {
    workers.emplace_back([&, w]() {
      AdbcError werr = ADBC_ERROR_INIT;
      ConnPair wp;
      OpenConn(&wp, &werr, uri);

      ArrowArrayStream stream{};
      MakeBatchStream(&stream, w * kRowsPerWorker, kRowsPerWorker);

      AdbcIngestReceipt rec{};
      AdbcStatusCode rc = AdbcConnectionWriteIngestPartition(
          &wp.conn, handle.bytes, handle.length, &stream, &rec, &werr);
      if (rc != ADBC_STATUS_OK) {
        std::lock_guard<std::mutex> g(err_mu);
        worker_errors.push_back(std::string("write: ") +
                                (werr.message ? werr.message : ""));
      } else {
        // Copy receipt bytes out so we can release the driver-owned struct
        // immediately. Mirrors the cross-process flow: caller serializes the
        // bytes and ships them to the coordinator.
        receipts[w].assign(rec.bytes, rec.bytes + rec.length);
        rec.release(&rec);
      }
      CloseConn(&wp, &werr);
    });
  }
  for (auto& t : workers) t.join();
  ASSERT_TRUE(worker_errors.empty()) << worker_errors[0];

  // Commit.
  std::vector<const uint8_t*> rec_ptrs(kNumWorkers);
  std::vector<size_t> rec_lens(kNumWorkers);
  for (int i = 0; i < kNumWorkers; i++) {
    rec_ptrs[i] = receipts[i].data();
    rec_lens[i] = receipts[i].size();
  }
  int64_t rows_committed = 0;
  ASSERT_EQ(AdbcConnectionCommitIngestPartitions(&coordinator.conn, handle.bytes,
                                                 handle.length, kNumWorkers,
                                                 rec_ptrs.data(), rec_lens.data(),
                                                 &rows_committed, &error),
            ADBC_STATUS_OK)
      << error.message;
  EXPECT_EQ(rows_committed, kNumWorkers * kRowsPerWorker);

  // Verify target row count.
  EXPECT_EQ(SelectCount(&coordinator, table, &error), kNumWorkers * kRowsPerWorker);

  handle.release(&handle);
  Cleanup(&coordinator, table, &error);
  CloseConn(&coordinator, &error);
}

TEST_F(PostgresPartitionedIngestTest, AbortDropsAllStagingIncludingOrphans) {
  const char* uri = RequireUri();
  if (!uri) return;
  const std::string table = "adbc_partitioned_ingest_abort_test";

  AdbcError error = ADBC_ERROR_INIT;
  ConnPair c;
  OpenConn(&c, &error, uri);
  Cleanup(&c, table, &error);

  ArrowSchema ingest_schema{};
  MakeIngestSchema(&ingest_schema);

  AdbcIngestHandle handle{};
  ASSERT_EQ(AdbcConnectionBeginIngestPartitions(
                &c.conn, nullptr, nullptr, table.c_str(),
                ADBC_INGEST_OPTION_MODE_CREATE, &ingest_schema, &handle, &error),
            ADBC_STATUS_OK)
      << error.message;
  ingest_schema.release(&ingest_schema);

  // Write three partitions but only collect two receipts (simulate one lost).
  std::vector<std::vector<uint8_t>> receipts;
  for (int w = 0; w < 3; w++) {
    ArrowArrayStream stream{};
    MakeBatchStream(&stream, w * 10, 10);
    AdbcIngestReceipt rec{};
    ASSERT_EQ(AdbcConnectionWriteIngestPartition(&c.conn, handle.bytes, handle.length,
                                                 &stream, &rec, &error),
              ADBC_STATUS_OK)
        << error.message;
    if (w < 2) receipts.emplace_back(rec.bytes, rec.bytes + rec.length);
    rec.release(&rec);
  }

  // Abort with only the two known receipts. Driver must clean up the orphan too.
  std::vector<const uint8_t*> rec_ptrs;
  std::vector<size_t> rec_lens;
  for (auto& r : receipts) {
    rec_ptrs.push_back(r.data());
    rec_lens.push_back(r.size());
  }
  ASSERT_EQ(AdbcConnectionAbortIngestPartitions(&c.conn, handle.bytes, handle.length,
                                                rec_ptrs.size(), rec_ptrs.data(),
                                                rec_lens.data(), &error),
            ADBC_STATUS_OK)
      << error.message;

  // Derive the handle's staging prefix from the wire format (4-byte magic
  // "PIH1" + 16-byte id) so the verification query is scoped to this test's
  // handle and not to every ingest ever run against the database.
  ASSERT_GE(handle.length, static_cast<size_t>(4 + 16));
  std::string handle_prefix = "adbc_stg_";
  static const char kHex[] = "0123456789abcdef";
  for (size_t i = 0; i < 16; i++) {
    uint8_t b = handle.bytes[4 + i];
    handle_prefix += kHex[b >> 4];
    handle_prefix += kHex[b & 0x0F];
  }
  handle_prefix += '_';

  // Verify no staging tables left under the handle's prefix.
  AdbcStatement stmt{};
  ASSERT_EQ(AdbcStatementNew(&c.conn, &stmt, &error), ADBC_STATUS_OK);
  std::string count_sql =
      "SELECT COUNT(*) FROM information_schema.tables "
      "WHERE table_schema = current_schema() "
      "AND table_name LIKE '" +
      handle_prefix + "%'";
  ASSERT_EQ(AdbcStatementSetSqlQuery(&stmt, count_sql.c_str(), &error), ADBC_STATUS_OK);
  ArrowArrayStream stream{};
  ASSERT_EQ(AdbcStatementExecuteQuery(&stmt, &stream, nullptr, &error), ADBC_STATUS_OK);
  ArrowSchema schema{};
  ArrowArray batch{};
  stream.get_schema(&stream, &schema);
  stream.get_next(&stream, &batch);
  nanoarrow::UniqueArrayView view;
  ArrowArrayViewInitFromSchema(view.get(), &schema, nullptr);
  ArrowArrayViewSetArray(view.get(), &batch, nullptr);
  int64_t leftover = ArrowArrayViewGetIntUnsafe(view->children[0], 0);
  EXPECT_EQ(leftover, 0);

  if (batch.release) batch.release(&batch);
  if (schema.release) schema.release(&schema);
  stream.release(&stream);
  AdbcStatementRelease(&stmt, &error);

  handle.release(&handle);
  Cleanup(&c, table, &error);
  CloseConn(&c, &error);
}

// With the coordinator connection already inside an outer transaction,
// CommitIngestPartitions must take the SAVEPOINT path: the ingest rows become
// visible to in-transaction SELECTs, and the lifetime of the ingest is tied to
// the outer transaction (persists on COMMIT, rolls back on ROLLBACK).
TEST_F(PostgresPartitionedIngestTest, CommitInsideOuterTransactionUsesSavepoint) {
  const char* uri = RequireUri();
  if (!uri) return;
  const std::string table = "adbc_partitioned_ingest_savepoint_test";

  AdbcError error = ADBC_ERROR_INIT;
  ConnPair coordinator;
  OpenConn(&coordinator, &error, uri);
  Cleanup(&coordinator, table, &error);

  ArrowSchema ingest_schema{};
  MakeIngestSchema(&ingest_schema);

  AdbcIngestHandle handle{};
  ASSERT_EQ(AdbcConnectionBeginIngestPartitions(
                &coordinator.conn, nullptr, nullptr, table.c_str(),
                ADBC_INGEST_OPTION_MODE_CREATE, &ingest_schema, &handle, &error),
            ADBC_STATUS_OK)
      << error.message;
  ingest_schema.release(&ingest_schema);

  // One worker writes one partition on a separate connection.
  constexpr int32_t kRows = 25;
  ConnPair worker;
  OpenConn(&worker, &error, uri);
  ArrowArrayStream stream{};
  MakeBatchStream(&stream, /*start_id=*/0, kRows);
  AdbcIngestReceipt rec{};
  ASSERT_EQ(AdbcConnectionWriteIngestPartition(&worker.conn, handle.bytes, handle.length,
                                               &stream, &rec, &error),
            ADBC_STATUS_OK)
      << error.message;
  std::vector<uint8_t> receipt_bytes(rec.bytes, rec.bytes + rec.length);
  rec.release(&rec);
  CloseConn(&worker, &error);

  // Put the coordinator connection into an outer transaction so Commit must
  // take the SAVEPOINT branch. Some drivers defer BEGIN to the next statement,
  // which would leave libpq in PQTRANS_IDLE and silently take the BEGIN/COMMIT
  // branch instead; issue a trivial SELECT to force the transaction open.
  ASSERT_EQ(AdbcConnectionSetOption(&coordinator.conn, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                    ADBC_OPTION_VALUE_DISABLED, &error),
            ADBC_STATUS_OK)
      << error.message;
  {
    // Run a no-result statement so libpq ends in PQTRANS_INTRANS rather than
    // PQTRANS_ACTIVE (which a streaming SELECT would leave behind until the
    // cursor is drained). DO $$ BEGIN END $$ executes and returns no rows.
    AdbcStatement stmt{};
    ASSERT_EQ(AdbcStatementNew(&coordinator.conn, &stmt, &error), ADBC_STATUS_OK);
    ASSERT_EQ(AdbcStatementSetSqlQuery(&stmt, "DO $$ BEGIN END $$", &error),
              ADBC_STATUS_OK);
    ASSERT_EQ(AdbcStatementExecuteQuery(&stmt, nullptr, nullptr, &error),
              ADBC_STATUS_OK)
        << error.message;
    AdbcStatementRelease(&stmt, &error);
  }

  const uint8_t* rec_ptr = receipt_bytes.data();
  size_t rec_len = receipt_bytes.size();
  int64_t rows_committed = 0;
  ASSERT_EQ(AdbcConnectionCommitIngestPartitions(&coordinator.conn, handle.bytes,
                                                 handle.length, 1, &rec_ptr, &rec_len,
                                                 &rows_committed, &error),
            ADBC_STATUS_OK)
      << error.message;
  EXPECT_EQ(rows_committed, kRows);

  // (a) Rows visible to in-transaction SELECT on the same connection.
  EXPECT_EQ(SelectCount(&coordinator, table, &error), kRows);

  // (b) A caller-driven ROLLBACK undoes the ingest: RELEASE SAVEPOINT merges
  // the ingest work into the outer transaction, and rolling back the outer
  // transaction rolls back everything in it.
  ASSERT_EQ(AdbcConnectionRollback(&coordinator.conn, &error), ADBC_STATUS_OK)
      << error.message;
  ASSERT_EQ(AdbcConnectionSetOption(&coordinator.conn, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                    "true", &error),
            ADBC_STATUS_OK)
      << error.message;
  EXPECT_EQ(SelectCount(&coordinator, table, &error), 0);

  handle.release(&handle);
  Cleanup(&coordinator, table, &error);
  CloseConn(&coordinator, &error);
}

// When CommitIngestPartitions fails mid-loop while inside an outer
// transaction, the savepoint must be fully unwound: ROLLBACK TO SAVEPOINT
// followed by RELEASE SAVEPOINT. If the RELEASE is missed, the caller's
// savepoint stack leaks an entry, and if the rollback is skipped the outer
// transaction is left in PQTRANS_INERROR. Force a failure by dropping a
// staging table out-of-band before Commit runs — the INSERT FROM that
// staging table will fail — then verify the outer transaction is still
// usable (not in error state) and that the next caller SAVEPOINT of the
// same name succeeds (proving the driver's savepoint was released).
TEST_F(PostgresPartitionedIngestTest, CommitFailureInOuterTxnReleasesSavepoint) {
  const char* uri = RequireUri();
  if (!uri) return;
  const std::string table = "adbc_partitioned_ingest_savepoint_abort_test";

  AdbcError error = ADBC_ERROR_INIT;
  ConnPair coordinator;
  OpenConn(&coordinator, &error, uri);
  Cleanup(&coordinator, table, &error);

  ArrowSchema ingest_schema{};
  MakeIngestSchema(&ingest_schema);

  AdbcIngestHandle handle{};
  ASSERT_EQ(AdbcConnectionBeginIngestPartitions(
                &coordinator.conn, nullptr, nullptr, table.c_str(),
                ADBC_INGEST_OPTION_MODE_CREATE, &ingest_schema, &handle, &error),
            ADBC_STATUS_OK)
      << error.message;
  ingest_schema.release(&ingest_schema);

  // Write one partition on a separate worker connection.
  constexpr int32_t kRows = 10;
  ConnPair worker;
  OpenConn(&worker, &error, uri);
  ArrowArrayStream stream{};
  MakeBatchStream(&stream, /*start_id=*/0, kRows);
  AdbcIngestReceipt rec{};
  ASSERT_EQ(AdbcConnectionWriteIngestPartition(&worker.conn, handle.bytes, handle.length,
                                               &stream, &rec, &error),
            ADBC_STATUS_OK)
      << error.message;
  std::vector<uint8_t> receipt_bytes(rec.bytes, rec.bytes + rec.length);
  rec.release(&rec);
  CloseConn(&worker, &error);

  // Derive the staging table name from the receipt wire format (4-byte
  // "PIR1" magic + u32 schema len + schema + u32 table len + table + ...).
  ASSERT_GE(receipt_bytes.size(), static_cast<size_t>(4 + 4));
  const uint8_t* rp = receipt_bytes.data() + 4;
  uint32_t schema_len = 0;
  std::memcpy(&schema_len, rp, sizeof(schema_len));
  rp += 4;
  std::string staging_schema(reinterpret_cast<const char*>(rp), schema_len);
  rp += schema_len;
  uint32_t tbl_len = 0;
  std::memcpy(&tbl_len, rp, sizeof(tbl_len));
  rp += 4;
  std::string staging_table(reinterpret_cast<const char*>(rp), tbl_len);
  std::string qualified_staging =
      "\"" + staging_schema + "\".\"" + staging_table + "\"";

  // Drop the staging table out-of-band so the Commit loop's INSERT will fail.
  {
    ConnPair saboteur;
    OpenConn(&saboteur, &error, uri);
    AdbcStatement stmt{};
    ASSERT_EQ(AdbcStatementNew(&saboteur.conn, &stmt, &error), ADBC_STATUS_OK);
    std::string drop_sql = "DROP TABLE " + qualified_staging;
    ASSERT_EQ(AdbcStatementSetSqlQuery(&stmt, drop_sql.c_str(), &error),
              ADBC_STATUS_OK);
    ASSERT_EQ(AdbcStatementExecuteQuery(&stmt, nullptr, nullptr, &error),
              ADBC_STATUS_OK)
        << error.message;
    AdbcStatementRelease(&stmt, &error);
    CloseConn(&saboteur, &error);
  }

  // Put the coordinator into an outer transaction and force libpq to
  // PQTRANS_INTRANS before calling Commit.
  ASSERT_EQ(AdbcConnectionSetOption(&coordinator.conn, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                    ADBC_OPTION_VALUE_DISABLED, &error),
            ADBC_STATUS_OK)
      << error.message;
  {
    // Run a no-result statement so libpq ends in PQTRANS_INTRANS rather than
    // PQTRANS_ACTIVE (which a streaming SELECT would leave behind until the
    // cursor is drained). DO $$ BEGIN END $$ executes and returns no rows.
    AdbcStatement stmt{};
    ASSERT_EQ(AdbcStatementNew(&coordinator.conn, &stmt, &error), ADBC_STATUS_OK);
    ASSERT_EQ(AdbcStatementSetSqlQuery(&stmt, "DO $$ BEGIN END $$", &error),
              ADBC_STATUS_OK);
    ASSERT_EQ(AdbcStatementExecuteQuery(&stmt, nullptr, nullptr, &error),
              ADBC_STATUS_OK)
        << error.message;
    AdbcStatementRelease(&stmt, &error);
  }

  // Commit must fail, but the outer transaction must remain usable.
  const uint8_t* rec_ptr = receipt_bytes.data();
  size_t rec_len = receipt_bytes.size();
  AdbcError commit_err = ADBC_ERROR_INIT;
  AdbcStatusCode rc = AdbcConnectionCommitIngestPartitions(
      &coordinator.conn, handle.bytes, handle.length, 1, &rec_ptr, &rec_len,
      /*rows_affected=*/nullptr, &commit_err);
  EXPECT_NE(rc, ADBC_STATUS_OK);
  if (commit_err.release) commit_err.release(&commit_err);

  // Post-failure, the outer transaction must still be usable: a SELECT must
  // succeed (would be rejected with "current transaction is aborted" if the
  // savepoint rollback was skipped).
  {
    AdbcStatement stmt{};
    ASSERT_EQ(AdbcStatementNew(&coordinator.conn, &stmt, &error), ADBC_STATUS_OK);
    ASSERT_EQ(AdbcStatementSetSqlQuery(&stmt, "SELECT 2", &error), ADBC_STATUS_OK);
    ArrowArrayStream post{};
    ASSERT_EQ(AdbcStatementExecuteQuery(&stmt, &post, nullptr, &error), ADBC_STATUS_OK)
        << error.message;
    post.release(&post);
    AdbcStatementRelease(&stmt, &error);
  }

  // The driver's savepoint must have been RELEASEd (not left on the stack):
  // if it were still live, a caller SAVEPOINT of the same name would still
  // work, so instead prove release by issuing a ROLLBACK TO on the driver's
  // savepoint name and expecting it to fail ("savepoint does not exist").
  // That asserts the savepoint is no longer on the stack after abort.
  {
    ASSERT_GE(handle.length, static_cast<size_t>(4 + 16));
    std::string driver_savepoint = "adbc_ingest_commit_";
    static const char kHex[] = "0123456789abcdef";
    for (size_t i = 0; i < 16; i++) {
      uint8_t b = handle.bytes[4 + i];
      driver_savepoint += kHex[b >> 4];
      driver_savepoint += kHex[b & 0x0F];
    }
    AdbcStatement stmt{};
    ASSERT_EQ(AdbcStatementNew(&coordinator.conn, &stmt, &error), ADBC_STATUS_OK);
    std::string sql = "ROLLBACK TO SAVEPOINT " + driver_savepoint;
    ASSERT_EQ(AdbcStatementSetSqlQuery(&stmt, sql.c_str(), &error), ADBC_STATUS_OK);
    AdbcError probe_err = ADBC_ERROR_INIT;
    AdbcStatusCode probe_rc =
        AdbcStatementExecuteQuery(&stmt, nullptr, nullptr, &probe_err);
    EXPECT_NE(probe_rc, ADBC_STATUS_OK)
        << "driver savepoint was not released after commit failure";
    if (probe_err.release) probe_err.release(&probe_err);
    AdbcStatementRelease(&stmt, &error);
  }

  // Cleanly commit the (now empty) outer transaction.
  AdbcConnectionRollback(&coordinator.conn, &error);
  AdbcConnectionSetOption(&coordinator.conn, ADBC_CONNECTION_OPTION_AUTOCOMMIT, "true",
                          &error);

  // Best-effort cleanup of any remaining staging tables.
  AdbcConnectionAbortIngestPartitions(&coordinator.conn, handle.bytes, handle.length, 0,
                                      nullptr, nullptr, &error);

  handle.release(&handle);
  Cleanup(&coordinator, table, &error);
  CloseConn(&coordinator, &error);
}

// Calling CommitIngestPartitions while the connection is in an aborted
// transaction must fail cleanly with ADBC_STATUS_INVALID_STATE instead of
// issuing SQL that would further mutate caller transaction state.
TEST_F(PostgresPartitionedIngestTest, CommitRejectsAbortedTransaction) {
  const char* uri = RequireUri();
  if (!uri) return;
  const std::string table = "adbc_partitioned_ingest_inerror_test";

  AdbcError error = ADBC_ERROR_INIT;
  ConnPair c;
  OpenConn(&c, &error, uri);
  Cleanup(&c, table, &error);

  ArrowSchema ingest_schema{};
  MakeIngestSchema(&ingest_schema);

  AdbcIngestHandle handle{};
  ASSERT_EQ(AdbcConnectionBeginIngestPartitions(
                &c.conn, nullptr, nullptr, table.c_str(),
                ADBC_INGEST_OPTION_MODE_CREATE, &ingest_schema, &handle, &error),
            ADBC_STATUS_OK)
      << error.message;
  ingest_schema.release(&ingest_schema);

  ArrowArrayStream stream{};
  MakeBatchStream(&stream, 0, 5);
  AdbcIngestReceipt rec{};
  ASSERT_EQ(AdbcConnectionWriteIngestPartition(&c.conn, handle.bytes, handle.length,
                                               &stream, &rec, &error),
            ADBC_STATUS_OK)
      << error.message;
  std::vector<uint8_t> receipt_bytes(rec.bytes, rec.bytes + rec.length);
  rec.release(&rec);

  // Put the connection in an aborted-transaction state: disable autocommit,
  // then issue a statement that errors (SELECT from a missing relation).
  ASSERT_EQ(AdbcConnectionSetOption(&c.conn, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                    ADBC_OPTION_VALUE_DISABLED, &error),
            ADBC_STATUS_OK)
      << error.message;
  {
    AdbcStatement stmt{};
    ASSERT_EQ(AdbcStatementNew(&c.conn, &stmt, &error), ADBC_STATUS_OK);
    ASSERT_EQ(AdbcStatementSetSqlQuery(
                  &stmt, "SELECT * FROM nonexistent_relation_adbc_x", &error),
              ADBC_STATUS_OK);
    AdbcStatementExecuteQuery(&stmt, nullptr, nullptr, &error);
    AdbcStatementRelease(&stmt, &error);
  }

  const uint8_t* rec_ptr = receipt_bytes.data();
  size_t rec_len = receipt_bytes.size();
  AdbcError commit_err = ADBC_ERROR_INIT;
  AdbcStatusCode rc = AdbcConnectionCommitIngestPartitions(
      &c.conn, handle.bytes, handle.length, 1, &rec_ptr, &rec_len,
      /*rows_affected=*/nullptr, &commit_err);
  EXPECT_EQ(rc, ADBC_STATUS_INVALID_STATE) << (commit_err.message ? commit_err.message : "");
  if (commit_err.release) commit_err.release(&commit_err);

  // Restore autocommit so Cleanup can drop the table.
  AdbcConnectionRollback(&c.conn, &error);
  AdbcConnectionSetOption(&c.conn, ADBC_CONNECTION_OPTION_AUTOCOMMIT, "true", &error);

  // Best-effort cleanup of staging tables left behind by the failed commit.
  AdbcConnectionAbortIngestPartitions(&c.conn, handle.bytes, handle.length, 0, nullptr,
                                      nullptr, &error);

  handle.release(&handle);
  Cleanup(&c, table, &error);
  CloseConn(&c, &error);
}
