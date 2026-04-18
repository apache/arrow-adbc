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
