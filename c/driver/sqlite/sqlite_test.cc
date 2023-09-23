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

#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>

#include <adbc.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>

#include "statement_reader.h"
#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

// -- ADBC Test Suite ------------------------------------------------

class SqliteQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    // Shared DB required for transaction tests
    return AdbcDatabaseSetOption(
        database, "uri", "file:Sqlite_Transactions?mode=memory&cache=shared", error);
  }

  AdbcStatusCode DropTable(struct AdbcConnection* connection, const std::string& name,
                           struct AdbcError* error) const override {
    adbc_validation::Handle<struct AdbcStatement> statement;
    RAISE_ADBC(AdbcStatementNew(connection, &statement.value, error));

    std::string query = "DROP TABLE IF EXISTS \"" + name + "\"";
    RAISE_ADBC(AdbcStatementSetSqlQuery(&statement.value, query.c_str(), error));
    RAISE_ADBC(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
    return AdbcStatementRelease(&statement.value, error);
  }

  AdbcStatusCode DropTempTable(struct AdbcConnection* connection, const std::string& name,
                               struct AdbcError* error) const override {
    adbc_validation::Handle<struct AdbcStatement> statement;
    RAISE_ADBC(AdbcStatementNew(connection, &statement.value, error));

    std::string query = "DROP TABLE IF EXISTS temp . \"" + name + "\"";
    RAISE_ADBC(AdbcStatementSetSqlQuery(&statement.value, query.c_str(), error));
    RAISE_ADBC(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
    return AdbcStatementRelease(&statement.value, error);
  }

  std::string BindParameter(int index) const override { return "?"; }

  ArrowType IngestSelectRoundTripType(ArrowType ingest_type) const override {
    switch (ingest_type) {
      case NANOARROW_TYPE_BOOL:
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_INT16:
      case NANOARROW_TYPE_INT32:
      case NANOARROW_TYPE_INT64:
      case NANOARROW_TYPE_UINT8:
      case NANOARROW_TYPE_UINT16:
      case NANOARROW_TYPE_UINT32:
      case NANOARROW_TYPE_UINT64:
        return NANOARROW_TYPE_INT64;
      case NANOARROW_TYPE_FLOAT:
      case NANOARROW_TYPE_DOUBLE:
        return NANOARROW_TYPE_DOUBLE;
      case NANOARROW_TYPE_LARGE_STRING:
      case NANOARROW_TYPE_DATE32:
      case NANOARROW_TYPE_TIMESTAMP:
        return NANOARROW_TYPE_STRING;
      default:
        return ingest_type;
    }
  }

  std::optional<std::string> PrimaryKeyTableDdl(std::string_view name) const override {
    std::string ddl = "CREATE TABLE ";
    ddl += name;
    ddl += " (id INTEGER PRIMARY KEY)";
    return ddl;
  }

  std::optional<std::string> CompositePrimaryKeyTableDdl(
      std::string_view name) const override {
    std::string ddl = "CREATE TABLE ";
    ddl += name;
    ddl += " (id_primary_col1 INTEGER, id_primary_col2 INTEGER,";
    ddl += " PRIMARY KEY (id_primary_col1, id_primary_col2));";
    return ddl;
  }

  bool supports_bulk_ingest(const char* mode) const override {
    return std::strcmp(mode, ADBC_INGEST_OPTION_MODE_APPEND) == 0 ||
           std::strcmp(mode, ADBC_INGEST_OPTION_MODE_CREATE) == 0;
  }
  bool supports_bulk_ingest_catalog() const override { return true; }
  bool supports_bulk_ingest_temporary() const override { return true; }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_get_option() const override { return false; }
  std::optional<adbc_validation::SqlInfoValue> supports_get_sql_info(
      uint32_t info_code) const override {
    switch (info_code) {
      case ADBC_INFO_DRIVER_NAME:
        return "ADBC SQLite Driver";
      case ADBC_INFO_DRIVER_VERSION:
        return "(unknown)";
      case ADBC_INFO_VENDOR_NAME:
        return "SQLite";
      case ADBC_INFO_VENDOR_VERSION:
        return "3.";
      default:
        return std::nullopt;
    }
  }

  std::string catalog() const override { return "main"; }
  std::string db_schema() const override { return ""; }
};

class SqliteDatabaseTest : public ::testing::Test, public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SqliteQuirks quirks_;
};
ADBCV_TEST_DATABASE(SqliteDatabaseTest)

class SqliteConnectionTest : public ::testing::Test,
                             public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  SqliteQuirks quirks_;
};
ADBCV_TEST_CONNECTION(SqliteConnectionTest)

TEST_F(SqliteConnectionTest, GetInfoMetadata) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error),
              adbc_validation::IsOkStatus(&error));

  adbc_validation::StreamReader reader;
  std::vector<uint32_t> info = {
      ADBC_INFO_DRIVER_NAME,
      ADBC_INFO_DRIVER_VERSION,
      ADBC_INFO_VENDOR_NAME,
      ADBC_INFO_VENDOR_VERSION,
  };
  ASSERT_THAT(AdbcConnectionGetInfo(&connection, info.data(), info.size(),
                                    &reader.stream.value, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());

  std::vector<uint32_t> seen;
  while (true) {
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    if (!reader.array->release) break;

    for (int64_t row = 0; row < reader.array->length; row++) {
      ASSERT_FALSE(ArrowArrayViewIsNull(reader.array_view->children[0], row));
      const uint32_t code =
          reader.array_view->children[0]->buffer_views[1].data.as_uint32[row];
      seen.push_back(code);

      int str_child_index = 0;
      struct ArrowArrayView* str_child =
          reader.array_view->children[1]->children[str_child_index];
      switch (code) {
        case ADBC_INFO_DRIVER_NAME: {
          ArrowStringView val = ArrowArrayViewGetStringUnsafe(str_child, 0);
          EXPECT_EQ("ADBC SQLite Driver", std::string(val.data, val.size_bytes));
          break;
        }
        case ADBC_INFO_DRIVER_VERSION: {
          ArrowStringView val = ArrowArrayViewGetStringUnsafe(str_child, 1);
          EXPECT_EQ("(unknown)", std::string(val.data, val.size_bytes));
          break;
        }
        case ADBC_INFO_VENDOR_NAME: {
          ArrowStringView val = ArrowArrayViewGetStringUnsafe(str_child, 2);
          EXPECT_EQ("SQLite", std::string(val.data, val.size_bytes));
          break;
        }
        case ADBC_INFO_VENDOR_VERSION: {
          ArrowStringView val = ArrowArrayViewGetStringUnsafe(str_child, 3);
          EXPECT_THAT(std::string(val.data, val.size_bytes),
                      ::testing::MatchesRegex("3\\..*"));
        }
        default:
          // Ignored
          break;
      }
    }
  }
  ASSERT_THAT(seen, ::testing::UnorderedElementsAreArray(info));
}

class SqliteStatementTest : public ::testing::Test,
                            public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  void TestSqlIngestUInt64() {
    std::vector<std::optional<uint64_t>> values = {std::nullopt, 0, INT64_MAX};
    return TestSqlIngestType(NANOARROW_TYPE_UINT64, values);
  }

  void TestSqlIngestBinary() { GTEST_SKIP() << "Cannot ingest BINARY (not implemented)"; }
  void TestSqlIngestDuration() {
    GTEST_SKIP() << "Cannot ingest DURATION (not implemented)";
  }
  void TestSqlIngestInterval() {
    GTEST_SKIP() << "Cannot ingest Interval (not implemented)";
  }

 protected:
  void ValidateIngestedTemporalData(struct ArrowArrayView* values, ArrowType type,
                                    enum ArrowTimeUnit unit,
                                    const char* timezone) override {
    switch (type) {
      case NANOARROW_TYPE_TIMESTAMP: {
        std::vector<std::optional<std::string>> expected;
        switch (unit) {
          case (NANOARROW_TIME_UNIT_SECOND):
            expected.insert(expected.end(),
                            {std::nullopt, "1969-12-31T23:59:18", "1970-01-01T00:00:00",
                             "1970-01-01T00:00:42"});
            break;
          case (NANOARROW_TIME_UNIT_MILLI):
            expected.insert(expected.end(),
                            {std::nullopt, "1969-12-31T23:59:59.958",
                             "1970-01-01T00:00:00.000", "1970-01-01T00:00:00.042"});
            break;
          case (NANOARROW_TIME_UNIT_MICRO):
            expected.insert(expected.end(),
                            {std::nullopt, "1969-12-31T23:59:59.999958",
                             "1970-01-01T00:00:00.000000", "1970-01-01T00:00:00.000042"});
            break;
          case (NANOARROW_TIME_UNIT_NANO):
            expected.insert(
                expected.end(),
                {std::nullopt, "1969-12-31T23:59:59.999999958",
                 "1970-01-01T00:00:00.000000000", "1970-01-01T00:00:00.000000042"});
            break;
        }
        ASSERT_NO_FATAL_FAILURE(
            adbc_validation::CompareArray<std::string>(values, expected));
        break;
      }
      default:
        FAIL() << "ValidateIngestedTemporalData not implemented for type " << type;
    }
  }

  SqliteQuirks quirks_;
};
ADBCV_TEST_STATEMENT(SqliteStatementTest)

TEST_F(SqliteStatementTest, SqlIngestNameEscaping) {
  ASSERT_THAT(quirks()->DropTable(&connection, "test-table", &error),
              adbc_validation::IsOkStatus(&error));

  std::string table = "test-table";
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(
      adbc_validation::MakeSchema(&schema.value, {{"index", NANOARROW_TYPE_INT64},
                                                  {"create", NANOARROW_TYPE_STRING}}),
      adbc_validation::IsOkErrno());
  ASSERT_THAT((adbc_validation::MakeBatch<int64_t, std::string>(
                  &schema.value, &array.value, &na_error, {42, -42, std::nullopt},
                  {"foo", std::nullopt, ""})),
              adbc_validation::IsOkErrno(&na_error));

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     table.c_str(), &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              adbc_validation::IsOkStatus(&error));

  int64_t rows_affected = 0;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
              adbc_validation::IsOkStatus(&error));
  ASSERT_EQ(3, rows_affected);
}

// -- SQLite Specific Tests ------------------------------------------

constexpr size_t kInferRows = 16;

using adbc_validation::CompareArray;
using adbc_validation::Handle;
using adbc_validation::IsOkErrno;
using adbc_validation::IsOkStatus;

/// Specific tests of the type-inferring reader
class SqliteReaderTest : public ::testing::Test {
 public:
  void SetUp() override {
    std::memset(&error, 0, sizeof(error));
    std::memset(&binder, 0, sizeof(binder));
    ASSERT_EQ(SQLITE_OK, sqlite3_open_v2(
                             ":memory:", &db,
                             SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE | SQLITE_OPEN_URI,
                             /*zVfs=*/nullptr));
  }
  void TearDown() override {
    if (error.release) error.release(&error);
    AdbcSqliteBinderRelease(&binder);
    sqlite3_finalize(stmt);
    sqlite3_close(db);
  }

  void Exec(const std::string& query) {
    ASSERT_EQ(SQLITE_OK, sqlite3_prepare_v2(db, query.c_str(), query.size(), &stmt,
                                            /*pzTail=*/nullptr));
    ASSERT_EQ(SQLITE_DONE, sqlite3_step(stmt));
    sqlite3_finalize(stmt);
    stmt = nullptr;
  }

  void Bind(struct ArrowArray* batch, struct ArrowSchema* schema) {
    ASSERT_THAT(AdbcSqliteBinderSetArray(&binder, batch, schema, &error),
                IsOkStatus(&error));
  }

  void Bind(struct ArrowArrayStream* stream) {
    ASSERT_THAT(AdbcSqliteBinderSetArrayStream(&binder, stream, &error),
                IsOkStatus(&error));
  }

  void ExecSelect(const std::string& values, size_t infer_rows,
                  adbc_validation::StreamReader* reader) {
    ASSERT_NO_FATAL_FAILURE(Exec("CREATE TABLE foo (col)"));
    ASSERT_NO_FATAL_FAILURE(Exec("INSERT INTO foo VALUES " + values));
    const std::string query = "SELECT * FROM foo";
    ASSERT_NO_FATAL_FAILURE(Exec(query, infer_rows, reader));
    ASSERT_EQ(1, reader->schema->n_children);
  }

  void Exec(const std::string& query, size_t infer_rows,
            adbc_validation::StreamReader* reader) {
    ASSERT_EQ(SQLITE_OK, sqlite3_prepare_v2(db, query.c_str(), query.size(), &stmt,
                                            /*pzTail=*/nullptr));
    struct AdbcSqliteBinder* binder =
        this->binder.schema.release ? &this->binder : nullptr;
    ASSERT_THAT(AdbcSqliteExportReader(db, stmt, binder, infer_rows,
                                       &reader->stream.value, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader->GetSchema());
  }

 protected:
  sqlite3* db = nullptr;
  sqlite3_stmt* stmt = nullptr;
  struct AdbcError error;
  struct AdbcSqliteBinder binder;
};

TEST_F(SqliteReaderTest, IntsNulls) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(ExecSelect("(NULL), (1), (NULL), (-1)", kInferRows, &reader));
  ASSERT_EQ(NANOARROW_TYPE_INT64, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(reader.array_view->children[0],
                                                {std::nullopt, 1, std::nullopt, -1}));
}

TEST_F(SqliteReaderTest, FloatsNulls) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(
      ExecSelect("(NULL), (1.0), (NULL), (-1.0), (0.0)", kInferRows, &reader));
  ASSERT_EQ(NANOARROW_TYPE_DOUBLE, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(CompareArray<double>(
      reader.array_view->children[0], {std::nullopt, 1.0, std::nullopt, -1.0, 0.0}));
}

TEST_F(SqliteReaderTest, IntsFloatsNulls) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(
      ExecSelect("(NULL), (1), (NULL), (-1.0), (0)", kInferRows, &reader));
  ASSERT_EQ(NANOARROW_TYPE_DOUBLE, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(CompareArray<double>(
      reader.array_view->children[0], {std::nullopt, 1.0, std::nullopt, -1.0, 0.0}));
}

TEST_F(SqliteReaderTest, IntsNullsStrsNullsInts) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(ExecSelect(
      R"((NULL), (1), (NULL), (-1), ("foo"), (NULL), (""), (24))", kInferRows, &reader));
  ASSERT_EQ(NANOARROW_TYPE_STRING, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(CompareArray<std::string>(
      reader.array_view->children[0],
      {std::nullopt, "1", std::nullopt, "-1", "foo", std::nullopt, "", "24"}));
}

TEST_F(SqliteReaderTest, IntExtremes) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(
      ExecSelect(R"((NULL), (9223372036854775807), (NULL), (-9223372036854775808))",
                 kInferRows, &reader));
  ASSERT_EQ(NANOARROW_TYPE_INT64, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0],
                            {std::nullopt, std::numeric_limits<int64_t>::max(),
                             std::nullopt, std::numeric_limits<int64_t>::min()}));
}

TEST_F(SqliteReaderTest, IntExtremesStrs) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(ExecSelect(
      R"((NULL), (9223372036854775807), (-9223372036854775808), (""), (9223372036854775807), (-9223372036854775808))",
      kInferRows, &reader));
  ASSERT_EQ(NANOARROW_TYPE_STRING, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(CompareArray<std::string>(reader.array_view->children[0],
                                                    {
                                                        std::nullopt,
                                                        "9223372036854775807",
                                                        "-9223372036854775808",
                                                        "",
                                                        "9223372036854775807",
                                                        "-9223372036854775808",
                                                    }));
}

TEST_F(SqliteReaderTest, FloatExtremes) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(
      ExecSelect(R"((NULL), (9e999), (NULL), (-9e999))", kInferRows, &reader));
  ASSERT_EQ(NANOARROW_TYPE_DOUBLE, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(CompareArray<double>(
      reader.array_view->children[0], {
                                          std::nullopt,
                                          std::numeric_limits<double>::infinity(),
                                          std::nullopt,
                                          -std::numeric_limits<double>::infinity(),
                                      }));
}

TEST_F(SqliteReaderTest, IntsFloatsStrs) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(
      ExecSelect(R"((1), (1.0), (""), (9e999), (-9e999))", kInferRows, &reader));
  ASSERT_EQ(NANOARROW_TYPE_STRING, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<std::string>(reader.array_view->children[0],
                                {"1.000000e+00", "1.000000e+00", "", "inf", "-inf"}));
}

TEST_F(SqliteReaderTest, InferIntReadInt) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(
      ExecSelect(R"((1), (NULL), (2), (NULL))", /*infer_rows=*/2, &reader));
  ASSERT_EQ(NANOARROW_TYPE_INT64, reader.fields[0].type);
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {1, std::nullopt}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {2, std::nullopt}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

TEST_F(SqliteReaderTest, InferIntRejectFloat) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(
      ExecSelect(R"((1), (NULL), (2E0), (NULL))", /*infer_rows=*/2, &reader));
  ASSERT_EQ(NANOARROW_TYPE_INT64, reader.fields[0].type);
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {1, std::nullopt}));

  ASSERT_THAT(reader.MaybeNext(), ::testing::Not(IsOkErrno()));
  ASSERT_THAT(reader.stream->get_last_error(&reader.stream.value),
              ::testing::HasSubstr(
                  "[SQLite] Type mismatch in column 0: expected INT64 but got DOUBLE"));
}

TEST_F(SqliteReaderTest, InferIntRejectStr) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(
      ExecSelect(R"((1), (NULL), (""), (NULL))", /*infer_rows=*/2, &reader));
  ASSERT_EQ(NANOARROW_TYPE_INT64, reader.fields[0].type);
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {1, std::nullopt}));

  ASSERT_THAT(reader.MaybeNext(), ::testing::Not(IsOkErrno()));
  ASSERT_THAT(
      reader.stream->get_last_error(&reader.stream.value),
      ::testing::HasSubstr(
          "[SQLite] Type mismatch in column 0: expected INT64 but got STRING/BINARY"));
}

TEST_F(SqliteReaderTest, InferFloatReadIntFloat) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(
      ExecSelect(R"((1E0), (NULL), (2E0), (3), (NULL))", /*infer_rows=*/2, &reader));
  ASSERT_EQ(NANOARROW_TYPE_DOUBLE, reader.fields[0].type);
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<double>(reader.array_view->children[0], {1.0, std::nullopt}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<double>(reader.array_view->children[0], {2.0, 3.0}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<double>(reader.array_view->children[0], {std::nullopt}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

TEST_F(SqliteReaderTest, InferFloatRejectStr) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(ExecSelect(R"((1E0), (NULL), (2E0), (3), (""), (NULL))",
                                     /*infer_rows=*/2, &reader));
  ASSERT_EQ(NANOARROW_TYPE_DOUBLE, reader.fields[0].type);
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<double>(reader.array_view->children[0], {1.0, std::nullopt}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<double>(reader.array_view->children[0], {2.0, 3.0}));

  ASSERT_THAT(reader.MaybeNext(), ::testing::Not(IsOkErrno()));
  ASSERT_THAT(
      reader.stream->get_last_error(&reader.stream.value),
      ::testing::HasSubstr(
          "[SQLite] Type mismatch in column 0: expected DOUBLE but got STRING/BINARY"));
}

TEST_F(SqliteReaderTest, InferStrReadAll) {
  adbc_validation::StreamReader reader;
  ASSERT_NO_FATAL_FAILURE(ExecSelect(R"((""), (NULL), (2), (3E0), ("foo"), (NULL))",
                                     /*infer_rows=*/2, &reader));
  ASSERT_EQ(NANOARROW_TYPE_STRING, reader.fields[0].type);
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<std::string>(reader.array_view->children[0], {"", std::nullopt}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<std::string>(reader.array_view->children[0], {"2", "3.0"}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<std::string>(reader.array_view->children[0], {"foo", std::nullopt}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

TEST_F(SqliteReaderTest, InferOneParam) {
  adbc_validation::StreamReader reader;
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> batch;

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value, {{"", NANOARROW_TYPE_INT64}}),
              IsOkErrno());
  ASSERT_THAT(
      adbc_validation::MakeBatch<int64_t>(&schema.value, &batch.value, /*error=*/nullptr,
                                          {std::nullopt, 2, 4, -1}),
      IsOkErrno());

  ASSERT_NO_FATAL_FAILURE(Bind(&batch.value, &schema.value));
  ASSERT_NO_FATAL_FAILURE(Exec("SELECT ?", /*infer_rows=*/2, &reader));

  ASSERT_EQ(1, reader.schema->n_children);
  ASSERT_EQ(NANOARROW_TYPE_INT64, reader.fields[0].type);
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {std::nullopt, 2}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(reader.array_view->children[0], {4, -1}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

TEST_F(SqliteReaderTest, InferOneParamStream) {
  adbc_validation::StreamReader reader;
  Handle<struct ArrowArrayStream> stream;
  Handle<struct ArrowSchema> schema;
  std::vector<struct ArrowArray> batches(3);

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value, {{"", NANOARROW_TYPE_INT64}}),
              IsOkErrno());
  ASSERT_THAT(adbc_validation::MakeBatch<int64_t>(&schema.value, &batches[0],
                                                  /*error=*/nullptr, {std::nullopt, 1}),
              IsOkErrno());
  ASSERT_THAT(adbc_validation::MakeBatch<int64_t>(&schema.value, &batches[1],
                                                  /*error=*/nullptr, {2, 3}),
              IsOkErrno());
  ASSERT_THAT(adbc_validation::MakeBatch<int64_t>(&schema.value, &batches[2],
                                                  /*error=*/nullptr, {4, std::nullopt}),
              IsOkErrno());
  adbc_validation::MakeStream(&stream.value, &schema.value, std::move(batches));

  ASSERT_NO_FATAL_FAILURE(Bind(&stream.value));
  ASSERT_NO_FATAL_FAILURE(Exec("SELECT ?", /*infer_rows=*/3, &reader));

  ASSERT_EQ(1, reader.schema->n_children);
  ASSERT_EQ(NANOARROW_TYPE_INT64, reader.fields[0].type);
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {std::nullopt, 1, 2}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {3, 4, std::nullopt}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

TEST_F(SqliteReaderTest, InferTypedParams) {
  adbc_validation::StreamReader reader;
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> batch;

  ASSERT_NO_FATAL_FAILURE(Exec("CREATE TABLE foo (idx, value)"));
  ASSERT_NO_FATAL_FAILURE(
      Exec(R"(INSERT INTO foo VALUES (0, "foo"), (1, NULL), (2, 4), (3, 1E2))"));

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value, {{"", NANOARROW_TYPE_INT64}}),
              IsOkErrno());
  ASSERT_THAT(adbc_validation::MakeBatch<int64_t>(&schema.value, &batch.value,
                                                  /*error=*/nullptr, {1, 2, 3, 0}),
              IsOkErrno());

  ASSERT_NO_FATAL_FAILURE(Bind(&batch.value, &schema.value));
  ASSERT_NO_FATAL_FAILURE(
      Exec("SELECT value FROM foo WHERE idx = ?", /*infer_rows=*/2, &reader));
  ASSERT_EQ(1, reader.schema->n_children);
  ASSERT_EQ(NANOARROW_TYPE_INT64, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {std::nullopt, 4}));
  ASSERT_THAT(reader.MaybeNext(), ::testing::Not(IsOkErrno()));
  ASSERT_THAT(reader.stream->get_last_error(&reader.stream.value),
              ::testing::HasSubstr(
                  "[SQLite] Type mismatch in column 0: expected INT64 but got DOUBLE"));
}

TEST_F(SqliteReaderTest, MultiValueParams) {
  // Regression test for apache/arrow-adbc#734
  adbc_validation::StreamReader reader;
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> batch;

  ASSERT_NO_FATAL_FAILURE(Exec("CREATE TABLE foo (col)"));
  ASSERT_NO_FATAL_FAILURE(
      Exec("INSERT INTO foo VALUES (1), (2), (2), (3), (3), (3), (4), (4), (4), (4)"));

  ASSERT_THAT(adbc_validation::MakeSchema(&schema.value, {{"", NANOARROW_TYPE_INT64}}),
              IsOkErrno());
  ASSERT_THAT(adbc_validation::MakeBatch<int64_t>(&schema.value, &batch.value,
                                                  /*error=*/nullptr, {4, 1, 3, 2}),
              IsOkErrno());

  ASSERT_NO_FATAL_FAILURE(Bind(&batch.value, &schema.value));
  ASSERT_NO_FATAL_FAILURE(
      Exec("SELECT col FROM foo WHERE col = ?", /*infer_rows=*/3, &reader));
  ASSERT_EQ(1, reader.schema->n_children);
  ASSERT_EQ(NANOARROW_TYPE_INT64, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {4, 4, 4}));

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {4, 1, 3}));

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {3, 3, 2}));

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(reader.array_view->children[0], {2}));

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

template <typename CType>
class SqliteNumericParamTest : public SqliteReaderTest,
                               public ::testing::WithParamInterface<ArrowType> {
 public:
  void Test(ArrowType expected_type) {
    adbc_validation::StreamReader reader;
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> batch;

    ASSERT_THAT(adbc_validation::MakeSchema(&schema.value, {{"", GetParam()}}),
                IsOkErrno());
    ASSERT_THAT(adbc_validation::MakeBatch<CType>(&schema.value, &batch.value,
                                                  /*error=*/nullptr,
                                                  {std::nullopt, 0, 1, 2, 4, 8}),
                IsOkErrno());

    ASSERT_NO_FATAL_FAILURE(Bind(&batch.value, &schema.value));
    ASSERT_NO_FATAL_FAILURE(Exec("SELECT ?", /*infer_rows=*/2, &reader));

    ASSERT_EQ(1, reader.schema->n_children);
    ASSERT_EQ(expected_type, reader.fields[0].type);
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NO_FATAL_FAILURE(
        CompareArray<CType>(reader.array_view->children[0], {std::nullopt, 0}));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NO_FATAL_FAILURE(CompareArray<CType>(reader.array_view->children[0], {1, 2}));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NO_FATAL_FAILURE(CompareArray<CType>(reader.array_view->children[0], {4, 8}));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }
};

class SqliteIntParamTest : public SqliteNumericParamTest<int64_t> {};

TEST_P(SqliteIntParamTest, BindInt) {
  ASSERT_NO_FATAL_FAILURE(Test(NANOARROW_TYPE_INT64));
}

INSTANTIATE_TEST_SUITE_P(IntTypes, SqliteIntParamTest,
                         ::testing::Values(NANOARROW_TYPE_UINT8, NANOARROW_TYPE_UINT16,
                                           NANOARROW_TYPE_UINT32, NANOARROW_TYPE_UINT64,
                                           NANOARROW_TYPE_INT8, NANOARROW_TYPE_INT16,
                                           NANOARROW_TYPE_INT32, NANOARROW_TYPE_INT64));

class SqliteFloatParamTest : public SqliteNumericParamTest<double> {};

TEST_P(SqliteFloatParamTest, BindFloat) {
  ASSERT_NO_FATAL_FAILURE(Test(NANOARROW_TYPE_DOUBLE));
}

INSTANTIATE_TEST_SUITE_P(FloatTypes, SqliteFloatParamTest,
                         ::testing::Values(
                             // XXX: AppendDouble currently doesn't really work with
                             // floats (FLT_MIN/FLT_MAX isn't the right thing)

                             // NANOARROW_TYPE_FLOAT,
                             NANOARROW_TYPE_DOUBLE));
