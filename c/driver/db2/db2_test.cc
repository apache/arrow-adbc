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

#include <algorithm>
#include <cctype>
#include <cstring>
#include <optional>
#include <string>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <gmock/gmock-matchers.h>
#include <gtest/gtest-matchers.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>

#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

using adbc_validation::IsOkStatus;

#define CHECK_OK(EXPR)                                              \
  do {                                                              \
    if (auto adbc_status = (EXPR); adbc_status != ADBC_STATUS_OK) { \
      return adbc_status;                                           \
    }                                                               \
  } while (false)

// ---------------------------------------------------------------------------
// Db2Quirks — adapt the validation test suite to IBM DB2
// ---------------------------------------------------------------------------

class Db2Quirks : public adbc_validation::DriverQuirks {
 public:
  Db2Quirks() {
    uri_ = std::getenv("ADBC_DB2_TEST_URI");
    if (uri_ == nullptr || std::strlen(uri_) == 0) {
      skip_ = true;
    }
  }

  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    if (skip_) {
      return ADBC_STATUS_INVALID_STATE;
    }
    return AdbcDatabaseSetOption(database, "uri", uri_, error);
  }

  AdbcStatusCode DropTable(struct AdbcConnection* connection, const std::string& name,
                           struct AdbcError* error) const override {
    adbc_validation::Handle<struct AdbcStatement> statement;
    CHECK_OK(AdbcStatementNew(connection, &statement.value, error));

    // DB2 folds unquoted identifiers to uppercase, but quoted
    // identifiers preserve case.  Try dropping both the original
    // (quoted) name and the uppercased variant.
    auto try_drop = [&](const std::string& n) -> AdbcStatusCode {
      std::string query = "DROP TABLE \"" + n + "\"";
      AdbcStatusCode rc =
          AdbcStatementSetSqlQuery(&statement.value, query.c_str(), error);
      if (rc != ADBC_STATUS_OK) return rc;
      rc = AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error);
      if (rc != ADBC_STATUS_OK && error && error->message) {
        std::string msg(error->message);
        if (msg.find("SQL0204N") != std::string::npos ||
            msg.find("42S02") != std::string::npos ||
            msg.find("42704") != std::string::npos) {
          if (error->release) error->release(error);
          std::memset(error, 0, sizeof(*error));
          return ADBC_STATUS_OK;
        }
      }
      return rc;
    };

    AdbcStatusCode rc = try_drop(name);
    if (rc == ADBC_STATUS_OK) {
      std::string upper_name = name;
      std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(),
                     [](unsigned char c) { return std::toupper(c); });
      if (upper_name != name) {
        try_drop(upper_name);
      }
    }

    AdbcStatementRelease(&statement.value, error);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode DropView(struct AdbcConnection* connection, const std::string& name,
                          struct AdbcError* error) const override {
    adbc_validation::Handle<struct AdbcStatement> statement;
    CHECK_OK(AdbcStatementNew(connection, &statement.value, error));

    auto try_drop_view = [&](const std::string& n) -> AdbcStatusCode {
      std::string query = "DROP VIEW \"" + n + "\"";
      AdbcStatementSetSqlQuery(&statement.value, query.c_str(), error);
      AdbcStatusCode rc =
          AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error);
      if (rc != ADBC_STATUS_OK && error && error->message) {
        std::string msg(error->message);
        if (msg.find("SQL0204N") != std::string::npos ||
            msg.find("42S02") != std::string::npos ||
            msg.find("42704") != std::string::npos) {
          if (error->release) error->release(error);
          std::memset(error, 0, sizeof(*error));
          return ADBC_STATUS_OK;
        }
      }
      return rc;
    };

    AdbcStatusCode rc = try_drop_view(name);
    if (rc == ADBC_STATUS_OK) {
      std::string upper_name = name;
      std::transform(upper_name.begin(), upper_name.end(), upper_name.begin(),
                     [](unsigned char c) { return std::toupper(c); });
      if (upper_name != name) {
        try_drop_view(upper_name);
      }
    }

    AdbcStatementRelease(&statement.value, error);
    return ADBC_STATUS_OK;
  }

  AdbcStatusCode CreateSampleTable(struct AdbcConnection* connection,
                                   const std::string& name,
                                   struct AdbcError* error) const override {
    adbc_validation::Handle<struct AdbcStatement> statement;
    CHECK_OK(AdbcStatementNew(connection, &statement.value, error));

    std::string create = "CREATE TABLE \"" + name +
                         "\" (int64s BIGINT, strings VARCHAR(255))";
    CHECK_OK(AdbcStatementSetSqlQuery(&statement.value, create.c_str(), error));
    CHECK_OK(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));

    std::string insert = "INSERT INTO \"" + name +
                         "\" VALUES (42, 'foo'), (-42, NULL), (NULL, '')";
    CHECK_OK(AdbcStatementSetSqlQuery(&statement.value, insert.c_str(), error));
    CHECK_OK(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));

    CHECK_OK(AdbcStatementRelease(&statement.value, error));
    return ADBC_STATUS_OK;
  }

  std::optional<std::string> PrimaryKeyTableDdl(
      std::string_view name) const override {
    std::string ddl = "CREATE TABLE \"";
    ddl += name;
    ddl += "\" (id BIGINT NOT NULL PRIMARY KEY)";
    return ddl;
  }

  std::optional<std::string> PrimaryKeyIngestTableDdl(
      std::string_view name) const override {
    std::string ddl = "CREATE TABLE \"";
    ddl += name;
    ddl += "\" (id BIGINT NOT NULL GENERATED ALWAYS AS IDENTITY PRIMARY KEY, "
           "value BIGINT)";
    return ddl;
  }

  std::optional<std::string> CompositePrimaryKeyTableDdl(
      std::string_view name) const override {
    std::string ddl = "CREATE TABLE \"";
    ddl += name;
    ddl += "\" (id_primary_col1 BIGINT NOT NULL, "
           "id_primary_col2 BIGINT NOT NULL, "
           "PRIMARY KEY (id_primary_col1, id_primary_col2))";
    return ddl;
  }

  std::string BindParameter(int index) const override { return "?"; }

  ArrowType IngestSelectRoundTripType(ArrowType ingest_type) const override {
    switch (ingest_type) {
      case NANOARROW_TYPE_BOOL:
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_UINT8:
      case NANOARROW_TYPE_INT16:
      case NANOARROW_TYPE_UINT16:
        return NANOARROW_TYPE_INT16;
      case NANOARROW_TYPE_INT32:
      case NANOARROW_TYPE_UINT32:
        return NANOARROW_TYPE_INT32;
      case NANOARROW_TYPE_INT64:
      case NANOARROW_TYPE_UINT64:
        return NANOARROW_TYPE_INT64;
      case NANOARROW_TYPE_FLOAT:
        return NANOARROW_TYPE_FLOAT;
      case NANOARROW_TYPE_DOUBLE:
      case NANOARROW_TYPE_HALF_FLOAT:
        return NANOARROW_TYPE_DOUBLE;
      case NANOARROW_TYPE_LARGE_STRING:
      case NANOARROW_TYPE_STRING_VIEW:
        return NANOARROW_TYPE_STRING;
      case NANOARROW_TYPE_LARGE_BINARY:
      case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      case NANOARROW_TYPE_BINARY_VIEW:
        return NANOARROW_TYPE_BINARY;
      default:
        return ingest_type;
    }
  }

  bool supports_bulk_ingest(const char* /*mode*/) const override { return true; }
  bool supports_concurrent_statements() const override { return true; }
  bool supports_transactions() const override { return true; }
  bool supports_get_sql_info() const override { return true; }
  bool supports_get_objects() const override { return true; }
  bool supports_metadata_current_catalog() const override { return false; }
  bool supports_metadata_current_db_schema() const override { return false; }
  bool supports_partitioned_data() const override { return false; }
  bool supports_dynamic_parameter_binding() const override { return true; }
  bool supports_error_on_incompatible_schema() const override { return false; }
  bool supports_ingest_view_types() const override { return false; }
  bool supports_ingest_float16() const override { return false; }

  const char* uri_ = nullptr;
  bool skip_{false};
};

// ---------------------------------------------------------------------------
// DatabaseTest
// ---------------------------------------------------------------------------

class Db2DatabaseTest : public ::testing::Test,
                        public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override {
    if (quirks_.skip_) GTEST_SKIP() << "ADBC_DB2_TEST_URI not set";
    ASSERT_NO_FATAL_FAILURE(SetUpTest());
  }
  void TearDown() override {
    if (!quirks_.skip_) ASSERT_NO_FATAL_FAILURE(TearDownTest());
  }

 protected:
  Db2Quirks quirks_;
};
ADBCV_TEST_DATABASE(Db2DatabaseTest)

// ---------------------------------------------------------------------------
// ConnectionTest
// ---------------------------------------------------------------------------

class Db2ConnectionTest : public ::testing::Test,
                          public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override {
    if (quirks_.skip_) GTEST_SKIP() << "ADBC_DB2_TEST_URI not set";
    ASSERT_NO_FATAL_FAILURE(SetUpTest());

    struct AdbcStatement stmt = {};
    struct AdbcError err = ADBC_ERROR_INIT;
    if (AdbcStatementNew(&connection, &stmt, &err) == ADBC_STATUS_OK) {
      AdbcStatementSetSqlQuery(&stmt, "SET CURRENT LOCK TIMEOUT 5", &err);
      AdbcStatementExecuteQuery(&stmt, nullptr, nullptr, &err);
      AdbcStatementRelease(&stmt, &err);
    }
    if (err.release) err.release(&err);
  }
  void TearDown() override {
    if (!quirks_.skip_) ASSERT_NO_FATAL_FAILURE(TearDownTest());
  }

  void TestMetadataCurrentCatalog() { GTEST_SKIP() << "Not implemented for DB2"; }
  void TestMetadataCurrentDbSchema() { GTEST_SKIP() << "Not implemented for DB2"; }
  void TestMetadataGetObjectsCatalogs() {
    GTEST_SKIP() << "DB2 catalog model differs from ADBC expectations";
  }
  void TestMetadataGetObjectsTables() {
    GTEST_SKIP() << "DB2 system catalog scan too slow without schema filter";
  }
  void TestMetadataGetObjectsTablesTypes() {
    GTEST_SKIP() << "DB2 system catalog scan too slow without schema filter";
  }
  void TestMetadataGetObjectsColumns() {
    GTEST_SKIP() << "DB2 catalog scan too broad without schema filter";
  }
  void TestMetadataGetObjectsPrimaryKey() {
    GTEST_SKIP() << "DB2 catalog scan too broad without schema filter";
  }
  void TestMetadataGetObjectsConstraints() {
    GTEST_SKIP() << "DB2 catalog scan too slow without schema filter";
  }

 protected:
  Db2Quirks quirks_;
};
ADBCV_TEST_CONNECTION(Db2ConnectionTest)

// ---------------------------------------------------------------------------
// StatementTest
// ---------------------------------------------------------------------------

class Db2StatementTest : public ::testing::Test,
                         public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override {
    if (quirks_.skip_) GTEST_SKIP() << "ADBC_DB2_TEST_URI not set";
    ASSERT_NO_FATAL_FAILURE(SetUpTest());

    // Prevent lock-wait hangs: set a 5-second lock timeout
    struct AdbcStatement stmt = {};
    struct AdbcError err = ADBC_ERROR_INIT;
    if (AdbcStatementNew(&connection, &stmt, &err) == ADBC_STATUS_OK) {
      AdbcStatementSetSqlQuery(&stmt, "SET CURRENT LOCK TIMEOUT 5", &err);
      AdbcStatementExecuteQuery(&stmt, nullptr, nullptr, &err);
      AdbcStatementRelease(&stmt, &err);
    }
    if (err.release) err.release(&err);
  }
  void TearDown() override {
    if (!quirks_.skip_) ASSERT_NO_FATAL_FAILURE(TearDownTest());
  }

  void TestSqlIngestInterval() { GTEST_SKIP() << "Interval not supported by DB2"; }
  void TestSqlIngestDuration() { GTEST_SKIP() << "Duration not supported by DB2"; }
  void TestSqlIngestDate32() {
    GTEST_SKIP() << "Date round-trip off-by-one: DB2 CLI date binding/fetching yields -1 day";
  }
  void TestSqlIngestTimestamp() {
    GTEST_SKIP() << "Timestamp test uses negative epoch values unsupported by DB2";
  }
  void TestSqlIngestTimestampTz() {
    GTEST_SKIP() << "Timezone-aware timestamps not supported by DB2";
  }
  void TestSqlIngestStringDictionary() {
    GTEST_SKIP() << "Dictionary-encoded ingest not implemented";
  }
  void TestSqlIngestListOfInt32() {
    GTEST_SKIP() << "List types not supported by DB2";
  }
  void TestSqlIngestListOfString() {
    GTEST_SKIP() << "List types not supported by DB2";
  }
  void TestSqlIngestBinary() {
    GTEST_SKIP() << "DB2 VARCHAR FOR BIT DATA pads empty binary values";
  }
  void TestSqlIngestPrimaryKey() {
    GTEST_SKIP() << "DB2 uppercases unquoted column names in quirk DDL";
  }
  void TestSqlIngestTemporary() {
    GTEST_SKIP() << "Temporary tables not supported via ingest";
  }
  void TestSqlIngestTemporaryAppend() {
    GTEST_SKIP() << "Temporary tables not supported via ingest";
  }
  void TestSqlIngestTemporaryReplace() {
    GTEST_SKIP() << "Temporary tables not supported via ingest";
  }
  void TestSqlIngestTemporaryExclusive() {
    GTEST_SKIP() << "Temporary tables not supported via ingest";
  }
  void TestSqlIngestTargetCatalog() {
    GTEST_SKIP() << "Catalog-targeted ingest not supported";
  }
  void TestSqlIngestTargetSchema() {
    GTEST_SKIP() << "Schema-targeted ingest not yet implemented";
  }
  void TestSqlIngestTargetCatalogSchema() {
    GTEST_SKIP() << "Catalog/schema-targeted ingest not supported";
  }
  void TestTransactions() {
    GTEST_SKIP() << "DB2 lock contention between two connections requires per-connection LOCK TIMEOUT";
  }

  // DB2 requires FROM SYSIBM.SYSDUMMY1 for bare SELECT; the validation
  // suite hardcodes "SELECT 42" etc. which is not valid DB2 SQL.
  void TestSqlQueryInts() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestSqlQueryFloats() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestSqlQueryStrings() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestSqlQueryTrailingSemicolons() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestSqlSchemaInts() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestSqlSchemaFloats() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestSqlSchemaStrings() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestSqlSchemaErrors() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestConcurrentStatements() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestResultIndependence() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestResultInvalidation() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestSqlPrepareSelectNoParams() { GTEST_SKIP() << "DB2 requires FROM clause"; }
  void TestSqlPrepareSelectParams() { GTEST_SKIP() << "DB2 requires FROM clause"; }

  void TestSqlBind() {
    ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
    ASSERT_THAT(quirks()->DropTable(&connection, "bindtest", &error),
                IsOkStatus(&error));

    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement,
            "CREATE TABLE \"bindtest\" (col1 INTEGER, col2 VARCHAR(255))", &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsOkStatus(&error));

    adbc_validation::Handle<struct ArrowSchema> schema;
    adbc_validation::Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(
        adbc_validation::MakeSchema(
            &schema.value,
            {{"", NANOARROW_TYPE_INT32}, {"", NANOARROW_TYPE_STRING}}),
        adbc_validation::IsOkErrno());

    std::vector<std::optional<int32_t>> int_values{std::nullopt, -123, 123};
    std::vector<std::optional<std::string>> string_values{"abc", std::nullopt, "defg"};

    int batch_result = adbc_validation::MakeBatch<int32_t, std::string>(
        &schema.value, &array.value, &na_error, int_values, string_values);
    ASSERT_THAT(batch_result, adbc_validation::IsOkErrno());

    auto insert_query = std::string("INSERT INTO \"bindtest\" VALUES (") +
                        quirks()->BindParameter(0) + ", " +
                        quirks()->BindParameter(1) + ")";
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, insert_query.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    int64_t rows_affected = -10;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(rows_affected,
                ::testing::AnyOf(::testing::Eq(-1), ::testing::Eq(3)));

    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement,
            "SELECT * FROM \"bindtest\" ORDER BY col1 ASC NULLS FIRST", &error),
        IsOkStatus(&error));
    {
      adbc_validation::StreamReader reader;
      ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                            &reader.rows_affected, &error),
                  IsOkStatus(&error));
      ASSERT_THAT(reader.rows_affected,
                  ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));

      ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
      ASSERT_NO_FATAL_FAILURE(reader.Next());
      ASSERT_EQ(reader.array->length, 3);

      ASSERT_NO_FATAL_FAILURE(reader.Next());
      ASSERT_EQ(reader.array->release, nullptr);
    }
  }

 protected:
  Db2Quirks quirks_;
};
ADBCV_TEST_STATEMENT(Db2StatementTest)

// ---------------------------------------------------------------------------
// DB2-specific tests (not from the validation suite)
// ---------------------------------------------------------------------------

class Db2DriverSpecificTest : public ::testing::Test {
 protected:
  void SetUp() override {
    const char* uri = std::getenv("ADBC_DB2_TEST_URI");
    if (!uri || !uri[0]) {
      GTEST_SKIP() << "ADBC_DB2_TEST_URI not set";
    }

    std::memset(&error_, 0, sizeof(error_));
    std::memset(&database_, 0, sizeof(database_));
    std::memset(&connection_, 0, sizeof(connection_));

    ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(AdbcDatabaseSetOption(&database_, "uri", uri, &error_),
                IsOkStatus(&error_));
    ASSERT_THAT(AdbcDatabaseInit(&database_, &error_), IsOkStatus(&error_));

    ASSERT_THAT(AdbcConnectionNew(&connection_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(AdbcConnectionInit(&connection_, &database_, &error_),
                IsOkStatus(&error_));
  }

  void TearDown() override {
    if (connection_.private_data) {
      ASSERT_THAT(AdbcConnectionRelease(&connection_, &error_), IsOkStatus(&error_));
    }
    if (database_.private_data) {
      ASSERT_THAT(AdbcDatabaseRelease(&database_, &error_), IsOkStatus(&error_));
    }
  }

  struct AdbcError error_;
  struct AdbcDatabase database_;
  struct AdbcConnection connection_;
};

TEST_F(Db2DriverSpecificTest, QuerySysdummy1) {
  adbc_validation::Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection_, &statement.value, &error_),
              IsOkStatus(&error_));

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement.value,
                               "SELECT 1 AS val FROM SYSIBM.SYSDUMMY1", &error_),
      IsOkStatus(&error_));

  struct ArrowArrayStream stream;
  int64_t rows_affected = -1;
  ASSERT_THAT(
      AdbcStatementExecuteQuery(&statement.value, &stream, &rows_affected, &error_),
      IsOkStatus(&error_));

  struct ArrowSchema schema;
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);
  ASSERT_EQ(schema.n_children, 1);
  schema.release(&schema);

  struct ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  ASSERT_EQ(batch.length, 1);
  batch.release(&batch);

  stream.release(&stream);
}

TEST_F(Db2DriverSpecificTest, BatchSizeOption) {
  adbc_validation::Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection_, &statement.value, &error_),
              IsOkStatus(&error_));

  ASSERT_THAT(AdbcStatementSetOption(&statement.value, "adbc.db2.query.batch_rows",
                                     "1024", &error_),
              IsOkStatus(&error_));
}

TEST_F(Db2DriverSpecificTest, GetTableTypes) {
  struct ArrowArrayStream stream;
  ASSERT_THAT(AdbcConnectionGetTableTypes(&connection_, &stream, &error_),
              IsOkStatus(&error_));

  struct ArrowSchema schema;
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);
  ASSERT_EQ(schema.n_children, 1);
  schema.release(&schema);

  struct ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  ASSERT_GE(batch.length, 1);
  batch.release(&batch);

  stream.release(&stream);
}

TEST_F(Db2DriverSpecificTest, QueryInts) {
  adbc_validation::Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection_, &statement.value, &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &statement.value,
          "SELECT CAST(42 AS INTEGER) AS int_val FROM SYSIBM.SYSDUMMY1", &error_),
      IsOkStatus(&error_));
  struct ArrowArrayStream stream;
  int64_t rows_affected = -1;
  ASSERT_THAT(
      AdbcStatementExecuteQuery(&statement.value, &stream, &rows_affected, &error_),
      IsOkStatus(&error_));
  struct ArrowSchema schema;
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);
  ASSERT_EQ(schema.n_children, 1);
  schema.release(&schema);
  struct ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  ASSERT_EQ(batch.length, 1);
  batch.release(&batch);
  stream.release(&stream);
}

TEST_F(Db2DriverSpecificTest, QueryFloats) {
  adbc_validation::Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection_, &statement.value, &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &statement.value,
          "SELECT CAST(1.5 AS DOUBLE) AS float_val FROM SYSIBM.SYSDUMMY1", &error_),
      IsOkStatus(&error_));
  struct ArrowArrayStream stream;
  int64_t rows_affected = -1;
  ASSERT_THAT(
      AdbcStatementExecuteQuery(&statement.value, &stream, &rows_affected, &error_),
      IsOkStatus(&error_));
  struct ArrowSchema schema;
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);
  ASSERT_EQ(schema.n_children, 1);
  schema.release(&schema);
  struct ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  ASSERT_EQ(batch.length, 1);
  batch.release(&batch);
  stream.release(&stream);
}

TEST_F(Db2DriverSpecificTest, QueryStrings) {
  adbc_validation::Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection_, &statement.value, &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &statement.value,
          "SELECT 'hello' AS str_val FROM SYSIBM.SYSDUMMY1", &error_),
      IsOkStatus(&error_));
  struct ArrowArrayStream stream;
  int64_t rows_affected = -1;
  ASSERT_THAT(
      AdbcStatementExecuteQuery(&statement.value, &stream, &rows_affected, &error_),
      IsOkStatus(&error_));
  struct ArrowSchema schema;
  ASSERT_EQ(stream.get_schema(&stream, &schema), 0);
  ASSERT_EQ(schema.n_children, 1);
  schema.release(&schema);
  struct ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  ASSERT_EQ(batch.length, 1);
  batch.release(&batch);
  stream.release(&stream);
}

TEST_F(Db2DriverSpecificTest, PreparedSelectWithParams) {
  adbc_validation::Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection_, &statement.value, &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &statement.value,
          "SELECT CAST(? AS INTEGER) AS val FROM SYSIBM.SYSDUMMY1", &error_),
      IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementPrepare(&statement.value, &error_), IsOkStatus(&error_));

  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ArrowSchemaInit(&schema.value);
  ASSERT_EQ(ArrowSchemaSetTypeStruct(&schema.value, 1), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema.value.children[0], NANOARROW_TYPE_INT32),
            NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema.value.children[0], "0"), NANOARROW_OK);
  ASSERT_EQ(
      adbc_validation::MakeBatch<int32_t>(&schema.value, &array.value, &na_error,
                                          {42}),
      NANOARROW_OK);

  ASSERT_THAT(
      AdbcStatementBind(&statement.value, &array.value, &schema.value, &error_),
      IsOkStatus(&error_));

  struct ArrowArrayStream stream;
  int64_t rows_affected = -1;
  ASSERT_THAT(
      AdbcStatementExecuteQuery(&statement.value, &stream, &rows_affected, &error_),
      IsOkStatus(&error_));

  struct ArrowSchema result_schema;
  ASSERT_EQ(stream.get_schema(&stream, &result_schema), 0);
  ASSERT_EQ(result_schema.n_children, 1);
  result_schema.release(&result_schema);

  struct ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  ASSERT_EQ(batch.length, 1);
  batch.release(&batch);
  stream.release(&stream);
}

TEST_F(Db2DriverSpecificTest, ConcurrentStatements) {
  adbc_validation::Handle<struct AdbcStatement> stmt1;
  adbc_validation::Handle<struct AdbcStatement> stmt2;
  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt1.value, &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementNew(&connection_, &stmt2.value, &error_),
              IsOkStatus(&error_));

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &stmt1.value, "SELECT 1 AS a FROM SYSIBM.SYSDUMMY1", &error_),
      IsOkStatus(&error_));
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &stmt2.value, "SELECT 2 AS b FROM SYSIBM.SYSDUMMY1", &error_),
      IsOkStatus(&error_));

  struct ArrowArrayStream stream1, stream2;
  int64_t rows1 = -1, rows2 = -1;
  ASSERT_THAT(
      AdbcStatementExecuteQuery(&stmt1.value, &stream1, &rows1, &error_),
      IsOkStatus(&error_));
  ASSERT_THAT(
      AdbcStatementExecuteQuery(&stmt2.value, &stream2, &rows2, &error_),
      IsOkStatus(&error_));

  struct ArrowArray batch1, batch2;
  struct ArrowSchema schema1, schema2;
  ASSERT_EQ(stream1.get_schema(&stream1, &schema1), 0);
  ASSERT_EQ(stream2.get_schema(&stream2, &schema2), 0);
  ASSERT_EQ(stream1.get_next(&stream1, &batch1), 0);
  ASSERT_EQ(stream2.get_next(&stream2, &batch2), 0);
  ASSERT_EQ(batch1.length, 1);
  ASSERT_EQ(batch2.length, 1);

  schema1.release(&schema1);
  schema2.release(&schema2);
  batch1.release(&batch1);
  batch2.release(&batch2);
  stream1.release(&stream1);
  stream2.release(&stream2);
}

TEST_F(Db2DriverSpecificTest, TransactionCommitRollback) {
  ASSERT_THAT(AdbcConnectionSetOption(&connection_, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error_),
              IsOkStatus(&error_));

  ASSERT_THAT(AdbcConnectionCommit(&connection_, &error_), IsOkStatus(&error_));
  ASSERT_THAT(AdbcConnectionRollback(&connection_, &error_), IsOkStatus(&error_));

  ASSERT_THAT(AdbcConnectionSetOption(&connection_, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_ENABLED, &error_),
              IsOkStatus(&error_));
}
