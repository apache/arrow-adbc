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

#include <cstdlib>
#include <cstring>
#include <limits>
#include <optional>
#include <variant>

#include <adbc.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>
#include "common/utils.h"

#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

using adbc_validation::IsOkStatus;
using adbc_validation::IsStatus;

class PostgresQuirks : public adbc_validation::DriverQuirks {
 public:
  AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                               struct AdbcError* error) const override {
    const char* uri = std::getenv("ADBC_POSTGRESQL_TEST_URI");
    if (!uri) {
      ADD_FAILURE() << "Must provide env var ADBC_POSTGRESQL_TEST_URI";
      return ADBC_STATUS_INVALID_ARGUMENT;
    }
    return AdbcDatabaseSetOption(database, "uri", uri, error);
  }

  AdbcStatusCode DropTable(struct AdbcConnection* connection, const std::string& name,
                           struct AdbcError* error) const override {
    struct AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    AdbcStatusCode status = AdbcStatementNew(connection, &statement, error);
    if (status != ADBC_STATUS_OK) return status;

    std::string query = "DROP TABLE IF EXISTS " + name;
    status = AdbcStatementSetSqlQuery(&statement, query.c_str(), error);
    if (status != ADBC_STATUS_OK) {
      std::ignore = AdbcStatementRelease(&statement, error);
      return status;
    }
    status = AdbcStatementExecuteQuery(&statement, nullptr, nullptr, error);
    std::ignore = AdbcStatementRelease(&statement, error);
    return status;
  }

  AdbcStatusCode DropView(struct AdbcConnection* connection, const std::string& name,
                          struct AdbcError* error) const override {
    struct AdbcStatement statement;
    std::memset(&statement, 0, sizeof(statement));
    AdbcStatusCode status = AdbcStatementNew(connection, &statement, error);
    if (status != ADBC_STATUS_OK) return status;

    std::string query = "DROP VIEW IF EXISTS " + name;
    status = AdbcStatementSetSqlQuery(&statement, query.c_str(), error);
    if (status != ADBC_STATUS_OK) {
      std::ignore = AdbcStatementRelease(&statement, error);
      return status;
    }
    status = AdbcStatementExecuteQuery(&statement, nullptr, nullptr, error);
    std::ignore = AdbcStatementRelease(&statement, error);
    return status;
  }

  std::string BindParameter(int index) const override {
    return "$" + std::to_string(index + 1);
  }

  std::optional<std::string> PrimaryKeyTableDdl(std::string_view name) const override {
    std::string ddl = "CREATE TABLE ";
    ddl += name;
    ddl += " (id SERIAL PRIMARY KEY)";
    return ddl;
  }

  std::string catalog() const override { return "postgres"; }
  std::string db_schema() const override { return "public"; }
};

class PostgresDatabaseTest : public ::testing::Test,
                             public adbc_validation::DatabaseTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  PostgresQuirks quirks_;
};
ADBCV_TEST_DATABASE(PostgresDatabaseTest)

class PostgresConnectionTest : public ::testing::Test,
                               public adbc_validation::ConnectionTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

 protected:
  PostgresQuirks quirks_;
};

TEST_F(PostgresConnectionTest, GetInfoMetadata) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  adbc_validation::StreamReader reader;
  std::vector<uint32_t> info = {
      ADBC_INFO_DRIVER_NAME,
      ADBC_INFO_DRIVER_VERSION,
      ADBC_INFO_VENDOR_NAME,
      ADBC_INFO_VENDOR_VERSION,
  };
  ASSERT_THAT(AdbcConnectionGetInfo(&connection, info.data(), info.size(),
                                    &reader.stream.value, &error),
              IsOkStatus(&error));
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
          EXPECT_EQ("ADBC PostgreSQL Driver", std::string(val.data, val.size_bytes));
          break;
        }
        case ADBC_INFO_DRIVER_VERSION: {
          ArrowStringView val = ArrowArrayViewGetStringUnsafe(str_child, 1);
          EXPECT_EQ("(unknown)", std::string(val.data, val.size_bytes));
          break;
        }
        case ADBC_INFO_VENDOR_NAME: {
          ArrowStringView val = ArrowArrayViewGetStringUnsafe(str_child, 2);
          EXPECT_EQ("PostgreSQL", std::string(val.data, val.size_bytes));
          break;
        }
        case ADBC_INFO_VENDOR_VERSION: {
          ArrowStringView val = ArrowArrayViewGetStringUnsafe(str_child, 3);
#ifdef __WIN32
          const char* pater = "\\d\\d\\d\\d\\d\\d";
#else
          const char* pater = "[0-9]{6}";
#endif
          EXPECT_THAT(std::string(val.data, val.size_bytes),
                      ::testing::MatchesRegex(pater));
          break;
        }
        default:
          // Ignored
          break;
      }
    }
  }
  ASSERT_THAT(seen, ::testing::UnorderedElementsAreArray(info));
}

TEST_F(PostgresConnectionTest, GetObjectsGetCatalogs) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  if (!quirks()->supports_get_objects()) {
    GTEST_SKIP();
  }

  adbc_validation::StreamReader reader;
  ASSERT_THAT(
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_CATALOGS, nullptr, nullptr,
                               nullptr, nullptr, nullptr, &reader.stream.value, &error),
      IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_GT(reader.array->length, 0);

  auto get_objects_data = adbc_validation::GetObjectsReader{&reader.array_view.value};
  ASSERT_NE(*get_objects_data, nullptr)
      << "could not initialize the AdbcGetObjectsData object";

  auto catalogs = {"postgres", "template0", "template1"};
  for (auto catalog : catalogs) {
    struct AdbcGetObjectsCatalog* cat =
        AdbcGetObjectsDataGetCatalogByName(*get_objects_data, catalog);
    ASSERT_NE(cat, nullptr) << "catalog " << catalog << " not found";
  }
}

TEST_F(PostgresConnectionTest, GetObjectsGetDbSchemas) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  if (!quirks()->supports_get_objects()) {
    GTEST_SKIP();
  }

  adbc_validation::StreamReader reader;
  ASSERT_THAT(AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_DB_SCHEMAS, nullptr,
                                       nullptr, nullptr, nullptr, nullptr,
                                       &reader.stream.value, &error),
              IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_GT(reader.array->length, 0);

  auto get_objects_data = adbc_validation::GetObjectsReader{&reader.array_view.value};
  ASSERT_NE(*get_objects_data, nullptr)
      << "could not initialize the AdbcGetObjectsData object";

  struct AdbcGetObjectsSchema* schema =
      AdbcGetObjectsDataGetSchemaByName(*get_objects_data, "postgres", "public");
  ASSERT_NE(schema, nullptr) << "schema public not found";
}

TEST_F(PostgresConnectionTest, GetObjectsGetAllFindsPrimaryKey) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  if (!quirks()->supports_get_objects()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "adbc_pkey_test", &error),
              IsOkStatus(&error));

  struct AdbcStatement statement;
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  {
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement, "CREATE TABLE adbc_pkey_test (ints INT, id SERIAL PRIMARY KEY)",
            &error),
        IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(reader.rows_affected, 0);
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }
  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));

  adbc_validation::StreamReader reader;
  ASSERT_THAT(
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_ALL, nullptr, nullptr,
                               nullptr, nullptr, nullptr, &reader.stream.value, &error),
      IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_GT(reader.array->length, 0);

  auto get_objects_data = adbc_validation::GetObjectsReader{&reader.array_view.value};
  ASSERT_NE(*get_objects_data, nullptr)
      << "could not initialize the AdbcGetObjectsData object";

  struct AdbcGetObjectsTable* table = AdbcGetObjectsDataGetTableByName(
      *get_objects_data, "postgres", "public", "adbc_pkey_test");
  ASSERT_NE(table, nullptr) << "could not find adbc_pkey_test table";

  ASSERT_EQ(table->n_table_columns, 2);
  struct AdbcGetObjectsColumn* column = AdbcGetObjectsDataGetColumnByName(
      *get_objects_data, "postgres", "public", "adbc_pkey_test", "id");
  ASSERT_NE(column, nullptr) << "could not find id column on adbc_pkey_test table";

  ASSERT_EQ(table->n_table_constraints, 1)
      << "expected 1 constraint on adbc_pkey_test table, found: "
      << table->n_table_constraints;

  struct AdbcGetObjectsConstraint* constraint = AdbcGetObjectsDataGetConstraintByName(
      *get_objects_data, "postgres", "public", "adbc_pkey_test", "adbc_pkey_test_pkey");
  ASSERT_NE(constraint, nullptr) << "could not find adbc_pkey_test_pkey constraint";

  auto constraint_type = std::string(constraint->constraint_type.data,
                                     constraint->constraint_type.size_bytes);
  ASSERT_EQ(constraint_type, "PRIMARY KEY");
  ASSERT_EQ(constraint->n_column_names, 1)
      << "expected constraint adbc_pkey_test_pkey to be applied to 1 column, found: "
      << constraint->n_column_names;

  auto constraint_column_name =
      std::string(constraint->constraint_column_names[0].data,
                  constraint->constraint_column_names[0].size_bytes);
  ASSERT_EQ(constraint_column_name, "id");
}

TEST_F(PostgresConnectionTest, GetObjectsGetAllFindsForeignKey) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  if (!quirks()->supports_get_objects()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "adbc_fkey_test", &error),
              IsOkStatus(&error));
  ASSERT_THAT(quirks()->DropTable(&connection, "adbc_fkey_test_base", &error),
              IsOkStatus(&error));

  struct AdbcStatement statement;
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  {
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(&statement,
                                 "CREATE TABLE adbc_fkey_test_base (id1 INT, id2 INT, "
                                 "PRIMARY KEY (id1, id2))",
                                 &error),
        IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(reader.rows_affected, 0);
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }
  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  {
    ASSERT_THAT(AdbcStatementSetSqlQuery(
                    &statement,
                    "CREATE TABLE adbc_fkey_test (fid1 INT, fid2 INT, "
                    "FOREIGN KEY (fid1, fid2) REFERENCES adbc_fkey_test_base(id1, id2))",
                    &error),
                IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(reader.rows_affected, 0);
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }
  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));

  adbc_validation::StreamReader reader;
  ASSERT_THAT(
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_ALL, nullptr, nullptr,
                               nullptr, nullptr, nullptr, &reader.stream.value, &error),
      IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_GT(reader.array->length, 0);

  auto get_objects_data = adbc_validation::GetObjectsReader{&reader.array_view.value};
  ASSERT_NE(*get_objects_data, nullptr)
      << "could not initialize the AdbcGetInfoData object";

  struct AdbcGetObjectsTable* table = AdbcGetObjectsDataGetTableByName(
      *get_objects_data, "postgres", "public", "adbc_fkey_test");
  ASSERT_NE(table, nullptr) << "could not find adbc_fkey_test table";
  ASSERT_EQ(table->n_table_constraints, 1)
      << "expected 1 constraint on adbc_fkey_test table, found: "
      << table->n_table_constraints;

  struct AdbcGetObjectsConstraint* constraint = AdbcGetObjectsDataGetConstraintByName(
      *get_objects_data, "postgres", "public", "adbc_fkey_test",
      "adbc_fkey_test_fid1_fid2_fkey");
  ASSERT_NE(constraint, nullptr)
      << "could not find adbc_fkey_test_fid1_fid2_fkey constraint";

  auto constraint_type = std::string(constraint->constraint_type.data,
                                     constraint->constraint_type.size_bytes);
  ASSERT_EQ(constraint_type, "FOREIGN KEY");
  ASSERT_EQ(constraint->n_column_names, 2)
      << "expected constraint adbc_fkey_test_fid1_fid2_fkey to be applied to 2 columns, "
         "found: "
      << constraint->n_column_names;

  for (auto i = 0; i < 2; i++) {
    auto str_vw = constraint->constraint_column_names[i];
    auto str = std::string(str_vw.data, str_vw.size_bytes);
    if (i == 0) {
      ASSERT_EQ(str, "fid1");
    } else if (i == 1) {
      ASSERT_EQ(str, "fid2");
    }
  }

  ASSERT_EQ(constraint->n_column_usages, 2)
      << "expected constraint adbc_fkey_test_fid1_fid2_fkey to have 2 usages, found: "
      << constraint->n_column_usages;

  for (auto i = 0; i < 2; i++) {
    struct AdbcGetObjectsUsage* usage = constraint->constraint_column_usages[i];
    auto catalog_str = std::string(usage->fk_catalog.data, usage->fk_catalog.size_bytes);
    ASSERT_EQ(catalog_str, "postgres");
    auto schema_str =
        std::string(usage->fk_db_schema.data, usage->fk_db_schema.size_bytes);
    ASSERT_EQ(schema_str, "public");
    auto table_str = std::string(usage->fk_table.data, usage->fk_table.size_bytes);
    ASSERT_EQ(table_str, "adbc_fkey_test_base");

    auto column_str =
        std::string(usage->fk_column_name.data, usage->fk_column_name.size_bytes);
    if (i == 0) {
      ASSERT_EQ(column_str, "id1");
    } else if (i == 1) {
      ASSERT_EQ(column_str, "id2");
    }
  }
}

TEST_F(PostgresConnectionTest, GetObjectsTableTypesFilter) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  if (!quirks()->supports_get_objects()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropView(&connection, "adbc_table_types_view_test", &error),
              IsOkStatus(&error));
  ASSERT_THAT(quirks()->DropTable(&connection, "adbc_table_types_table_test", &error),
              IsOkStatus(&error));

  {
    adbc_validation::Handle<struct AdbcStatement> statement;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));

    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement.value,
            "CREATE TABLE adbc_table_types_table_test (id1 INT, id2 INT)", &error),
        IsOkStatus(&error));

    int64_t rows_affected = 0;
    ASSERT_THAT(
        AdbcStatementExecuteQuery(&statement.value, nullptr, &rows_affected, &error),
        IsOkStatus(&error));
  }

  {
    adbc_validation::Handle<struct AdbcStatement> statement;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement.value,
                                         "CREATE VIEW adbc_table_types_view_test AS ( "
                                         "SELECT * FROM adbc_table_types_table_test)",
                                         &error),
                IsOkStatus(&error));
    int64_t rows_affected = 0;
    ASSERT_THAT(
        AdbcStatementExecuteQuery(&statement.value, nullptr, &rows_affected, &error),
        IsOkStatus(&error));
  }

  adbc_validation::StreamReader reader;
  std::vector<const char*> table_types = {"view", nullptr};
  ASSERT_THAT(AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_ALL, nullptr,
                                       nullptr, nullptr, table_types.data(), nullptr,
                                       &reader.stream.value, &error),
              IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_GT(reader.array->length, 0);

  auto get_objects_data = adbc_validation::GetObjectsReader{&reader.array_view.value};
  ASSERT_NE(*get_objects_data, nullptr)
      << "could not initialize the AdbcGetInfoData object";

  struct AdbcGetObjectsTable* table = AdbcGetObjectsDataGetTableByName(
      *get_objects_data, "postgres", "public", "adbc_table_types_table_test");
  ASSERT_EQ(table, nullptr) << "unexpected table adbc_table_types_table_test found";

  struct AdbcGetObjectsTable* view = AdbcGetObjectsDataGetTableByName(
      *get_objects_data, "postgres", "public", "adbc_table_types_view_test");
  ASSERT_NE(view, nullptr) << "did not find view adbc_table_types_view_test";
}

TEST_F(PostgresConnectionTest, MetadataGetTableSchemaInjection) {
  if (!quirks()->supports_bulk_ingest()) {
    GTEST_SKIP();
  }
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_THAT(quirks()->EnsureSampleTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  adbc_validation::Handle<ArrowSchema> schema;
  ASSERT_THAT(AdbcConnectionGetTableSchema(&connection, /*catalog=*/nullptr,
                                           /*db_schema=*/nullptr,
                                           "0'::int; DROP TABLE bulk_ingest;--",
                                           &schema.value, &error),
              IsStatus(ADBC_STATUS_IO, &error));

  ASSERT_THAT(
      AdbcConnectionGetTableSchema(&connection, /*catalog=*/nullptr,
                                   /*db_schema=*/"0'::int; DROP TABLE bulk_ingest;--",
                                   "DROP TABLE bulk_ingest;", &schema.value, &error),
      IsStatus(ADBC_STATUS_IO, &error));

  ASSERT_THAT(AdbcConnectionGetTableSchema(&connection, /*catalog=*/nullptr,
                                           /*db_schema=*/nullptr, "bulk_ingest",
                                           &schema.value, &error),
              IsOkStatus(&error));

  ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareSchema(
      &schema.value, {{"int64s", NANOARROW_TYPE_INT64, true},
                      {"strings", NANOARROW_TYPE_STRING, true}}));
}

ADBCV_TEST_CONNECTION(PostgresConnectionTest)

class PostgresStatementTest : public ::testing::Test,
                              public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  void TestSqlIngestInt8() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestUInt8() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestUInt16() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestUInt32() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestUInt64() { GTEST_SKIP() << "Not implemented"; }

  void TestSqlPrepareErrorParamCountMismatch() { GTEST_SKIP() << "Not yet implemented"; }
  void TestSqlPrepareGetParameterSchema() { GTEST_SKIP() << "Not yet implemented"; }
  void TestSqlPrepareSelectParams() { GTEST_SKIP() << "Not yet implemented"; }

  void TestConcurrentStatements() {
    // TODO: refactor driver so that we read all the data as soon as
    // we ExecuteQuery() since that's how libpq already works - then
    // we can actually support concurrent statements (because there is
    // no concurrency)
    GTEST_SKIP() << "Not yet implemented";
  }

 protected:
  PostgresQuirks quirks_;
};
ADBCV_TEST_STATEMENT(PostgresStatementTest)

TEST_F(PostgresStatementTest, UpdateInExecuteQuery) {
  ASSERT_THAT(quirks()->DropTable(&connection, "adbc_test", &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  {
    ASSERT_THAT(AdbcStatementSetSqlQuery(
                    &statement,
                    "CREATE TABLE adbc_test (ints INT, id SERIAL PRIMARY KEY)", &error),
                IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(reader.rows_affected, 0);
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }

  {
    // Use INSERT INTO
    ASSERT_THAT(AdbcStatementSetSqlQuery(
                    &statement, "INSERT INTO adbc_test (ints) VALUES (1), (2)", &error),
                IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(reader.rows_affected, 0);
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }

  {
    // Use INSERT INTO ... RETURNING
    ASSERT_THAT(AdbcStatementSetSqlQuery(
                    &statement,
                    "INSERT INTO adbc_test (ints) VALUES (3), (4) RETURNING id", &error),
                IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(reader.rows_affected, -1);
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(reader.array->release, nullptr);
    ASSERT_EQ(reader.array->n_children, 1);
    ASSERT_EQ(reader.array->length, 2);
    ASSERT_EQ(reader.array_view->children[0]->buffer_views[1].data.as_int32[0], 3);
    ASSERT_EQ(reader.array_view->children[0]->buffer_views[1].data.as_int32[1], 4);
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }
}

struct TypeTestCase {
  std::string name;
  std::string sql_type;
  std::string sql_literal;
  ArrowType arrow_type;
  std::variant<bool, int64_t, double, std::string> scalar;

  static std::string FormatName(const ::testing::TestParamInfo<TypeTestCase>& info) {
    return info.param.name;
  }
};

void PrintTo(const TypeTestCase& value, std::ostream* os) { (*os) << value.name; }

class PostgresTypeTest : public ::testing::TestWithParam<TypeTestCase> {
 public:
  void SetUp() override {
    ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(quirks_.SetupDatabase(&database_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(AdbcDatabaseInit(&database_, &error_), IsOkStatus(&error_));

    ASSERT_THAT(AdbcConnectionNew(&connection_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(AdbcConnectionInit(&connection_, &database_, &error_),
                IsOkStatus(&error_));

    ASSERT_THAT(AdbcStatementNew(&connection_, &statement_, &error_),
                IsOkStatus(&error_));

    ASSERT_THAT(quirks_.DropTable(&connection_, "foo", &error_), IsOkStatus(&error_));
  }
  void TearDown() override {
    if (statement_.private_data) {
      ASSERT_THAT(AdbcStatementRelease(&statement_, &error_), IsOkStatus(&error_));
    }
    if (connection_.private_data) {
      ASSERT_THAT(AdbcConnectionRelease(&connection_, &error_), IsOkStatus(&error_));
    }
    if (database_.private_data) {
      ASSERT_THAT(AdbcDatabaseRelease(&database_, &error_), IsOkStatus(&error_));
    }

    if (error_.release) error_.release(&error_);
  }

 protected:
  PostgresQuirks quirks_;
  struct AdbcError error_ = {};
  struct AdbcDatabase database_ = {};
  struct AdbcConnection connection_ = {};
  struct AdbcStatement statement_ = {};
};

TEST_P(PostgresTypeTest, SelectValue) {
  // create table
  std::string query = "CREATE TABLE foo (col ";
  query += GetParam().sql_type;
  query += ")";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement_, query.c_str(), &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement_, /*out=*/nullptr,
                                        /*rows_affected=*/nullptr, &error_),
              IsOkStatus(&error_));

  // insert value
  query = "INSERT INTO foo(col) VALUES ( ";
  query += GetParam().sql_literal;
  query += ")";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement_, query.c_str(), &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement_, /*out=*/nullptr,
                                        /*rows_affected=*/nullptr, &error_),
              IsOkStatus(&error_));

  // select
  adbc_validation::StreamReader reader;
  query = "SELECT * FROM foo";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement_, query.c_str(), &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement_, &reader.stream.value,
                                        /*rows_affected=*/nullptr, &error_),
              IsOkStatus(&error_));

  // check type
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareSchema(
      &reader.schema.value, {{std::nullopt, GetParam().arrow_type, true}}));
  if (GetParam().arrow_type == NANOARROW_TYPE_TIMESTAMP) {
    if (GetParam().sql_type.find("WITH TIME ZONE") == std::string::npos) {
      ASSERT_STREQ(reader.schema->children[0]->format, "tsu:");
    } else {
      ASSERT_STREQ(reader.schema->children[0]->format, "tsu:UTC");
    }
  }

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_FALSE(ArrowArrayViewIsNull(&reader.array_view.value, 0));
  ASSERT_FALSE(ArrowArrayViewIsNull(reader.array_view->children[0], 0));

  // check value
  ASSERT_NO_FATAL_FAILURE(std::visit(
      [&](auto&& arg) -> void {
        using T = std::decay_t<decltype(arg)>;
        if constexpr (std::is_same_v<T, bool>) {
          ASSERT_EQ(static_cast<int64_t>(arg),
                    ArrowArrayViewGetIntUnsafe(reader.array_view->children[0], 0));
        } else if constexpr (std::is_same_v<T, int64_t>) {
          ASSERT_EQ(arg, ArrowArrayViewGetIntUnsafe(reader.array_view->children[0], 0));
        } else if constexpr (std::is_same_v<T, double>) {
          ASSERT_EQ(arg,
                    ArrowArrayViewGetDoubleUnsafe(reader.array_view->children[0], 0));
        } else if constexpr (std::is_same_v<T, std::string>) {
          ArrowStringView view =
              ArrowArrayViewGetStringUnsafe(reader.array_view->children[0], 0);
          ASSERT_EQ(arg.size(), view.size_bytes);
          ASSERT_EQ(0, std::strncmp(arg.c_str(), view.data, arg.size()));
        } else {
          FAIL() << "Unimplemented case";
        }
      },
      GetParam().scalar));

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

static std::initializer_list<TypeTestCase> kBoolTypeCases = {
    {"BOOL_TRUE", "BOOLEAN", "TRUE", NANOARROW_TYPE_BOOL, true},
    {"BOOL_FALSE", "BOOLEAN", "FALSE", NANOARROW_TYPE_BOOL, false},
};
static std::initializer_list<TypeTestCase> kBinaryTypeCases = {
    {"BYTEA", "BYTEA", R"('\000\001\002\003\004\005\006\007'::bytea)",
     NANOARROW_TYPE_BINARY, std::string("\x00\x01\x02\x03\x04\x05\x06\x07", 8)},
    {"TEXT", "TEXT", "'foobar'", NANOARROW_TYPE_STRING, "foobar"},
    {"CHAR6_1", "CHAR(6)", "'foo'", NANOARROW_TYPE_STRING, "foo   "},
    {"CHAR6_2", "CHAR(6)", "'foobar'", NANOARROW_TYPE_STRING, "foobar"},
    {"VARCHAR", "VARCHAR", "'foobar'", NANOARROW_TYPE_STRING, "foobar"},
};
static std::initializer_list<TypeTestCase> kFloatTypeCases = {
    {"REAL", "REAL", "-1E0", NANOARROW_TYPE_FLOAT, -1.0},
    {"DOUBLE_PRECISION", "DOUBLE PRECISION", "-1E0", NANOARROW_TYPE_DOUBLE, -1.0},
};
static std::initializer_list<TypeTestCase> kIntTypeCases = {
    {"SMALLINT", "SMALLINT", std::to_string(std::numeric_limits<int16_t>::min()),
     NANOARROW_TYPE_INT16, static_cast<int64_t>(std::numeric_limits<int16_t>::min())},
    {"INT", "INT", std::to_string(std::numeric_limits<int32_t>::min()),
     NANOARROW_TYPE_INT32, static_cast<int64_t>(std::numeric_limits<int32_t>::min())},
    {"BIGINT", "BIGINT", std::to_string(std::numeric_limits<int64_t>::min()),
     NANOARROW_TYPE_INT64, std::numeric_limits<int64_t>::min()},
    {"SERIAL", "SERIAL", std::to_string(std::numeric_limits<int32_t>::max()),
     NANOARROW_TYPE_INT32, static_cast<int64_t>(std::numeric_limits<int32_t>::max())},
    {"BIGSERIAL", "BIGSERIAL", std::to_string(std::numeric_limits<int64_t>::max()),
     NANOARROW_TYPE_INT64, std::numeric_limits<int64_t>::max()},
};
static std::initializer_list<TypeTestCase> kDateTypeCases = {
    {"DATE0", "DATE", "'1970-01-01'", NANOARROW_TYPE_DATE32, int64_t(0)},
    {"DATE1", "DATE", "'2000-01-01'", NANOARROW_TYPE_DATE32, int64_t(10957)},
    {"DATE2", "DATE", "'1950-01-01'", NANOARROW_TYPE_DATE32, int64_t(-7305)},
};
static std::initializer_list<TypeTestCase> kTimeTypeCases = {
    {"TIME_WITHOUT_TIME_ZONE", "TIME WITHOUT TIME ZONE", "'00:00'", NANOARROW_TYPE_TIME64,
     int64_t(0)},
    {"TIME_WITHOUT_TIME_ZONE_VAL", "TIME WITHOUT TIME ZONE", "'01:02:03.123123'",
     NANOARROW_TYPE_TIME64, int64_t(3'723'123'123)},
    {"TIME_6_WITHOUT_TIME_ZONE", "TIME (6) WITHOUT TIME ZONE", "'00:00'",
     NANOARROW_TYPE_TIME64, int64_t(0)},
    {"TIME_6_WITHOUT_TIME_ZONE_VAL", "TIME (6) WITHOUT TIME ZONE", "'01:02:03.123123'",
     NANOARROW_TYPE_TIME64, int64_t(3'723'123'123)},
    {"TIME_5_WITHOUT_TIME_ZONE", "TIME (5) WITHOUT TIME ZONE", "'00:00'",
     NANOARROW_TYPE_TIME64, int64_t(0)},
    {"TIME_5_WITHOUT_TIME_ZONE_VAL", "TIME (5) WITHOUT TIME ZONE", "'01:02:03.123123'",
     NANOARROW_TYPE_TIME64, int64_t(3'723'123'120)},
    {"TIME_4_WITHOUT_TIME_ZONE", "TIME (4) WITHOUT TIME ZONE", "'00:00'",
     NANOARROW_TYPE_TIME64, int64_t(0)},
    {"TIME_4_WITHOUT_TIME_ZONE_VAL", "TIME (4) WITHOUT TIME ZONE", "'01:02:03.123123'",
     NANOARROW_TYPE_TIME64, int64_t(3'723'123'100)},
    {"TIME_3_WITHOUT_TIME_ZONE", "TIME (3) WITHOUT TIME ZONE", "'00:00'",
     NANOARROW_TYPE_TIME64, int64_t(0)},
    {"TIME_3_WITHOUT_TIME_ZONE_VAL", "TIME (3) WITHOUT TIME ZONE", "'01:02:03.123123'",
     NANOARROW_TYPE_TIME64, int64_t(3'723'123'000)},
    {"TIME_2_WITHOUT_TIME_ZONE", "TIME (2) WITHOUT TIME ZONE", "'00:00'",
     NANOARROW_TYPE_TIME64, int64_t(0)},
    {"TIME_2_WITHOUT_TIME_ZONE_VAL", "TIME (2) WITHOUT TIME ZONE", "'01:02:03.123123'",
     NANOARROW_TYPE_TIME64, int64_t(3'723'120'000)},
    {"TIME_1_WITHOUT_TIME_ZONE", "TIME (1) WITHOUT TIME ZONE", "'00:00'",
     NANOARROW_TYPE_TIME64, int64_t(0)},
    {"TIME_1_WITHOUT_TIME_ZONE_VAL", "TIME (1) WITHOUT TIME ZONE", "'01:02:03.123123'",
     NANOARROW_TYPE_TIME64, int64_t(3'723'100'000)},
    {"TIME_0_WITHOUT_TIME_ZONE", "TIME (0) WITHOUT TIME ZONE", "'00:00'",
     NANOARROW_TYPE_TIME64, int64_t(0)},
    {"TIME_0_WITHOUT_TIME_ZONE_VAL", "TIME (0) WITHOUT TIME ZONE", "'01:02:03.123123'",
     NANOARROW_TYPE_TIME64, int64_t(3'723'000'000)},
};
static std::initializer_list<TypeTestCase> kTimestampTypeCases = {
    {"TIMESTAMP_WITHOUT_TIME_ZONE", "TIMESTAMP WITHOUT TIME ZONE",
     "'1970-01-01 00:00:00.000000'", NANOARROW_TYPE_TIMESTAMP, int64_t(0)},
    {"TIMESTAMP_WITHOUT_TIME_ZONE_VAL", "TIMESTAMP WITHOUT TIME ZONE",
     "'1970-01-02 03:04:05.123123'", NANOARROW_TYPE_TIMESTAMP, int64_t(97'445'123'123)},
    {"TIMESTAMP_6_WITHOUT_TIME_ZONE", "TIMESTAMP (6) WITHOUT TIME ZONE",
     "'1970-01-01 00:00:00.000000'", NANOARROW_TYPE_TIMESTAMP, int64_t(0)},
    {"TIMESTAMP_6_WITHOUT_TIME_ZONE_VAL", "TIMESTAMP (6) WITHOUT TIME ZONE",
     "'1970-01-02 03:04:05.123123'", NANOARROW_TYPE_TIMESTAMP, int64_t(97'445'123'123)},
    {"TIMESTAMP_5_WITHOUT_TIME_ZONE", "TIMESTAMP (5) WITHOUT TIME ZONE",
     "'1970-01-01 00:00:00.000000'", NANOARROW_TYPE_TIMESTAMP, int64_t(0)},
    {"TIMESTAMP_5_WITHOUT_TIME_ZONE_VAL", "TIMESTAMP (5) WITHOUT TIME ZONE",
     "'1970-01-02 03:04:05.123123'", NANOARROW_TYPE_TIMESTAMP, int64_t(97'445'123'120)},
    {"TIMESTAMP_4_WITHOUT_TIME_ZONE", "TIMESTAMP (4) WITHOUT TIME ZONE",
     "'1970-01-01 00:00:00.000000'", NANOARROW_TYPE_TIMESTAMP, int64_t(0)},
    {"TIMESTAMP_4_WITHOUT_TIME_ZONE_VAL", "TIMESTAMP (4) WITHOUT TIME ZONE",
     "'1970-01-02 03:04:05.123123'", NANOARROW_TYPE_TIMESTAMP, int64_t(97'445'123'100)},
    {"TIMESTAMP_3_WITHOUT_TIME_ZONE", "TIMESTAMP (3) WITHOUT TIME ZONE",
     "'1970-01-01 00:00:00.000000'", NANOARROW_TYPE_TIMESTAMP, int64_t(0)},
    {"TIMESTAMP_3_WITHOUT_TIME_ZONE_VAL", "TIMESTAMP (3) WITHOUT TIME ZONE",
     "'1970-01-02 03:04:05.123123'", NANOARROW_TYPE_TIMESTAMP, int64_t(97'445'123'000)},
    {"TIMESTAMP_2_WITHOUT_TIME_ZONE", "TIMESTAMP (2) WITHOUT TIME ZONE",
     "'1970-01-01 00:00:00.000000'", NANOARROW_TYPE_TIMESTAMP, int64_t(0)},
    {"TIMESTAMP_2_WITHOUT_TIME_ZONE_VAL", "TIMESTAMP (2) WITHOUT TIME ZONE",
     "'1970-01-02 03:04:05.123123'", NANOARROW_TYPE_TIMESTAMP, int64_t(97'445'120'000)},
    {"TIMESTAMP_1_WITHOUT_TIME_ZONE", "TIMESTAMP (1) WITHOUT TIME ZONE",
     "'1970-01-01 00:00:00.000000'", NANOARROW_TYPE_TIMESTAMP, int64_t(0)},
    {"TIMESTAMP_1_WITHOUT_TIME_ZONE_VAL", "TIMESTAMP (1) WITHOUT TIME ZONE",
     "'1970-01-02 03:04:05.123123'", NANOARROW_TYPE_TIMESTAMP, int64_t(97'445'100'000)},
    {"TIMESTAMP_0_WITHOUT_TIME_ZONE", "TIMESTAMP (0) WITHOUT TIME ZONE",
     "'1970-01-01 00:00:00.000000'", NANOARROW_TYPE_TIMESTAMP, int64_t(0)},
    {"TIMESTAMP_0_WITHOUT_TIME_ZONE_VAL", "TIMESTAMP (0) WITHOUT TIME ZONE",
     "'1970-01-02 03:04:05.123123'", NANOARROW_TYPE_TIMESTAMP, int64_t(97'445'000'000)},
    {"TIMESTAMP_WITH_TIME_ZONE", "TIMESTAMP WITH TIME ZONE",
     "'1970-01-01 00:00:00.000000+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(-1'800'000'000)},
    {"TIMESTAMP_WITH_TIME_ZONE_VAL", "TIMESTAMP WITH TIME ZONE",
     "'1970-01-02 03:04:05.123123+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(95'645'123'123)},
    {"TIMESTAMP_6_WITH_TIME_ZONE", "TIMESTAMP (6) WITH TIME ZONE",
     "'1970-01-01 00:00:00.000000+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(-1'800'000'000)},
    {"TIMESTAMP_6_WITH_TIME_ZONE_VAL", "TIMESTAMP (6) WITH TIME ZONE",
     "'1970-01-02 03:04:05.123123+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(95'645'123'123)},
    {"TIMESTAMP_5_WITH_TIME_ZONE", "TIMESTAMP (5) WITH TIME ZONE",
     "'1970-01-01 00:00:00.000000+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(-1'800'000'000)},
    {"TIMESTAMP_5_WITH_TIME_ZONE_VAL", "TIMESTAMP (5) WITH TIME ZONE",
     "'1970-01-02 03:04:05.123123+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(95'645'123'120)},
    {"TIMESTAMP_4_WITH_TIME_ZONE", "TIMESTAMP (4) WITH TIME ZONE",
     "'1970-01-01 00:00:00.000000+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(-1'800'000'000)},
    {"TIMESTAMP_4_WITH_TIME_ZONE_VAL", "TIMESTAMP (4) WITH TIME ZONE",
     "'1970-01-02 03:04:05.123123+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(95'645'123'100)},
    {"TIMESTAMP_3_WITH_TIME_ZONE", "TIMESTAMP (3) WITH TIME ZONE",
     "'1970-01-01 00:00:00.000000+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(-1'800'000'000)},
    {"TIMESTAMP_3_WITH_TIME_ZONE_VAL", "TIMESTAMP (3) WITH TIME ZONE",
     "'1970-01-02 03:04:05.123123+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(95'645'123'000)},
    {"TIMESTAMP_2_WITH_TIME_ZONE", "TIMESTAMP (2) WITH TIME ZONE",
     "'1970-01-01 00:00:00.000000+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(-1'800'000'000)},
    {"TIMESTAMP_2_WITH_TIME_ZONE_VAL", "TIMESTAMP (2) WITH TIME ZONE",
     "'1970-01-02 03:04:05.123123+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(95'645'120'000)},
    {"TIMESTAMP_1_WITH_TIME_ZONE", "TIMESTAMP (1) WITH TIME ZONE",
     "'1970-01-01 00:00:00.000000+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(-1'800'000'000)},
    {"TIMESTAMP_1_WITH_TIME_ZONE_VAL", "TIMESTAMP (1) WITH TIME ZONE",
     "'1970-01-02 03:04:05.123123+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(95'645'100'000)},
    {"TIMESTAMP_0_WITH_TIME_ZONE", "TIMESTAMP (0) WITH TIME ZONE",
     "'1970-01-01 00:00:00.000000+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(-1'800'000'000)},
    {"TIMESTAMP_0_WITH_TIME_ZONE_VAL", "TIMESTAMP (0) WITH TIME ZONE",
     "'1970-01-02 03:04:05.123123+00:30'", NANOARROW_TYPE_TIMESTAMP,
     int64_t(95'645'000'000)},
};

INSTANTIATE_TEST_SUITE_P(BoolType, PostgresTypeTest, testing::ValuesIn(kBoolTypeCases),
                         TypeTestCase::FormatName);
INSTANTIATE_TEST_SUITE_P(BinaryTypes, PostgresTypeTest,
                         testing::ValuesIn(kBinaryTypeCases), TypeTestCase::FormatName);
INSTANTIATE_TEST_SUITE_P(FloatTypes, PostgresTypeTest, testing::ValuesIn(kFloatTypeCases),
                         TypeTestCase::FormatName);
INSTANTIATE_TEST_SUITE_P(IntTypes, PostgresTypeTest, testing::ValuesIn(kIntTypeCases),
                         TypeTestCase::FormatName);
INSTANTIATE_TEST_SUITE_P(DateTypes, PostgresTypeTest, testing::ValuesIn(kDateTypeCases),
                         TypeTestCase::FormatName);
INSTANTIATE_TEST_SUITE_P(TimeTypes, PostgresTypeTest, testing::ValuesIn(kTimeTypeCases),
                         TypeTestCase::FormatName);
INSTANTIATE_TEST_SUITE_P(TimestampTypes, PostgresTypeTest,
                         testing::ValuesIn(kTimestampTypeCases),
                         TypeTestCase::FormatName);
