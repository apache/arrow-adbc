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

#include <array>
#include <cerrno>
#include <cmath>
#include <cstdlib>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <utility>
#include <variant>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <arrow-adbc/driver/postgresql.h>
#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow.hpp>

#include "common/options.h"
#include "common/utils.h"
#include "validation/adbc_validation.h"
#include "validation/adbc_validation_util.h"

using adbc_validation::Handle;
using adbc_validation::IsOkStatus;
using adbc_validation::IsStatus;
using std::string_literals::operator""s;

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
    Handle<struct AdbcStatement> statement;
    RAISE_ADBC(AdbcStatementNew(connection, &statement.value, error));

    std::string query = "DROP TABLE IF EXISTS \"" + name + "\"";
    RAISE_ADBC(AdbcStatementSetSqlQuery(&statement.value, query.c_str(), error));
    RAISE_ADBC(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
    return AdbcStatementRelease(&statement.value, error);
  }

  AdbcStatusCode DropTable(struct AdbcConnection* connection, const std::string& name,
                           const std::string& db_schema,
                           struct AdbcError* error) const override {
    Handle<struct AdbcStatement> statement;
    RAISE_ADBC(AdbcStatementNew(connection, &statement.value, error));

    std::string query = "DROP TABLE IF EXISTS \"" + db_schema + "\".\"" + name + "\"";
    RAISE_ADBC(AdbcStatementSetSqlQuery(&statement.value, query.c_str(), error));
    RAISE_ADBC(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
    return AdbcStatementRelease(&statement.value, error);
  }

  AdbcStatusCode DropTempTable(struct AdbcConnection* connection, const std::string& name,
                               struct AdbcError* error) const override {
    Handle<struct AdbcStatement> statement;
    RAISE_ADBC(AdbcStatementNew(connection, &statement.value, error));

    std::string query = "DROP TABLE IF EXISTS pg_temp . \"" + name + "\"";
    RAISE_ADBC(AdbcStatementSetSqlQuery(&statement.value, query.c_str(), error));
    RAISE_ADBC(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
    return AdbcStatementRelease(&statement.value, error);
  }

  AdbcStatusCode DropView(struct AdbcConnection* connection, const std::string& name,
                          struct AdbcError* error) const override {
    Handle<struct AdbcStatement> statement;
    RAISE_ADBC(AdbcStatementNew(connection, &statement.value, error));

    std::string query = "DROP VIEW IF EXISTS \"" + name + "\"";
    RAISE_ADBC(AdbcStatementSetSqlQuery(&statement.value, query.c_str(), error));
    RAISE_ADBC(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
    return AdbcStatementRelease(&statement.value, error);
  }

  AdbcStatusCode EnsureDbSchema(struct AdbcConnection* connection,
                                const std::string& name,
                                struct AdbcError* error) const override {
    Handle<struct AdbcStatement> statement;
    RAISE_ADBC(AdbcStatementNew(connection, &statement.value, error));

    std::string query = "CREATE SCHEMA IF NOT EXISTS \"" + name + "\"";
    RAISE_ADBC(AdbcStatementSetSqlQuery(&statement.value, query.c_str(), error));
    RAISE_ADBC(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
    return AdbcStatementRelease(&statement.value, error);
  }

  std::string BindParameter(int index) const override {
    return "$" + std::to_string(index + 1);
  }

  ArrowType IngestSelectRoundTripType(ArrowType ingest_type) const override {
    switch (ingest_type) {
      case NANOARROW_TYPE_INT8:
      case NANOARROW_TYPE_UINT8:
        return NANOARROW_TYPE_INT16;
      case NANOARROW_TYPE_UINT16:
        return NANOARROW_TYPE_INT32;
      case NANOARROW_TYPE_UINT32:
      case NANOARROW_TYPE_UINT64:
        return NANOARROW_TYPE_INT64;
      case NANOARROW_TYPE_HALF_FLOAT:
        return NANOARROW_TYPE_FLOAT;
      case NANOARROW_TYPE_DURATION:
        return NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO;
      case NANOARROW_TYPE_LARGE_STRING:
      case NANOARROW_TYPE_STRING_VIEW:
        return NANOARROW_TYPE_STRING;
      case NANOARROW_TYPE_LARGE_BINARY:
      case NANOARROW_TYPE_FIXED_SIZE_BINARY:
      case NANOARROW_TYPE_BINARY_VIEW:
        return NANOARROW_TYPE_BINARY;
      case NANOARROW_TYPE_DECIMAL128:
      case NANOARROW_TYPE_DECIMAL256:
        return NANOARROW_TYPE_STRING;
      default:
        return ingest_type;
    }
  }

  std::optional<std::string> PrimaryKeyTableDdl(std::string_view name) const override {
    std::string ddl = "CREATE TABLE ";
    ddl += name;
    ddl += " (id SERIAL PRIMARY KEY)";
    return ddl;
  }

  std::optional<std::string> PrimaryKeyIngestTableDdl(
      std::string_view name) const override {
    std::string ddl = "CREATE TABLE ";
    ddl += name;
    ddl += " (id BIGSERIAL PRIMARY KEY, value BIGINT)";
    return ddl;
  }

  std::optional<std::string> CompositePrimaryKeyTableDdl(
      std::string_view name) const override {
    std::string ddl = "CREATE TABLE ";
    ddl += name;
    ddl += " (id_primary_col1 SERIAL, id_primary_col2 SERIAL,";
    ddl += " PRIMARY KEY (id_primary_col1, id_primary_col2));";
    return ddl;
  }

  std::optional<std::string> ForeignKeyChildTableDdl(
      std::string_view child_name, std::string_view parent_name_1,
      std::string_view parent_name_2) const override {
    std::string ddl = "CREATE TABLE ";
    ddl += child_name;
    ddl += " (id_child_col1 SERIAL PRIMARY KEY, ";
    ddl += " id_child_col2 SERIAL, ";
    ddl += " id_child_col3 SERIAL, ";
    ddl += " FOREIGN KEY (id_child_col3) REFERENCES ";
    ddl += parent_name_1;
    ddl += " (id),";
    ddl += " FOREIGN KEY (id_child_col1, id_child_col2) REFERENCES ";
    ddl += parent_name_2;
    ddl += " (id_primary_col1, id_primary_col2))";
    return ddl;
  }

  std::string catalog() const override { return "postgres"; }
  std::string db_schema() const override { return "public"; }

  bool supports_bulk_ingest_catalog() const override { return false; }
  bool supports_bulk_ingest_db_schema() const override { return true; }
  bool supports_bulk_ingest_temporary() const override { return true; }
  bool supports_cancel() const override { return true; }
  bool supports_execute_schema() const override { return true; }
  std::optional<adbc_validation::SqlInfoValue> supports_get_sql_info(
      uint32_t info_code) const override {
    switch (info_code) {
      case ADBC_INFO_DRIVER_ADBC_VERSION:
        return ADBC_VERSION_1_1_0;
      case ADBC_INFO_DRIVER_NAME:
        return "ADBC PostgreSQL Driver";
      case ADBC_INFO_DRIVER_VERSION:
        return "(unknown)";
      case ADBC_INFO_VENDOR_NAME:
        return "PostgreSQL";
      default:
        return std::nullopt;
    }
  }
  bool supports_metadata_current_catalog() const override { return true; }
  bool supports_metadata_current_db_schema() const override { return true; }
  bool supports_statistics() const override { return true; }
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

int Canary(const struct AdbcError*) { return 0; }

TEST_F(PostgresDatabaseTest, AdbcDriverBackwardsCompatibility) {
  struct AdbcDriver driver;
  std::memset(&driver, 0, ADBC_DRIVER_1_1_0_SIZE);
  driver.ErrorGetDetailCount = Canary;

  ASSERT_THAT(::AdbcDriverPostgresqlInit(ADBC_VERSION_1_0_0, &driver, &error),
              IsOkStatus(&error));

  ASSERT_EQ(Canary, driver.ErrorGetDetailCount);

  ASSERT_THAT(::AdbcDriverPostgresqlInit(424242, &driver, &error),
              IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
}

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
      ADBC_INFO_DRIVER_NAME, ADBC_INFO_DRIVER_VERSION, ADBC_INFO_DRIVER_ADBC_VERSION,
      ADBC_INFO_VENDOR_NAME, ADBC_INFO_VENDOR_VERSION,
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
      const uint32_t offset =
          reader.array_view->children[1]->buffer_views[1].data.as_int32[row];
      seen.push_back(code);

      struct ArrowArrayView* str_child = reader.array_view->children[1]->children[0];
      struct ArrowArrayView* int_child = reader.array_view->children[1]->children[2];
      switch (code) {
        case ADBC_INFO_DRIVER_NAME: {
          ArrowStringView val = ArrowArrayViewGetStringUnsafe(str_child, offset);
          EXPECT_EQ("ADBC PostgreSQL Driver", std::string(val.data, val.size_bytes));
          break;
        }
        case ADBC_INFO_DRIVER_VERSION: {
          ArrowStringView val = ArrowArrayViewGetStringUnsafe(str_child, offset);
          EXPECT_EQ("(unknown)", std::string(val.data, val.size_bytes));
          break;
        }
        case ADBC_INFO_VENDOR_NAME: {
          ArrowStringView val = ArrowArrayViewGetStringUnsafe(str_child, offset);
          EXPECT_EQ("PostgreSQL", std::string(val.data, val.size_bytes));
          break;
        }
        case ADBC_INFO_VENDOR_VERSION: {
          ArrowStringView val = ArrowArrayViewGetStringUnsafe(str_child, offset);
#ifdef __WIN32
          const char* pater = "\\d\\d\\d\\d\\d\\d";
#else
          const char* pater = "[0-9]{6}";
#endif
          EXPECT_THAT(std::string(val.data, val.size_bytes),
                      ::testing::MatchesRegex(pater));
          break;
        }
        case ADBC_INFO_DRIVER_ADBC_VERSION: {
          EXPECT_EQ(ADBC_VERSION_1_1_0, ArrowArrayViewGetIntUnsafe(int_child, offset));
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
        InternalAdbcGetObjectsDataGetCatalogByName(*get_objects_data, catalog);
    ASSERT_NE(cat, nullptr) << "catalog " << catalog << " not found";
  }
}

TEST_F(PostgresConnectionTest, GetObjectsGetDbSchemas) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

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
      InternalAdbcGetObjectsDataGetSchemaByName(*get_objects_data, "postgres", "public");
  ASSERT_NE(schema, nullptr) << "schema public not found";
}

TEST_F(PostgresConnectionTest, GetObjectsGetAllFindsPrimaryKey) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

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
    ASSERT_EQ(reader.rows_affected, -1);
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

  struct AdbcGetObjectsTable* table = InternalAdbcGetObjectsDataGetTableByName(
      *get_objects_data, "postgres", "public", "adbc_pkey_test");
  ASSERT_NE(table, nullptr) << "could not find adbc_pkey_test table";

  ASSERT_EQ(table->n_table_columns, 2);
  struct AdbcGetObjectsColumn* column = InternalAdbcGetObjectsDataGetColumnByName(
      *get_objects_data, "postgres", "public", "adbc_pkey_test", "id");
  ASSERT_NE(column, nullptr) << "could not find id column on adbc_pkey_test table";

  ASSERT_EQ(table->n_table_constraints, 1)
      << "expected 1 constraint on adbc_pkey_test table, found: "
      << table->n_table_constraints;

  struct AdbcGetObjectsConstraint* constraint =
      InternalAdbcGetObjectsDataGetConstraintByName(*get_objects_data, "postgres",
                                                    "public", "adbc_pkey_test",
                                                    "adbc_pkey_test_pkey");
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
    ASSERT_EQ(reader.rows_affected, -1);
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
    ASSERT_EQ(reader.rows_affected, -1);
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

  struct AdbcGetObjectsTable* table = InternalAdbcGetObjectsDataGetTableByName(
      *get_objects_data, "postgres", "public", "adbc_fkey_test");
  ASSERT_NE(table, nullptr) << "could not find adbc_fkey_test table";
  ASSERT_EQ(table->n_table_constraints, 1)
      << "expected 1 constraint on adbc_fkey_test table, found: "
      << table->n_table_constraints;

  const std::string version = adbc_validation::GetDriverVendorVersion(&connection);
  const std::string search_name =
      version < "120000" ? "adbc_fkey_test_fid1_fkey" : "adbc_fkey_test_fid1_fid2_fkey";
  struct AdbcGetObjectsConstraint* constraint =
      InternalAdbcGetObjectsDataGetConstraintByName(
          *get_objects_data, "postgres", "public", "adbc_fkey_test", search_name.c_str());
  ASSERT_NE(constraint, nullptr) << "could not find " << search_name << " constraint";

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

  struct AdbcGetObjectsTable* table = InternalAdbcGetObjectsDataGetTableByName(
      *get_objects_data, "postgres", "public", "adbc_table_types_table_test");
  ASSERT_EQ(table, nullptr) << "unexpected table adbc_table_types_table_test found";

  struct AdbcGetObjectsTable* view = InternalAdbcGetObjectsDataGetTableByName(
      *get_objects_data, "postgres", "public", "adbc_table_types_view_test");
  ASSERT_NE(view, nullptr) << "did not find view adbc_table_types_view_test";
}

TEST_F(PostgresConnectionTest, MetadataSetCurrentDbSchema) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  {
    adbc_validation::Handle<struct AdbcStatement> statement;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementSetSqlQuery(
                    &statement.value, "CREATE SCHEMA IF NOT EXISTS testschema", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));

    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement.value,
            "CREATE TABLE IF NOT EXISTS testschema.schematable (ints INT)", &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
  }

  adbc_validation::Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
              IsOkStatus(&error));

  // Table does not exist in this schema
  error.vendor_code = ADBC_ERROR_VENDOR_CODE_PRIVATE_DATA;
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement.value, "SELECT * FROM schematable", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  // 42P01 = table not found
  ASSERT_EQ("42P01", std::string_view(error.sqlstate, 5));
  ASSERT_NE(0, AdbcErrorGetDetailCount(&error));
  bool found = false;
  for (int i = 0; i < AdbcErrorGetDetailCount(&error); i++) {
    struct AdbcErrorDetail detail = AdbcErrorGetDetail(&error, i);
    if (std::strcmp(detail.key, "PG_DIAG_MESSAGE_PRIMARY") == 0) {
      found = true;
      std::string_view message(reinterpret_cast<const char*>(detail.value),
                               detail.value_length);
      ASSERT_THAT(message, ::testing::HasSubstr("schematable"));
    }
  }
  error.release(&error);
  ASSERT_TRUE(found) << "Did not find expected error detail";

  ASSERT_THAT(
      AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA,
                              "testschema", &error),
      IsOkStatus(&error));

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement.value, "SELECT * FROM schematable", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
}

TEST_F(PostgresConnectionTest, MetadataSetCurrentDbSchemaInit) {
  // Regression test: setting the schema before Init (which Python does)

  // 1. Create the schema
  {
    ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

    adbc_validation::Handle<struct AdbcStatement> statement;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));

    ASSERT_THAT(
        AdbcStatementSetSqlQuery(&statement.value,
                                 "CREATE SCHEMA IF NOT EXISTS regtestschema", &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
  }

  // 2. Initialize a connection with the schema
  {
    ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
    ASSERT_THAT(
        AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA,
                                "regtestschema", &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

    ASSERT_THAT(adbc_validation::ConnectionGetOption(
                    &connection, ADBC_CONNECTION_OPTION_CURRENT_DB_SCHEMA, &error),
                ::testing::Optional("regtestschema"s));
  }
}

TEST_F(PostgresConnectionTest, MetadataGetSchemaCaseSensitiveTable) {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  // Create sample table
  {
    adbc_validation::Handle<struct AdbcStatement> statement;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement.value,
                                         "DROP TABLE IF EXISTS \"Uppercase\"", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));

    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement.value, "CREATE TABLE \"Uppercase\" (ints INT, strs TEXT)", &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  // Check its schema
  nanoarrow::UniqueSchema schema;
  ASSERT_THAT(AdbcConnectionGetTableSchema(&connection, nullptr, nullptr, "Uppercase",
                                           schema.get(), &error),
              IsOkStatus(&error));

  ASSERT_NE(schema->release, nullptr);
  ASSERT_STREQ(schema->format, "+s");
  ASSERT_EQ(schema->n_children, 2);
  ASSERT_STREQ(schema->children[0]->format, "i");
  ASSERT_STREQ(schema->children[1]->format, "u");
  ASSERT_STREQ(schema->children[0]->name, "ints");
  ASSERT_STREQ(schema->children[1]->name, "strs");

  // Do we have to release the connection here?
}

TEST_F(PostgresConnectionTest, MetadataGetStatistics) {
  if (!quirks()->supports_statistics()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  // Create sample table
  {
    adbc_validation::Handle<struct AdbcStatement> statement;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement.value,
                                         "DROP TABLE IF EXISTS statstable", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));

    ASSERT_THAT(
        AdbcStatementSetSqlQuery(&statement.value,
                                 "CREATE TABLE statstable (ints INT, strs TEXT)", &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));

    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement.value,
            "INSERT INTO statstable VALUES (1, 'a'), (NULL, 'bcd'), (-5, NULL)", &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement.value, "ANALYZE statstable", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
  }

  adbc_validation::StreamReader reader;
  ASSERT_THAT(
      AdbcConnectionGetStatistics(&connection, nullptr, quirks()->db_schema().c_str(),
                                  "statstable", 1, &reader.stream.value, &error),
      IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());

  ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareSchema(
      &reader.schema.value, {
                                {"catalog_name", NANOARROW_TYPE_STRING, true},
                                {"catalog_db_schemas", NANOARROW_TYPE_LIST, false},
                            }));

  ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareSchema(
      reader.schema->children[1]->children[0],
      {
          {"db_schema_name", NANOARROW_TYPE_STRING, true},
          {"db_schema_statistics", NANOARROW_TYPE_LIST, false},
      }));

  ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareSchema(
      reader.schema->children[1]->children[0]->children[1]->children[0],
      {
          {"table_name", NANOARROW_TYPE_STRING, false},
          {"column_name", NANOARROW_TYPE_STRING, true},
          {"statistic_key", NANOARROW_TYPE_INT16, false},
          {"statistic_value", NANOARROW_TYPE_DENSE_UNION, false},
          {"statistic_is_approximate", NANOARROW_TYPE_BOOL, false},
      }));

  ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareSchema(
      reader.schema->children[1]->children[0]->children[1]->children[0]->children[3],
      {
          {"int64", NANOARROW_TYPE_INT64, true},
          {"uint64", NANOARROW_TYPE_UINT64, true},
          {"float64", NANOARROW_TYPE_DOUBLE, true},
          {"binary", NANOARROW_TYPE_BINARY, true},
      }));

  std::vector<std::tuple<std::optional<std::string>, int16_t, int64_t>> seen;
  while (true) {
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    if (!reader.array->release) break;

    for (int64_t catalog_index = 0; catalog_index < reader.array->length;
         catalog_index++) {
      struct ArrowStringView catalog_name =
          ArrowArrayViewGetStringUnsafe(reader.array_view->children[0], catalog_index);
      ASSERT_EQ(quirks()->catalog(),
                std::string_view(catalog_name.data,
                                 static_cast<int64_t>(catalog_name.size_bytes)));

      struct ArrowArrayView* catalog_db_schemas = reader.array_view->children[1];
      struct ArrowArrayView* schema_stats = catalog_db_schemas->children[0]->children[1];
      struct ArrowArrayView* stats =
          catalog_db_schemas->children[0]->children[1]->children[0];
      for (int64_t schema_index =
               ArrowArrayViewListChildOffset(catalog_db_schemas, catalog_index);
           schema_index <
           ArrowArrayViewListChildOffset(catalog_db_schemas, catalog_index + 1);
           schema_index++) {
        struct ArrowStringView schema_name = ArrowArrayViewGetStringUnsafe(
            catalog_db_schemas->children[0]->children[0], schema_index);
        ASSERT_EQ(quirks()->db_schema(),
                  std::string_view(schema_name.data,
                                   static_cast<int64_t>(schema_name.size_bytes)));

        for (int64_t stat_index =
                 ArrowArrayViewListChildOffset(schema_stats, schema_index);
             stat_index < ArrowArrayViewListChildOffset(schema_stats, schema_index + 1);
             stat_index++) {
          struct ArrowStringView table_name =
              ArrowArrayViewGetStringUnsafe(stats->children[0], stat_index);
          ASSERT_EQ("statstable",
                    std::string_view(table_name.data,
                                     static_cast<int64_t>(table_name.size_bytes)));
          std::optional<std::string> column_name;
          if (!ArrowArrayViewIsNull(stats->children[1], stat_index)) {
            struct ArrowStringView value =
                ArrowArrayViewGetStringUnsafe(stats->children[1], stat_index);
            column_name = std::string(value.data, value.size_bytes);
          }
          ASSERT_TRUE(ArrowArrayViewGetIntUnsafe(stats->children[4], stat_index));

          const int16_t stat_key = static_cast<int16_t>(
              ArrowArrayViewGetIntUnsafe(stats->children[2], stat_index));
          const int32_t offset =
              stats->children[3]->buffer_views[1].data.as_int32[stat_index];
          int64_t stat_value;
          switch (stat_key) {
            case ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_KEY:
            case ADBC_STATISTIC_DISTINCT_COUNT_KEY:
            case ADBC_STATISTIC_NULL_COUNT_KEY:
            case ADBC_STATISTIC_ROW_COUNT_KEY:
              stat_value = static_cast<int64_t>(
                  std::round(100 * ArrowArrayViewGetDoubleUnsafe(
                                       stats->children[3]->children[2], offset)));
              break;
            default:
              continue;
          }
          seen.emplace_back(std::move(column_name), stat_key, stat_value);
        }
      }
    }
  }

  ASSERT_THAT(seen,
              ::testing::UnorderedElementsAreArray(
                  std::vector<std::tuple<std::optional<std::string>, int16_t, int64_t>>{
                      {"ints", ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_KEY, 400},
                      {"strs", ADBC_STATISTIC_AVERAGE_BYTE_WIDTH_KEY, 300},
                      {"ints", ADBC_STATISTIC_NULL_COUNT_KEY, 100},
                      {"strs", ADBC_STATISTIC_NULL_COUNT_KEY, 100},
                      {"ints", ADBC_STATISTIC_DISTINCT_COUNT_KEY, 200},
                      {"strs", ADBC_STATISTIC_DISTINCT_COUNT_KEY, 200},
                      {std::nullopt, ADBC_STATISTIC_ROW_COUNT_KEY, 300},
                  }));
}

ADBCV_TEST_CONNECTION(PostgresConnectionTest)

class PostgresStatementTest : public ::testing::Test,
                              public adbc_validation::StatementTest {
 public:
  const adbc_validation::DriverQuirks* quirks() const override { return &quirks_; }
  void SetUp() override { ASSERT_NO_FATAL_FAILURE(SetUpTest()); }
  void TearDown() override { ASSERT_NO_FATAL_FAILURE(TearDownTest()); }

  void TestSqlPrepareErrorParamCountMismatch() { GTEST_SKIP() << "Not yet implemented"; }

  void TestSqlPrepareGetParameterSchema() {
    ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetSqlQuery(
                    &statement,
                    "SELECT $1::INTEGER, $2::TEXT, $3::BOOLEAN, $4::TIMESTAMP", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));

    {
      adbc_validation::Handle<struct ArrowSchema> schema;
      ASSERT_THAT(AdbcStatementGetParameterSchema(&statement, &schema.value, &error),
                  IsOkStatus(&error));
      if (schema->release != nullptr) {
        ASSERT_EQ(4, schema->n_children);

        // $1::INTEGER should map to int4 (PostgreSQL name) and int32 format
        ASSERT_STREQ("int4", schema->children[0]->name);
        EXPECT_STREQ("i", schema->children[0]->format);

        // $2::TEXT should map to text and string format
        ASSERT_STREQ("text", schema->children[1]->name);
        EXPECT_STREQ("u", schema->children[1]->format);

        // $3::BOOLEAN should map to bool and bool format
        ASSERT_STREQ("bool", schema->children[2]->name);
        EXPECT_STREQ("b", schema->children[2]->format);

        // $4::TIMESTAMP should map to timestamp and timestamp format
        ASSERT_STREQ("timestamp", schema->children[3]->name);
        EXPECT_EQ(0, strncmp("ts", schema->children[3]->format, 2));
      }
    }

    // Test with complex PostgreSQL types
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement,
                                         "SELECT $1::JSONB, $2::UUID, $3::INET", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));

    {
      adbc_validation::Handle<struct ArrowSchema> schema;
      ASSERT_THAT(AdbcStatementGetParameterSchema(&statement, &schema.value, &error),
                  IsOkStatus(&error));
      if (schema->release != nullptr) {
        ASSERT_EQ(3, schema->n_children);

        // $1::JSONB should map to jsonb with string format
        ASSERT_STREQ("jsonb", schema->children[0]->name);
        EXPECT_STREQ("u", schema->children[0]->format);

        // $2::UUID should map to uuid with binary format
        ASSERT_STREQ("uuid", schema->children[1]->name);
        EXPECT_STREQ("z", schema->children[1]->format);

        // $3::INET should map to inet with binary format
        ASSERT_STREQ("inet", schema->children[2]->name);
        EXPECT_STREQ("z", schema->children[2]->format);
      }
    }

    // Test with no parameters
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 42", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));

    {
      adbc_validation::Handle<struct ArrowSchema> schema;
      ASSERT_THAT(AdbcStatementGetParameterSchema(&statement, &schema.value, &error),
                  IsOkStatus(&error));
      ASSERT_EQ(0, schema->n_children);
    }

    // Test error condition: GetParameterSchema without SetSqlQuery
    ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
    {
      adbc_validation::Handle<struct ArrowSchema> schema;
      ASSERT_THAT(AdbcStatementGetParameterSchema(&statement, &schema.value, &error),
                  IsStatus(ADBC_STATUS_INVALID_STATE, &error));
      ASSERT_THAT(error.message,
                  ::testing::HasSubstr("Must SetSqlQuery before GetParameterSchema"));
    }
  }

  void TestSqlPrepareSelectParams() { GTEST_SKIP() << "Not yet implemented"; }

  void TestConcurrentStatements() {
    // TODO: refactor driver so that we read all the data as soon as
    // we ExecuteQuery() since that's how libpq already works - then
    // we can actually support concurrent statements (because there is
    // no concurrency)
    GTEST_SKIP() << "Not yet implemented";
  }

 protected:
  void ValidateIngestedTemporalData(struct ArrowArrayView* values, ArrowType type,
                                    enum ArrowTimeUnit unit,
                                    const char* timezone) override {
    switch (type) {
      case NANOARROW_TYPE_TIMESTAMP: {
        std::vector<std::optional<int64_t>> expected;
        switch (unit) {
          case (NANOARROW_TIME_UNIT_SECOND):
            expected.insert(expected.end(), {std::nullopt, -42000000, 0, 42000000});
            break;
          case (NANOARROW_TIME_UNIT_MILLI):
            expected.insert(expected.end(), {std::nullopt, -42000, 0, 42000});
            break;
          case (NANOARROW_TIME_UNIT_MICRO):
            expected.insert(expected.end(), {std::nullopt, -42, 0, 42});
            break;
          case (NANOARROW_TIME_UNIT_NANO):
            expected.insert(expected.end(), {std::nullopt, 0, 0, 0});
            break;
        }
        ASSERT_NO_FATAL_FAILURE(
            adbc_validation::CompareArray<std::int64_t>(values, expected));
        break;
      }
      case NANOARROW_TYPE_DURATION: {
        struct ArrowInterval neg_interval;
        struct ArrowInterval zero_interval;
        struct ArrowInterval pos_interval;

        ArrowIntervalInit(&neg_interval, type);
        ArrowIntervalInit(&zero_interval, type);
        ArrowIntervalInit(&pos_interval, type);

        neg_interval.months = 0;
        neg_interval.days = 0;
        zero_interval.months = 0;
        zero_interval.days = 0;
        pos_interval.months = 0;
        pos_interval.days = 0;

        switch (unit) {
          case (NANOARROW_TIME_UNIT_SECOND):
            neg_interval.ns = -42000000000;
            zero_interval.ns = 0;
            pos_interval.ns = 42000000000;
            break;
          case (NANOARROW_TIME_UNIT_MILLI):
            neg_interval.ns = -42000000;
            zero_interval.ns = 0;
            pos_interval.ns = 42000000;
            break;
          case (NANOARROW_TIME_UNIT_MICRO):
            neg_interval.ns = -42000;
            zero_interval.ns = 0;
            pos_interval.ns = 42000;
            break;
          case (NANOARROW_TIME_UNIT_NANO):
            // lower than us precision is lost
            neg_interval.ns = 0;
            zero_interval.ns = 0;
            pos_interval.ns = 0;
            break;
        }
        const std::vector<std::optional<ArrowInterval*>> expected = {
            std::nullopt, &neg_interval, &zero_interval, &pos_interval};
        ASSERT_NO_FATAL_FAILURE(
            adbc_validation::CompareArray<ArrowInterval*>(values, expected));
        break;
      }
      default:
        FAIL() << "ValidateIngestedTemporalData not implemented for type " << type;
    }
  }

  PostgresQuirks quirks_;
};
ADBCV_TEST_STATEMENT(PostgresStatementTest)

TEST_F(PostgresStatementTest, TransactionStatus) {
  using adbc_validation::ConnectionGetOption;
  const char* txn_status = "adbc.postgresql.transaction_status";
  ASSERT_THAT(quirks()->DropTable(&connection, "txntest", &error), IsOkStatus(&error));

  ASSERT_EQ("idle", ConnectionGetOption(&connection, txn_status, &error));

  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error),
              IsOkStatus(&error));

  ASSERT_EQ("intrans", ConnectionGetOption(&connection, txn_status, &error));

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  {
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 1", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());

    ASSERT_EQ("active", ConnectionGetOption(&connection, txn_status, &error));

    ASSERT_THAT(AdbcConnectionRollback(&connection, &error), IsOkStatus(&error));
    ASSERT_EQ("intrans", ConnectionGetOption(&connection, txn_status, &error));
  }
  {
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 1", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());

    ASSERT_EQ("active", ConnectionGetOption(&connection, txn_status, &error));

    ASSERT_THAT(AdbcConnectionCommit(&connection, &error), IsOkStatus(&error));
    ASSERT_EQ("intrans", ConnectionGetOption(&connection, txn_status, &error));
  }
}

TEST_F(PostgresStatementTest, SqlIngestSchema) {
  const std::string schema_name = "testschema";

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement,
                                       "CREATE SCHEMA IF NOT EXISTS testschema", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  std::string drop = "DROP TABLE IF EXISTS testschema.schematable";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, drop.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  {
    adbc_validation::Handle<struct ArrowSchema> schema;
    adbc_validation::Handle<struct ArrowArray> batch;

    ArrowSchemaInit(&schema.value);
    ASSERT_THAT(ArrowSchemaSetTypeStruct(&schema.value, 1), adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT64),
                adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetName(schema->children[0], "ints"),
                adbc_validation::IsOkErrno());

    ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(
                    &schema.value, &batch.value, static_cast<struct ArrowError*>(nullptr),
                    {-1, 0, 1, std::nullopt})),
                adbc_validation::IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       "schematable", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_CREATE, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
                                       schema_name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement, &batch.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }
}

TEST_F(PostgresStatementTest, SqlIngestTemporaryTable) {
  ASSERT_THAT(quirks()->DropTempTable(&connection, "temptable", &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementSetSqlQuery(
                  &statement, "CREATE TEMPORARY TABLE temptable (ints BIGINT)", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionCommit(&connection, &error), IsOkStatus(&error));

  {
    adbc_validation::Handle<struct ArrowSchema> schema;
    adbc_validation::Handle<struct ArrowArray> batch;

    ArrowSchemaInit(&schema.value);
    ASSERT_THAT(ArrowSchemaSetTypeStruct(&schema.value, 1), adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT64),
                adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetName(schema->children[0], "ints"),
                adbc_validation::IsOkErrno());

    ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(
                    &schema.value, &batch.value, static_cast<struct ArrowError*>(nullptr),
                    {-1, 0, 1, std::nullopt})),
                adbc_validation::IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       "temptable", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_APPEND, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement, &batch.value, &schema.value, &error),
                IsOkStatus(&error));
    // because temporary table
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  }

  ASSERT_THAT(AdbcConnectionRollback(&connection, &error), IsOkStatus(&error));

  {
    adbc_validation::Handle<struct ArrowSchema> schema;
    adbc_validation::Handle<struct ArrowArray> batch;

    ArrowSchemaInit(&schema.value);
    ASSERT_THAT(ArrowSchemaSetTypeStruct(&schema.value, 1), adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_INT64),
                adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetName(schema->children[0], "ints"),
                adbc_validation::IsOkErrno());

    ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(
                    &schema.value, &batch.value, static_cast<struct ArrowError*>(nullptr),
                    {-1, 0, 1, std::nullopt})),
                adbc_validation::IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       "temptable", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_APPEND, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement, &batch.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_ENABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }
}

TEST_F(PostgresStatementTest, SqlIngestTimestampOverflow) {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  {
    adbc_validation::Handle<struct ArrowSchema> schema;
    adbc_validation::Handle<struct ArrowArray> batch;

    ArrowSchemaInit(&schema.value);
    ASSERT_THAT(ArrowSchemaSetTypeStruct(&schema.value, 1), adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetName(schema->children[0], "$1"),
                adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_TIMESTAMP,
                                           NANOARROW_TIME_UNIT_SECOND, nullptr),
                adbc_validation::IsOkErrno());

    ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(
                    &schema.value, &batch.value, static_cast<struct ArrowError*>(nullptr),
                    {std::numeric_limits<int64_t>::max()})),
                adbc_validation::IsOkErrno());

    ASSERT_THAT(
        AdbcStatementSetSqlQuery(&statement, "SELECT CAST($1 AS TIMESTAMP)", &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement, &batch.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsStatus(ADBC_STATUS_INTERNAL, &error));
    ASSERT_THAT(
        error.message,
        ::testing::HasSubstr(
            "Row 0 timestamp value 9223372036854775807 with unit 0 would overflow"));
  }

  {
    adbc_validation::Handle<struct ArrowSchema> schema;
    adbc_validation::Handle<struct ArrowArray> batch;

    ArrowSchemaInit(&schema.value);
    ASSERT_THAT(ArrowSchemaSetTypeStruct(&schema.value, 1), adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetName(schema->children[0], "$1"),
                adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_TIMESTAMP,
                                           NANOARROW_TIME_UNIT_SECOND, nullptr),
                adbc_validation::IsOkErrno());

    ASSERT_THAT((adbc_validation::MakeBatch<int64_t>(
                    &schema.value, &batch.value, static_cast<struct ArrowError*>(nullptr),
                    {std::numeric_limits<int64_t>::min()})),
                adbc_validation::IsOkErrno());

    ASSERT_THAT(
        AdbcStatementSetSqlQuery(&statement, "SELECT CAST($1 AS TIMESTAMP)", &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement, &batch.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsStatus(ADBC_STATUS_INTERNAL, &error));
    ASSERT_THAT(
        error.message,
        ::testing::HasSubstr(
            "Row 0 timestamp value -9223372036854775808 with unit 0 would overflow"));
  }
}

TEST_F(PostgresStatementTest, SqlIngestJson) {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  std::string drop = "DROP TABLE IF EXISTS jsontable";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, drop.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  {
    adbc_validation::Handle<struct ArrowSchema> schema;
    adbc_validation::Handle<struct ArrowArray> batch;

    ArrowSchemaInit(&schema.value);
    ASSERT_THAT(ArrowSchemaSetTypeStruct(&schema.value, 1), adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_STRING),
                adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetName(schema->children[0], "j"),
                adbc_validation::IsOkErrno());

    nanoarrow::UniqueBuffer buffer;
    ASSERT_THAT(ArrowMetadataBuilderInit(buffer.get(), nullptr),
                adbc_validation::IsOkErrno());
    ASSERT_THAT(
        ArrowMetadataBuilderAppend(buffer.get(), ArrowCharView("ARROW:extension:name"),
                                   ArrowCharView("arrow.json")),
        adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetMetadata(schema->children[0],
                                       reinterpret_cast<char*>(buffer->data)),
                adbc_validation::IsOkErrno());

    ASSERT_THAT((adbc_validation::MakeBatch<std::string>(
                    &schema.value, &batch.value, static_cast<struct ArrowError*>(nullptr),
                    {R"({"a": 1, "b": [1, 2, 3]})", std::nullopt})),
                adbc_validation::IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       "jsontable", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_CREATE, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement, &batch.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  // Check round-trip
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT * FROM jsontable", &error),
              IsOkStatus(&error));
  adbc_validation::StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));

  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_EQ(1, reader.fields.size());
  ASSERT_EQ(NANOARROW_TYPE_STRING, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ArrowStringView view = ArrowArrayViewGetStringUnsafe(reader.array_view->children[0], 0);
  std::string_view v(view.data, static_cast<size_t>(view.size_bytes));
  ASSERT_EQ(R"({"a": 1, "b": [1, 2, 3]})", v);
}

// Ensure the table is actually created with the JSON type by trying to ingest
// invalid JSON
TEST_F(PostgresStatementTest, SqlIngestJsonInvalid) {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  std::string drop = "DROP TABLE IF EXISTS jsontable";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, drop.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> batch;

  ArrowSchemaInit(&schema.value);
  ASSERT_THAT(ArrowSchemaSetTypeStruct(&schema.value, 1), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_STRING),
              adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowSchemaSetName(schema->children[0], "j"), adbc_validation::IsOkErrno());

  nanoarrow::UniqueBuffer buffer;
  ASSERT_THAT(ArrowMetadataBuilderInit(buffer.get(), nullptr),
              adbc_validation::IsOkErrno());
  ASSERT_THAT(
      ArrowMetadataBuilderAppend(buffer.get(), ArrowCharView("ARROW:extension:name"),
                                 ArrowCharView("arrow.json")),
      adbc_validation::IsOkErrno());
  ASSERT_THAT(
      ArrowSchemaSetMetadata(schema->children[0], reinterpret_cast<char*>(buffer->data)),
      adbc_validation::IsOkErrno());

  ASSERT_THAT((adbc_validation::MakeBatch<std::string>(
                  &schema.value, &batch.value, static_cast<struct ArrowError*>(nullptr),
                  {R"({)", std::nullopt})),
              adbc_validation::IsOkErrno());

  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     "jsontable", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                     ADBC_INGEST_OPTION_MODE_CREATE, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, &batch.value, &schema.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  ASSERT_THAT(error.message, ::testing::HasSubstr("invalid input syntax for type json"));
}

TEST_F(PostgresStatementTest, SqlIngestJsonb) {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  std::string drop = "DROP TABLE IF EXISTS jsontable";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, drop.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  drop = "CREATE TABLE jsontable (j JSONB)";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, drop.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  {
    adbc_validation::Handle<struct ArrowSchema> schema;
    adbc_validation::Handle<struct ArrowArray> batch;

    ArrowSchemaInit(&schema.value);
    ASSERT_THAT(ArrowSchemaSetTypeStruct(&schema.value, 1), adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_STRING),
                adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetName(schema->children[0], "j"),
                adbc_validation::IsOkErrno());

    nanoarrow::UniqueBuffer buffer;
    ASSERT_THAT(ArrowMetadataBuilderInit(buffer.get(), nullptr),
                adbc_validation::IsOkErrno());
    ASSERT_THAT(
        ArrowMetadataBuilderAppend(buffer.get(), ArrowCharView("ARROW:extension:name"),
                                   ArrowCharView("arrow.json")),
        adbc_validation::IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetMetadata(schema->children[0],
                                       reinterpret_cast<char*>(buffer->data)),
                adbc_validation::IsOkErrno());

    ASSERT_THAT((adbc_validation::MakeBatch<std::string>(
                    &schema.value, &batch.value, static_cast<struct ArrowError*>(nullptr),
                    {R"({"a": 1, "b": [1, 2, 3]})", std::nullopt})),
                adbc_validation::IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       "jsontable", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_APPEND, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement, &batch.value, &schema.value, &error),
                IsOkStatus(&error));
    // TODO(https://github.com/apache/arrow-adbc/issues/3293): we need a
    // different extension type for JSONB so the driver can know to generate
    // the appropriate COPY representation
    // (JSON-representation-version-prefixed JSON string).
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsStatus(ADBC_STATUS_INVALID_ARGUMENT, &error));
  }
}

TEST_F(PostgresStatementTest, SqlReadIntervalOverflow) {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  {
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement, "SELECT CAST('P0Y0M0DT2562048H0M0S' AS INTERVAL)", &error),
        IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(reader.rows_affected, -1);
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_THAT(reader.MaybeNext(),
                adbc_validation::IsErrno(EINVAL, &reader.stream.value, nullptr));
    ASSERT_THAT(reader.stream->get_last_error(&reader.stream.value),
                ::testing::HasSubstr("Interval with time value 9223372800000000 usec "
                                     "would overflow when converting to nanoseconds"));
    ASSERT_EQ(reader.array->release, nullptr);
  }

  {
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement, "SELECT CAST('P0Y0M0DT-2562048H0M0S' AS INTERVAL)", &error),
        IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(reader.rows_affected, -1);
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_THAT(reader.MaybeNext(),
                adbc_validation::IsErrno(EINVAL, &reader.stream.value, nullptr));
    ASSERT_THAT(reader.stream->get_last_error(&reader.stream.value),
                ::testing::HasSubstr("Interval with time value -9223372800000000 usec "
                                     "would overflow when converting to nanoseconds"));
    ASSERT_EQ(reader.array->release, nullptr);
  }
}

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
    ASSERT_EQ(reader.rows_affected, -1);
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
    ASSERT_EQ(reader.rows_affected, 2);
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

TEST_F(PostgresStatementTest, ExecuteSchemaParameterizedQuery) {
  nanoarrow::UniqueSchema schema_bind;
  ArrowSchemaInit(schema_bind.get());
  ASSERT_THAT(ArrowSchemaSetTypeStruct(schema_bind.get(), 1),
              adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowSchemaSetType(schema_bind->children[0], NANOARROW_TYPE_STRING),
              adbc_validation::IsOkErrno());

  nanoarrow::UniqueArrayStream bind;
  nanoarrow::EmptyArrayStream(schema_bind.get()).ToArrayStream(bind.get());

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT $1", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBindStream(&statement, bind.get(), &error), IsOkStatus());

  nanoarrow::UniqueSchema schema;
  ASSERT_THAT(AdbcStatementExecuteSchema(&statement, schema.get(), &error),
              IsOkStatus(&error));

  ASSERT_EQ(1, schema->n_children);
  ASSERT_STREQ("u", schema->children[0]->format);

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

TEST_F(PostgresStatementTest, ExecuteParameterizedQueryWithResult) {
  nanoarrow::UniqueSchema schema_bind;
  ArrowSchemaInit(schema_bind.get());
  ASSERT_THAT(ArrowSchemaSetTypeStruct(schema_bind.get(), 1),
              adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowSchemaSetType(schema_bind->children[0], NANOARROW_TYPE_INT32),
              adbc_validation::IsOkErrno());

  nanoarrow::UniqueArray bind;
  ASSERT_THAT(ArrowArrayInitFromSchema(bind.get(), schema_bind.get(), nullptr),
              adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayStartAppending(bind.get()), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayAppendInt(bind->children[0], 123), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayFinishElement(bind.get()), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayAppendInt(bind->children[0], 456), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayFinishElement(bind.get()), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayAppendNull(bind->children[0], 1), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayFinishElement(bind.get()), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayFinishBuildingDefault(bind.get(), nullptr),
              adbc_validation::IsOkErrno());

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT $1", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, bind.get(), schema_bind.get(), &error),
              IsOkStatus());

  {
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(reader.rows_affected, -1);
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_EQ(reader.schema->n_children, 1);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->length, 1);
    ASSERT_EQ(reader.array_view->children[0]->buffer_views[1].data.as_int32[0], 123);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->length, 1);
    ASSERT_EQ(reader.array_view->children[0]->buffer_views[1].data.as_int32[0], 456);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->length, 1);
    ASSERT_EQ(reader.array->children[0]->null_count, 1);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }
}

TEST_F(PostgresStatementTest, ExecuteParameterizedQueryWithRowsAffected) {
  // Check that when executing one or more parameterized queries that the corresponding
  // affected row count is added.
  ASSERT_THAT(quirks()->DropTable(&connection, "adbc_test", &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  {
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(&statement, "CREATE TABLE adbc_test (ints INT)", &error),
        IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(reader.rows_affected, -1);
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }

  {
    // Use INSERT INTO
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement, "INSERT INTO adbc_test (ints) VALUES (123), (456)", &error),
        IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(reader.rows_affected, 2);
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }

  nanoarrow::UniqueSchema schema_bind;
  ArrowSchemaInit(schema_bind.get());
  ASSERT_THAT(ArrowSchemaSetTypeStruct(schema_bind.get(), 1),
              adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowSchemaSetType(schema_bind->children[0], NANOARROW_TYPE_INT32),
              adbc_validation::IsOkErrno());

  nanoarrow::UniqueArray bind;
  ASSERT_THAT(ArrowArrayInitFromSchema(bind.get(), schema_bind.get(), nullptr),
              adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayStartAppending(bind.get()), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayAppendInt(bind->children[0], 123), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayFinishElement(bind.get()), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayAppendInt(bind->children[0], 456), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayFinishElement(bind.get()), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowArrayFinishBuildingDefault(bind.get(), nullptr),
              adbc_validation::IsOkErrno());

  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement,
                                       "DELETE FROM adbc_test WHERE ints = $1", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, bind.get(), schema_bind.get(), &error),
              IsOkStatus());

  {
    int64_t rows_affected = -2;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_EQ(rows_affected, 2);
  }

  {
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT * from adbc_test", &error),
                IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }
}

TEST_F(PostgresStatementTest, SqlExecuteCopyZeroRowOutputError) {
  ASSERT_THAT(quirks()->DropTable(&connection, "adbc_test", &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  {
    ASSERT_THAT(AdbcStatementSetSqlQuery(
                    &statement, "CREATE TABLE adbc_test (id int primary key, data jsonb)",
                    &error),
                IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
  }

  {
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement, "insert into adbc_test (id, data) values (1, null)", &error),
        IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
  }

  {
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement, "insert into adbc_test (id, data) values (2, '1')", &error),
        IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
  }

  {
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(&statement,
                                 "SELECT id, data from adbc_test JOIN "
                                 "jsonb_array_elements(adbc_test.data) AS foo ON true",
                                 &error),
        IsOkStatus(&error));
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus());
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_EQ(reader.MaybeNext(), EINVAL);

    AdbcStatusCode status = ADBC_STATUS_OK;
    const struct AdbcError* detail =
        AdbcErrorFromArrayStream(&reader.stream.value, &status);
    ASSERT_NE(nullptr, detail);
    ASSERT_EQ(ADBC_STATUS_INVALID_ARGUMENT, status);
    ASSERT_EQ("22023", std::string_view(detail->sqlstate, 5));
  }
}

TEST_F(PostgresStatementTest, BatchSizeHint) {
  ASSERT_THAT(quirks()->EnsureSampleTable(&connection, "batch_size_hint_test", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  // Setting the batch size hint to a negative or non-integer value should fail
  ASSERT_EQ(AdbcStatementSetOption(&statement, "adbc.postgresql.batch_size_hint_bytes",
                                   "-1", nullptr),
            ADBC_STATUS_INVALID_ARGUMENT);
  ASSERT_EQ(AdbcStatementSetOption(&statement, "adbc.postgresql.batch_size_hint_bytes",
                                   "not a valid number", nullptr),
            ADBC_STATUS_INVALID_ARGUMENT);

  // For this test, use a batch size of 1 byte to force every row to be its own batch
  ASSERT_THAT(AdbcStatementSetOption(&statement, "adbc.postgresql.batch_size_hint_bytes",
                                     "1", &error),
              IsOkStatus(&error));

  {
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement, "SELECT int64s from batch_size_hint_test ORDER BY int64s LIMIT 3",
            &error),
        IsOkStatus(&error));

    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->length, 1);
    ASSERT_EQ(ArrowArrayViewGetIntUnsafe(reader.array_view->children[0], 0), -42);
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->length, 1);
    ASSERT_EQ(ArrowArrayViewGetIntUnsafe(reader.array_view->children[0], 0), 42);
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->length, 1);
    ASSERT_TRUE(ArrowArrayViewIsNull(reader.array_view->children[0], 0));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }
}

// Test that an ADBC 1.0.0-sized error still works
TEST_F(PostgresStatementTest, AdbcErrorBackwardsCompatibility) {
  struct AdbcError error;
  std::memset(&error, 0, ADBC_ERROR_1_1_0_SIZE);
  struct AdbcDriver canary;
  error.private_data = &canary;
  error.private_driver = &canary;

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "SELECT * FROM thistabledoesnotexist", &error),
      IsOkStatus(&error));
  adbc_validation::StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsStatus(ADBC_STATUS_NOT_FOUND, &error));
  ASSERT_EQ("42P01", std::string_view(error.sqlstate, 5));
  ASSERT_EQ(0, AdbcErrorGetDetailCount(&error));
  ASSERT_EQ(&canary, error.private_data);
  ASSERT_EQ(&canary, error.private_driver);
  error.release(&error);
}

TEST_F(PostgresStatementTest, Cancel) {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  const char* query = R"(DROP TABLE IF EXISTS test_cancel;
            CREATE TABLE test_cancel (ints INT);
            INSERT INTO test_cancel (ints)
            SELECT g :: INT FROM GENERATE_SERIES(1, 65536) temp(g);)";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT * FROM test_cancel", &error),
              IsOkStatus(&error));
  adbc_validation::StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementCancel(&statement, &error), IsOkStatus(&error));

  int retcode = 0;
  while (true) {
    retcode = reader.MaybeNext();
    if (retcode != 0 || !reader.array->release) break;
  }

  ASSERT_EQ(ECANCELED, retcode);
  AdbcStatusCode status = ADBC_STATUS_OK;
  const struct AdbcError* detail =
      AdbcErrorFromArrayStream(&reader.stream.value, &status);
  ASSERT_NE(nullptr, detail);
  ASSERT_EQ(ADBC_STATUS_CANCELLED, status);
  ASSERT_EQ("57014", std::string_view(detail->sqlstate, 5));
  ASSERT_NE(0, AdbcErrorGetDetailCount(detail));
}

TEST_F(PostgresStatementTest, MultipleStatementsSingleQuery) {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  const char* query = R"(DROP TABLE IF EXISTS test_query_statements;
            CREATE TABLE test_query_statements (ints INT);
            INSERT INTO test_query_statements VALUES((1));
            INSERT INTO test_query_statements VALUES((2));
            INSERT INTO test_query_statements VALUES((3));)";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "SELECT * FROM test_query_statements", &error),
      IsOkStatus(&error));

  adbc_validation::StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));
  reader.GetSchema();
  ASSERT_THAT(reader.MaybeNext(), adbc_validation::IsOkErrno());
  ASSERT_EQ(reader.array->length, 3);
}

TEST_F(PostgresStatementTest, SetUseCopyFalse) {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  const char* query = R"(DROP TABLE IF EXISTS test_query_set_copy_false;
            CREATE TABLE test_query_set_copy_false (ints INT);
            INSERT INTO test_query_set_copy_false VALUES((1));
            INSERT INTO test_query_set_copy_false VALUES((NULL));
            INSERT INTO test_query_set_copy_false VALUES((3));)";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  // Check option setting/getting
  ASSERT_EQ(
      adbc_validation::StatementGetOption(&statement, "adbc.postgresql.use_copy", &error),
      "true");

  ASSERT_THAT(AdbcStatementSetOption(&statement, "adbc.postgresql.use_copy",
                                     "not true or false", &error),
              IsStatus(ADBC_STATUS_INVALID_ARGUMENT));

  ASSERT_THAT(AdbcStatementSetOption(&statement, "adbc.postgresql.use_copy",
                                     ADBC_OPTION_VALUE_ENABLED, &error),
              IsOkStatus(&error));
  ASSERT_EQ(
      adbc_validation::StatementGetOption(&statement, "adbc.postgresql.use_copy", &error),
      "true");

  ASSERT_THAT(AdbcStatementSetOption(&statement, "adbc.postgresql.use_copy",
                                     ADBC_OPTION_VALUE_DISABLED, &error),
              IsOkStatus(&error));
  ASSERT_EQ(
      adbc_validation::StatementGetOption(&statement, "adbc.postgresql.use_copy", &error),
      "false");

  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement,
                                       "SELECT * FROM test_query_set_copy_false", &error),
              IsOkStatus(&error));

  adbc_validation::StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));

  ASSERT_EQ(reader.rows_affected, 3);

  reader.GetSchema();
  ASSERT_EQ(reader.schema->n_children, 1);
  ASSERT_STREQ(reader.schema->children[0]->format, "i");
  ASSERT_STREQ(reader.schema->children[0]->name, "ints");

  ASSERT_THAT(reader.MaybeNext(), adbc_validation::IsOkErrno());
  ASSERT_EQ(reader.array->length, 3);
  ASSERT_EQ(reader.array->n_children, 1);
  ASSERT_EQ(reader.array->children[0]->null_count, 1);

  ASSERT_THAT(reader.MaybeNext(), adbc_validation::IsOkErrno());
  ASSERT_EQ(reader.array->release, nullptr);
}

TEST_F(PostgresStatementTest, SqlQueryInt2vector) {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  const char* query = R"(SELECT CAST('-1 42 0' AS int2vector) AS thevector;)";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query, &error), IsOkStatus(&error));

  adbc_validation::StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));

  reader.GetSchema();
  ASSERT_EQ(reader.schema->n_children, 1);
  ASSERT_STREQ(reader.schema->children[0]->format, "+l");
  ASSERT_STREQ(reader.schema->children[0]->name, "thevector");
  ASSERT_EQ(reader.schema->children[0]->n_children, 1);
  ASSERT_STREQ(reader.schema->children[0]->children[0]->format, "s");

  ASSERT_THAT(reader.MaybeNext(), adbc_validation::IsOkErrno());
  ASSERT_EQ(reader.array->length, 1);
  ASSERT_EQ(reader.array->n_children, 1);
  ASSERT_EQ(reader.array->children[0]->null_count, 0);
  const auto* offsets =
      reinterpret_cast<const int32_t*>(reader.array->children[0]->buffers[1]);
  ASSERT_EQ(offsets[0], 0);
  ASSERT_EQ(offsets[1], 3);

  ASSERT_EQ(reader.array->children[0]->children[0]->null_count, 0);
  ASSERT_EQ(reader.array->children[0]->children[0]->length, 3);
  const auto* data = reinterpret_cast<const int16_t*>(
      reader.array->children[0]->children[0]->buffers[1]);
  ASSERT_EQ(data[0], -1);
  ASSERT_EQ(data[1], 42);
  ASSERT_EQ(data[2], 0);

  ASSERT_THAT(reader.MaybeNext(), adbc_validation::IsOkErrno());
  ASSERT_EQ(reader.array->release, nullptr);
}

TEST_F(PostgresStatementTest, UnknownOid) {
  // Regression test for https://github.com/apache/arrow-adbc/issues/2448
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(
                  &statement, "SELECT typacl FROM pg_type WHERE oid <= 6157", &error),
              IsOkStatus(&error));
  adbc_validation::StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_EQ(1, reader.fields.size());
  ASSERT_EQ(NANOARROW_TYPE_BINARY, reader.fields[0].type);
  struct ArrowStringView extension_name = reader.fields[0].extension_name;
  ASSERT_EQ("arrow.opaque",
            std::string_view(extension_name.data,
                             static_cast<size_t>(extension_name.size_bytes)));
  struct ArrowStringView extension_metadata = reader.fields[0].extension_metadata;
  ASSERT_EQ(R"({"type_name": "unnamed<oid:1034>", "vendor_name": "PostgreSQL"})",
            std::string_view(extension_metadata.data,
                             static_cast<size_t>(extension_metadata.size_bytes)));
}

TEST_F(PostgresStatementTest, SqlQueryJsonb) {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  // Setup table
  ASSERT_THAT(AdbcStatementSetSqlQuery(
                  &statement,
                  "DROP TABLE IF EXISTS jsonbtest; CREATE TABLE jsonbtest (value JSONB);",
                  &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  // Insert data
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> batch;
  ArrowSchemaInit(&schema.value);
  ASSERT_THAT(ArrowSchemaSetTypeStruct(&schema.value, 1), adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowSchemaSetType(schema->children[0], NANOARROW_TYPE_STRING),
              adbc_validation::IsOkErrno());
  ASSERT_THAT(ArrowSchemaSetName(schema->children[0], "value"),
              adbc_validation::IsOkErrno());

  // We need the extension type for the driver to bind data properly
  nanoarrow::UniqueBuffer buffer;
  ASSERT_THAT(ArrowMetadataBuilderInit(buffer.get(), nullptr),
              adbc_validation::IsOkErrno());
  ASSERT_THAT(
      ArrowMetadataBuilderAppend(buffer.get(), ArrowCharView("ARROW:extension:name"),
                                 ArrowCharView("arrow.json")),
      adbc_validation::IsOkErrno());
  ASSERT_THAT(
      ArrowSchemaSetMetadata(schema->children[0], reinterpret_cast<char*>(buffer->data)),
      adbc_validation::IsOkErrno());

  ASSERT_THAT((adbc_validation::MakeBatch<std::string>(
                  &schema.value, &batch.value, static_cast<struct ArrowError*>(nullptr),
                  {R"({"a": 1, "b": [1, 2, 3]})", std::nullopt})),
              adbc_validation::IsOkErrno());

  ASSERT_THAT(AdbcStatementSetSqlQuery(
                  &statement, "INSERT INTO jsonbtest(value) VALUES ($1)", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, &batch.value, &schema.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  // Check round-trip
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT * FROM jsonbtest", &error),
              IsOkStatus(&error));
  adbc_validation::StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));

  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_EQ(1, reader.fields.size());
  ASSERT_EQ(NANOARROW_TYPE_STRING, reader.fields[0].type);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ArrowStringView view = ArrowArrayViewGetStringUnsafe(reader.array_view->children[0], 0);
  std::string_view v(view.data, static_cast<size_t>(view.size_bytes));
  ASSERT_EQ(R"({"a": 1, "b": [1, 2, 3]})", v);
}

struct TypeTestCase {
  std::string name;
  std::string sql_type;
  std::string sql_literal;
  ArrowType arrow_type;
  std::variant<bool, int64_t, double, std::string, ArrowInterval> scalar;

  static std::string FormatName(const ::testing::TestParamInfo<TypeTestCase>& info) {
    return info.param.name;
  }
};

ArrowInterval MonthDayNano(int32_t months, int32_t days, int64_t nanos) {
  return {NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO, months, days, 0, nanos};
}

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
  std::string value = GetParam().sql_literal;
  if ((value == "'-inf'") || (value == "'inf'")) {
    const std::string version = adbc_validation::GetDriverVendorVersion(&connection_);
    if (version < "140000") {
      GTEST_SKIP() << "-inf and inf not implemented until postgres 14";
    }
  }
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
      &reader.schema.value, {{"", GetParam().arrow_type, true}}));
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
          std::string_view v(view.data, static_cast<size_t>(view.size_bytes));
          ASSERT_EQ(arg, v);
        } else if constexpr (std::is_same_v<T, ArrowInterval>) {
          ArrowInterval interval;
          std::memset(&interval, 0, sizeof(interval));
          ArrowArrayViewGetIntervalUnsafe(reader.array_view->children[0], 0, &interval);
          // The getter doesn't set this.
          // EXPECT_EQ(arg.type, interval.type);
          EXPECT_EQ(arg.months, interval.months);
          EXPECT_EQ(arg.days, interval.days);
          EXPECT_EQ(arg.ms, interval.ms);
          EXPECT_EQ(arg.ns, interval.ns);
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
    {"BYTEA", "BYTEA", R"('\000\001\002\003\004\005\006\007'::bytea)"s,
     NANOARROW_TYPE_BINARY, std::string("\x00\x01\x02\x03\x04\x05\x06\x07", 8)},
    {"TEXT", "TEXT", "'foobar'", NANOARROW_TYPE_STRING, "foobar"s},
    {"CHAR6_1", "CHAR(6)", "'foo'", NANOARROW_TYPE_STRING, "foo   "s},
    {"CHAR6_2", "CHAR(6)", "'foobar'", NANOARROW_TYPE_STRING, "foobar"s},
    {"VARCHAR", "VARCHAR", "'foobar'", NANOARROW_TYPE_STRING, "foobar"s},
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
static std::initializer_list<TypeTestCase> kNumericTypeCases = {
    {"NUMERIC_TRAILING0", "NUMERIC", "1000000", NANOARROW_TYPE_STRING, "1000000"s},
    {"NUMERIC_LEADING0", "NUMERIC", "0.00001234", NANOARROW_TYPE_STRING, "0.00001234"s},
    {"NUMERIC_TRAILING02", "NUMERIC", "'1.0000'", NANOARROW_TYPE_STRING, "1.0000"s},
    {"NUMERIC_NEGATIVE", "NUMERIC", "-123.456", NANOARROW_TYPE_STRING, "-123.456"s},
    {"NUMERIC_POSITIVE", "NUMERIC", "123.456", NANOARROW_TYPE_STRING, "123.456"s},
    {"NUMERIC_NAN", "NUMERIC", "'nan'", NANOARROW_TYPE_STRING, "nan"s},
    {"NUMERIC_NINF", "NUMERIC", "'-inf'", NANOARROW_TYPE_STRING, "-inf"s},
    {"NUMERIC_PINF", "NUMERIC", "'inf'", NANOARROW_TYPE_STRING, "inf"s},
    {"MONEY", "MONEY", "12.34", NANOARROW_TYPE_INT64, int64_t(1234)},
};
static std::initializer_list<TypeTestCase> kDateTypeCases = {
    {"DATE0", "DATE", "'1970-01-01'", NANOARROW_TYPE_DATE32, int64_t(0)},
    {"DATE1", "DATE", "'2000-01-01'", NANOARROW_TYPE_DATE32, int64_t(10957)},
    {"DATE2", "DATE", "'1950-01-01'", NANOARROW_TYPE_DATE32, int64_t(-7305)},
};
static std::initializer_list<TypeTestCase> kIntervalTypeCases = {
    {
        "INTERVAL",
        "INTERVAL",
        "'P-1Y2M42DT1H1M1S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(-10, 42, (1L * 60 * 60 + 60L + 1L) * 1'000'000'000),
    },
    {
        "INTERVAL2",
        "INTERVAL",
        "'P0Y0M0DT0H0M0.1S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, 100L * 1'000'000),
    },
    {
        "INTERVAL3",
        "INTERVAL",
        "'P0Y0M0DT0H0M0.01S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, 10L * 1'000'000),
    },
    {
        "INTERVAL4",
        "INTERVAL",
        "'P0Y0M0DT0H0M0.001S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, 1L * 1'000'000),
    },
    {
        "INTERVAL5",
        "INTERVAL",
        "'P0Y0M0DT0H0M0.0001S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, 100'000L),
    },
    {
        "INTERVAL6",
        "INTERVAL",
        "'P0Y0M0DT0H0M0.00001S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, 10'000L),
    },
    {
        "INTERVAL7",
        "INTERVAL",
        "'P0Y0M0DT0H0M0.000001S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, 1'000L),
    },
    {
        "INTERVAL_YEAR",
        "INTERVAL YEAR",
        "'16Y'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(16 * 12, 0, 0),
    },
    {
        "INTERVAL_MONTH",
        "INTERVAL MONTH",
        "'P0Y-2M0D'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(-2, 0, 0),
    },
    {
        "INTERVAL_DAY",
        "INTERVAL DAY",
        "'-102D'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, -102, 0),
    },
    {
        "INTERVAL_HOUR",
        "INTERVAL HOUR",
        "'12H'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, 12L * 60 * 60 * 1'000'000'000),
    },
    {
        "INTERVAL_MINUTE",
        "INTERVAL MINUTE",
        "'P0Y0M0DT0H-5M0S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, -5L * 60 * 1'000'000'000),
    },
    {
        "INTERVAL_SECOND",
        "INTERVAL SECOND",
        "'P0Y0M0DT0H0M42S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, 42L * 1'000'000'000),
    },
    {
        "INTERVAL_YEAR_TO_MONTH",
        "INTERVAL YEAR TO MONTH",
        "'P1Y1M0D'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(13, 0, 0),
    },
    {
        "INTERVAL_DAY_TO_HOUR",
        "INTERVAL DAY TO HOUR",
        "'P0Y0M1DT-2H0M0S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 1, -2L * 60 * 60 * 1'000'000'000),
    },
    {
        "INTERVAL_DAY_TO_MINUTE",
        "INTERVAL DAY TO MINUTE",
        "'P0Y0M1DT-2H1M0S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 1, (-2L * 60 + 1L) * 60 * 1'000'000'000),
    },
    {
        "INTERVAL_DAY_TO_SECOND",
        "INTERVAL DAY TO SECOND",
        "'P0Y0M1DT-2H1M-1S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 1, ((-2L * 60 + 1L) * 60 - 1L) * 1'000'000'000),
    },
    {
        "INTERVAL_HOUR_TO_MINUTE",
        "INTERVAL HOUR TO MINUTE",
        "'P0Y0M0DT-2H1M0S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, (-2L * 60 + 1L) * 60 * 1'000'000'000),
    },
    {
        "INTERVAL_HOUR_TO_SECOND",
        "INTERVAL HOUR TO SECOND",
        "'P0Y0M0DT-2H1M-1S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, ((-2L * 60 + 1L) * 60 - 1L) * 1'000'000'000),
    },
    {
        "INTERVAL_MINUTE_TO_SECOND",
        "INTERVAL MINUTE TO SECOND",
        "'P0Y0M0DT0H1M-1S'",
        NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO,
        MonthDayNano(0, 0, 59L * 1'000'000'000),
    },
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
INSTANTIATE_TEST_SUITE_P(NumericType, PostgresTypeTest,
                         testing::ValuesIn(kNumericTypeCases), TypeTestCase::FormatName);
INSTANTIATE_TEST_SUITE_P(DateTypes, PostgresTypeTest, testing::ValuesIn(kDateTypeCases),
                         TypeTestCase::FormatName);
INSTANTIATE_TEST_SUITE_P(IntervalTypes, PostgresTypeTest,
                         testing::ValuesIn(kIntervalTypeCases), TypeTestCase::FormatName);
INSTANTIATE_TEST_SUITE_P(TimeTypes, PostgresTypeTest, testing::ValuesIn(kTimeTypeCases),
                         TypeTestCase::FormatName);
INSTANTIATE_TEST_SUITE_P(TimestampTypes, PostgresTypeTest,
                         testing::ValuesIn(kTimestampTypeCases),
                         TypeTestCase::FormatName);

struct DecimalTestCase {
  const enum ArrowType type;
  const int32_t precision;
  const int32_t scale;
  const std::vector<std::array<uint64_t, 4>> data;
  const std::vector<std::optional<std::string>> expected;
};

class PostgresDecimalTest : public ::testing::TestWithParam<DecimalTestCase> {
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

    ASSERT_THAT(quirks_.DropTable(&connection_, "bulk_ingest", &error_),
                IsOkStatus(&error_));
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

TEST_P(PostgresDecimalTest, SelectValue) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  const enum ArrowType type = GetParam().type;
  const int32_t precision = GetParam().precision;
  const int32_t scale = GetParam().scale;
  const auto data = GetParam().data;
  const auto expected = GetParam().expected;
  const size_t nrecords = expected.size();

  int32_t bitwidth;
  switch (type) {
    case NANOARROW_TYPE_DECIMAL128:
      bitwidth = 128;
      break;
    case NANOARROW_TYPE_DECIMAL256:
      bitwidth = 256;
      break;
    default:
      FAIL();
  }

  // this is a bit of a hack to make std::vector play nicely with
  // a dynamic number of stack-allocated ArrowDecimal objects
  constexpr size_t max_decimals = 10;
  struct ArrowDecimal decimals[max_decimals];
  if (nrecords > max_decimals) {
    FAIL() << " max_decimals exceeded for test case - please change parametrization";
  }

  std::vector<std::optional<ArrowDecimal*>> values;
  for (size_t i = 0; i < nrecords; i++) {
    ArrowDecimalInit(&decimals[i], bitwidth, precision, scale);
    uint8_t buf[32];
    const auto record = data[i];
    memcpy(buf, record.data(), sizeof(buf));
    ArrowDecimalSetBytes(&decimals[i], buf);
    values.push_back(&decimals[i]);
  }

  auto expected_with_null{expected};
  expected_with_null.insert(expected_with_null.begin(), std::nullopt);
  values.push_back(std::nullopt);

  ArrowSchemaInit(&schema.value);
  ASSERT_EQ(ArrowSchemaSetTypeStruct(&schema.value, 1), 0);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema.value.children[0], type, precision, scale),
            0);
  ASSERT_EQ(ArrowSchemaSetName(schema.value.children[0], "col"), 0);

  ASSERT_THAT(adbc_validation::MakeBatch<ArrowDecimal*>(&schema.value, &array.value,
                                                        &na_error, values),
              adbc_validation::IsOkErrno());

  ASSERT_THAT(AdbcStatementSetOption(&statement_, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     "bulk_ingest", &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(AdbcStatementBind(&statement_, &array.value, &schema.value, &error_),
              IsOkStatus(&error_));

  int64_t rows_affected = 0;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement_, nullptr, &rows_affected, &error_),
              IsOkStatus(&error_));
  ASSERT_THAT(rows_affected,
              ::testing::AnyOf(::testing::Eq(values.size()), ::testing::Eq(-1)));

  ASSERT_THAT(AdbcStatementSetSqlQuery(
                  &statement_,
                  "SELECT * FROM bulk_ingest ORDER BY \"col\" ASC NULLS FIRST", &error_),
              IsOkStatus(&error_));

  {
    adbc_validation::StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement_, &reader.stream.value,
                                          &reader.rows_affected, &error_),
                IsOkStatus(&error_));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(values.size()), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ArrowType round_trip_type = quirks_.IngestSelectRoundTripType(type);
    ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareSchema(
        &reader.schema.value, {{"col", round_trip_type, true}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(values.size(), reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_NO_FATAL_FAILURE(adbc_validation::CompareArray<std::string>(
        reader.array_view->children[0], expected_with_null));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }
}

static std::vector<std::array<uint64_t, 4>> kDecimalData = {
    // -12345600000
    {18446744061363951616ULL, 18446744073709551615ULL, 0, 0},
    // 1234
    {1234ULL, 0, 0, 0},
    // 100000000
    {100000000ULL, 0, 0, 0},
    // 12345600000
    {12345600000ULL, 0, 0, 0},
    // 100000000000000
    {100000000000000ULL, 0, 0, 0},
    // 2342394230592232349023094
    {8221368519775271798ULL, 126981ULL, 0, 0},
};

static std::vector<std::array<uint64_t, 4>> kDecimal256Data = {
    // 1234567890123456789012345678901234567890123456789012345678901234567890123456
    {17877984925544397504ULL, 5352188884907840935ULL, 234631617561833724ULL,
     196678011949953713ULL},
    // -1234567890123456789012345678901234567890123456789012345678901234567890123456
    {568759148165154112ULL, 13094555188801710680ULL, 18212112456147717891ULL,
     18250066061759597902ULL},
};

static std::initializer_list<DecimalTestCase> kDecimal128Cases = {
    {NANOARROW_TYPE_DECIMAL128,
     38,
     8,
     kDecimalData,
     {"-123.456", "0.00001234", "1", "123.456", "1000000",
      "23423942305922323.49023094"}}};

static std::initializer_list<DecimalTestCase> kDecimal128NoScaleCases = {
    {NANOARROW_TYPE_DECIMAL128,
     38,
     0,
     kDecimalData,
     {"-12345600000", "1234", "100000000", "12345600000", "100000000000000",
      "2342394230592232349023094"}}};

static std::initializer_list<DecimalTestCase> kDecimal256Cases = {
    {NANOARROW_TYPE_DECIMAL256,
     38,
     8,
     kDecimalData,
     {"-123.456", "0.00001234", "1", "123.456", "1000000",
      "23423942305922323.49023094"}}};

static std::initializer_list<DecimalTestCase> kDecimal256NoScaleCases = {
    {NANOARROW_TYPE_DECIMAL256,
     38,
     0,
     kDecimalData,
     {"-12345600000", "1234", "100000000", "12345600000", "100000000000000",
      "2342394230592232349023094"}}};

static std::initializer_list<DecimalTestCase> kDecimal256LargeCases = {
    {NANOARROW_TYPE_DECIMAL256,
     76,
     8,
     kDecimal256Data,
     {
         "-12345678901234567890123456789012345678901234567890123456789012345678.90123456",
         "12345678901234567890123456789012345678901234567890123456789012345678.90123456",
     }}};

static std::initializer_list<DecimalTestCase> kDecimal256LargeNoScaleCases = {
    {NANOARROW_TYPE_DECIMAL256,
     76,
     0,
     kDecimal256Data,
     {
         "-1234567890123456789012345678901234567890123456789012345678901234567890123456",
         "1234567890123456789012345678901234567890123456789012345678901234567890123456",
     }}};

INSTANTIATE_TEST_SUITE_P(Decimal128Tests, PostgresDecimalTest,
                         testing::ValuesIn(kDecimal128Cases));
INSTANTIATE_TEST_SUITE_P(Decimal128NoScale, PostgresDecimalTest,
                         testing::ValuesIn(kDecimal128NoScaleCases));
INSTANTIATE_TEST_SUITE_P(Decimal256Tests, PostgresDecimalTest,
                         testing::ValuesIn(kDecimal128Cases));
INSTANTIATE_TEST_SUITE_P(Decimal256NoScale, PostgresDecimalTest,
                         testing::ValuesIn(kDecimal128NoScaleCases));
INSTANTIATE_TEST_SUITE_P(Decimal256LargeTests, PostgresDecimalTest,
                         testing::ValuesIn(kDecimal256LargeCases));
INSTANTIATE_TEST_SUITE_P(Decimal256LargeNoScale, PostgresDecimalTest,
                         testing::ValuesIn(kDecimal256LargeNoScaleCases));
