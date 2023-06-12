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

  std::string BindParameter(int index) const override {
    return "$" + std::to_string(index + 1);
  }

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

  void TestMetadataGetObjectsTablesTypes() { GTEST_SKIP() << "Not yet implemented"; }

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

  bool seen_postgres_db = false;
  bool seen_template0_db = false;
  bool seen_tempalte1_db = false;

  do {
    for (int64_t row = 0; row < reader.array->length; row++) {
      ArrowStringView val =
          ArrowArrayViewGetStringUnsafe(reader.array_view->children[0], row);
      auto val_str = std::string(val.data, val.size_bytes);
      if (val_str == "postgres") {
        seen_postgres_db = true;
      } else if (val_str == "template0") {
        seen_template0_db = true;
      } else if (val_str == "template1") {
        seen_tempalte1_db = true;
      }
    }
    ASSERT_NO_FATAL_FAILURE(reader.Next());
  } while (reader.array->release);

  EXPECT_TRUE(seen_postgres_db) << "postgres database does not exist";
  EXPECT_TRUE(seen_template0_db) << "template0 database does not exist";
  EXPECT_TRUE(seen_tempalte1_db) << "template1 database does not exist";
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

  bool seen_public = false;

  struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
  struct ArrowArrayView* catalog_db_schema_names = catalog_db_schemas_list->children[0];

  do {
    for (int64_t catalog_idx = 0; catalog_idx < reader.array->length; catalog_idx++) {
      ArrowStringView db_name =
          ArrowArrayViewGetStringUnsafe(reader.array_view->children[0], catalog_idx);
      auto db_str = std::string(db_name.data, db_name.size_bytes);

      auto schema_list_start =
          ArrowArrayViewListChildOffset(catalog_db_schemas_list, catalog_idx);
      auto schema_list_end =
          ArrowArrayViewListChildOffset(catalog_db_schemas_list, catalog_idx + 1);

      if (db_str == "postgres") {
        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, catalog_idx));
        for (auto db_schemas_index = schema_list_start;
             db_schemas_index < schema_list_end; db_schemas_index++) {
          ArrowStringView schema_name = ArrowArrayViewGetStringUnsafe(
              catalog_db_schema_names->children[0], db_schemas_index);
          auto schema_str = std::string(schema_name.data, schema_name.size_bytes);
          if (schema_str == "public") {
            seen_public = true;
          }
        }
      } else {
        ASSERT_EQ(schema_list_start, schema_list_end);
      }
    }
    ASSERT_NO_FATAL_FAILURE(reader.Next());
  } while (reader.array->release);

  ASSERT_TRUE(seen_public) << "public schema does not exist";
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

  adbc_validation::StreamReader reader;
  ASSERT_THAT(
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_ALL, nullptr, nullptr,
                               nullptr, nullptr, nullptr, &reader.stream.value, &error),
      IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_GT(reader.array->length, 0);

  bool seen_id_column = false;
  bool seen_primary_key = false;

  struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
  struct ArrowArrayView* catalog_db_schemas_items = catalog_db_schemas_list->children[0];
  struct ArrowArrayView* db_schema_name_col = catalog_db_schemas_items->children[0];
  struct ArrowArrayView* db_schema_tables_col = catalog_db_schemas_items->children[1];

  struct ArrowArrayView* schema_table_items = db_schema_tables_col->children[0];
  struct ArrowArrayView* table_name_col = schema_table_items->children[0];
  struct ArrowArrayView* table_columns_col = schema_table_items->children[2];
  struct ArrowArrayView* table_constraints_col = schema_table_items->children[3];

  struct ArrowArrayView* table_columns_items = table_columns_col->children[0];
  struct ArrowArrayView* column_name_col = table_columns_items->children[0];

  struct ArrowArrayView* table_constraints_items = table_constraints_col->children[0];
  struct ArrowArrayView* constraint_name_col = table_constraints_items->children[0];
  struct ArrowArrayView* constraint_type_col = table_constraints_items->children[1];
  struct ArrowArrayView* constraint_column_names_col =
      table_constraints_items->children[2];

  do {
    for (int64_t catalog_idx = 0; catalog_idx < reader.array->length; catalog_idx++) {
      ArrowStringView db_name =
          ArrowArrayViewGetStringUnsafe(reader.array_view->children[0], catalog_idx);
      auto db_str = std::string(db_name.data, db_name.size_bytes);

      auto schema_list_start =
          ArrowArrayViewListChildOffset(catalog_db_schemas_list, catalog_idx);
      auto schema_list_end =
          ArrowArrayViewListChildOffset(catalog_db_schemas_list, catalog_idx + 1);

      if (db_str == "postgres") {
        for (auto db_schemas_index = schema_list_start;
             db_schemas_index < schema_list_end; db_schemas_index++) {
          ArrowStringView schema_name =
              ArrowArrayViewGetStringUnsafe(db_schema_name_col, db_schemas_index);
          auto schema_str = std::string(schema_name.data, schema_name.size_bytes);
          if (schema_str == "public") {
            for (auto tables_index = ArrowArrayViewListChildOffset(db_schema_tables_col,
                                                                   db_schemas_index);
                 tables_index < ArrowArrayViewListChildOffset(db_schema_tables_col,
                                                              db_schemas_index + 1);
                 tables_index++) {
              ArrowStringView table_name =
                  ArrowArrayViewGetStringUnsafe(table_name_col, tables_index);
              auto table_str = std::string(table_name.data, table_name.size_bytes);
              if (table_str == "adbc_pkey_test") {
                for (auto columns_index =
                         ArrowArrayViewListChildOffset(table_columns_col, tables_index);
                     columns_index <
                     ArrowArrayViewListChildOffset(table_columns_col, tables_index + 1);
                     columns_index++) {
                  ArrowStringView column_name =
                      ArrowArrayViewGetStringUnsafe(column_name_col, columns_index);
                  auto column_str = std::string(column_name.data, column_name.size_bytes);
                  if (column_str == "id") {
                    seen_id_column = true;
                  }
                }
              }
              for (auto constraints_index =
                       ArrowArrayViewListChildOffset(table_constraints_col, tables_index);
                   constraints_index <
                   ArrowArrayViewListChildOffset(table_constraints_col, tables_index + 1);
                   constraints_index++) {
                ArrowStringView constraint_name =
                    ArrowArrayViewGetStringUnsafe(constraint_name_col, constraints_index);
                auto constraint_name_str =
                    std::string(constraint_name.data, constraint_name.size_bytes);
                ArrowStringView constraint_type =
                    ArrowArrayViewGetStringUnsafe(constraint_type_col, constraints_index);
                auto constraint_type_str =
                    std::string(constraint_type.data, constraint_type.size_bytes);

                for (auto constraint_names_index = ArrowArrayViewListChildOffset(
                         constraint_column_names_col, constraints_index);
                     constraint_names_index <
                     ArrowArrayViewListChildOffset(constraint_column_names_col,
                                                   constraints_index + 1);
                     constraint_names_index++) {
                  ArrowStringView constraint_column_name = ArrowArrayViewGetStringUnsafe(
                      constraint_column_names_col->children[0], constraint_names_index);
                  auto constraint_column_name_str = std::string(
                      constraint_column_name.data, constraint_column_name.size_bytes);
                  if ((constraint_column_name_str == "id") &
                      (constraint_name_str == "adbc_pkey_test_pkey") &
                      (constraint_type_str == "PRIMARY KEY")) {
                    seen_primary_key = true;
                  }
                }
              }
            }
          }
        }
      }
    }
    ASSERT_NO_FATAL_FAILURE(reader.Next());
  } while (reader.array->release);

  ASSERT_TRUE(seen_id_column) << "could not find column 'id' on table adbc_pkey_test";
  ASSERT_TRUE(seen_primary_key) << "could not find primary key for adbc_pkey_test";
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

  adbc_validation::StreamReader reader;
  ASSERT_THAT(
      AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_ALL, nullptr, nullptr,
                               nullptr, nullptr, nullptr, &reader.stream.value, &error),
      IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_GT(reader.array->length, 0);

  bool seen_fid1 = false;
  bool seen_fid2 = false;

  struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
  struct ArrowArrayView* catalog_db_schemas_items = catalog_db_schemas_list->children[0];
  struct ArrowArrayView* db_schema_name_col = catalog_db_schemas_items->children[0];
  struct ArrowArrayView* db_schema_tables_col = catalog_db_schemas_items->children[1];

  struct ArrowArrayView* schema_table_items = db_schema_tables_col->children[0];
  struct ArrowArrayView* table_name_col = schema_table_items->children[0];
  // struct ArrowArrayView* table_columns_col = schema_table_items->children[2];
  struct ArrowArrayView* table_constraints_col = schema_table_items->children[3];

  // struct ArrowArrayView* table_columns_items = table_columns_col->children[0];
  // struct ArrowArrayView* column_name_col = table_columns_items->children[0];

  struct ArrowArrayView* table_constraints_items = table_constraints_col->children[0];
  struct ArrowArrayView* constraint_name_col = table_constraints_items->children[0];
  struct ArrowArrayView* constraint_type_col = table_constraints_items->children[1];
  // struct ArrowArrayView* constraint_column_names_col =
  //     table_constraints_items->children[2];

  struct ArrowArrayView* constraint_column_usages_col =
      table_constraints_items->children[3];
  struct ArrowArrayView* constraint_column_usage_items =
      constraint_column_usages_col->children[0];
  struct ArrowArrayView* fk_catalog_col = constraint_column_usage_items->children[0];
  struct ArrowArrayView* fk_db_schema_col = constraint_column_usage_items->children[1];
  struct ArrowArrayView* fk_table_col = constraint_column_usage_items->children[2];
  struct ArrowArrayView* fk_column_name_col = constraint_column_usage_items->children[3];

  do {
    for (int64_t catalog_idx = 0; catalog_idx < reader.array->length; catalog_idx++) {
      ArrowStringView db_name =
          ArrowArrayViewGetStringUnsafe(reader.array_view->children[0], catalog_idx);
      auto db_str = std::string(db_name.data, db_name.size_bytes);

      auto schema_list_start =
          ArrowArrayViewListChildOffset(catalog_db_schemas_list, catalog_idx);
      auto schema_list_end =
          ArrowArrayViewListChildOffset(catalog_db_schemas_list, catalog_idx + 1);

      if (db_str == "postgres") {
        for (auto db_schemas_index = schema_list_start;
             db_schemas_index < schema_list_end; db_schemas_index++) {
          ArrowStringView schema_name =
              ArrowArrayViewGetStringUnsafe(db_schema_name_col, db_schemas_index);
          auto schema_str = std::string(schema_name.data, schema_name.size_bytes);
          if (schema_str == "public") {
            for (auto tables_index = ArrowArrayViewListChildOffset(db_schema_tables_col,
                                                                   db_schemas_index);
                 tables_index < ArrowArrayViewListChildOffset(db_schema_tables_col,
                                                              db_schemas_index + 1);
                 tables_index++) {
              ArrowStringView table_name =
                  ArrowArrayViewGetStringUnsafe(table_name_col, tables_index);
              auto table_str = std::string(table_name.data, table_name.size_bytes);
              if (table_str == "adbc_fkey_test") {
                for (auto constraints_index = ArrowArrayViewListChildOffset(
                         table_constraints_col, tables_index);
                     constraints_index < ArrowArrayViewListChildOffset(
                                             table_constraints_col, tables_index + 1);
                     constraints_index++) {
                  ArrowStringView constraint_name = ArrowArrayViewGetStringUnsafe(
                      constraint_name_col, constraints_index);
                  auto constraint_name_str =
                      std::string(constraint_name.data, constraint_name.size_bytes);
                  ArrowStringView constraint_type = ArrowArrayViewGetStringUnsafe(
                      constraint_type_col, constraints_index);
                  auto constraint_type_str =
                      std::string(constraint_type.data, constraint_type.size_bytes);

                  if (constraint_type_str == "FOREIGN KEY") {
                    for (auto usage_index = ArrowArrayViewListChildOffset(
                             constraint_column_usages_col, constraints_index);
                         usage_index <
                         ArrowArrayViewListChildOffset(constraint_column_usages_col,
                                                       constraints_index + 1);
                         usage_index++) {
                      ArrowStringView fk_catalog_name =
                          ArrowArrayViewGetStringUnsafe(fk_catalog_col, usage_index);
                      auto fk_catalog_name_str =
                          std::string(fk_catalog_name.data, fk_catalog_name.size_bytes);
                      ArrowStringView fk_schema_name =
                          ArrowArrayViewGetStringUnsafe(fk_db_schema_col, usage_index);
                      auto fk_schema_name_str =
                          std::string(fk_schema_name.data, fk_schema_name.size_bytes);
                      ArrowStringView fk_table_name =
                          ArrowArrayViewGetStringUnsafe(fk_table_col, usage_index);
                      auto fk_table_name_str =
                          std::string(fk_table_name.data, fk_table_name.size_bytes);
                      ArrowStringView fk_column_name =
                          ArrowArrayViewGetStringUnsafe(fk_column_name_col, usage_index);
                      auto fk_column_name_str =
                          std::string(fk_column_name.data, fk_column_name.size_bytes);
                      if ((fk_catalog_name_str == "postgres") &&
                          (fk_schema_name_str == "public") &&
                          (fk_table_name_str == "adbc_fkey_test_base")) {
                        // TODO: the current implementation makes it so the length of
                        // constraint_column_names is not the same as the length of
                        // constraint_column_usage as the latter applies only to foreign
                        // keys; should these be the same? If so can pairwise iterate
                        // and check source column -> foreign table column mapping
                        if (fk_column_name_str == "id1") {
                          seen_fid1 = true;
                        } else if (fk_column_name_str == "id2") {
                          seen_fid2 = true;
                        }
                      }
                    }
                  }
                }
              }
            }
          }
        }
      }
    }
    ASSERT_NO_FATAL_FAILURE(reader.Next());
  } while (reader.array->release);

  ASSERT_TRUE(seen_fid1)
      << "could not find foreign key relationship for 'fid1' on adbc_fkey_test";
  ASSERT_TRUE(seen_fid2)
      << "could not find foreign key relationship for 'fid2' on adbc_fkey_test";
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
  void TestSqlIngestInt16() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestInt32() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestUInt8() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestUInt16() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestUInt32() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestUInt64() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestFloat32() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestFloat64() { GTEST_SKIP() << "Not implemented"; }
  void TestSqlIngestBinary() { GTEST_SKIP() << "Not implemented"; }

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
INSTANTIATE_TEST_SUITE_P(TimestampTypes, PostgresTypeTest,
                         testing::ValuesIn(kTimestampTypeCases),
                         TypeTestCase::FormatName);
