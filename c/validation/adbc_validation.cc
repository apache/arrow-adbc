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

#include "adbc_validation.h"

#include <algorithm>
#include <cerrno>
#include <cstring>
#include <limits>
#include <optional>
#include <string>
#include <string_view>
#include <tuple>
#include <utility>
#include <vector>

#include <adbc.h>
#include <gmock/gmock.h>
#include <gtest/gtest-matchers.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>

#include "adbc_validation_util.h"

namespace adbc_validation {

namespace {
/// Nanoarrow helpers

#define NULLABLE true
#define NOT_NULL false

/// Assertion helpers

#define CHECK_OK(EXPR)                                              \
  do {                                                              \
    if (auto adbc_status = (EXPR); adbc_status != ADBC_STATUS_OK) { \
      return adbc_status;                                           \
    }                                                               \
  } while (false)

/// case insensitive string compare
bool iequals(std::string_view s1, std::string_view s2) {
  return std::equal(s1.begin(), s1.end(), s2.begin(), s2.end(),
                    [](unsigned char a, unsigned char b) {
                      return std::tolower(a) == std::tolower(b);
                    });
}

}  // namespace

//------------------------------------------------------------
// DriverQuirks

AdbcStatusCode DoIngestSampleTable(struct AdbcConnection* connection,
                                   const std::string& name, struct AdbcError* error) {
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  CHECK_OK(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64},
                                      {"strings", NANOARROW_TYPE_STRING}}));
  CHECK_OK((MakeBatch<int64_t, std::string>(&schema.value, &array.value, &na_error,
                                            {42, -42, std::nullopt},
                                            {"foo", std::nullopt, ""})));

  Handle<struct AdbcStatement> statement;
  CHECK_OK(AdbcStatementNew(connection, &statement.value, error));
  CHECK_OK(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                  name.c_str(), error));
  CHECK_OK(AdbcStatementBind(&statement.value, &array.value, &schema.value, error));
  CHECK_OK(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, error));
  CHECK_OK(AdbcStatementRelease(&statement.value, error));
  return ADBC_STATUS_OK;
}

void IngestSampleTable(struct AdbcConnection* connection, struct AdbcError* error) {
  ASSERT_THAT(DoIngestSampleTable(connection, "bulk_ingest", error), IsOkStatus(error));
}

AdbcStatusCode DriverQuirks::EnsureSampleTable(struct AdbcConnection* connection,
                                               const std::string& name,
                                               struct AdbcError* error) const {
  CHECK_OK(DropTable(connection, name, error));
  return CreateSampleTable(connection, name, error);
}

AdbcStatusCode DriverQuirks::CreateSampleTable(struct AdbcConnection* connection,
                                               const std::string& name,
                                               struct AdbcError* error) const {
  if (!supports_bulk_ingest()) {
    return ADBC_STATUS_NOT_IMPLEMENTED;
  }
  return DoIngestSampleTable(connection, name, error);
}

//------------------------------------------------------------
// Tests of AdbcDatabase

void DatabaseTest::SetUpTest() {
  std::memset(&error, 0, sizeof(error));
  std::memset(&database, 0, sizeof(database));
}

void DatabaseTest::TearDownTest() {
  if (database.private_data) {
    ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
  }
  if (error.release) {
    error.release(&error);
  }
}

void DatabaseTest::TestNewInit() {
  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->SetupDatabase(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database, &error), IsOkStatus(&error));
  ASSERT_NE(nullptr, database.private_data);
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
  ASSERT_EQ(nullptr, database.private_data);

  ASSERT_THAT(AdbcDatabaseRelease(&database, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));
}

void DatabaseTest::TestRelease() {
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));

  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
  ASSERT_EQ(nullptr, database.private_data);
}

//------------------------------------------------------------
// Tests of AdbcConnection

void ConnectionTest::SetUpTest() {
  std::memset(&error, 0, sizeof(error));
  std::memset(&database, 0, sizeof(database));
  std::memset(&connection, 0, sizeof(connection));

  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->SetupDatabase(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database, &error), IsOkStatus(&error));
}

void ConnectionTest::TearDownTest() {
  if (connection.private_data) {
    ASSERT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
  }
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
  if (error.release) {
    error.release(&error);
  }
}

void ConnectionTest::TestNewInit() {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
  ASSERT_EQ(NULL, connection.private_data);

  ASSERT_THAT(AdbcConnectionRelease(&connection, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));
}

void ConnectionTest::TestRelease() {
  ASSERT_THAT(AdbcConnectionRelease(&connection, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));

  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
  ASSERT_EQ(NULL, connection.private_data);

  // TODO: what should happen if we Release() with open connections?
}

void ConnectionTest::TestConcurrent() {
  struct AdbcConnection connection2;
  memset(&connection2, 0, sizeof(connection2));

  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionNew(&connection2, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection2, &database, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionRelease(&connection2, &error), IsOkStatus(&error));
}

//------------------------------------------------------------
// Tests of autocommit (without data)

void ConnectionTest::TestAutocommitDefault() {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  // Even if not supported, the driver should act as if autocommit is
  // enabled, and return INVALID_STATE if the client tries to commit
  // or rollback
  ASSERT_THAT(AdbcConnectionCommit(&connection, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));
  ASSERT_THAT(AdbcConnectionRollback(&connection, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));

  // Invalid option value
  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      "invalid", &error),
              ::testing::Not(IsOkStatus(&error)));
}

void ConnectionTest::TestAutocommitToggle() {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));
  if (!quirks()->supports_transactions()) {
    GTEST_SKIP();
  }

  // It is OK to enable autocommit when it is already enabled
  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_ENABLED, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error),
              IsOkStatus(&error));
  // It is OK to disable autocommit when it is already enabled
  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error),
              IsOkStatus(&error));
}

//------------------------------------------------------------
// Tests of metadata

void ConnectionTest::TestMetadataGetInfo() {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  if (!quirks()->supports_get_sql_info()) {
    GTEST_SKIP();
  }

  StreamReader reader;
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
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      &reader.schema.value, {
                                {"info_name", NANOARROW_TYPE_UINT32, NOT_NULL},
                                {"info_value", NANOARROW_TYPE_DENSE_UNION, NULLABLE},
                            }));
  ASSERT_NO_FATAL_FAILURE(
      CompareSchema(reader.schema->children[1],
                    {
                        {"string_value", NANOARROW_TYPE_STRING, NULLABLE},
                        {"bool_value", NANOARROW_TYPE_BOOL, NULLABLE},
                        {"int64_value", NANOARROW_TYPE_INT64, NULLABLE},
                        {"int32_bitmask", NANOARROW_TYPE_INT32, NULLABLE},
                        {"string_list", NANOARROW_TYPE_LIST, NULLABLE},
                        {"int32_to_int32_list_map", NANOARROW_TYPE_MAP, NULLABLE},
                    }));
  ASSERT_NO_FATAL_FAILURE(CompareSchema(reader.schema->children[1]->children[4],
                                        {
                                            {"item", NANOARROW_TYPE_STRING, NULLABLE},
                                        }));
  ASSERT_NO_FATAL_FAILURE(CompareSchema(reader.schema->children[1]->children[5],
                                        {
                                            {"entries", NANOARROW_TYPE_STRUCT, NOT_NULL},
                                        }));
  ASSERT_NO_FATAL_FAILURE(
      CompareSchema(reader.schema->children[1]->children[5]->children[0],
                    {
                        {"key", NANOARROW_TYPE_INT32, NOT_NULL},
                        {"value", NANOARROW_TYPE_LIST, NULLABLE},
                    }));
  ASSERT_NO_FATAL_FAILURE(
      CompareSchema(reader.schema->children[1]->children[5]->children[0]->children[1],
                    {
                        {"item", NANOARROW_TYPE_INT32, NULLABLE},
                    }));

  std::vector<uint32_t> seen;
  while (true) {
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    if (!reader.array->release) break;

    for (int64_t row = 0; row < reader.array->length; row++) {
      ASSERT_FALSE(ArrowArrayViewIsNull(reader.array_view->children[0], row));
      const uint32_t code =
          reader.array_view->children[0]->buffer_views[1].data.as_uint32[row];
      seen.push_back(code);

      switch (code) {
        case ADBC_INFO_DRIVER_NAME:
        case ADBC_INFO_DRIVER_VERSION:
        case ADBC_INFO_VENDOR_NAME:
        case ADBC_INFO_VENDOR_VERSION:
          // UTF8
          ASSERT_EQ(uint8_t(0),
                    reader.array_view->children[1]->buffer_views[0].data.as_uint8[row]);
        default:
          // Ignored
          break;
      }
    }
  }
  ASSERT_THAT(seen, ::testing::UnorderedElementsAreArray(info));
}

void ConnectionTest::TestMetadataGetTableSchema() {
  if (!quirks()->supports_bulk_ingest()) {
    GTEST_SKIP();
  }
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection, &error));

  Handle<ArrowSchema> schema;
  ASSERT_THAT(AdbcConnectionGetTableSchema(&connection, /*catalog=*/nullptr,
                                           /*db_schema=*/nullptr, "bulk_ingest",
                                           &schema.value, &error),
              IsOkStatus(&error));

  ASSERT_NO_FATAL_FAILURE(
      CompareSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64, NULLABLE},
                                    {"strings", NANOARROW_TYPE_STRING, NULLABLE}}));
}

void ConnectionTest::TestMetadataGetTableTypes() {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  StreamReader reader;
  ASSERT_THAT(AdbcConnectionGetTableTypes(&connection, &reader.stream.value, &error),
              IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      &reader.schema.value, {{"table_type", NANOARROW_TYPE_STRING, NOT_NULL}}));
  ASSERT_NO_FATAL_FAILURE(reader.Next());
}

void CheckGetObjectsSchema(struct ArrowSchema* schema) {
  ASSERT_NO_FATAL_FAILURE(
      CompareSchema(schema, {
                                {"catalog_name", NANOARROW_TYPE_STRING, NULLABLE},
                                {"catalog_db_schemas", NANOARROW_TYPE_LIST, NULLABLE},
                            }));
  struct ArrowSchema* db_schema_schema = schema->children[1]->children[0];
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      db_schema_schema, {
                            {"db_schema_name", NANOARROW_TYPE_STRING, NULLABLE},
                            {"db_schema_tables", NANOARROW_TYPE_LIST, NULLABLE},
                        }));
  struct ArrowSchema* table_schema = db_schema_schema->children[1]->children[0];
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      table_schema, {
                        {"table_name", NANOARROW_TYPE_STRING, NOT_NULL},
                        {"table_type", NANOARROW_TYPE_STRING, NOT_NULL},
                        {"table_columns", NANOARROW_TYPE_LIST, NULLABLE},
                        {"table_constraints", NANOARROW_TYPE_LIST, NULLABLE},
                    }));
  struct ArrowSchema* column_schema = table_schema->children[2]->children[0];
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      column_schema, {
                         {"column_name", NANOARROW_TYPE_STRING, NOT_NULL},
                         {"ordinal_position", NANOARROW_TYPE_INT32, NULLABLE},
                         {"remarks", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_data_type", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_type_name", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_column_size", NANOARROW_TYPE_INT32, NULLABLE},
                         {"xdbc_decimal_digits", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_num_prec_radix", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_nullable", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_column_def", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_sql_data_type", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_datetime_sub", NANOARROW_TYPE_INT16, NULLABLE},
                         {"xdbc_char_octet_length", NANOARROW_TYPE_INT32, NULLABLE},
                         {"xdbc_is_nullable", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_scope_catalog", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_scope_schema", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_scope_table", NANOARROW_TYPE_STRING, NULLABLE},
                         {"xdbc_is_autoincrement", NANOARROW_TYPE_BOOL, NULLABLE},
                         {"xdbc_is_generatedcolumn", NANOARROW_TYPE_BOOL, NULLABLE},
                     }));

  struct ArrowSchema* constraint_schema = table_schema->children[3]->children[0];
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      constraint_schema, {
                             {"constraint_name", NANOARROW_TYPE_STRING, NULLABLE},
                             {"constraint_type", NANOARROW_TYPE_STRING, NOT_NULL},
                             {"constraint_column_names", NANOARROW_TYPE_LIST, NOT_NULL},
                             {"constraint_column_usage", NANOARROW_TYPE_LIST, NULLABLE},
                         }));
  ASSERT_NO_FATAL_FAILURE(CompareSchema(
      constraint_schema->children[2], {
                                          {std::nullopt, NANOARROW_TYPE_STRING, NULLABLE},
                                      }));

  struct ArrowSchema* usage_schema = constraint_schema->children[3]->children[0];
  ASSERT_NO_FATAL_FAILURE(
      CompareSchema(usage_schema, {
                                      {"fk_catalog", NANOARROW_TYPE_STRING, NULLABLE},
                                      {"fk_db_schema", NANOARROW_TYPE_STRING, NULLABLE},
                                      {"fk_table", NANOARROW_TYPE_STRING, NOT_NULL},
                                      {"fk_column_name", NANOARROW_TYPE_STRING, NOT_NULL},
                                  }));
}

void ConnectionTest::TestMetadataGetObjectsCatalogs() {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  if (!quirks()->supports_get_objects()) {
    GTEST_SKIP();
  }

  {
    StreamReader reader;
    ASSERT_THAT(AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_CATALOGS, nullptr,
                                         nullptr, nullptr, nullptr, nullptr,
                                         &reader.stream.value, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    // We requested catalogs, so expect at least one catalog, and
    // 'catalog_db_schemas' should be null
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        ASSERT_TRUE(ArrowArrayViewIsNull(reader.array_view->children[1], row))
            << "Row " << row << " should have null catalog_db_schemas";
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);
  }

  {
    // Filter with a nonexistent catalog - we should get nothing
    StreamReader reader;
    ASSERT_THAT(AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_CATALOGS,
                                         "this catalog does not exist", nullptr, nullptr,
                                         nullptr, nullptr, &reader.stream.value, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    if (reader.array->release) {
      ASSERT_EQ(0, reader.array->length);
      ASSERT_NO_FATAL_FAILURE(reader.Next());
      ASSERT_EQ(nullptr, reader.array->release);
    }
  }
}

void ConnectionTest::TestMetadataGetObjectsDbSchemas() {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));

  if (!quirks()->supports_get_objects()) {
    GTEST_SKIP();
  }

  {
    // Expect at least one catalog, at least one schema, and tables should be null
    StreamReader reader;
    ASSERT_THAT(AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_DB_SCHEMAS,
                                         nullptr, nullptr, nullptr, nullptr, nullptr,
                                         &reader.stream.value, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        // type: list<db_schema_schema>
        struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
        // type: db_schema_schema (struct)
        struct ArrowArrayView* catalog_db_schemas = catalog_db_schemas_list->children[0];
        // type: list<table_schema>
        struct ArrowArrayView* db_schema_tables_list = catalog_db_schemas->children[1];

        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, row))
            << "Row " << row << " should have non-null catalog_db_schemas";

        ArrowStringView catalog_name =
            ArrowArrayViewGetStringUnsafe(reader.array_view->children[0], row);

        const int64_t start_offset =
            ArrowArrayViewListChildOffset(catalog_db_schemas_list, row);
        const int64_t end_offset =
            ArrowArrayViewListChildOffset(catalog_db_schemas_list, row + 1);
        ASSERT_GE(end_offset, start_offset)
            << "Row " << row << " (Catalog "
            << std::string(catalog_name.data, catalog_name.size_bytes)
            << ") should have nonempty catalog_db_schemas ";
        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, row));
        for (int64_t list_index = start_offset; list_index < end_offset; list_index++) {
          ASSERT_TRUE(ArrowArrayViewIsNull(db_schema_tables_list, row + list_index))
              << "Row " << row << " should have null db_schema_tables";
        }
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);
  }

  {
    // Filter with a nonexistent DB schema - we should get nothing
    StreamReader reader;
    ASSERT_THAT(AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_DB_SCHEMAS,
                                         nullptr, "this schema does not exist", nullptr,
                                         nullptr, nullptr, &reader.stream.value, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, row))
            << "Row " << row << " should have non-null catalog_db_schemas";

        const int64_t start_offset =
            ArrowArrayViewListChildOffset(catalog_db_schemas_list, row);
        const int64_t end_offset =
            ArrowArrayViewListChildOffset(catalog_db_schemas_list, row + 1);
        ASSERT_EQ(start_offset, end_offset);
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);
  }
}

void ConnectionTest::TestMetadataGetObjectsTables() {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));
  if (!quirks()->supports_get_objects()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->EnsureSampleTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  std::vector<std::pair<const char*, bool>> test_cases = {
      {nullptr, true}, {"bulk_%", true}, {"asdf%", false}};
  for (const auto& expected : test_cases) {
    std::string scope = "Filter: ";
    scope += expected.first ? expected.first : "(no filter)";
    scope += "; table should exist? ";
    scope += expected.second ? "true" : "false";
    SCOPED_TRACE(scope);

    StreamReader reader;
    ASSERT_THAT(AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_TABLES, nullptr,
                                         nullptr, expected.first, nullptr, nullptr,
                                         &reader.stream.value, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    bool found_expected_table = false;
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        // type: list<db_schema_schema>
        struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
        // type: db_schema_schema (struct)
        struct ArrowArrayView* catalog_db_schemas = catalog_db_schemas_list->children[0];
        // type: list<table_schema>
        struct ArrowArrayView* db_schema_tables_list = catalog_db_schemas->children[1];
        // type: table_schema (struct)
        struct ArrowArrayView* db_schema_tables = db_schema_tables_list->children[0];

        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, row))
            << "Row " << row << " should have non-null catalog_db_schemas";

        for (int64_t db_schemas_index =
                 ArrowArrayViewListChildOffset(catalog_db_schemas_list, row);
             db_schemas_index <
             ArrowArrayViewListChildOffset(catalog_db_schemas_list, row + 1);
             db_schemas_index++) {
          ASSERT_FALSE(ArrowArrayViewIsNull(db_schema_tables_list, db_schemas_index))
              << "Row " << row << " should have non-null db_schema_tables";

          for (int64_t tables_index =
                   ArrowArrayViewListChildOffset(db_schema_tables_list, db_schemas_index);
               tables_index <
               ArrowArrayViewListChildOffset(db_schema_tables_list, db_schemas_index + 1);
               tables_index++) {
            ArrowStringView table_name = ArrowArrayViewGetStringUnsafe(
                db_schema_tables->children[0], tables_index);
            if (iequals(std::string(table_name.data, table_name.size_bytes),
                        "bulk_ingest")) {
              found_expected_table = true;
            }

            ASSERT_TRUE(ArrowArrayViewIsNull(db_schema_tables->children[2], tables_index))
                << "Row " << row << " should have null table_columns";
            ASSERT_TRUE(ArrowArrayViewIsNull(db_schema_tables->children[3], tables_index))
                << "Row " << row << " should have null table_constraints";
          }
        }
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);

    ASSERT_EQ(expected.second, found_expected_table)
        << "Did (not) find table in metadata";
  }
}

void ConnectionTest::TestMetadataGetObjectsTablesTypes() {
  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));
  if (!quirks()->supports_get_objects()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->EnsureSampleTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  std::vector<const char*> table_types(2);
  table_types[0] = "this_table_type_does_not_exist";
  table_types[1] = nullptr;
  {
    StreamReader reader;
    ASSERT_THAT(AdbcConnectionGetObjects(&connection, ADBC_OBJECT_DEPTH_TABLES, nullptr,
                                         nullptr, nullptr, table_types.data(), nullptr,
                                         &reader.stream.value, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    bool found_expected_table = false;
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        // type: list<db_schema_schema>
        struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
        // type: db_schema_schema (struct)
        struct ArrowArrayView* catalog_db_schemas = catalog_db_schemas_list->children[0];
        // type: list<table_schema>
        struct ArrowArrayView* db_schema_tables_list = catalog_db_schemas->children[1];
        // type: table_schema (struct)
        struct ArrowArrayView* db_schema_tables = db_schema_tables_list->children[0];

        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, row))
            << "Row " << row << " should have non-null catalog_db_schemas";

        for (int64_t db_schemas_index =
                 ArrowArrayViewListChildOffset(catalog_db_schemas_list, row);
             db_schemas_index <
             ArrowArrayViewListChildOffset(catalog_db_schemas_list, row + 1);
             db_schemas_index++) {
          ASSERT_FALSE(ArrowArrayViewIsNull(db_schema_tables_list, db_schemas_index))
              << "Row " << row << " should have non-null db_schema_tables";

          for (int64_t tables_index =
                   ArrowArrayViewListChildOffset(db_schema_tables_list, db_schemas_index);
               tables_index <
               ArrowArrayViewListChildOffset(db_schema_tables_list, db_schemas_index + 1);
               tables_index++) {
            ArrowStringView table_name = ArrowArrayViewGetStringUnsafe(
                db_schema_tables->children[0], tables_index);
            if (std::string_view(table_name.data, table_name.size_bytes) ==
                "bulk_ingest") {
              found_expected_table = true;
            }

            ASSERT_TRUE(ArrowArrayViewIsNull(db_schema_tables->children[2], tables_index))
                << "Row " << row << " should have null table_columns";
            ASSERT_TRUE(ArrowArrayViewIsNull(db_schema_tables->children[3], tables_index))
                << "Row " << row << " should have null table_constraints";
          }
        }
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);

    ASSERT_FALSE(found_expected_table) << "Should not find table in metadata";
  }
}

void ConnectionTest::TestMetadataGetObjectsColumns() {
  if (!quirks()->supports_get_objects()) {
    GTEST_SKIP();
  }
  // TODO: test could be more robust if we ingested a few tables
  ASSERT_EQ(ADBC_OBJECT_DEPTH_COLUMNS, ADBC_OBJECT_DEPTH_ALL);

  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->EnsureSampleTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  struct TestCase {
    std::optional<std::string> filter;
    std::vector<std::string> column_names;
    std::vector<int32_t> ordinal_positions;
  };

  std::vector<TestCase> test_cases;
  test_cases.push_back({std::nullopt, {"int64s", "strings"}, {1, 2}});
  test_cases.push_back({"in%", {"int64s"}, {1}});

  for (const auto& test_case : test_cases) {
    std::string scope = "Filter: ";
    scope += test_case.filter ? *test_case.filter : "(no filter)";
    SCOPED_TRACE(scope);

    StreamReader reader;
    std::vector<std::string> column_names;
    std::vector<int32_t> ordinal_positions;

    ASSERT_THAT(
        AdbcConnectionGetObjects(
            &connection, ADBC_OBJECT_DEPTH_COLUMNS, nullptr, nullptr, nullptr, nullptr,
            test_case.filter.has_value() ? test_case.filter->c_str() : nullptr,
            &reader.stream.value, &error),
        IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CheckGetObjectsSchema(&reader.schema.value));
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_GT(reader.array->length, 0);
    bool found_expected_table = false;
    do {
      for (int64_t row = 0; row < reader.array->length; row++) {
        // type: list<db_schema_schema>
        struct ArrowArrayView* catalog_db_schemas_list = reader.array_view->children[1];
        // type: db_schema_schema (struct)
        struct ArrowArrayView* catalog_db_schemas = catalog_db_schemas_list->children[0];
        // type: list<table_schema>
        struct ArrowArrayView* db_schema_tables_list = catalog_db_schemas->children[1];
        // type: table_schema (struct)
        struct ArrowArrayView* db_schema_tables = db_schema_tables_list->children[0];
        // type: list<column_schema>
        struct ArrowArrayView* table_columns_list = db_schema_tables->children[2];
        // type: column_schema (struct)
        struct ArrowArrayView* table_columns = table_columns_list->children[0];
        // type: list<usage_schema>
        struct ArrowArrayView* table_constraints_list = db_schema_tables->children[3];

        ASSERT_FALSE(ArrowArrayViewIsNull(catalog_db_schemas_list, row))
            << "Row " << row << " should have non-null catalog_db_schemas";

        for (int64_t db_schemas_index =
                 ArrowArrayViewListChildOffset(catalog_db_schemas_list, row);
             db_schemas_index <
             ArrowArrayViewListChildOffset(catalog_db_schemas_list, row + 1);
             db_schemas_index++) {
          ASSERT_FALSE(ArrowArrayViewIsNull(db_schema_tables_list, db_schemas_index))
              << "Row " << row << " should have non-null db_schema_tables";

          ArrowStringView db_schema_name = ArrowArrayViewGetStringUnsafe(
              catalog_db_schemas->children[0], db_schemas_index);

          for (int64_t tables_index =
                   ArrowArrayViewListChildOffset(db_schema_tables_list, db_schemas_index);
               tables_index <
               ArrowArrayViewListChildOffset(db_schema_tables_list, db_schemas_index + 1);
               tables_index++) {
            ArrowStringView table_name = ArrowArrayViewGetStringUnsafe(
                db_schema_tables->children[0], tables_index);

            ASSERT_FALSE(ArrowArrayViewIsNull(table_columns_list, tables_index))
                << "Row " << row << " should have non-null table_columns";
            ASSERT_FALSE(ArrowArrayViewIsNull(table_constraints_list, tables_index))
                << "Row " << row << " should have non-null table_constraints";

            if (iequals(std::string(table_name.data, table_name.size_bytes),
                        "bulk_ingest") &&
                iequals(std::string(db_schema_name.data, db_schema_name.size_bytes),
                        quirks()->db_schema())) {
              found_expected_table = true;

              for (int64_t columns_index =
                       ArrowArrayViewListChildOffset(table_columns_list, tables_index);
                   columns_index <
                   ArrowArrayViewListChildOffset(table_columns_list, tables_index + 1);
                   columns_index++) {
                ArrowStringView name = ArrowArrayViewGetStringUnsafe(
                    table_columns->children[0], columns_index);
                std::string temp(name.data, name.size_bytes);
                std::transform(temp.begin(), temp.end(), temp.begin(),
                               [](unsigned char c) { return std::tolower(c); });
                column_names.push_back(std::move(temp));
                ordinal_positions.push_back(
                    static_cast<int32_t>(ArrowArrayViewGetIntUnsafe(
                        table_columns->children[1], columns_index)));
              }
            }
          }
        }
      }
      ASSERT_NO_FATAL_FAILURE(reader.Next());
    } while (reader.array->release);

    ASSERT_TRUE(found_expected_table) << "Did (not) find table in metadata";
    ASSERT_EQ(test_case.column_names, column_names);
    ASSERT_EQ(test_case.ordinal_positions, ordinal_positions);
  }
}

void ConnectionTest::TestMetadataGetObjectsConstraints() {
  // TODO: can't be done portably (need to create tables with primary keys and such)
}

//------------------------------------------------------------
// Tests of AdbcStatement

void StatementTest::SetUpTest() {
  std::memset(&error, 0, sizeof(error));
  std::memset(&database, 0, sizeof(database));
  std::memset(&connection, 0, sizeof(connection));
  std::memset(&statement, 0, sizeof(statement));

  ASSERT_THAT(AdbcDatabaseNew(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->SetupDatabase(&database, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseInit(&database, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionNew(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection, &database, &error), IsOkStatus(&error));
}

void StatementTest::TearDownTest() {
  if (statement.private_data) {
    EXPECT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
  }
  EXPECT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
  EXPECT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
  if (error.release) {
    error.release(&error);
  }
}

void StatementTest::TestNewInit() {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
  ASSERT_EQ(NULL, statement.private_data);

  ASSERT_THAT(AdbcStatementRelease(&statement, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  // Cannot execute
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));
}

void StatementTest::TestRelease() {
  ASSERT_THAT(AdbcStatementRelease(&statement, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
  ASSERT_EQ(NULL, statement.private_data);
}

template <typename CType>
void StatementTest::TestSqlIngestType(ArrowType type,
                                      const std::vector<std::optional<CType>>& values) {
  if (!quirks()->supports_bulk_ingest()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(MakeSchema(&schema.value, {{"col", type}}), IsOkErrno());
  ASSERT_THAT(MakeBatch<CType>(&schema.value, &array.value, &na_error, values),
              IsOkErrno());

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));

  int64_t rows_affected = 0;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(rows_affected,
              ::testing::AnyOf(::testing::Eq(values.size()), ::testing::Eq(-1)));

  ASSERT_THAT(AdbcStatementSetSqlQuery(
                  &statement,
                  "SELECT * FROM bulk_ingest ORDER BY \"col\" ASC NULLS FIRST", &error),
              IsOkStatus(&error));
  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(values.size()), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ArrowType round_trip_type = quirks()->IngestSelectRoundTripType(type);
    ASSERT_NO_FATAL_FAILURE(
        CompareSchema(&reader.schema.value, {{"col", round_trip_type, NULLABLE}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(values.size(), reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    if (round_trip_type == type) {
      // XXX: for now we can't compare values; we would need casting
      ASSERT_NO_FATAL_FAILURE(
          CompareArray<CType>(reader.array_view->children[0], values));
    }

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }
}

template <typename CType>
void StatementTest::TestSqlIngestNumericType(ArrowType type) {
  std::vector<std::optional<CType>> values = {
      std::nullopt,
  };

  if constexpr (std::is_floating_point_v<CType>) {
    // XXX: sqlite and others seem to have trouble with extreme
    // values. Likely a bug on our side, but for now, avoid them.
    values.push_back(static_cast<CType>(-1.5));
    values.push_back(static_cast<CType>(1.5));
  } else {
    values.push_back(std::numeric_limits<CType>::lowest());
    values.push_back(std::numeric_limits<CType>::max());
  }

  return TestSqlIngestType(type, values);
}

void StatementTest::TestSqlIngestUInt8() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<uint8_t>(NANOARROW_TYPE_UINT8));
}

void StatementTest::TestSqlIngestUInt16() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<uint16_t>(NANOARROW_TYPE_UINT16));
}

void StatementTest::TestSqlIngestUInt32() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<uint32_t>(NANOARROW_TYPE_UINT32));
}

void StatementTest::TestSqlIngestUInt64() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<uint64_t>(NANOARROW_TYPE_UINT64));
}

void StatementTest::TestSqlIngestInt8() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<int8_t>(NANOARROW_TYPE_INT8));
}

void StatementTest::TestSqlIngestInt16() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<int16_t>(NANOARROW_TYPE_INT16));
}

void StatementTest::TestSqlIngestInt32() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<int32_t>(NANOARROW_TYPE_INT32));
}

void StatementTest::TestSqlIngestInt64() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<int64_t>(NANOARROW_TYPE_INT64));
}

void StatementTest::TestSqlIngestFloat32() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<float>(NANOARROW_TYPE_FLOAT));
}

void StatementTest::TestSqlIngestFloat64() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<double>(NANOARROW_TYPE_DOUBLE));
}

void StatementTest::TestSqlIngestString() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::string>(
      NANOARROW_TYPE_STRING, {std::nullopt, "", "", "1234", "例"}));
}

void StatementTest::TestSqlIngestBinary() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::string>(
      NANOARROW_TYPE_BINARY, {std::nullopt, "", "\x00\x01\x02\x04", "\xFE\xFF"}));
}

void StatementTest::TestSqlIngestAppend() {
  if (!quirks()->supports_bulk_ingest()) {
    GTEST_SKIP();
  }

  // Ingest
  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT(MakeBatch<int64_t>(&schema.value, &array.value, &na_error, {42}),
              IsOkErrno());

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));

  int64_t rows_affected = 0;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

  // Now append

  // Re-initialize since Bind() should take ownership of data
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT(
      MakeBatch<int64_t>(&schema.value, &array.value, &na_error, {-42, std::nullopt}),
      IsOkErrno());

  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                     ADBC_INGEST_OPTION_MODE_APPEND, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(2), ::testing::Eq(-1)));

  // Read data back
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_ingest", &error),
              IsOkStatus(&error));
  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CompareSchema(&reader.schema.value,
                                          {{"int64s", NANOARROW_TYPE_INT64, NULLABLE}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(3, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_NO_FATAL_FAILURE(
        CompareArray<int64_t>(reader.array_view->children[0], {42, -42, std::nullopt}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlIngestErrors() {
  if (!quirks()->supports_bulk_ingest()) {
    GTEST_SKIP();
  }

  // Ingest without bind
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));
  if (error.release) error.release(&error);

  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  // Append to nonexistent table
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                     ADBC_INGEST_OPTION_MODE_APPEND, &error),
              IsOkStatus(&error));
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT(
      MakeBatch<int64_t>(&schema.value, &array.value, &na_error, {-42, std::nullopt}),
      IsOkErrno(&na_error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              ::testing::Not(IsOkStatus(&error)));
  if (error.release) error.release(&error);

  // Ingest...
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                     ADBC_INGEST_OPTION_MODE_CREATE, &error),
              IsOkStatus(&error));
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT(
      MakeBatch<int64_t>(&schema.value, &array.value, &na_error, {-42, std::nullopt}),
      IsOkErrno(&na_error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  // ...then try to overwrite it
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT(
      MakeBatch<int64_t>(&schema.value, &array.value, &na_error, {-42, std::nullopt}),
      IsOkErrno(&na_error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              ::testing::Not(IsOkStatus(&error)));
  if (error.release) error.release(&error);

  // ...then try to append an incompatible schema
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64},
                                         {"coltwo", NANOARROW_TYPE_INT64}}),
              IsOkErrno());
  ASSERT_THAT(
      (MakeBatch<int64_t, int64_t>(&schema.value, &array.value, &na_error, {}, {})),
      IsOkErrno(&na_error));

  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                     ADBC_INGEST_OPTION_MODE_APPEND, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              ::testing::Not(IsOkStatus(&error)));
}

void StatementTest::TestSqlIngestMultipleConnections() {
  if (!quirks()->supports_bulk_ingest()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                  {42, -42, std::nullopt})),
              IsOkErrno());

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));

  int64_t rows_affected = 0;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));
  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));

  {
    struct AdbcConnection connection2 = {};
    ASSERT_THAT(AdbcConnectionNew(&connection2, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionInit(&connection2, &database, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementNew(&connection2, &statement, &error), IsOkStatus(&error));

    ASSERT_THAT(
        AdbcStatementSetSqlQuery(
            &statement, "SELECT * FROM bulk_ingest ORDER BY \"int64s\" DESC NULLS LAST",
            &error),
        IsOkStatus(&error));

    {
      StreamReader reader;
      ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                            &reader.rows_affected, &error),
                  IsOkStatus(&error));
      ASSERT_THAT(reader.rows_affected,
                  ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));

      ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
      ASSERT_NO_FATAL_FAILURE(CompareSchema(
          &reader.schema.value, {{"int64s", NANOARROW_TYPE_INT64, NULLABLE}}));

      ASSERT_NO_FATAL_FAILURE(reader.Next());
      ASSERT_NE(nullptr, reader.array->release);
      ASSERT_EQ(3, reader.array->length);
      ASSERT_EQ(1, reader.array->n_children);

      ASSERT_NO_FATAL_FAILURE(
          CompareArray<int64_t>(reader.array_view->children[0], {42, -42, std::nullopt}));

      ASSERT_NO_FATAL_FAILURE(reader.Next());
      ASSERT_EQ(nullptr, reader.array->release);
    }

    ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
    ASSERT_THAT(AdbcConnectionRelease(&connection2, &error), IsOkStatus(&error));
  }
}

void StatementTest::TestSqlIngestSample() {
  if (!quirks()->supports_bulk_ingest()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->EnsureSampleTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(
                  &statement, "SELECT * FROM bulk_ingest ORDER BY int64s ASC NULLS FIRST",
                  &error),
              IsOkStatus(&error));
  StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(reader.rows_affected,
              ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));

  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_NO_FATAL_FAILURE(CompareSchema(&reader.schema.value,
                                        {{"int64s", NANOARROW_TYPE_INT64, NULLABLE},
                                         {"strings", NANOARROW_TYPE_STRING, NULLABLE}}));

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_EQ(3, reader.array->length);
  ASSERT_EQ(2, reader.array->n_children);

  ASSERT_NO_FATAL_FAILURE(
      CompareArray<int64_t>(reader.array_view->children[0], {std::nullopt, -42, 42}));
  ASSERT_NO_FATAL_FAILURE(CompareArray<std::string>(reader.array_view->children[1],
                                                    {"", std::nullopt, "foo"}));

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

void StatementTest::TestSqlPartitionedInts() {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 42", &error),
              IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  Handle<struct AdbcPartitions> partitions;
  int64_t rows_affected = 0;

  if (!quirks()->supports_partitioned_data()) {
    ASSERT_THAT(AdbcStatementExecutePartitions(&statement, &schema.value,
                                               &partitions.value, &rows_affected, &error),
                IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error));
    GTEST_SKIP();
  }

  ASSERT_THAT(AdbcStatementExecutePartitions(&statement, &schema.value, &partitions.value,
                                             &rows_affected, &error),
              IsOkStatus(&error));
  // Assume only 1 partition
  ASSERT_EQ(1, partitions->num_partitions);
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));
  // it's allowed for Executepartitions to return a nil schema if one is not available
  if (schema->release != nullptr) {
    ASSERT_EQ(1, schema->n_children);
  }

  Handle<struct AdbcConnection> connection2;
  StreamReader reader;
  ASSERT_THAT(AdbcConnectionNew(&connection2.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection2.value, &database, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionReadPartition(&connection2.value, partitions->partitions[0],
                                          partitions->partition_lengths[0],
                                          &reader.stream.value, &error),
              IsOkStatus(&error));

  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_EQ(1, reader.schema->n_children);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_EQ(1, reader.array->length);
  ASSERT_EQ(1, reader.array->n_children);

  switch (reader.fields[0].type) {
    case NANOARROW_TYPE_INT32:
      ASSERT_NO_FATAL_FAILURE(
          CompareArray<int32_t>(reader.array_view->children[0], {42}));
      break;
    case NANOARROW_TYPE_INT64:
      ASSERT_NO_FATAL_FAILURE(
          CompareArray<int64_t>(reader.array_view->children[0], {42}));
      break;
    default:
      FAIL() << "Unexpected data type: " << reader.fields[0].type;
  }

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

void StatementTest::TestSqlPrepareGetParameterSchema() {
  if (!quirks()->supports_dynamic_parameter_binding()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  std::string query = "SELECT ";
  query += quirks()->BindParameter(0);
  query += ", ";
  query += quirks()->BindParameter(1);

  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  // if schema cannot be determined we should get NOT IMPLEMENTED returned
  ASSERT_THAT(AdbcStatementGetParameterSchema(&statement, &schema.value, &error),
              ::testing::AnyOf(IsOkStatus(&error),
                               IsStatus(ADBC_STATUS_NOT_IMPLEMENTED, &error)));
  if (schema->release != nullptr) {
    ASSERT_EQ(2, schema->n_children);
  }
  // Can't assume anything about names or types here
}

void StatementTest::TestSqlPrepareSelectNoParams() {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 1", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));

  StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(reader.rows_affected,
              ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_EQ(1, reader.schema->n_children);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_NE(nullptr, reader.array->release);
  ASSERT_EQ(1, reader.array->length);
  ASSERT_EQ(1, reader.array->n_children);

  switch (reader.fields[0].type) {
    case NANOARROW_TYPE_INT32:
      ASSERT_NO_FATAL_FAILURE(CompareArray<int32_t>(reader.array_view->children[0], {1}));
      break;
    case NANOARROW_TYPE_INT64:
      ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(reader.array_view->children[0], {1}));
      break;
    default:
      FAIL() << "Unexpected data type: " << reader.fields[0].type;
  }

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

void StatementTest::TestSqlPrepareSelectParams() {
  if (!quirks()->supports_dynamic_parameter_binding()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  std::string query = "SELECT ";
  query += quirks()->BindParameter(0);
  query += ", ";
  query += quirks()->BindParameter(1);
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64},
                                         {"strings", NANOARROW_TYPE_STRING}}),
              IsOkErrno());
  ASSERT_THAT((MakeBatch<int64_t, std::string>(&schema.value, &array.value, &na_error,
                                               {42, -42, std::nullopt},
                                               {"", std::nullopt, "bar"})),
              IsOkErrno());
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));

  StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(reader.rows_affected,
              ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  ASSERT_EQ(2, reader.schema->n_children);

  const std::vector<std::optional<int32_t>> expected_int32{42, -42, std::nullopt};
  const std::vector<std::optional<int64_t>> expected_int64{42, -42, std::nullopt};
  const std::vector<std::optional<std::string>> expected_string{"", std::nullopt, "bar"};

  int64_t nrows = 0;
  while (nrows < 3) {
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(2, reader.array->n_children);

    auto start = nrows;
    auto end = nrows + reader.array->length;

    ASSERT_LT(start, expected_int32.size());
    ASSERT_LE(end, expected_int32.size());

    switch (reader.fields[0].type) {
      case NANOARROW_TYPE_INT32:
        ASSERT_NO_FATAL_FAILURE(CompareArray<int32_t>(
            reader.array_view->children[0],
            {expected_int32.begin() + start, expected_int32.begin() + end}));
        break;
      case NANOARROW_TYPE_INT64:
        ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(
            reader.array_view->children[0],
            {expected_int64.begin() + start, expected_int64.begin() + end}));
        break;
      default:
        FAIL() << "Unexpected data type: " << reader.fields[0].type;
    }
    ASSERT_NO_FATAL_FAILURE(CompareArray<std::string>(
        reader.array_view->children[1],
        {expected_string.begin() + start, expected_string.begin() + end}));
    nrows += reader.array->length;
  }
  ASSERT_EQ(3, nrows);

  ASSERT_NO_FATAL_FAILURE(reader.Next());
  ASSERT_EQ(nullptr, reader.array->release);
}

void StatementTest::TestSqlPrepareUpdate() {
  if (!quirks()->supports_bulk_ingest() ||
      !quirks()->supports_dynamic_parameter_binding()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  // Create table
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                  {42, -42, std::nullopt})),
              IsOkErrno());
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  // Prepare
  std::string query =
      "INSERT INTO bulk_ingest VALUES (" + quirks()->BindParameter(0) + ")";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));

  // Bind and execute
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                  {42, -42, std::nullopt})),
              IsOkErrno());
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  // Read data back
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_ingest", &error),
              IsOkStatus(&error));
  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(6), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CompareSchema(&reader.schema.value,
                                          {{"int64s", NANOARROW_TYPE_INT64, NULLABLE}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(6, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(
        reader.array_view->children[0], {42, -42, std::nullopt, 42, -42, std::nullopt}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }
}

void StatementTest::TestSqlPrepareUpdateNoParams() {
  // TODO: prepare something like INSERT 1, then execute it and confirm it's executed once

  // TODO: then bind a table with 0 cols and X rows and confirm it executes multiple times
}

void StatementTest::TestSqlPrepareUpdateStream() {
  if (!quirks()->supports_bulk_ingest() ||
      !quirks()->supports_dynamic_parameter_binding()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));
  struct ArrowError na_error;

  const std::vector<SchemaField> fields = {{"ints", NANOARROW_TYPE_INT64}};

  // Create table
  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;

    ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       "bulk_ingest", &error),
                IsOkStatus(&error));
    ASSERT_THAT(MakeSchema(&schema.value, fields), IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error, {})),
                IsOkErrno(&na_error));
    ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  // Generate stream
  Handle<struct ArrowArrayStream> stream;
  Handle<struct ArrowSchema> schema;
  std::vector<struct ArrowArray> batches(2);

  ASSERT_THAT(MakeSchema(&schema.value, fields), IsOkErrno());
  ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &batches[0], &na_error,
                                  {1, 2, std::nullopt, 3})),
              IsOkErrno(&na_error));
  ASSERT_THAT(
      MakeBatch<int64_t>(&schema.value, &batches[1], &na_error, {std::nullopt, 3}),
      IsOkErrno(&na_error));
  MakeStream(&stream.value, &schema.value, std::move(batches));

  // Prepare
  std::string query =
      "INSERT INTO bulk_ingest VALUES (" + quirks()->BindParameter(0) + ")";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));

  // Bind and execute
  ASSERT_THAT(AdbcStatementBindStream(&statement, &stream.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  // Read data back
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT * FROM bulk_ingest", &error),
              IsOkStatus(&error));
  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(6), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(
        CompareSchema(&reader.schema.value, {{"ints", NANOARROW_TYPE_INT64, NULLABLE}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(6, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(
        reader.array_view->children[0], {1, 2, std::nullopt, 3, std::nullopt, 3}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  // TODO: import released stream

  // TODO: stream that errors on get_schema

  // TODO: stream that errors on get_next (first call)

  // TODO: stream that errors on get_next (second call)
}

void StatementTest::TestSqlPrepareErrorNoQuery() {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementPrepare(&statement, &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));
  if (error.release) error.release(&error);
}

// TODO: need test of overlapping reads - make sure behavior is as described

void StatementTest::TestSqlPrepareErrorParamCountMismatch() {
  if (!quirks()->supports_dynamic_parameter_binding()) {
    GTEST_SKIP();
  }

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  StreamReader reader;

  std::string query = "SELECT ";
  query += quirks()->BindParameter(0);
  query += ", ";
  query += quirks()->BindParameter(1);

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                  {42, -42, std::nullopt})),
              IsOkErrno());

  ASSERT_THAT(
      ([&]() -> AdbcStatusCode {
        CHECK_OK(AdbcStatementBind(&statement, &array.value, &schema.value, &error));
        CHECK_OK(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                           &reader.rows_affected, &error));
        return ADBC_STATUS_OK;
      })(),
      ::testing::Not(IsOkStatus(&error)));
}

void StatementTest::TestSqlQueryInts() {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 42", &error),
              IsOkStatus(&error));

  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_EQ(1, reader.schema->n_children);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(1, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    switch (reader.fields[0].type) {
      case NANOARROW_TYPE_INT32:
        ASSERT_NO_FATAL_FAILURE(
            CompareArray<int32_t>(reader.array_view->children[0], {42}));
        break;
      case NANOARROW_TYPE_INT64:
        ASSERT_NO_FATAL_FAILURE(
            CompareArray<int64_t>(reader.array_view->children[0], {42}));
        break;
      default:
        FAIL() << "Unexpected data type: " << reader.fields[0].type;
    }

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlQueryFloats() {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT CAST(1.5 AS FLOAT)", &error),
              IsOkStatus(&error));

  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_EQ(1, reader.schema->n_children);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(1, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_FALSE(ArrowArrayViewIsNull(&reader.array_view.value, 0));
    ASSERT_FALSE(ArrowArrayViewIsNull(reader.array_view->children[0], 0));
    switch (reader.fields[0].type) {
      case NANOARROW_TYPE_FLOAT:
        ASSERT_NO_FATAL_FAILURE(
            CompareArray<float>(reader.array_view->children[0], {1.5f}));
        break;
      case NANOARROW_TYPE_DOUBLE:
        ASSERT_NO_FATAL_FAILURE(
            CompareArray<double>(reader.array_view->children[0], {1.5}));
        break;
      default:
        FAIL() << "Unexpected data type: " << reader.fields[0].type;
    }

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlQueryStrings() {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 'SaShiSuSeSo'", &error),
              IsOkStatus(&error));

  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_EQ(1, reader.schema->n_children);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(1, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_FALSE(ArrowArrayViewIsNull(&reader.array_view.value, 0));
    ASSERT_FALSE(ArrowArrayViewIsNull(reader.array_view->children[0], 0));
    switch (reader.fields[0].type) {
      case NANOARROW_TYPE_STRING: {
        ASSERT_NO_FATAL_FAILURE(
            CompareArray<std::string>(reader.array_view->children[0], {"SaShiSuSeSo"}));
        break;
      }
      default:
        FAIL() << "Unexpected data type: " << reader.fields[0].type;
    }

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlQueryErrors() {
  // Invalid query
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  AdbcStatusCode code =
      AdbcStatementSetSqlQuery(&statement, "this is not a query", &error);
  if (code == ADBC_STATUS_OK) {
    code = AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error);
  }
  ASSERT_NE(ADBC_STATUS_OK, code);
}

void StatementTest::TestTransactions() {
  if (!quirks()->supports_transactions() || quirks()->ddl_implicit_commit_txn()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  Handle<struct AdbcConnection> connection2;
  ASSERT_THAT(AdbcConnectionNew(&connection2.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection2.value, &database, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error),
              IsOkStatus(&error));

  // Uncommitted change
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection, &error));

  // Query on first connection should succeed
  {
    Handle<struct AdbcStatement> statement;
    StreamReader reader;

    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(&statement.value, "SELECT * FROM bulk_ingest", &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  }

  if (error.release) error.release(&error);

  // Query on second connection should fail
  ASSERT_THAT(([&]() -> AdbcStatusCode {
                Handle<struct AdbcStatement> statement;
                StreamReader reader;

                CHECK_OK(AdbcStatementNew(&connection2.value, &statement.value, &error));
                CHECK_OK(AdbcStatementSetSqlQuery(&statement.value,
                                                  "SELECT * FROM bulk_ingest", &error));
                CHECK_OK(AdbcStatementExecuteQuery(&statement.value, &reader.stream.value,
                                                   &reader.rows_affected, &error));
                return ADBC_STATUS_OK;
              })(),
              ::testing::Not(IsOkStatus(&error)));

  if (error.release) error.release(&error);

  // Rollback
  ASSERT_THAT(AdbcConnectionRollback(&connection, &error), IsOkStatus(&error));

  // Query on first connection should fail
  ASSERT_THAT(([&]() -> AdbcStatusCode {
                Handle<struct AdbcStatement> statement;
                StreamReader reader;

                CHECK_OK(AdbcStatementNew(&connection, &statement.value, &error));
                CHECK_OK(AdbcStatementSetSqlQuery(&statement.value,
                                                  "SELECT * FROM bulk_ingest", &error));
                CHECK_OK(AdbcStatementExecuteQuery(&statement.value, &reader.stream.value,
                                                   &reader.rows_affected, &error));
                return ADBC_STATUS_OK;
              })(),
              ::testing::Not(IsOkStatus(&error)));

  // Commit
  ASSERT_NO_FATAL_FAILURE(IngestSampleTable(&connection, &error));
  ASSERT_THAT(AdbcConnectionCommit(&connection, &error), IsOkStatus(&error));

  // Query on second connection should succeed
  {
    Handle<struct AdbcStatement> statement;
    StreamReader reader;

    ASSERT_THAT(AdbcStatementNew(&connection2.value, &statement.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(
        AdbcStatementSetSqlQuery(&statement.value, "SELECT * FROM bulk_ingest", &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  }
}

void StatementTest::TestConcurrentStatements() {
  Handle<struct AdbcStatement> statement1;
  Handle<struct AdbcStatement> statement2;

  ASSERT_THAT(AdbcStatementNew(&connection, &statement1.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementNew(&connection, &statement2.value, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement1.value, "SELECT 'SaShiSuSeSo'", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement2.value, "SELECT 'SaShiSuSeSo'", &error),
              IsOkStatus(&error));

  StreamReader reader1;
  StreamReader reader2;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement1.value, &reader1.stream.value,
                                        &reader1.rows_affected, &error),
              IsOkStatus(&error));

  if (quirks()->supports_concurrent_statements()) {
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement2.value, &reader2.stream.value,
                                          &reader2.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader2.GetSchema());
  } else {
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement2.value, &reader2.stream.value,
                                          &reader2.rows_affected, &error),
                ::testing::Not(IsOkStatus(&error)));
    ASSERT_EQ(nullptr, reader2.stream.value.release);
  }
  // Original stream should still be valid
  ASSERT_NO_FATAL_FAILURE(reader1.GetSchema());
}

void StatementTest::TestResultInvalidation() {
  // Start reading from a statement, then overwrite it
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 42", &error),
              IsOkStatus(&error));

  StreamReader reader1;
  StreamReader reader2;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader1.stream.value,
                                        &reader1.rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader1.GetSchema());

  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader2.stream.value,
                                        &reader2.rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader2.GetSchema());

  // First reader should not fail, but may give no data
  ASSERT_NO_FATAL_FAILURE(reader1.Next());
}

#undef NOT_NULL
#undef NULLABLE
}  // namespace adbc_validation
