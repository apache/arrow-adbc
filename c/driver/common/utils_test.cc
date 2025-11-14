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
#include <string>
#include <string_view>
#include <vector>

#include <gmock/gmock.h>
#include <gtest/gtest.h>

#include "utils.h"

TEST(TestStringBuilder, TestBasic) {
  struct InternalAdbcStringBuilder str;
  int ret;
  ret = InternalAdbcStringBuilderInit(&str, /*initial_size=*/64);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.capacity, 64);

  ret = InternalAdbcStringBuilderAppend(&str, "%s", "BASIC TEST");
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.size, 10);
  EXPECT_STREQ(str.buffer, "BASIC TEST");

  InternalAdbcStringBuilderReset(&str);
}

TEST(TestStringBuilder, TestBoundary) {
  struct InternalAdbcStringBuilder str;
  int ret;
  ret = InternalAdbcStringBuilderInit(&str, /*initial_size=*/10);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.capacity, 10);

  ret = InternalAdbcStringBuilderAppend(&str, "%s", "BASIC TEST");
  EXPECT_EQ(ret, 0);
  // should resize to include \0
  EXPECT_EQ(str.capacity, 11);
  EXPECT_EQ(str.size, 10);
  EXPECT_STREQ(str.buffer, "BASIC TEST");

  InternalAdbcStringBuilderReset(&str);
}

TEST(TestStringBuilder, TestMultipleAppends) {
  struct InternalAdbcStringBuilder str;
  int ret;
  ret = InternalAdbcStringBuilderInit(&str, /*initial_size=*/2);
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.capacity, 2);

  ret = InternalAdbcStringBuilderAppend(&str, "%s", "BASIC");
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.capacity, 6);
  EXPECT_EQ(str.size, 5);
  EXPECT_STREQ(str.buffer, "BASIC");

  ret = InternalAdbcStringBuilderAppend(&str, "%s", " TEST");
  EXPECT_EQ(ret, 0);
  EXPECT_EQ(str.capacity, 11);
  EXPECT_EQ(str.size, 10);
  EXPECT_STREQ(str.buffer, "BASIC TEST");

  InternalAdbcStringBuilderReset(&str);
}

TEST(ErrorDetails, Adbc100) {
  struct AdbcError error;
  std::memset(&error, 0, ADBC_ERROR_1_1_0_SIZE);

  InternalAdbcSetError(&error, "My message");

  ASSERT_EQ(nullptr, error.private_data);
  ASSERT_EQ(nullptr, error.private_driver);

  {
    std::string detail = "detail";
    InternalAdbcAppendErrorDetail(
        &error, "key", reinterpret_cast<const uint8_t*>(detail.data()), detail.size());
  }

  ASSERT_EQ(0, InternalAdbcCommonErrorGetDetailCount(&error));
  struct AdbcErrorDetail detail = InternalAdbcCommonErrorGetDetail(&error, 0);
  ASSERT_EQ(nullptr, detail.key);
  ASSERT_EQ(nullptr, detail.value);
  ASSERT_EQ(0, detail.value_length);

  error.release(&error);
}

TEST(ErrorDetails, Adbc110) {
  struct AdbcError error = ADBC_ERROR_INIT;
  InternalAdbcSetError(&error, "My message");

  ASSERT_NE(nullptr, error.private_data);
  ASSERT_EQ(nullptr, error.private_driver);

  {
    std::string detail = "detail";
    InternalAdbcAppendErrorDetail(
        &error, "key", reinterpret_cast<const uint8_t*>(detail.data()), detail.size());
  }

  ASSERT_EQ(1, InternalAdbcCommonErrorGetDetailCount(&error));
  struct AdbcErrorDetail detail = InternalAdbcCommonErrorGetDetail(&error, 0);
  ASSERT_STREQ("key", detail.key);
  ASSERT_EQ("detail", std::string_view(reinterpret_cast<const char*>(detail.value),
                                       detail.value_length));

  detail = InternalAdbcCommonErrorGetDetail(&error, -1);
  ASSERT_EQ(nullptr, detail.key);
  ASSERT_EQ(nullptr, detail.value);
  ASSERT_EQ(0, detail.value_length);

  detail = InternalAdbcCommonErrorGetDetail(&error, 2);
  ASSERT_EQ(nullptr, detail.key);
  ASSERT_EQ(nullptr, detail.value);
  ASSERT_EQ(0, detail.value_length);

  error.release(&error);
  ASSERT_EQ(nullptr, error.private_data);
  ASSERT_EQ(nullptr, error.private_driver);
}

TEST(ErrorDetails, RoundTripValues) {
  struct AdbcError error = ADBC_ERROR_INIT;
  InternalAdbcSetError(&error, "My message");

  struct Detail {
    std::string key;
    std::vector<uint8_t> value;
  };

  std::vector<Detail> details = {
      {"x-key-1", {0, 1, 2, 3}}, {"x-key-2", {1, 1}}, {"x-key-3", {128, 129, 200, 0, 1}},
      {"x-key-4", {97, 98, 99}}, {"x-key-5", {42}},
  };

  for (const auto& detail : details) {
    InternalAdbcAppendErrorDetail(&error, detail.key.c_str(), detail.value.data(),
                                  detail.value.size());
  }

  ASSERT_EQ(details.size(), InternalAdbcCommonErrorGetDetailCount(&error));
  for (int i = 0; i < static_cast<int>(details.size()); i++) {
    struct AdbcErrorDetail detail = InternalAdbcCommonErrorGetDetail(&error, i);
    ASSERT_EQ(details[i].key, detail.key);
    ASSERT_EQ(details[i].value.size(), detail.value_length);
    ASSERT_THAT(std::vector<uint8_t>(detail.value, detail.value + detail.value_length),
                ::testing::ElementsAreArray(details[i].value));
  }

  error.release(&error);
}

// https://github.com/apache/arrow-adbc/issues/1100
TEST(AdbcGetObjectsData, GetObjectsByName) {
  // Mock objects where the names being compared share the same prefix
  struct AdbcGetObjectsData mock_data;
  struct AdbcGetObjectsCatalog mock_catalog;
  struct AdbcGetObjectsSchema mock_schema;
  struct AdbcGetObjectsTable mock_table, mock_table_suffix;
  struct AdbcGetObjectsColumn mock_column, mock_column_suffix;
  struct AdbcGetObjectsConstraint mock_constraint, mock_constraint_suffix;

  mock_catalog.catalog_name = {"mock_catalog", (int64_t)strlen("mock_catalog")};
  mock_schema.db_schema_name = {"mock_schema", (int64_t)strlen("mock_schema")};
  mock_table.table_name = {"table", (int64_t)strlen("table")};
  mock_table_suffix.table_name = {"table_suffix", (int64_t)strlen("table_suffix")};
  mock_column.column_name = {"column", (int64_t)strlen("column")};
  mock_column_suffix.column_name = {"column_suffix", (int64_t)strlen("column_suffix")};
  mock_constraint.constraint_name = {"constraint", (int64_t)strlen("constraint")};
  mock_constraint_suffix.constraint_name = {"constraint_suffix",
                                            (int64_t)strlen("constraint_suffix")};

  struct AdbcGetObjectsCatalog* catalogs[] = {&mock_catalog};
  mock_data.catalogs = catalogs;
  mock_data.n_catalogs = 1;

  struct AdbcGetObjectsSchema* schemas[] = {&mock_schema};
  mock_catalog.catalog_db_schemas = schemas;
  mock_catalog.n_db_schemas = 1;

  struct AdbcGetObjectsTable* tables[] = {&mock_table, &mock_table_suffix};
  mock_schema.db_schema_tables = tables;
  mock_schema.n_db_schema_tables = 2;

  struct AdbcGetObjectsColumn* columns[] = {&mock_column, &mock_column_suffix};
  mock_table.table_columns = columns;
  mock_table.n_table_columns = 2;

  struct AdbcGetObjectsConstraint* constraints[] = {&mock_constraint,
                                                    &mock_constraint_suffix};
  mock_table.table_constraints = constraints;
  mock_table.n_table_constraints = 2;

  EXPECT_EQ(InternalAdbcGetObjectsDataGetTableByName(&mock_data, "mock_catalog",
                                                     "mock_schema", "table"),
            &mock_table);
  EXPECT_EQ(InternalAdbcGetObjectsDataGetTableByName(&mock_data, "mock_catalog",
                                                     "mock_schema", "table_suffix"),
            &mock_table_suffix);
  EXPECT_EQ(InternalAdbcGetObjectsDataGetTableByName(&mock_data, "mock_catalog",
                                                     "mock_schema", "nonexistent"),
            nullptr);

  EXPECT_EQ(InternalAdbcGetObjectsDataGetCatalogByName(&mock_data, "mock_catalog"),
            &mock_catalog);
  EXPECT_EQ(InternalAdbcGetObjectsDataGetCatalogByName(&mock_data, "nonexistent"),
            nullptr);

  EXPECT_EQ(InternalAdbcGetObjectsDataGetSchemaByName(&mock_data, "mock_catalog",
                                                      "mock_schema"),
            &mock_schema);
  EXPECT_EQ(InternalAdbcGetObjectsDataGetSchemaByName(&mock_data, "mock_catalog",
                                                      "nonexistent"),
            nullptr);

  EXPECT_EQ(InternalAdbcGetObjectsDataGetColumnByName(&mock_data, "mock_catalog",
                                                      "mock_schema", "table", "column"),
            &mock_column);
  EXPECT_EQ(InternalAdbcGetObjectsDataGetColumnByName(
                &mock_data, "mock_catalog", "mock_schema", "table", "column_suffix"),
            &mock_column_suffix);
  EXPECT_EQ(InternalAdbcGetObjectsDataGetColumnByName(
                &mock_data, "mock_catalog", "mock_schema", "table", "nonexistent"),
            nullptr);

  EXPECT_EQ(InternalAdbcGetObjectsDataGetConstraintByName(
                &mock_data, "mock_catalog", "mock_schema", "table", "constraint"),
            &mock_constraint);
  EXPECT_EQ(InternalAdbcGetObjectsDataGetConstraintByName(
                &mock_data, "mock_catalog", "mock_schema", "table", "constraint_suffix"),
            &mock_constraint_suffix);
  EXPECT_EQ(InternalAdbcGetObjectsDataGetConstraintByName(
                &mock_data, "mock_catalog", "mock_schema", "table", "nonexistent"),
            nullptr);
}
