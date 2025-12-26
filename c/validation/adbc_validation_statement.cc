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

#include <cstring>
#include <string>
#include <utility>
#include <vector>

#include <arrow-adbc/adbc.h>
#include <gmock/gmock.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>
#include <nanoarrow/nanoarrow.hpp>

#include "adbc_validation_util.h"
#include "common/options.h"

namespace adbc_validation {

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
  if (connection.private_data) {
    EXPECT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
  }
  if (database.private_data) {
    EXPECT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));
  }
  if (error.release) {
    error.release(&error);
  }
}

void StatementTest::ResetTest() {
  TearDownTest();
  SetUpTest();
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
void StatementTest::TestSqlIngestType(SchemaField field,
                                      const std::vector<std::optional<CType>>& values,
                                      bool dictionary_encode) {
  // Override the field name
  field.name = "col";

  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(MakeSchema(&schema.value, {field}), IsOkErrno());
  ASSERT_THAT(MakeBatch<CType>(&schema.value, &array.value, &na_error, values),
              IsOkErrno());

  if (dictionary_encode) {
    // Create a dictionary-encoded version of the target schema
    Handle<struct ArrowSchema> dict_schema;
    ASSERT_THAT(ArrowSchemaInitFromType(&dict_schema.value, NANOARROW_TYPE_INT32),
                IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetName(&dict_schema.value, schema.value.children[0]->name),
                IsOkErrno());
    ASSERT_THAT(ArrowSchemaSetName(schema.value.children[0], nullptr), IsOkErrno());

    // Swap it into the target schema
    ASSERT_THAT(ArrowSchemaAllocateDictionary(&dict_schema.value), IsOkErrno());
    ArrowSchemaMove(schema.value.children[0], dict_schema.value.dictionary);
    ArrowSchemaMove(&dict_schema.value, schema.value.children[0]);

    // Create a dictionary-encoded array with easy 0...n indices so that the
    // matched values will be the same.
    Handle<struct ArrowArray> dict_array;
    ASSERT_THAT(ArrowArrayInitFromType(&dict_array.value, NANOARROW_TYPE_INT32),
                IsOkErrno());
    ASSERT_THAT(ArrowArrayStartAppending(&dict_array.value), IsOkErrno());
    for (size_t i = 0; i < values.size(); i++) {
      ASSERT_THAT(ArrowArrayAppendInt(&dict_array.value, static_cast<int64_t>(i)),
                  IsOkErrno());
    }
    ASSERT_THAT(ArrowArrayFinishBuildingDefault(&dict_array.value, nullptr), IsOkErrno());

    // Swap it into the target batch
    ASSERT_THAT(ArrowArrayAllocateDictionary(&dict_array.value), IsOkErrno());
    ArrowArrayMove(array.value.children[0], dict_array.value.dictionary);
    ArrowArrayMove(&dict_array.value, array.value.children[0]);
  }

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

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &statement, "SELECT * FROM \"bulk_ingest\" ORDER BY \"col\" ASC NULLS FIRST",
          &error),
      IsOkStatus(&error));
  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(values.size()), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    SchemaField round_trip_field = quirks()->IngestSelectRoundTripType(field);
    ASSERT_NO_FATAL_FAILURE(CompareSchema(&reader.schema.value, {round_trip_field}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(values.size(), reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    if (round_trip_field.type == field.type) {
      // XXX: for now we can't compare values; we would need casting
      ASSERT_NO_FATAL_FAILURE(
          CompareArray<CType>(reader.array_view->children[0], values));
    }

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }
  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
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
  } else if (type == ArrowType::NANOARROW_TYPE_DATE32) {
    // Windows does not seem to support negative date values
    values.push_back(static_cast<CType>(0));
    values.push_back(static_cast<CType>(42));
  } else {
    values.push_back(std::numeric_limits<CType>::lowest());
    values.push_back(std::numeric_limits<CType>::max());
  }

  return TestSqlIngestType(type, values, false);
}

void StatementTest::TestSqlIngestBool() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<bool>(NANOARROW_TYPE_BOOL));
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

void StatementTest::TestSqlIngestFloat16() {
  if (!quirks()->supports_ingest_float16()) {
    GTEST_SKIP();
  }

  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<float>(NANOARROW_TYPE_HALF_FLOAT));
}

void StatementTest::TestSqlIngestFloat32() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<float>(NANOARROW_TYPE_FLOAT));
}

void StatementTest::TestSqlIngestFloat64() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<double>(NANOARROW_TYPE_DOUBLE));
}

void StatementTest::TestSqlIngestString() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::string>(
      NANOARROW_TYPE_STRING, {std::nullopt, "", "", "1234", "例"}, false));
}

void StatementTest::TestSqlIngestLargeString() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::string>(
      NANOARROW_TYPE_LARGE_STRING, {std::nullopt, "", "", "1234", "例"}, false));
}

void StatementTest::TestSqlIngestStringView() {
  if (!quirks()->supports_ingest_view_types()) {
    GTEST_SKIP();
  }

  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::string>(
      NANOARROW_TYPE_STRING_VIEW, {std::nullopt, "", "", "longer than 12 bytes", "例"},
      false));
}

void StatementTest::TestSqlIngestBinary() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::vector<std::byte>>(
      NANOARROW_TYPE_BINARY,
      {std::nullopt, std::vector<std::byte>{},
       std::vector<std::byte>{std::byte{0x00}, std::byte{0x01}},
       std::vector<std::byte>{std::byte{0x01}, std::byte{0x02}, std::byte{0x03},
                              std::byte{0x04}},
       std::vector<std::byte>{std::byte{0xfe}, std::byte{0xff}}},
      false));
}

void StatementTest::TestSqlIngestLargeBinary() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::vector<std::byte>>(
      NANOARROW_TYPE_LARGE_BINARY,
      {std::nullopt, std::vector<std::byte>{},
       std::vector<std::byte>{std::byte{0x00}, std::byte{0x01}},
       std::vector<std::byte>{std::byte{0x01}, std::byte{0x02}, std::byte{0x03},
                              std::byte{0x04}},
       std::vector<std::byte>{std::byte{0xfe}, std::byte{0xff}}},
      false));
}

void StatementTest::TestSqlIngestFixedSizeBinary() {
  SchemaField field = SchemaField::FixedSize("col", NANOARROW_TYPE_FIXED_SIZE_BINARY, 4);
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::string>(
      field, {std::nullopt, "abcd", "efgh", "ijkl", "mnop"}, false));
}

void StatementTest::TestSqlIngestBinaryView() {
  if (!quirks()->supports_ingest_view_types()) {
    GTEST_SKIP();
  }

  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::vector<std::byte>>(
      NANOARROW_TYPE_LARGE_BINARY,
      {std::nullopt, std::vector<std::byte>{},
       std::vector<std::byte>{std::byte{0x00}, std::byte{0x01}},
       std::vector<std::byte>{std::byte{0x01}, std::byte{0x02}, std::byte{0x03},
                              std::byte{0x04}},
       std::vector<std::byte>{std::byte{0xfe}, std::byte{0xff}}},
      false));
}

void StatementTest::TestSqlIngestDate32() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestNumericType<int32_t>(NANOARROW_TYPE_DATE32));
}

template <ArrowType type, enum ArrowTimeUnit TU>
void StatementTest::TestSqlIngestTemporalType(const char* timezone) {
  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  const std::vector<std::optional<int64_t>> values = {std::nullopt, -42, 0, 42};

  // much of this code is shared with TestSqlIngestType with minor
  // changes to allow for various time units to be tested
  ArrowSchemaInit(&schema.value);
  ArrowSchemaSetTypeStruct(&schema.value, 1);
  ArrowSchemaSetTypeDateTime(schema->children[0], type, TU, timezone);
  ArrowSchemaSetName(schema->children[0], "col");
  ASSERT_THAT(MakeBatch<int64_t>(&schema.value, &array.value, &na_error, values),
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

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &statement, "SELECT * FROM \"bulk_ingest\" ORDER BY \"col\" ASC NULLS FIRST",
          &error),
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

    ValidateIngestedTemporalData(reader.array_view->children[0], type, TU, timezone);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

void StatementTest::ValidateIngestedTemporalData(struct ArrowArrayView* values,
                                                 ArrowType type, enum ArrowTimeUnit unit,
                                                 const char* timezone) {
  FAIL() << "ValidateIngestedTemporalData is not implemented in the base class";
}

void StatementTest::TestSqlIngestDuration() {
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_DURATION, NANOARROW_TIME_UNIT_SECOND>(
          nullptr)));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_DURATION, NANOARROW_TIME_UNIT_MILLI>(
          nullptr)));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_DURATION, NANOARROW_TIME_UNIT_MICRO>(
          nullptr)));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_DURATION, NANOARROW_TIME_UNIT_NANO>(
          nullptr)));
}

void StatementTest::TestSqlIngestTimestamp() {
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_SECOND>(
          nullptr)));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MILLI>(
          nullptr)));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MICRO>(
          nullptr)));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_NANO>(
          nullptr)));
}

void StatementTest::TestSqlIngestTimestampTz() {
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_SECOND>(
          "UTC")));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MILLI>(
          "UTC")));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MICRO>(
          "UTC")));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_NANO>(
          "UTC")));

  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_SECOND>(
          "America/Los_Angeles")));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MILLI>(
          "America/Los_Angeles")));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_MICRO>(
          "America/Los_Angeles")));
  ASSERT_NO_FATAL_FAILURE(
      (TestSqlIngestTemporalType<NANOARROW_TYPE_TIMESTAMP, NANOARROW_TIME_UNIT_NANO>(
          "America/Los_Angeles")));
}

void StatementTest::TestSqlIngestInterval() {
  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  const enum ArrowType type = NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO;
  // values are days, months, ns
  struct ArrowInterval neg_interval;
  struct ArrowInterval zero_interval;
  struct ArrowInterval pos_interval;

  ArrowIntervalInit(&neg_interval, type);
  ArrowIntervalInit(&zero_interval, type);
  ArrowIntervalInit(&pos_interval, type);

  neg_interval.months = -5;
  neg_interval.days = -5;
  neg_interval.ns = -42000;

  pos_interval.months = 5;
  pos_interval.days = 5;
  pos_interval.ns = 42000;

  const std::vector<std::optional<ArrowInterval*>> values = {
      std::nullopt, &neg_interval, &zero_interval, &pos_interval};

  ASSERT_THAT(MakeSchema(&schema.value, {{"col", type}}), IsOkErrno());

  ASSERT_THAT(MakeBatch<ArrowInterval*>(&schema.value, &array.value, &na_error, values),
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

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &statement, "SELECT * FROM \"bulk_ingest\" ORDER BY \"col\" ASC NULLS FIRST",
          &error),
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
      ASSERT_NO_FATAL_FAILURE(
          CompareArray<ArrowInterval*>(reader.array_view->children[0], values));
    }

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }
  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlIngestStringDictionary() {
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::string>(NANOARROW_TYPE_STRING,
                                                         {"", "", "1234", "例"},
                                                         /*dictionary_encode*/ true));
}

void StatementTest::TestSqlIngestListOfInt32() {
  SchemaField field =
      SchemaField::Nested("col", NANOARROW_TYPE_LIST, {{"item", NANOARROW_TYPE_INT32}});
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::vector<int32_t>>(
      field, {std::nullopt, std::vector<int32_t>{1, 2, 3}, std::vector<int32_t>{4, 5}},
      /*dictionary_encode*/ false));
}

void StatementTest::TestSqlIngestListOfString() {
  SchemaField field =
      SchemaField::Nested("col", NANOARROW_TYPE_LIST, {{"item", NANOARROW_TYPE_STRING}});
  ASSERT_NO_FATAL_FAILURE(TestSqlIngestType<std::vector<std::string>>(
      field,
      {std::nullopt, std::vector<std::string>{"abc", "defg"},
       std::vector<std::string>{"hijk"}},
      /*dictionary_encode*/ false));
}

void StatementTest::TestSqlIngestStreamZeroArrays() {
  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  ASSERT_THAT(MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT32}}), IsOkErrno());

  Handle<struct ArrowArrayStream> bind;
  nanoarrow::EmptyArrayStream(&schema.value).ToArrayStream(&bind.value);

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBindStream(&statement, &bind.value, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "SELECT * FROM \"bulk_ingest\"", &error),
      IsOkStatus(&error));

  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(0), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ArrowType round_trip_type = quirks()->IngestSelectRoundTripType(NANOARROW_TYPE_INT32);
    ASSERT_NO_FATAL_FAILURE(
        CompareSchema(&reader.schema.value, {{"col", round_trip_type, NULLABLE}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }
}

void StatementTest::TestSqlIngestTableEscaping() {
  std::string name = "create_table_escaping";

  ASSERT_THAT(quirks()->DropTable(&connection, name, &error), IsOkStatus(&error));
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(MakeSchema(&schema.value, {{"index", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                  {42, -42, std::nullopt})),
              IsOkErrno());

  Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     name.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlIngestColumnEscaping() {
  std::string name = "create";

  ASSERT_THAT(quirks()->DropTable(&connection, name, &error), IsOkStatus(&error));
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(MakeSchema(&schema.value, {{"index", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                  {42, -42, std::nullopt})),
              IsOkErrno());

  Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     name.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlIngestAppend() {
  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE) ||
      !quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_APPEND)) {
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
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "SELECT * FROM \"bulk_ingest\"", &error),
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

void StatementTest::TestSqlIngestReplace() {
  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_REPLACE)) {
    GTEST_SKIP();
  }

  // Ingest

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
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                     ADBC_INGEST_OPTION_MODE_REPLACE, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));

  int64_t rows_affected = 0;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

  // Read data back
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "SELECT * FROM \"bulk_ingest\"", &error),
      IsOkStatus(&error));
  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CompareSchema(&reader.schema.value,
                                          {{"int64s", NANOARROW_TYPE_INT64, NULLABLE}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(1, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(reader.array_view->children[0], {42}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  // Replace
  // Re-initialize since Bind() should take ownership of data
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT(MakeBatch<int64_t>(&schema.value, &array.value, &na_error, {-42, -42}),
              IsOkErrno());

  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                     ADBC_INGEST_OPTION_MODE_REPLACE, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(2), ::testing::Eq(-1)));

  // Read data back
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "SELECT * FROM \"bulk_ingest\"", &error),
      IsOkStatus(&error));
  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(2), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(CompareSchema(&reader.schema.value,
                                          {{"int64s", NANOARROW_TYPE_INT64, NULLABLE}}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(2, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_NO_FATAL_FAILURE(
        CompareArray<int64_t>(reader.array_view->children[0], {-42, -42}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }
}

void StatementTest::TestSqlIngestCreateAppend() {
  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE_APPEND)) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  // Ingest

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
  ASSERT_THAT(AdbcStatementSetOption(&statement, ADBC_INGEST_OPTION_MODE,
                                     ADBC_INGEST_OPTION_MODE_CREATE_APPEND, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));

  int64_t rows_affected = 0;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));

  // Append
  // Re-initialize since Bind() should take ownership of data
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT(MakeBatch<int64_t>(&schema.value, &array.value, &na_error, {42, 42}),
              IsOkErrno());

  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(2), ::testing::Eq(-1)));

  // Read data back
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "SELECT * FROM \"bulk_ingest\"", &error),
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
        CompareArray<int64_t>(reader.array_view->children[0], {42, 42, 42}));

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(nullptr, reader.array->release);
  }

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlIngestErrors() {
  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
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

  if (!quirks()->supports_error_on_incompatible_schema()) {
    return;
  }

  // ...then try to append an incompatible schema
  ASSERT_THAT(MakeSchema(&schema.value, {{"int64s", NANOARROW_TYPE_INT64},
                                         {"coltwo", NANOARROW_TYPE_INT64}}),
              IsOkErrno());
  ASSERT_THAT(
      (MakeBatch<int64_t, int64_t>(&schema.value, &array.value, &na_error, {-42}, {-42})),
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
  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
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
            &statement,
            "SELECT * FROM \"bulk_ingest\" ORDER BY \"int64s\" DESC NULLS LAST", &error),
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
  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->EnsureSampleTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &statement, "SELECT * FROM \"bulk_ingest\" ORDER BY int64s ASC NULLS FIRST",
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

void StatementTest::TestSqlIngestTargetCatalog() {
  if (!quirks()->supports_bulk_ingest_catalog() ||
      !quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    GTEST_SKIP();
  }

  std::string catalog = quirks()->catalog();
  std::string name = "bulk_ingest";

  ASSERT_THAT(quirks()->DropTable(&connection, name, &error), IsOkStatus(&error));
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(MakeSchema(&schema.value, {{"ints", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                  {42, -42, std::nullopt})),
              IsOkErrno());

  Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_CATALOG,
                                     catalog.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     name.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlIngestTargetSchema() {
  if (!quirks()->supports_bulk_ingest_db_schema() ||
      !quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    GTEST_SKIP();
  }

  std::string db_schema = quirks()->db_schema();
  std::string name = "bulk_ingest";

  ASSERT_THAT(quirks()->DropTable(&connection, name, &error), IsOkStatus(&error));
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(MakeSchema(&schema.value, {{"ints", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                  {42, -42, std::nullopt})),
              IsOkErrno());

  Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(
      AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
                             db_schema.c_str(), &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     name.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlIngestTargetCatalogSchema() {
  if (!quirks()->supports_bulk_ingest_catalog() ||
      !quirks()->supports_bulk_ingest_db_schema() ||
      !quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    GTEST_SKIP();
  }

  std::string catalog = quirks()->catalog();
  std::string db_schema = quirks()->db_schema();
  std::string name = "bulk_ingest";

  ASSERT_THAT(quirks()->DropTable(&connection, name, &error), IsOkStatus(&error));
  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(MakeSchema(&schema.value, {{"ints", NANOARROW_TYPE_INT64}}), IsOkErrno());
  ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                  {42, -42, std::nullopt})),
              IsOkErrno());

  Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_CATALOG,
                                     catalog.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(
      AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
                             db_schema.c_str(), &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                     name.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlIngestTemporary() {
  if (!quirks()->supports_bulk_ingest_temporary() ||
      !quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    GTEST_SKIP();
  }

  Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
              IsOkStatus(&error));

  std::string name = "bulk_ingest";

  ASSERT_THAT(quirks()->DropTable(&connection, name, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->DropTempTable(&connection, name, &error), IsOkStatus(&error));

  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"ints", NANOARROW_TYPE_INT64}}), IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                    {42, -42, std::nullopt})),
                IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_ENABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"ints", NANOARROW_TYPE_INT64}}), IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                    {42, -42, std::nullopt})),
                IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_DISABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlIngestTemporaryAppend() {
  // Append to temp table shouldn't affect actual table and vice versa
  if (!quirks()->supports_bulk_ingest_temporary() ||
      !quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE) ||
      !quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_APPEND)) {
    GTEST_SKIP();
  }

  Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
              IsOkStatus(&error));

  std::string name = "bulk_ingest";

  ASSERT_THAT(quirks()->DropTable(&connection, name, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->DropTempTable(&connection, name, &error), IsOkStatus(&error));

  // Create both tables with different schemas
  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"ints", NANOARROW_TYPE_INT64}}), IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                    {42, -42, std::nullopt})),
                IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_ENABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"strs", NANOARROW_TYPE_STRING}}),
                IsOkErrno());
    ASSERT_THAT((MakeBatch<std::string>(&schema.value, &array.value, &na_error,
                                        {"foo", "bar", std::nullopt})),
                IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_DISABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  // Append to the temporary table
  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"ints", NANOARROW_TYPE_INT64}}), IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error, {0, 1, 2})),
                IsOkErrno());

    Handle<struct AdbcStatement> statement2;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement2.value, &error),
                IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementSetOption(&statement2.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_ENABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement2.value, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_APPEND, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement2.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement2.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement2.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  // Append to the normal table
  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"strs", NANOARROW_TYPE_STRING}}),
                IsOkErrno());
    ASSERT_THAT(
        (MakeBatch<std::string>(&schema.value, &array.value, &na_error, {"", "a", "b"})),
        IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_DISABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_APPEND, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlIngestTemporaryReplace() {
  // Replace temp table shouldn't affect actual table and vice versa
  if (!quirks()->supports_bulk_ingest_temporary() ||
      !quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE) ||
      !quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_APPEND) ||
      !quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_REPLACE)) {
    GTEST_SKIP();
  }

  Handle<struct AdbcStatement> statement;
  ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
              IsOkStatus(&error));

  std::string name = "bulk_ingest";

  ASSERT_THAT(quirks()->DropTable(&connection, name, &error), IsOkStatus(&error));
  ASSERT_THAT(quirks()->DropTempTable(&connection, name, &error), IsOkStatus(&error));

  // Create both tables with different schemas
  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"ints", NANOARROW_TYPE_INT64}}), IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                    {42, -42, std::nullopt})),
                IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_ENABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"strs", NANOARROW_TYPE_STRING}}),
                IsOkErrno());
    ASSERT_THAT((MakeBatch<std::string>(&schema.value, &array.value, &na_error,
                                        {"foo", "bar", std::nullopt})),
                IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_DISABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  // Replace both tables with different schemas
  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"ints2", NANOARROW_TYPE_INT64},
                                           {"strs2", NANOARROW_TYPE_STRING}}),
                IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t, std::string>(&schema.value, &array.value, &na_error,
                                                 {0, 1, std::nullopt},
                                                 {"foo", "bar", std::nullopt})),
                IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_ENABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_REPLACE, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"ints3", NANOARROW_TYPE_INT64}}),
                IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error, {1, 2, 3})),
                IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_DISABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_REPLACE, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  // Now append to the replaced tables to check that the schemas are as expected
  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"ints2", NANOARROW_TYPE_INT64},
                                           {"strs2", NANOARROW_TYPE_STRING}}),
                IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t, std::string>(&schema.value, &array.value, &na_error,
                                                 {0, 1, std::nullopt},
                                                 {"foo", "bar", std::nullopt})),
                IsOkErrno());

    Handle<struct AdbcStatement> statement2;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement2.value, &error),
                IsOkStatus(&error));

    ASSERT_THAT(AdbcStatementSetOption(&statement2.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_ENABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement2.value, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_APPEND, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement2.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement2.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement2.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"ints3", NANOARROW_TYPE_INT64}}),
                IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error, {4, 5, 6})),
                IsOkErrno());

    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_DISABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_APPEND, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
  }

  ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlIngestTemporaryExclusive() {
  // Can't set target schema/catalog with temp table
  if (!quirks()->supports_bulk_ingest_temporary() ||
      !quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE)) {
    GTEST_SKIP();
  }

  std::string name = "bulk_ingest";
  ASSERT_THAT(quirks()->DropTempTable(&connection, name, &error), IsOkStatus(&error));

  if (quirks()->supports_bulk_ingest_catalog()) {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"ints", NANOARROW_TYPE_INT64}}), IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                    {42, -42, std::nullopt})),
                IsOkErrno());

    std::string catalog = quirks()->catalog();

    Handle<struct AdbcStatement> statement;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_ENABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(
        AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_CATALOG,
                               catalog.c_str(), &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsStatus(ADBC_STATUS_INVALID_STATE, &error));
    ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
  }

  if (quirks()->supports_bulk_ingest_db_schema()) {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"ints", NANOARROW_TYPE_INT64}}), IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                    {42, -42, std::nullopt})),
                IsOkErrno());

    std::string db_schema = quirks()->db_schema();

    Handle<struct AdbcStatement> statement;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TEMPORARY,
                                       ADBC_OPTION_VALUE_ENABLED, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(
        AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_DB_SCHEMA,
                               db_schema.c_str(), &error),
        IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsStatus(ADBC_STATUS_INVALID_STATE, &error));
    ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
  }
}

void StatementTest::TestSqlIngestPrimaryKey() {
  std::string name = "pkeytest";
  auto ddl = quirks()->PrimaryKeyIngestTableDdl(name);
  if (!ddl) {
    GTEST_SKIP();
  }
  ASSERT_THAT(quirks()->DropTable(&connection, name, &error), IsOkStatus(&error));

  // Create table
  {
    Handle<struct AdbcStatement> statement;
    StreamReader reader;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement.value, ddl->c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
  }

  // Ingest without the primary key
  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value, {{"value", NANOARROW_TYPE_INT64}}),
                IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                    {42, -42, std::nullopt})),
                IsOkErrno());

    Handle<struct AdbcStatement> statement;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_APPEND, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
  }

  // Ingest with the primary key
  {
    Handle<struct ArrowSchema> schema;
    Handle<struct ArrowArray> array;
    struct ArrowError na_error;
    ASSERT_THAT(MakeSchema(&schema.value,
                           {
                               {"id", NANOARROW_TYPE_INT64},
                               {"value", NANOARROW_TYPE_INT64},
                           }),
                IsOkErrno());
    ASSERT_THAT((MakeBatch<int64_t, int64_t>(&schema.value, &array.value, &na_error,
                                             {4, 5, 6}, {1, 0, -1})),
                IsOkErrno());

    Handle<struct AdbcStatement> statement;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_TARGET_TABLE,
                                       name.c_str(), &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetOption(&statement.value, ADBC_INGEST_OPTION_MODE,
                                       ADBC_INGEST_OPTION_MODE_APPEND, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementBind(&statement.value, &array.value, &schema.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, nullptr, nullptr, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementRelease(&statement.value, &error), IsOkStatus(&error));
  }

  // Get the data
  {
    Handle<struct AdbcStatement> statement;
    StreamReader reader;
    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetSqlQuery(
                    &statement.value, "SELECT * FROM pkeytest ORDER BY id ASC", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, &reader.stream.value, nullptr,
                                          &error),
                IsOkStatus(&error));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_EQ(2, reader.schema->n_children);
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(6, reader.array->length);
    ASSERT_EQ(2, reader.array->n_children);

    // Different databases start numbering at 0 or 1 for the primary key
    // column, so can't compare it
    // TODO(https://github.com/apache/arrow-adbc/issues/938): if the test
    // helpers converted data to plain C++ values we could do a more
    // sophisticated assertion
    ASSERT_NO_FATAL_FAILURE(CompareArray<int64_t>(reader.array_view->children[1],
                                                  {42, -42, std::nullopt, 1, 0, -1}));
  }
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
  if (quirks()->supports_rows_affected()) {
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));
  } else {
    ASSERT_THAT(reader.rows_affected,
                ::testing::Not(::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1))));
  }

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
  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE) ||
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
      "INSERT INTO \"bulk_ingest\" VALUES (" + quirks()->BindParameter(0) + ")";
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
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "SELECT * FROM \"bulk_ingest\"", &error),
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
  if (!quirks()->supports_bulk_ingest(ADBC_INGEST_OPTION_MODE_CREATE) ||
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
      "INSERT INTO \"bulk_ingest\" VALUES (" + quirks()->BindParameter(0) + ")";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, query.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));

  // Bind and execute
  ASSERT_THAT(AdbcStatementBindStream(&statement, &stream.value, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  // Read data back
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "SELECT * FROM \"bulk_ingest\"", &error),
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

void StatementTest::TestSqlBind() {
  if (!quirks()->supports_dynamic_parameter_binding()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  ASSERT_THAT(quirks()->DropTable(&connection, "bindtest", &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementSetSqlQuery(
                  &statement, "CREATE TABLE bindtest (col1 INTEGER, col2 TEXT)", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  Handle<struct ArrowSchema> schema;
  Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_THAT(MakeSchema(&schema.value,
                         {{"", NANOARROW_TYPE_INT32}, {"", NANOARROW_TYPE_STRING}}),
              IsOkErrno());

  std::vector<std::optional<int32_t>> int_values{std::nullopt, -123, 123};
  std::vector<std::optional<std::string>> string_values{"abc", std::nullopt, "defg"};

  int batch_result = MakeBatch<int32_t, std::string>(
      &schema.value, &array.value, &na_error, int_values, string_values);
  ASSERT_THAT(batch_result, IsOkErrno());

  auto insert_query = std::string("INSERT INTO bindtest VALUES (") +
                      quirks()->BindParameter(0) + ", " + quirks()->BindParameter(1) +
                      ")";
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, insert_query.c_str(), &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementPrepare(&statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementBind(&statement, &array.value, &schema.value, &error),
              IsOkStatus(&error));
  int64_t rows_affected = -10;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(-1), ::testing::Eq(3)));

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &statement, "SELECT * FROM bindtest ORDER BY col1 ASC NULLS FIRST", &error),
      IsOkStatus(&error));
  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->length, 3);
    CompareArray(reader.array_view->children[0], int_values);
    CompareArray(reader.array_view->children[1], string_values);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_EQ(reader.array->release, nullptr);
  }
}

void StatementTest::TestSqlQueryEmpty() {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  ASSERT_THAT(quirks()->DropTable(&connection, "queryempty", &error), IsOkStatus(&error));
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "CREATE TABLE queryempty (FOO INT)", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "SELECT * FROM queryempty WHERE 1=0", &error),
      IsOkStatus(&error));
  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_THAT(reader.rows_affected,
                ::testing::AnyOf(::testing::Eq(0), ::testing::Eq(-1)));

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_EQ(1, reader.schema->n_children);

    while (true) {
      ASSERT_NO_FATAL_FAILURE(reader.Next());
      if (!reader.array->release) {
        break;
      }
      ASSERT_EQ(0, reader.array->length);
    }
  }
  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
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
    if (quirks()->supports_rows_affected()) {
      ASSERT_THAT(reader.rows_affected,
                  ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));
    } else {
      ASSERT_THAT(reader.rows_affected,
                  ::testing::Not(::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1))));
    }

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
    if (quirks()->supports_rows_affected()) {
      ASSERT_THAT(reader.rows_affected,
                  ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));
    } else {
      ASSERT_THAT(reader.rows_affected,
                  ::testing::Not(::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1))));
    }

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
    if (quirks()->supports_rows_affected()) {
      ASSERT_THAT(reader.rows_affected,
                  ::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1)));
    } else {
      ASSERT_THAT(reader.rows_affected,
                  ::testing::Not(::testing::AnyOf(::testing::Eq(1), ::testing::Eq(-1))));
    }

    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
    ASSERT_EQ(1, reader.schema->n_children);

    ASSERT_NO_FATAL_FAILURE(reader.Next());
    ASSERT_NE(nullptr, reader.array->release);
    ASSERT_EQ(1, reader.array->length);
    ASSERT_EQ(1, reader.array->n_children);

    ASSERT_FALSE(ArrowArrayViewIsNull(&reader.array_view.value, 0));
    ASSERT_FALSE(ArrowArrayViewIsNull(reader.array_view->children[0], 0));
    switch (reader.fields[0].type) {
      case NANOARROW_TYPE_LARGE_STRING:
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

void StatementTest::TestSqlQueryInsertRollback() {
  if (!quirks()->supports_transactions()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "rollbacktest", &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement,
                                       "CREATE TABLE \"rollbacktest\" (a INT)", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionCommit(&connection, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementSetSqlQuery(
                  &statement, "INSERT INTO \"rollbacktest\" (a) VALUES (1)", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionRollback(&connection, &error), IsOkStatus(&error));

  adbc_validation::StreamReader reader;
  ASSERT_THAT(
      AdbcStatementSetSqlQuery(&statement, "SELECT * FROM \"rollbacktest\"", &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));

  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());

  int64_t total_rows = 0;
  while (true) {
    ASSERT_NO_FATAL_FAILURE(reader.Next());
    if (!reader.array->release) break;

    total_rows += reader.array->length;
  }

  ASSERT_EQ(0, total_rows);
}

void StatementTest::TestSqlQueryCancel() {
  if (!quirks()->supports_cancel()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 'SaShiSuSeSo'", &error),
              IsOkStatus(&error));

  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());

    ASSERT_THAT(AdbcStatementCancel(&statement, &error), IsOkStatus(&error));
    while (true) {
      int err = reader.MaybeNext();
      if (err != 0) {
        ASSERT_THAT(err, ::testing::AnyOf(0, IsErrno(ECANCELED, &reader.stream.value,
                                                     /*ArrowError*/ nullptr)));
      }
      if (!reader.array->release) break;
    }
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

void StatementTest::TestSqlQueryTrailingSemicolons() {
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT current_date;;;", &error),
              IsOkStatus(&error));

  {
    StreamReader reader;
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
  }

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlQueryRowsAffectedDelete() {
  ASSERT_THAT(quirks()->DropTable(&connection, "delete_test", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement,
                                       "CREATE TABLE \"delete_test\" (foo INT)", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &statement, "INSERT INTO \"delete_test\" (foo) VALUES (1), (2), (3), (4), (5)",
          &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementSetSqlQuery(
                  &statement, "DELETE FROM \"delete_test\" WHERE foo >= 3", &error),
              IsOkStatus(&error));

  int64_t rows_affected = 0;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, &rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_THAT(rows_affected, ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));
}

void StatementTest::TestSqlQueryRowsAffectedDeleteStream() {
  ASSERT_THAT(quirks()->DropTable(&connection, "delete_test", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement,
                                       "CREATE TABLE \"delete_test\" (foo INT)", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(
      AdbcStatementSetSqlQuery(
          &statement, "INSERT INTO \"delete_test\" (foo) VALUES (1), (2), (3), (4), (5)",
          &error),
      IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, nullptr, nullptr, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcStatementSetSqlQuery(
                  &statement, "DELETE FROM \"delete_test\" WHERE foo >= 3", &error),
              IsOkStatus(&error));

  adbc_validation::StreamReader reader;
  ASSERT_THAT(
      AdbcStatementExecuteQuery(&statement, nullptr, &reader.rows_affected, &error),
      IsOkStatus(&error));
  ASSERT_THAT(reader.rows_affected,
              ::testing::AnyOf(::testing::Eq(3), ::testing::Eq(-1)));
}

void StatementTest::TestTransactions() {
  if (!quirks()->supports_transactions() || quirks()->ddl_implicit_commit_txn()) {
    GTEST_SKIP();
  }

  ASSERT_THAT(quirks()->DropTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  if (quirks()->supports_get_option()) {
    auto autocommit =
        ConnectionGetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT, &error);
    ASSERT_THAT(autocommit,
                ::testing::Optional(::testing::StrEq(ADBC_OPTION_VALUE_ENABLED)));
  }

  Handle<struct AdbcConnection> connection2;
  ASSERT_THAT(AdbcConnectionNew(&connection2.value, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionInit(&connection2.value, &database, &error),
              IsOkStatus(&error));

  ASSERT_THAT(AdbcConnectionSetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT,
                                      ADBC_OPTION_VALUE_DISABLED, &error),
              IsOkStatus(&error));

  if (quirks()->supports_get_option()) {
    auto autocommit =
        ConnectionGetOption(&connection, ADBC_CONNECTION_OPTION_AUTOCOMMIT, &error);
    ASSERT_THAT(autocommit,
                ::testing::Optional(::testing::StrEq(ADBC_OPTION_VALUE_DISABLED)));
  }

  // Uncommitted change
  ASSERT_THAT(quirks()->CreateSampleTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));

  // Query on first connection should succeed
  {
    Handle<struct AdbcStatement> statement;
    StreamReader reader;

    ASSERT_THAT(AdbcStatementNew(&connection, &statement.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement.value,
                                         "SELECT * FROM \"bulk_ingest\"", &error),
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
                CHECK_OK(AdbcStatementSetSqlQuery(
                    &statement.value, "SELECT * FROM \"bulk_ingest\"", &error));
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
                CHECK_OK(AdbcStatementSetSqlQuery(
                    &statement.value, "SELECT * FROM \"bulk_ingest\"", &error));
                CHECK_OK(AdbcStatementExecuteQuery(&statement.value, &reader.stream.value,
                                                   &reader.rows_affected, &error));
                return ADBC_STATUS_OK;
              })(),
              ::testing::Not(IsOkStatus(&error)));

  // Rollback
  ASSERT_THAT(AdbcConnectionRollback(&connection, &error), IsOkStatus(&error));

  // Commit
  ASSERT_THAT(quirks()->CreateSampleTable(&connection, "bulk_ingest", &error),
              IsOkStatus(&error));
  ASSERT_THAT(AdbcConnectionCommit(&connection, &error), IsOkStatus(&error));

  // Query on second connection should succeed
  {
    Handle<struct AdbcStatement> statement;
    StreamReader reader;

    ASSERT_THAT(AdbcStatementNew(&connection2.value, &statement.value, &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementSetSqlQuery(&statement.value,
                                         "SELECT * FROM \"bulk_ingest\"", &error),
                IsOkStatus(&error));
    ASSERT_THAT(AdbcStatementExecuteQuery(&statement.value, &reader.stream.value,
                                          &reader.rows_affected, &error),
                IsOkStatus(&error));
    ASSERT_NO_FATAL_FAILURE(reader.GetSchema());
  }
}

void StatementTest::TestSqlSchemaInts() {
  if (!quirks()->supports_execute_schema()) {
    GTEST_SKIP() << "Not supported";
  }

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 42", &error),
              IsOkStatus(&error));

  nanoarrow::UniqueSchema schema;
  ASSERT_THAT(AdbcStatementExecuteSchema(&statement, schema.get(), &error),
              IsOkStatus(&error));

  ASSERT_EQ(1, schema->n_children);
  ASSERT_THAT(schema->children[0]->format, ::testing::AnyOfArray({
                                               ::testing::StrEq("i"),  // int32
                                               ::testing::StrEq("l"),  // int64
                                           }));

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlSchemaFloats() {
  if (!quirks()->supports_execute_schema()) {
    GTEST_SKIP() << "Not supported";
  }

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT CAST(1.5 AS FLOAT)", &error),
              IsOkStatus(&error));

  nanoarrow::UniqueSchema schema;
  ASSERT_THAT(AdbcStatementExecuteSchema(&statement, schema.get(), &error),
              IsOkStatus(&error));

  ASSERT_EQ(1, schema->n_children);
  ASSERT_THAT(schema->children[0]->format, ::testing::AnyOfArray({
                                               ::testing::StrEq("f"),  // float32
                                               ::testing::StrEq("g"),  // float64
                                           }));

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlSchemaStrings() {
  if (!quirks()->supports_execute_schema()) {
    GTEST_SKIP() << "Not supported";
  }

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 'hi'", &error),
              IsOkStatus(&error));

  nanoarrow::UniqueSchema schema;
  ASSERT_THAT(AdbcStatementExecuteSchema(&statement, schema.get(), &error),
              IsOkStatus(&error));

  ASSERT_EQ(1, schema->n_children);
  ASSERT_THAT(schema->children[0]->format, ::testing::AnyOfArray({
                                               ::testing::StrEq("u"),  // string
                                               ::testing::StrEq("U"),  // large_string
                                           }));

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
}

void StatementTest::TestSqlSchemaErrors() {
  if (!quirks()->supports_execute_schema()) {
    GTEST_SKIP() << "Not supported";
  }

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));

  nanoarrow::UniqueSchema schema;
  ASSERT_THAT(AdbcStatementExecuteSchema(&statement, schema.get(), &error),
              IsStatus(ADBC_STATUS_INVALID_STATE, &error));

  ASSERT_THAT(AdbcStatementRelease(&statement, &error), IsOkStatus(&error));
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

struct ADBC_EXPORT AdbcError100 {
  char* message;
  int32_t vendor_code;
  char sqlstate[5];
  void (*release)(struct AdbcError100* error);
};

// Test that an ADBC 1.0.0-sized error still works
void StatementTest::TestErrorCompatibility() {
  static_assert(sizeof(AdbcError100) == ADBC_ERROR_1_0_0_SIZE, "Wrong size");
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
              ::testing::Not(IsOkStatus(&error)));
  ASSERT_EQ(&canary, error.private_data);
  ASSERT_EQ(&canary, error.private_driver);
  error.release(&error);
}

void StatementTest::TestResultIndependence() {
  // If we have a result reader, and we close the statement (and other
  // resources), either the statement should error, or the reader should be
  // closeable and should error on other operations

  ASSERT_THAT(AdbcStatementNew(&connection, &statement, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcStatementSetSqlQuery(&statement, "SELECT 42", &error),
              IsOkStatus(&error));

  StreamReader reader;
  ASSERT_THAT(AdbcStatementExecuteQuery(&statement, &reader.stream.value,
                                        &reader.rows_affected, &error),
              IsOkStatus(&error));
  ASSERT_NO_FATAL_FAILURE(reader.GetSchema());

  auto status = AdbcStatementRelease(&statement, &error);
  if (status != ADBC_STATUS_OK) {
    // That's ok, this driver prevents closing the statement while readers are open
    return;
  }
  ASSERT_THAT(AdbcConnectionRelease(&connection, &error), IsOkStatus(&error));
  ASSERT_THAT(AdbcDatabaseRelease(&database, &error), IsOkStatus(&error));

  // Must not crash (but it's up to the driver whether it errors or succeeds)
  std::ignore = reader.MaybeNext();
  // Implicitly StreamReader calls release() on destruction, that should not
  // crash either
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

  // First reader may fail, or may succeed but give no data
  reader1.MaybeNext();
}

}  // namespace adbc_validation
