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

#include <optional>
#include <tuple>

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.hpp>

#include "postgres_copy_test_common.h"
#include "postgresql/copy/writer.h"
#include "validation/adbc_validation_util.h"

namespace adbcpq {

class PostgresCopyStreamWriteTester {
 public:
  ArrowErrorCode Init(struct ArrowSchema* schema, struct ArrowArray* array,
                      struct ArrowError* error = nullptr) {
    NANOARROW_RETURN_NOT_OK(writer_.Init(schema));
    NANOARROW_RETURN_NOT_OK(writer_.InitFieldWriters(error));
    NANOARROW_RETURN_NOT_OK(writer_.SetArray(array));
    return NANOARROW_OK;
  }

  ArrowErrorCode WriteAll(struct ArrowError* error) {
    NANOARROW_RETURN_NOT_OK(writer_.WriteHeader(error));

    int result;
    do {
      result = writer_.WriteRecord(error);
    } while (result == NANOARROW_OK);

    return result;
  }

  ArrowErrorCode WriteArray(struct ArrowArray* array, struct ArrowError* error) {
    writer_.SetArray(array);
    int result;
    do {
      result = writer_.WriteRecord(error);
    } while (result == NANOARROW_OK);

    return result;
  }

  const struct ArrowBuffer& WriteBuffer() const { return writer_.WriteBuffer(); }

  void Rewind() { writer_.Rewind(); }

 private:
  PostgresCopyStreamWriter writer_;
};

TEST(PostgresCopyUtilsTest, PostgresCopyWriteBoolean) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  adbc_validation::Handle<struct ArrowBuffer> buffer;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_BOOL}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<bool>(&schema.value, &array.value, &na_error,
                                             {true, false, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyBoolean) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyBoolean[i]);
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyWriteInt8) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT8}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int8_t>(&schema.value, &array.value, &na_error,
                                               {-123, -1, 1, 123, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopySmallInt) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopySmallInt[i]);
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyWriteInt16) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT16}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int16_t>(&schema.value, &array.value, &na_error,
                                                {-123, -1, 1, 123, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopySmallInt) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopySmallInt[i]);
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyWriteInt32) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT32}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int32_t>(&schema.value, &array.value, &na_error,
                                                {-123, -1, 1, 123, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyInteger) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyInteger[i]);
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyWriteInt64) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT64}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                                {-123, -1, 1, 123, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyBigInt) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyBigInt[i]);
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyWriteReal) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_FLOAT}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<float>(&schema.value, &array.value, &na_error,
                                              {-123.456, -1, 1, 123.456, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyReal) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyReal[i]) << " mismatch at index: " << i;
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyWriteDoublePrecision) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_DOUBLE}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<double>(&schema.value, &array.value, &na_error,
                                               {-123.456, -1, 1, 123.456, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyDoublePrecision) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyDoublePrecision[i]);
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyWriteDate) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_DATE32}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int32_t>(&schema.value, &array.value, &na_error,
                                                {-25567, 47482, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyDate) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyDate[i]);
  }
}

// This buffer is similar to the read variant above but removes special values
// nan, ±inf as they are not supported via the Arrow Decimal types
// COPY (SELECT CAST(col AS NUMERIC) AS col FROM (  VALUES (NULL), (-123.456),
// ('0.00001234'), (1.0000), (123.456), (1000000)) AS drvd(col))
// TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyNumericWrite[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x0c, 0x00, 0x02, 0x00, 0x00, 0x40, 0x00, 0x00, 0x03, 0x00, 0x7b, 0x11,
    0xd0, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0xff, 0xfe, 0x00, 0x00, 0x00,
    0x08, 0x04, 0xd2, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x02, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x7b, 0x11, 0xd0, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x0a, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, 0xff, 0xff};

TEST(PostgresCopyUtilsTest, PostgresCopyWriteNumeric) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  constexpr enum ArrowType type = NANOARROW_TYPE_DECIMAL128;
  constexpr int32_t size = 128;
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 8;

  struct ArrowDecimal decimal1;
  struct ArrowDecimal decimal2;
  struct ArrowDecimal decimal3;
  struct ArrowDecimal decimal4;
  struct ArrowDecimal decimal5;

  ArrowDecimalInit(&decimal1, size, 19, 8);
  ArrowDecimalSetInt(&decimal1, -12345600000);
  ArrowDecimalInit(&decimal2, size, 19, 8);
  ArrowDecimalSetInt(&decimal2, 1234);
  ArrowDecimalInit(&decimal3, size, 19, 8);
  ArrowDecimalSetInt(&decimal3, 100000000);
  ArrowDecimalInit(&decimal4, size, 19, 8);
  ArrowDecimalSetInt(&decimal4, 12345600000);
  ArrowDecimalInit(&decimal5, size, 19, 8);
  ArrowDecimalSetInt(&decimal5, 100000000000000);

  const std::vector<std::optional<ArrowDecimal*>> values = {
      std::nullopt, &decimal1, &decimal2, &decimal3, &decimal4, &decimal5};

  ArrowSchemaInit(&schema.value);
  ASSERT_EQ(ArrowSchemaSetTypeStruct(&schema.value, 1), 0);
  ASSERT_EQ(
      PrivateArrowSchemaSetTypeDecimal(schema.value.children[0], type, precision, scale),
      0);
  ASSERT_EQ(ArrowSchemaSetName(schema.value.children[0], "col"), 0);
  ASSERT_EQ(adbc_validation::MakeBatch<ArrowDecimal*>(&schema.value, &array.value,
                                                      &na_error, values),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyNumericWrite) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyNumericWrite[i]) << " at position " << i;
  }
}

using TimestampTestParamType =
    std::tuple<enum ArrowTimeUnit, const char*, std::vector<std::optional<int64_t>>>;

class PostgresCopyWriteTimestampTest
    : public testing::TestWithParam<TimestampTestParamType> {};

TEST_P(PostgresCopyWriteTimestampTest, WritesProperBufferValues) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  TimestampTestParamType parameters = GetParam();
  enum ArrowTimeUnit unit = std::get<0>(parameters);
  const char* timezone = std::get<1>(parameters);

  const std::vector<std::optional<int64_t>> values = std::get<2>(parameters);

  ArrowSchemaInit(&schema.value);
  ArrowSchemaSetTypeStruct(&schema.value, 1);
  ArrowSchemaSetTypeDateTime(schema->children[0], NANOARROW_TYPE_TIMESTAMP, unit,
                             timezone);
  ArrowSchemaSetName(schema->children[0], "col");
  ASSERT_EQ(
      adbc_validation::MakeBatch<int64_t>(&schema.value, &array.value, &na_error, values),
      ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyTimestamp) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyTimestamp[i]);
  }
}

static const std::vector<TimestampTestParamType> ts_values{
    {NANOARROW_TIME_UNIT_SECOND, nullptr, {-2208943504, 4102490096, std::nullopt}},
    {NANOARROW_TIME_UNIT_MILLI, nullptr, {-2208943504000, 4102490096000, std::nullopt}},
    {NANOARROW_TIME_UNIT_MICRO,
     nullptr,
     {-2208943504000000, 4102490096000000, std::nullopt}},
    {NANOARROW_TIME_UNIT_NANO,
     nullptr,
     {-2208943504000000000, 4102490096000000000, std::nullopt}},
    {NANOARROW_TIME_UNIT_SECOND, "UTC", {-2208943504, 4102490096, std::nullopt}},
    {NANOARROW_TIME_UNIT_MILLI, "UTC", {-2208943504000, 4102490096000, std::nullopt}},
    {NANOARROW_TIME_UNIT_MICRO,
     "UTC",
     {-2208943504000000, 4102490096000000, std::nullopt}},
    {NANOARROW_TIME_UNIT_NANO,
     "UTC",
     {-2208943504000000000, 4102490096000000000, std::nullopt}},
    {NANOARROW_TIME_UNIT_SECOND,
     "America/New_York",
     {-2208943504, 4102490096, std::nullopt}},
    {NANOARROW_TIME_UNIT_MILLI,
     "America/New_York",
     {-2208943504000, 4102490096000, std::nullopt}},
    {NANOARROW_TIME_UNIT_MICRO,
     "America/New_York",
     {-2208943504000000, 4102490096000000, std::nullopt}},
    {NANOARROW_TIME_UNIT_NANO,
     "America/New_York",
     {-2208943504000000000, 4102490096000000000, std::nullopt}},
};

INSTANTIATE_TEST_SUITE_P(PostgresCopyWriteTimestamp, PostgresCopyWriteTimestampTest,
                         testing::ValuesIn(ts_values));

TEST(PostgresCopyUtilsTest, PostgresCopyWriteInterval) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  const enum ArrowType type = NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO;
  // values are days, months, ns
  struct ArrowInterval neg_interval;
  struct ArrowInterval pos_interval;

  ArrowIntervalInit(&neg_interval, type);
  ArrowIntervalInit(&pos_interval, type);

  neg_interval.months = -1;
  neg_interval.days = -2;
  neg_interval.ns = -4000000000;

  pos_interval.months = 1;
  pos_interval.days = 2;
  pos_interval.ns = 4000000000;

  const std::vector<std::optional<ArrowInterval*>> values = {&neg_interval, &pos_interval,
                                                             std::nullopt};

  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", type}}), ADBC_STATUS_OK);

  ASSERT_EQ(adbc_validation::MakeBatch<ArrowInterval*>(&schema.value, &array.value,
                                                       &na_error, values),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyInterval) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyInterval[i]);
  }
}

// Writing a DURATION from NANOARROW produces INTERVAL in postgres without day/month
// COPY (SELECT CAST(col AS INTERVAL) FROM (  VALUES ('-4 seconds'),
// ('4 seconds'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT BINARY);
static uint8_t kTestPgCopyDuration[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x10, 0xff, 0xff, 0xff, 0xff, 0xff, 0xc2, 0xf7, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x10, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x3d, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
using DurationTestParamType =
    std::tuple<enum ArrowTimeUnit, std::vector<std::optional<int64_t>>>;

class PostgresCopyWriteDurationTest
    : public testing::TestWithParam<DurationTestParamType> {};

TEST_P(PostgresCopyWriteDurationTest, WritesProperBufferValues) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  const enum ArrowType type = NANOARROW_TYPE_DURATION;

  DurationTestParamType parameters = GetParam();
  enum ArrowTimeUnit unit = std::get<0>(parameters);
  const std::vector<std::optional<int64_t>> values = std::get<1>(parameters);

  ArrowSchemaInit(&schema.value);
  ArrowSchemaSetTypeStruct(&schema.value, 1);
  ArrowSchemaSetTypeDateTime(schema->children[0], type, unit, nullptr);
  ArrowSchemaSetName(schema->children[0], "col");
  ASSERT_EQ(
      adbc_validation::MakeBatch<int64_t>(&schema.value, &array.value, &na_error, values),
      ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyDuration) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyDuration[i]);
  }
}

static const std::vector<DurationTestParamType> duration_params{
    {NANOARROW_TIME_UNIT_SECOND, {-4, 4, std::nullopt}},
    {NANOARROW_TIME_UNIT_MILLI, {-4000, 4000, std::nullopt}},
    {NANOARROW_TIME_UNIT_MICRO, {-4000000, 4000000, std::nullopt}},
    {NANOARROW_TIME_UNIT_NANO, {-4000000000, 4000000000, std::nullopt}},
};

INSTANTIATE_TEST_SUITE_P(PostgresCopyWriteDuration, PostgresCopyWriteDurationTest,
                         testing::ValuesIn(duration_params));

TEST(PostgresCopyUtilsTest, PostgresCopyWriteString) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_STRING}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<std::string>(
                &schema.value, &array.value, &na_error, {"abc", "1234", std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyText) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyText[i]);
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyWriteLargeString) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(
      adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_LARGE_STRING}}),
      ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<std::string>(
                &schema.value, &array.value, &na_error, {"abc", "1234", std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyText) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyText[i]);
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyWriteBinary) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_BINARY}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<std::vector<std::byte>>(
                &schema.value, &array.value, &na_error,
                {std::vector<std::byte>{},
                 std::vector<std::byte>{std::byte{0x00}, std::byte{0x01}},
                 std::vector<std::byte>{std::byte{0x01}, std::byte{0x02}, std::byte{0x03},
                                        std::byte{0x04}},
                 std::vector<std::byte>{std::byte{0xfe}, std::byte{0xff}}, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyBinary) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyBinary[i]) << "failure at index " << i;
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyWriteMultiBatch) {
  // Regression test for https://github.com/apache/arrow-adbc/issues/1310
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT32}}),
            NANOARROW_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int32_t>(&schema.value, &array.value, &na_error,
                                                {-123, -1, 1, 123, std::nullopt}),
            NANOARROW_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  size_t buf_size = sizeof(kTestPgCopyInteger) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyInteger[i]);
  }

  tester.Rewind();
  ASSERT_EQ(tester.WriteArray(&array.value, nullptr), ENODATA);

  buf = tester.WriteBuffer();
  // Ignore the header and footer
  buf_size = sizeof(kTestPgCopyInteger) - 21;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyInteger[i + 19]);
  }
}

}  // namespace adbcpq
