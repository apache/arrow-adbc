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

#include <limits>
#include <memory>
#include <optional>
#include <string>
#include <tuple>
#include <vector>

#include <gtest/gtest-param-test.h>
#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.hpp>

#include "postgres_copy_test_common.h"
#include "postgresql/copy/writer.h"
#include "postgresql/database.h"
#include "validation/adbc_validation_util.h"

using adbc_validation::IsOkStatus;

namespace adbcpq {

// COPY (SELECT CAST(col AS NUMERIC(18, 6)) AS col FROM (VALUES
// ('5000000000.000001'), ('4999999999.000001'), ('10000.000001'),
// ('9999.000001'), ('10001.000001'), ('100000000.000001'),
// ('-10000.000001')) AS drvd(col))
// TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyNumericZeroIntegerGroups[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x05, 0x00,
    0x02, 0x00, 0x00, 0x00, 0x06, 0x00, 0x32, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x64, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x05, 0x00, 0x02, 0x00, 0x00, 0x00,
    0x06, 0x00, 0x31, 0x27, 0x0f, 0x27, 0x0f, 0x00, 0x00, 0x00, 0x64, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x10, 0x00, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, 0x06, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x64, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x03, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x06, 0x27, 0x0f, 0x00, 0x00, 0x00, 0x64, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x10, 0x00, 0x04, 0x00, 0x01, 0x00, 0x00, 0x00, 0x06, 0x00, 0x01, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x64, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x05, 0x00,
    0x02, 0x00, 0x00, 0x00, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x64, 0x00, 0x01, 0x00, 0x00, 0x00, 0x10, 0x00, 0x04, 0x00, 0x01, 0x40, 0x00, 0x00,
    0x06, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x64, 0xff, 0xff};

class PostgresCopyStreamWriteTester {
 public:
  ArrowErrorCode Init(struct ArrowSchema* schema, struct ArrowArray* array,
                      const PostgresTypeResolver& type_resolver,
                      struct ArrowError* error = nullptr) {
    NANOARROW_RETURN_NOT_OK(writer_.Init(schema));
    NANOARROW_RETURN_NOT_OK(writer_.InitFieldWriters(type_resolver, nullptr,
                                                     /*disable_decimal_fast_path=*/false,
                                                     error));
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

static AdbcStatusCode SetupDatabase(struct AdbcDatabase* database,
                                    struct AdbcError* error) {
  const char* uri = std::getenv("ADBC_POSTGRESQL_TEST_URI");
  if (!uri) {
    ADD_FAILURE() << "Must provide env var ADBC_POSTGRESQL_TEST_URI";
    return ADBC_STATUS_INVALID_ARGUMENT;
  }
  return AdbcDatabaseSetOption(database, "uri", uri, error);
}

class PostgresCopyTest : public ::testing::Test {
 public:
  void SetUp() override {
    ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(SetupDatabase(&database_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(AdbcDatabaseInit(&database_, &error_), IsOkStatus(&error_));

    const auto pg_db =
        *reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database_.private_data);
    type_resolver_ = pg_db->type_resolver();
  }
  void TearDown() override {
    if (database_.private_data) {
      ASSERT_THAT(AdbcDatabaseRelease(&database_, &error_), IsOkStatus(&error_));
    }

    if (error_.release) error_.release(&error_);
  }

 protected:
  struct AdbcError error_ = {};
  struct AdbcDatabase database_ = {};
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
};

template <enum ArrowType Type>
static void AssertNumericZeroIntegerGroupsRoundTrip(
    const PostgresTypeResolver& type_resolver) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  constexpr int32_t size = (Type == NANOARROW_TYPE_DECIMAL128) ? 128 : 256;
  constexpr int32_t precision = 18;
  constexpr int32_t scale = 6;

  struct ArrowDecimal decimals[7];
  constexpr struct ArrowStringView digits[] = {
      {"5000000000000001", 16}, {"4999999999000001", 16}, {"10000000001", 11},
      {"9999000001", 10},       {"10001000001", 11},      {"100000000000001", 15},
      {"-10000000001", 12}};

  std::vector<std::optional<ArrowDecimal*>> values;
  values.reserve(7);
  for (size_t i = 0; i < 7; i++) {
    ArrowDecimalInit(&decimals[i], size, precision, scale);
    ASSERT_EQ(ArrowDecimalSetDigits(&decimals[i], digits[i]), 0);
    values.push_back(&decimals[i]);
  }

  ArrowSchemaInit(&schema.value);
  ASSERT_EQ(ArrowSchemaSetTypeStruct(&schema.value, 1), 0);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema.value.children[0], Type, precision, scale),
            0);
  ASSERT_EQ(ArrowSchemaSetName(schema.value.children[0], "col"), 0);
  ASSERT_EQ(adbc_validation::MakeBatch<ArrowDecimal*>(&schema.value, &array.value,
                                                      &na_error, values),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, type_resolver), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyNumericZeroIntegerGroups) - 2;
  ASSERT_EQ(buf.size_bytes, static_cast<int64_t>(buf_size));
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyNumericZeroIntegerGroups[i])
        << " at position " << i;
  }
}

TEST_F(PostgresCopyTest, PostgresCopyWriteBoolean) {
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
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

TEST_F(PostgresCopyTest, PostgresCopyWriteInt8) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT8}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int8_t>(&schema.value, &array.value, &na_error,
                                               {-123, -1, 1, 123, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

TEST_F(PostgresCopyTest, PostgresCopyWriteInt16) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT16}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int16_t>(&schema.value, &array.value, &na_error,
                                                {-123, -1, 1, 123, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

TEST_F(PostgresCopyTest, PostgresCopyWriteInt32) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT32}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int32_t>(&schema.value, &array.value, &na_error,
                                                {-123, -1, 1, 123, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

TEST_F(PostgresCopyTest, PostgresCopyWriteInt64) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_INT64}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                                {-123, -1, 1, 123, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

// COPY (SELECT CAST("col" AS SMALLINT) AS "col" FROM (  VALUES (0), (255),
// (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyUInt8[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02,
    0x00, 0xff, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST_F(PostgresCopyTest, PostgresCopyWriteUInt8) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_UINT8}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<uint8_t>(
                &schema.value, &array.value, &na_error,
                {0, (std::numeric_limits<uint8_t>::max)(), std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyUInt8) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyUInt8[i]);
  }
}

// COPY (SELECT CAST("col" AS INTEGER) AS "col" FROM (  VALUES (0), (65535),
// (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyUInt16[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00,
    0x00, 0xff, 0xff, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST_F(PostgresCopyTest, PostgresCopyWriteUInt16) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_UINT16}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<uint16_t>(
                &schema.value, &array.value, &na_error,
                {0, (std::numeric_limits<uint16_t>::max)(), std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyUInt16) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyUInt16[i]);
  }
}

// COPY (SELECT CAST("col" AS BIGINT) AS "col" FROM (  VALUES (0), (2^32-1),
// (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyUInt32[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST_F(PostgresCopyTest, PostgresCopyWriteUInt32) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_UINT32}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<uint32_t>(
                &schema.value, &array.value, &na_error,
                {0, (std::numeric_limits<uint32_t>::max)(), std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyUInt32) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyUInt32[i]);
  }
}

TEST_F(PostgresCopyTest, PostgresCopyWriteReal) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_FLOAT}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<float>(&schema.value, &array.value, &na_error,
                                              {-123.456, -1, 1, 123.456, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

TEST_F(PostgresCopyTest, PostgresCopyWriteDoublePrecision) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_DOUBLE}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<double>(&schema.value, &array.value, &na_error,
                                               {-123.456, -1, 1, 123.456, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

TEST_F(PostgresCopyTest, PostgresCopyWriteDate) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_DATE32}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<int32_t>(&schema.value, &array.value, &na_error,
                                                {-25567, 47482, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

TEST_F(PostgresCopyTest, PostgresCopyWriteTime) {
  // COPY (SELECT CAST(col AS TIME) FROM (VALUES ('00:00:00'), ('23:59:59'),
  // (NULL)) AS drvd(col)) TO STDOUT WITH (FORMAT binary);
  static const uint8_t expected[] = {
      0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
      0x14, 0x1d, 0xc8, 0x1d, 0xc0, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
  struct TimeTestParamType {
    enum ArrowType type;
    enum ArrowTimeUnit unit;
    std::vector<std::optional<int64_t>> values;
    const uint8_t* expected;
    size_t expected_size;
  };
  const TimeTestParamType params[] = {
      {NANOARROW_TYPE_TIME32,
       NANOARROW_TIME_UNIT_SECOND,
       {0, 86399, std::nullopt},
       expected,
       sizeof(expected)},
      {NANOARROW_TYPE_TIME32,
       NANOARROW_TIME_UNIT_MILLI,
       {0, 86399000, std::nullopt},
       expected,
       sizeof(expected)},
      {NANOARROW_TYPE_TIME64,
       NANOARROW_TIME_UNIT_MICRO,
       {0, 86399000000, 49376123456, std::nullopt},
       kTestPgCopyTime,
       sizeof(kTestPgCopyTime)},
      {NANOARROW_TYPE_TIME64,
       NANOARROW_TIME_UNIT_NANO,
       {0, 86399000000000, 49376123456000, std::nullopt},
       kTestPgCopyTime,
       sizeof(kTestPgCopyTime)},
  };

  for (const auto& param : params) {
    SCOPED_TRACE(param.unit);
    adbc_validation::Handle<struct ArrowSchema> schema;
    adbc_validation::Handle<struct ArrowArray> array;
    struct ArrowError na_error;

    ArrowSchemaInit(&schema.value);
    ASSERT_EQ(ArrowSchemaSetTypeStruct(&schema.value, 1), NANOARROW_OK);
    ASSERT_EQ(
        ArrowSchemaSetTypeDateTime(schema->children[0], param.type, param.unit, nullptr),
        NANOARROW_OK);
    ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);
    ASSERT_EQ(adbc_validation::MakeBatch<int64_t>(&schema.value, &array.value, &na_error,
                                                  param.values),
              ADBC_STATUS_OK);

    PostgresCopyStreamWriteTester tester;
    ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
    ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

    const struct ArrowBuffer buf = tester.WriteBuffer();
    // The end marker is sent separately by the caller.
    const size_t buf_size = param.expected_size - 2;
    ASSERT_EQ(buf.size_bytes, buf_size);
    for (size_t i = 0; i < buf_size; i++) {
      ASSERT_EQ(buf.data[i], param.expected[i]);
    }
  }
}

// This buffer is similar to the read variant above but removes special values
// nan, ±inf as they are not supported via the Arrow Decimal types
// COPY (SELECT CAST(col AS NUMERIC) AS col FROM (VALUES
// (NULL), (999999999999999999999999999999.99999999),
// (-999999999999999999999999999999.99999999),
// (0), (1234), (92233720368.54775807), (-92233720368.54775808),
// (-123.456), ('0.00001234'), (1), (123.456), (1000000)) AS drvd(col))
// TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyNumericWrite[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x1c, 0x00, 0x0a, 0x00, 0x07, 0x00, 0x00, 0x00, 0x08, 0x00, 0x63, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x27, 0x0f, 0x00, 0x01, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x0a, 0x00, 0x07, 0x40,
    0x00, 0x00, 0x08, 0x00, 0x63, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x0a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x04, 0xd2, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x12, 0x00, 0x05, 0x00, 0x02, 0x00, 0x00, 0x00, 0x08, 0x03, 0x9a, 0x0d,
    0x2c, 0x01, 0x70, 0x15, 0x65, 0x16, 0xaf, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00,
    0x05, 0x00, 0x02, 0x40, 0x00, 0x00, 0x08, 0x03, 0x9a, 0x0d, 0x2c, 0x01, 0x70, 0x15,
    0x65, 0x16, 0xb0, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x02, 0x00, 0x00, 0x40,
    0x00, 0x00, 0x03, 0x00, 0x7b, 0x11, 0xd0, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00,
    0x01, 0xff, 0xfe, 0x00, 0x00, 0x00, 0x08, 0x04, 0xd2, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x0a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x0c, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x03, 0x00, 0x7b, 0x11,
    0xd0, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x64, 0xff, 0xff};

TEST_F(PostgresCopyTest, PostgresCopyWriteNumeric) {
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
  struct ArrowDecimal decimal_max_64;
  struct ArrowDecimal decimal_min_64;
  struct ArrowDecimal decimal_zero;
  struct ArrowDecimal decimal_no_frac;
  struct ArrowDecimal decimal_max_128;
  struct ArrowDecimal decimal_min_128;

  ArrowDecimalInit(&decimal1, size, precision, scale);
  ArrowDecimalSetInt(&decimal1, -12345600000);
  ArrowDecimalInit(&decimal2, size, precision, scale);
  ArrowDecimalSetInt(&decimal2, 1234);
  ArrowDecimalInit(&decimal3, size, precision, scale);
  ArrowDecimalSetInt(&decimal3, 100000000);
  ArrowDecimalInit(&decimal4, size, precision, scale);
  ArrowDecimalSetInt(&decimal4, 12345600000);
  ArrowDecimalInit(&decimal5, size, precision, scale);
  ArrowDecimalSetInt(&decimal5, 100000000000000);

  ArrowDecimalInit(&decimal_max_64, size, precision, scale);
  ArrowDecimalSetInt(&decimal_max_64, 9223372036854775807LL);

  ArrowDecimalInit(&decimal_min_64, size, precision, scale);
  ArrowDecimalSetInt(&decimal_min_64, -9223372036854775807LL - 1);

  ArrowDecimalInit(&decimal_zero, size, precision, scale);
  ArrowDecimalSetInt(&decimal_zero, 0);

  ArrowDecimalInit(&decimal_no_frac, size, precision, scale);
  ArrowDecimalSetInt(&decimal_no_frac, 123400000000LL);  // 1234 * 10^8

  ArrowDecimalInit(&decimal_max_128, size, precision, scale);
  struct ArrowStringView max_digits_8;
  max_digits_8.data = "99999999999999999999999999999999999999";
  max_digits_8.size_bytes = 38;
  ArrowDecimalSetDigits(&decimal_max_128, max_digits_8);

  ArrowDecimalInit(&decimal_min_128, size, precision, scale);
  struct ArrowStringView min_digits_8;
  min_digits_8.data = "-99999999999999999999999999999999999999";
  min_digits_8.size_bytes = 39;
  ArrowDecimalSetDigits(&decimal_min_128, min_digits_8);

  const std::vector<std::optional<ArrowDecimal*>> values = {
      std::nullopt,     &decimal_max_128, &decimal_min_128, &decimal_zero,
      &decimal_no_frac, &decimal_max_64,  &decimal_min_64,  &decimal1,
      &decimal2,        &decimal3,        &decimal4,        &decimal5};

  ArrowSchemaInit(&schema.value);
  ASSERT_EQ(ArrowSchemaSetTypeStruct(&schema.value, 1), 0);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema.value.children[0], type, precision, scale),
            0);
  ASSERT_EQ(ArrowSchemaSetName(schema.value.children[0], "col"), 0);
  ASSERT_EQ(adbc_validation::MakeBatch<ArrowDecimal*>(&schema.value, &array.value,
                                                      &na_error, values),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

// Regression test for bug where 44.123456 with Decimal(10,6) became 4412.345500
// COPY (SELECT CAST(col AS NUMERIC) AS col FROM (VALUES
// (99999999999999999999999999999999.999999),
// (-99999999999999999999999999999999.999999),
// (0), (1000000000000), (9223372036854.775807), (-9223372036854.775808),
// (44.123456), (0.123456), (123.456789)) AS drvd(col)) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyNumericScale6[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x0a, 0x00,
    0x07, 0x00, 0x00, 0x00, 0x06, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x26, 0xac, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x1c, 0x00, 0x0a, 0x00, 0x07, 0x40, 0x00, 0x00, 0x06, 0x27, 0x0f, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x26, 0xac, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0x00, 0x03, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x14, 0x00, 0x06, 0x00,
    0x03, 0x00, 0x00, 0x00, 0x06, 0x00, 0x09, 0x08, 0xb9, 0x1c, 0x23, 0x1a, 0xc6, 0x1e,
    0x4e, 0x02, 0xbc, 0x00, 0x01, 0x00, 0x00, 0x00, 0x14, 0x00, 0x06, 0x00, 0x03, 0x40,
    0x00, 0x00, 0x06, 0x00, 0x09, 0x08, 0xb9, 0x1c, 0x23, 0x1a, 0xc6, 0x1e, 0x4e, 0x03,
    0x20, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x06, 0x00, 0x2c, 0x04, 0xd2, 0x15, 0xe0, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00,
    0x02, 0xff, 0xff, 0x00, 0x00, 0x00, 0x06, 0x04, 0xd2, 0x15, 0xe0, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x0e, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x06, 0x00, 0x7b, 0x11,
    0xd7, 0x22, 0xc4, 0xff, 0xff};

TEST_F(PostgresCopyTest, PostgresCopyWriteNumericScale6) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  constexpr enum ArrowType type = NANOARROW_TYPE_DECIMAL128;
  constexpr int32_t size = 128;
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 6;

  struct ArrowDecimal decimal1;
  struct ArrowDecimal decimal2;
  struct ArrowDecimal decimal3;
  struct ArrowDecimal decimal_max_64;
  struct ArrowDecimal decimal_min_64;
  struct ArrowDecimal decimal_zero;
  struct ArrowDecimal decimal_no_frac;
  struct ArrowDecimal decimal_max_128;
  struct ArrowDecimal decimal_min_128;

  ArrowDecimalInit(&decimal1, size, precision, scale);
  ArrowDecimalSetInt(&decimal1, 44123456);

  ArrowDecimalInit(&decimal2, size, precision, scale);
  ArrowDecimalSetInt(&decimal2, 123456);

  ArrowDecimalInit(&decimal3, size, precision, scale);
  ArrowDecimalSetInt(&decimal3, 123456789);

  ArrowDecimalInit(&decimal_max_64, size, precision, scale);
  ArrowDecimalSetInt(&decimal_max_64, 9223372036854775807LL);

  ArrowDecimalInit(&decimal_min_64, size, precision, scale);
  ArrowDecimalSetInt(&decimal_min_64, -9223372036854775807LL - 1);

  ArrowDecimalInit(&decimal_zero, size, precision, scale);
  ArrowDecimalSetInt(&decimal_zero, 0);

  ArrowDecimalInit(&decimal_no_frac, size, precision, scale);
  ArrowDecimalSetInt(&decimal_no_frac, 1000000000000000000LL);

  ArrowDecimalInit(&decimal_max_128, size, precision, scale);
  struct ArrowStringView max_digits;
  max_digits.data = "99999999999999999999999999999999999999";
  max_digits.size_bytes = 38;
  ArrowDecimalSetDigits(&decimal_max_128, max_digits);

  ArrowDecimalInit(&decimal_min_128, size, precision, scale);
  struct ArrowStringView min_digits;
  min_digits.data = "-99999999999999999999999999999999999999";
  min_digits.size_bytes = 39;  // 38 digits + 1 for '-' sign
  ArrowDecimalSetDigits(&decimal_min_128, min_digits);

  const std::vector<std::optional<ArrowDecimal*>> values = {
      &decimal_max_128, &decimal_min_128, &decimal_zero,
      &decimal_no_frac, &decimal_max_64,  &decimal_min_64,
      &decimal1,        &decimal2,        &decimal3};

  ArrowSchemaInit(&schema.value);
  ASSERT_EQ(ArrowSchemaSetTypeStruct(&schema.value, 1), 0);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema.value.children[0], type, precision, scale),
            0);
  ASSERT_EQ(ArrowSchemaSetName(schema.value.children[0], "col"), 0);
  ASSERT_EQ(adbc_validation::MakeBatch<ArrowDecimal*>(&schema.value, &array.value,
                                                      &na_error, values),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();

  constexpr size_t buf_size = sizeof(kTestPgCopyNumericScale6) - 2;
  ASSERT_EQ(buf.size_bytes, static_cast<int64_t>(buf_size));

  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyNumericScale6[i]) << " at position " << i;
  }
}

// Test for scale=5 (remainder 1 when divided by 4)
// COPY (SELECT CAST(col AS NUMERIC) AS col FROM (VALUES
// (999999999999999999999999999999999.99999),
// (-999999999999999999999999999999999.99999),
// (0), (10000000000000), (92233720368547.75807), (-92233720368547.75808),
// (12.34567), (-9.87654), (0.00123)) AS drvd(col)) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyNumericScale5[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x1e, 0x00, 0x0b, 0x00,
    0x08, 0x00, 0x00, 0x00, 0x05, 0x00, 0x09, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x23, 0x28, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x1e, 0x00, 0x0b, 0x00, 0x08, 0x40, 0x00, 0x00, 0x05, 0x00,
    0x09, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x23, 0x28, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00,
    0x01, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x14, 0x00, 0x06, 0x00, 0x03, 0x00, 0x00, 0x00, 0x05, 0x00, 0x5c, 0x09, 0x21, 0x07,
    0xf4, 0x21, 0x63, 0x1d, 0x9c, 0x1b, 0x58, 0x00, 0x01, 0x00, 0x00, 0x00, 0x14, 0x00,
    0x06, 0x00, 0x03, 0x40, 0x00, 0x00, 0x05, 0x00, 0x5c, 0x09, 0x21, 0x07, 0xf4, 0x21,
    0x63, 0x1d, 0x9c, 0x1f, 0x40, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x03, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x05, 0x00, 0x0c, 0x0d, 0x80, 0x1b, 0x58, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x0e, 0x00, 0x03, 0x00, 0x00, 0x40, 0x00, 0x00, 0x05, 0x00, 0x09, 0x22,
    0x3d, 0x0f, 0xa0, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x02, 0xff, 0xff, 0x00,
    0x00, 0x00, 0x05, 0x00, 0x0c, 0x0b, 0xb8, 0xff, 0xff};

TEST_F(PostgresCopyTest, PostgresCopyWriteNumericScale5) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  constexpr enum ArrowType type = NANOARROW_TYPE_DECIMAL128;
  constexpr int32_t size = 128;
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 5;

  struct ArrowDecimal decimal1;
  struct ArrowDecimal decimal2;
  struct ArrowDecimal decimal3;
  struct ArrowDecimal decimal_max_64;
  struct ArrowDecimal decimal_min_64;
  struct ArrowDecimal decimal_zero;
  struct ArrowDecimal decimal_no_frac;
  struct ArrowDecimal decimal_max_128;
  struct ArrowDecimal decimal_min_128;

  ArrowDecimalInit(&decimal1, size, precision, scale);
  ArrowDecimalSetInt(&decimal1, 1234567);

  ArrowDecimalInit(&decimal2, size, precision, scale);
  ArrowDecimalSetInt(&decimal2, -987654);

  ArrowDecimalInit(&decimal3, size, precision, scale);
  ArrowDecimalSetInt(&decimal3, 123);

  ArrowDecimalInit(&decimal_max_64, size, precision, scale);
  ArrowDecimalSetInt(&decimal_max_64, 9223372036854775807LL);

  ArrowDecimalInit(&decimal_min_64, size, precision, scale);
  ArrowDecimalSetInt(&decimal_min_64, -9223372036854775807LL - 1);

  ArrowDecimalInit(&decimal_zero, size, precision, scale);
  ArrowDecimalSetInt(&decimal_zero, 0);

  ArrowDecimalInit(&decimal_no_frac, size, precision, scale);
  ArrowDecimalSetInt(&decimal_no_frac, 1000000000000000000LL);

  ArrowDecimalInit(&decimal_max_128, size, precision, scale);
  struct ArrowStringView max_digits_5;
  max_digits_5.data = "99999999999999999999999999999999999999";
  max_digits_5.size_bytes = 38;
  ArrowDecimalSetDigits(&decimal_max_128, max_digits_5);

  ArrowDecimalInit(&decimal_min_128, size, precision, scale);
  struct ArrowStringView min_digits_5;
  min_digits_5.data = "-99999999999999999999999999999999999999";
  min_digits_5.size_bytes = 39;
  ArrowDecimalSetDigits(&decimal_min_128, min_digits_5);

  const std::vector<std::optional<ArrowDecimal*>> values = {
      &decimal_max_128, &decimal_min_128, &decimal_zero,
      &decimal_no_frac, &decimal_max_64,  &decimal_min_64,
      &decimal1,        &decimal2,        &decimal3};

  ArrowSchemaInit(&schema.value);
  ASSERT_EQ(ArrowSchemaSetTypeStruct(&schema.value, 1), 0);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema.value.children[0], type, precision, scale),
            0);
  ASSERT_EQ(ArrowSchemaSetName(schema.value.children[0], "col"), 0);
  ASSERT_EQ(adbc_validation::MakeBatch<ArrowDecimal*>(&schema.value, &array.value,
                                                      &na_error, values),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  constexpr size_t buf_size = sizeof(kTestPgCopyNumericScale5) - 2;
  ASSERT_EQ(buf.size_bytes, static_cast<int64_t>(buf_size));
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyNumericScale5[i]) << " at position " << i;
  }
}

// Test for scale=7 (remainder 3 when divided by 4)
// COPY (SELECT CAST(col AS NUMERIC) AS col FROM (VALUES
// (9999999999999999999999999999999.9999999),
// (-9999999999999999999999999999999.9999999),
// (0), (1000), (922337203685.4775807), (-922337203685.4775808),
// (5.1234567), (-123.456789), (0.0000001)) AS drvd(col)) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyNumericScale7[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x0a, 0x00,
    0x07, 0x00, 0x00, 0x00, 0x07, 0x03, 0xe7, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x06, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x1c, 0x00, 0x0a, 0x00, 0x07, 0x40, 0x00, 0x00, 0x07, 0x03, 0xe7, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x27, 0x06, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x03, 0xe8, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x05, 0x00,
    0x02, 0x00, 0x00, 0x00, 0x07, 0x24, 0x07, 0x0e, 0x88, 0x0e, 0x65, 0x12, 0xa7, 0x1f,
    0x86, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x05, 0x00, 0x02, 0x40, 0x00, 0x00,
    0x07, 0x24, 0x07, 0x0e, 0x88, 0x0e, 0x65, 0x12, 0xa7, 0x1f, 0x90, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x0e, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x07, 0x00, 0x05, 0x04,
    0xd2, 0x16, 0x26, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0e, 0x00, 0x03, 0x00, 0x00, 0x40,
    0x00, 0x00, 0x06, 0x00, 0x7b, 0x11, 0xd7, 0x22, 0xc4, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x0a, 0x00, 0x01, 0xff, 0xfe, 0x00, 0x00, 0x00, 0x07, 0x00, 0x0a, 0xff, 0xff};

TEST_F(PostgresCopyTest, PostgresCopyWriteNumericScale7) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  constexpr enum ArrowType type = NANOARROW_TYPE_DECIMAL128;
  constexpr int32_t size = 128;
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 7;

  struct ArrowDecimal decimal1;
  struct ArrowDecimal decimal2;
  struct ArrowDecimal decimal3;
  struct ArrowDecimal decimal_max_64;
  struct ArrowDecimal decimal_min_64;
  struct ArrowDecimal decimal_zero;
  struct ArrowDecimal decimal_no_frac;
  struct ArrowDecimal decimal_max_128;
  struct ArrowDecimal decimal_min_128;

  ArrowDecimalInit(&decimal1, size, precision, scale);
  ArrowDecimalSetInt(&decimal1, 51234567);

  // This represents -123.456789, but NUMERIC(10,7) will display it as -123.4567890
  ArrowDecimalInit(&decimal2, size, precision, scale);
  ArrowDecimalSetInt(&decimal2, -1234567890);

  // 0.0000001 with scale=7 -> internal value: 1
  ArrowDecimalInit(&decimal3, size, precision, scale);
  ArrowDecimalSetInt(&decimal3, 1);

  ArrowDecimalInit(&decimal_max_64, size, precision, scale);
  ArrowDecimalSetInt(&decimal_max_64, 9223372036854775807LL);

  ArrowDecimalInit(&decimal_min_64, size, precision, scale);
  ArrowDecimalSetInt(&decimal_min_64, -9223372036854775807LL - 1);

  ArrowDecimalInit(&decimal_zero, size, precision, scale);
  ArrowDecimalSetInt(&decimal_zero, 0);

  ArrowDecimalInit(&decimal_no_frac, size, precision, scale);
  ArrowDecimalSetInt(&decimal_no_frac, 10000000000LL);  // 1000 * 10^7 (1000.0000000)

  ArrowDecimalInit(&decimal_max_128, size, precision, scale);
  struct ArrowStringView max_digits_7;
  max_digits_7.data = "99999999999999999999999999999999999999";
  max_digits_7.size_bytes = 38;
  ArrowDecimalSetDigits(&decimal_max_128, max_digits_7);

  ArrowDecimalInit(&decimal_min_128, size, precision, scale);
  struct ArrowStringView min_digits_7;
  min_digits_7.data = "-99999999999999999999999999999999999999";
  min_digits_7.size_bytes = 39;
  ArrowDecimalSetDigits(&decimal_min_128, min_digits_7);

  const std::vector<std::optional<ArrowDecimal*>> values = {
      &decimal_max_128, &decimal_min_128, &decimal_zero,
      &decimal_no_frac, &decimal_max_64,  &decimal_min_64,
      &decimal1,        &decimal2,        &decimal3};

  ArrowSchemaInit(&schema.value);
  ASSERT_EQ(ArrowSchemaSetTypeStruct(&schema.value, 1), 0);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema.value.children[0], type, precision, scale),
            0);
  ASSERT_EQ(ArrowSchemaSetName(schema.value.children[0], "col"), 0);
  ASSERT_EQ(adbc_validation::MakeBatch<ArrowDecimal*>(&schema.value, &array.value,
                                                      &na_error, values),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  constexpr size_t buf_size = sizeof(kTestPgCopyNumericScale7) - 2;

  ASSERT_EQ(buf.size_bytes, static_cast<int64_t>(buf_size));
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyNumericScale7[i]) << " at position " << i;
  }
}

// Test for scale=0 (integers)
// COPY (SELECT CAST(col AS NUMERIC) AS col FROM (VALUES
// (99999999999999999999999999999999999999),
// (-99999999999999999999999999999999999999),
// (0), (1000000000000000000000000000000000), (9223372036854775807),
// (-9223372036854775808), (1), (100), (1000), (-100000)) AS drvd(col))
// TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyNumericScale0[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x0a, 0x00,
    0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x1c, 0x00, 0x0a, 0x00, 0x09, 0x40, 0x00, 0x00, 0x00, 0x00, 0x63, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x27, 0x0f, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0x00, 0x08, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x05, 0x00,
    0x04, 0x00, 0x00, 0x00, 0x00, 0x03, 0x9a, 0x0d, 0x2c, 0x01, 0x70, 0x15, 0x65, 0x16,
    0xaf, 0x00, 0x01, 0x00, 0x00, 0x00, 0x12, 0x00, 0x05, 0x00, 0x04, 0x40, 0x00, 0x00,
    0x00, 0x03, 0x9a, 0x0d, 0x2c, 0x01, 0x70, 0x15, 0x65, 0x16, 0xb0, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x0a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x64, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x03, 0xe8, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0a, 0x00, 0x01, 0x00, 0x01, 0x40,
    0x00, 0x00, 0x00, 0x00, 0x0a, 0xff, 0xff};

TEST_F(PostgresCopyTest, PostgresCopyWriteNumericScale0) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  constexpr enum ArrowType type = NANOARROW_TYPE_DECIMAL128;
  constexpr int32_t size = 128;
  constexpr int32_t precision = 38;
  constexpr int32_t scale = 0;

  struct ArrowDecimal decimal0;
  struct ArrowDecimal decimal1;
  struct ArrowDecimal decimal2;
  struct ArrowDecimal decimal3;
  struct ArrowDecimal decimal4;
  struct ArrowDecimal decimal_max_64;
  struct ArrowDecimal decimal_min_64;
  struct ArrowDecimal decimal_max_128;
  struct ArrowDecimal decimal_min_128;
  struct ArrowDecimal decimal_no_frac;

  ArrowDecimalInit(&decimal0, size, precision, scale);
  ArrowDecimalSetInt(&decimal0, 0);

  ArrowDecimalInit(&decimal1, size, precision, scale);
  ArrowDecimalSetInt(&decimal1, 1);

  ArrowDecimalInit(&decimal2, size, precision, scale);
  ArrowDecimalSetInt(&decimal2, 100);

  ArrowDecimalInit(&decimal3, size, precision, scale);
  ArrowDecimalSetInt(&decimal3, 1000);

  ArrowDecimalInit(&decimal4, size, precision, scale);
  ArrowDecimalSetInt(&decimal4, -100000);

  ArrowDecimalInit(&decimal_max_64, size, precision, scale);
  ArrowDecimalSetInt(&decimal_max_64, 9223372036854775807LL);

  ArrowDecimalInit(&decimal_min_64, size, precision, scale);
  ArrowDecimalSetInt(&decimal_min_64, -9223372036854775807LL - 1);

  ArrowDecimalInit(&decimal_max_128, size, precision, scale);
  struct ArrowStringView max_digits_0;
  max_digits_0.data = "99999999999999999999999999999999999999";
  max_digits_0.size_bytes = 38;
  ArrowDecimalSetDigits(&decimal_max_128, max_digits_0);

  ArrowDecimalInit(&decimal_min_128, size, precision, scale);
  struct ArrowStringView min_digits_0;
  min_digits_0.data = "-99999999999999999999999999999999999999";
  min_digits_0.size_bytes = 39;
  ArrowDecimalSetDigits(&decimal_min_128, min_digits_0);

  ArrowDecimalInit(&decimal_no_frac, size, precision, scale);
  struct ArrowStringView no_frac_digits_0;
  no_frac_digits_0.data = "1000000000000000000000000000000000";
  no_frac_digits_0.size_bytes = 34;
  ArrowDecimalSetDigits(&decimal_no_frac, no_frac_digits_0);

  const std::vector<std::optional<ArrowDecimal*>> values = {
      &decimal_max_128, &decimal_min_128, &decimal0, &decimal_no_frac, &decimal_max_64,
      &decimal_min_64,  &decimal1,        &decimal2, &decimal3,        &decimal4};

  ArrowSchemaInit(&schema.value);
  ASSERT_EQ(ArrowSchemaSetTypeStruct(&schema.value, 1), 0);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema.value.children[0], type, precision, scale),
            0);
  ASSERT_EQ(ArrowSchemaSetName(schema.value.children[0], "col"), 0);
  ASSERT_EQ(adbc_validation::MakeBatch<ArrowDecimal*>(&schema.value, &array.value,
                                                      &na_error, values),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  constexpr size_t buf_size = sizeof(kTestPgCopyNumericScale0) - 2;
  ASSERT_EQ(buf.size_bytes, static_cast<int64_t>(buf_size));
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyNumericScale0[i]) << " at position " << i;
  }
}

// Test negative scale
// COPY (SELECT CAST(col AS NUMERIC) AS col FROM (VALUES
//   (12300), (-12300), (0), (922337203685477580700),
//   (99999999999999999999999999999999999900),
//   (-99999999999999999999999999999999999900))
// AS drvd(col)) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyNumericNegScale2[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x02, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x08, 0xfc, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x0c, 0x00, 0x02, 0x00, 0x01, 0x40, 0x00, 0x00, 0x00, 0x00, 0x01, 0x08, 0xfc, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x14, 0x00, 0x06, 0x00, 0x05, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x09, 0x08, 0xb9, 0x1c, 0x23, 0x1a, 0xc6, 0x1e, 0x4e, 0x02, 0xbc, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x1c, 0x00, 0x0a, 0x00, 0x09, 0x00, 0x00, 0x00, 0x00, 0x00, 0x63, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x26, 0xac, 0x00, 0x01, 0x00, 0x00, 0x00, 0x1c, 0x00, 0x0a, 0x00, 0x09, 0x40,
    0x00, 0x00, 0x00, 0x00, 0x63, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27,
    0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x27, 0x0f, 0x26, 0xac, 0xff, 0xff};

TEST_F(PostgresCopyTest, PostgresCopyWriteNumericNegativeScale) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  constexpr enum ArrowType type = NANOARROW_TYPE_DECIMAL128;
  constexpr int32_t size = 128;
  constexpr int32_t precision = 38;
  constexpr int32_t scale = -2;

  struct ArrowDecimal decimal1;
  struct ArrowDecimal decimal2;
  struct ArrowDecimal decimal_zero;
  struct ArrowDecimal decimal_large;
  struct ArrowDecimal decimal_max_128;
  struct ArrowDecimal decimal_min_128;

  ArrowDecimalInit(&decimal1, size, precision, scale);
  ArrowDecimalSetInt(&decimal1, 123);

  ArrowDecimalInit(&decimal2, size, precision, scale);
  ArrowDecimalSetInt(&decimal2, -123);

  ArrowDecimalInit(&decimal_zero, size, precision, scale);
  ArrowDecimalSetInt(&decimal_zero, 0);

  ArrowDecimalInit(&decimal_large, size, precision, scale);
  ArrowDecimalSetInt(&decimal_large, 9223372036854775807LL);

  ArrowDecimalInit(&decimal_max_128, size, precision, scale);
  struct ArrowStringView max_digits;
  max_digits.data = "999999999999999999999999999999999999";
  max_digits.size_bytes = 36;
  ArrowDecimalSetDigits(&decimal_max_128, max_digits);

  ArrowDecimalInit(&decimal_min_128, size, precision, scale);
  struct ArrowStringView min_digits;
  min_digits.data = "-999999999999999999999999999999999999";
  min_digits.size_bytes = 37;  // 36 digits + 1 for '-' sign
  ArrowDecimalSetDigits(&decimal_min_128, min_digits);

  const std::vector<std::optional<ArrowDecimal*>> values = {
      &decimal1,      &decimal2,        &decimal_zero,
      &decimal_large, &decimal_max_128, &decimal_min_128};

  ArrowSchemaInit(&schema.value);
  ASSERT_EQ(ArrowSchemaSetTypeStruct(&schema.value, 1), 0);
  ASSERT_EQ(ArrowSchemaSetTypeDecimal(schema.value.children[0], type, precision, scale),
            0);
  ASSERT_EQ(ArrowSchemaSetName(schema.value.children[0], "col"), 0);
  ASSERT_EQ(adbc_validation::MakeBatch<ArrowDecimal*>(&schema.value, &array.value,
                                                      &na_error, values),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  constexpr size_t buf_size = sizeof(kTestPgCopyNumericNegScale2) - 2;
  ASSERT_EQ(buf.size_bytes, static_cast<int64_t>(buf_size));
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyNumericNegScale2[i]) << " at position " << i;
  }
}

TEST_F(PostgresCopyTest, PostgresCopyWriteNumericPreservesZeroIntegerGroupsDecimal128) {
  AssertNumericZeroIntegerGroupsRoundTrip<NANOARROW_TYPE_DECIMAL128>(*type_resolver_);
}

TEST_F(PostgresCopyTest, PostgresCopyWriteNumericPreservesZeroIntegerGroupsDecimal256) {
  AssertNumericZeroIntegerGroupsRoundTrip<NANOARROW_TYPE_DECIMAL256>(*type_resolver_);
}

using TimestampTestParamType =
    std::tuple<enum ArrowTimeUnit, const char*, std::vector<std::optional<int64_t>>>;

class PostgresCopyWriteTimestampTest
    : public testing::TestWithParam<TimestampTestParamType> {
  void SetUp() override {
    ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(SetupDatabase(&database_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(AdbcDatabaseInit(&database_, &error_), IsOkStatus(&error_));

    const auto pg_db =
        *reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database_.private_data);
    type_resolver_ = pg_db->type_resolver();
  }
  void TearDown() override {
    if (database_.private_data) {
      ASSERT_THAT(AdbcDatabaseRelease(&database_, &error_), IsOkStatus(&error_));
    }

    if (error_.release) error_.release(&error_);
  }

 protected:
  struct AdbcError error_ = {};
  struct AdbcDatabase database_ = {};
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
};

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
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

TEST_F(PostgresCopyTest, PostgresCopyWriteInterval) {
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
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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
    : public testing::TestWithParam<DurationTestParamType> {
  void SetUp() override {
    ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(SetupDatabase(&database_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(AdbcDatabaseInit(&database_, &error_), IsOkStatus(&error_));

    const auto pg_db =
        *reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database_.private_data);
    type_resolver_ = pg_db->type_resolver();
  }
  void TearDown() override {
    if (database_.private_data) {
      ASSERT_THAT(AdbcDatabaseRelease(&database_, &error_), IsOkStatus(&error_));
    }

    if (error_.release) error_.release(&error_);
  }

 protected:
  struct AdbcError error_ = {};
  struct AdbcDatabase database_ = {};
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
};

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
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

TEST_F(PostgresCopyTest, PostgresCopyWriteString) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;
  ASSERT_EQ(adbc_validation::MakeSchema(&schema.value, {{"col", NANOARROW_TYPE_STRING}}),
            ADBC_STATUS_OK);
  ASSERT_EQ(adbc_validation::MakeBatch<std::string>(
                &schema.value, &array.value, &na_error, {"abc", "1234", std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

TEST_F(PostgresCopyTest, PostgresCopyWriteLargeString) {
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
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

TEST_F(PostgresCopyTest, PostgresCopyWriteBinary) {
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
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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

class PostgresCopyListTest : public testing::TestWithParam<enum ArrowType> {
 public:
  void SetUp() override {
    ASSERT_THAT(AdbcDatabaseNew(&database_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(SetupDatabase(&database_, &error_), IsOkStatus(&error_));
    ASSERT_THAT(AdbcDatabaseInit(&database_, &error_), IsOkStatus(&error_));

    const auto pg_db =
        *reinterpret_cast<std::shared_ptr<PostgresDatabase>*>(database_.private_data);
    type_resolver_ = pg_db->type_resolver();
  }
  void TearDown() override {
    if (database_.private_data) {
      ASSERT_THAT(AdbcDatabaseRelease(&database_, &error_), IsOkStatus(&error_));
    }

    if (error_.release) error_.release(&error_);
  }

 protected:
  struct AdbcError error_ = {};
  struct AdbcDatabase database_ = {};
  std::shared_ptr<PostgresTypeResolver> type_resolver_;
};

// COPY (SELECT CAST("col" AS SMALLINT ARRAY) AS "col" FROM (  VALUES ('{-123, -1}'),
// ('{0, 1, 123}'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopySmallIntegerArray[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x02, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0xff, 0x85, 0x00, 0x00, 0x00, 0x02, 0xff,
    0xff, 0x00, 0x01, 0x00, 0x00, 0x00, 0x26, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x15, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x02, 0x00, 0x7b, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST_P(PostgresCopyListTest, PostgresCopyWriteListSmallInt) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(
                &schema.value, {adbc_validation::SchemaField::Nested(
                                   "col", GetParam(), {{"item", NANOARROW_TYPE_INT16}})}),
            ADBC_STATUS_OK);

  ASSERT_EQ(adbc_validation::MakeBatch<std::vector<int16_t>>(
                &schema.value, &array.value, &na_error,
                {std::vector<int16_t>{-123, -1}, std::vector<int16_t>{0, 1, 123},
                 std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopySmallIntegerArray) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopySmallIntegerArray[i]) << "failure at index " << i;
  }
}

TEST_P(PostgresCopyListTest, PostgresCopyWriteListInteger) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(
                &schema.value, {adbc_validation::SchemaField::Nested(
                                   "col", GetParam(), {{"item", NANOARROW_TYPE_INT32}})}),
            ADBC_STATUS_OK);

  ASSERT_EQ(adbc_validation::MakeBatch<std::vector<int32_t>>(
                &schema.value, &array.value, &na_error,
                {std::vector<int32_t>{-123, -1}, std::vector<int32_t>{0, 1, 123},
                 std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyIntegerArray) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyIntegerArray[i]) << "failure at index " << i;
  }
}

// COPY (SELECT CAST("col" AS BIGINT ARRAY) AS "col" FROM (  VALUES ('{-123, -1}'), ('{0,
// 1, 123}'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyBigIntegerArray[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x02, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff,
    0x85, 0x00, 0x00, 0x00, 0x08, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x38, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x08, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x08, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x7b, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST_P(PostgresCopyListTest, PostgresCopyWriteListBigInt) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(
                &schema.value, {adbc_validation::SchemaField::Nested(
                                   "col", GetParam(), {{"item", NANOARROW_TYPE_INT64}})}),
            ADBC_STATUS_OK);

  ASSERT_EQ(adbc_validation::MakeBatch<std::vector<int64_t>>(
                &schema.value, &array.value, &na_error,
                {std::vector<int64_t>{-123, -1}, std::vector<int64_t>{0, 1, 123},
                 std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyBigIntegerArray) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyBigIntegerArray[i]) << "failure at index " << i;
  }
}

// COPY (SELECT CAST("col" AS TEXT ARRAY) AS "col" FROM (  VALUES ('{"foo", "bar"}'),
// ('{"baz", "qux", "quux"}'), (NULL)) AS drvd("col")) TO '/tmp/pgout.data' WITH (FORMAT
// binary);
static const uint8_t kTestPgCopyTextArray[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x22, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19, 0x00, 0x00,
    0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x66, 0x6f, 0x6f,
    0x00, 0x00, 0x00, 0x03, 0x62, 0x61, 0x72, 0x00, 0x01, 0x00, 0x00, 0x00, 0x2a,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x19, 0x00,
    0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x03, 0x62, 0x61,
    0x7a, 0x00, 0x00, 0x00, 0x03, 0x71, 0x75, 0x78, 0x00, 0x00, 0x00, 0x04, 0x71,
    0x75, 0x75, 0x78, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST_P(PostgresCopyListTest, PostgresCopyWriteListVarchar) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(
      adbc_validation::MakeSchema(
          &schema.value, {adbc_validation::SchemaField::Nested(
                             "col", GetParam(), {{"item", NANOARROW_TYPE_STRING}})}),
      ADBC_STATUS_OK);

  ASSERT_EQ(adbc_validation::MakeBatch<std::vector<std::string>>(
                &schema.value, &array.value, &na_error,
                {std::vector<std::string>{"foo", "bar"},
                 std::vector<std::string>{"baz", "qux", "quux"}, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyTextArray) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyTextArray[i]) << "failure at index " << i;
  }
}

INSTANTIATE_TEST_SUITE_P(ArrowListTypes, PostgresCopyListTest,
                         testing::Values(NANOARROW_TYPE_LIST, NANOARROW_TYPE_LARGE_LIST));

// COPY (SELECT CAST("col" AS INTEGER ARRAY) AS "col" FROM (  VALUES ('{1, 2}'),
// ('{-1, -2}'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT BINARY);
static const uint8_t kTestPgCopyFixedSizeIntegerArray[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x02, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x04, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x02, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
    0x04, 0xff, 0xff, 0xff, 0xfe, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};
TEST_F(PostgresCopyTest, PostgresCopyWriteFixedSizeListInteger) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> array;
  struct ArrowError na_error;

  ASSERT_EQ(ArrowSchemaInitFromType(&schema.value, NANOARROW_TYPE_STRUCT), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaAllocateChildren(&schema.value, 1), NANOARROW_OK);

  ArrowSchemaInit(schema->children[0]);
  ASSERT_EQ(
      ArrowSchemaSetTypeFixedSize(schema->children[0], NANOARROW_TYPE_FIXED_SIZE_LIST, 2),
      NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), NANOARROW_OK);
  ASSERT_EQ(ArrowSchemaSetType(schema->children[0]->children[0], NANOARROW_TYPE_INT32),
            NANOARROW_OK);

  ASSERT_EQ(adbc_validation::MakeBatch<std::vector<int32_t>>(
                &schema.value, &array.value, &na_error,
                {std::vector<int32_t>{1, 2}, std::vector<int32_t>{-1, -2}, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester tester;
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);

  const struct ArrowBuffer buf = tester.WriteBuffer();
  // The last 2 bytes of a message can be transmitted via PQputCopyData
  // so no need to test those bytes from the Writer
  constexpr size_t buf_size = sizeof(kTestPgCopyFixedSizeIntegerArray) - 2;
  ASSERT_EQ(buf.size_bytes, buf_size);
  for (size_t i = 0; i < buf_size; i++) {
    ASSERT_EQ(buf.data[i], kTestPgCopyFixedSizeIntegerArray[i])
        << "failure at index " << i;
  }
}

// Regression test for https://github.com/apache/arrow-adbc/issues/4319.
// When the source array has offset > 0 (a sliced parent), the list writer
// must read child offsets at (array_view->offset + index), not at index.
// Writing rows 3..5 of a 6-row source via offset/length must produce the
// same body as writing those rows as a fresh 3-row array.
TEST_P(PostgresCopyListTest, PostgresCopyWriteListSlicedMatchesDirect) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> source;
  adbc_validation::Handle<struct ArrowArray> tail;
  struct ArrowError na_error;

  ASSERT_EQ(adbc_validation::MakeSchema(
                &schema.value, {adbc_validation::SchemaField::Nested(
                                   "col", GetParam(), {{"item", NANOARROW_TYPE_INT32}})}),
            ADBC_STATUS_OK);

  ASSERT_EQ(
      adbc_validation::MakeBatch<std::vector<int32_t>>(
          &schema.value, &source.value, &na_error,
          {std::vector<int32_t>{1, 2}, std::vector<int32_t>{3, 4, 5}, std::nullopt,
           std::vector<int32_t>{6}, std::vector<int32_t>{7, 8}, std::vector<int32_t>{9}}),
      ADBC_STATUS_OK);

  ASSERT_EQ(
      adbc_validation::MakeBatch<std::vector<int32_t>>(
          &schema.value, &tail.value, &na_error,
          {std::vector<int32_t>{6}, std::vector<int32_t>{7, 8}, std::vector<int32_t>{9}}),
      ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester ref_tester;
  ASSERT_EQ(ref_tester.Init(&schema.value, &tail.value, *type_resolver_), NANOARROW_OK);
  ASSERT_EQ(ref_tester.WriteAll(nullptr), ENODATA);
  const struct ArrowBuffer ref_buf = ref_tester.WriteBuffer();

  // Slice: hide the first 3 rows by setting offset/length on the struct
  // root and on the list-typed column.
  source->offset = 0;
  source->length = 3;
  source->children[0]->offset = 3;
  source->children[0]->length = 3;

  PostgresCopyStreamWriteTester sliced_tester;
  ASSERT_EQ(sliced_tester.Init(&schema.value, &source.value, *type_resolver_),
            NANOARROW_OK);
  ASSERT_EQ(sliced_tester.WriteAll(nullptr), ENODATA);
  const struct ArrowBuffer sliced_buf = sliced_tester.WriteBuffer();

  ASSERT_EQ(sliced_buf.size_bytes, ref_buf.size_bytes);
  for (int64_t i = 0; i < sliced_buf.size_bytes; i++) {
    ASSERT_EQ(sliced_buf.data[i], ref_buf.data[i]) << "failure at index " << i;
  }
}

// Same regression check for FIXED_SIZE_LIST, which takes the
// IsFixedSize=true branch in PostgresCopyListFieldWriter.
TEST_F(PostgresCopyTest, PostgresCopyWriteFixedSizeListSlicedMatchesDirect) {
  adbc_validation::Handle<struct ArrowSchema> schema;
  adbc_validation::Handle<struct ArrowArray> source;
  adbc_validation::Handle<struct ArrowArray> tail;
  struct ArrowError na_error;

  // Two FIXED_SIZE_LIST schemas of size 2 — one for the 6-row source, one
  // for the 3-row reference. Both are independently allocated because
  // MakeBatch consumes the schema state.
  auto build_schema = [](struct ArrowSchema* out) {
    ASSERT_EQ(ArrowSchemaInitFromType(out, NANOARROW_TYPE_STRUCT), NANOARROW_OK);
    ASSERT_EQ(ArrowSchemaAllocateChildren(out, 1), NANOARROW_OK);
    ArrowSchemaInit(out->children[0]);
    ASSERT_EQ(
        ArrowSchemaSetTypeFixedSize(out->children[0], NANOARROW_TYPE_FIXED_SIZE_LIST, 2),
        NANOARROW_OK);
    ASSERT_EQ(ArrowSchemaSetName(out->children[0], "col"), NANOARROW_OK);
    ASSERT_EQ(ArrowSchemaSetType(out->children[0]->children[0], NANOARROW_TYPE_INT32),
              NANOARROW_OK);
  };

  adbc_validation::Handle<struct ArrowSchema> tail_schema;
  build_schema(&schema.value);
  build_schema(&tail_schema.value);

  ASSERT_EQ(adbc_validation::MakeBatch<std::vector<int32_t>>(
                &schema.value, &source.value, &na_error,
                {std::vector<int32_t>{1, 2}, std::vector<int32_t>{3, 4}, std::nullopt,
                 std::vector<int32_t>{5, 6}, std::vector<int32_t>{7, 8}, std::nullopt}),
            ADBC_STATUS_OK);

  ASSERT_EQ(adbc_validation::MakeBatch<std::vector<int32_t>>(
                &tail_schema.value, &tail.value, &na_error,
                {std::vector<int32_t>{5, 6}, std::vector<int32_t>{7, 8}, std::nullopt}),
            ADBC_STATUS_OK);

  PostgresCopyStreamWriteTester ref_tester;
  ASSERT_EQ(ref_tester.Init(&tail_schema.value, &tail.value, *type_resolver_),
            NANOARROW_OK);
  ASSERT_EQ(ref_tester.WriteAll(nullptr), ENODATA);
  const struct ArrowBuffer ref_buf = ref_tester.WriteBuffer();

  source->offset = 0;
  source->length = 3;
  source->children[0]->offset = 3;
  source->children[0]->length = 3;

  PostgresCopyStreamWriteTester sliced_tester;
  ASSERT_EQ(sliced_tester.Init(&schema.value, &source.value, *type_resolver_),
            NANOARROW_OK);
  ASSERT_EQ(sliced_tester.WriteAll(nullptr), ENODATA);
  const struct ArrowBuffer sliced_buf = sliced_tester.WriteBuffer();

  ASSERT_EQ(sliced_buf.size_bytes, ref_buf.size_bytes);
  for (int64_t i = 0; i < sliced_buf.size_bytes; i++) {
    ASSERT_EQ(sliced_buf.data[i], ref_buf.data[i]) << "failure at index " << i;
  }
}

// COPY (SELECT CAST("col" AS INTEGER ARRAY) AS "col" FROM (VALUES ('{NULL,42}'),
// ('{0,7}'), ('{NULL,NULL}'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static const uint8_t kTestPgCopyNullIntegerArray[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x20, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00,
    0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
    0x04, 0x00, 0x00, 0x00, 0x2a, 0x00, 0x01, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00,
    0x00, 0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00,
    0x02, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x07, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x1c, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x17,
    0x00, 0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

// Regression for GH-4846: handle null elements inside lists
TEST_F(PostgresCopyTest, PostgresCopyWriteNullListElements) {
  for (const auto type :
       {NANOARROW_TYPE_LIST, NANOARROW_TYPE_LARGE_LIST, NANOARROW_TYPE_FIXED_SIZE_LIST}) {
    for (const bool sliced : {false, true}) {
      adbc_validation::Handle<struct ArrowSchema> schema;
      adbc_validation::Handle<struct ArrowArray> array;
      ASSERT_EQ(ArrowSchemaInitFromType(&schema.value, NANOARROW_TYPE_STRUCT), 0);
      ASSERT_EQ(ArrowSchemaAllocateChildren(&schema.value, 1), 0);
      ArrowSchemaInit(schema->children[0]);
      if (type == NANOARROW_TYPE_FIXED_SIZE_LIST) {
        ASSERT_EQ(ArrowSchemaSetTypeFixedSize(schema->children[0], type, 2), 0);
      } else {
        ASSERT_EQ(ArrowSchemaSetType(schema->children[0], type), 0);
      }
      ASSERT_EQ(ArrowSchemaSetName(schema->children[0], "col"), 0);
      ASSERT_EQ(
          ArrowSchemaSetType(schema->children[0]->children[0], NANOARROW_TYPE_INT32), 0);
      ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, &schema.value, nullptr), 0);
      ASSERT_EQ(ArrowArrayStartAppending(&array.value), 0);
      using Row = std::optional<std::vector<std::optional<int32_t>>>;
      const std::vector<Row> rows = {
          std::vector<std::optional<int32_t>>{std::nullopt, 42},
          std::vector<std::optional<int32_t>>{0, 7},
          std::vector<std::optional<int32_t>>{std::nullopt, std::nullopt}, std::nullopt};
      auto append_row = [&](const Row& row) {
        auto* list = array->children[0];
        if (!row.has_value()) {
          ASSERT_EQ(ArrowArrayAppendNull(list, 1), 0);
        } else {
          for (const auto& value : *row) {
            if (value.has_value()) {
              ASSERT_EQ(ArrowArrayAppendInt(list->children[0], *value), 0);
            } else {
              ASSERT_EQ(ArrowArrayAppendNull(list->children[0], 1), 0);
            }
          }
          ASSERT_EQ(ArrowArrayFinishElement(list), 0);
        }
        ASSERT_EQ(ArrowArrayFinishElement(&array.value), 0);
      };
      if (sliced) {
        ASSERT_NO_FATAL_FAILURE(append_row(std::vector<std::optional<int32_t>>{8, 9}));
      }
      for (const auto& row : rows) {
        ASSERT_NO_FATAL_FAILURE(append_row(row));
      }
      ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, nullptr), 0);
      if (sliced) {
        array->length = rows.size();
        array->children[0]->offset = 1;
        array->children[0]->length = rows.size();
      }

      PostgresCopyStreamWriteTester tester;
      ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), 0);
      ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);
      const struct ArrowBuffer buf = tester.WriteBuffer();
      // The last 2 bytes of a message can be transmitted via PQputCopyData
      // so no need to test those bytes from the Writer
      constexpr size_t buf_size = sizeof(kTestPgCopyNullIntegerArray) - 2;
      ASSERT_EQ(buf.size_bytes, buf_size);
      for (size_t i = 0; i < buf_size; i++) {
        ASSERT_EQ(buf.data[i], kTestPgCopyNullIntegerArray[i])
            << "failure at index " << i;
      }
    }
  }
}

TEST_F(PostgresCopyTest, PostgresCopyWriteNullDictionaryListElements) {
  for (const auto type : {NANOARROW_TYPE_STRING, NANOARROW_TYPE_BINARY}) {
    SCOPED_TRACE(type);
    std::vector<uint8_t> reference;
    for (const bool dictionary : {false, true}) {
      adbc_validation::Handle<struct ArrowSchema> schema;
      adbc_validation::Handle<struct ArrowArray> array;
      ASSERT_EQ(
          adbc_validation::MakeSchema(
              &schema.value, {adbc_validation::SchemaField::Nested(
                                 "col", NANOARROW_TYPE_LIST,
                                 {{"item", dictionary ? NANOARROW_TYPE_INT8 : type}})}),
          ADBC_STATUS_OK);
      if (dictionary) {
        auto* child = schema->children[0]->children[0];
        ASSERT_EQ(ArrowSchemaAllocateDictionary(child), 0);
        ASSERT_EQ(ArrowSchemaInitFromType(child->dictionary, type), 0);
      }
      ASSERT_EQ(ArrowArrayInitFromSchema(&array.value, &schema.value, nullptr), 0);
      ASSERT_EQ(ArrowArrayStartAppending(&array.value), 0);
      auto* list = array->children[0];
      auto* child = list->children[0];
      ArrowBufferView value;
      value.data.as_char = "x";
      value.size_bytes = 1;
      if (dictionary) {
        ASSERT_EQ(ArrowArrayAppendBytes(child->dictionary, value), 0);
        ASSERT_EQ(ArrowArrayAppendNull(child->dictionary, 1), 0);
        ASSERT_EQ(ArrowArrayFinishBuildingDefault(child->dictionary, nullptr), 0);
      }
      for (int row = 0; row < 3; ++row) {
        // Separate null indices from null dictionary values so each must set
        // the header flag. A null index's default zero payload points to "x".
        if (row == 0 || (!dictionary && row == 1)) {
          ASSERT_EQ(ArrowArrayAppendNull(child, 1), 0);
        } else if (dictionary) {
          ASSERT_EQ(ArrowArrayAppendInt(child, row == 1 ? 1 : 0), 0);
        } else {
          ASSERT_EQ(ArrowArrayAppendBytes(child, value), 0);
        }
        if (dictionary) {
          ASSERT_EQ(ArrowArrayAppendInt(child, 0), 0);
        } else {
          ASSERT_EQ(ArrowArrayAppendBytes(child, value), 0);
        }
        ASSERT_EQ(ArrowArrayFinishElement(list), 0);
        ASSERT_EQ(ArrowArrayFinishElement(&array.value), 0);
      }
      ASSERT_EQ(ArrowArrayFinishBuildingDefault(&array.value, nullptr), 0);
      PostgresCopyStreamWriteTester tester;
      ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), 0);
      ASSERT_EQ(tester.WriteAll(nullptr), ENODATA);
      const auto buf = tester.WriteBuffer();
      const std::vector<uint8_t> actual(buf.data, buf.data + buf.size_bytes);
      if (dictionary) {
        EXPECT_EQ(actual, reference);
      } else {
        reference = actual;
      }
    }
  }
}

TEST_F(PostgresCopyTest, PostgresCopyWriteMultiBatch) {
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
  ASSERT_EQ(tester.Init(&schema.value, &array.value, *type_resolver_), NANOARROW_OK);
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
