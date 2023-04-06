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

#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>

#include "postgres_copy_utils.h"

using adbcpq::PostgresCopyStreamReader;
using adbcpq::PostgresType;

class PostgresCopyStreamTester {
 public:
  ArrowErrorCode Init(const PostgresType& root_type, ArrowError* error = nullptr) {
    NANOARROW_RETURN_NOT_OK(reader_.Init(root_type));
    NANOARROW_RETURN_NOT_OK(reader_.InferOutputSchema(error));
    NANOARROW_RETURN_NOT_OK(reader_.InitFieldReaders(error));
    return NANOARROW_OK;
  }

  ArrowErrorCode ReadAll(ArrowBufferView* data, ArrowError* error = nullptr) {
    NANOARROW_RETURN_NOT_OK(reader_.ReadHeader(data, error));

    int result;
    do {
      result = reader_.ReadRecord(data, error);
    } while (result == NANOARROW_OK);

    return result;
  }

  void GetSchema(ArrowSchema* out) { reader_.GetSchema(out); }

  ArrowErrorCode GetArray(ArrowArray* out, ArrowError* error = nullptr) {
    return reader_.GetArray(out, error);
  }

 private:
  PostgresCopyStreamReader reader_;
};

// COPY (SELECT CAST("col" AS BOOLEAN) AS "col" FROM (  VALUES (TRUE), (FALSE), (NULL)) AS
// drvd("col")) TO STDOUT;
static uint8_t kTestPgCopyBoolean[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x01, 0x01, 0x00, 0x01,
    0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST(PostgresCopyUtilsTest, PostgresCopyReadBoolean) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyBoolean;
  data.size_bytes = sizeof(kTestPgCopyBoolean);

  auto col_type = PostgresType(PostgresType::PG_RECV_BOOL);
  PostgresType input_type(PostgresType::PG_RECV_RECORD);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);

  // Apparently the output above contains an extra 0xff 0xff at the end
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyBoolean, sizeof(kTestPgCopyBoolean));
  ASSERT_EQ(data.size_bytes, 0);

  struct ArrowArray array;
  ASSERT_EQ(tester.GetArray(&array), NANOARROW_OK);
  ASSERT_EQ(array.length, 3);
  ASSERT_EQ(array.n_children, 1);

  const uint8_t* validity =
      reinterpret_cast<const uint8_t*>(array.children[0]->buffers[0]);
  const uint8_t* data_buffer =
      reinterpret_cast<const uint8_t*>(array.children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 2));

  ASSERT_TRUE(ArrowBitGet(data_buffer, 0));
  ASSERT_FALSE(ArrowBitGet(data_buffer, 1));
  ASSERT_FALSE(ArrowBitGet(data_buffer, 2));

  array.release(&array);
}

// COPY (SELECT CAST("col" AS SMALLINT) AS "col" FROM (  VALUES (-123), (-1), (1), (123),
// (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopySmallInt[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x02, 0xff, 0x85, 0x00, 0x01, 0x00, 0x00, 0x00, 0x02, 0xff, 0xff, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x02, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x02, 0x00, 0x7b, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST(PostgresCopyUtilsTest, PostgresCopyReadSmallInt) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopySmallInt;
  data.size_bytes = sizeof(kTestPgCopySmallInt);

  auto col_type = PostgresType(PostgresType::PG_RECV_INT2);
  PostgresType input_type(PostgresType::PG_RECV_RECORD);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopySmallInt, sizeof(kTestPgCopySmallInt));
  ASSERT_EQ(data.size_bytes, 0);

  struct ArrowArray array;
  ASSERT_EQ(tester.GetArray(&array), NANOARROW_OK);
  ASSERT_EQ(array.length, 5);
  ASSERT_EQ(array.n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array.children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const int16_t*>(array.children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_TRUE(ArrowBitGet(validity, 2));
  ASSERT_TRUE(ArrowBitGet(validity, 3));
  ASSERT_FALSE(ArrowBitGet(validity, 4));

  ASSERT_EQ(data_buffer[0], -123);
  ASSERT_EQ(data_buffer[1], -1);
  ASSERT_EQ(data_buffer[2], 1);
  ASSERT_EQ(data_buffer[3], 123);
  ASSERT_EQ(data_buffer[4], 0);

  array.release(&array);
}

// COPY (SELECT CAST("col" AS INTEGER) AS "col" FROM (  VALUES (-123), (-1), (1), (123),
// (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyInteger[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff,
    0x85, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00,
    0x00, 0x00, 0x7b, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST(PostgresCopyUtilsTest, PostgresCopyReadInteger) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyInteger;
  data.size_bytes = sizeof(kTestPgCopyInteger);

  auto col_type = PostgresType(PostgresType::PG_RECV_INT4);
  PostgresType input_type(PostgresType::PG_RECV_RECORD);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyInteger, sizeof(kTestPgCopyInteger));
  ASSERT_EQ(data.size_bytes, 0);

  struct ArrowArray array;
  ASSERT_EQ(tester.GetArray(&array), NANOARROW_OK);
  ASSERT_EQ(array.length, 5);
  ASSERT_EQ(array.n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array.children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const int32_t*>(array.children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_TRUE(ArrowBitGet(validity, 2));
  ASSERT_TRUE(ArrowBitGet(validity, 3));
  ASSERT_FALSE(ArrowBitGet(validity, 4));

  ASSERT_EQ(data_buffer[0], -123);
  ASSERT_EQ(data_buffer[1], -1);
  ASSERT_EQ(data_buffer[2], 1);
  ASSERT_EQ(data_buffer[3], 123);
  ASSERT_EQ(data_buffer[4], 0);

  array.release(&array);
}

// COPY (SELECT CAST("col" AS BIGINT) AS "col" FROM (  VALUES (-123), (-1), (1), (123),
// (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyBigInt[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0x85, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0xff, 0xff, 0xff,
    0xff, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x7b, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST(PostgresCopyUtilsTest, PostgresCopyReadBigInt) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyBigInt;
  data.size_bytes = sizeof(kTestPgCopyBigInt);

  auto col_type = PostgresType(PostgresType::PG_RECV_INT8);
  PostgresType input_type(PostgresType::PG_RECV_RECORD);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyBigInt, sizeof(kTestPgCopyBigInt));
  ASSERT_EQ(data.size_bytes, 0);

  struct ArrowArray array;
  ASSERT_EQ(tester.GetArray(&array), NANOARROW_OK);
  ASSERT_EQ(array.length, 5);
  ASSERT_EQ(array.n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array.children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const int64_t*>(array.children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_TRUE(ArrowBitGet(validity, 2));
  ASSERT_TRUE(ArrowBitGet(validity, 3));
  ASSERT_FALSE(ArrowBitGet(validity, 4));

  ASSERT_EQ(data_buffer[0], -123);
  ASSERT_EQ(data_buffer[1], -1);
  ASSERT_EQ(data_buffer[2], 1);
  ASSERT_EQ(data_buffer[3], 123);
  ASSERT_EQ(data_buffer[4], 0);

  array.release(&array);
}

// COPY (SELECT CAST("col" AS REAL) AS "col" FROM (  VALUES (-123.456), (-1), (1),
// (123.456), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyReal[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xc2, 0xf6, 0xe9,
    0x79, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xbf, 0x80, 0x00, 0x00, 0x00, 0x01, 0x00,
    0x00, 0x00, 0x04, 0x3f, 0x80, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x42,
    0xf6, 0xe9, 0x79, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST(PostgresCopyUtilsTest, PostgresCopyReadReal) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyReal;
  data.size_bytes = sizeof(kTestPgCopyReal);

  auto col_type = PostgresType(PostgresType::PG_RECV_FLOAT4);
  PostgresType input_type(PostgresType::PG_RECV_RECORD);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyReal, sizeof(kTestPgCopyReal));
  ASSERT_EQ(data.size_bytes, 0);

  struct ArrowArray array;
  ASSERT_EQ(tester.GetArray(&array), NANOARROW_OK);
  ASSERT_EQ(array.length, 5);
  ASSERT_EQ(array.n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array.children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const float*>(array.children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_TRUE(ArrowBitGet(validity, 2));
  ASSERT_TRUE(ArrowBitGet(validity, 3));
  ASSERT_FALSE(ArrowBitGet(validity, 4));

  ASSERT_FLOAT_EQ(data_buffer[0], -123.456);
  ASSERT_EQ(data_buffer[1], -1);
  ASSERT_EQ(data_buffer[2], 1);
  ASSERT_FLOAT_EQ(data_buffer[3], 123.456);
  ASSERT_EQ(data_buffer[4], 0);

  array.release(&array);
}

// COPY (SELECT CAST("col" AS DOUBLE PRECISION) AS "col" FROM (  VALUES (-123.456), (-1),
// (1), (123.456), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyDoublePrecision[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0xc0, 0x5e, 0xdd,
    0x2f, 0x1a, 0x9f, 0xbe, 0x77, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0xbf, 0xf0, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x3f, 0xf0, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x08, 0x40, 0x5e, 0xdd,
    0x2f, 0x1a, 0x9f, 0xbe, 0x77, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST(PostgresCopyUtilsTest, PostgresCopyReadDoublePrecision) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyDoublePrecision;
  data.size_bytes = sizeof(kTestPgCopyDoublePrecision);

  auto col_type = PostgresType(PostgresType::PG_RECV_FLOAT8);
  PostgresType input_type(PostgresType::PG_RECV_RECORD);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyDoublePrecision,
            sizeof(kTestPgCopyDoublePrecision));
  ASSERT_EQ(data.size_bytes, 0);

  struct ArrowArray array;
  ASSERT_EQ(tester.GetArray(&array), NANOARROW_OK);
  ASSERT_EQ(array.length, 5);
  ASSERT_EQ(array.n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array.children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const double*>(array.children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_TRUE(ArrowBitGet(validity, 2));
  ASSERT_TRUE(ArrowBitGet(validity, 3));
  ASSERT_FALSE(ArrowBitGet(validity, 4));

  ASSERT_DOUBLE_EQ(data_buffer[0], -123.456);
  ASSERT_EQ(data_buffer[1], -1);
  ASSERT_EQ(data_buffer[2], 1);
  ASSERT_DOUBLE_EQ(data_buffer[3], 123.456);
  ASSERT_EQ(data_buffer[4], 0);

  array.release(&array);
}

// COPY (SELECT CAST("col" AS TEXT) AS "col" FROM (  VALUES ('abc'), ('1234'),
// (NULL::text)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyText[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00,
    0x03, 0x61, 0x62, 0x63, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x31, 0x32,
    0x33, 0x34, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST(PostgresCopyUtilsTest, PostgresCopyReadText) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyText;
  data.size_bytes = sizeof(kTestPgCopyText);

  auto col_type = PostgresType(PostgresType::PG_RECV_TEXT);
  PostgresType input_type(PostgresType::PG_RECV_RECORD);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyText, sizeof(kTestPgCopyText));
  ASSERT_EQ(data.size_bytes, 0);

  struct ArrowArray array;
  ASSERT_EQ(tester.GetArray(&array), NANOARROW_OK);
  ASSERT_EQ(array.length, 3);
  ASSERT_EQ(array.n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array.children[0]->buffers[0]);
  auto offsets = reinterpret_cast<const int32_t*>(array.children[0]->buffers[1]);
  auto data_buffer = reinterpret_cast<const char*>(array.children[0]->buffers[2]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 2));

  ASSERT_EQ(offsets[0], 0);
  ASSERT_EQ(offsets[1], 3);
  ASSERT_EQ(offsets[2], 7);
  ASSERT_EQ(offsets[3], 7);

  ASSERT_EQ(std::string(data_buffer + 0, 3), "abc");
  ASSERT_EQ(std::string(data_buffer + 3, 4), "1234");

  array.release(&array);
}

// COPY (SELECT CAST("col" AS INTEGER ARRAY) AS "col" FROM (  VALUES ('{-123, -1}'), ('{0,
// 1, 123}'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyIntegerArray[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x24, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x02, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0xff, 0xff, 0xff, 0x85, 0x00, 0x00, 0x00,
    0x04, 0xff, 0xff, 0xff, 0xff, 0x00, 0x01, 0x00, 0x00, 0x00, 0x2c, 0x00, 0x00, 0x00,
    0x01, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x03, 0x00,
    0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    0x04, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00, 0x7b, 0x00,
    0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST(PostgresCopyUtilsTest, PostgresCopyReadArray) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyIntegerArray;
  data.size_bytes = sizeof(kTestPgCopyIntegerArray);

  auto col_type = PostgresType(PostgresType::PG_RECV_INT4).Array();
  PostgresType input_type(PostgresType::PG_RECV_RECORD);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyIntegerArray,
            sizeof(kTestPgCopyIntegerArray));
  ASSERT_EQ(data.size_bytes, 0);

  struct ArrowArray array;
  ASSERT_EQ(tester.GetArray(&array), NANOARROW_OK);
  ASSERT_EQ(array.length, 3);
  ASSERT_EQ(array.n_children, 1);
  ASSERT_EQ(array.children[0]->n_children, 1);
  ASSERT_EQ(array.children[0]->children[0]->length, 5);

  auto validity = reinterpret_cast<const uint8_t*>(array.children[0]->buffers[0]);
  auto offsets = reinterpret_cast<const int32_t*>(array.children[0]->buffers[1]);
  auto data_buffer =
      reinterpret_cast<const int32_t*>(array.children[0]->children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 2));

  ASSERT_EQ(offsets[0], 0);
  ASSERT_EQ(offsets[1], 2);
  ASSERT_EQ(offsets[2], 5);
  ASSERT_EQ(offsets[3], 5);

  ASSERT_EQ(data_buffer[0], -123);
  ASSERT_EQ(data_buffer[1], -1);
  ASSERT_EQ(data_buffer[2], 0);
  ASSERT_EQ(data_buffer[3], 1);
  ASSERT_EQ(data_buffer[4], 123);

  array.release(&array);
}

// CREATE TYPE custom_record AS (nested1 integer, nested2 double precision);
// COPY (SELECT CAST("col" AS custom_record) AS "col" FROM (  VALUES ('(123, 456.789)'),
// ('(12, 345.678)'), (NULL)) AS drvd("col")) TO STDOUT WITH (FORMAT binary);
static uint8_t kTestPgCopyCustomRecord[] = {
    0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00,
    0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x20, 0x00,
    0x00, 0x00, 0x02, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00,
    0x00, 0x7b, 0x00, 0x00, 0x02, 0xbd, 0x00, 0x00, 0x00, 0x08, 0x40, 0x7c, 0x8c,
    0x9f, 0xbe, 0x76, 0xc8, 0xb4, 0x00, 0x01, 0x00, 0x00, 0x00, 0x20, 0x00, 0x00,
    0x00, 0x02, 0x00, 0x00, 0x00, 0x17, 0x00, 0x00, 0x00, 0x04, 0x00, 0x00, 0x00,
    0x0c, 0x00, 0x00, 0x02, 0xbd, 0x00, 0x00, 0x00, 0x08, 0x40, 0x75, 0x9a, 0xd9,
    0x16, 0x87, 0x2b, 0x02, 0x00, 0x01, 0xff, 0xff, 0xff, 0xff, 0xff, 0xff};

TEST(PostgresCopyUtilsTest, PostgresCopyReadCustomRecord) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyCustomRecord;
  data.size_bytes = sizeof(kTestPgCopyCustomRecord);

  auto col_type = PostgresType(PostgresType::PG_RECV_RECORD);
  col_type.AppendChild("nested1", PostgresType(PostgresType::PG_RECV_INT4));
  col_type.AppendChild("nested2", PostgresType(PostgresType::PG_RECV_FLOAT8));
  PostgresType input_type(PostgresType::PG_RECV_RECORD);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyCustomRecord,
            sizeof(kTestPgCopyCustomRecord));
  ASSERT_EQ(data.size_bytes, 0);

  struct ArrowArray array;
  ASSERT_EQ(tester.GetArray(&array), NANOARROW_OK);
  ASSERT_EQ(array.length, 3);
  ASSERT_EQ(array.n_children, 1);
  ASSERT_EQ(array.children[0]->n_children, 2);
  ASSERT_EQ(array.children[0]->children[0]->length, 3);
  ASSERT_EQ(array.children[0]->children[1]->length, 3);

  auto validity = reinterpret_cast<const uint8_t*>(array.children[0]->buffers[0]);
  auto data_buffer1 =
      reinterpret_cast<const int32_t*>(array.children[0]->children[0]->buffers[1]);
  auto data_buffer2 =
      reinterpret_cast<const double*>(array.children[0]->children[1]->buffers[1]);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 2));

  ASSERT_EQ(data_buffer1[0], 123);
  ASSERT_EQ(data_buffer1[1], 12);
  ASSERT_EQ(data_buffer1[2], 0);

  ASSERT_DOUBLE_EQ(data_buffer2[0], 456.789);
  ASSERT_DOUBLE_EQ(data_buffer2[1], 345.678);
  ASSERT_DOUBLE_EQ(data_buffer2[2], 0);

  array.release(&array);
}
