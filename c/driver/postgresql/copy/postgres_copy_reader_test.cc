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
#include <limits>
#include <memory>
#include <string>
#include <vector>

#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.hpp>

#include "postgres_copy_test_common.h"
#include "postgresql/copy/reader.h"

namespace adbcpq {

class PostgresCopyStreamTester {
 public:
  ArrowErrorCode Init(const PostgresType& root_type, ArrowError* error = nullptr) {
    NANOARROW_RETURN_NOT_OK(reader_.Init(root_type));
    NANOARROW_RETURN_NOT_OK(reader_.InferOutputSchema("PostgreSQL Tester", error));
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

TEST(PostgresCopyUtilsTest, PostgresCopyReadBoolean) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyBoolean;
  data.size_bytes = sizeof(kTestPgCopyBoolean);

  auto col_type = PostgresType(PostgresTypeId::kBool);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);

  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyBoolean, sizeof(kTestPgCopyBoolean));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  const uint8_t* validity =
      reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  const uint8_t* data_buffer =
      reinterpret_cast<const uint8_t*>(array->children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 2));

  ASSERT_TRUE(ArrowBitGet(data_buffer, 0));
  ASSERT_FALSE(ArrowBitGet(data_buffer, 1));
  ASSERT_FALSE(ArrowBitGet(data_buffer, 2));
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadSmallInt) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopySmallInt;
  data.size_bytes = sizeof(kTestPgCopySmallInt);

  auto col_type = PostgresType(PostgresTypeId::kInt2);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopySmallInt, sizeof(kTestPgCopySmallInt));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 5);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const int16_t*>(array->children[0]->buffers[1]);
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
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadInteger) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyInteger;
  data.size_bytes = sizeof(kTestPgCopyInteger);

  auto col_type = PostgresType(PostgresTypeId::kInt4);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyInteger, sizeof(kTestPgCopyInteger));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 5);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const int32_t*>(array->children[0]->buffers[1]);
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
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadBigInt) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyBigInt;
  data.size_bytes = sizeof(kTestPgCopyBigInt);

  auto col_type = PostgresType(PostgresTypeId::kInt8);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyBigInt, sizeof(kTestPgCopyBigInt));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 5);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const int64_t*>(array->children[0]->buffers[1]);
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
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadReal) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyReal;
  data.size_bytes = sizeof(kTestPgCopyReal);

  auto col_type = PostgresType(PostgresTypeId::kFloat4);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyReal, sizeof(kTestPgCopyReal));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 5);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const float*>(array->children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_TRUE(ArrowBitGet(validity, 2));
  ASSERT_TRUE(ArrowBitGet(validity, 3));
  ASSERT_FALSE(ArrowBitGet(validity, 4));

  ASSERT_FLOAT_EQ(data_buffer[0], -123.456f);
  ASSERT_EQ(data_buffer[1], -1);
  ASSERT_EQ(data_buffer[2], 1);
  ASSERT_FLOAT_EQ(data_buffer[3], 123.456f);
  ASSERT_EQ(data_buffer[4], 0);
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadDoublePrecision) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyDoublePrecision;
  data.size_bytes = sizeof(kTestPgCopyDoublePrecision);

  auto col_type = PostgresType(PostgresTypeId::kFloat8);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyDoublePrecision,
            sizeof(kTestPgCopyDoublePrecision));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 5);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const double*>(array->children[0]->buffers[1]);
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
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadDate) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyDate;
  data.size_bytes = sizeof(kTestPgCopyDate);

  auto col_type = PostgresType(PostgresTypeId::kDate);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyDate, sizeof(kTestPgCopyDate));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const int32_t*>(array->children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 2));

  ASSERT_EQ(data_buffer[0], -25567);
  ASSERT_EQ(data_buffer[1], 47482);
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadTime) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyTime;
  data.size_bytes = sizeof(kTestPgCopyTime);

  auto col_type = PostgresType(PostgresTypeId::kTime);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyTime, sizeof(kTestPgCopyTime));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 4);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const int64_t*>(array->children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_TRUE(ArrowBitGet(validity, 2));
  ASSERT_FALSE(ArrowBitGet(validity, 3));

  ASSERT_EQ(data_buffer[0], 0);
  ASSERT_EQ(data_buffer[1], 86399000000);
  ASSERT_EQ(data_buffer[2], 49376123456);
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadNumeric) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyNumeric;
  data.size_bytes = sizeof(kTestPgCopyNumeric);

  auto col_type = PostgresType(PostgresTypeId::kNumeric);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyNumeric, sizeof(kTestPgCopyNumeric));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 9);
  ASSERT_EQ(array->n_children, 1);

  nanoarrow::UniqueSchema schema;
  tester.GetSchema(schema.get());

  nanoarrow::UniqueArrayView array_view;
  ASSERT_EQ(ArrowArrayViewInitFromSchema(array_view.get(), schema.get(), nullptr),
            NANOARROW_OK);
  ASSERT_EQ(array_view->children[0]->storage_type, NANOARROW_TYPE_STRING);
  ASSERT_EQ(ArrowArrayViewSetArray(array_view.get(), array.get(), nullptr), NANOARROW_OK);

  auto validity = array_view->children[0]->buffer_views[0].data.as_uint8;
  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_TRUE(ArrowBitGet(validity, 2));
  ASSERT_TRUE(ArrowBitGet(validity, 3));
  ASSERT_TRUE(ArrowBitGet(validity, 4));
  ASSERT_TRUE(ArrowBitGet(validity, 5));
  ASSERT_TRUE(ArrowBitGet(validity, 6));
  ASSERT_TRUE(ArrowBitGet(validity, 7));
  ASSERT_FALSE(ArrowBitGet(validity, 8));

  struct ArrowStringView item;
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 0);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "1000000");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 1);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "0.00001234");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 2);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "1.0000");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 3);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "-123.456");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 4);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "123.456");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 5);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "nan");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 6);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "-inf");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 7);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "inf");
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadNumeric16_10) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyNumeric16_10;
  data.size_bytes = sizeof(kTestPgCopyNumeric16_10);

  auto col_type = PostgresType(PostgresTypeId::kNumeric);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyNumeric16_10,
            sizeof(kTestPgCopyNumeric16_10));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 7);
  ASSERT_EQ(array->n_children, 1);

  nanoarrow::UniqueSchema schema;
  tester.GetSchema(schema.get());

  nanoarrow::UniqueArrayView array_view;
  ASSERT_EQ(ArrowArrayViewInitFromSchema(array_view.get(), schema.get(), nullptr),
            NANOARROW_OK);
  ASSERT_EQ(array_view->children[0]->storage_type, NANOARROW_TYPE_STRING);
  ASSERT_EQ(ArrowArrayViewSetArray(array_view.get(), array.get(), nullptr), NANOARROW_OK);

  auto validity = array_view->children[0]->buffer_views[0].data.as_uint8;
  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_TRUE(ArrowBitGet(validity, 2));
  ASSERT_TRUE(ArrowBitGet(validity, 3));
  ASSERT_TRUE(ArrowBitGet(validity, 4));
  ASSERT_TRUE(ArrowBitGet(validity, 5));
  ASSERT_FALSE(ArrowBitGet(validity, 6));

  struct ArrowStringView item;
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 0);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "0.0000000000");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 1);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "1.0123400000");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 2);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "1.0123456789");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 3);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "-1.0123400000");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 4);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "-1.0123456789");
  item = ArrowArrayViewGetStringUnsafe(array_view->children[0], 5);
  EXPECT_EQ(std::string(item.data, item.size_bytes), "nan");
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadTimestamp) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyTimestamp;
  data.size_bytes = sizeof(kTestPgCopyTimestamp);

  auto col_type = PostgresType(PostgresTypeId::kTimestamp);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyTimestamp, sizeof(kTestPgCopyTimestamp));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto data_buffer = reinterpret_cast<const int64_t*>(array->children[0]->buffers[1]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 3));

  ASSERT_EQ(data_buffer[0], -2208943504000000);
  ASSERT_EQ(data_buffer[1], 4102490096000000);
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadInterval) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyInterval;
  data.size_bytes = sizeof(kTestPgCopyInterval);

  auto col_type = PostgresType(PostgresTypeId::kInterval);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyInterval, sizeof(kTestPgCopyInterval));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  nanoarrow::UniqueSchema schema;
  tester.GetSchema(schema.get());

  nanoarrow::UniqueArrayView array_view;
  ASSERT_EQ(ArrowArrayViewInitFromSchema(array_view.get(), schema.get(), nullptr),
            NANOARROW_OK);
  ASSERT_EQ(array_view->children[0]->storage_type,
            NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
  ASSERT_EQ(ArrowArrayViewSetArray(array_view.get(), array.get(), nullptr), NANOARROW_OK);

  auto validity = array_view->children[0]->buffer_views[0].data.as_uint8;
  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 2));

  struct ArrowInterval interval;
  ArrowIntervalInit(&interval, NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO);
  ArrowArrayViewGetIntervalUnsafe(array_view->children[0], 0, &interval);
  ASSERT_EQ(interval.months, -1);
  ASSERT_EQ(interval.days, -2);
  ASSERT_EQ(interval.ns, -4000000000);
  ArrowArrayViewGetIntervalUnsafe(array_view->children[0], 1, &interval);
  ASSERT_EQ(interval.months, 1);
  ASSERT_EQ(interval.days, 2);
  ASSERT_EQ(interval.ns, 4000000000);
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadText) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyText;
  data.size_bytes = sizeof(kTestPgCopyText);

  auto col_type = PostgresType(PostgresTypeId::kText);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyText, sizeof(kTestPgCopyText));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto offsets = reinterpret_cast<const int32_t*>(array->children[0]->buffers[1]);
  auto data_buffer = reinterpret_cast<const char*>(array->children[0]->buffers[2]);
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
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadEnum) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyEnum;
  data.size_bytes = sizeof(kTestPgCopyEnum);

  auto col_type = PostgresType(PostgresTypeId::kEnum);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyEnum, sizeof(kTestPgCopyEnum));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto offsets = reinterpret_cast<const int32_t*>(array->children[0]->buffers[1]);
  auto data_buffer = reinterpret_cast<const char*>(array->children[0]->buffers[2]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 2));

  ASSERT_EQ(offsets[0], 0);
  ASSERT_EQ(offsets[1], 2);
  ASSERT_EQ(offsets[2], 5);
  ASSERT_EQ(offsets[3], 5);

  ASSERT_EQ(std::string(data_buffer + 0, 2), "ok");
  ASSERT_EQ(std::string(data_buffer + 2, 3), "sad");
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadJson) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyJson;
  data.size_bytes = sizeof(kTestPgCopyJson);

  auto col_type = PostgresType(PostgresTypeId::kJson);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyJson, sizeof(kTestPgCopyJson));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto offsets = reinterpret_cast<const int32_t*>(array->children[0]->buffers[1]);
  auto data_buffer = reinterpret_cast<const char*>(array->children[0]->buffers[2]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 2));

  ASSERT_EQ(offsets[0], 0);
  ASSERT_EQ(offsets[1], 9);
  ASSERT_EQ(offsets[2], 18);
  ASSERT_EQ(offsets[3], 18);

  ASSERT_EQ(std::string(data_buffer, 9), "[1, 2, 3]");
  ASSERT_EQ(std::string(data_buffer + 9, 9), "[4, 5, 6]");
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadJsonb) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyJsonb;
  data.size_bytes = sizeof(kTestPgCopyJsonb);

  auto col_type = PostgresType(PostgresTypeId::kJsonb);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  struct ArrowError error;
  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data, &error), ENODATA) << error.message;
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyJsonb, sizeof(kTestPgCopyJsonb));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;

  ASSERT_EQ(tester.GetArray(array.get(), &error), NANOARROW_OK) << error.message;
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto offsets = reinterpret_cast<const int32_t*>(array->children[0]->buffers[1]);
  auto data_buffer = reinterpret_cast<const char*>(array->children[0]->buffers[2]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 2));

  ASSERT_EQ(offsets[0], 0);
  ASSERT_EQ(offsets[1], 9);
  ASSERT_EQ(offsets[2], 18);
  ASSERT_EQ(offsets[3], 18);

  ASSERT_EQ(std::string(data_buffer, 9), "[1, 2, 3]");
  ASSERT_EQ(std::string(data_buffer + 9, 9), "[4, 5, 6]");
}

TEST(PostgresCopyUtilsTest, JsonbInvalidLength) {
  const uint8_t buffer[] = {0x01};
  const struct {
    int32_t field_size_bytes;
    int64_t input_size_bytes;
  } cases[] = {{0, 0}, {0, 1}, {1, 0}, {2, 1}};

  for (const auto& test_case : cases) {
    SCOPED_TRACE(test_case.field_size_bytes);
    SCOPED_TRACE(test_case.input_size_bytes);

    nanoarrow::UniqueSchema schema;
    ASSERT_EQ(NANOARROW_OK, ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_STRING));

    nanoarrow::UniqueArray array;
    ASSERT_EQ(NANOARROW_OK, ArrowArrayInitFromSchema(array.get(), schema.get(), nullptr));
    ASSERT_EQ(NANOARROW_OK, ArrowArrayStartAppending(array.get()));

    PostgresCopyJsonbFieldReader reader;
    ASSERT_EQ(NANOARROW_OK, reader.InitSchema(schema.get()));
    ASSERT_EQ(NANOARROW_OK, reader.InitArray(array.get()));

    ArrowBufferView data;
    data.data.as_uint8 = buffer;
    data.size_bytes = test_case.input_size_bytes;
    ArrowError error = {};
    ASSERT_EQ(EINVAL,
              reader.Read(&data, test_case.field_size_bytes, array.get(), &error));
    ASSERT_NE(error.message[0], '\0');
    ASSERT_EQ(data.data.as_uint8, buffer);
    ASSERT_EQ(data.size_bytes, test_case.input_size_bytes);
    ASSERT_EQ(array->length, 0);
    ASSERT_EQ(ArrowArrayBuffer(array.get(), 1)->size_bytes, sizeof(int32_t));
    ASSERT_EQ(ArrowArrayBuffer(array.get(), 2)->size_bytes, 0);
    ASSERT_EQ(NANOARROW_OK, ArrowArrayFinishBuilding(
                                array.get(), NANOARROW_VALIDATION_LEVEL_FULL, &error))
        << error.message;
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadBinary) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyBinary;
  data.size_bytes = sizeof(kTestPgCopyBinary);

  auto col_type = PostgresType(PostgresTypeId::kBytea);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyBinary, sizeof(kTestPgCopyBinary));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 5);
  ASSERT_EQ(array->n_children, 1);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto offsets = reinterpret_cast<const int32_t*>(array->children[0]->buffers[1]);
  auto data_buffer = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[2]);
  ASSERT_NE(validity, nullptr);
  ASSERT_NE(data_buffer, nullptr);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_TRUE(ArrowBitGet(validity, 2));
  ASSERT_TRUE(ArrowBitGet(validity, 3));
  ASSERT_FALSE(ArrowBitGet(validity, 4));

  ASSERT_EQ(offsets[0], 0);
  ASSERT_EQ(offsets[1], 0);
  ASSERT_EQ(offsets[2], 2);
  ASSERT_EQ(offsets[3], 6);
  ASSERT_EQ(offsets[4], 8);
  ASSERT_EQ(offsets[5], 8);

  ASSERT_EQ(data_buffer[0], 0x00);
  ASSERT_EQ(data_buffer[1], 0x01);
  ASSERT_EQ(data_buffer[2], 0x01);
  ASSERT_EQ(data_buffer[3], 0x02);
  ASSERT_EQ(data_buffer[4], 0x03);
  ASSERT_EQ(data_buffer[5], 0x04);
  ASSERT_EQ(data_buffer[6], 0xfe);
  ASSERT_EQ(data_buffer[7], 0xff);
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadArray) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyIntegerArray;
  data.size_bytes = sizeof(kTestPgCopyIntegerArray);

  auto col_type = PostgresType(PostgresTypeId::kInt4).Array();
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyIntegerArray,
            sizeof(kTestPgCopyIntegerArray));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);
  ASSERT_EQ(array->children[0]->n_children, 1);
  ASSERT_EQ(array->children[0]->children[0]->length, 5);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto offsets = reinterpret_cast<const int32_t*>(array->children[0]->buffers[1]);
  auto data_buffer =
      reinterpret_cast<const int32_t*>(array->children[0]->children[0]->buffers[1]);
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
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadArrayDimensionOverflow) {
  // An int4 array with three dimensions of INT32_MAX elements each.
  const uint8_t buffer[] = {
      0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a, 0x00, 0x00, 0x00,
      0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x01, 0x00, 0x00, 0x00, 0x24, 0x00,
      0x00, 0x00, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0x7f, 0xff,
      0xff, 0xff, 0x00, 0x00, 0x00, 0x01, 0x7f, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00,
      0x01, 0x7f, 0xff, 0xff, 0xff, 0x00, 0x00, 0x00, 0x01, 0xff, 0xff};
  ArrowBufferView data;
  data.data.as_uint8 = buffer;
  data.size_bytes = sizeof(buffer);

  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", PostgresType(PostgresTypeId::kInt4).Array());

  PostgresCopyStreamTester tester;
  ArrowError error = {};
  ASSERT_EQ(tester.Init(input_type, &error), NANOARROW_OK) << error.message;
  ASSERT_EQ(tester.ReadAll(&data, &error), EINVAL) << error.message;
  ASSERT_STREQ(error.message, "Array element count overflow");
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadArrayEmpty) {
  const uint8_t buffer[] = {0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a,
                            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                            0x01, 0x00, 0x00, 0x00, 0x0c, 0x00, 0x00, 0x00, 0x00, 0x00,
                            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0xff, 0xff};
  ArrowBufferView data;
  data.data.as_uint8 = buffer;
  data.size_bytes = sizeof(buffer);

  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", PostgresType(PostgresTypeId::kInt4).Array());

  PostgresCopyStreamTester tester;
  ArrowError error = {};
  ASSERT_EQ(tester.Init(input_type, &error), NANOARROW_OK) << error.message;
  ASSERT_EQ(tester.ReadAll(&data, &error), ENODATA) << error.message;
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get(), &error), NANOARROW_OK) << error.message;
  ASSERT_EQ(array->length, 1);
  const auto* list = array->children[0];
  ASSERT_EQ(list->null_count, 0);
  ASSERT_EQ(list->children[0]->length, 0);
  const auto* offsets = static_cast<const int32_t*>(list->buffers[1]);
  ASSERT_EQ(offsets[0], 0);
  ASSERT_EQ(offsets[1], 0);
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadArrayNegativeDimensionLength) {
  const uint8_t buffer[] = {0x50, 0x47, 0x43, 0x4f, 0x50, 0x59, 0x0a, 0xff, 0x0d, 0x0a,
                            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
                            0x01, 0x00, 0x00, 0x00, 0x14, 0x00, 0x00, 0x00, 0x01, 0x00,
                            0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x17, 0xff, 0xff, 0xff,
                            0xff, 0x00, 0x00, 0x00, 0x01, 0xff, 0xff};
  ArrowBufferView data;
  data.data.as_uint8 = buffer;
  data.size_bytes = sizeof(buffer);

  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", PostgresType(PostgresTypeId::kInt4).Array());

  PostgresCopyStreamTester tester;
  ArrowError error = {};
  ASSERT_EQ(tester.Init(input_type, &error), NANOARROW_OK) << error.message;
  ASSERT_EQ(tester.ReadAll(&data, &error), EINVAL) << error.message;
  ASSERT_STREQ(error.message, "Negative array dimension length: -1");
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadInt2vector) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyInt2vector;
  data.size_bytes = sizeof(kTestPgCopyInt2vector);

  auto col_type = PostgresType(PostgresTypeId::kInt2vector);
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("empty", col_type);
  input_type.AppendChild("len1", col_type);
  input_type.AppendChild("len2", col_type);
  input_type.AppendChild("len4", col_type);

  PostgresCopyStreamTester tester;
  ArrowError error;
  ASSERT_EQ(tester.Init(input_type, &error), NANOARROW_OK) << error.message;
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyInt2vector, sizeof(kTestPgCopyInt2vector));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 1);
  ASSERT_EQ(array->n_children, 4);

  for (int col = 0; col < 4; col++) {
    ASSERT_EQ(array->children[col]->n_children, 1);
  }

  {
    auto* child = array->children[0];
    ASSERT_EQ(child->children[0]->length, 0);
    auto offsets = reinterpret_cast<const int32_t*>(child->buffers[1]);
    auto data_buffer = reinterpret_cast<const int16_t*>(child->children[0]->buffers[1]);
    ASSERT_NE(data_buffer, nullptr);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 0);
  }
  {
    auto* child = array->children[1];
    ASSERT_EQ(child->children[0]->length, 1);
    auto offsets = reinterpret_cast<const int32_t*>(child->buffers[1]);
    auto data_buffer = reinterpret_cast<const int16_t*>(child->children[0]->buffers[1]);
    ASSERT_NE(data_buffer, nullptr);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 1);

    EXPECT_EQ(data_buffer[0], -32768);
  }
  {
    auto* child = array->children[2];
    ASSERT_EQ(child->children[0]->length, 2);
    auto offsets = reinterpret_cast<const int32_t*>(child->buffers[1]);
    auto data_buffer = reinterpret_cast<const int16_t*>(child->children[0]->buffers[1]);
    ASSERT_NE(data_buffer, nullptr);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 2);

    EXPECT_EQ(data_buffer[0], -32768);
    EXPECT_EQ(data_buffer[1], 32767);
  }
  {
    auto* child = array->children[3];
    ASSERT_EQ(child->children[0]->length, 4);
    auto offsets = reinterpret_cast<const int32_t*>(child->buffers[1]);
    auto data_buffer = reinterpret_cast<const int16_t*>(child->children[0]->buffers[1]);
    ASSERT_NE(data_buffer, nullptr);

    EXPECT_EQ(offsets[0], 0);
    EXPECT_EQ(offsets[1], 4);

    EXPECT_EQ(data_buffer[0], -1);
    EXPECT_EQ(data_buffer[1], 0);
    EXPECT_EQ(data_buffer[2], 1);
    EXPECT_EQ(data_buffer[3], 42);
  }
}

TEST(PostgresCopyUtilsTest, PostgresCopyReadCustomRecord) {
  ArrowBufferView data;
  data.data.as_uint8 = kTestPgCopyCustomRecord;
  data.size_bytes = sizeof(kTestPgCopyCustomRecord);

  auto col_type = PostgresType(PostgresTypeId::kRecord);
  col_type.AppendChild("nested1", PostgresType(PostgresTypeId::kInt4));
  col_type.AppendChild("nested2", PostgresType(PostgresTypeId::kFloat8));
  PostgresType input_type(PostgresTypeId::kRecord);
  input_type.AppendChild("col", col_type);

  PostgresCopyStreamTester tester;
  ASSERT_EQ(tester.Init(input_type), NANOARROW_OK);
  ASSERT_EQ(tester.ReadAll(&data), ENODATA);
  ASSERT_EQ(data.data.as_uint8 - kTestPgCopyCustomRecord,
            sizeof(kTestPgCopyCustomRecord));
  ASSERT_EQ(data.size_bytes, 0);

  nanoarrow::UniqueArray array;
  ASSERT_EQ(tester.GetArray(array.get()), NANOARROW_OK);
  ASSERT_EQ(array->length, 3);
  ASSERT_EQ(array->n_children, 1);
  ASSERT_EQ(array->children[0]->n_children, 2);
  ASSERT_EQ(array->children[0]->children[0]->length, 3);
  ASSERT_EQ(array->children[0]->children[1]->length, 3);

  auto validity = reinterpret_cast<const uint8_t*>(array->children[0]->buffers[0]);
  auto data_buffer1 =
      reinterpret_cast<const int32_t*>(array->children[0]->children[0]->buffers[1]);
  auto data_buffer2 =
      reinterpret_cast<const double*>(array->children[0]->children[1]->buffers[1]);

  ASSERT_TRUE(ArrowBitGet(validity, 0));
  ASSERT_TRUE(ArrowBitGet(validity, 1));
  ASSERT_FALSE(ArrowBitGet(validity, 2));

  ASSERT_EQ(data_buffer1[0], 123);
  ASSERT_EQ(data_buffer1[1], 12);
  ASSERT_EQ(data_buffer1[2], 0);

  ASSERT_DOUBLE_EQ(data_buffer2[0], 456.789);
  ASSERT_DOUBLE_EQ(data_buffer2[1], 345.678);
  ASSERT_DOUBLE_EQ(data_buffer2[2], 0);
}

TEST(PostgresCopyUtilsTest, BinaryOverflow) {
  constexpr int32_t field_size_bytes = 1024 * 1024 * 4;
  std::vector<uint8_t> buffer(field_size_bytes);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(NANOARROW_OK, ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_BINARY));

  nanoarrow::UniqueArray array;
  ASSERT_EQ(NANOARROW_OK, ArrowArrayInitFromType(array.get(), NANOARROW_TYPE_BINARY));
  ASSERT_EQ(NANOARROW_OK, ArrowArrayStartAppending(array.get()));

  PostgresCopyBinaryFieldReader reader;
  ASSERT_EQ(NANOARROW_OK, reader.InitSchema(schema.get()));
  ASSERT_EQ(NANOARROW_OK, reader.InitArray(array.get()));
  for (int i = 0; i < 511; i++) {
    ArrowBufferView data;
    data.size_bytes = static_cast<int64_t>(buffer.size());
    data.data.data = buffer.data();
    ASSERT_EQ(NANOARROW_OK, reader.Read(&data, field_size_bytes, array.get(), nullptr));
  }
  ArrowBufferView data;
  data.size_bytes = static_cast<int64_t>(buffer.size());
  data.data.data = buffer.data();
  ASSERT_EQ(EOVERFLOW, reader.Read(&data, field_size_bytes, array.get(), nullptr));
}

TEST(PostgresCopyUtilsTest, JsonbOverflow) {
  constexpr int32_t field_size_bytes = 1024 * 1024 * 4;
  std::vector<uint8_t> buffer(field_size_bytes, 1);

  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(NANOARROW_OK, ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_BINARY));

  nanoarrow::UniqueArray array;
  ASSERT_EQ(NANOARROW_OK, ArrowArrayInitFromType(array.get(), NANOARROW_TYPE_BINARY));
  ASSERT_EQ(NANOARROW_OK, ArrowArrayStartAppending(array.get()));

  PostgresCopyJsonbFieldReader reader;
  ASSERT_EQ(NANOARROW_OK, reader.InitSchema(schema.get()));
  ASSERT_EQ(NANOARROW_OK, reader.InitArray(array.get()));
  for (int i = 0; i < 512; i++) {
    ArrowBufferView data;
    data.size_bytes = static_cast<int64_t>(buffer.size());
    data.data.data = buffer.data();
    ASSERT_EQ(NANOARROW_OK, reader.Read(&data, field_size_bytes, array.get(), nullptr));
  }
  ArrowBufferView data;
  data.size_bytes = static_cast<int64_t>(buffer.size());
  data.data.data = buffer.data();
  ASSERT_EQ(EOVERFLOW, reader.Read(&data, field_size_bytes, array.get(), nullptr));
}

// N.B. unfortunately testing with NUMERIC is difficult since we can't add all
// that much to the buffer on each iteration, so the unit test takes too long

namespace {

template <typename T>
std::vector<uint8_t> RawBytes(T value) {
  std::vector<uint8_t> out(sizeof(T));
  std::memcpy(out.data(), &value, sizeof(T));
  return out;
}

void InitReaderForTest(PostgresCopyFieldReader* reader, ArrowSchema* schema,
                       nanoarrow::UniqueArray* array) {
  ASSERT_EQ(NANOARROW_OK, reader->InitSchema(schema));
  ASSERT_EQ(NANOARROW_OK, ArrowArrayInitFromSchema(array->get(), schema, nullptr));
  ASSERT_EQ(NANOARROW_OK, ArrowArrayStartAppending(array->get()));
  ASSERT_EQ(NANOARROW_OK, reader->InitArray(array->get()));
}

// Test a reader where the payload is shorter than expected
void CheckTruncatedPayload(PostgresCopyFieldReader* reader, ArrowArray* array,
                           int32_t width) {
  for (int32_t available = 0; available < width; available++) {
    // Use padded backing storage so that the unsafe reader stays inside the
    // allocation and the test fails deterministically instead of crashing.
    std::vector<uint8_t> backing(static_cast<size_t>(width), 0);
    ArrowBufferView data;
    data.data.as_uint8 = backing.data();
    data.size_bytes = available;

    const uint8_t* input_pointer = data.data.as_uint8;
    const int64_t input_size = data.size_bytes;
    const int64_t output_length = array->length;

    ArrowError error;
    std::memset(&error, 0, sizeof(ArrowError));
    ASSERT_EQ(EINVAL, reader->Read(&data, width, array, &error));

    const std::string expected = "Expected " + std::to_string(width) +
                                 " bytes of field data but got " +
                                 std::to_string(available) + " bytes of input";
    ASSERT_STREQ(error.message, expected.c_str());
    ASSERT_EQ(data.data.as_uint8, input_pointer);
    ASSERT_EQ(data.size_bytes, input_size);
    ASSERT_EQ(array->length, output_length);
  }
}

// Check a valid payload of all zeroes of the correct length
void CheckZeroPayload(PostgresCopyFieldReader* reader, ArrowArray* array, int32_t width,
                      const std::vector<uint8_t>& expected_data) {
  std::vector<uint8_t> backing(static_cast<size_t>(width), 0);
  ArrowBufferView data;
  data.data.as_uint8 = backing.data();
  data.size_bytes = width;

  const uint8_t* input_pointer = data.data.as_uint8;
  const int64_t output_index = array->length;

  ArrowError error;
  std::memset(&error, 0, sizeof(ArrowError));
  ASSERT_EQ(NANOARROW_OK, reader->Read(&data, width, array, &error));
  ASSERT_EQ(data.data.as_uint8, input_pointer + width);
  ASSERT_EQ(data.size_bytes, 0);
  ASSERT_EQ(array->length, output_index + 1);

  ArrowBitmap* validity = ArrowArrayValidityBitmap(array);
  if (validity->buffer.data != nullptr) {
    ASSERT_TRUE(ArrowBitGet(validity->buffer.data, output_index));
  }

  ArrowBuffer* data_buffer = ArrowArrayBuffer(array, 1);
  ASSERT_EQ(data_buffer->size_bytes, static_cast<int64_t>(expected_data.size()));
  ASSERT_EQ(std::memcmp(data_buffer->data, expected_data.data(), expected_data.size()),
            0);
}

// Check a valid payload that should resolve to null (because size is -1 or 0)
void CheckNullPayload(PostgresCopyFieldReader* reader, ArrowArray* array, int32_t width,
                      int32_t field_size_bytes) {
  std::vector<uint8_t> backing(static_cast<size_t>(width), 0);
  ArrowBufferView data;
  data.data.as_uint8 = backing.data();
  data.size_bytes = width;

  const uint8_t* input_pointer = data.data.as_uint8;
  const int64_t output_index = array->length;

  ArrowError error;
  std::memset(&error, 0, sizeof(ArrowError));
  ASSERT_EQ(NANOARROW_OK, reader->Read(&data, field_size_bytes, array, &error));
  ASSERT_EQ(data.data.as_uint8, input_pointer);
  ASSERT_EQ(data.size_bytes, width);
  ASSERT_EQ(array->length, output_index + 1);

  ArrowBitmap* validity = ArrowArrayValidityBitmap(array);
  ASSERT_NE(validity->buffer.data, nullptr);
  ASSERT_FALSE(ArrowBitGet(validity->buffer.data, output_index));
}

// Check an invalid payload (because the field length is wrong)
void CheckInvalidFieldSize(PostgresCopyFieldReader* reader, ArrowArray* array,
                           int32_t field_size_bytes,
                           const std::string& expected_message) {
  std::vector<uint8_t> backing(static_cast<size_t>(field_size_bytes) + 1, 0);
  ArrowBufferView data;
  data.data.as_uint8 = backing.data();
  data.size_bytes = static_cast<int64_t>(backing.size());

  const uint8_t* input_pointer = data.data.as_uint8;
  const int64_t input_size = data.size_bytes;
  const int64_t output_length = array->length;

  ArrowError error;
  std::memset(&error, 0, sizeof(ArrowError));
  ASSERT_EQ(EINVAL, reader->Read(&data, field_size_bytes, array, &error));
  ASSERT_STREQ(error.message, expected_message.c_str());
  ASSERT_EQ(data.data.as_uint8, input_pointer);
  ASSERT_EQ(data.size_bytes, input_size);
  ASSERT_EQ(array->length, output_length);
}

template <typename T, typename U>
void CheckTemporalPayload(PostgresTypeId type_id, T value, U network_value,
                          const char* expected_error, T expected_value = 0) {
  SCOPED_TRACE(value);
  PostgresType type(type_id);
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(type.SetSchema(schema.get(), "PostgreSQL Tester"), NANOARROW_OK);
  ArrowError error{};
  std::unique_ptr<PostgresCopyFieldReader> reader;
  ASSERT_EQ(MakeCopyFieldReader(type, schema.get(), &reader, &error), NANOARROW_OK)
      << error.message;
  nanoarrow::UniqueArray array;
  ASSERT_NO_FATAL_FAILURE(InitReaderForTest(reader.get(), schema.get(), &array));

  ArrowBufferView data;
  data.data.data = &network_value;
  data.size_bytes = sizeof(network_value);
  const int result = reader->Read(&data, sizeof(network_value), array.get(), &error);
  const ArrowBuffer* buffer = ArrowArrayBuffer(array.get(), 1);
  if (expected_error) {
    EXPECT_EQ(result, EINVAL);
    EXPECT_NE(std::string(error.message).find(expected_error), std::string::npos);
    EXPECT_EQ(array->length, 0);
    EXPECT_EQ(buffer->size_bytes, 0);
  } else {
    ASSERT_EQ(result, NANOARROW_OK) << error.message;
    EXPECT_EQ(array->length, 1);
    ASSERT_EQ(buffer->size_bytes, sizeof(T));
    EXPECT_EQ(std::memcmp(buffer->data, &expected_value, sizeof(T)), 0);
  }
}

}  // namespace

TEST(FieldReaderInvalidInputTest, Boolean) {
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(NANOARROW_OK, ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_BOOL));
  auto reader = std::make_unique<PostgresCopyBooleanFieldReader>();
  nanoarrow::UniqueArray array;
  ASSERT_NO_FATAL_FAILURE(InitReaderForTest(reader.get(), schema.get(), &array));

  CheckTruncatedPayload(reader.get(), array.get(), 1);
  CheckZeroPayload(reader.get(), array.get(), 1, RawBytes<int8_t>(0));
  CheckNullPayload(reader.get(), array.get(), 1, -1);
  CheckNullPayload(reader.get(), array.get(), 1, 0);
  CheckInvalidFieldSize(reader.get(), array.get(), 2,
                        "Expected field with one byte but found field with 2 bytes");
  CheckInvalidFieldSize(reader.get(), array.get(), 3,
                        "Expected field with one byte but found field with 3 bytes");
}

TEST(FieldReaderInvalidInputTest, Int16) {
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(NANOARROW_OK, ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT16));
  auto reader = std::make_unique<PostgresCopyNetworkEndianFieldReader<int16_t>>();
  nanoarrow::UniqueArray array;
  ASSERT_NO_FATAL_FAILURE(InitReaderForTest(reader.get(), schema.get(), &array));

  CheckTruncatedPayload(reader.get(), array.get(), 2);
  CheckZeroPayload(reader.get(), array.get(), 2, RawBytes<int16_t>(0));
  CheckNullPayload(reader.get(), array.get(), 2, -1);
  CheckNullPayload(reader.get(), array.get(), 2, 0);
  CheckInvalidFieldSize(reader.get(), array.get(), 1,
                        "Expected field with 2 bytes but found field with 1 bytes");
  CheckInvalidFieldSize(reader.get(), array.get(), 3,
                        "Expected field with 2 bytes but found field with 3 bytes");
}

TEST(FieldReaderInvalidInputTest, Int32) {
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(NANOARROW_OK, ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT32));
  auto reader = std::make_unique<PostgresCopyNetworkEndianFieldReader<int32_t>>();
  nanoarrow::UniqueArray array;
  ASSERT_NO_FATAL_FAILURE(InitReaderForTest(reader.get(), schema.get(), &array));

  CheckTruncatedPayload(reader.get(), array.get(), 4);
  CheckZeroPayload(reader.get(), array.get(), 4, RawBytes<int32_t>(0));
  CheckNullPayload(reader.get(), array.get(), 4, -1);
  CheckNullPayload(reader.get(), array.get(), 4, 0);
  CheckInvalidFieldSize(reader.get(), array.get(), 3,
                        "Expected field with 4 bytes but found field with 3 bytes");
  CheckInvalidFieldSize(reader.get(), array.get(), 5,
                        "Expected field with 4 bytes but found field with 5 bytes");
}

TEST(FieldReaderInvalidInputTest, Int64) {
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(NANOARROW_OK, ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_INT64));
  auto reader = std::make_unique<PostgresCopyNetworkEndianFieldReader<int64_t>>();
  nanoarrow::UniqueArray array;
  ASSERT_NO_FATAL_FAILURE(InitReaderForTest(reader.get(), schema.get(), &array));

  CheckTruncatedPayload(reader.get(), array.get(), 8);
  CheckZeroPayload(reader.get(), array.get(), 8, RawBytes<int64_t>(0));
  CheckNullPayload(reader.get(), array.get(), 8, -1);
  CheckNullPayload(reader.get(), array.get(), 8, 0);
  CheckInvalidFieldSize(reader.get(), array.get(), 7,
                        "Expected field with 8 bytes but found field with 7 bytes");
  CheckInvalidFieldSize(reader.get(), array.get(), 9,
                        "Expected field with 8 bytes but found field with 9 bytes");
}

TEST(FieldReaderInvalidInputTest, Float) {
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(NANOARROW_OK, ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_FLOAT));
  auto reader = std::make_unique<PostgresCopyNetworkEndianFieldReader<uint32_t>>();
  nanoarrow::UniqueArray array;
  ASSERT_NO_FATAL_FAILURE(InitReaderForTest(reader.get(), schema.get(), &array));

  CheckTruncatedPayload(reader.get(), array.get(), 4);
  CheckZeroPayload(reader.get(), array.get(), 4, RawBytes<uint32_t>(0));
  CheckNullPayload(reader.get(), array.get(), 4, -1);
  CheckNullPayload(reader.get(), array.get(), 4, 0);
  CheckInvalidFieldSize(reader.get(), array.get(), 3,
                        "Expected field with 4 bytes but found field with 3 bytes");
  CheckInvalidFieldSize(reader.get(), array.get(), 5,
                        "Expected field with 4 bytes but found field with 5 bytes");
}

TEST(FieldReaderInvalidInputTest, Double) {
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(NANOARROW_OK, ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_DOUBLE));
  auto reader = std::make_unique<PostgresCopyNetworkEndianFieldReader<uint64_t>>();
  nanoarrow::UniqueArray array;
  ASSERT_NO_FATAL_FAILURE(InitReaderForTest(reader.get(), schema.get(), &array));

  CheckTruncatedPayload(reader.get(), array.get(), 8);
  CheckZeroPayload(reader.get(), array.get(), 8, RawBytes<uint64_t>(0));
  CheckNullPayload(reader.get(), array.get(), 8, -1);
  CheckNullPayload(reader.get(), array.get(), 8, 0);
  CheckInvalidFieldSize(reader.get(), array.get(), 7,
                        "Expected field with 8 bytes but found field with 7 bytes");
  CheckInvalidFieldSize(reader.get(), array.get(), 9,
                        "Expected field with 8 bytes but found field with 9 bytes");
}

TEST(FieldReaderInvalidInputTest, Date) {
  constexpr int32_t kPostgresDateEpoch = 10957;
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(NANOARROW_OK, ArrowSchemaInitFromType(schema.get(), NANOARROW_TYPE_DATE32));
  auto reader = std::make_unique<
      PostgresCopyNetworkEndianFieldReader<int32_t, kPostgresDateEpoch>>();
  nanoarrow::UniqueArray array;
  ASSERT_NO_FATAL_FAILURE(InitReaderForTest(reader.get(), schema.get(), &array));

  CheckTruncatedPayload(reader.get(), array.get(), 4);
  CheckZeroPayload(reader.get(), array.get(), 4, RawBytes<int32_t>(kPostgresDateEpoch));
  CheckNullPayload(reader.get(), array.get(), 4, -1);
  CheckNullPayload(reader.get(), array.get(), 4, 0);
  CheckInvalidFieldSize(reader.get(), array.get(), 3,
                        "Expected field with 4 bytes but found field with 3 bytes");
  CheckInvalidFieldSize(reader.get(), array.get(), 5,
                        "Expected field with 4 bytes but found field with 5 bytes");
}

TEST(FieldReaderInvalidInputTest, Timestamp) {
  constexpr int64_t kPostgresTimestampEpoch = 946684800000000;
  nanoarrow::UniqueSchema schema;
  ArrowSchemaInit(schema.get());
  ASSERT_EQ(NANOARROW_OK,
            ArrowSchemaSetTypeDateTime(schema.get(), NANOARROW_TYPE_TIMESTAMP,
                                       NANOARROW_TIME_UNIT_MICRO, nullptr));
  auto reader = std::make_unique<
      PostgresCopyNetworkEndianFieldReader<int64_t, kPostgresTimestampEpoch>>();
  nanoarrow::UniqueArray array;
  ASSERT_NO_FATAL_FAILURE(InitReaderForTest(reader.get(), schema.get(), &array));

  CheckTruncatedPayload(reader.get(), array.get(), 8);
  CheckZeroPayload(reader.get(), array.get(), 8,
                   RawBytes<int64_t>(kPostgresTimestampEpoch));
  CheckNullPayload(reader.get(), array.get(), 8, -1);
  CheckNullPayload(reader.get(), array.get(), 8, 0);
  CheckInvalidFieldSize(reader.get(), array.get(), 7,
                        "Expected field with 8 bytes but found field with 7 bytes");
  CheckInvalidFieldSize(reader.get(), array.get(), 9,
                        "Expected field with 8 bytes but found field with 9 bytes");
}

TEST(FieldReaderInvalidInputTest, DateEpochRange) {
  constexpr auto kMin = (std::numeric_limits<int32_t>::min)();
  constexpr auto kMax = (std::numeric_limits<int32_t>::max)();
  for (int32_t value : {kMin, kMax}) {
    CheckTemporalPayload(PostgresTypeId::kDate, value, ToNetworkInt32(value), "infinity");
  }
  constexpr int32_t kEpoch = 10957;
  constexpr int32_t kFirstInRange = kMin + 1;
  constexpr int32_t kLastInRange = kMax - kEpoch;
  CheckTemporalPayload(PostgresTypeId::kDate, kFirstInRange,
                       ToNetworkInt32(kFirstInRange), nullptr, kFirstInRange + kEpoch);
  CheckTemporalPayload(PostgresTypeId::kDate, kLastInRange, ToNetworkInt32(kLastInRange),
                       nullptr, kMax);
  constexpr int32_t kFirstOverflow = kLastInRange + 1;
  CheckTemporalPayload(PostgresTypeId::kDate, kFirstOverflow,
                       ToNetworkInt32(kFirstOverflow), "out of range");
}

TEST(FieldReaderInvalidInputTest, TimestampEpochRange) {
  constexpr auto kMin = (std::numeric_limits<int64_t>::min)();
  constexpr auto kMax = (std::numeric_limits<int64_t>::max)();
  constexpr int64_t kEpoch = 946684800000000;
  constexpr int64_t kFirstInRange = kMin + 1;
  constexpr int64_t kLastInRange = kMax - kEpoch;
  constexpr int64_t kMaxPostgresTimestamp = 0x7fff5bb3b29fffff;
  for (auto type : {PostgresTypeId::kTimestamp, PostgresTypeId::kTimestamptz}) {
    SCOPED_TRACE(static_cast<int>(type));
    for (int64_t value : {kMin, kMax}) {
      CheckTemporalPayload(type, value, ToNetworkInt64(value), "infinity");
    }
    for (int64_t value : {kLastInRange + 1, kMaxPostgresTimestamp}) {
      CheckTemporalPayload(type, value, ToNetworkInt64(value), "out of range");
    }
    CheckTemporalPayload(type, kFirstInRange, ToNetworkInt64(kFirstInRange), nullptr,
                         kFirstInRange + kEpoch);
    CheckTemporalPayload(type, kLastInRange, ToNetworkInt64(kLastInRange), nullptr, kMax);
  }
}

TEST(FieldReaderInvalidInputTest, Interval) {
  nanoarrow::UniqueSchema schema;
  ASSERT_EQ(NANOARROW_OK, ArrowSchemaInitFromType(
                              schema.get(), NANOARROW_TYPE_INTERVAL_MONTH_DAY_NANO));
  auto reader = std::make_unique<PostgresCopyIntervalFieldReader>();
  nanoarrow::UniqueArray array;
  ASSERT_NO_FATAL_FAILURE(InitReaderForTest(reader.get(), schema.get(), &array));

  const std::vector<uint8_t> expected(16, 0);

  CheckTruncatedPayload(reader.get(), array.get(), 16);
  CheckZeroPayload(reader.get(), array.get(), 16, expected);
  CheckNullPayload(reader.get(), array.get(), 16, -1);
  CheckNullPayload(reader.get(), array.get(), 16, 0);
  CheckInvalidFieldSize(reader.get(), array.get(), 15,
                        "Expected field with 16 bytes but found field with 15 bytes");
  CheckInvalidFieldSize(reader.get(), array.get(), 17,
                        "Expected field with 16 bytes but found field with 17 bytes");
}

}  // namespace adbcpq
