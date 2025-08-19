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

#include <string>

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

  ASSERT_FLOAT_EQ(data_buffer[0], -123.456);
  ASSERT_EQ(data_buffer[1], -1);
  ASSERT_EQ(data_buffer[2], 1);
  ASSERT_FLOAT_EQ(data_buffer[3], 123.456);
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

}  // namespace adbcpq
