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

#include "nanoarrow_pg.h"

using namespace adbcpq;


TEST(PostgresNanoarrowTest, PostgresTypeBasic) {
  PostgresType type(PostgresType::PG_RECV_BOOL);
  EXPECT_EQ(type.field_name(), "");
  EXPECT_EQ(type.typname(), "");
  EXPECT_EQ(type.recv(), PostgresType::PG_RECV_BOOL);
  EXPECT_EQ(type.oid(), 0);
  EXPECT_EQ(type.n_children(), 0);

  PostgresType with_info = type.WithPgTypeInfo(1234, "some_typename");
  EXPECT_EQ(with_info.oid(), 1234);
  EXPECT_EQ(with_info.typname(), "some_typename");
  EXPECT_EQ(with_info.recv(), type.recv());

  PostgresType with_name = type.WithFieldName("some name");
  EXPECT_EQ(with_name.field_name(), "some name");
  EXPECT_EQ(with_name.oid(), type.oid());
  EXPECT_EQ(with_name.recv(), type.recv());

  PostgresType array = type.Array(12345, "array type name");
  EXPECT_EQ(array.oid(), 12345);
  EXPECT_EQ(array.typname(), "array type name");
  EXPECT_EQ(array.n_children(), 1);
  EXPECT_EQ(array.child(0)->oid(), type.oid());
  EXPECT_EQ(array.child(0)->recv(), type.recv());

  PostgresType range = type.Range(12345, "range type name");
  EXPECT_EQ(range.oid(), 12345);
  EXPECT_EQ(range.typname(), "range type name");
  EXPECT_EQ(range.n_children(), 1);
  EXPECT_EQ(range.child(0)->oid(), type.oid());
  EXPECT_EQ(range.child(0)->recv(), type.recv());

  PostgresType domain = type.Domain(123456, "domain type name");
  EXPECT_EQ(domain.oid(), 123456);
  EXPECT_EQ(domain.typname(), "domain type name");
  EXPECT_EQ(domain.recv(), type.recv());

  PostgresType record(PostgresType::PG_RECV_RECORD);
  record.AddRecordChild("col1", type);
  EXPECT_EQ(record.recv(), PostgresType::PG_RECV_RECORD);
  EXPECT_EQ(record.n_children(), 1);
  EXPECT_EQ(record.child(0)->recv(), type.recv());
  EXPECT_EQ(record.child(0)->field_name(), "col1");
}

TEST(PostgresNanoarrowTest, PostgresTypeSetSchema) {
  ArrowSchema schema;

  ArrowSchemaInit(&schema);
  EXPECT_EQ(PostgresType(PostgresType::PG_RECV_BOOL).SetSchema(&schema), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "b");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(PostgresType(PostgresType::PG_RECV_INT2).SetSchema(&schema), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "s");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(PostgresType(PostgresType::PG_RECV_INT4).SetSchema(&schema), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "i");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(PostgresType(PostgresType::PG_RECV_INT8).SetSchema(&schema), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "l");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(PostgresType(PostgresType::PG_RECV_FLOAT4).SetSchema(&schema), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "f");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(PostgresType(PostgresType::PG_RECV_FLOAT8).SetSchema(&schema), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "g");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(PostgresType(PostgresType::PG_RECV_TEXT).SetSchema(&schema), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "u");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(PostgresType(PostgresType::PG_RECV_BYTEA).SetSchema(&schema), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "z");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  EXPECT_EQ(PostgresType(PostgresType::PG_RECV_BOOL).Array().SetSchema(&schema), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+l");
  EXPECT_STREQ(schema.children[0]->format, "b");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  PostgresType record(PostgresType::PG_RECV_RECORD);
  record.AddRecordChild("col1", PostgresType(PostgresType::PG_RECV_BOOL));
  EXPECT_EQ(record.SetSchema(&schema), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+s");
  EXPECT_STREQ(schema.children[0]->format, "b");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  PostgresType unknown(PostgresType::PG_RECV_BRIN_MINMAX_MULTI_SUMMARY);
  EXPECT_EQ(unknown.WithPgTypeInfo(0, "some_name").SetSchema(&schema), NANOARROW_OK);
  EXPECT_STREQ(schema.format, "z");

  ArrowStringView value = ArrowCharView("<not found>");
  ArrowMetadataGetValue(schema.metadata, ArrowCharView("ADBC:posgresql:typname"), &value);
  EXPECT_EQ(std::string(value.data, value.size_bytes), "some_name");
  schema.release(&schema);
}

TEST(PostgresNanoarrowTest, PostgresTypeAllBase) {
  auto base_types = PostgresType::AllBase();
  EXPECT_EQ(base_types["array_recv"].recv(), PostgresType::PG_RECV_ARRAY);
  EXPECT_EQ(base_types["array_recv"].typname(), "array");
  EXPECT_EQ(base_types.size(), PostgresType::PgRecvAllBase().size());
}
