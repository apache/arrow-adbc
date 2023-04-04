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

#include <utility>

#include <gtest/gtest.h>
#include <nanoarrow/nanoarrow.h>

#include "postgres_type.h"

using adbcpq::PostgresType;
using adbcpq::PostgresTypeResolver;

class MockTypeResolver : public PostgresTypeResolver {
 public:
  ArrowErrorCode Init() {
    auto recv_base = PostgresType::PgRecvAllBase(false);
    PostgresTypeResolver::Item item;
    item.oid = 0;

    // Insert all the base types
    for (auto recv : recv_base) {
      std::string typreceive = PostgresType::PgRecvName(recv);
      std::string typname = PostgresType::PgRecvTypname(recv);
      item.oid++;
      item.typname = typname.c_str();
      item.typreceive = typreceive.c_str();
      NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));
      oids_[recv] = item.oid;
    }

    // Insert one of each nested type
    item.oid++;
    item.typname = "_bool";
    item.typreceive = "array_recv";
    item.child_oid = oid(PostgresType::PG_RECV_BOOL);
    NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));
    oids_[PostgresType::PG_RECV_ARRAY] = item.oid;

    item.oid++;
    item.typname = "boolrange";
    item.typreceive = "range_recv";
    item.base_oid = oid(PostgresType::PG_RECV_BOOL);
    NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));
    oids_[PostgresType::PG_RECV_RANGE] = item.oid;

    item.oid++;
    item.typname = "custombool";
    item.typreceive = "domain_recv";
    item.base_oid = oid(PostgresType::PG_RECV_BOOL);
    NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));
    oids_[PostgresType::PG_RECV_DOMAIN] = item.oid;

    item.oid++;
    uint32_t class_oid = item.oid;
    std::vector<std::pair<uint32_t, std::string>> record_fields_ = {
        {oid(PostgresType::PG_RECV_INT4), "int4_col"},
        {oid(PostgresType::PG_RECV_TEXT), "text_col"}};
    classes_.insert({class_oid, record_fields_});

    item.oid++;
    item.typname = "customrecord";
    item.typreceive = "record_recv";
    item.class_oid = class_oid;

    NANOARROW_RETURN_NOT_OK(Insert(item, nullptr));
    oids_[PostgresType::PG_RECV_RECORD] = item.oid;

    return NANOARROW_OK;
  }

  uint32_t oid(PostgresType::PgRecv recv) { return oids_[recv]; }

  ArrowErrorCode ResolveClass(uint32_t oid,
                              std::vector<std::pair<uint32_t, std::string>>* out,
                              ArrowError* error) override {
    auto result = classes_.find(oid);
    if (result == classes_.end()) {
      return PostgresTypeResolver::ResolveClass(oid, out, error);
    }

    *out = (*result).second;
    return NANOARROW_OK;
  }

 private:
  std::unordered_map<PostgresType::PgRecv, uint32_t> oids_;
  std::unordered_map<uint32_t, std::vector<std::pair<uint32_t, std::string>>> classes_;
};

TEST(PostgresTypeTest, PostgresTypeBasic) {
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
  record.AppendChild("col1", type);
  EXPECT_EQ(record.recv(), PostgresType::PG_RECV_RECORD);
  EXPECT_EQ(record.n_children(), 1);
  EXPECT_EQ(record.child(0)->recv(), type.recv());
  EXPECT_EQ(record.child(0)->field_name(), "col1");
}

TEST(PostgresTypeTest, PostgresTypeSetSchema) {
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
  EXPECT_EQ(PostgresType(PostgresType::PG_RECV_BOOL).Array().SetSchema(&schema),
            NANOARROW_OK);
  EXPECT_STREQ(schema.format, "+l");
  EXPECT_STREQ(schema.children[0]->format, "b");
  schema.release(&schema);

  ArrowSchemaInit(&schema);
  PostgresType record(PostgresType::PG_RECV_RECORD);
  record.AppendChild("col1", PostgresType(PostgresType::PG_RECV_BOOL));
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

TEST(PostgresTypeTest, PostgresTypeAllBase) {
  auto base_types = PostgresType::AllBase();
  EXPECT_EQ(base_types["array_recv"].recv(), PostgresType::PG_RECV_ARRAY);
  EXPECT_EQ(base_types["array_recv"].typname(), "array");
  EXPECT_EQ(base_types.size(), PostgresType::PgRecvAllBase().size());
}

TEST(PostgresTypeTest, PostgresTypeResolver) {
  PostgresTypeResolver resolver;
  ArrowError error;
  PostgresType type;
  PostgresTypeResolver::Item item;

  // Check error for type not found
  EXPECT_EQ(resolver.Find(123, &type, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Postgres type with oid 123 not found");

  // Check error for unsupported recv name
  item.oid = 123;
  item.typname = "invalid";
  item.typreceive = "invalid_recv";
  EXPECT_EQ(resolver.Insert(item, &error), ENOTSUP);
  EXPECT_STREQ(
      ArrowErrorMessage(&error),
      "Base type not found for type 'invalid' with receive function 'invalid_recv'");

  // Check error for Array with unknown child
  item.typname = "some_array";
  item.typreceive = "array_recv";
  item.child_oid = 1234;
  EXPECT_EQ(resolver.Insert(item, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Postgres type with oid 1234 not found");

  // Check error for Range with unknown child
  item.typname = "some_range";
  item.typreceive = "range_recv";
  item.base_oid = 12345;
  EXPECT_EQ(resolver.Insert(item, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Postgres type with oid 12345 not found");

  // Check error for Domain with unknown child
  item.typname = "some_domain";
  item.typreceive = "domain_recv";
  item.base_oid = 123456;
  EXPECT_EQ(resolver.Insert(item, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Postgres type with oid 123456 not found");

  // Check error for Record with unknown class
  item.typname = "some_record";
  item.typreceive = "record_recv";
  item.class_oid = 123456;
  EXPECT_EQ(resolver.Insert(item, &error), EINVAL);
  EXPECT_STREQ(ArrowErrorMessage(&error), "Class definition with oid 123456 not found");

  // Check insert/resolve of regular type
  item.typname = "some_type_name";
  item.typreceive = "boolrecv";
  item.oid = 10;
  EXPECT_EQ(resolver.Insert(item, &error), NANOARROW_OK);
  EXPECT_EQ(resolver.Find(10, &type, &error), NANOARROW_OK);
  EXPECT_EQ(type.oid(), 10);
  EXPECT_EQ(type.typname(), "some_type_name");
  EXPECT_EQ(type.recv(), PostgresType::PG_RECV_BOOL);

  // Check insert/resolve of array type
  item.oid = 11;
  item.typname = "some_array_type_name";
  item.typreceive = "array_recv";
  item.child_oid = 10;
  EXPECT_EQ(resolver.Insert(item, &error), NANOARROW_OK);
  EXPECT_EQ(resolver.Find(11, &type, &error), NANOARROW_OK);
  EXPECT_EQ(type.oid(), 11);
  EXPECT_EQ(type.typname(), "some_array_type_name");
  EXPECT_EQ(type.recv(), PostgresType::PG_RECV_ARRAY);
  EXPECT_EQ(type.child(0)->oid(), 10);
  EXPECT_EQ(type.child(0)->recv(), PostgresType::PG_RECV_BOOL);

  // Check insert/resolve of range type
  item.oid = 12;
  item.typname = "some_range_type_name";
  item.typreceive = "range_recv";
  item.base_oid = 10;
  EXPECT_EQ(resolver.Insert(item, &error), NANOARROW_OK);
  EXPECT_EQ(resolver.Find(12, &type, &error), NANOARROW_OK);
  EXPECT_EQ(type.oid(), 12);
  EXPECT_EQ(type.typname(), "some_range_type_name");
  EXPECT_EQ(type.recv(), PostgresType::PG_RECV_RANGE);
  EXPECT_EQ(type.child(0)->oid(), 10);
  EXPECT_EQ(type.child(0)->recv(), PostgresType::PG_RECV_BOOL);

  // Check insert/resolve of domain type
  item.oid = 13;
  item.typname = "some_domain_type_name";
  item.typreceive = "domain_recv";
  item.base_oid = 10;
  EXPECT_EQ(resolver.Insert(item, &error), NANOARROW_OK);
  EXPECT_EQ(resolver.Find(13, &type, &error), NANOARROW_OK);
  EXPECT_EQ(type.oid(), 13);
  EXPECT_EQ(type.typname(), "some_domain_type_name");
  EXPECT_EQ(type.recv(), PostgresType::PG_RECV_BOOL);
}

TEST(PostgresTypeTest, PostgresTypeResolveRecord) {
  // Use the mock resolver for the record test since it already has one
  MockTypeResolver resolver;
  ASSERT_EQ(resolver.Init(), NANOARROW_OK);

  PostgresType type;
  EXPECT_EQ(resolver.Find(resolver.oid(PostgresType::PG_RECV_RECORD), &type, nullptr),
            NANOARROW_OK);
  EXPECT_EQ(type.oid(), resolver.oid(PostgresType::PG_RECV_RECORD));
  EXPECT_EQ(type.n_children(), 2);
  EXPECT_EQ(type.child(0)->field_name(), "int4_col");
  EXPECT_EQ(type.child(0)->recv(), PostgresType::PG_RECV_INT4);
  EXPECT_EQ(type.child(1)->field_name(), "text_col");
  EXPECT_EQ(type.child(1)->recv(), PostgresType::PG_RECV_TEXT);
}
