/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Scalars.Variant;
using Apache.Arrow.Types;
using Moq;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Client
{
    public class ExtensionTypeTests
    {
        private static readonly Guid s_guid = new Guid("0f8fad5b-d9cb-469f-a165-70867728950e");

        [Fact]
        public void GuidExtensionArray()
        {
            GuidArray array = new GuidArray.Builder().Append(s_guid).AppendNull().Build();

            using AdbcDataReader reader = GetReader(new Field("id", GuidType.Default, true), array);

            Assert.Equal(typeof(Guid), reader.GetFieldType(0));
            Assert.IsType<GuidType>(reader.GetFieldArrowType(0));
            Assert.Equal(typeof(Guid), reader.GetSchemaTable()!.Rows[0][SchemaTableColumn.DataType]);

            Assert.True(reader.Read());
            Assert.Equal(s_guid, reader.GetGuid(0));
            Assert.Equal(s_guid, reader.GetValue(0));
            Assert.True(reader.Read());
            Assert.Equal(DBNull.Value, reader.GetValue(0));
            Assert.False(reader.Read());
        }

        /// <summary>
        /// Arrays imported through the C Data Interface describe extension types only through
        /// field metadata on the storage type.
        /// </summary>
        [Fact]
        public void GuidFromStorageWithExtensionMetadata()
        {
            GuidArray array = new GuidArray.Builder().Append(s_guid).AppendNull().Build();
            Field field = new Field("id", new FixedSizeBinaryType(16), true, ExtensionMetadata("arrow.uuid"));

            using AdbcDataReader reader = GetReader(field, array.Storage);

            Assert.Equal(typeof(Guid), reader.GetFieldType(0));
            Assert.IsType<GuidType>(reader.GetFieldArrowType(0));

            Assert.True(reader.Read());
            Assert.Equal(s_guid, reader.GetGuid(0));
            Assert.IsType<GuidArray>(reader.GetArrowArray(0));
            Assert.True(reader.Read());
            Assert.Equal(DBNull.Value, reader.GetValue(0));
        }

        [Fact]
        public void UnresolvableExtensionMetadataUsesStorage()
        {
            // arrow.uuid requires fixed_size_binary(16) storage, so this can't be resolved
            BinaryArray array = new BinaryArray.Builder().Append(new byte[] { 1, 2, 3 }).Build();
            Field field = new Field("id", BinaryType.Default, true, ExtensionMetadata("arrow.uuid"));

            using AdbcDataReader reader = GetReader(field, array);

            Assert.Equal(typeof(byte[]), reader.GetFieldType(0));
            Assert.True(reader.Read());
            Assert.Equal(new byte[] { 1, 2, 3 }, reader.GetValue(0));
        }

        [Fact]
        public void FixedSizeBinary()
        {
            GuidArray array = new GuidArray.Builder().Append(s_guid).Build();

            using AdbcDataReader reader = GetReader(new Field("bytes", new FixedSizeBinaryType(16), true), array.Storage);

            Assert.Equal(typeof(byte[]), reader.GetFieldType(0));
            Assert.True(reader.Read());
            Assert.Equal(GuidArray.GuidToBytes(s_guid), reader.GetValue(0));
        }

        [Fact]
        public void Bool8()
        {
            Bool8Array array = new Bool8Array.Builder().Append(true).Append(false).AppendNull().Build();

            using AdbcDataReader reader = GetReader(new Field("flag", new Int8Type(), true, ExtensionMetadata("arrow.bool8")), array.Storage);

            Assert.Equal(typeof(bool), reader.GetFieldType(0));
            Assert.True(reader.Read());
            Assert.True(reader.GetBoolean(0));
            Assert.True(reader.Read());
            Assert.False(reader.GetBoolean(0));
            Assert.True(reader.Read());
            Assert.Equal(DBNull.Value, reader.GetValue(0));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void TimestampWithOffset(bool fromStorage)
        {
            DateTimeOffset value = new DateTimeOffset(2026, 10, 18, 9, 30, 15, TimeSpan.FromHours(-7));
            TimestampWithOffsetArray array = new TimestampWithOffsetArray.Builder().Append(value).AppendNull().Build();

            Field field = fromStorage ?
                new Field("ts", array.Storage.Data.DataType, true, ExtensionMetadata("arrow.timestamp_with_offset")) :
                new Field("ts", array.ExtensionType, true);
            using AdbcDataReader reader = GetReader(field, fromStorage ? array.Storage : array);

            Assert.Equal(typeof(DateTimeOffset), reader.GetFieldType(0));
            Assert.True(reader.Read());
            DateTimeOffset result = (DateTimeOffset)reader.GetValue(0);
            Assert.Equal(value, result);
            Assert.Equal(value.Offset, result.Offset);
            Assert.True(reader.Read());
            Assert.Equal(DBNull.Value, reader.GetValue(0));
        }

        [Theory]
        [InlineData(false)]
        [InlineData(true)]
        public void VariantAsJsonString(bool fromStorage)
        {
            VariantArray array = BuildVariantArray();

            using AdbcDataReader reader = GetReader(VariantField(array, fromStorage), fromStorage ? array.Storage : array);

            Assert.Equal(typeof(string), reader.GetFieldType(0));
            Assert.IsType<StringType>(reader.GetFieldArrowType(0));

            Assert.True(reader.Read());
            Assert.Equal("{\"name\":\"widget\",\"sizes\":[1,2]}", reader.GetString(0));
            Assert.True(reader.Read());
            Assert.Equal("42", reader.GetString(0));
            Assert.True(reader.Read());
            Assert.Equal("null", reader.GetString(0));
            Assert.True(reader.Read());
            Assert.Equal(DBNull.Value, reader.GetValue(0));
        }

        [Fact]
        public void VariantAsObject()
        {
            VariantArray array = BuildVariantArray();

            using AdbcDataReader reader = GetReader(VariantField(array, fromStorage: true), array.Storage, StructBehavior.Strict);

            Assert.Equal(typeof(object), reader.GetFieldType(0));
            Assert.IsType<VariantType>(reader.GetFieldArrowType(0));

            Assert.True(reader.Read());
            Dictionary<string, object?> obj = Assert.IsType<Dictionary<string, object?>>(reader.GetValue(0));
            Assert.Equal("widget", obj["name"]);
            Assert.Equal(new List<object?> { (sbyte)1, (sbyte)2 }, obj["sizes"]);
            Assert.True(reader.Read());
            Assert.Equal((sbyte)42, reader.GetValue(0));
            Assert.Equal(42, reader.GetInt32(0));
            Assert.True(reader.Read());
            Assert.Equal(DBNull.Value, reader.GetValue(0));
            Assert.True(reader.Read());
            Assert.Equal(DBNull.Value, reader.GetValue(0));
        }

        [Fact]
        public void UnknownExtensionTypeUsesStorage()
        {
            StringArray storage = new StringArray.Builder().Append("{\"a\":1}").Build();
            TestJsonType type = new TestJsonType();

            using AdbcDataReader reader = GetReader(new Field("doc", type, true), new ExtensionArray(type, storage));

            Assert.Equal(typeof(string), reader.GetFieldType(0));
            Assert.True(reader.Read());
            Assert.Equal("{\"a\":1}", reader.GetValue(0));
        }

        [Fact]
        public void UserRegisteredExtensionDefinition()
        {
            StringArray storage = new StringArray.Builder().Append("{\"a\":1}").Build();
            Field field = new Field("doc", StringType.Default, true, ExtensionMetadata(TestJsonType.ExtensionName));

            using (ExtensionTypeRegistry.Default.RegisterTemporary(new TestJsonExtensionDefinition()))
            {
                using AdbcDataReader reader = GetReader(field, storage);

                Assert.IsType<TestJsonType>(reader.GetFieldArrowType(0));
                Assert.True(reader.Read());
                Assert.IsType<ExtensionArray>(reader.GetArrowArray(0));
                Assert.Equal("{\"a\":1}", reader.GetValue(0));
            }
        }

        [Fact]
        public void BindGuidAndDateTimeOffsetParameters()
        {
            DateTimeOffset value = new DateTimeOffset(2026, 10, 18, 9, 30, 15, TimeSpan.FromHours(-7));
            RecordBatch? bound = null;
            Mock<AdbcStatement> mockStatement = new Mock<AdbcStatement>();
            mockStatement.Setup(x => x.Bind(It.IsAny<RecordBatch>(), It.IsAny<Schema>())).Callback<RecordBatch, Schema>((b, s) => bound = b);
            mockStatement.Setup(x => x.ExecuteUpdate()).Returns(new UpdateResult(1));

            using AdbcCommand cmd = new AdbcCommand(mockStatement.Object, new Adbc.Client.AdbcConnection());
            cmd.Parameters.Add(new AdbcParameter { ParameterName = "id", DbType = DbType.Guid, Value = s_guid });
            cmd.Parameters.Add(new AdbcParameter { ParameterName = "idString", DbType = DbType.Guid, Value = s_guid.ToString() });
            cmd.Parameters.Add(new AdbcParameter { ParameterName = "ts", DbType = DbType.DateTimeOffset, Value = value });
            cmd.ExecuteNonQuery();

            Assert.NotNull(bound);
            Assert.IsType<GuidType>(bound!.Schema.GetFieldByIndex(0).DataType);
            Assert.Equal(s_guid, ((GuidArray)bound.Column(0)).GetGuid(0));
            Assert.Equal(s_guid, ((GuidArray)bound.Column(1)).GetGuid(0));

            TimestampType timestampType = Assert.IsType<TimestampType>(bound.Schema.GetFieldByIndex(2).DataType);
            Assert.Equal("UTC", timestampType.Timezone);
            Assert.Equal(value, ((TimestampArray)bound.Column(2)).GetTimestamp(0));
        }

        [Fact]
        public void PrepareMapsExtensionTypes()
        {
            Schema parameterSchema = new Schema(new[]
            {
                new Field("0", GuidType.Default, true),
                new Field("1", new FixedSizeBinaryType(16), true, ExtensionMetadata("arrow.uuid")),
                new Field("2", TimestampWithOffsetType.Default, true),
                new Field("3", Bool8Type.Default, true),
            }, null);
            Mock<AdbcStatement> mockStatement = new Mock<AdbcStatement>();
            mockStatement.Setup(x => x.GetParameterSchema()).Returns(parameterSchema);

            using AdbcCommand cmd = new AdbcCommand(mockStatement.Object, new Adbc.Client.AdbcConnection());
            cmd.Prepare();

            Assert.Equal(DbType.Guid, cmd.Parameters[0].DbType);
            Assert.Equal(DbType.Guid, cmd.Parameters[1].DbType);
            Assert.Equal(DbType.DateTimeOffset, cmd.Parameters[2].DbType);
            Assert.Equal(DbType.Boolean, cmd.Parameters[3].DbType);
        }

        private static VariantArray BuildVariantArray()
        {
            VariantValue obj = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "name", VariantValue.FromString("widget") },
                { "sizes", VariantValue.FromArray(VariantValue.FromInt8(1), VariantValue.FromInt8(2)) },
            });

            return new VariantArray.Builder()
                .Append(obj)
                .Append(VariantValue.FromInt8(42))
                .Append(VariantValue.Null)
                .AppendNull()
                .Build();
        }

        private static Field VariantField(VariantArray array, bool fromStorage) => fromStorage ?
            new Field("v", array.Storage.Data.DataType, true, ExtensionMetadata("arrow.parquet.variant")) :
            new Field("v", array.ExtensionType, true);

        private static Dictionary<string, string> ExtensionMetadata(string name) => new Dictionary<string, string>
        {
            { "ARROW:extension:name", name },
            { "ARROW:extension:metadata", "" },
        };

        private static AdbcDataReader GetReader(Field field, IArrowArray array, StructBehavior structBehavior = StructBehavior.JsonString)
        {
            Schema schema = new Schema(new[] { field }, null);
            RecordBatch batch = new RecordBatch(schema, new[] { array }, array.Length);
            QueryResult queryResult = new QueryResult(array.Length, new MockArrayStream(schema, new List<RecordBatch> { batch }));

            Mock<AdbcStatement> mockStatement = new Mock<AdbcStatement>();
            mockStatement.Setup(x => x.ExecuteQuery()).Returns(queryResult);

            Adbc.Client.AdbcConnection connection = new Adbc.Client.AdbcConnection { StructBehavior = structBehavior };
            AdbcCommand cmd = new AdbcCommand(mockStatement.Object, connection);
            return cmd.ExecuteReader();
        }

        private sealed class TestJsonType : ExtensionType
        {
            public const string ExtensionName = "test.json";

            public TestJsonType() : base(StringType.Default) { }

            public override string Name => ExtensionName;
            public override string ExtensionMetadata => "";

            public override ExtensionArray CreateArray(IArrowArray storageArray) => new ExtensionArray(this, storageArray);
        }

        private sealed class TestJsonExtensionDefinition : ExtensionDefinition
        {
            public override string ExtensionName => TestJsonType.ExtensionName;

            public override bool TryCreateType(IArrowType storageType, string metadata, out ExtensionType type)
            {
                type = new TestJsonType();
                return storageType is StringType;
            }
        }
    }
}
