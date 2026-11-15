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
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Operations.Shredding;
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
            GuidArray array = new GuidArray.Builder().Append(s_guid).AppendNull().Build();

            using AdbcDataReader reader = GetReader(new Field("bytes", new FixedSizeBinaryType(16), true), array.Storage);

            Assert.Equal(typeof(byte[]), reader.GetFieldType(0));
            Assert.True(reader.Read());
            Assert.Equal(GuidArray.GuidToBytes(s_guid), reader.GetValue(0));
            Assert.True(reader.Read());
            Assert.Equal(DBNull.Value, reader.GetValue(0));
        }

        [Theory]
        [InlineData(StructBehavior.Strict)]
        [InlineData(StructBehavior.JsonString)]
        public void NestedGuidFromStorageWithExtensionMetadata(StructBehavior structBehavior)
        {
            GuidArray guids = new GuidArray.Builder().Append(s_guid).Build();
            Field child = new Field("id", new FixedSizeBinaryType(16), true, ExtensionMetadata("arrow.uuid"));
            StructType structType = new StructType(new[] { child });
            StructArray structArray = new StructArray(structType, 1, new[] { guids.Storage }, ArrowBuffer.Empty);

            ListType listType = new ListType(child);
            ListArray listArray = new ListArray(
                listType, 1, new ArrowBuffer.Builder<int>().Append(0).Append(1).Build(), guids.Storage, ArrowBuffer.Empty);

            LargeListType largeListType = new LargeListType(child);
            LargeListArray largeListArray = new LargeListArray(
                largeListType, 1, new ArrowBuffer.Builder<long>().Append(0).Append(1).Build(), guids.Storage, ArrowBuffer.Empty);

            FixedSizeListType fixedSizeListType = new FixedSizeListType(child, 1);
            FixedSizeListArray fixedSizeListArray = new FixedSizeListArray(fixedSizeListType, 1, guids.Storage, ArrowBuffer.Empty);

            using AdbcDataReader reader = GetReader(
                new[]
                {
                    new Field("s", structType, true),
                    new Field("l", listType, true),
                    new Field("ll", largeListType, true),
                    new Field("fl", fixedSizeListType, true),
                },
                new IArrowArray[] { structArray, listArray, largeListArray, fixedSizeListArray },
                structBehavior);

            for (int i = 1; i <= 3; i++)
            {
                Assert.Equal(typeof(GuidArray), reader.GetFieldType(i));
                Assert.Equal(typeof(GuidArray), reader.GetColumnSchema()[i].DataType);
                Assert.Equal(typeof(GuidArray), reader.GetSchemaTable()!.Rows[i][SchemaTableColumn.DataType]);
            }

            Assert.True(reader.Read());
            if (structBehavior == StructBehavior.Strict)
            {
                Dictionary<string, object?> obj = Assert.IsType<Dictionary<string, object?>>(reader.GetValue(0));
                Assert.Equal(s_guid, obj["id"]);
            }
            else
            {
                Assert.Equal($"{{\"id\":\"{s_guid}\"}}", reader.GetString(0));
            }

            for (int i = 1; i <= 3; i++)
            {
                GuidArray values = Assert.IsType<GuidArray>(reader.GetValue(i));
                Assert.Equal(s_guid, Assert.Single(values));
            }
        }

        [Theory]
        [InlineData(StructBehavior.Strict)]
        [InlineData(StructBehavior.JsonString)]
        public void NestedVariant(StructBehavior structBehavior)
        {
            VariantArray variants = BuildVariantArray();
            Field child = VariantField(variants, fromStorage: true);
            StructType structType = new StructType(new[] { child });
            StructArray structArray = new StructArray(structType, variants.Length, new[] { variants.Storage }, ArrowBuffer.Empty);

            using AdbcDataReader reader = GetReader(new Field("s", structType, true), structArray, structBehavior);

            Assert.True(reader.Read());
            if (structBehavior == StructBehavior.Strict)
            {
                Dictionary<string, object?> obj = Assert.IsType<Dictionary<string, object?>>(reader.GetValue(0));
                Dictionary<string, object?> v = Assert.IsType<Dictionary<string, object?>>(obj["v"]);
                Assert.Equal("widget", v["name"]);
                Assert.Equal(new List<object?> { (sbyte)1, (sbyte)2 }, v["sizes"]);
            }
            else
            {
                Assert.Equal("{\"v\":{\"name\":\"widget\",\"sizes\":[1,2]}}", reader.GetString(0));
            }
        }

        [Fact]
        public void ClosedReaderDoesNotReturnCachedColumns()
        {
            GuidArray array = new GuidArray.Builder().Append(s_guid).Build();
            Field field = new Field("id", new FixedSizeBinaryType(16), true, ExtensionMetadata("arrow.uuid"));

            AdbcDataReader reader = GetReader(field, array.Storage);
            Assert.True(reader.Read());
            Assert.Equal(s_guid, reader.GetValue(0));

            reader.Close();
            Assert.Throws<InvalidOperationException>(() => reader.GetValue(0));
            Assert.Throws<InvalidOperationException>(() => reader.GetArrowArray(0));
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

        /// <summary>
        /// JSON numbers can't represent NaN or infinity, so they're written as strings,
        /// the same as in struct JSON. The .NET values keep the actual numbers.
        /// </summary>
        [Theory]
        [InlineData(StructBehavior.JsonString)]
        [InlineData(StructBehavior.Strict)]
        public void VariantNonFiniteValues(StructBehavior structBehavior)
        {
            VariantValue obj = VariantValue.FromObject(new Dictionary<string, VariantValue>
            {
                { "d", VariantValue.FromDouble(double.NaN) },
                { "f", VariantValue.FromFloat(float.PositiveInfinity) },
                { "values", VariantValue.FromArray(VariantValue.FromDouble(double.NegativeInfinity)) },
            });
            VariantArray array = new VariantArray.Builder()
                .Append(obj)
                .Append(VariantValue.FromDouble(double.NaN))
                .Build();

            using AdbcDataReader reader = GetReader(VariantField(array, fromStorage: true), array.Storage, structBehavior);

            Assert.True(reader.Read());
            if (structBehavior == StructBehavior.JsonString)
            {
                Assert.Equal("{\"d\":\"NaN\",\"f\":\"Infinity\",\"values\":[\"-Infinity\"]}", reader.GetString(0));
                Assert.True(reader.Read());
                Assert.Equal("\"NaN\"", reader.GetString(0));
            }
            else
            {
                Dictionary<string, object?> value = Assert.IsType<Dictionary<string, object?>>(reader.GetValue(0));
                Assert.True(double.IsNaN(Assert.IsType<double>(value["d"])));
                Assert.Equal(float.PositiveInfinity, value["f"]);
                Assert.Equal(new List<object?> { double.NegativeInfinity }, value["values"]);
                Assert.True(reader.Read());
                Assert.True(double.IsNaN(Assert.IsType<double>(reader.GetValue(0))));
            }
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

        /// <summary>
        /// Shredded variants can only be read once the application opts in through
        /// <see cref="ShreddedVariants.Reader"/>. This is a single test because the setting is global.
        /// </summary>
        [Fact]
        public void ShreddedVariant()
        {
            // Each reader disposes its batch, so each one gets a new array.
            static AdbcDataReader GetShreddedReader(StructBehavior structBehavior)
            {
                ShredSchema shredSchema = ShredSchema.Primitive(ShredType.Int8);
                var (metadata, rows) = VariantShredder.Shred(new[] { VariantValue.FromInt8(42), VariantValue.FromString("residual") }, shredSchema);
                VariantArray array = ShreddedVariantArrayBuilder.Build(shredSchema, metadata, rows);
                Assert.True(array.IsShredded);
                return GetReader(VariantField(array, fromStorage: true), array.Storage, structBehavior);
            }

            Assert.Null(ShreddedVariants.Reader);
            using (AdbcDataReader reader = GetShreddedReader(StructBehavior.Strict))
            {
                Assert.True(reader.Read());
                NotSupportedException e = Assert.Throws<NotSupportedException>(() => reader.GetValue(0));
                Assert.Contains("ShreddedVariants.Reader", e.Message);
            }

            ShreddedVariants.Reader = (variants, index) => variants.GetLogicalVariantValue(index);
            try
            {
                using AdbcDataReader reader = GetShreddedReader(StructBehavior.Strict);
                Assert.True(reader.Read());
                Assert.Equal((sbyte)42, reader.GetValue(0));
                Assert.True(reader.Read());
                Assert.Equal("residual", reader.GetValue(0));

                using AdbcDataReader jsonReader = GetShreddedReader(StructBehavior.JsonString);
                Assert.True(jsonReader.Read());
                Assert.Equal("42", jsonReader.GetString(0));
                Assert.True(jsonReader.Read());
                Assert.Equal("\"residual\"", jsonReader.GetString(0));
            }
            finally
            {
                ShreddedVariants.Reader = null;
            }
        }

        [Fact]
        public void VariantTimestampNtzIsUnspecified()
        {
            VariantArray array = new VariantArray.Builder()
                .Append(VariantValue.FromTimestampNtzNanos(1_000_000_000L))
                .Build();

            using AdbcDataReader reader = GetReader(VariantField(array, fromStorage: false), array, StructBehavior.Strict);

            Assert.True(reader.Read());
            DateTime value = Assert.IsType<DateTime>(reader.GetValue(0));
            Assert.Equal(DateTimeKind.Unspecified, value.Kind);
            Assert.Equal(new DateTime(1970, 1, 1, 0, 0, 1), value);
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
            cmd.Parameters.Add(new AdbcParameter { ParameterName = "idBytes", DbType = DbType.Guid, Value = GuidArray.GuidToBytes(s_guid) });
            cmd.Parameters.Add(new AdbcParameter { ParameterName = "ts", DbType = DbType.DateTimeOffset, Value = value });
            cmd.ExecuteNonQuery();

            Assert.NotNull(bound);
            Assert.IsType<GuidType>(bound!.Schema.GetFieldByIndex(0).DataType);
            Assert.Equal(s_guid, ((GuidArray)bound.Column(0)).GetGuid(0));
            Assert.Equal(s_guid, ((GuidArray)bound.Column(1)).GetGuid(0));
            Assert.Equal(s_guid, ((GuidArray)bound.Column(2)).GetGuid(0));

            TimestampType timestampType = Assert.IsType<TimestampType>(bound.Schema.GetFieldByIndex(3).DataType);
            Assert.Equal("UTC", timestampType.Timezone);
            Assert.Equal(value, ((TimestampArray)bound.Column(3)).GetTimestamp(0));
        }

        [Theory]
        [InlineData(DbType.Guid)]
        [InlineData(DbType.DateTimeOffset)]
        [InlineData(DbType.Int32)]
        [InlineData(DbType.String)]
        public void BindDBNullParameter(DbType dbType)
        {
            RecordBatch? bound = null;
            Mock<AdbcStatement> mockStatement = new Mock<AdbcStatement>();
            mockStatement.Setup(x => x.Bind(It.IsAny<RecordBatch>(), It.IsAny<Schema>())).Callback<RecordBatch, Schema>((b, s) => bound = b);
            mockStatement.Setup(x => x.ExecuteUpdate()).Returns(new UpdateResult(1));

            using AdbcCommand cmd = new AdbcCommand(mockStatement.Object, new Adbc.Client.AdbcConnection());
            cmd.Parameters.Add(new AdbcParameter { ParameterName = "p", DbType = dbType, Value = DBNull.Value });
            cmd.ExecuteNonQuery();

            Assert.NotNull(bound);
            Assert.True(bound!.Schema.GetFieldByIndex(0).IsNullable);
            Assert.True(bound.Column(0).IsNull(0));
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
            return GetReader(new[] { field }, new[] { array }, structBehavior);
        }

        private static AdbcDataReader GetReader(Field[] fields, IArrowArray[] arrays, StructBehavior structBehavior)
        {
            Schema schema = new Schema(fields, null);
            RecordBatch batch = new RecordBatch(schema, arrays, arrays[0].Length);
            QueryResult queryResult = new QueryResult(arrays[0].Length, new MockArrayStream(schema, new List<RecordBatch> { batch }));

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
