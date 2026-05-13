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
using System.Data.SqlTypes;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Golden-output tests for <c>IArrowArrayExtensions.ValueAt(index, StructResultType.JsonString)</c>
    /// — i.e. the private <c>SerializeToJson</c> path. Each test builds a single-row <see cref="StructArray"/>
    /// containing one field of a particular Arrow type, runs it through the JSON conversion, and asserts
    /// the exact string output.
    /// </summary>
    public class SerializeStructToJsonTests
    {
        private static string SerializeRow(StructArray structArray, int index = 0)
            => (string)structArray.ValueAt(index, StructResultType.JsonString)!;

        private static StructArray SingleFieldStruct(string fieldName, IArrowType type, IArrowArray values)
        {
            var structType = new StructType(new[] { new Field(fieldName, type, nullable: true) });
            return new StructArray(structType, values.Length, new[] { values }, new ArrowBuffer.BitmapBuilder().Build());
        }

        [Fact]
        public void Bool()
        {
            var values = new BooleanArray.Builder().Append(true).Build();
            var s = SingleFieldStruct("f", BooleanType.Default, values);
            Assert.Equal("{\"f\":true}", SerializeRow(s));
        }

        [Fact]
        public void Int8() => AssertJson("{\"f\":-7}", Int8Type.Default, new Int8Array.Builder().Append((sbyte)-7).Build());

        [Fact]
        public void Int16() => AssertJson("{\"f\":-300}", Int16Type.Default, new Int16Array.Builder().Append((short)-300).Build());

        [Fact]
        public void Int32() => AssertJson("{\"f\":-70000}", Int32Type.Default, new Int32Array.Builder().Append(-70000).Build());

        [Fact]
        public void Int64() => AssertJson("{\"f\":-5000000000}", Int64Type.Default, new Int64Array.Builder().Append(-5_000_000_000L).Build());

        [Fact]
        public void UInt8() => AssertJson("{\"f\":200}", UInt8Type.Default, new UInt8Array.Builder().Append((byte)200).Build());

        [Fact]
        public void UInt16() => AssertJson("{\"f\":60000}", UInt16Type.Default, new UInt16Array.Builder().Append((ushort)60000).Build());

        [Fact]
        public void UInt32() => AssertJson("{\"f\":4000000000}", UInt32Type.Default, new UInt32Array.Builder().Append(4_000_000_000u).Build());

        [Fact]
        public void UInt64() => AssertJson("{\"f\":9000000000000000000}", UInt64Type.Default, new UInt64Array.Builder().Append(9_000_000_000_000_000_000ul).Build());

        [Fact]
        public void Float() => AssertJson("{\"f\":1.5}", FloatType.Default, new FloatArray.Builder().Append(1.5f).Build());

        [Fact]
        public void Double() => AssertJson("{\"f\":3.25}", DoubleType.Default, new DoubleArray.Builder().Append(3.25).Build());

        [Fact]
        public void String() => AssertJson("{\"f\":\"hi\"}", StringType.Default, new StringArray.Builder().Append("hi").Build());

        [Fact]
        public void StringWithEscapes()
            // JsonSerializer / Utf8JsonWriter default encoder escapes '"' as \u0022
            // (relaxed encoder would produce \").
            => AssertJson("{\"f\":\"a\\u0022b\\nc\"}", StringType.Default, new StringArray.Builder().Append("a\"b\nc").Build());

        [Fact]
        public void Binary()
        {
            var values = new BinaryArray.Builder().Append(new byte[] { 0x01, 0x02, 0x03 }).Build();
            AssertJson("{\"f\":\"AQID\"}", BinaryType.Default, values);
        }

        [Fact]
        public void Date32()
        {
            var d = new DateTime(2026, 4, 18, 0, 0, 0, DateTimeKind.Unspecified);
            var values = new Date32Array.Builder().Append(d).Build();
            AssertJson("{\"f\":\"2026-04-18T00:00:00\"}", Date32Type.Default, values);
        }

        [Fact]
        public void Date64()
        {
            var d = new DateTime(2026, 4, 18, 0, 0, 0, DateTimeKind.Unspecified);
            var values = new Date64Array.Builder().Append(d).Build();
            AssertJson("{\"f\":\"2026-04-18T00:00:00\"}", Date64Type.Default, values);
        }

        [Fact]
        public void Timestamp()
        {
            var ts = new DateTimeOffset(2026, 4, 18, 12, 34, 56, TimeSpan.Zero);
            var values = new TimestampArray.Builder(TimestampType.Default).Append(ts).Build();
            AssertJson("{\"f\":\"2026-04-18T12:34:56+00:00\"}", TimestampType.Default, values);
        }

        [Fact]
        public void Time32Seconds()
        {
            // 5 minutes past midnight
            var builder = new Time32Array.Builder(TimeUnit.Second);
            builder.Append(300);
            AssertJson("{\"f\":\"00:05:00\"}", new Time32Type(TimeUnit.Second), builder.Build());
        }

        [Fact]
        public void Time64Microseconds()
        {
            var builder = new Time64Array.Builder(TimeUnit.Microsecond);
            builder.Append(1_000_000L); // 1 second
            AssertJson("{\"f\":\"00:00:01\"}", new Time64Type(TimeUnit.Microsecond), builder.Build());
        }

        [Fact]
        public void Decimal32_AsNumber()
        {
            var type = new Decimal32Type(9, 2);
            var builder = new Decimal32Array.Builder(type);
            builder.Append(12.34m);
            AssertJson("{\"f\":12.34}", type, builder.Build());
        }

        [Fact]
        public void Decimal64_AsNumber()
        {
            var type = new Decimal64Type(15, 3);
            var builder = new Decimal64Array.Builder(type);
            builder.Append(123456789.012m);
            AssertJson("{\"f\":123456789.012}", type, builder.Build());
        }

        [Fact]
        public void Decimal128_NarrowPrecision_AsNumber()
        {
            // Precision <= 15 — every value fits in a double-round-tripping decimal,
            // so we emit a bare JSON number.
            var type = new Decimal128Type(10, 2);
            var builder = new Decimal128Array.Builder(type);
            builder.Append(123.45m);
            AssertJson("{\"f\":123.45}", type, builder.Build());
        }

        [Fact]
        public void Decimal128_WidePrecision_AsString()
        {
            // Precision > 15 — values may not round-trip through double. Emit as a JSON
            // string; consumers who care about precision parse it with a decimal type.
            var type = new Decimal128Type(20, 2);
            var builder = new Decimal128Array.Builder(type);
            builder.Append(12345678901234567.89m);
            AssertJson("{\"f\":\"12345678901234567.89\"}", type, builder.Build());
        }

        [Fact]
        public void Decimal256_AsString()
        {
            // ValueAt returns Decimal256 values as strings (GetString), so the JSON is a string.
            var type = new Decimal256Type(30, 2);
            var builder = new Decimal256Array.Builder(type);
            builder.Append(9999.99m);
            AssertJson("{\"f\":\"9999.99\"}", type, builder.Build());
        }

        [Fact]
        public void NullField()
        {
            // Build an Int32Array with a single null entry.
            var builder = new Int32Array.Builder();
            builder.AppendNull();
            AssertJson("{\"f\":null}", Int32Type.Default, builder.Build());
        }

        [Fact]
        public void ListOfInt32()
        {
            var listBuilder = new ListArray.Builder(Int32Type.Default);
            var valuesBuilder = (Int32Array.Builder)listBuilder.ValueBuilder;
            listBuilder.Append();
            valuesBuilder.AppendRange(new[] { 1, 2, 3 });
            ListArray list = listBuilder.Build();

            var s = SingleFieldStruct("f", new ListType(Int32Type.Default), list);
            Assert.Equal("{\"f\":[1,2,3]}", SerializeRow(s));
        }

        [Fact]
        public void NestedStruct()
        {
            // outer struct containing an inner struct with two int fields
            var innerA = new Int32Array.Builder().Append(10).Build();
            var innerB = new StringArray.Builder().Append("ten").Build();

            var innerType = new StructType(new[]
            {
                new Field("a", Int32Type.Default, nullable: true),
                new Field("b", StringType.Default, nullable: true),
            });

            var innerStruct = new StructArray(
                innerType, 1,
                new IArrowArray[] { innerA, innerB },
                new ArrowBuffer.BitmapBuilder().Build());

            var outerType = new StructType(new[] { new Field("inner", innerType, nullable: true) });
            var outer = new StructArray(
                outerType, 1,
                new IArrowArray[] { innerStruct },
                new ArrowBuffer.BitmapBuilder().Build());

            string actual = SerializeRow(outer);
            Assert.True("{\"inner\":{\"a\":10,\"b\":\"ten\"}}" == actual, $"actual: {actual}");
        }

        private static void AssertJson(string expected, IArrowType type, IArrowArray values)
        {
            var s = SingleFieldStruct("f", type, values);
            string actual = SerializeRow(s);
            // Untruncated failure output so golden mismatches are actionable.
            Assert.True(expected == actual, $"\nexpected: {expected}\nactual:   {actual}");
        }
    }
}
