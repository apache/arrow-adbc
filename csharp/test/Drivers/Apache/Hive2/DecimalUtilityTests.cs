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
using System.Diagnostics;
using System.Globalization;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    /// <summary>
    /// Class for testing the Decimal Utilities tests.
    /// </summary>
    public class DecimalUtilityTests(ITestOutputHelper outputHelper)
    {
        private readonly ITestOutputHelper _outputHelper = outputHelper;

        [SkippableTheory]
        [MemberData(nameof(Decimal128Data))]
        public void TestCanConvertDecimal(string value, int precision, int scale, int byteWidth, byte[] expected, SqlDecimal? expectedDecimal = default)
        {
            byte[] actual = new byte[byteWidth];
            DecimalUtility.GetBytes(value, precision, scale, byteWidth, actual);
            Assert.Equal(expected, actual);
            Assert.Equal(0, byteWidth % 4);
            int[] buffer = new int[byteWidth / 4];
            for (int i = 0; i < buffer.Length; i++)
            {
                buffer[i] = BitConverter.ToInt32(actual, i * sizeof(int));
            }
            SqlDecimal actualDecimal = GetSqlDecimal128(actual, 0, precision, scale);
            if (expectedDecimal != null) Assert.Equal(expectedDecimal, actualDecimal);
        }

        [Fact(Skip = "Run manually to confirm equivalent performance")]
        public void TestConvertDecimalPerformance()
        {
            Stopwatch stopwatch = new();

            int testCount = 1000000;
            string testValue = "99999999999999999999999999999999999999";
            int byteWidth = 16;
            byte[] buffer = new byte[byteWidth];
            Decimal128Array.Builder builder = new Decimal128Array.Builder(new Types.Decimal128Type(38, 0));
            stopwatch.Restart();
            for (int i = 0; i < testCount; i++)
            {
                if (decimal.TryParse(testValue, NumberStyles.Float, NumberFormatInfo.InvariantInfo, out var actualDecimal))
                {
                    builder.Append(new SqlDecimal(actualDecimal));
                }
                else
                {
                    builder.Append(testValue);
                }
            }
            stopwatch.Stop();
            _outputHelper.WriteLine($"Decimal128Builder.Append: {testCount} iterations took {stopwatch.ElapsedMilliseconds} elapsed milliseconds");

            stopwatch.Restart();
            for (int i = 0; i < testCount; i++)
            {
                DecimalUtility.GetBytes(testValue, 38, 0, byteWidth, buffer);
                builder.Append(buffer);
            }
            stopwatch.Stop();
            _outputHelper.WriteLine($"DecimalUtility.GetBytes: {testCount} iterations took {stopwatch.ElapsedMilliseconds} elapsed milliseconds");
        }

        public static IEnumerable<object[]> Decimal128Data()
        {
            yield return new object[] { "0", 1, 0, 16, new byte[] { 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(0) };

            yield return new object[] { "1", 1, 0, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "1E0", 1, 0, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "10e-1", 1, 0, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "0.1e1", 1, 0, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };

            yield return new object[] { "12", 2, 0, 16, new byte[] { 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(12) };
            yield return new object[] { "12E0", 2, 0, 16, new byte[] { 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(12) };
            yield return new object[] { "120e-1", 2, 0, 16, new byte[] { 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(12) };
            yield return new object[] { "1.2e1", 2, 0, 16, new byte[] { 12, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(12) };

            yield return new object[] { "99999999999999999999999999999999999999", 38, 0, 16, new byte[] { 255, 255, 255, 255, 63, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75 } };
            yield return new object[] { "99999999999999999999999999999999999999E0", 38, 0, 16, new byte[] { 255, 255, 255, 255, 63, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75 } };
            yield return new object[] { "999999999999999999999999999999999999990e-1", 38, 0, 16, new byte[] { 255, 255, 255, 255, 63, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75 } };
            yield return new object[] { "0.99999999999999999999999999999999999999e38", 38, 0, 16, new byte[] { 255, 255, 255, 255, 63, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75 } };

            yield return new object[] { "-1", 1, 0, 16, new byte[] { 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-1) };
            yield return new object[] { "-1E0", 1, 0, 16, new byte[] { 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-1) };
            yield return new object[] { "-10e-1", 1, 0, 16, new byte[] { 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-1) };
            yield return new object[] { "-0.1e1", 1, 0, 16, new byte[] { 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-1) };

            yield return new object[] { "-12", 2, 0, 16, new byte[] { 244, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-12) };
            yield return new object[] { "-12E0", 2, 0, 16, new byte[] { 244, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-12) };
            yield return new object[] { "-120e-1", 2, 0, 16, new byte[] { 244, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-12) };
            yield return new object[] { "-1.2e1", 2, 0, 16, new byte[] { 244, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-12) };

            yield return new object[] { "1", 38, 0, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "1E0", 38, 0, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "10e-1", 38, 0, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "0.1e1", 38, 0, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };

            yield return new object[] { "1", 3, 2, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "1E0", 3, 2, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "10e-1", 3, 2, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "0.1e1", 3, 2, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };

            yield return new object[] { "1", 38, 2, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "1E0", 38, 2, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "10e-1", 38, 2, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };
            yield return new object[] { "0.1e1", 38, 2, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(1) };

            yield return new object[] { "0.1", 38, 1, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(0.1) };
            yield return new object[] { "0.1E0", 38, 1, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(0.1) };
            yield return new object[] { "1e-1", 38, 1, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(0.1) };
            yield return new object[] { "0.01e1", 38, 1, 16, new byte[] { 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(0.1) };

            yield return new object[] { "0.1", 38, 3, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(0.1) };
            yield return new object[] { "0.1E0", 38, 3, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(0.1) };
            yield return new object[] { "1e-1", 38, 3, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(0.1) };
            yield return new object[] { "0.01e1", 38, 3, 16, new byte[] { 100, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0 }, new SqlDecimal(0.1) };

            yield return new object[] { "-0.1", 38, 3, 16, new byte[] { 156, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-0.1) };
            yield return new object[] { "-0.1E0", 38, 3, 16, new byte[] { 156, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-0.1) };
            yield return new object[] { "-1e-1", 38, 3, 16, new byte[] { 156, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-0.1) };
            yield return new object[] { "-0.01e1", 38, 3, 16, new byte[] { 156, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255, 255 }, new SqlDecimal(-0.1) };

            yield return new object[] { "0.99999999999999999999999999999999999999", 38, 38, 16, new byte[] { 255, 255, 255, 255, 63, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75 } };
            yield return new object[] { "0.99999999999999999999999999999999999999E0", 38, 38, 16, new byte[] { 255, 255, 255, 255, 63, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75 } };
            yield return new object[] { "9.99999999999999999999999999999999999990e-1", 38, 38, 16, new byte[] { 255, 255, 255, 255, 63, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75 } };
            yield return new object[] { "0.0000000000000000000000000000000000000099999999999999999999999999999999999999e38", 38, 38, 16, new byte[] { 255, 255, 255, 255, 63, 34, 138, 9, 122, 196, 134, 90, 168, 76, 59, 75 } };
        }

        private static SqlDecimal GetSqlDecimal128(in byte[] valueBuffer, int index, int precision, int scale)
        {
            const int byteWidth = 16;
            const int intWidth = byteWidth / 4;
            const int longWidth = byteWidth / 8;

            byte mostSignificantByte = valueBuffer.AsSpan()[(index + 1) * byteWidth - 1];
            bool isPositive = (mostSignificantByte & 0x80) == 0;

            if (isPositive)
            {
                ReadOnlySpan<int> value = valueBuffer.AsSpan().CastTo<int>().Slice(index * intWidth, intWidth);
                return new SqlDecimal((byte)precision, (byte)scale, true, value[0], value[1], value[2], value[3]);
            }
            else
            {
                ReadOnlySpan<long> value = valueBuffer.AsSpan().CastTo<long>().Slice(index * longWidth, longWidth);
                long data1 = -value[0];
                long data2 = data1 == 0 ? -value[1] : ~value[1];

                return new SqlDecimal((byte)precision, (byte)scale, false, (int)(data1 & 0xffffffff), (int)(data1 >> 32), (int)(data2 & 0xffffffff), (int)(data2 >> 32));
            }
        }
    }
}
