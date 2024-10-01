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
using System.Globalization;
using System.Text;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class HiveServer2ReaderTest
    {
        private const bool IsValid = true;
        private const bool IsNotValid = false;

        [Theory]
        [MemberData(nameof(GetDateTestData), /* isKnownFormat */ true)]
        internal void TestCanConvertKnownFormatDate(string date, DateTime expected, bool isValid)
        {
            ReadOnlySpan<byte> dateSpan = Encoding.UTF8.GetBytes(date).AsSpan();
            if (isValid)
            {
                Assert.True(HiveServer2Reader.TryParse(dateSpan, out DateTime dateTime));
                Assert.Equal(expected, dateTime);
            }
            else
            {
                Assert.False(HiveServer2Reader.TryParse(dateSpan, out DateTime _));
            }
        }

        [Theory]
        [MemberData(nameof(GetDateTestData), /* isKnownFormat */ false)]
        internal void TestCanConvertUnknownFormatDate(string date, DateTime expected, bool isValid)
        {
            var builder = new StringArray.Builder();
            builder.Append(date);
            var stringArray = builder.Build();
            if (isValid)
            {
                var dateArray = HiveServer2Reader.ConvertToDate32(stringArray, stringArray.Data.DataType);
                Assert.Equal(1, dateArray.Length);
                Assert.Equal(expected, dateArray.GetDateTime(0));
            }
            else
            {
                Assert.Throws<FormatException>(() => HiveServer2Reader.ConvertToDate32(stringArray, stringArray.Data.DataType));
            }
        }

        [Theory]
        [MemberData(nameof(GetTimestampTestData), /* isKnownFormat */ true)]
        internal void TestCanConvertKnownFormatTimestamp(string date, DateTimeOffset expected, bool isValid)
        {
            ReadOnlySpan<byte> dateSpan = Encoding.UTF8.GetBytes(date).AsSpan();
            if (isValid)
            {
                Assert.True(HiveServer2Reader.TryParse(dateSpan, out DateTimeOffset dateTime));
                Assert.Equal(expected, dateTime);
            }
            else
            {
                Assert.False(HiveServer2Reader.TryParse(dateSpan, out DateTimeOffset _));
            }
        }

        [Theory]
        [MemberData(nameof(GetTimestampTestData), /* isKnownFormat */ false)]
        internal void TestCanConvertUnknownFormatTimestamp(string date, DateTimeOffset expected, bool isValid)
        {
            var builder = new StringArray.Builder();
            builder.Append(date);
            var stringArray = builder.Build();
            if (isValid)
            {
                TimestampArray timestampArray = HiveServer2Reader.ConvertToTimestamp(stringArray, stringArray.Data.DataType);
                Assert.Equal(1, timestampArray.Length);
                Assert.Equal(expected, timestampArray.GetTimestamp(0));
            }
            else
            {
                Assert.Throws<FormatException>(() => HiveServer2Reader.ConvertToTimestamp(stringArray, stringArray.Data.DataType));
            }
        }

        public static TheoryData<string, DateTime, bool> GetDateTestData(bool isKnownFormat)
        {
            string[] dates =
                [
                    "0001-01-01",
                    "0001-12-31",
                    "1970-01-01",
                    "2024-12-31",
                    "9999-12-31",
                ];

            var data = new TheoryData<string, DateTime, bool>();
            foreach (string date in dates)
            {
                data.Add(date, DateTime.Parse(date, CultureInfo.InvariantCulture), IsValid);
            }

            // Conditionally invalid component separators
            string[] leadingSpaces = ["", " "];
            string[] TrailingSpaces = ["", " "];
            string[] separators = ["/", " "];
            foreach (string leadingSpace in leadingSpaces)
            {
                foreach (string trailingSpace in TrailingSpaces)
                {
                    foreach (string separator in separators)
                    {
                        foreach (string date in dates)
                        {
                            data.Add(leadingSpace + date.Replace("-", separator) + trailingSpace, DateTime.Parse(date), !isKnownFormat);
                        }
                    }
                }
            }

            // Always invalid for a date separator
            separators = [":"];
            foreach (string leadingSpace in leadingSpaces)
            {
                foreach (string trailingSpace in TrailingSpaces)
                {
                    foreach (string separator in separators)
                    {
                        foreach (string date in dates)
                        {
                            data.Add(leadingSpace + date.Replace("-", separator) + trailingSpace, default, IsNotValid);
                        }
                    }
                }
            }

            string[] invalidDates =
                [
                    "0001-01-00",
                    "0001-01-32",
                    "0001-02-30",
                    "0001-13-01",
                    "00a1-01-01",
                    "0001-a1-01",
                    "0001-01-a1",
                    "001a-01-01",
                    "0001-1a-01",
                    "0001-01-1a",
                ];
            foreach (string date in invalidDates)
            {
                data.Add(date, default, IsNotValid);
            }

            return data;
        }

        public static TheoryData<string, DateTimeOffset, bool> GetTimestampTestData(bool isKnownFormat)
        {
            string[] dates =
                [
                    "0001-01-01 00:00:00",
                    "9999-12-31 23:59:59",
                    "0001-01-01 00:00:00.1000000",
                    "0001-12-31 00:00:00.0100000",
                    "1970-01-01 00:00:00.0010000",
                    "2024-12-31 00:00:00.0001000",
                    "9999-12-31 00:00:00.0000100",
                    "9999-12-31 00:00:00.",
                    "9999-12-31 00:00:00.9",
                    "9999-12-31 00:00:00.99",
                    "9999-12-31 00:00:00.999",
                    "9999-12-31 00:00:00.9999",
                    "9999-12-31 00:00:00.99999",
                    "9999-12-31 00:00:00.999999",
                    "9999-12-31 00:00:00.999999",
                    "9999-12-31 00:00:00.9999990",
                    "9999-12-31 00:00:00.99999900",
                ];

            var data = new TheoryData<string, DateTimeOffset, bool>();
            foreach (string date in dates)
            {
                data.Add(date, DateTimeOffset.Parse(date, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal), IsValid);
            }

            // Conditionally invalid component separators
            string[] leadingSpaces = ["", " "];
            string[] TrailingSpaces = ["", " "];
            string[] dateSeparators = ["/", " "];
            foreach (string leadingSpace in leadingSpaces)
            {
                foreach (string trailingSpace in TrailingSpaces)
                {
                    foreach (string separator in dateSeparators)
                    {
                        foreach (string date in dates)
                        {
                            data.Add(
                                leadingSpace + date.Replace("-", separator) + trailingSpace,
                                DateTimeOffset.Parse(date, CultureInfo.InvariantCulture, DateTimeStyles.AssumeUniversal),
                                !isKnownFormat);
                        }
                    }
                }
            }

            // Always an invalid separator for date.
            dateSeparators = [":"];
            foreach (string leadingSpace in leadingSpaces)
            {
                foreach (string trailingSpace in TrailingSpaces)
                {
                    foreach (string separator in dateSeparators)
                    {
                        foreach (string date in dates)
                        {
                            data.Add(leadingSpace + date.Replace("-", separator) + trailingSpace, default, IsNotValid);
                        }
                    }
                }
            }

            string[] invalidDates =
                [
                    "0001-01-00 00:00:00",
                    "0001-01-32 00:00:00",
                    "0001-02-30 00:00:00",
                    "0001-13-01 00:00:00",
                    "abcd-13-01 00:00:00",
                    "0001-12-01 00:00:00.abc",
                    "00a1-01-01 00:00:00",
                    "0001-a1-01 00:00:00",
                    "0001-01-a1 00:00:00",
                    "0001-01-01 a0:00:00",
                    "0001-01-01 00:a0:00",
                    "0001-01-01 00:00:a0",
                    "001a-01-01 00:00:00",
                    "0010-1a-01 00:00:00",
                    "0010-10-1a 00:00:00",
                ];
            foreach (string date in invalidDates)
            {
                data.Add(date, default, IsNotValid);
            }

            return data;
        }
    }
}
