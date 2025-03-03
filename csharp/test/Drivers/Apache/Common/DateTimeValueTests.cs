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
using System.Globalization;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Common
{
    // TODO: When supported, use prepared statements instead of SQL string literals
    //      Which will better test how the driver handles values sent/received

    /// <summary>
    /// Validates that specific date, timestamp and interval values can be inserted, retrieved and targeted correctly
    /// </summary>
    public abstract class DateTimeValueTests<TConfig, TEnv> : TestBase<TConfig, TEnv>
        where TConfig : TestConfiguration
        where TEnv : CommonTestEnvironment<TConfig>
    {
        // Spark handles microseconds but not nanoseconds. Truncated to 6 decimal places.
        const string DateTimeZoneFormat = "yyyy-MM-dd'T'HH:mm:ss'.'ffffffK";
        const string DateTimeFormat = "yyyy-MM-dd' 'HH:mm:ss";
        protected const string DateFormat = "yyyy-MM-dd";

        private static readonly DateTimeOffset[] s_timestampBasicValues =
        [
#if NET5_0_OR_GREATER
            DateTimeOffset.UnixEpoch,
#endif
            DateTimeOffset.MaxValue,
            DateTimeOffset.UtcNow,
        ];

        private static readonly DateTimeOffset[] s_timestampExtendedValues =
        [
            DateTimeOffset.MinValue,
            DateTimeOffset.UtcNow.ToOffset(TimeSpan.FromHours(4))
        ];

        public DateTimeValueTests(ITestOutputHelper output, TestEnvironment<TConfig>.Factory<TEnv> testEnvFactory)
            : base(output, testEnvFactory) { }

        /// <summary>
        /// Validates if driver can send and receive specific Timstamp values correctly
        /// </summary>
        public virtual async Task TestTimestampData(DateTimeOffset value, string columnType)
        {
            string columnName = "TIMESTAMPTYPE";
            using TemporaryTable table = await NewTemporaryTableAsync(Statement, string.Format("{0} {1}", columnName, columnType));

            string format = TestEnvironment.GetValueForProtocolVersion(DateTimeFormat, DateTimeZoneFormat)!;
            string formattedValue = $"{value.ToString(format, CultureInfo.InvariantCulture)}";
            DateTimeOffset truncatedValue = DateTimeOffset.ParseExact(formattedValue, format, CultureInfo.InvariantCulture);

            object expectedValue = TestEnvironment.GetValueForProtocolVersion(formattedValue, truncatedValue)!;
            await ValidateInsertSelectDeleteSingleValueAsync(
                table.TableName,
                columnName,
                expectedValue,
                GetFormattedTimestampValue(formattedValue));
        }

        protected abstract string GetFormattedTimestampValue(string value);

        /// <summary>
        /// Validates if driver can send and receive specific no timezone Timstamp values correctly
        /// </summary>
        [SkippableTheory]
        [MemberData(nameof(TimestampBasicData), "DATE")]
        public async Task TestDateData(DateTimeOffset value, string columnType)
        {
            string columnName = "DATETYPE";
            using TemporaryTable table = await NewTemporaryTableAsync(Statement, string.Format("{0} {1}", columnName, columnType));

            string formattedValue = $"{value.ToString(DateFormat, CultureInfo.InvariantCulture)}";
            DateTimeOffset truncatedValue = DateTimeOffset.ParseExact(formattedValue, DateFormat, CultureInfo.InvariantCulture);

            // Remove timezone offset
            object expectedValue = TestEnvironment.GetValueForProtocolVersion(formattedValue, new DateTimeOffset(truncatedValue.DateTime, TimeSpan.Zero))!;
            await ValidateInsertSelectDeleteSingleValueAsync(
                table.TableName,
                columnName,
                expectedValue,
                "TO_DATE(" + QuoteValue(formattedValue) + ")");
        }

        public static IEnumerable<object[]> TimestampBasicData(string columnType)
        {
            foreach (DateTimeOffset timestamp in s_timestampBasicValues)
            {
                yield return new object[] { timestamp, columnType };
            }
        }

        public static IEnumerable<object[]> TimestampExtendedData(string columnType)
        {
            foreach (DateTimeOffset timestamp in s_timestampExtendedValues)
            {
                yield return new object[] { timestamp, columnType };
            }
        }
    }
}
