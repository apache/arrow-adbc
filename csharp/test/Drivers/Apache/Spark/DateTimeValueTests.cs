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
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    public class DateTimeValueTests : Common.DateTimeValueTests<SparkTestConfiguration, SparkTestEnvironment>
    {
        public DateTimeValueTests(ITestOutputHelper output)
            : base(output, new SparkTestEnvironment.Factory())
        { }

        /// <summary>
        /// Tests INTERVAL data types (YEAR-MONTH and DAY-SECOND).
        /// </summary>
        /// <param name="intervalClause">The INTERVAL to test.</param>
        /// <param name="value">The expected return value.</param>
        /// <returns></returns>
        [SkippableTheory]
        [InlineData("INTERVAL 1 YEAR", "1-0")]
        [InlineData("INTERVAL 1 YEAR 2 MONTH", "1-2")]
        [InlineData("INTERVAL 2 MONTHS", "0-2")]
        [InlineData("INTERVAL -1 YEAR", "-1-0")]
        [InlineData("INTERVAL -1 YEAR 2 MONTH", "-0-10")]
        [InlineData("INTERVAL -2 YEAR 2 MONTH", "-1-10")]
        [InlineData("INTERVAL 1 YEAR -2 MONTH", "0-10")]
        [InlineData("INTERVAL 178956970 YEAR", "178956970-0")]
        [InlineData("INTERVAL 178956969 YEAR 11 MONTH", "178956969-11")]
        [InlineData("INTERVAL -178956970 YEAR", "-178956970-0")]
        [InlineData("INTERVAL 0 DAYS 0 HOURS 0 MINUTES 0 SECONDS", "0 00:00:00.000000000")]
        [InlineData("INTERVAL 1 DAYS", "1 00:00:00.000000000")]
        [InlineData("INTERVAL 2 HOURS", "0 02:00:00.000000000")]
        [InlineData("INTERVAL 3 MINUTES", "0 00:03:00.000000000")]
        [InlineData("INTERVAL 4 SECONDS", "0 00:00:04.000000000")]
        [InlineData("INTERVAL 1 DAYS 2 HOURS", "1 02:00:00.000000000")]
        [InlineData("INTERVAL 1 DAYS 2 HOURS 3 MINUTES", "1 02:03:00.000000000")]
        [InlineData("INTERVAL 1 DAYS 2 HOURS 3 MINUTES 4 SECONDS", "1 02:03:04.000000000")]
        [InlineData("INTERVAL 1 DAYS 2 HOURS 3 MINUTES 4.123123123 SECONDS", "1 02:03:04.123123000")] // Only to microseconds
        [InlineData("INTERVAL 106751990 DAYS 23 HOURS 59 MINUTES 59.999999 SECONDS", "106751990 23:59:59.999999000")]
        [InlineData("INTERVAL 106751991 DAYS 0 HOURS 0 MINUTES 0 SECONDS", "106751991 00:00:00.000000000")]
        [InlineData("INTERVAL -106751991 DAYS 0 HOURS 0 MINUTES 0 SECONDS", "-106751991 00:00:00.000000000")]
        [InlineData("INTERVAL -106751991 DAYS 23 HOURS 59 MINUTES 59.999999 SECONDS", "-106751990 00:00:00.000001000")]
        public async Task TestIntervalData(string intervalClause, string value)
        {
            string selectStatement = $"SELECT {intervalClause} AS INTERVAL_VALUE;";
            await SelectAndValidateValuesAsync(selectStatement, value, 1);
        }

        [SkippableTheory]
        [MemberData(nameof(TimestampBasicData), "TIMESTAMP")]
        [MemberData(nameof(TimestampExtendedData), "TIMESTAMP")]
        public override Task TestTimestampData(DateTimeOffset value, string columnType)
        {
            return base.TestTimestampData(value, columnType);
        }

        protected override string GetFormattedTimestampValue(string value)
        {
            return "TO_TIMESTAMP(" + QuoteValue(value) + ")";
        }
    }
}
