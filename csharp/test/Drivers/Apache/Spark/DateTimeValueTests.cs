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
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    public class DateTimeValueTests : Common.DateTimeValueTests<SparkTestConfiguration, SparkTestEnvironment>
    {
        public DateTimeValueTests(ITestOutputHelper output)
            : base(output, new SparkTestEnvironment.Factory())
        { }

        [SkippableTheory]
        [MemberData(nameof(TimestampData), "TIMESTAMP_LTZ")]
        public async Task TestTimestampDataDatabricks(DateTimeOffset value, string columnType)
        {
            Skip.If(TestEnvironment.ServerType != SparkServerType.Databricks);
            await base.TestTimestampData(value, columnType);
        }

        /// <summary>
        /// Validates if driver can send and receive specific no timezone Timstamp values correctly
        /// </summary>
        [SkippableTheory]
        [MemberData(nameof(TimestampData), "TIMESTAMP_NTZ")]
        public async Task TestTimestampNoTimezoneDataDatabricks(DateTimeOffset value, string columnType)
        {
            Skip.If(TestEnvironment.ServerType != SparkServerType.Databricks);
            string columnName = "TIMESTAMPTYPE";
            using TemporaryTable table = await NewTemporaryTableAsync(Statement, string.Format("{0} {1}", columnName, columnType));

            string formattedValue = $"{value.ToString(DateFormat, CultureInfo.InvariantCulture)}";
            DateTimeOffset truncatedValue = DateTimeOffset.ParseExact(formattedValue, DateFormat, CultureInfo.InvariantCulture);

            await ValidateInsertSelectDeleteSingleValueAsync(
                table.TableName,
                columnName,
                // Remove timezone offset
                new DateTimeOffset(truncatedValue.DateTime, TimeSpan.Zero),
                QuoteValue(formattedValue));
        }
    }
}
