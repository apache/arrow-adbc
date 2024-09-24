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
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    // TODO: When supported, use prepared statements instead of SQL string literals
    //      Which will better test how the driver handles values sent/received

    /// <summary>
    /// Validates that specific string and character values can be inserted, retrieved and targeted correctly
    /// </summary>
    public class StringValueTests : TestBase<SparkTestConfiguration, SparkTestEnvironment>
    {
        public StringValueTests(ITestOutputHelper output) : base(output, new SparkTestEnvironment.Factory()) { }

        public static IEnumerable<object[]> ByteArrayData(int size)
        {
            var rnd = new Random();
            byte[] bytes = new byte[size];
            rnd.NextBytes(bytes);
            yield return new object[] { bytes };
        }

        /// <summary>
        /// Validates if driver can send and receive specific String values correctly.
        /// </summary>
        [SkippableTheory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("你好")]
        [InlineData("String contains formatting characters tab\t, newline\n, carriage return\r.", SparkServerType.Databricks)]
        [InlineData(" Leading and trailing spaces ")]
        internal async Task TestStringData(string? value, SparkServerType? serverType = default)
        {
            Skip.If(serverType != null && TestEnvironment.ServerType != serverType);
            string columnName = "STRINGTYPE";
            using TemporaryTable table = await NewTemporaryTableAsync(Statement, string.Format("{0} {1}", columnName, "STRING"));
            await ValidateInsertSelectDeleteSingleValueAsync(
                table.TableName,
                columnName,
                value,
                value != null ? QuoteValue(value) : value);
        }

        /// <summary>
        /// Validates if driver can send and receive specific VARCHAR values correctly.
        /// </summary>
        [SkippableTheory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("你好")]
        [InlineData("String contains formatting characters tab\t, newline\n, carriage return\r.", SparkServerType.Databricks)]
        [InlineData(" Leading and trailing spaces ")]
        internal async Task TestVarcharData(string? value, SparkServerType? serverType = default)
        {
            Skip.If(serverType != null && TestEnvironment.ServerType != serverType);
            string columnName = "VARCHARTYPE";
            using TemporaryTable table = await NewTemporaryTableAsync(Statement, string.Format("{0} {1}", columnName, "VARCHAR(100)"));
            await ValidateInsertSelectDeleteSingleValueAsync(
                table.TableName,
                columnName,
                value,
                value != null ? QuoteValue(value) : value);
        }

        private bool IsBelowMinimumVersion(string? minVersion) => minVersion != null && VendorVersionAsVersion < Version.Parse(minVersion);

        /// <summary>
        /// Validates if driver can send and receive specific VARCHAR values correctly.
        /// </summary>
        [SkippableTheory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("你好")]
        [InlineData("String contains formatting characters tab\t, newline\n, carriage return\r.", SparkServerType.Databricks)]
        [InlineData(" Leading and trailing spaces ")]
        internal async Task TestCharData(string? value, SparkServerType? serverType = default)
        {
            Skip.If(serverType != null && TestEnvironment.ServerType != serverType);
            string columnName = "CHARTYPE";
            int fieldLength = 100;
            using TemporaryTable table = await NewTemporaryTableAsync(Statement, string.Format("{0} {1}", columnName, $"CHAR({fieldLength})"));

            string? formattedValue = value != null ? QuoteValue(value.PadRight(fieldLength)) : value;
            string? paddedValue = value != null ? value.PadRight(fieldLength) : value;

            await InsertSingleValueAsync(table.TableName, columnName, formattedValue);
            await SelectAndValidateValuesAsync(table.TableName, columnName, paddedValue, 1, formattedValue);
            string whereClause = GetWhereClause(columnName, formattedValue ?? paddedValue);
            if (SupportsDelete) await DeleteFromTableAsync(table.TableName, whereClause, 1);
        }

        /// <summary>
        /// Validates if driver fails to insert invalid length of VARCHAR value.
        /// </summary>
        [SkippableTheory]
        [InlineData("String whose length is too long for VARCHAR(10).")]
        public async Task TestVarcharExceptionData(string value)
        {
            string columnName = "VARCHARTYPE";
            using TemporaryTable table = await NewTemporaryTableAsync(Statement, string.Format("{0} {1}", columnName, "VARCHAR(10)"));
            AdbcException exception = await Assert.ThrowsAsync<HiveServer2Exception>(async () => await ValidateInsertSelectDeleteSingleValueAsync(
                table.TableName,
                columnName,
                value,
                value != null ? QuoteValue(value) : value));

            bool serverTypeDatabricks = TestEnvironment.ServerType == SparkServerType.Databricks;
            string[] expectedTexts = serverTypeDatabricks
                ? ["DELTA_EXCEED_CHAR_VARCHAR_LIMIT", "DeltaInvariantViolationException"]
                : ["Exceeds", "length limitation: 10"];
            AssertContainsAll(expectedTexts, exception.Message);

            string? expectedSqlState = serverTypeDatabricks ? "22001" : null;
            Assert.Equal(expectedSqlState, exception.SqlState);
        }

    }
}
