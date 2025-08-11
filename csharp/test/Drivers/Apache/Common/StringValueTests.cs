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
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Common
{
    // TODO: When supported, use prepared statements instead of SQL string literals
    //      Which will better test how the driver handles values sent/received

    /// <summary>
    /// Validates that specific string and character values can be inserted, retrieved and targeted correctly
    /// </summary>
    public abstract class StringValueTests<TConfig, TEnv> : TestBase<TConfig, TEnv>
        where TConfig : TestConfiguration
        where TEnv : CommonTestEnvironment<TConfig>
    {
        public StringValueTests(ITestOutputHelper output, TestEnvironment<TConfig>.Factory<TEnv> testEnvFactory)
            : base(output, testEnvFactory) { }

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
        [InlineData(" Leading and trailing spaces ")]
        protected async Task TestStringData(string? value)
        {
            string columnName = "STRINGTYPE";
            using TemporaryTable table = await NewTemporaryTableAsync(Connection, string.Format("{0} {1}", columnName, "STRING"));
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
        [InlineData(" Leading and trailing spaces ")]
        protected async Task TestVarcharData(string? value)
        {
            string columnName = "VARCHARTYPE";
            string typeName = "VARCHAR(100)";
            using TemporaryTable table = await NewTemporaryTableAsync(Connection, string.Format("{0} {1}", columnName, typeName));
            await ValidateInsertSelectDeleteSingleValueAsync(
                table.TableName,
                columnName,
                value,
                value != null ? $"CAST({QuoteValue(value)} as {typeName})" : value);
        }

        /// <summary>
        /// Validates if driver can send and receive specific VARCHAR values correctly.
        /// </summary>
        internal virtual async Task TestCharData(string? value)
        {
            string columnName = "CHARTYPE";
            int fieldLength = 100;
            string typeName = $"CHAR({fieldLength})";
            using TemporaryTable table = await NewTemporaryTableAsync(Connection, string.Format("{0} {1}", columnName, typeName));

            string? formattedValue = value != null ? $"CAST({QuoteValue(value.PadRight(fieldLength))} as {typeName})" : value;
            string? paddedValue = value != null ? value.PadRight(fieldLength) : value;

            await InsertSingleValueAsync(table.TableName, columnName, formattedValue);
            await SelectAndValidateValuesAsync(table.TableName, columnName, paddedValue, 1, formattedValue);
            string whereClause = GetWhereClause(columnName, formattedValue ?? paddedValue);
            if (SupportsDelete) await DeleteFromTableAsync(table.TableName, whereClause, 1);
        }

        /// <summary>
        /// Validates if driver fails to insert invalid length of VARCHAR value.
        /// </summary>
        protected virtual async Task TestVarcharExceptionData(string value, string[] expectedTexts, string? expectedSqlState)
        {
            string columnName = "VARCHARTYPE";
            using TemporaryTable table = await NewTemporaryTableAsync(Connection, string.Format("{0} {1}", columnName, "VARCHAR(10)"));
            AdbcException exception = await Assert.ThrowsAsync<HiveServer2Exception>(async () => await ValidateInsertSelectDeleteSingleValueAsync(
                GetSelectSingleValueStatement(table.TableName, columnName, value.Substring(0, 10)),
                table.TableName,
                columnName,
                value,
                value != null ? QuoteValue(value) : value));

            AssertContainsAll(expectedTexts, exception.Message);
            Assert.Equal(expectedSqlState, exception.SqlState);
        }
    }
}
