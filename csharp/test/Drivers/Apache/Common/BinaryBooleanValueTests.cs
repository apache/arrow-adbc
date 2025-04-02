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
using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Common
{
    // TODO: When supported, use prepared statements instead of SQL string literals
    //      Which will better test how the driver handles values sent/received

    /// <summary>
    /// Validates that specific binary and boolean values can be inserted, retrieved and targeted correctly
    /// </summary>
    public abstract class BinaryBooleanValueTests<TConfig, TEnv> : TestBase<TConfig, TEnv>
        where TConfig : TestConfiguration
        where TEnv : CommonTestEnvironment<TConfig>
    {
        public BinaryBooleanValueTests(ITestOutputHelper output, TestEnvironment<TConfig>.Factory<TEnv> testEnvFactory)
            : base(output, testEnvFactory) { }

        public static IEnumerable<object[]> ByteArrayData(int size)
        {
            var rnd = new Random();
            byte[] bytes = new byte[size];
            rnd.NextBytes(bytes);
            yield return new object[] { bytes };
        }

        public static IEnumerable<object[]> AsciiArrayData(int size)
        {
            const string values = "abcdefghijklmnopqrstuvwxyz0123456789";
            StringBuilder builder = new StringBuilder();
            Random rnd = new();
            byte[] bytes = new byte[size];
            for (int i = 0; i < size; i++)
            {
                builder.Append(values[rnd.Next(values.Length)]);
            }
            yield return new object[] { Encoding.UTF8.GetBytes(builder.ToString()) };
        }

        /// <summary>
        /// Validates if driver can send and receive specific Binary values correctly.
        /// </summary>
        public virtual async Task TestBinaryData(byte[]? value)
        {
            string columnName = "BINARYTYPE";
            using TemporaryTable table = await NewTemporaryTableAsync(Statement, string.Format("{0} {1}", columnName, "BINARY"));
            string? formattedValue = GetFormattedBinaryValue(value);
            await ValidateInsertSelectDeleteSingleValueAsync(
                table.TableName,
                columnName,
                value,
                formattedValue);
        }

        /// <summary>
        /// Get the data source specific format for a binary literal.
        /// </summary>
        /// <param name="value"></param>
        /// <returns>Null if value is null, otherwise the data source specific format for a binary literal.</returns>
        protected abstract string? GetFormattedBinaryValue(byte[]? value);

        /// <summary>
        /// Validates if driver can send and receive specific Boolean values correctly.
        /// </summary>
        [SkippableTheory]
        [InlineData(null)]
        [InlineData(true)]
        [InlineData(false)]
        public async Task TestBooleanData(bool? value)
        {
            string columnName = "BOOLEANTYPE";
            using TemporaryTable table = await NewTemporaryTableAsync(Statement, string.Format("{0} {1}", columnName, "BOOLEAN"));
            string? formattedValue = value == null ? null : $"{value?.ToString(CultureInfo.InvariantCulture)}";
            await ValidateInsertSelectDeleteTwoValuesAsync(
                table.TableName,
                columnName,
                value,
                formattedValue);
        }

        /// <summary>
        /// Validates if driver can receive specific NULL values correctly.
        /// </summary>
        public virtual async Task TestNullData(string projectionClause)
        {
            string selectStatement = $"SELECT {projectionClause}";
            // Note: by default, this returns as String type, not NULL type.
            await SelectAndValidateValuesAsync(selectStatement, (object?)null, 1);
        }

        [SkippableTheory]
        [InlineData(1)]
        [InlineData(7)]
        [InlineData(8)]
        [InlineData(9)]
        [InlineData(15)]
        [InlineData(16)]
        [InlineData(17)]
        [InlineData(23)]
        [InlineData(24)]
        [InlineData(25)]
        [InlineData(31)]
        [InlineData(32)] // Full integer
        [InlineData(33)]
        [InlineData(39)]
        [InlineData(40)]
        [InlineData(41)]
        [InlineData(47)]
        [InlineData(48)]
        [InlineData(49)]
        [InlineData(63)]
        [InlineData(64)] // Full 2 integers
        [InlineData(65)]
        public async Task TestMultilineNullData(int numberOfValues)
        {
            Random rnd = new();
            int percentIsNull = 50;

            object?[] values = new object?[numberOfValues];
            for (int i = 0; i < numberOfValues; i++)
            {
                values[i] = rnd.Next(0, 100) < percentIsNull ? null : rnd.Next(0, 2) != 0;
            }
            string columnName = "BOOLEANTYPE";
            string indexColumnName = "INDEXCOL";
            using TemporaryTable table = await NewTemporaryTableAsync(Statement, string.Format("{0} {1}, {2} {3}", indexColumnName, "INT", columnName, "BOOLEAN"));
            await ValidateInsertSelectDeleteMultipleValuesAsync(table.TableName, columnName, indexColumnName, values);
        }
    }
}
