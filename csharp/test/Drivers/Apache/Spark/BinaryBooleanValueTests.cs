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

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    // TODO: When supported, use prepared statements instead of SQL string literals
    //      Which will better test how the driver handles values sent/received

    /// <summary>
    /// Validates that specific binary and boolean values can be inserted, retrieved and targeted correctly
    /// </summary>
    public class BinaryBooleanValueTests : SparkTestBase
    {
        public BinaryBooleanValueTests(ITestOutputHelper output) : base(output) { }

        public static IEnumerable<object[]> ByteArrayData(int size)
        {
            var rnd = new Random();
            byte[] bytes = new byte[size];
            rnd.NextBytes(bytes);
            yield return new object[] { bytes };
        }

        /// <summary>
        /// Validates if driver can send and receive specific Binary values correctly.
        /// </summary>
        [SkippableTheory]
        [MemberData(nameof(ByteArrayData), 0)]
        [MemberData(nameof(ByteArrayData), 2)]
        [MemberData(nameof(ByteArrayData), 1024)]
        public async Task TestBinaryData(byte[] value)
        {
            string columnName = "BINARYTYPE";
            using TemporaryTable table = NewTemporaryTable(Statement, string.Format("{0} {1}", columnName, "BINARY"));
            string formattedValue = $"X'{BitConverter.ToString(value).Replace("-", "")}'";
            await ValidateInsertSelectDeleteSingleValue(
                table.TableName,
                columnName,
                value,
                formattedValue);
        }

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
            using TemporaryTable table = NewTemporaryTable(Statement, string.Format("{0} {1}", columnName, "BOOLEAN"));
            string? formattedValue =  value == null ? null : QuoteValue($"{value?.ToString(CultureInfo.InvariantCulture)}");
            await ValidateInsertSelectDeleteSingleValue(
                table.TableName,
                columnName,
                value,
                formattedValue);
        }

        /// <summary>
        /// Validates if driver can receive specific NULL values correctly.
        /// </summary>
        [SkippableTheory]
        [InlineData("NULL")]
        [InlineData("CAST(NULL AS INT)")]
        [InlineData("CAST(NULL AS BIGINT)")]
        [InlineData("CAST(NULL AS SMALLINT)")]
        [InlineData("CAST(NULL AS TINYINT)")]
        [InlineData("CAST(NULL AS FLOAT)")]
        [InlineData("CAST(NULL AS DOUBLE)")]
        [InlineData("CAST(NULL AS DECIMAL(38,0))")]
        [InlineData("CAST(NULL AS STRING)")]
        [InlineData("CAST(NULL AS VARCHAR(10))")]
        [InlineData("CAST(NULL AS CHAR(10))")]
        [InlineData("CAST(NULL AS BOOLEAN)")]
        // TODO: Returns byte[] [] (i.e., empty array) - expecting null value.
        //[InlineData("CAST(NULL AS BINARY)", Skip = "Returns empty array - expecting null value.")]
        public async Task TestNullData(string projectionClause)
        {
            string selectStatement = $"SELECT {projectionClause};";
            // Note: by default, this returns as String type, not NULL type.
            await SelectAndValidateValues(selectStatement, null, 1);
        }
    }
}
