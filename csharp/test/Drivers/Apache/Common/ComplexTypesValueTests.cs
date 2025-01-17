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

using System.Threading.Tasks;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Common
{
    // TODO: When supported, use prepared statements instead of SQL string literals
    //      Which will better test how the driver handles values sent/received

    /// <summary>
    /// Validates that specific complex structured types can be inserted, retrieved and targeted correctly
    /// </summary>
    public abstract class ComplexTypesValueTests<TConfig, TEnv> : TestBase<TConfig, TEnv>
        where TConfig : TestConfiguration
        where TEnv : HiveServer2TestEnvironment<TConfig>
    {
        public ComplexTypesValueTests(ITestOutputHelper output, TestEnvironment<TConfig>.Factory<TEnv> testEnvFactory)
            : base(output, testEnvFactory) { }

        /// <summary>
        /// Validates if driver can send and receive specific array of integer values correctly.
        /// </summary>
        [SkippableTheory]
        [InlineData("ARRAY(CAST(1 AS INT), 2, 3)", "[1,2,3]")]
        [InlineData("ARRAY(CAST(1 AS LONG), 2, 3)", "[1,2,3]")]
        [InlineData("ARRAY(CAST(1 AS DOUBLE), 2, 3)", "[1.0,2.0,3.0]")]
        [InlineData("ARRAY(CAST(1 AS NUMERIC(38,0)), 2, 3)", "[1,2,3]")]
        [InlineData("ARRAY(CAST('John Doe' AS STRING), 2, 3)", """["John Doe","2","3"]""")]
        // Note: Timestamp returned adjusted to UTC.
        [InlineData("ARRAY(CAST('2024-01-01T00:00:00-07:00' AS TIMESTAMP), CAST('2024-02-02T02:02:02+01:30' AS TIMESTAMP), CAST('2024-03-03T03:03:03Z' AS TIMESTAMP))", """[2024-01-01 07:00:00,2024-02-02 00:32:02,2024-03-03 03:03:03]""")]
        [InlineData("ARRAY(CAST('2024-01-01T00:00:00Z' AS DATE), CAST('2024-02-02T02:02:02Z' AS DATE), CAST('2024-03-03T03:03:03Z' AS DATE))", """[2024-01-01,2024-02-02,2024-03-03]""")]
        [InlineData("ARRAY(INTERVAL 123 YEARS 11 MONTHS, INTERVAL 5 YEARS, INTERVAL 6 MONTHS)", """[123-11,5-0,0-6]""")]
        public async Task TestArrayData(string projection, string value)
        {
            string selectStatement = $"SELECT {projection};";
            await SelectAndValidateValuesAsync(selectStatement, value, 1);
        }

        /// <summary>
        /// Validates if driver can send and receive specific map values correctly.
        /// </summary>
        [SkippableTheory]
        [InlineData("MAP(1, 'John Doe', 2, 'Jane Doe', 3, 'Jack Doe')", """{1:"John Doe",2:"Jane Doe",3:"Jack Doe"}""")]
        [InlineData("MAP('John Doe', 1, 'Jane Doe', 2, 'Jack Doe', 3)", """{"Jack Doe":3,"Jane Doe":2,"John Doe":1}""")]
        public async Task TestMapData(string projection, string value)
        {
            string selectStatement = $"SELECT {projection};";
            await SelectAndValidateValuesAsync(selectStatement, value, 1);
        }

        /// <summary>
        /// Validates if driver can send and receive specific map values correctly.
        /// </summary>
        [SkippableTheory]
        [InlineData("STRUCT(CAST(1 AS INT), CAST('John Doe' AS STRING))", """{"col1":1,"col2":"John Doe"}""")]
        [InlineData("STRUCT(CAST('John Doe' AS STRING), CAST(1 AS INT))", """{"col1":"John Doe","col2":1}""")]
        public async Task TestStructData(string projection, string value)
        {
            string selectStatement = $"SELECT {projection};";
            await SelectAndValidateValuesAsync(selectStatement, value, 1);
        }
    }
}
