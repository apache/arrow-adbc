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
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    public class StringValueTests(ITestOutputHelper output)
        : StringValueTests<DatabricksTestConfiguration, DatabricksTestEnvironment>(output, new DatabricksTestEnvironment.Factory())
    {
        [SkippableTheory]
        [InlineData("String contains formatting characters tab\t, newline\n, carriage return\r.")]
        internal async Task TestStringDataDatabricks(string? value)
        {
            await TestStringData(value);
        }

        [SkippableTheory]
        [InlineData("String contains formatting characters tab\t, newline\n, carriage return\r.")]
        internal async Task TestVarcharDataDatabricks(string? value)
        {
            await TestVarcharData(value);
        }

        [SkippableTheory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" Leading and trailing spaces ")]
        [InlineData("你好")]
        internal override async Task TestCharData(string? value)
        {
            await base.TestCharData(value);
        }

        [SkippableTheory]
        [InlineData("String contains formatting characters tab\t, newline\n, carriage return\r.")]
        internal async Task TestCharDataDatabricks(string? value)
        {
            await base.TestCharData(value);
        }

        protected override async Task TestVarcharExceptionData(string value, string[] expectedTexts, string? expectedSqlState)
        {
            await base.TestVarcharExceptionData(value, expectedTexts, expectedSqlState);
        }

        [SkippableTheory]
        [InlineData("String whose length is too long for VARCHAR(10).", new string[] { "DELTA_EXCEED_CHAR_VARCHAR_LIMIT" }, "22001")]
        public async Task TestVarcharExceptionDataDatabricks(string value, string[] expectedTexts, string? expectedSqlState)
        {
            await base.TestVarcharExceptionData(value, expectedTexts, expectedSqlState);
        }
    }
}
