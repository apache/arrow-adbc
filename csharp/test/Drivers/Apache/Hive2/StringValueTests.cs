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
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class StringValueTests(ITestOutputHelper output)
        : Common.StringValueTests<ApacheTestConfiguration, HiveServer2TestEnvironment>(output, new HiveServer2TestEnvironment.Factory())
    {
        [SkippableTheory]
        [InlineData("String whose length is too long for VARCHAR(10).", new string[] { "Error while compiling statement", "ParseException", "extraneous input 'who' expecting EOF near '<EOF>'" }, "42000")]
        protected async Task TestVarcharExceptionDataImpala(string value, string[] expectedTexts, string? expectedSqlState)
        {
            await base.TestVarcharExceptionData(value, expectedTexts, expectedSqlState);
        }

        [SkippableTheory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData(" Leading and trailing spaces ")]
        internal override Task TestCharData(string? value)
        {
            return base.TestCharData(value);
        }
    }
}
