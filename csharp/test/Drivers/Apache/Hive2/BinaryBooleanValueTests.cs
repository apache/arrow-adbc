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

using System.Text;
using System.Threading.Tasks;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class BinaryBooleanValueTests : Common.BinaryBooleanValueTests<ApacheTestConfiguration, HiveServer2TestEnvironment>
    {
        public BinaryBooleanValueTests(ITestOutputHelper output)
            : base(output, new HiveServer2TestEnvironment.Factory())
        {
        }

        [SkippableTheory]
        [InlineData(null)]
        [MemberData(nameof(AsciiArrayData), 0)]
        [MemberData(nameof(AsciiArrayData), 2)]
        [MemberData(nameof(AsciiArrayData), 1024)]
        public override Task TestBinaryData(byte[]? value)
        {
            return base.TestBinaryData(value);
        }

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
        [InlineData("CAST(NULL AS BINARY)")]
        public override Task TestNullData(string projectionClause)
        {
            return base.TestNullData(projectionClause);
        }

        protected override string? GetFormattedBinaryValue(byte[]? value)
        {
            return value != null ? $"CAST ('{Encoding.UTF8.GetString(value)}' as BINARY)" : null;
        }
    }
}
