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

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class DateTimeValueTests : Common.DateTimeValueTests<ApacheTestConfiguration, HiveServer2TestEnvironment>
    {
        public DateTimeValueTests(ITestOutputHelper output)
            : base(output, new HiveServer2TestEnvironment.Factory())
        { }

        protected override string GetFormattedTimestampValue(string value)
        {
            return "TO_TIMESTAMP(" + QuoteValue(value) + ")";
        }
    }
}
