﻿/*
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

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Impala
{
    public class NumericValueTests : Common.NumericValueTests<ApacheTestConfiguration, ImpalaTestEnvironment>
    {
        public NumericValueTests(ITestOutputHelper output)
            : base(output, new ImpalaTestEnvironment.Factory())
        {
        }

        [SkippableTheory]
        [InlineData(0)]
        [InlineData(0.2)]
        [InlineData(15e-03)]
        [InlineData(1.234E+2)]
        [InlineData(double.NegativeInfinity)]
        [InlineData(double.PositiveInfinity)]
        public override Task TestDoubleValuesInsertSelectDelete(double value)
        {
            return base.TestDoubleValuesInsertSelectDelete(value);
        }

        [SkippableTheory]
        [InlineData(0)]
        [InlineData(25)]
        [InlineData(float.NegativeInfinity)]
        [InlineData(float.PositiveInfinity)]
        // TODO: Solve server issue when non-integer float value is used in where clause.
        //[InlineData(25.1)]
        //[InlineData(0.2)]
        //[InlineData(15e-03)]
        //[InlineData(1.234E+2)]
        //[InlineData(float.MinValue)]
        //[InlineData(float.MaxValue)]
        public override Task TestFloatValuesInsertSelectDelete(float value)
        {
            return base.TestFloatValuesInsertSelectDelete(value);
        }
    }
}
