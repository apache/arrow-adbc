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
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class HiveServer2ParametersTest
    {
        [SkippableTheory]
        [MemberData(nameof(GetParametersTestData))]
        public void TestParametersParse(string? dataTypeConversion, IReadOnlyCollection<HiveServer2DataTypeConversion> expected)
        {
            Assert.Equal(expected, HiveServer2DataTypeConversionConstants.Parse(dataTypeConversion));
        }

        public static IEnumerable<object?[]> GetParametersTestData()
        {
            // Default
            yield return new object?[] { null, new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.Scalar } };
            yield return new object?[] { "", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.Scalar } };
            yield return new object?[] { ",", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.Scalar } };
            // Explicit
            yield return new object?[] { $"scalar", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.Scalar } };
            yield return new object?[] { $"none", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.None } };
            // Ignore "empty"
            yield return new object?[] { $"scalar,", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.Scalar } };
            yield return new object?[] { $",scalar,", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.Scalar } };
            // Combined, embedded space, mixed-case
            yield return new object?[] { $"none,scalar", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.None, HiveServer2DataTypeConversion.Scalar, } };
            yield return new object?[] { $" nOnE, scAlAr ", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.None, HiveServer2DataTypeConversion.Scalar, } };
            yield return new object?[] { $", none, scalar, ", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.None, HiveServer2DataTypeConversion.Scalar, } };
            yield return new object?[] { $"scalar,none", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.None, HiveServer2DataTypeConversion.Scalar, } };
            // Invalid options
            yield return new object?[] { $"xxx", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.Invalid, } };
            yield return new object?[] { $"none,scalar,xxx", new List<HiveServer2DataTypeConversion>() { HiveServer2DataTypeConversion.Invalid, HiveServer2DataTypeConversion.None, HiveServer2DataTypeConversion.Scalar } };
        }
    }
}
