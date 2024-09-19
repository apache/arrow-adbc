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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class HiveServer2ParametersTest
    {
        [SkippableTheory]
        [MemberData(nameof(GetParametersTestData))]
        public void TestParametersParse(string? dataTypeConversion, HiveServer2DataTypeConversion expected, Type? excptionType = default)
        {
            if (excptionType == default)
                Assert.Equal(expected, HiveServer2DataTypeConversionConstants.Parse(dataTypeConversion));
            else
                Assert.Throws(excptionType, () => HiveServer2DataTypeConversionConstants.Parse(dataTypeConversion));
        }

        public static IEnumerable<object?[]> GetParametersTestData()
        {
            // Default
            yield return new object?[] { null, HiveServer2DataTypeConversion.Scalar };
            yield return new object?[] { "", HiveServer2DataTypeConversion.Scalar };
            yield return new object?[] { ",", HiveServer2DataTypeConversion.Scalar };
            // Explicit
            yield return new object?[] { $"scalar", HiveServer2DataTypeConversion.Scalar };
            yield return new object?[] { $"none", HiveServer2DataTypeConversion.None };
            // Ignore "empty", embedded space, mixed-case
            yield return new object?[] { $"scalar,", HiveServer2DataTypeConversion.Scalar };
            yield return new object?[] { $",scalar,", HiveServer2DataTypeConversion.Scalar };
            yield return new object?[] { $",scAlAr,", HiveServer2DataTypeConversion.Scalar };
            yield return new object?[] { $"scAlAr", HiveServer2DataTypeConversion.Scalar };
            yield return new object?[] { $" scalar ", HiveServer2DataTypeConversion.Scalar };
            // Combined - conflicting
            yield return new object?[] { $"none,scalar", HiveServer2DataTypeConversion.None | HiveServer2DataTypeConversion.Scalar, typeof(ArgumentOutOfRangeException) };
            yield return new object?[] { $" nOnE, scAlAr ", HiveServer2DataTypeConversion.None | HiveServer2DataTypeConversion.Scalar, typeof(ArgumentOutOfRangeException) };
            yield return new object?[] { $", none, scalar, ", HiveServer2DataTypeConversion.None | HiveServer2DataTypeConversion.Scalar , typeof(ArgumentOutOfRangeException) };
            yield return new object?[] { $"scalar,none", HiveServer2DataTypeConversion.None | HiveServer2DataTypeConversion.Scalar , typeof(ArgumentOutOfRangeException) };
            // Invalid options
            yield return new object?[] { $"xxx", HiveServer2DataTypeConversion.Empty, typeof(ArgumentOutOfRangeException) };
            yield return new object?[] { $"none,scalar,xxx", HiveServer2DataTypeConversion.None | HiveServer2DataTypeConversion.Scalar, typeof(ArgumentOutOfRangeException)  };
        }
    }
}
