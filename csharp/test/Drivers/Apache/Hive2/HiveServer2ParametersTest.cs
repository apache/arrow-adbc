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
        [MemberData(nameof(GetParametersDataTypeConvTestData))]
        internal void TestParametersDataTypeConvParse(string? dataTypeConversion, DataTypeConversion expected, Type? exceptionType = default)
        {
            if (exceptionType == default)
                Assert.Equal(expected, DataTypeConversionParser.Parse(dataTypeConversion));
            else
                Assert.Throws(exceptionType, () => DataTypeConversionParser.Parse(dataTypeConversion));
        }

        [SkippableTheory]
        [MemberData(nameof(GetParametersTlsOptionTestData))]
        internal void TestParametersTlsOptionParse(string? tlsOptions, HiveServer2TlsOption expected, Type? exceptionType = default)
        {
            if (exceptionType == default)
                Assert.Equal(expected, TlsOptionsParser.Parse(tlsOptions));
            else
                Assert.Throws(exceptionType, () => TlsOptionsParser.Parse(tlsOptions));
        }

        public static IEnumerable<object?[]> GetParametersDataTypeConvTestData()
        {
            // Default
            yield return new object?[] { null, DataTypeConversion.Scalar };
            yield return new object?[] { "", DataTypeConversion.Scalar };
            yield return new object?[] { ",", DataTypeConversion.Scalar };
            // Explicit
            yield return new object?[] { $"scalar", DataTypeConversion.Scalar };
            yield return new object?[] { $"none", DataTypeConversion.None };
            // Ignore "empty", embedded space, mixed-case
            yield return new object?[] { $"scalar,", DataTypeConversion.Scalar };
            yield return new object?[] { $",scalar,", DataTypeConversion.Scalar };
            yield return new object?[] { $",scAlAr,", DataTypeConversion.Scalar };
            yield return new object?[] { $"scAlAr", DataTypeConversion.Scalar };
            yield return new object?[] { $" scalar ", DataTypeConversion.Scalar };
            // Combined - conflicting
            yield return new object?[] { $"none,scalar", DataTypeConversion.None | DataTypeConversion.Scalar, typeof(ArgumentOutOfRangeException) };
            yield return new object?[] { $" nOnE, scAlAr ", DataTypeConversion.None | DataTypeConversion.Scalar, typeof(ArgumentOutOfRangeException) };
            yield return new object?[] { $", none, scalar, ", DataTypeConversion.None | DataTypeConversion.Scalar , typeof(ArgumentOutOfRangeException) };
            yield return new object?[] { $"scalar,none", DataTypeConversion.None | DataTypeConversion.Scalar , typeof(ArgumentOutOfRangeException) };
            // Invalid options
            yield return new object?[] { $"xxx", DataTypeConversion.Empty, typeof(ArgumentOutOfRangeException) };
            yield return new object?[] { $"none,scalar,xxx", DataTypeConversion.None | DataTypeConversion.Scalar, typeof(ArgumentOutOfRangeException)  };
        }

        public static IEnumerable<object?[]> GetParametersTlsOptionTestData()
        {
            // Default
            yield return new object?[] { null, HiveServer2TlsOption.Empty };
            yield return new object?[] { "", HiveServer2TlsOption.Empty};
            yield return new object?[] { " ", HiveServer2TlsOption.Empty };
            // Explicit
            yield return new object?[] { $"{TlsOptions.AllowSelfSigned}", HiveServer2TlsOption.AllowSelfSigned };
            yield return new object?[] { $"{TlsOptions.AllowHostnameMismatch}", HiveServer2TlsOption.AllowHostnameMismatch };
            // Ignore empty
            yield return new object?[] { $",{TlsOptions.AllowSelfSigned}", HiveServer2TlsOption.AllowSelfSigned };
            yield return new object?[] { $",{TlsOptions.AllowHostnameMismatch},", HiveServer2TlsOption.AllowHostnameMismatch };
            // Combined, embedded space, mixed-case
            yield return new object?[] { $"{TlsOptions.AllowSelfSigned},{TlsOptions.AllowHostnameMismatch}", HiveServer2TlsOption.AllowSelfSigned | HiveServer2TlsOption.AllowHostnameMismatch };
            yield return new object?[] { $"{TlsOptions.AllowHostnameMismatch},{TlsOptions.AllowSelfSigned}", HiveServer2TlsOption.AllowSelfSigned  | HiveServer2TlsOption.AllowHostnameMismatch };
            yield return new object?[] { $" {TlsOptions.AllowHostnameMismatch} , {TlsOptions.AllowSelfSigned} ", HiveServer2TlsOption.AllowSelfSigned | HiveServer2TlsOption.AllowHostnameMismatch };
            yield return new object?[] { $"{TlsOptions.AllowSelfSigned.ToUpperInvariant()},{TlsOptions.AllowHostnameMismatch.ToUpperInvariant()}", HiveServer2TlsOption.AllowSelfSigned | HiveServer2TlsOption.AllowHostnameMismatch };
            // Invalid
            yield return new object?[] { $"xxx,{TlsOptions.AllowSelfSigned.ToUpperInvariant()},{TlsOptions.AllowHostnameMismatch.ToUpperInvariant()}", HiveServer2TlsOption.Empty, typeof(ArgumentOutOfRangeException) };
        }
    }
}
