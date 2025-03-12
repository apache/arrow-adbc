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

using System;
using System.Collections.Generic;
using System.IO;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class HiveServer2TlsImplTest
    {
        [SkippableTheory]
        [MemberData(nameof(GetSslOptionsTestData))]
        internal void TestValidateTlsOptions(Dictionary<string, string>? dataTypeConversion, TlsProperties expected, Type? exceptionType = default)
        {
            if (exceptionType == default)
                Assert.Equivalent(expected, HiveServer2TlsImpl.GetTlsOptions(dataTypeConversion ?? new Dictionary<string, string>()));
            else
                Assert.Throws(exceptionType, () => HiveServer2TlsImpl.GetTlsOptions(dataTypeConversion ?? new Dictionary<string, string>()));
        }

        public static IEnumerable<object?[]> GetSslOptionsTestData()
        {
            yield return new object?[] { new Dictionary<string, string> { { TlsOptions.IsSslEnabled, "False" } }, new TlsProperties { IsSslEnabled = false } };
            yield return new object?[] { new Dictionary<string, string> { { AdbcOptions.Uri, "https://arrow.apache.org" } }, new TlsProperties { IsSslEnabled = true, EnableServerCertificateValidation = true } };
            // uri takes precedence over ssl option
            yield return new object?[] { new Dictionary<string, string> { { AdbcOptions.Uri, "https://arrow.apache.org" }, { TlsOptions.IsSslEnabled, "False" } }, new TlsProperties { IsSslEnabled = true, EnableServerCertificateValidation = true } };
            // other ssl options are ignored if enableServerCertificateValidation is disabled
            yield return new object?[] { new Dictionary<string, string> { { TlsOptions.IsSslEnabled, "True" }, { TlsOptions.EnableServerCertificateValidation, "False" }, { TlsOptions.AllowSelfSigned, "True" }, { TlsOptions.AllowHostnameMismatch, "True" } }, new TlsProperties { IsSslEnabled = true, EnableServerCertificateValidation = false } };
            // other ssl options are ignored if ssl is disabled
            yield return new object?[] { new Dictionary<string, string> { { TlsOptions.IsSslEnabled, "False" }, { TlsOptions.AllowSelfSigned, "True" }, { TlsOptions.AllowHostnameMismatch, "True" } }, new TlsProperties { IsSslEnabled = false } };
            // case insensitive boolean string parsing
            yield return new object?[] { new Dictionary<string, string> { { TlsOptions.IsSslEnabled, "false" } }, new TlsProperties { IsSslEnabled = false } };
            yield return new object?[] { new Dictionary<string, string> { { TlsOptions.IsSslEnabled, "True" } }, new TlsProperties { IsSslEnabled = true, EnableServerCertificateValidation = true, AllowSelfSigned = false, AllowHostnameMismatch = false } };
            yield return new object?[] { new Dictionary<string, string> { { TlsOptions.IsSslEnabled, "tRUe" }, { TlsOptions.AllowSelfSigned, "true" } }, new TlsProperties { IsSslEnabled = true, EnableServerCertificateValidation = true, AllowSelfSigned = true, AllowHostnameMismatch = false } };
            yield return new object?[] { new Dictionary<string, string> { { TlsOptions.IsSslEnabled, "TruE" }, { TlsOptions.AllowSelfSigned, "True" }, { TlsOptions.AllowHostnameMismatch, "True" } }, new TlsProperties { IsSslEnabled = true, EnableServerCertificateValidation = true, AllowSelfSigned = true, AllowHostnameMismatch = true } };
            // certificate path is ignored if self signed is not allowed
            yield return new object?[] { new Dictionary<string, string> { { TlsOptions.IsSslEnabled, "True" }, { TlsOptions.AllowSelfSigned, "False" }, { TlsOptions.AllowHostnameMismatch, "True" }, { TlsOptions.TrustedCertificatePath, "" } }, new TlsProperties { IsSslEnabled = true, EnableServerCertificateValidation = true, AllowSelfSigned = false, AllowHostnameMismatch = true } };
            // invalid certificate path
            yield return new object?[] { new Dictionary<string, string> { { TlsOptions.IsSslEnabled, "True" }, { TlsOptions.AllowSelfSigned, "True" }, { TlsOptions.AllowHostnameMismatch, "True" }, { TlsOptions.TrustedCertificatePath, "" } }, null, typeof(FileNotFoundException) };
        }
    }
}
