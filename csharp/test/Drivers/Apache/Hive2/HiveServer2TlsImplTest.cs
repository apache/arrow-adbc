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
using System.IO;
using System.Security.Cryptography.X509Certificates;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class HiveServer2TlsImplTest
    {
        [SkippableTheory]
        [MemberData(nameof(GetHttpTlsOptionsTestData))]
        internal void TestValidateHttpTlsOptions(Dictionary<string, string>? dataTypeConversion, TlsProperties expected, Type? exceptionType = default)
        {
            if (exceptionType == default)
                Assert.Equivalent(expected, HiveServer2TlsImpl.GetHttpTlsOptions(dataTypeConversion ?? new Dictionary<string, string>()));
            else
                Assert.Throws(exceptionType, () => HiveServer2TlsImpl.GetHttpTlsOptions(dataTypeConversion ?? new Dictionary<string, string>()));
        }

        [SkippableTheory]
        [MemberData(nameof(GetStandardTlsOptionsTestData))]
        internal void TestValidateStandardTlsOptions(Dictionary<string, string>? dataTypeConversion, TlsProperties expected, Type? exceptionType = default)
        {
            if (exceptionType == default)
                Assert.Equivalent(expected, HiveServer2TlsImpl.GetStandardTlsOptions(dataTypeConversion ?? new Dictionary<string, string>()));
            else
                Assert.Throws(exceptionType, () => HiveServer2TlsImpl.GetStandardTlsOptions(dataTypeConversion ?? new Dictionary<string, string>()));
        }

        public static IEnumerable<object?[]> GetHttpTlsOptionsTestData()
        {
            // Tls is enabled by default
            yield return new object?[] { new Dictionary<string, string> {  }, new TlsProperties { IsTlsEnabled = true } };
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.IsTlsEnabled, "abc" } }, new TlsProperties { IsTlsEnabled = true } };

            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.IsTlsEnabled, "False" } }, new TlsProperties { IsTlsEnabled = false } };
            yield return new object?[] { new Dictionary<string, string> { { AdbcOptions.Uri, "https://arrow.apache.org" } }, new TlsProperties { IsTlsEnabled = true, DisableServerCertificateValidation = false } };
            // uri takes precedence over ssl option
            yield return new object?[] { new Dictionary<string, string> { { AdbcOptions.Uri, "https://arrow.apache.org" }, { HttpTlsOptions.IsTlsEnabled, "False" } }, new TlsProperties { IsTlsEnabled = true, DisableServerCertificateValidation = false } };
            // other ssl options are ignored if disableServerCertificateValidation is set to true
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.IsTlsEnabled, "True" }, { HttpTlsOptions.DisableServerCertificateValidation, "True" }, { HttpTlsOptions.AllowSelfSigned, "True" }, { HttpTlsOptions.AllowHostnameMismatch, "True" } }, new TlsProperties { IsTlsEnabled = true, DisableServerCertificateValidation = true } };
            // other ssl options are ignored if ssl is disabled
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.IsTlsEnabled, "False" }, { HttpTlsOptions.AllowSelfSigned, "True" }, { HttpTlsOptions.AllowHostnameMismatch, "True" } }, new TlsProperties { IsTlsEnabled = false } };
            // case insensitive boolean string parsing
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.IsTlsEnabled, "false" } }, new TlsProperties { IsTlsEnabled = false } };
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.IsTlsEnabled, "True" } }, new TlsProperties { IsTlsEnabled = true, DisableServerCertificateValidation = false, AllowSelfSigned = false, AllowHostnameMismatch = false } };
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.IsTlsEnabled, "tRUe" }, { HttpTlsOptions.AllowSelfSigned, "true" } }, new TlsProperties { IsTlsEnabled = true, DisableServerCertificateValidation = false, AllowSelfSigned = true, AllowHostnameMismatch = false } };
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.IsTlsEnabled, "TruE" }, { HttpTlsOptions.AllowSelfSigned, "True" }, { HttpTlsOptions.AllowHostnameMismatch, "True" } }, new TlsProperties { IsTlsEnabled = true, DisableServerCertificateValidation = false, AllowSelfSigned = true, AllowHostnameMismatch = true } };
            // invalid certificate path
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.IsTlsEnabled, "True" }, { HttpTlsOptions.AllowSelfSigned, "True" }, { HttpTlsOptions.AllowHostnameMismatch, "True" }, { HttpTlsOptions.TrustedCertificatePath, "" } }, null, typeof(FileNotFoundException) };

            // RevocationMode tests - default is Online
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.IsTlsEnabled, "True" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
            // RevocationMode = online
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.RevocationMode, "online" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
            // RevocationMode = offline
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.RevocationMode, "offline" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Offline } };
            // RevocationMode = nocheck
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.RevocationMode, "nocheck" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.NoCheck } };
            // RevocationMode case insensitive
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.RevocationMode, "ONLINE" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.RevocationMode, "OffLine" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Offline } };
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.RevocationMode, "NoCheck" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.NoCheck } };
            // RevocationMode with whitespace
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.RevocationMode, " online " } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
            // RevocationMode invalid value defaults to Online
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.RevocationMode, "invalid" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
            yield return new object?[] { new Dictionary<string, string> { { HttpTlsOptions.RevocationMode, "" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
        }

        public static IEnumerable<object?[]> GetStandardTlsOptionsTestData()
        {
            // Tls is enabled by default
            yield return new object?[] { new Dictionary<string, string> { }, new TlsProperties { IsTlsEnabled = true } };
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.IsTlsEnabled, "abc" } }, new TlsProperties { IsTlsEnabled = true } };

            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.IsTlsEnabled, "False" } }, new TlsProperties { IsTlsEnabled = false } };
            // other ssl options are ignored if disableServerCertificateValidation is set to true
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.IsTlsEnabled, "True" }, { StandardTlsOptions.DisableServerCertificateValidation, "True" }, { StandardTlsOptions.AllowSelfSigned, "True" }, { StandardTlsOptions.AllowHostnameMismatch, "True" } }, new TlsProperties { IsTlsEnabled = true, DisableServerCertificateValidation = true } };
            // other ssl options are ignored if ssl is disabled
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.IsTlsEnabled, "False" }, { StandardTlsOptions.AllowSelfSigned, "True" }, { StandardTlsOptions.AllowHostnameMismatch, "True" } }, new TlsProperties { IsTlsEnabled = false } };
            // case insensitive boolean string parsing
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.IsTlsEnabled, "false" } }, new TlsProperties { IsTlsEnabled = false } };
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.IsTlsEnabled, "True" } }, new TlsProperties { IsTlsEnabled = true, DisableServerCertificateValidation = false, AllowSelfSigned = false, AllowHostnameMismatch = false } };
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.IsTlsEnabled, "tRUe" }, { StandardTlsOptions.AllowSelfSigned, "true" } }, new TlsProperties { IsTlsEnabled = true, DisableServerCertificateValidation = false, AllowSelfSigned = true, AllowHostnameMismatch = false } };
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.IsTlsEnabled, "TruE" }, { StandardTlsOptions.AllowSelfSigned, "True" }, { StandardTlsOptions.AllowHostnameMismatch, "True" } }, new TlsProperties { IsTlsEnabled = true, DisableServerCertificateValidation = false, AllowSelfSigned = true, AllowHostnameMismatch = true } };
            // invalid certificate path
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.IsTlsEnabled, "True" }, { StandardTlsOptions.AllowSelfSigned, "True" }, { StandardTlsOptions.AllowHostnameMismatch, "True" }, { StandardTlsOptions.TrustedCertificatePath, "" } }, null, typeof(FileNotFoundException) };

            // RevocationMode tests - default is Online
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.IsTlsEnabled, "True" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
            // RevocationMode = online
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.RevocationMode, "online" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
            // RevocationMode = offline
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.RevocationMode, "offline" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Offline } };
            // RevocationMode = nocheck
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.RevocationMode, "nocheck" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.NoCheck } };
            // RevocationMode case insensitive
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.RevocationMode, "ONLINE" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.RevocationMode, "OffLine" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Offline } };
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.RevocationMode, "NoCheck" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.NoCheck } };
            // RevocationMode with whitespace
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.RevocationMode, " online " } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
            // RevocationMode invalid value defaults to Online
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.RevocationMode, "invalid" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
            yield return new object?[] { new Dictionary<string, string> { { StandardTlsOptions.RevocationMode, "" } }, new TlsProperties { IsTlsEnabled = true, RevocationMode = X509RevocationMode.Online } };
        }
    }
}
