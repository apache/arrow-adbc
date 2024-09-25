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

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    [Flags]
    internal enum HiveServer2TlsOption
    {
        Empty = 0,
        AllowInvalidCertificate = 1,
        AllowInvalidHostnames = 2,
    }

    internal static class TlsOptionsParser
    {

        internal const string SupportedList = TlsOptions.AllowInvalidCertificate + "," + TlsOptions.AllowInvalidHostnames;

        internal static HiveServer2TlsOption Parse(string? tlsOptions)
        {
            HiveServer2TlsOption options = HiveServer2TlsOption.Empty;
            if (tlsOptions == null) return options;

            string[] valueList = tlsOptions.Split(',');
            foreach (string tlsOption in valueList)
            {
                options |= (tlsOption?.Trim().ToLowerInvariant()) switch
                {
                    null or "" => HiveServer2TlsOption.Empty,
                    TlsOptions.AllowInvalidCertificate => HiveServer2TlsOption.AllowInvalidCertificate,
                    TlsOptions.AllowInvalidHostnames => HiveServer2TlsOption.AllowInvalidHostnames,
                    _ => throw new ArgumentOutOfRangeException(nameof(tlsOptions), tlsOption, "Invalid or unsupported TLS option"),
                };
            }
            return options;
        }
    }
}
