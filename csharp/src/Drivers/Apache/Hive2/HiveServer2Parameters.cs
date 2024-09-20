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
    public static class HiveServer2DataTypeConversionConstants
    {
        public const string None = "none";
        public const string Scalar = "scalar";
        public const string SupportedList = None + ", " + Scalar;

        public static HiveServer2DataTypeConversion Parse(string? dataTypeConversion)
        {
            HiveServer2DataTypeConversion result = HiveServer2DataTypeConversion.Empty;

            if (string.IsNullOrWhiteSpace(dataTypeConversion)) {
                // Default
                return HiveServer2DataTypeConversion.Scalar;
            }

            string[] conversions = dataTypeConversion!.Split(',');
            foreach (string? conversion in conversions)
            {
                result |= (conversion?.Trim().ToLowerInvariant()) switch
                {
                    null or "" => HiveServer2DataTypeConversion.Empty,
                    None => HiveServer2DataTypeConversion.None,
                    Scalar => HiveServer2DataTypeConversion.Scalar,
                    _ => throw new ArgumentOutOfRangeException(nameof(dataTypeConversion), conversion, "Invalid or unsupported data type conversion"),
                };
            }

            if (result.HasFlag(HiveServer2DataTypeConversion.None) && result.HasFlag(HiveServer2DataTypeConversion.Scalar))
            {
                throw new ArgumentOutOfRangeException(nameof(dataTypeConversion), dataTypeConversion, "Conflicting data type conversion options");
            }
            // Default
            if (result == HiveServer2DataTypeConversion.Empty) result = HiveServer2DataTypeConversion.Scalar;

            return result;
        }
    }

    [Flags]
    public enum HiveServer2DataTypeConversion
    {
        Empty = 0,
        None = 1,
        Scalar = 2,
    }

    public static class HiveServer2TlsOptionConstants
    {
        public const string AllowInvalidCertificate = "allowinvalidcertificate";
        public const string AllowInvalidHostnames = "allowinvalidhostnames";
        internal const string SupportedList = AllowInvalidCertificate + "," + AllowInvalidHostnames;

        public static HiveServer2TlsOption Parse(string? tlsOptions)
        {
            HiveServer2TlsOption options = HiveServer2TlsOption.Empty;
            if (tlsOptions == null) return options;

            string[] valueList = tlsOptions.Split(',');
            foreach (string tlsOption in valueList)
            {
                options |= (tlsOption?.Trim().ToLowerInvariant()) switch
                {
                    null or "" => HiveServer2TlsOption.Empty,
                    AllowInvalidCertificate => HiveServer2TlsOption.AllowInvalidCertificate,
                    AllowInvalidHostnames => HiveServer2TlsOption.AllowInvalidHostnames,
                    _ => throw new ArgumentOutOfRangeException(nameof(tlsOptions), tlsOption, "Invalid or unsupported TLS option"),
                };
            }
            return options;
        }
    }

    [Flags]
    public enum HiveServer2TlsOption
    {
        Empty = 0,
        AllowInvalidCertificate = 1,
        AllowInvalidHostnames = 2,
    }
}
