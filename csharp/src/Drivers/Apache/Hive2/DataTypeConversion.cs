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
    internal enum DataTypeConversion
    {
        Empty = 0,
        None = 1,
        Scalar = 2,
    }

    internal static class DataTypeConversionParser
    {
        internal const string SupportedList = DataTypeConversionOptions.None + ", " + DataTypeConversionOptions.Scalar;

        internal static DataTypeConversion Parse(string? dataTypeConversion)
        {
            DataTypeConversion result = DataTypeConversion.Empty;

            if (string.IsNullOrWhiteSpace(dataTypeConversion))
            {
                // Default
                return DataTypeConversion.Scalar;
            }

            string[] conversions = dataTypeConversion!.Split(',');
            foreach (string? conversion in conversions)
            {
                result |= (conversion?.Trim().ToLowerInvariant()) switch
                {
                    null or "" => DataTypeConversion.Empty,
                    DataTypeConversionOptions.None => DataTypeConversion.None,
                    DataTypeConversionOptions.Scalar => DataTypeConversion.Scalar,
                    _ => throw new ArgumentOutOfRangeException(nameof(dataTypeConversion), conversion, "Invalid or unsupported data type conversion"),
                };
            }

            if (result.HasFlag(DataTypeConversion.None) && result.HasFlag(DataTypeConversion.Scalar))
            {
                throw new ArgumentOutOfRangeException(nameof(dataTypeConversion), dataTypeConversion, "Conflicting data type conversion options");
            }
            // Default
            if (result == DataTypeConversion.Empty) result = DataTypeConversion.Scalar;

            return result;
        }
    }
}
