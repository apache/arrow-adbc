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

using System.Collections.Generic;

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    public static class HiveServer2DataTypeConversionConstants
    {
        public const string None = "none";
        public const string Scalar = "scalar";
        public const string SupportedList = None + ", " + Scalar;

        public static IReadOnlyCollection<HiveServer2DataTypeConversion> Parse(string? dataTypeConversion)
        {
            // Using a sorted set to make testing repeatable.
            var result = new SortedSet<HiveServer2DataTypeConversion>();
            if (dataTypeConversion == null) {
                // Default
                result.Add(HiveServer2DataTypeConversion.Scalar);
                return result;
            }

            string[] conversions = dataTypeConversion.Split(',');
            foreach (string? conversion in conversions)
            {
                HiveServer2DataTypeConversion dataTypeConversionValue = (conversion?.Trim().ToLowerInvariant()) switch
                {
                    null or "" => HiveServer2DataTypeConversion.Empty,
                    None => HiveServer2DataTypeConversion.None,
                    Scalar => HiveServer2DataTypeConversion.Scalar,
                    _ => HiveServer2DataTypeConversion.Invalid,
                };

                if (!result.Contains(dataTypeConversionValue) && dataTypeConversionValue != HiveServer2DataTypeConversion.Empty) result.Add(dataTypeConversionValue);
            }

            // Default
            if (result.Count == 0) result.Add(HiveServer2DataTypeConversion.Scalar);

            return result;
        }
    }

    public enum HiveServer2DataTypeConversion
    {
        Invalid = 0,
        None,
        Scalar,
        Empty = int.MaxValue,
    }
}
