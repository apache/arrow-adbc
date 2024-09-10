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
            var result = new HashSet<HiveServer2DataTypeConversion>();
            if (dataTypeConversion == null) {
                return result;
            }
            string[] conversions = dataTypeConversion.Split(',');
            foreach (string? conversion in conversions)
            {
                HiveServer2DataTypeConversion dataTypeConversionValue = (conversion?.Trim().ToLowerInvariant()) switch
                {
                    null or "" => HiveServer2DataTypeConversion.Empty,
                    Scalar => HiveServer2DataTypeConversion.Scalar,
                    None => HiveServer2DataTypeConversion.None,
                    _ => HiveServer2DataTypeConversion.Invalid,
                };

                if (!result.Contains(dataTypeConversionValue)) result.Add(dataTypeConversionValue);
            }

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
