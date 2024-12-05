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
using System.Text.RegularExpressions;

namespace Apache.Arrow.Adbc.Client
{
    internal class ConnectionStringKeywords
    {
        public const string ConnectionTimeout = "adbcconnectiontimeout";
        public const string CommandTimeout = "adbccommandtimeout";
        public const string StructBehavior = "structbehavior";
        public const string DecimalBehavior = "decimalbehavior";
    }

    internal class ConnectionStringParser
    {
        public static TimeoutValue ParseTimeoutValue(string value)
        {
            string pattern = @"\(([^,]+),\s*([^,]+),\s*([^,]+)\)";

            // Match the regex
            Match match = Regex.Match(value, pattern);

            if (match.Success)
            {
                string driverPropertyName = match.Groups[1].Value.Trim();
                string timeoutAsString = match.Groups[2].Value.Trim();
                string units = match.Groups[3].Value.Trim();

                if (units != "s" && units != "ms")
                {
                    throw new InvalidOperationException("invalid units");
                }

                TimeoutValue timeoutValue = new TimeoutValue
                {
                    DriverPropertyName = driverPropertyName,
                    Value = int.Parse(timeoutAsString),
                    Units = units
                };

                return timeoutValue;
            }
            else
            {
                throw new ArgumentOutOfRangeException(nameof(value));
            }
        }
    }

    internal class TimeoutValue
    {
        public string DriverPropertyName { get; set; } = string.Empty;

        public int Value { get; set; }

        // seconds=s
        // milliseconds=ms
        /// <remarks>
        /// While these can be helpful, the DbConnection and DbCommand
        /// objects limit the use of these.
        /// </remarks>
        public string Units { get; set; } = string.Empty;
    }
}
