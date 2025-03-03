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

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal class HiveServer2ConnectionFactory
    {
        public static HiveServer2Connection NewConnection(IReadOnlyDictionary<string, string> properties)
        {
            if (!properties.TryGetValue(HiveServer2Parameters.TransportType, out string? type))
            {

                throw new ArgumentException($"Required property '{HiveServer2Parameters.TransportType}' is missing. Supported types: {HiveServer2TransportTypeParser.SupportedList}", nameof(properties));
            }
            if (!HiveServer2TransportTypeParser.TryParse(type, out HiveServer2TransportType typeValue))
            {
                throw new ArgumentOutOfRangeException(nameof(properties), $"Unsupported or unknown value '{type}' given for property '{HiveServer2Parameters.TransportType}'. Supported types: {HiveServer2TransportTypeParser.SupportedList}");
            }
            return new HiveServer2HttpConnection(properties);
        }
    }
}
