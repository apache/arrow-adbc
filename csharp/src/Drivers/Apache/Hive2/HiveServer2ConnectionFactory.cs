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
            bool _ = properties.TryGetValue(HiveServer2Parameters.Type, out string? type);
            bool __ = HiveServer2TypeParser.TryParse(type, out HiveServer2Type typeValue);
            return typeValue switch
            {
                HiveServer2Type.Http => new HiveServer2HttpConnection(properties),
                HiveServer2Type.Empty => throw new ArgumentException($"Required property '{HiveServer2Parameters.Type}' is missing. Supported types: {HiveServer2TypeParser.SupportedList}", nameof(properties)),
                _ => throw new ArgumentOutOfRangeException(nameof(properties), $"Unsupported or unknown value '{type}' given for property '{HiveServer2Parameters.Type}'. Supported types: {HiveServer2TypeParser.SupportedList}"),
            };
        }

    }
}
