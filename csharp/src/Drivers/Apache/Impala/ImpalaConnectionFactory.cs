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

namespace Apache.Arrow.Adbc.Drivers.Apache.Impala
{
    internal class ImpalaConnectionFactory
    {
        public static ImpalaConnection NewConnection(IReadOnlyDictionary<string, string> properties)
        {
            if (!properties.TryGetValue(ImpalaParameters.Type, out string? type) && string.IsNullOrEmpty(type))
            {
                throw new ArgumentException($"Required property '{ImpalaParameters.Type}' is missing. Supported types: {ServerTypeParser.SupportedList}", nameof(properties));
            }
            if (!ServerTypeParser.TryParse(type, out ImpalaServerType serverTypeValue))
            {
                throw new ArgumentOutOfRangeException(nameof(properties), $"Unsupported or unknown value '{type}' given for property '{ImpalaParameters.Type}'. Supported types: {ServerTypeParser.SupportedList}");
            }
            return serverTypeValue switch
            {
                ImpalaServerType.Http => new ImpalaHttpConnection(properties),
                ImpalaServerType.Standard => new ImpalaStandardConnection(properties),
                _ => throw new ArgumentOutOfRangeException(nameof(properties), $"Unsupported or unknown value '{type}' given for property '{ImpalaParameters.Type}'. Supported types: {ServerTypeParser.SupportedList}"),
            };
        }
    }
}
