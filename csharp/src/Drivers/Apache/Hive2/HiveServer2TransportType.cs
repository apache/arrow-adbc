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

namespace Apache.Arrow.Adbc.Drivers.Apache.Hive2
{
    internal enum HiveServer2TransportType
    {
        Http,
        Empty = int.MaxValue,
    }

    internal static class HiveServer2TransportTypeParser
    {
        internal const string SupportedList = HiveServer2TransportTypeConstants.Http;

        internal static bool TryParse(string? serverType, out HiveServer2TransportType serverTypeValue)
        {
            switch (serverType?.Trim().ToLowerInvariant())
            {
                case null:
                case "":
                    serverTypeValue = HiveServer2TransportType.Empty;
                    return true;
                case HiveServer2TransportTypeConstants.Http:
                    serverTypeValue = HiveServer2TransportType.Http;
                    return true;
                default:
                    serverTypeValue = default;
                    return false;
            }
        }
    }
}
