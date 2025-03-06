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

namespace Apache.Arrow.Adbc.Drivers.Apache.Spark
{
    internal enum SparkServerType
    {
        Http,
        Databricks,
        Standard,
        Empty = int.MaxValue,
    }

    internal static class ServerTypeParser
    {
        internal const string SupportedList = SparkServerTypeConstants.Http + ", " + SparkServerTypeConstants.Databricks;

        internal static bool TryParse(string? serverType, out SparkServerType serverTypeValue)
        {
            switch (serverType?.Trim().ToLowerInvariant())
            {
                case null:
                case "":
                    serverTypeValue = SparkServerType.Empty;
                    return true;
                case SparkServerTypeConstants.Databricks:
                    serverTypeValue = SparkServerType.Databricks;
                    return true;
                case SparkServerTypeConstants.Http:
                    serverTypeValue = SparkServerType.Http;
                    return true;
                case SparkServerTypeConstants.Standard:
                    serverTypeValue = SparkServerType.Standard;
                    return true;
                default:
                    serverTypeValue = default;
                    return false;
            }
        }
    }
}
