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
    /// <summary>
    /// Parameters used for connecting to Spark data sources.
    /// </summary>
    public static class SparkParameters
    {
        public const string HostName = "adbc.spark.host";
        public const string Port = "adbc.spark.port";
        public const string Path = "adbc.spark.path";
        public const string Token = "adbc.spark.token";
        public const string AuthType = "adbc.spark.auth_type";
        public const string Type = "adbc.spark.type";
    }

    public static class SparkAuthTypeConstants
    {
        public const string AuthTypeBasic = "basic";
        public const string AuthTypeToken = "token";

        public static bool TryParse(string? authType, out SparkAuthType authTypeValue)
        {
            switch (authType?.Trim().ToLowerInvariant())
            {
                case null:
                case "":
                    authTypeValue = SparkAuthType.Empty;
                    return true;
                case AuthTypeBasic:
                    authTypeValue = SparkAuthType.Basic;
                    return true;
                case AuthTypeToken:
                    authTypeValue = SparkAuthType.Token;
                    return true;
                default:
                    authTypeValue = SparkAuthType.Invalid;
                    return false;
            }
        }
    }

    public enum SparkAuthType
    {
        Invalid = 0,
        Basic,
        Token,
        Empty = int.MaxValue,
    }

    public static class SparkServerTypeConstants
    {
        public const string ServerTypeDatabricks = "databricks";
        public const string ServerTypeHttp = "http";
        public const string ServerTypeStandard = "standard";
        public const string ServerTypeHDInsight = "hdinsight";
        internal const string ServerTypeSupportedList = ServerTypeStandard + ", " + ServerTypeHttp + ", " + ServerTypeDatabricks;

        public static bool TryParse(string? serverType, out SparkServerType serverTypeValue)
        {
            switch (serverType?.Trim().ToLowerInvariant())
            {
                case null:
                case "":
                    serverTypeValue = SparkServerType.Empty;
                    return true;
                case ServerTypeDatabricks:
                    serverTypeValue = SparkServerType.Databricks;
                    return true;
                case ServerTypeHttp:
                    serverTypeValue = SparkServerType.Http;
                    return true;
                case ServerTypeStandard:
                    serverTypeValue = SparkServerType.Standard;
                    return true;
                case ServerTypeHDInsight:
                    serverTypeValue = SparkServerType.HDInsight;
                    return true;
                default:
                    serverTypeValue = SparkServerType.Invalid;
                    return false;
            }
        }
    }

    public enum SparkServerType
    {
        Invalid = 0,
        Databricks,
        Http,
        Standard,
        HDInsight,
        Empty = int.MaxValue,
    }

}
