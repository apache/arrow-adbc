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

using static System.Net.WebRequestMethods;

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
        public const string DataTypeConv = "adbc.spark.data_type_conv";
    }

    public static class SparkAuthTypeConstants
    {
        public const string None = "none";
        public const string UsernameOnly = "username_only";
        public const string Basic = "basic";
        public const string Token = "token";

        public static bool TryParse(string? authType, out SparkAuthType authTypeValue)
        {
            switch (authType?.Trim().ToLowerInvariant())
            {
                case null:
                case "":
                    authTypeValue = SparkAuthType.Empty;
                    return true;
                case None:
                    authTypeValue = SparkAuthType.None;
                    return true;
                case UsernameOnly:
                    authTypeValue = SparkAuthType.UsernameOnly;
                    return true;
                case Basic:
                    authTypeValue = SparkAuthType.Basic;
                    return true;
                case Token:
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
        None,
        UsernameOnly,
        Basic,
        Token,
        Empty = int.MaxValue,
    }

    public static class SparkServerTypeConstants
    {
        public const string Http = "http";
        public const string Databricks = "databricks";
        public const string Standard = "standard";
        public const string HDInsight = "hdinsight";
        internal const string SupportedList = Http + ", " + Databricks;

        public static bool TryParse(string? serverType, out SparkServerType serverTypeValue)
        {
            switch (serverType?.Trim().ToLowerInvariant())
            {
                case null:
                case "":
                    serverTypeValue = SparkServerType.Empty;
                    return true;
                case Databricks:
                    serverTypeValue = SparkServerType.Databricks;
                    return true;
                case Http:
                    serverTypeValue = SparkServerType.Http;
                    return true;
                case Standard:
                    serverTypeValue = SparkServerType.Standard;
                    return true;
                case HDInsight:
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
        Http,
        Databricks,
        Standard,
        HDInsight,
        Empty = int.MaxValue,
    }

    public static class SparkDataTypeConversionConstants
    {
        public const string None = "none";
        public const string Scalar = "scalar";
        public const string SupportedList = None;

        public static bool TryParse(string? dataTypeConversion, out SparkDataTypeConversion dataTypeConversionValue)
        {
            switch (dataTypeConversion?.Trim().ToLowerInvariant())
            {
                case null:
                case "":
                case Scalar:
                    dataTypeConversionValue = SparkDataTypeConversion.Scalar;
                    return true;
                case None:
                    dataTypeConversionValue = SparkDataTypeConversion.None;
                    return true;
                default:
                    dataTypeConversionValue = SparkDataTypeConversion.Invalid;
                    return false;
            }
        }
    }

    public enum SparkDataTypeConversion
    {
        Invalid = 0,
        None,
        Scalar,
        Empty = int.MaxValue,
    }
}
