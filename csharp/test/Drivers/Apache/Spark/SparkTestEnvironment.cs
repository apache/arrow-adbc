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
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    public class SparkTestEnvironment : TestEnvironment<SparkTestConfiguration>
    {
        public class Factory : Factory<SparkTestEnvironment>
        {
            public override SparkTestEnvironment Create(Func<AdbcConnection> getConnection) => new SparkTestEnvironment(getConnection);
        }

        private SparkTestEnvironment(Func<AdbcConnection> getConnection) : base(getConnection) { }

        public override string TestConfigVariable => "SPARK_TEST_CONFIG_FILE";

        public override string SqlDataResourceLocation => ServerType == SparkServerType.Databricks
            ? "Spark/Resources/SparkData-Databricks.sql"
            : "Spark/Resources/SparkData.sql";

        public override int ExpectedColumnCount => ServerType == SparkServerType.Databricks ? 19 : 17;

        public override AdbcDriver CreateNewDriver() => new SparkDriver();

        public override string GetCreateTemporaryTableStatement(string tableName, string columns)
        {
            return string.Format("CREATE TABLE {0} ({1})", tableName, columns);
        }

        public string? GetValueForProtocolVersion(string? hiveValue, string? databrickValue) =>
            ServerType != SparkServerType.Databricks && ((HiveServer2Connection)Connection).DataTypeConversion.HasFlag(DataTypeConversion.None) ? hiveValue : databrickValue;

        public object? GetValueForProtocolVersion(object? hiveValue, object? databrickValue) =>
            ServerType != SparkServerType.Databricks && ((HiveServer2Connection)Connection).DataTypeConversion.HasFlag(DataTypeConversion.None) ? hiveValue : databrickValue;

        public override string Delimiter => "`";

        public override Dictionary<string, string> GetDriverParameters(SparkTestConfiguration testConfiguration)
        {
            Dictionary<string, string> parameters = new(StringComparer.OrdinalIgnoreCase);

            if (!string.IsNullOrEmpty(testConfiguration.HostName))
            {
                parameters.Add(SparkParameters.HostName, testConfiguration.HostName!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Uri))
            {
                parameters.Add(AdbcOptions.Uri, testConfiguration.Uri!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Port))
            {
                parameters.Add(SparkParameters.Port, testConfiguration.Port!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Path))
            {
                parameters.Add(SparkParameters.Path, testConfiguration.Path!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Token))
            {
                parameters.Add(SparkParameters.Token, testConfiguration.Token!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Username))
            {
                parameters.Add(AdbcOptions.Username, testConfiguration.Username!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Password))
            {
                parameters.Add(AdbcOptions.Password, testConfiguration.Password!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.AuthType))
            {
                parameters.Add(SparkParameters.AuthType, testConfiguration.AuthType!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.Type))
            {
                parameters.Add(SparkParameters.Type, testConfiguration.Type!);
            }
            if (!string.IsNullOrEmpty(testConfiguration.DataTypeConversion))
            {
                parameters.Add(SparkParameters.DataTypeConv, testConfiguration.DataTypeConversion!);
            }

            return parameters;
        }

        internal SparkServerType ServerType => ((SparkConnection)Connection).ServerType;

        public override string VendorVersion => ((HiveServer2Connection)Connection).VendorVersion;

        public override bool SupportsDelete => ServerType == SparkServerType.Databricks;

        public override bool SupportsUpdate => ServerType == SparkServerType.Databricks;

        public override bool SupportCatalogName => ServerType == SparkServerType.Databricks;

        public override bool ValidateAffectedRows => ServerType == SparkServerType.Databricks;

        public override string GetInsertStatement(string tableName, string columnName, string? value) =>
            string.Format("INSERT INTO {0} ({1}) SELECT {2};", tableName, columnName, value ?? "NULL");
    }
}
