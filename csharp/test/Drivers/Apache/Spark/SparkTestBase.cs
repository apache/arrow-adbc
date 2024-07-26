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
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Hive.Service.Rpc.Thrift;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    public class SparkTestBase : TestBase<SparkTestConfiguration>
    {
        public SparkTestBase(ITestOutputHelper? outputHelper) : base(outputHelper) { }

        protected override string TestConfigVariable => "SPARK_TEST_CONFIG_FILE";

        protected override string SqlDataResourceLocation => VendorVersionAsVersion >= Version.Parse("3.4.0")
            ? "Spark/Resources/SparkData-3.4.sql"
            : "Spark/Resources/SparkData.sql";

        protected override int ExpectedColumnCount => VendorVersionAsVersion >= Version.Parse("3.4.0") ? 19 : 17;

        protected override AdbcDriver NewDriver => new SparkDriver();

        protected string? GetValueForProtocolVersion(string? hiveValue, string? databrickValue) => IsHiveServer2Protocol ? hiveValue : databrickValue;

        protected object? GetValueForProtocolVersion(object? hiveValue, object? databrickValue) => IsHiveServer2Protocol ? hiveValue : databrickValue;

        protected override async ValueTask<TemporaryTable> NewTemporaryTableAsync(AdbcStatement statement, string columns)
        {
            string tableName = NewTableName();
            // Note: Databricks/Spark doesn't support TEMPORARY table.
            string sqlUpdate = string.Format("CREATE TABLE {0} ({1})", tableName, columns);
            OutputHelper?.WriteLine(sqlUpdate);
            return await TemporaryTable.NewTemporaryTableAsync(statement, tableName, sqlUpdate);
        }

        protected override string Delimiter => "`";

        protected override Dictionary<string, string> GetDriverParameters(SparkTestConfiguration testConfiguration)
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

            return parameters;
        }

        protected TProtocolVersion ProtocolVersion => ((HiveServer2Connection)Connection).ProtocolVersion;

        protected bool IsHiveServer2Protocol => ((HiveServer2Connection)Connection).IsHiveServer2Protocol;

        protected override string VendorVersion => ((HiveServer2Connection)Connection).VendorVersion;

        protected override bool SupportsDelete => !IsHiveServer2Protocol;

        protected override bool SupportsUpdate => !IsHiveServer2Protocol;

        protected override bool ValidateAffectedRows => !IsHiveServer2Protocol;
    }
}
