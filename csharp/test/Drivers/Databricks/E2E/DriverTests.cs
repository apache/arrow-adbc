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
using System.IO;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    public class DriverTests : DriverTests<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public DriverTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        [SkippableTheory]
        [MemberData(nameof(CatalogNamePatternData))]
        public override void CanGetObjectsCatalogs(string? pattern)
        {
            GetObjectsCatalogsTest(pattern);
        }

        [SkippableTheory]
        [MemberData(nameof(DbSchemasNamePatternData))]
        public override void CanGetObjectsDbSchemas(string dbSchemaPattern)
        {
            GetObjectsDbSchemasTest(dbSchemaPattern);
        }

        [SkippableTheory]
        [MemberData(nameof(TableNamePatternData))]
        public override void CanGetObjectsTables(string tableNamePattern)
        {
            GetObjectsTablesTest(tableNamePattern);
        }


        [SkippableFact]
        public async Task CanGetObjectsOnNoColumnTable()
        {
            string? catalogName = TestConfiguration.Metadata.Catalog;
            string? schemaName = TestConfiguration.Metadata.Schema;
            string tableName = Guid.NewGuid().ToString("N");
            string fullTableName = string.Format(
                "{0}{1}{2}",
                string.IsNullOrEmpty(catalogName) ? string.Empty : DelimitIdentifier(catalogName) + ".",
                string.IsNullOrEmpty(schemaName) ? string.Empty : DelimitIdentifier(schemaName) + ".",
                DelimitIdentifier(tableName));
            using TemporaryTable temporaryTable = await TemporaryTable.NewTemporaryTableAsync(
                Statement,
                fullTableName,
                $"CREATE TABLE IF NOT EXISTS {fullTableName} ();",
                OutputHelper);
            using AdbcConnection adbcConnection = NewConnection();

            GetObjectsTablesTest(tableNamePattern: tableName, expectedTableName: tableName);
        }

        public override void CanDetectInvalidServer()
        {
            AdbcDriver driver = NewDriver;
            Assert.NotNull(driver);
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);

            bool hasUri = parameters.TryGetValue(AdbcOptions.Uri, out var uri) && !string.IsNullOrEmpty(uri);
            bool hasHostName = parameters.TryGetValue(SparkParameters.HostName, out var hostName) && !string.IsNullOrEmpty(hostName);
            if (hasUri)
            {
                parameters[AdbcOptions.Uri] = "http://unknownhost.azure.com/cliservice";
            }
            else if (hasHostName)
            {
                parameters[SparkParameters.HostName] = "unknownhost.azure.com";
            }
            else
            {
                Assert.Fail($"Unexpected configuration. Must provide '{AdbcOptions.Uri}' or '{SparkParameters.HostName}'.");
            }

            AdbcDatabase database = driver.Open(parameters);
            AggregateException exception = Assert.ThrowsAny<AggregateException>(() => database.Connect(parameters));
            OutputHelper?.WriteLine(exception.Message);
        }

        public override void CanDetectInvalidAuthentication()
        {
            AdbcDriver driver = NewDriver;
            Assert.NotNull(driver);
            Dictionary<string, string> parameters = GetDriverParameters(TestConfiguration);

            bool hasToken = parameters.TryGetValue(SparkParameters.Token, out var token) && !string.IsNullOrEmpty(token);
            bool hasAccessToken = parameters.TryGetValue(SparkParameters.Token, out var access_token) && !string.IsNullOrEmpty(access_token);
            bool hasUsername = parameters.TryGetValue(AdbcOptions.Username, out var username) && !string.IsNullOrEmpty(username);
            bool hasPassword = parameters.TryGetValue(AdbcOptions.Password, out var password) && !string.IsNullOrEmpty(password);
            if (hasToken)
            {
                parameters[SparkParameters.Token] = "invalid-token";
            }
            else if (hasAccessToken)
            {
                parameters[SparkParameters.AccessToken] = "invalid-access-token";
            }
            else if (hasUsername && hasPassword)
            {
                parameters[AdbcOptions.Password] = "invalid-password";
            }
            else
            {
                Assert.Fail($"Unexpected configuration. Must provide '{SparkParameters.Token}' or '{SparkParameters.AccessToken}' or '{AdbcOptions.Username}' and '{AdbcOptions.Password}'.");
            }

            AdbcDatabase database = driver.Open(parameters);
            AggregateException exception = Assert.ThrowsAny<AggregateException>(() => database.Connect(parameters));
            OutputHelper?.WriteLine(exception.Message);
        }

        protected override IReadOnlyList<int> GetUpdateExpectedResults()
        {
            int affectedRows = ValidateAffectedRows ? 1 : -1;
            return ClientTests.GetUpdateExpectedResults(affectedRows, true);
        }

        public static IEnumerable<object[]> CatalogNamePatternData()
        {
            string? catalogName = new DriverTests(null).TestConfiguration?.Metadata?.Catalog;
            return GetPatterns(catalogName);
        }

        public static IEnumerable<object[]> DbSchemasNamePatternData()
        {
            string? dbSchemaName = new DriverTests(null).TestConfiguration?.Metadata?.Schema;
            return GetPatterns(dbSchemaName);
        }

        public static IEnumerable<object[]> TableNamePatternData()
        {
            string? tableName = new DriverTests(null).TestConfiguration?.Metadata?.Table;
            return GetPatterns(tableName);
        }

        protected override bool TypeHasDecimalDigits(Metadata.AdbcColumn column)
        {
            switch (column.XdbcDataType!.Value)
            {
                case (short)SupportedDriverDataType.DECIMAL:
                case (short)SupportedDriverDataType.NUMERIC:
                    return true;
                default:
                    return false;
            }
        }

        protected override bool TypeHasColumnSize(Metadata.AdbcColumn column)
        {
            switch (column.XdbcDataType!.Value)
            {
                case (short)SupportedDriverDataType.DECIMAL:
                case (short)SupportedDriverDataType.NUMERIC:
                case (short)SupportedDriverDataType.CHAR:
                case (short)SupportedDriverDataType.VARCHAR:
                    return true;
                default:
                    return false;
            }
        }

        [SkippableFact]
        public void CanLoadConfigFileParameters()
        {
            // Create a temporary config file with test parameters
            string tempConfigFile = Path.GetTempFileName();
            try
            {
                // Write test parameters to config file
                var configParams = new Dictionary<string, string>
                {
                    { "test_param_1", "config_value_1" },
                    { "test_param_2", "config_value_2" },
                    { DatabricksParameters.UseCloudFetch, "true" },
                    { DatabricksParameters.EnableDirectResults, "false" }
                };

                // Convert to JSON
                string jsonConfig = System.Text.Json.JsonSerializer.Serialize(configParams, new System.Text.Json.JsonSerializerOptions
                {
                    WriteIndented = true
                });
                File.WriteAllText(tempConfigFile, jsonConfig);

                // Set environment variable to point to our config file
                Environment.SetEnvironmentVariable("ADBC_CONFIG_FILE", tempConfigFile);

                try
                {
                    // Test that config file parameters are loaded and merged with explicit parameters
                    AdbcDriver driver = NewDriver;
                    Dictionary<string, string> explicitParams = new()
                    {
                        { "test_param_1", "override_value" }, // This should override config file
                        { "explicit_param", "explicit_value" } // This should be added
                    };
                    
                    // Open the database and get the merged properties
                    using DatabricksDatabase database = (DatabricksDatabase)driver.Open(explicitParams);
                    var mergedProperties = database.MergedProperties;
                    
                    // Convert to dictionary for easier access
                    var propertiesDict = mergedProperties.ToDictionary(kvp => kvp.Key, kvp => kvp.Value);
                    
                    // Verify that config file parameters are present
                    Assert.True(propertiesDict.ContainsKey("test_param_2"), "Config file parameter 'test_param_2' should be present");
                    Assert.Equal("config_value_2", propertiesDict["test_param_2"]);
                    
                    // Verify that explicit parameters override config file parameters
                    Assert.True(propertiesDict.ContainsKey("test_param_1"), "Parameter 'test_param_1' should be present");
                    Assert.Equal("override_value", propertiesDict["test_param_1"], "Explicit parameter should override config file value");
                    
                    // Verify that explicit-only parameters are present
                    Assert.True(propertiesDict.ContainsKey("explicit_param"), "Explicit parameter 'explicit_param' should be present");
                    Assert.Equal("explicit_value", propertiesDict["explicit_param"]);
                    
                    // Verify that Databricks-specific parameters from config file are present
                    Assert.True(propertiesDict.ContainsKey(DatabricksParameters.UseCloudFetch), "CloudFetch parameter should be present");
                    Assert.Equal("true", propertiesDict[DatabricksParameters.UseCloudFetch]);
                    Assert.True(propertiesDict.ContainsKey(DatabricksParameters.EnableDirectResults), "DirectResults parameter should be present");
                    Assert.Equal("false", propertiesDict[DatabricksParameters.EnableDirectResults]);
                }
                finally
                {
                    // Clean up environment variable
                    Environment.SetEnvironmentVariable("ADBC_CONFIG_FILE", null);
                }
            }
            finally
            {
                // Clean up temp file
                if (File.Exists(tempConfigFile))
                {
                    File.Delete(tempConfigFile);
                }
            }
        }
    }
}
