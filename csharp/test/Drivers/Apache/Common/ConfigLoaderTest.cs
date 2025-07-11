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
using System.IO;
using System.Reflection;
using Xunit;
using Apache.Arrow.Adbc.Drivers.Apache.Common;
using Apache.Arrow.Adbc;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Common
{
    public class ConfigLoaderTest
    {
        [Fact]
        public void TestConfigLoaderNoEnvironmentVariable()
        {
            // Create a dictionary with explicitly provided parameters
            var explicitParams = new Dictionary<string, string>
            {
                { "param1", "value1" },
                { "param2", "value2" }
            };

            // Ensure environment variable is not set
            Environment.SetEnvironmentVariable(ConfigLoader.ConfigFilePathEnvVar, null);

            // Load and merge the parameters
            var mergedParams = ConfigLoader.LoadAndMergeConfig(explicitParams);

            // Verify the parameters are unchanged
            Assert.Equal(2, mergedParams.Count);
            Assert.Equal("value1", mergedParams["param1"]);
            Assert.Equal("value2", mergedParams["param2"]);
        }

        [Fact]
        public void TestConfigLoaderFileNotFound()
        {
            try
            {
                // Set environment variable to non-existent file
                Environment.SetEnvironmentVariable(ConfigLoader.ConfigFilePathEnvVar, "non_existent_file.json");

                // Create a dictionary with explicitly provided parameters
                var explicitParams = new Dictionary<string, string>
                {
                    { "param1", "value1" }
                };

                // Attempt to load and merge the parameters
                var exception = Assert.Throws<AdbcException>(() => 
                    ConfigLoader.LoadAndMergeConfig(explicitParams));

                // Verify the exception message
                Assert.Contains("Failed to load configuration from file", exception.Message);
                Assert.Contains("non_existent_file.json", exception.Message);
                Assert.Equal(AdbcStatusCode.InvalidArgument, exception.Status);
            }
            finally
            {
                // Clean up
                Environment.SetEnvironmentVariable(ConfigLoader.ConfigFilePathEnvVar, null);
            }
        }

        [Fact]
        public void TestConfigLoaderWithValidFile()
        {
            // Create a temporary JSON file
            string tempFile = Path.GetTempFileName();
            try
            {
                // Write JSON content to the file
                File.WriteAllText(tempFile, @"
                {
                    """ + AdbcOptions.Connection.CurrentCatalog + @""": ""test_catalog"",
                    """ + AdbcOptions.Connection.CurrentDbSchema + @""": ""test_schema"",
                    """ + SparkParameters.AuthType + @""": ""oauth"",
                    """ + SparkParameters.AccessToken + @""": ""test_token"",
                    ""numeric_value"": 123,
                    ""boolean_value"": true
                }");

                // Set environment variable to the temp file
                Environment.SetEnvironmentVariable(ConfigLoader.ConfigFilePathEnvVar, tempFile);

                // Create a dictionary with explicitly provided parameters
                var explicitParams = new Dictionary<string, string>
                {
                    { AdbcOptions.Connection.CurrentCatalog, "override_catalog" }, // This should override the value in the config file
                    { "custom.parameter", "custom_value" } // This should be preserved
                };

                // Load and merge the parameters
                var mergedParams = ConfigLoader.LoadAndMergeConfig(explicitParams);

                // Verify the merged parameters
                Assert.Equal("override_catalog", mergedParams[AdbcOptions.Connection.CurrentCatalog]); // Should be overridden
                Assert.Equal("test_schema", mergedParams[AdbcOptions.Connection.CurrentDbSchema]);
                Assert.Equal("oauth", mergedParams[SparkParameters.AuthType]);
                Assert.Equal("test_token", mergedParams[SparkParameters.AccessToken]);
                Assert.Equal("123", mergedParams["numeric_value"]); // Converted to string
                Assert.Equal("true", mergedParams["boolean_value"]); // Converted to string
                Assert.Equal("custom_value", mergedParams["custom.parameter"]); // Should be preserved
            }
            finally
            {
                // Clean up
                Environment.SetEnvironmentVariable(ConfigLoader.ConfigFilePathEnvVar, null);
                if (File.Exists(tempFile))
                {
                    File.Delete(tempFile);
                }
            }
        }

        [Fact]
        public void TestConfigLoaderWithInvalidJson()
        {
            // Create a temporary JSON file
            string tempFile = Path.GetTempFileName();
            try
            {
                // Write invalid JSON content to the file
                File.WriteAllText(tempFile, @"{ ""key"": ""value"", invalid_json }");

                // Set environment variable to the temp file
                Environment.SetEnvironmentVariable(ConfigLoader.ConfigFilePathEnvVar, tempFile);

                // Create a dictionary with explicitly provided parameters
                var explicitParams = new Dictionary<string, string>
                {
                    { "param1", "value1" }
                };

                // Attempt to load and merge the parameters
                var exception = Assert.Throws<AdbcException>(() => 
                    ConfigLoader.LoadAndMergeConfig(explicitParams));

                // Verify the exception message
                Assert.Contains("Invalid JSON format", exception.Message);
                Assert.Equal(AdbcStatusCode.InvalidArgument, exception.Status);
            }
            finally
            {
                // Clean up
                Environment.SetEnvironmentVariable(ConfigLoader.ConfigFilePathEnvVar, null);
                if (File.Exists(tempFile))
                {
                    File.Delete(tempFile);
                }
            }
        }
    }
}