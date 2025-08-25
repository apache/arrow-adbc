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
using System.Text.Json;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    /// <summary>
    /// Unit tests for the DatabricksConfiguration class.
    /// </summary>
    public class DatabricksConfigurationTest : IDisposable
    {
        private readonly List<string> _tempFiles = new();

        /// <summary>
        /// Tests that FileNotFoundException is thrown for non-existent files.
        /// </summary>
        [Fact]
        public void FromFile_FileNotFound_ThrowsFileNotFoundException()
        {
            // Arrange
            var nonExistentFile = Path.Combine(Path.GetTempPath(), "nonexistent.json");

            // Act & Assert
            Assert.Throws<FileNotFoundException>(() => DatabricksConfiguration.FromFile(nonExistentFile));
        }

        /// <summary>
        /// Tests that JsonException is thrown for invalid JSON.
        /// </summary>
        [Fact]
        public void FromFile_InvalidJson_ThrowsJsonException()
        {
            // Arrange
            var invalidJsonFile = CreateTempFile("invalid.json", "{ invalid json");

            // Act & Assert
            Assert.Throws<JsonException>(() => DatabricksConfiguration.FromFile(invalidJsonFile));
        }

        /// <summary>
        /// Tests loading configuration from empty JSON object.
        /// </summary>
        [Fact]
        public void FromFile_EmptyJson_ReturnsEmptyConfiguration()
        {
            // Arrange
            var emptyJsonFile = CreateTempFile("empty.json", "{}");

            // Act
            var config = DatabricksConfiguration.FromFile(emptyJsonFile);

            // Assert
            Assert.NotNull(config);
            Assert.NotNull(config.Properties);
            Assert.Empty(config.Properties);
        }

        /// <summary>
        /// Tests that JSON with comments is parsed correctly.
        /// </summary>
        [Fact]
        public void FromFile_JsonWithComments_IgnoresComments()
        {
            // Arrange
            var jsonWithComments = $@"{{
                // This is a test Databricks configuration
                ""{SparkParameters.HostName}"": ""test.databricks.com"",
                /* Multi-line
                   comment for auth */
                ""{SparkParameters.AuthType}"": ""Bearer"",
                ""{DatabricksParameters.UseCloudFetch}"": ""true""
            }}";
            var configFile = CreateTempFile("with-comments.json", jsonWithComments);

            // Act
            var config = DatabricksConfiguration.FromFile(configFile);

            // Assert
            Assert.NotNull(config);
            Assert.Equal(3, config.Properties.Count);
            Assert.Equal("test.databricks.com", config.Properties[SparkParameters.HostName]);
            Assert.Equal("Bearer", config.Properties[SparkParameters.AuthType]);
            Assert.Equal("true", config.Properties[DatabricksParameters.UseCloudFetch]);
        }

        /// <summary>
        /// Tests that JSON with trailing commas is parsed correctly.
        /// </summary>
        [Fact]
        public void FromFile_JsonWithTrailingCommas_ParsesSuccessfully()
        {
            // Arrange
            var jsonWithTrailingCommas = $@"{{
                ""{SparkParameters.HostName}"": ""test.databricks.com"",
                ""{DatabricksParameters.MaxBytesPerFile}"": ""10485760"",
            }}";
            var configFile = CreateTempFile("with-trailing-commas.json", jsonWithTrailingCommas);

            // Act
            var config = DatabricksConfiguration.FromFile(configFile);

            // Assert
            Assert.NotNull(config);
            Assert.Equal(2, config.Properties.Count);
            Assert.Equal("test.databricks.com", config.Properties[SparkParameters.HostName]);
            Assert.Equal("10485760", config.Properties[DatabricksParameters.MaxBytesPerFile]);
        }

        /// <summary>
        /// Tests case-insensitive key parsing.
        /// </summary>
        [Theory]
        [InlineData("hostname", "test.databricks.com")]
        [InlineData("HOSTNAME", "test.databricks.com")]
        [InlineData("HostName", "test.databricks.com")]
        [InlineData("authtype", "Bearer")]
        [InlineData("AuthType", "Bearer")]
        public void FromFile_CaseInsensitiveKeys_ParsesCorrectly(string keyName, string expectedValue)
        {
            // Arrange
            var json = $@"{{ ""{keyName}"": ""{expectedValue}"" }}";
            var configFile = CreateTempFile("case-test.json", json);

            // Act
            var config = DatabricksConfiguration.FromFile(configFile);

            // Assert
            Assert.NotNull(config);
            Assert.Single(config.Properties);
            Assert.Contains(config.Properties, kvp => kvp.Value == expectedValue);
        }

        /// <summary>
        /// Tests loading configuration from environment variable.
        /// </summary>
        [Fact]
        public void FromEnvironmentVariable_ValidPath_LoadsConfiguration()
        {
            // Arrange
            var properties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "env.databricks.com",
                [SparkParameters.AuthType] = "Bearer",
                [DatabricksParameters.EnableDirectResults] = "true"
            };
            var configFile = CreateTempJsonFile(properties);
            var envVar = "TEST_ENV_VAR_" + Guid.NewGuid().ToString("N").Substring(0, 8);

            try
            {
                Environment.SetEnvironmentVariable(envVar, configFile);

                // Act
                var config = DatabricksConfiguration.FromEnvironmentVariable(envVar);

                // Assert
                Assert.NotNull(config);
                Assert.Equal(properties.Count, config.Properties.Count);
                Assert.Equal("env.databricks.com", config.Properties[SparkParameters.HostName]);
                Assert.Equal("true", config.Properties[DatabricksParameters.EnableDirectResults]);
            }
            finally
            {
                Environment.SetEnvironmentVariable(envVar, null);
            }
        }


        /// <summary>
        /// Tests TryFromEnvironmentVariable with invalid configuration.
        /// </summary>
        [Fact]
        public void TryFromEnvironmentVariable_InvalidConfig_ReturnsNull()
        {
            // Arrange
            var nonExistentEnvVar = "NONEXISTENT_TRY_VAR_" + Guid.NewGuid().ToString("N");

            // Act
            var config = DatabricksConfiguration.TryFromEnvironmentVariable(nonExistentEnvVar);

            // Assert
            Assert.Null(config);
        }

        /// <summary>
        /// Tests CanLoadFromEnvironment with valid environment variable.
        /// </summary>
        [Fact]
        public void CanLoadFromEnvironment_ValidPath_ReturnsTrue()
        {
            // Arrange
            var configFile = CreateTempJsonFile(new Dictionary<string, string> { ["test"] = "value" });
            var envVar = "TEST_CAN_LOAD_" + Guid.NewGuid().ToString("N").Substring(0, 8);

            try
            {
                Environment.SetEnvironmentVariable(envVar, configFile);

                // Act
                var canLoad = DatabricksConfiguration.CanLoadFromEnvironment(envVar, out string? filePath);

                // Assert
                Assert.True(canLoad);
                Assert.Equal(configFile, filePath);
            }
            finally
            {
                Environment.SetEnvironmentVariable(envVar, null);
            }
        }

        /// <summary>
        /// Tests CanLoadFromEnvironment with invalid paths.
        /// </summary>
        [Theory]
        [InlineData(null)]
        [InlineData("")]
        [InlineData("   ")]
        public void CanLoadFromEnvironment_InvalidEnvVar_ReturnsFalse(string? envVarName)
        {
            // Act
            var canLoad = DatabricksConfiguration.CanLoadFromEnvironment(envVarName!, out string? filePath);

            // Assert
            Assert.False(canLoad);
            Assert.Null(filePath);
        }

        /// <summary>
        /// Tests CanLoadFromEnvironment with unset environment variable.
        /// </summary>
        [Fact]
        public void CanLoadFromEnvironment_UnsetVar_ReturnsFalse()
        {
            // Arrange
            var unsetEnvVar = "UNSET_ENV_VAR_" + Guid.NewGuid().ToString("N");

            // Act
            var canLoad = DatabricksConfiguration.CanLoadFromEnvironment(unsetEnvVar, out string? filePath);

            // Assert
            Assert.False(canLoad);
            Assert.Null(filePath);
        }

        /// <summary>
        /// Tests CanLoadFromEnvironment with non-existent file.
        /// </summary>
        [Fact]
        public void CanLoadFromEnvironment_NonExistentFile_ReturnsFalse()
        {
            // Arrange
            var envVar = "TEST_NONEXISTENT_" + Guid.NewGuid().ToString("N").Substring(0, 8);
            var nonExistentFile = Path.Combine(Path.GetTempPath(), "nonexistent.json");

            try
            {
                Environment.SetEnvironmentVariable(envVar, nonExistentFile);

                // Act
                var canLoad = DatabricksConfiguration.CanLoadFromEnvironment(envVar, out string? filePath);

                // Assert
                Assert.False(canLoad);
                Assert.Equal(nonExistentFile, filePath);
            }
            finally
            {
                Environment.SetEnvironmentVariable(envVar, null);
            }
        }

        /// <summary>
        /// Tests that property values are actually overridden based on precedence settings in DatabricksConnection.
        /// Validates that the correct property values (EnablePKFK and UseCloudFetch) are used based on precedence.
        /// </summary>
        [Theory]
        [InlineData("true")] // Environment takes precedence, should use environment values (false, false)
        [InlineData("false")] // Constructor takes precedence, should use constructor values (true, true)
        public void DatabricksConnection_PropertyOverridePrecedence_ValidatesCorrectValues(string precedenceValue)
        {
            // Arrange
            var envProperties = new Dictionary<string, string>
            {
                [DatabricksParameters.DriverConfigTakePrecedence] = precedenceValue,
                [DatabricksParameters.EnablePKFK] = "false",
                [DatabricksParameters.UseCloudFetch] = "false"
            };
            var configFile = CreateTempJsonFile(envProperties);
            var envVar = "TEST_PRECEDENCE_" + Guid.NewGuid().ToString("N").Substring(0, 8);

            var constructorProperties = new Dictionary<string, string>
            {
                [SparkParameters.HostName] = "test-host",
                [SparkParameters.Token] = "test-token",
                [DatabricksParameters.EnablePKFK] = "true",
                [DatabricksParameters.UseCloudFetch] = "true",
                [DatabricksParameters.CloudFetchMaxRetries] = "5"
            };

            try
            {
                Environment.SetEnvironmentVariable(DatabricksConnection.DefaultConfigEnvironmentVariable, configFile);

                // Act
                using var connection = new DatabricksConnection(constructorProperties);

                // Assert - Validate that the correct property values are being used based on precedence
                if (precedenceValue == "true")
                {
                    // Environment takes precedence: should use environment values (false, false)
                    Assert.False(connection.EnablePKFK, "Environment precedence should use EnablePKFK=false from environment config");
                    Assert.False(connection.UseCloudFetch, "Environment precedence should use UseCloudFetch=false from environment config");
                }
                else
                {
                    // Constructor takes precedence: should use constructor values (true, true)
                    Assert.True(connection.EnablePKFK, "Constructor precedence should use EnablePKFK=true from constructor properties");
                    Assert.True(connection.UseCloudFetch, "Constructor precedence should use UseCloudFetch=true from constructor properties");
                }
            }
            finally
            {
                Environment.SetEnvironmentVariable(DatabricksConnection.DefaultConfigEnvironmentVariable, null);
            }
        }


        #region Helper Methods

        /// <summary>
        /// Creates a temporary JSON file with the specified properties.
        /// </summary>
        private string CreateTempJsonFile(Dictionary<string, string> properties)
        {
            var json = JsonSerializer.Serialize(properties, new JsonSerializerOptions { WriteIndented = true });
            return CreateTempFile("config.json", json);
        }

        /// <summary>
        /// Creates a temporary file with the specified content.
        /// </summary>
        private string CreateTempFile(string fileName, string content)
        {
            var tempFile = Path.Combine(Path.GetTempPath(), $"{Guid.NewGuid():N}_{fileName}");
            File.WriteAllText(tempFile, content);
            _tempFiles.Add(tempFile);
            return tempFile;
        }

        /// <summary>
        /// Cleans up temporary files.
        /// </summary>
        public void Dispose()
        {
            foreach (var tempFile in _tempFiles)
            {
                try
                {
                    if (File.Exists(tempFile))
                        File.Delete(tempFile);
                }
                catch
                {
                    // Ignore cleanup errors
                }
            }
        }

        #endregion
    }
}
