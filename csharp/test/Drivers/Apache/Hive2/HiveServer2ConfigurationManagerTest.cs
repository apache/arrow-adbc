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
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class HiveServer2ConfigurationManagerTest : IDisposable
    {
        private readonly Dictionary<string, string> _originalEnvVars = new Dictionary<string, string>();
        
        public HiveServer2ConfigurationManagerTest()
        {
            // Save original environment variables that we'll modify
            SaveEnvironmentVariable("ADBC_ENV_ONLY");
            SaveEnvironmentVariable("ADBC_ENV_DB_OVERRIDE");
            SaveEnvironmentVariable("ADBC_ENV_CONN_OVERRIDE");
            SaveEnvironmentVariable("ADBC_ENV_BOTH_OVERRIDE");
        }

        public void Dispose()
        {
            // Restore original environment variables
            foreach (var kvp in _originalEnvVars)
            {
                if (kvp.Value != null)
                {
                    Environment.SetEnvironmentVariable(kvp.Key, kvp.Value);
                }
                else
                {
                    Environment.SetEnvironmentVariable(kvp.Key, null);
                }
            }
        }
        
        private void SaveEnvironmentVariable(string name)
        {
            _originalEnvVars[name] = Environment.GetEnvironmentVariable(name);
        }
        
        [Fact]
        public void TestEnvironmentVariableConversion()
        {
            string paramName = "adbc.databricks.cloudfetch.enabled";
            string envVarName = "ADBC_DATABRICKS_CLOUDFETCH_ENABLED";
            
            Assert.Equal(envVarName, HiveServer2ConfigurationManager.ParameterToEnvironmentVariable(paramName));
            Assert.Equal(paramName, HiveServer2ConfigurationManager.EnvironmentVariableToParameter(envVarName));
        }
        
        [Fact]
        public void TestEnvironmentVariableToParameterThrowsForInvalidPrefix()
        {
            Assert.Throws<ArgumentException>(() => 
                HiveServer2ConfigurationManager.EnvironmentVariableToParameter("INVALID_PREFIX_VAR"));
        }
        
        [Fact]
        public void TestGetMergedProperties()
        {
            // Set environment variables
            Environment.SetEnvironmentVariable("ADBC_ENV_ONLY", "env-only-value");
            Environment.SetEnvironmentVariable("ADBC_ENV_DB_OVERRIDE", "env-value");
            Environment.SetEnvironmentVariable("ADBC_ENV_CONN_OVERRIDE", "env-value");
            Environment.SetEnvironmentVariable("ADBC_ENV_BOTH_OVERRIDE", "env-value");
            
            // Create database properties
            var dbProps = new Dictionary<string, string>
            {
                { "db.only", "db-only-value" },
                { "env.db.override", "db-value" },
                { "db.conn.override", "db-value" },
                { "env.both.override", "db-value" }
            };
            
            // Create connection options
            var connOptions = new Dictionary<string, string>
            {
                { "conn.only", "conn-only-value" },
                { "env.conn.override", "conn-value" },
                { "db.conn.override", "conn-value" },
                { "env.both.override", "conn-value" }
            };
            
            // Create configuration manager
            var configManager = new HiveServer2ConfigurationManager(dbProps);
            configManager.SetConnectionOptions(connOptions);
            
            // Get merged properties
            var merged = configManager.GetMergedProperties();
            
            // Verify values
            Assert.Equal("env-only-value", merged["env.only"]);
            Assert.Equal("db-only-value", merged["db.only"]);
            Assert.Equal("conn-only-value", merged["conn.only"]);
            Assert.Equal("db-value", merged["env.db.override"]);
            Assert.Equal("conn-value", merged["env.conn.override"]);
            Assert.Equal("conn-value", merged["db.conn.override"]);
            Assert.Equal("conn-value", merged["env.both.override"]);
        }
        
        [Fact]
        public void TestGetProperty()
        {
            // Set environment variables
            Environment.SetEnvironmentVariable("ADBC_ENV_ONLY", "env-only-value");
            Environment.SetEnvironmentVariable("ADBC_ENV_DB_OVERRIDE", "env-value");
            Environment.SetEnvironmentVariable("ADBC_ENV_CONN_OVERRIDE", "env-value");
            
            // Create database properties
            var dbProps = new Dictionary<string, string>
            {
                { "db.only", "db-only-value" },
                { "env.db.override", "db-value" },
                { "db.conn.override", "db-value" }
            };
            
            // Create connection options
            var connOptions = new Dictionary<string, string>
            {
                { "conn.only", "conn-only-value" },
                { "db.conn.override", "conn-value" },
                { "env.conn.override", "conn-value" }
            };
            
            // Create configuration manager
            var configManager = new HiveServer2ConfigurationManager(dbProps);
            configManager.SetConnectionOptions(connOptions);
            
            // Verify individual property values
            Assert.Equal("env-only-value", configManager.GetProperty("env.only"));
            Assert.Equal("db-only-value", configManager.GetProperty("db.only"));
            Assert.Equal("conn-only-value", configManager.GetProperty("conn.only"));
            Assert.Equal("db-value", configManager.GetProperty("env.db.override"));
            Assert.Equal("conn-value", configManager.GetProperty("env.conn.override"));
            Assert.Equal("conn-value", configManager.GetProperty("db.conn.override"));
            Assert.Equal("default-value", configManager.GetProperty("non.existent.key", "default-value"));
        }
        
        [Fact]
        public void TestTryGetProperty()
        {
            // Set environment variables
            Environment.SetEnvironmentVariable("ADBC_ENV_ONLY", "env-only-value");
            
            // Create database properties
            var dbProps = new Dictionary<string, string>
            {
                { "db.only", "db-only-value" }
            };
            
            // Create connection options
            var connOptions = new Dictionary<string, string>
            {
                { "conn.only", "conn-only-value" }
            };
            
            // Create configuration manager
            var configManager = new HiveServer2ConfigurationManager(dbProps);
            configManager.SetConnectionOptions(connOptions);
            
            // Verify TryGetProperty behavior
            Assert.True(configManager.TryGetProperty("env.only", out var envValue));
            Assert.Equal("env-only-value", envValue);
            
            Assert.True(configManager.TryGetProperty("db.only", out var dbValue));
            Assert.Equal("db-only-value", dbValue);
            
            Assert.True(configManager.TryGetProperty("conn.only", out var connValue));
            Assert.Equal("conn-only-value", connValue);
            
            Assert.False(configManager.TryGetProperty("non.existent.key", out var nonExistentValue));
            Assert.Null(nonExistentValue);
        }
        
        [Fact]
        public void TestNullConnectionOptions()
        {
            // Set environment variables
            Environment.SetEnvironmentVariable("ADBC_ENV_ONLY", "env-only-value");
            
            // Create database properties
            var dbProps = new Dictionary<string, string>
            {
                { "db.only", "db-only-value" }
            };
            
            // Create configuration manager
            var configManager = new HiveServer2ConfigurationManager(dbProps);
            
            // Don't set connection options (null)
            
            // Get merged properties
            var merged = configManager.GetMergedProperties();
            
            // Verify values
            Assert.Equal("env-only-value", merged["env.only"]);
            Assert.Equal("db-only-value", merged["db.only"]);
        }
    }
}