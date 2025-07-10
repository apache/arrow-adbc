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
using System.Reflection;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    public class HiveServer2DatabaseEnvironmentTest : IDisposable
    {
        private readonly Dictionary<string, string> _originalEnvVars = new Dictionary<string, string>();
        
        public HiveServer2DatabaseEnvironmentTest()
        {
            // Save original environment variables that we'll modify
            SaveEnvironmentVariable("ADBC_SPARK_HOST");
            SaveEnvironmentVariable("ADBC_SPARK_PORT");
            SaveEnvironmentVariable("ADBC_SPARK_TOKEN");
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
        public void TestDatabaseUsesEnvironmentVariables()
        {
            // Set environment variables
            Environment.SetEnvironmentVariable("ADBC_SPARK_HOST", "env-host");
            Environment.SetEnvironmentVariable("ADBC_SPARK_PORT", "10000");
            Environment.SetEnvironmentVariable("ADBC_SPARK_TOKEN", "env-token");
            
            // Create database with minimal properties
            var props = new Dictionary<string, string>();
            
            // Create the database
            var db = new HiveServer2Database(props);
            
            // Get the configuration manager field using reflection
            var configManagerField = typeof(HiveServer2Database).GetField("_configManager", 
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(configManagerField);
            
            var configManager = configManagerField.GetValue(db) as HiveServer2ConfigurationManager;
            Assert.NotNull(configManager);
            
            // Verify values
            Assert.Equal("env-host", configManager.GetProperty("adbc.spark.host"));
            Assert.Equal("10000", configManager.GetProperty("adbc.spark.port"));
            Assert.Equal("env-token", configManager.GetProperty("adbc.spark.token"));
        }
        
        [Fact]
        public void TestExplicitPropertiesTakePrecedenceOverEnvironmentVariables()
        {
            // Set environment variables
            Environment.SetEnvironmentVariable("ADBC_SPARK_HOST", "env-host");
            Environment.SetEnvironmentVariable("ADBC_SPARK_PORT", "10000");
            
            // Create database with explicit properties
            var props = new Dictionary<string, string>
            {
                { "adbc.spark.host", "explicit-host" },
                { "adbc.spark.token", "explicit-token" }
            };
            
            // Create the database
            var db = new HiveServer2Database(props);
            
            // Get the configuration manager field using reflection
            var configManagerField = typeof(HiveServer2Database).GetField("_configManager", 
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(configManagerField);
            
            var configManager = configManagerField.GetValue(db) as HiveServer2ConfigurationManager;
            Assert.NotNull(configManager);
            
            // Verify values
            Assert.Equal("explicit-host", configManager.GetProperty("adbc.spark.host")); // Explicit property takes precedence
            Assert.Equal("10000", configManager.GetProperty("adbc.spark.port")); // From environment variable
            Assert.Equal("explicit-token", configManager.GetProperty("adbc.spark.token")); // From explicit properties
        }
        
        [Fact]
        public void TestConnectMergesConnectionOptions()
        {
            // Set environment variables
            Environment.SetEnvironmentVariable("ADBC_SPARK_ENV", "env-value");
            
            // Create database with properties
            var dbProps = new Dictionary<string, string>
            {
                { "adbc.spark.db", "db-value" }
            };
            
            // Create the database
            var db = new HiveServer2Database(dbProps);
            
            // Get the configuration manager field using reflection
            var configManagerField = typeof(HiveServer2Database).GetField("_configManager", 
                BindingFlags.NonPublic | BindingFlags.Instance);
            Assert.NotNull(configManagerField);
            
            var configManager = configManagerField.GetValue(db) as HiveServer2ConfigurationManager;
            Assert.NotNull(configManager);
            
            // Create connection options
            var connOptions = new Dictionary<string, string>
            {
                { "adbc.spark.conn", "conn-value" },
                { "adbc.spark.db", "conn-override-value" } // This should override the db property
            };
            
            // Set the connection options in the configuration manager
            configManager.SetConnectionOptions(connOptions);
            
            // Verify merged properties
            var mergedProps = configManager.GetMergedProperties();
            
            Assert.Equal("env-value", mergedProps["adbc.spark.env"]);
            Assert.Equal("conn-override-value", mergedProps["adbc.spark.db"]); // Connection option overrides db property
            Assert.Equal("conn-value", mergedProps["adbc.spark.conn"]);
        }
    }
}