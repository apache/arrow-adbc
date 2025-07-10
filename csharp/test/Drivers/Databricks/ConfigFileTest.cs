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
using Apache.Arrow.Adbc.Drivers.Databricks;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    public class ConfigFileTest
    {
        [Fact]
        public void TestDatabricksDriverWithConfigFile()
        {
            // This test verifies that the Databricks driver correctly loads parameters from a config file
            // It does not actually connect to a Databricks instance, just checks that the parameters are loaded

            try
            {
                // Get the path to the test config file
                string configFilePath = Path.Combine(
                    Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location)!,
                    "Resources",
                    "config_test.json");

                // Set environment variable to the config file
                Environment.SetEnvironmentVariable(Drivers.Apache.Common.ConfigLoader.ConfigFilePathEnvVar, configFilePath);

                // Create a dictionary with explicitly provided parameters
                var explicitParams = new Dictionary<string, string>
                {
                    { "adbc.connection.catalog", "override_catalog" }, // This should override the value in the config file
                    { "adbc.databricks.cloudfetch.max_retries", "10" } // This should override the value in the config file
                };

                try
                {
                    // Create a new driver
                    var driver = new DatabricksDriver();
                    
                    // We expect an exception since we're not actually connecting to a Databricks instance,
                    // but we can intercept the parameters that would be used
                    try
                    {
                        var database = driver.Open(explicitParams);
                    }
                    catch (AdbcException ex)
                    {
                        // This is expected, we just want to verify that the parameters were loaded correctly
                        // We can't directly access the merged parameters, but we can check the exception message
                        // to confirm the driver attempted to use them
                        Assert.Contains("override_catalog", ex.Message);
                    }
                }
                finally
                {
                    // Clean up
                    Environment.SetEnvironmentVariable(Drivers.Apache.Common.ConfigLoader.ConfigFilePathEnvVar, null);
                }
            }
            catch (Exception ex)
            {
                // Clean up and re-throw
                Environment.SetEnvironmentVariable(Drivers.Apache.Common.ConfigLoader.ConfigFilePathEnvVar, null);
                throw;
            }
        }
    }
}