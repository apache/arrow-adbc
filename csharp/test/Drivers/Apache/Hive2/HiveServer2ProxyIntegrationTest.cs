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
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Hive2
{
    [Collection("HiveServer2TestEnvironment")]
    public class HiveServer2ProxyIntegrationTest : IDisposable
    {
        private readonly HiveServer2TestEnvironment _environment;
        private readonly ITestOutputHelper _output;

        public HiveServer2ProxyIntegrationTest(HiveServer2TestEnvironment environment, ITestOutputHelper output)
        {
            _environment = environment;
            _output = output;
        }

        public void Dispose()
        {
            GC.SuppressFinalize(this);
        }

        [SkippableFact]
        public async Task TestProxyConnection()
        {
            Skip.IfNot(TestConfiguration.HasTestConfiguration(_environment.TestConfigVariable),
                $"Test configuration not found. Set {_environment.TestConfigVariable} environment variable.");

            // This test requires a proxy server to be configured in the test config
            // Check if proxy settings are available in the test configuration
            var config = TestConfiguration.GetTestConfiguration<ApacheTestConfiguration>(_environment.TestConfigVariable);
            Skip.If(config.HttpOptions?.Proxy == null || config.HttpOptions.Proxy.UseProxy != true,
                "Proxy configuration not found in test configuration. Set http_options.proxy.use_proxy to true.");

            // Log the proxy configuration being used
            _output.WriteLine($"Using proxy: {config.HttpOptions.Proxy.ProxyHost}:{config.HttpOptions.Proxy.ProxyPort}");
            if (config.HttpOptions.Proxy.ProxyAuth == true)
            {
                _output.WriteLine("Using proxy authentication");
            }

            // Create a connection with proxy settings
            using AdbcConnection connection = _environment.CreateNewConnection();
            
            // Execute a simple query to verify the connection works through the proxy
            using AdbcStatement statement = connection.CreateStatement();
            statement.SqlQuery = "SELECT 1 as test_value";
            using IArrowArrayStream stream = await statement.ExecuteQueryAsync();
            
            // Read the result and verify
            RecordBatch batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.ColumnCount);
            Assert.Equal("test_value", batch.Schema.GetFieldByIndex(0).Name);
            
            // Verify we got a result
            var array = batch.Column(0);
            Assert.Equal(1, array.Length);
        }
    }
}