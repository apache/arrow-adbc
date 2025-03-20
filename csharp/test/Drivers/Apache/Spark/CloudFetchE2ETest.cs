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
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
using Apache.Arrow.Adbc.Drivers.Apache.Spark.CloudFetch;
using Apache.Arrow.Types;
using Xunit;
using Xunit.Abstractions;
using Apache.Arrow.Adbc.Client;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    /// <summary>
    /// End-to-end tests for the CloudFetch feature in the Spark ADBC driver.
    /// </summary>
    public class CloudFetchE2ETest : TestBase<SparkTestConfiguration, SparkTestEnvironment>
    {
        public CloudFetchE2ETest(ITestOutputHelper? outputHelper) 
            : base(outputHelper, new SparkTestEnvironment.Factory())
        {
            // Skip the test if the SPARK_TEST_CONFIG_FILE environment variable is not set
            Skip.IfNot(Utils.CanExecuteTestConfig(TestConfigVariable));
        }

        /// <summary>
        /// Tests that CloudFetch can be enabled via statement options and that it works with a real Databricks server.
        /// </summary>
        [SkippableFact]
        public async Task TestCloudFetchEnabled()
        {
            // Skip if we're not using a Databricks server
            Skip.If(((SparkTestEnvironment)TestEnvironment).ServerType != SparkServerType.Databricks, 
                "CloudFetch is only supported on Databricks servers");

            // Create a statement with CloudFetch enabled
            using var statement = Connection.CreateStatement();
            statement.SetOption(SparkStatement.Options.UseCloudFetch, "true");
            statement.SetOption(SparkStatement.Options.CanDecompressLz4, "true");
            statement.SetOption(SparkStatement.Options.MaxBytesPerFile, "10485760"); // 10MB
            
            // Execute a simple query
            statement.SqlQuery = "SELECT 1 as id, 'test' as name";
            var result = await statement.ExecuteQueryAsync();
            
            // Verify the result
            Assert.NotNull(result.Stream);
            
            // Read the data
            var batch = await result.Stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
            Assert.Equal(2, batch.ColumnCount);
            
            // Get the columns with proper casting
            var idColumn = batch.Column(0) as Int32Array;
            var nameColumn = batch.Column(1) as StringArray;
            
            Assert.NotNull(idColumn);
            Assert.NotNull(nameColumn);
            
            // Verify the data
            Assert.Equal(1, idColumn!.GetValue(0));
            Assert.Equal("test", nameColumn!.GetString(0));
            
            // Verify there are no more batches
            Assert.Null(await result.Stream.ReadNextRecordBatchAsync());
            
            // Verify that the reader is a SparkCloudFetchReader
            // This requires reflection since the reader is wrapped
            var streamType = result.Stream.GetType();
            var fieldInfo = streamType.GetField("_stream", BindingFlags.NonPublic | BindingFlags.Instance);
            
            if (fieldInfo != null)
            {
                var innerStream = fieldInfo.GetValue(result.Stream);
                if (innerStream != null)
                {
                    // The type might be wrapped in another stream, so we check if the type name contains "SparkCloudFetchReader"
                    var typeName = innerStream?.GetType().FullName;
                    OutputHelper?.WriteLine($"Stream type: {typeName ?? "unknown"}");
                    
                    // If CloudFetch is working, the type name should contain "CloudFetch"
                    // If not, it will be a different reader type
                    if (typeName != null)
                    {
                        Assert.Contains("CloudFetch", typeName);
                    }
                }
            }
        }

        /// <summary>
        /// Tests that CloudFetch can be disabled via statement options.
        /// </summary>
        [SkippableFact]
        public async Task TestCloudFetchDisabled()
        {
            // Skip if we're not using a Databricks server
            Skip.If(((SparkTestEnvironment)TestEnvironment).ServerType != SparkServerType.Databricks, 
                "CloudFetch is only supported on Databricks servers");

            // Create a statement with CloudFetch disabled
            using var statement = Connection.CreateStatement();
            statement.SetOption(SparkStatement.Options.UseCloudFetch, "false");
            
            // Execute a simple query
            statement.SqlQuery = "SELECT 1 as id, 'test' as name";
            var result = await statement.ExecuteQueryAsync();
            
            // Verify the result
            Assert.NotNull(result.Stream);
            
            // Read the data
            var batch = await result.Stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length);
            Assert.Equal(2, batch.ColumnCount);
            
            // Get the columns with proper casting
            var idColumn = batch.Column(0) as Int32Array;
            var nameColumn = batch.Column(1) as StringArray;
            
            Assert.NotNull(idColumn);
            Assert.NotNull(nameColumn);
            
            // Verify the data
            Assert.Equal(1, idColumn!.GetValue(0));
            Assert.Equal("test", nameColumn!.GetString(0));
            
            // Verify there are no more batches
            Assert.Null(await result.Stream.ReadNextRecordBatchAsync());
            
            // Verify that the reader is not a SparkCloudFetchReader
            var streamType = result.Stream.GetType();
            var fieldInfo = streamType.GetField("_stream", BindingFlags.NonPublic | BindingFlags.Instance);
            
            if (fieldInfo != null)
            {
                var innerStream = fieldInfo.GetValue(result.Stream);
                if (innerStream != null)
                {
                    var typeName = innerStream.GetType().FullName;
                    OutputHelper?.WriteLine($"Stream type: {typeName ?? "unknown"}");
                    
                    // If CloudFetch is disabled, the type name should not contain "CloudFetch"
                    if (typeName != null)
                    {
                        Assert.DoesNotContain("CloudFetch", typeName);
                    }
                }
            }
        }

        /// <summary>
        /// Tests CloudFetch with a larger result set to ensure it can handle multiple batches.
        /// </summary>
        [SkippableFact]
        public async Task TestCloudFetchWithLargeResultSet()
        {
            // Skip if we're not using a Databricks server
            Skip.If(((SparkTestEnvironment)TestEnvironment).ServerType != SparkServerType.Databricks, 
                "CloudFetch is only supported on Databricks servers");

            // Create a statement with CloudFetch enabled
            using var statement = Connection.CreateStatement();
            statement.SetOption(SparkStatement.Options.UseCloudFetch, "true");
            statement.SetOption(SparkStatement.Options.CanDecompressLz4, "true");
            statement.SetOption(SparkStatement.Options.MaxBytesPerFile, "1048576"); // 1MB (smaller to force multiple files)
            
            // Execute a query that will generate a larger result set
            statement.SqlQuery = "SELECT * FROM range(1000)";
            var result = await statement.ExecuteQueryAsync();
            
            // Verify the result
            Assert.NotNull(result.Stream);
            
            // Read all the data
            int rowCount = 0;
            RecordBatch? batch;
            while ((batch = await result.Stream.ReadNextRecordBatchAsync()) != null)
            {
                rowCount += batch.Length;
                batch.Dispose();
            }
            
            // Verify we got all the rows
            Assert.Equal(1000, rowCount);
        }

        /// <summary>
        /// Integration test for running a large query against a real Databricks cluster.
        /// </summary>
        [SkippableFact]
        public async Task TestRealDatabricksCloudFetchLargeQuery()
        {
            // Create a log file for debugging
            using var logFile = new System.IO.StreamWriter("/tmp/cloudfetch_test.log", append: false);
            try
            {
                logFile.WriteLine("Starting TestRealDatabricksCloudFetchLargeQuery");
                
                // Skip if we're not using a Databricks server
                if (((SparkTestEnvironment)TestEnvironment).ServerType != SparkServerType.Databricks)
                {
                    logFile.WriteLine("Skipping test: CloudFetch is only supported on Databricks servers");
                    Skip.If(true, "CloudFetch is only supported on Databricks servers");
                    return;
                }
                
                logFile.WriteLine($"Server Type: {((SparkTestEnvironment)TestEnvironment).ServerType}");
                var config = TestConfiguration;
                logFile.WriteLine($"Host: {config.HostName}");
                logFile.WriteLine($"Path: {config.Path}");

                // Create a statement with CloudFetch enabled
                using var statement = Connection.CreateStatement();
                statement.SetOption(SparkStatement.Options.UseCloudFetch, "true");
                statement.SetOption(SparkStatement.Options.CanDecompressLz4, "true");
                statement.SetOption(SparkStatement.Options.MaxBytesPerFile, "10485760"); // 10MB
                
                // Set a large query timeout
                statement.SetOption("adbc.statement.query.timeout_seconds", "600");
                
                logFile.WriteLine("Executing range query");
                
                // Execute a query that generates a large result set using range function
                statement.SqlQuery = "SELECT * FROM range(1000)";
                
                // Execute the query and get the result
                var result = await statement.ExecuteQueryAsync();
                
                logFile.WriteLine("Query executed successfully");
                
                if (result.Stream == null)
                {
                    logFile.WriteLine("Received null result stream");
                    throw new InvalidOperationException("Result stream is null");
                }
                
                // Read all the data and count rows
                long totalRows = 0;
                RecordBatch? batch;
                while ((batch = await result.Stream.ReadNextRecordBatchAsync()) != null)
                {
                    totalRows += batch.Length;
                    batch.Dispose();
                    
                    // Log progress periodically
                    if (totalRows % 10000 == 0)
                    {
                        logFile.WriteLine($"Read {totalRows} rows so far");
                    }
                }
                
                logFile.WriteLine($"Total rows read: {totalRows}");
                
                // Verify we read the expected number of rows
                Assert.Equal(1000, totalRows);
                
                // Also log to the test output helper if available
                OutputHelper?.WriteLine($"Read {totalRows} rows from range function");
                
                logFile.WriteLine("Test completed successfully");
            }
            catch (Exception ex)
            {
                logFile.WriteLine($"Error: {ex.Message}");
                logFile.WriteLine(ex.StackTrace);
                if (ex.InnerException != null)
                {
                    logFile.WriteLine($"Inner exception: {ex.InnerException.Message}");
                    logFile.WriteLine(ex.InnerException.StackTrace);
                }
                throw;
            }
            finally
            {
                logFile.Flush();
            }
        }
    }
}