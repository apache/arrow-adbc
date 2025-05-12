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
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Databricks;
using Apache.Arrow.Adbc.Tests.Xunit;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    public class LongRunningQueryTest : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public LongRunningQueryTest(ITestOutputHelper? outputHelper) : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        [SkippableFact]
        public async Task LongRunningQueryWithHeartbeats()
        {
            // Skip if heartbeat sleep time is not configured
            Skip.IfNot(TestConfiguration.HeartbeatSleepMs > 0, "Heartbeat sleep time not configured");

            // Create a connection with a short heartbeat interval for testing
            using var connection = NewConnection();

            // Create a statement
            using var statement = connection.CreateStatement();

            // Execute a long-running query that will take more than the heartbeat interval
            statement.SqlQuery = $@"
                SELECT sleep_udf({TestConfiguration.HeartbeatSleepMs}) AS slept
            ";

            // Execute the query and verify we get results
            var result = statement.ExecuteQuery();
            Assert.NotNull(result.Stream);

            // Read the results
            var batch = await result.Stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch.Length); // Should have one row with the count

            // Verify the count is what we expect
            var count = batch.Column(0).GetValue(0);
            Assert.NotNull(count);
            OutputHelper?.WriteLine($"Query completed successfully with count: {count}");
        }

        [SkippableTheory]
        [InlineData(true)]  // With CloudFetch
        [InlineData(false)] // Without CloudFetch
        public async Task LongRunningFetchWithHeartbeats(bool useCloudFetch)
        {
            // Skip if heartbeat sleep time is not configured
            Skip.IfNot(TestConfiguration.HeartbeatSleepMs > 0, "Heartbeat sleep time not configured");

            // Create a connection with a short heartbeat interval for testing
            using var connection = NewConnection();
            connection.Properties[DatabricksParameters.UseCloudFetch] = useCloudFetch.ToString().ToLower();
            OutputHelper?.WriteLine($"Testing with CloudFetch {(useCloudFetch ? "enabled" : "disabled")}");

            // Create a statement
            using var statement = connection.CreateStatement();

            // Execute a query that generates a large dataset (enough to trigger CloudFetch)
            statement.SqlQuery = @"
                SELECT id, CONCAT('string_', id) AS str_value, id * 1.5 AS float_value, id % 2 = 0 AS bool_value
                FROM RANGE(1000000)
            ";

            OutputHelper?.WriteLine("Executing query...");
            var result = statement.ExecuteQuery();
            Assert.NotNull(result.Stream);

            // Read the first batch
            OutputHelper?.WriteLine("Reading first batch...");
            var batch = await result.Stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.True(batch.Length > 0, "Should have received at least one row");
            OutputHelper?.WriteLine($"Successfully read first batch of {batch.Length} rows");

            // Simulate a very slow consumer by waiting for the configured time
            int waitTimeMs = TestConfiguration.HeartbeatSleepMs;
            OutputHelper?.WriteLine($"Starting long wait period of {waitTimeMs}ms to simulate slow consumer...");
            
            // Log progress every 5 seconds
            int logIntervalMs = 5000;
            int elapsedMs = 0;
            while (elapsedMs < waitTimeMs)
            {
                await Task.Delay(logIntervalMs);
                elapsedMs += logIntervalMs;
                OutputHelper?.WriteLine($"Still waiting... {elapsedMs}ms elapsed");
            }

            OutputHelper?.WriteLine("Wait period completed, resuming data fetching...");

            // Read all remaining batches
            int totalRows = batch.Length;
            int batchCount = 1;
            while (true)
            {
                batch = await result.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                    break;

                batchCount++;
                totalRows += batch.Length;
                OutputHelper?.WriteLine($"Read batch {batchCount} with {batch.Length} rows");
            }

            Assert.True(totalRows == 1000000, "Should have read 1 million rows");
            OutputHelper?.WriteLine($"Successfully completed long-running fetch test, read {totalRows} total rows in {batchCount} batches");
        }
    }
} 