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
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Apache.Arrow.Adbc.Tests.Xunit;
using Apache.Hive.Service.Rpc.Thrift;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Databricks
{
    /// <summary>
    /// Experimental tests to understand CloseOperation behavior in Databricks
    /// </summary>
    public class CloseOperationExperimentTests : TestBase<DatabricksTestConfiguration, DatabricksTestEnvironment>
    {
        public CloseOperationExperimentTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new DatabricksTestEnvironment.Factory())
        {
        }

        /// <summary>
        /// Test 1: CloseOperation while operation is still executing
        /// Expected: Operation should stop executing on the server
        /// </summary>
        [SkippableFact]
        public async Task Test1_CloseOperation_DuringExecution()
        {
            OutputHelper?.WriteLine("=== Test 1: CloseOperation during execution ===");

            using AdbcConnection connection = NewConnection();
            var statement = (DatabricksStatement)connection.CreateStatement();

            try
            {
                // Create a very large query that will take a long time to execute
                // 1M x 1M cross join = 1 trillion rows
                statement.SqlQuery = @"
                    SELECT t1.id AS id1, t2.id AS id2
                    FROM RANGE(100000000) t1
                    CROSS JOIN RANGE(100000000) t2";

                OutputHelper?.WriteLine($"Starting query execution at {DateTime.Now:HH:mm:ss.fff}");

                // Start the query but don't wait for results
                var queryTask = statement.ExecuteQueryAsync();

                // Give it a moment to start executing on the server
                await Task.Delay(10000);

                OutputHelper?.WriteLine($"Calling CloseOperation at {DateTime.Now:HH:mm:ss.fff}");

                // Call CloseOperation while it's still executing. Comment this to verify CloseOperation is stopping operation
                var closeResponse = await statement.TestCloseOperationAsync();

                OutputHelper?.WriteLine($"CloseOperation response: StatusCode={closeResponse.Status?.StatusCode}");
                if (closeResponse.Status?.InfoMessages != null)
                {
                    foreach (var msg in closeResponse.Status.InfoMessages)
                    {
                        OutputHelper?.WriteLine($"  Info: {msg}");
                    }
                }

                // Now check what happens with the original query
                try
                {
                    var result = await queryTask;
                    OutputHelper?.WriteLine("Query task completed after CloseOperation");

                    // Try to read data
                    if (result.Stream != null)
                    {
                        var batch = await result.Stream.ReadNextRecordBatchAsync();
                        OutputHelper?.WriteLine($"Was able to read batch: {batch != null} (rows: {batch?.Length})");
                    }
                }
                catch (Exception ex)
                {
                    OutputHelper?.WriteLine($"Query task threw exception after CloseOperation: {ex.GetType().Name}: {ex.Message}");
                }
            }
            finally
            {
                statement.Dispose();
            }

            OutputHelper?.WriteLine("=== Test 1 Complete ===\n");
        }

        /// <summary>
        /// Test 2: CloseOperation while consuming results (operation completed but data still being fetched)
        /// Expected: Should prevent further consumption of results
        /// </summary>
        [SkippableFact]
        public async Task Test2_CloseOperation_DuringConsumption()
        {
            OutputHelper?.WriteLine("=== Test 2: CloseOperation during result consumption ===");

            using AdbcConnection connection = NewConnection();
            var statement = (DatabricksStatement)connection.CreateStatement();

            try
            {
                // Use a query that completes quickly but returns many rows
                statement.SqlQuery = @"
                    SELECT 
                        id,
                        CAST(id AS STRING) as id_str,
                        id * 2 as doubled
                    FROM RANGE(100000000)"; // 100 million rows

                OutputHelper?.WriteLine($"Executing query at {DateTime.Now:HH:mm:ss.fff}");

                var result = await statement.ExecuteQueryAsync();
                Assert.NotNull(result.Stream);

                OutputHelper?.WriteLine($"Query executed, consuming ALL results at {DateTime.Now:HH:mm:ss.fff}");

                // Read ALL batches until the end
                int batchesRead = 0;
                int totalRows = 0;

                while (true)
                {
                    var batch = await result.Stream.ReadNextRecordBatchAsync();
                    if (batch == null)
                    {
                        OutputHelper?.WriteLine("Reached end of result stream");
                        break;
                    }
                    
                    batchesRead++;
                    totalRows += batch.Length;
                    
                    // Log every 10th batch to avoid too much output
                    if (batchesRead % 10 == 0 || batchesRead == 1)
                    {
                        OutputHelper?.WriteLine($"Batch {batchesRead}: {batch.Length} rows (total so far: {totalRows})");
                    }
                    
                    batch.Dispose();
                }

                OutputHelper?.WriteLine($"Read {batchesRead} batches with {totalRows} rows total");
                OutputHelper?.WriteLine($"Calling CloseOperation at {DateTime.Now:HH:mm:ss.fff}");

                // Now close the operation while there's still data to consume
                var closeResponse = await statement.TestCloseOperationAsync();

                OutputHelper?.WriteLine($"CloseOperation response: StatusCode={closeResponse.Status?.StatusCode}");
                if (closeResponse.Status?.InfoMessages != null)
                {
                    foreach (var msg in closeResponse.Status.InfoMessages)
                    {
                        OutputHelper?.WriteLine($"  Info: {msg}");
                    }
                }

                // Try to read more batches after CloseOperation
                OutputHelper?.WriteLine("Attempting to read more batches after CloseOperation...");

                try
                {
                    for (int i = 0; i < 3; i++)
                    {
                        var batch = await result.Stream.ReadNextRecordBatchAsync();
                        if (batch != null)
                        {
                            OutputHelper?.WriteLine($"Unexpectedly read batch after close: {batch.Length} rows");
                            batch.Dispose();
                        }
                        else
                        {
                            OutputHelper?.WriteLine("Received null batch after CloseOperation");
                            break;
                        }
                    }
                }
                catch (Exception ex)
                {
                    OutputHelper?.WriteLine($"Exception when reading after CloseOperation: {ex.GetType().Name}: {ex.Message}");
                }
            }
            finally
            {
                statement.Dispose();
            }

            OutputHelper?.WriteLine("=== Test 2 Complete ===\n");
        }
    }
}