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
using System.Linq;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache;
using Apache.Arrow.Adbc.Tests.Drivers.Apache.Common;
using Xunit;
using Xunit.Abstractions;

namespace Apache.Arrow.Adbc.Tests.Drivers.Apache.Spark
{
    public class StatementTests : Common.StatementTests<SparkTestConfiguration, SparkTestEnvironment>
    {
        public StatementTests(ITestOutputHelper? outputHelper)
            : base(outputHelper, new SparkTestEnvironment.Factory())
        {
        }

        [SkippableTheory]
        [ClassData(typeof(LongRunningStatementTimeoutTestData))]
        internal override void StatementTimeoutTest(StatementWithExceptions statementWithExceptions)
        {
            base.StatementTimeoutTest(statementWithExceptions);
        }

        [SkippableFact]
        public async void PrefetchingReturnCorrectResult()
        {
            // This test specifically checks that prefetch with multiple batches return correct results

            // Get a new connection and create a statement
            using AdbcConnection connection = NewConnection();
            using AdbcStatement statement = connection.CreateStatement();

            // Set a small batch size to ensure multiple batches
            statement.SetOption(ApacheParameters.BatchSize, "5");

            // Execute a query that returns multiple batches
            // We need multiple columns to ensure the Arrow reader has work to do
            statement.SqlQuery = "SELECT id, CAST(id AS STRING) as id_string, id * 2 as id_doubled FROM RANGE(100)";
            QueryResult result = statement.ExecuteQuery();


            // In SparkDatabricksReader, prefetching happens:
            // 1. When index == 0 in ProcessFetchedBatches
            // 2. When we first start reading

            // So let's read a few batches with some pauses to simulate processing
            // and ensure everything works correctly
            OutputHelper?.WriteLine("Starting batch processing with pauses (simulating real app):");

            var batches = new List<RecordBatch>();
            try
            {
                for (int i = 0; i < 5; i++)
                {
                    if (result.Stream == null)
                        return;
                    // The read operation should trigger prefetching of the next batch
                    var batch = await result.Stream.ReadNextRecordBatchAsync();
                    if (batch == null)
                    {
                        OutputHelper?.WriteLine($"Batch {i}: End of data reached");
                        break;
                    }

                    batches.Add(batch);
                    OutputHelper?.WriteLine($"Batch {i}: Read {batch.Length} rows");

                    // Simulate processing time - this gives time for prefetching to occur
                    System.Threading.Thread.Sleep(20);
                }

                // Verify we got batches
                Assert.True(batches.Count > 0, "Should have received at least one batch");
                Assert.True(batches.All(b => b.Length > 0), "All batches should have rows");

                // Calculate total rows read
                int totalRows = batches.Sum(b => b.Length);
                OutputHelper?.WriteLine($"Read {totalRows} rows in {batches.Count} batches");

                // Continue reading the remaining batches
                int remainingBatches = 0;
                int remainingRows = 0;

                while (true)
                {
                    if (result.Stream == null)
                        return;
                    using var batch = await result.Stream.ReadNextRecordBatchAsync();
                    if (batch == null)
                        break;

                    remainingBatches++;
                    remainingRows += batch.Length;
                }

                OutputHelper?.WriteLine($"Read remaining {remainingRows} rows in {remainingBatches} batches");

                // Verify we read all 100 rows total
                Assert.Equal(100, totalRows + remainingRows);
            }
            finally
            {
                // Clean up the batches we kept
                foreach (var batch in batches)
                {
                    batch.Dispose();
                }
            }
        }

        internal class LongRunningStatementTimeoutTestData : ShortRunningStatementTimeoutTestData
        {
            public LongRunningStatementTimeoutTestData()
            {
                string longRunningQuery = "SELECT COUNT(*) AS total_count\nFROM (\n  SELECT t1.id AS id1, t2.id AS id2\n  FROM RANGE(1000000) t1\n  CROSS JOIN RANGE(100000) t2\n) subquery\nWHERE MOD(id1 + id2, 2) = 0";

                Add(new(5, longRunningQuery, typeof(TimeoutException)));
                Add(new(null, longRunningQuery, typeof(TimeoutException)));
                Add(new(0, longRunningQuery, null));
            }
        }
    }
}
