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
using Apache.Arrow.Adbc.Drivers.Apache.Spark;
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

        [SkippableTheory]
        [InlineData(true, "CloudFetch enabled")]
        [InlineData(false, "CloudFetch disabled")]
        public async Task LZ4DecompressionCapabilityTest(bool useCloudFetch, string configName)
        {
            OutputHelper?.WriteLine($"Testing with LZ4 decompression capability enabled ({configName})");

            // Create a connection using the test configuration
            using AdbcConnection connection = NewConnection();
            using var statement = connection.CreateStatement();

            // Set options for LZ4 decompression (enabled by default) and CloudFetch as specified
            statement.SetOption(SparkStatement.Options.UseCloudFetch, useCloudFetch.ToString().ToLower());
            OutputHelper?.WriteLine($"CloudFetch is {(useCloudFetch ? "enabled" : "disabled")}");
            OutputHelper?.WriteLine("LZ4 decompression capability is enabled by default");

            // Execute a query that should return data
            statement.SqlQuery = "SELECT id, CAST(id AS STRING) as id_string, id * 2 as id_doubled FROM RANGE(100)";
            QueryResult result = statement.ExecuteQuery();

            // Verify we have a valid stream
            Assert.NotNull(result.Stream);

            // Read all batches
            int totalRows = 0;
            int batchCount = 0;

            while (result.Stream != null)
            {
                using var batch = await result.Stream.ReadNextRecordBatchAsync();
                if (batch == null)
                    break;

                batchCount++;
                totalRows += batch.Length;
                OutputHelper?.WriteLine($"Batch {batchCount}: Read {batch.Length} rows");
            }

            // Verify we got all rows
            Assert.Equal(100, totalRows);
            OutputHelper?.WriteLine($"Successfully read {totalRows} rows in {batchCount} batches with {configName}");
            OutputHelper?.WriteLine("NOTE: Whether actual LZ4 compression was used is determined by the server");
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

        [SkippableFact]
        public async Task CanGetPrimaryKeysDatabricks()
        {
            Skip.If(TestEnvironment.ServerType != SparkServerType.Databricks);
            await base.CanGetPrimaryKeys(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        [SkippableFact]
        public async Task CanGetCrossReferenceFromParentTableDatabricks()
        {
            Skip.If(TestEnvironment.ServerType != SparkServerType.Databricks);
            await base.CanGetCrossReferenceFromParentTable(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        [SkippableFact]
        public async Task CanGetCrossReferenceFromChildTableDatabricks()
        {
            Skip.If(TestEnvironment.ServerType != SparkServerType.Databricks);
            await base.CanGetCrossReferenceFromChildTable(TestConfiguration.Metadata.Catalog, TestConfiguration.Metadata.Schema);
        }

        protected override void PrepareCreateTableWithPrimaryKeys(out string sqlUpdate, out string tableNameParent, out string fullTableNameParent, out IReadOnlyList<string> primaryKeys)
        {
            CreateNewTableName(out tableNameParent, out fullTableNameParent);
            sqlUpdate = $"CREATE TABLE IF NOT EXISTS {fullTableNameParent} (INDEX INT, NAME STRING, PRIMARY KEY (INDEX, NAME))";
            primaryKeys = ["index", "name"];
        }
    }
}
