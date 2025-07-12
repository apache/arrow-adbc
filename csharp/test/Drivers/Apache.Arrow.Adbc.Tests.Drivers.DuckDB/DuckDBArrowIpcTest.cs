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
using Apache.Arrow.Adbc.Drivers.DuckDB;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.DuckDB
{
    public class DuckDBArrowIpcTest : IAsyncLifetime
    {
        private DuckDBDriver? _driver;
        private AdbcDatabase? _database;
        private AdbcConnection? _connection;

        public async Task InitializeAsync()
        {
            _driver = new DuckDBDriver();
            _database = _driver.Open(new Dictionary<string, string>
            {
                { DuckDBParameters.DataSource, ":memory:" }
            });
            _connection = _database.Connect(null);
            await Task.CompletedTask;
        }

        public async Task DisposeAsync()
        {
            _connection?.Dispose();
            _database?.Dispose();
            await Task.CompletedTask;
        }

        [Fact]
        public async Task TestArrowIpcQuery()
        {
            // Try to install the nanoarrow extension
            using (var statement = _connection!.CreateStatement())
            {
                try
                {
                    statement.SqlQuery = "INSTALL nanoarrow FROM community";
                    await statement.ExecuteUpdateAsync();
                    
                    statement.SqlQuery = "LOAD nanoarrow";
                    await statement.ExecuteUpdateAsync();
                }
                catch
                {
                    // Extension might not be available in CI
                    return;
                }
            }

            // Create test data
            using (var statement = _connection!.CreateStatement())
            {
                statement.SqlQuery = @"
                    CREATE TABLE test_ipc AS 
                    SELECT * FROM (VALUES 
                        (1, 'one', 1.1::DOUBLE),
                        (2, 'two', 2.2::DOUBLE),
                        (3, 'three', 3.3::DOUBLE)
                    ) t(id, name, value)";
                await statement.ExecuteUpdateAsync();
            }

            // Query with Arrow IPC enabled
            using (var statement = _connection!.CreateStatement())
            {
                // Ensure Arrow IPC is enabled
                statement.SetOption(DuckDBParameters.UseArrowIpc, "true");
                
                statement.SqlQuery = "SELECT * FROM test_ipc ORDER BY id";
                var result = await statement.ExecuteQueryAsync();
                
                using var stream = result.Stream;
                Assert.NotNull(stream);
                
                // Read all batches
                var rowCount = 0;
                RecordBatch? batch;
                while ((batch = await stream!.ReadNextRecordBatchAsync()) != null)
                {
                    using (batch)
                    {
                        rowCount += batch.Length;
                        
                        // Verify schema
                        Assert.Equal(3, batch.ColumnCount);
                        Assert.IsType<Int32Array>(batch.Column(0));
                        Assert.IsType<StringArray>(batch.Column(1));
                        Assert.IsType<DoubleArray>(batch.Column(2));
                    }
                }
                
                Assert.Equal(3, rowCount);
            }
        }

        [Fact]
        public async Task TestArrowIpcFallback()
        {
            // Query with Arrow IPC disabled (fallback mode)
            using (var statement = _connection!.CreateStatement())
            {
                // Disable Arrow IPC to test fallback
                statement.SetOption(DuckDBParameters.UseArrowIpc, "false");
                
                statement.SqlQuery = "SELECT 1 as id, 'test' as name, 3.14::DOUBLE as value";
                var result = await statement.ExecuteQueryAsync();
                
                using var stream = result.Stream;
                Assert.NotNull(stream);
                
                var batch = await stream!.ReadNextRecordBatchAsync();
                Assert.NotNull(batch);
                
                using (batch!)
                {
                    Assert.Equal(1, batch.Length);
                    Assert.Equal(3, batch.ColumnCount);
                    
                    // Verify values
                    var intArray = (Int32Array)batch.Column(0);
                    Assert.Equal(1, intArray.GetValue(0));
                    
                    var strArray = (StringArray)batch.Column(1);
                    Assert.Equal("test", strArray.GetString(0));
                    
                    var dblArray = (DoubleArray)batch.Column(2);
                    Assert.Equal(3.14, dblArray.GetValue(0)!.Value, 0.001);
                }
            }
        }
    }
}