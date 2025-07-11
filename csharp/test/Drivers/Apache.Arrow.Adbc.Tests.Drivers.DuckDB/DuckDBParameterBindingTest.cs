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
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.DuckDB;
using Apache.Arrow.Ipc;
using Apache.Arrow.Memory;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.DuckDB
{
    public class DuckDBParameterBindingTest : IAsyncLifetime
    {
        private DuckDBDriver? _driver;
        private AdbcDatabase? _database;
        private AdbcConnection? _connection;
        private readonly NativeMemoryAllocator _allocator = new NativeMemoryAllocator();

        public async Task InitializeAsync()
        {
            _driver = new DuckDBDriver();
            _database = _driver.Open(new Dictionary<string, string>
            {
                { DuckDBParameters.DataSource, ":memory:" }
            });
            _connection = _database.Connect(null);
            
            // Create test table
            using var statement = _connection.CreateStatement();
            statement.SqlQuery = @"
                CREATE TABLE test_table (
                    id INTEGER,
                    name VARCHAR,
                    value DOUBLE,
                    active BOOLEAN,
                    created_date DATE,
                    data BLOB
                )";
            await statement.ExecuteUpdateAsync();
        }

        public async Task DisposeAsync()
        {
            _connection?.Dispose();
            _database?.Dispose();
            await Task.CompletedTask;
        }

        [Fact]
        public async Task CanBindSimpleParameters()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = "INSERT INTO test_table (id, name, value, active) VALUES (?, ?, ?, ?)";

            // Create parameter batch
            var batch = new RecordBatch.Builder(_allocator)
                .Append("id", false, col => col.Int32(array => array.Append(1).Append(2).Append(3)))
                .Append("name", false, col => col.String(array => array.Append("Alice").Append("Bob").Append("Charlie")))
                .Append("value", false, col => col.Double(array => array.Append(100.5).Append(200.75).Append(300.25)))
                .Append("active", false, col => col.Boolean(array => array.Append(true).Append(false).Append(true)))
                .Build();

            statement.Bind(batch, batch.Schema);
            var result = await statement.ExecuteUpdateAsync();
            
            Assert.Equal(3, result.AffectedRows);

            // Verify the data was inserted
            statement.SqlQuery = "SELECT * FROM test_table ORDER BY id";
            var queryResult = await statement.ExecuteQueryAsync();
            using var stream = queryResult.Stream;
            
            var resultBatch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(resultBatch);
            Assert.Equal(3, resultBatch!.Length);
            
            var idArray = (Int32Array)resultBatch.Column(0);
            Assert.Equal(1, idArray.GetValue(0));
            Assert.Equal(2, idArray.GetValue(1));
            Assert.Equal(3, idArray.GetValue(2));
            
            var nameArray = (StringArray)resultBatch.Column(1);
            Assert.Equal("Alice", nameArray.GetString(0));
            Assert.Equal("Bob", nameArray.GetString(1));
            Assert.Equal("Charlie", nameArray.GetString(2));
            
            batch.Dispose();
        }

        [Fact]
        public async Task CanBindWithNulls()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)";

            // Create parameter batch with nulls
            var batch = new RecordBatch.Builder(_allocator)
                .Append("id", false, col => col.Int32(array => array.Append(4).AppendNull().Append(6)))
                .Append("name", false, col => col.String(array => array.AppendNull().Append("Eve").AppendNull()))
                .Append("value", false, col => col.Double(array => array.Append(400.0).AppendNull().Append(600.0)))
                .Build();

            statement.Bind(batch, batch.Schema);
            var result = await statement.ExecuteUpdateAsync();
            
            Assert.Equal(3, result.AffectedRows);

            // Verify nulls were handled correctly
            statement.SqlQuery = "SELECT * FROM test_table WHERE id >= 4 OR id IS NULL ORDER BY id";
            var queryResult = await statement.ExecuteQueryAsync();
            using var stream = queryResult.Stream;
            
            var resultBatch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(resultBatch);
            Assert.Equal(3, resultBatch!.Length);
            
            // Check nulls
            var idArray = (Int32Array)resultBatch.Column(0);
            Assert.Equal(4, idArray.GetValue(0));
            Assert.True(idArray.IsNull(1));
            Assert.Equal(6, idArray.GetValue(2));
            
            var nameArray = (StringArray)resultBatch.Column(1);
            Assert.True(nameArray.IsNull(0));
            Assert.Equal("Eve", nameArray.GetString(1));
            Assert.True(nameArray.IsNull(2));
            
            batch.Dispose();
        }

        [Fact]
        public async Task CanBindDateTimeTypes()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = "INSERT INTO test_table (id, created_date) VALUES (?, ?)";

            var date1 = new DateTime(2024, 1, 15);
            var date2 = new DateTime(2024, 6, 30);
            var date3 = new DateTime(2024, 12, 31);

            // Create parameter batch with dates
            var batch = new RecordBatch.Builder(_allocator)
                .Append("id", false, col => col.Int32(array => array.Append(7).Append(8).Append(9)))
                .Append("created_date", false, col => col.Date32(array => 
                    array.Append(date1).Append(date2).Append(date3)))
                .Build();

            statement.Bind(batch, batch.Schema);
            var result = await statement.ExecuteUpdateAsync();
            
            Assert.Equal(3, result.AffectedRows);

            // Verify dates
            statement.SqlQuery = "SELECT id, created_date FROM test_table WHERE id >= 7 ORDER BY id";
            var queryResult = await statement.ExecuteQueryAsync();
            using var stream = queryResult.Stream;
            
            var resultBatch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(resultBatch);
            Assert.Equal(3, resultBatch!.Length);
            
            var dateArray = (Date32Array)resultBatch.Column(1);
            Assert.Equal(date1.Date, dateArray.GetDateTime(0)!.Value.Date);
            Assert.Equal(date2.Date, dateArray.GetDateTime(1)!.Value.Date);
            Assert.Equal(date3.Date, dateArray.GetDateTime(2)!.Value.Date);
            
            batch.Dispose();
        }

        [Fact]
        public async Task CanBindBinaryData()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = "INSERT INTO test_table (id, data) VALUES (?, ?)";

            var data1 = new byte[] { 0x01, 0x02, 0x03 };
            var data2 = new byte[] { 0xFF, 0xEE, 0xDD, 0xCC };
            var data3 = new byte[] { };

            // Create parameter batch with binary data
            var batch = new RecordBatch.Builder(_allocator)
                .Append("id", false, col => col.Int32(array => array.Append(10).Append(11).Append(12)))
                .Append("data", false, col => col.Binary(array => 
                    array.Append(data1.AsSpan()).Append(data2.AsSpan()).Append(data3.AsSpan())))
                .Build();

            statement.Bind(batch, batch.Schema);
            var result = await statement.ExecuteUpdateAsync();
            
            Assert.Equal(3, result.AffectedRows);

            // Verify binary data
            statement.SqlQuery = "SELECT id, data FROM test_table WHERE id >= 10 ORDER BY id";
            var queryResult = await statement.ExecuteQueryAsync();
            using var stream = queryResult.Stream;
            
            var resultBatch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(resultBatch);
            Assert.Equal(3, resultBatch!.Length);
            
            var binaryArray = (BinaryArray)resultBatch.Column(1);
            Assert.Equal(data1, binaryArray.GetBytes(0).ToArray());
            Assert.Equal(data2, binaryArray.GetBytes(1).ToArray());
            Assert.Equal(data3, binaryArray.GetBytes(2).ToArray());
            
            batch.Dispose();
        }

        [Fact]
        public async Task CanBindStream()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = "INSERT INTO test_table (id, name, value) VALUES (?, ?, ?)";

            // Create multiple batches
            var batches = new List<RecordBatch>();
            
            // Batch 1
            batches.Add(new RecordBatch.Builder(_allocator)
                .Append("id", false, col => col.Int32(array => array.Append(13).Append(14)))
                .Append("name", false, col => col.String(array => array.Append("Batch1-1").Append("Batch1-2")))
                .Append("value", false, col => col.Double(array => array.Append(1000.0).Append(2000.0)))
                .Build());
            
            // Batch 2
            batches.Add(new RecordBatch.Builder(_allocator)
                .Append("id", false, col => col.Int32(array => array.Append(15).Append(16)))
                .Append("name", false, col => col.String(array => array.Append("Batch2-1").Append("Batch2-2")))
                .Append("value", false, col => col.Double(array => array.Append(3000.0).Append(4000.0)))
                .Build());

            // Create stream from batches
            var schema = batches[0].Schema;
            using var stream = new MockArrowArrayStream(schema, batches);
            
            statement.BindStream(stream);
            var result = await statement.ExecuteUpdateAsync();
            
            Assert.Equal(4, result.AffectedRows); // 2 rows per batch * 2 batches

            // Verify all data was inserted
            statement.SqlQuery = "SELECT COUNT(*) FROM test_table WHERE id >= 13";
            var queryResult = await statement.ExecuteQueryAsync();
            await using var resultStream = queryResult.Stream;
            
            var resultBatch = await resultStream.ReadNextRecordBatchAsync();
            Assert.NotNull(resultBatch);
            var countArray = (Int64Array)resultBatch!.Column(0);
            Assert.Equal(4, countArray.GetValue(0));
            
            foreach (var batch in batches)
            {
                batch.Dispose();
            }
        }

        [Fact]
        public async Task CanHandleEmptyBatch()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = "INSERT INTO test_table (id, name) VALUES (?, ?)";

            // Create empty batch
            var batch = new RecordBatch.Builder(_allocator)
                .Append("id", false, col => col.Int32(array => { })) // Empty array
                .Append("name", false, col => col.String(array => { })) // Empty array
                .Build();

            statement.Bind(batch, batch.Schema);
            var result = await statement.ExecuteUpdateAsync();
            
            Assert.Equal(0, result.AffectedRows);
            
            batch.Dispose();
        }

        [Fact]
        public async Task CanReuseStatementWithDifferentBindings()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = "INSERT INTO test_table (id, name) VALUES (?, ?)";

            // First binding
            var batch1 = new RecordBatch.Builder(_allocator)
                .Append("id", false, col => col.Int32(array => array.Append(17)))
                .Append("name", false, col => col.String(array => array.Append("First")))
                .Build();

            statement.Bind(batch1, batch1.Schema);
            var result1 = await statement.ExecuteUpdateAsync();
            Assert.Equal(1, result1.AffectedRows);

            // Second binding - reuse same statement
            var batch2 = new RecordBatch.Builder(_allocator)
                .Append("id", false, col => col.Int32(array => array.Append(18).Append(19)))
                .Append("name", false, col => col.String(array => array.Append("Second").Append("Third")))
                .Build();

            statement.Bind(batch2, batch2.Schema);
            var result2 = await statement.ExecuteUpdateAsync();
            Assert.Equal(2, result2.AffectedRows);

            // Verify both sets were inserted
            statement.SqlQuery = "SELECT COUNT(*) FROM test_table WHERE id >= 17";
            var queryResult = await statement.ExecuteQueryAsync();
            using var stream = queryResult.Stream;
            
            var resultBatch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(resultBatch);
            var countArray = (Int64Array)resultBatch!.Column(0);
            Assert.Equal(3, countArray.GetValue(0));
            
            batch1.Dispose();
            batch2.Dispose();
        }

        // Helper class for creating Arrow streams
        private class MockArrowArrayStream : IArrowArrayStream
        {
            private readonly Schema _schema;
            private readonly IEnumerator<RecordBatch> _enumerator;

            public MockArrowArrayStream(Schema schema, IEnumerable<RecordBatch> batches)
            {
                _schema = schema;
                _enumerator = batches.GetEnumerator();
            }

            public Schema Schema => _schema;

            public RecordBatch Current => _enumerator.Current;

            public bool MoveNext() => _enumerator.MoveNext();

            public void Dispose() => _enumerator.Dispose();

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                return ValueTask.FromResult(MoveNext() ? Current : null);
            }
        }
    }
}