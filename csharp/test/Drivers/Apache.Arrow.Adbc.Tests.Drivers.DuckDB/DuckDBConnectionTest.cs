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

using System.Collections.Generic;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.DuckDB;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests.Drivers.DuckDB
{
    public class DuckDBConnectionTest : IAsyncLifetime
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
        public async Task CanExecuteQuery()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = "SELECT 1 as id, 'test' as name, 3.14 as value";

            var result = await statement.ExecuteQueryAsync();
            using var stream = result.Stream;

            var batch = await stream!.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(3, batch!.ColumnCount);
            Assert.Equal(1, batch.Length);

            // Check column types
            Assert.Equal("id", batch.Schema.GetFieldByIndex(0).Name);
            Assert.Equal("name", batch.Schema.GetFieldByIndex(1).Name);
            Assert.Equal("value", batch.Schema.GetFieldByIndex(2).Name);

            // Check values
            var idArray = (Int32Array)batch.Column(0);
            Assert.Equal(1, idArray.GetValue(0));

            var nameArray = (StringArray)batch.Column(1);
            Assert.Equal("test", nameArray.GetString(0));

            var valueArray = (DoubleArray)batch.Column(2);
            Assert.Equal(3.14, valueArray.GetValue(0));

            // Should be no more batches
            var nextBatch = await stream.ReadNextRecordBatchAsync();
            Assert.Null(nextBatch);
        }

        [Fact]
        public async Task CanExecuteUpdate()
        {
            using var statement = _connection!.CreateStatement();
            
            // Create table
            statement.SqlQuery = "CREATE TABLE test_table (id INTEGER, name VARCHAR)";
            var createResult = await statement.ExecuteUpdateAsync();
            Assert.Equal(-1, createResult.AffectedRows); // DDL returns -1

            // Insert data
            statement.SqlQuery = "INSERT INTO test_table VALUES (1, 'test1'), (2, 'test2')";
            var insertResult = await statement.ExecuteUpdateAsync();
            Assert.Equal(2, insertResult.AffectedRows);

            // Update data
            statement.SqlQuery = "UPDATE test_table SET name = 'updated' WHERE id = 1";
            var updateResult = await statement.ExecuteUpdateAsync();
            Assert.Equal(1, updateResult.AffectedRows);

            // Delete data
            statement.SqlQuery = "DELETE FROM test_table WHERE id = 2";
            var deleteResult = await statement.ExecuteUpdateAsync();
            Assert.Equal(1, deleteResult.AffectedRows);
        }

        [Fact]
        public async Task CanGetTableTypes()
        {
            using var stream = _connection!.GetTableTypes();
            var batch = await stream!.ReadNextRecordBatchAsync();
            
            Assert.NotNull(batch);
            Assert.Equal(1, batch!.ColumnCount);
            Assert.Equal("table_type", batch.Schema.GetFieldByIndex(0).Name);
            
            var tableTypes = (StringArray)batch.Column(0);
            Assert.True(tableTypes.Length > 0);
        }

        [Fact]
        public async Task CanGetInfo()
        {
            var codes = new[]
            {
                AdbcInfoCode.DriverName,
                AdbcInfoCode.DriverVersion,
                AdbcInfoCode.VendorName,
                AdbcInfoCode.VendorVersion,
                AdbcInfoCode.VendorSql
            };

            using var stream = _connection!.GetInfo(codes);
            var batch = await stream!.ReadNextRecordBatchAsync();
            
            Assert.NotNull(batch);
            Assert.Equal(2, batch!.ColumnCount);
            Assert.Equal(codes.Length, batch.Length);
            
            var infoArray = (UInt32Array)batch.Column(0);
            Assert.Equal((uint)AdbcInfoCode.DriverName, infoArray.GetValue(0));
        }

        [Fact]
        public async Task CanHandleNullValues()
        {
            using var statement = _connection!.CreateStatement();
            statement.SqlQuery = "SELECT NULL as nullable_column, 42 as non_null_column";

            var result = await statement.ExecuteQueryAsync();
            using var stream = result.Stream;

            var batch = await stream!.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            
            var nullableArray = batch!.Column(0);
            Assert.True(nullableArray.IsNull(0));
            
            var nonNullArray = (Int32Array)batch.Column(1);
            Assert.False(nonNullArray.IsNull(0));
            Assert.Equal(42, nonNullArray.GetValue(0));
        }

        [Fact]
        public async Task CanHandleMultipleBatches()
        {
            using var statement = _connection!.CreateStatement();
            
            // Create a table with more rows than default batch size
            statement.SqlQuery = "CREATE TABLE numbers AS SELECT range AS num FROM range(3000)";
            await statement.ExecuteUpdateAsync();

            // Set batch size to 1000
            statement.SetOption(DuckDBParameters.BatchSize, "1000");
            statement.SqlQuery = "SELECT * FROM numbers ORDER BY num";

            var result = await statement.ExecuteQueryAsync();
            using var stream = result.Stream;

            var totalRows = 0;
            var batchCount = 0;

            while (await stream!.ReadNextRecordBatchAsync() is RecordBatch batch)
            {
                batchCount++;
                totalRows += batch.Length;
                
                // Verify batch size (except possibly the last batch)
                if (batchCount < 3)
                {
                    Assert.Equal(1000, batch.Length);
                }
            }

            Assert.Equal(3, batchCount);
            Assert.Equal(3000, totalRows);
        }

        [Fact]
        public async Task CanGetTableSchema()
        {
            using var statement = _connection!.CreateStatement();
            
            // Create a test table
            statement.SqlQuery = @"
                CREATE TABLE schema_test (
                    id INTEGER NOT NULL,
                    name VARCHAR(100),
                    value DOUBLE,
                    created_date DATE
                )";
            await statement.ExecuteUpdateAsync();

            // Get the schema
            var schema = _connection.GetTableSchema(null, null, "schema_test");
            
            Assert.Equal(4, schema.FieldsList.Count);
            
            Assert.Equal("id", schema.FieldsList[0].Name);
            Assert.Equal(Int32Type.Default.TypeId, schema.FieldsList[0].DataType.TypeId);
            
            Assert.Equal("name", schema.FieldsList[1].Name);
            Assert.Equal(StringType.Default.TypeId, schema.FieldsList[1].DataType.TypeId);
            
            Assert.Equal("value", schema.FieldsList[2].Name);
            Assert.Equal(DoubleType.Default.TypeId, schema.FieldsList[2].DataType.TypeId);
            
            Assert.Equal("created_date", schema.FieldsList[3].Name);
            Assert.Equal(Date32Type.Default.TypeId, schema.FieldsList[3].DataType.TypeId);
        }
    }
}