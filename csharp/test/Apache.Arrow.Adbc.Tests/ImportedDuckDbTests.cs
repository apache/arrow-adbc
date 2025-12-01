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
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    public class ImportedDuckDbTests : IClassFixture<DuckDbFixture>
    {
        readonly DuckDbFixture _duckDb;

        public ImportedDuckDbTests(DuckDbFixture duckDb)
        {
            _duckDb = duckDb;
        }

        [Fact]
        public async Task SimpleEndToEndTest()
        {
            using var database = _duckDb.OpenDatabase("test.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "CREATE TABLE integers(foo INTEGER, bar INTEGER);";
            statement.ExecuteUpdate();

            statement.SqlQuery = "INSERT INTO integers VALUES (3, 4), (5, 6), (7, 8);";
            statement.ExecuteUpdate();

            statement.SqlQuery = "SELECT * from integers";
            var results = statement.ExecuteQuery();

            using var stream = results.Stream;
            Assert.NotNull(stream);

            var schema = stream.Schema;
            Assert.Equal(2, schema.FieldsList.Count);
            Assert.Equal(ArrowTypeId.Int32, schema.FieldsList[0].DataType.TypeId);
            Assert.Equal(ArrowTypeId.Int32, schema.FieldsList[1].DataType.TypeId);

            var firstBatch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(firstBatch);
            Assert.Equal(3, firstBatch.Length);
            Int32Array? column = firstBatch.Column(0) as Int32Array;
            Assert.NotNull(column);
            Assert.Equal(3, column.Values[0]);
            Assert.Equal(5, column.Values[1]);
            Assert.Equal(7, column.Values[2]);

            var secondBatch = await stream.ReadNextRecordBatchAsync();
            Assert.Null(secondBatch);
        }

        [Fact]
        public void TransactionsTest()
        {
            using var database = _duckDb.OpenDatabase("transactions.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "CREATE TABLE test(column1 INTEGER);";
            statement.ExecuteUpdate();

            connection.AutoCommit = false;

            // Insert into connection1
            statement.SqlQuery = "INSERT INTO test VALUES (3), (5), (7);";
            statement.ExecuteUpdate();
            Assert.Equal(3, GetResultCount(statement, "SELECT * from test"));

            // Validate that we don't see the data on connection2
            using var connection2 = database.Connect(null);
            using var statement2 = connection2.CreateStatement();
            Assert.Equal(0, GetResultCount(statement2, "SELECT * from test"));

            // ... until after a commit on connection1
            connection.Commit();
            Assert.Equal(3, GetResultCount(statement2, "SELECT * from test"));

            connection.AutoCommit = true;

            // With AutoCommit on, we immediately see the results on another connection
            statement.SqlQuery = "INSERT INTO test VALUES (2), (4);";
            statement.ExecuteUpdate();
            Assert.Equal(5, GetResultCount(statement2, "SELECT * from test"));

            connection.AutoCommit = false;

            // Now you see it...
            statement.SqlQuery = "INSERT INTO test VALUES (6);";
            statement.ExecuteUpdate();
            Assert.Equal(6, GetResultCount(statement, "SELECT * from test"));

            // Now you don't
            connection.Rollback();
            Assert.Equal(5, GetResultCount(statement, "SELECT * from test"));
        }

        [Fact(Skip = "DuckDb doesn't support ADBC readonly option yet")]
        public void ReadOnlyTest()
        {
            using var database = _duckDb.OpenDatabase("readonly.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "CREATE TABLE test(column1 INTEGER);";
            statement.ExecuteUpdate();

            connection.ReadOnly = true;

            AdbcException exception = Assert.ThrowsAny<AdbcException>(() =>
            {
                statement.SqlQuery = "INSERT INTO test VALUES (3), (5), (7);";
                statement.ExecuteUpdate();
            });

            connection.ReadOnly = false;

            statement.SqlQuery = "INSERT INTO test VALUES (2), (4);";
            statement.ExecuteUpdate();
            Assert.Equal(2, GetResultCount(statement, "SELECT * from test"));
        }

        [Fact]
        public void ReadOnlyFails()
        {
            using var database = _duckDb.OpenDatabase("readonly.db");
            using var connection = database.Connect(null);

            Assert.ThrowsAny<AdbcException>(() =>
            {
                connection.ReadOnly = true;
            });
        }

        [Fact]
        public void SetIsolationLevelFails()
        {
            using var database = _duckDb.OpenDatabase("isolation.db");
            using var connection = database.Connect(null);

            Assert.ThrowsAny<AdbcException>(() =>
            {
                connection.IsolationLevel = IsolationLevel.Default;
            });
        }

        [Fact]
        public void IngestData()
        {
            using var database = _duckDb.OpenDatabase("ingest.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();
            statement.SetOption("adbc.ingest.target_table", "ingested");
            statement.SetOption("adbc.ingest.mode", "adbc.ingest.mode.create");

            Schema schema = new Schema([new Field("key", Int32Type.Default, false), new Field("value", StringType.Default, false)], null);
            RecordBatch recordBatch = new RecordBatch(schema, [
                new Int32Array.Builder().AppendRange([1, 2, 3]).Build(),
                new StringArray.Builder().AppendRange(["foo", "bar", "baz"]).Build()
                ], 3);
            statement.Bind(recordBatch, schema);
            statement.ExecuteUpdate();

            Schema foundSchema = connection.GetTableSchema(null, null, "ingested");
            Assert.Equal(schema.FieldsList.Count, foundSchema.FieldsList.Count);

            Assert.Equal(3, GetResultCount(statement, "SELECT * from ingested"));

            using var statement2 = connection.BulkIngest("ingested", BulkIngestMode.Append);

            recordBatch = new RecordBatch(schema, [
                new Int32Array.Builder().AppendRange([4, 5]).Build(),
                new StringArray.Builder().AppendRange(["quux", "zozzle"]).Build()
                ], 2);
            statement2.Bind(recordBatch, schema);
            statement2.ExecuteUpdate();

            Assert.Equal(5, GetResultCount(statement2, "SELECT * from ingested"));

            // TODO: Pass a schema once the DuckDB "quoting identifiers" bug has been fixed
            using var statement3 = connection.BulkIngest(null, null, "ingested", BulkIngestMode.Append, isTemporary: false);

            recordBatch = new RecordBatch(schema, [
                new Int32Array.Builder().AppendRange([6]).Build(),
                new StringArray.Builder().AppendRange(["antidisestablishmentarianism"]).Build()
                ], 1);
            statement3.Bind(recordBatch, schema);
            statement3.ExecuteUpdate();

            Assert.Equal(6, GetResultCount(statement3, "SELECT * from main.ingested"));
        }

        [Fact]
        public void PrepareAndBind()
        {
            using var database = _duckDb.OpenDatabase("bind.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "select ?, ?";
            statement.Prepare();
            var schema = statement.GetParameterSchema();
            Assert.Equal(2, schema.FieldsList.Count);
            if (RuntimeInformation.IsOSPlatform(OSPlatform.Windows))
            {
                Assert.Equal(string.Empty, schema.FieldsList[0].Name);
            }
            else
            {
                Assert.Equal("0", schema.FieldsList[0].Name);
            }
            Assert.Equal(ArrowTypeId.Null, schema.FieldsList[0].DataType.TypeId);
            Assert.Equal("1", schema.FieldsList[1].Name);
            Assert.Equal(ArrowTypeId.Null, schema.FieldsList[1].DataType.TypeId);

            schema = new Schema([new Field("0", Int32Type.Default, false), new Field("1", StringType.Default, false)], null);
            RecordBatch recordBatch = new RecordBatch(schema, [
                new Int32Array.Builder().AppendRange([1]).Build(),
                new StringArray.Builder().AppendRange(["foo"]).Build()
                ], 1);
            statement.Bind(recordBatch, schema);

            var results = statement.ExecuteQuery();
            Assert.Equal(1, GetResultCount(results));
        }

        [Fact]
        public async Task GetTableTypes()
        {
            using var database = _duckDb.OpenDatabase("tabletypes.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            var types = connection.GetTableTypes();
            Assert.Single(types.Schema.FieldsList);
            var data = await types.ReadNextRecordBatchAsync();
            Assert.Null(data); // Not yet supported in DuckDB
        }

        [Fact]
        public async Task GetCatalogs()
        {
            using var database = _duckDb.OpenDatabase("tablecatalogs.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "CREATE TABLE test(column1 INTEGER);";
            statement.ExecuteUpdate();

            var catalogs = connection.GetObjects(AdbcConnection.GetObjectsDepth.Catalogs, null, null, null, null, null);
            Assert.NotNull(catalogs);
            Assert.Equal(2, catalogs.Schema.FieldsList.Count);

            var found = new HashSet<string>();
            RecordBatch? group;
            do
            {
                group = await catalogs.ReadNextRecordBatchAsync();
                if (group != null && group.Column(0) is StringArray column1)
                {
                    found.UnionWith(column1);
                }
            } while (group != null);
            Assert.Equal(3, found.Count);
            found.ExceptWith(["system", "tablecatalogs", "temp"]);
            Assert.Empty(found);
        }

        private static long GetResultCount(AdbcStatement statement, string query)
        {
            statement.SqlQuery = query;
            var results = statement.ExecuteQuery();
            return GetResultCount(results);
        }

        private static long GetResultCount(QueryResult results)
        {
            long count = 0;
            using (var stream = results.Stream ?? throw new InvalidOperationException("no results found"))
            {
                RecordBatch batch;
                while ((batch = stream.ReadNextRecordBatchAsync().Result) != null)
                {
                    count += batch.Length;
                }
            }
            return count;
        }
    }
}
