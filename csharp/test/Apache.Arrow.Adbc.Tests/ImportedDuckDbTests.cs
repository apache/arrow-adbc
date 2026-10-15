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
using System.Runtime.InteropServices;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Ipc;
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
            Assert.Equal("0", schema.FieldsList[0].Name);
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

        [Fact]
        public void DriverVersionIs11()
        {
            Assert.Equal(AdbcVersion.Version_1_1_0, _duckDb.DriverVersion);
        }

        [Fact]
        public void ExecuteSchema()
        {
            using var database = _duckDb.OpenDatabase("executeschema.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "CREATE TABLE test(foo INTEGER, bar VARCHAR, baz DOUBLE);";
            statement.ExecuteUpdate();

            statement.SqlQuery = "SELECT foo, bar, baz, foo + 1 AS foo1 FROM test";
            Schema schema = statement.ExecuteSchema();

            Assert.Equal(4, schema.FieldsList.Count);
            Assert.Equal("foo", schema.FieldsList[0].Name);
            Assert.Equal(ArrowTypeId.Int32, schema.FieldsList[0].DataType.TypeId);
            Assert.Equal("bar", schema.FieldsList[1].Name);
            Assert.Equal(ArrowTypeId.String, schema.FieldsList[1].DataType.TypeId);
            Assert.Equal("baz", schema.FieldsList[2].Name);
            Assert.Equal(ArrowTypeId.Double, schema.FieldsList[2].DataType.TypeId);
            Assert.Equal("foo1", schema.FieldsList[3].Name);
            Assert.Equal(ArrowTypeId.Int32, schema.FieldsList[3].DataType.TypeId);

            // The schema should match what's actually returned on execution
            var results = statement.ExecuteQuery();
            using var stream = results.Stream;
            Assert.NotNull(stream);
            Assert.Equal(schema.FieldsList.Count, stream.Schema.FieldsList.Count);
            for (int i = 0; i < schema.FieldsList.Count; i++)
            {
                Assert.Equal(schema.FieldsList[i].Name, stream.Schema.FieldsList[i].Name);
                Assert.Equal(schema.FieldsList[i].DataType.TypeId, stream.Schema.FieldsList[i].DataType.TypeId);
            }
        }

        [Fact]
        public void ExecuteSchemaDoesNotExecute()
        {
            using var database = _duckDb.OpenDatabase("executeschemanoexec.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "CREATE TABLE test(foo INTEGER);";
            statement.ExecuteUpdate();

            statement.SqlQuery = "INSERT INTO test VALUES (1), (2) RETURNING foo";
            Schema schema = statement.ExecuteSchema();
            Assert.Single(schema.FieldsList);
            Assert.Equal(ArrowTypeId.Int32, schema.FieldsList[0].DataType.TypeId);

            Assert.Equal(0, GetResultCount(statement, "SELECT * FROM test"));
        }

        [Fact]
        public void CancelIdle()
        {
            using var database = _duckDb.OpenDatabase("cancelidle.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            // Cancelling with nothing in progress is a no-op, and doesn't break later use
            connection.Cancel();
            statement.Cancel();

            Assert.Equal(3, GetResultCount(statement, "SELECT * FROM range(3)"));
        }

        [Fact]
        public async Task CancelStatement()
        {
            using var database = _duckDb.OpenDatabase("cancelstatement.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "SELECT count(*) FROM range(1000000000000)";
            Task<long> query = Task.Run(() => GetResultCount(statement.ExecuteQuery()));

            await CancelUntilCompleteAsync(statement.Cancel, query);
            AdbcException exception = await Assert.ThrowsAnyAsync<AdbcException>(() => query);
            // DuckDB reports interruption as InvalidArgument rather than Cancelled
            Assert.Contains("Interrupted", exception.Message);
        }

        [Fact]
        public async Task CancelConnection()
        {
            using var database = _duckDb.OpenDatabase("cancelconnection.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "SELECT count(*) FROM range(1000000000000)";
            Task<long> query = Task.Run(() => GetResultCount(statement.ExecuteQuery()));

            await CancelUntilCompleteAsync(connection.Cancel, query);
            AdbcException exception = await Assert.ThrowsAnyAsync<AdbcException>(() => query);
            // DuckDB reports interruption as InvalidArgument rather than Cancelled
            Assert.Contains("Interrupted", exception.Message);
        }

        [Fact]
        public void DatabaseGetOption()
        {
            using var database = _duckDb.OpenDatabase("dbgetoption.db");

            Assert.EndsWith("dbgetoption.db", database.GetOption("path"));
            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => database.GetOption("unknown")).Status);
        }

        [Fact]
        public void TypedDatabaseOptionsAreAppliedAtOpen()
        {
            // A long goes through DatabaseSetOptionInt rather than DatabaseSetOption
            using var database = _duckDb.OpenDatabase("typedopen.db", new Dictionary<string, object> { { "threads", 3L } });
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            Assert.Equal("3", GetScalarString(statement, "SELECT current_setting('threads')::VARCHAR"));
            Assert.Equal("3", database.GetOption("threads"));
        }

        [Fact]
        public async Task OpenAndConnectAsync()
        {
            using var database = await _duckDb.OpenDatabaseAsync("openasync.db");
            using var connection = await database.ConnectAsync(null);
            using var statement = connection.CreateStatement();

            Assert.Equal(3, GetResultCount(statement, "SELECT * FROM range(3)"));
        }

        [Fact]
        public void StandardConnectionOptionsComeFromTheDriver()
        {
            using var database = _duckDb.OpenDatabase("standardoptions.db");
            using var connection = database.Connect(null);

            // Nothing has been set through this connection, so these are read from DuckDB
            Assert.True(connection.AutoCommit);
            Assert.Equal(AdbcOptions.Enabled, connection.GetOption(AdbcOptions.Connection.Autocommit));

            connection.AutoCommit = false;
            Assert.Equal(AdbcOptions.Disabled, connection.GetOption(AdbcOptions.Connection.Autocommit));

            // In DuckDB 1.5.6, reading the current catalog or schema causes a later attempt
            // to disable autocommit to be silently ignored, so these are checked last.
            Assert.Equal("standardoptions", connection.CurrentCatalog);
            Assert.Equal("main", connection.CurrentDbSchema);
        }

        [Fact]
        public async Task SetCurrentDbSchema()
        {
            using var database = _duckDb.OpenDatabase("currentschema.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "CREATE SCHEMA other";
            statement.ExecuteUpdate();

            await connection.SetOptionAsync(AdbcOptions.Connection.CurrentDbSchema, "other");
            Assert.Equal("other", await connection.GetOptionAsync(AdbcOptions.Connection.CurrentDbSchema));
            Assert.Equal("other", connection.CurrentDbSchema);

            statement.SqlQuery = "CREATE TABLE unqualified(x INTEGER)";
            statement.ExecuteUpdate();
            Assert.Equal("other", GetScalarString(statement, "SELECT schema_name FROM duckdb_tables() WHERE table_name = 'unqualified'"));

            connection.CurrentDbSchema = "main";
            Assert.Equal("main", GetScalarString(statement, "SELECT current_schema()"));
        }

        [Fact]
        public void ConnectWithTypedOptions()
        {
            using var database = _duckDb.OpenDatabase("connecttyped.db");

            // The bool is sent to DuckDB as the string "false". (That it's sent before the
            // connection is initialized is covered by ExportedDriverRoundTrip11Tests.)
            using var connection = database.ConnectWithTypedOptions(new Dictionary<string, object>
            {
                { AdbcOptions.Connection.Autocommit, false },
            });

            Assert.Equal(AdbcOptions.Disabled, connection.GetOption(AdbcOptions.Connection.Autocommit));
            Assert.False(connection.AutoCommit);
        }

        [Fact]
        public async Task ConnectWithTypedOptionsAsync()
        {
            using var database = _duckDb.OpenDatabase("connecttypedasync.db");

            using var connection = await database.ConnectWithTypedOptionsAsync(new Dictionary<string, object>
            {
                { AdbcOptions.Connection.Autocommit, false },
            });

            Assert.Equal(AdbcOptions.Disabled, connection.GetOption(AdbcOptions.Connection.Autocommit));
        }

        [Fact]
        public void UnsupportedOptions()
        {
            using var database = _duckDb.OpenDatabase("unsupportedoptions.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => connection.GetOption("unknown")).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => connection.GetOptionInt64("unknown")).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => connection.GetOptionDouble("unknown")).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => connection.GetOptionBytes("unknown")).Status);
            Assert.Equal(AdbcStatusCode.NotImplemented, Assert.ThrowsAny<AdbcException>(() => connection.SetOption("unknown", "x")).Status);
            Assert.Equal(AdbcStatusCode.NotImplemented, Assert.ThrowsAny<AdbcException>(() => connection.SetOptionDouble("unknown", 1.5)).Status);
            Assert.Equal(AdbcStatusCode.NotImplemented, Assert.ThrowsAny<AdbcException>(() => connection.SetOptionBytes("unknown", new byte[] { 1 })).Status);

            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => statement.GetOption("unknown")).Status);

            // The spec says NotImplemented, but DuckDB 1.5.6 returns InvalidArgument for
            // unknown statement options. Accept either so a DuckDB fix doesn't break this.
            AdbcStatusCode status = Assert.ThrowsAny<AdbcException>(() => statement.SetOptionInt64("unknown", 1)).Status;
            Assert.Contains(status, new[] { AdbcStatusCode.NotImplemented, AdbcStatusCode.InvalidArgument });
        }

        [Fact]
        public void StatementGetOption()
        {
            using var database = _duckDb.OpenDatabase("statementoption.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SetOption(AdbcOptions.Ingest.TargetTable, "target");
            Assert.Equal("target", statement.GetOption(AdbcOptions.Ingest.TargetTable));
        }

        [Fact]
        public void ErrorDetails()
        {
            using var database = _duckDb.OpenDatabase("errordetails.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "SELECT CAST('abc' AS INTEGER)";
            AdbcException exception = Assert.ThrowsAny<AdbcException>(() => statement.ExecuteQuery());

            Assert.Equal(AdbcStatusCode.InvalidArgument, exception.Status);
            Assert.Contains("Conversion Error", exception.Message);
            KeyValuePair<string, byte[]> detail = Assert.Single(exception.Details, d => d.Key == "duckdb:error_type");
            Assert.Equal("Conversion", Encoding.UTF8.GetString(detail.Value));

            // The 1.1 error layout reuses the vendor code field, so it mustn't leak through
            Assert.Equal(0, exception.NativeError);
        }

        [Fact]
        public async Task Statistics()
        {
            using var database = _duckDb.OpenDatabase("statistics.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            using (IArrowArrayStream names = connection.GetStatisticsNames())
            {
                Assert.Equal(new[] { "statistic_name", "statistic_key" }, names.Schema.FieldsList.Select(f => f.Name));
                Assert.Equal(ArrowTypeId.String, names.Schema.FieldsList[0].DataType.TypeId);
                Assert.Equal(ArrowTypeId.Int16, names.Schema.FieldsList[1].DataType.TypeId);
            }

            statement.SqlQuery = "CREATE TABLE stats AS SELECT * FROM range(10) r(x)";
            statement.ExecuteUpdate();

            using IArrowArrayStream statistics = connection.GetStatistics(null, null, "stats", approximate: false);
            Assert.Equal(new[] { "catalog_name", "catalog_db_schemas" }, statistics.Schema.FieldsList.Select(f => f.Name));

            var catalogs = new List<string?>();
            RecordBatch? batch;
            while ((batch = await statistics.ReadNextRecordBatchAsync()) != null)
            {
                using (batch)
                {
                    StringArray names = (StringArray)batch.Column(0);
                    for (int i = 0; i < names.Length; i++)
                    {
                        catalogs.Add(names.GetString(i));
                    }
                }
            }
            Assert.Contains("statistics", catalogs);
        }

        [Fact]
        public async Task ExecuteAsyncWithCancellationToken()
        {
            using var database = _duckDb.OpenDatabase("executeasync.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();
            using var cts = new CancellationTokenSource();

            statement.SqlQuery = "CREATE TABLE test(x INTEGER)";
            await statement.ExecuteUpdateAsync(cts.Token);

            statement.SqlQuery = "INSERT INTO test VALUES (1), (2)";
            UpdateResult update = await statement.ExecuteUpdateAsync(cts.Token);
            Assert.Equal(2, update.AffectedRows);

            statement.SqlQuery = "SELECT * FROM test";
            QueryResult result = await statement.ExecuteQueryAsync(cts.Token);
            Assert.Equal(2, GetResultCount(result));
        }

        [Fact]
        public async Task CancellationTokenCancelsQuery()
        {
            using var database = _duckDb.OpenDatabase("canceltoken.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();
            using var cts = new CancellationTokenSource();

            statement.SqlQuery = "SELECT count(*) FROM range(1000000000000)";
            Task<QueryResult> query = statement.ExecuteQueryAsync(cts.Token).AsTask();

            await Task.Delay(500);
            cts.Cancel();

            // The token calls Cancel once. Keep cancelling in case that happened before
            // DuckDB started the query; the token stays cancelled either way.
            await CancelUntilCompleteAsync(statement.Cancel, query);
            OperationCanceledException exception = await Assert.ThrowsAnyAsync<OperationCanceledException>(() => query);
            Assert.Equal(cts.Token, exception.CancellationToken);

            // There's no inner exception if the token was cancelled before the work started
            if (exception.InnerException != null)
            {
                // DuckDB reports interruption as InvalidArgument rather than Cancelled
                Assert.Contains("Interrupted", Assert.IsAssignableFrom<AdbcException>(exception.InnerException).Message);
            }
        }

        [Fact]
        public async Task TransactionsAsync()
        {
            using var database = _duckDb.OpenDatabase("transactionsasync.db");
            using var connection = database.Connect(null);
            using var statement = connection.CreateStatement();

            statement.SqlQuery = "CREATE TABLE test(x INTEGER)";
            statement.ExecuteUpdate();

            connection.AutoCommit = false;

            statement.SqlQuery = "INSERT INTO test VALUES (1)";
            statement.ExecuteUpdate();
            await connection.RollbackAsync();
            Assert.Equal(0, GetResultCount(statement, "SELECT * FROM test"));

            statement.SqlQuery = "INSERT INTO test VALUES (2)";
            statement.ExecuteUpdate();
            await connection.CommitAsync();
            Assert.Equal(1, GetResultCount(statement, "SELECT * FROM test"));
        }

        /// <summary>
        /// Calls <paramref name="cancel"/> until <paramref name="query"/> finishes. DuckDB clears
        /// an interrupt when a query begins, so a single Cancel that arrives before the query
        /// has started (as it can on a busy CI runner) would leave the query running indefinitely.
        /// </summary>
        private static async Task CancelUntilCompleteAsync(Action cancel, Task query)
        {
            DateTime deadline = DateTime.UtcNow + TimeSpan.FromSeconds(30);
            while (!query.IsCompleted && DateTime.UtcNow < deadline)
            {
                cancel();
                await Task.WhenAny(query, Task.Delay(250));
            }
            Assert.True(query.IsCompleted, "The query was not cancelled");
        }

        private static string? GetScalarString(AdbcStatement statement, string query)
        {
            statement.SqlQuery = query;
            using IArrowArrayStream stream = statement.ExecuteQuery().Stream ?? throw new InvalidOperationException("no results found");
            using RecordBatch batch = stream.ReadNextRecordBatchAsync().Result ?? throw new InvalidOperationException("no rows found");
            return ((StringArray)batch.Column(0)).GetString(0);
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
