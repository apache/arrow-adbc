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
using Apache.Arrow.Adbc.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Runs a managed <see cref="AdbcDriver"/> fixture through
    /// <see cref="CAdbcDriverExporter"/> to populate a CAdbcDriver struct,
    /// then loads that struct via <see cref="CAdbcDriverImporter"/> as if it
    /// came from a native library. Exercises the ADBC 1.0 C-ABI marshaling
    /// without producing or loading a native DLL.
    /// </summary>
    public class ExportedDriverRoundTripTests
    {
        [Fact]
        public async Task RoundTripSimpleQuery()
        {
            var fixture = new FixtureDriver();

            using AdbcDriver imported = CAdbcDriverImporter.Load(CreateAdapter(fixture));

            using AdbcDatabase db = imported.Open(new Dictionary<string, string> { { "uri", "ignored" } });
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();

            stmt.SqlQuery = "SELECT 42";
            QueryResult result = stmt.ExecuteQuery();

            using IArrowArrayStream stream = result.Stream!;
            Assert.NotNull(stream);

            Schema schema = stream.Schema;
            Assert.Single(schema.FieldsList);
            Assert.Equal(ArrowTypeId.Int32, schema.FieldsList[0].DataType.TypeId);
            Assert.Equal("answer", schema.FieldsList[0].Name);

            RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);
            Assert.Equal(1, batch!.Length);
            Int32Array column = Assert.IsType<Int32Array>(batch.Column(0));
            Assert.Equal(42, column.Values[0]);

            Assert.Null(await stream.ReadNextRecordBatchAsync());
        }

        [Fact]
        public void SqlQueryIsMarshaledToProducer()
        {
            var fixture = new FixtureDriver();

            using AdbcDriver imported = CAdbcDriverImporter.Load(CreateAdapter(fixture));
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();

            const string query = "SELECT 'hello', 'world'";
            stmt.SqlQuery = query;
            stmt.ExecuteQuery();

            Assert.Equal(query, fixture.LastStatement!.ReceivedQuery);
        }

        [Fact]
        public void OpenParametersAreMarshaledToProducer()
        {
            var fixture = new FixtureDriver();

            using AdbcDriver imported = CAdbcDriverImporter.Load(CreateAdapter(fixture));
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>
            {
                { "first", "1" },
                { "second", "2" },
            });

            Assert.Equal("1", fixture.LastDatabase!.Options["first"]);
            Assert.Equal("2", fixture.LastDatabase!.Options["second"]);
        }

        [Fact]
        public void ProducerExceptionPropagatesAsAdbcException()
        {
            var fixture = new FixtureDriver { ThrowOnExecute = new InvalidOperationException("boom") };

            using AdbcDriver imported = CAdbcDriverImporter.Load(CreateAdapter(fixture));
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();
            stmt.SqlQuery = "SELECT 1";

            AdbcException ex = Assert.ThrowsAny<AdbcException>(() => stmt.ExecuteQuery());
            Assert.Contains("boom", ex.Message);
        }

        [Fact]
        public void StatementSetOptionIsMarshaledToProducer()
        {
            var fixture = new FixtureDriver();

            using AdbcDriver imported = CAdbcDriverImporter.Load(CreateAdapter(fixture));
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();

            stmt.SetOption("adbc.ingest.target_table", "my_table");
            stmt.SetOption("adbc.ingest.mode", "append");

            Assert.Equal("my_table", fixture.LastStatement!.Options["adbc.ingest.target_table"]);
            Assert.Equal("append", fixture.LastStatement!.Options["adbc.ingest.mode"]);
        }

        [Fact]
        public void StatementCancelIsMarshaledToProducer()
        {
            var fixture = new FixtureDriver();

            using AdbcDriver imported = CAdbcDriverImporter.Load(CreateAdapter(fixture));
            Assert.Equal(AdbcVersion.Version_1_1_0, imported.DriverVersion);

            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();

            stmt.Cancel();

            Assert.True(fixture.LastStatement!.WasCancelled);
        }

        [Fact]
        public void ConnectionCancelIsMarshaledToProducer()
        {
            var fixture = new FixtureDriver();

            using AdbcDriver imported = CAdbcDriverImporter.Load(CreateAdapter(fixture));
            Assert.Equal(AdbcVersion.Version_1_1_0, imported.DriverVersion);

            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);

            conn.Cancel();

            Assert.True(fixture.LastConnection!.WasCancelled);
        }

        [Fact]
        public void StatementExecuteSchemaReturnsSchemaWithoutExecutingQuery()
        {
            var fixture = new FixtureDriver();

            using AdbcDriver imported = CAdbcDriverImporter.Load(CreateAdapter(fixture));
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();
            stmt.SqlQuery = "SELECT 42";

            Schema schema = stmt.ExecuteSchema();
            Assert.Single(schema.FieldsList);
            Assert.Equal("answer", schema.FieldsList[0].Name);
            Assert.Equal(ArrowTypeId.Int32, schema.FieldsList[0].DataType.TypeId);

            Assert.False(fixture.LastStatement!.WasExecuted);
        }

        [Fact]
        public void V1_0_0_DriverInitRejectsV1_1_0_OnlyFunctions()
        {
            // Force the importer to settle on v1.0.0 by having the producer's init
            // refuse v1.1.0. This proves the v1.1.0 functions are gated by version.
            var fixture = new FixtureDriver();
            AdbcDriverInit adapter = (int version, ref CAdbcDriver nativeDriver, ref CAdbcError error) =>
            {
                if (version != AdbcVersion.Version_1_0_0) { return AdbcStatusCode.NotImplemented; }
                return CallExporter(version, ref nativeDriver, ref error, fixture);
            };

            using AdbcDriver imported = CAdbcDriverImporter.Load(adapter);
            Assert.Equal(AdbcVersion.Version_1_0_0, imported.DriverVersion);

            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();

            // ExecuteSchema, Cancel are 1.1.0-only: on a v1.0.0 driver the importer's
            // built-in defaults return NotImplemented.
            AdbcException ex = Assert.ThrowsAny<AdbcException>(() => stmt.ExecuteSchema());
            Assert.Equal(AdbcStatusCode.NotImplemented, ex.Status);
        }

        private static AdbcStatusCode CallExporter(int version, ref CAdbcDriver nativeDriver, ref CAdbcError error, AdbcDriver driver)
        {
            unsafe
            {
                fixed (CAdbcDriver* dp = &nativeDriver)
                fixed (CAdbcError* ep = &error)
                {
                    return CAdbcDriverExporter.AdbcDriverInit(version, dp, ep, driver);
                }
            }
        }

        private static AdbcDriverInit CreateAdapter(AdbcDriver driver)
        {
            return (int version, ref CAdbcDriver nativeDriver, ref CAdbcError error) =>
            {
                unsafe
                {
                    fixed (CAdbcDriver* dp = &nativeDriver)
                    fixed (CAdbcError* ep = &error)
                    {
                        return CAdbcDriverExporter.AdbcDriverInit(version, dp, ep, driver);
                    }
                }
            };
        }

        private sealed class FixtureDriver : AdbcDriver
        {
            public FixtureDatabase? LastDatabase { get; private set; }
            public FixtureConnection? LastConnection { get; private set; }
            public FixtureStatement? LastStatement { get; private set; }
            public Exception? ThrowOnExecute { get; set; }

            public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
            {
                var db = new FixtureDatabase(this, parameters);
                LastDatabase = db;
                return db;
            }

            internal void RecordConnection(FixtureConnection conn) => LastConnection = conn;
            internal void RecordStatement(FixtureStatement stmt) => LastStatement = stmt;
        }

        private sealed class FixtureDatabase : AdbcDatabase
        {
            private readonly FixtureDriver _driver;
            public Dictionary<string, string> Options { get; }

            public FixtureDatabase(FixtureDriver driver, IReadOnlyDictionary<string, string> parameters)
            {
                _driver = driver;
#if NET6_0_OR_GREATER
                Options = new Dictionary<string, string>(parameters);
#else
                Options = new Dictionary<string, string>(parameters.Count);
                foreach (KeyValuePair<string, string> pair in parameters)
                {
                    Options.Add(pair.Key, pair.Value);
                }
#endif
            }

            public override void SetOption(string key, string value) => Options[key] = value;

            public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
            {
                var conn = new FixtureConnection(_driver);
                _driver.RecordConnection(conn);
                return conn;
            }
        }

        private sealed class FixtureConnection : AdbcConnection
        {
            private readonly FixtureDriver _driver;

            public FixtureConnection(FixtureDriver driver) { _driver = driver; }

            public bool WasCancelled { get; private set; }

            public override void Cancel() => WasCancelled = true;

            public override AdbcStatement CreateStatement()
            {
                var stmt = new FixtureStatement(_driver);
                _driver.RecordStatement(stmt);
                return stmt;
            }

            public override IArrowArrayStream GetObjects(
                GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern,
                string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
                => throw AdbcException.NotImplemented("fixture does not support GetObjects");

            public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
                => throw AdbcException.NotImplemented("fixture does not support GetTableSchema");

            public override IArrowArrayStream GetTableTypes()
                => throw AdbcException.NotImplemented("fixture does not support GetTableTypes");
        }

        private sealed class FixtureStatement : AdbcStatement
        {
            private readonly FixtureDriver _driver;
            private string? _sqlQuery;

            public FixtureStatement(FixtureDriver driver) { _driver = driver; }

            public string? ReceivedQuery => _sqlQuery;
            public Dictionary<string, string> Options { get; } = new Dictionary<string, string>();
            public bool WasCancelled { get; private set; }
            public bool WasExecuted { get; private set; }

            public override string? SqlQuery
            {
                get => _sqlQuery;
                set => _sqlQuery = value;
            }

            public override void SetOption(string key, string value) => Options[key] = value;

            public override void Cancel() => WasCancelled = true;

            public override Schema ExecuteSchema() => BuildResultSchema();

            public override QueryResult ExecuteQuery()
            {
                if (_driver.ThrowOnExecute != null) { throw _driver.ThrowOnExecute; }

                WasExecuted = true;
                Schema schema = BuildResultSchema();
                var column = new Int32Array.Builder().Append(42).Build();
                var batch = new RecordBatch(schema, new IArrowArray[] { column }, 1);
                return new QueryResult(1, new SingleBatchStream(schema, batch));
            }

            public override UpdateResult ExecuteUpdate()
                => throw AdbcException.NotImplemented("fixture does not support ExecuteUpdate");

            private static Schema BuildResultSchema() => new Schema.Builder()
                .Field(f => f.Name("answer").DataType(Int32Type.Default).Nullable(false))
                .Build();
        }

        private sealed class SingleBatchStream : IArrowArrayStream
        {
            private readonly Schema _schema;
            private RecordBatch? _batch;

            public SingleBatchStream(Schema schema, RecordBatch batch)
            {
                _schema = schema;
                _batch = batch;
            }

            public Schema Schema => _schema;

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                RecordBatch? result = _batch;
                _batch = null;
                return new ValueTask<RecordBatch?>(result);
            }

            public void Dispose() { _batch?.Dispose(); _batch = null; }
        }
    }
}
