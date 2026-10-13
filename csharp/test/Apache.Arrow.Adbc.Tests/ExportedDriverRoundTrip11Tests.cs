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
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Round-trips the ADBC 1.1.0 API surface through <see cref="CAdbcDriverExporter"/>
    /// and <see cref="CAdbcDriverImporter"/>: typed options, statistics, error details
    /// and the standard connection options.
    /// </summary>
    public class ExportedDriverRoundTrip11Tests
    {
        [Fact]
        public void TypedOpenParametersAreMarshaledToProducer()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);

            using AdbcDatabase db = imported.Open(new Dictionary<string, object>
            {
                { "string", "value" },
                { "int", 42L },
                { "double", 2.5 },
                { "bytes", new byte[] { 1, 2, 3 } },
                { "bool", true },
            });

            Dictionary<string, object> options = fixture.LastDatabase!.Options;
            Assert.Equal("value", options["string"]);
            Assert.Equal(42L, options["int"]);
            Assert.Equal(2.5, options["double"]);
            Assert.Equal(new byte[] { 1, 2, 3 }, options["bytes"]);
            Assert.Equal("true", options["bool"]);
        }

        [Fact]
        public void DatabaseTypedOptionsRoundTrip()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());

            db.SetOption("s", "text");
            db.SetOptionInt64("i", long.MinValue);
            db.SetOptionDouble("d", -0.125);
            db.SetOptionBytes("b", new byte[] { 0, 255 });

            Assert.Equal("text", db.GetOption("s"));
            Assert.Equal(long.MinValue, db.GetOptionInt64("i"));
            Assert.Equal(-0.125, db.GetOptionDouble("d"));
            Assert.Equal(new byte[] { 0, 255 }, db.GetOptionBytes("b"));
        }

        [Fact]
        public void DatabaseGetOptionReportsOptionsTheProducerDoesNotTrack()
        {
            // A 1.0-style producer that never overrode GetOption still answers
            // DatabaseGetOption for options set through the C API.
            var fixture = new Driver11 { DatabaseTracksOptions = false };
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string> { { "uri", "memory://" } });

            Assert.Equal("memory://", db.GetOption("uri"));
            AdbcException ex = Assert.ThrowsAny<AdbcException>(() => db.GetOption("missing"));
            Assert.Equal(AdbcStatusCode.NotFound, ex.Status);
        }

        [Fact]
        public void LongStringOptionUsesBufferRetry()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();

            string longValue = new string('x', 1000) + "é";
            stmt.SetOption("long", longValue);
            Assert.Equal(longValue, stmt.GetOption("long"));

            byte[] longBytes = new byte[1000];
            new Random(1).NextBytes(longBytes);
            stmt.SetOptionBytes("long_bytes", longBytes);
            Assert.Equal(longBytes, stmt.GetOptionBytes("long_bytes"));
        }

        [Fact]
        public void ConnectionTypedOptionsRoundTrip()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(new Dictionary<string, string> { { "at_connect", "yes" } });

            conn.SetOptionInt64("timeout", 30);
            conn.SetOptionDouble("ratio", 0.75);

            Assert.Equal(30L, fixture.LastConnection!.Options["timeout"]);
            Assert.Equal(0.75, fixture.LastConnection!.Options["ratio"]);
            Assert.Equal(30L, conn.GetOptionInt64("timeout"));
            Assert.Equal(0.75, conn.GetOptionDouble("ratio"));
            Assert.Equal("yes", conn.GetOption("at_connect"));
        }

        [Fact]
        public void StandardConnectionOptionsComeFromProperties()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);

            // The producer doesn't override GetOption for these keys; the base
            // AdbcConnection implementation answers from its properties.
            Assert.Equal(AdbcOptions.Enabled, conn.GetOption(AdbcOptions.Connection.Autocommit));
            Assert.True(conn.AutoCommit);
            Assert.Equal(IsolationLevel.Default, conn.IsolationLevel);

            conn.CurrentCatalog = "main";
            Assert.Equal("main", fixture.LastConnection!.CurrentCatalog);
            Assert.Equal("main", conn.CurrentCatalog);
            Assert.Equal("main", conn.GetOption(AdbcOptions.Connection.CurrentCatalog));

            // The producer doesn't support a current schema at all
            Assert.Null(conn.CurrentDbSchema);
        }

        [Fact]
        public void TypedConnectOptionsReachTheProducerBeforeInit()
        {
            var fixture = new Driver11 { DatabaseHandlesTypedConnect = true };
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.ConnectWithTypedOptions(new Dictionary<string, object>
            {
                { "s", "text" },
                { "n", 5L },
                { "d", 0.5 },
                { "cert", new byte[] { 1, 2, 3 } },
            });

            IReadOnlyDictionary<string, object> received = fixture.LastTypedConnectOptions!;
            Assert.Equal("text", received["s"]);
            Assert.Equal(5L, received["n"]);
            Assert.Equal(0.5, received["d"]);
            Assert.Equal(new byte[] { 1, 2, 3 }, received["cert"]);
        }

        [Fact]
        public void TypedConnectOptionsFallBackToConnectForExistingProducers()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.ConnectWithTypedOptions(new Dictionary<string, object>
            {
                { "n", 5L },
                { "cert", new byte[] { 1, 2, 3 } },
            });

            Assert.Null(fixture.LastTypedConnectOptions);
            Assert.Equal("5", fixture.LastConnection!.Options["n"]);
            Assert.Equal(new byte[] { 1, 2, 3 }, fixture.LastConnection!.Options["cert"]);
        }

        [Fact]
        public void AutoCommitSetBeforeInitIsReported()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(new Dictionary<string, string>
            {
                { AdbcOptions.Connection.Autocommit, AdbcOptions.Disabled },
            });

            Assert.False(fixture.LastConnection!.AutoCommitAtConnect);
            Assert.Equal(AdbcOptions.Disabled, conn.GetOption(AdbcOptions.Connection.Autocommit));
            Assert.False(conn.AutoCommit);
        }

        [Fact]
        public void ExecuteUpdateRoundTripsWithoutAResultStream()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();

            stmt.SqlQuery = "UPDATE";
            Assert.Equal(42, stmt.ExecuteUpdate().AffectedRows);
        }

        [Fact]
        public void StatisticsAreMarshaled()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);

            using (IArrowArrayStream names = conn.GetStatisticsNames())
            {
                Assert.Equal("statistic_name", names.Schema.FieldsList[0].Name);
            }

            using (IArrowArrayStream stats = conn.GetStatistics("cat", null, "tbl", approximate: true))
            {
                Assert.Equal("statistic_name", stats.Schema.FieldsList[0].Name);
            }
            Assert.Equal(("cat", (string?)null, "tbl", true), fixture.LastConnection!.LastStatisticsRequest);
        }

        [Fact]
        public void ErrorDetailsArePropagated()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();
            stmt.SqlQuery = "FAIL";

            AdbcException ex = Assert.ThrowsAny<AdbcException>(() => stmt.ExecuteQuery());
            Assert.Equal(AdbcStatusCode.InvalidData, ex.Status);
            Assert.Equal("42000", ex.SqlState);
            Assert.Contains("query failed", ex.Message);
            Assert.Equal(2, ex.Details.Count);
            Assert.Equal("query_id", ex.Details[0].Key);
            Assert.Equal("q-123", Encoding.UTF8.GetString(ex.Details[0].Value));
            Assert.Equal("empty", ex.Details[1].Key);
            Assert.Empty(ex.Details[1].Value);
        }

        [Fact]
        public void ExecutePartitionedRoundTrips()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();

            stmt.SqlQuery = "PARTITIONS";
            PartitionedResult result = stmt.ExecutePartitioned();

            Assert.Equal(5, result.AffectedRows);
            Assert.Equal("x", Assert.Single(result.Schema.FieldsList).Name);
            Assert.Equal(2, result.PartitionDescriptors.Count);
            Assert.Equal(new byte[] { 1, 2 }, result.PartitionDescriptors[0].Descriptor.ToArray());
            Assert.Equal(new byte[] { 3 }, result.PartitionDescriptors[1].Descriptor.ToArray());
        }

        [Fact]
        public void ExecutePartitionedFailureDoesNotReleaseGarbage()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();

            // The producer fails without writing the partitions struct
            stmt.SqlQuery = "OTHER";
            for (int i = 0; i < 20; i++)
            {
                Assert.Equal(AdbcStatusCode.NotImplemented, Assert.ThrowsAny<AdbcException>(() => stmt.ExecutePartitioned()).Status);
            }
        }

        [Fact]
        public void ErrorWithoutDetailsReportsVendorCode()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();
            stmt.SqlQuery = "FAIL_PLAIN";

            AdbcException ex = Assert.ThrowsAny<AdbcException>(() => stmt.ExecuteQuery());
            Assert.Equal("HY000", ex.SqlState);
            Assert.Equal(1234, ex.NativeError);
            Assert.Empty(ex.Details);
        }

        [Fact]
        public void UnknownOptionIsNotFound()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();

            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => conn.GetOption("nope")).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => stmt.GetOptionInt64("nope")).Status);
        }

        [Fact]
        public void OperationCanceledExceptionIsReportedAsCancelled()
        {
            using AdbcDriver imported = Load(new Driver11());
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();
            stmt.SqlQuery = "FAIL_CANCELED";

            AdbcException ex = Assert.ThrowsAny<AdbcException>(() => stmt.ExecuteQuery());
            Assert.Equal(AdbcStatusCode.Cancelled, ex.Status);
        }

        [Fact]
        public async Task CancelReachesImportedStatement()
        {
            var fixture = new Driver11();
            using AdbcDriver imported = Load(fixture);
            using AdbcDatabase db = imported.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();
            stmt.SqlQuery = "BLOCK";

            Task<QueryResult> pending = Task.Run(() => stmt.ExecuteQuery());
            Assert.True(fixture.LastStatement!.Started.Wait(TimeSpan.FromSeconds(10)));
            stmt.Cancel();

            AdbcException ex = await Assert.ThrowsAnyAsync<AdbcException>(() => pending);
            Assert.Equal(AdbcStatusCode.Cancelled, ex.Status);
        }

        [Fact]
        public void Version1_0DriverFallsBackForTypedSetters()
        {
            var fixture = new Driver11();
            AdbcDriverInit adapter = (int version, ref CAdbcDriver nativeDriver, ref CAdbcError error) =>
            {
                if (version != AdbcVersion.Version_1_0_0) { return AdbcStatusCode.NotImplemented; }
                return CallExporter(version, ref nativeDriver, ref error, fixture);
            };

            using AdbcDriver imported = CAdbcDriverImporter.Load(adapter);
            Assert.Equal(AdbcVersion.Version_1_0_0, imported.DriverVersion);

            using AdbcDatabase db = imported.Open(new Dictionary<string, object> { { "n", 7L } });
            Assert.Equal("7", fixture.LastDatabase!.Options["n"]);

            using AdbcConnection conn = db.Connect(null);
            conn.SetOptionInt64("timeout", 5);
            Assert.Equal("5", fixture.LastConnection!.Options["timeout"]);

            // A 1.0 driver can't report option values, so the getters fall back to the
            // base class defaults, as the C driver manager does
            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => conn.GetOption("timeout")).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => conn.GetOptionInt64("timeout")).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => conn.GetOption(AdbcOptions.Connection.Autocommit)).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => db.GetOption("n")).Status);
            Assert.Null(conn.CurrentCatalog);

            using AdbcStatement stmt = conn.CreateStatement();
            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => stmt.GetOptionDouble(AdbcOptions.Statement.Progress)).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.ThrowsAny<AdbcException>(() => stmt.GetOptionBytes("b")).Status);
        }

        [Fact]
        public void Version1_0DriverRejectsBinaryOpenParameters()
        {
            var fixture = new Driver11();
            AdbcDriverInit adapter = (int version, ref CAdbcDriver nativeDriver, ref CAdbcError error) =>
            {
                if (version != AdbcVersion.Version_1_0_0) { return AdbcStatusCode.NotImplemented; }
                return CallExporter(version, ref nativeDriver, ref error, fixture);
            };

            using AdbcDriver imported = CAdbcDriverImporter.Load(adapter);

            AdbcException ex = Assert.ThrowsAny<AdbcException>(() => imported.Open(new Dictionary<string, object>
            {
                { "n", 7L },
                { "b", new byte[] { 1 } },
            }));
            Assert.Equal(AdbcStatusCode.NotImplemented, ex.Status);
            Assert.True(fixture.LastDatabase!.Disposed);
        }

        private static AdbcDriver Load(AdbcDriver driver)
        {
            AdbcDriver imported = CAdbcDriverImporter.Load(
                (int version, ref CAdbcDriver nativeDriver, ref CAdbcError error) => CallExporter(version, ref nativeDriver, ref error, driver));
            Assert.Equal(AdbcVersion.Version_1_1_0, imported.DriverVersion);
            return imported;
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

        /// <summary>
        /// Stores options with their types so tests can see which typed setter was used.
        /// </summary>
        private sealed class OptionBag
        {
            public Dictionary<string, object> Values { get; } = new Dictionary<string, object>();

            public object Get(string key) =>
                Values.TryGetValue(key, out object? value) ? value : throw new AdbcException($"no option {key}", AdbcStatusCode.NotFound);
        }

        private sealed class Driver11 : AdbcDriver
        {
            public bool DatabaseTracksOptions { get; set; } = true;
            public bool DatabaseHandlesTypedConnect { get; set; }
            public IReadOnlyDictionary<string, object>? LastTypedConnectOptions { get; set; }
            public Database11? LastDatabase { get; private set; }
            public Connection11? LastConnection { get; set; }
            public Statement11? LastStatement { get; set; }

            public override int DriverVersion => AdbcVersion.Version_1_1_0;

            public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
            {
                var options = new Dictionary<string, object>();
                foreach (KeyValuePair<string, string> pair in parameters) { options[pair.Key] = pair.Value; }
                return LastDatabase = new Database11(this, options);
            }

            public override AdbcDatabase Open(IReadOnlyDictionary<string, object> parameters)
            {
                var options = new Dictionary<string, object>();
                foreach (KeyValuePair<string, object> pair in parameters) { options[pair.Key] = pair.Value; }
                return LastDatabase = new Database11(this, options);
            }
        }

        private sealed class Database11 : AdbcDatabase
        {
            private readonly Driver11 _driver;
            public Dictionary<string, object> Options { get; }

            public Database11(Driver11 driver, Dictionary<string, object> options)
            {
                _driver = driver;
                Options = options;
            }

            public override void SetOption(string key, string value) => Options[key] = value;
            public override void SetOptionInt64(string key, long value) => Options[key] = value;
            public override void SetOptionDouble(string key, double value) => Options[key] = value;
            public override void SetOptionBytes(string key, ReadOnlySpan<byte> value) => Options[key] = value.ToArray();

            public bool Disposed { get; private set; }
            public override void Dispose() => Disposed = true;

            public override string GetOption(string key) =>
                _driver.DatabaseTracksOptions && Options.TryGetValue(key, out object? v) ? (string)v : base.GetOption(key);
            public override long GetOptionInt64(string key) => (long)Options[key];
            public override double GetOptionDouble(string key) => (double)Options[key];
            public override byte[] GetOptionBytes(string key) => (byte[])Options[key];

            public override AdbcConnection ConnectWithTypedOptions(IReadOnlyDictionary<string, object>? options)
            {
                if (!_driver.DatabaseHandlesTypedConnect)
                {
                    return base.ConnectWithTypedOptions(options);
                }

                _driver.LastTypedConnectOptions = options;
                return _driver.LastConnection = new Connection11(_driver);
            }

            public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
            {
                var conn = new Connection11(_driver);
                if (options != null)
                {
                    foreach (KeyValuePair<string, string> pair in options)
                    {
                        // Like many drivers, consume autocommit at connect time without
                        // overriding the AutoCommit property.
                        if (pair.Key == AdbcOptions.Connection.Autocommit)
                        {
                            conn.AutoCommitAtConnect = AdbcOptions.GetEnabled(pair.Value);
                            continue;
                        }
                        conn.Options[pair.Key] = pair.Value;
                    }
                }
                return _driver.LastConnection = conn;
            }
        }

        private sealed class Connection11 : AdbcConnection
        {
            private readonly Driver11 _driver;
            private readonly OptionBag _options = new OptionBag();
            private string? _catalog;

            public Connection11(Driver11 driver) { _driver = driver; }

            public Dictionary<string, object> Options => _options.Values;
            public bool? AutoCommitAtConnect { get; set; }
            public (string?, string?, string, bool)? LastStatisticsRequest { get; private set; }

            public override string? CurrentCatalog
            {
                get => _catalog;
                set => _catalog = value;
            }

            public override void SetOption(string key, string value) => Options[key] = value;
            public override void SetOptionInt64(string key, long value) => Options[key] = value;
            public override void SetOptionDouble(string key, double value) => Options[key] = value;
            public override void SetOptionBytes(string key, ReadOnlySpan<byte> value) => Options[key] = value.ToArray();

            public override string GetOption(string key) =>
                Options.TryGetValue(key, out object? v) && v is string s ? s : base.GetOption(key);
            public override long GetOptionInt64(string key) => (long)_options.Get(key);
            public override double GetOptionDouble(string key) => (double)_options.Get(key);

            public override AdbcStatement CreateStatement() => _driver.LastStatement = new Statement11();

            public override IArrowArrayStream GetStatisticsNames() => NameStream();

            public override IArrowArrayStream GetStatistics(string? catalogPattern, string? schemaPattern, string tableNamePattern, bool approximate)
            {
                LastStatisticsRequest = (catalogPattern, schemaPattern, tableNamePattern, approximate);
                return NameStream();
            }

            public override IArrowArrayStream GetObjects(
                GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern,
                string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
                => throw AdbcException.NotImplemented("GetObjects");

            public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
                => throw AdbcException.NotImplemented("GetTableSchema");

            public override IArrowArrayStream GetTableTypes()
                => throw AdbcException.NotImplemented("GetTableTypes");

            private static IArrowArrayStream NameStream()
            {
                Schema schema = new Schema.Builder()
                    .Field(f => f.Name("statistic_name").DataType(StringType.Default).Nullable(false))
                    .Build();
                StringArray names = new StringArray.Builder().Append("custom").Build();
                return new SingleBatchStream(schema, new RecordBatch(schema, new IArrowArray[] { names }, 1));
            }
        }

        private sealed class Statement11 : AdbcStatement
        {
            private readonly OptionBag _options = new OptionBag();
            private readonly ManualResetEventSlim _cancelled = new ManualResetEventSlim();

            public ManualResetEventSlim Started { get; } = new ManualResetEventSlim();

            public override void SetOption(string key, string value) => _options.Values[key] = value;
            public override void SetOptionBytes(string key, ReadOnlySpan<byte> value) => _options.Values[key] = value.ToArray();
            public override string GetOption(string key) => (string)_options.Get(key);
            public override byte[] GetOptionBytes(string key) => (byte[])_options.Get(key);

            public override void Cancel() => _cancelled.Set();

            public override QueryResult ExecuteQuery()
            {
                switch (SqlQuery)
                {
                    case "FAIL":
                        throw new DetailedException();
                    case "FAIL_PLAIN":
                        throw new PlainException();
                    case "FAIL_CANCELED":
                        throw new OperationCanceledException();
                    case "BLOCK":
                        Started.Set();
                        if (_cancelled.Wait(TimeSpan.FromSeconds(30)))
                        {
                            throw new AdbcException("cancelled", AdbcStatusCode.Cancelled);
                        }
                        throw new TimeoutException();
                    default:
                        throw AdbcException.NotImplemented("ExecuteQuery");
                }
            }

            public override UpdateResult ExecuteUpdate() =>
                SqlQuery == "UPDATE" ? new UpdateResult(42) : throw AdbcException.NotImplemented("ExecuteUpdate");

            public override PartitionedResult ExecutePartitioned()
            {
                if (SqlQuery != "PARTITIONS")
                {
                    return base.ExecutePartitioned();
                }

                Schema schema = new Schema.Builder()
                    .Field(f => f.Name("x").DataType(Int32Type.Default).Nullable(false))
                    .Build();
                return new PartitionedResult(schema, 5, new[]
                {
                    new PartitionDescriptor(new byte[] { 1, 2 }),
                    new PartitionDescriptor(new byte[] { 3 }),
                });
            }
        }

        private sealed class DetailedException : AdbcException
        {
            public DetailedException() : base("query failed", AdbcStatusCode.InvalidData) { }

            public override string? SqlState => "42000";

            public override IReadOnlyList<KeyValuePair<string, byte[]>> Details => new[]
            {
                new KeyValuePair<string, byte[]>("query_id", Encoding.UTF8.GetBytes("q-123")),
                new KeyValuePair<string, byte[]>("empty", new byte[0]),
            };
        }

        private sealed class PlainException : AdbcException
        {
            public PlainException() : base("plain failure", AdbcStatusCode.IOError) { }

            public override string? SqlState => "HY000";
            public override int NativeError => 1234;
        }

        private sealed class SingleBatchStream : IArrowArrayStream
        {
            private RecordBatch? _batch;

            public SingleBatchStream(Schema schema, RecordBatch batch)
            {
                Schema = schema;
                _batch = batch;
            }

            public Schema Schema { get; }

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
