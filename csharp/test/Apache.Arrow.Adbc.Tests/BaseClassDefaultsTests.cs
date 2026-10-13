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
using Apache.Arrow.Ipc;
using Xunit;

namespace Apache.Arrow.Adbc.Tests
{
    /// <summary>
    /// Tests the default implementations of the ADBC 1.1.0 members of the ADBC
    /// base classes.
    /// </summary>
    public class BaseClassDefaultsTests
    {
        [Fact]
        public void DoubleOptionsRoundTripThroughStrings()
        {
            // "R" formatting doesn't round-trip this value on .NET Framework
            const double value = 0.84551240822557006;
            var stmt = new OptionsStatement();

            stmt.SetOptionDouble("d", value);
            Assert.Equal(value, stmt.GetOptionDouble("d"));
        }

        [Fact]
        public void TypedOptionsDefaultToStringOptions()
        {
            var stmt = new OptionsStatement();

            stmt.SetOptionInt64("i", -12);
            stmt.SetOptionDouble("d", 0.1);

            Assert.Equal("-12", stmt.Options["i"]);
            Assert.Equal("0.1", stmt.Options["d"]);
            Assert.Equal(-12L, stmt.GetOptionInt64("i"));
            Assert.Equal(0.1, stmt.GetOptionDouble("d"));

            stmt.SetOption("s", "not a number");
            Assert.Equal(AdbcStatusCode.InvalidArgument, Assert.Throws<AdbcException>(() => stmt.GetOptionInt64("s")).Status);
            Assert.Equal(AdbcStatusCode.NotImplemented, Assert.Throws<AdbcException>(() => stmt.SetOptionBytes("b", new byte[1])).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.Throws<AdbcException>(() => stmt.GetOptionBytes("b")).Status);
        }

        [Fact]
        public void ConnectionGetOptionAnswersStandardOptions()
        {
            var conn = new RecordingConnection();

            Assert.Equal(AdbcOptions.Enabled, conn.GetOption(AdbcOptions.Connection.Autocommit));
            Assert.Equal(AdbcOptions.Disabled, conn.GetOption(AdbcOptions.Connection.ReadOnly));
            Assert.Equal(AdbcOptions.IsolationLevels.Default, conn.GetOption(AdbcOptions.Connection.IsolationLevel));
            Assert.Equal(AdbcStatusCode.NotFound, Assert.Throws<AdbcException>(() => conn.GetOption(AdbcOptions.Connection.CurrentCatalog)).Status);
            Assert.Equal(AdbcStatusCode.NotFound, Assert.Throws<AdbcException>(() => conn.GetOption("unknown")).Status);
        }

        [Fact]
        public void CurrentCatalogSetterDefaultsToSetOption()
        {
            var conn = new RecordingConnection();

            conn.CurrentCatalog = "cat";
            conn.CurrentDbSchema = "schema";

            Assert.Equal("cat", conn.Options[AdbcOptions.Connection.CurrentCatalog]);
            Assert.Equal("schema", conn.Options[AdbcOptions.Connection.CurrentDbSchema]);
        }

        [Fact]
        public void ConnectWithTypedOptionsDefaultsToConnect()
        {
            var db = new ConnectingDatabase();

            using AdbcConnection conn = db.ConnectWithTypedOptions(new Dictionary<string, object>
            {
                { "s", "text" },
                { "b", false },
                { "l", 4L },
                { "d", 2.5 },
                { "bytes", new byte[] { 1, 2 } },
                { "memory", new ReadOnlyMemory<byte>(new byte[] { 3 }) },
            });

            Assert.Equal(new Dictionary<string, string> { { "s", "text" }, { "b", "false" }, { "l", "4" }, { "d", "2.5" } }, db.LastStringOptions);
            var binary = ((RecordingConnection)conn).BinaryOptions;
            Assert.Equal(new byte[] { 1, 2 }, binary["bytes"]);
            Assert.Equal(new byte[] { 3 }, binary["memory"]);

            using AdbcConnection noOptions = db.ConnectWithTypedOptions(null);
            Assert.Null(db.LastStringOptions);
        }

        [Fact]
        public void ConnectWithTypedOptionsDisposesTheConnectionIfABinaryOptionFails()
        {
            var db = new ConnectingDatabase();

            Assert.Throws<AdbcException>(() => db.ConnectWithTypedOptions(new Dictionary<string, object>
            {
                { RecordingConnection.RejectedKey, new byte[] { 1 } },
            }));
            Assert.True(db.LastConnection!.Disposed);
        }

        [Fact]
        public void DatabaseObjectSetOptionDispatchesByType()
        {
            var db = new RecordingDatabase();

            db.SetOption("s", (object)"text");
            db.SetOption("b", (object)true);
            db.SetOption("i", (object)3);
            db.SetOption("l", (object)4L);
            db.SetOption("f", (object)1.5f);
            db.SetOption("d", (object)2.5);
            db.SetOption("bytes", (object)new byte[] { 9 });

            Assert.Equal("text", db.Options["s"]);
            Assert.Equal("true", db.Options["b"]);
            Assert.Equal(3L, db.Options["i"]);
            Assert.Equal(4L, db.Options["l"]);
            Assert.Equal(1.5, db.Options["f"]);
            Assert.Equal(2.5, db.Options["d"]);
            Assert.Equal(new byte[] { 9 }, db.Options["bytes"]);

            Assert.Equal(AdbcStatusCode.InvalidArgument, Assert.Throws<AdbcException>(() => db.SetOption("x", (object)DateTime.Now)).Status);
        }

        [Fact]
        public void DriverOpenWithObjectsConvertsToStrings()
        {
            var driver = new StringOnlyDriver();

            driver.Open(new Dictionary<string, object> { { "n", 5 }, { "d", 0.5 }, { "s", "x" }, { "b", false } });

            Assert.Equal("5", driver.Parameters!["n"]);
            Assert.Equal("0.5", driver.Parameters["d"]);
            Assert.Equal("x", driver.Parameters["s"]);
            Assert.Equal("false", driver.Parameters["b"]);
        }

        private sealed class OptionsStatement : AdbcStatement
        {
            public Dictionary<string, string> Options { get; } = new Dictionary<string, string>();

            public override void SetOption(string key, string value) => Options[key] = value;
            public override string GetOption(string key) => Options.TryGetValue(key, out string? v) ? v : base.GetOption(key);

            public override QueryResult ExecuteQuery() => throw new NotImplementedException();
            public override UpdateResult ExecuteUpdate() => throw new NotImplementedException();
        }

        private sealed class RecordingConnection : AdbcConnection
        {
            public Dictionary<string, string> Options { get; } = new Dictionary<string, string>();

            public const string RejectedKey = "test.rejected";

            public Dictionary<string, byte[]> BinaryOptions { get; } = new Dictionary<string, byte[]>();
            public bool Disposed { get; private set; }

            public override void SetOptionBytes(string key, ReadOnlySpan<byte> value)
            {
                if (key == RejectedKey)
                {
                    throw new AdbcException("rejected", AdbcStatusCode.InvalidArgument);
                }
                BinaryOptions[key] = value.ToArray();
            }

            public override void Dispose() => Disposed = true;

            public override void SetOption(string key, string value) => Options[key] = value;

            public override IArrowArrayStream GetTableTypes() => throw new NotImplementedException();

            public override AdbcStatement CreateStatement() => throw new NotImplementedException();

            public override IArrowArrayStream GetObjects(
                GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern,
                string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
                => throw new NotImplementedException();

            public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName)
                => throw new NotImplementedException();
        }

        private sealed class ConnectingDatabase : AdbcDatabase
        {
            public IReadOnlyDictionary<string, string>? LastStringOptions { get; private set; }
            public RecordingConnection? LastConnection { get; private set; }

            public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options)
            {
                LastStringOptions = options;
                return LastConnection = new RecordingConnection();
            }
        }

        private sealed class RecordingDatabase : AdbcDatabase
        {
            public Dictionary<string, object> Options { get; } = new Dictionary<string, object>();

            public override void SetOption(string key, string value) => Options[key] = value;
            public override void SetOptionInt64(string key, long value) => Options[key] = value;
            public override void SetOptionDouble(string key, double value) => Options[key] = value;
            public override void SetOptionBytes(string key, ReadOnlySpan<byte> value) => Options[key] = value.ToArray();

            public override AdbcConnection Connect(IReadOnlyDictionary<string, string>? options) => throw new NotImplementedException();
        }

        private sealed class StringOnlyDriver : AdbcDriver
        {
            public IReadOnlyDictionary<string, string>? Parameters { get; private set; }

            public override AdbcDatabase Open(IReadOnlyDictionary<string, string> parameters)
            {
                Parameters = parameters;
                return new RecordingDatabase();
            }
        }
    }
}
