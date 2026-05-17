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
using System.IO;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.C;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Xunit;

namespace Apache.Arrow.Adbc.TestFixture.Tests
{
    /// <summary>
    /// Loads the AOT-published <c>Apache.Arrow.Adbc.TestFixture.Native</c> shared
    /// library through <see cref="CAdbcDriverImporter"/> and exercises both
    /// sides of the C ABI: import path (driver manager → native lib) and export
    /// path (native lib → managed fixture inside the AOT'd assembly).
    ///
    /// <para>The path to the published library is taken from the
    /// <c>ADBC_TEST_AOT_FIXTURE_PATH</c> environment variable. When unset the
    /// tests skip rather than fail, so the suite stays green in local builds
    /// that haven't run <c>dotnet publish</c> against the native project.</para>
    /// </summary>
    public class AotFixtureTests
    {
        private const string FixturePathEnvVar = "ADBC_TEST_AOT_FIXTURE_PATH";

        private static string? ResolveFixturePath()
        {
            string? path = Environment.GetEnvironmentVariable(FixturePathEnvVar);
            return string.IsNullOrEmpty(path) || !File.Exists(path) ? null : path;
        }

        private static AdbcDriver LoadFixture()
        {
            string? path = ResolveFixturePath();
            Skip.IfNot(
                path != null,
                $"Set {FixturePathEnvVar} to the AOT-published Apache.Arrow.Adbc.TestFixture.Native shared library to run this test.");
            return CAdbcDriverImporter.Load(path!);
        }

        [SkippableFact]
        public void DriverNegotiatesV1_1_0()
        {
            using AdbcDriver driver = LoadFixture();
            Assert.Equal(AdbcVersion.Version_1_1_0, driver.DriverVersion);
        }

        [SkippableFact]
        public async Task ExecuteQueryRoundTripsThroughAotBoundary()
        {
            using AdbcDriver driver = LoadFixture();
            using AdbcDatabase db = driver.Open(new Dictionary<string, string>
            {
                { "db.host", "example.com" },
            });
            using AdbcConnection conn = db.Connect(new Dictionary<string, string>
            {
                { "conn.user", "alice" },
            });
            using AdbcStatement stmt = conn.CreateStatement();
            stmt.SetOption("stmt.timeout", "30");
            stmt.SqlQuery = "SELECT * FROM fixture";

            QueryResult result = stmt.ExecuteQuery();
            using IArrowArrayStream stream = result.Stream!;
            Assert.NotNull(stream);

            // Fixture schema: key utf8 not null, value utf8 nullable
            Schema schema = stream.Schema;
            Assert.Equal(2, schema.FieldsList.Count);
            Assert.Equal("key", schema.FieldsList[0].Name);
            Assert.Equal("value", schema.FieldsList[1].Name);

            using RecordBatch? batch = await stream.ReadNextRecordBatchAsync();
            Assert.NotNull(batch);

            StringArray keys = Assert.IsType<StringArray>(batch!.Column(0));
            StringArray values = Assert.IsType<StringArray>(batch.Column(1));
            var resultMap = new Dictionary<string, string?>();
            for (int i = 0; i < batch.Length; i++)
            {
                resultMap[keys.GetString(i)] = values.GetString(i);
            }

            // Echo'd values prove the C ABI carried the inputs across.
            Assert.Equal("SELECT * FROM fixture", resultMap["sql"]);
            Assert.Equal("30", resultMap["stmt:stmt.timeout"]);
            // db.host arrived via DatabaseSetOption; conn.user via ConnectionSetOption.
            // Both go into the same option bag exposed to the statement.
            Assert.Equal("example.com", resultMap["db:db.host"]);
            Assert.Equal("alice", resultMap["db:conn.user"]);

            Assert.Null(await stream.ReadNextRecordBatchAsync());
        }

        [SkippableFact]
        public void ExecuteSchemaReturnsSchemaOnly()
        {
            using AdbcDriver driver = LoadFixture();
            using AdbcDatabase db = driver.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();
            stmt.SqlQuery = "SELECT 1";

            Schema schema = stmt.ExecuteSchema();
            Assert.Equal(2, schema.FieldsList.Count);
            Assert.Equal(ArrowTypeId.String, schema.FieldsList[0].DataType.TypeId);
        }

        [SkippableFact]
        public void GetTableTypesReturnsFixtureValues()
        {
            using AdbcDriver driver = LoadFixture();
            using AdbcDatabase db = driver.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);

            using IArrowArrayStream stream = conn.GetTableTypes();
            Assert.Equal("table_type", stream.Schema.FieldsList[0].Name);
        }

        [SkippableFact]
        public void CancelDoesNotThrow()
        {
            using AdbcDriver driver = LoadFixture();
            using AdbcDatabase db = driver.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);
            using AdbcStatement stmt = conn.CreateStatement();

            stmt.Cancel();
            conn.Cancel();
            // Success means the call dispatched across the C ABI and returned
            // AdbcStatusCode.Success rather than NotImplemented.
        }

        [SkippableFact]
        public void NotImplementedSurfaceIsReported()
        {
            using AdbcDriver driver = LoadFixture();
            using AdbcDatabase db = driver.Open(new Dictionary<string, string>());
            using AdbcConnection conn = db.Connect(null);

            // The fixture deliberately throws NotImplemented for GetObjects; verify
            // that maps back to an AdbcException with NotImplemented status.
            AdbcException ex = Assert.ThrowsAny<AdbcException>(
                () => conn.GetObjects(AdbcConnection.GetObjectsDepth.All, null, null, null, null, null));
            Assert.Equal(AdbcStatusCode.NotImplemented, ex.Status);
        }
    }
}
