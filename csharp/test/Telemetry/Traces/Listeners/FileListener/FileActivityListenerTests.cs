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

using System.Diagnostics;
using Apache.Arrow.Adbc.Telemetry.Traces.Listeners;
using Apache.Arrow.Adbc.Telemetry.Traces.Listeners.FileListener;
using Apache.Arrow.Adbc.Tracing;
using Apache.Arrow.Ipc;

namespace Apache.Arrow.Adbc.Tests.Telemetry.Traces.Listeners.FileListener
{
    public class FileActivityListenerTests
    {
        private const string TraceLocation = "adbc.trace.location";

        [Theory]
        [InlineData(null, false)]
        [InlineData("", false)]
        [InlineData(" ", false)]
        [InlineData(ListenersOptions.Exporters.None, false)]
        [InlineData(ListenersOptions.Exporters.AdbcFile, true)]
        public void TestTryActivateFileListener(string? exporterOption, bool expected)
        {
            Assert.Equal(expected, FileActivityListener.TryActivateFileListener("TestSource", exporterOption, out FileActivityListener? listener));
            Assert.True(expected == (listener != null));
            listener?.Dispose();
        }

        [Fact]
        public async Task CanTraceConcurrentConnections()
        {
            const int numConnections = 5;
            const int numActivitiesPerConnection = 1000;
            string folderLocation = Path.Combine(Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData), TracingFileTests.NewName());

            try
            {
                TestConnection[] connections = new TestConnection[numConnections];
                for (int i = 0; i < numConnections; i++)
                {
                    connections[i] = new TestConnection(new Dictionary<string, string>
                    {
                        { ListenersOptions.Exporter, ListenersOptions.Exporters.AdbcFile },
                        { TraceLocation, folderLocation },
                    });
                }

                Task[] tasks = new Task[numConnections];
                for (int i = 0; i < numConnections; i++)
                {
                    TestConnection testConnection = connections[i];
                    int connectionId = i;
                    tasks[i] = Task.Run(async () =>
                    {
                        for (int j = 0; j < numActivitiesPerConnection; j++)
                        {
                            await testConnection.EmulateWorkAsync("Key", $"Value-{connectionId}-{j}");
                        }
                    });
                }
                for (int i = 0; i < numConnections; i++)
                {
                    await tasks[i];
                    tasks[i].Dispose();
                }
                for (int i = 0; i < numConnections; i++)
                {
                    var testConnection = (TestConnection)connections[i];
                    testConnection.Dispose();
                }

                Assert.True(Directory.Exists(folderLocation));
                DirectoryInfo dirInfo = new(folderLocation);
                FileInfo[] files = dirInfo.GetFiles();
                Assert.True(files.Length > 0, "No trace files were created.");
                int totalLines = 0;
                foreach (FileInfo file in files)
                {
                    totalLines += File.ReadAllLines(file.FullName).Length;
                }
                Assert.Equal(numConnections * numActivitiesPerConnection, totalLines);
            }
            finally
            {
                if (Directory.Exists(folderLocation))
                {
                    Directory.Delete(folderLocation, recursive: true);
                }
            }
        }

        private class TestConnection : TracingConnection
        {
            private static readonly string s_assemblyName = typeof(TestConnection).Assembly.GetName().Name!;
            private static readonly string s_assemblyVersion = typeof(TestConnection).Assembly.GetName().Version!.ToString();

            private readonly string _traceId = Guid.NewGuid().ToString("N");
            private readonly FileActivityListener? _fileListener;

            public TestConnection(IReadOnlyDictionary<string, string> properties)
                : base(properties)
            {
                properties.TryGetValue(TraceLocation, out string? tracesLocation);
                properties.TryGetValue(ListenersOptions.Exporter, out string? exporterOption);
                bool shouldListenTo(ActivitySource source) => source.Tags?.Any(t => ReferenceEquals(t.Key, _traceId)) == true;
                FileActivityListener.TryActivateFileListener(ActivitySourceName, exporterOption, out _fileListener, shouldListenTo, tracesLocation: tracesLocation);
            }

            public async Task EmulateWorkAsync(string key, string value)
            {
                await this.TraceActivityAsync(async (activity) =>
                {
                    activity?.AddTag(key, value);
                    // Simulate some work
                    await Task.Yield();
                });
            }

            public override IEnumerable<KeyValuePair<string, object?>>? GetActivitySourceTags(IReadOnlyDictionary<string, string> properties)
            {
                IEnumerable<KeyValuePair<string, object?>>? tags = base.GetActivitySourceTags(properties);
                tags ??= [];
                tags = tags.Concat([new(_traceId, null)]);
                return tags;
            }

            public override string AssemblyVersion => s_assemblyVersion;

            public override string AssemblyName => s_assemblyName;

            public override AdbcStatement CreateStatement() => throw new NotImplementedException();
            public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern) => throw new NotImplementedException();
            public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName) => throw new NotImplementedException();
            public override IArrowArrayStream GetTableTypes() => throw new NotImplementedException();

            protected override void Dispose(bool disposing)
            {
                _fileListener?.Dispose();
                base.Dispose(disposing);
            }
        }
    }
}
