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
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using OpenTelemetry;
using OpenTelemetry.Trace;
using Xunit;
using Xunit.Abstractions;
using Apache.Arrow.Adbc.Telemetry.Traces.Exporters.FileExporter;

namespace Apache.Arrow.Adbc.Tests.Telemetry.Traces.Exporters.FileExporter
{
    public class FileExporterTests : IDisposable
    {
        private readonly ITestOutputHelper? _outputHelper;
        private bool _disposed;
        private readonly string _activitySourceName;
        private readonly ActivitySource _activitySource;
        private static readonly string s_localApplicationDataFolderPath = Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData);

        public FileExporterTests(ITestOutputHelper? outputHelper)
        {
            _outputHelper = outputHelper;
            _activitySourceName = ExportersBuilderTests.NewName();
            _activitySource = new ActivitySource(_activitySourceName);
        }

        [Fact]
        internal async Task CanSetCustomTraceFolder()
        {
            string customFolderName = ExportersBuilderTests.NewName();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);

            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                using (TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(_activitySourceName, traceFolder)
                    .Build())
                {
                    await AddEvent("test");
                }
                Assert.True(Directory.Exists(traceFolder));
                DirectoryInfo traceDirectory = new(traceFolder);
                FileInfo[] files = traceDirectory.GetFiles();
                Assert.Single(files);
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        [Fact]
        internal async Task CanSetCustomFileBaseName()
        {
            const string customFileBaseName = "custom-base-name";
            string customFolderName = ExportersBuilderTests.NewName();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);

            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                using (TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(customFileBaseName, traceFolder)
                    .Build())
                {
                    await AddEvent("test");
                }

                Assert.True(Directory.Exists(traceFolder));
                DirectoryInfo traceDirectory = new(traceFolder);
                FileInfo[] files = traceDirectory.GetFiles();
                Assert.Single(files);
                Assert.StartsWith(customFileBaseName, files[0].Name);
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        [Fact]
        internal async Task CanSetCustomMaxFileSize()
        {
            const long maxTraceFileSizeKb = 50;
            const long kilobyte = 1024;
            string customFolderName = ExportersBuilderTests.NewName();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);

            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                TracerProviderBuilder x = Sdk.CreateTracerProviderBuilder();
                using (TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(_activitySourceName, traceFolder, maxTraceFileSizeKb)
                    .Build())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        await AddEvent("test");
                        await Task.Delay(TimeSpan.FromMilliseconds(0.1));
                    }
                }

                Assert.True(Directory.Exists(traceFolder));
                DirectoryInfo traceDirectory = new(traceFolder);
                string searchPattern = _activitySourceName + "-trace-*.log";
                FileInfo[] files = [.. traceDirectory
                    .EnumerateFiles(searchPattern, SearchOption.TopDirectoryOnly)
                    .OrderByDescending(f => f.LastWriteTimeUtc)];
                //FileInfo[] files = traceDirectory.GetFiles();
                Assert.True(files.Length > 2, $"actual # of trace files: {files.Length}");
                Assert.True(files.All(f => f.Name.StartsWith(_activitySourceName)));
                StringBuilder summary = new();
                for (int i = 0; i < files.Length; i++)
                {
                    summary.AppendLine($"{i}: {files[i].Name}: {files[i].Length}: {files[i].LastWriteTimeUtc}");
                }
                for (int i = 0; i < files.Length; i++)
                {
                    long expectedUpperSizeLimit = maxTraceFileSizeKb * 2 * kilobyte;
                    Assert.True(files[i].Length < expectedUpperSizeLimit, summary.ToString());
                }
                _outputHelper?.WriteLine($"number of files: {files.Length}");
                Console.WriteLine($"number of files: {files.Length}");
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        [Fact]
        internal async Task CanSetCustomMaxFiles()
        {
            const long maxTraceFileSizeKb = 5;
            const int maxTraceFiles = 3;
            string customFolderName = ExportersBuilderTests.NewName();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);

            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                using (TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(_activitySourceName, traceFolder, maxTraceFileSizeKb, maxTraceFiles)
                    .Build())
                {
                    for (int i = 0; i < 1000; i++)
                    {
                        await AddEvent("test");
                        await Task.Delay(TimeSpan.FromMilliseconds(0.1));
                    }
                }

                Assert.True(Directory.Exists(traceFolder));
                DirectoryInfo traceDirectory = new(traceFolder);
                FileInfo[] files = traceDirectory.GetFiles();
                Assert.True(files.Length > 2, $"actual # of trace files: {files.Length}");
                Assert.True(files.Length <= maxTraceFiles, $"Expecting {maxTraceFiles} files. Actual # of files: {files.Length}");
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        [Fact]
        internal async Task CanSetSingleMaxFiles()
        {
            const long maxTraceFileSizeKb = 5;
            const int maxTraceFiles = 1;
            string customFolderName = ExportersBuilderTests.NewName();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);

            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                using (TracerProvider provider = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(_activitySourceName, traceFolder, maxTraceFileSizeKb, maxTraceFiles)
                    .Build())
                {
                    for (int i = 0; i < 100; i++)
                    {
                        await AddEvent("test");
                        await Task.Delay(TimeSpan.FromMilliseconds(0.11));
                    }
                }

                Assert.True(Directory.Exists(traceFolder));
                DirectoryInfo traceDirectory = new(traceFolder);
                FileInfo[] files = traceDirectory.GetFiles();
                Assert.Single(files);
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        [Theory]
        [InlineData("", "abc", 1, 1, typeof(ArgumentNullException))]
        [InlineData(" ", "abc", 1, 1, typeof(ArgumentNullException))]
        [InlineData("abc", "abc", 0, 1, typeof(ArgumentException))]
        [InlineData("abc", "abc", -1, 1, typeof(ArgumentException))]
        [InlineData("abc", "abc", 1, 0, typeof(ArgumentException))]
        [InlineData("abc", "abc", 1, -1, typeof(ArgumentException))]
        internal void CanDetectInvalidOptions(string fileBaseName, string? traceLocation, long maxTraceFileSizeKb, int maxTraceFiles, Type expectedException)
        {
            string customFolderName = ExportersBuilderTests.NewName();
            string? traceFolder = traceLocation != null ? Path.Combine(s_localApplicationDataFolderPath, traceLocation) : null;
            _ = Assert.Throws(expectedException, () =>
                Sdk.CreateTracerProviderBuilder()
                .AddSource(_activitySourceName)
                .AddAdbcFileExporter(fileBaseName, traceFolder, maxTraceFileSizeKb, maxTraceFiles)
                .Build());
        }

        [Fact]
        internal async Task CanTraceMultipleConcurrentWriters()
        {
            const int writeCount = 1000;
            var delay = TimeSpan.FromSeconds(8);
            string customFolderName = ExportersBuilderTests.NewName();
            string traceFolder = Path.Combine(s_localApplicationDataFolderPath, customFolderName);

            if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            try
            {
                // This simulates two drivers/connections in the same process trying to write to the same
                // trace file(s). In fact, because the FileExporter is a singleton by the combined key of
                // the activity source name and the trace file path, both providers will register only one listener.
                TracerProvider provider1 = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(_activitySourceName, traceFolder)
                    .Build();
                TracerProvider provider2 = Sdk.CreateTracerProviderBuilder()
                    .AddSource(_activitySourceName)
                    .AddAdbcFileExporter(_activitySourceName, traceFolder)
                    .Build();
                var tasks = new Task[]
                {
                    Task.Run(async () => await TraceActivities("activity1", writeCount, provider1)),
                    Task.Run(async () => await TraceActivities("activity2", writeCount, provider2)),
                };
                await Task.WhenAll(tasks);
                await Task.Delay(500);
                provider1.Dispose();
                provider2.Dispose();

                int activity1Count = 0;
                int activity2Count = 0;
                foreach (string file in Directory.GetFiles(traceFolder))
                {
                    foreach (string line in File.ReadLines(file, Encoding.UTF8))
                    {
                        if (line.Contains("activity1"))
                        {
                            activity1Count++;
                        }
                        else if (line.Contains("activity2"))
                        {
                            activity2Count++;
                        }
                    }
                }
                // Note, because we don't reference count, one of the listeners will likely
                // close the shared instance before the other is finished.
                // That can result in some events not being written.
                Assert.InRange(activity1Count, writeCount * 0.8, writeCount);
                Assert.InRange(activity2Count, writeCount * 0.8, writeCount);
            }
            finally
            {
                if (Directory.Exists(traceFolder)) Directory.Delete(traceFolder, true);
            }
        }

        private async Task TraceActivities(string activityName, int writeCount, TracerProvider provider)
        {
            for (int i = 0; i < writeCount; i++)
            {
                await StartActivity(activityName);
                await Task.Delay(TimeSpan.FromMilliseconds(0.1));
            }
            provider.ForceFlush(2000);
        }

        private Task AddEvent(string eventName, string activityName = nameof(AddEvent))
        {
            using Activity? activity = _activitySource.StartActivity(activityName);
            activity?.AddEvent(new ActivityEvent(eventName));
            return Task.CompletedTask;
        }

        private Task StartActivity(string activityName = nameof(StartActivity))
        {
            using Activity? activity = _activitySource.StartActivity(activityName);
            return Task.CompletedTask;
        }

        protected virtual void Dispose(bool disposing)
        {
            if (!_disposed)
            {
                if (disposing)
                {
                    _activitySource.Dispose();
                }
                _disposed = true;
            }
        }

        public void Dispose()
        {
            // Do not change this code. Put cleanup code in 'Dispose(bool disposing)' method
            Dispose(disposing: true);
            GC.SuppressFinalize(this);
        }
    }
}
